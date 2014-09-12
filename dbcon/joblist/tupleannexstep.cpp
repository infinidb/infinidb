/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

//  $Id: tupleannexstep.cpp 9662 2013-07-01 20:34:26Z pleblanc $


//#define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>
using namespace std;

#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
using namespace boost;

#include "messagequeue.h"
using namespace messageqcpp;

#include "loggingid.h"
#include "errorcodes.h"
using namespace logging;

#include "calpontsystemcatalog.h"
#include "aggregatecolumn.h"
#include "constantcolumn.h"
#include "simplecolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "hasher.h"
#include "simpleallocator.h"
using namespace utils;

#include "funcexp.h"
#include "jobstep.h"
#include "jlf_common.h"
#include "tupleconstantstep.h"
#include "limitedorderby.h"

#include "tupleannexstep.h"

namespace
{
typedef tr1::unordered_map<uint8_t*, uint8_t*, TupleHasher, TupleComparator,
							SimpleAllocator<pair<uint8_t* const, uint8_t*> > > DistinctMap_t;
};


namespace joblist
{

TupleAnnexStep::TupleAnnexStep(
	uint32_t sessionId,
	uint32_t txnId,
	uint32_t verId,
	uint32_t statementId,
	const JobInfo& jobInfo) :
		fSessionId(sessionId),
		fTxnId(txnId),
		fVerId(verId),
		fStatementId(statementId),
		fInputDL(NULL),
		fOutputDL(NULL),
		fInputIterator(0),
		fOutputIterator(0),
		fRowsReturned(0),
		fEndOfResult(false),
		fDelivery(false),
		fDistinct(false),
		fOrderBy(NULL),
		fConstant(NULL),
		fFeInstance(funcexp::FuncExp::instance()),
		fJobList(jobInfo.jobListPtr)
{
	fExtendedInfo = "TXS: ";
}


TupleAnnexStep::~TupleAnnexStep()
{
	if (fOrderBy)
		delete fOrderBy;
	fOrderBy = NULL;

	if (fConstant)
		delete fConstant;
	fConstant = NULL;
}


void TupleAnnexStep::setOutputRowGroup(const rowgroup::RowGroup& rg)
{
	throw runtime_error("Disabled, use initialize() to set output RowGroup.");
}


void TupleAnnexStep::initialize(const RowGroup& rgIn, const JobInfo& jobInfo)
{
	fRowGroupIn = rgIn;
	fRowGroupIn.initRow(&fRowIn);

	if (fOrderBy)
	{
		fOrderBy->distinct(fDistinct);
		fOrderBy->initialize(rgIn, jobInfo);
	}

	if (fConstant == NULL)
	{
		vector<uint> oids, oidsIn = fRowGroupIn.getOIDs();
		vector<uint> keys, keysIn = fRowGroupIn.getKeys();
		vector<uint> scale, scaleIn = fRowGroupIn.getScale();
		vector<uint> precision, precisionIn = fRowGroupIn.getPrecision();
		vector<CalpontSystemCatalog::ColDataType> types, typesIn = fRowGroupIn.getColTypes();
		vector<uint> pos, posIn = fRowGroupIn.getOffsets();

		size_t n = jobInfo.nonConstDelCols.size();
		oids.insert(oids.end(), oidsIn.begin(), oidsIn.begin() + n);
		keys.insert(keys.end(), keysIn.begin(), keysIn.begin() + n);
		scale.insert(scale.end(), scaleIn.begin(), scaleIn.begin() + n);
		precision.insert(precision.end(), precisionIn.begin(), precisionIn.begin() + n);
		types.insert(types.end(), typesIn.begin(), typesIn.begin() + n);
		pos.insert(pos.end(), posIn.begin(), posIn.begin() + n + 1);

		fRowGroupOut = RowGroup(oids.size(), pos, oids, keys, types, scale, precision);
	}
	else
	{
		fConstant->initialize(jobInfo, &rgIn);
		fRowGroupOut = fConstant->getOutputRowGroup();
	}

	fRowGroupOut.initRow(&fRowOut);
	fRowGroupDeliver = fRowGroupOut;
}


void TupleAnnexStep::run()
{
	if (fInputJobStepAssociation.outSize() == 0)
		throw logic_error("No input data list for annex step.");

	fInputDL = fInputJobStepAssociation.outAt(0)->rowGroupDL();
	if (fInputDL == NULL)
		throw logic_error("Input is not a RowGroup data list.");

	fInputIterator = fInputDL->getIterator();

	if (fOutputJobStepAssociation.outSize() == 0)
		throw logic_error("No output data list for non-delivery annex step.");

	fOutputDL = fOutputJobStepAssociation.outAt(0)->rowGroupDL();
	if (fOutputDL == NULL)
		throw logic_error("Output is not a RowGroup data list.");

	if (fDelivery == true)
	{
		fOutputIterator = fOutputDL->getIterator();
	}

	fRunner.reset(new boost::thread(Runner(this)));
}


void TupleAnnexStep::join()
{
	if (fRunner)
		fRunner->join();
}


uint TupleAnnexStep::nextBand(messageqcpp::ByteStream &bs)
{
	shared_array<uint8_t> rgDataOut;
	bool more = false;
	uint rowCount = 0;

	try
	{
		bs.restart();

		more = fOutputDL->next(fOutputIterator, &rgDataOut);
		if (traceOn() && dlTimes.FirstReadTime().tv_sec ==0)
			dlTimes.setFirstReadTime();

		if (more && (0 == fOutputJobStepAssociation.status()) && !die)
		{
			fRowGroupDeliver.setData(rgDataOut.get());
			bs.load(fRowGroupDeliver.getData(), fRowGroupDeliver.getDataSize());
			rowCount = fRowGroupDeliver.getRowCount();
		}
		else
		{
			if (more) more = fOutputDL->next(fOutputIterator, &rgDataOut);
			fEndOfResult = true;
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(ERR_IN_DELIVERY);
		while (more) more = fOutputDL->next(fOutputIterator, &rgDataOut);
		fEndOfResult = true;
	}
	catch(...)
	{
		catchHandler("TupleAnnexStep next band caught an unknown exception", fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(ERR_IN_DELIVERY);
		while (more) more = fOutputDL->next(fOutputIterator, &rgDataOut);
		fEndOfResult = true;
	}

	if (fEndOfResult)
	{
		// send an empty / error band
		rgDataOut.reset(new uint8_t[fRowGroupDeliver.getEmptySize()]);
		fRowGroupDeliver.setData(rgDataOut.get());
		fRowGroupDeliver.resetRowGroup(0);
		fRowGroupDeliver.setStatus(fOutputJobStepAssociation.status());
		bs.load(rgDataOut.get(), fRowGroupDeliver.getDataSize());

		if (traceOn())
		{
			dlTimes.setLastReadTime();
			dlTimes.setEndOfInputTime();
		}

		if (traceOn())
			printCalTrace();
	}

	return rowCount;
}


void TupleAnnexStep::execute()
{
	if (fOrderBy)
		executeWithOrderBy();
	else if (fDistinct)
		executeNoOrderByWithDistinct();
	else
		executeNoOrderBy();
}


void TupleAnnexStep::executeNoOrderBy()
{
	shared_array<uint8_t> rgDataIn;
	shared_array<uint8_t> rgDataOut;
	bool more = false;
	bool hitLimit = false;

	try
	{
		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn()) dlTimes.setFirstReadTime();

		while (more && (0 == fInputJobStepAssociation.status()) && !die && !hitLimit)
		{
			fRowGroupIn.setData(rgDataIn.get());
			fRowGroupIn.getRow(0, &fRowIn);

			// Get a new output rowgroup for each input rowgroup to preserve the rids
			rgDataOut.reset(new uint8_t[fRowGroupOut.getDataSize(fRowGroupIn.getRowCount())]);
			fRowGroupOut.setData(rgDataOut.get());
			fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());
			fRowGroupOut.setDBRoot(fRowGroupIn.getDBRoot());
			fRowGroupOut.getRow(0, &fRowOut);

			for (uint64_t i = 0; i < fRowGroupIn.getRowCount() &&
					(0 == fInputJobStepAssociation.status()) && !die && !hitLimit; ++i)
			{
				if (fConstant)
					fConstant->fillInConstants(fRowIn, fRowOut);
				else
					memcpy(fRowOut.getData(), fRowIn.getData(), fRowOut.getSize());

				fRowGroupOut.incRowCount();
				if (++fRowsReturned < fLimit)
				{
					fRowOut.nextRow();
					fRowIn.nextRow();
				}
				else
				{
					hitLimit = true;
					fJobList->abortOnLimit((JobStep*) this);
				}
			}

			if (fRowGroupOut.getRowCount() > 0)
			{
				fOutputDL->insert(rgDataOut);
			}

			more = fInputDL->next(fInputIterator, &rgDataIn);
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(ERR_IN_PROCESS);
	}
	catch(...)
	{
		catchHandler("TupleAnnexStep execute caught an unknown exception", fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(ERR_IN_PROCESS);
	}

	while (more) more = fInputDL->next(fInputIterator, &rgDataIn);


	if (traceOn() && !fDelivery)
	{
		dlTimes.setLastReadTime();
		dlTimes.setEndOfInputTime();
		printCalTrace();
	}

	// Bug 3136, let mini stats to be formatted if traceOn.
	fOutputDL->endOfInput();
}


void TupleAnnexStep::executeNoOrderByWithDistinct()
{
	uint64_t keyLength = fRowOut.getSize() - 2;
	utils::TupleHasher   hasher(keyLength);
	utils::TupleComparator comp(keyLength);
	shared_ptr<utils::SimplePool> pool(new utils::SimplePool);
	utils::SimpleAllocator<pair<uint8_t* const, uint8_t*> > alloc(pool);
	scoped_ptr<DistinctMap_t> distinctMap(new DistinctMap_t(10, hasher, comp, alloc));
	vector<shared_array<uint8_t> > dataVec;
	shared_array<uint8_t> rgDataIn;
	shared_array<uint8_t> rgDataOut;
	bool more = false;
	bool hitLimit = false;

	rgDataOut.reset(new uint8_t[fRowGroupOut.getDataSize(8192)]);
	fRowGroupOut.setData(rgDataOut.get());
	fRowGroupOut.resetRowGroup(0);
	fRowGroupOut.getRow(0, &fRowOut);

	try
	{
		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn()) dlTimes.setFirstReadTime();

		while (more && (0 == fInputJobStepAssociation.status()) && !die && !hitLimit)
		{
			fRowGroupIn.setData(rgDataIn.get());
			fRowGroupIn.getRow(0, &fRowIn);

			for (uint64_t i = 0; i < fRowGroupIn.getRowCount() &&
					(0 == fInputJobStepAssociation.status()) && !die && !hitLimit; ++i)
			{
				if (fConstant)
					fConstant->fillInConstants(fRowIn, fRowOut);
				else
					memcpy(fRowOut.getData(), fRowIn.getData(), fRowOut.getSize());

				fRowIn.nextRow();

				if (distinctMap->find(fRowOut.getData()+2) == distinctMap->end())
				{
					distinctMap->insert(make_pair(fRowOut.getData()+2, fRowOut.getData()));

					fRowGroupOut.incRowCount();
					if (++fRowsReturned < fLimit)
					{
						fRowOut.nextRow();
					}
					else
					{
						hitLimit = true;
						fJobList->abortOnLimit((JobStep*) this);
					}

					if (fRowGroupOut.getRowCount() >= 8192)
					{
						dataVec.push_back(rgDataOut);
						rgDataOut.reset(new uint8_t[fRowGroupOut.getDataSize(8192)]);
						fRowGroupOut.setData(rgDataOut.get());
						fRowGroupOut.resetRowGroup(0);
						fRowGroupOut.getRow(0, &fRowOut);
					}
				}
			}

			more = fInputDL->next(fInputIterator, &rgDataIn);
		}

		if (fRowGroupOut.getRowCount() > 0)
			dataVec.push_back(rgDataOut);

		for (vector<shared_array<uint8_t> >::iterator i = dataVec.begin(); i != dataVec.end(); i++)
		{
			rgDataOut = *i;
			fRowGroupOut.setData(rgDataOut.get());
			fOutputDL->insert(rgDataOut);
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(ERR_IN_PROCESS);
	}
	catch(...)
	{
		catchHandler("TupleAnnexStep execute caught an unknown exception", fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(ERR_IN_PROCESS);
	}

	while (more) more = fInputDL->next(fInputIterator, &rgDataIn);

	if (traceOn() && !fDelivery)
	{
		dlTimes.setLastReadTime();
		dlTimes.setEndOfInputTime();
		printCalTrace();
	}

	// Bug 3136, let mini stats to be formatted if traceOn.
	fOutputDL->endOfInput();
}


void TupleAnnexStep::executeWithOrderBy()
{
	shared_array<uint8_t> rgDataIn;
	shared_array<uint8_t> rgDataOut;
	bool more = false;

	try
	{
		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn()) dlTimes.setFirstReadTime();

		while (more && (0 == fInputJobStepAssociation.status()) && !die)
		{
			fRowGroupIn.setData(rgDataIn.get());
			fRowGroupIn.getRow(0, &fRowIn);

			for (uint64_t i = 0;
				 i < fRowGroupIn.getRowCount() && (0 == fInputJobStepAssociation.status()) && !die;
				 ++i)
			{
				fOrderBy->processRow(fRowIn);
				fRowIn.nextRow();
			}

			more = fInputDL->next(fInputIterator, &rgDataIn);
		}

		fOrderBy->finalize();

		if (fOutputJobStepAssociation.status() == 0 && !die)
		{
			while (fOrderBy->getData(rgDataIn))
			{
				if (fConstant == NULL &&
					fRowGroupOut.getColumnCount() == fRowGroupIn.getColumnCount())
				{
					rgDataOut = rgDataIn;
					fRowGroupOut.setData(rgDataOut.get());
				}
				else
				{
					fRowGroupIn.setData(rgDataIn.get());
					fRowGroupIn.getRow(0, &fRowIn);

					rgDataOut.reset(new uint8_t[fRowGroupOut.getDataSize(fRowGroupIn.getRowCount())]);
					fRowGroupOut.setData(rgDataOut.get());
					fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());
					fRowGroupOut.setDBRoot(fRowGroupIn.getDBRoot());
					fRowGroupOut.getRow(0, &fRowOut);

					for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
					{
						if (fConstant)
							fConstant->fillInConstants(fRowIn, fRowOut);
						else
							memcpy(fRowOut.getData(), fRowIn.getData(), fRowOut.getSize());

						fRowGroupOut.incRowCount();
						fRowOut.nextRow();
						fRowIn.nextRow();
					}
				}

				if (fRowGroupOut.getRowCount() > 0)
				{
					fRowsReturned += fRowGroupOut.getRowCount();
					fOutputDL->insert(rgDataOut);
				}
			}
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(ERR_IN_PROCESS);
	}
	catch(...)
	{
		catchHandler("TupleAnnexStep execute caught an unknown exception", fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(ERR_IN_PROCESS);
	}

	while (more) more = fInputDL->next(fInputIterator, &rgDataIn);

	if (traceOn() && !fDelivery)
	{
		dlTimes.setLastReadTime();
		dlTimes.setEndOfInputTime();
		printCalTrace();
	}

	// Bug 3136, let mini stats to be formatted if traceOn.
	fOutputDL->endOfInput();
}


const RowGroup& TupleAnnexStep::getOutputRowGroup() const
{
	return fRowGroupOut;
}


const RowGroup& TupleAnnexStep::getDeliveredRowGroup() const
{
	return fRowGroupDeliver;
}


const string TupleAnnexStep::toString() const
{
	ostringstream oss;
	oss << "AnnexStep   ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
		oss << fInputJobStepAssociation.outAt(i);

	oss << " out:";
	for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
		oss << fOutputJobStepAssociation.outAt(i);

	if (fOrderBy)
		oss << "    " << fOrderBy->toString();
	if (fConstant)
		oss << "    " << fConstant->toString();
	oss << endl;

	return oss.str();
}


void TupleAnnexStep::printCalTrace()
{
	time_t t = time (0);
	char timeString[50];
	ctime_r (&t, timeString);
	timeString[strlen (timeString )-1] = '\0';
	ostringstream logStr;
	logStr  << "ses:" << fSessionId << " st: " << fStepId << " finished at "<< timeString
			<< "; total rows returned-" << fRowsReturned << endl
			<< "\t1st read " << dlTimes.FirstReadTimeString()
			<< "; EOI " << dlTimes.EndOfInputTimeString() << "; runtime-"
			<< JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime())
			<< "s;\n\tJob completion status " << fOutputJobStepAssociation.status() << endl;
	logEnd(logStr.str().c_str());
	fExtendedInfo += logStr.str();
	formatMiniStats();
}


void TupleAnnexStep::formatMiniStats()
{
	ostringstream oss;
	oss << "TNS "
		<< "UM "
		<< "- "
		<< "- "
		<< "- "
		<< "- "
		<< "- "
		<< "- "
		<< JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << " "
		<< fRowsReturned << " ";
	fMiniInfo += oss.str();
}


}   //namespace
// vim:ts=4 sw=4:

