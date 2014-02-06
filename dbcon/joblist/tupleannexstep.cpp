/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

//  $Id: tupleannexstep.cpp 9661 2013-07-01 20:33:05Z pleblanc $


//#define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>
#ifdef _MSC_VER
#include <unordered_set>
#else
#include <tr1/unordered_set>
#endif
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
#include "constantcolumn.h"
#include "simplecolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "hasher.h"
#include "stlpoolallocator.h"
using namespace utils;

#include "funcexp.h"
#include "jobstep.h"
#include "jlf_common.h"
#include "tupleconstantstep.h"
#include "limitedorderby.h"

#include "tupleannexstep.h"

namespace
{
struct TAHasher {
	joblist::TupleAnnexStep *ts;
	utils::Hasher_r h;
	TAHasher(joblist::TupleAnnexStep *t) : ts(t) { }
	uint64_t operator()(const rowgroup::Row::Pointer &) const;
};
struct TAEq {
	joblist::TupleAnnexStep *ts;
	TAEq(joblist::TupleAnnexStep *t) : ts(t) { }
	bool operator()(const rowgroup::Row::Pointer &, const rowgroup::Row::Pointer &) const;
};
//TODO:  Generalize these and put them back in utils/common/hasher.h
typedef tr1::unordered_set<rowgroup::Row::Pointer, TAHasher, TAEq,
							STLPoolAllocator<rowgroup::Row::Pointer> > DistinctMap_t;
};

inline uint64_t TAHasher::operator()(const Row::Pointer &p) const
{
	Row &row = ts->row1;
	row.setPointer(p);
	return row.hash();
}

inline bool TAEq::operator()(const Row::Pointer &d1, const Row::Pointer &d2) const
{
	Row &r1 = ts->row1, &r2 = ts->row2;
	r1.setPointer(d1);
	r2.setPointer(d2);
	return r1.equals(r2);
}

namespace joblist
{

TupleAnnexStep::TupleAnnexStep(const JobInfo& jobInfo) :
		JobStep(jobInfo),
		fInputDL(NULL),
		fOutputDL(NULL),
		fInputIterator(0),
		fOutputIterator(0),
		fRowsProcessed(0),
		fRowsReturned(0),
		fLimitStart(0),
		fLimitCount(-1),
		fLimitHit(false),
		fEndOfResult(false),
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

		fRowGroupOut = RowGroup(oids.size(), pos, oids, keys, types, scale, precision, jobInfo.stringTableThreshold);
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
		throw logic_error("No output data list for annex step.");

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
	RGData rgDataOut;
	bool more = false;
	uint rowCount = 0;

	try
	{
		bs.restart();

		more = fOutputDL->next(fOutputIterator, &rgDataOut);
		if (traceOn() && dlTimes.FirstReadTime().tv_sec ==0)
			dlTimes.setFirstReadTime();

		if (more && !cancelled())
		{
			fRowGroupDeliver.setData(&rgDataOut);
			fRowGroupDeliver.serializeRGData(bs);
			rowCount = fRowGroupDeliver.getRowCount();
		}
		else
		{
			while (more)
				more = fOutputDL->next(fOutputIterator, &rgDataOut);
			fEndOfResult = true;
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), ERR_IN_DELIVERY, fErrorInfo, fSessionId);
		while (more)
			more = fOutputDL->next(fOutputIterator, &rgDataOut);
		fEndOfResult = true;
	}
	catch(...)
	{
		catchHandler("TupleAnnexStep next band caught an unknown exception",
					 ERR_IN_DELIVERY, fErrorInfo, fSessionId);
		while (more)
			more = fOutputDL->next(fOutputIterator, &rgDataOut);
		fEndOfResult = true;
	}

	if (fEndOfResult)
	{
		// send an empty / error band
		rgDataOut.reinit(fRowGroupDeliver, 0);
		fRowGroupDeliver.setData(&rgDataOut);
		fRowGroupDeliver.resetRowGroup(0);
		fRowGroupDeliver.setStatus(status());
		fRowGroupDeliver.serializeRGData(bs);

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
	RGData rgDataIn;
	RGData rgDataOut;
	bool more = false;

	try
	{
		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn()) dlTimes.setFirstReadTime();

		while (more && !cancelled() && !fLimitHit)
		{
			fRowGroupIn.setData(&rgDataIn);
			fRowGroupIn.getRow(0, &fRowIn);

			// Get a new output rowgroup for each input rowgroup to preserve the rids
			rgDataOut.reinit(fRowGroupOut, fRowGroupIn.getRowCount());
			fRowGroupOut.setData(&rgDataOut);
			fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());
			fRowGroupOut.setDBRoot(fRowGroupIn.getDBRoot());
			fRowGroupOut.getRow(0, &fRowOut);

			for (uint64_t i = 0; i < fRowGroupIn.getRowCount() && !cancelled() && !fLimitHit; ++i)
			{
				// skip first limit-start rows
				if (fRowsProcessed++ < fLimitStart)
				{
					fRowIn.nextRow();
					continue;
				}

				if (fConstant)
					fConstant->fillInConstants(fRowIn, fRowOut);
				else
					copyRow(fRowIn, &fRowOut);

				fRowGroupOut.incRowCount();
				if (++fRowsReturned < fLimitCount)
				{
					fRowOut.nextRow();
					fRowIn.nextRow();
				}
				else
				{
					fLimitHit = true;
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
		catchHandler(ex.what(), ERR_IN_PROCESS, fErrorInfo, fSessionId);
	}
	catch(...)
	{
		catchHandler("TupleAnnexStep execute caught an unknown exception",
					 ERR_IN_PROCESS, fErrorInfo, fSessionId);
	}

	while (more)
		more = fInputDL->next(fInputIterator, &rgDataIn);

	if (traceOn())
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
	scoped_ptr<DistinctMap_t> distinctMap(new DistinctMap_t(10, TAHasher(this), TAEq(this)));
	vector<RGData> dataVec;
	RGData rgDataIn;
	RGData rgDataOut;
	bool more = false;

	rgDataOut.reinit(fRowGroupOut);
	fRowGroupOut.setData(&rgDataOut);
	fRowGroupOut.resetRowGroup(0);
	fRowGroupOut.getRow(0, &fRowOut);

	fRowGroupOut.initRow(&row1);
	fRowGroupOut.initRow(&row2);

	try
	{
		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn()) dlTimes.setFirstReadTime();

		while (more && !cancelled() && !fLimitHit)
		{
			fRowGroupIn.setData(&rgDataIn);
			fRowGroupIn.getRow(0, &fRowIn);

			for (uint64_t i = 0; i < fRowGroupIn.getRowCount() && !cancelled() && !fLimitHit; ++i)
			{
				pair<DistinctMap_t::iterator, bool> inserted;
				if (fConstant)
					fConstant->fillInConstants(fRowIn, fRowOut);
				else
					copyRow(fRowIn, &fRowOut);

				++fRowsProcessed;
				fRowIn.nextRow();

				inserted = distinctMap->insert(fRowOut.getPointer());
				if (inserted.second) {
					fRowGroupOut.incRowCount();
					fRowOut.nextRow();
					if (UNLIKELY(++fRowsReturned >= fLimitCount))
					{
						fLimitHit = true;
						fJobList->abortOnLimit((JobStep*) this);
					}

					if (UNLIKELY(fRowGroupOut.getRowCount() >= 8192))
					{
						dataVec.push_back(rgDataOut);
						rgDataOut.reinit(fRowGroupOut);
						fRowGroupOut.setData(&rgDataOut);
						fRowGroupOut.resetRowGroup(0);
						fRowGroupOut.getRow(0, &fRowOut);
					}
				}
			}

			more = fInputDL->next(fInputIterator, &rgDataIn);
		}

		if (fRowGroupOut.getRowCount() > 0)
			dataVec.push_back(rgDataOut);

		for (vector<RGData>::iterator i = dataVec.begin(); i != dataVec.end(); i++)
		{
			rgDataOut = *i;
			fRowGroupOut.setData(&rgDataOut);
			fOutputDL->insert(rgDataOut);
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), ERR_IN_PROCESS, fErrorInfo, fSessionId);
	}
	catch(...)
	{
		catchHandler("TupleAnnexStep execute caught an unknown exception",
					 ERR_IN_PROCESS, fErrorInfo, fSessionId);
	}

	while (more)
		more = fInputDL->next(fInputIterator, &rgDataIn);

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
	RGData rgDataIn;
	RGData rgDataOut;
	bool more = false;

	try
	{
		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn()) dlTimes.setFirstReadTime();

		while (more && !cancelled())
		{
			fRowGroupIn.setData(&rgDataIn);
			fRowGroupIn.getRow(0, &fRowIn);

			for (uint64_t i = 0; i < fRowGroupIn.getRowCount() && !cancelled(); ++i)
			{
				fOrderBy->processRow(fRowIn);
				fRowIn.nextRow();
			}

			more = fInputDL->next(fInputIterator, &rgDataIn);
		}

		fOrderBy->finalize();

		if (!cancelled())
		{
			while (fOrderBy->getData(rgDataIn))
			{
				if (fConstant == NULL &&
					fRowGroupOut.getColumnCount() == fRowGroupIn.getColumnCount())
				{
					rgDataOut = rgDataIn;
					fRowGroupOut.setData(&rgDataOut);
				}
				else
				{
					fRowGroupIn.setData(&rgDataIn);
					fRowGroupIn.getRow(0, &fRowIn);

					rgDataOut.reinit(fRowGroupOut, fRowGroupIn.getRowCount());
					fRowGroupOut.setData(&rgDataOut);
					fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());
					fRowGroupOut.setDBRoot(fRowGroupIn.getDBRoot());
					fRowGroupOut.getRow(0, &fRowOut);

					for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
					{
						if (fConstant)
							fConstant->fillInConstants(fRowIn, fRowOut);
						else
							copyRow(fRowIn, &fRowOut);

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
		catchHandler(ex.what(), ERR_IN_PROCESS, fErrorInfo, fSessionId);
	}
	catch(...)
	{
		catchHandler("TupleAnnexStep execute caught an unknown exception",
					 ERR_IN_PROCESS, fErrorInfo, fSessionId);
	}

	while (more)
		more = fInputDL->next(fInputIterator, &rgDataIn);

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


void TupleAnnexStep::deliverStringTableRowGroup(bool b)
{
	fRowGroupOut.setUseStringTable(b);
	fRowGroupDeliver.setUseStringTable(b);
}


bool TupleAnnexStep::deliverStringTableRowGroup() const
{
	idbassert(fRowGroupOut.usesStringTable() == fRowGroupDeliver.usesStringTable());
	return fRowGroupDeliver.usesStringTable();
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
			<< "s;\n\tJob completion status " << status() << endl;
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

