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

//  $Id: tuplehavingstep.cpp 9709 2013-07-20 06:08:46Z xlou $


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

#include "jobstep.h"
#include "rowgroup.h"
using namespace rowgroup;

#include "funcexp.h"

#include "jlf_common.h"
#include "tuplehavingstep.h"


namespace joblist
{

TupleHavingStep::TupleHavingStep(const JobInfo& jobInfo) :
		ExpressionStep(jobInfo),
		fInputDL(NULL),
		fOutputDL(NULL),
		fInputIterator(0),
		fRowsReturned(0),
		fEndOfResult(false),
		fFeInstance(funcexp::FuncExp::instance())
{
	fExtendedInfo = "THS: ";
}


TupleHavingStep::~TupleHavingStep()
{
}


void TupleHavingStep::setOutputRowGroup(const rowgroup::RowGroup& rg)
{
    throw runtime_error("Disabled, use initialize() to set output RowGroup.");
}


void TupleHavingStep::initialize(const RowGroup& rgIn, const JobInfo& jobInfo)
{
	fRowGroupIn = rgIn;
	fRowGroupIn.initRow(&fRowIn);

	map<uint, uint> keyToIndexMap;
	for (uint64_t i = 0; i < fRowGroupIn.getKeys().size(); ++i)
		if (keyToIndexMap.find(fRowGroupIn.getKeys()[i]) == keyToIndexMap.end())
			keyToIndexMap.insert(make_pair(fRowGroupIn.getKeys()[i], i));
	updateInputIndex(keyToIndexMap, jobInfo);

	vector<uint> oids, oidsIn = fRowGroupIn.getOIDs();
	vector<uint> keys, keysIn = fRowGroupIn.getKeys();
	vector<uint> scale, scaleIn = fRowGroupIn.getScale();
	vector<uint> precision, precisionIn = fRowGroupIn.getPrecision();
	vector<CalpontSystemCatalog::ColDataType> types, typesIn = fRowGroupIn.getColTypes();
	vector<uint> pos, posIn = fRowGroupIn.getOffsets();

	size_t n = 0;
	RetColsVector::const_iterator i = jobInfo.deliveredCols.begin();
	while (i != jobInfo.deliveredCols.end())
		if (NULL == dynamic_cast<const ConstantColumn*>(i++->get()))
			n++;

	oids.insert(oids.end(), oidsIn.begin(), oidsIn.begin() + n);
	keys.insert(keys.end(), keysIn.begin(), keysIn.begin() + n);
	scale.insert(scale.end(), scaleIn.begin(), scaleIn.begin() + n);
	precision.insert(precision.end(), precisionIn.begin(), precisionIn.begin() + n);
	types.insert(types.end(), typesIn.begin(), typesIn.begin() + n);
	pos.insert(pos.end(), posIn.begin(), posIn.begin() + n + 1);

	fRowGroupOut = RowGroup(oids.size(), pos, oids, keys, types, scale, precision, jobInfo.stringTableThreshold);
	fRowGroupOut.initRow(&fRowOut);
}


void TupleHavingStep::expressionFilter(const ParseTree* filter, JobInfo& jobInfo)
{
	// let base class handle the simple columns
	ExpressionStep::expressionFilter(filter, jobInfo);

	// extract simple columns from parse tree
	vector<AggregateColumn*> acv;
	fExpressionFilter->walk(getAggCols, &acv);
	fColumns.insert(fColumns.end(), acv.begin(), acv.end());
}


void TupleHavingStep::run()
{
	if (fInputJobStepAssociation.outSize() == 0)
		throw logic_error("No input data list for having step.");

	fInputDL = fInputJobStepAssociation.outAt(0)->rowGroupDL();
	if (fInputDL == NULL)
		throw logic_error("Input is not a RowGroup data list.");

	fInputIterator = fInputDL->getIterator();

	if (fDelivery == false)
	{
		if (fOutputJobStepAssociation.outSize() == 0)
			throw logic_error("No output data list for non-delivery having step.");

		fOutputDL = fOutputJobStepAssociation.outAt(0)->rowGroupDL();
		if (fOutputDL == NULL)
			throw logic_error("Output is not a RowGroup data list.");

		fRunner.reset(new boost::thread(Runner(this)));
	}
}


void TupleHavingStep::join()
{
	if (fRunner)
		fRunner->join();
}


uint TupleHavingStep::nextBand(messageqcpp::ByteStream &bs)
{
	RGData rgDataIn;
	RGData rgDataOut;
	bool more = false;
	uint rowCount = 0;

	try
	{
		bs.restart();

		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (dlTimes.FirstReadTime().tv_sec ==0)
            dlTimes.setFirstReadTime();

		if (!more || cancelled())
		{
			fEndOfResult = true;
		}

		bool emptyRowGroup = true;
		while (more && !fEndOfResult && emptyRowGroup)
		{
			if (cancelled())
			{
				while (more)
					more = fInputDL->next(fInputIterator, &rgDataIn);
				break;
			}

			fRowGroupIn.setData(&rgDataIn);
			rgDataOut.reinit(fRowGroupOut, fRowGroupIn.getRowCount());
			fRowGroupOut.setData(&rgDataOut);

			doHavingFilters();

			if (fRowGroupOut.getRowCount() > 0)
			{
				emptyRowGroup = false;
				fRowGroupOut.serializeRGData(bs);
				rowCount = fRowGroupOut.getRowCount();
			}
			else
			{
				more = fInputDL->next(fInputIterator, &rgDataIn);
			}
		}

		if (!more)
		{
			fEndOfResult = true;
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), fSessionId);
		if (status() == 0)
			status(tupleHavingStepErr);
		while (more)
			more = fInputDL->next(fInputIterator, &rgDataIn);
		fEndOfResult = true;
	}
	catch(...)
	{
		catchHandler("TupleHavingStep next band caught an unknown exception", fSessionId);
		if (status() == 0)
			status(tupleHavingStepErr);
		while (more)
			more = fInputDL->next(fInputIterator, &rgDataIn);
		fEndOfResult = true;
	}

	if (fEndOfResult)
	{
		// send an empty / error band
		rgDataOut.reinit(fRowGroupOut, 0);
		fRowGroupOut.setData(&rgDataOut);
		fRowGroupOut.resetRowGroup(0);
		fRowGroupOut.setStatus(status());
		fRowGroupOut.serializeRGData(bs);

		dlTimes.setLastReadTime();
		dlTimes.setEndOfInputTime();

		if (traceOn())
			printCalTrace();
	}

	return rowCount;
}


void TupleHavingStep::execute()
{
	RGData rgDataIn;
	RGData rgDataOut;
	bool more = false;

	try
	{
		more = fInputDL->next(fInputIterator, &rgDataIn);
		dlTimes.setFirstReadTime();

		if (!more && cancelled())
		{
			fEndOfResult = true;
		}

		while (more && !fEndOfResult)
		{
			fRowGroupIn.setData(&rgDataIn);
			rgDataOut.reinit(fRowGroupOut, fRowGroupIn.getRowCount());
			fRowGroupOut.setData(&rgDataOut);

			doHavingFilters();

			more = fInputDL->next(fInputIterator, &rgDataIn);
			if (cancelled())
			{
				fEndOfResult = true;
			}
			else
			{
				fOutputDL->insert(rgDataOut);
			}
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), fSessionId);
		if (status() == 0)
			status(tupleHavingStepErr);
	}
	catch(...)
	{
		catchHandler("TupleHavingStep execute caught an unknown exception", fSessionId);
		if (status() == 0)
			status(tupleHavingStepErr);
	}

	while (more)
		more = fInputDL->next(fInputIterator, &rgDataIn);

	fEndOfResult = true;
	fOutputDL->endOfInput();

	dlTimes.setLastReadTime();
	dlTimes.setEndOfInputTime();

	if (traceOn())
		printCalTrace();
}


void TupleHavingStep::doHavingFilters()
{
	fRowGroupIn.getRow(0, &fRowIn);
	fRowGroupOut.getRow(0, &fRowOut);
	fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());

	for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
	{
		if(fFeInstance->evaluate(fRowIn, fExpressionFilter))
		{
			copyRow(fRowIn, &fRowOut);
			fRowGroupOut.incRowCount();
			fRowOut.nextRow();
		}

		fRowIn.nextRow();
	}

	fRowsReturned += fRowGroupOut.getRowCount();
}


const RowGroup& TupleHavingStep::getOutputRowGroup() const
{
	return fRowGroupOut;
}


const RowGroup& TupleHavingStep::getDeliveredRowGroup() const
{
	return fRowGroupOut;
}


void TupleHavingStep::deliverStringTableRowGroup(bool b)
{
	fRowGroupOut.setUseStringTable(b);
}


bool TupleHavingStep::deliverStringTableRowGroup() const
{
	return fRowGroupOut.usesStringTable();
}


const string TupleHavingStep::toString() const
{
	ostringstream oss;
	oss << "HavingStep   ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
		oss << fInputJobStepAssociation.outAt(i);

	oss << " out:";
	for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
		oss << fOutputJobStepAssociation.outAt(i);

	oss << endl;

	return oss.str();
}


void TupleHavingStep::printCalTrace()
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


void TupleHavingStep::formatMiniStats()
{
	fMiniInfo += "THS ";
	fMiniInfo += "UM ";
	fMiniInfo += "- ";
	fMiniInfo += "- ";
	fMiniInfo += "- ";
	fMiniInfo += "- ";
	fMiniInfo += "- ";
	fMiniInfo += "- ";
	fMiniInfo += JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(),dlTimes.FirstReadTime()) + " ";
	fMiniInfo += "- ";
}


}   //namespace
// vim:ts=4 sw=4:

