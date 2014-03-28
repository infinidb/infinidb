/* Copyright (C) 2013 Calpont Corp.

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

//  $Id: tupleconstantstep.cpp 9649 2013-06-25 16:08:05Z xlou $


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
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "calpontsystemcatalog.h"
#include "constantcolumn.h"
using namespace execplan;

#include "jobstep.h"
#include "rowgroup.h"
using namespace rowgroup;

#include "jlf_common.h"
#include "tupleconstantstep.h"


namespace joblist
{
// static utility method
SJSTEP TupleConstantStep::addConstantStep(const JobInfo& jobInfo, const rowgroup::RowGroup* rg)
{
	TupleConstantStep* tcs = NULL;

	if (jobInfo.constantCol != CONST_COL_ONLY)
	{
		tcs = new TupleConstantStep(jobInfo);
	}
	else
	{
		tcs = new TupleConstantOnlyStep(jobInfo);
	}

	tcs->initialize(jobInfo, rg);
	SJSTEP spcs(tcs);
	return spcs;
}


TupleConstantStep::TupleConstantStep(const JobInfo& jobInfo) :
		JobStep(jobInfo),
		fRowsReturned(0),
		fInputDL(NULL),
		fOutputDL(NULL),
		fInputIterator(0),
		fEndOfResult(false)
{
	fExtendedInfo = "TCS: ";
}


TupleConstantStep::~TupleConstantStep()
{
}


void TupleConstantStep::setOutputRowGroup(const rowgroup::RowGroup& rg)
{
	throw runtime_error("Disabled, use initialize() to set output RowGroup.");
}


void TupleConstantStep::initialize(const JobInfo& jobInfo, const RowGroup* rgIn)
{
	vector<uint> oids, oidsIn = fRowGroupIn.getOIDs();
	vector<uint> keys, keysIn = fRowGroupIn.getKeys();
	vector<uint> scale, scaleIn = fRowGroupIn.getScale();
	vector<uint> precision, precisionIn = fRowGroupIn.getPrecision();
	vector<CalpontSystemCatalog::ColDataType> types, typesIn = fRowGroupIn.getColTypes();
	vector<uint> pos;
	pos.push_back(2);

	if (rgIn)
	{
		fRowGroupIn = *rgIn;
		fRowGroupIn.initRow(&fRowIn);
		oidsIn = fRowGroupIn.getOIDs();
		keysIn = fRowGroupIn.getKeys();
		scaleIn = fRowGroupIn.getScale();
		precisionIn = fRowGroupIn.getPrecision();
		typesIn = fRowGroupIn.getColTypes();
	}

	for (uint64_t i = 0, j = 0; i < jobInfo.deliveredCols.size(); i++)
	{
		const ConstantColumn* cc =
						dynamic_cast<const ConstantColumn*>(jobInfo.deliveredCols[i].get());
		if (cc != NULL)
		{
			CalpontSystemCatalog::ColType ct = cc->resultType();
			if (ct.colDataType == CalpontSystemCatalog::VARCHAR)
				ct.colWidth++;

			//Round colWidth up
			if (ct.colWidth == 3)
				ct.colWidth = 4;
			else if (ct.colWidth == 5 || ct.colWidth == 6 || ct.colWidth == 7)
				ct.colWidth = 8;

			oids.push_back(-1);
			keys.push_back(-1);
			scale.push_back(ct.scale);
			precision.push_back(ct.precision);
			types.push_back(ct.colDataType);
			pos.push_back(pos.back() + ct.colWidth);

			fIndexConst.push_back(i);
		}
		else
		{
			// select (select a) from region;
			if (j >= oidsIn.size() && jobInfo.tableList.empty())
			{
				throw IDBExcept(ERR_NO_FROM);
			}
			idbassert(j < oidsIn.size());

			oids.push_back(oidsIn[j]);
			keys.push_back(keysIn[j]);
			scale.push_back(scaleIn[j]);
			precision.push_back(precisionIn[j]);
			types.push_back(typesIn[j]);
			pos.push_back(pos.back() + fRowGroupIn.getColumnWidth(j));
			j++;

			fIndexMapping.push_back(i);
		}
	}

	fRowGroupOut = RowGroup(oids.size(), pos, oids, keys, types, scale, precision,
		jobInfo.stringTableThreshold);
	fRowGroupOut.initRow(&fRowOut);
	fRowGroupOut.initRow(&fRowConst, true);

	constructContanstRow(jobInfo);
}


void TupleConstantStep::constructContanstRow(const JobInfo& jobInfo)
{
	// construct a row with only the constant values
	fConstRowData.reset(new uint8_t[fRowConst.getSize()]);
	fRowConst.setData(fConstRowData.get());
	const vector<CalpontSystemCatalog::ColDataType>& types = fRowGroupOut.getColTypes();
	for (vector<uint64_t>::iterator i = fIndexConst.begin(); i != fIndexConst.end(); i++)
	{
		const ConstantColumn* cc =
						dynamic_cast<const ConstantColumn*>(jobInfo.deliveredCols[*i].get());
		const execplan::Result c = cc->result();

		if (cc->type() == ConstantColumn::NULLDATA)
		{
			if (types[*i] == CalpontSystemCatalog::CHAR ||
				types[*i] == CalpontSystemCatalog::VARCHAR)
			{
				fRowConst.setStringField("", *i);
			}
			else if (isUnsigned(types[*i]))
			{
				fRowConst.setUintField(fRowConst.getNullValue(*i), *i);
			}
			else
			{
				fRowConst.setIntField(fRowConst.getSignedNullValue(*i), *i);
			}

			continue;
		}


		switch (types[*i])
		{
			case CalpontSystemCatalog::TINYINT:
			case CalpontSystemCatalog::SMALLINT:
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::BIGINT:
			case CalpontSystemCatalog::DATE:
			case CalpontSystemCatalog::DATETIME:
			{
				fRowConst.setIntField(c.intVal, *i);
				break;
			}

			case CalpontSystemCatalog::DECIMAL:
			case CalpontSystemCatalog::UDECIMAL:
			{
				fRowConst.setIntField(c.decimalVal.value, *i);
				break;
			}

			case CalpontSystemCatalog::FLOAT:
			case CalpontSystemCatalog::UFLOAT:
			{
				fRowConst.setFloatField(c.floatVal, *i);
				break;
			}

			case CalpontSystemCatalog::DOUBLE:
			case CalpontSystemCatalog::UDOUBLE:
			{
				fRowConst.setDoubleField(c.doubleVal, *i);
				break;
			}

			case CalpontSystemCatalog::CHAR:
			case CalpontSystemCatalog::VARCHAR:
			{
				fRowConst.setStringField(c.strVal, *i);
				break;
			}

			case CalpontSystemCatalog::UTINYINT:
			case CalpontSystemCatalog::USMALLINT:
			case CalpontSystemCatalog::UMEDINT:
			case CalpontSystemCatalog::UINT:
			case CalpontSystemCatalog::UBIGINT:
			{
				fRowConst.setUintField(c.uintVal, *i);
				break;
			}

			default:
			{
				throw runtime_error("un-supported constant column type.");
				break;
			}
		} // switch
	} // for constant columns
}


void TupleConstantStep::run()
{
	if (fInputJobStepAssociation.outSize() == 0)
		throw logic_error("No input data list for constant step.");

	fInputDL = fInputJobStepAssociation.outAt(0)->rowGroupDL();
	if (fInputDL == NULL)
		throw logic_error("Input is not a RowGroup data list.");

	fInputIterator = fInputDL->getIterator();

	if (fDelivery == false)
	{
		if (fOutputJobStepAssociation.outSize() == 0)
			throw logic_error("No output data list for non-delivery constant step.");

		fOutputDL = fOutputJobStepAssociation.outAt(0)->rowGroupDL();
		if (fOutputDL == NULL)
			throw logic_error("Output is not a RowGroup data list.");

		fRunner.reset(new boost::thread(Runner(this)));
	}
}


void TupleConstantStep::join()
{
	if (fRunner)
		fRunner->join();
}


uint TupleConstantStep::nextBand(messageqcpp::ByteStream &bs)
{
	RGData rgDataIn;
	RGData rgDataOut;
	bool more = false;
	uint rowCount = 0;

	try
	{
		bs.restart();

		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn() && dlTimes.FirstReadTime().tv_sec == 0)
			dlTimes.setFirstReadTime();

		if (!more && cancelled())
		{
			fEndOfResult = true;
		}

		if (more && !fEndOfResult)
		{
			fRowGroupIn.setData(&rgDataIn);
			rgDataOut.reinit(fRowGroupOut, fRowGroupIn.getRowCount());
			fRowGroupOut.setData(&rgDataOut);

			fillInConstants();
			fRowGroupOut.serializeRGData(bs);
			rowCount = fRowGroupOut.getRowCount();
		}
		else
		{
			fEndOfResult = true;
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), tupleConstantStepErr, fErrorInfo, fSessionId);
		while (more)
			more = fInputDL->next(fInputIterator, &rgDataIn);
		fEndOfResult = true;
	}
	catch(...)
	{
		catchHandler("TupleConstantStep next band caught an unknown exception",
					 tupleConstantStepErr, fErrorInfo, fSessionId);
		while (more)
			more = fInputDL->next(fInputIterator, &rgDataIn);
		fEndOfResult = true;
	}

	if (fEndOfResult)
	{
		// send an empty / error band
		RGData rgData(fRowGroupOut, 0);
		fRowGroupOut.setData(&rgData);
		fRowGroupOut.resetRowGroup(0);
		fRowGroupOut.setStatus(status());
		fRowGroupOut.serializeRGData(bs);

		if (traceOn())
		{
			dlTimes.setLastReadTime();
			dlTimes.setEndOfInputTime();
			printCalTrace();
		}
	}

	return rowCount;
}


void TupleConstantStep::execute()
{
	RGData rgDataIn;
	RGData rgDataOut;
	bool more = false;

	try
	{
		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn()) dlTimes.setFirstReadTime();

		if (!more && cancelled())
		{
			fEndOfResult = true;
		}

		while (more && !fEndOfResult)
		{
			fRowGroupIn.setData(&rgDataIn);
			rgDataOut.reinit(fRowGroupOut, fRowGroupIn.getRowCount());
			fRowGroupOut.setData(&rgDataOut);

			fillInConstants();

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
		catchHandler(ex.what(), tupleConstantStepErr, fErrorInfo, fSessionId);
	}
	catch(...)
	{
		catchHandler("TupleConstantStep execute caught an unknown exception",
					 tupleConstantStepErr, fErrorInfo, fSessionId);
	}

//	if (!fEndOfResult)
		while (more)
			more = fInputDL->next(fInputIterator, &rgDataIn);

	// Bug 3136, let mini stats to be formatted if traceOn.
	if (traceOn())
	{
		dlTimes.setLastReadTime();
		dlTimes.setEndOfInputTime();
		printCalTrace();
	}

	fEndOfResult = true;
	fOutputDL->endOfInput();
}


void TupleConstantStep::fillInConstants()
{
	fRowGroupIn.getRow(0, &fRowIn);
	fRowGroupOut.getRow(0, &fRowOut);

	if (fIndexConst.size() > 1 || fIndexConst[0] != 0)
	{
		for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
		{
			copyRow(fRowConst, &fRowOut);

			fRowOut.setRid(fRowIn.getRelRid());
			for (uint64_t j = 0; j < fIndexMapping.size(); ++j)
				fRowIn.copyField(fRowOut, fIndexMapping[j], j);

			fRowIn.nextRow();
			fRowOut.nextRow();
		}
	}
	else // only first column is constant
	{
		//size_t n = fRowOut.getOffset(fRowOut.getColumnCount()) - fRowOut.getOffset(1);
		for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
		{
			fRowOut.setRid(fRowIn.getRelRid());
			fRowConst.copyField(fRowOut, 0, 0);
			for (uint i = 1; i < fRowOut.getColumnCount(); i++)
				fRowIn.copyField(fRowOut, i, i-1);

			fRowIn.nextRow();
			fRowOut.nextRow();
		}
	}

	fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());
	fRowGroupOut.setRowCount(fRowGroupIn.getRowCount());
	fRowsReturned += fRowGroupOut.getRowCount();
}


void TupleConstantStep::fillInConstants(const rowgroup::Row& rowIn, rowgroup::Row& rowOut)
{
	if (fIndexConst.size() > 1 || fIndexConst[0] != 0)
	{
		copyRow(fRowConst, &rowOut);
		//memcpy(rowOut.getData(), fRowConst.getData(), fRowConst.getSize());
		rowOut.setRid(rowIn.getRelRid());
		for (uint64_t j = 0; j < fIndexMapping.size(); ++j)
			rowIn.copyField(rowOut, fIndexMapping[j], j);
			//rowIn.copyField(rowOut.getData() + rowOut.getOffset(fIndexMapping[j]), j);
	}
	else // only first column is constant
	{
		//size_t n = rowOut.getOffset(rowOut.getColumnCount()) - rowOut.getOffset(1);
		rowOut.setRid(rowIn.getRelRid());
		fRowConst.copyField(rowOut, 0, 0);
		//fRowConst.copyField(rowOut.getData()+2, 0); // hardcoded 2 for rid length
		for (uint i = 1; i < rowOut.getColumnCount(); i++)
			rowIn.copyField(rowOut, i, i-1);
		//memcpy(rowOut.getData()+rowOut.getOffset(1), rowIn.getData()+2, n);
	}
}


const RowGroup& TupleConstantStep::getOutputRowGroup() const
{
	return fRowGroupOut;
}


const RowGroup& TupleConstantStep::getDeliveredRowGroup() const
{
	return fRowGroupOut;
}


void TupleConstantStep::deliverStringTableRowGroup(bool b)
{
	fRowGroupOut.setUseStringTable(b);
}


bool TupleConstantStep::deliverStringTableRowGroup() const
{
	return fRowGroupOut.usesStringTable();
}


const string TupleConstantStep::toString() const
{
	ostringstream oss;
	oss << "ConstantStep   ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
		oss << fInputJobStepAssociation.outAt(i);

	oss << " out:";
	for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
		oss << fOutputJobStepAssociation.outAt(i);

	oss << endl;

	return oss.str();
}


void TupleConstantStep::printCalTrace()
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


void TupleConstantStep::formatMiniStats()
{
	ostringstream oss;
	oss << "TCS "
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


// class TupleConstantOnlyStep
TupleConstantOnlyStep::TupleConstantOnlyStep(const JobInfo& jobInfo) : TupleConstantStep(jobInfo)
{
//	fExtendedInfo = "TCOS: ";
}


TupleConstantOnlyStep::~TupleConstantOnlyStep()
{
}


void TupleConstantOnlyStep::initialize(const RowGroup& rgIn, const JobInfo& jobInfo)
{
	vector<uint> oids;
	vector<uint> keys;
	vector<uint> scale;
	vector<uint> precision;
	vector<CalpontSystemCatalog::ColDataType> types;
	vector<uint> pos;
	pos.push_back(2);

	for (uint64_t i = 0; i < jobInfo.deliveredCols.size(); i++)
	{
		const ConstantColumn* cc =
						dynamic_cast<const ConstantColumn*>(jobInfo.deliveredCols[i].get());
		if (cc == NULL)
			throw runtime_error("none constant column found.");

		CalpontSystemCatalog::ColType ct = cc->resultType();
		if (ct.colDataType == CalpontSystemCatalog::VARCHAR)
			ct.colWidth++;

		//Round colWidth up
		if (ct.colWidth == 3)
			ct.colWidth = 4;
		else if (ct.colWidth == 5 || ct.colWidth == 6 || ct.colWidth == 7)
			ct.colWidth = 8;

		oids.push_back(-1);
		keys.push_back(-1);
		scale.push_back(ct.scale);
		precision.push_back(ct.precision);
		types.push_back(ct.colDataType);
		pos.push_back(pos.back() + ct.colWidth);

		fIndexConst.push_back(i);
	}

	fRowGroupOut = RowGroup(oids.size(), pos, oids, keys, types, scale, precision, jobInfo.stringTableThreshold);
	fRowGroupOut.initRow(&fRowOut);
	fRowGroupOut.initRow(&fRowConst, true);

	constructContanstRow(jobInfo);
}


void TupleConstantOnlyStep::run()
{
	if (fDelivery == false)
	{
		if (fOutputJobStepAssociation.outSize() == 0)
			throw logic_error("No output data list for non-delivery constant step.");

		fOutputDL = fOutputJobStepAssociation.outAt(0)->rowGroupDL();
		if (fOutputDL == NULL)
			throw logic_error("Output is not a RowGroup data list.");

		try
		{
			RGData rgDataOut(fRowGroupOut, 1);
			fRowGroupOut.setData(&rgDataOut);

			if (traceOn()) dlTimes.setFirstReadTime();
			fillInConstants();

			fOutputDL->insert(rgDataOut);
		}
		catch(...)
		{
			catchHandler("TupleConstantOnlyStep run caught an unknown exception",
						 tupleConstantStepErr, fErrorInfo, fSessionId);
		}

		if (traceOn())
		{
			dlTimes.setLastReadTime();
			dlTimes.setEndOfInputTime();
			printCalTrace();
		}

		// Bug 3136, let mini stats to be formatted if traceOn.
		fEndOfResult = true;
		fOutputDL->endOfInput();
	}
}


uint TupleConstantOnlyStep::nextBand(messageqcpp::ByteStream &bs)
{
	RGData rgDataOut;
	uint rowCount = 0;

	if (!fEndOfResult)
	{
		try
		{
			bs.restart();

			if (traceOn() && dlTimes.FirstReadTime().tv_sec == 0)
				dlTimes.setFirstReadTime();

			rgDataOut.reinit(fRowGroupOut, 1);
			fRowGroupOut.setData(&rgDataOut);

			fillInConstants();
			fRowGroupOut.serializeRGData(bs);
			rowCount = fRowGroupOut.getRowCount();
		}
		catch(const std::exception& ex)
		{
			catchHandler(ex.what(), tupleConstantStepErr, fErrorInfo, fSessionId);
		}
		catch(...)
		{
			catchHandler("TupleConstantStep next band caught an unknown exception",
						 tupleConstantStepErr, fErrorInfo, fSessionId);
		}

		fEndOfResult = true;
	}
	else
	{
		// send an empty / error band
		RGData rgData(fRowGroupOut, 0);
		fRowGroupOut.setData(&rgData);
		fRowGroupOut.resetRowGroup(0);
		fRowGroupOut.setStatus(status());
		fRowGroupOut.serializeRGData(bs);

		if (traceOn())
		{
			dlTimes.setLastReadTime();
			dlTimes.setEndOfInputTime();
			printCalTrace();
		}
	}

	return rowCount;
}


void TupleConstantOnlyStep::fillInConstants()
{
	fRowGroupOut.getRow(0, &fRowOut);
	idbassert(fRowConst.getSize() == fRowOut.getSize());
	copyRow(fRowConst, &fRowOut);
	fRowGroupOut.resetRowGroup(0);
	fRowGroupOut.setRowCount(1);
	fRowsReturned = 1;
}


const string TupleConstantOnlyStep::toString() const
{
	ostringstream oss;
	oss << "ConstantOnlyStep ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

	oss << " out:";
	for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
		oss << fOutputJobStepAssociation.outAt(i);

	oss << endl;

	return oss.str();
}


// class TupleConstantBooleanStep
TupleConstantBooleanStep::TupleConstantBooleanStep(const JobInfo& jobInfo, bool value) :
	TupleConstantStep(jobInfo),
	fValue(value)
{
//	fExtendedInfo = "TCBS: ";
}


TupleConstantBooleanStep::~TupleConstantBooleanStep()
{
}


void TupleConstantBooleanStep::initialize(const RowGroup& rgIn, const JobInfo&)
{
	fRowGroupOut = rgIn;
	fRowGroupOut.initRow(&fRowOut);
	fRowGroupOut.initRow(&fRowConst, true);
}


void TupleConstantBooleanStep::run()
{
	if (fDelivery == false)
	{
		if (fOutputJobStepAssociation.outSize() == 0)
			throw logic_error("No output data list for non-delivery constant step.");

		fOutputDL = fOutputJobStepAssociation.outAt(0)->rowGroupDL();
		if (fOutputDL == NULL)
			throw logic_error("Output is not a RowGroup data list.");

		if (traceOn())
		{
			dlTimes.setFirstReadTime();
			dlTimes.setLastReadTime();
			dlTimes.setEndOfInputTime();
			printCalTrace();
		}

		// Bug 3136, let mini stats to be formatted if traceOn.
		fOutputDL->endOfInput();
	}
}


uint TupleConstantBooleanStep::nextBand(messageqcpp::ByteStream &bs)
{
	// send an empty band
	RGData rgData(fRowGroupOut, 0);
	fRowGroupOut.setData(&rgData);
	fRowGroupOut.resetRowGroup(0);
	fRowGroupOut.setStatus(status());
	fRowGroupOut.serializeRGData(bs);

	if (traceOn())
	{
		dlTimes.setFirstReadTime();
		dlTimes.setLastReadTime();
		dlTimes.setEndOfInputTime();
		printCalTrace();
	}

	return 0;
}


const string TupleConstantBooleanStep::toString() const
{
	ostringstream oss;
	oss << "ConstantBooleanStep ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

	oss << " out:";
	for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
		oss << fOutputJobStepAssociation.outAt(i);

	oss << endl;

	return oss.str();
}


}   //namespace
// vim:ts=4 sw=4:

