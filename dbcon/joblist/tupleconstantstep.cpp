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

//  $Id: tupleconstantstep.cpp 7396 2011-02-03 17:54:36Z rdempsey $


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
		
		tcs = new TupleConstantStep(JobStepAssociation(jobInfo.status),
									JobStepAssociation(jobInfo.status),
									jobInfo.sessionId,
									jobInfo.txnId,
									jobInfo.verId,
									jobInfo.statementId);
	}
	else
	{
		tcs = new TupleConstantOnlyStep(JobStepAssociation(jobInfo.status),
										JobStepAssociation(jobInfo.status),
										jobInfo.sessionId,
										jobInfo.txnId,
										jobInfo.verId,
										jobInfo.statementId);
	}

	tcs->initialize(jobInfo, rg);
	SJSTEP spcs(tcs);
	return spcs;
}


TupleConstantStep::TupleConstantStep(
	const JobStepAssociation& inputJobStepAssociation,
	const JobStepAssociation& outputJobStepAssociation,
	uint32_t sessionId,
	uint32_t txnId,
	uint32_t verId,
	uint32_t statementId) :
		fInputJobStepAssociation(inputJobStepAssociation),
		fOutputJobStepAssociation(outputJobStepAssociation),
		fSessionId(sessionId),
		fTxnId(txnId),
		fVerId(verId),
		fStepId(0),
		fStatementId(statementId),
		fRowsReturned(0),
		fInputDL(NULL),
		fOutputDL(NULL),
		fInputIterator(0),
		fEndOfResult(false),
		fDelivery(false)
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
			assert(j < oidsIn.size());

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

	fRowGroupOut = RowGroup(oids.size(), pos, oids, keys, types, scale, precision);
	fRowGroupOut.initRow(&fRowOut);
	fRowGroupOut.initRow(&fRowConst);

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
				fRowConst.setStringField("", *i);
			else
				fRowConst.setIntField(fRowConst.getSignedNullValue(*i), *i);

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
			{
				fRowConst.setIntField(c.decimalVal.value, *i);
				break;
			}

			case CalpontSystemCatalog::FLOAT:
			{
				fRowConst.setFloatField(c.floatVal, *i);
				break;
			}

			case CalpontSystemCatalog::DOUBLE:
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
	shared_array<uint8_t> rgDataIn;
	shared_array<uint8_t> rgDataOut;
	bool more = false;
	uint rowCount = 0;

	try
	{
		bs.restart();

		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn() && dlTimes.FirstReadTime().tv_sec == 0)
			dlTimes.setFirstReadTime();

		if (!more && (0 < fInputJobStepAssociation.status() || die))
		{
			fEndOfResult = true;
		}

		if (more && !fEndOfResult)
		{
			fRowGroupIn.setData(rgDataIn.get());
			rgDataOut.reset(new uint8_t[fRowGroupOut.getDataSize(fRowGroupIn.getRowCount())]);
			fRowGroupOut.setData(rgDataOut.get());

			fillInConstants();
			bs.load(fRowGroupOut.getData(), fRowGroupOut.getDataSize());
			rowCount = fRowGroupOut.getRowCount();
		}
		else
		{
			fEndOfResult = true;
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(tupleConstantStepErr);
		while (more) more = fInputDL->next(fInputIterator, &rgDataIn);
		fEndOfResult = true;
	}
	catch(...)
	{
		catchHandler("TupleConstantStep next band caught an unknown exception", fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(tupleConstantStepErr);
		while (more) more = fInputDL->next(fInputIterator, &rgDataIn);
		fEndOfResult = true;
	}

	if (fEndOfResult)
	{
		// send an empty / error band
		shared_array<uint8_t> rgData(new uint8_t[fRowGroupOut.getEmptySize()]);
		fRowGroupOut.setData(rgData.get());
		fRowGroupOut.resetRowGroup(0);
		fRowGroupOut.setStatus(fOutputJobStepAssociation.status());
		bs.load(rgData.get(), fRowGroupOut.getDataSize());

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
	shared_array<uint8_t> rgDataIn;
	shared_array<uint8_t> rgDataOut;
	bool more = false;

	try
	{
		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn()) dlTimes.setFirstReadTime();

		if (!more && (0 < fInputJobStepAssociation.status() || die))
		{
			fEndOfResult = true;
		}

		while (more && !fEndOfResult)
		{
			fRowGroupIn.setData(rgDataIn.get());
			rgDataOut.reset(new uint8_t[fRowGroupOut.getDataSize(fRowGroupIn.getRowCount())]);
			fRowGroupOut.setData(rgDataOut.get());

			fillInConstants();

			more = fInputDL->next(fInputIterator, &rgDataIn);
			if (0 < fInputJobStepAssociation.status() || die)
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
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(tupleConstantStepErr);
	}
	catch(...)
	{
		catchHandler("TupleConstantStep execute caught an unknown exception", fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(tupleConstantStepErr);
	}

	if (!fEndOfResult)
		while (more) more = fInputDL->next(fInputIterator, &rgDataIn);

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


void TupleConstantStep::fillInConstants()
{
	fRowGroupIn.getRow(0, &fRowIn);
	fRowGroupOut.getRow(0, &fRowOut);

	if (fIndexConst.size() > 1 || fIndexConst[0] != 0)
	{
		for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
		{
			memcpy(fRowOut.getData(), fRowConst.getData(), fRowConst.getSize());

			fRowOut.setRid(fRowIn.getRid());
			for (uint64_t j = 0; j < fIndexMapping.size(); ++j)
				fRowIn.copyField(fRowOut.getData() + fRowOut.getOffset(fIndexMapping[j]), j);

			fRowIn.nextRow();
			fRowOut.nextRow();
		}
	}
	else // only first column is constant
	{
		size_t n = fRowOut.getOffset(fRowOut.getColumnCount()) - fRowOut.getOffset(1);
		for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
		{
			fRowOut.setRid(fRowIn.getRid());
			fRowConst.copyField(fRowOut.getData()+2, 0); // hardcoded 2 for rid length
			memcpy(fRowOut.getData()+fRowOut.getOffset(1), fRowIn.getData()+2, n);

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
		memcpy(rowOut.getData(), fRowConst.getData(), fRowConst.getSize());
		rowOut.setRid(rowIn.getRid());
		for (uint64_t j = 0; j < fIndexMapping.size(); ++j)
			rowIn.copyField(rowOut.getData() + rowOut.getOffset(fIndexMapping[j]), j);
	}
	else // only first column is constant
	{
		size_t n = rowOut.getOffset(rowOut.getColumnCount()) - rowOut.getOffset(1);
		rowOut.setRid(rowIn.getRid());
		fRowConst.copyField(rowOut.getData()+2, 0); // hardcoded 2 for rid length
		memcpy(rowOut.getData()+rowOut.getOffset(1), rowIn.getData()+2, n);
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
	timeString[ strlen (timeString )-1] = '\0';
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
TupleConstantOnlyStep::TupleConstantOnlyStep(
	const JobStepAssociation& inputJobStepAssociation,
	const JobStepAssociation& outputJobStepAssociation,
	uint32_t sessionId,
	uint32_t txnId,
	uint32_t verId,
	uint32_t statementId)  :  TupleConstantStep(inputJobStepAssociation,
												outputJobStepAssociation,
												sessionId,
												txnId,
												verId,
												statementId)
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

	fRowGroupOut = RowGroup(oids.size(), pos, oids, keys, types, scale, precision);
	fRowGroupOut.initRow(&fRowOut);
	fRowGroupOut.initRow(&fRowConst);

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
			shared_array<uint8_t> rgDataOut;
			rgDataOut.reset(new uint8_t[fRowGroupOut.getDataSize(1)]);
			fRowGroupOut.setData(rgDataOut.get());

			if (traceOn()) dlTimes.setFirstReadTime();
			fillInConstants();

			fOutputDL->insert(rgDataOut);
		}
		catch(...)
		{
			catchHandler("TupleConstantOnlyStep run caught an unknown exception", fSessionId);
			if (fOutputJobStepAssociation.status() == 0)
				fOutputJobStepAssociation.status(tupleConstantStepErr);
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
	shared_array<uint8_t> rgDataOut;
	uint rowCount = 0;

	if (!fEndOfResult)
	{
		try
		{
			bs.restart();

			if (traceOn() && dlTimes.FirstReadTime().tv_sec == 0)
				dlTimes.setFirstReadTime();

			rgDataOut.reset(new uint8_t[fRowGroupOut.getDataSize(1)]);
			fRowGroupOut.setData(rgDataOut.get());

			fillInConstants();
			bs.load(fRowGroupOut.getData(), fRowGroupOut.getDataSize());
			rowCount = fRowGroupOut.getRowCount();
		}
		catch(const std::exception& ex)
		{
			catchHandler(ex.what(), fSessionId);
			if (fOutputJobStepAssociation.status() == 0)
				fOutputJobStepAssociation.status(tupleConstantStepErr);
		}
		catch(...)
		{
			catchHandler("TupleConstantStep next band caught an unknown exception", fSessionId);
			if (fOutputJobStepAssociation.status() == 0)
				fOutputJobStepAssociation.status(tupleConstantStepErr);
		}

		fEndOfResult = true;
	}
	else
	{
		// send an empty / error band
		shared_array<uint8_t> rgData(new uint8_t[fRowGroupOut.getEmptySize()]);
		fRowGroupOut.setData(rgData.get());
		fRowGroupOut.resetRowGroup(0);
		fRowGroupOut.setStatus(fOutputJobStepAssociation.status());
		bs.load(rgData.get(), fRowGroupOut.getDataSize());

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
	memcpy(fRowOut.getData(), fRowConst.getData(), fRowConst.getSize());
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
TupleConstantBooleanStep::TupleConstantBooleanStep(
	const JobStepAssociation& inputJobStepAssociation,
	const JobStepAssociation& outputJobStepAssociation,
	uint32_t sessionId,
	uint32_t txnId,
	uint32_t verId,
	uint32_t statementId,
	bool value) : TupleConstantStep(inputJobStepAssociation,
									outputJobStepAssociation,
									sessionId,
									txnId,
									verId,
									statementId),
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
	fRowGroupOut.initRow(&fRowConst);
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
	shared_array<uint8_t> rgData(new uint8_t[fRowGroupOut.getEmptySize()]);
	fRowGroupOut.setData(rgData.get());
	fRowGroupOut.resetRowGroup(0);
	fRowGroupOut.setStatus(fOutputJobStepAssociation.status());
	bs.load(rgData.get(), fRowGroupOut.getDataSize());

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

