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

//  $Id: subquerystep.cpp 6370 2010-03-18 02:58:09Z xlou $


#include <iostream>
//#define NDEBUG
#include <cassert>
using namespace std;

#include <boost/scoped_array.hpp>
#include <boost/shared_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "parsetree.h"
#include "logicoperator.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "funcexp.h"

#include "jobstep.h"
#include "jlf_common.h"
#include "jlf_tuplejoblist.h"
#include "expressionstep.h"
#include "subquerystep.h"
using namespace joblist;


namespace joblist
{

SubQueryStep::SubQueryStep(
	uint32_t sessionId,
	uint32_t txnId,
	uint32_t statementId) :
		fSessionId(sessionId),
		fTxnId(txnId),
		fStepId(0),
		fStatementId(statementId),
		fRowsReturned(0)
{
	fExtendedInfo = "SQS: ";
}

SubQueryStep::~SubQueryStep()
{
}

void SubQueryStep::run()
{
	fSubJobList->doQuery();
}

void SubQueryStep::join()
{
	if (fRunner)
		fRunner->join();
}

void SubQueryStep::abort()
{
	JobStep::abort();
	fSubJobList->abort();
}

const string SubQueryStep::toString() const
{
	ostringstream oss;
	oss << "SubQueryStep    ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

	if (fOutputJobStepAssociation.outSize() > 0)
	{
		oss << " out:";
		for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
			oss << fOutputJobStepAssociation.outAt(i);
	}

	return oss.str();
}


/*
void SubQueryStep::printCalTrace()
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


void SubQueryStep::formatMiniStats()
{
	ostringstream oss;
	oss << "SQS "
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
*/


SubAdapterStep::SubAdapterStep(uint32_t sessionId, uint32_t txnId, uint32_t statementId, SJSTEP& s)
	: fSessionId(sessionId)
	, fTxnId(txnId)
	, fStepId(0)
	, fStatementId(statementId)
	, fTableOid(s->tableOid())
	, fSubStep(s)
	, fRowsReturned(0)
	, fEndOfResult(false)
	, fDelivery(false)
	, fInputIterator(0)
	, fOutputIterator(0)
{
	fAlias = s->alias();
	fView = s->view();
	fInputJobStepAssociation = s->outputAssociation();
	fRowGroupIn = dynamic_cast<SubQueryStep*>(s.get())->getOutputRowGroup();
	setOutputRowGroup(fRowGroupIn);
}


SubAdapterStep::~SubAdapterStep()
{
}

void SubAdapterStep::abort()
{
	JobStep::abort();
	if (fSubStep)
		fSubStep->abort();
}

void SubAdapterStep::run()
{
	if (fInputJobStepAssociation.outSize() == 0)
		throw logic_error("No input data list for constant step.");

	fInputDL = fInputJobStepAssociation.outAt(0)->rowGroupDL();
	if (fInputDL == NULL)
		throw logic_error("Input is not a RowGroup data list.");

	fInputIterator = fInputDL->getIterator();

	if (fOutputJobStepAssociation.outSize() == 0)
		throw logic_error("No output data list for non-delivery subquery adapter step.");

	fOutputDL = fOutputJobStepAssociation.outAt(0)->rowGroupDL();
	if (fOutputDL == NULL)
		throw logic_error("Output is not a RowGroup data list.");

	if (fDelivery)
		fOutputIterator = fOutputDL->getIterator();

	fRunner.reset(new boost::thread(Runner(this)));
}


void SubAdapterStep::join()
{
	if (fRunner)
		fRunner->join();
}


uint SubAdapterStep::nextBand(messageqcpp::ByteStream &bs)
{
	shared_array<uint8_t> rgDataOut;
	bool more = false;
	uint rowCount = 0;

	try
	{
		bs.restart();
		
		more = fOutputDL->next(fOutputIterator, &rgDataOut);
		if (!more || (0 < fOutputJobStepAssociation.status() || die))
		{
			//@bug4459.
			while (more) more = fOutputDL->next(fOutputIterator, &rgDataOut);			
			fEndOfResult = true;
		}

		if (more && !fEndOfResult)
		{
			fRowGroupDeliver.setData(rgDataOut.get());
			bs.load(rgDataOut.get(), fRowGroupDeliver.getDataSize());
			rowCount = fRowGroupDeliver.getRowCount();
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(tupleConstantStepErr);
		while (more) more = fOutputDL->next(fOutputIterator, &rgDataOut);
		fEndOfResult = true;
	}
	catch(...)
	{
		catchHandler("TupleConstantStep next band caught an unknown exception", fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(tupleConstantStepErr);
		while (more) more = fOutputDL->next(fOutputIterator, &rgDataOut);
		fEndOfResult = true;
	}

	if (fEndOfResult)
	{
		// send an empty / error band
		shared_array<uint8_t> rgData(new uint8_t[fRowGroupDeliver.getEmptySize()]);
		fRowGroupDeliver.setData(rgData.get());
		fRowGroupDeliver.resetRowGroup(0);
		fRowGroupDeliver.setStatus(fOutputJobStepAssociation.status());
		bs.load(rgData.get(), fRowGroupDeliver.getDataSize());
	}

	return rowCount;
}


void SubAdapterStep::setFeRowGroup(const rowgroup::RowGroup& rg)
{
	fRowGroupFe = rg;
}


void SubAdapterStep::setOutputRowGroup(const rowgroup::RowGroup& rg)
{
	fRowGroupOut = fRowGroupDeliver = rg;
	if (fRowGroupFe.getColumnCount() == (uint) -1)
		fIndexMap = makeMapping(fRowGroupIn, fRowGroupOut);
	else
		fIndexMap = makeMapping(fRowGroupFe, fRowGroupOut);

	checkDupOutputColumns();
}


void SubAdapterStep::checkDupOutputColumns()
{
	map<uint, uint> keymap; // map<unique col key, col index in the row group>
	fDupColumns.clear();
	const vector<uint>& keys = fRowGroupDeliver.getKeys();
	for (uint i = 0; i < keys.size(); i++)
	{
		map<uint, uint>::iterator j = keymap.find(keys[i]);
		if (j == keymap.end())
			keymap.insert(make_pair(keys[i], i));           // map key to col index
		else
			fDupColumns.push_back(make_pair(i, j->second)); // dest/src index pair
	}
}


void SubAdapterStep::dupOutputColumns(Row& row)
{
	for (uint64_t i = 0; i < fDupColumns.size(); i++)
		row.copyField(fDupColumns[i].first, fDupColumns[i].second);
}


void SubAdapterStep::outputRow(Row& rowIn, Row& rowOut)
{
	applyMapping(fIndexMap, rowIn, &rowOut);

	if (fDupColumns.size() > 0)
		dupOutputColumns(rowOut);

	fRowGroupOut.incRowCount();
	rowOut.nextRow();
}


const string SubAdapterStep::toString() const
{
	ostringstream oss;
	oss << "SubAdapterStep  ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

	if (fInputJobStepAssociation.outSize() > 0)
			oss << fInputJobStepAssociation.outAt(0);

	if (fOutputJobStepAssociation.outSize() > 0)
			oss << fOutputJobStepAssociation.outAt(0);

	return oss.str();
}


void SubAdapterStep::execute()
{
	shared_array<uint8_t> rgDataIn;
	shared_array<uint8_t> rgDataOut;
	Row rowIn;
	Row rowFe;
	Row rowOut;
	fRowGroupIn.initRow(&rowIn);
	fRowGroupOut.initRow(&rowOut);

	scoped_array<uint8_t> rowFeData;
	if (fRowGroupFe.getColumnCount() != (uint) -1)
	{
		fRowGroupFe.initRow(&rowFe);
		rowFeData.reset(new uint8_t[rowFe.getSize()]);
		rowFe.setData(rowFeData.get());
	}

	bool more = false;
	try
	{
		fSubStep->run();

		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn()) dlTimes.setFirstReadTime();

		while (more && 0 == fInputJobStepAssociation.status() && !die)
		{
			fRowGroupIn.setData(rgDataIn.get());
			rgDataOut.reset(new uint8_t[fRowGroupOut.getDataSize(fRowGroupIn.getRowCount())]);
			fRowGroupOut.setData(rgDataOut.get());

			fRowGroupIn.getRow(0, &rowIn);
			fRowGroupOut.getRow(0, &rowOut);
			fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());

			for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
			{
				if(fExpression.get() == NULL)
				{
					outputRow(rowIn, rowOut);
				}
				else if (rowFeData.get() == NULL)
				{
					if(fExpression->evaluate(&rowIn))
					{
						outputRow(rowIn, rowOut);
					}
				}
				else
				{
					memcpy(rowFe.getData(), rowIn.getData(), rowIn.getSize());
					if(fExpression->evaluate(&rowFe))
					{
						outputRow(rowFe, rowOut);
					}
				}

				rowIn.nextRow();
			}

			if (fRowGroupOut.getRowCount() > 0)
			{
				fRowsReturned += fRowGroupOut.getRowCount();
				fOutputDL->insert(rgDataOut);
			}

			more = fInputDL->next(fInputIterator, &rgDataIn);
			
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(ERR_EXEMGR_MALFUNCTION);
	}
	catch(...)
	{
		catchHandler("SubAdapterStep execute caught an unknown exception", fSessionId);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(ERR_EXEMGR_MALFUNCTION);
	}

	if (fOutputJobStepAssociation.status() > 0 || die)
		while (more) more = fInputDL->next(fInputIterator, &rgDataIn);

	if (traceOn())
	{
		dlTimes.setLastReadTime();
		dlTimes.setEndOfInputTime();
		printCalTrace();
	}

	// Bug 3136, let mini stats to be formatted if traceOn.
	fOutputDL->endOfInput();
}


void SubAdapterStep::addExpression(const JobStepVector& exps, JobInfo& jobInfo)
{
	// maps key to the index in the RG
	map<uint, uint> keyToIndexMap;
	const vector<uint>& keys = fRowGroupIn.getKeys();
	for (size_t i = 0; i < keys.size(); i++)
		keyToIndexMap[keys[i]] = i;

	// combine the expression to one parse tree
	ParseTree* filter = NULL;
	for (JobStepVector::const_iterator it = exps.begin(); it != exps.end(); it++)
	{
		ExpressionStep* e = dynamic_cast<ExpressionStep*>(it->get());
		idbassert(e);

		e->updateInputIndex(keyToIndexMap, jobInfo);
		if (filter != NULL)
		{
			ParseTree* left = filter;
			ParseTree* right = new ParseTree();
			right->copyTree(*(e->expressionFilter()));
			filter = new ParseTree(new LogicOperator("and"));
			filter->left(left);
			filter->right(right);
		}
		else
		{
			filter = new ParseTree();
			filter->copyTree(*(e->expressionFilter()));
		}
	}

	// add to the expression wrapper
	if (fExpression.get() == NULL)
		fExpression.reset(new funcexp::FuncExpWrapper());
	fExpression->addFilter(shared_ptr<execplan::ParseTree>(filter));
}


void SubAdapterStep::addExpression(const vector<SRCP>& exps)
{
	// add to the function wrapper
	if (fExpression.get() == NULL)
		fExpression.reset(new funcexp::FuncExpWrapper());

	for (vector<SRCP>::const_iterator i = exps.begin(); i != exps.end(); i++)
		fExpression->addReturnedColumn(*i);
}


void SubAdapterStep::printCalTrace()
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


void SubAdapterStep::formatMiniStats()
{
/*
	ostringstream oss;
	oss << "SAS "
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
*/
}


}
// vim:ts=4 sw=4:

