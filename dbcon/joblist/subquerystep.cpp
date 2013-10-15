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

SubQueryStep::SubQueryStep(const JobInfo& jobInfo)
	: JobStep(jobInfo)
	, fRowsReturned(0)
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
			<< "s;\n\tJob completion status " << status() << endl;
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


SubAdapterStep::SubAdapterStep(SJSTEP& s, const JobInfo& jobInfo)
	: JobStep(jobInfo)
	, fTableOid(s->tableOid())
	, fSubStep(s)
	, fRowsReturned(0)
	, fEndOfResult(false)
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
		throw logic_error("No input data list for subquery adapter step.");

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
	RGData rgDataOut;
	bool more = false;
	uint rowCount = 0;

	try
	{
		bs.restart();
		
		more = fOutputDL->next(fOutputIterator, &rgDataOut);
		if (!more || cancelled())
		{
			//@bug4459.
			while (more)
				more = fOutputDL->next(fOutputIterator, &rgDataOut);			
			fEndOfResult = true;
		}

		if (more && !fEndOfResult)
		{
			fRowGroupDeliver.setData(&rgDataOut);
			fRowGroupDeliver.serializeRGData(bs);
			rowCount = fRowGroupDeliver.getRowCount();
		}
	}
	catch(const std::exception& ex)
	{
		catchHandler(ex.what(), fSessionId);
		if (status() == 0)
			status(ERR_IN_DELIVERY);
		while (more)
			more = fOutputDL->next(fOutputIterator, &rgDataOut);
		fEndOfResult = true;
	}
	catch(...)
	{
		catchHandler("SubAdapterStep next band caught an unknown exception", fSessionId);
		if (status() == 0)
			status(ERR_IN_DELIVERY);
		while (more)
			more = fOutputDL->next(fOutputIterator, &rgDataOut);
		fEndOfResult = true;
	}

	if (fEndOfResult)
	{
		// send an empty / error band
		RGData rgData(fRowGroupDeliver, 0);
		fRowGroupDeliver.setData(&rgData);
		fRowGroupDeliver.resetRowGroup(0);
		fRowGroupDeliver.setStatus(status());
		fRowGroupDeliver.serializeRGData(bs);
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


void SubAdapterStep::deliverStringTableRowGroup(bool b)
{
	fRowGroupOut.setUseStringTable(b);
	fRowGroupDeliver.setUseStringTable(b);
}


bool SubAdapterStep::deliverStringTableRowGroup() const
{
	idbassert(fRowGroupOut.usesStringTable() == fRowGroupDeliver.usesStringTable());
	return fRowGroupDeliver.usesStringTable();
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
	RGData rgDataIn;
	RGData rgDataOut;
	Row rowIn;
	Row rowFe;
	Row rowOut;
	fRowGroupIn.initRow(&rowIn);
	fRowGroupOut.initRow(&rowOut);

	RGData rowFeData;
	bool usesFE = false;
	if (fRowGroupFe.getColumnCount() != (uint) -1)
	{
		usesFE = true;
		fRowGroupFe.initRow(&rowFe, true);
		rowFeData = RGData(fRowGroupFe, 1);
		fRowGroupFe.setData(&rowFeData);
		fRowGroupFe.getRow(0, &rowFe);
	}

	bool more = false;
	try
	{
		fSubStep->run();

		more = fInputDL->next(fInputIterator, &rgDataIn);
		if (traceOn()) dlTimes.setFirstReadTime();

		while (more && !cancelled())
		{
			fRowGroupIn.setData(&rgDataIn);
			rgDataOut.reinit(fRowGroupOut, fRowGroupIn.getRowCount());
			fRowGroupOut.setData(&rgDataOut);
			fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());

			fRowGroupIn.getRow(0, &rowIn);
			fRowGroupOut.getRow(0, &rowOut);

			for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
			{
				if(fExpression.get() == NULL)
				{
					outputRow(rowIn, rowOut);
				}
				else if (!usesFE)
				{
					if(fExpression->evaluate(&rowIn))
					{
						outputRow(rowIn, rowOut);
					}
				}
				else
				{
					copyRow(rowIn, &rowFe, rowIn.getColumnCount());
					//memcpy(rowFe.getData(), rowIn.getData(), rowIn.getSize());
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
		if (status() == 0)
			status(ERR_EXEMGR_MALFUNCTION);
	}
	catch(...)
	{
		catchHandler("SubAdapterStep execute caught an unknown exception", fSessionId);
		if (status() == 0)
			status(ERR_EXEMGR_MALFUNCTION);
	}

	if (cancelled())
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
			<< "s;\n\tJob completion status " << status() << endl;
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

