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

//   $Id: joblistfactory.cpp 9324 2013-03-21 21:30:23Z zzhu $


#include <iostream>
#include <stack>
#include <iterator>
#include <algorithm>
//#define NDEBUG
#include <cassert>
#include <vector>
#include <set>
#include <map>
#include <limits>
using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
using namespace boost;

#define JOBLISTFACTORY_DLLEXPORT
#include "joblistfactory.h"
#undef JOBLISTFACTORY_DLLEXPORT

#include "calpontexecutionplan.h"
#include "calpontselectexecutionplan.h"
#include "dbrm.h"
#include "filter.h"
#include "simplefilter.h"
#include "constantfilter.h"
#include "existsfilter.h"
#include "selectfilter.h"
#include "returnedcolumn.h"
#include "aggregatecolumn.h"
#include "groupconcatcolumn.h"
#include "arithmeticcolumn.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "simplecolumn.h"
#include "rowcolumn.h"
#include "treenodeimpl.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "configcpp.h"
using namespace config;

#include "messagelog.h"
using namespace logging;

#include "elementtype.h"
#include "joblist.h"
#include "jobstep.h"
#include "primitivestep.h"
#include "jl_logger.h"
#include "jlf_execplantojoblist.h"
#include "rowaggregation.h"
#include "tuplehashjoin.h"
#include "tupleunion.h"
#include "expressionstep.h"
#include "tupleconstantstep.h"
#include "tuplehavingstep.h"

#include "jlf_common.h"
#include "jlf_graphics.h"
#include "jlf_subquery.h"
#include "jlf_tuplejoblist.h"

#include "rowgroup.h"
using namespace rowgroup;


namespace
{
using namespace joblist;

//Find the next step downstream from *in. Assumes only the first such step is needed.
const JobStepVector::iterator getNextStep(JobStepVector::iterator& in, JobStepVector& list)
{
	JobStepVector::iterator end = list.end();

	for (unsigned i = 0; i < in->get()->outputAssociation().outSize(); ++i)
	{
		JobStepVector::iterator iter = list.begin();
		AnyDataListSPtr outAdl = in->get()->outputAssociation().outAt(i);

		while (iter != end)
		{
			if (iter != in)
			{
				AnyDataListSPtr inAdl;
				for (unsigned j = 0; j < iter->get()->inputAssociation().outSize(); j++)
				{
					inAdl = iter->get()->inputAssociation().outAt(j);
					if (inAdl.get() == outAdl.get())
						return iter;
				}
			}
			++iter;
		}
	}

	return end;
}


bool checkCombinable(JobStep* jobStepPtr)
{
	if (typeid(*(jobStepPtr)) == typeid(pColScanStep))
	{
		return true;
	}
	else if (typeid(*(jobStepPtr)) == typeid(pColStep))
	{
		return true;
	}
	else if (typeid(*(jobStepPtr)) == typeid(pDictionaryStep))
	{
		return true;
	}
	else if (typeid(*(jobStepPtr)) == typeid(PassThruStep))
	{
		return true;
	}
	else if (typeid(*(jobStepPtr)) == typeid(FilterStep))
	{
		return true;
	}

	return false;
}

//------------------------------------------------------------------------------
// Get the necessary RID size for a column OID based on the current HWM
// for that column.  Possible return values are 8 bytes or 4 bytes.
//------------------------------------------------------------------------------
uint32_t getRidSizeBasedOnHwm(
	BRM::DBRM& dbrm,
	const execplan::CalpontSystemCatalog::OID& colOid,
	int        colWidth)
{
	uint32_t sizeOfRid = 8; // default to 8 bytes

	//...Get HWM for the specified column OID
	BRM::HWM_t hwm;
	int err = dbrm.getHWM(colOid, hwm);
	if (err != 0)
	{
		std::ostringstream errmsg;
		errmsg << "Error getting HWM for column OID " << colOid << "; " << err;
		throw std::runtime_error(errmsg.str());
	}

	//...Calculate highest RID, to see if it can be held within a 4 byte
	//   unsigned integer.  For example...
	//   For an 8 byte column, hwm<=4194302 yields RID that fits in uint32_t.
	//   hwm>=4194303 yields RID that requires uint64_t.
	double d_hwm        = hwm;
	double d_blksize    = BLOCK_SIZE;
	double d_colwid     = colWidth;
	double d_highestRID = ((d_hwm + 1.0) * d_blksize) / d_colwid;
	uint64_t highestRID = (uint64_t)d_highestRID;

	if (highestRID <= numeric_limits<uint32_t>::max())
		sizeOfRid = 4;

	return sizeOfRid;
}


void projectSimpleColumn(const SimpleColumn* sc, JobStepVector& jsv, JobInfo& jobInfo)
{
	if (sc == NULL)
		throw logic_error("projectSimpleColumn: sc is null");

	CalpontSystemCatalog::OID oid = sc->oid();
	CalpontSystemCatalog::OID tbl_oid = tableOid(sc, jobInfo.csc);
	string alias(extractTableAlias(sc));
	string view(sc->viewName());
	CalpontSystemCatalog::OID dictOid = 0;
	CalpontSystemCatalog::ColType ct;
	pColStep* pcs = NULL;
	pDictionaryStep* pds = NULL;
	bool tokenOnly = false;
	TupleInfo ti;

	if (!sc->schemaName().empty())
	{
		SJSTEP sjstep;

//      always tuples after release 3.0
//		if (!jobInfo.tryTuples)
//			jobInfo.tables.insert(make_table(sc->schemaName(), sc->tableName()));

//		if (jobInfo.trace)
//			cout << "doProject Emit pCol for SimpleColumn " << oid << endl;

		ct = sc->colType();
//XXX use this before connector sets colType in sc correctly.
		if (sc->isInfiniDB())
			ct = jobInfo.csc->colType(sc->oid());
//X
		pcs = new pColStep(JobStepAssociation(jobInfo.status),
						 JobStepAssociation(jobInfo.status),
						 0,
						 jobInfo.csc,
						 oid,
						 tbl_oid,
						 ct,
						 jobInfo.sessionId,
						 jobInfo.txnId,
						 jobInfo.verId,
						 0,
						 jobInfo.statementId,
						 jobInfo.rm,
						 jobInfo.flushInterval,
						 jobInfo.isExeMgr);
		pcs->logger(jobInfo.logger);
		pcs->alias(alias);
		pcs->view(view);
		pcs->name(sc->columnName());
		pcs->cardinality(sc->cardinality());
		//pcs->setOrderRids(true);

		sjstep.reset(pcs);
		jsv.push_back(sjstep);

		dictOid = isDictCol(ct);
		ti = setTupleInfo(ct, oid, jobInfo, tbl_oid, sc, alias);
		pcs->tupleId(ti.key);

		if (dictOid > 0 && jobInfo.hasAggregation)
		{
			map<uint, bool>::iterator it =
				jobInfo.tokenOnly.find(getTupleKey(jobInfo, sc));
			if (it != jobInfo.tokenOnly.end())
				tokenOnly = it->second;
		}

		if (dictOid > 0 && !tokenOnly)
		{
			//This is a double-step step
//			if (jobInfo.trace)
//				cout << "doProject Emit pGetSignature for SimpleColumn " << dictOid << endl;

			pds = new pDictionaryStep(JobStepAssociation(jobInfo.status),
							JobStepAssociation(jobInfo.status),
							0,
							jobInfo.csc,
							dictOid,
							ct.ddn.compressionType,
							tbl_oid,
							jobInfo.sessionId,
							jobInfo.txnId,
							jobInfo.verId,
							0,
							jobInfo.statementId,
							jobInfo.rm,
							jobInfo.flushInterval);
			pds->logger(jobInfo.logger);
			jobInfo.keyInfo->dictOidToColOid[dictOid] = oid;
			pds->alias(alias);
			pds->view(view);
			pds->name(sc->columnName());
			pds->cardinality(sc->cardinality());
			//pds->setOrderRids(true);

			//Associate these two linked steps
			JobStepAssociation outJs(jobInfo.status);
			AnyDataListSPtr spdl1(new AnyDataList());
			RowGroupDL* dl1 = new RowGroupDL(1, jobInfo.fifoSize);
			spdl1->rowGroupDL(dl1);
			dl1->OID(oid);

			// not a tokenOnly column
			setTupleInfo(ct, dictOid, jobInfo, tbl_oid, sc, alias);
			jobInfo.tokenOnly[getTupleKey(jobInfo, sc)] = false;
			outJs.outAdd(spdl1);

			pcs->outputAssociation(outJs);
			pds->inputAssociation(outJs);

			sjstep.reset(pds);
			jsv.push_back(sjstep);

			oid = dictOid; // dictionary column
			ti = setTupleInfo(ct, oid, jobInfo, tbl_oid, sc, alias);
			pds->tupleId(ti.key);
			jobInfo.keyInfo->dictKeyMap[pcs->tupleId()] = ti.key;
		}
	}
	else // must be vtable mode
	{
		oid = (tbl_oid+1) + sc->colPosition();
		ct = jobInfo.vtableColTypes[UniqId(oid, sc)];
		ti = setTupleInfo(ct, oid, jobInfo, tbl_oid, sc, alias);
	}

	if (dictOid > 0 && tokenOnly)
	{
		// scale is not used by string columns
		// borrow it to indicate token is used in projection, not the real string.
		ti.scale = 8;
	}

	jobInfo.pjColList.push_back(ti);
}

const JobStepVector doProject(const RetColsVector& retCols, JobInfo& jobInfo)
{
	JobStepVector jsv;
	SJSTEP sjstep;

	for (unsigned i = 0; i < retCols.size(); i++)
	{
		const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(retCols[i].get());
		if (sc != NULL)
		{
			projectSimpleColumn(sc, jsv, jobInfo);
		}
		else
		{
			const ArithmeticColumn* ac = NULL;
			const FunctionColumn* fc = NULL;
			uint64_t eid = -1;
			CalpontSystemCatalog::ColType ct;
			ExpressionStep* es = new ExpressionStep(jobInfo.sessionId,
													jobInfo.txnId,
													jobInfo.verId,
													jobInfo.statementId);
			es->logger(jobInfo.logger);
			es->expression(retCols[i], jobInfo);
			sjstep.reset(es);

			if ((ac = dynamic_cast<const ArithmeticColumn*>(retCols[i].get())) != NULL)
			{
				eid = ac->expressionId();
				ct = ac->resultType();
			}
			else if ((fc = dynamic_cast<const FunctionColumn*>(retCols[i].get())) != NULL)
			{
				eid = fc->expressionId();
				ct = fc->resultType();
			}
			else
			{
				std::ostringstream errmsg;
				errmsg << "doProject: unhandled returned column: " << typeid(*retCols[i]).name();
				cerr << boldStart << errmsg.str() << boldStop << endl;
				throw logic_error(errmsg.str());
			}

			// set expression tuple Info
			TupleInfo ti(setExpTupleInfo(ct, eid, retCols[i].get()->alias(), jobInfo));
			uint key = ti.key;

			if (find(jobInfo.expressionVec.begin(), jobInfo.expressionVec.end(), key) ==
				jobInfo.expressionVec.end())
				jobInfo.returnedExpressions.push_back(sjstep);

			//put place hold column in projection list
			jobInfo.pjColList.push_back(ti);
		}
	}

	return jsv;
}

void checkHavingClause(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	TupleHavingStep* ths = new  TupleHavingStep(jobInfo.sessionId,
												jobInfo.txnId,
												jobInfo.verId,
												jobInfo.statementId);
	ths->logger(jobInfo.logger);
	ths->expressionFilter(csep->having(), jobInfo);
	jobInfo.havingStep.reset(ths);

	// simple columns in select clause
	set<UniqId> scInSelect;
	for (RetColsVector::iterator		i  = jobInfo.nonConstCols.begin();
										i != jobInfo.nonConstCols.end();
										i++)
	{
		SimpleColumn* sc = dynamic_cast<SimpleColumn*>(i->get());
		if (sc != NULL)
		{
			if (sc->schemaName().empty())
				sc->oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());

			scInSelect.insert(UniqId(sc));
		}
	}

	// simple columns in gruop by clause
	set<UniqId> scInGroupBy;
	for (RetColsVector::iterator  i  = csep->groupByCols().begin();
										i != csep->groupByCols().end();
										i++)
	{
		SimpleColumn* sc = dynamic_cast<SimpleColumn*>(i->get());
		if (sc != NULL)
		{
			if (sc->schemaName().empty() && sc->oid() == 0)
			{
				if (sc->colPosition() == (uint64_t) -1)
				{
					// from select subquery
					SRCP ss = csep->returnedCols()[sc->orderPos()];
					(*i) = ss;
				}
				else
				{
					sc->oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());
				}
			}

			scInGroupBy.insert(UniqId(sc));
		}
	}

	bool  aggInHaving = false;
	const vector<ReturnedColumn*>& columns = ths->columns();
	for (vector<ReturnedColumn*>::const_iterator i = columns.begin(); i != columns.end(); i++)
	{
		// evaluate aggregate columns in having
		AggregateColumn* agc = dynamic_cast<AggregateColumn*>(*i);
		if (agc)
		{
			addAggregateColumn(agc, -1, jobInfo.nonConstCols, jobInfo);
			aggInHaving = true;
		}
		else
		{
			// simple columns used in having and in group by clause must be in rowgroup
			SimpleColumn* sc = dynamic_cast<SimpleColumn*>(*i);
			if (sc != NULL)
			{
				if (sc->schemaName().empty())
					sc->oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());

				UniqId scId(sc);
				if (scInGroupBy.find(scId) != scInGroupBy.end() &&
					scInSelect.find(scId)  == scInSelect.end())
				{
					jobInfo.nonConstCols.push_back(SRCP(sc->clone()));
				}
			}
		}
	}

	if (aggInHaving == false)
	{
		// treated the same as where clause if no aggregate column in having.
		jobInfo.havingStep.reset();

		// parse the having expression
		const ParseTree* filters = csep->having();
		if (filters != 0)
		{
			filters->walk(JLF_ExecPlanToJobList::walkTree, &jobInfo);
		}

		if (!jobInfo.stack.empty())
		{
			idbassert(jobInfo.stack.size() == 1);
			jobInfo.havingStepVec = jobInfo.stack.top();
			jobInfo.stack.pop();
		}
	}
}

void preProcessFunctionOnAggregation(const vector<SimpleColumn*>& scs,
									 const vector<AggregateColumn*>& aggs,
									 JobInfo& jobInfo)
{
	// append the simple columns if not already projected
	set<UniqId> scProjected;
	for (RetColsVector::iterator i  = jobInfo.projectionCols.begin();
								 i != jobInfo.projectionCols.end();
								 i++)
	{
		SimpleColumn* sc = dynamic_cast<SimpleColumn*>(i->get());
		if (sc != NULL)
		{
			if (sc->schemaName().empty())
				sc->oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());

			scProjected.insert(UniqId(sc));
		}
	}

	for (vector<SimpleColumn*>::const_iterator i = scs.begin(); i != scs.end(); i++)
	{
		if (scProjected.find(UniqId(*i)) == scProjected.end())
		{
			jobInfo.projectionCols.push_back(SRCP((*i)->clone()));
			scProjected.insert(UniqId(*i));
		}
	}

	// append the aggregate columns in arithmetic/function cloulmn to the projection list
	for (vector<AggregateColumn*>::const_iterator i = aggs.begin(); i != aggs.end(); i++)
	{
		addAggregateColumn(*i, -1, jobInfo.projectionCols, jobInfo);
	}
}

void checkReturnedColumns(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	for (uint64_t i = 0; i < jobInfo.deliveredCols.size(); i++)
	{
		if (NULL == dynamic_cast<const ConstantColumn*>(jobInfo.deliveredCols[i].get()))
			jobInfo.nonConstCols.push_back(jobInfo.deliveredCols[i]);
	}

	// save the original delivered non constant columns
	jobInfo.nonConstDelCols = jobInfo.nonConstCols;

	if (jobInfo.nonConstCols.size() != jobInfo.deliveredCols.size())
	{
		jobInfo.constantCol = CONST_COL_EXIST;

		// bug 2531, all constant column.
		if (jobInfo.nonConstCols.size() == 0)
		{
			if (csep->columnMap().size() > 0)
				jobInfo.nonConstCols.push_back((*(csep->columnMap().begin())).second);
			else
				jobInfo.constantCol = CONST_COL_ONLY;
		}
	}

	for (uint64_t i = 0; i < jobInfo.nonConstCols.size(); i++)
	{
		AggregateColumn* agc = dynamic_cast<AggregateColumn*>(jobInfo.nonConstCols[i].get());
		if (agc)
			addAggregateColumn(agc, i, jobInfo.nonConstCols, jobInfo);
	}

	if (csep->having() != NULL)
		checkHavingClause(csep, jobInfo);

	jobInfo.projectionCols = jobInfo.nonConstCols;
	for (uint64_t i = 0; i < jobInfo.nonConstCols.size(); i++)
	{
		const ArithmeticColumn* ac =
					dynamic_cast<const ArithmeticColumn*>(jobInfo.nonConstCols[i].get());
		const FunctionColumn* fc =
					dynamic_cast<const FunctionColumn*>(jobInfo.nonConstCols[i].get());
		if (ac != NULL && ac->aggColumnList().size() > 0)
		{
			jobInfo.nonConstCols[i]->outputIndex(i);
			preProcessFunctionOnAggregation(ac->simpleColumnList(), ac->aggColumnList(), jobInfo);
		}
		else if (fc != NULL && fc->aggColumnList().size() > 0)
		{
			jobInfo.nonConstCols[i]->outputIndex(i);
			preProcessFunctionOnAggregation(fc->simpleColumnList(), fc->aggColumnList(), jobInfo);
		}
	}
}

/*
This function is to get a unique non-constant column list for grouping.
After sub-query is supported, GROUP BY column can be a column from SELECT or FROM sub-queries,
which has empty schema name, and 0 oid (if SELECT).  In order to distinguish these columns,
data member fSequence is used to indicate the column position in FROM sub-query's select list,
the table OID for sub-query vtable is assumed to CNX_VTABLE_ID, the column OIDs for that vtable
is caculated based on this table OID and column position.
The data member fOrderPos is used to indicate the column position in the outer select clause,
this value is set to -1 if the column is not selected (implicit group by). For select sub-query,
the fSequence is not set, so orderPos is used to locate the column.
*/
void checkGroupByCols(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	// order by columns may be not in the select and [group by] clause
	const CalpontSelectExecutionPlan::OrderByColumnList& orderByCols = csep->orderByCols();
	for (uint64_t i = 0; i < orderByCols.size(); i++)
	{
		if (orderByCols[i]->orderPos() == (uint64_t)(-1))
		{
			jobInfo.deliveredCols.push_back(orderByCols[i]);

			// @bug 3025
			// Append the non-aggregate orderby column to group by, if there is group by clause.
			// Duplicates will be removed by next if block.
			if (csep->groupByCols().size() > 0)
			{
				// Not an aggregate column and not an expression of aggregation.
				if (dynamic_cast<AggregateColumn*>(orderByCols[i].get()) == NULL &&
					orderByCols[i]->aggColumnList().empty())
					csep->groupByCols().push_back(orderByCols[i]);
			}
		}
	}

	if (csep->groupByCols().size() > 0)
	{
		set<UniqId> colInGroupBy;
		RetColsVector uniqGbCols;
		for (RetColsVector::iterator  i  = csep->groupByCols().begin();
											i != csep->groupByCols().end();
											i++)
		{
			// skip constant columns
			if (dynamic_cast<ConstantColumn*>(i->get()) != NULL)
				continue;

			if ((*i)->orderPos() == (uint64_t) -1)
				jobInfo.hasImplicitGroupBy = true;

			ReturnedColumn *rc = i->get();
			SimpleColumn* sc = dynamic_cast<SimpleColumn*>(rc);

			bool selectSubquery = false;
			if (sc && sc->schemaName().empty() && sc->oid() == 0)
			{
				if (sc->colPosition() == (uint64_t) -1)
				{
					// from select subquery
					// sc->orderPos() should NOT be -1 because it is a SELECT sub-query.
					SRCP ss = csep->returnedCols()[sc->orderPos()];
					(*i) = ss;
					selectSubquery = true;

					// At this point whatever sc pointed to is invalid
					// update the rc and sc
					rc = ss.get();
					sc = dynamic_cast<SimpleColumn*>(rc);
				}
				else
				{
					sc->oid(tableOid(sc, jobInfo.csc)+1+sc->colPosition());
				}
			}

			UniqId col;
			if (sc)
				col = UniqId(sc);
			else
				col = UniqId(rc->expressionId(), rc->alias(), "", "");

			if (colInGroupBy.find(col) == colInGroupBy.end() || selectSubquery)
			{
				colInGroupBy.insert(col);
				uniqGbCols.push_back(*i);
			}
		}

		if (csep->groupByCols().size() != uniqGbCols.size())
			(csep)->groupByCols(uniqGbCols);
	}
}

void checkAggregation(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	checkGroupByCols(csep, jobInfo);
	checkReturnedColumns(csep, jobInfo);
	RetColsVector& retCols = jobInfo.projectionCols;

	jobInfo.hasDistinct = csep->distinct();
	if (csep->distinct() == true)
	{
		jobInfo.hasAggregation = true;
	}
	else if (csep->groupByCols().size() > 0)
	{
		// groupby without aggregate functions is supported.
		jobInfo.hasAggregation = true;
	}
	else
	{
		for (uint64_t i = 0; i < retCols.size(); i++)
		{
			if (dynamic_cast<AggregateColumn*>(retCols[i].get()) != NULL)
			{
				jobInfo.hasAggregation = true;
				break;
			}
		}
	}
}

void updateAggregateColType(AggregateColumn* ac, const SRCP* srcp, int op, JobInfo& jobInfo)
{
	CalpontSystemCatalog::ColType ct;
	const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(srcp->get());
	const ArithmeticColumn* ar = NULL;
	const FunctionColumn* fc = NULL;
	if (sc != NULL)
		ct = sc->resultType();
	else if ((ar = dynamic_cast<const ArithmeticColumn*>(srcp->get())) != NULL)
		ct = ar->resultType();
	else if ((fc = dynamic_cast<const FunctionColumn*>(srcp->get())) != NULL)
		ct = fc->resultType();

	if (op == AggregateColumn::SUM || op == AggregateColumn::DISTINCT_SUM)
	{
		if (ct.colDataType == CalpontSystemCatalog::TINYINT ||
			ct.colDataType == CalpontSystemCatalog::SMALLINT ||
			ct.colDataType == CalpontSystemCatalog::MEDINT ||
			ct.colDataType == CalpontSystemCatalog::INT ||
			ct.colDataType == CalpontSystemCatalog::BIGINT ||
			ct.colDataType == CalpontSystemCatalog::DECIMAL)
		{
			ct.colWidth = sizeof(int64_t);
			if (ct.scale != 0)
				ct.colDataType = CalpontSystemCatalog::DECIMAL;
			else
				ct.colDataType = CalpontSystemCatalog::BIGINT;
			ct.precision = 19;
		}
	}
	else if (op == AggregateColumn::STDDEV_POP || op == AggregateColumn::STDDEV_SAMP ||
			 op == AggregateColumn::VAR_POP    || op == AggregateColumn::VAR_SAMP)
	{
		ct.colWidth = sizeof(double);
		ct.colDataType = CalpontSystemCatalog::DOUBLE;
		ct.scale = 0;
		ct.precision = 0;
	}
	else
	{
		ct = ac->resultType();
	}

	ac->resultType(ct);

	// update the original if this aggregate column is cloned from function on aggregation
	pair<multimap<ReturnedColumn*, ReturnedColumn*>::iterator,
		 multimap<ReturnedColumn*, ReturnedColumn*>::iterator> range =
			jobInfo.cloneAggregateColMap.equal_range(ac);
	for (multimap<ReturnedColumn*, ReturnedColumn*>::iterator i=range.first; i!=range.second; ++i)
		(i->second)->resultType(ct);
}


const JobStepVector doAggProject(const CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	map<uint, uint> projectColMap;   // projected column map    -- unique
	RetColsVector pcv;               // projected column vector -- may have duplicates

	// add the groupby cols in the front part of the project column vector (pcv)
	const CalpontSelectExecutionPlan::GroupByColumnList& groupByCols = csep->groupByCols();
	uint64_t lastGroupByPos = 0;
	for (uint64_t i = 0; i < groupByCols.size(); i++)
	{
		pcv.push_back(groupByCols[i]);
		lastGroupByPos++;

		const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(groupByCols[i].get());
		const ArithmeticColumn* ac = NULL;
		const FunctionColumn* fc = NULL;
		if (sc != NULL)
		{
			CalpontSystemCatalog::OID gbOid = sc->oid();
			CalpontSystemCatalog::OID tblOid = tableOid(sc, jobInfo.csc);
			CalpontSystemCatalog::OID dictOid = 0;
			CalpontSystemCatalog::ColType ct;
			if (!sc->schemaName().empty())
			{
				ct = sc->colType();
//XXX use this before connector sets colType in sc correctly.
				if (sc->isInfiniDB())
					ct = jobInfo.csc->colType(sc->oid());
//X
				dictOid = isDictCol(ct);
			}
			else
			{
				gbOid = (tblOid+1) + sc->colPosition();
				ct = jobInfo.vtableColTypes[UniqId(gbOid, sc)];
			}

			// As of bug3695, make sure varbinary is not used in group by.
			if (ct.colDataType == CalpontSystemCatalog::VARBINARY)
				throw runtime_error ("VARBINARY in group by is not supported.");

			string alias(extractTableAlias(sc));
			string view(sc->viewName());
			TupleInfo ti(setTupleInfo(ct, gbOid, jobInfo, tblOid, sc, alias));
			uint tupleKey = ti.key;
			if (projectColMap.find(tupleKey) == projectColMap.end())
				projectColMap[tupleKey] = i;

			// for dictionary columns, replace the token oid with string oid
			if (dictOid > 0)
			{
				jobInfo.tokenOnly[tupleKey] = false;
				ti = setTupleInfo(ct, dictOid, jobInfo, tblOid, sc, alias);
				jobInfo.keyInfo->dictKeyMap[tupleKey] = ti.key;
				tupleKey = ti.key;
			}
			jobInfo.groupByColVec.push_back(tupleKey);
		}
		else if ((ac = dynamic_cast<const ArithmeticColumn*>(groupByCols[i].get())) != NULL)
		{
			uint64_t eid = ac->expressionId();
			CalpontSystemCatalog::ColType ct = ac->resultType();
			TupleInfo ti(setExpTupleInfo(ct, eid, ac->alias(), jobInfo));
			uint tupleKey = ti.key;
			jobInfo.groupByColVec.push_back(tupleKey);
			if (projectColMap.find(tupleKey) == projectColMap.end())
				projectColMap[tupleKey] = i;
		}
		else if ((fc = dynamic_cast<const FunctionColumn*>(groupByCols[i].get())) != NULL)
		{
			uint64_t eid = fc->expressionId();
			CalpontSystemCatalog::ColType ct = fc->resultType();
			TupleInfo ti(setExpTupleInfo(ct, eid, fc->alias(), jobInfo));
			uint tupleKey = ti.key;
			jobInfo.groupByColVec.push_back(tupleKey);
			if (projectColMap.find(tupleKey) == projectColMap.end())
				projectColMap[tupleKey] = i;
		}
		else
		{
			std::ostringstream errmsg;
			errmsg << "doAggProject: unsupported group by column: "
					 << typeid(*groupByCols[i]).name();
			cerr << boldStart << errmsg.str() << boldStop << endl;
			throw logic_error(errmsg.str());
		}
	}

	// process the returned columns
	RetColsVector& retCols = jobInfo.projectionCols;
	for (uint64_t i = 0; i < retCols.size(); i++)
	{
		GroupConcatColumn* gcc = dynamic_cast<GroupConcatColumn*>(retCols[i].get());
		if (gcc != NULL)
		{
			const SRCP* srcp = &(gcc->functionParms());
			const RowColumn* rcp = dynamic_cast<const RowColumn*>(srcp->get());

			const vector<SRCP>& cols = rcp->columnVec();
			for (vector<SRCP>::const_iterator j = cols.begin(); j != cols.end(); j++)
			{
				if (dynamic_cast<const ConstantColumn*>(j->get()) == NULL)
					retCols.push_back(*j);
			}

			vector<SRCP>& orderCols = gcc->orderCols();
			for (vector<SRCP>::iterator k = orderCols.begin(); k != orderCols.end(); k++)
			{
				if (dynamic_cast<const ConstantColumn*>(k->get()) == NULL)
					retCols.push_back(*k);
			}

			continue;
		}

		const SRCP* srcp = &(retCols[i]);
		const AggregateColumn* ag = dynamic_cast<const AggregateColumn*>(retCols[i].get());
		if (ag != NULL)
			srcp = &(ag->functionParms());

		const ArithmeticColumn* ac = dynamic_cast<const ArithmeticColumn*>(srcp->get());
		const FunctionColumn* fc = dynamic_cast<const FunctionColumn*>(srcp->get());
		if (ac != NULL || fc != NULL)
		{
			// bug 3728, make a dummy expression step for each expression.
			scoped_ptr<ExpressionStep> es(new ExpressionStep(jobInfo.sessionId,
															 jobInfo.txnId,
															 jobInfo.verId,
															 jobInfo.statementId));
			es->logger(jobInfo.logger);
			es->expression(*srcp, jobInfo);
		}
	}

	map<uint, CalpontSystemCatalog::OID> dictMap; // bug 1853, the tupleKey - dictoid map
	for (uint64_t i = 0; i < retCols.size(); i++)
	{
		const SRCP* srcp = &(retCols[i]);
		const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(srcp->get());
		bool doDistinct = (csep->distinct() && csep->groupByCols().empty());
		uint tupleKey = -1;
		string alias;
		string view;

		// returned column could be groupby column, a simplecoulumn not a agregatecolumn
		int op = 0;
		CalpontSystemCatalog::OID dictOid = 0;
		CalpontSystemCatalog::ColType ct, aggCt;

		if (sc == NULL)
		{
			GroupConcatColumn* gcc = dynamic_cast<GroupConcatColumn*>(retCols[i].get());
			if (gcc != NULL)
			{
				jobInfo.groupConcatCols.push_back(retCols[i]);

				uint64_t eid = gcc->expressionId();
				ct = gcc->resultType();
				TupleInfo ti(setExpTupleInfo(ct, eid, gcc->alias(), jobInfo));
				tupleKey = ti.key;
				jobInfo.returnedColVec.push_back(make_pair(tupleKey, gcc->aggOp()));

				continue;
			}

			AggregateColumn* ac = dynamic_cast<AggregateColumn*>(retCols[i].get());
			if (ac != NULL)
			{
				srcp = &(ac->functionParms());
				sc = dynamic_cast<const SimpleColumn*>(srcp->get());
				if (ac->constCol().get() != NULL)
				{
					// replace the aggregate on constant with a count(*)
					SRCP clone(new AggregateColumn(*ac, ac->sessionID()));
					jobInfo.constAggregate.insert(make_pair(i, clone));
					ac->aggOp(AggregateColumn::COUNT_ASTERISK);
					ac->distinct(false);
				}
				op = ac->aggOp();
				doDistinct = ac->distinct();
				updateAggregateColType(ac, srcp, op, jobInfo);
				aggCt = ac->resultType();

				// As of bug3695, make sure varbinary is not used in aggregation.
				if (sc != NULL && sc->resultType().colDataType == CalpontSystemCatalog::VARBINARY)
					throw runtime_error ("VARBINARY in aggregate function is not supported.");
			}
		}

		// simple column selected or aggregated
		if (sc != NULL)
		{
			// one column only need project once
			CalpontSystemCatalog::OID retOid = sc->oid();
			CalpontSystemCatalog::OID tblOid = tableOid(sc, jobInfo.csc);
			alias = extractTableAlias(sc);
			view = sc->viewName();
			if (!sc->schemaName().empty())
			{
				ct = sc->colType();
//XXX use this before connector sets colType in sc correctly.
				if (sc->isInfiniDB())
					ct = jobInfo.csc->colType(sc->oid());
//X
				dictOid = isDictCol(ct);
			}
			else
			{
				retOid = (tblOid+1) + sc->colPosition();
				ct = jobInfo.vtableColTypes[UniqId(retOid, sc)];
			}

			TupleInfo ti(setTupleInfo(ct, retOid, jobInfo, tblOid, sc, alias));
			tupleKey = ti.key;

			// this is a string column
			if (dictOid > 0)
			{
				map<uint, bool>::iterator findit = jobInfo.tokenOnly.find(tupleKey);
				// if the column has never seen, and the op is count: possible need count only.
				if (AggregateColumn::COUNT == op || AggregateColumn::COUNT_ASTERISK == op)
				{
					if (findit == jobInfo.tokenOnly.end())
						jobInfo.tokenOnly[tupleKey] = true;
				}
				// if aggregate other than count, token is not enough.
				else if (op != 0 || doDistinct)
				{
					jobInfo.tokenOnly[tupleKey] = false;
				}

				findit = jobInfo.tokenOnly.find(tupleKey);
				if (!(findit != jobInfo.tokenOnly.end() && findit->second == true))
				{
					dictMap[tupleKey] = dictOid;
					jobInfo.keyInfo->dictOidToColOid[dictOid] = retOid;
					ti = setTupleInfo(ct, dictOid, jobInfo, tblOid, sc, alias);
					jobInfo.keyInfo->dictKeyMap[tupleKey] = ti.key;
				}
			}
		}
		else
		{
			const ArithmeticColumn* ac = NULL;
			const FunctionColumn* fc = NULL;
			bool hasAggCols = false;
			if ((ac = dynamic_cast<const ArithmeticColumn*>(srcp->get())) != NULL)
			{
				if (ac->aggColumnList().size() > 0)
					hasAggCols = true;
			}
			else if ((fc = dynamic_cast<const FunctionColumn*>(srcp->get())) != NULL)
			{
				if (fc->aggColumnList().size() > 0)
					hasAggCols = true;
			}
			else
			{
				std::ostringstream errmsg;
				errmsg << "doAggProject: unsupported column: " << typeid(*(srcp->get())).name();
				cerr << boldStart << errmsg.str() << boldStop << endl;
				throw logic_error(errmsg.str());
			}

			uint64_t eid = srcp->get()->expressionId();
			ct = srcp->get()->resultType();
			TupleInfo ti(setExpTupleInfo(ct, eid, srcp->get()->alias(), jobInfo));
			tupleKey = ti.key;
			if (hasAggCols)
				jobInfo.expressionVec.push_back(tupleKey);
		}

		// add to project list
		if (projectColMap.find(tupleKey) == projectColMap.end())
		{
			RetColsVector::iterator it = pcv.end();
			if (doDistinct)
				it = pcv.insert(pcv.begin()+lastGroupByPos++, *srcp);
			else
				it = pcv.insert(pcv.end(), *srcp);

			projectColMap[tupleKey] = distance(pcv.begin(), it);
		}
		else if (doDistinct) // @bug4250, move forward distinct column if necessary.
		{
			uint pos = projectColMap[tupleKey];
			if (pos >= lastGroupByPos)
			{
				pcv[pos] = pcv[lastGroupByPos];
				pcv[lastGroupByPos] = *srcp;

				// @bug4935, update the projectColMap after swapping
				map<uint, uint>::iterator j = projectColMap.begin();
				for (; j != projectColMap.end(); j++)
					if (j->second == lastGroupByPos)
						j->second = pos;

				projectColMap[tupleKey] = lastGroupByPos;
				lastGroupByPos++;
			}
		}

		if (doDistinct && dictOid > 0)
			tupleKey = jobInfo.keyInfo->dictKeyMap[tupleKey];

		// remember the columns to be returned
		jobInfo.returnedColVec.push_back(make_pair(tupleKey, op));
		if (op == AggregateColumn::AVG || op == AggregateColumn::DISTINCT_AVG)
			jobInfo.scaleOfAvg[tupleKey] = (ct.scale << 8) + aggCt.scale;

		// bug 1499 distinct processing, save unique distinct columns
		if (doDistinct &&
			(jobInfo.distinctColVec.end() ==
				find(jobInfo.distinctColVec.begin(), jobInfo.distinctColVec.end(), tupleKey)))
		{
			jobInfo.distinctColVec.push_back(tupleKey);
		}
	}


	// for dictionary columns not count only, replace the token oid with string oid
	for (vector<pair<uint32_t, int> >::iterator it = jobInfo.returnedColVec.begin();
			it != jobInfo.returnedColVec.end(); it++)
	{
		// if the column is a dictionary column and not count only
		bool tokenOnly = false;
		map<uint, bool>::iterator i = jobInfo.tokenOnly.find(it->first);
		if (i != jobInfo.tokenOnly.end())
			tokenOnly = i->second;

		if (dictMap.find(it->first) != dictMap.end() && !tokenOnly)
		{
			uint tupleKey = jobInfo.keyInfo->dictKeyMap[it->first];
			int op = it->second;
			*it = make_pair(tupleKey, op);
		}
	}

	return doProject(pcv, jobInfo);
}


template <typename T>
class Uniqer : public unary_function<typename T::value_type, void>
{
private:
	typedef typename T::mapped_type Mt_;
	class Pred : public unary_function<const Mt_, bool>
	{
	public:
		Pred(const Mt_& retCol) : fRetCol(retCol) { }
		bool operator()(const Mt_ rc) const
		{
			return fRetCol->sameColumn(rc.get());
		}
	private:
 		const Mt_& fRetCol;
	};
public:
	void operator()(typename T::value_type mapItem)
	{
		Pred pred(mapItem.second);
		RetColsVector::iterator iter;
		iter = find_if(fRetColsVec.begin(), fRetColsVec.end(), pred);
		if (iter == fRetColsVec.end())
		{
			//Add this ReturnedColumn
			fRetColsVec.push_back(mapItem.second);
		}
	}
	RetColsVector fRetColsVec;
};

uint16_t numberSteps(JobStepVector& steps, uint16_t stepNo, uint32_t flags)
{
	JobStepVector::iterator iter = steps.begin();
	JobStepVector::iterator end = steps.end();
	while (iter != end)
	{
		// don't number the delimiters
		//if (dynamic_cast<OrDelimiter*>(iter->get()) != NULL)
		//{
		//	++iter;
		//	continue;
		//}

		JobStep* pJobStep = iter->get();
		pJobStep->stepId(stepNo);
		pJobStep->setTraceFlags(flags);
		stepNo++;
		++iter;
	}
	return stepNo;
}

void changePcolStepToPcolScan(JobStepVector::iterator& it, JobStepVector::iterator& end)
{
	pColStep* colStep = dynamic_cast<pColStep*>(it->get());
	pColScanStep* scanStep = 0;
	//Might be a pDictionaryScan step
	if (colStep)
	{
		scanStep = new pColScanStep(*colStep);
	}
	else
	{
		//If we have a pDictionaryScan-pColStep duo, then change the pColStep
		if (typeid(*(it->get())) == typeid(pDictionaryScan) &&
			distance(it, end) > 1 &&
			typeid(*((it + 1)->get())) == typeid(pColStep))
		{
			++it;
			colStep = dynamic_cast<pColStep*>(it->get());
			scanStep = new pColScanStep(*colStep);
		}
	}

	if (scanStep)
	{
		it->reset(scanStep);
	}
}

uint shouldSort(const JobStep* inJobStep, int colWidth)
{
	//only pColStep and pColScan have colType
	const pColStep *inStep = dynamic_cast<const pColStep*>(inJobStep);
	if (inStep && colWidth > inStep->colType().colWidth)
	{
		return 1;
	}
	const pColScanStep *inScan = dynamic_cast<const pColScanStep*>(inJobStep);
	if (inScan && colWidth > inScan->colType().colWidth)
	{
		return 1;
	}
	return 0;
}

void convertPColStepInProjectToPassThru(JobStepVector& psv, JobInfo& jobInfo)
{
	for (JobStepVector::iterator iter = psv.begin(); iter != psv.end(); ++iter)
	{
		if (typeid(*(iter->get())) == typeid(pColStep))
		{
			JobStepAssociation ia = iter->get()->inputAssociation();
			DataList_t* fifoDlp = ia.outAt(0).get()->dataList();
			pColStep* colStep = dynamic_cast<pColStep*>(iter->get());

			if (fifoDlp)
			{
				if (iter->get()->oid() >= 3000 && iter->get()->oid() == fifoDlp->OID())
				{
					PassThruStep* pts = 0;
					pts = new PassThruStep(*colStep, jobInfo.isExeMgr);
					pts->alias(colStep->alias());
					pts->view(colStep->view());
					pts->name(colStep->name());
					pts->tupleId(iter->get()->tupleId());
					iter->reset(pts);
				}
			}
		}
	}
}

// optimize filter order
//   perform none string filters first because string filter joins the tokens.
void optimizeFilterOrder(JobStepVector& qsv)
{
	// move all none string filters
	uint64_t pdsPos = 0;
//	int64_t  orbranch = 0;
	for (; pdsPos < qsv.size(); ++pdsPos)
	{
		// skip the or branches
//		OrDelimiterLhs* lhs = dynamic_cast<OrDelimiterLhs*>(qsv[pdsPos].get());
//		if (lhs != NULL)
//		{
//			orbranch++;
//			continue;
//		}
//
//		if (orbranch > 0)
//		{
//			UnionStep* us = dynamic_cast<UnionStep*>(qsv[pdsPos].get());
//			if (us)
//				orbranch--;
//		}
//		else
		{
			pDictionaryScan* pds = dynamic_cast<pDictionaryScan*>(qsv[pdsPos].get());
			if (pds)
				break;
		}
	}

	// no pDictionaryScan step
	if (pdsPos >= qsv.size())
		return;

	// get the filter steps that are not in or branches
	vector<uint64_t> pcolIdVec;
	JobStepVector pcolStepVec;
//	orbranch = 0;
	for (uint64_t i = pdsPos; i < qsv.size(); ++i)
	{
//		OrDelimiterLhs* lhs = dynamic_cast<OrDelimiterLhs*>(qsv[pdsPos].get());
//		if (lhs != NULL)
//		{
//			orbranch++;
//			continue;
//		}

//		if (orbranch > 0)
//		{
//			UnionStep* us = dynamic_cast<UnionStep*>(qsv[pdsPos].get());
//			if (us)
//				orbranch--;
//		}
//		else
		{
			pColStep *pcol = dynamic_cast<pColStep*>(qsv[i].get());
			if (pcol != NULL && pcol->filterCount() > 0)
				pcolIdVec.push_back(i);
		}
	}

	for (vector<uint64_t>::reverse_iterator r = pcolIdVec.rbegin(); r < pcolIdVec.rend(); ++r)
	{
		pcolStepVec.push_back(qsv[*r]);
		qsv.erase(qsv.begin() + (*r));
	}

	qsv.insert(qsv.begin() + pdsPos, pcolStepVec.rbegin(), pcolStepVec.rend());
}

void exceptionHandler(JobList* joblist, const JobInfo& jobInfo, const string& logMsg,
	logging::LOG_TYPE logLevel = LOG_TYPE_ERROR)
{
	cerr << "### JobListFactory ses:" << jobInfo.sessionId << " caught: " << logMsg << endl;
	Message::Args args;
	args.add(logMsg);
	jobInfo.logger->logMessage(logLevel, LogMakeJobList, args,
		LoggingID(5, jobInfo.sessionId, jobInfo.txnId, 0));
	// dummy delivery map, workaround for (qb == 2) in main.cpp
	DeliveredTableMap dtm;
	SJSTEP dummyStep;
	dtm[0] = dummyStep;
	joblist->addDelivery(dtm);
}


void parseExecutionPlan(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
	JobStepVector& querySteps, JobStepVector& projectSteps, DeliveredTableMap& deliverySteps)
{
	const ParseTree* filters = csep->filters();
	jobInfo.deliveredCols = csep->returnedCols();
	if (filters != 0)
	{
		filters->walk(JLF_ExecPlanToJobList::walkTree, &jobInfo);
	}

	if (jobInfo.trace)
		cout << endl << "Stack: " << endl;

	if (!jobInfo.stack.empty())
	{
		idbassert(jobInfo.stack.size() == 1);
		querySteps = jobInfo.stack.top();
		jobInfo.stack.pop();

		// do some filter order optimization
		optimizeFilterOrder(querySteps);
	}

	if (jobInfo.selectAndFromSubs.size() > 0)
	{
		querySteps.insert(querySteps.begin(),
							jobInfo.selectAndFromSubs.begin(), jobInfo.selectAndFromSubs.end());
	}

	// bug3391, move forward the aggregation check for no aggregte having clause.
	checkAggregation(csep, jobInfo);

	// include filters in having clause, if any.
	if (jobInfo.havingStepVec.size() > 0)
		querySteps.insert(querySteps.begin(),
                       		jobInfo.havingStepVec.begin(), jobInfo.havingStepVec.end());

	//Need to change the leading pColStep to a pColScanStep
	//Keep a list of the (table OIDs,alias) that we've already processed for @bug 598 self-join
	set<uint> seenTableIds;

	//Stack of seenTables to make sure the left-hand side and right-hand have the same content
	stack<set<uint> > seenTableStack;

	if (!querySteps.empty())
	{
		JobStepVector::iterator iter = querySteps.begin();
		JobStepVector::iterator end = querySteps.end();
		for (; iter != end; ++iter)
		{
			idbassert(iter->get());

			// As of bug3695, make sure varbinary is not used in filters.
			if (typeid(*(iter->get())) == typeid(pColStep))
			{
				// only pcolsteps, no pcolscan yet.
				pColStep* pcol = dynamic_cast<pColStep*>(iter->get());
				if (pcol->colType().colDataType == CalpontSystemCatalog::VARBINARY)
				{
					if (pcol->filterCount() != 1)
						throw runtime_error ("VARBINARY in filter or function is not supported.");

					// error out if the filter is not "is null" or "is not null"
					// should block "= null" and "!= null" ???
					messageqcpp::ByteStream filter = pcol->filterString();
					uint8_t op = 0;
					filter >> op;
					bool nullOp = (op == COMPARE_EQ || op == COMPARE_NE || op == COMPARE_NIL);
					filter >> op;  // skip roundFlag
					uint64_t value = 0;
					filter >> value;
					nullOp = nullOp && (value == 0xfffffffffffffffeULL);

					if (nullOp == false)
						throw runtime_error ("VARBINARY in filter or function is not supported.");
				}
			}

//			// save the current seentable for right-hand side
//			if (typeid(*(iter->get())) == typeid(OrDelimiterLhs))
//			{
//				seenTableStack.push(seenTableIds);
//				continue;
//			}
//
//			// restore the seentable
//			else if (typeid(*(iter->get())) == typeid(OrDelimiterRhs))
//			{
//				seenTableIds = seenTableStack.top();
//				seenTableStack.pop();
//				continue;
//			}

			pColStep* colStep = dynamic_cast<pColStep*>(iter->get());
			if (colStep != NULL)
			{
				string alias(colStep->alias());
				string view(colStep->view());
				//If this is the first time we've seen this table or alias
				uint tableId = 0;
				tableId = getTableKey(jobInfo, colStep->tupleId());

				if (seenTableIds.find(tableId) == seenTableIds.end())
					changePcolStepToPcolScan(iter, end);

				//Mark this OID as seen
				seenTableIds.insert(tableId);
			}
		}
	}

	//build the project steps
	if (jobInfo.deliveredCols.empty())
	{
		throw logic_error("No delivery column.");
	}

	// if any aggregate columns
	if (jobInfo.hasAggregation == true)
	{
		projectSteps = doAggProject(csep, jobInfo);
	}
	else
	{
		projectSteps = doProject(jobInfo.nonConstCols, jobInfo);
	}

	// bug3736, have jobInfo include the column map info.
	const CalpontSelectExecutionPlan::ColumnMap& retCols = csep->columnMap();
	CalpontSelectExecutionPlan::ColumnMap::const_iterator i = retCols.begin();
	for (; i != retCols.end(); i++)
	{
		SimpleColumn* sc = dynamic_cast<SimpleColumn*>(i->second.get());
		if (sc && !sc->schemaName().empty())
		{
			CalpontSystemCatalog::OID tblOid = tableOid(sc, jobInfo.csc);
			CalpontSystemCatalog::ColType ct = sc->colType();
//XXX use this before connector sets colType in sc correctly.
			if (sc->isInfiniDB())
				ct = jobInfo.csc->colType(sc->oid());
//X


			string alias(extractTableAlias(sc));
			TupleInfo ti(setTupleInfo(ct, sc->oid(), jobInfo, tblOid, sc, alias));
			uint colKey = ti.key;
			uint tblKey = getTableKey(jobInfo, colKey);
			jobInfo.columnMap[tblKey].push_back(colKey);
		}
	}

	// special case, select without a table, like: select 1;
	if (jobInfo.constantCol == CONST_COL_ONLY)
		return;

	//If there are no filters (select * from table;) then add one simple scan
	//TODO: more work here...
	// @bug 497 fix. populate a map of tableoid for querysteps. tablescan
	// cols whose table does not belong to the map
	typedef set<uint> tableIDMap_t;
	tableIDMap_t tableIDMap;
	JobStepVector::iterator qsiter = querySteps.begin();
	JobStepVector::iterator qsend = querySteps.end();
	uint tableId = 0;
	while (qsiter != qsend)
	{
		JobStep* js = qsiter->get();
		if (js->tupleId() != (uint64_t) -1)
			tableId = getTableKey(jobInfo, js->tupleId());
		tableIDMap.insert(tableId);
		++qsiter;
	}

	JobStepVector::iterator jsiter = projectSteps.begin();
	JobStepVector::iterator jsend = projectSteps.end();
	while (jsiter != jsend)
	{
		JobStep* js = jsiter->get();
		if (js->tupleId() != (uint64_t) -1)
			tableId = getTableKey(jobInfo, js->tupleId());
		else
			tableId = getTableKey(jobInfo, js);

		if (typeid(*(jsiter->get())) == typeid(pColStep) &&
				tableIDMap.find(tableId) == tableIDMap.end())
		{
			SJSTEP step0 = *jsiter;
			pColStep* colStep = dynamic_cast<pColStep*>(step0.get());
			pColScanStep* scanStep = new pColScanStep(*colStep);
			//clear out any output association so we get a nice, new one during association
			scanStep->outputAssociation(JobStepAssociation(jobInfo.status));
			step0.reset(scanStep);
			querySteps.push_back(step0);
			js = step0.get();
			tableId = getTableKey(jobInfo, js->tupleId());
			tableIDMap.insert(tableId);
		}
		++jsiter;
	}
}


// v-table mode
void makeVtableModeSteps(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
	JobStepVector& querySteps, JobStepVector& projectSteps, DeliveredTableMap& deliverySteps)
{
	// @bug4848, enhance and unify limit handling.
        // @bug5176. Do limit only for select.
	if (csep->queryType() == "SELECT" && csep->limitNum() != (uint64_t) -1)
	{
		// special case for outer query order by limit -- return all
		if (jobInfo.subId == 0 && csep->hasOrderBy())
			jobInfo.limitCount = (uint64_t) -1;

		// support order by and limit in sub-query/union
		else if (csep->orderByCols().size() > 0)
			addOrderByAndLimit(csep, jobInfo);

		// limit without order by in any query
		else
			jobInfo.limitCount = csep->limitStart() + csep->limitNum();
	}

	// Bug 2123.  Added overrideLargeSideEstimate parm below.  True if the query was written
	// with a hint telling us to skip the estimatation process for determining the large side
	// table and instead use the table order in the from clause.
	associateTupleJobSteps(querySteps, projectSteps, deliverySteps,
								jobInfo, csep->overrideLargeSideEstimate());
	uint16_t stepNo = jobInfo.subId * 10000;
	numberSteps(querySteps, stepNo, jobInfo.traceFlags);
//	SJSTEP ds = deliverySteps.begin()->second;
	idbassert(deliverySteps.begin()->second.get());
//	ds->stepId(stepNo);
//	ds->setTraceFlags(jobInfo.traceFlags);
}

}

namespace joblist
{

void makeJobSteps(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
	JobStepVector& querySteps, JobStepVector& projectSteps, DeliveredTableMap& deliverySteps)
{
	// v-table mode, switch to tuple methods and return the tuple joblist.
	//@Bug 1958 Build table list only for tryTuples.
	const CalpontSelectExecutionPlan::SelectList& fromSubquery = csep->derivedTableList();
	int i = 0;
	for (CalpontSelectExecutionPlan::TableList::const_iterator it = csep->tableList().begin();
		it != csep->tableList().end();
		it++)
	{
		CalpontSystemCatalog::OID oid;
		if (it->schema.empty())
			oid = doFromSubquery(fromSubquery[i++].get(), it->alias, it->view, jobInfo);
		else if (it->fIsInfiniDB)
			oid = jobInfo.csc->tableRID(*it).objnum;
		else
			oid = 0;

		uint tableUid = makeTableKey(jobInfo, oid, it->table, it->alias, it->schema, it->view);
		jobInfo.tableList.push_back(tableUid);
	}

	// add select suqueries
	preprocessSelectSubquery(csep, jobInfo);

	// semi-join may appear in having clause
	if (csep->having() != NULL)
		preprocessHavingClause(csep, jobInfo);

	// parse plan and make jobstep list
	parseExecutionPlan(csep, jobInfo, querySteps, projectSteps, deliverySteps);
	makeVtableModeSteps(csep, jobInfo, querySteps, projectSteps, deliverySteps);
}

void makeUnionJobSteps(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
	JobStepVector& querySteps, JobStepVector&, DeliveredTableMap& deliverySteps)
{
	CalpontSelectExecutionPlan::SelectList& selectVec = csep->unionVec();
	uint8_t distinctUnionNum = csep->distinctUnionNum();
	RetColsVector unionRetCols = csep->returnedCols();
	JobStepVector unionFeeders;
//	CalpontSelectExecutionPlan* ep = NULL;
	for (CalpontSelectExecutionPlan::SelectList::iterator cit = selectVec.begin();
		 cit != selectVec.end();
		 cit++)
	{
//		JobStepVector qSteps;
//		JobStepVector pSteps;
//		DeliveredTableMap dSteps;
//		JobInfo queryJobInfo = jobInfo;
//		ep = dynamic_cast<CalpontSelectExecutionPlan*>((*cit).get());
//		makeJobSteps(ep, queryJobInfo, qSteps, pSteps, dSteps);
//		querySteps.insert(querySteps.end(), qSteps.begin(), qSteps.end());
//		unionFeeders.push_back(dSteps[execplan::CNX_VTABLE_ID]);
		SJSTEP sub = doUnionSub(cit->get(), jobInfo);
		querySteps.push_back(sub);
		unionFeeders.push_back(sub);
	}

	jobInfo.deliveredCols = unionRetCols;
	SJSTEP unionStep(unionQueries(unionFeeders, distinctUnionNum, jobInfo));
	querySteps.push_back(unionStep);
	uint16_t stepNo = jobInfo.subId * 10000;
	numberSteps(querySteps, stepNo, jobInfo.traceFlags);
	deliverySteps[execplan::CNX_VTABLE_ID] = unionStep;
}


}

namespace
{

SJLP makeJobList_(
	CalpontExecutionPlan* cplan,
	ResourceManager& rm,
	bool isExeMgr,
	unsigned& errCode, string& emsg)
{
	CalpontSelectExecutionPlan* csep = dynamic_cast<CalpontSelectExecutionPlan*>(cplan);
	CalpontSystemCatalog* csc = CalpontSystemCatalog::makeCalpontSystemCatalog(csep->sessionID());

	// We have to go ahead and create JobList now so we can store the joblist's
	// projectTableOID pointer in JobInfo for use during jobstep creation.
	SErrorInfo status(new ErrorInfo());
	shared_ptr<TupleKeyInfo> keyInfo(new TupleKeyInfo);
	shared_ptr<int> subCount(new int);
	*subCount = 0;
	JobList* jl = new TupleJobList(isExeMgr);
	jl->priority(csep->priority());
	jl->statusPtr(status);
	rm.setTraceFlags(csep->traceFlags());

	//Stuff a util struct with some stuff we always need
	JobInfo jobInfo(rm);
	jobInfo.sessionId = csep->sessionID();
	jobInfo.txnId = csep->txnID();
	jobInfo.verId = csep->verID();
	jobInfo.statementId = csep->statementID();
	jobInfo.csc = csc;
	//TODO: clean up the vestiges of the bool trace
	jobInfo.trace = csep->traceOn();
	jobInfo.traceFlags = csep->traceFlags();
	jobInfo.isExeMgr = isExeMgr;
//	jobInfo.tryTuples = tryTuples; // always tuples after release 3.0
	jobInfo.stringScanThreshold = csep->stringScanThreshold();
	jobInfo.status = status;
	jobInfo.keyInfo = keyInfo;
	jobInfo.subCount = subCount;
	jobInfo.projectingTableOID = jl->projectingTableOIDPtr();
	jobInfo.jobListPtr = jl;

	// set fifoSize to 1 for CalpontSystemCatalog query
	if (csep->sessionID() & 0x80000000)
		jobInfo.fifoSize = 1;
	else if (csep->traceOn())
		cout << (*csep) << endl;

	try
	{
		JobStepVector querySteps;
		JobStepVector projectSteps;
		DeliveredTableMap deliverySteps;

		if (csep->unionVec().size() == 0)
			makeJobSteps(csep, jobInfo, querySteps, projectSteps, deliverySteps);
		else
			makeUnionJobSteps(csep, jobInfo, querySteps, projectSteps, deliverySteps);

		uint16_t stepNo = numberSteps(querySteps, 0, jobInfo.traceFlags);
		stepNo = numberSteps(projectSteps, stepNo, jobInfo.traceFlags);

		struct timeval stTime;
		if (jobInfo.trace)
		{
			ostringstream oss;
			oss << endl;
			oss << endl << "job parms: " << endl;
			oss << "maxBuckets = " << jobInfo.maxBuckets << ", maxElems = " << jobInfo.maxElems <<
			", flushInterval = " << jobInfo.flushInterval <<
				", fifoSize = " << jobInfo.fifoSize <<
				", ScanLimit/Threshold = " << jobInfo.scanLbidReqLimit << "/" <<
				jobInfo.scanLbidReqThreshold << endl;
			oss << endl << "job filter steps: " << endl;
			ostream_iterator<JobStepVector::value_type> oIter(oss, "\n");
			copy(querySteps.begin(), querySteps.end(), oIter);
			oss << endl << "job project steps: " << endl;
			copy(projectSteps.begin(), projectSteps.end(), oIter);
			oss << endl << "job delivery steps: " << endl;
			DeliveredTableMap::iterator dsi = deliverySteps.begin();
			while (dsi != deliverySteps.end())
			{
				oss << dynamic_cast<const JobStep*>(dsi->second.get()) << endl;
				++dsi;
			}

			oss << endl;
			gettimeofday(&stTime, 0);

			struct tm tmbuf;
#ifdef _MSC_VER
			errno_t p = 0;
			time_t t = stTime.tv_sec;
			p = localtime_s(&tmbuf, &t);
			if (p != 0)
				memset(&tmbuf, 0, sizeof(tmbuf));
#else
			localtime_r(&stTime.tv_sec, &tmbuf);
#endif
			ostringstream tms;
			tms << setfill('0')
				<< setw(4) << (tmbuf.tm_year+1900)
				<< setw(2) << (tmbuf.tm_mon+1)
				<< setw(2) << (tmbuf.tm_mday)
				<< setw(2) << (tmbuf.tm_hour)
				<< setw(2) << (tmbuf.tm_min)
				<< setw(2) << (tmbuf.tm_sec)
				<< setw(6) << (stTime.tv_usec);
			string tmstr(tms.str());
			string jsrname("jobstep."+tmstr+".dot");
			ofstream dotFile(jsrname.c_str());
			jlf_graphics::writeDotCmds(dotFile, querySteps, projectSteps);

			char timestamp[80];
#ifdef _MSC_VER
			t = stTime.tv_sec;
			p = ctime_s(timestamp, 80, &t);
			if (p != 0)
				strcpy(timestamp, "UNKNOWN");
#else
			ctime_r((const time_t*)&stTime.tv_sec, timestamp);
#endif
			oss << "runtime updates: start at " << timestamp;
			cout << oss.str();
			Message::Args args;
			args.add(oss.str());
			jobInfo.logger->logMessage(LOG_TYPE_DEBUG, LogSQLTrace, args,
				LoggingID(5, jobInfo.sessionId, jobInfo.txnId, 0));
			cout << flush;
		}
		else
		{
			gettimeofday(&stTime, 0);
		}

		// Finish initializing the JobList object
		jl->addQuery(querySteps);
		jl->addProject(projectSteps);
		jl->addDelivery(deliverySteps);

		dynamic_cast<TupleJobList*>(jl)->setDeliveryFlag(true);
	}
	catch (IDBExcept& iex)
	{
		jobInfo.status->errCode = iex.errorCode();
		errCode = iex.errorCode();
		exceptionHandler(jl, jobInfo, iex.what(), LOG_TYPE_DEBUG);
		emsg = iex.what();
		goto bailout;
	}
	catch (QueryDataExcept& uee)
	{
		jobInfo.status->errCode = uee.errorCode();
		errCode = uee.errorCode();
		exceptionHandler(jl, jobInfo, uee.what(), LOG_TYPE_DEBUG);
		emsg = uee.what();
		goto bailout;
	}
	catch (const std::exception& ex)
	{
		jobInfo.status->errCode = makeJobListErr;
		errCode = makeJobListErr;
		exceptionHandler(jl, jobInfo, ex.what());
		emsg = ex.what();
		goto bailout;
	}
	catch (...)
	{
		jobInfo.status->errCode = makeJobListErr;
		errCode = makeJobListErr;
		exceptionHandler(jl, jobInfo, "an exception");
		emsg = "An unknown internal joblist error";
		goto bailout;
	}

	goto done;

bailout:
	delete jl;
	jl = 0;
	if (emsg.empty())
		emsg = "An unknown internal joblist error";

done:
	SJLP jlp(jl);
	return jlp;
}

}

namespace joblist
{

/* static */
SJLP JobListFactory::makeJobList(
	CalpontExecutionPlan* cplan,
	ResourceManager& rm,
	bool tryTuple,
	bool isExeMgr)
{
	SJLP ret;
	string emsg;
	unsigned errCode = 0;
	ret = makeJobList_(cplan, rm, isExeMgr, errCode, emsg);

	if (!ret)
	{
		ret.reset(new TupleJobList(isExeMgr));
		SErrorInfo status(new ErrorInfo);
		status->errCode = errCode;
		status->errMsg  = emsg;
		ret->statusPtr(status);
	}

	return ret;
}

}
// vim:ts=4 sw=4:

