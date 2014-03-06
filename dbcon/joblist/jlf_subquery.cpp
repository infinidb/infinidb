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

//  $Id: jlf_subquery.cpp 6419 2010-03-30 04:28:32Z xlou $


#include <iostream>
#include <stack>
#include <iterator>
//#define NDEBUG
#include <cassert>
#include <vector>
using namespace std;

#include <boost/shared_ptr.hpp>
using namespace boost;

#include "calpontsystemcatalog.h"
#include "logicoperator.h"
#include "constantcolumn.h"
#include "existsfilter.h"
#include "predicateoperator.h"
#include "pseudocolumn.h"
#include "selectfilter.h"
#include "simplefilter.h"
#include "simplescalarfilter.h"
#include "windowfunctioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "idberrorinfo.h"
#include "errorids.h"
#include "exceptclasses.h"
#include "dataconvert.h"
using namespace logging;

#include "elementtype.h"
#include "jobstep.h"
#include "jlf_common.h"
#include "jlf_execplantojoblist.h"
#include "expressionstep.h"
#include "tupleconstantstep.h"
#include "tuplehashjoin.h"
#include "subquerystep.h"
#include "subquerytransformer.h"
#include "jlf_subquery.h"
using namespace joblist;


namespace
{

void getColumnValue(ConstantColumn** cc, uint64_t i, const Row& row)
{
	ostringstream oss;
	int64_t data = 0;
	switch (row.getColTypes()[i])
	{
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::BIGINT:
			if (row.getScale(i) == 0)
			{
				oss << row.getIntField(i);
				*cc = new ConstantColumn(oss.str(), row.getIntField(i));
				break;
			}
			// else > 0; fall through

		case CalpontSystemCatalog::DECIMAL:
		case CalpontSystemCatalog::UDECIMAL:
			data = row.getIntField(i);
			oss << (data / IDB_pow[row.getScale(i)]);
			if (row.getScale(i) > 0)
			{
				if (data > 0)
					oss << "." << (data % IDB_pow[row.getScale(i)]);
				else if (data < 0)
					oss << "." << (-data % IDB_pow[row.getScale(i)]);
			}
			*cc = new ConstantColumn(oss.str(),
									IDB_Decimal(data, row.getScale(i), row.getPrecision(i)));
			break;

        case CalpontSystemCatalog::UTINYINT:
        case CalpontSystemCatalog::USMALLINT:
        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UBIGINT:
            oss << row.getUintField(i);
            *cc = new ConstantColumn(oss.str(), row.getUintField(i));
            break;

		case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
			oss << fixed << row.getFloatField(i);
			*cc = new ConstantColumn(oss.str(), (double) row.getFloatField(i));
			break;
			break;

		case CalpontSystemCatalog::DOUBLE:
			oss << fixed << row.getDoubleField(i);
			*cc = new ConstantColumn(oss.str(), row.getDoubleField(i));
			break;

		case CalpontSystemCatalog::DATE:
			oss << dataconvert::DataConvert::dateToString(row.getUintField<4>(i));
			*cc = new ConstantColumn(oss.str());
			break;

		case CalpontSystemCatalog::DATETIME:
			oss << dataconvert::DataConvert::datetimeToString(row.getUintField<8>(i));
			*cc = new ConstantColumn(oss.str());
			break;

		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
			oss << (char*) (row.getStringField(i).c_str());
			*cc = new ConstantColumn(oss.str());
			break;

		default:
			oss << "Unsupported data type: " << row.getColTypes()[i];
			throw QueryDataExcept(oss.str(), dataTypeErr);
	}
}


bool simpleScalarFilterToParseTree(SimpleScalarFilter* sf, ParseTree*& pt, JobInfo& jobInfo)
{
	const vector<SRCP>& cols = sf->cols();
	CalpontSelectExecutionPlan* csep = sf->sub().get();
	SOP sop = sf->op();

	// For row construct, supports only =, <> in Release 1.1.
	// Other operators are errored out by connector.
	string lop("and");
	if ((cols.size() > 1)  && (sop->data() == "<>"))
		lop = "or";

	// Transformer sub to a scalar result.
	SErrorInfo errorInfo(jobInfo.errorInfo);
	SimpleScalarTransformer transformer(&jobInfo, errorInfo, false);
	transformer.makeSubQueryStep(csep);

	// Do not catch exceptions here, let caller handle them.
	transformer.run();

	// if subquery errored out
	if (errorInfo->errCode)
	{
		ostringstream oss;
		oss << "Sub-query failed: ";
		if (errorInfo->errMsg.empty())
		{
			oss << "error code " << errorInfo->errCode;
			errorInfo->errMsg = oss.str();
		}
		throw runtime_error(errorInfo->errMsg);
	}

	// Construct simple filters based on the scalar result.
	bool isScalar = false;
	if (transformer.emptyResultSet() == false)
	{
		const Row& row = transformer.resultRow();
		uint64_t i = 0;
		for (; i < cols.size(); i++)
		{
			// = null is always false
			if (row.isNullValue(i) == true)
				break;

			// set fResult for cc
			ConstantColumn* cc = NULL;
			getColumnValue(&cc, i, row);
			sop->setOpType(cols[i]->resultType(), cc->resultType());

			SimpleFilter* sf = new SimpleFilter(sop, cols[i]->clone(), cc);
			if (i == 0)
			{
				pt = new ParseTree(sf);
			}
			else
			{
				ParseTree* left = pt;
				pt = new ParseTree(new LogicOperator(lop));
				pt->left(left);
				pt->right(new ParseTree(sf));
			}
		}

		if (i >= cols.size())
			isScalar = true;
	}

	return isScalar;
}


void sfInHaving(ParseTree* pt, void*)
{
	SelectFilter* sf = dynamic_cast<SelectFilter*>(pt->data());

	if (sf != NULL)
		throw IDBExcept(ERR_NON_SUPPORT_HAVING);
}


void ssfInHaving(ParseTree* pt, void* obj)
{
	JobInfo* jobInfo = reinterpret_cast<JobInfo*>(obj);
	SimpleScalarFilter* ssf = dynamic_cast<SimpleScalarFilter*>(pt->data());

	if (ssf != NULL)
	{
		ParseTree* parseTree = NULL;
		if (simpleScalarFilterToParseTree(ssf, parseTree, *jobInfo))
		{
			// replace simple scalar filter with simple filters
			delete pt->data();
			pt->left(parseTree->left());
			pt->right(parseTree->right());
			pt->data(parseTree->data());

			// don't delete the parseTree, it has been placed in the plan.
			// delete parseTree;
		}
		else
		{
			// not a scalar result
			// replace simple scalar filter with simple filters
			delete pt->data();
			delete parseTree;
			jobInfo->constantFalse = true;
		}
	}
}


void getSemiJoins(ParseTree* pt, void* obj)
{
	SimpleFilter* sf = dynamic_cast<SimpleFilter*>(pt->data());

	if (sf != NULL)
	{
		SimpleColumn* sc1  = dynamic_cast<SimpleColumn*>(sf->lhs());
		SimpleColumn* sc2  = dynamic_cast<SimpleColumn*>(sf->rhs());

		bool semijoin = false;
		if (sc1 != NULL && sc1->joinInfo() != 0)
			semijoin = true;

		if (sc2 != NULL && sc2->joinInfo() != 0)
			semijoin = true;

		if (semijoin)
		{
			ParseTree** semiJoins = reinterpret_cast<ParseTree**>(obj);
			if (*semiJoins == NULL)
			{
				*semiJoins = new ParseTree(sf);
			}
			else
			{
				ParseTree* left = *semiJoins;
				*semiJoins = new ParseTree(new LogicOperator("and"));
				(*semiJoins)->left(left);
				(*semiJoins)->right(new ParseTree(sf));
			}

			pt->data(NULL);
		}
	}
}


ParseTree* trim(ParseTree*& pt)
{
	ParseTree* lhs = pt->left();
	if (lhs)
		pt->left(trim(lhs));

	ParseTree* rhs = pt->right();
	if (rhs)
		pt->right(trim(rhs));

	if ((lhs == NULL) && (rhs == NULL) && (pt->data() == NULL))
	{
		delete pt;
		pt = NULL;
	}
	else if ((lhs == NULL || rhs == NULL) && dynamic_cast<LogicOperator*>(pt->data()))
	{
		idbassert(dynamic_cast<LogicOperator*>(pt->data())->data() == "and");
		ParseTree* br = pt;
		ParseTree* nl = NULL; // the left()/right() are overloaded

		if (lhs == NULL && rhs != NULL)
			pt = rhs;
		else if (lhs != NULL && rhs == NULL)
			pt = lhs;
		else
			pt = NULL;

		br->left(nl);
		br->right(nl);
		delete br;
	}

	return pt;
}


void handleNotIn(JobStepVector& jsv, JobInfo* jobInfo)
{
	// convert CORRELATED join (but not MATCHNULLS) to expression.
	for (JobStepVector::iterator i = jsv.begin(); i != jsv.end(); i++)
	{
		TupleHashJoinStep* thjs = dynamic_cast<TupleHashJoinStep*>(i->get());
		if (!thjs || !(thjs->getJoinType() & CORRELATED) || (thjs->getJoinType() & MATCHNULLS) )
			continue;

		ReturnedColumn* lhs = thjs->column1()->clone();
		ReturnedColumn* rhs = thjs->column2()->clone();

		SOP sop(new PredicateOperator("="));
		sop->setOpType(lhs->resultType(), rhs->resultType());
		sop->resultType(sop->operationType());
		SimpleFilter* sf = new SimpleFilter(sop, lhs, rhs);

		ExpressionStep* es = new ExpressionStep(*jobInfo);
		if (es == NULL)
			throw runtime_error("Failed to create ExpressionStep 2");

		es->expressionFilter(sf, *jobInfo);
		i->reset(es);

		delete sf;
	}
}


bool isNotInSubquery(JobStepVector& jsv)
{
	// use MATCHNULLS(execplan::JOIN_NULL_MATCH) to identify NOT IN and NOT EXIST
	bool notIn = false;
	for (JobStepVector::iterator i = jsv.begin(); i != jsv.end(); i++)
	{
		TupleHashJoinStep* thjs = dynamic_cast<TupleHashJoinStep*>(i->get());
		if (thjs)
		{
			if (thjs->getJoinType() & MATCHNULLS)
			{
				// only NOT IN will be specially treated.
				notIn = true;
				break;
			}
		}
	}

	return notIn;
}


void alterCsepInExistsFilter(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	// This is for window function in IN/EXISTS sub-query.
	// Replace an expression of window functions with indivisual window functions.
	RetColsVector& retCols = csep->returnedCols();
	RetColsVector wcs;
	for (RetColsVector::iterator i = retCols.begin(); i < retCols.end(); i++)
	{
		const vector<WindowFunctionColumn*>& wcList = (*i)->windowfunctionColumnList();
		if (!wcList.empty())
		{
			vector<WindowFunctionColumn*>::const_iterator j = wcList.begin();
			while (j != wcList.end())
				wcs.push_back(SRCP((*j++)->clone()));

			// a little optimization, eliminate the expression in select clause
			// replace the window function expression with the 1st windowfunction
			(*i) = wcs[0];
		}
	}

	if (wcs.size() > 1)
		retCols.insert(retCols.end(), wcs.begin()+1, wcs.end());
}


void doCorrelatedExists(const ExistsFilter* ef, JobInfo& jobInfo)
{
	// Transformer sub to a subquery step.
	SErrorInfo errorInfo(jobInfo.errorInfo);
	SubQueryTransformer transformer(&jobInfo, errorInfo);
	CalpontSelectExecutionPlan* csep = ef->sub().get();
	alterCsepInExistsFilter(csep, jobInfo);
	SJSTEP subQueryStep = transformer.makeSubQueryStep(csep);

	// @bug3524, special handling of not in.
	JobStepVector& jsv = transformer.correlatedSteps();
	if (isNotInSubquery(jsv) == true)
		handleNotIn(jsv, transformer.subJobInfo());

	transformer.updateCorrelateInfo();
	jsv.push_back(subQueryStep);
	jobInfo.stack.push(jsv);
}


void doNonCorrelatedExists(const ExistsFilter* ef, JobInfo& jobInfo)
{
	// Assume: exists-subquery without a from clause, like exists (select 1)
	//         always evalustes to true
	bool noFrom = (ef->sub()->tableList().size() == 0);
	bool exists = !ef->notExists();

	if (!noFrom)
	{
		// Transformer sub to a scalar result set.
		SErrorInfo errorInfo(new ErrorInfo());
		SimpleScalarTransformer transformer(&jobInfo, errorInfo, true);
		transformer.makeSubQueryStep(ef->sub().get());

		// @bug 2839. error out in-relelvant correlated column case
		if (!transformer.correlatedSteps().empty())
		{
			JobStepVector::const_iterator it = transformer.correlatedSteps().begin();
			string tn;
			for (; it != transformer.correlatedSteps().end(); ++it)
			{
				// 1. if tuplehashjoin, check alias1 and alias2; otherwise, check alias
				// 2. alias start with "$sub_"
				TupleHashJoinStep *thjs = dynamic_cast<TupleHashJoinStep*>(it->get());
				if (thjs)
				{
					if (thjs->alias1().empty() || thjs->alias1().compare(0,5,"$sub_"))
						tn = thjs->alias2();
					else
						tn = thjs->alias1();
				}
				else
				{
					tn = it->get()->alias();
				}
			}

			Message::Args args;
			if (tn.empty() || tn.compare(0,5,"$sub_"))
				tn = "sub-query";
			args.add(tn);
			throw IDBExcept(ERR_MISS_JOIN_IN_SUB, args);
		}

		// Catch more_than_1_row exception only
		try
		{
			transformer.run();
		}
		catch (MoreThan1RowExcept&)
		{
			// no-op
		};

		// Check if the exists condition is satisfied.
		//((!transformer.emptyResultSet() && !ef->notExists()) ||
		// ( transformer.emptyResultSet() &&  ef->notExists()))
		exists = (transformer.emptyResultSet() == ef->notExists());
	}

	JobStepVector jsv;
	SJSTEP tcs(new TupleConstantBooleanStep(jobInfo, exists));
	jsv.push_back(tcs);
	jobInfo.stack.push(jsv);
}


const SRCP doSelectSubquery(CalpontExecutionPlan* ep, SRCP& sc, JobInfo& jobInfo)
{
	CalpontSelectExecutionPlan* csep = dynamic_cast<CalpontSelectExecutionPlan*>(ep);
	SRCP rc;
	SErrorInfo errorInfo(jobInfo.errorInfo);
	jobInfo.subView = dynamic_cast<SimpleColumn*>(sc.get())->viewName();
	SubQueryTransformer transformer(&jobInfo, errorInfo);
	transformer.setVarbinaryOK();
	SJSTEP subQueryStep = transformer.makeSubQueryStep(csep);
	if (transformer.correlatedSteps().size() > 0)
	{
		transformer.updateCorrelateInfo();
		JobStepVector jsv = transformer.correlatedSteps();
		jsv.push_back(subQueryStep);
		jobInfo.selectAndFromSubs.insert(jobInfo.selectAndFromSubs.end(), jsv.begin(), jsv.end());
		const RetColsVector& retCol = csep->returnedCols();
		for (uint64_t i = 0; i < retCol.size(); i++)
		{
			if (retCol[i]->colSource() == 0)
			{
				rc = transformer.virtualTable().columns()[i];
				break;
			}
		}
	}
	else
	{
		// Non-correlated subquery
		// Do not catch exceptions here, let caller handle them.
		SimpleScalarTransformer simpleTransformer(transformer);
		simpleTransformer.run();

		// Costruct a simple column based on the scalar result.
		ConstantColumn* cc = NULL;
		if (simpleTransformer.emptyResultSet() == false)
		{
			// set value for cc
			const Row& row = simpleTransformer.resultRow();
			if (!row.isNullValue(0))
				getColumnValue(&cc, 0, row);
		}

		// Empty set or null value
		if (cc == NULL)
		{
			cc = new ConstantColumn("");
			cc->type(ConstantColumn::NULLDATA);
		}

		rc.reset(cc);
	}

	return rc;
}


}


namespace joblist
{

void doSimpleScalarFilter(ParseTree* p, JobInfo& jobInfo)
{
	SimpleScalarFilter* sf = dynamic_cast<SimpleScalarFilter*>(p->data());
	idbassert(sf != NULL);
	ParseTree* parseTree = NULL;

	// Parse filters to job step.
	if (simpleScalarFilterToParseTree(sf, parseTree, jobInfo))
	{
		// update the plan for supporting OR in future.
		ParseTree* ccp = (p);
		delete ccp->data();
		ccp->left(parseTree->left());
		ccp->right(parseTree->right());
		ccp->data(parseTree->data());

		// create job steps for each simple filter
		parseTree->walk(JLF_ExecPlanToJobList::walkTree, &jobInfo);

		// don't delete the parseTree, it has been placed in the plan.
		// delete parseTree;
	}
	else
	{
		// not a scalar result
		delete parseTree;
		JobStepVector jsv;
		SJSTEP tcs(new TupleConstantBooleanStep(jobInfo, false));
		jsv.push_back(tcs);
		jobInfo.stack.push(jsv);
	}
}


void doExistsFilter(const ParseTree* p, JobInfo& jobInfo)
{
	const ExistsFilter* ef = dynamic_cast<const ExistsFilter*>(p->data());
	idbassert(ef != NULL);
	if (ef->correlated())
		doCorrelatedExists(ef, jobInfo);
	else
		doNonCorrelatedExists(ef, jobInfo);
}


void doSelectFilter(const ParseTree* p, JobInfo& jobInfo)
{
	const SelectFilter* sf = dynamic_cast<const SelectFilter*>(p->data());
	idbassert(sf != NULL);

	SErrorInfo errorInfo(jobInfo.errorInfo);
	SubQueryTransformer transformer(&jobInfo, errorInfo);
	SJSTEP subQueryStep = transformer.makeSubQueryStep(sf->sub().get());
	transformer.updateCorrelateInfo();
	JobStepVector jsv = transformer.correlatedSteps();

	jsv.push_back(subQueryStep);

	const vector<SRCP>& cols = sf->cols();
	SOP sop = sf->op();

	// For row construct, supports only =, <> in Release 1.1.
	// Other operators are errored out by connector.
	string lop("and");
	if ((cols.size() > 1)  && (sop->data() == "<>"))
		lop = "or";

	// @bug3780, select filters are not additional comparison, but fe2.
	// When parsing the sub query, correlated columns may be added as group by column,
	// s is the start position of the original selected columns.
	uint64_t s = sf->returnedColPos();
	ParseTree* pt = NULL;
	const VirtualTable& vt = transformer.virtualTable();
	for (uint64_t i = 0; i < cols.size(); i++)
	{
		ReturnedColumn* lhs = cols[i].get()->clone();
		ReturnedColumn* rhs = vt.columns()[s+i].get()->clone();
		sop->setOpType(lhs->resultType(), rhs->resultType());
		if (i == 0)
		{
			pt = new ParseTree(new SimpleFilter(sop, lhs, rhs));
		}
		else
		{
			ParseTree* left = pt;
			pt = new ParseTree(new LogicOperator(lop));
			pt->left(left);
			pt->right(new ParseTree(new SimpleFilter(sop, lhs, rhs)));
		}
	}

	if (pt != NULL)
	{
		ExpressionStep* es = new ExpressionStep(jobInfo);
		es->expressionFilter(pt, jobInfo);
		es->selectFilter(true);
		delete pt;

		jsv.push_back(SJSTEP(es));
	}

	jobInfo.stack.push(jsv);

}


void preprocessHavingClause(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	ParseTree* havings = (csep->having());
	idbassert(havings != NULL); // check having exists before calling this function.

	// check select filter in having
	havings->walk(sfInHaving, &jobInfo);

	// check simple scalar filters in having
	havings->walk(ssfInHaving, &jobInfo);

	// check correlated columns in having
	ParseTree* semiJoins = NULL;
	havings->walk(getSemiJoins, &semiJoins);
	trim(havings);

	if (semiJoins != NULL)
	{
		ParseTree* newFilters = new ParseTree(new LogicOperator("and"));
		newFilters->left(csep->filters());
		newFilters->right(semiJoins);

		csep->filters(newFilters);
		csep->having(havings);
	}
}


int doFromSubquery(CalpontExecutionPlan* ep, const string& alias, const string& view, JobInfo& jobInfo)
{
	CalpontSelectExecutionPlan* csep = dynamic_cast<CalpontSelectExecutionPlan*>(ep);
	SErrorInfo errorInfo(jobInfo.errorInfo);
	jobInfo.subView = view;
	SubQueryTransformer transformer(&jobInfo, errorInfo, alias);
	transformer.setVarbinaryOK();
	SJSTEP subQueryStep = transformer.makeSubQueryStep(csep, true);
	subQueryStep->view(view);
	SJSTEP subAd(new SubAdapterStep(subQueryStep, jobInfo));
	jobInfo.selectAndFromSubs.push_back(subAd);

	return CNX_VTABLE_ID;
}


void addOrderByAndLimit(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	// make sure there is a LIMIT
	if (csep->orderByCols().size() > 0 && csep->limitNum() == (uint64_t) -1)
		return;

	jobInfo.limitStart = csep->limitStart();
	jobInfo.limitCount = csep->limitNum();

	CalpontSelectExecutionPlan::OrderByColumnList& orderByCols = csep->orderByCols();
	for (uint64_t i = 0; i < orderByCols.size(); i++)
	{
		// skip constant columns
		if (dynamic_cast<ConstantColumn*>(orderByCols[i].get()) != NULL)
			continue;

		uint32_t tupleKey = -1;
		SimpleColumn* sc = dynamic_cast<SimpleColumn*>(orderByCols[i].get());
		if (sc != NULL)
		{
			CalpontSystemCatalog::OID oid = sc->oid();
			CalpontSystemCatalog::OID tblOid = tableOid(sc, jobInfo.csc);
			CalpontSystemCatalog::OID dictOid = 0;
			CalpontSystemCatalog::ColType ct;
			string alias(extractTableAlias(sc));
			string view(sc->viewName());
			string schema(sc->schemaName());
//			string name(sc->columnName());
			if (!sc->schemaName().empty())
			{
				ct = sc->colType();
//XXX use this before connector sets colType in sc correctly.
//    type of pseudo column is set by connector
				if (sc->isInfiniDB() && !(dynamic_cast<PseudoColumn*>(sc)))
					ct = jobInfo.csc->colType(sc->oid());
//X
				dictOid = isDictCol(ct);
			}
			else
			{
				if (sc->colPosition() == (uint64_t) -1)
				{
					sc = dynamic_cast<SimpleColumn*>(jobInfo.deliveredCols[sc->orderPos()].get());
				}
				else
				{
					sc->oid((tblOid+1) + sc->colPosition());
				}
				oid = sc->oid();
				ct = jobInfo.vtableColTypes[UniqId(oid, alias, schema, view)];
			}

			tupleKey = getTupleKey(jobInfo, sc);

			// for dictionary columns, replace the token oid with string oid
			if (dictOid > 0)
			{
				tupleKey = jobInfo.keyInfo->dictKeyMap[tupleKey];
			}
		}
		else
		{
			const ReturnedColumn* rc = dynamic_cast<const ReturnedColumn*>(orderByCols[i].get());
			uint64_t eid = rc->expressionId();
			CalpontSystemCatalog::ColType ct = rc->resultType();
			tupleKey = getExpTupleKey(jobInfo, eid);
		}

		jobInfo.orderByColVec.push_back(make_pair(tupleKey, orderByCols[i]->asc()));
	}
}


void preprocessSelectSubquery(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	RetColsVector& retCols = (csep->returnedCols());
	CalpontSelectExecutionPlan::SelectList::const_iterator sub = csep->selectSubList().begin();
	for (RetColsVector::iterator i = retCols.begin(); i != retCols.end(); i++)
	{
		if ((*i)->colSource() == execplan::SELECT_SUB)
		{
			(*i) = doSelectSubquery(sub->get(), *i, jobInfo);
			sub++;
		}
	}
}


SJSTEP doUnionSub(CalpontExecutionPlan* ep, JobInfo& jobInfo)
{
	CalpontSelectExecutionPlan* csep = dynamic_cast<CalpontSelectExecutionPlan*>(ep);
	SErrorInfo errorInfo(jobInfo.errorInfo);
	SubQueryTransformer transformer(&jobInfo, errorInfo);
	transformer.setVarbinaryOK();
	SJSTEP subQueryStep = transformer.makeSubQueryStep(csep, false);
	SJSTEP subAd(new SubAdapterStep(subQueryStep, jobInfo));
	return subAd;
}


}
// vim:ts=4 sw=4:

