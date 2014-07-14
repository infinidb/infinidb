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

//  $Id: expressionstep.cpp 9681 2013-07-11 22:58:05Z xlou $


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
#include "arithmeticcolumn.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "pseudocolumn.h"
#include "simplecolumn.h"
#include "windowfunctioncolumn.h"
#include "constantfilter.h"
#include "simplefilter.h"
using namespace execplan;

#include "jlf_common.h"
#include "jlf_tuplejoblist.h"
#include "rowgroup.h"
using namespace rowgroup;

#include "expressionstep.h"

namespace joblist
{
ExpressionStep::ExpressionStep(const JobInfo& jobInfo) :
	JobStep(jobInfo),
	fExpressionFilter(NULL),
	fExpressionId(-1),
	fVarBinOK(false),
	fSelectFilter(false),
	fAssociatedJoinId(0),
	fDoJoin(false),
	fVirtual(false)
{
}


ExpressionStep::ExpressionStep(const ExpressionStep& rhs) :
	JobStep(rhs),
	fExpression(rhs.expression()),
	fExpressionFilter(NULL),
	fExpressionId(rhs.expressionId()),
	fAliases(rhs.aliases()),
	fViews(rhs.views()),
	fSchemas(rhs.schemas()),
	fTableKeys(rhs.tableKeys()),
	fColumnKeys(rhs.columnKeys()),
	fVarBinOK(rhs.fVarBinOK),
	fSelectFilter(rhs.fSelectFilter),
	fAssociatedJoinId(rhs.fAssociatedJoinId),
	fDoJoin(rhs.fDoJoin),
	fVirtual(rhs.fVirtual)
{
	if (rhs.expressionFilter() != NULL)
		fExpressionFilter = new ParseTree(*(rhs.expressionFilter()));
}


ExpressionStep::~ExpressionStep()
{
	if (fExpressionFilter != NULL)
		delete fExpressionFilter;
}


void ExpressionStep::run()
{
}


void ExpressionStep::join()
{
}


void ExpressionStep::expression(const SRCP exp, JobInfo& jobInfo)
{
	fExpression = exp;

	// set expression Id
	ArithmeticColumn* ac = dynamic_cast<ArithmeticColumn*>(fExpression.get());
	FunctionColumn*   fc = dynamic_cast<FunctionColumn*>(fExpression.get());
	fExpressionId = exp.get()->expressionId();

	if (ac != NULL || fc != NULL)
		addColumn(exp.get(), jobInfo);
}


void ExpressionStep::expressionFilter(const Filter* filter, JobInfo& jobInfo)
{
	Filter *f = filter->clone();
	fExpressionFilter = new ParseTree(f);
	idbassert(fExpressionFilter != NULL);
	if (fExpressionFilter == NULL)
	{
		std::ostringstream errmsg;
		errmsg << "ExpressionStep: Failed to create a new ParseTree";
		cerr << boldStart << errmsg.str() << boldStop << endl;
		throw runtime_error(errmsg.str());
	}

	// populate the oid vectors
	SimpleFilter* sf = NULL;
	ConstantFilter* cf = NULL;
	if ((sf = dynamic_cast<SimpleFilter*>(f)) != NULL)
	{
		addColumn(sf->lhs(), jobInfo);
		addColumn(sf->rhs(), jobInfo);

		if (sf->op()->data() == "=")
			functionJoinCheck(sf, jobInfo);
	}
	else if ((cf = dynamic_cast<ConstantFilter*>(f)) != NULL)
	{
		//addColumn(cf->col().get(), jobInfo);
		for (uint32_t i = 0; i < cf->simpleColumnList().size(); i++)
			addColumn(cf->simpleColumnList()[i], jobInfo);
	}
}


void ExpressionStep::expressionFilter(const ParseTree* filter, JobInfo& jobInfo)
{
	fExpressionFilter = new ParseTree();
	idbassert(fExpressionFilter != NULL);
	if (fExpressionFilter == NULL)
	{
		std::ostringstream errmsg;
		errmsg << "ExpressionStep: Failed to create a new ParseTree";
		cerr << boldStart << errmsg.str() << boldStop << endl;
		throw runtime_error(errmsg.str());
	}
	fExpressionFilter->copyTree(*filter);

	// extract simple columns from parse tree
	vector<SimpleColumn*> scv;
	fExpressionFilter->walk(getSimpleCols, &scv);

	// populate the oid vectors
	for (vector<SimpleColumn*>::iterator it = scv.begin(); it != scv.end(); it++)
		addColumn(*it, jobInfo);
}


void ExpressionStep::addColumn(ReturnedColumn* rc, JobInfo& jobInfo)
{
	const vector<SimpleColumn*>*         scs = NULL;
	const vector<WindowFunctionColumn*>* wcs = NULL;
	ArithmeticColumn*                    ac = NULL;
	FunctionColumn*                      fc = NULL;
	SimpleColumn*                        sc = NULL;
	WindowFunctionColumn*                wc = NULL;

	// workaround for exp(sc) in (sub) where correlation is set on exp, but not sc.
	//     populate it to sc for correct scope resolution
	uint64_t correlated = rc->joinInfo();
	if (NULL != (ac = dynamic_cast<ArithmeticColumn*>(rc)))
	{
		scs = &(ac->simpleColumnList());
		wcs = &(ac->windowfunctionColumnList());
	}
	else if (NULL != (fc = dynamic_cast<FunctionColumn*>(rc)))
	{
		scs = &(fc->simpleColumnList());
		wcs = &(fc->windowfunctionColumnList());
		fVarBinOK = ((strcmp(fc->functionName().c_str(), "hex") == 0) ||
					 (strcmp(fc->functionName().c_str(), "length") == 0));
	}

	if (scs != NULL || wcs != NULL)
	{
		if (scs != NULL)
		{
			vector<SimpleColumn*>::const_iterator cit = scs->begin();
			vector<SimpleColumn*>::const_iterator end = scs->end();
			while (cit != end)
			{
				SimpleColumn* sc = *cit;
				sc->joinInfo(sc->joinInfo() | correlated);
				populateColumnInfo(*cit, jobInfo);
				++cit;
			}
		}

		if (wcs != NULL)
		{
			vector<WindowFunctionColumn*>::const_iterator cit = wcs->begin();
			vector<WindowFunctionColumn*>::const_iterator end = wcs->end();
			while (cit != end)
			{
				populateColumnInfo(*cit, jobInfo);
				++cit;
			}
		}
	}
	else if (NULL != (sc = dynamic_cast<SimpleColumn*>(rc)))
	{
		populateColumnInfo(sc, jobInfo);
	}
	else if (NULL != (wc = dynamic_cast<WindowFunctionColumn*>(rc)))
	{
		populateColumnInfo(rc, jobInfo);
	}
	else if (NULL != (dynamic_cast<AggregateColumn*>(rc)))
	{
		populateColumnInfo(rc, jobInfo);
	}
	else
	{
		ConstantColumn* cc = dynamic_cast<ConstantColumn*>(rc);

//		idbassert(cc != NULL)
		if (cc == NULL)
		{
			std::ostringstream errmsg;
			errmsg << "ExpressionStep: " << typeid(*rc).name() << " in expression.";
			cerr << boldStart << errmsg.str() << boldStop << endl;
			throw logic_error(errmsg.str());
		}
	}
}


void ExpressionStep::populateColumnInfo(ReturnedColumn* rc, JobInfo& jobInfo)
{
	// As of bug3695, make sure varbinary is not used in function expression.
	if (rc->resultType().colDataType == CalpontSystemCatalog::VARBINARY && !fVarBinOK)
		throw runtime_error("VARBINARY in filter or function is not supported.");

	SimpleColumn* sc = dynamic_cast<SimpleColumn*>(rc);
	WindowFunctionColumn* wc = NULL;
	AggregateColumn* ac = NULL;
	if (NULL != sc)
		return populateColumnInfo(sc, jobInfo);
	else if (NULL != (wc = dynamic_cast<WindowFunctionColumn*>(rc)))
		return populateColumnInfo(wc, jobInfo);
	else if (NULL != (ac = dynamic_cast<AggregateColumn*>(rc)))
		return populateColumnInfo(ac, jobInfo);
	else  // for now only allow simple and windowfunction column, more work to do.
		throw runtime_error("Error in parsing expression.");
}


void ExpressionStep::populateColumnInfo(SimpleColumn* sc, JobInfo& jobInfo)
{
	// As of bug3695, make sure varbinary is not used in function expression.
	if (sc->resultType().colDataType == CalpontSystemCatalog::VARBINARY && !fVarBinOK)
		throw runtime_error ("VARBINARY in filter or function is not supported.");

	CalpontSystemCatalog::OID tblOid = joblist::tableOid(sc, jobInfo.csc);
	string alias = extractTableAlias(sc);
	string view = sc->viewName();
	string schema = sc->schemaName();
	fTableOids.push_back(tblOid);
	CalpontSystemCatalog::ColType ct;
	if (schema.empty())
	{
		sc->oid(tblOid+1+sc->colPosition());
		ct = sc->resultType();
	}
	else if (sc->isInfiniDB() == false)
	{
		ct = sc->colType();
	}
	else
	{
		ct = sc->colType();
//XXX use this before connector sets colType in sc correctly.
//    type of pseudo column is set by connector
		if (dynamic_cast<PseudoColumn*>(sc) == NULL)
			ct = jobInfo.csc->colType(sc->oid());
//X
		if (ct.scale == 0)       // keep passed original ct for decimal type
			sc->resultType(ct);  // update from mysql type to calpont type
	}
	fAliases.push_back(alias);
	fViews.push_back(view);
	fSchemas.push_back(schema);
	fTableKeys.push_back(makeTableKey(jobInfo, sc));
	fColumns.push_back(sc);

	TupleInfo ti(setTupleInfo(ct, sc->oid(), jobInfo, tblOid, sc, alias));
	fColumnKeys.push_back(ti.key);
	// @bug 2990, MySQL date/datetime type is different from IDB type
	if (ti.dtype == CalpontSystemCatalog::DATE || ti.dtype == CalpontSystemCatalog::DATETIME)
	{
		if (ti.dtype != ct.colDataType)
		{
			ct.colWidth = ti.width;
			ct.colDataType = ti.dtype;
			ct.scale = ti.scale;
			ct.precision = ti.precision;
			sc->resultType(ct);
		}
	}

	CalpontSystemCatalog::OID dictOid = joblist::isDictCol(ct);
	if (dictOid > 0)
	{
		uint32_t tupleKey = ti.key;
		jobInfo.tokenOnly[tupleKey] = false;
		jobInfo.keyInfo->dictOidToColOid[dictOid] = sc->oid();
		ti = setTupleInfo(ct, dictOid, jobInfo, tblOid, sc, alias);
		jobInfo.keyInfo->dictKeyMap[tupleKey] = ti.key;
	}
}


void ExpressionStep::populateColumnInfo(WindowFunctionColumn* wc, JobInfo& jobInfo)
{
	// As of bug3695, make sure varbinary is not used in function expression.
	if (wc->resultType().colDataType == CalpontSystemCatalog::VARBINARY && !fVarBinOK)
		throw runtime_error("VARBINARY in filter or function is not supported.");

	// This is for window function in IN/EXISTS sub-query.
	// In 4.0 implementation, the window function is cloned to where clause in a simple filter.
	// This can be identified because SQL syntax does NOT allow window function in where clause.
	// Workaround here until a better way found.
	TupleInfo ti(setExpTupleInfo(wc->resultType(), wc->expressionId(), wc->alias(), jobInfo));
	uint64_t wcKey = ti.key;
	string alias("");
	string view("");
	string schema("");
	fTableOids.push_back(jobInfo.keyInfo->tupleKeyToTableOid[wcKey]);
	fAliases.push_back(alias);
	fViews.push_back(view);
	fSchemas.push_back(schema);
	fTableKeys.push_back(jobInfo.keyInfo->colKeyToTblKey[wcKey]);
	fColumnKeys.push_back(wcKey);
	fColumns.push_back(wc);
}


void ExpressionStep::populateColumnInfo(AggregateColumn* ac, JobInfo& jobInfo)
{
	// As of bug3695, make sure varbinary is not used in function expression.
	if (ac->resultType().colDataType == CalpontSystemCatalog::VARBINARY && !fVarBinOK)
		throw runtime_error("VARBINARY in filter or function is not supported.");

	// This is for aggregate function in IN/EXISTS sub-query.
	TupleInfo ti(setExpTupleInfo(ac->resultType(), ac->expressionId(), ac->alias(), jobInfo));
	uint64_t acKey = ti.key;
	string alias("");
	string view("");
	string schema("");
	fTableOids.push_back(jobInfo.keyInfo->tupleKeyToTableOid[acKey]);
	fAliases.push_back(alias);
	fViews.push_back(view);
	fSchemas.push_back(schema);
	fTableKeys.push_back(jobInfo.keyInfo->colKeyToTblKey[acKey]);
	fColumnKeys.push_back(acKey);
	fColumns.push_back(ac);
}


void ExpressionStep::updateInputIndex(map<uint32_t, uint32_t>& indexMap, const JobInfo& jobInfo)
{
	// expression is handled as function join already
	if (fDoJoin)
		return;

	if (jobInfo.trace)
		cout << "Input indices of Expression:" << (int64_t) fExpressionId << endl;

	for (vector<ReturnedColumn*>::iterator it = fColumns.begin(); it != fColumns.end(); ++it)
	{
		SimpleColumn* sc = dynamic_cast<SimpleColumn*>(*it);

		if (sc != NULL)
		{
			CalpontSystemCatalog::OID oid = sc->oid();
			CalpontSystemCatalog::OID dictOid = 0;
			CalpontSystemCatalog::ColType ct;
			uint32_t key = fColumnKeys[distance(fColumns.begin(), it)];
			if (sc->schemaName().empty())
			{
				ct = sc->resultType();
			}
			else if (sc->isInfiniDB() == false)
			{
				ct = sc->colType();
			}
			else
			{
				ct = sc->colType();
//XXX use this before connector sets colType in sc correctly.
//    type of pseudo column is set by connector
				if (dynamic_cast<PseudoColumn*>(sc) == NULL)
					ct = jobInfo.csc->colType(oid);
//X
				dictOid = joblist::isDictCol(ct);
				if (dictOid > 0)
					key = jobInfo.keyInfo->dictKeyMap[key];
			}
			sc->inputIndex(indexMap[key]);

			if (jobInfo.trace)
				cout <<"OID/key:"<<(dictOid?dictOid:oid)<<"/"<<key<<"("<<sc->tableAlias()<<"):";
		}
		else
		{
			(*it)->inputIndex(indexMap[getExpTupleKey(jobInfo, (*it)->expressionId())]);

			if (jobInfo.trace)
				cout << "EID:" << (*it)->expressionId();
		}

		if (jobInfo.trace)
			cout << (*it)->inputIndex() << endl;
	}

	// if substutes exist, update the original column
	for (map<SimpleColumn*, ReturnedColumn*>::iterator k = fSubMap.begin(); k != fSubMap.end(); k++)
		k->second->inputIndex(k->first->inputIndex());
}


void ExpressionStep::updateOutputIndex(map<uint32_t, uint32_t>& indexMap, const JobInfo& jobInfo)
{
	fExpression->outputIndex(indexMap[getExpTupleKey(jobInfo, fExpressionId)]);

	if (jobInfo.trace)
	{
		cout << "output index of Expression:" << (int64_t) fExpressionId << ":"
				<< fExpression->outputIndex() << endl << endl;
	}
}

void ExpressionStep::updateColumnOidAlias(JobInfo& jobInfo)
{
	for (size_t i = 0; i < fColumns.size(); i++)
	{
		SimpleColumn* sc = dynamic_cast<SimpleColumn*>(fColumns[i]);
		// virtual table columns
		if (sc != NULL && sc->schemaName().empty())
		{
			fTableOids[i] = joblist::tableOid(sc, jobInfo.csc);
			fAliases[i] = extractTableAlias(sc);
		}
	}
}


void ExpressionStep::substitute(uint64_t i, const SSC& ssc)
{
	fVsc.insert(ssc);  // save a local copy in case the virtual table in subquery be out of scope.
	fSubMap[ssc.get()] = fColumns[i];
	fColumns[i] = ssc.get();
}


void ExpressionStep::functionJoinCheck(SimpleFilter* sf, JobInfo& jobInfo)
{
	// only handle one & only one table at each side, and not the same table
	fFunctionJoinInfo.reset(new FunctionJoinInfo);
	if ((parseFuncJoinColumn(sf->lhs(), jobInfo) == false) ||
	    (parseFuncJoinColumn(sf->rhs(), jobInfo) == false) ||
	    (fFunctionJoinInfo->fTableKey[0] == fFunctionJoinInfo->fTableKey[1]) ||
	    (!compatibleColumnTypes(sf->lhs()->resultType(), sf->rhs()->resultType(), true)))
	{
		// for better error message
		if (fFunctionJoinInfo->fTableKey.size() == 2)
		{
			uint32_t t1 = fFunctionJoinInfo->fTableKey[0];
			uint32_t t2 = fFunctionJoinInfo->fTableKey[1];
			jobInfo.incompatibleJoinMap[t1] = t2;
			jobInfo.incompatibleJoinMap[t2] = t1;
		}

		// not convertible
		fFunctionJoinInfo.reset();
		return;
	}

    SJSTEP sjstep;
	ReturnedColumn* sfLhs = sf->lhs();
	ReturnedColumn* sfRhs = sf->rhs();
	if (dynamic_cast<SimpleColumn*>(sfLhs) == NULL)
	{
		SRCP lhs(sfLhs->clone());
		ExpressionStep* esLhs = new ExpressionStep(jobInfo);
		esLhs->expression(lhs, jobInfo);
		sjstep.reset(esLhs);
	}
	fFunctionJoinInfo->fStep.push_back(sjstep);
	sjstep.reset();

	if (dynamic_cast<SimpleColumn*>(sfRhs) == NULL)
	{
		SRCP rhs(sfRhs->clone());
		ExpressionStep* esRhs = new ExpressionStep(jobInfo);
		esRhs->expression(rhs, jobInfo);
		sjstep.reset(esRhs);
	}
	fFunctionJoinInfo->fStep.push_back(sjstep);

	JoinType jt = INNER;
	if (sfLhs->returnAll())
		jt = LEFTOUTER;
	else if (sfRhs->returnAll())
		jt = RIGHTOUTER;

	uint64_t joinInfo = sfLhs->joinInfo() | sfRhs->joinInfo();
	int64_t  correlatedSide = 0;
	ExpressionStep* ve = NULL;
	if (joinInfo != 0)
	{
		if (joinInfo & JOIN_SEMI)
			jt |= SEMI;

		if (joinInfo & JOIN_ANTI)
			jt |= ANTI;

		if (joinInfo & JOIN_SCALAR)
			jt |= SCALAR;

		if (joinInfo & JOIN_NULL_MATCH)
			jt |= MATCHNULLS;

		if (joinInfo & JOIN_CORRELATED)
			jt |= CORRELATED;

		if (joinInfo & JOIN_OUTER_SELECT)
			jt |= LARGEOUTER;

		if (sfLhs->joinInfo() & JOIN_CORRELATED)
		{
			correlatedSide = 1;
			ve = dynamic_cast<ExpressionStep*>(fFunctionJoinInfo->fStep[1].get());
		}
		else if (sfRhs->joinInfo() & JOIN_CORRELATED)
		{
			correlatedSide = 2;
			ve = dynamic_cast<ExpressionStep*>(fFunctionJoinInfo->fStep[0].get());
		}
	}

	if (ve != NULL)
		ve->virtualStep();

	fFunctionJoinInfo->fJoinType = jt;
	fFunctionJoinInfo->fCorrelatedSide = correlatedSide;
	fFunctionJoinInfo->fJoinId = ++jobInfo.joinNum;

	jobInfo.functionJoins.push_back(this);
}


bool ExpressionStep::parseFuncJoinColumn(ReturnedColumn* rc, JobInfo& jobInfo)
{
	set<uint32_t> tids;     // tables used in the expression
	set<uint32_t> cids;     // columns used in the expression
	uint32_t key = -1;      // join key
	uint32_t tid = -1;      // table Id of the simple column
	bool isSc = false;      // rc is a simple column

	ArithmeticColumn* ac = dynamic_cast<ArithmeticColumn*>(rc);
	FunctionColumn*   fc = dynamic_cast<FunctionColumn*>(rc);
	SimpleColumn*     sc = dynamic_cast<SimpleColumn*>(rc);
	if (sc != NULL)
	{
		isSc = true;
		key = getTupleKey(jobInfo, sc);
		tid = getTableKey(jobInfo, key);
		if (jobInfo.keyInfo->dictKeyMap.find(key) != jobInfo.keyInfo->dictKeyMap.end())
			key = jobInfo.keyInfo->dictKeyMap[key];
		tids.insert(tid);
		cids.insert(key);
	}
	else if (ac != NULL || fc != NULL)
	{
		TupleInfo ti(setExpTupleInfo(rc, jobInfo));
		key = ti.key;
		for (uint32_t i = 0; i < rc->simpleColumnList().size(); i++)
		{
			sc = rc->simpleColumnList()[i];
			uint32_t cid = getTupleKey(jobInfo, sc);
			tid = getTableKey(jobInfo, cid);
			tids.insert(tid);
			cids.insert(cid);
		}
	}

	int32_t tableOid = -1;
	int32_t oid = -1;
	string alias;
	string view;
	string schema;
	if (sc && tids.size() == 1)
	{
		tableOid = joblist::tableOid(sc, jobInfo.csc);
		oid = sc->oid();
		alias = extractTableAlias(sc);
		view = sc->viewName();
		schema = sc->schemaName();
	}
	else if (dynamic_cast<AggregateColumn*>(rc)  || dynamic_cast<WindowFunctionColumn*>(rc) ||
	         dynamic_cast<ArithmeticColumn*>(rc) || dynamic_cast<FunctionColumn*>(rc))
	{
		tableOid = execplan::CNX_VTABLE_ID;
		oid = rc->expressionId();
		alias = jobInfo.subAlias;
	}
	else
	{
		return false;
	}

	if (isSc == false)
		jobInfo.keyInfo->functionJoinKeys.insert(key);

	fFunctionJoinInfo->fExpression.push_back(rc);
	fFunctionJoinInfo->fJoinKey.push_back(key);
	fFunctionJoinInfo->fTableKey.push_back(tid);
	fFunctionJoinInfo->fColumnKeys.push_back(cids);
	fFunctionJoinInfo->fTableOid.push_back(tableOid);
	fFunctionJoinInfo->fOid.push_back(oid);
	fFunctionJoinInfo->fSequence.push_back(rc->sequence());
	fFunctionJoinInfo->fAlias.push_back(alias);
	fFunctionJoinInfo->fView.push_back(view);
	fFunctionJoinInfo->fSchema.push_back(schema);

	return true;
}


const string ExpressionStep::toString() const
{
	ostringstream oss;
	oss << "ExpressionStep  ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
		oss << fInputJobStepAssociation.outAt(i);

	return oss.str();
}


}   //namespace
// vim:ts=4 sw=4:

