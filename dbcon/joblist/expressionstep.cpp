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
#include "simplecolumn.h"
#include "windowfunctioncolumn.h"
#include "constantfilter.h"
#include "simplefilter.h"
using namespace execplan;

#include "jlf_common.h"
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
	fAssociatedJoinId(0)
{
}


ExpressionStep::ExpressionStep(const ExpressionStep& rhs) :
	JobStep(rhs),
	fExpression(rhs.expression()),
	fExpressionFilter(NULL),
	fExpressionId(rhs.expressionId()),
	fTableOids(tableOids()),
	fOids(rhs.oids()),
	fAliases(rhs.aliases()),
	fViews(rhs.views()),
	fSchemas(rhs.schemas()),
	fTableKeys(rhs.tableKeys()),
	fColumnKeys(rhs.columnKeys()),
	fVarBinOK(rhs.fVarBinOK),
	fSelectFilter(rhs.fSelectFilter),
	fAssociatedJoinId(rhs.fAssociatedJoinId)
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
	FunctionColumn*   fc = NULL;
	if (ac != NULL)
		fExpressionId = ac->expressionId();
	else if ((fc = dynamic_cast<FunctionColumn*>(fExpression.get())) != NULL)
		fExpressionId = fc->expressionId();
	else
		throw runtime_error("Unsupported Expression");

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
	}
	else if ((cf = dynamic_cast<ConstantFilter*>(f)) != NULL)
	{
		//addColumn(cf->col().get(), jobInfo);
		for (uint i = 0; i < cf->simpleColumnList().size(); i++)
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
	if (NULL != sc)
		return populateColumnInfo(sc, jobInfo);
	else if (NULL != (wc = dynamic_cast<WindowFunctionColumn*>(rc)))
		return populateColumnInfo(wc, jobInfo);
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
		ct = jobInfo.csc->colType(sc->oid());
//X
		if (ct.scale == 0)       // keep passed original ct for decimal type
			sc->resultType(ct);  // update from mysql type to calpont type
	}
	fOids.push_back(sc->oid());
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
		uint tupleKey = ti.key;
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
	fOids.push_back(wc->expressionId());
	fAliases.push_back(alias);
	fViews.push_back(view);
	fSchemas.push_back(schema);
	fTableKeys.push_back(jobInfo.keyInfo->colKeyToTblKey[wcKey]);
	fColumnKeys.push_back(wcKey);
	fColumns.push_back(wc);
}


void ExpressionStep::updateInputIndex(map<uint, uint>& indexMap, const JobInfo& jobInfo)
{
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
			uint key = fColumnKeys[distance(fColumns.begin(), it)];
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
				ct = jobInfo.csc->colType(oid);
				dictOid = joblist::isDictCol(ct);
				if (dictOid > 0)
					key = jobInfo.keyInfo->dictKeyMap[key];
			}
			sc->inputIndex(indexMap[key]);

			if (jobInfo.trace)
				cout << "OID:" << (dictOid ? dictOid : oid) << "(" << sc->tableAlias() << "):";
		}
		else
		{
			ArithmeticColumn*     ac = dynamic_cast<ArithmeticColumn*>(*it);
			AggregateColumn*      ag = NULL;
			FunctionColumn*       fc = NULL;
			WindowFunctionColumn* wc = NULL;

			if (ac != NULL)
			{
				ac->inputIndex(indexMap[getExpTupleKey(jobInfo, ac->expressionId())]);
			}
			else if ((ag = dynamic_cast<AggregateColumn*>(*it)) != NULL)
			{
				ag->inputIndex(indexMap[getExpTupleKey(jobInfo, ag->expressionId())]);
			}
			else if ((fc = dynamic_cast<FunctionColumn*>(*it)) != NULL)
			{
				fc->inputIndex(indexMap[getExpTupleKey(jobInfo, fc->expressionId())]);
			}
			else if ((wc = dynamic_cast<WindowFunctionColumn*>(*it)) != NULL)
			{
				wc->inputIndex(indexMap[getExpTupleKey(jobInfo, wc->expressionId())]);
			}

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


void ExpressionStep::updateOutputIndex(map<uint, uint>& indexMap, const JobInfo& jobInfo)
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
			fOids[i] = sc->oid();
			fAliases[i] = extractTableAlias(sc);
		}
	}
}


void ExpressionStep::substitute(uint64_t i, const SSC& ssc)
{
	fSubstitutes.push_back(ssc);
	fSubMap[ssc.get()] = fColumns[i];
	fColumns[i] = ssc.get();
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

