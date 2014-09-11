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

//  $Id: expressionstep.cpp 8326 2012-02-15 18:58:10Z xlou $


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
#include "constantfilter.h"
#include "simplefilter.h"
using namespace execplan;

#include "jlf_common.h"
#include "rowgroup.h"
using namespace rowgroup;

#include "expressionstep.h"

namespace joblist
{
ExpressionStep::ExpressionStep(
	uint32_t sessionId,
	uint32_t txnId,
	uint32_t verId,
	uint32_t statementId) :
		fSessionId(sessionId),
		fTxnId(txnId),
		fVerId(verId),
		fStatementId(statementId),
		fExpressionFilter(NULL),
		fExpressionId(-1),
		fVarBinOK(false),
		fSelectFilter(false),
		fAssociatedJoinId(0)
{
}


ExpressionStep::ExpressionStep(const ExpressionStep& rhs) :
	fInputJobStepAssociation(rhs.inputAssociation()),
	fOutputJobStepAssociation(rhs.outputAssociation()),
	fSessionId(rhs.sessionId()),
	fTxnId(rhs.txnId()),
	fVerId(rhs.verId()),
	fStepId(rhs.stepId()),
	fStatementId(rhs.statementId()),
	fExpression(rhs.expression()),
	fExpressionFilter(NULL),
	fExpressionId(rhs.expressionId()),
	fTableOids(tableOids()),
	fOids(rhs.oids()),
	fAliases(rhs.aliases()),
	fViews(rhs.views()),
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
	assert(fExpressionFilter != NULL);
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
	assert(fExpressionFilter != NULL);
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
	const vector<SimpleColumn*>* cols = NULL;
	const ArithmeticColumn*     ac = NULL;
	const FunctionColumn*       fc = NULL; 
	SimpleColumn*         sc = NULL;

	if (NULL != (ac = dynamic_cast<const ArithmeticColumn*>(rc)))
	{
		cols = &(ac->simpleColumnList());
	}
	else if (NULL != (fc = dynamic_cast<const FunctionColumn*>(rc)))
	{
		cols = &(fc->simpleColumnList());
		fVarBinOK = ((strcmp(fc->functionName().c_str(), "hex") == 0) ||
					 (strcmp(fc->functionName().c_str(), "length") == 0));
	}


	if (cols != NULL)
	{

		vector<SimpleColumn*>::const_iterator cit = cols->begin();
		vector<SimpleColumn*>::const_iterator end = cols->end();
		while (cit != end)
		{
			populateColumnInfo(*cit, jobInfo);
			++cit;
		}
	}
	else if (NULL != (sc = dynamic_cast<SimpleColumn*>(rc)))
	{
		populateColumnInfo(sc, jobInfo);
	}
	else
	{
		const ConstantColumn* cc = dynamic_cast<const ConstantColumn*>(rc);

		assert(cc != NULL);
		if (cc == NULL)
		{
			std::ostringstream errmsg;
			errmsg << "ExpressionStep: " << typeid(*rc).name() << " in expression.";
			cerr << boldStart << errmsg.str() << boldStop << endl;
			throw logic_error(errmsg.str());
		}
	}
}


void ExpressionStep::populateColumnInfo(SimpleColumn* sc, JobInfo& jobInfo)
{
	// As of bug3695, make sure varbinary is not used in function expression.
	if (sc->resultType().colDataType == CalpontSystemCatalog::VARBINARY && !fVarBinOK) 
		throw runtime_error ("VARBINARY in filter or function is not supported.");

	CalpontSystemCatalog::OID tblOid = joblist::tableOid(sc, jobInfo.csc);
	string alias = extractTableAlias(sc);
	string view = sc->viewName();
	fTableOids.push_back(tblOid);
	CalpontSystemCatalog::ColType ct;
	if (sc->schemaName().empty())
	{
		sc->oid(tblOid+1+sc->colPosition());
		ct = sc->resultType();
	}
	else
	{
		ct = jobInfo.csc->colType(sc->oid());
		if (ct.scale == 0)       // keep passed original ct for decimal type
			sc->resultType(ct);  // update from mysql type to calpont type
	}
	fOids.push_back(sc->oid());
	fAliases.push_back(alias);
	fViews.push_back(view);
	fColumns.push_back(sc);

	TupleInfo ti(setTupleInfo(ct, sc->oid(), jobInfo, tblOid, sc, alias));
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


void ExpressionStep::updateInputIndex(map<uint, uint>& indexMap, const JobInfo& jobInfo)
{
	for (vector<ReturnedColumn*>::iterator it = fColumns.begin(); it != fColumns.end(); ++it)
	{
		SimpleColumn* sc = dynamic_cast<SimpleColumn*>(*it);

		if (sc != NULL)
		{
			CalpontSystemCatalog::OID oid = sc->oid();
			CalpontSystemCatalog::ColType ct;
			if (sc->schemaName().empty())
			{
				ct = sc->resultType();
			}
			else
			{
				ct = jobInfo.csc->colType(oid);
				CalpontSystemCatalog::OID dictOid = joblist::isDictCol(ct);
				if (dictOid > 0)
					oid = dictOid;
			}
			sc->inputIndex(indexMap[getTupleKey(
				jobInfo, oid, extractTableAlias(sc), sc->viewName())]);
		}
		else
		{
			ArithmeticColumn* ac = dynamic_cast<ArithmeticColumn*>(*it);
			AggregateColumn*  ag = NULL;
			FunctionColumn*   fc = NULL; 

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
		}
	}

	if (jobInfo.trace)
	{
		cout << "Input indices of Expression:" << (int64_t) fExpressionId << endl;
		for (vector<ReturnedColumn*>::iterator it = fColumns.begin(); it != fColumns.end(); ++it)
		{
			SimpleColumn* sc = dynamic_cast<SimpleColumn*>(*it);
			if (sc != NULL)
				cout << "OID:" << sc->oid() << "(" << sc->tableAlias() << "):";
			else
				cout << "EID:" << (*it)->expressionId();

			cout << (*it)->inputIndex() << endl;
		}
	}
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

