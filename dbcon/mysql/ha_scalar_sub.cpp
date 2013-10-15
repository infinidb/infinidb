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

/***********************************************************************
*   $Id: ha_scalar_sub.cpp 6418 2010-03-29 21:55:08Z zzhu $
*
*
***********************************************************************/
/** @file */
/** class ScalarSub definition */

//#define NDEBUG
#include <cassert>
#include <vector>
using namespace std;

#include "idb_mysql.h"

#include "parsetree.h"
#include "logicoperator.h"
#include "selectfilter.h"
#include "simplescalarfilter.h"
#include "predicateoperator.h"
#include "rowcolumn.h"
#include "simplecolumn.h"
#include "simplefilter.h"
#include "constantcolumn.h"
using namespace execplan;

#include "errorids.h"
using namespace logging;

#include "ha_subquery.h"

namespace cal_impl_if
{
ScalarSub::ScalarSub(gp_walk_info& gwip) : WhereSubQuery(gwip), fReturnedColPos(0)
{}

ScalarSub::ScalarSub(gp_walk_info& gwip, Item_func* func) :
	WhereSubQuery(gwip, func), fReturnedColPos(0)
{}

ScalarSub::ScalarSub(gp_walk_info& gwip, const execplan::SRCP& column, Item_subselect* sub, Item_func* func):
	WhereSubQuery(gwip, column, sub, func), fReturnedColPos(0)
{}

ScalarSub::ScalarSub(const ScalarSub& rhs) :
	WhereSubQuery(rhs.gwip(), rhs.fColumn, rhs.fSub, rhs.fFunc),
	fReturnedColPos(rhs.fReturnedColPos)
{}

ScalarSub::~ScalarSub()
{}

execplan::ParseTree* ScalarSub::transform()
{
	if (!fFunc)
		return NULL;

	// @todo need to handle scalar IN and BETWEEN specially
	// this blocks handles only one subselect scalar
	// arg[0]: column | arg[1]: subselect
	//idbassert(fGwip.rcWorkStack.size() >= 2);
	if (fFunc->functype() == Item_func::BETWEEN)
		return transform_between();
	if (fFunc->functype() == Item_func::IN_FUNC)
		return transform_in();
	
	ReturnedColumn* rhs = NULL;
	ReturnedColumn* lhs = NULL;
	if (!fGwip.rcWorkStack.empty())
	{
		rhs = fGwip.rcWorkStack.top();
		fGwip.rcWorkStack.pop();
	}
	if (!fGwip.rcWorkStack.empty())
	{
		lhs = fGwip.rcWorkStack.top();
		fGwip.rcWorkStack.pop();
	}

	PredicateOperator *op = new PredicateOperator(fFunc->func_name());	
	if (!lhs && (fFunc->functype() == Item_func::ISNULL_FUNC || 
		           fFunc->functype() == Item_func::ISNOTNULL_FUNC))
	{
		fSub = (Item_subselect*)(fFunc->arguments()[0]);
		fColumn.reset(new ConstantColumn("", ConstantColumn::NULLDATA));
		delete rhs;
		return buildParseTree(op);
	}
	
	bool reverseOp = false;
	SubSelect* sub = dynamic_cast<SubSelect*>(rhs);
	if (!sub)
	{
		reverseOp = true;
		delete lhs;
		lhs = rhs;	
		fSub = (Item_subselect*)(fFunc->arguments()[0]);			
	}
	else
	{
		delete rhs;
		fSub = (Item_subselect*)(fFunc->arguments()[1]);
	}
	fColumn.reset(lhs); // column should be in the stack already. in, between may be different	
	//PredicateOperator *op = new PredicateOperator(fFunc->func_name());
	if (reverseOp)
		op->reverseOp();

	return buildParseTree(op);
}

execplan::ParseTree* ScalarSub::transform_between()
{
	//idbassert(fGwip.rcWorkStack.size() >= 3);
	if (fGwip.rcWorkStack.size() < 3)
	{
		fGwip.fatalParseError = true;
		fGwip.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SCALAR);
		return NULL;
	}
	ReturnedColumn* op3 = fGwip.rcWorkStack.top();
	fGwip.rcWorkStack.pop();
	ReturnedColumn* op2 = fGwip.rcWorkStack.top();
	fGwip.rcWorkStack.pop();
	ReturnedColumn* op1 = fGwip.rcWorkStack.top();
	fGwip.rcWorkStack.pop();
	fColumn.reset(op1);
	
	ParseTree* lhs = NULL;
	ParseTree* rhs = NULL;
	PredicateOperator* op_LE = new PredicateOperator("<=");
	PredicateOperator* op_GE = new PredicateOperator(">=");
	
	SubSelect* sub2 = dynamic_cast<SubSelect*>(op3);
	fSub = (Item_subselect*)(fFunc->arguments()[2]);
	if (sub2)
	{
		rhs = buildParseTree(op_LE);
		delete sub2;
	}
	else
	{
		SOP sop;
		sop.reset(op_LE);
		rhs = new ParseTree(new SimpleFilter(sop, fColumn.get(), op3));
	}
		
	SubSelect* sub1 = dynamic_cast<SubSelect*>(op2);
	fSub = (Item_subselect*)(fFunc->arguments()[1]);
	if (sub1)
	{
		lhs = buildParseTree(op_GE);
		delete sub1;
	}
	else
	{
		SOP sop;
		sop.reset(op_GE);
		lhs = new ParseTree(new SimpleFilter(sop, fColumn.get(), op2));
	}
	
	if (!rhs || !lhs)
	{
		fGwip.fatalParseError = true;
		fGwip.parseErrorText = "non-supported scalar subquery";
		fGwip.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SCALAR);
		return NULL;
	}
	ParseTree* pt = new ParseTree (new LogicOperator("and"));
	pt->left(lhs);
	pt->right(rhs);
	return pt;
}

execplan::ParseTree* ScalarSub::transform_in()
{
	fGwip.fatalParseError = true;
	fGwip.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SCALAR);
	return NULL;
}

execplan::ParseTree* ScalarSub::buildParseTree(PredicateOperator* op)
{
	idbassert(fColumn.get() && fSub && fFunc);
	
	vector<SRCP> cols;
	Filter *filter;
	RowColumn* rcol = dynamic_cast<RowColumn*>(fColumn.get());
	if (rcol)
	{
		// IDB only supports (c1,c2..) =/!= (subquery)
		if (fFunc->functype() != Item_func::EQ_FUNC && fFunc->functype() != Item_func::NE_FUNC)
		{
			fGwip.fatalParseError = true;
			fGwip.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_INVALID_OPERATOR_WITH_LIST);
			return NULL;
		}
		cols = rcol->columnVec();
	}
	else
		cols.push_back(fColumn);
		
	SCSEP csep(new CalpontSelectExecutionPlan());
	csep->sessionID(fGwip.sessionid);
	csep->location(CalpontSelectExecutionPlan::WHERE);
	csep->subType (CalpontSelectExecutionPlan::SINGLEROW_SUBS);
	
	// gwi for the sub query
	gp_walk_info gwi;
	gwi.thd = fGwip.thd;
	gwi.subQuery = this;
	
	// @4827 merge table list to gwi in case there is FROM sub to be referenced
	// in the FROM sub
	uint derivedTbCnt = fGwip.derivedTbList.size();
	uint tbCnt = fGwip.tbList.size();

	gwi.tbList.insert(gwi.tbList.begin(), fGwip.tbList.begin(), fGwip.tbList.end());
	gwi.derivedTbList.insert(gwi.derivedTbList.begin(), fGwip.derivedTbList.begin(), fGwip.derivedTbList.end());
	
	if (getSelectPlan(gwi, *(fSub->get_select_lex()), csep) != 0)
	{
		//@todo more in error handling
		if (!gwi.fatalParseError)
		{
			fGwip.fatalParseError = true;
			fGwip.parseErrorText = "Error occured in ScalarSub::transform()";
		}
		else
		{
			fGwip.fatalParseError = gwi.fatalParseError;
			fGwip.parseErrorText = gwi.parseErrorText;
		}
		return NULL;
	}
	
	// error out non-support case for now: comparison out of semi join tables.
	// only check for simplecolumn
	if (!gwi.correlatedTbNameVec.empty())
	{
		for (uint i = 0; i < cols.size(); i++)
		{
			SimpleColumn* sc = dynamic_cast<SimpleColumn*>(cols[i].get());
			if (sc)
			{
				CalpontSystemCatalog::TableAliasName tan = make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias());
				uint j = 0;
				for (; j < gwi.correlatedTbNameVec.size(); j++)
					if (tan == gwi.correlatedTbNameVec[j])
						break;
				if (j == gwi.correlatedTbNameVec.size())
				{
					fGwip.fatalParseError = true;
					fGwip.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SCALAR);
					return NULL;
				}
			}
		}
	}
	
	// remove outer query tables
	CalpontSelectExecutionPlan::TableList tblist;
	if (csep->tableList().size() >= tbCnt)
		tblist.insert(tblist.begin(),csep->tableList().begin()+tbCnt, csep->tableList().end());
	CalpontSelectExecutionPlan::SelectList derivedTbList;
	if (csep->derivedTableList().size() >= derivedTbCnt)
		derivedTbList.insert(derivedTbList.begin(), csep->derivedTableList().begin()+derivedTbCnt, csep->derivedTableList().end());
	
	csep->tableList(tblist);
	csep->derivedTableList(derivedTbList);
	
	if (fSub->is_correlated)
	{
		SelectFilter *subFilter = new SelectFilter();
		subFilter->correlated(true);
		subFilter->cols(cols);
		subFilter->sub(csep);
		subFilter->op(SOP(op));
		subFilter->returnedColPos(fReturnedColPos);
		filter = subFilter;
	}
	else
	{
		SimpleScalarFilter *subFilter = new SimpleScalarFilter();
		subFilter->cols(cols);
		subFilter->sub(csep);
		subFilter->op(SOP(op));
		filter = subFilter;
	}
	return new ParseTree(filter);	
	
}

}
