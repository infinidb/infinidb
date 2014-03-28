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

/***********************************************************************
*   $Id: ha_in_sub.cpp 6407 2010-03-26 19:36:56Z zzhu $
*
*
***********************************************************************/
/** @file */
/** class InSub definition */

#include <stdint.h>
//#define NDEBUG
#include <cassert>
#include <vector>
using namespace std;

#include "idb_mysql.h"

#include "parsetree.h"
#include "logicoperator.h"
#include "existsfilter.h"
#include "simplescalarfilter.h"
#include "selectfilter.h"
#include "simplefilter.h"
#include "predicateoperator.h"
#include "rowcolumn.h"
using namespace execplan;

#include "errorids.h"
using namespace logging;

#include "ha_subquery.h"

namespace cal_impl_if
{
extern void parse_item (Item *item, vector<Item_field*>& field_vec, bool& hasNonSupportItem, uint16& parseInfo);

void makeAntiJoin(const ParseTree* n)
{
	TreeNode *tn = n->data();
	SimpleFilter *sf = dynamic_cast<SimpleFilter*>(tn);
	if (!sf)
		return;
	uint64_t lJoinInfo = sf->lhs()->joinInfo();
	
	if (lJoinInfo & JOIN_SEMI)
	{
		lJoinInfo &= ~JOIN_SEMI;
		lJoinInfo |= JOIN_ANTI;
		if (lJoinInfo & JOIN_NULLMATCH_CANDIDATE)
			lJoinInfo |= JOIN_NULL_MATCH;
		sf->lhs()->joinInfo(lJoinInfo);
	}
	uint64_t rJoinInfo = sf->rhs()->joinInfo();
	if (rJoinInfo & JOIN_SEMI)
	{
		rJoinInfo &= ~JOIN_SEMI;
		rJoinInfo |= JOIN_ANTI;
		if (rJoinInfo & JOIN_NULLMATCH_CANDIDATE)
			rJoinInfo |= JOIN_NULL_MATCH;
		sf->rhs()->joinInfo(rJoinInfo);
	}
}

InSub::InSub(gp_walk_info& gwip) : WhereSubQuery(gwip) 
{}

InSub::InSub(gp_walk_info& gwip, Item_func* func) :
	WhereSubQuery(gwip, func)
{}

InSub::InSub(const InSub& rhs) :
	WhereSubQuery(rhs.gwip(), rhs.fColumn, rhs.fSub, rhs.fFunc)
{}

InSub::~InSub()
{}

/** MySQL transform (NOT) IN subquery to (NOT) EXIST
 *
 */
execplan::ParseTree* InSub::transform()
{
	if (!fFunc)
		return NULL;

	// @todo need to handle scalar IN and BETWEEN specially
	// this blocks handles only one subselect scalar
	// arg[0]: column | arg[1]: subselect
	//assert (fFunc->arg_count == 2 && fGwip.rcWorkStack.size() >= 2);
	if (fFunc->arg_count != 2 || fGwip.rcWorkStack.size() < 2)
	{
		fGwip.fatalParseError = true;
		fGwip.parseErrorText = "Unsupported item in IN subquery";
		return NULL;
	}
	
	ReturnedColumn* rhs = fGwip.rcWorkStack.top();
	fGwip.rcWorkStack.pop();
	delete rhs;
	ReturnedColumn* lhs = fGwip.rcWorkStack.top();
	fGwip.rcWorkStack.pop();	
	delete lhs;	
	
	fSub = (Item_subselect*)(fFunc->arguments()[1]);
	idbassert(fSub && fFunc);
	
	SCSEP csep (new CalpontSelectExecutionPlan());
	csep->sessionID(fGwip.sessionid);	
	csep->location(CalpontSelectExecutionPlan::WHERE);
	csep->subType (CalpontSelectExecutionPlan::IN_SUBS);
	
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
		fGwip.fatalParseError = true;
		if (gwi.fatalParseError && !gwi.parseErrorText.empty())
			fGwip.parseErrorText = gwi.parseErrorText;
		else
			fGwip.parseErrorText = "Error occured in InSub::transform()";
		return NULL;
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
		
	ExistsFilter *subFilter = new ExistsFilter();
	subFilter->sub(csep);
	
	if (gwi.subQuery->correlated())
		subFilter->correlated(true);
	else
		subFilter->correlated(false);
	if (fGwip.clauseType == HAVING && subFilter->correlated())
	{
		fGwip.fatalParseError = true;
		fGwip.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_NON_SUPPORT_HAVING);
	}
	
	return new ParseTree(subFilter);	
}

/**
 * Handle MySQL's plugin functions
 * This is mostly for handling the null related functions that MySQL adds to the execution plan
 */
void InSub::handleFunc(gp_walk_info* gwip, Item_func* func)
{
	if (func->functype() == Item_func::TRIG_COND_FUNC || func->functype() == Item_func::COND_OR_FUNC)
	{
		// purpose: remove the isnull() function from the parsetree in ptWorkStack.
		// IDB handles the null semantics in the join operation
		// trigcond(or_cond) is the only form we recognize for now
		if (func->arg_count > 2)
		{
			fGwip.fatalParseError = true;
			fGwip.parseErrorText = "Unsupported item in IN subquery";
			return;
		}
		Item_cond* cond;
		
		if (func->functype() == Item_func::TRIG_COND_FUNC)
		{
			Item* item;
			if (func->arguments()[0]->type() == Item::REF_ITEM)
				item = (Item_ref*)(func->arguments()[0])->real_item();
			else
				item = func->arguments()[0];
			cond = (Item_cond*)(item);
		}
		else
		{
			cond = (Item_cond*)(func);
		}
		if (cond->functype() == Item_func::COND_OR_FUNC)
		{
			// (cache=item) case. do nothing. ignore trigcond()?
			if (cond->argument_list()->elements == 1)
				return;
			// (cache=item or isnull(item)) case. remove "or isnull()"
			if (cond->argument_list()->elements == 2)
			{
				// don't know how to deal with this. don't think it's a fatal error either.
				if (gwip->ptWorkStack.empty())
					return;
				ParseTree *pt = gwip->ptWorkStack.top();
				if (!pt->left() || !pt->right())
					return;
				SimpleFilter *sf = dynamic_cast<SimpleFilter*>(pt->left()->data());
				//assert (sf && sf->op()->op() == execplan::OP_ISNULL);
				if (!sf || sf->op()->op() != execplan::OP_ISNULL)
					return;
				delete sf;
				sf = dynamic_cast<SimpleFilter*>(pt->right()->data());
				//idbassert(sf && sf->op()->op() == execplan::OP_EQ);
				if (!sf || sf->op()->op() != execplan::OP_EQ)
					return;
					
				// set NULLMATCH for both operand. It's really a setting for the join.
				// should only set NULLMATCH when the subtype is NOT_IN. for some IN subquery 
				// with aggregation column, MySQL inefficiently convert to:
				// (cache=item or item is null) and item is not null, which is equivalent to
				// cache = item. Do not set NULLMATCH for this case.
				// Because we don't know IN or NOTIN yet, set candidate bit and switch to NULLMATCH
				// later in handleNot function.
				if (sf->lhs()->joinInfo() & JOIN_CORRELATED)
					sf->lhs()->joinInfo(sf->lhs()->joinInfo() | JOIN_NULLMATCH_CANDIDATE);
				if (sf->rhs()->joinInfo() & JOIN_CORRELATED)
					sf->rhs()->joinInfo(sf->rhs()->joinInfo() | JOIN_NULLMATCH_CANDIDATE);
				pt = pt->right();
				gwip->ptWorkStack.pop();
				gwip->ptWorkStack.push(pt);
			}
		}
		else if (cond->functype() == Item_func::EQ_FUNC)
		{
			// not in (select const ...)
			if (gwip->ptWorkStack.empty())
				return;
			ParseTree *pt = gwip->ptWorkStack.top();
			SimpleFilter *sf = dynamic_cast<SimpleFilter*>(pt->data());
			if (!sf || sf->op()->op() != execplan::OP_EQ)
				return;
					
			if (sf->lhs()->joinInfo() & JOIN_CORRELATED)
				sf->lhs()->joinInfo(sf->lhs()->joinInfo() | JOIN_NULLMATCH_CANDIDATE);
			if (sf->rhs()->joinInfo() & JOIN_CORRELATED)
				sf->rhs()->joinInfo(sf->rhs()->joinInfo() | JOIN_NULLMATCH_CANDIDATE);
		}
	}
}

/**
 * This is invoked when a NOT function is got. It's usually the case NOT<IN optimizer>
 * This function will simple turn the semi join to anti join
 *
 */
void InSub::handleNot()
{
	ParseTree *pt = fGwip.ptWorkStack.top();
	ExistsFilter *subFilter = dynamic_cast<ExistsFilter*>(pt->data());
	idbassert(subFilter);
	subFilter->notExists(true);
	SCSEP csep = subFilter->sub();
	const ParseTree* ptsub = csep->filters();
	if (ptsub)
		ptsub->walk(makeAntiJoin);
	ptsub = csep->having();
	if (ptsub)
		ptsub->walk(makeAntiJoin);
}

}
