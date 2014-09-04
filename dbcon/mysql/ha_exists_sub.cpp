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

/***********************************************************************
*   $Id: ha_exists_sub.cpp 6426 2010-03-30 18:46:11Z zzhu $
*
*
***********************************************************************/
/** @file */
/** class ExistsSub definition */

//#define NDEBUG
#include <cassert>

#include "idb_mysql.h"

#include "parsetree.h"
#include "existsfilter.h"
#include "simplefilter.h"
#include "constantcolumn.h"
using namespace execplan;

#include "errorids.h"

#include "ha_subquery.h"

namespace cal_impl_if
{
extern void makeAntiJoin(const ParseTree* n);

void checkCorrelation(const ParseTree* n, void* obj)
{
	ExistsFilter *ef = reinterpret_cast<ExistsFilter*>(obj);
	TreeNode *tn = n->data();
	SimpleFilter *sf = dynamic_cast<SimpleFilter*>(tn);
	if (!sf)
		return;
	uint64_t lJoinInfo = sf->lhs()->joinInfo();
	uint64_t rJoinInfo = sf->rhs()->joinInfo();
	
	if (lJoinInfo & JOIN_CORRELATED)
	{
		ConstantColumn *cc = dynamic_cast<ConstantColumn*>(sf->rhs());
		if ((!cc || (cc && sf->op()->op() == OP_EQ)) && !(rJoinInfo & JOIN_CORRELATED))
			ef->correlated(true);
	}
	if (rJoinInfo & JOIN_CORRELATED)
	{
		ConstantColumn *cc = dynamic_cast<ConstantColumn*>(sf->lhs());
		if ((!cc || (cc && sf->op()->op() == OP_EQ)) && !(lJoinInfo & JOIN_CORRELATED))
			ef->correlated(true);
	}
}

ExistsSub::ExistsSub(gp_walk_info& gwip) : WhereSubQuery(gwip) 
{}

ExistsSub::ExistsSub(gp_walk_info& gwip, Item_subselect* sub) :
	WhereSubQuery(gwip, sub)
{}

ExistsSub::~ExistsSub()
{}

execplan::ParseTree* ExistsSub::transform()
{
	idbassert(fSub);
	
	SCSEP csep(new CalpontSelectExecutionPlan());
	csep->sessionID(fGwip.sessionid);	
	csep->location(CalpontSelectExecutionPlan::WHERE);
	csep->subType (CalpontSelectExecutionPlan::EXISTS_SUBS);
	
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

	if (fSub->get_select_lex()->with_sum_func)
	{
		fGwip.fatalParseError = true;
		fGwip.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_AGG_EXISTS);
		return NULL;
	}
	
	if (getSelectPlan(gwi, *(fSub->get_select_lex()), csep) != 0)
	{
		fGwip.fatalParseError = true;
		if (gwi.fatalParseError && !gwi.parseErrorText.empty())
			fGwip.parseErrorText = gwi.parseErrorText;
		else
			fGwip.parseErrorText = "Error occured in ExistsSub::transform()";
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
	subFilter->correlated(false);
	subFilter->sub(csep);
	const ParseTree* pt = csep->filters();
	if (pt)
		pt->walk(checkCorrelation, subFilter);
	return new ParseTree(subFilter);	
}

/**
 * This is invoked when a NOT function is got. It's usually the case NOT<IN optimizer>
 * This function will simple turn the semi join to anti join
 *
 */
void ExistsSub::handleNot()
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
