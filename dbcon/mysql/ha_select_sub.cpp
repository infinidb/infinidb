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
*   $Id: ha_select_sub.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** class SelectSubQuery definition */

//#define NDEBUG
#include <cassert>
using namespace std;

#include "idb_mysql.h"

#include "parsetree.h"
#include "logicoperator.h"
#include "selectfilter.h"
#include "simplescalarfilter.h"
#include "predicateoperator.h"
#include "rowcolumn.h"
#include "simplecolumn.h"
using namespace execplan;

#include "errorids.h"
using namespace logging;

#include "ha_subquery.h"

namespace cal_impl_if
{
SelectSubQuery::SelectSubQuery(gp_walk_info& gwip) : SubQuery(gwip), fSelSub (NULL)
{}

SelectSubQuery::SelectSubQuery(gp_walk_info& gwip, Item_subselect* selSub) :
	SubQuery(gwip),
	fSelSub(selSub)
{}

SelectSubQuery::~SelectSubQuery()
{}

SCSEP SelectSubQuery::transform()
{
	idbassert(fSelSub);
	SCSEP csep(new CalpontSelectExecutionPlan());
	csep->sessionID(fGwip.sessionid);	
	csep->subType (CalpontSelectExecutionPlan::SELECT_SUBS);
	
	// gwi for the sub query
	gp_walk_info gwi;
	gwi.thd = fGwip.thd;
	gwi.subQuery = this;

	// @4632 merge table list to gwi in case there is FROM sub to be referenced
	// in the SELECT sub
	uint derivedTbCnt = fGwip.derivedTbList.size();
	uint tbCnt = fGwip.tbList.size();
	
	gwi.tbList.insert(gwi.tbList.begin(), fGwip.tbList.begin(), fGwip.tbList.end());
	gwi.derivedTbList.insert(gwi.derivedTbList.begin(), fGwip.derivedTbList.begin(), fGwip.derivedTbList.end());

	if (getSelectPlan(gwi, *(fSelSub->get_select_lex()), csep) != 0)
	{
		if (!gwi.fatalParseError)
		{
			fGwip.fatalParseError = true;
			fGwip.parseErrorText = "Error occured in SelectSubQuery::transform()";
		}
		else
		{
			fGwip.fatalParseError = gwi.fatalParseError;
			fGwip.parseErrorText = gwi.parseErrorText;
		}
		csep.reset();
		return csep;
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
	return csep;	
}

}
