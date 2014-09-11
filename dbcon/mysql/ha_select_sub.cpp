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
*   $Id: ha_select_sub.cpp 7409 2011-02-08 14:38:50Z rdempsey $
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
SelectSubQuery::SelectSubQuery() : fSelSub (NULL)
{}

SelectSubQuery::SelectSubQuery(Item_subselect* selSub) :
	fSelSub(selSub)
{}

SelectSubQuery::~SelectSubQuery()
{}

CalpontSelectExecutionPlan* SelectSubQuery::transform()
{
	assert(fSelSub);
	CalpontSelectExecutionPlan* csep = new CalpontSelectExecutionPlan();
	csep->sessionID(fGwip->sessionid);	
	csep->subType (CalpontSelectExecutionPlan::SELECT_SUBS);
	
	// gwi for the sub query
	gp_walk_info gwi;
	gwi.thd = fGwip->thd;
	gwi.subQuery = this;

	if (getSelectPlan(gwi, *(fSelSub->get_select_lex()), *csep) != 0)
	{
		if (!gwi.fatalParseError)
		{
			fGwip->fatalParseError = true;
			fGwip->parseErrorText = "Error occured in SelectSubQuery::transform()";
		}
		else
		{
			fGwip->fatalParseError = gwi.fatalParseError;
			fGwip->parseErrorText = gwi.parseErrorText;
		}
		return NULL;
	}
	return csep;	
}

}
