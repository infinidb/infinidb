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
*   $Id: ha_from_sub.cpp 6377 2010-03-22 20:18:47Z zzhu $
*
*
***********************************************************************/
/** @file */
/** class FromSubSelect definition */

//#define NDEBUG
#include <cassert>

#include "idb_mysql.h"

#include "parsetree.h"
#include "simplefilter.h"
using namespace execplan;

#include "ha_subquery.h"

namespace cal_impl_if
{
FromSubQuery::FromSubQuery(gp_walk_info& gwip) : SubQuery(gwip) 
{}

FromSubQuery::FromSubQuery(gp_walk_info& gwip, SELECT_LEX* sub) :
	SubQuery(gwip),
	fFromSub(sub)
{}

FromSubQuery::~FromSubQuery()
{}

SCSEP FromSubQuery::transform()
{
	assert (fFromSub);
	SCSEP csep(new CalpontSelectExecutionPlan());
	csep->sessionID(fGwip.sessionid);	
	csep->location(CalpontSelectExecutionPlan::FROM);
	csep->subType (CalpontSelectExecutionPlan::FROM_SUBS);
	
	// gwi for the sub query
	gp_walk_info gwi;
	gwi.thd = fGwip.thd;
	gwi.subQuery = this;
	gwi.viewName = fGwip.viewName;

	if (getSelectPlan(gwi, *fFromSub, csep) != 0)
	{
		fGwip.fatalParseError = true;		
		if (!gwi.parseErrorText.empty())
			fGwip.parseErrorText = gwi.parseErrorText;
		else
			fGwip.parseErrorText = "Error occured in FromSubQuery::transform()";
		csep.reset();
		return csep;
	}
	csep->derivedTbAlias(fAlias); // always lower case
	return csep;	
}

}
