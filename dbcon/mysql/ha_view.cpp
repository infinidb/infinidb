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
*   $Id: ha_view.cpp 8073 2011-10-27 16:08:46Z zzhu $
*
*
***********************************************************************/

#include "idb_mysql.h"

#include <string>
using namespace std;

#include <boost/algorithm/string/case_conv.hpp>
using namespace boost;

#include "errorids.h"
using namespace logging;

#include "parsetree.h"
#include "simplefilter.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "ha_subquery.h"
#include "ha_view.h"

namespace cal_impl_if
{
extern uint buildOuterJoin(gp_walk_info& gwi, SELECT_LEX& select_lex);
extern string getViewName(TABLE_LIST* table_ptr);

CalpontSystemCatalog::TableAliasName& View::viewName()
{
	return fViewName;
}

void View::viewName(execplan::CalpontSystemCatalog::TableAliasName& viewName)
{
	fViewName = viewName;
}

void View::transform()
{
	CalpontSelectExecutionPlan* csep = new CalpontSelectExecutionPlan();
	csep->sessionID(fParentGwip->sessionid);	
	
	// gwi for the sub query
	gp_walk_info gwi;
	gwi.thd = fParentGwip->thd;	
	
	JOIN* join = fSelect.join;
	Item_cond* icp = 0;
	if (join != 0)
		icp = reinterpret_cast<Item_cond*>(join->conds);

	uint32_t sessionID = csep->sessionID();
	gwi.sessionid = sessionID;
	CalpontSystemCatalog *csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	csc->identity(CalpontSystemCatalog::FE);
	gwi.csc = csc;

	// traverse the table list of the view
	TABLE_LIST* table_ptr = fSelect.get_table_list();
	CalpontSelectExecutionPlan::SelectList derivedTbList;

	// @bug 1796. Remember table order on the FROM list.
	gwi.clauseType = FROM;
	try {
		for (; table_ptr; table_ptr= table_ptr->next_local)
		{
			// mysql put vtable here for from sub. we ignore it
			if (string(table_ptr->table_name).find("$vtable") != string::npos)
				continue;
				
			string viewName = getViewName(table_ptr);

			if (table_ptr->derived)
			{
				SELECT_LEX *select_cursor = table_ptr->derived->first_select();
				FromSubQuery *fromSub = new FromSubQuery(select_cursor);
				string alias(table_ptr->alias);
				gwi.viewName = make_aliasview("", alias, table_ptr->belong_to_view->alias, "");
				fromSub->gwip(&gwi);
				algorithm::to_lower(alias);
				fromSub->alias(alias);
				gwi.derivedTbList.push_back(SCSEP(fromSub->transform()));
				// set alias to both table name and alias name of the derived table
				//z//CalpontSystemCatalog::TableAliasName tn = make_aliasview("", "", alias, fViewName.alias);
				CalpontSystemCatalog::TableAliasName tn = make_aliasview("", "", alias, viewName);
				gwi.tbList.push_back(tn);
				gwi.tableMap[tn] = 0;
				gwi.thd->infinidb_vtable.isUnion = true; //by-pass the 2nd pass of rnd_init
			}
			else if (table_ptr->view)
			{
				// for nested view, the view name is vout.vin... format
				//z//string viewName = fViewName.alias + string(".") + table_ptr->alias; 
				//view->viewName(fViewName);
				CalpontSystemCatalog::TableAliasName tn = make_aliasview(table_ptr->db, table_ptr->table_name, table_ptr->alias, viewName);
				gwi.viewList.push_back(tn);
				gwi.viewName = make_aliastable(table_ptr->db, table_ptr->table_name, viewName);
				View *view = new View(table_ptr->view->select_lex, &gwi);
				view->viewName(gwi.viewName);
				view->transform();
			}
			else
			{
				csc->columnRIDs(make_table(table_ptr->db, table_ptr->table_name), true);
				// build the alias name for this table. carry view info
				//string alias = fViewName.alias + "_" + table_ptr->table_name;
				//z//CalpontSystemCatalog::TableAliasName tn = make_aliasview(table_ptr->db, table_ptr->table_name, table_ptr->alias, fViewName.alias);
				CalpontSystemCatalog::TableAliasName tn = make_aliasview(table_ptr->db, table_ptr->table_name, table_ptr->alias, viewName);
				gwi.tbList.push_back(tn);				
				gwi.tableMap[tn] = 0;
				fParentGwip->tableMap[tn] = 0;
			}
		}
		if (gwi.fatalParseError)
		{
			setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
			//return HA_ERR_UNSUPPORTED;
			return;
		}
	}
	catch (IDBExcept& ie)
	{
		setError(gwi.thd, HA_ERR_INTERNAL_ERROR, ie.what());
		CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
		return /*HA_ERR_INTERNAL_ERROR*/;
	}
	catch (...)
	{
		string emsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);		
		setError(gwi.thd, HA_ERR_INTERNAL_ERROR, emsg);
		CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
		return /*HA_ERR_INTERNAL_ERROR*/;
	}
	
	// No need to check return status?
	buildOuterJoin(gwi, fSelect);

	// merge table list to parent select
	fParentGwip->tbList.insert(fParentGwip->tbList.begin(), gwi.tbList.begin(), gwi.tbList.end());
	fParentGwip->derivedTbList.insert(fParentGwip->derivedTbList.begin(), gwi.derivedTbList.begin(), gwi.derivedTbList.end());
	fParentGwip->correlatedTbNameVec.insert(fParentGwip->correlatedTbNameVec.begin(), gwi.correlatedTbNameVec.begin(), gwi.correlatedTbNameVec.end());
	
	// merge non-collapsed outer join to parent select
	stack<ParseTree*> tmpstack;
	while (!gwi.ptWorkStack.empty())
	{
		tmpstack.push(gwi.ptWorkStack.top());
		gwi.ptWorkStack.pop();
	}
	
	while (!tmpstack.empty())
	{
		fParentGwip->ptWorkStack.push(tmpstack.top());
		tmpstack.pop();
	}
}

}
