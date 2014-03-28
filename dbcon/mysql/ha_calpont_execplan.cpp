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

/*
 * $Id: ha_calpont_execplan.cpp 9749 2013-08-15 04:00:39Z zzhu $
 */

/** @file */

#include <string>
#include <iostream>
#include <stack>
#ifdef _MSC_VER
#include <unordered_map>
#include <unordered_set>
#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#endif
#include <fstream>
#include <sstream>
#include <cerrno>
#include <cstring>
#include <time.h>
//#define NDEBUG
#include <cassert>
#include <vector>
#include <map>
#include <limits>
using namespace std;

#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/regex.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "errorids.h"
using namespace logging;

#include "idb_mysql.h"

#include "ha_calpont_impl_if.h"
#include "ha_subquery.h"
//#include "ha_view.h"
using namespace cal_impl_if;

#include "calpontselectexecutionplan.h"
#include "calpontsystemcatalog.h"
#include "simplecolumn_int.h"
#include "simplecolumn_uint.h"
#include "simplecolumn_decimal.h"
#include "aggregatecolumn.h"
#include "constantcolumn.h"
#include "simplefilter.h"
#include "constantfilter.h"
#include "functioncolumn.h"
#include "arithmeticcolumn.h"
#include "arithmeticoperator.h"
#include "logicoperator.h"
#include "predicateoperator.h"
#include "rowcolumn.h"
#include "selectfilter.h"
#include "existsfilter.h"
#include "groupconcatcolumn.h"
#include "outerjoinonfilter.h"
#include "intervalcolumn.h"
using namespace execplan;

#include "funcexp.h"
#include "functor.h"
using namespace funcexp;

const uint64_t AGG_BIT = 0x01;
const uint64_t SUB_BIT = 0x02;
const uint64_t AF_BIT = 0x04;

//#define OUTER_JOIN_DEBUG
namespace
{
	string lower(string str)
	{
		algorithm::to_lower(str);
		return str;
	}
}

// Enterprise View
#ifndef SKIP_VIEW
#include "ha_view.h"
#endif


namespace cal_impl_if {

//@bug5228. need to escape backtick `
string escapeBackTick(const char* str)
{
	if (!str)
		return "";
	string ret;
	for (uint i = 0; str[i] != 0; i++)
	{
		if (str[i] == '`')
			ret.append("``");
		else
			ret.append(1, str[i]);
	}
	return ret;
}

void clearStacks(gp_walk_info& gwi)
{
	while(!gwi.rcWorkStack.empty())
		gwi.rcWorkStack.pop();
	while(!gwi.ptWorkStack.empty())
		gwi.ptWorkStack.pop();
}

bool nonConstFunc(Item_func* ifp)
{
	if (strcasecmp(ifp->func_name(), "rand") == 0 || strcasecmp(ifp->func_name(), "sysdate") == 0)
		return true;
	for (uint i = 0; i < ifp->arg_count; i++)
	{
		if (ifp->arguments()[i]->type() == Item::FUNC_ITEM &&
			  nonConstFunc(((Item_func*)ifp->arguments()[i])))
			return true;
	}
	return false;
}

string getViewName(TABLE_LIST* table_ptr)
{
		string viewName;
		TABLE_LIST *view = table_ptr->referencing_view;
		if (view)
		{
			if (!view->derived)
				viewName = view->alias;
			while ((view = view->referencing_view))
			{
				if (view->derived) continue;
				viewName = view->alias + string(".") + viewName;
			}
		}
		return viewName;
}

uint buildOuterJoin(gp_walk_info& gwi, SELECT_LEX& select_lex)
{
	// check non-collapsed outer join
	// this set contains all processed embedded joins. duplicate joins are ignored
	set<TABLE_LIST*> embeddingSet;
	TABLE_LIST* table_ptr = select_lex.get_table_list();
	gp_walk_info gwi_outer = gwi;
	gwi_outer.subQuery = NULL;
	gwi_outer.hasSubSelect = false;
	vector <Item_field*> tmpVec;

	for (; table_ptr; table_ptr= table_ptr->next_local)
	{
		gwi_outer.innerTables.clear();
		clearStacks(gwi_outer);
		gwi_outer.subQuery = NULL;
		gwi_outer.hasSubSelect = false;

		// View is already processed in view::transform
		// @bug5319. view is sometimes treated as derived table and
		// fromSub::transform does not build outer join filters.
		if (!table_ptr->derived && table_ptr->view)
			continue;

		CalpontSystemCatalog:: TableAliasName tan = make_aliasview(
			(table_ptr->db ? table_ptr->db : ""),
			(table_ptr->table_name ? table_ptr->table_name : ""),
			(table_ptr->alias ? table_ptr->alias : ""),
			getViewName(table_ptr));

		if (table_ptr->outer_join && table_ptr->on_expr)
		{
			Item_cond* expr = reinterpret_cast<Item_cond*>(table_ptr->on_expr);
			gwi_outer.innerTables.insert(tan);

#ifdef DEBUG_WALK_COND
			if (table_ptr->alias)
				cout << table_ptr->alias ;
			else if (table_ptr->alias)
				cout << table_ptr->alias;
			cout << " outer table expression: " << endl;
			expr->traverse_cond(debug_walk, &gwi_outer, Item::POSTFIX);
#endif
			expr->traverse_cond(gp_walk, &gwi_outer, Item::POSTFIX);
		}
		// this part is ambiguous. Not quite sure how MySQL's lay out the outer join filters in the structure
		else if (table_ptr->embedding && table_ptr->embedding->outer_join && table_ptr->embedding->on_expr)
		{
			// all the tables in nested_join are inner tables.
			TABLE_LIST *table;
			List_iterator<TABLE_LIST> li(table_ptr->embedding->nested_join->join_list);
			while ((table= li++))
			{
				CalpontSystemCatalog:: TableAliasName ta = make_aliasview(
					(table->db ? table->db : ""),
					(table->table_name ? table->table_name : ""),
					(table->alias ? table->alias : ""),
					getViewName(table));
				gwi_outer.innerTables.insert(ta);
			}

			if (embeddingSet.find(table_ptr->embedding) != embeddingSet.end())
				continue;
			embeddingSet.insert(table_ptr->embedding);
			Item_cond* expr = reinterpret_cast<Item_cond*>(table_ptr->embedding->on_expr);

#ifdef DEBUG_WALK_COND
			cout << "inner tables: " << endl;
			set<CalpontSystemCatalog::TableAliasName>::const_iterator it;
			for (it = gwi_outer.innerTables.begin(); it != gwi_outer.innerTables.end(); ++it)
				cout << (*it) << " ";
			cout << endl;
			expr->traverse_cond(debug_walk, &gwi_outer, Item::POSTFIX);
#endif
			expr->traverse_cond(gp_walk, &gwi_outer, Item::POSTFIX);
		}
		// @bug 2849
		else if (table_ptr->embedding && table_ptr->embedding->nested_join)
		{
			// if this is dervied table process phase, mysql may have not developed the plan
			// completely. Return and let it finish. It will come to rnd_init again.
			if (table_ptr->embedding->is_natural_join && table_ptr->derived)
			{
				if (gwi.thd->derived_tables_processing)
				{
					gwi.thd->infinidb_vtable.isUnion = false;
					gwi.thd->infinidb_vtable.isUpdateWithDerive = true;
					return -1;
				}
			}
			if (embeddingSet.find(table_ptr->embedding) != embeddingSet.end())
				continue;

			gwi_outer.innerTables.insert(tan);
			embeddingSet.insert(table_ptr->embedding);
			List<TABLE_LIST> *inners = &(table_ptr->embedding->nested_join->join_list);
			List_iterator_fast<TABLE_LIST> li(*inners);
			TABLE_LIST* curr;
			while ((curr = li++))
			{
				if (curr->on_expr)
				{
					if (!curr->outer_join) // only handle nested JOIN for now
					{
						gwi_outer.innerTables.clear();
						Item_cond* expr = reinterpret_cast<Item_cond*>(curr->on_expr);

				#ifdef DEBUG_WALK_COND
						expr->traverse_cond(debug_walk, &gwi_outer, Item::POSTFIX);
				#endif
						expr->traverse_cond(gp_walk, &gwi_outer, Item::POSTFIX);
					}
				}
			}
		}

		// Error out subquery in outer join on filter for now
		if (gwi_outer.hasSubSelect)
		{
			gwi.fatalParseError = true;
			gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_OUTER_JOIN_SUBSELECT);
			setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
			return -1;
		}

		// build outerjoinon filter
		ParseTree *filters = NULL, *ptp = NULL, /**rhs = NULL*/*lhs = NULL;

		while (!gwi_outer.ptWorkStack.empty())
		{
			filters = gwi_outer.ptWorkStack.top();
			gwi_outer.ptWorkStack.pop();
			if (gwi_outer.ptWorkStack.empty())
				break;
			ptp = new ParseTree(new LogicOperator("and"));
			ptp->right(filters);
			lhs = gwi_outer.ptWorkStack.top();
			gwi_outer.ptWorkStack.pop();
			ptp->left(lhs);
			gwi_outer.ptWorkStack.push(ptp);
		}

		// should have only 1 pt left in stack.
		if (filters)
		{
			SPTP on_sp(filters);
			OuterJoinOnFilter *onFilter = new OuterJoinOnFilter(on_sp);
			ParseTree *pt = new ParseTree(onFilter);
			gwi.ptWorkStack.push(pt);
		}
	}

	embeddingSet.clear();
	return 0;
}

ParseTree* buildRowPredicate(RowColumn* lhs, RowColumn* rhs, string predicateOp)
{
	PredicateOperator *po = new PredicateOperator(predicateOp);
	shared_ptr<Operator> sop(po);
	LogicOperator *lo = NULL;
	if (predicateOp == "=")
		lo = new LogicOperator("and");
	else
		lo = new LogicOperator("or");

	ParseTree *pt = new ParseTree(lo);
	sop->setOpType(lhs->columnVec()[0]->resultType(), rhs->columnVec()[0]->resultType());
	SimpleFilter *sf = new SimpleFilter(sop, lhs->columnVec()[0].get(), rhs->columnVec()[0].get());
	pt->left(new ParseTree(sf));
	for (uint i = 1; i < lhs->columnVec().size(); i++)
	{
		sop.reset(po->clone());
		sop->setOpType(lhs->columnVec()[i]->resultType(), rhs->columnVec()[i]->resultType());
		SimpleFilter *sf = new SimpleFilter(sop, lhs->columnVec()[i].get(), rhs->columnVec()[i].get());
		pt->right(new ParseTree(sf));
		if (i+1 < lhs->columnVec().size())
		{
			ParseTree *lpt = pt;
			pt = new ParseTree(lo->clone());
			pt->left(lpt);
		}
	}
	return pt;
}

void buildRowColumnFilter(gp_walk_info* gwip, RowColumn* rhs, RowColumn* lhs, Item_func* ifp)
{
	if (ifp->functype() == Item_func::EQ_FUNC || ifp->functype() == Item_func::NE_FUNC)
	{
		// (c1,c2,..) = (v1,v2,...) transform to: c1=v1 and c2=v2 and ...
		assert (!lhs->columnVec().empty() && lhs->columnVec().size() == rhs->columnVec().size());
		gwip->ptWorkStack.push(buildRowPredicate(rhs,lhs,ifp->func_name()));
	}
	else if (ifp->functype() == Item_func::IN_FUNC)
	{
		// (c1,c2,...) in ((v11,v12,...),(v21,v22,...)...) transform to:
		// ((c1 = v11 and c2 = v12 and ...) or (c1 = v21 and c2 = v22 and ...) or ...)
		// and c1 in (v11,v21,...) and c2 in (v12,v22,...) => to favor CP
		Item_func_opt_neg* inp = (Item_func_opt_neg*)ifp;
		string predicateOp, logicOp;
		if (inp->negated)
		{
			predicateOp = "<>";
			logicOp = "and";
		}
		else
		{
			predicateOp = "=";
			logicOp = "or";
		}
		LogicOperator *lo = new LogicOperator(logicOp);

		// 1st round. build the equivalent filters
		// two entries have been popped from the stack already: lhs and rhs
		stack<ReturnedColumn*> tmpStack;
		vector<RowColumn*> valVec;
		tmpStack.push(rhs);
		tmpStack.push(lhs);
		assert (gwip->rcWorkStack.size() >= ifp->arg_count - 2);
		for (uint i = 2; i < ifp->arg_count; i++)
		{
			tmpStack.push(gwip->rcWorkStack.top());
			if (!gwip->rcWorkStack.empty())
				gwip->rcWorkStack.pop();
		}
		RowColumn *columns = dynamic_cast<RowColumn*>(tmpStack.top());
		tmpStack.pop();
		RowColumn *vals = dynamic_cast<RowColumn*>(tmpStack.top());
		valVec.push_back(vals);
		tmpStack.pop();
		ParseTree *pt = buildRowPredicate(columns, vals, predicateOp);
		while (!tmpStack.empty())
		{
			ParseTree *pt1 = new ParseTree(lo->clone());
			pt1->left(pt);
			vals = dynamic_cast<RowColumn*>(tmpStack.top());
			valVec.push_back(vals);
			tmpStack.pop();
			pt1->right(buildRowPredicate(columns->clone(), vals, predicateOp));
			pt = pt1;
		}
		gwip->ptWorkStack.push(pt);

		// done for NOTIN clause
		if (predicateOp == "<>")
			return;

		// 2nd round. add the filter to favor casual partition for IN clause
		shared_ptr<Operator> sop;
		shared_ptr<SimpleColumn> ssc;
		stack<ParseTree*> tmpPtStack;
		for (uint i = 0; i < columns->columnVec().size(); i++)
		{
			ConstantFilter* cf = new ConstantFilter();

			sop.reset(lo->clone());
			cf->op(sop);
			SimpleColumn *sc = dynamic_cast<SimpleColumn*>(columns->columnVec()[i].get());

			// no optimization for non-simple column because CP won't apply
			if (!sc)
				continue;

			ssc.reset(sc->clone());
			cf->col(ssc);

			uint j = 0;
			for (; j < valVec.size(); j++)
			{
				sop.reset(new PredicateOperator(predicateOp));
				ConstantColumn *cc = dynamic_cast<ConstantColumn*>(valVec[j]->columnVec()[i].get());
				// no optimization for non-constant value because CP won't apply
				if (!cc)
					break;
				sop->setOpType(sc->resultType(), valVec[j]->resultType());
				cf->pushFilter(new SimpleFilter(sop, sc->clone(),
							   valVec[j]->columnVec()[i]->clone()));
			}
			if (j < valVec.size())
				continue;
			tmpPtStack.push(new ParseTree(cf));
		}

		// "and" all the filters together
		ParseTree *ptp = NULL;
		pt = NULL;
		while (!tmpPtStack.empty())
		{
			pt = tmpPtStack.top();
			tmpPtStack.pop();
			if (tmpPtStack.empty())
				break;
			ptp = new ParseTree(new LogicOperator("and"));
			ptp->left(pt);
			pt = tmpPtStack.top();
			tmpPtStack.pop();
			ptp->right(pt);
			tmpPtStack.push(ptp);
		}

		if (pt)
		{
			tmpPtStack.push(pt);

			// AND with the pt built from the first round
			if (!gwip->ptWorkStack.empty() && !tmpPtStack.empty())
			{
				pt = new ParseTree(new LogicOperator("and"));
				pt->left(gwip->ptWorkStack.top());
				gwip->ptWorkStack.pop();
				pt->right(tmpPtStack.top());
				tmpPtStack.pop();
			}

			// Push the final pt tree for this IN clause
			gwip->ptWorkStack.push(pt);
		}
	}
	else
	{
		gwip->fatalParseError = true;
		gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FUNC_MULTI_COL);
		return;
	}
}

void buildPredicateItem(Item_func* ifp, gp_walk_info* gwip)
{
	shared_ptr<Operator> sop(new PredicateOperator(ifp->func_name()));

	if (ifp->functype() == Item_func::BETWEEN)
	{
		idbassert (gwip->rcWorkStack.size() >= 3);
		ReturnedColumn* rhs = gwip->rcWorkStack.top();
		gwip->rcWorkStack.pop();
		ReturnedColumn* lhs = gwip->rcWorkStack.top();
		gwip->rcWorkStack.pop();
		gwip->rcWorkStack.pop(); // pop gwip->scsp;
		Item_func_opt_neg* inp = (Item_func_opt_neg*)ifp;
		ConstantFilter* cf = 0;
		if (inp->negated)
		{
			sop.reset(new PredicateOperator(">"));
			sop->setOpType(gwip->scsp->resultType(), rhs->resultType());
			cf = new ConstantFilter(sop, gwip->scsp->clone(), rhs);
			sop.reset(new LogicOperator("or"));
			cf->op(sop);
			sop.reset(new PredicateOperator("<"));
			sop->setOpType(gwip->scsp->resultType(), rhs->resultType());
		}
		else
		{
			sop.reset(new PredicateOperator("<="));
			sop->setOpType(gwip->scsp->resultType(), rhs->resultType());
			cf = new ConstantFilter(sop, gwip->scsp->clone(), rhs);
			sop.reset(new LogicOperator("and"));
			cf->op(sop);
			sop.reset(new PredicateOperator(">="));
			sop->setOpType(gwip->scsp->resultType(), rhs->resultType());
		}
		cf->pushFilter(new SimpleFilter(sop, gwip->scsp->clone(), lhs));
		cf->functionName(gwip->funcName);
		String str;
		// @bug5811
		ifp->print(&str, QT_INFINIDB_DERIVED);
		IDEBUG(cout << str.c_ptr() << endl);
		if (str.c_ptr())
			cf->data(str.c_ptr());
		ParseTree* ptp = new ParseTree(cf);
		gwip->ptWorkStack.push(ptp);
	}
	else if (ifp->functype() == Item_func::IN_FUNC)
	{
		idbassert(gwip->rcWorkStack.size() >= 2);
		ReturnedColumn* rhs = gwip->rcWorkStack.top();
		gwip->rcWorkStack.pop();
		ReturnedColumn* lhs = gwip->rcWorkStack.top();
		gwip->rcWorkStack.pop();

		// @bug3038
		RowColumn *rrhs = dynamic_cast<RowColumn*>(rhs);
		RowColumn *rlhs = dynamic_cast<RowColumn*>(lhs);
		if (rrhs && rlhs)
		{
			buildRowColumnFilter(gwip, rrhs, rlhs, ifp);
			return;
		}

		ConstantColumn *crhs = dynamic_cast<ConstantColumn*>(rhs);
		ConstantColumn *clhs = dynamic_cast<ConstantColumn*>(lhs);
		if (!crhs || !clhs)
		{
			gwip->fatalParseError = true;
			gwip->parseErrorText = "non constant value in IN clause";
			return;
		}

		string eqop;
		string cmbop;
		Item_func_opt_neg* inp = (Item_func_opt_neg*)ifp;
		if (inp->negated)
		{
			eqop = "<>";
			cmbop = "and";
		}
		else
		{
			eqop = "=";
			cmbop = "or";
		}

		sop.reset(new PredicateOperator(eqop));
		sop->setOpType(gwip->scsp->resultType(), rhs->resultType());
		ConstantFilter* cf = 0;

		cf = new ConstantFilter(sop, gwip->scsp->clone(), rhs);
		sop.reset(new LogicOperator(cmbop));
		cf->op(sop);
		sop.reset(new PredicateOperator(eqop));
		sop->setOpType(gwip->scsp->resultType(), lhs->resultType());
		cf->pushFilter(new SimpleFilter(sop, gwip->scsp->clone(), lhs));
		while (!gwip->rcWorkStack.empty())
		{
			lhs = gwip->rcWorkStack.top();
			if (dynamic_cast<ConstantColumn*>(lhs) == 0) break;
			gwip->rcWorkStack.pop();
			sop.reset(new PredicateOperator(eqop));
			sop->setOpType(gwip->scsp->resultType(),lhs->resultType());
			cf->pushFilter(new SimpleFilter(sop, gwip->scsp->clone(), lhs));
		}
		if (!gwip->rcWorkStack.empty())
			gwip->rcWorkStack.pop(); // pop gwip->scsp
		if (cf->filterList().size() < inp->arg_count-1)
		{
			gwip->fatalParseError = true;
			gwip->parseErrorText = "non constant value in IN clause";
			delete cf;
			return;
		}
		cf->functionName(gwip->funcName);
		String str;
		// @bug5811
		ifp->print(&str, QT_INFINIDB_DERIVED);
		IDEBUG(cout << str.c_ptr() << endl);
		if (str.c_ptr())
			cf->data(str.c_ptr());
		ParseTree* ptp = new ParseTree(cf);
		gwip->ptWorkStack.push(ptp);
	}

	else if (ifp->functype() == Item_func::ISNULL_FUNC ||
			 ifp->functype() == Item_func::ISNOTNULL_FUNC)
	{
		ReturnedColumn* rhs = NULL;
		if (!gwip->rcWorkStack.empty())
		{
			rhs = gwip->rcWorkStack.top();
			gwip->rcWorkStack.pop();
		}
		else
		{
			rhs = buildReturnedColumn(ifp->arguments()[0], *gwip, gwip->fatalParseError);
		}
		if (rhs && !gwip->fatalParseError)
			buildConstPredicate(ifp, rhs, gwip);
		else if (!rhs) // @bug 3802
		{
			gwip->fatalParseError = true;
			gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
			return;
		}
	}

	else if (ifp->functype() == Item_func::GUSERVAR_FUNC)
	{
		Item_func_get_user_var* udf = reinterpret_cast<Item_func_get_user_var*>(ifp);
		String buf;
		if (udf->result_type() == INT_RESULT)
		{
            if (udf->unsigned_flag)
            {
                gwip->rcWorkStack.push(new ConstantColumn((uint64_t)udf->val_uint()));
            }
            else
            {
                gwip->rcWorkStack.push(new ConstantColumn((int64_t)udf->val_int()));
            }
		}
		else
		{
			//const String* str = udf->val_str(&buf);
			udf->val_str(&buf);
			if (!buf.ptr())
			{
				ostringstream oss;
				oss << "Unknown user variable: " << udf->name.str;
				gwip->parseErrorText = oss.str();
				gwip->fatalParseError = true;
				return;
			}

			if (udf->result_type() == STRING_RESULT)
				gwip->rcWorkStack.push(new ConstantColumn(buf.c_ptr()));
			else
            {
				gwip->rcWorkStack.push(new ConstantColumn(buf.c_ptr(), ConstantColumn::NUM));
            }
		}
	}
	else if (ifp->functype() == Item_func::NEG_FUNC)
	{
		//peek at the (hopefully) ConstantColumn on the top of stack, negate it in place
		ConstantColumn* ccp = dynamic_cast<ConstantColumn*>(gwip->rcWorkStack.top());
		if (!ccp)
		{
			ostringstream oss;
			oss << "Attempt to negate a non-constant column";
			gwip->parseErrorText = oss.str();
			gwip->fatalParseError = true;
			return;
		}
		string cval = ccp->constval();
		string newval;
		if (cval[0] == '-')
			newval.assign(cval, 1, string::npos);
		else
			newval = "-" + cval;
		ccp->constval(newval);
	}
	else if (ifp->functype() == Item_func::NOT_FUNC)
	{
		if (gwip->condPush && ifp->next->type() == Item::SUBSELECT_ITEM)
			return;
		if (ifp->next && ifp->next->type() == Item::SUBSELECT_ITEM && gwip->lastSub)
		{
			gwip->lastSub->handleNot();
			return;
		}

		idbassert(ifp->arg_count == 1);
		ParseTree *ptp = 0;
		if (isPredicateFunction(ifp->arguments()[0], gwip) || ifp->arguments()[0]->type() == Item::COND_ITEM)
		{
			// negate it in place
			if (!gwip->ptWorkStack.empty())
				ptp = gwip->ptWorkStack.top();
			SimpleFilter* sf = 0;
			if (ptp)
			{
				sf = dynamic_cast<SimpleFilter*>(ptp->data());
				if (sf)
					sf->op()->reverseOp();
			}
		}
		else
		{
			// transfrom the not item to item = 0
			ReturnedColumn* rhs = 0;
			if (!gwip->rcWorkStack.empty())
			{
				rhs = gwip->rcWorkStack.top();
				gwip->rcWorkStack.pop();
			}
			else
			{
				rhs = buildReturnedColumn(ifp->arguments()[0], *gwip, gwip->fatalParseError);
			}
			if (rhs && !gwip->fatalParseError)
				buildConstPredicate(ifp, rhs, gwip);
			else if (!rhs) // @bug3802
			{
				gwip->fatalParseError = true;
				gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
				return;
			}
		}
	}

	else //std rel ops (incl "like")
	{
		if (gwip->rcWorkStack.size() < 2)
		{
			idbassert(ifp->arg_count == 2);
			if (isPredicateFunction(ifp->arguments()[0], gwip) ||
				  ifp->arguments()[0]->type() == Item::COND_ITEM ||
				  isPredicateFunction(ifp->arguments()[1], gwip) ||
				  ifp->arguments()[1]->type() == Item::COND_ITEM)
			{
				gwip->fatalParseError = true;
				gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
			}
			return;
		}

		ReturnedColumn* rhs = gwip->rcWorkStack.top();
		gwip->rcWorkStack.pop();
		ReturnedColumn* lhs = gwip->rcWorkStack.top();
		gwip->rcWorkStack.pop();

		// @bug3038. rowcolumn filter
		RowColumn* rrhs = dynamic_cast<RowColumn*>(rhs);
		RowColumn* rlhs = dynamic_cast<RowColumn*>(lhs);

		if (rrhs && rlhs)
		{
			buildRowColumnFilter(gwip, rrhs, rlhs, ifp);
			return;
		}

		// push the column that is associated with the correlated column to the returned
		// column list, so the materialized view have the complete projection list.
		// e.g. tout.c1 in (select tin.c1 from tin where tin.c2=tout.c2);
		// the projetion list of subquery will have tin.c1, tin.c2.
		ReturnedColumn* correlatedCol = NULL;
		ReturnedColumn* localCol = NULL;
		if (rhs->joinInfo() & JOIN_CORRELATED)
		{
			correlatedCol = rhs;
			localCol = lhs;
		}
		else if (lhs->joinInfo() & JOIN_CORRELATED)
		{
			correlatedCol = lhs;
			localCol = rhs;
		}
		if (correlatedCol && localCol)
		{
			ConstantColumn* cc = dynamic_cast<ConstantColumn*>(localCol);
			if ((!cc || (cc && ifp->functype() == Item_func::EQ_FUNC)) &&
				 !(localCol->joinInfo() & JOIN_CORRELATED))
			{
				localCol->sequence(gwip->returnedCols.size());
				localCol->expressionId(gwip->expressionId++);
				ReturnedColumn *rc = localCol->clone();
				rc->colSource(rc->colSource() | CORRELATED_JOIN);
				gwip->returnedCols.push_back(SRCP(rc));
				if (rc->hasWindowFunc())
					gwip->windowFuncList.push_back(rc);
			}

			// push the correlated join partner to the group by list only when there's aggregate
			// and we don't push aggregate column to the group by
			// @bug4756. mysql does not always give correct information about whether there is
			// aggregate on the SELECT list. Need to figure that by ourselves and then decide
			// to add the group by or not.
			if (gwip->subQuery)
			{
				if (!localCol->hasAggregate() && !localCol->hasWindowFunc())
					gwip->subGroupByCols.push_back(SRCP(localCol->clone()));
			}

			if (sop->op() == OP_EQ)
			{
				if (gwip->subSelectType == CalpontSelectExecutionPlan::IN_SUBS ||
					gwip->subSelectType == CalpontSelectExecutionPlan::EXISTS_SUBS)
					correlatedCol->joinInfo(correlatedCol->joinInfo() | JOIN_SEMI);
				else if (gwip->subSelectType == CalpontSelectExecutionPlan::NOT_IN_SUBS ||
					   gwip->subSelectType == CalpontSelectExecutionPlan::NOT_EXISTS_SUBS)
					correlatedCol->joinInfo(correlatedCol->joinInfo() | JOIN_ANTI);
			}
		}

		SimpleFilter *sf = new SimpleFilter();
		SimpleColumn *slhs = NULL;
		SimpleColumn *srhs = NULL;

		//@bug 2101 for when there are only constants in a delete or update where clause (eg "where 5 < 6").
		//There will be no field column and it will get here only if the comparison is true.
		if (gwip->columnMap.empty() &&
			  ((current_thd->lex->sql_command == SQLCOM_UPDATE) ||
			  (current_thd->lex->sql_command == SQLCOM_UPDATE_MULTI) ||
			  (current_thd->lex->sql_command == SQLCOM_DELETE) ||
			  (current_thd->lex->sql_command == SQLCOM_DELETE_MULTI)))
		{
			IDEBUG( cout << "deleted func with 2 const columns" << endl );
			delete rhs;
			delete lhs;
			return;
		}

		// handle noop (only for table mode)
		if (rhs->data() == "noop" || lhs->data() == "noop")
		{
			sop.reset(new Operator("noop"));
		}
		else
		{
			for (uint i = 0; i < ifp->arg_count; i++)
			{
				if (isPredicateFunction(ifp->arguments()[i], gwip))
				{
					gwip->fatalParseError = true;
					gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_SUB_EXPRESSION);
				}
			}
		}

		sf->op(sop);
		sf->lhs(lhs);
		sf->rhs(rhs);
		sop->setOpType(lhs->resultType(), rhs->resultType());
		sop->resultType(sop->operationType());
		slhs = dynamic_cast<SimpleColumn*>(lhs);
		srhs = dynamic_cast<SimpleColumn*>(rhs);

		if (srhs && slhs && sop->op() == OP_EQ)
		{
			CalpontSystemCatalog::TableAliasName tan_lhs = make_aliasview(slhs->schemaName(), slhs->tableName(), slhs->tableAlias(), slhs->viewName());
			CalpontSystemCatalog::TableAliasName tan_rhs = make_aliasview(srhs->schemaName(), srhs->tableName(), srhs->tableAlias(), srhs->viewName());

			set<CalpontSystemCatalog::TableAliasName>::const_iterator it;

			// @bug 1632. Alias should be taken account to the identity of tables for selfjoin to work
			if (tan_lhs != tan_rhs) // join
			{
				if (!gwip->condPush) // vtable
				{
					if (!gwip->innerTables.empty())
					{
						bool notInner = true;

						for (it = gwip->innerTables.begin(); it != gwip->innerTables.end(); ++it)
						{
							if (slhs->tableAlias() == it->alias && slhs->viewName() == it->view)
								notInner = false;
						}

						if (notInner)
							slhs->returnAll(true);
						IDEBUG( cout << "setting returnAll on " << slhs->oid() << endl);
					}
					if (!gwip->innerTables.empty())
					{
						bool notInner = true;

						for (it = gwip->innerTables.begin(); it != gwip->innerTables.end(); ++it)
						{
							if (srhs->tableAlias() == it->alias && srhs->viewName() == it->view)
								notInner = false;
						}

						if (notInner)
							srhs->returnAll(true);
						IDEBUG( cout << "setting returnAll on " << srhs->oid() << endl );
					}

					ParseTree* ptp = new ParseTree(sf);
					gwip->ptWorkStack.push(ptp);
				}
			}
			else
			{
				ParseTree* ptp = new ParseTree(sf);
				gwip->ptWorkStack.push(ptp);
			}
		}
		else
		{
			ParseTree* ptp = new ParseTree(sf);
			gwip->ptWorkStack.push(ptp);
		}
	}
}

void buildConstPredicate(Item_func* ifp, ReturnedColumn* rhs, gp_walk_info* gwip)
{
	SimpleFilter *sf = new SimpleFilter();
	shared_ptr<Operator> sop(new PredicateOperator(ifp->func_name()));
	ConstantColumn *lhs = 0;

	if (ifp->functype() == Item_func::ISNULL_FUNC)
	{
		lhs = new ConstantColumn("", ConstantColumn::NULLDATA);
		sop.reset(new PredicateOperator("isnull"));
	}
	else if (ifp->functype() == Item_func::ISNOTNULL_FUNC)
	{
		lhs = new ConstantColumn("", ConstantColumn::NULLDATA);
		sop.reset(new PredicateOperator("isnotnull"));
	}
	else //if (ifp->functype() == Item_func::NOT_FUNC)
	{
		lhs = new ConstantColumn((int64_t)0, ConstantColumn::NUM);
		sop.reset(new PredicateOperator("="));
	}

	CalpontSystemCatalog::ColType opType = rhs->resultType();
	if ( (opType.colDataType == CalpontSystemCatalog::CHAR && opType.colWidth <= 8) ||
		  (opType.colDataType == CalpontSystemCatalog::VARCHAR && opType.colWidth < 8) ||
		  (opType.colDataType == CalpontSystemCatalog::VARBINARY && opType.colWidth < 8) )
	{
		opType.colDataType = execplan::CalpontSystemCatalog::BIGINT;
		opType.colWidth = 8;
	}
	sop->operationType(opType);
	sf->op(sop);

	//yes, these are backwards
	assert (lhs);
	sf->lhs(rhs);
	sf->rhs(lhs);
	ParseTree* ptp = new ParseTree(sf);
	gwip->ptWorkStack.push(ptp);
}

SimpleColumn* buildSimpleColFromDerivedTable(gp_walk_info& gwi, Item_field* ifp)
{
	SimpleColumn* sc = NULL;

	// view name
	string viewName = "";
	if (ifp->cached_table)
		viewName = getViewName(ifp->cached_table);

	for (uint i = 0; i < gwi.tbList.size(); i++)
	{
		if (sc) break;
		if (!gwi.tbList[i].schema.empty() && !gwi.tbList[i].table.empty() &&
			   (!ifp->table_name || strcasecmp(ifp->table_name, gwi.tbList[i].alias.c_str()) == 0))
		{
			CalpontSystemCatalog::TableName tn(gwi.tbList[i].schema, gwi.tbList[i].table);
			CalpontSystemCatalog::RIDList oidlist = gwi.csc->columnRIDs(tn, true);
			for (unsigned int j = 0; j < oidlist.size(); j++)
			{
				CalpontSystemCatalog::TableColName tcn = gwi.csc->colName(oidlist[j].objnum);
				CalpontSystemCatalog::ColType ct = gwi.csc->colType(oidlist[j].objnum);
				if (strcasecmp(ifp->field_name, tcn.column.c_str()) == 0)
				{
					// @bug4827. Remove the checking because outside tables could be the same
					// name as inner tables. This function is to identify column from a table,
					// as long as it matches the next step in predicate parsing will tell the scope
					// of the column.
					/*if (sc)
					{
						gwi.fatalParseError = true;
						Message::Args args;
						args.add(ifp->name);
						gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_AMBIGUOUS_COL, args);
						return NULL;
					}*/

					sc = new SimpleColumn();
					sc->columnName(tcn.column);
					sc->tableName(tcn.table);
					sc->schemaName(tcn.schema);
					sc->oid(oidlist[j].objnum);
					// @bug 3003. Keep column alias if it has.
					if (!ifp->is_autogenerated_name)
						sc->alias(ifp->name);
					else
						sc->alias(tcn.column);
					sc->tableAlias(lower(gwi.tbList[i].alias));
					sc->viewName(lower(viewName));
					sc->resultType(ct);
					break;
				}
			}
		}
	}

	if (sc)
		return sc;

	for (uint i = 0; i < gwi.derivedTbList.size(); i++)
	{
		if (sc) break;

		CalpontSelectExecutionPlan* csep = dynamic_cast<CalpontSelectExecutionPlan*>(gwi.derivedTbList[i].get());
		if (!ifp->table_name || strcasecmp(ifp->table_name, csep->derivedTbAlias().c_str()) == 0)
		{
			CalpontSelectExecutionPlan::ReturnedColumnList cols = csep->returnedCols();
			for (uint j = 0; j < cols.size(); j++)
			{
				// @bug 3167 duplicate column alias is full name
				SimpleColumn *col = dynamic_cast<SimpleColumn*>(cols[j].get());
				if (strcasecmp(ifp->field_name, cols[j]->alias().c_str()) == 0 ||
					  (col && cols[j]->alias().find(".") != string::npos &&
					  	strcasecmp(ifp->field_name, col->columnName().c_str()) == 0))
				{
					// @bug4827. Remove the checking because outside tables could be the same
					// name as inner tables. This function is to identify column from a table,
					// as long as it matches the next step in predicate parsing will tell the scope
					// of the column.
					/*if (sc)
					{
						gwi.fatalParseError = true;
						Message::Args args;
						args.add(ifp->name);
						gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_AMBIGUOUS_COL, args);
						return NULL;
					}*/
					sc = new SimpleColumn();
					if (!col)
						sc->columnName(cols[j]->alias());
					else
						sc->columnName(col->columnName());
					// @bug 3003. Keep column alias if it has.
					if (!ifp->is_autogenerated_name)
						sc->alias(ifp->name);
					else
						sc->alias(cols[j]->alias());
					sc->tableName(csep->derivedTbAlias());
					sc->colPosition(j);
					string tableAlias(csep->derivedTbAlias());
					sc->tableAlias(lower(tableAlias));
					sc->viewName(lower(viewName));
					sc->resultType(cols[j]->resultType());
					if (col)
						sc->isInfiniDB(col->isInfiniDB());
					break;
				}
			}
		}
	}
	if (!sc)
	{
		gwi.fatalParseError = true;
		Message::Args args;
		string name;
		if (ifp->db_name)
			name += string(ifp->db_name) + ".";
		if (ifp->table_name)
			name += string(ifp->table_name) + ".";
		name += ifp->name;
		args.add(name);
		gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_UNKNOWN_COL, args);
	}
	return sc;
}

// for FROM clause subquery. get all the columns from real tables and derived tables.
void collectAllCols(gp_walk_info& gwi, Item_field* ifp)
{
	// view name
	string viewName = "";
	if (ifp->cached_table)
		viewName = getViewName(ifp->cached_table);

	if (gwi.derivedTbList.empty())
	{
		gwi.fatalParseError = true;
		Message::Args args;
		args.add("*");
		gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_UNKNOWN_COL, args);

	}
	string tableName = (ifp->table_name? string(ifp->table_name) : "");
	CalpontSelectExecutionPlan::SelectList::const_iterator it = gwi.derivedTbList.begin();
	for (uint i = 0; i < gwi.tbList.size(); i++)
	{
		SRCP srcp;
		// derived table
		if (gwi.tbList[i].schema.empty())
		{
			CalpontSelectExecutionPlan* csep = dynamic_cast<CalpontSelectExecutionPlan*>((*it).get());
			++it;
			if (!tableName.empty() && strcasecmp(tableName.c_str(), csep->derivedTbAlias().c_str()) != 0)
				continue;
			CalpontSelectExecutionPlan::ReturnedColumnList cols = csep->returnedCols();
			for (uint j = 0; j < cols.size(); j++)
			{
				SimpleColumn* sc = new SimpleColumn();
				sc->columnName(cols[j]->alias());
				sc->colPosition(j);
				string tableAlias(csep->derivedTbAlias());
				sc->tableAlias(lower(tableAlias));
				sc->viewName(lower(gwi.tbList[i].view));
				sc->resultType(cols[j]->resultType());
				srcp.reset(sc);
				gwi.returnedCols.push_back(srcp);
				gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(sc->columnName(), srcp));
			}
		}
		// real tables
		else
		{
			CalpontSystemCatalog::TableName tn(gwi.tbList[i].schema, gwi.tbList[i].table);
			if (!tableName.empty() && (strcasecmp(tableName.c_str(), tn.table.c_str()) != 0 &&
				  strcasecmp(tableName.c_str(), gwi.tbList[i].alias.c_str()) != 0 ))
				continue;
			CalpontSystemCatalog::RIDList oidlist = gwi.csc->columnRIDs(tn, true);
			for (unsigned int j = 0; j < oidlist.size(); j++)
			{
				SimpleColumn* sc = new SimpleColumn();
				CalpontSystemCatalog::TableColName tcn = gwi.csc->colName(oidlist[j].objnum);
				CalpontSystemCatalog::ColType ct = gwi.csc->colType(oidlist[j].objnum);
				sc->columnName(tcn.column);
				sc->tableName(tcn.table);
				sc->schemaName(tcn.schema);
				sc->oid(oidlist[j].objnum);
				sc->alias(tcn.column);
				sc->resultType(ct);
				sc->tableAlias(lower(gwi.tbList[i].alias));
				sc->viewName(lower(viewName));
				srcp.reset(sc);
				gwi.returnedCols.push_back(srcp);
				gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(sc->columnName(), srcp));
			}
		}
	}
}

void buildSubselectFunc(Item_func* ifp, gp_walk_info* gwip)
{
	// @bug 3035
	if (!isPredicateFunction(ifp, gwip))
	{
		gwip->fatalParseError = true;
		gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_FUNC_SUB);
		return;
	}

	WhereSubQuery* subquery = NULL;
	for (uint i = 0; i < ifp->arg_count; i++)
	{
		if (ifp->arguments()[i]->type() == Item::SUBSELECT_ITEM)
		{
			Item_subselect* sub = (Item_subselect*)ifp->arguments()[i];

			switch (sub->substype())
			{
				case Item_subselect::SINGLEROW_SUBS:
					subquery = new ScalarSub(*gwip, ifp);
					break;
				case Item_subselect::IN_SUBS:
					subquery = new InSub(*gwip, ifp);
					break;
				case Item_subselect::EXISTS_SUBS:
					// exists sub has been handled earlier. here is for not function
					if (ifp->functype() == Item_func::NOT_FUNC)
					{
						if (gwip->lastSub)
							gwip->lastSub->handleNot();
					}
					break;
				default:
					Message::Args args;
					gwip->fatalParseError = true;
					gwip->parseErrorText = "non supported subquery";
					return;
			}
		}
	}
	if (subquery)
	{
		gwip->hasSubSelect = true;
		SubQuery* orig = gwip->subQuery;
		gwip->subQuery = subquery;
		// no need to check NULL for now. error will be handled in gp_walk
		gwip->ptWorkStack.push(subquery->transform());
		// recover original sub. Save current sub for Not handling.
		gwip->lastSub = subquery;
		gwip->subQuery = orig;
	}
	return;
}

bool isPredicateFunction(Item* item, gp_walk_info* gwip)
{
	if (item->type() != Item::FUNC_ITEM)
		return false;
	Item_func* ifp = (Item_func*)item;
	return ( ifp->functype() == Item_func::EQ_FUNC ||
		ifp->functype() == Item_func::NE_FUNC ||
		ifp->functype() == Item_func::LT_FUNC ||
		ifp->functype() == Item_func::LE_FUNC ||
		ifp->functype() == Item_func::GE_FUNC ||
		ifp->functype() == Item_func::GT_FUNC ||
		ifp->functype() == Item_func::LIKE_FUNC ||
		ifp->functype() == Item_func::BETWEEN ||
		ifp->functype() == Item_func::IN_FUNC ||
		(ifp->functype() == Item_func::ISNULL_FUNC &&
		  (gwip->clauseType == WHERE || gwip->clauseType == HAVING)) ||
		(ifp->functype() == Item_func::ISNOTNULL_FUNC &&
		  (gwip->clauseType == WHERE || gwip->clauseType == HAVING))||
		ifp->functype() == Item_func::NOT_FUNC ||
		ifp->functype() == Item_func::ISNOTNULLTEST_FUNC ||
		ifp->functype() == Item_func::TRIG_COND_FUNC ||
		string(ifp->func_name()) == "<in_optimizer>" ||
		string(ifp->func_name()) == "xor");
}

void setError(THD* thd, uint errcode, string errmsg)
{
	thd->main_da.can_overwrite_status = true;
	if (errmsg.empty())
		errmsg = "Unknown error";
	thd->main_da.set_error_status(thd, errcode, errmsg.c_str());
	thd->infinidb_vtable.mysql_optimizer_off = false;
	thd->infinidb_vtable.override_largeside_estimate = false;
}

const string bestTableName(const Item_field* ifp)
{
	idbassert(ifp);
	if (!ifp->table_name)
		return "";
	if (!ifp->field)
		return ifp->table_name;
	string table_name(ifp->table_name);
	string field_table_table_name;
	if (ifp->cached_table)
		field_table_table_name = ifp->cached_table->table_name;
	else if (ifp->field->table && ifp->field->table->s && ifp->field->table->s->table_name.str)
		field_table_table_name = ifp->field->table->s->table_name.str;

	string tn;
	if (!field_table_table_name.empty())
		tn = field_table_table_name;
	else
		tn = table_name;
	return tn;
}

uint setAggOp(AggregateColumn* ac, Item_sum* isp)
{
	Item_sum::Sumfunctype agg_type = isp->sum_func();
	uint rc = 0;
	switch (agg_type) {
		case Item_sum::COUNT_FUNC:
			ac->aggOp(AggregateColumn::COUNT);
			return rc;
		case Item_sum::SUM_FUNC:
			ac->aggOp(AggregateColumn::SUM);
			return rc;
		case Item_sum::AVG_FUNC:
			ac->aggOp(AggregateColumn::AVG);
			return rc;
		case Item_sum::MIN_FUNC:
			ac->aggOp(AggregateColumn::MIN);
			return rc;
		case Item_sum::MAX_FUNC:
			ac->aggOp(AggregateColumn::MAX);
			return rc;
		case Item_sum::COUNT_DISTINCT_FUNC:
			ac->aggOp(AggregateColumn::DISTINCT_COUNT);
			ac->distinct(true);
			return rc;
		case Item_sum::SUM_DISTINCT_FUNC:
			ac->aggOp(AggregateColumn::DISTINCT_SUM);
			ac->distinct(true);
			return rc;
		case Item_sum::AVG_DISTINCT_FUNC:
			ac->aggOp(AggregateColumn::DISTINCT_AVG);
			ac->distinct(true);
			return rc;
		case Item_sum::STD_FUNC:
		{
			Item_sum_variance* var = (Item_sum_variance*)isp;
			if (var->sample)
				ac->aggOp(AggregateColumn::STDDEV_SAMP);
			else
				ac->aggOp(AggregateColumn::STDDEV_POP);
			return rc;
		}
		case Item_sum::VARIANCE_FUNC:
		{
			Item_sum_variance* var = (Item_sum_variance*)isp;
			if (var->sample)
				ac->aggOp(AggregateColumn::VAR_SAMP);
			else
				ac->aggOp(AggregateColumn::VAR_POP);
			return rc;
		}
		case Item_sum::GROUP_CONCAT_FUNC:
		{
			Item_func_group_concat* gc = (Item_func_group_concat*)isp;
			ac->aggOp(AggregateColumn::GROUP_CONCAT);
			ac->distinct(gc->isDistinct());
			return rc;
		}
		case Item_sum::SUM_BIT_FUNC:
		{
			string funcName = isp->func_name();
			if (funcName.compare("bit_and(") == 0)
				ac->aggOp(AggregateColumn::BIT_AND);
			else if (funcName.compare("bit_or(") == 0)
				ac->aggOp(AggregateColumn::BIT_OR);
			else if (funcName.compare("bit_xor(") == 0)
				ac->aggOp(AggregateColumn::BIT_XOR);
			else
				return HA_ERR_UNSUPPORTED;
			return rc;
		}
		default:
			return HA_ERR_UNSUPPORTED;
	}
}

#ifdef DEBUG_WALK_COND
void debug_walk(const Item *item, void *arg)
{
	switch (item->type())
	{
	case Item::FIELD_ITEM:
	{
		Item_field* ifp = (Item_field*)item;
		cout << "FIELD_ITEM: " << (ifp->db_name ? ifp->db_name : "") << '.' << bestTableName(ifp) << '.' << ifp->field_name << endl;
		break;
	}
	case Item::INT_ITEM:
	{
		Item_int* iip = (Item_int*)item;
		cout << "INT_ITEM: ";
		if (iip->name) cout << iip->name << " (from name string)" << endl;
		else cout << iip->val_int() << endl;
		break;
	}
	case Item::STRING_ITEM:
	{
		Item_string* isp = (Item_string*)item;
		String val, *str = isp->val_str(&val);
		cout << "STRING_ITEM: >" << str->c_ptr() << '<' << endl;
		break;
	}
	case Item::REAL_ITEM:
	{
		cout << "REAL_ITEM" << endl;
		break;
	}
	case Item::DECIMAL_ITEM:
	{
		cout << "DECIMAL_ITEM" << endl;
		break;
	}
	case Item::FUNC_ITEM:
	{
		Item_func* ifp = (Item_func*)item;
		Item_func_opt_neg* inp;
		cout << "FUNC_ITEM: ";
		switch (ifp->functype())
		{
		case Item_func::UNKNOWN_FUNC: // 0
			cout << ifp->func_name() << endl;
			break;
		case Item_func::GT_FUNC: // 7
			cout << '>' << endl;
			break;
		case Item_func::EQ_FUNC: // 1
			cout << '=' << endl;
			break;
		case Item_func::GE_FUNC:
			cout << ">=" << endl;
			break;
		case Item_func::LE_FUNC:
			cout << "<=" << endl;
			break;
		case Item_func::LT_FUNC:
			cout << '<' << endl;
			break;
		case Item_func::NE_FUNC:
			cout << "<>" << endl;
			break;
		case Item_func::NEG_FUNC: // 45
			cout << "unary minus" << endl;
			break;
		case Item_func::IN_FUNC: // 16
			inp = (Item_func_opt_neg*)ifp;
			if (inp->negated) cout << "not ";
			cout << "in" << endl;
			break;
		case Item_func::BETWEEN:
			inp = (Item_func_opt_neg*)ifp;
			if (inp->negated) cout << "not ";
			cout << "between" << endl;
			break;
		case Item_func::ISNULL_FUNC: // 10
			cout << "is null" << endl;
			break;
		case Item_func::ISNOTNULL_FUNC: // 11
			cout << "is not null" << endl;
			break;
		case Item_func::NOT_ALL_FUNC:
			cout << "not_all" << endl;
			break;
		default:
			cout << "type=" << ifp->functype() << endl;
			break;
		}
		//for (uint i = 0; i < ifp->arg_count; i++)
		//	debug_walk(ifp->arguments()[i], 0);
		break;
	}
	case Item::COND_ITEM:
	{
		Item_cond* icp = (Item_cond*)item;
		cout << "COND_ITEM: " << icp->func_name() << endl;
		break;
	}
	case Item::SUM_FUNC_ITEM:
	{
		Item_sum* isp = (Item_sum*)item;
		Item_sum_int*            isip = 0;
		Item_sum_distinct*       isdp = 0;
		Item_sum_avg*            isap = 0;
		Item_sum_count*          iscp = 0;
		Item_sum_count_distinct* iscdp = 0;
		Item_sum_min*            isnp = 0;
		Item_sum_max*            isxp = 0;
		//Item*                    ip = 0;
		//uint                     arg_count = 0;

		switch (isp->sum_func())
		{
		case Item_sum::SUM_FUNC:
			isip = (Item_sum_int*)isp;
			//ip = *isip->args;
			//arg_count = isip->arg_count;
			cout << "SUM_FUNC: " << isip->name << endl;
			break;
		case Item_sum::SUM_DISTINCT_FUNC:
			isdp = (Item_sum_distinct*)isp;
			//ip = *isdp->args;
			//arg_count = isdp->arg_count;
			cout << "SUM_DISTINCT_FUNC: " << isdp->name << endl;
			break;
		case Item_sum::AVG_FUNC:
			isap = (Item_sum_avg*)isp;
			//ip = *isap->args;
			//arg_count = isap->arg_count;
			cout << "AVG_FUNC: " << isap->name << endl;
			break;
		case Item_sum::COUNT_FUNC:
			iscp = (Item_sum_count*)isp;
			//ip = *iscp->args;
			//arg_count = iscp->arg_count;
			cout << "COUNT_FUNC: " << iscp->name << endl;
			break;
		case Item_sum::COUNT_DISTINCT_FUNC:
			iscdp = (Item_sum_count_distinct*)isp;
			//ip = *iscdp->args;
			//arg_count = iscdp->arg_count;
			cout << "COUNT_DISTINCT_FUNC: " << iscdp->name << endl;
			break;
		case Item_sum::MIN_FUNC:
			isnp = (Item_sum_min*)isp;
			//ip = *isnp->args;
			//arg_count = isnp->arg_count;
			cout << "MIN_FUNC: " << isnp->name << endl;
			break;
		case Item_sum::MAX_FUNC:
			isxp = (Item_sum_max*)isp;
			//ip = *isxp->args;
			//arg_count = isxp->arg_count;
			cout << "MAX_FUNC: " << isxp->name << endl;
			break;
		default:
			cout << "SUM_FUNC_ITEM type=" << isp->sum_func() << endl;
			break;
		}
//		while (ip)
//		{
//			debug_walk(ip, 0);
//			ip = ip->next;
//		}
		break;
	}
#if 1 // subquery debug
	case Item::SUBSELECT_ITEM:
	{
		Item_subselect* sub = (Item_subselect*)item;
		cout << "SUBSELECT Item: " << sub->substype() << endl;
		JOIN* join = sub->get_select_lex()->join;
		if (join)
		{
			Item_cond* cond = reinterpret_cast<Item_cond*>(join->conds);
			if (cond)
				cond->traverse_cond(debug_walk, arg, Item::POSTFIX);
		}
		cout << "Finish subselect item traversing" << endl;
		break;
	}
	case Item::REF_ITEM:
	{
		Item_ref* ref = (Item_ref*)item;
		if (ref->real_item()->type() == Item::CACHE_ITEM)
		{
			Item* field = ((Item_cache*)ref->real_item())->get_example();
			if (field->type() == Item::FIELD_ITEM)
			{
				Item_field* ifp = (Item_field*)field;
				//ifp->cached_table->select_lex->select_number gives the select level. could be used on alias.
				// could also be used to tell correlated join (equal level).
				cout << "CACHED FIELD_ITEM: " << ifp->db_name << '.' << bestTableName(ifp) << '.' << ifp->field_name << endl;
				break;
			}
		}
		else if (ref->real_item()->type() == Item::FIELD_ITEM)
		{
			Item_field* ifp = (Item_field*)ref->real_item();
			cout << "REF FIELD_ITEM: " << ifp->db_name << '.' << bestTableName(ifp) << '.' << ifp->field_name << endl;
			break;
		}
		cout << "UNKNOWN REF ITEM" << endl;
		break;
	}
	case Item::ROW_ITEM:
	{
		Item_row* row = (Item_row*)item;
		cout << "ROW_ITEM: " << endl;
		for (uint i = 0; i < row->cols(); i++)
			debug_walk(row->element_index(i), 0);
	}
	case Item::WINDOW_FUNC_ITEM:
	{
		cout << "Window Function Item" << endl;
	}
#endif
	default:
	{
		cout << "UNKNOWN_ITEM type " << item->type() << endl;
		break;
	}
	}
}
#endif

/* get the smallest column of a table. Used for filling columnmap */
SimpleColumn* getSmallestColumn(boost::shared_ptr<CalpontSystemCatalog> csc,
								CalpontSystemCatalog::TableName& tn,
								CalpontSystemCatalog::TableAliasName& tan,
								TABLE* table,
								gp_walk_info& gwi)
{
	// derived table
	if (tan.schema.empty())
	{
		for (uint i = 0; i < gwi.derivedTbList.size(); i++)
		{
			CalpontSelectExecutionPlan *csep = dynamic_cast<CalpontSelectExecutionPlan*>(gwi.derivedTbList[i].get());
			if (tan.alias == csep->derivedTbAlias())
			{
				assert (!csep->returnedCols().empty());
				ReturnedColumn* rc = dynamic_cast<ReturnedColumn*>(csep->returnedCols()[0].get());
				SimpleColumn *sc = new SimpleColumn();
				sc->columnName(rc->alias());
				sc->sequence(0);
				sc->tableAlias(lower(tan.alias));
				//if (!gwi.viewAlias.empty())
				//	sc->tableAlias(lower(gwi.viewAlias) + string(".") + sc->tableAlias());
				return sc;
			}
		}
		throw runtime_error ("getSmallestColumn: Internal error.");
	}

	// check engine type
	if (!tan.fIsInfiniDB)
	{
		// get the first column to project. @todo optimization to get the smallest one for foreign engine.
		Field *field = *(table->field);
		SimpleColumn* sc = new SimpleColumn(table->s->db.str, table->s->table_name.str, field->field_name, tan.fIsInfiniDB, gwi.sessionid);
		string alias(table->alias);
		sc->tableAlias(lower(alias));
		sc->isInfiniDB(false);
		sc->resultType(fieldType_MysqlToIDB(field));
		sc->oid(field->field_index+1);
		return sc;
	}

	CalpontSystemCatalog::RIDList oidlist = csc->columnRIDs(tn, true);
	CalpontSystemCatalog::TableColName tcn;
	int minColWidth = -1;
	int minWidthColOffset = 0;
	for (unsigned int j = 0; j < oidlist.size(); j++)
	{
			CalpontSystemCatalog::ColType ct = csc->colType(oidlist[j].objnum);

			if (ct.colDataType == CalpontSystemCatalog::VARBINARY)
				continue;

			if (minColWidth == -1 || ct.colWidth < minColWidth)
			{
				minColWidth = ct.colWidth;
				minWidthColOffset= j;
			}
	}

	tcn = csc->colName(oidlist[minWidthColOffset].objnum);
	SimpleColumn *sc = new SimpleColumn(tcn.schema, tcn.table, tcn.column, csc->sessionID());
	sc->tableAlias(lower(tan.alias));
	sc->viewName(lower(tan.view));
	sc->resultType(csc->colType(oidlist[minWidthColOffset].objnum));
	return sc;
}

CalpontSystemCatalog::ColType fieldType_MysqlToIDB (const Field* field)
{
	CalpontSystemCatalog::ColType ct;
	ct.precision = 4;
	switch (field->result_type())
	{
		case INT_RESULT:
			ct.colDataType = CalpontSystemCatalog::BIGINT;
			ct.colWidth = 8;
			break;
		case STRING_RESULT:
			ct.colDataType = CalpontSystemCatalog::VARCHAR;
			ct.colWidth = field->field_length;
			break;
		case DECIMAL_RESULT:
		{
			Field_decimal* idp = (Field_decimal*)field;
			ct.colDataType = CalpontSystemCatalog::DECIMAL;
			ct.colWidth = 8;
			ct.scale = idp->dec;
			if (ct.scale == 0)
				ct.precision = idp->field_length - 1;
			else
				ct.precision = idp->field_length - idp->dec;
			break;
		}
		case REAL_RESULT:
			ct.colDataType = CalpontSystemCatalog::DOUBLE;
			ct.colWidth = 8;
			break;
		default:
			IDEBUG( cout << "fieldType_MysqlToIDB:: Unknown result type of MySQL "
						 << field->result_type() << endl );
			break;
	}
	return ct;
}

CalpontSystemCatalog::ColType colType_MysqlToIDB (const Item* item)
{
	CalpontSystemCatalog::ColType ct;
	ct.precision = 4;
	switch (item->result_type())
	{
		case INT_RESULT:
			if (item->unsigned_flag)
			{
				ct.colDataType = CalpontSystemCatalog::UBIGINT;
			}
			else
			{
				ct.colDataType = CalpontSystemCatalog::BIGINT;
			}
			ct.colWidth = 8;
			break;
		case STRING_RESULT:
			ct.colDataType = CalpontSystemCatalog::VARCHAR;
			ct.colWidth = item->max_length;
			// force token
			if (item->type() == Item::FUNC_ITEM)
			{
				if (ct.colWidth < 20)
					ct.colWidth = 20; // for infinidb date length
				if (ct.colWidth > 65535)
					ct.colWidth = 65535;
			}
			// @bug5083. MySQL gives string type for date/datetime column.
			// need to adjust here.
			if (item->type() == Item::FIELD_ITEM)
			{
				if (item->field_type() == MYSQL_TYPE_DATE)
				{
					ct.colDataType = CalpontSystemCatalog::DATE;
					ct.colWidth = 4;
				}
				else if (item->field_type() == MYSQL_TYPE_DATETIME ||
					       item->field_type() == MYSQL_TYPE_TIMESTAMP)
				{
					ct.colDataType = CalpontSystemCatalog::DATETIME;
					ct.colWidth = 8;
				}
			}
			break;
/* FIXME:
		case xxxBINARY_RESULT:
			ct.colDataType = CalpontSystemCatalog::VARBINARY;
			ct.colWidth = item->max_length;
			// force token
			if (item->type() == Item::FUNC_ITEM)
			{
				if (ct.colWidth < 20)
					ct.colWidth = 20; // for infinidb date length
			}
			break;
*/
		case DECIMAL_RESULT:
		{
			Item_decimal* idp = (Item_decimal*)item;
			ct.colDataType = CalpontSystemCatalog::DECIMAL;
			ct.colWidth = 8;
			ct.scale = idp->decimals;
			if (ct.scale == 0)
				ct.precision = idp->max_length - 1;
			else
				ct.precision = idp->max_length - idp->decimals;
			break;
		}
		case REAL_RESULT:
			ct.colDataType = CalpontSystemCatalog::DOUBLE;
			ct.colWidth = 8;
			break;
		default:
			IDEBUG( cout << "colType_MysqlToIDB:: Unknown result type of MySQL "
						 << item->result_type() << endl );
			break;
	}
	return ct;
}

ReturnedColumn* buildReturnedColumn(Item* item, gp_walk_info& gwi, bool& nonSupport)
{
	ReturnedColumn* rc = NULL;
	if ( gwi.thd)
	{
		if ( ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) || ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ))
		{
			if ( !item->fixed)
			{
				item->fix_fields(gwi.thd, (Item**)&item);
			}
		}
	}
	switch (item->type())
	{
		case Item::FIELD_ITEM:
		{
			Item_field* ifp = (Item_field*)item;
			return buildSimpleColumn(ifp, gwi);
		}
		case Item::INT_ITEM:
		case Item::VARBIN_ITEM:
		{
			String val, *str = item->val_str(&val);
			if (item->unsigned_flag)
			{
				//cc = new ConstantColumn(str->c_ptr(), (uint64_t)item->val_uint(), ConstantColumn::NUM);
				// It seems that str at this point is crap if val_uint() is > MAX_BIGINT. By using
				// this constructor, ConstantColumn is built with the proper string. For whatever reason,
				// ExeMgr converts the fConstval member to numeric, rather than using the existing numeric
				// values available, so it's important to have fConstval correct.
				rc = new ConstantColumn((uint64_t)item->val_uint(), ConstantColumn::NUM);
			}
			else
			{
				rc = new ConstantColumn(str->c_ptr(), (int64_t)item->val_int(), ConstantColumn::NUM);
			}
			//return cc;
			break;
		}
		case Item::STRING_ITEM:
		{
			String val, *str = item->val_str(&val);
			rc = new ConstantColumn(str->c_ptr());
			break;
		}
		case Item::REAL_ITEM:
		{
			String val, *str = item->val_str(&val);
			rc = new ConstantColumn(str->c_ptr(), item->val_real());
			break;
		}
		case Item::DECIMAL_ITEM:
		{
			rc = buildDecimalColumn(item, gwi);
			break;
		}
		case Item::FUNC_ITEM:
		{
			Item_func* ifp = (Item_func*)item;
			string func_name = ifp->func_name();

			// try to evaluate const F&E. only for select clause
			vector <Item_field*> tmpVec;
			//bool hasAggColumn = false;
			uint16_t parseInfo = 0;
			parse_item(ifp, tmpVec, gwi.fatalParseError, parseInfo);

			if (parseInfo & SUB_BIT)
			{
				gwi.fatalParseError = true;
				gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SELECT_SUB);
				setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
				break;
			}

			if (!gwi.fatalParseError &&
				  !nonConstFunc(ifp) &&
				  !(parseInfo & AF_BIT) &&
				  (tmpVec.size() == 0))
			{
				String val, *str = ifp->val_str(&val);
				if (!str)
				{
					rc = new ConstantColumn("", ConstantColumn::NULLDATA);
				}
				else if (ifp->result_type() == STRING_RESULT)
				{
					rc = new ConstantColumn(str->c_ptr(), ConstantColumn::LITERAL);
					rc->resultType(colType_MysqlToIDB(item));
				}
				else if (ifp->result_type() == DECIMAL_RESULT)
					rc = buildDecimalColumn(ifp, gwi);
				else
				{
					rc = new ConstantColumn(str->c_ptr(), ConstantColumn::NUM);
					rc->resultType(colType_MysqlToIDB(item));
				}
				break;
			}

			if (func_name == "+" || func_name == "-" || func_name == "*" || func_name == "/" )
				return buildArithmeticColumn(ifp, gwi, nonSupport);
			else
				return buildFunctionColumn(ifp, gwi, nonSupport);
		}
		case Item::SUM_FUNC_ITEM:
		{
			return buildAggregateColumn(item, gwi);
		}
		case Item::REF_ITEM:
		{
			Item_ref* ref = (Item_ref*)item;
			if ((*(ref->ref))->type() == Item::SUM_FUNC_ITEM)
			{
				return buildAggregateColumn(*(ref->ref), gwi);
			}
			else if ((*(ref->ref))->type() == Item::FIELD_ITEM)
				return buildReturnedColumn(*(ref->ref), gwi, nonSupport);
			else if ((*(ref->ref))->type() == Item::REF_ITEM)
				return buildReturnedColumn(*(((Item_ref*)(*(ref->ref)))->ref), gwi, nonSupport);
			else if ((*(ref->ref))->type() == Item::FUNC_ITEM)
				return buildFunctionColumn((Item_func*)(*(ref->ref)), gwi, nonSupport);
			else
			{
				gwi.fatalParseError = true;
				gwi.parseErrorText = "Unknown REF item";
				break;
			}
		}
		case Item::NULL_ITEM:
		{
			if (gwi.condPush)
				return new SimpleColumn("noop");
			return new ConstantColumn("", ConstantColumn::NULLDATA);
		}
		case Item::CACHE_ITEM:
		{
			Item* col = ((Item_cache*)item)->get_example();
			rc = buildReturnedColumn(col, gwi, nonSupport);
			if (rc)
			{
				ConstantColumn *cc = dynamic_cast<ConstantColumn*>(rc);
				if (!cc)
				{
					rc->joinInfo(rc->joinInfo() | JOIN_CORRELATED);
					if (gwi.subQuery)
						gwi.subQuery->correlated(true);
				}
			}
			break;
		}
		case Item::WINDOW_FUNC_ITEM:
		{
			return buildWindowFunctionColumn(item, gwi, nonSupport);
		}
		case Item::INTERVAL_ITEM:
		{
			Item_interval* interval = (Item_interval*)item;
			SRCP srcp;
			srcp.reset(buildReturnedColumn(interval->item, gwi, nonSupport));
			if (!srcp)
				return NULL;

			rc = new IntervalColumn(srcp, (int)interval->interval);
			rc->resultType(srcp->resultType());
			break;
		}
		case Item::SUBSELECT_ITEM:
		{
			gwi.hasSubSelect = true;
			break;
		}

		default:
		{
			gwi.fatalParseError = true;
			gwi.parseErrorText = "Unknown item type";
			break;
		}
	}
	if (rc && item->name)
		rc->alias(item->name);
	return rc;
}

ArithmeticColumn* buildArithmeticColumn(Item_func* item, gp_walk_info& gwi, bool& nonSupport)
{
	ArithmeticColumn* ac = new ArithmeticColumn();
	Item** sfitempp = item->arguments();
	ArithmeticOperator* aop = new ArithmeticOperator(item->func_name());
	ParseTree *pt = new ParseTree(aop);
	//ReturnedColumn *lhs = 0, *rhs = 0;
	ParseTree *lhs = 0, *rhs = 0;
	SRCP srcp;

	if (item->name)
		ac->alias(item->name);

	// arg_count should generally be 2, except negate expression
	if (item->arg_count == 2)
	{
		if (gwi.clauseType == SELECT || /*gwi.clauseType == HAVING || */gwi.clauseType == GROUP_BY || gwi.clauseType == FROM) // select list
		{
			lhs = new ParseTree(buildReturnedColumn(sfitempp[0], gwi, nonSupport));
			rhs = new ParseTree(buildReturnedColumn(sfitempp[1], gwi, nonSupport));
			if (!lhs->data() || !rhs->data())
			{
				gwi.fatalParseError = true;
				if (gwi.parseErrorText.empty())
				{
					gwi.parseErrorText = "Un-recognized Arithmetic Operand";
				}
				return NULL;
			}
			else if (nonSupport)
			{
				return NULL;
			}
		}
		else // where clause
		{
			if (isPredicateFunction(sfitempp[1], &gwi))
			{
				if (gwi.ptWorkStack.empty())
				{
					rhs = new ParseTree(buildReturnedColumn(sfitempp[1], gwi, nonSupport));
				}
				else
				{
					rhs = gwi.ptWorkStack.top();
					gwi.ptWorkStack.pop();
				}
			}
			else
			{
				if (gwi.rcWorkStack.empty())
				{
					rhs = new ParseTree(buildReturnedColumn(sfitempp[1], gwi, nonSupport));
				}
				else
				{
					rhs = new ParseTree(gwi.rcWorkStack.top());
					gwi.rcWorkStack.pop();
				}
			}
			if (isPredicateFunction(sfitempp[0], &gwi))
			{
				if (gwi.ptWorkStack.empty())
				{
					lhs = new ParseTree(buildReturnedColumn(sfitempp[0], gwi, nonSupport));
				}
				else
				{
					lhs = gwi.ptWorkStack.top();
					gwi.ptWorkStack.pop();
				}
			}
			else
			{
				if (gwi.rcWorkStack.empty())
				{
					lhs = new ParseTree(buildReturnedColumn(sfitempp[0], gwi, nonSupport));
				}
				else
				{
					lhs = new ParseTree(gwi.rcWorkStack.top());
					gwi.rcWorkStack.pop();
				}
			}
		}

		//aop->operationType(lhs->resultType(), rhs->resultType());
		pt->left(lhs);
		pt->right(rhs);
	}
	else
	{
		ConstantColumn *cc = new ConstantColumn(string("0"), (int64_t)0);
		if (gwi.clauseType == SELECT || gwi.clauseType == HAVING || gwi.clauseType == GROUP_BY) // select clause
		{
			rhs = new ParseTree(buildReturnedColumn(sfitempp[0], gwi, nonSupport));
		}
		else
		{
			if (gwi.rcWorkStack.empty())
			{
				rhs = new ParseTree(buildReturnedColumn(sfitempp[0], gwi, nonSupport));
			}
			else
			{
				rhs = new ParseTree(gwi.rcWorkStack.top());
				gwi.rcWorkStack.pop();
			}
		}
		pt->left(cc);
		pt->right(rhs);
	}

	//aop->resultType(colType_MysqlToIDB(item));
	// @bug5715. Use InfiniDB adjusted coltype for result type.
    // decimal arithmetic operation gives double result when the session variable is set.
	//idbassert(pt->left() && pt->right() && pt->left()->data() && pt->right()->data());
	CalpontSystemCatalog::ColType mysql_type = colType_MysqlToIDB(item);
	if (gwi.thd->variables.infinidb_double_for_decimal_math == 1)
		aop->adjustResultType(mysql_type);
	else
		aop->resultType(mysql_type);

	// adjust decimal result type according to internalDecimalScale
	if (gwi.internalDecimalScale >= 0 && aop->resultType().colDataType == CalpontSystemCatalog::DECIMAL)
	{
		CalpontSystemCatalog::ColType ct = aop->resultType();
		ct.scale = gwi.internalDecimalScale;
		aop->resultType(ct);
	}

	aop->operationType(aop->resultType());
	ac->expression(pt);
	ac->resultType(aop->resultType());
	ac->operationType(aop->operationType());
	ac->expressionId(gwi.expressionId++);
	// @3391. optimization. try to associate expression ID to the expression on the select list
	if (gwi.clauseType != SELECT)
	{
		for (uint i = 0; i < gwi.returnedCols.size(); i++)
		{
			if ((!ac->alias().empty()) && strcasecmp(ac->alias().c_str(), gwi.returnedCols[i]->alias().c_str()) == 0)
			{
				ac->expressionId(gwi.returnedCols[i]->expressionId());
				break;
			}
		}
	}
	return ac;
}

ReturnedColumn* buildFunctionColumn(Item_func* ifp, gp_walk_info& gwi, bool& nonSupport)
{
	string funcName = ifp->func_name();
	FuncExp* funcExp = FuncExp::instance();
	Func* functor;
	FunctionColumn *fc = NULL;

	// Arithmetic exp
	if (funcName == "+" || funcName == "-" || funcName == "*" || funcName == "/" )
	{
		ArithmeticColumn *ac = buildArithmeticColumn(ifp, gwi, nonSupport);
		return ac;
	}

	 // comment out for now until case function is fully tested.
	else if (funcName == "case")
	{
		fc = buildCaseFunction(ifp, gwi, nonSupport);
	}

	else if ((functor = funcExp->getFunctor(funcName)))
	{
		// where clause isnull still treated as predicate operator
		if ((funcName == "isnull" || funcName == "isnotnull") &&
			  (gwi.clauseType == WHERE || gwi.clauseType == HAVING))
			  return NULL;
		if (funcName == "in" || funcName == " IN " || funcName == "between")
		{
			// if F&E involved, build function. otherwise, fall to the old path.
			// @todo need more checks here
			if (ifp->arguments()[0]->type() == Item::ROW_ITEM)
			{
				return NULL;
			}

			if (ifp->arguments()[0]->type() == Item::FIELD_ITEM ||
				  (ifp->arguments()[0]->type() == Item::REF_ITEM &&
				  	(*(((Item_ref*)ifp->arguments()[0])->ref))->type() == Item::FIELD_ITEM))
			{
				bool fe = false;
				for (uint i = 1; i < ifp->arg_count; i++)
				{
					if (!(ifp->arguments()[i]->type() == Item::INT_ITEM ||
						  ifp->arguments()[i]->type() == Item::STRING_ITEM ||
						  ifp->arguments()[i]->type() == Item::REAL_ITEM ||
						  ifp->arguments()[i]->type() == Item::DECIMAL_ITEM ||
						  ifp->arguments()[i]->type() == Item::NULL_ITEM))
					{
						if (ifp->arguments()[i]->type() == Item::FUNC_ITEM)
						{
							// try to identify const F&E. fall to primitive if parms are constant F&E.
							vector <Item_field*> tmpVec;
							uint16_t parseInfo = 0;
							parse_item(ifp->arguments()[i], tmpVec, gwi.fatalParseError, parseInfo);

							if (!gwi.fatalParseError && !(parseInfo & AF_BIT) && tmpVec.size() == 0)
								continue;
						}
						fe = true;
						break;
					}
				}
				if (!fe) return NULL;
			}
			Item_func_opt_neg* inp = (Item_func_opt_neg*)ifp;
			if (inp->negated)
				funcName = "not" + funcName;
		}

		// @todo non-support function as argument. need to do post process. Assume all support for now
		fc = new FunctionColumn();
		FunctionParm funcParms;
		SPTP sptp;
		ClauseType clauseType = gwi.clauseType;
		if (gwi.clauseType == SELECT || /*gwi.clauseType == HAVING || */gwi.clauseType == GROUP_BY) // select clause
		{
			for (uint i = 0; i < ifp->arg_count; i++)
			{
				// group by clause try to see if the arguments are alias
				if (gwi.clauseType == GROUP_BY && ifp->arguments()[i]->name)
				{
					uint j = 0;
					for (; j < gwi.returnedCols.size(); j++)
					{
						if (string (ifp->arguments()[i]->name) == gwi.returnedCols[j]->alias())
						{
							ReturnedColumn *rc = gwi.returnedCols[j]->clone();
							rc->orderPos(j);
							sptp.reset(new ParseTree(rc));
							funcParms.push_back(sptp);
							break;
						}
					}
					if (j != gwi.returnedCols.size())
						continue;
				}
				// special handling for function that takes a filter arguments, like if().
				// @todo. merge this logic to buildParseTree().
				if (funcName == "if" && i == 0)
				{
					// make sure the rcWorkStack is cleaned.
					gwi.clauseType = WHERE;
					sptp.reset(buildParseTree((Item_func*)(ifp->arguments()[i]), gwi, nonSupport));
					gwi.clauseType = clauseType;
					if (!sptp)
					{
						nonSupport = true;
						return NULL;
					}
					funcParms.push_back(sptp);
					continue;
				}
				// @bug 3039
				if (isPredicateFunction(ifp->arguments()[i], &gwi) || ifp->arguments()[i]->with_subselect)
				{
					nonSupport = true;
					gwi.fatalParseError = true;
					gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_SUB_EXPRESSION);
					return NULL;
				}

				ReturnedColumn* rc = buildReturnedColumn(ifp->arguments()[i], gwi, nonSupport);
				if (!rc || nonSupport)
				{
					nonSupport = true;
					return NULL;
				}
				sptp.reset(new ParseTree(rc));
				funcParms.push_back(sptp);
			}
		}
		else // where clause
		{
			stack<SPTP> tmpPtStack;;
			for (uint i = 0; i < ifp->arg_count; i++)
			{
				if (funcName == "if" )
				{
					// bug5438
					gwi.clauseType = SELECT;
					if (i == 0)
					{
						// @bug 2366. build item from arguments to avoid parm sequence complexity
						sptp.reset(buildParseTree((Item_func*)(ifp->arguments()[i]), gwi, nonSupport));
						if (nonSupport)
							return NULL;
						funcParms.push_back(sptp);
						if (!gwi.ptWorkStack.empty())
							gwi.ptWorkStack.pop();
						continue;
					}
					else
					{
						ReturnedColumn *rc = buildReturnedColumn(ifp->arguments()[i], gwi, nonSupport);
						if (!rc || nonSupport)
						{
							nonSupport = true;
							return NULL;
						}
						sptp.reset(new ParseTree(rc));
						funcParms.push_back(sptp);
						if (!gwi.rcWorkStack.empty())
							gwi.rcWorkStack.pop();
					}
					gwi.clauseType = clauseType;
				}
				else
				{
					if (isPredicateFunction(ifp->arguments()[i], &gwi) || ifp->arguments()[i]->with_subselect)
					{
						nonSupport = true;
						gwi.fatalParseError = true;
						gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_SUB_EXPRESSION);
						return NULL;
					}
					else
					{
						if (gwi.rcWorkStack.empty())
						{
							sptp.reset(new ParseTree(buildReturnedColumn(ifp->arguments()[i], gwi, nonSupport)));
						}
						else
						{
							sptp.reset(new ParseTree(gwi.rcWorkStack.top()));
							gwi.rcWorkStack.pop();
						}
					}
					tmpPtStack.push(sptp);
				}
			}

			while (!tmpPtStack.empty())
			{
				funcParms.push_back(tmpPtStack.top());
				tmpPtStack.pop();
			}
		}

		// the followings are special treatment of some functions
		if (funcName == "week" && funcParms.size() == 1)
		{
			THD* thd = current_thd;
			sptp.reset(new ParseTree(new ConstantColumn(static_cast<uint64_t>(thd->variables.default_week_format))));
			funcParms.push_back(sptp);
		}

		// add the keyword unit argument for interval function
		if (funcName == "date_add_interval" || funcName == "extract" || funcName == "timestampdiff")
		{

			addIntervalArgs(ifp, funcParms);
		}

		// check for unsupported arguments add the keyword unit argument for extract functions
		if (funcName == "extract")
		{
			Item_date_add_interval* idai = (Item_date_add_interval*)ifp;
			switch (idai->int_type) {
				case INTERVAL_DAY_MICROSECOND:
				{
					nonSupport = true;
					gwi.fatalParseError = true;
					Message::Args args;
					string info = funcName + " with DAY_MICROSECOND parameter";
					args.add(info);
					gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
					return NULL;
				}
				case INTERVAL_HOUR_MICROSECOND:
				{
					nonSupport = true;
					gwi.fatalParseError = true;
					Message::Args args;
					string info = funcName + " with HOUR_MICROSECOND parameter";
					args.add(info);
					gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
					return NULL;
				}
				case INTERVAL_MINUTE_MICROSECOND:
				{
					nonSupport = true;
					gwi.fatalParseError = true;
					Message::Args args;
					string info = funcName + " with MINUTE_MICROSECOND parameter";
					args.add(info);
					gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
					return NULL;
				}
				default:
					break;
			}
		}

		// add the keyword unit argument and char length for cast functions
		if (funcName == "cast_as_char" )
		{
			castCharArgs(ifp, funcParms);
		}

		// add the length and scale arguments
		if (funcName == "decimal_typecast" )
		{
			castDecimalArgs(ifp, funcParms);
		}

		// add the type argument
		if (funcName == "get_format")
		{
			castTypeArgs(ifp, funcParms);
		}

		// add my_time_zone
		if (funcName == "unix_timestamp")
		{
#ifndef _MSC_VER
			time_t tmp_t= 1;
			struct tm tmp;
			localtime_r(&tmp_t, &tmp);
			sptp.reset(new ParseTree(new ConstantColumn(static_cast<int64_t>(tmp.tm_gmtoff), ConstantColumn::NUM)));
#else
			//FIXME: Get GMT offset (in seconds east of GMT) in Windows...
			sptp.reset(new ParseTree(new ConstantColumn(static_cast<int64_t>(0), ConstantColumn::NUM)));
#endif
			funcParms.push_back(sptp);
		}

		// add the default seed to rand function without arguments
		if (funcName == "rand")
		{
			if (funcParms.size() == 0)
			{
				sptp.reset(new ParseTree(new ConstantColumn((int64_t)gwi.thd->rand.seed1, ConstantColumn::NUM)));
				funcParms.push_back(sptp);
				sptp.reset(new ParseTree(new ConstantColumn((int64_t)gwi.thd->rand.seed2, ConstantColumn::NUM)));
				funcParms.push_back(sptp);
				gwi.no_parm_func_list.push_back(fc);
			}
			else
			{
				ConstantColumn* cc = dynamic_cast<ConstantColumn*>(funcParms[0]->data());
				if (cc)
					gwi.no_parm_func_list.push_back(fc);
			}
		}
		if (funcName == "sysdate")
		{
			gwi.no_parm_func_list.push_back(fc);
		}

		// add the sign for addtime function
		if (funcName == "add_time")
		{
			Item_func_add_time* addtime = (Item_func_add_time*)ifp;
			sptp.reset(new ParseTree(new ConstantColumn((int64_t)addtime->get_sign())));
			funcParms.push_back(sptp);
		}

		fc->functionName(funcName);
		fc->functionParms(funcParms);
		fc->resultType(colType_MysqlToIDB(ifp));

		// MySQL give string result type for date function, but has the flag set.
		// we should set the result type to be datetime for comparision.
		if (ifp->is_datetime())
		{
			CalpontSystemCatalog::ColType ct;
			ct.colDataType = CalpontSystemCatalog::DATETIME;
			ct.colWidth = 8;
			fc->resultType(ct);
		}
		if (funcName == "cast_as_date")
		{
			CalpontSystemCatalog::ColType ct;
			ct.colDataType = CalpontSystemCatalog::DATE;
			ct.colWidth = 4;
			fc->resultType(ct);
		}

		fc->operationType(functor->operationType(funcParms, fc->resultType()));
		fc->expressionId(gwi.expressionId++);
	}
	else if (ifp->type() == Item::COND_ITEM ||
		ifp->functype() == Item_func::EQ_FUNC ||
		ifp->functype() == Item_func::NE_FUNC ||
		ifp->functype() == Item_func::LT_FUNC ||
		ifp->functype() == Item_func::LE_FUNC ||
		ifp->functype() == Item_func::GE_FUNC ||
		ifp->functype() == Item_func::GT_FUNC ||
		ifp->functype() == Item_func::LIKE_FUNC ||
		ifp->functype() == Item_func::BETWEEN ||
		ifp->functype() == Item_func::IN_FUNC ||
		ifp->functype() == Item_func::ISNULL_FUNC ||
		ifp->functype() == Item_func::ISNOTNULL_FUNC ||
		ifp->functype() == Item_func::NOT_FUNC)
		return NULL;
	else
	{
		nonSupport = true;
		gwi.fatalParseError = true;
		Message::Args args;
		args.add(funcName);
		gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
		return NULL;
	}

	// adjust decimal result type according to internalDecimalScale
	if (!fc)
		return NULL;
	if (gwi.internalDecimalScale >= 0 && fc->resultType().colDataType == CalpontSystemCatalog::DECIMAL)
	{
		CalpontSystemCatalog::ColType ct = fc->resultType();
		ct.scale = gwi.internalDecimalScale;
		fc->resultType(ct);
	}
	if (ifp->name)
		fc->alias(ifp->name);

	// @3391. optimization. try to associate expression ID to the expression on the select list
	if (gwi.clauseType != SELECT)
	{
		for (uint i = 0; i < gwi.returnedCols.size(); i++)
		{
			if ((!fc->alias().empty()) && strcasecmp(fc->alias().c_str(), gwi.returnedCols[i]->alias().c_str()) == 0)
				fc->expressionId(gwi.returnedCols[i]->expressionId());
		}
	}
	return fc;
}

FunctionColumn* buildCaseFunction(Item_func* item, gp_walk_info& gwi, bool& nonSupport)
{
	FunctionColumn* fc = new FunctionColumn();
	FunctionParm funcParms;
	SPTP sptp;
	stack<SPTP> tmpPtStack;
	FuncExp* funcexp = FuncExp::instance();
	string funcName = "case_simple";

	if (((Item_func_case*)item)->first_expr_num == -1)
		funcName = "case_searched";

	if (gwi.clauseType == SELECT || gwi.clauseType == HAVING || gwi.clauseType == GROUP_BY) // select clause
	{
		// the first argument
		if (funcName == "case_searched")
		{
			for (uint i = 0; i < item->arg_count; i++)
			{
				if (i % 2 == 0 && i != 1 && i != item->arg_count-1)
				{
					sptp.reset(buildParseTree((Item_func*)(item->arguments()[i]), gwi, nonSupport));
					funcParms.push_back(sptp);
				}
				else
				{
					ReturnedColumn* parm = buildReturnedColumn(item->arguments()[i], gwi, nonSupport);
					if (parm)
					{
						sptp.reset(new ParseTree(parm));
					}
					else
					{
						sptp.reset(buildParseTree((Item_func*)(item->arguments()[i]), gwi, nonSupport));
					}
					funcParms.push_back(sptp);
				}
			}
		}
		else
		{
			for (uint i = 0; i < item->arg_count; i++)
			{
				ReturnedColumn* parm = buildReturnedColumn(item->arguments()[i], gwi, nonSupport);
				if (parm)
				{
					sptp.reset(new ParseTree(parm));
				}
				else
				{
					sptp.reset(buildParseTree((Item_func*)(item->arguments()[i]), gwi, nonSupport));
				}
				funcParms.push_back(sptp);
			}
		}
	}
	else // where clause
	{
		// so buildXXXcolumn function will not pop stack.
		gwi.clauseType = SELECT;
		if (funcName == "case_searched")
		{
			for (uint i = 0; i < item->arg_count; i++)
			{
				if (i % 2 == 0 && i != item->arg_count-1)
				{
					// build item from arguments to avoid parm sequence complexity
					sptp.reset(buildParseTree((Item_func*)(item->arguments()[i]), gwi, nonSupport));
					funcParms.push_back(sptp);
					if (!gwi.ptWorkStack.empty())
						gwi.ptWorkStack.pop();
				}
				else
				{
					ReturnedColumn* parm = buildReturnedColumn(item->arguments()[i], gwi, nonSupport);
					if (parm)
					{
						sptp.reset(new ParseTree(buildReturnedColumn(item->arguments()[i], gwi, nonSupport)));
						if (!gwi.rcWorkStack.empty())
							gwi.rcWorkStack.pop();
					}
					else
					{
						sptp.reset(buildParseTree((Item_func*)(item->arguments()[i]), gwi, nonSupport));
						if (!gwi.ptWorkStack.empty())
							gwi.ptWorkStack.pop();
					}
					funcParms.push_back(sptp);
				}
			}
		}
		else // simple_case
		{
			for (uint i = 0; i < item->arg_count; i++)
			{
				ReturnedColumn* parm = buildReturnedColumn(item->arguments()[i], gwi, nonSupport);
				if (parm)
				{
					sptp.reset(new ParseTree(parm));
					if (!gwi.rcWorkStack.empty())
						gwi.rcWorkStack.pop();
				}
				else
				{
					sptp.reset(buildParseTree((Item_func*)(item->arguments()[i]), gwi, nonSupport));
					if (!gwi.ptWorkStack.empty())
						gwi.ptWorkStack.pop();
				}
				funcParms.push_back(sptp);
			}
		}
		// recover clause type
		gwi.clauseType = WHERE;
	}

	if (gwi.fatalParseError)
	{
		setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
		return NULL;
	}

	Func* functor = funcexp->getFunctor(funcName);
	fc->resultType(colType_MysqlToIDB(item));
	fc->operationType(functor->operationType(funcParms, fc->resultType()));
	fc->functionName(funcName);
	fc->functionParms(funcParms);
	fc->expressionId(gwi.expressionId++);
	return fc;
}

ConstantColumn* buildDecimalColumn(Item *item, gp_walk_info &gwi)
{
	Item_decimal* idp = (Item_decimal*)item;
	IDB_Decimal infinidb_decimal;
	String val, *str = item->val_str(&val);
	ostringstream infinidb_decimal_val;
	uint i = 0;
	if (str->c_ptr()[0] == '+' || str->c_ptr()[0] == '-')
	{
		infinidb_decimal_val << str->c_ptr()[0];
		i = 1;
	}
	for (; i < str->length(); i++)
	{
		if (str->c_ptr()[i] == '.')
			continue;
		infinidb_decimal_val << str->c_ptr()[i];
	}
	infinidb_decimal.value = strtoll(infinidb_decimal_val.str().c_str(), 0, 10);

	if (gwi.internalDecimalScale >= 0 && idp->decimals > gwi.internalDecimalScale)
  {
  	infinidb_decimal.scale = gwi.internalDecimalScale;
  	double val = (double)(infinidb_decimal.value / pow((double)10, idp->decimals - gwi.internalDecimalScale));
		infinidb_decimal.value = (int64_t)(val > 0 ? val + 0.5 : val - 0.5);
	}
	else
		infinidb_decimal.scale = idp->decimals;
	infinidb_decimal.precision = idp->max_length - idp->decimals;
	return new ConstantColumn(str->c_ptr(), infinidb_decimal);
}

SimpleColumn* buildSimpleColumn(Item_field* ifp, gp_walk_info& gwi)
{
	if (!gwi.csc)
	{
		gwi.csc = CalpontSystemCatalog::makeCalpontSystemCatalog(gwi.sessionid);
		gwi.csc->identity(CalpontSystemCatalog::FE);
	}

	// support FRPM subquery. columns from the derived table has no definition
	if (!ifp->field || !ifp->db_name || strlen(ifp->db_name) == 0)
		return buildSimpleColFromDerivedTable(gwi, ifp);
	CalpontSystemCatalog::ColType ct;
	bool infiniDB = true;

	try
	{
		// check foreign engine
		if (ifp->cached_table && ifp->cached_table->table)
			infiniDB = isInfiniDB(ifp->cached_table->table);
		// @bug4509. ifp->cached_table could be null for myisam sometimes
		else if (ifp->field && ifp->field->table)
			infiniDB = isInfiniDB(ifp->field->table);

		if (infiniDB)
		{
			ct = gwi.csc->colType(
		         gwi.csc->lookupOID(make_tcn(ifp->db_name, bestTableName(ifp), ifp->field_name)));
		}
		else
		{
			ct = colType_MysqlToIDB(ifp);
		}
	}
	catch (std::exception& ex )
	{
		gwi.fatalParseError = true;
		gwi.parseErrorText = ex.what();
		return NULL;
	}
	SimpleColumn* sc = NULL;

	switch (ct.colDataType)
	{
		case CalpontSystemCatalog::TINYINT:
			if (ct.scale == 0)
				sc = new SimpleColumn_INT<1>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
			else
			{
				sc = new SimpleColumn_Decimal<1>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
				ct.colDataType = CalpontSystemCatalog::DECIMAL;
			}
			break;
		case CalpontSystemCatalog::SMALLINT:
			if (ct.scale == 0)
				sc = new SimpleColumn_INT<2>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
			else
			{
				sc = new SimpleColumn_Decimal<2>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
				ct.colDataType = CalpontSystemCatalog::DECIMAL;
			}
			break;
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::MEDINT:
			if (ct.scale == 0)
				sc = new SimpleColumn_INT<4>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
			else
			{
				sc = new SimpleColumn_Decimal<4>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
				ct.colDataType = CalpontSystemCatalog::DECIMAL;
			}
			break;
		case CalpontSystemCatalog::BIGINT:
			if (ct.scale == 0)
				sc = new SimpleColumn_INT<8>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
			else
			{
				sc = new SimpleColumn_Decimal<8>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
				ct.colDataType = CalpontSystemCatalog::DECIMAL;
			}
			break;
        case CalpontSystemCatalog::UTINYINT:
            sc = new SimpleColumn_UINT<1>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
            break;
        case CalpontSystemCatalog::USMALLINT:
            sc = new SimpleColumn_UINT<2>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
            break;
        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
            sc = new SimpleColumn_UINT<4>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
            break;
        case CalpontSystemCatalog::UBIGINT:
            sc = new SimpleColumn_UINT<8>(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
            break;
		default:
			sc = new SimpleColumn(ifp->db_name, bestTableName(ifp), ifp->field_name, infiniDB, gwi.sessionid);
	}
	sc->resultType(ct);
	string tbname(ifp->table_name);
	sc->tableAlias(lower(tbname));

	// view name
	if (ifp->cached_table)
		sc->viewName(lower(getViewName(ifp->cached_table)));

	sc->alias(ifp->name);
	sc->isInfiniDB(infiniDB);
	//SRCP srcp(sc);
	//gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name), srcp));
//	TABLE_LIST* tmp = (ifp->cached_table ? ifp->cached_table : 0);
	//gwi.tableMap[make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias(), sc->isInfiniDB())] = make_pair(1, tmp);
	if (!infiniDB && ifp->field)
		sc->oid(ifp->field->field_index + 1); // ExeMgr requires offset started from 1
	return sc;
}

ParseTree* buildParseTree(Item_func* item, gp_walk_info& gwi, bool& nonSupport)
{
	ParseTree* pt = 0;
	Item_cond* icp = (Item_cond*)item;
	// debug
	//cout << "Build Parsetree: " << endl;
	//icp->traverse_cond(debug_walk, &gwi, Item::POSTFIX);

	//@bug5044. PPSTFIX walking should always be treated as WHERE clause filter
	ClauseType clauseType = gwi.clauseType;
	gwi.clauseType = WHERE;
	icp->traverse_cond(gp_walk, &gwi, Item::POSTFIX);
	gwi.clauseType = clauseType;
	if (gwi.fatalParseError)
		return NULL;

	// bug 2840. if the filter/function is constant, result is in rcWorkStack
	if (!gwi.ptWorkStack.empty())
	{
		pt = gwi.ptWorkStack.top();
		gwi.ptWorkStack.pop();
	}
	else if (!gwi.rcWorkStack.empty())
	{
		pt = new ParseTree (gwi.rcWorkStack.top());
		gwi.rcWorkStack.pop();
	}

	return pt;
}

AggregateColumn* buildAggregateColumn(Item* item, gp_walk_info& gwi)
{
	Item_sum* isp = reinterpret_cast<Item_sum*>(item);
	Item** sfitempp = isp->args;
	SRCP parm;
	// @bug4756
	if (gwi.clauseType == SELECT)
		gwi.aggOnSelect = true;

	// N.B. arg_count is the # of formal parms to the agg fcn. InifniDB only supports 1 argument
	if (isp->arg_count > 1 && isp->sum_func() != Item_sum::GROUP_CONCAT_FUNC)
	{
		gwi.fatalParseError = true;
		gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_MUL_ARG_AGG);
		return NULL;
	}

	AggregateColumn* ac = NULL;
	if (isp->sum_func() == Item_sum::GROUP_CONCAT_FUNC)
		ac = new GroupConcatColumn(gwi.sessionid);
	else
		ac = new AggregateColumn(gwi.sessionid);

	if (isp->name)
		ac->alias(isp->name);

	if ((setAggOp(ac, isp)))
	{
		gwi.fatalParseError = true;
		gwi.parseErrorText = "Non supported aggregate type on the select clause";
		return NULL;
	}

	// special parsing for group_concat
	if (isp->sum_func() == Item_sum::GROUP_CONCAT_FUNC)
	{
		Item_func_group_concat *gc = (Item_func_group_concat*)isp;
		vector<SRCP> orderCols;
		RowColumn *rowCol = new RowColumn();
		vector<SRCP> selCols;
		uint32_t select_ctn = gc->count_field();
		ReturnedColumn *rc = NULL;
		for (uint i = 0; i < select_ctn; i++)
		{
			rc = buildReturnedColumn(sfitempp[i], gwi, gwi.fatalParseError);
			if (!rc || gwi.fatalParseError)
				return NULL;
			selCols.push_back(SRCP(rc));
		}

		ORDER **order_item, **end;
		for (order_item= gc->order, end=order_item + gc->order_field();order_item < end;
       order_item++)
		{
				Item *ord_col= *(*order_item)->item;
				if (ord_col->type() == Item::INT_ITEM)
				{
					Item_int* id = (Item_int*)ord_col;
					if (id->val_int() > (int)selCols.size())
					{
						gwi.fatalParseError = true;
						return NULL;
					}
					rc = selCols[id->val_int()-1]->clone();
					rc->orderPos(id->val_int()-1);
				}
				else
				{
					rc = buildReturnedColumn(ord_col, gwi, gwi.fatalParseError);
				}
					rc->asc((*order_item)->asc);
			orderCols.push_back(SRCP(rc));
		}

		rowCol->columnVec(selCols);
		(dynamic_cast<GroupConcatColumn*>(ac))->orderCols(orderCols);
		parm.reset(rowCol);
		if (gc->str_separator())
			(dynamic_cast<GroupConcatColumn*>(ac))->separator(gc->str_separator()->c_ptr());
	}
	else
	{
		for (uint i = 0; i < isp->arg_count; i++)
		{
			Item* sfitemp = sfitempp[i];
			Item::Type sfitype = sfitemp->type();
			switch (sfitype)
			{
				case Item::FIELD_ITEM:
				{
					Item_field* ifp = reinterpret_cast<Item_field*>(sfitemp);
					SimpleColumn* sc = buildSimpleColumn(ifp, gwi);
					if (!sc)
					{
						gwi.fatalParseError = true;
						break;
					}

					parm.reset(sc);
					gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name), parm));
					TABLE_LIST* tmp = (ifp->cached_table ? ifp->cached_table : 0);
					gwi.tableMap[make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias(), sc->isInfiniDB())] = make_pair(1, tmp);
					break;
				}
				case Item::INT_ITEM:
				case Item::STRING_ITEM:
				case Item::REAL_ITEM:
				case Item::DECIMAL_ITEM:
				{
					// treat as count(*)
					if (ac->aggOp() == AggregateColumn::COUNT)
						ac->aggOp(AggregateColumn::COUNT_ASTERISK);

					gwi.count_asterisk_list.push_back(ac);
					ac->constCol(SRCP(buildReturnedColumn(sfitemp, gwi, gwi.fatalParseError)));
					break;
				}
				case Item::NULL_ITEM:
				{
					//ac->aggOp(AggregateColumn::COUNT);
					parm.reset(new ConstantColumn("", ConstantColumn::NULLDATA));
					//ac->functionParms(parm);
					gwi.count_asterisk_list.push_back(ac);
					ac->constCol(SRCP(buildReturnedColumn(sfitemp, gwi, gwi.fatalParseError)));
					break;
				}
				case Item::FUNC_ITEM:
				{
					Item_func* ifp = (Item_func*)sfitemp;
					ReturnedColumn * rc = 0;

					// check count(1+1) case
					vector <Item_field*> tmpVec;
					uint16_t parseInfo = 0;
					parse_item(ifp, tmpVec, gwi.fatalParseError, parseInfo);
					if (parseInfo & SUB_BIT)
					{
						gwi.fatalParseError = true;
						break;
					}
					else if (!gwi.fatalParseError &&
						       !(parseInfo & AGG_BIT) &&
						       !(parseInfo & AF_BIT) &&
						       tmpVec.size() == 0)
					{
						rc = buildFunctionColumn(ifp, gwi, gwi.fatalParseError);
						FunctionColumn* fc = dynamic_cast<FunctionColumn*>(rc);
						if ((fc && fc->functionParms().empty()) || !fc)
						{
							//ac->aggOp(AggregateColumn::COUNT_ASTERISK);
							ReturnedColumn* rc = buildReturnedColumn(sfitemp, gwi, gwi.fatalParseError);
							if (dynamic_cast<ConstantColumn*>(rc))
							{
								//@bug5229. handle constant function on aggregate argument
								gwi.count_asterisk_list.push_back(ac);
								ac->constCol(SRCP(rc));
								break;
							}
						}
					}

					// MySQL carelessly allows correlated aggregate function on the WHERE clause.
					// Here is the work around to deal with that inconsistence.
					// e.g., SELECT (SELECT t.c FROM t1 AS t WHERE t.b=MAX(t1.b + 0)) FROM t1;
					ClauseType clauseType = gwi.clauseType;
					if (gwi.clauseType == WHERE)
						gwi.clauseType = HAVING;
					// @bug 3603. for cases like max(rand()). try to build function first.
					if (!rc)
						rc = buildFunctionColumn(ifp, gwi, gwi.fatalParseError);

					parm.reset(rc);
					gwi.clauseType = clauseType;
					if (gwi.fatalParseError)
						break;

					//ac->functionParms(parm);
					break;
				}
				case Item::REF_ITEM:
				{
					ReturnedColumn *rc = buildReturnedColumn(sfitemp, gwi, gwi.fatalParseError);
					if (rc)
					{
						parm.reset(rc);
						//ac->functionParms(parm);
						break;
					}
				}
				default:
				{
					gwi.fatalParseError = true;
					//gwi.parseErrorText = "Non-supported Item in Aggregate function";
				}
			}
			if (gwi.fatalParseError)
			{
				if (gwi.parseErrorText.empty())
				{
					Message::Args args;
					if (item->name)
						args.add(item->name);
					else
						args.add("");
					gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_AGG_ARGS, args);
				}
				return NULL;
			}
		}
	}

	if (parm)
	{
		ac->functionParms(parm);
		if (isp->sum_func() == Item_sum::AVG_FUNC ||
			isp->sum_func() == Item_sum::AVG_DISTINCT_FUNC)
		{
			CalpontSystemCatalog::ColType ct = parm->resultType();
			switch (ct.colDataType)
			{
				case CalpontSystemCatalog::TINYINT:
				case CalpontSystemCatalog::SMALLINT:
				case CalpontSystemCatalog::MEDINT:
				case CalpontSystemCatalog::INT:
				case CalpontSystemCatalog::BIGINT:
				case CalpontSystemCatalog::DECIMAL:
				case CalpontSystemCatalog::UDECIMAL:
				case CalpontSystemCatalog::UTINYINT:
				case CalpontSystemCatalog::USMALLINT:
				case CalpontSystemCatalog::UMEDINT:
				case CalpontSystemCatalog::UINT:
				case CalpontSystemCatalog::UBIGINT:
					ct.colDataType = CalpontSystemCatalog::DECIMAL;
					ct.colWidth = 8;
					ct.scale += 4;
					break;

#if PROMOTE_FLOAT_TO_DOUBLE_ON_SUM
				case CalpontSystemCatalog::FLOAT:
				case CalpontSystemCatalog::UFLOAT:
				case CalpontSystemCatalog::DOUBLE:
				case CalpontSystemCatalog::UDOUBLE:
					ct.colDataType = CalpontSystemCatalog::DOUBLE;
					ct.colWidth = 8;
					break;
#endif

				default:
					break;
			}
			ac->resultType(ct);
		}
		else if (isp->sum_func() == Item_sum::COUNT_FUNC ||
				 isp->sum_func() == Item_sum::COUNT_DISTINCT_FUNC)
		{
			CalpontSystemCatalog::ColType ct;
			ct.colDataType = CalpontSystemCatalog::BIGINT;
			ct.colWidth = 8;
			ct.scale = parm->resultType().scale;
			ac->resultType(ct);
		}
		else if (isp->sum_func() == Item_sum::SUM_FUNC ||
				 isp->sum_func() == Item_sum::SUM_DISTINCT_FUNC)
		{
			CalpontSystemCatalog::ColType ct = parm->resultType();
			switch (ct.colDataType)
			{
				case CalpontSystemCatalog::TINYINT:
				case CalpontSystemCatalog::SMALLINT:
				case CalpontSystemCatalog::MEDINT:
				case CalpontSystemCatalog::INT:
				case CalpontSystemCatalog::BIGINT:
					ct.colDataType = CalpontSystemCatalog::BIGINT;
				// no break, let fall through

				case CalpontSystemCatalog::DECIMAL:
				case CalpontSystemCatalog::UDECIMAL:
					ct.colWidth = 8;
					break;

				case CalpontSystemCatalog::UTINYINT:
				case CalpontSystemCatalog::USMALLINT:
				case CalpontSystemCatalog::UMEDINT:
				case CalpontSystemCatalog::UINT:
				case CalpontSystemCatalog::UBIGINT:
					ct.colDataType = CalpontSystemCatalog::UBIGINT;
					ct.colWidth = 8;
					break;

#if PROMOTE_FLOAT_TO_DOUBLE_ON_SUM
				case CalpontSystemCatalog::FLOAT:
				case CalpontSystemCatalog::UFLOAT:
				case CalpontSystemCatalog::DOUBLE:
				case CalpontSystemCatalog::UDOUBLE:
					ct.colDataType = CalpontSystemCatalog::DOUBLE;
					ct.colWidth = 8;
					break;
#endif

				default:
					break;
			}
			ac->resultType(ct);
		}
		else if (isp->sum_func() == Item_sum::STD_FUNC ||
				 isp->sum_func() == Item_sum::VARIANCE_FUNC)
		{
			CalpontSystemCatalog::ColType ct;
			ct.colDataType = CalpontSystemCatalog::DOUBLE;
			ct.colWidth = 8;
			ct.scale = 0;
			ac->resultType(ct);
		}
		else if (isp->sum_func() == Item_sum::SUM_BIT_FUNC)
		{
			CalpontSystemCatalog::ColType ct;
			ct.colDataType = CalpontSystemCatalog::BIGINT;
			ct.colWidth = 8;
			ct.scale = 0;
			ct.precision = -16; // borrowed to indicate skip null value check on connector
			ac->resultType(ct);
		}
		else if (isp->sum_func() == Item_sum::GROUP_CONCAT_FUNC)
		{
			//Item_func_group_concat* gc = (Item_func_group_concat*)isp;
			CalpontSystemCatalog::ColType ct;
			ct.colDataType = CalpontSystemCatalog::VARCHAR;
			ct.colWidth = isp->max_length;
			ct.precision = 0;
			ac->resultType(ct);
		}
		else
		{
			ac->resultType(parm->resultType());
		}
	}
	else
	{
		ac->resultType(colType_MysqlToIDB(isp));
	}

	// adjust decimal result type according to internalDecimalScale
	if (gwi.internalDecimalScale >= 0 && ac->resultType().colDataType == CalpontSystemCatalog::DECIMAL)
	{
		CalpontSystemCatalog::ColType ct = ac->resultType();
		ct.scale = gwi.internalDecimalScale;
		ac->resultType(ct);
	}

	// check for same aggregate on the select list
	ac->expressionId(gwi.expressionId++);
	if (gwi.clauseType != SELECT)
	{
		for (uint i = 0; i < gwi.returnedCols.size(); i++)
		{
			if (*ac == gwi.returnedCols[i].get())
				ac->expressionId(gwi.returnedCols[i]->expressionId());
		}
	}
	return ac;
}

void addIntervalArgs(Item_func* ifp, FunctionParm& functionParms)
{
	string funcName = ifp->func_name();
	int interval_type = -1;
	if (funcName == "date_add_interval")
		interval_type = ((Item_date_add_interval*)ifp)->int_type;
	else if (funcName == "timestampdiff")
		interval_type = ((Item_func_timestamp_diff*)ifp)->int_type;
	else if (funcName == "extract")
		interval_type = ((Item_extract*)ifp)->int_type;

	functionParms.push_back(getIntervalType(interval_type));
	SPTP sptp;
	if (funcName == "date_add_interval")
	{
		if (((Item_date_add_interval*)ifp)->date_sub_interval)
		{
			sptp.reset(new ParseTree(new ConstantColumn((int64_t)OP_SUB)));
			functionParms.push_back(sptp);
		}
		else
		{
			sptp.reset(new ParseTree(new ConstantColumn((int64_t)OP_ADD)));
			functionParms.push_back(sptp);
		}
	}
}

SPTP getIntervalType(int interval_type)
{
	SPTP sptp;
    sptp.reset(new ParseTree(new ConstantColumn((int64_t)interval_type)));
	return sptp;
}

void castCharArgs(Item_func* ifp, FunctionParm& functionParms)
{
	Item_char_typecast * idai = (Item_char_typecast *)ifp;

	SPTP sptp;
	sptp.reset(new ParseTree(new ConstantColumn((int64_t)idai->castLength())));
	functionParms.push_back(sptp);
}

void castDecimalArgs(Item_func* ifp, FunctionParm& functionParms)
{
	Item_decimal_typecast * idai = (Item_decimal_typecast *)ifp;
	SPTP sptp;
	sptp.reset(new ParseTree(new ConstantColumn((int64_t)idai->decimals)));
	functionParms.push_back(sptp);
	// max length including sign and/or decimal points
	if (idai->decimals == 0)
		sptp.reset(new ParseTree(new ConstantColumn((int64_t)idai->max_length-1)));
	else
		sptp.reset(new ParseTree(new ConstantColumn((int64_t)idai->max_length-2)));
	functionParms.push_back(sptp);
}

void castTypeArgs(Item_func* ifp, FunctionParm& functionParms)
{
	Item_func_get_format* get_format = (Item_func_get_format*)ifp;
	SPTP sptp;
	if (get_format->type == MYSQL_TIMESTAMP_DATE)
		sptp.reset(new ParseTree(new ConstantColumn("DATE")));
	else
		sptp.reset(new ParseTree(new ConstantColumn("DATETIME")));
	functionParms.push_back(sptp);
}

void gp_walk(const Item *item, void *arg)
{
	gp_walk_info* gwip = reinterpret_cast<gp_walk_info*>(arg);
	idbassert(gwip);

	//Bailout...
	if (gwip->fatalParseError) return;

	Item::Type itype = item->type();
	if (itype == Item::FUNC_ITEM && string(((Item_func*)item)->func_name()) == "xor")
		itype = Item::COND_ITEM;

	switch (itype)
	{
		case Item::FIELD_ITEM:
		{
			Item_field* ifp = (Item_field*)item;
			if (ifp)
			{
				SimpleColumn *scp = buildSimpleColumn(ifp, *gwip);
				if (!scp)
					break;
				string aliasTableName(scp->tableAlias());

				if (ifp->depended_from)
				{
					scp->joinInfo(scp->joinInfo() | JOIN_CORRELATED);
					if (gwip->subQuery)
						gwip->subQuery->correlated(true);

					// for error out non-support select filter case (comparison outside semi join tables)
					gwip->correlatedTbNameVec.push_back(make_aliastable(scp->schemaName(), scp->tableName(), scp->tableAlias()));

					// imply semi for scalar for now.
					if (gwip->subSelectType == CalpontSelectExecutionPlan::SINGLEROW_SUBS)
						scp->joinInfo(scp->joinInfo() | JOIN_SCALAR | JOIN_SEMI);
					if (gwip->subSelectType == CalpontSelectExecutionPlan::SELECT_SUBS)
						scp->joinInfo(scp->joinInfo() | JOIN_SCALAR | JOIN_OUTER_SELECT);
				}
				scp->tableAlias(lower(aliasTableName));
				gwip->rcWorkStack.push(scp->clone());
				shared_ptr<SimpleColumn> scsp(scp);
				gwip->scsp = scsp;
				gwip->funcName.clear();
				gwip->columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name), scsp));

				//@bug4636 take where clause column as dummy projection column, but only on local column.
				// varbinary aggregate is not supported yet, so rule it out
				if (!((scp->joinInfo() & JOIN_CORRELATED) || scp->colType().colDataType == CalpontSystemCatalog::VARBINARY))
				{
					TABLE_LIST* tmp = (ifp->cached_table ? ifp->cached_table : 0);
					gwip->tableMap[make_aliastable(scp->schemaName(), scp->tableName(), scp->tableAlias(), scp->isInfiniDB())] =
					           make_pair(1, tmp);
				}
			}
			break;
		}
		case Item::INT_ITEM:
		{
			Item_int* iip = (Item_int*)item;
			gwip->rcWorkStack.push(buildReturnedColumn(iip, *gwip, gwip->fatalParseError));
			break;
		}
		case Item::STRING_ITEM:
		{
			Item_string* isp = (Item_string*)item;
			if (isp)
			{
				// @bug 3669. trim trailing spaces for the compare value
				if (isp->cmp_context == STRING_RESULT)
				{
					String val, *str = isp->val_str(&val);
					string cval(str->c_ptr());
					size_t spos = cval.find_last_not_of(" ");
					if (spos != string::npos)
						cval = cval.substr(0, spos+1);
					gwip->rcWorkStack.push(new ConstantColumn(cval));
					break;
				}

				gwip->rcWorkStack.push(buildReturnedColumn(isp, *gwip, gwip->fatalParseError));
			}
			break;
		}
		case Item::REAL_ITEM:
		{
			Item_float* ifp = (Item_float*)item;
			gwip->rcWorkStack.push(buildReturnedColumn(ifp, *gwip, gwip->fatalParseError));
			break;
		}
		case Item::DECIMAL_ITEM:
		{
			Item_decimal* idp = (Item_decimal*)item;
 			gwip->rcWorkStack.push(buildReturnedColumn(idp, *gwip, gwip->fatalParseError));
			break;
		}
		case Item::VARBIN_ITEM:
		{
			Item_hex_string* hdp = (Item_hex_string*)item;
 			gwip->rcWorkStack.push(buildReturnedColumn(hdp, *gwip, gwip->fatalParseError));
 			break;
		}
		case Item::NULL_ITEM:
		{
			if (gwip->condPush)
			{
				// push noop for unhandled item
				SimpleColumn *rc = new SimpleColumn("noop");
				gwip->rcWorkStack.push(rc);
				break;
			}
			gwip->rcWorkStack.push(new ConstantColumn("", ConstantColumn::NULLDATA));
			break;
		}
		case Item::FUNC_ITEM:
		{
			Item_func* ifp = (Item_func*)item;
			string funcName = ifp->func_name();

			if (!gwip->condPush)
			{
				if (ifp->with_subselect || funcName == "<in_optimizer>")
				{
					buildSubselectFunc(ifp, gwip);
					return;
				}
				if (ifp->arguments() && ifp->arg_count > 0)
				{
					for (uint i = 0; i < ifp->arg_count; i++)
					{
						if (ifp->arguments()[i]->type() == Item::SUBSELECT_ITEM)
						{
							// This is probably NOT IN subquery with derived table in it.
							// for some reason, MySQL has not fully optimized the plan at this point.
							// noop here, and eventually MySQL will continue its optimization and get
							// to rnd_init again.
							if (ifp->functype() == Item_func::NOT_FUNC)
								return;
							buildSubselectFunc(ifp, gwip);
							return;
						}
					}
				}
				if (ifp->functype() == Item_func::TRIG_COND_FUNC && gwip->subQuery)
				{
					gwip->subQuery->handleFunc(gwip, ifp);
					break;
				}
				// having clause null function added by MySQL
				if (ifp->functype() ==  Item_func::ISNOTNULLTEST_FUNC)
				{
					// @bug 4215. remove the argument in rcWorkStack.
					if (!gwip->rcWorkStack.empty())
						gwip->rcWorkStack.pop();
					break;
				}
			}

			// try to evaluate const F&E
			vector <Item_field*> tmpVec;
			uint16_t parseInfo = 0;
			parse_item(ifp, tmpVec, gwip->fatalParseError, parseInfo);

			// table mode takes only one table filter
			if (gwip->condPush)
			{
				set<string> tableSet;
				for (uint i = 0; i < tmpVec.size(); i++)
				{
					if (tmpVec[i]->table_name)
						tableSet.insert(tmpVec[i]->table_name);
				}
				if (tableSet.size() > 1)
					break;
			}

			if (!gwip->fatalParseError && !(parseInfo & AGG_BIT) &&
				  !(parseInfo & SUB_BIT) &&
				  !nonConstFunc(ifp) &&
				  !(parseInfo & AF_BIT) &&
				  tmpVec.size() == 0)
			{
				String val, *str = ifp->val_str(&val);

				ConstantColumn *cc = NULL;
				if (!str) //@ bug 2844 check whether parameter is defined
				{
					cc = new ConstantColumn("", ConstantColumn::NULLDATA);
				}
				else if (ifp->result_type() == STRING_RESULT)
				{
					cc = new ConstantColumn(str->c_ptr(), ConstantColumn::LITERAL);
				}
				else if (ifp->result_type() == DECIMAL_RESULT)
				{
					cc = buildDecimalColumn(ifp, *gwip);
				}
				else
				{
					cc = new ConstantColumn(str->c_ptr(), ConstantColumn::NUM);
					cc->resultType(colType_MysqlToIDB(item));
				}

				for (uint i = 0; i < ifp->arg_count && !gwip->rcWorkStack.empty(); i++)
				{
					gwip->rcWorkStack.pop();
				}

				// bug 3137. If filter constant like 1=0, put it to ptWorkStack
				if (isPredicateFunction(ifp, gwip))
					gwip->ptWorkStack.push(new ParseTree(cc));
				else
					gwip->rcWorkStack.push(cc);
				if (str)
					IDEBUG( cout << "Const F&E " << item->name << " evaluate: " << str->ptr() << endl );
				break;
			}

			ReturnedColumn* rc = NULL;

			// @bug4488. Process function for table mode also, not just vtable mode.
			rc = buildFunctionColumn(ifp, *gwip, gwip->fatalParseError);

			if (gwip->fatalParseError)
			{
				if (gwip->clauseType == SELECT)
					return;
				// @bug 2585
				if (gwip->parseErrorText.empty())
				{
					Message::Args args;
					args.add(funcName);
					gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
				}
				return;
			}

			// predicate operators fall in the old path
			if (rc)
			{
				// @bug 2383. For some reason func_name() for "in" gives " IN " always
				if (funcName == "between" || funcName == "in" || funcName == " IN ")
					gwip->ptWorkStack.push(new ParseTree(rc));
				else
					gwip->rcWorkStack.push(rc);
			}
			else
			{
				// push to pt or rc stack is handled inside the function
				buildPredicateItem(ifp, gwip);
			}
			break;
		}
		case Item::SUM_FUNC_ITEM:
		{
			Item_sum *isp = (Item_sum*)item;
			ReturnedColumn *rc = buildAggregateColumn(isp, *gwip);
			if (rc)
				gwip->rcWorkStack.push(rc);
			break;
		}
		case Item::COND_ITEM:
		{
			Item_cond* icp = (Item_cond*)item;

			if (icp)
			{
				// @bug2932. if ptWorkStack contains less items than the condition's arguments,
				// the missing one should be in the rcWorkStack, unless the it's subselect.
				// @todo need to figure out a way to combine these two stacks while walking.
				//if (gwip->ptWorkStack.size() < icp->argument_list()->elements)
				{
					List_iterator_fast<Item> li(*(icp->argument_list()));
	  			while (Item *it= li++)
	  			{
	  				if ((it->type() == Item::FIELD_ITEM
	  					  || it->type() == Item::INT_ITEM
	  					  || it->type() == Item::DECIMAL_ITEM
	  					  || it->type() == Item::STRING_ITEM
	  					  || it->type() == Item::REAL_ITEM
	  					  || it->type() == Item::NULL_ITEM
	  					  || (it->type() == Item::FUNC_ITEM
	  					  && !isPredicateFunction(it, gwip))) && !gwip->rcWorkStack.empty()
	  					 )
	  				{
	  					gwip->ptWorkStack.push(new ParseTree(gwip->rcWorkStack.top()));
	  					gwip->rcWorkStack.pop();
	  				}
	  			}
  			}
				// @bug1603. MySQL's filter tree is a multi-tree grouped by operator. So more than
				// two filters saved on the stack so far might belong to this operator.
				uint leftInStack = gwip->ptWorkStack.size() - icp->argument_list()->elements + 1;
				while (true)
				{
					if (gwip->ptWorkStack.size() < 2)
						break;

					ParseTree* lhs = gwip->ptWorkStack.top();
					gwip->ptWorkStack.pop();
					SimpleFilter *lsf = dynamic_cast<SimpleFilter*>(lhs->data());
					if (lsf && lsf->op()->data() == "noop")
					{
						if (icp->functype() == Item_func::COND_OR_FUNC)
						{
							gwip->parseErrorText = "Unhandled item in WHERE or HAVING clause";
							gwip->fatalParseError = true;
							break;
						}
						else
							continue;
					}

					ParseTree* rhs = gwip->ptWorkStack.top();
					gwip->ptWorkStack.pop();
					SimpleFilter *rsf = dynamic_cast<SimpleFilter*>(rhs->data());
					if (rsf && rsf->op()->data() == "noop")
					{
						if (icp->functype() == Item_func::COND_OR_FUNC)
						{
							gwip->parseErrorText = "Unhandled item in WHERE or HAVING clause";
							gwip->fatalParseError = true;
							break;
						}
						else
						{
							gwip->ptWorkStack.push(lhs);
							continue;
						}
					}
					Operator* op = new LogicOperator(icp->func_name());
					ParseTree* ptp = new ParseTree(op);

					// bug 3459. error out non-supported OR with correlated subquery
					if (icp->functype() == Item_func::COND_OR_FUNC)
					{
						SelectFilter *lsel = dynamic_cast<SelectFilter*>(lhs->data());
						SelectFilter *rsel = dynamic_cast<SelectFilter*>(rhs->data());
						ExistsFilter *lexists = dynamic_cast<ExistsFilter*>(lhs->data());
						ExistsFilter *rexists = dynamic_cast<ExistsFilter*>(rhs->data());

						if ((lsel && lsel->correlated()) || (rsel && rsel->correlated()) ||
							   (lexists && lexists->correlated()) || (rexists && rexists->correlated()))
						{
							gwip->fatalParseError = true;
							gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_CORRELATED_SUB_OR);
							break;
						}
					}

					ptp->left(lhs);
					ptp->right(rhs);
					gwip->ptWorkStack.push(ptp);
					if (gwip->ptWorkStack.size() == leftInStack)
						break;
				}

				// special handling for subquery with aggregate. MySQL adds isnull function to the selected
				// column. InfiniDB will remove it and set nullmatch flag if it's NOT_IN sub.
				// @todo need more checking here to make sure it's not a user input OR operator
				if (icp->functype() == Item_func::COND_OR_FUNC && gwip->subQuery)
					gwip->subQuery->handleFunc(gwip, icp);
			}
			break;
		}
		case Item::REF_ITEM:
		{
			Item* col = *(((Item_ref*)item)->ref);
			ReturnedColumn *rc = NULL;
			// ref item is not pre-walked. force clause type to SELECT
			ClauseType clauseType = gwip->clauseType;
			gwip->clauseType = SELECT;
			if (col->type() != Item::COND_ITEM)
				rc = buildReturnedColumn(col, *gwip, gwip->fatalParseError);
			SimpleColumn *sc = dynamic_cast<SimpleColumn*>(rc);
			if (sc)
			{
				shared_ptr<SimpleColumn> scsp(sc->clone());
				gwip->scsp = scsp;
			}

			bool cando = true;
			gwip->clauseType = clauseType;
			if (rc)
			{
				if (((Item_ref*)item)->depended_from)
				{
					rc->joinInfo(rc->joinInfo() | JOIN_CORRELATED);
					if (gwip->subQuery)
						gwip->subQuery->correlated(true);

					SimpleColumn *scp = dynamic_cast<SimpleColumn*>(rc);
					if (scp)
						gwip->correlatedTbNameVec.push_back(make_aliastable(scp->schemaName(), scp->tableName(), scp->tableAlias()));
					if (gwip->subSelectType == CalpontSelectExecutionPlan::SINGLEROW_SUBS)
						rc->joinInfo(rc->joinInfo() | JOIN_SCALAR | JOIN_SEMI);
					if (gwip->subSelectType == CalpontSelectExecutionPlan::SELECT_SUBS)
						rc->joinInfo(rc->joinInfo() | JOIN_SCALAR | JOIN_OUTER_SELECT);
				}
				gwip->rcWorkStack.push(rc);
			}
			else if (col->type() == Item::FUNC_ITEM)
			{
				// sometimes mysql treat having filter items inconsistently. In such cases,
				// which are always predicate operator, the function (gp_key>3) comes in as
				// one item.
				Item_func* ifp = (Item_func*)col;
				for (uint i = 0; i < ifp->arg_count; i++)
				{
					ReturnedColumn *operand = NULL;
					if (ifp->arguments()[i]->type() == Item::REF_ITEM)
					{
						Item* op = *(((Item_ref*)ifp->arguments()[i])->ref);
						operand = buildReturnedColumn(op, *gwip, gwip->fatalParseError);
					}
					else
						operand = buildReturnedColumn(ifp->arguments()[i], *gwip, gwip->fatalParseError);
					if (operand)
						gwip->rcWorkStack.push(operand);
					else
					{
						cando = false;
						break;
					}
				}
				if (cando)
					buildPredicateItem(ifp, gwip);
			}
			else if (col->type() == Item::COND_ITEM)
			{
				Item_func* ifp = (Item_func*)col;
				gwip->ptWorkStack.push(buildParseTree(ifp, *gwip, gwip->fatalParseError));
			}
			else
				cando = false;

			if (!cando)
			{
				ostringstream oss;
				oss << "Unhandled Item type: " << item->type();
				gwip->parseErrorText = oss.str();
				gwip->fatalParseError = true;
			}
			break;
		}
		case Item::SUBSELECT_ITEM:
		{
			if (gwip->condPush) // table mode
				break;
			Item_subselect* sub = (Item_subselect*)item;

			if (sub->substype() == Item_subselect::EXISTS_SUBS)
			{
				SubQuery *orig = gwip->subQuery;
				ExistsSub* existsSub = new ExistsSub(*gwip, sub);
				gwip->hasSubSelect = true;
				gwip->subQuery = existsSub;
				gwip->ptWorkStack.push(existsSub->transform());
				current_thd->infinidb_vtable.isUnion = true; // only temp. bypass the 2nd phase.
				// recover original
				gwip->subQuery = orig;
				gwip->lastSub = existsSub;
			}

			// store a dummy subselect object. the transform is handled in item_func.
			SubSelect *subselect = new SubSelect();
			gwip->rcWorkStack.push(subselect);
			break;
		}
		case Item::ROW_ITEM:
		{
			Item_row* row = (Item_row*)item;
			RowColumn *rowCol = new RowColumn();
			vector<SRCP> cols;
			// temp change clause type because the elements of row column are not walked yet
			gwip->clauseType = SELECT;
			for (uint i = 0; i < row->cols(); i++)
				cols.push_back(SRCP(buildReturnedColumn(row->element_index(i), *gwip, gwip->fatalParseError)));
			gwip->clauseType = WHERE;
			rowCol->columnVec(cols);
			gwip->rcWorkStack.push(rowCol);
			break;
		}
		case Item::WINDOW_FUNC_ITEM:
		{
			gwip->hasWindowFunc = true;
			Item_func_window* ifa = (Item_func_window*)item;
			ReturnedColumn* af = buildWindowFunctionColumn(ifa, *gwip, gwip->fatalParseError);
			if (af)
				gwip->rcWorkStack.push(af);
			break;
		}
		default:
		{
			if (gwip->condPush)
			{
				// push noop for unhandled item
				SimpleColumn *rc = new SimpleColumn("noop");
				gwip->rcWorkStack.push(rc);
				break;
			}
			ostringstream oss;
			oss << "Unhandled Item type: " << item->type();
			gwip->parseErrorText = oss.str();
			gwip->fatalParseError = true;
			break;
		}
	}
	return;
}

/** @info this function recursivly walks an item's arguments and push all
 *  the involved item_fields to the passed in vector. It's used in parsing
 *  functions or arithmetic expressions for vtable post process.
 */
void parse_item (Item *item, vector<Item_field*>& field_vec, bool& hasNonSupportItem, uint16_t& parseInfo)
{
	Item::Type itype = item->type();
	switch (itype)
	{
		case Item::FIELD_ITEM:
		{
			Item_field* ifp = reinterpret_cast<Item_field*>(item);
			field_vec.push_back(ifp);
			return;
		}
		case Item::SUM_FUNC_ITEM:
		{
			//hasAggColumn = true;
			parseInfo |= AGG_BIT;
			Item_sum* isp = reinterpret_cast<Item_sum*>(item);
			Item** sfitempp = isp->args;
			for (uint i = 0; i < isp->arg_count; i++)
				parse_item(sfitempp[i], field_vec, hasNonSupportItem, parseInfo);
			break;
		}
		case Item::FUNC_ITEM:
		{
			Item_func* isp = reinterpret_cast<Item_func*>(item);
			Item** sfitempp = isp->arguments();
			for (uint i = 0; i < isp->arg_count; i++)
				parse_item(sfitempp[i], field_vec, hasNonSupportItem, parseInfo);
			break;
		}
		case Item::COND_ITEM:
		{
			Item_cond* icp = reinterpret_cast<Item_cond*>(item);
			List_iterator_fast<Item> it(*(icp->argument_list()));
			Item *cond_item;
			while ((cond_item = it++))
				parse_item(cond_item, field_vec, hasNonSupportItem, parseInfo);
			break;
		}
		case Item::REF_ITEM:
		{
			while (true)
			{
				Item_ref* ref = (Item_ref*)item;
				if ((*(ref->ref))->type() == Item::SUM_FUNC_ITEM)
				{
					parseInfo |= AGG_BIT;
					Item_sum* isp = reinterpret_cast<Item_sum*>(*(ref->ref));
					Item** sfitempp = isp->args;
					// special handling for count(*). This should not be treated as constant.
					if (isp->arg_count == 1 &&
						  (sfitempp[0]->type() == Item::INT_ITEM ||
						   sfitempp[0]->type() == Item::STRING_ITEM ||
						   sfitempp[0]->type() == Item::REAL_ITEM	||
						   sfitempp[0]->type() == Item::DECIMAL_ITEM))
						field_vec.push_back((Item_field*)item); //dummy
					for (uint i = 0; i < isp->arg_count; i++)
						parse_item(sfitempp[i], field_vec, hasNonSupportItem, parseInfo);
					break;
				}
				else if ((*(ref->ref))->type() == Item::FIELD_ITEM)
				{
					Item_field* ifp = reinterpret_cast<Item_field*>(*(ref->ref));
					field_vec.push_back(ifp);
					break;
				}
				else if ((*(ref->ref))->type() == Item::FUNC_ITEM)
				{
					Item_func* isp = reinterpret_cast<Item_func*>(*(ref->ref));
					Item** sfitempp = isp->arguments();
					for (uint i = 0; i < isp->arg_count; i++)
						parse_item(sfitempp[i], field_vec, hasNonSupportItem, parseInfo);
					break;
				}
				else if ((*(ref->ref))->type() == Item::CACHE_ITEM)
				{
					Item_cache* isp = reinterpret_cast<Item_cache*>(*(ref->ref));
					parse_item(isp->get_example(), field_vec, hasNonSupportItem, parseInfo);
					break;
				}
				else if ((*(ref->ref))->type() == Item::REF_ITEM)
				{
					item = (*(ref->ref));
					continue;
				}
				else
				{
					//cout << "UNKNOWN REF Item" << endl;
					break;
				}
			}
			break;
		}
		case Item::SUBSELECT_ITEM:
			parseInfo |= SUB_BIT;
			break;
		case Item::ROW_ITEM:
		{
			Item_row *row = (Item_row*)item;
			for (uint i = 0; i < row->cols(); i++)
				parse_item(row->element_index(i), field_vec, hasNonSupportItem, parseInfo);
			break;
		}
		case Item::WINDOW_FUNC_ITEM:
			parseInfo |= AF_BIT;
		default:
			return;
	}
}

bool isInfiniDB(TABLE* table_ptr)
{
#if (defined(_MSC_VER) && defined(_DEBUG)) || defined(SAFE_MUTEX)
	if (!(table_ptr->s && (*table_ptr->s->db_plugin)->name.str))
#else
	if (!(table_ptr->s && (table_ptr->s->db_plugin)->name.str))
#endif
		return true;

#if (defined(_MSC_VER) && defined(_DEBUG)) || defined(SAFE_MUTEX)
	string engineName = (*table_ptr->s->db_plugin)->name.str;
#else
	string engineName = table_ptr->s->db_plugin->name.str;
#endif

	if (engineName == "InfiniDB")
		return true;
	else
		return false;
}

int getSelectPlan(gp_walk_info& gwi, SELECT_LEX& select_lex, SCSEP& csep, bool isUnion)
{
	// by pass the derived table resolve phase of mysql
	if (!(((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) ||
		 ((gwi.thd->lex)->sql_command == SQLCOM_DELETE ) ||
		 ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) ||
		 ((gwi.thd->lex)->sql_command == SQLCOM_DELETE_MULTI ) ) && gwi.thd->derived_tables_processing)
	{
		gwi.thd->infinidb_vtable.isUnion = false;
		return -1;
	}

	// rollup is currently not supported
	if (select_lex.olap == ROLLUP_TYPE)
	{
		gwi.fatalParseError = true;
		gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_ROLLUP_NOT_SUPPORT);
		setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
		return HA_ERR_UNSUPPORTED;
	}

	gwi.internalDecimalScale = (gwi.thd->variables.infinidb_use_decimal_scale ? gwi.thd->variables.infinidb_decimal_scale : -1);
	gwi.subSelectType = csep->subType();

	JOIN* join = select_lex.join;
	Item_cond* icp = 0;
	if (join != 0)
		icp = reinterpret_cast<Item_cond*>(join->conds);

	// if icp is null, try to find the where clause other where
	if (!join && gwi.thd->lex->derived_tables)
	{
		if (select_lex.prep_where)
			icp = (Item_cond*)(select_lex.prep_where);
		else if (select_lex.where)
			icp = (Item_cond*)(select_lex.where);
	}
	else if (!join && ( ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) ||
		      ((gwi.thd->lex)->sql_command == SQLCOM_DELETE ) ||
		      ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) ||
		      ((gwi.thd->lex)->sql_command == SQLCOM_DELETE_MULTI )))
	{
		icp = reinterpret_cast<Item_cond*>(select_lex.where);
	}
	uint32_t sessionID = csep->sessionID();
	gwi.sessionid = sessionID;
	boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	csc->identity(CalpontSystemCatalog::FE);
	gwi.csc = csc;

	// @bug 2123. Override large table estimate if infinidb_ordered hint was used.
	// @bug 2404. Always override if the infinidb_ordered_only variable is turned on.
	if (gwi.thd->infinidb_vtable.override_largeside_estimate || gwi.thd->variables.infinidb_ordered_only)
		csep->overrideLargeSideEstimate(true);

	// @bug 3321. Set max number of blocks in a dictionary file to be scanned for filtering
	csep->stringScanThreshold(gwi.thd->variables.infinidb_string_scan_threshold);

	csep->stringTableThreshold(gwi.thd->variables.infinidb_stringtable_threshold);

	// populate table map and trigger syscolumn cache for all the tables (@bug 1637).
	// all tables on FROM list must have at least one col in colmap
	TABLE_LIST* table_ptr = select_lex.get_table_list();
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

			// Enterprise View
#ifdef SKIP_VIEW
			if (table_ptr->view)
			{
				Message::Args args;
				args.add("View");
				setError(gwi.thd, HA_ERR_UNSUPPORTED, (IDBErrorInfo::instance()->errorMsg(ERR_ENTERPRISE_ONLY, args)));
				return HA_ERR_UNSUPPORTED;
			}
#endif

			// @todo process from subquery
			if (table_ptr->derived)
			{
				SELECT_LEX *select_cursor = table_ptr->derived->first_select();
				FromSubQuery fromSub(gwi, select_cursor);
				string alias(table_ptr->alias);
				fromSub.alias(lower(alias));

				CalpontSystemCatalog::TableAliasName tn = make_aliasview("", "", alias, viewName);
				// @bug 3852. check return execplan
				SCSEP plan = fromSub.transform();
				if (!plan)
				{
					setError(gwi.thd, HA_ERR_INTERNAL_ERROR, fromSub.gwip().parseErrorText);
					CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
					return HA_ERR_INTERNAL_ERROR;
				}

				gwi.derivedTbList.push_back(plan);
				gwi.tbList.push_back(tn);
				gwi.tableMap[tn] = make_pair(0, table_ptr);
				gwi.thd->infinidb_vtable.isUnion = true; //by-pass the 2nd pass of rnd_init
			}
#ifndef SKIP_VIEW
			else if (table_ptr->view)
			{
				View *view = new View(table_ptr->view->select_lex, &gwi);
				CalpontSystemCatalog::TableAliasName tn = make_aliastable(table_ptr->db, table_ptr->table_name, table_ptr->alias);
				view->viewName(tn);
				gwi.viewList.push_back(view);
				view->transform();
			}
#endif
			else
			{
				// check foreign engine tables
				bool infiniDB = (table_ptr->table ? isInfiniDB(table_ptr->table) : true);

				// trigger system catalog cache
				if (infiniDB)
					csc->columnRIDs(make_table(table_ptr->db, table_ptr->table_name), true);

				CalpontSystemCatalog::TableAliasName tn = make_aliasview(table_ptr->db, table_ptr->table_name, table_ptr->alias, viewName, infiniDB);
				gwi.tbList.push_back(tn);
				gwi.tableMap[tn] = make_pair(0,table_ptr);
			}
		}
		if (gwi.fatalParseError)
		{
			setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
			return HA_ERR_UNSUPPORTED;
		}
	}
	catch (IDBExcept& ie)
	{
		setError(gwi.thd, HA_ERR_INTERNAL_ERROR, ie.what());
		CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
		// @bug 3852. set error status for gwi.
		gwi.fatalParseError = true;
		gwi.parseErrorText = ie.what();
		return HA_ERR_INTERNAL_ERROR;
	}
	catch (...)
	{
		string emsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);
		// @bug3852 set error status for gwi.
		gwi.fatalParseError = true;
		gwi.parseErrorText = emsg;
		setError(gwi.thd, HA_ERR_INTERNAL_ERROR, emsg);
		CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
		return HA_ERR_INTERNAL_ERROR;
	}

	csep->tableList(gwi.tbList);

	bool unionSel = false;
	if (!isUnion && select_lex.master_unit()->is_union())
	{
		gwi.thd->infinidb_vtable.isUnion = true;
		CalpontSelectExecutionPlan::SelectList unionVec;
		SELECT_LEX *select_cursor = select_lex.master_unit()->first_select();
		unionSel = true;
		uint8_t distUnionNum = 0;

		for (SELECT_LEX *sl= select_cursor; sl; sl= sl->next_select())
		{
			SCSEP plan(new CalpontSelectExecutionPlan());
			plan->txnID(csep->txnID());
			plan->verID(csep->verID());
			plan->sessionID(csep->sessionID());
			plan->traceFlags(csep->traceFlags());
			plan->data(csep->data());

			// @bug 3853. When one or more sides or union queries contain derived tables,
			// sl->join->zero_result_cause is not trustable. Since we've already handled
			// constant filter now (0/1), we can relax the following checking.
			// @bug 2547. ignore union unit of zero result set case
//			if (sl->join)
//			{
//				sl->join->optimize();
				// @bug 3067. not clear MySQL's behavior. when in subquery, this variable
				// is not trustable.
//				if (sl->join->zero_result_cause && !gwi.subQuery)
//					continue;
//			}

			// gwi for the union unit
			gp_walk_info union_gwi;
			union_gwi.thd = gwi.thd;
			uint err = 0;
			if ((err = getSelectPlan(union_gwi, *sl, plan, unionSel)) != 0)
				return err;

			unionVec.push_back(SCEP(plan));

			// distinct union num
			if (sl == select_lex.master_unit()->union_distinct)
				distUnionNum = unionVec.size();

#ifdef DEBUG_WALK_COND
			IDEBUG( cout << ">>>> UNION DEBUG" << endl );
			JOIN* join = sl->join;
			Item_cond* icp = 0;
			if (join != 0)
				icp = reinterpret_cast<Item_cond*>(join->conds);
			if (icp)
				icp->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
			IDEBUG ( cout << *plan << endl );
			IDEBUG ( cout << "<<<<UNION DEBUG" << endl );
#endif
		}
		csep->unionVec(unionVec);
		csep->distinctUnionNum(distUnionNum);
		if (unionVec.empty())
			gwi.thd->infinidb_vtable.impossibleWhereOnUnion = true;
	}

	gwi.clauseType = WHERE;

	if (icp)
	{
		//@bug 3039. fix fields for constants
		if (!icp->fixed)
			icp->fix_fields(gwi.thd, (Item**)&icp);
		gwi.fatalParseError = false;
#ifdef DEBUG_WALK_COND
		cout << "------------------ WHERE -----------------------" << endl;
		icp->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
		cout << "------------------------------------------------\n" << endl;
#endif

		icp->traverse_cond(gp_walk, &gwi, Item::POSTFIX);
		if (gwi.fatalParseError)
		{
			// if this is dervied table process phase, mysql may have not developed the plan
			// completely. Do not error and eventually mysql will call JOIN::exec() again.
			// related to bug 2922. Need to find a way to skip calling rnd_init for derived table
			// processing.
			if (gwi.thd->derived_tables_processing)
			{
				gwi.thd->infinidb_vtable.isUnion = false;
				gwi.thd->infinidb_vtable.isUpdateWithDerive = true;
				return -1;
			}
			setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
			return HA_ERR_UNSUPPORTED;
		}
	}

		// ZZ - the followinig debug shows the structure of nested outer join. should
		// use a recursive function.
#ifdef OUTER_JOIN_DEBUG
		List<TABLE_LIST> *tables = &(select_lex.top_join_list);
		List_iterator_fast<TABLE_LIST> ti(*tables);
		TABLE_LIST *inner;
		TABLE_LIST **table= (TABLE_LIST **)gwi.thd->alloc(sizeof(TABLE_LIST*) * tables->elements);
		for (TABLE_LIST **t= table + (tables->elements - 1); t >= table; t--)
			*t= ti++;

		DBUG_ASSERT(tables->elements >= 1);

		TABLE_LIST **end= table + tables->elements;
		for (TABLE_LIST **tbl= table; tbl < end; tbl++)
		{
			TABLE_LIST *curr= *tbl;
		if (curr->table_name)
			cout << curr->table_name << " ";
		else
			cout << curr->alias << endl;
		if (curr->outer_join)
			cout << " is inner table" << endl;
		else if (curr->straight)
			cout << "straight_join" << endl;
		else
			cout << "join" << endl;

		if (curr->nested_join)
		{
			List<TABLE_LIST> *inners = &(curr->nested_join->join_list);
			List_iterator_fast<TABLE_LIST> li(*inners);
			TABLE_LIST **inner= (TABLE_LIST **)gwi.thd->alloc(sizeof(TABLE_LIST*) * inners->elements);
			for (TABLE_LIST **t= inner + (inners->elements - 1); t >= inner; t--)
				*t= li++;
			TABLE_LIST **end1= inner + inners->elements;
			for (TABLE_LIST **tb= inner; tb < end1; tb++)
			{
				TABLE_LIST *curr1= *tb;
				cout << curr1->alias << endl;
				if (curr1->on_expr)
				{
					curr1->on_expr->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
				}
			}
		}
		if (curr->on_expr)
		{
			curr->on_expr->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
		}
	}
#endif

	uint32_t failed = buildOuterJoin(gwi, select_lex);

	// @bug5764. build outer join for view, make sure outerjoin filter is appended
	// to the end of the filter list.
	for (uint i = 0; i < gwi.viewList.size(); i++)
	{
		failed = gwi.viewList[i]->processOuterJoin(gwi);
		if (failed)
			break;
	}
	if (failed != 0)
		return failed;

	ParseTree* filters = NULL;
	ParseTree* ptp = NULL;
	ParseTree* lhs = NULL;

	// @bug 2932. for "select * from region where r_name" case. if icp not null and
	// ptWorkStack empty, the item is in rcWorkStack.
	if (icp && gwi.ptWorkStack.empty() && !gwi.rcWorkStack.empty())
	{
		filters = new ParseTree(gwi.rcWorkStack.top());
		gwi.rcWorkStack.pop();
	}

	while (!gwi.ptWorkStack.empty())
	{
		filters = gwi.ptWorkStack.top();
		gwi.ptWorkStack.pop();
		if (gwi.ptWorkStack.empty())
			break;
		ptp = new ParseTree(new LogicOperator("and"));
		//ptp->left(filters);
		ptp->right(filters);
		lhs = gwi.ptWorkStack.top();
		gwi.ptWorkStack.pop();
		//ptp->right(rhs);
		ptp->left(lhs);
		gwi.ptWorkStack.push(ptp);
	}

	if (filters)
	{
		csep->filters(filters);
		filters->drawTree("/tmp/filter1.dot");
	}

	gwi.clauseType = SELECT;
#ifdef DEBUG_WALK_COND
{
	cout << "------------------- SELECT --------------------" << endl;
	List_iterator_fast<Item> it(select_lex.item_list);
	Item *item;
	while ((item = it++))
	{
		debug_walk(item, 0);
	}
	cout << "-----------------------------------------------\n" << endl;
}
#endif

	// populate returnedcolumnlist and columnmap
	List_iterator_fast<Item> it(select_lex.item_list);
	Item *item;
	vector <Item_field*> funcFieldVec;
	string sel_cols_in_create;
	string sel_cols_in_select;
	bool redo = false;

	// empty rcWorkStack and ptWorkStack. They should all be empty by now.
	clearStacks(gwi);

	// indicate the starting pos of scalar returned column, because some join column
	// has been inserted to the returned column list.
	if (gwi.subQuery)
	{
		ScalarSub* scalar = dynamic_cast<ScalarSub*>(gwi.subQuery);
		if (scalar)
			scalar->returnedColPos(gwi.returnedCols.size());
	}

	CalpontSelectExecutionPlan::SelectList selectSubList;

	while ((item= it++))
	{
		string itemAlias = (item->name? item->name : "");

		// @bug 5916. Need to keep checking until getting concrete item in case
		// of nested view.
		while (item->type() == Item::REF_ITEM)
		{
			Item_ref* ref = (Item_ref*)item;
			item = (*(ref->ref));
		}

		Item::Type itype = item->type();
		switch (itype)
		{
			case Item::FIELD_ITEM:
			{
				Item_field* ifp = (Item_field*)item;
				SimpleColumn *sc = NULL;

				// FROM clause subquery handling
				if (!ifp->field || !ifp->db_name || strlen(ifp->db_name) == 0)
				{
					// select * from (select ...);
					if (string(ifp->field_name) == "*")
					{
						// collect all the columns from table list
						collectAllCols(gwi, ifp);
						// @todo selectCols need to be handled for post process?
						break;
					}
					// no table alias prefixed on the selected column
					else
					{
						// @todo find out the which derived table this column is from by matching field name
						sc = buildSimpleColFromDerivedTable(gwi, ifp);
					}
				}
				else
				{
					sc = buildSimpleColumn(ifp, gwi);
				}
				if (sc)
				{
					shared_ptr<SimpleColumn> spsc(sc);
					if (sel_cols_in_create.length() != 0)
						sel_cols_in_create += ", ";
					string fullname;
					String str;
					ifp->print(&str, QT_INFINIDB_NO_QUOTE);
					fullname = str.c_ptr();
					//sel_cols_in_create += fullname;
					if (ifp->is_autogenerated_name) // no alias
					{
						sel_cols_in_create += fullname + " `" + escapeBackTick(str.c_ptr()) + "`";
						sc->alias(fullname);
					}
					else // alias
					{
						if (!itemAlias.empty())
							sc->alias(itemAlias);
						sel_cols_in_create += fullname + " `" + escapeBackTick(sc->alias().c_str()) + "`";
					}
					if (ifp->is_autogenerated_name)
						gwi.selectCols.push_back("`" + escapeBackTick(fullname.c_str()) + "`" + " `" +
						                        escapeBackTick(itemAlias.empty()? ifp->name : itemAlias.c_str()) + "`");
					else
						gwi.selectCols.push_back("`" + escapeBackTick((itemAlias.empty()? ifp->name : itemAlias.c_str())) + "`");

					gwi.returnedCols.push_back(spsc);

					gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name), spsc));
					TABLE_LIST* tmp = 0;
					if (ifp->cached_table)
						tmp = ifp->cached_table;
					gwi.tableMap[make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias(), sc->isInfiniDB())] =
						               make_pair(1, tmp);
				}
				else
				{
					setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
					delete sc;
					return HA_ERR_UNSUPPORTED;
				}
				break;
			}
			//aggregate column
			case Item::SUM_FUNC_ITEM:
			{
				AggregateColumn *ac = buildAggregateColumn(item, gwi);
				if (gwi.fatalParseError)
				{
					// e.g., non-support ref column
					setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
					delete ac;
					return HA_ERR_UNSUPPORTED;
				}

				// add this agg col to returnedColumnList
				shared_ptr<AggregateColumn> spac(ac);
				gwi.returnedCols.push_back(spac);
				gwi.selectCols.push_back('`' + escapeBackTick(spac->alias().c_str()) + '`');
				String str;
				item->print(&str, QT_INFINIDB_NO_QUOTE);
				if (sel_cols_in_create.length() != 0)
					sel_cols_in_create += ", ";
				sel_cols_in_create += string(str.c_ptr()) + " `" + escapeBackTick(spac->alias().c_str()) + "`";
				break;
			}
			case Item::FUNC_ITEM:
			{
				Item_func* ifp = reinterpret_cast<Item_func*>(item);

				// @bug4383. error out non-support stored function
				if (ifp->functype() == Item_func::FUNC_SP)
				{
					gwi.fatalParseError = true;
					gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_SP_FUNCTION_NOT_SUPPORT);
					setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
					return HA_ERR_UNSUPPORTED;
				}

				if (string(ifp->func_name()) == "xor")
				{
					gwi.fatalParseError = true;
					gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
					setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
					return HA_ERR_UNSUPPORTED;
				}

				uint16_t parseInfo = 0;
				vector <Item_field*> tmpVec;
				bool hasNonSupportItem = false;
				parse_item(ifp, tmpVec, hasNonSupportItem, parseInfo);
				if (ifp->with_subselect ||
					  string(ifp->func_name()) == string("<in_optimizer>") ||
					  ifp->functype() == Item_func::NOT_ALL_FUNC ||
					  parseInfo & SUB_BIT)
				{
					gwi.fatalParseError = true;
					gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SELECT_SUB);
					setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
					return HA_ERR_UNSUPPORTED;
				}

				ReturnedColumn* rc = buildFunctionColumn(ifp, gwi, hasNonSupportItem);

				if (rc)
				{
					if (!hasNonSupportItem && !nonConstFunc(ifp) && !(parseInfo & AF_BIT) && tmpVec.size() == 0)
					{
						if (isUnion || unionSel || gwi.subSelectType != CalpontSelectExecutionPlan::MAIN_SELECT ||
							  parseInfo & SUB_BIT || select_lex.group_list.elements != 0)
						{
							SRCP srcp(buildReturnedColumn(item, gwi, gwi.fatalParseError));
							gwi.returnedCols.push_back(srcp);
							if (ifp->name)
								srcp->alias(ifp->name);
							continue;
						}

						if ( ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) ||
							   ((gwi.thd->lex)->sql_command == SQLCOM_DELETE ) ||
							   ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) ||
							   ((gwi.thd->lex)->sql_command == SQLCOM_DELETE_MULTI ) )
						{ }
						else
						{
							redo = true;
							String str;
							ifp->print(&str, QT_INFINIDB_NO_QUOTE);
							gwi.selectCols.push_back(string(str.c_ptr()) + " " + "`" + escapeBackTick(item->name) + "`");
						}
						break;
					}

					SRCP srcp(rc);
					gwi.returnedCols.push_back(srcp);
					if ( ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE ) || ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE_MULTI ))
					{ }
					else
					{
						String str;
						ifp->print(&str, QT_INFINIDB_NO_QUOTE);
						if (sel_cols_in_create.length() != 0)
							sel_cols_in_create += ", ";
						sel_cols_in_create += string(str.c_ptr()) + " `" + ifp->name + "`";
						gwi.selectCols.push_back("`" + escapeBackTick(ifp->name) + "`");
					}
				}
				else // InfiniDB Non support functions still go through post process for now
				{
					hasNonSupportItem = false;
					uint before_size = funcFieldVec.size();
					parse_item(ifp, funcFieldVec, hasNonSupportItem, parseInfo);
					uint after_size = funcFieldVec.size();
					// group by func and func in subquery can not be post processed
					// @bug3881. set_user_var can not be treated as constant function
					// @bug5716. Try to avoid post process function for union query.
					if ((gwi.subQuery || select_lex.group_list.elements != 0 ||
					     !csep->unionVec().empty() || isUnion) &&
						 !hasNonSupportItem && (after_size-before_size) == 0 &&
						 !(parseInfo & AGG_BIT) && !(parseInfo & SUB_BIT) &&
						 string(ifp->func_name()) != "set_user_var")
					{
						String val, *str = ifp->val_str(&val);
						ConstantColumn *cc = NULL;
						if (!str)
						{
							cc = new ConstantColumn("", ConstantColumn::NULLDATA);
						}
						else if (ifp->result_type() == STRING_RESULT)
						{
							cc = new ConstantColumn(str->c_ptr(), ConstantColumn::LITERAL);
						}
						else if (ifp->result_type() == DECIMAL_RESULT)
						{
							cc = buildDecimalColumn(ifp, gwi);
						}
						else
						{
							cc = new ConstantColumn(str->c_ptr(), ConstantColumn::NUM);
							cc->resultType(colType_MysqlToIDB(item));
						}
						SRCP srcp(cc);
						if (ifp->name)
							cc->alias(ifp->name);
						gwi.returnedCols.push_back(srcp);

						// clear the error set by buildFunctionColumn
						gwi.fatalParseError = false;
						gwi.parseErrorText = "";
						break;
					}
					else if (hasNonSupportItem || parseInfo & AGG_BIT || parseInfo & SUB_BIT ||
							   (gwi.fatalParseError && gwi.subQuery))
					{
						if (gwi.parseErrorText.empty())
						{
							Message::Args args;
							args.add(ifp->func_name());
							gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
						}
						setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
						return HA_ERR_UNSUPPORTED;
					}
					else if ( gwi.subQuery && (isPredicateFunction(ifp, &gwi) || ifp->type() == Item::COND_ITEM ))
					{
						gwi.fatalParseError = true;
						gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
						setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
						return HA_ERR_UNSUPPORTED;
					}

					//@Bug 3030 Add error check for dml statement
					if ( ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE ) || ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE_MULTI ) )
					{
						if ( after_size-before_size != 0 )
						{
							gwi.parseErrorText = ifp->func_name();
							return -1;
						}
					}

					//@Bug 3021. Bypass postprocess for update and delete.
					//if ( ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE ) || ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE_MULTI ))
					//{}
					else
					{
						// @bug 3881. Here is the real redo part.
						redo = true;
						// @bug 1706
						String funcStr;
						ifp->print(&funcStr, QT_INFINIDB);
						gwi.selectCols.push_back(string(funcStr.c_ptr()) + " `" + escapeBackTick(ifp->name) + "`");
						// clear the error set by buildFunctionColumn
						gwi.fatalParseError = false;
						gwi.parseErrorText = "";
					}
				}
				break;
			}
			case Item::INT_ITEM:
			{
				if ( ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE ) || ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE_MULTI ))
				{ }
				else
				{
					// do not push the dummy column (mysql added) to returnedCol
					if (item->name && string(item->name) == "Not_used")
						continue;

					// @bug3509. Constant column is sent to ExeMgr now.
					SRCP srcp(buildReturnedColumn(item, gwi, gwi.fatalParseError));
					if (item->name)
						srcp->alias(item->name);
					gwi.returnedCols.push_back(srcp);

					Item_int* isp = reinterpret_cast<Item_int*>(item);
					ostringstream oss;
					oss << isp->value << " `" << escapeBackTick(srcp->alias().c_str()) << "`";
					if (sel_cols_in_create.length() != 0)
						sel_cols_in_create += ", ";
					sel_cols_in_create += oss.str();
					gwi.selectCols.push_back(oss.str());
				}
				break;
			}
			case Item::STRING_ITEM:
			{
				if ( ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE ) || ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE_MULTI ))
				{ }
				else
				{
					SRCP srcp(buildReturnedColumn(item, gwi, gwi.fatalParseError));
					gwi.returnedCols.push_back(srcp);
					if (item->name)
						srcp->alias(item->name);

					Item_string* isp = reinterpret_cast<Item_string*>(item);
					String val;
					String* str = isp->val_str(&val);
					string name = "'" + string(str->c_ptr()) + "'" + " " + "`" + escapeBackTick(srcp->alias().c_str()) + "`";

					if (sel_cols_in_create.length() != 0)
						sel_cols_in_create += ", ";
					sel_cols_in_create += name;
					gwi.selectCols.push_back(name);
				}
				break;
			}
			case Item::DECIMAL_ITEM:
			{
				if ( ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE ) || ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE_MULTI ))
				{ }
				else
				{
					SRCP srcp(buildReturnedColumn(item, gwi, gwi.fatalParseError));
					gwi.returnedCols.push_back(srcp);
					if (item->name)
						srcp->alias(item->name);

					Item_decimal* isp = reinterpret_cast<Item_decimal*>(item);
					String val, *str = isp->val_str(&val);
					ostringstream oss;
					oss << str->c_ptr() << " `" << escapeBackTick(srcp->alias().c_str()) << "`";
					if (sel_cols_in_create.length() != 0)
						sel_cols_in_create += ", ";
					sel_cols_in_create += oss.str();
					gwi.selectCols.push_back(oss.str());
				}
				break;
			}
			case Item::NULL_ITEM:
			{
				if ( ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE ) || ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) || ((gwi.thd->lex)->sql_command == SQLCOM_DELETE_MULTI ) )
				{ }
				else
				{
					SRCP srcp(buildReturnedColumn(item, gwi, gwi.fatalParseError));
					gwi.returnedCols.push_back(srcp);
					if (item->name)
						srcp->alias(item->name);
					string name = string("null `") + escapeBackTick(srcp->alias().c_str()) + string("`") ;
					if (sel_cols_in_create.length() != 0)
						sel_cols_in_create += ", ";
					sel_cols_in_create += name;
					gwi.selectCols.push_back("null");
				}
				break;
			}
			case Item::SUBSELECT_ITEM:
			{
				Item_subselect* sub = (Item_subselect*)item;
				if (sub->substype() != Item_subselect::SINGLEROW_SUBS)
				{
					gwi.fatalParseError = true;
					gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SELECT_SUB);
					setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
					return HA_ERR_UNSUPPORTED;
				}
#ifdef DEBUG_WALK_COND
				cout << "SELECT clause SUBSELECT Item: " << sub->substype() << endl;
				JOIN* join = sub->get_select_lex()->join;
				if (join)
				{
					Item_cond* cond = reinterpret_cast<Item_cond*>(join->conds);
					if (cond)
						cond->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
				}
				cout << "Finish SELECT clause subselect item traversing" << endl;
#endif
				SelectSubQuery *selectSub = new SelectSubQuery(gwi, sub);
				//selectSub->gwip(&gwi);
				SCSEP ssub = selectSub->transform();
				if (!ssub || gwi.fatalParseError)
				{
					if (gwi.parseErrorText.empty())
						gwi.parseErrorText = "Unsupported Item in SELECT subquery.";
					setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
					return HA_ERR_UNSUPPORTED;
				}
				selectSubList.push_back(ssub);
				SimpleColumn* rc = new SimpleColumn();
				rc->colSource(rc->colSource() | SELECT_SUB);

				if (sub->get_select_lex()->get_table_list())
					rc->viewName(lower(getViewName(sub->get_select_lex()->get_table_list())));
				if (sub->name)
					rc->alias(sub->name);

				gwi.returnedCols.push_back(SRCP(rc));
				String str;
				sub->get_select_lex()->print(gwi.thd, &str, QT_INFINIDB_NO_QUOTE);
				sel_cols_in_create += "(" + string(str.c_ptr()) + ")";
				if (sub->name)
				{
					sel_cols_in_create += "`" + escapeBackTick(sub->name) + "`";
					gwi.selectCols.push_back(sub->name);
				}
				else
				{
					sel_cols_in_create += "`" + escapeBackTick(str.c_ptr()) + "`";
					gwi.selectCols.push_back("`" + escapeBackTick(str.c_ptr()) + "`");
				}
				break;
			}
			case Item::COND_ITEM:
			{
				gwi.fatalParseError = true;
				gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
				setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
				return HA_ERR_UNSUPPORTED;
			}
			case Item::WINDOW_FUNC_ITEM:
			{
				SRCP srcp(buildWindowFunctionColumn(item, gwi, gwi.fatalParseError));
				if (!srcp || gwi.fatalParseError)
				{
					if (gwi.parseErrorText.empty())
						gwi.parseErrorText = "Unsupported Item in SELECT subquery.";
					setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
					return HA_ERR_UNSUPPORTED;
				}
				gwi.returnedCols.push_back(srcp);
				break;
			}
			default:
			{
				break;
			}
		}
	}

	// @bug4388 normalize the project coltypes for union main select list
	if (!csep->unionVec().empty())
	{
		for (uint i = 0; i < gwi.returnedCols.size(); i++)
		{
			vector<CalpontSystemCatalog::ColType> coltypes;
			for (uint j = 0; j < csep->unionVec().size(); j++)
			{
				coltypes.push_back(
				   dynamic_cast<CalpontSelectExecutionPlan*>(csep->unionVec()[j].get())->returnedCols()[i]->resultType());
			}
			gwi.returnedCols[i]->resultType(dataconvert::DataConvert::convertUnionColType(coltypes));
		}
	}

	// Having clause handling
	gwi.clauseType = HAVING;
	clearStacks(gwi);
	ParseTree* havingFilter = 0;
	// clear fatalParseError that may be left from post process functions
	gwi.fatalParseError = false;
	gwi.parseErrorText = "";

	if (select_lex.having != 0)
	{
		Item_cond* having = reinterpret_cast<Item_cond*>(select_lex.having);
#ifdef DEBUG_WALK_COND
		cout << "------------------- HAVING ---------------------" << endl;
		having->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
		cout << "------------------------------------------------\n" << endl;
#endif
		having->traverse_cond(gp_walk, &gwi, Item::POSTFIX);
		if (gwi.fatalParseError)
		{
			setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
			return HA_ERR_UNSUPPORTED;
		}

		ParseTree* ptp = 0;
		ParseTree* rhs = 0;

		// @bug 4215. some function filter will be in the rcWorkStack.
		if (gwi.ptWorkStack.empty() && !gwi.rcWorkStack.empty())
		{
			havingFilter = new ParseTree(gwi.rcWorkStack.top());
			gwi.rcWorkStack.pop();
		}

		while (!gwi.ptWorkStack.empty())
		{
			havingFilter = gwi.ptWorkStack.top();
			gwi.ptWorkStack.pop();
			if (gwi.ptWorkStack.empty())
				break;
			ptp = new ParseTree(new LogicOperator("and"));
			ptp->left(havingFilter);
			rhs = gwi.ptWorkStack.top();
			gwi.ptWorkStack.pop();
			ptp->right(rhs);
			gwi.ptWorkStack.push(ptp);
		}
	}

	// for post process expressions on the select list
	// error out post process for union and sub select unit
	if (isUnion || gwi.subSelectType != CalpontSelectExecutionPlan::MAIN_SELECT)
	{
		if (funcFieldVec.size() != 0 && !gwi.fatalParseError)
		{
			string emsg("Fatal parse error in vtable mode: Unsupported Items in union or sub select unit");
			setError(gwi.thd, HA_ERR_UNSUPPORTED, emsg);
			return HA_ERR_UNSUPPORTED;
		}
	}

	for (uint i = 0; i < funcFieldVec.size(); i++)
	{
		//SimpleColumn *sc = new SimpleColumn(funcFieldVec[i]->db_name, bestTableName(funcFieldVec[i])/*funcFieldVec[i]->table_name*/, funcFieldVec[i]->field_name, sessionID);
		SimpleColumn *sc = buildSimpleColumn(funcFieldVec[i], gwi);
		if (!sc || gwi.fatalParseError)
		{
			string emsg;
			if (gwi.parseErrorText.empty())
			{
				emsg = "un-recognized column";
				if (funcFieldVec[i]->name)
					emsg += string(funcFieldVec[i]->name);
			}
			else
			{
				emsg = gwi.parseErrorText;
			}
			setError(gwi.thd, HA_ERR_UNSUPPORTED, emsg);
			return HA_ERR_UNSUPPORTED;
		}
		String str;
		funcFieldVec[i]->print(&str, QT_INFINIDB_NO_QUOTE);
		sc->alias(string(str.c_ptr()));
		//sc->tableAlias(funcFieldVec[i]->table_name);
		sc->tableAlias(sc->tableAlias());
		SRCP srcp(sc);
		uint j = 0;
		for (; j < gwi.returnedCols.size(); j++)
		{
			if (sc->sameColumn(gwi.returnedCols[j].get()))
			{
				SimpleColumn *field = dynamic_cast<SimpleColumn*>(gwi.returnedCols[j].get());
				if (field && field->alias() == sc->alias())
					break;
			}
		}
		if (j == gwi.returnedCols.size())
		{
			gwi.returnedCols.push_back(srcp);
			gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(string(funcFieldVec[i]->field_name), srcp));
			if (sel_cols_in_create.length() != 0)
				sel_cols_in_create += ", ";
			string fullname;
			fullname = str.c_ptr();
			sel_cols_in_create += fullname + " `" + escapeBackTick(fullname.c_str()) + "`";
			TABLE_LIST* tmp = (funcFieldVec[i]->cached_table ? funcFieldVec[i]->cached_table : 0);
			gwi.tableMap[make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias(), sc->isInfiniDB())] =
			             make_pair(1, tmp);
		}
	}

	// post-process Order by list and expressions on select by redo phase1. only for vtable
	// ignore ORDER BY clause for union select unit
	string ord_cols = "";   // for normal select phase
	SRCP minSc;             // min width projected column. for count(*) use

	// Group by list. not valid for union main query
	if (gwi.thd->infinidb_vtable.vtable_state == THD::INFINIDB_CREATE_VTABLE && !unionSel)
	{
		gwi.clauseType = GROUP_BY;
		Item* nonSupportItem = NULL;
		ORDER* groupcol = reinterpret_cast<ORDER*>(select_lex.group_list.first);

		// check if window functions are in order by. InfiniDB process order by list if
		// window functions are involved, either in order by or projection.
		bool hasWindowFunc = gwi.hasWindowFunc;
		gwi.hasWindowFunc = false;
		for (; groupcol; groupcol= groupcol->next)
		{
			if ((*(groupcol->item))->type() == Item::WINDOW_FUNC_ITEM)
				gwi.hasWindowFunc = true;
		}
		if (gwi.hasWindowFunc)
		{
			gwi.fatalParseError = true;
			gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_WF_NOT_ALLOWED, "GROUP BY clause");
			setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
			return HA_ERR_UNSUPPORTED;
		}
		gwi.hasWindowFunc = hasWindowFunc;

		groupcol = reinterpret_cast<ORDER*>(select_lex.group_list.first);

		for (; groupcol; groupcol= groupcol->next)
		{
			Item* groupItem = *(groupcol->item);
			if (groupItem->type() == Item::REF_ITEM)
				groupItem = (*((Item_ref*)groupItem)->ref);

			if (groupItem->type() == Item::FUNC_ITEM)
			{
				Item_func* ifp = (Item_func*)groupItem;
				ReturnedColumn *fc = buildFunctionColumn(ifp, gwi, gwi.fatalParseError);
				if (!fc || gwi.fatalParseError)
				{
					nonSupportItem = ifp;
					break;
				}
				if (groupcol->in_field_list && groupcol->counter_used)
				{
					fc = gwi.returnedCols[groupcol->counter-1].get();
					SRCP srcp(gwi.returnedCols[groupcol->counter-1]->clone());
					// check if no column parm
					for (uint i = 0; i < gwi.no_parm_func_list.size(); i++)
					{
						if (gwi.no_parm_func_list[i]->expressionId() == fc->expressionId())
						{
							gwi.no_parm_func_list.push_back(dynamic_cast<FunctionColumn*>(srcp.get()));
							break;
						}
					}

					srcp->orderPos(groupcol->counter-1);
					gwi.groupByCols.push_back(srcp);
					continue;
				}
				else if (!groupItem->is_autogenerated_name) // alias
				{
					uint i = 0;
					for (; i < gwi.returnedCols.size(); i++)
					{
						if (string(groupItem->name) == gwi.returnedCols[i]->alias())
						{
							ReturnedColumn *rc = gwi.returnedCols[i]->clone();
							rc->orderPos(i);
							gwi.groupByCols.push_back(SRCP(rc));
							break;
						}
					}
					if (i == gwi.returnedCols.size())
					{
						nonSupportItem = groupItem;
						break;
					}
				}
				else
				{
					uint i = 0;
					for (; i < gwi.returnedCols.size(); i++)
					{
						if (fc->operator==(gwi.returnedCols[i].get()))
						{
							ReturnedColumn *rc = gwi.returnedCols[i]->clone();
							rc->orderPos(i);
							gwi.groupByCols.push_back(SRCP(rc));
							break;
						}
					}
					if (i == gwi.returnedCols.size())
					{
						gwi.groupByCols.push_back(SRCP(fc));
						break;
					}
				}
			}
			else if (groupItem->type() == Item::FIELD_ITEM)
			{
				Item_field* ifp = (Item_field*)groupItem;
				// this GB col could be an alias of F&E on the SELECT clause, not necessarily a field.
				ReturnedColumn *rc = buildSimpleColumn(ifp, gwi);
				SimpleColumn* sc = dynamic_cast<SimpleColumn*>(rc);

				for (uint j = 0; j < gwi.returnedCols.size(); j++)
				{
					if (sc)
					{
						if (sc->sameColumn(gwi.returnedCols[j].get()))
						{
							sc->orderPos(j);
							break;
						}
						else if (strcasecmp(sc->alias().c_str(), gwi.returnedCols[j]->alias().c_str()) == 0)
						{
							rc = gwi.returnedCols[j].get()->clone();
							rc->orderPos(j);
							break;
						}
					}
					else
					{
						if (ifp->name && string(ifp->name) == gwi.returnedCols[j].get()->alias())
						{
							rc = gwi.returnedCols[j].get()->clone();
							rc->orderPos(j);
							break;
						}
					}
				}
				if (!rc)
				{
					nonSupportItem = ifp;
					break;
				}
				SRCP srcp(rc);

				// bug 3151
				AggregateColumn* ac = dynamic_cast<AggregateColumn*>(rc);
				if (ac)
				{
					nonSupportItem = ifp;
					break;
				}
				gwi.groupByCols.push_back(srcp);
				gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name), srcp));
			}
			// @bug5638. The group by column is constant but not couter, alias has to match a column
			// on the select list
			else if (!groupcol->counter_used &&
			         (groupItem->type() == Item::INT_ITEM ||
			          groupItem->type() == Item::STRING_ITEM ||
			          groupItem->type() == Item::REAL_ITEM ||
			          groupItem->type() == Item::DECIMAL_ITEM))
			{
				ReturnedColumn* rc = 0;
				for (uint j = 0; j < gwi.returnedCols.size(); j++)
				{
					if (groupItem->name && string(groupItem->name) == gwi.returnedCols[j].get()->alias())
					{
						rc = gwi.returnedCols[j].get()->clone();
						rc->orderPos(j);
						break;
					}
				}
				if (!rc)
				{
					nonSupportItem = groupItem;
					break;
				}
				gwi.groupByCols.push_back(SRCP(rc));
			}
			else if ((*(groupcol->item))->type() == Item::SUBSELECT_ITEM)
			{
				if (!groupcol->in_field_list || !groupItem->name)
				{
					nonSupportItem = groupItem;
				}
				else
				{
					uint i = 0;
					for (; i < gwi.returnedCols.size(); i++)
					{
						if (string(groupItem->name) == gwi.returnedCols[i]->alias())
						{
							ReturnedColumn *rc = gwi.returnedCols[i]->clone();
							rc->orderPos(i);
							gwi.groupByCols.push_back(SRCP(rc));
							break;
						}
					}
					if (i == gwi.returnedCols.size())
					{
						nonSupportItem = groupItem;
					}
				}
			}
			// @bug 3761.
			else if (groupcol->counter_used)
			{
				if (gwi.returnedCols.size() <= (uint)(groupcol->counter-1))
				{
					nonSupportItem = groupItem;
				}
				else
				{
					gwi.groupByCols.push_back(SRCP(gwi.returnedCols[groupcol->counter-1]->clone()));
				}
			}
			else
			{
				nonSupportItem = groupItem;
			}

		}

		// @bug 4756. Add internal groupby column for correlated join to the groupby list
		if (gwi.aggOnSelect && !gwi.subGroupByCols.empty())
			gwi.groupByCols.insert(gwi.groupByCols.end(), gwi.subGroupByCols.begin(), gwi.subGroupByCols.end());

		// this is window func on SELECT becuase ORDER BY has not been processed
		if (!gwi.windowFuncList.empty() && !gwi.subGroupByCols.empty())
		{
			for (uint i = 0; i < gwi.windowFuncList.size(); i++)
			{
				if (gwi.windowFuncList[i]->hasWindowFunc())
				{
					vector<WindowFunctionColumn*> windowFunctions = gwi.windowFuncList[i]->windowfunctionColumnList();
					for (uint j = 0; j < windowFunctions.size(); j++)
						windowFunctions[j]->addToPartition(gwi.subGroupByCols);
				}
			}
		}

		if (nonSupportItem)
		{
			Message::Args args;
			if (nonSupportItem->name)
				args.add("'" + string(nonSupportItem->name) + "'");
			else
				args.add("");
			gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_GROUP_BY, args);
			setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
			return HA_ERR_UNSUPPORTED;
		}
	}

	if (gwi.thd->infinidb_vtable.vtable_state == THD::INFINIDB_CREATE_VTABLE)
	{
		SQL_LIST order_list = select_lex.order_list;
		ORDER* ordercol = reinterpret_cast<ORDER*>(order_list.first);
		string create_query(gwi.thd->infinidb_vtable.create_vtable_query.c_ptr());
		string select_query(gwi.thd->infinidb_vtable.select_vtable_query.c_ptr());
		string lower_create_query(gwi.thd->infinidb_vtable.create_vtable_query.c_ptr());
		string lower_select_query(gwi.thd->infinidb_vtable.select_vtable_query.c_ptr());
		algorithm::to_lower(lower_create_query);
		algorithm::to_lower(lower_select_query);


		// check if window functions are in order by. InfiniDB process order by list if
		// window functions are involved, either in order by or projection.
		for (; ordercol; ordercol= ordercol->next)
		{
			if ((*(ordercol->item))->type() == Item::WINDOW_FUNC_ITEM)
				gwi.hasWindowFunc = true;
		}

		// re-visit the first of ordercol list
		ordercol = reinterpret_cast<ORDER*>(order_list.first);

		// for subquery, order+limit by will be supported in infinidb. build order by columns
		// @todo union order by and limit support
		if (gwi.hasWindowFunc || gwi.subSelectType != CalpontSelectExecutionPlan::MAIN_SELECT)
		{
			for (; ordercol; ordercol= ordercol->next)
			{
				ReturnedColumn* rc = NULL;
				if (ordercol->in_field_list && ordercol->counter_used)
				{
					rc = gwi.returnedCols[ordercol->counter-1]->clone();
					rc->orderPos(ordercol->counter-1);
				}
				else
				{
					Item* ord_item = *(ordercol->item);
					if (ord_item->type() == Item::INT_ITEM)
						rc = gwi.returnedCols[((Item_int*)ord_item)->val_int()-1]->clone();
					else if (ord_item->type() == Item::SUBSELECT_ITEM)
						gwi.fatalParseError = true;
					else
						rc = buildReturnedColumn(ord_item, gwi, gwi.fatalParseError);
					if (!rc || gwi.fatalParseError)
					{
						string emsg = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_ORDER_BY);
						gwi.parseErrorText = emsg;
						setError(gwi.thd, HA_ERR_UNSUPPORTED, emsg);
						return HA_ERR_UNSUPPORTED;
					}
				}
				if (ordercol->asc)
 					rc->asc(true);
				else
					rc->asc(false);
				gwi.orderByCols.push_back(SRCP(rc));
			}
		}
		else if (!isUnion)
		{
			vector <Item_field*> fieldVec;
			bool addToSel;

			// the following order by is just for redo phase
			if (!unionSel)
			{
				for (; ordercol; ordercol= ordercol->next)
				{
					Item* ord_item = *(ordercol->item);
					if (ord_item->type() == Item::REF_ITEM)
						ord_item = (*((Item_ref*)ord_item)->ref);
					// @bug 1706. re-construct the order by item one by one
					//Item* ord_item = *(ordercol->item);
					if (ord_cols.length() != 0)
						ord_cols += ", ";
					addToSel = true;
					string fullname;

					if (ordercol->in_field_list && ordercol->counter_used)
					{
						ostringstream oss;
						oss << ordercol->counter;
						ord_cols += oss.str();
						if (!ordercol->asc)
							ord_cols += " desc";
						continue;
					}

					else if (ord_item->type() == Item::FUNC_ITEM)
					{
						// @bug 2621. order by alias
						if (!ord_item->is_autogenerated_name && ord_item->name)
						{
							ord_cols += ord_item->name;
							continue;
						}
						// if there's group by clause or aggregate column, check to see
						// if this item or the arguments is on the GB list.
						ReturnedColumn *rc = 0;
						// check if this order by column is on the select list
						Item_func* ifp = (Item_func*)(*(ordercol->item));
						rc = buildFunctionColumn(ifp, gwi, gwi.fatalParseError);
						if (rc)
						{
							for (uint i = 0; i < gwi.returnedCols.size(); i++)
							{
								if (rc && rc->operator==(gwi.returnedCols[i].get()))
								{
									ostringstream oss;
									oss << i+1;
									ord_cols += oss.str();
									addToSel = false;
									break;
								}
							}
						}
						if (addToSel)
						{
							FunctionColumn *fc = dynamic_cast<FunctionColumn*>(rc);
							if (fc)
							{
								addToSel = false;
								redo = true;
								string ord_func = string(ifp->func_name()) + "(";
								for (uint i = 0; i < fc->functionParms().size(); i++)
								{
									if (i != 0)
										ord_func += ",";
									for (uint j = 0; j < gwi.returnedCols.size(); j++)
									{
										if (fc->functionParms()[i]->data()->operator==(gwi.returnedCols[j].get()))
										{
											ord_func += "`" + escapeBackTick(gwi.returnedCols[j]->alias().c_str()) + "`";
											continue;
										}

											AggregateColumn *ac = dynamic_cast<AggregateColumn*>(fc->functionParms()[i]->data());
											if (ac)
											{
												gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_ORDER_BY);
												setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
												return HA_ERR_UNSUPPORTED;
											}
											addToSel = true;
											//continue;

									}
								}
								ord_func += ")";
								if (!addToSel)
									ord_cols += ord_func;
							}
						}
					}
					else if (ord_item->type() == Item::SUBSELECT_ITEM)
					{
						string emsg = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_ORDER_BY);
						setError(gwi.thd, HA_ERR_UNSUPPORTED, emsg);
						return HA_ERR_UNSUPPORTED;
					}

					else if (ord_item->type() == Item::SUM_FUNC_ITEM)
					{
						ReturnedColumn *ac = 0;

						Item_sum* ifp = (Item_sum*)(*(ordercol->item));
						// @bug3477. add aggregate column to the select list of the create phase.
						ac = buildAggregateColumn(ifp, gwi);
						if (!ac)
						{
							setError(gwi.thd, HA_ERR_UNSUPPORTED,
							         IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_ORDER_BY));
							return HA_ERR_UNSUPPORTED;
						}
						// check if this order by column is on the select list
						for (uint i = 0; i < gwi.returnedCols.size(); i++)
						{
							AggregateColumn *ret = dynamic_cast<AggregateColumn*>(gwi.returnedCols[i].get());
							if (!ret)
								continue;

							if (ac->operator==(gwi.returnedCols[i].get()))
							{
								ostringstream oss;
								oss << i+1;
								ord_cols += oss.str();
								addToSel = false;
								break;
							}
						}

						if (ac || !gwi.groupByCols.empty())
						{
							if (addToSel)
							{
								redo = true;
								// @bug 3076. do not add the argument of aggregate function to the SELECT list,
								// instead, add the whole column
								String str;
								ord_item->print(&str, QT_INFINIDB_NO_QUOTE);
								if (sel_cols_in_create.length() != 0)
									sel_cols_in_create += ", ";
								sel_cols_in_create += str.c_ptr();
								//gwi.selectCols.push_back(" `" + string(str.c_ptr()) + "`");
								SRCP srcp(ac);
								gwi.returnedCols.push_back(srcp);
								ord_cols += " `" + escapeBackTick(str.c_ptr()) + "`";
							}
							if (!ordercol->asc)
								ord_cols += " desc";
							continue;
						}
					}
					else if (ord_item->name && ord_item->type() == Item::FIELD_ITEM)
					{
						Item_field *field = reinterpret_cast<Item_field*>(ord_item);
						ReturnedColumn *rc = buildSimpleColumn(field, gwi);
						if (field->db_name)
							fullname += string(field->db_name) + ".";
						if (field->table_name)
							fullname += string(field->table_name) + ".";
						if (field->field_name)
							fullname += string(field->field_name);

							for (uint i = 0; i < gwi.returnedCols.size(); i++)
							{
								SimpleColumn* sc = dynamic_cast<SimpleColumn*>(gwi.returnedCols[i].get());
								if (sc && ((Item_field*)ord_item)->cached_table &&
										(strcasecmp(getViewName(((Item_field*)ord_item)->cached_table).c_str(), sc->viewName().c_str()) != 0))
									continue;
								if (strcasecmp(fullname.c_str(),gwi.returnedCols[i]->alias().c_str()) == 0 ||
									  strcasecmp(ord_item->name,gwi.returnedCols[i]->alias().c_str()) == 0)
								{
									ord_cols += string(" `") + escapeBackTick(gwi.returnedCols[i]->alias().c_str()) + '`';
									addToSel = false;
									break;
								}
								if (sc && sc->sameColumn(rc))
								{
									ostringstream oss;
									oss << i+1;
									ord_cols += oss.str();
									addToSel = false;
									break;
								}
							}
					}
					if (addToSel)
					{
						// @bug 2719. Error out order by not on the distinct select list.
						if (select_lex.options & SELECT_DISTINCT)
						{
							gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_ORDERBY_NOT_IN_DISTINCT);
							setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
							return HA_ERR_UNSUPPORTED;
						}
						bool hasNonSupportItem = false;
						uint16_t parseInfo = 0;
						parse_item(ord_item, fieldVec, hasNonSupportItem, parseInfo);
						if (hasNonSupportItem)
						{
							string emsg = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_ORDER_BY);
							setError(gwi.thd, HA_ERR_UNSUPPORTED, emsg);
							return HA_ERR_UNSUPPORTED;
						}
						String str;
						ord_item->print(&str, QT_INFINIDB);
						ord_cols += str.c_ptr();
					}
					if (!ordercol->asc)
						ord_cols += " desc";
				}
			}

			// populate string to be added to the select list for order by
			redo = (redo || fieldVec.size() != 0);
			for (uint i = 0; i < fieldVec.size(); i++)
			{
				SimpleColumn* sc = buildSimpleColumn(fieldVec[i], gwi);
				if (!sc)
				{
					string emsg = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_ORDER_BY);
					setError(gwi.thd, HA_ERR_UNSUPPORTED, emsg);
					return HA_ERR_UNSUPPORTED;
				}
				String str;
				fieldVec[i]->print(&str, QT_INFINIDB_NO_QUOTE);
				sc->alias(string(str.c_ptr()));
				SRCP srcp(sc);
				uint j = 0;
				for (; j < gwi.returnedCols.size(); j++)
				{
					if (sc->sameColumn(gwi.returnedCols[j].get()))
					{
						SimpleColumn *field = dynamic_cast<SimpleColumn*>(gwi.returnedCols[j].get());
						if (field && field->alias() == sc->alias())
							break;
					}
				}
				if (j == gwi.returnedCols.size())
				{
					string fullname;
					if (sel_cols_in_create.length() != 0)
							sel_cols_in_create += ", ";

					fullname = str.c_ptr();
					sel_cols_in_create += fullname + " `" + escapeBackTick(fullname.c_str()) + "`";

					gwi.returnedCols.push_back(srcp);
					gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(string(fieldVec[i]->field_name), srcp));
					TABLE_LIST* tmp = (fieldVec[i]->cached_table ? fieldVec[i]->cached_table : 0);
					gwi.tableMap[make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias(), sc->isInfiniDB())] =
					             make_pair(1, tmp);
				}
			}
		}

		// make sure columnmap, returnedcols and count(*) arg_list are not empty
		TableMap::iterator tb_iter = gwi.tableMap.begin();
		try {
				for (; tb_iter != gwi.tableMap.end(); tb_iter++)
				{
					if ((*tb_iter).second.first == 1) continue;
					CalpontSystemCatalog::TableAliasName tan = (*tb_iter).first;
					CalpontSystemCatalog::TableName tn = make_table((*tb_iter).first.schema, (*tb_iter).first.table);
					SimpleColumn *sc = getSmallestColumn(csc, tn, tan, (*tb_iter).second.second->table, gwi);
					SRCP srcp(sc);
					gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(sc->columnName(), srcp));
					(*tb_iter).second.first = 1;
				}
		}
		catch (runtime_error& e)
		{
			setError(gwi.thd, HA_ERR_INTERNAL_ERROR, e.what());
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			return HA_ERR_INTERNAL_ERROR;
		}
		catch (...)
		{
			string emsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);
			setError(gwi.thd, HA_ERR_INTERNAL_ERROR, emsg);
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			return HA_ERR_INTERNAL_ERROR;
		}

		if (!gwi.count_asterisk_list.empty() || !gwi.no_parm_func_list.empty() || gwi.returnedCols.empty())
		{
			// get the smallest column from colmap
			CalpontSelectExecutionPlan::ColumnMap::const_iterator iter;
			int minColWidth = 0;
			CalpontSystemCatalog::ColType ct;
			try {
				for (iter = gwi.columnMap.begin(); iter != gwi.columnMap.end(); ++iter)
				{
					// should always not null
					SimpleColumn *sc = dynamic_cast<SimpleColumn*>(iter->second.get());
					if (sc && !(sc->joinInfo() & JOIN_CORRELATED))
					{
						ct = csc->colType(sc->oid());

						if (minColWidth == 0)
						{
							minColWidth = ct.colWidth;
							minSc = iter->second;
						}
						else if (ct.colWidth < minColWidth)
						{
							minColWidth = ct.colWidth;
							minSc = iter->second;
						}
					}
				}
			} catch (...)
			{
				string emsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);
				setError(gwi.thd, HA_ERR_INTERNAL_ERROR, emsg);
				CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
				return HA_ERR_INTERNAL_ERROR;
			}

			if (gwi.returnedCols.empty())
				gwi.returnedCols.push_back(minSc);
		}

		if (!isUnion && !gwi.hasWindowFunc && gwi.subSelectType == CalpontSelectExecutionPlan::MAIN_SELECT)
		{
			std::ostringstream vtb;
		  vtb << "infinidb_vtable.$vtable_" << gwi.thd->thread_id;
		  //vtb << "$vtable_" << gwi.thd->thread_id;
			// re-construct the select query and redo phase 1
			if (redo)
			{
				// select now() from region case. returnedCols should have minSc.
				if (sel_cols_in_create.length() == 0)
				{
					SimpleColumn *sc = dynamic_cast<SimpleColumn*>(gwi.returnedCols[0].get());
					if (sc)
						sel_cols_in_create = dynamic_cast<SimpleColumn*>(gwi.returnedCols[0].get())->columnName();
					else
						sel_cols_in_create = gwi.returnedCols[0]->alias();
				}
				// select * from derived table case
				if (gwi.selectCols.empty())
					sel_cols_in_create = " * ";
				create_query = "create temporary table " + vtb.str() + " as select " + sel_cols_in_create + " from ";
				TABLE_LIST* table_ptr = select_lex.get_table_list();

				bool firstTb = true;

				// put all tables, derived tables and views on the list
				//TABLE_LIST* table_ptr = select_lex.get_table_list();
				set<string> aliasSet; // to avoid duplicate table alias
				for (; table_ptr; table_ptr= table_ptr->next_global)
				{
					if (string(table_ptr->table_name).find("$vtable") != string::npos)
						continue;

					if (table_ptr->derived)
					{
						if (aliasSet.find(table_ptr->alias) != aliasSet.end())
							continue;
						String str;
						(table_ptr->derived->first_select())->print(gwi.thd, &str, QT_INFINIDB_NO_QUOTE);
						if (!firstTb)
							create_query += ", ";
						create_query += "(" + string(str.c_ptr()) +") " + string(table_ptr->alias);
						firstTb = false;
						aliasSet.insert(table_ptr->alias);
					}
					else if (table_ptr->view)
					{
						if (aliasSet.find(table_ptr->alias) != aliasSet.end())
							continue;
						if (!firstTb)
							create_query += ", ";
						create_query += string(table_ptr->db) + "." + string(table_ptr->table_name) +
						                string(" `") + escapeBackTick(table_ptr->alias) + string("`");
						aliasSet.insert(table_ptr->alias);
						firstTb = false;
					}
					else
					{
						// table referenced by view is represented by viewAlias_tableAlias.
						// consistent with item.cc field print.
						if (table_ptr->referencing_view)
						{
							if (aliasSet.find(string(table_ptr->referencing_view->alias) + "_" +
							                string(table_ptr->alias)) != aliasSet.end())
								continue;
							if (!firstTb)
								create_query += ", ";
							create_query += string(table_ptr->db) + "." + string(table_ptr->table_name) + string(" ");
							create_query += string(" `") +
							                escapeBackTick(table_ptr->referencing_view->alias) + "_" +
							                escapeBackTick(table_ptr->alias) + string("`");
							aliasSet.insert(string(table_ptr->referencing_view->alias) + "_" +
							                string(table_ptr->alias));
						}
						else
						{
							if (aliasSet.find(table_ptr->alias) != aliasSet.end())
								continue;
							if (!firstTb)
								create_query += ", ";
							create_query += string(table_ptr->db) + "." + string(table_ptr->table_name) + string(" ");
							create_query += string("`") + escapeBackTick(table_ptr->alias) + string("`");
							aliasSet.insert(table_ptr->alias);
						}
						firstTb = false;
					}
				}


				gwi.thd->infinidb_vtable.create_vtable_query.free();
				gwi.thd->infinidb_vtable.create_vtable_query.append(create_query.c_str(), create_query.length());
				gwi.thd->infinidb_vtable.vtable_state = THD::INFINIDB_REDO_PHASE1;	// redo phase 1

				// turn off select distinct from post process unless there're post process functions
				// on the select list.
				string sel_query = "select ";
				if (/*join->select_options*/select_lex.options & SELECT_DISTINCT && redo)
					sel_query = "select distinct ";
				else
					sel_query = "select ";

				// select * from derived table...
				if (gwi.selectCols.size() == 0)
					sel_query += " * ";
				for (uint i = 0; i < gwi.selectCols.size(); i++)
				{
					sel_query += gwi.selectCols[i];
					if ( i+1 != gwi.selectCols.size())
						sel_query += ", ";
				}
				select_query.replace(lower_select_query.find("select *"), string("select *").length(), sel_query);
			}
			else
			{
				// remove order by clause in case this phase has been executed before.
				// need a better fix later, like skip all the other non-optimized phase.
				size_t pos = lower_select_query.find("order by");
				if (pos != string::npos)
					select_query.replace(pos, lower_select_query.length()-pos, "");
				//select_query = "select * from " + vtb.str();
				if (unionSel)
					order_list = select_lex.master_unit()->global_parameters->order_list;
				ordercol = reinterpret_cast<ORDER*>(order_list.first);
				ord_cols = "";
				for (; ordercol; ordercol= ordercol->next)
				{
					Item* ord_item = *(ordercol->item);
					// @bug 1706. re-construct the order by item one by one, because the ord_cols constucted so far
					// is for REDO phase.
					if (ord_cols.length() != 0)
						ord_cols += ", ";
					if (ordercol->in_field_list && ordercol->counter_used)
					{
						ostringstream oss;
						oss << ordercol->counter;
						ord_cols += oss.str();
					}
					else if (ord_item->type() == Item::SUM_FUNC_ITEM)
					{
						Item_sum* ifp = (Item_sum*)(*(ordercol->item));
						ReturnedColumn *fc = buildAggregateColumn(ifp, gwi);
						for (uint i = 0; i < gwi.returnedCols.size(); i++)
						{
							if (fc->operator==(gwi.returnedCols[i].get()))
							{
								ostringstream oss;
								oss << i+1;
								ord_cols += oss.str();
								break;
							}
						}
						//continue;
					}
					// @bug 3518. if order by clause = selected column, use position.
					else if (ord_item->name && ord_item->type() == Item::FIELD_ITEM)
					{
						Item_field *field = reinterpret_cast<Item_field*>(ord_item);
						string fullname;
						if (field->db_name)
							fullname += string(field->db_name) + ".";
						if (field->table_name)
							fullname += string(field->table_name) + ".";
						if (field->field_name)
							fullname += string(field->field_name);

						uint32_t i = 0;
						for (i = 0; i < gwi.returnedCols.size(); i++)
						{
							SimpleColumn* sc = dynamic_cast<SimpleColumn*>(gwi.returnedCols[i].get());
							if (sc && ((Item_field*)ord_item)->cached_table &&
									(strcasecmp(getViewName(((Item_field*)ord_item)->cached_table).c_str(), sc->viewName().c_str()) != 0))
								continue;
							if (strcasecmp(fullname.c_str(),gwi.returnedCols[i]->alias().c_str()) == 0 ||
								  strcasecmp(ord_item->name,gwi.returnedCols[i]->alias().c_str()) == 0)
							{
								ostringstream oss;
								oss << i+1;
								ord_cols += oss.str();
								break;
							}
						}
						if (i == gwi.returnedCols.size())
							ord_cols += string(" `") + escapeBackTick(ord_item->name) + '`';
					}

					else if (ord_item->name)
					{
						// for union order by 1 case. For unknown reason, it doesn't show in_field_list
						if (ord_item->type() == Item::INT_ITEM)
						{
							ord_cols += ord_item->name;
						}
						else if (ord_item->type() == Item::SUBSELECT_ITEM)
						{
							string emsg = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_ORDER_BY);
							setError(gwi.thd, HA_ERR_UNSUPPORTED, emsg);
							return HA_ERR_UNSUPPORTED;
						}
						else
						{
							ord_cols += string(" `") + escapeBackTick(ord_item->name) + '`';
						}
					}
					else if (ord_item->type() == Item::FUNC_ITEM)
					{
						// @bug5636. check if this order by column is on the select list
						ReturnedColumn* rc = buildFunctionColumn((Item_func*)(ord_item), gwi, gwi.fatalParseError);
						for (uint32_t i = 0; i < gwi.returnedCols.size(); i++)
						{
							if (rc && rc->operator==(gwi.returnedCols[i].get()))
							{
								ostringstream oss;
								oss << i+1;
								ord_cols += oss.str();
								break;
							}
						}
					}
					else
					{
						String str;
						ord_item->print(&str, QT_INFINIDB_NO_QUOTE);
						ord_cols += string(str.c_ptr());
					}
					if (!ordercol->asc)
						ord_cols += " desc";
				}
			}

			if (ord_cols.length() > 0)	// has order by
			{
				gwi.thd->infinidb_vtable.has_order_by = true;
				csep->hasOrderBy(true);
				ord_cols = " order by " + ord_cols;
				select_query += ord_cols;
			}
		}

		// check limit clause
		if (gwi.thd->variables.select_limit != (uint64_t)-1)
			gwi.thd->infinidb_vtable.has_limit = true;

		if (unionSel || gwi.subSelectType != CalpontSelectExecutionPlan::MAIN_SELECT)
		{
			if (select_lex.master_unit()->global_parameters->explicit_limit)
			{
				if (select_lex.master_unit()->global_parameters->offset_limit)
				{
					Item_int* offset = (Item_int*)select_lex.master_unit()->global_parameters->offset_limit;
					csep->limitStart(offset->val_int());
				}
				if (select_lex.master_unit()->global_parameters->select_limit)
				{
					Item_int* select = (Item_int*)select_lex.master_unit()->global_parameters->select_limit;
					csep->limitNum(select->val_int());
				}
				if (unionSel && gwi.subSelectType == CalpontSelectExecutionPlan::MAIN_SELECT)
				{
					ostringstream limit;
					limit << " limit ";
					limit << csep->limitStart() << ", ";
					limit << csep->limitNum();
					select_query += limit.str();
					gwi.thd->infinidb_vtable.has_limit = true;
				}
			}
		}
		else if (isUnion && select_lex.explicit_limit)
		{
			if (select_lex.braces)
			{
				if (select_lex.offset_limit)
					csep->limitStart(((Item_int*)select_lex.offset_limit)->val_int());
				if (select_lex.select_limit)
					csep->limitNum(((Item_int*)select_lex.select_limit)->val_int());
			}
		}
		else if (select_lex.explicit_limit)
		{
			uint limitOffset = 0, limitNum = -1;
			if (join)
			{
				limitOffset = (join->unit)->offset_limit_cnt;
				limitNum = (join->unit)->select_limit_cnt - (join->unit)->offset_limit_cnt;
			}
			else
			{
				if (select_lex.master_unit()->global_parameters->offset_limit)
				{
					Item_int* offset = (Item_int*)select_lex.master_unit()->global_parameters->offset_limit;
					limitOffset = offset->val_int();
				}
				if (select_lex.master_unit()->global_parameters->select_limit)
				{
					Item_int* select = (Item_int*)select_lex.master_unit()->global_parameters->select_limit;
					limitNum = select->val_int();
				}
			}

			// relate to bug4848. let mysql drive limit when limit session variable set.
			// do not set in csep. @bug5096. ignore session limit setting for dml
			if ((gwi.thd->variables.select_limit == (uint64_t)-1 ||
				  (gwi.thd->variables.select_limit != (uint64_t)-1 &&
				  gwi.thd->infinidb_vtable.vtable_state != THD::INFINIDB_CREATE_VTABLE))&&
				  !csep->hasOrderBy())
			{
				csep->limitStart(limitOffset);
				csep->limitNum(limitNum);
			}
			else
			{
				ostringstream limit;
				limit << " limit " << limitOffset << ", " << limitNum;
				select_query += limit.str();
				gwi.thd->infinidb_vtable.has_limit = true;
			}
		}

		gwi.thd->infinidb_vtable.select_vtable_query.free();
		gwi.thd->infinidb_vtable.select_vtable_query.append(select_query.c_str(), select_query.length());

		// We don't currently support limit with correlated subquery
		if (csep->limitNum() != (uint64_t)-1 &&
			 gwi.subQuery && !gwi.correlatedTbNameVec.empty())
		{
			gwi.fatalParseError = true;
			gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_LIMIT_SUB);
			setError(gwi.thd, HA_ERR_INTERNAL_ERROR, gwi.parseErrorText);
			return HA_ERR_UNSUPPORTED;
		}
	}

	if (/*join->select_options*/select_lex.options & SELECT_DISTINCT)
		csep->distinct(true);

	// add the smallest column to count(*) parm.
	// select constant in subquery case
	std::vector<AggregateColumn*>::iterator coliter;
	if (!minSc)
		minSc = gwi.returnedCols[0];

	// @bug3523, count(*) on subquery always pick column[0].
	SimpleColumn* sc = dynamic_cast<SimpleColumn*>(minSc.get());
	if (sc && sc->schemaName().empty())
	{
		if (gwi.derivedTbList.size() >= 1)
		{
			SimpleColumn* sc1 = new SimpleColumn();

			sc1->tableName(sc->tableName());
			sc1->tableAlias(sc->tableAlias());
			sc1->viewName(lower(sc->viewName()));
			sc1->colPosition(0);
			minSc.reset(sc1);
		}
	}

	for (coliter = gwi.count_asterisk_list.begin(); coliter != gwi.count_asterisk_list.end(); ++coliter)
	{
		(*coliter)->functionParms(minSc);
	}

	std::vector<FunctionColumn*>::iterator funciter;

	SPTP sptp(new ParseTree(minSc.get()->clone()));
	for (funciter = gwi.no_parm_func_list.begin(); funciter != gwi.no_parm_func_list.end(); ++funciter)
	{
		FunctionParm funcParms = (*funciter)->functionParms();
		funcParms.push_back(sptp);
		(*funciter)->functionParms(funcParms);
	}

	csep->groupByCols(gwi.groupByCols);
	csep->orderByCols(gwi.orderByCols);
	csep->returnedCols(gwi.returnedCols);
	csep->columnMap(gwi.columnMap);
	csep->having(havingFilter);
	csep->derivedTableList(gwi.derivedTbList);
	csep->selectSubList(selectSubList);

	gwi.thd->infinidb_vtable.duplicate_field_name = false;
	return 0;
}

int cp_get_plan(THD* thd, SCSEP& csep)
{
	LEX* lex = thd->lex;
	idbassert(lex != 0);

	SELECT_LEX select_lex = lex->select_lex;
	gp_walk_info gwi;
	gwi.thd = thd;
	int status = getSelectPlan(gwi, select_lex, csep);
	//if (getSelectPlan(gwi, select_lex, csep) > 0)
	if (status > 0)
		return HA_ERR_INTERNAL_ERROR;
	else if (status < 0)
		return status;
	//cout << thd->infinidb_vtable.create_vtable_query.c_ptr() << endl;
	//cout << thd->infinidb_vtable.select_vtable_query.c_ptr() << endl;

	return 0;
}

int cp_get_table_plan(THD* thd, SCSEP& csep, cal_table_info& ti)
{
	gp_walk_info* gwi = ti.condInfo;
	if (!gwi)
		gwi = new gp_walk_info();
	gwi->thd = thd;
	LEX* lex = thd->lex;
	idbassert(lex != 0);
	uint32_t sessionID = csep->sessionID();
	gwi->sessionid = sessionID;
	TABLE* table = ti.msTablePtr;
	boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	csc->identity(CalpontSystemCatalog::FE);

	// get all columns that mysql needs to fetch
	MY_BITMAP *read_set= table->read_set;
	Field **f_ptr,*field;
	gwi->columnMap.clear();
	for (f_ptr=table->field ; (field= *f_ptr) ; f_ptr++)
	{
		if (bitmap_is_set(read_set, field->field_index))
		{
			SimpleColumn* sc = new SimpleColumn(table->s->db.str, table->s->table_name.str, field->field_name, sessionID);
			string alias(table->alias);
			sc->tableAlias(lower(alias));
			assert (sc);
			shared_ptr<SimpleColumn> spsc(sc);
			gwi->returnedCols.push_back(spsc);
			gwi->columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(string(field->field_name), spsc));
		}
	}

	if (gwi->columnMap.empty())
	{
		CalpontSystemCatalog::TableName tn = make_table(table->s->db.str, table->s->table_name.str);
		CalpontSystemCatalog::TableAliasName tan = make_aliastable(table->s->db.str, table->s->table_name.str, table->alias);
		SimpleColumn *sc = getSmallestColumn(csc, tn, tan, table, *gwi);
		SRCP srcp(sc);
		gwi->columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(sc->columnName(), srcp));
		gwi->returnedCols.push_back(srcp);
	}

	// get filter
	if (ti.condInfo)
	{
		gp_walk_info* gwi = ti.condInfo;
		ParseTree* filters = 0;
		ParseTree* ptp = 0;
		ParseTree* rhs = 0;
		while (!gwi->ptWorkStack.empty())
		{
			filters = gwi->ptWorkStack.top();
			gwi->ptWorkStack.pop();
			SimpleFilter* sf = dynamic_cast<SimpleFilter*>(filters->data());
			if (sf && sf->op()->data() == "noop")
			{
				delete filters;
				filters = 0;
				if (gwi->ptWorkStack.empty())
					break;
				continue;
			}
			if (gwi->ptWorkStack.empty())
				break;
			ptp = new ParseTree(new LogicOperator("and"));
			ptp->left(filters);
			rhs = gwi->ptWorkStack.top();
			gwi->ptWorkStack.pop();
			ptp->right(rhs);
			gwi->ptWorkStack.push(ptp);
		}

		csep->filters(filters);
	}
	csep->returnedCols(gwi->returnedCols);
	csep->columnMap(gwi->columnMap);
	CalpontSelectExecutionPlan::TableList tblist;
	tblist.push_back(make_aliastable(table->s->db.str, table->s->table_name.str, table->alias));
	csep->tableList(tblist);

	// @bug 3321. Set max number of blocks in a dictionary file to be scanned for filtering
	csep->stringScanThreshold(gwi->thd->variables.infinidb_string_scan_threshold);

	return 0;
}
}
