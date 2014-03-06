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
*   $Id: ha_window_function.cpp 9586 2013-05-31 22:02:06Z zzhu $
*
*
***********************************************************************/
#include <iostream>
#include <string>
using namespace std;

#include "idb_mysql.h"
#include "ha_calpont_impl_if.h"

#include "arithmeticcolumn.h"
#include "arithmeticoperator.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "intervalcolumn.h"
#include "windowfunctioncolumn.h"
using namespace execplan;

#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "functor.h"
#include "funcexp.h"
using namespace funcexp;

namespace cal_impl_if {

ReturnedColumn* nullOnError(gp_walk_info& gwi)
{
	if (gwi.hasSubSelect)
	{
		gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_NON_SUPPORT_SELECT_SUB);
		setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
	}

	if (gwi.parseErrorText.empty())
	{
		gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WF_NON_SUPPORT);
		setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
	}
	return NULL;
}

WF_FRAME frame(BOUND& bound)
{
	switch (bound)
	{
	case PRECEDING:
		return WF_PRECEDING;
	case FOLLOWING:
		return WF_FOLLOWING;
	case UNBOUNDED_PRECEDING:
		return WF_UNBOUNDED_PRECEDING;
	case UNBOUNDED_FOLLOWING:
		return WF_UNBOUNDED_FOLLOWING;
	case CURRENT_ROW:
		return WF_CURRENT_ROW;
	default:
		return WF_UNKNOWN;
	}
}

ReturnedColumn* buildBoundExp(WF_Boundary& bound, SRCP& order, gp_walk_info& gwi)
{
	bool addOp = true;
	ReturnedColumn* rc = NULL;
	if (bound.fFrame == execplan::WF_PRECEDING)
	{
		if (order->asc())
			addOp = false;
	}
	else if (!order->asc()) // must be WF_FOLLOWING
		addOp = false;

	funcexp::FunctionParm funcParms;
	SPTP sptp;
	IntervalColumn* intervalCol = dynamic_cast<IntervalColumn*>(bound.fVal.get());
	// @todo error out non constant. only support literal interval for now.
	if (!intervalCol && order->resultType().colDataType == CalpontSystemCatalog::DATE)
	{
		intervalCol = new IntervalColumn(bound.fVal, (int)IntervalColumn::INTERVAL_DAY);
		bound.fVal.reset(intervalCol);
	}

	if (intervalCol)
	{
		// date_add
		rc = new FunctionColumn();
		string funcName = "date_add_interval";

		// put interval val column to bound
		(dynamic_cast<FunctionColumn*>(rc))->functionName(funcName);
		sptp.reset(new ParseTree(order->clone()));
		funcParms.push_back(sptp);
		sptp.reset(new ParseTree(intervalCol->val()->clone()));
		funcParms.push_back(sptp);
		funcParms.push_back(getIntervalType(intervalCol->intervalType()));
		SRCP srcp(intervalCol->val());
		bound.fVal = srcp;
		if (addOp)
		{
			sptp.reset(new ParseTree(new ConstantColumn("ADD")));
			funcParms.push_back(sptp);
		}
		else
		{
			sptp.reset(new ParseTree(new ConstantColumn("SUB")));
			funcParms.push_back(sptp);
		}
		(dynamic_cast<FunctionColumn*>(rc))->functionParms(funcParms);
		CalpontSystemCatalog::ColType ct;
		ct.colDataType = CalpontSystemCatalog::DATETIME;
		ct.colWidth = 8;
		rc->resultType(ct);
		FuncExp* funcexp = FuncExp::instance();
		Func* functor = funcexp->getFunctor(funcName);
		(dynamic_cast<FunctionColumn*>(rc))->operationType(functor->operationType(funcParms, rc->resultType()));
		rc->expressionId(gwi.expressionId++);
		return rc;
	}

	// arithmetic
	rc = new ArithmeticColumn();
	ArithmeticOperator* aop;
	if (addOp)
		aop = new ArithmeticOperator("+");
	else
		aop = new ArithmeticOperator("-");
	ParseTree *pt = new ParseTree(aop);
	ParseTree *lhs = 0, *rhs = 0;
	lhs = new ParseTree(order->clone());
	rhs = new ParseTree(bound.fVal->clone());
	pt->left(lhs);
	pt->right(rhs);
	aop->resultType(order->resultType());
	aop->operationType(aop->resultType());
	(dynamic_cast<ArithmeticColumn*>(rc))->expression(pt);
	rc->resultType(aop->resultType());
	rc->operationType(aop->operationType());
	rc->expressionId(gwi.expressionId++);
	return rc;
}

ReturnedColumn* buildWindowFunctionColumn(Item* item, gp_walk_info& gwi, bool& nonSupport)
{
	//@todo fix print for create view
	//String str;
	//item->print(&str, QT_INFINIDB_NO_QUOTE);
	//cout << "XXXXXX " << str.c_ptr() << endl;

	gwi.hasWindowFunc = true;
	Item_func_window* wf = (Item_func_window*)item;
	string funcName = wf->func_name();
	WindowFunctionColumn* ac = new WindowFunctionColumn(funcName);
	ac->distinct(wf->isDistinct());
	Window_context *wf_ctx = wf->window_ctx();
	SRCP srcp;

	// arguments
	vector<SRCP> funcParms;
	for (uint32_t i = 0; i < wf->arg_count; i++)
	{
		srcp.reset(buildReturnedColumn(wf->arguments()[i], gwi, nonSupport));
		if (!srcp)
			return nullOnError(gwi);
		funcParms.push_back(srcp);
		if (gwi.clauseType == WHERE && !gwi.rcWorkStack.empty())
			gwi.rcWorkStack.pop();
	}
	ac->functionParms(funcParms);

	// Partition by
	if (wf_ctx)
	{
		vector<SRCP> partitions;
		for (uint32_t i = 0; i < wf_ctx->partition_count; i++)
		{
			srcp.reset(buildReturnedColumn(wf_ctx->partitions[i], gwi, nonSupport));
			if (!srcp)
				return nullOnError(gwi);
			partitions.push_back(srcp);
		}
		ac->partitions(partitions);

		// Order by
		if (wf_ctx->ordering)
		{
			WF_OrderBy orderBy;
			// order columns
			if (wf_ctx->ordering->orders)
			{
				vector<SRCP> orders;
				ORDER* orderCol = reinterpret_cast<ORDER*>(wf_ctx->ordering->orders->first);
				for (; orderCol; orderCol= orderCol->next)
				{
					Item* orderItem = *(orderCol->item);
					srcp.reset(buildReturnedColumn(orderItem, gwi, nonSupport));
					if (!srcp)
						return nullOnError(gwi);
					srcp->asc(orderCol->asc);
					srcp->nullsFirst(orderCol->nulls); // nulls 1-nulls first 0-nulls last
					orders.push_back(srcp);
				}
				orderBy.fOrders = orders;
			}

			// window frame
			WF_Frame frm;
			if (wf_ctx->ordering->frame)
			{
				frm.fIsRange = wf_ctx->ordering->frame->isRange;
				// start
				if (wf_ctx->ordering->frame->start)
				{
					frm.fStart.fFrame = frame(wf_ctx->ordering->frame->start->bound);

					if (wf_ctx->ordering->frame->start->item)
					{
						frm.fStart.fVal.reset(buildReturnedColumn(wf_ctx->ordering->frame->start->item, gwi, nonSupport));
						if (!frm.fStart.fVal)
							return nullOnError(gwi);

						// 1. check expr is numeric type (rows) or interval (range)
						bool boundTypeErr = false;
						switch (frm.fStart.fVal->resultType().colDataType)
						{
							case CalpontSystemCatalog::CHAR:
							case CalpontSystemCatalog::VARCHAR:
							case CalpontSystemCatalog::VARBINARY:
							case CalpontSystemCatalog::BLOB:
							case CalpontSystemCatalog::CLOB:
								boundTypeErr = true;
								break;
							case CalpontSystemCatalog::DATE:
							case CalpontSystemCatalog::DATETIME:
								if (!frm.fIsRange)
									boundTypeErr = true;
								else if (dynamic_cast<IntervalColumn*>(frm.fStart.fVal.get()) == NULL)
									boundTypeErr = true;
								break;
							default: //okay
								break;
						}
						if (boundTypeErr)
						{
							gwi.fatalParseError = true;
							gwi.parseErrorText =
							   logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WF_INVALID_BOUND_TYPE,
							                colDataTypeToString(frm.fStart.fVal->resultType().colDataType));
							return nullOnError(gwi);
						}
					}
				}

				// end
				if (wf_ctx->ordering->frame->end)
				{
					frm.fEnd.fFrame = frame(wf_ctx->ordering->frame->end->bound);
					if (wf_ctx->ordering->frame->end->item)
					{
						frm.fEnd.fVal.reset(buildReturnedColumn(wf_ctx->ordering->frame->end->item, gwi, nonSupport));
						if (!frm.fEnd.fVal)
							return nullOnError(gwi);

						// check expr is numeric type (rows) or interval (range)
						bool boundTypeErr = false;
						switch (frm.fEnd.fVal->resultType().colDataType)
						{
							case CalpontSystemCatalog::CHAR:
							case CalpontSystemCatalog::VARCHAR:
							case CalpontSystemCatalog::VARBINARY:
							case CalpontSystemCatalog::BLOB:
							case CalpontSystemCatalog::CLOB:
								boundTypeErr = true;
								break;
							case CalpontSystemCatalog::DATE:
							case CalpontSystemCatalog::DATETIME:
								if (!frm.fIsRange)
									boundTypeErr = true;
								else if (dynamic_cast<IntervalColumn*>(frm.fEnd.fVal.get()) == NULL)
									boundTypeErr = true;
								break;
							default: //okay
								break;
						}
						if (boundTypeErr)
						{
							gwi.fatalParseError = true;
							gwi.parseErrorText =
							   logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WF_INVALID_BOUND_TYPE,
							                colDataTypeToString(frm.fStart.fVal->resultType().colDataType));
							return nullOnError(gwi);
						}
					}
				}
				else // no end specified. default end to current row
				{
					frm.fEnd.fFrame = WF_CURRENT_ROW;
				}

				if (frm.fStart.fVal || frm.fEnd.fVal)
				{
					// check order by key only 1 (should be error-ed out in parser. double check here)
					if (frm.fIsRange && orderBy.fOrders.size() > 1)
					{
						gwi.fatalParseError = true;
						gwi.parseErrorText =
						   logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WF_INVALID_ORDER_KEY);
						return nullOnError(gwi);
					}

					// check order by key type is numeric or date/datetime
					bool orderTypeErr = false;
					if (frm.fIsRange && orderBy.fOrders.size() == 1)
					{
						switch (orderBy.fOrders[0]->resultType().colDataType)
						{
							case CalpontSystemCatalog::CHAR:
							case CalpontSystemCatalog::VARCHAR:
							case CalpontSystemCatalog::VARBINARY:
							case CalpontSystemCatalog::BLOB:
							case CalpontSystemCatalog::CLOB:
								orderTypeErr = true;
								break;
							default: //okay
								// interval bound has to have date/datetime order key
								if ((dynamic_cast<IntervalColumn*>(frm.fStart.fVal.get()) != NULL ||
									  dynamic_cast<IntervalColumn*>(frm.fEnd.fVal.get()) != NULL))
								{
									if (orderBy.fOrders[0]->resultType().colDataType != CalpontSystemCatalog::DATE &&
									   orderBy.fOrders[0]->resultType().colDataType != CalpontSystemCatalog::DATETIME)
										orderTypeErr = true;
								}
								else
								{
									if (orderBy.fOrders[0]->resultType().colDataType == CalpontSystemCatalog::DATETIME)
										orderTypeErr = true;
								}
								break;
						}
						if (orderTypeErr)
						{
							gwi.fatalParseError = true;
							gwi.parseErrorText =
								   logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WF_INVALID_ORDER_TYPE,
								             colDataTypeToString(orderBy.fOrders[0]->resultType().colDataType));
							return nullOnError(gwi);
						}
					}
				}

				// construct +,- or interval function for boundary
				if (frm.fIsRange && frm.fStart.fVal)
				{
					frm.fStart.fBound.reset(buildBoundExp(frm.fStart, orderBy.fOrders[0], gwi));
					if (!frm.fStart.fBound)
						return nullOnError(gwi);
				}
				if (frm.fIsRange && frm.fEnd.fVal)
				{
					frm.fEnd.fBound.reset(buildBoundExp(frm.fEnd, orderBy.fOrders[0], gwi));
					if (!frm.fEnd.fVal)
						return nullOnError(gwi);
				}
			}
			else
			{
				frm.fStart.fFrame = WF_UNBOUNDED_PRECEDING;
				frm.fEnd.fFrame = WF_CURRENT_ROW;
			}

			orderBy.fFrame = frm;
			ac->orderBy(orderBy);
		}
	}

	if (gwi.fatalParseError || nonSupport)
	{
		if (gwi.parseErrorText.empty())
			gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WF_NON_SUPPORT);
		setError(gwi.thd, HA_ERR_UNSUPPORTED, gwi.parseErrorText);
		return NULL;
	}
    
	ac->resultType(colType_MysqlToIDB(wf));
	
	// bug5736. Make the result type double for some window functions when
    // infinidb_double_for_decimal_math is set.
    ac->adjustResultType();
	    
	ac->expressionId(gwi.expressionId++);
	if (item->name)
		ac->alias(item->name);
	
	// put ac on windowFuncList
	gwi.windowFuncList.push_back(ac);
	return ac;
	
}

}
