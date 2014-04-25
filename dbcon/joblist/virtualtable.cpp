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

//  $Id: virtualtable.cpp 6412 2010-03-29 04:58:09Z xlou $


#include <iostream>
//#define NDEBUG
#include <cassert>
using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
using namespace boost;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "returnedcolumn.h"
#include "aggregatecolumn.h"
#include "windowfunctioncolumn.h"
#include "arithmeticcolumn.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "simplecolumn.h"
using namespace execplan;

#include "jobstep.h"
#include "jlf_tuplejoblist.h"
#include "virtualtable.h"


namespace joblist
{

VirtualTable::VirtualTable() : fTableOid(CNX_VTABLE_ID), fVarBinOK(false)
{
}


void VirtualTable::initialize()
{
}


void VirtualTable::addColumn(const SRCP& column)
{
	// As of bug3695, make sure varbinary is not used in subquery.
	if (column->resultType().colDataType == CalpontSystemCatalog::VARBINARY && !fVarBinOK)
		throw runtime_error ("VARBINARY in subquery is not supported.");

	AggregateColumn*      agc = NULL;
	ArithmeticColumn*     arc = NULL;
	ConstantColumn*       cc  = NULL;
	FunctionColumn*       fc  = NULL;
	SimpleColumn*         sc  = NULL;
	WindowFunctionColumn* wc  = NULL;

	string columnName;
	ostringstream oss;
	UniqId colId;
	if ((sc = dynamic_cast<SimpleColumn*>(column.get())) != NULL)
	{
		if (sc->schemaName().empty())
			sc->oid(fTableOid+sc->colPosition()+1);
		columnName = sc->columnName();
		colId = UniqId(sc);
	}
	else if ((agc = dynamic_cast<AggregateColumn*>(column.get())) != NULL)
	{
//		oss << agc->functionName() << "_" << agc->expressionId();
//		oss << "Aggregate_" << agc->expressionId();
		columnName = agc->data();
		colId = UniqId(agc->expressionId(), "", "", "");
	}
	else if ((wc = dynamic_cast<WindowFunctionColumn*>(column.get())) != NULL)
	{
//		oss << wc->functionName() << "_" << wc->expressionId();
//		oss << "Window_" << wc->expressionId();
		columnName = wc->data();
		colId = UniqId(wc->expressionId(), "", "", "");
	}
	else if ((arc = dynamic_cast<ArithmeticColumn*>(column.get())) != NULL)
	{
//		oss << "Arithmetic_" << arc->expressionId();
		columnName = arc->data();
		colId = UniqId(arc->expressionId(), "", "", "");
	}
	else if ((fc = dynamic_cast<FunctionColumn*>(column.get())) != NULL)
	{
//		oss << fc->functionName() << "_" << fc->expressionId();
		columnName = fc->data();
		colId = UniqId(fc->expressionId(), "", "", "");
	}
	else if ((cc = dynamic_cast<ConstantColumn*>(column.get())) != NULL)
	{
//		oss << "Constant_" << cc->expressionId();
		columnName = cc->data();
		colId = UniqId(cc->expressionId(), cc->alias(), "", fView);
	}
	else // new column type has added, but this code is not updated.
	{
		oss << "not supported column type: " << typeid(*(column.get())).name();
		throw runtime_error(oss.str());
	}

	if (columnName.empty())
		columnName = column->alias();

	SimpleColumn* vc = new SimpleColumn();
	vc->tableName(fName);
	vc->tableAlias(fAlias);
	vc->columnName(columnName);
	vc->alias(column->alias());
	vc->viewName(fView);

	uint32_t index = fColumns.size();
	vc->colPosition(index);
	vc->oid(fTableOid+index+1);
	vc->resultType(column->resultType());

	SSC ssc(vc);
	fColumns.push_back(ssc);
	fColumnTypes.push_back(column->resultType());
	fColumnMap.insert(make_pair(colId, index));
}


const CalpontSystemCatalog::OID& VirtualTable::columnOid(uint32_t i) const
{
	idbassert(i < fColumns.size());
	return fColumns[i]->oid();
}

void VirtualTable::columnType(CalpontSystemCatalog::ColType& type, uint32_t i)
{
	idbassert(i < fColumnTypes.size());
	fColumnTypes[i] = type;
	fColumns[i]->resultType(type);
}


const CalpontSystemCatalog::ColType& VirtualTable::columnType(uint32_t i) const
{
	idbassert(i < fColumnTypes.size());
	return fColumnTypes[i];
}


}
// vim:ts=4 sw=4:

