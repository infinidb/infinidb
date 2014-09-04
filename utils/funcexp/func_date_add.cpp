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

/****************************************************************************
* $Id: func_date_add.cpp 3048 2012-04-04 15:33:45Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_dtm.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
#include "funchelpers.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_date_add::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	resultType.colDataType = CalpontSystemCatalog::DATETIME;
	resultType.colWidth = 8;

	return resultType;
}


int64_t Func_date_add::getIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	int64_t val = 0;
	switch (parm[0]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		{
			val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
			break;
		}
		case execplan::CalpontSystemCatalog::DECIMAL:
		{
			if (parm[0]->data()->resultType().scale)
				val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
			break;
		}
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));
			break;
		}
		case execplan::CalpontSystemCatalog::DATE:
		{
			val = parm[0]->data()->getDatetimeIntVal(row, isNull);
			break;
		}
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			val = parm[0]->data()->getDatetimeIntVal(row, isNull);
			break;
		}
		default:
		{
			isNull = true;
		}
	}
	
	if (isNull || val == -1)
	{
		isNull = true;
		return 0;
	}	
	
	//uint64_t time = parm[0]->data()->getIntVal(row, isNull);
	string expr = parm[1]->data()->getStrVal(row, isNull);
	string unit = parm[2]->data()->getStrVal(row, isNull);
	string funcType = parm[3]->data()->getStrVal(row, isNull);

	bool dateType = /*true*/false;

	//if (parm[0]->data()->resultType().colDataType != CalpontSystemCatalog::DATE)
	//	dateType = false;

	uint64_t value = dateAdd( val, expr, unit, dateType, funcType );

	if ( value == 0 )
		isNull = true;

	return value;
}


string Func_date_add::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return dataconvert::DataConvert::datetimeToString(getIntVal(row, parm, isNull, ct));

}


} // namespace funcexp
// vim:ts=4 sw=4:
