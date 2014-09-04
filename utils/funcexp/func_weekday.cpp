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
* $Id: func_weekday.cpp 2477 2011-04-01 16:07:35Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_weekday::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}


int64_t Func_weekday::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	uint32_t year = 0;
	uint32_t month = 0;
	uint32_t day = 0;
	int64_t val = 0;
	int32_t dayofweek= 0;
	uint32_t weekday = 0;
	switch (parm[0]->data()->resultType().colDataType)
	{
		case CalpontSystemCatalog::DATE:
			val = parm[0]->data()->getIntVal(row, isNull);
			year = (uint32_t)((val >> 16) & 0xffff);
			month = (uint32_t)((val >> 12) & 0xf);
			day = (uint32_t)((val >> 6) & 0x3f);
			break;
		case CalpontSystemCatalog::DATETIME:
			val = parm[0]->data()->getIntVal(row, isNull);
			year = (uint32_t)((val >> 48) & 0xffff);
			month = (uint32_t)((val >> 44) & 0xf);
			day = (uint32_t)((val >> 38) & 0x3f);
			break;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
			val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));
			if (val == -1)
			{
				isNull = true;
				return -1;
			}
			else
			{
				year = (uint32_t)((val >> 48) & 0xffff);
				month = (uint32_t)((val >> 44) & 0xf);
				day = (uint32_t)((val >> 38) & 0x3f);
			}
			break;
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::INT:
			val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
			if (val == -1)
			{
				isNull = true;
				return -1;
			}
			else
			{
				year = (uint32_t)((val >> 48) & 0xffff);
				month = (uint32_t)((val >> 44) & 0xf);
				day = (uint32_t)((val >> 38) & 0x3f);
			}
			break;	
		case CalpontSystemCatalog::DECIMAL:
			if (parm[0]->data()->resultType().scale == 0)
			{
				val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
				if (val == -1)
				{
					isNull = true;
					return -1;
				}
				else
				{
					year = (uint32_t)((val >> 48) & 0xffff);
					month = (uint32_t)((val >> 44) & 0xf);
					day = (uint32_t)((val >> 38) & 0x3f);
				}
			}
			break;
		default:
			isNull = true;
			return -1;
	}
	dayofweek= calc_weekday(calc_daynr(year, month, day), 1) + 1;
	weekday = ((dayofweek-2)>=0) ? (dayofweek-2) : (dayofweek+5);
	return weekday;
}


} // namespace funcexp
// vim:ts=4 sw=4:
