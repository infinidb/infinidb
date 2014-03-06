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
* $Id: func_quarter.cpp 3495 2013-01-21 14:09:51Z rdempsey $
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

CalpontSystemCatalog::ColType Func_quarter::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}


int64_t Func_quarter::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	// try to cast to date/datetime
	int64_t val = 0, month = 0;
	switch (parm[0]->data()->resultType().colDataType)
	{
		case CalpontSystemCatalog::DATE:
			val = parm[0]->data()->getIntVal(row, isNull);
			month = (val >> 12) & 0xf;
			break;
		case CalpontSystemCatalog::DATETIME:
			val = parm[0]->data()->getIntVal(row, isNull);
			month = (val >> 44) & 0xf;
			break;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:			
		{
			val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));
			if (val == -1)
				isNull = true;
			else
				month = (val >> 44) & 0xf;
			break;
		}
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::INT:
		{
			val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
			if (val == -1)
				isNull = true;
			else
				month = (val >> 44) & 0xf;
			break;	
		}
		case CalpontSystemCatalog::DECIMAL:
		{
			if (parm[0]->data()->resultType().scale == 0)
			{
				val = dataconvert::DataConvert::intToDate(parm[0]->data()->getIntVal(row, isNull));
				if (val == -1)
					isNull = true;
				else
					month = (val >> 12) & 0xf;
				break;
			}
			else
			{
				isNull = true;
			}
		}
		default:
			isNull = true;
	}
	return (int64_t)((month+2)/3);
}


} // namespace funcexp
// vim:ts=4 sw=4:
