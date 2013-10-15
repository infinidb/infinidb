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

/****************************************************************************
* $Id: func_maketime.cpp 2665 2011-06-01 20:42:52Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <iomanip>
using namespace std;

#include "functor_str.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

namespace funcexp
{

CalpontSystemCatalog::ColType Func_maketime::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

string Func_maketime::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	int64_t hour = 0;
	int64_t min = 0;
	int64_t sec = 0;

	//get hour
	switch (parm[0]->data()->resultType().colDataType)
	{
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		{
			double value = parm[0]->data()->getDoubleVal(row, isNull);
			hour = (int64_t) value;
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
		{
			IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
			hour = d.value / helpers::power(d.scale);
			int lefto = (d.value - hour * helpers::power(d.scale)) / helpers::power(d.scale-1);
			if ( hour >= 0 && lefto > 4 )
				hour++;
			if ( hour < 0 && lefto < -4 )
				hour--;
			break;
		}
		default:
			isNull = true;
			return "";
	}

	//get minute
	switch (parm[1]->data()->resultType().colDataType)
	{
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		{
			double value = parm[1]->data()->getDoubleVal(row, isNull);
			min = (int64_t) value;
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
		{
			IDB_Decimal d = parm[1]->data()->getDecimalVal(row, isNull);
			min = d.value / helpers::power(d.scale);
			int lefto = (d.value - min * helpers::power(d.scale)) / helpers::power(d.scale-1);
			if ( min >= 0 && lefto > 4 )
				min++;
			if ( min < 0 && lefto < -4 )
				min--;
			break;
		}
		default:
			isNull = true;
			return "";
	}

	if (min < 0 || min > 59)
	{
		isNull = true;
		return "";
	}

	//get second
	switch (parm[2]->data()->resultType().colDataType)
	{
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		{
			double value = parm[2]->data()->getDoubleVal(row, isNull);
			sec = (int64_t) value;
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
		{
			IDB_Decimal d = parm[2]->data()->getDecimalVal(row, isNull);
			sec = d.value / helpers::power(d.scale);
			int lefto = (d.value - sec * helpers::power(d.scale)) / helpers::power(d.scale-1);
			if ( sec >= 0 && lefto > 4 )
				sec++;
			if ( sec < 0 && lefto < -4 )
				sec--;
			break;
		}
		default:
			isNull = true;
			return "";
	}

	if (sec < 0 || sec > 59)
	{
		isNull = true;
		return "";
	}

	if (hour > 838)
	{
		hour = 838;
		min = 59;
		sec = 59;
	}

	if (hour < -838)
	{
		hour = -838;
		min = 59;
		sec = 59;
	}

	// in worst case hour is 4 characters (3 digits + '-') so max
    // string length is 11 (4:2:2 + '\0')
    char buf[11];
    snprintf(buf, 11, "%02d:%02d:%02d", (int) hour, (int) min, (int) sec);
    return buf;
}


} // namespace funcexp
// vim:ts=4 sw=4:
