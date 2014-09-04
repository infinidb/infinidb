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
* $Id: func_makedate.cpp 2665 2011-06-01 20:42:52Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_dtm.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

using namespace funcexp;

namespace
{
uint64_t makedate(rowgroup::Row& row,
					FunctionParm& parm,
					bool& isNull)
{
	int64_t year = 0;
	string dayofyear;

	//get year
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
			year = (int64_t) value;
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
		{
			IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
			year = d.value / power(d.scale);
			int lefto = (d.value - year * power(d.scale)) / power(d.scale-1);
			if ( year >= 0 && lefto > 4 )
				year++;
			if ( year < 0 && lefto < -4 )
				year--;
			break;
		}
		default:
			isNull = true;
			return 0;
	}

	if (year < 1000 || year > 9999) {
		isNull = true;
		return 0;
	}

	//get dayofyear
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
			dayofyear = parm[1]->data()->getStrVal(row, isNull);

			if (atoi(dayofyear.c_str()) < 1)
			{
				isNull = true;
				return 0;
			}

			break;
		}
		case CalpontSystemCatalog::DECIMAL:
		{
			IDB_Decimal d = parm[1]->data()->getDecimalVal(row, isNull);
			int64_t tmp = d.value / power(d.scale);
			int lefto = (d.value - tmp * power(d.scale)) / power(d.scale-1);
			if ( tmp >= 0 && lefto > 4 )
				tmp++;
			if ( tmp < 0 && lefto < -4 )
				tmp--;

			if (tmp < 1)
			{
				isNull = true;
				return 0;
			}

			ostringstream oss;
			oss << tmp;
			dayofyear = oss.str();
			break;
		}
		default:
			isNull = true;
			return 0;
	}

	//convert year into date-format
	ostringstream oss;
	oss << year << "-01-01";

	uint64_t intDate = dataconvert::DataConvert::stringToDate(oss.str()) - 1;

	uint64_t value = dateAdd( intDate, dayofyear, "DAY", true, "add" );

	if ( value == 0 ) {
		isNull = true;
	}

	return value;
}

}


namespace funcexp
{

CalpontSystemCatalog::ColType Func_makedate::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}


int64_t Func_makedate::getIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	return makedate(row, parm, isNull);
}


string Func_makedate::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	uint64_t value = makedate(row, parm, isNull);
	if (isNull)
		return "";

	return dataconvert::DataConvert::dateToString(value);
}


} // namespace funcexp
// vim:ts=4 sw=4:
