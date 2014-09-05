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
* $Id: func_from_days.cpp 3616 2013-03-04 14:56:29Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_dtm.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

namespace funcexp
{

CalpontSystemCatalog::ColType Func_from_days::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_from_days::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	return getDatetimeIntVal(row, parm, isNull, op_ct);
}

string Func_from_days::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	ostringstream oss;
	oss << getIntVal(row, parm, isNull, ct);
	return oss.str();
}

int32_t Func_from_days::getDateIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return (getDatetimeIntVal(row, parm, isNull, ct) >> 32) & 0xFFFFFFC0;
}

int64_t Func_from_days::getDatetimeIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	uint ret_year, ret_month, ret_day;
	uint year, temp, leap_day, day_of_year, days_in_year;
	uint month_pos;
	
	double val1 = parm[0]->data()->getDoubleVal(row, isNull);
	int64_t daynr = (int64_t)(val1 > 0 ? val1 + 0.5 : val1 - 0.5);

	if (daynr <= 365 || daynr >= 3652500)
	{
		return 0;
	}
	else
	{
		year = daynr * 100 / 36525;
		temp =(((year-1)/100+1)*3)/4;
		day_of_year = (daynr - (long) year * 365) - (year-1)/4 + temp;
		while (day_of_year > (days_in_year= helpers::calc_days_in_year(year)))
		{
			day_of_year-=days_in_year;
			year++;
		}
		leap_day=0;
		if (days_in_year == 366)
		{
			if (day_of_year > 31+28)
			{
				day_of_year--;
				if (day_of_year == 31+28)
				leap_day=1;		/* Handle leapyears leapday */
			}
		}

		for (month_pos = 0, ret_month = 1 ; day_of_year > helpers::daysInMonth[month_pos]; month_pos++, ret_month++)
			day_of_year-= helpers::daysInMonth[month_pos];

		ret_year=year;
		ret_day=day_of_year+leap_day;
	}
	
	DateTime aDaytime;
	aDaytime.year = ret_year;
	aDaytime.month = ret_month;
	aDaytime.day = ret_day;
	aDaytime.hour = 0;
	aDaytime.minute = 0;
	aDaytime.second = 0;
	aDaytime.msecond = 0;
	return (*(reinterpret_cast<uint64_t *> (&aDaytime)));
}


} // namespace funcexp
// vim:ts=4 sw=4:
