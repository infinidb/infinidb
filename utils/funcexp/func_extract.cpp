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
* $Id: func_extract.cpp 3283 2012-09-13 20:21:54Z zzhu $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_int.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_extract::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}
/*
class to_lower
{
    public:
        char operator() (char c) const            // notice the return type
        {
            return tolower(c);
        }
};
*/

long long dateGet( uint64_t time, string unit, bool dateType )
{
	transform (unit.begin(), unit.end(), unit.begin(), to_lower());

	uint32_t year = 0, 
	         month = 0, 
	         day = 0, 
	         hour = 0, 
	         min = 0, 
	         sec = 0, 
	         msec = 0,
		     lyear = 0;

	if (dateType)
	{
		year = (uint32_t)((time >> 16) & 0xffff);
		month = (uint32_t)((time >> 12) & 0xf);
		day = (uint32_t)((time >> 6) & 0x3f);
	}
	else
	{
		year = (uint32_t)((time >> 48) & 0xffff);
		month = (uint32_t)((time >> 44) & 0xf);
		day = (uint32_t)((time >> 38) & 0x3f);
		hour = (uint32_t)((time >> 32) & 0x3f);
		min = (uint32_t)((time >> 26) & 0x3f);
		sec = (uint32_t)((time >> 20) & 0x3f);
		msec = (uint32_t)((time & 0xfffff));
	}

	if ( unit == "year" )
		return year;

	if ( unit == "month" )
		return month;

	if ( unit == "day" )
		return day;

	if ( unit == "hour" )
		return hour;

	if ( unit == "minute" )
		return min;

	if ( unit == "second" )
		return sec;

	if ( unit == "microsecond" )
		return msec;

	if ( unit == "quarter" )
		return month/4 +1;

	if ( unit == "week" ) {
		char buf[256];
		char* ptr = buf;
		sprintf(ptr, "%02d", funcexp::calc_week(year, month, day, week_mode(0), &lyear));
		return atoi(ptr);
	}

	if ( unit == "year_month" )
		return (year*100)+month;

	if ( unit == "day_hour" )
		return (day*100)+hour;

	if ( unit == "day_minute" )
		return (day*10000)+(hour*100)+min;

	if ( unit == "day_second" )
		return (day*1000000)+(hour*10000)+(min*100)+sec;

	if ( unit == "hour_minute" )
		return (hour*100)+min;

	if ( unit == "hour_second" )
		return (hour*10000)+(min*100)+sec;

	if ( unit == "minute_second" )
		return (min*100)+sec;

//	if ( unit == "day_microsecond" )
//		return (day*1000000000000)+(hour*10000000000)+(min*100000000)+(sec*1000000)+msec;

//	if ( unit == "hour_microsecond" )
//		return (hour*10000000000)+(min*100000000)+(sec*1000000)+msec;

//	if ( unit == "minute_microsecond" )
//		return (min*100000000)+(sec*1000000)+msec;

	if ( unit == "second_microsecond" )
		return (sec*1000000)+msec;

	throw runtime_error("unit type is not supported: " + unit);
}


int64_t Func_extract::getIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	string unit = parm[1]->data()->getStrVal(row, isNull);
	uint64_t time;	
	
	//@bug4678 handle conversion from non date/datetime datatype
	switch (parm[0]->data()->resultType().colDataType)
	{
	case CalpontSystemCatalog::DATE:
	case CalpontSystemCatalog::DATETIME:
		time = parm[0]->data()->getDatetimeIntVal(row, isNull);
		break;
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::CHAR:
	{
		string val = parm[0]->data()->getStrVal(row, isNull);
		time = dataconvert::DataConvert::stringToDatetime(val);
		break;
	}
	case CalpontSystemCatalog::INT:
	case CalpontSystemCatalog::TINYINT:
	case CalpontSystemCatalog::MEDINT:
	case CalpontSystemCatalog::BIGINT:
	case CalpontSystemCatalog::SMALLINT:
	{
		int64_t val = parm[0]->data()->getIntVal(row, isNull);
		time = dataconvert::DataConvert::intToDatetime(val);
		break;
	}
	default:
		time = parm[0]->data()->getIntVal(row, isNull);
	}

	long long value = dateGet( time, unit, false );

	return value;
}


} // namespace funcexp
// vim:ts=4 sw=4:
