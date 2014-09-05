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
* $Id: func_date_add.cpp 3616 2013-03-04 14:56:29Z rdempsey $
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
namespace helpers
{
uint64_t dateAdd( uint64_t time, string expr, string unit, bool dateType, string funcType )
{
	int array[10];
	int64_t array2[10];

	int month_length[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
	
	transform (unit.begin(), unit.end(), unit.begin(), to_lower());

	int32_t year = 0, 
	         month = 0, 
	         day = 0, 
	         hour = 0, 
			 monthSave = 0;
	
	int64_t min = 0, 
	         sec = 0, 
	         msec = 0;

	int32_t yearAdd = 0, 
	         monthAdd = 0, 
	         dayAdd = 0, 
	         hourAdd = 0;
	
	int64_t minAdd = 0, 
	         secAdd = 0, 
	         msecAdd = 0;
	
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

	monthSave = month;

	bool validUnit = false;

	int index = -1;

	if( (unit == "minute")||(unit == "second")||(unit == "microsecond") )
		index = getNumbers( expr, array2, funcType);
	else
		index = getNumbers(expr, array, funcType);
	
	if ( index <= 0 )
		throw runtime_error("expression type is not supported");


	if ( unit == "year" ) {
		yearAdd = array[0];
		validUnit = true;
	}

	if ( unit == "quarter" ) {
		monthAdd = array[0] * 3;
		validUnit = true;
	}

	if ( unit == "month" ) {
		monthAdd = array[0];
		validUnit = true;
	}

	if ( unit == "week" ) {
		dayAdd = array[0] *7;
		validUnit = true;
	}

	if ( unit == "day" ) {
		dayAdd = array[0];
		validUnit = true;
	}

	if ( unit == "hour" ) {
		hourAdd = array[0];
		validUnit = true;
	}

	if ( unit == "minute" ) {
		minAdd = array2[0];
		validUnit = true;
	}

	if ( unit == "second" ) {
		secAdd = array2[0];
		validUnit = true;
	}

	if ( unit == "microsecond" ) {
		msecAdd = array2[0];
		validUnit = true;
	}

	if ( unit == "year_month" ) {
		if ( index > 2 ) {
			return 0;
		}
		if ( index == 1 )
			monthAdd = array[0];
		else
		{
			yearAdd = array[0];
			monthAdd = array[1];
		}

		validUnit = true;
	}

	if ( unit == "day_hour" ) {
		if ( index > 2 ) {
			return 0;
		}
		if ( index == 1 )
			hourAdd = array[0];
		else
		{
			dayAdd = array[0];
			hourAdd = array[1];
		}

		validUnit = true;
	}

	if ( unit == "day_minute" ) {
		if ( index > 3 ) {
			return 0;
		}
		if ( index == 1 )
			minAdd = array[0];
		else
		{
			if ( index == 2 )
			{
				hourAdd = array[0];
				minAdd = array[1];
			}
			else
			{
				dayAdd = array[0];
				hourAdd = array[1];
				minAdd = array[2];
			}
		}

		validUnit = true;
	}

	if ( unit == "day_second" ) {
		if ( index > 4 ) {
			return 0;
		}
		if ( index == 1 )
			secAdd = array[0];
		else
		{
			if ( index == 2 )
			{
				minAdd = array[0];
				secAdd = array[1];
			}
			else
			{
				if ( index == 3 )
				{
					hourAdd = array[0];
					minAdd = array[1];
					secAdd = array[2];
				}
				else
				{
					dayAdd = array[0];
					hourAdd = array[1];
					minAdd = array[2];
					secAdd = array[3];
				}
			}
		}

		validUnit = true;
	}

	if ( unit == "hour_minute" ) {
		if ( index > 2 ) {
			return 0;
		}
		if ( index == 1 )
			minAdd = array[0];
		else
		{
			hourAdd = array[0];
			minAdd = array[1];
		}

		validUnit = true;
	}

	if ( unit == "hour_second" ) {
		if ( index > 3 ) {
			return 0;
		}
		if ( index == 1 )
			secAdd = array[0];
		else
		{
			if ( index == 2 )
			{
				minAdd = array[0];
				secAdd = array[1];
			}
			else
			{
				hourAdd = array[0];
				minAdd = array[1];
				secAdd = array[2];
			}
		}

		validUnit = true;
	}

	if ( unit == "minute_second" ) {
		if ( index > 2 ) {
			return 0;
		}
		if ( index == 1 )
			secAdd = array[0];
		else
		{
			minAdd = array[0];
			secAdd = array[1];
		}

		validUnit = true;
	}

	if ( unit == "day_microsecond" ) {
		if ( index > 5 ) {
			return 0;
		}
		if ( index == 1 )
			msecAdd = array[0];
		else
		{
			if ( index == 2 )
			{
				secAdd = array[0];
				msecAdd = array[1];
			}
			else
			{
				if ( index == 3 )
				{
					minAdd = array[0];
					secAdd = array[1];
					msecAdd = array[2];
				}
				else
				{
					if ( index == 4 )
					{
						hourAdd = array[0];
						minAdd = array[1];
						secAdd = array[2];
						msecAdd = array[3];
					}
					else
					{
						dayAdd = array[0];
						hourAdd = array[1];
						minAdd = array[2];
						secAdd = array[3];
						msecAdd = array[4];
					}
				}
			}
		}

		validUnit = true;
	}

	if ( unit == "hour_microsecond" ) {
		if ( index > 4 ) {
			return 0;
		}
		if ( index == 1 )
			msecAdd = array[0];
		else
		{
			if ( index == 2 )
			{
				secAdd = array[0];
				msecAdd = array[1];
			}
			else
			{
				if ( index == 3 )
				{
					minAdd = array[0];
					secAdd = array[1];
					msecAdd = array[2];
				}
				else
				{
					hourAdd = array[0];
					minAdd = array[1];
					secAdd = array[2];
					msecAdd = array[3];
				}
			}
		}

		validUnit = true;
	}

	if ( unit == "minute_microsecond" ) {
		if ( index > 3 ) {
			return 0;
		}
		if ( index == 1 )
			msecAdd = array[0];
		else
		{
			if ( index == 2 )
			{
				secAdd = array[0];
				msecAdd = array[1];
			}
			else
			{
				minAdd = array[0];
				secAdd = array[1];
				msecAdd = array[2];
			}
		}

		validUnit = true;
	}

	if ( unit == "second_microsecond" ) {
		if ( index > 2 ) {
			return 0;
		}
		if ( index == 1 )
			msecAdd = array[0];
		else
		{
			secAdd = array[0];
			msecAdd = array[1];
		}

		validUnit = true;
	}

	if ( !validUnit )
		throw runtime_error("unit type is not supported");

	// calulate new date

	year += yearAdd;
	month += monthAdd;
	day += dayAdd;
	hour += hourAdd;
	min += minAdd;
	sec += secAdd;
	msec += msecAdd;

	if ( msec > 999999 ) {
		int64_t secs = msec / 1000000;
		sec += secs;
		msec = msec - ( secs * 1000000 );
	}

	if ( msec < 0 ) {
		int64_t secs = 1 + (-msec / 1000000);
		sec -= secs;
		msec = msec + ( secs * 1000000 );
	}

	if ( sec > 59 ) {
		int64_t mins = sec / 60;
		min += mins;
		sec = sec - ( mins * 60 );
	}

	if ( sec < 0 ) {
		int64_t mins = 0;
		if ( ( sec + (-sec / 60) * 60 ) == 0 &&
			sec != 0)
			mins = (-sec / 60);
		else
			mins = 1 + (-sec / 60);

		min -= mins;
		sec = sec + ( mins * 60 );
		if ( sec >= 60 )
			sec = 0;
	}

	if ( min > 59 ) {
		int hours = min / 60;
		hour += hours;
		min = min - ( hours * 60 );
	}

	if ( min < 0 ) {
		int hours = 0;
		if ( ( min + (-min / 60) * 60 ) == 0 &&
			min != 0)
			hours = (-min / 60);
		else
			hours = 1 + (-min / 60);

		hour -= hours;
		min = min + ( hours * 60 );
		if ( min >= 60 )
			min = 0;
	}

	if ( hour > 23 ) {
		int days = hour / 24;
		day += days;
		hour = hour - ( days * 24 );
	}

	if ( hour < 0 ) {
		int days = 0;
		if ( ( hour + (-hour / 24) * 24 ) == 0 &&
			hour != 0)
			days = (-hour / 24);
		else
			days = 1 + (-hour / 24);

		day -= days;
		hour = hour + ( days * 24 );
		if ( hour >= 24 )
			hour = 0;
	}

	int tmpYear = year;
	if ( isLeapYear(tmpYear) )
		month_length[2] = 29;
	else
		month_length[2] = 28;

	if ( day > 0 ) {
		while(true)
		{
			int years = (monthSave-1) / 12;
//			tmpYear += years;
			if ( isLeapYear(tmpYear) )
				month_length[2] = 29;
			else
				month_length[2] = 28;

			int tmpMonth = monthSave - ( years * 12 );
			if ( day <= month_length[tmpMonth] )
				break;
			month++;
			day = day - month_length[tmpMonth];
			monthSave++;
			if ( monthSave > 12 ) {
				monthSave = 1;
				tmpYear++;
			}
		}
	}
	else
	{
		while(true)
		{
			if ( day > 0 )
				break;
			if ( -day < month_length[monthSave] ) {
				month--;
				monthSave--;
				if ( monthSave == 0 ) {
					monthSave = 12;
					tmpYear--;
				}
				if (monthSave == 2) {
			//		if ( dataconvert::DataConvert::isLeapYear(year) )
					if ( isLeapYear(tmpYear) )
						month_length[2] = 29;
				}
				if (day < 1 )
					day = month_length[monthSave] + day;
				// BUG 5448 - changed from '==' to '<='
				if ( day <= 0 ) {
					month--;
					monthSave--;
					if ( monthSave == 0 ) {
						monthSave = 12;
						tmpYear--;
					}
					if (monthSave == 2) {
				//		if ( dataconvert::DataConvert::isLeapYear(year) )
						if ( isLeapYear(tmpYear) )
							month_length[2] = 29;
					}
					day = day + month_length[monthSave];
				}
				break;
			}

			month--;
			monthSave--;
			if ( monthSave == 0 ) {
				monthSave = 12;
				tmpYear--;
				if ( isLeapYear(tmpYear) )
					month_length[2] = 29;
				else
					month_length[2] = 28;
			}
			day = day + month_length[monthSave];
		}
	}


	if ( month > 12 ) {
		int years = (month-1) / 12;
		year += years;
		month = month - ( years * 12 );
	}

	if ( month < 1 ) {
		int years = 1 + ((-month) / 12);
		year -= years;
		month = month + ( years * 12 );
	}

	if ( isLeapYear(year) )
		month_length[2] = 29;
	else
		month_length[2] = 28;

	if ( day > month_length[month] )
		day = month_length[month];

	if ( year < 1000 || year > 9999 ) {
		return 0;
	}


	uint64_t value;
	dataconvert::DateTime aDatetime;
	aDatetime.year = year;
	aDatetime.month = month;
	aDatetime.day = day;
	aDatetime.hour = hour;
	aDatetime.minute = min;
	aDatetime.second = sec;
	aDatetime.msecond = msec;
	value = *(reinterpret_cast<uint64_t*>(&aDatetime));

	return value;

}
} // namespace funcexp::helpers

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

	uint64_t value = helpers::dateAdd( val, expr, unit, dateType, funcType );

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
