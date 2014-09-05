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

//  $Id: funchelpers.h 3616 2013-03-04 14:56:29Z rdempsey $

/** @file */

#ifndef FUNCHELPERS_H__
#define FUNCHELPERS_H__

#include <string>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/regex.hpp>
#include <boost/tokenizer.hpp>

#include "dataconvert.h"

#define ULONGLONG_MAX ulonglong_max

namespace funcexp
{
namespace helpers
{

// 10 ** i
const int64_t powerOf10_c[] = {
	1ll,
	10ll,
	100ll,
	1000ll,
	10000ll,
	100000ll,
	1000000ll,
	10000000ll,
	100000000ll,
	1000000000ll,
	10000000000ll,
	100000000000ll,
	1000000000000ll,
	10000000000000ll,
	100000000000000ll,
	1000000000000000ll,
	10000000000000000ll,
	100000000000000000ll,
	1000000000000000000ll };


// max integer number of i digits
const int64_t maxNumber_c[] = {
	0ll,
	9ll,
	99ll,
	999ll,
	9999ll,
	99999ll,
	999999ll,
	9999999ll,
	99999999ll,
	999999999ll,
	9999999999ll,
	99999999999ll,
	999999999999ll,
	9999999999999ll,
	99999999999999ll,
	999999999999999ll,
	9999999999999999ll,
	99999999999999999ll,
	999999999999999999ll };


/* Flags for calc_week() function.  */
const uint week_monday_first = 1;
const uint week_Year = 2;
const uint week_first_weekday = 4;
const uint TIMESTAMP_MAX_YEAR = 2038;
const uint TIMESTAMP_MIN_YEAR = (1970 - 1);
const int TIMESTAMP_MIN_VALUE = 1;
const int64_t TIMESTAMP_MAX_VALUE = 0x7FFFFFFFL;
const unsigned long long MAX_NEGATIVE_NUMBER	= 0x8000000000000000ULL;
const long long LONGLONG_MIN =  0x8000000000000000LL;
const int INIT_CNT = 9;
const unsigned long LFACTOR  = 1000000000;
const unsigned long long LFACTOR1 = 10000000000ULL;
const unsigned long long LFACTOR2 = 100000000000ULL;
const unsigned long long ulonglong_max= ~(unsigned long long) 0;

static std::string monthFullNames[13] = 
{
	"NON_VALID",
	"January",
	"February",
	"March",
	"April",
	"May",
	"June",
	"July",
	"August",
	"September",
	"October",
	"November",
	"December"	
};

static std::string monthAbNames[13] = 
{
	"NON_VALID",
	"Jan",
	"Feb",
	"Mar",
	"Apr",
	"May",
	"Jun",
	"Jul",
	"Aug",
	"Sep",
	"Oct",
	"Nov",
	"Dec"	
};

static std::string weekdayFullNames[8] = 
{
	"Monday",
	"Tuesday",
	"Wednesday",
	"Thursday",
	"Friday",
	"Saturday",
	"Sunday"
};

static std::string weekdayAbNames[8] = 
{
	"Mon",
	"Tue",
	"Wed",
	"Thu",
	"Fri",
	"Sat",
	"Sun"
};

static std::string dayOfMonth[32] = 
{"0th", "1st", "2nd", "3rd", "4th",
 "5th", "6th", "7th", "8th", "9th",
 "10th", "11th", "12th", "13th", "14th",
 "15th", "16th", "17th", "18th", "19th", "20th", 
 "21st", "22nd", "23rd", "24th", "25th",
 "26th", "27th", "28th", "29th", "30th",
 "31st"
 };

static uint daysInMonth[13] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};
static char daysInMonth1[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};

inline uint getDaysInMonth(uint month)
{ return ( (month < 1 || month > 12) ? 0 : daysInMonth[month-1]);}
 
inline uint32_t calc_daynr(uint32_t year, uint32_t month, uint32_t day)
{
  uint32_t delsum;
  int temp;

  delsum= (uint32_t) (365L * year+ 31*(month-1) +day);
  if (month <= 2)
      year--;
  else
    delsum-= (uint32_t) (month*4+23)/10;
  temp=(int) ((year/100+1)*3)/4;
  return (delsum+(int) year/4-temp);
}

inline uint32_t calc_weekday(uint32_t daynr, bool sunday_first_day_of_week)
{
  return ((uint32_t) ((daynr + 5L + (sunday_first_day_of_week ? 1L : 0L)) % 7));
}

inline uint32_t calc_days_in_year(uint year)
{
  return ((year & 3) == 0 && (year%100 || (year%400 == 0 && year)) ?
          366 : 365);
}

inline uint32_t calc_week(uint32_t year, uint32_t month, uint32_t day, int16_t week_behaviour, uint32_t *lyear)
{
  uint days;
  uint32_t daynr=calc_daynr(year,month,day);
  uint32_t first_daynr=calc_daynr(year,1,1);
  bool monday_first= week_behaviour & week_monday_first;
  bool week_year= ((week_behaviour & week_Year) != 0);
  bool first_weekday= ((week_behaviour & week_first_weekday) != 0);

  uint weekday=calc_weekday(first_daynr, !monday_first);
  *lyear=year;

  if (month == 1 && day <= 7-weekday)
  {
    if (!week_year && 
	((first_weekday && weekday != 0) ||
	 (!first_weekday && weekday >= 4))) {
      return 0;
	}
    week_year= 1;
    (*lyear)--;
    first_daynr-= (days=calc_days_in_year(*lyear));
    weekday= (weekday + 53*7- days) % 7;
  }

  if ((first_weekday && weekday != 0) ||
      (!first_weekday && weekday >= 4))
    days= daynr - (first_daynr+ (7-weekday));
  else
    days= daynr - (first_daynr - weekday);

  if (week_year && days >= 52*7)
  {
    weekday= (weekday + calc_days_in_year(*lyear)) % 7;
    if ((!first_weekday && weekday < 4) ||
	(first_weekday && weekday == 0))
    {
      (*lyear)++;
      return 1;
    }
  }
  return days/7+1;
}

inline bool calc_time_diff(int64_t time1, int64_t time2, int l_sign, long long *seconds_out, long long *microseconds_out)
{
  int64_t days;
  bool neg;
  int64_t microseconds;

	uint64_t year1 = 0, 
	         month1 = 0, 
	         day1 = 0, 
	         hour1 = 0, 
	         min1 = 0, 
	         sec1 = 0, 
	         msec1 = 0;

	uint64_t year2 = 0, 
	         month2 = 0, 
	         day2 = 0, 
	         hour2 = 0, 
	         min2 = 0, 
	         sec2 = 0, 
	         msec2 = 0;
	
	year1 = (uint32_t)((time1 >> 48) & 0xffff);
	month1 = (uint32_t)((time1 >> 44) & 0xf);
	day1 = (uint32_t)((time1 >> 38) & 0x3f);
	hour1 = (uint32_t)((time1 >> 32) & 0x3f);
	min1 = (uint32_t)((time1 >> 26) & 0x3f);
	sec1 = (uint32_t)((time1 >> 20) & 0x3f);
	msec1 = (uint32_t)((time1 & 0xfffff));

	year2 = (uint32_t)((time2 >> 48) & 0xffff);
	month2 = (uint32_t)((time2 >> 44) & 0xf);
	day2 = (uint32_t)((time2 >> 38) & 0x3f);
	hour2 = (uint32_t)((time2 >> 32) & 0x3f);
	min2 = (uint32_t)((time2 >> 26) & 0x3f);
	sec2 = (uint32_t)((time2 >> 20) & 0x3f);
	msec2 = (uint32_t)((time2 & 0xfffff));

	days= calc_daynr(year1, month1, day1);

	days-= l_sign*calc_daynr(year2, month2, day2);

  	microseconds= ((long long)days*(long)(86400) +
                 (long long)(hour1*3600L +
                            min1*60L +
                            sec1) -
                 l_sign*(long long)(hour2*3600L +
                                   min2*60L +
                                   sec2)) * (long long)(1000000) +
                (long long)msec1 -
                l_sign*(long long)msec2;

  neg= 0;
  if (microseconds < 0)
  {
    microseconds= -microseconds;
    neg= 1;
  }
  *seconds_out= microseconds/1000000L;
  *microseconds_out= (long long) (microseconds%1000000L);
  return neg;
}

inline uint32_t week_mode(uint mode)
{
  uint32_t week_format= (mode & 7);
  if (!(week_format & week_monday_first))
    week_format^= week_first_weekday;
  return week_format;
}

inline int power ( int16_t a ) {
	int b = 1;
	for ( int i=0 ; i < a ; i++ )
	{
		b = b * 10;
	}
	return b;
}

inline void decimalPlaceDec(int64_t& d, int64_t& p, int8_t& s)
{
	// find new scale if D < s
	if (d < s)
	{
		int64_t t = s;
		s = d;  // the new scale
		d -= t;
		int64_t i = (d >= 0) ? d : (-d);
		while (i--)
			p *= 10;
	}
	else
	{
		d = s;
	}
}

inline bool isLeapYear ( int year)
{
	bool result;

	if ( (year%4) != 0 )		//  or:	if ( year%4 )
		result = false;			//  means: if year is not divisible by 4
	else if ( (year%400) == 0 )	//  or:	if ( !(year%400) )
		result = true;			//  means: if year is divisible by 400
	else if ( (year%100) == 0 )	//  or:	if ( !(year%100) )
		result = false;			//  means: if year is divisible by 100
	else						//  (but not by 400, since that case
		result = true;			//  considered already)

	return ( result );
}

inline uint32_t convertMonth (std::string month)
{
	uint32_t value = 0;
	
	boost::algorithm::to_lower(month);

		if ( month == "jan" || month == "january" )
		{
			value = 1;
		}
		else if ( month == "feb" || month == "february" )
		{
			value = 2;
		}
		else if ( month == "mar" || month == "march" )
		{
			value = 3;
		}
		else if ( month == "apr" || month == "april" )
		{
			value = 4;
		}
		else if ( month == "may" )
		{
			value = 5;
		}
		else if ( month == "jun" || month == "june" )
		{
			value = 6;
		}
		else if ( month == "jul" || month == "july" )
		{
			value = 7;
		}
		else if ( month == "aug" || month == "august" )
		{
			value = 8;
		}
		else if ( month == "sep" || month == "september" )
		{
			value = 9;
		}
		else if ( month == "oct" || month == "october" )
		{
			value = 10;
		}
		else if ( month == "nov" || month == "november" )
		{
			value = 11;
		}
		else if ( month == "dec" || month == "december" )
		{
			value = 12;
		}
		else
		{
			value = 0;
		}
	return value;
}

class to_lower
{
    public:
        char operator() (char c) const            // notice the return type
        {
            return tolower(c);
        }
};

inline int  getNumbers( const std::string expr, int64_t *array, std::string funcType)
{
	int index = 0;

	int funcNeg = 1;
	if ( funcType == "SUB" )
		funcNeg = -1;

	if ( expr.size() == 0 )
		return 0;

	// @bug 4703 reworked this code to avoid use of incrementally
	//           built string to hold temporary values while
	//           scanning expr for numbers.  This function is now
	//           covered by a unit test in tdriver.cpp
	bool foundNumber = false;
	int64_t  number = 0;
	int  neg = 1;
	for ( unsigned int i=0 ; i < expr.size() ; i++ )
	{
		char value = expr[i];
		if ( (value >= '0' && value <= '9') )
		{
			foundNumber = true;
			number = ( number * 10 ) + ( value - '0' );
		}
		else if ( value == '-' && !foundNumber )
		{
			neg = -1;
		}
		else if ( value == '-')
		{
			// this is actually an error condition - it means that
			// input came in with something like NN-NN (i.e. a dash
			// between two numbers.  To match prior code we will
			// return the number up to the dash and just return
			array[index] = number * funcNeg * neg;
			index++;

			return index;
		}
		else
		{
			if ( foundNumber )
			{
				array[index] = number * funcNeg * neg;
				number = 0;
				neg = 1;
				index++;

				if ( index > 9 )
					return index;
			}
		}
	}

	if ( foundNumber )
	{
		array[index] = number * funcNeg * neg;
		index++;
	}

	return index;
}


inline int  getNumbers( const std::string expr, int *array, std::string funcType)
{
	int index = 0;

	int funcNeg = 1;
	if ( funcType == "SUB" )
		funcNeg = -1;

	if ( expr.size() == 0 )
		return 0;

	// @bug 4703 reworked this code to avoid use of incrementally
	//           built string to hold temporary values while
	//           scanning expr for numbers.  This function is now
	//           covered by a unit test in tdriver.cpp
	bool foundNumber = false;
	int  number = 0;
	int  neg = 1;
	for ( unsigned int i=0 ; i < expr.size() ; i++ )
	{
		char value = expr[i];
		if ( (value >= '0' && value <= '9') )
		{
			foundNumber = true;
			number = ( number * 10 ) + ( value - '0' );
		}
		else if ( value == '-' && !foundNumber )
		{
			neg = -1;
		}
		else if ( value == '-')
		{
			// this is actually an error condition - it means that
			// input came in with something like NN-NN (i.e. a dash
			// between two numbers.  To match prior code we will
			// return the number up to the dash and just return
			array[index] = number * funcNeg * neg;
			index++;

			return index;
		}
		else
		{
			if ( foundNumber )
			{
				array[index] = number * funcNeg * neg;
				number = 0;
				neg = 1;
				index++;

				if ( index > 9 )
					return index;
			}
		}
	}

	if ( foundNumber )
	{
		array[index] = number * funcNeg * neg;
		index++;
	}

	return index;
}

inline int convertWeek (std::string week) //Sunday = 0
{
	int value = -1;
	boost::to_lower(week);

	if ( week == "sunday" || week == "sun" )
	{
		value = 0;
	}
	else if ( week == "monday" || week == "mon" )
	{
		value = 1;
	}
	else if ( week == "tuesday" || week == "tue" )
	{
		value = 2;
	}
	else if ( week == "wednesday" || week == "wed" )
	{
		value = 3;
	}
	else if ( week == "thursday" || week == "thu" )
	{
		value = 4;
	}
	else if ( week == "friday" || week == "fri" )
	{
		value = 5;
	}
	else if ( week == "saturday" || week == "sat" )
	{
		value = 6;
	}	
	else
	{
		value = -1;
	}
	return value;

}

/* Change a daynr to year, month and day */
/* Daynr 0 is returned as date 00.00.00 */

inline void get_date_from_daynr(long daynr,dataconvert::DateTime & dateTime)
{
	uint year,temp,leap_day,day_of_year,days_in_year;
	char *month_pos;

	if (daynr <= 365L || daynr >= 3652500)
	{	/* Fix if wrong daynr */
		dateTime.year= dateTime.month = dateTime.day =0;
	}
	else
	{
		year= (uint) (daynr*100 / 36525L);
		temp=(((year-1)/100+1)*3)/4;
		day_of_year=(uint) (daynr - (long) year * 365L) - (year-1)/4 +temp;
		while (day_of_year > (days_in_year= calc_days_in_year(year)))
		{
			day_of_year-=days_in_year;
			(year)++;
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
		dateTime.month=1;
		for (month_pos= daysInMonth1 ;
			day_of_year > (uint) *month_pos ;
			day_of_year-= *(month_pos++), (dateTime.month)++)
			;
		dateTime.year=year;
		dateTime.day=day_of_year+leap_day;
	}
}

uint64_t dateAdd(uint64_t, std::string, std::string, bool, std::string);
const std::string IDB_date_format(const dataconvert::DateTime&, const std::string&);
const std::string timediff(int64_t, int64_t);
const char *convNumToStr(int64_t, char*, int);

} //namespace funcexp::helpers
} //namespace funcexp

#endif
