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

//  $Id: funchelpers.h 3921 2013-06-19 18:59:56Z bwilkinson $

/** @file */

#ifndef FUNCHELPERS_H__
#define FUNCHELPERS_H__

#include <string>

#ifndef _MSC_VER
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>
#endif

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/regex.hpp>
#include <boost/tokenizer.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>

#include "dataconvert.h"
#include "operator.h"
#include "intervalcolumn.h"
#include "treenode.h"

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

// Given a date, calculate the number of days since year 0
inline uint32_t calc_mysql_daynr( uint32_t year, uint32_t month, uint32_t day )
{
	if( !dataconvert::isDateValid( day, month, year ) )
		return 0;

	boost::gregorian::date d( year, month, day );
	// this is the number of days between the beginning of the
	// Gregorian calendar (November 24, 4714 B.C.) and the starting
	// date offset in MySQL which is year 0.
	const uint32_t JULIAN_DAY_OFFSET = 1721060;
    return d.julian_day() - JULIAN_DAY_OFFSET;
}

// convert from a MySQL day number (offset from year 0) to a date
inline void get_date_from_mysql_daynr(long daynr,dataconvert::DateTime & dateTime)
{
	// the MySQL day numbers for min/max boost supported dates
	const int BOOST_START_OFFSET = 511340;  // 1400-01-01
	const int BOOST_END_OFFSET   = 3652424; // 9999-12-31

	if( daynr < BOOST_START_OFFSET || daynr > BOOST_END_OFFSET )
	{
		dateTime.year= dateTime.month = dateTime.day =0;
	}
	else
	{
		boost::gregorian::date d(1400,1,1);
		d += boost::gregorian::date_duration( daynr - BOOST_START_OFFSET );
		dateTime.year  = d.year();
		dateTime.month = d.month();
		dateTime.day   = d.day();
	}
}

// Returns the weekday index for a given date:
//   if sundayFirst:
//       0 = Sunday, 1 = Monday, ..., 6 = Saturday
//   else:
//       0 = Monday, 1 = Tuesday, ..., 6 = Sunday
inline uint32_t calc_mysql_weekday( uint32_t year, uint32_t month, uint32_t day, bool sundayFirst )
{
	if( !dataconvert::isDateValid( day, month, year ) )
		return 0;

	boost::gregorian::date d( year, month, day );
	uint32_t ret = d.day_of_week();
	return sundayFirst ? ret : (ret + 6) % 7;
}

// Flags for calc_mysql_week
const uint WEEK_MONDAY_FIRST  = 1;
const uint WEEK_NO_ZERO       = 2;
const uint WEEK_GT_THREE_DAYS = 4;

// Takes a MySQL WEEK() function mode setting and converts to a bitmask
// used by calc_mysql_week()
inline int16_t convert_mysql_mode_to_modeflags( int16_t mode )
{
	// For some crazy reason, MySQL assigns modes 1, 3, 4, and 6 to use
	// the setting for >3 days in first week which does not align to
	// any particular bit.  This small piece of code sets bit 2
	// (WEEK_GT_THREE_DAYS) for modes 1 & 3 and disables for modes
	// 5 & 7 which is exactly what we want.
	if( mode & WEEK_MONDAY_FIRST )
		mode ^= WEEK_GT_THREE_DAYS;

	return mode;
}

// Returns a week index conforming to the MySQL WEEK() function.  Note
// that modeflags is not the MySQL mode - it is a bitmask of the abvoe
// 3 flags.  The utility function convert_mysql_mode_to_modeflags should
// be applied to the MySQL mode before calling this function (or the
// flags may be used directly).  The optional argument is for callers
// that need to know the year that the week actually corresponds to -
// see MySQL documentation for how the year returned can be different
// than the year of the input date
inline uint32_t calc_mysql_week( uint32_t year, uint32_t month, uint32_t day,
						   	     int16_t modeflags,
						   	     uint32_t* weekyear = 0 )
{
	// need to make sure that the date is valid for boost::gregorian::date
	if( !dataconvert::isDateValid( day, month, year ) )
		return 0;

	boost::gregorian::date d( year, month, day );

	// default this to the year of the input date - will update in the
	// scenarios where it is either 1 behind or 1 ahead
	if( weekyear )
		*weekyear = d.year();

	// get a date object for the first day of they year in question
	boost::gregorian::date yearfirst = boost::gregorian::date(d.year(),1,1);

	// figure out which day of week Jan-01 is
	uint32_t firstweekday = calc_mysql_weekday( d.year(), 1, 1, !( modeflags & WEEK_MONDAY_FIRST ) );

	// calculate the offset to the first week starting day
	uint32_t firstoffset = firstweekday ? ( 7 - firstweekday ) : 0;

	// julian day number for the first day of the first week
	uint32_t baseday = yearfirst.julian_day() + firstoffset;

	// if using a modeflags where the first week must have >3 days in this year
	// we need to check where firstweekday fell.  There are 3 cases for Jan-01
	//    1) Jan-01 fell on week start - there are obviously at least
	//       three days this year and baseday already correct
	//    2) Jan-01 fell on weekday 1 - 3 - this means that our baseday is
	//       pointing to next week but there must be at least 3 days this year
	//       so we need to adjust baseday back by one week
	//    3) Jan-01 fell on weekday 4 - 6 - this means there cannot be >3
	//       days in the week with Jan 01 so our baseday is already correct
	if( modeflags & WEEK_GT_THREE_DAYS )
	{
		if( firstweekday > 0 && firstweekday < 4 )
		{
			baseday = baseday - 7;
		}
	}

	uint32_t weeknum = 0;
	if( d.julian_day() < baseday && ( modeflags & WEEK_NO_ZERO ) )
	{
		// this is somewhat of a pain - since we aren't using a 0 week, the date
		// is actually going to be in the last week of last year and we need to
		// figure out which number that is.  This code is identical to above
		// except we are compuing for last year instead of current year.
		boost::gregorian::date lastyearfirst = boost::gregorian::date(d.year()-1,1,1);
		firstweekday = calc_mysql_weekday( d.year()-1, 1, 1, !( modeflags & WEEK_MONDAY_FIRST ) );
		firstoffset = firstweekday ? ( 7 - firstweekday ) : 0;
		baseday = lastyearfirst.julian_day() + firstoffset;

		// same logic as above
		if( modeflags & WEEK_GT_THREE_DAYS )
		{
			if( firstweekday > 0 && firstweekday < 4 )
			{
				baseday = baseday - 7;
			}
		}
		weeknum = (d.julian_day() - baseday)/ 7 + 1;

		if( weekyear )
			*weekyear = d.year() - 1;
	}
	else
	{
		weeknum = (d.julian_day() >= baseday) ? (d.julian_day() - baseday)/ 7 + 1 : 0;

		if( ( modeflags & WEEK_GT_THREE_DAYS ) &&
			( modeflags & WEEK_NO_ZERO ) && weeknum > 52 )
		{
			// if this happens we aren't sure whether the week will be 53 or 1.
			// to get 53, we know this has to be a December date and can subtract
			// our date from 32 to figure out how many days of this week
			uint32_t daysthisyear = 32 - d.day();
			uint32_t firstweekday = calc_mysql_weekday( d.year(), d.month(), d.day(), !( modeflags & 0x1 ) );
			// to be 1st week of next year there must be > 3 days of next year
			if( ( firstweekday + daysthisyear ) < 4 )
			{
				weeknum = 1;
				if( weekyear )
					*weekyear = d.year() + 1;
			}
		}
	}

	return weeknum;
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

    days= calc_mysql_daynr(year1, month1, day1);

    days-= l_sign*calc_mysql_daynr(year2, month2, day2);

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

inline int  getNumbers( const std::string& expr, int64_t *array, execplan::OpType funcType)
{
	int index = 0;

	int funcNeg = 1;
	if ( funcType == execplan::OP_SUB )
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




inline int  getNumbers( const std::string& expr, int *array, execplan::OpType funcType)
{
	int index = 0;

	int funcNeg = 1;
	if ( funcType == execplan::OP_SUB )
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

inline
string intToString(int64_t i)
{
    char buf[32];
#ifndef _MSC_VER
    snprintf(buf, 32, "%" PRId64 "", i);
#else
    snprintf(buf, 32, "%lld", i);
#endif
    return buf;
}

inline
string uintToString(uint64_t i)
{
    char buf[32];
#ifndef _MSC_VER
    snprintf(buf, 32, "%" PRIu64 "", i);
#else
    snprintf(buf, 32, "%llu", i);
#endif
    return buf;
}

inline
string doubleToString(double d)
{
    // double's can be *really* long to print out.  Max mysql
    // is e308 so allow for 308 + 36 decimal places minimum.
    char buf[384];
    snprintf(buf, 384, "%f", d);
    return buf;
}

inline
string decimalToString( execplan::IDB_Decimal x, int p )
{
    char buf[32];
#ifndef __LP64__
    if (x.scale > 0)
        snprintf( buf, 32, "%lld.%.*lld", x.value / p, (int) x.scale, x.value % p );
    else
        snprintf( buf, 32, "%lld", x.value * p );
#else
    if (x.scale > 0)
        snprintf( buf, 32, "%ld.%.*ld", x.value / p, (int) x.scale, x.value % p );
    else
        snprintf( buf, 32, "%ld", x.value * p );
#endif
    return buf;
}

uint64_t dateAdd( uint64_t time, const std::string& expr, execplan::IntervalColumn::interval_type unit, bool dateType, execplan::OpType funcType );
const std::string IDB_date_format(const dataconvert::DateTime&, const std::string&);
const std::string timediff(int64_t, int64_t);
const char *convNumToStr(int64_t, char*, int);

} //namespace funcexp::helpers
} //namespace funcexp

#endif
