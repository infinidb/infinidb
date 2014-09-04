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
* $Id: dataconvert.cpp 3763 2013-05-07 13:06:00Z dcathey $
*
*
****************************************************************************/

#include <string>
#include <cmath>
#include <errno.h>
#include <limits>
#include <ctime>
using namespace std;
#include <boost/algorithm/string/case_conv.hpp>
using namespace boost::algorithm;
#include <boost/tokenizer.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
using namespace boost::gregorian;
#include "calpontsystemcatalog.h"
#include "calpontselectexecutionplan.h"
#include "columnresult.h"
using namespace execplan;

#include "joblisttypes.h"

#include "errorcodes.h"
#include "exceptclasses.h"
#define DATACONVERT_DLLEXPORT
#include "dataconvert.h"
#undef DATACONVERT_DLLEXPORT

#ifndef __linux__
typedef u_long ulong;
#endif

using namespace logging;

namespace {

const short MIN_TINYINT = numeric_limits<int8_t>::min() + 2; //-126;
const short MAX_TINYINT = numeric_limits<int8_t>::max(); //127;
const short MIN_SMALLINT = numeric_limits<int16_t>::min() + 2; //-32766;
const short MAX_SMALLINT = numeric_limits<int16_t>::max(); //32767;
const int MIN_INT = numeric_limits<int32_t>::min() + 2; //-2147483646;
const int MAX_INT = numeric_limits<int32_t>::max(); //2147483647;
const long long MIN_BIGINT = numeric_limits<int64_t>::min() + 2; //-9223372036854775806LL;
const float MAX_FLOAT = numeric_limits<float>::max(); //3.402823466385289e+38
const float MIN_FLOAT = -numeric_limits<float>::max();
const double MAX_DOUBLE = numeric_limits<double>::max(); //1.797693134862316e+308
const double MIN_DOUBLE = -numeric_limits<double>::max();

const int64_t infinidb_precision[19] = {
0,
9,
99,
999,
9999,
99999,
999999,
9999999,
99999999,
999999999,
9999999999LL,
99999999999LL,
999999999999LL,
9999999999999LL,
99999999999999LL,
999999999999999LL,
9999999999999999LL,
99999999999999999LL,
999999999999999999LL
};

template <class T>
bool from_string(T& t, const std::string& s, std::ios_base& (*f)(std::ios_base&))
{
	std::istringstream iss(s);
	return !(iss >> f >> t).fail();
}

uint64_t pow10_(int32_t scale)
{
	if (scale <= 0) return 1;
	idbassert(scale < 20);
	uint64_t res = 1;
	for (int32_t i = 0; i < scale; i++)
		res *= 10;
	return res;
}

bool number_value ( const string& data )
{
	for (unsigned int i = 0; i < strlen(data.c_str()); i++)
	{
		if (data[i] > '9' || data[i] < '0')
		{
			if (data[i] != '+' && data[i] != '-' && data[i] != '.' && data[i] != ' ' &&
					data[i] != 'E' && data[i] != 'e')
			{
				throw QueryDataExcept("value is not numerical.", formatErr);
			}
		}
	}

	return true;
}


int64_t string_to_ll( const string& data, bool& pushwarning )
{
	char *ep = NULL;
	const char *str = data.c_str();
	errno = 0;
	int64_t value = strtoll(str, &ep, 10);

	//  (no digits) || (more chars)  || (other errors & value = 0)
	if ((ep == str) || (*ep != '\0') || (errno != 0 && value == 0))
		throw QueryDataExcept("value is not numerical.", formatErr);

	if (errno == ERANGE && (value == numeric_limits<int64_t>::max() || value == numeric_limits<int64_t>::min()))
		pushwarning = true;

	return value;
}

int64_t number_int_value(const string& data,
						 const CalpontSystemCatalog::ColType& ct,
						 bool& pushwarning,
						 bool  noRoundup)
{
	// copy of the original input
	string valStr(data);

	// in case, the values are in parentheses
	string::size_type x = valStr.find('(');
	string::size_type y = valStr.find(')');
	while (x < string::npos)
	{
		// erase y first
		if (y == string::npos)
			throw QueryDataExcept("'(' is not matched.", formatErr);
		valStr.erase(y, 1);
		valStr.erase(x, 1);
		x = valStr.find('(');
		y = valStr.find(')');
	}
	if (y != string::npos)
		throw QueryDataExcept("')' is not matched.", formatErr);

	// convert to fixed-point notation if input is in scientific notation
	if (valStr.find('E') < string::npos || valStr.find('e') < string::npos)
	{
		size_t epos = valStr.find('E');
		if (epos == string::npos)
			epos = valStr.find('e');

		// get the coefficient
		string coef = valStr.substr(0, epos);
		// get the exponent
		string exp = valStr.substr(epos+1);
		bool overflow = false;
		int64_t exponent = string_to_ll(exp, overflow);
		// if the exponent can not be hold in 64-bit, not supported or saturated.
		if (overflow)
			throw QueryDataExcept("value is invalid.", formatErr);

		// find the optional "." point
		size_t dpos = coef.find('.');
		if (dpos != string::npos)
		{
			// move "." to the end by mutiply 10 ** (# of franction digits)
			coef.erase(dpos, 1);
			exponent -= coef.length() - dpos;
		}

		if (exponent >= 0)
		{
			coef.resize(coef.length() + exponent, '0');
		}
		else
		{
			size_t bpos = coef.find_first_of("0123456789");
			size_t epos = coef.length();
			size_t mpos = -exponent;
			dpos = epos - mpos;
			int64_t padding = (int64_t)mpos - (int64_t)(epos-bpos);
			if (padding > 0)
			{
				coef.insert(bpos, padding, '0');
				dpos = bpos;
			}
			coef.insert(dpos, ".");
		}

		valStr = coef;
	}

	// apply the scale
	if (ct.scale != 0)
	{
		uint64_t scale = (uint64_t) (ct.scale < 0) ? (-ct.scale) : (ct.scale);
		size_t dpos = valStr.find('.');
		string intPart = valStr.substr(0, dpos);
		string leftStr;
		if (ct.scale > 0)
		{
			if (dpos != string::npos)
			{
				// decimal point exist, prepare "#scale" digits in fraction part
				++dpos;
				string frnStr = valStr.substr(dpos, scale);
				if (frnStr.length() < scale)
					frnStr.resize(scale, '0');  // padding digit 0, not null.

				// effectly shift "#scale" digits to left.
				intPart += frnStr;
				leftStr = valStr.substr(dpos);
				leftStr.erase(0, scale);
			}
			else
			{
				// no decimal point, shift "#scale" digits to left.
				intPart.resize(intPart.length()+scale, '0');  // padding digit 0, not null.
			}
		}
		else // if (ct.scale < 0) -- in ct.scale != 0 block
		{
			if (dpos != string::npos)
			{
				// decimal point exist, get the fraction part
				++dpos;
				leftStr = valStr.substr(dpos);
			}

// add above to keep the old behavior, to comply with tdriver
// uncomment code below to support negative scale
#if 0
			// check if enough digits in the integer part
			size_t spos = intPart.find_first_of("0123456789");
			if (string::npos == spos)
				spos = intPart.length();
			size_t len = intPart.substr(spos).length();
			if (len < scale)
				intPart.insert(spos, scale - len, '0');  // padding digit 0, not null.

			leftStr = intPart.substr(intPart.length()-scale) + leftStr;
			intPart.erase(intPart.length()-scale, scale);
#endif
		}

		valStr = intPart;
		if (leftStr.length() > 0)
			valStr += "." + leftStr;
	}

	// now, convert to long long int
	string intStr(valStr);
	string frnStr = "";
	size_t dp = valStr.find('.');
	int    roundup = 0;
	if (dp != string::npos)
	{
		//Check if need round up
		int frac1 = string_to_ll(valStr.substr(dp+1, 1), pushwarning);
		if ((!noRoundup) && frac1 >= 5)
			roundup = 1;

		intStr.erase(dp);
		frnStr = valStr.substr(dp+1);
		if ( intStr.length() == 0 )
			intStr = "0";
		else if (( intStr.length() == 1 ) && ( (intStr[0] == '+') || (intStr[0] == '-') ) )
		{
			intStr.insert( 1, 1, '0');
		}
	}

	int64_t intVal = string_to_ll(intStr, pushwarning);
	//@Bug 3350 negative value round up.
	intVal += intVal>=0?roundup:-roundup;
	bool dummy = false;
	int64_t frnVal = (frnStr.length() > 0) ? string_to_ll(frnStr, dummy) : 0;
	if (frnVal != 0)
		pushwarning = true;

	switch (ct.colDataType)
	{
		case CalpontSystemCatalog::TINYINT:
			if (intVal < MIN_TINYINT)
			{
		 		intVal = MIN_TINYINT;
				pushwarning = true;
			}
			else if (intVal > MAX_TINYINT)
			{
				intVal = MAX_TINYINT;
				pushwarning = true;
			}
			break;
		case CalpontSystemCatalog::SMALLINT:
			if (intVal < MIN_SMALLINT)
			{
				intVal = MIN_SMALLINT;
				pushwarning = true;
			}
			else if (intVal > MAX_SMALLINT)
			{
				intVal = MAX_SMALLINT;
				pushwarning = true;
			}
			break;
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			if (intVal < MIN_INT)
			{
				intVal = MIN_INT;
				pushwarning = true;
			}
			else if (intVal > MAX_INT)
			{
				intVal = MAX_INT;
				pushwarning = true;
			}
			break;
		case CalpontSystemCatalog::BIGINT:
			if (intVal < MIN_BIGINT)
			{
				intVal = MIN_BIGINT;
				pushwarning = true;
			}
			break;
		case CalpontSystemCatalog::DECIMAL:
			if (ct.colWidth == 1)
			{
					if (intVal < MIN_TINYINT)
					{
						intVal = MIN_TINYINT;
						pushwarning = true;
					}
					else if (intVal > MAX_TINYINT)
					{
						intVal = MAX_TINYINT;
						pushwarning = true;
					}
			}
			else if (ct.colWidth == 2)
			{
				if (intVal < MIN_SMALLINT)
				{
					intVal = MIN_SMALLINT;
					pushwarning = true;
				}
				else if (intVal > MAX_SMALLINT)
				{
					intVal = MAX_SMALLINT;
					pushwarning = true;
				}
			}
			else if (ct.colWidth == 4)
			{
				if (intVal < MIN_INT)
				{
					intVal = MIN_INT;
					pushwarning = true;
				}
				else if (intVal > MAX_INT)
				{
					intVal = MAX_INT;
					pushwarning = true;
				}
			}
			else if (ct.colWidth == 8)
			{
				if (intVal < MIN_BIGINT)
				{
					intVal = MIN_BIGINT;
					pushwarning = true;
				}
			}
			break;
		default:
			break;
	}

	// @ bug 3285 make sure the value is in precision range for decimal data type
	if ( (ct.colDataType == CalpontSystemCatalog::DECIMAL) || (ct.scale > 0))
	{
		int64_t rangeUp = infinidb_precision[ct.precision];
		int64_t rangeLow = -rangeUp;

		if (intVal > rangeUp)
		{
			intVal = rangeUp;
			pushwarning = true;
		}
		else if (intVal < rangeLow)
		{
			intVal = rangeLow;
			pushwarning = true;
		}
	}
	return intVal;
}

bool isLeapYear ( int year)
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

bool isDateValid ( int day, int month, int year)
{
	bool valid = true;
	int month_length[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

	if (month == 2)
	{
		if ( isLeapYear(year) )
			month_length[2] = 29;
			//  29 days in February in a leap year (including year 2000)
	}
	if ( ( year < 1000 ) || ( year > 9999 ) )
		valid = false;
	else if ( month < 1 || month > 12 )
		valid = false;
	else if ( day < 1 || day > month_length[month] )
		valid = false;

	return ( valid );
}

bool isDateTimeValid ( int hour, int minute, int second, int microSecond)
{
	bool valid = false;
	if ( hour >= 0 && hour <= 24 )
	{
		if ( minute >= 0 && minute < 60 )
		{
			if ( second >= 0 && second < 60 )
			{
				if ( microSecond >= 0 && microSecond <= 999999 )
				{
					valid = true;
				}
			}
		}
	}
	return valid;
}


} // namespace anon

namespace dataconvert
{

// various code from my_time.h, my_time.c, m_ctype.h, and ctype-latin1.c
// to get a faster str_to_datetime

typedef enum mysql_timestamp_type
{
	MYSQL_TIMESTAMP_NONE,        // String wasn't a timestamp
	MYSQL_TIMESTAMP_DATE,        // DATE string (YY MM and DD parts ok)
	MYSQL_TIMESTAMP_DATETIME,    // Full timestamp
	MYSQL_TIMESTAMP_ERROR
} mysql_timestamp_type;

typedef unsigned char uchar;
typedef bool my_bool;

const uint MAX_DATE_PARTS = 8;
static unsigned char internal_format_positions[]=
{0, 1, 2, 3, 4, 5, 6, 255};

const unsigned YY_PART_YEAR = 70;
uchar days_in_month[]= {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};

#define ULL(A) A ## ULL

uint64_t log_10_int[20]=
{
  1, 10, 100, 1000, 10000UL, 100000UL, 1000000UL, 10000000UL,
  ULL(100000000), ULL(1000000000), ULL(10000000000), ULL(100000000000),
  ULL(1000000000000), ULL(10000000000000), ULL(100000000000000),
  ULL(1000000000000000), ULL(10000000000000000), ULL(100000000000000000),
  ULL(1000000000000000000), ULL(10000000000000000000)
};

// Pulled this over from ctype-latin1.c for implementing the my_isdigit,
// my_isspace, etc. provides some performance gain over calling std functions
// in ctype.h.   Leading '0' left but commented out from mysql impl as
// the original #defines always jumped right over it anyway
static uchar ctype_latin1[] = {
   // 0,
   32, 32, 32, 32, 32, 32, 32, 32, 32, 40, 40, 40, 40, 40, 32, 32,
   32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
   72, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
  132,132,132,132,132,132,132,132,132,132, 16, 16, 16, 16, 16, 16,
   16,129,129,129,129,129,129,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, 16, 16, 16, 16, 16,
   16,130,130,130,130,130,130,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, 16, 16, 16, 16, 32,
   16,  0, 16,  2, 16, 16, 16, 16, 16, 16,  1, 16,  1,  0,  1,  0,
    0, 16, 16, 16, 16, 16, 16, 16, 16, 16,  2, 16,  2,  0,  2,  1,
   72, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
   16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    1,  1,  1,  1,  1,  1,  1, 16,  1,  1,  1,  1,  1,  1,  1,  2,
    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  2,  2,  2,  2,  2, 16,  2,  2,  2,  2,  2,  2,  2,  2
};

#define	_MY_NMR	04	/* Numeral (digit) */
#define	_MY_SPC	010	/* Spacing character */
#define	_MY_PNT	020	/* Punctuation */

#define	my_isdigit(c)  (ctype_latin1[(uchar) (c)] & _MY_NMR)
#define	my_isspace(c)  (ctype_latin1[(uchar) (c)] & _MY_SPC)
#define	my_ispunct(c)  (ctype_latin1[(uchar) (c)] & _MY_PNT)

/* Flags to str_to_datetime */
#define TIME_FUZZY_DATE		1
#define TIME_DATETIME_ONLY	2
/* Must be same as MODE_NO_ZERO_IN_DATE */
#define TIME_NO_ZERO_IN_DATE    (65536L*2*2*2*2*2*2*2)
/* Must be same as MODE_NO_ZERO_DATE */
#define TIME_NO_ZERO_DATE	(TIME_NO_ZERO_IN_DATE*2)
#define TIME_INVALID_DATES	(TIME_NO_ZERO_DATE*2)

#define DBUG_RETURN return
#define set_if_bigger(a,b)  do { if ((a) < (b)) (a)=(b); } while(0)

// Need to use an alternate data structure (as opposed to Date/DateTime
// because mysql implementations expect to assign to fields and then
// do error checking and thus field lengths cannot be constrained as
// they are in the native IDB structs
typedef struct st_mysql_time
{
  unsigned int  year, month, day, hour, minute, second;
  unsigned long second_part;

  st_mysql_time() :
	  year(0),
	  month(0),
	  day(0),
	  hour(0),
	  minute(0),
	  second(0),
	  second_part(0)
  	  {}
} MYSQL_TIME;

uint calc_days_in_year(uint year)
{
  return ((year & 3) == 0 && (year%100 || (year%400 == 0 && year)) ?
          366 : 365);
}

my_bool check_date(const MYSQL_TIME *ltime, my_bool not_zero_date,
                   ulong flags, int *was_cut)
{
  if (not_zero_date)
  {
    if ((((flags & TIME_NO_ZERO_IN_DATE) || !(flags & TIME_FUZZY_DATE)) &&
         (ltime->month == 0 || ltime->day == 0)) ||
        (!(flags & TIME_INVALID_DATES) &&
         ltime->month && ltime->day > days_in_month[ltime->month-1] &&
         (ltime->month != 2 || calc_days_in_year(ltime->year) != 366 ||
          ltime->day != 29)))
    {
      *was_cut= 2;
      return true;
    }
  }
  else if (flags & TIME_NO_ZERO_DATE)
  {
    /*
      We don't set *was_cut here to signal that the problem was a zero date
      and not an invalid date
    */
    return true;
  }
  return false;
}

/*
  Convert a timestamp string to a MYSQL_TIME value.

  SYNOPSIS
    str_to_datetime()
    str                 String to parse
    length              Length of string
    l_time              Date is stored here
    flags               Bitmap of following items
                        TIME_FUZZY_DATE    Set if we should allow partial dates
                        TIME_DATETIME_ONLY Set if we only allow full datetimes.
                        TIME_NO_ZERO_IN_DATE	Don't allow partial dates
                        TIME_NO_ZERO_DATE	Don't allow 0000-00-00 date
                        TIME_INVALID_DATES	Allow 2000-02-31
    was_cut             0	Value OK
			1   If value was cut during conversion (i.e. extraneous trailing text found)
			2	check_date(date,flags) considers date invalid

  DESCRIPTION
    At least the following formats are recogniced (based on number of digits)
    YYMMDD, YYYYMMDD, YYMMDDHHMMSS, YYYYMMDDHHMMSS
    YY-MM-DD, YYYY-MM-DD, YY-MM-DD HH.MM.SS
    YYYYMMDDTHHMMSS  where T is a the character T (ISO8601)
    Also dates where all parts are zero are allowed

    The second part may have an optional .###### fraction part.

  NOTES
   This function should work with a format position vector as long as the
   following things holds:
   - All date are kept together and all time parts are kept together
   - Date and time parts must be separated by blank
   - Second fractions must come after second part and be separated
     by a '.'.  (The second fractions are optional)
   - AM/PM must come after second fractions (or after seconds if no fractions)
   - Year must always been specified.
   - If time is before date, then we will use datetime format only if
     the argument consist of two parts, separated by space.
     Otherwise we will assume the argument is a date.
   - The hour part must be specified in hour-minute-second order.

  RETURN VALUES
    MYSQL_TIMESTAMP_NONE        String wasn't a timestamp, like
                                [DD [HH:[MM:[SS]]]].fraction.
                                l_time is not changed.
    MYSQL_TIMESTAMP_DATE        DATE string (YY MM and DD parts ok)
    MYSQL_TIMESTAMP_DATETIME    Full timestamp
    MYSQL_TIMESTAMP_ERROR       Timestamp with wrong values.
                                All elements in l_time is set to 0
*/

mysql_timestamp_type
str_to_datetime(const char *str, uint length, MYSQL_TIME* l_time,
                uint flags, int *was_cut)
{
  uint field_length, year_length=0, digits, i, number_of_fields;
  uint date[MAX_DATE_PARTS], date_len[MAX_DATE_PARTS];
  uint add_hours= 0, start_loop;
  ulong not_zero_date, allow_space;
  bool is_internal_format;
  const char *pos, *last_field_pos=0;
  const char *end=str+length;
  const unsigned char *format_position;
  bool found_delimitier= 0, found_space= 0;
  uint frac_pos, frac_len;
  mysql_timestamp_type time_type=MYSQL_TIMESTAMP_ERROR;
  // DBUG_ENTER("str_to_datetime");
  // DBUG_PRINT("ENTER",("str: %.*s",length,str));


  // LINT_INIT(field_length);
  // LINT_INIT(last_field_pos);

  *was_cut= 0;

  /* Skip space at start */
  for (; str != end && my_isspace(*str) ; str++)
    ;
  if (str == end || ! my_isdigit(*str))
  {
    *was_cut= 1;
    return MYSQL_TIMESTAMP_NONE;
  }

  is_internal_format= 0;
  /* This has to be changed if want to activate different timestamp formats */
  format_position= internal_format_positions;

  /*
    Calculate number of digits in first part.
    If length= 8 or >= 14 then year is of format YYYY.
    (YYYY-MM-DD,  YYYYMMDD, YYYYYMMDDHHMMSS)
  */
  for (pos=str;
	   pos != end && (my_isdigit(*pos) || *pos == 'T');
       pos++)
    ;

  digits= (uint) (pos-str);
  start_loop= 0;                                /* Start of scan loop */
  date_len[format_position[0]]= 0;              /* Length of year field */
  if (pos == end || *pos == '.')
  {
    /* Found date in internal format (only numbers like YYYYMMDD) */
    year_length= (digits == 4 || digits == 8 || digits >= 14) ? 4 : 2;
    field_length= year_length;
    is_internal_format= 1;
    format_position= internal_format_positions;
  }
  else
  {
    if (format_position[0] >= 3)                /* If year is after HHMMDD */
    {
      /*
        If year is not in first part then we have to determinate if we got
        a date field or a datetime field.
        We do this by checking if there is two numbers separated by
        space in the input.
      */
      while (pos < end && !my_isspace(*pos))
        pos++;
      while (pos < end && !my_isdigit(*pos))
        pos++;
      if (pos == end)
      {
        if (flags & TIME_DATETIME_ONLY)
        {
          *was_cut= 1;
          DBUG_RETURN(MYSQL_TIMESTAMP_NONE);   /* Can't be a full datetime */
        }
        /* Date field.  Set hour, minutes and seconds to 0 */
        date[0]= date[1]= date[2]= date[3]= date[4]= 0;
        start_loop= 5;                         /* Start with first date part */
      }
    }

    field_length= format_position[0] == 0 ? 4 : 2;
  }

  /*
    Only allow space in the first "part" of the datetime field and:
    - after days, part seconds
    - before and after AM/PM (handled by code later)

    2003-03-03 20:00:20 AM
    20:00:20.000000 AM 03-03-2000
  */
  i= max((uint) format_position[0], (uint) format_position[1]);
  set_if_bigger(i, (uint) format_position[2]);
  allow_space= ((1 << i) | (1 << format_position[6]));
  allow_space&= (1 | 2 | 4 | 8);

  not_zero_date= 0;
  for (i = start_loop;
       i < MAX_DATE_PARTS-1 && str != end &&
         my_isdigit(*str);
       i++)
  {
    const char *start= str;
    ulong tmp_value= (uint) (unsigned char) (*str++ - '0');

    /*
      Internal format means no delimiters; every field has a fixed
      width. Otherwise, we scan until we find a delimiter and discard
      leading zeroes -- except for the microsecond part, where leading
      zeroes are significant, and where we never process more than six
      digits.
    */
    bool     scan_until_delim= !is_internal_format &&
                                  ((i != format_position[6]));

    while (str != end && my_isdigit(str[0]) &&
           (scan_until_delim || --field_length))
    {
      tmp_value=tmp_value*10 + (ulong) (unsigned char) (*str - '0');
      str++;
    }
    date_len[i]= (uint) (str - start);
    if (tmp_value > 999999)                     /* Impossible date part */
    {
      *was_cut= 1;
      DBUG_RETURN(MYSQL_TIMESTAMP_NONE);
    }
    date[i]=tmp_value;
    not_zero_date|= tmp_value;

    /* Length of next field */
    field_length= format_position[i+1] == 0 ? 4 : 2;

    if ((last_field_pos= str) == end)
    {
      i++;                                      /* Register last found part */
      break;
    }
    /* Allow a 'T' after day to allow CCYYMMDDT type of fields */
    if (i == format_position[2] && *str == 'T')
    {
      str++;                                    /* ISO8601:  CCYYMMDDThhmmss */
      continue;
    }
    if (i == format_position[5])                /* Seconds */
    {
      if (*str == '.')                          /* Followed by part seconds */
      {
        str++;
        field_length= 6;                        /* 6 digits */
      }
      continue;
    }
    while (str != end &&
           (my_ispunct(*str) ||
            my_isspace(*str)))
    {
      if (my_isspace(*str))
      {
        if (!(allow_space & (1 << i)))
        {
          *was_cut= 1;
          DBUG_RETURN(MYSQL_TIMESTAMP_NONE);
        }
        found_space= 1;
      }
      str++;
      found_delimitier= 1;                      /* Should be a 'normal' date */
    }
    /* Check if next position is AM/PM */
    if (i == format_position[6])                /* Seconds, time for AM/PM */
    {
      i++;                                      /* Skip AM/PM part */
      if (format_position[7] != 255)            /* If using AM/PM */
      {
        if (str+2 <= end && (str[1] == 'M' || str[1] == 'm'))
        {
          if (str[0] == 'p' || str[0] == 'P')
            add_hours= 12;
          else if (str[0] != 'a' || str[0] != 'A')
            continue;                           /* Not AM/PM */
          str+= 2;                              /* Skip AM/PM */
          /* Skip space after AM/PM */
          while (str != end && my_isspace(*str))
            str++;
        }
      }
    }
    last_field_pos= str;
  }
  if (found_delimitier && !found_space && (flags & TIME_DATETIME_ONLY))
  {
    *was_cut= 1;
    DBUG_RETURN(MYSQL_TIMESTAMP_NONE);          /* Can't be a datetime */
  }

  str= last_field_pos;

  number_of_fields= i - start_loop;
  while (i < MAX_DATE_PARTS)
  {
    date_len[i]= 0;
    date[i++]= 0;
  }

  if (!is_internal_format)
  {
    year_length= date_len[(uint) format_position[0]];
    if (!year_length)                           /* Year must be specified */
    {
      *was_cut= 1;
      DBUG_RETURN(MYSQL_TIMESTAMP_NONE);
    }

    l_time->year=               date[(uint) format_position[0]];
    l_time->month=              date[(uint) format_position[1]];
    l_time->day=                date[(uint) format_position[2]];
    l_time->hour=               date[(uint) format_position[3]];
    l_time->minute=             date[(uint) format_position[4]];
    l_time->second=             date[(uint) format_position[5]];

    frac_pos= (uint) format_position[6];
    frac_len= date_len[frac_pos];
    if (frac_len < 6)
      date[frac_pos]*= (uint) log_10_int[6 - frac_len];
    l_time->second_part= date[frac_pos];

    if (format_position[7] != (uchar) 255)
    {
      if (l_time->hour > 12)
      {
        *was_cut= 1;
        goto err;
      }
      l_time->hour= l_time->hour%12 + add_hours;
    }
  }
  else
  {
    l_time->year=       date[0];
    l_time->month=      date[1];
    l_time->day=        date[2];
    l_time->hour=       date[3];
    l_time->minute=     date[4];
    l_time->second=     date[5];
    if (date_len[6] < 6)
      date[6]*= (uint) log_10_int[6 - date_len[6]];
    l_time->second_part=date[6];
  }
  // in mysql code but unused
  // l_time->neg= 0;

  if (year_length == 2 && not_zero_date)
    l_time->year+= (l_time->year < YY_PART_YEAR ? 2000 : 1900);

  if (number_of_fields < 3 ||
      l_time->year > 9999 || l_time->month > 12 ||
      l_time->day > 31 || l_time->hour > 23 ||
      l_time->minute > 59 || l_time->second > 59)
  {
    /* Only give warning for a zero date if there is some garbage after */
    if (!not_zero_date)                         /* If zero date */
    {
      for (; str != end ; str++)
      {
        if (!my_isspace(*str))
        {
          not_zero_date= 1;                     /* Give warning */
          break;
        }
      }
    }
    *was_cut= (not_zero_date == 1);
    goto err;
  }

  if (check_date(l_time, not_zero_date != 0, flags, was_cut))
    goto err;

  // use return value for this
  time_type= (number_of_fields <= 3 ?
                      MYSQL_TIMESTAMP_DATE : MYSQL_TIMESTAMP_DATETIME);

  for (; str != end ; str++)
  {
    if (!my_isspace(*str))
    {
      *was_cut= 1;
      break;
    }
  }

  DBUG_RETURN(time_type);

err:
  memset(l_time, 0, sizeof(*l_time));
  DBUG_RETURN(MYSQL_TIMESTAMP_ERROR);
}

bool stringToDateStruct( const string& data, Date& date )
{
	MYSQL_TIME mtime;
	int		   was_cut = 0;
	mysql_timestamp_type ttype = str_to_datetime(data.c_str(), strlen(data.c_str()),&mtime,TIME_NO_ZERO_IN_DATE | TIME_NO_ZERO_DATE, &was_cut);
	if (ttype == MYSQL_TIMESTAMP_ERROR || ttype == MYSQL_TIMESTAMP_NONE || was_cut)
		return false;

	date.year = mtime.year;
	date.month = mtime.month;
	date.day = mtime.day;
	return true;
}

bool stringToDatetimeStruct(const string& data, DateTime& dtime, bool* date)
{
	MYSQL_TIME mtime;
	int		   was_cut = 0;
	mysql_timestamp_type ttype = str_to_datetime(data.c_str(), strlen(data.c_str()),&mtime,TIME_NO_ZERO_IN_DATE | TIME_NO_ZERO_DATE, &was_cut);

	if (ttype == MYSQL_TIMESTAMP_ERROR || ttype == MYSQL_TIMESTAMP_NONE || was_cut)
		return false;
	else if (ttype == MYSQL_TIMESTAMP_DATE)
	{
		if (date)
			*date = true;

		dtime.hour = 0;
		dtime.minute = 0;
		dtime.second = 0;
		dtime.msecond = 0;
	}
	else
	{
		dtime.hour = mtime.hour;
		dtime.minute = mtime.minute;
		dtime.second = mtime.second;
		dtime.msecond = mtime.second_part;
	}
	dtime.year = mtime.year;
	dtime.month = mtime.month;
	dtime.day = mtime.day;

	return true;
}

boost::any
	DataConvert::convertColumnData( execplan::CalpontSystemCatalog::ColType colType,
	const std::string& dataOrig, bool& pushWarning, bool nulFlag, bool noRoundup )
{
	boost::any value;
	std::string data( dataOrig );
	pushWarning = false;
	execplan::CalpontSystemCatalog::ColDataType type = colType.colDataType;
	//if ( !data.empty() )
	if (!nulFlag)
	{
		switch(type)
		{
			case execplan::CalpontSystemCatalog::BIT:
			{
				unsigned int x = data.find("(");
				if (x <= data.length())
				{
					data.replace ( x, 1, " ");
				}

				x = data.find(")");
				if (x <= data.length())
				{
					data.replace (x, 1, " ");
				}

				if (number_int_value (data, colType, pushWarning, noRoundup))
				{
					bool bitvalue;
					if (from_string<bool>(bitvalue, data, std::dec ))
					{
						value = bitvalue;
					}
					else
					{
						throw QueryDataExcept("range, valid value or conversion error on BIT type.", formatErr);
					}
				}
			}
			break;

			case execplan::CalpontSystemCatalog::TINYINT:
				value = (char) number_int_value(data, colType, pushWarning, noRoundup);
			break;

			case execplan::CalpontSystemCatalog::SMALLINT:
				value = (short) number_int_value(data, colType, pushWarning, noRoundup);
			break;

			case execplan::CalpontSystemCatalog::MEDINT:
			case execplan::CalpontSystemCatalog::INT:
				value = (int) number_int_value(data, colType, pushWarning, noRoundup);
			break;

			case execplan::CalpontSystemCatalog::BIGINT:
				value = (long long) number_int_value(data, colType, pushWarning, noRoundup);
			break;
			case execplan::CalpontSystemCatalog::DECIMAL:
				if (colType.colWidth == 1)
					value = (char) number_int_value(data, colType, pushWarning, noRoundup);
				else if (colType.colWidth == 2)
					value = (short) number_int_value(data, colType, pushWarning, noRoundup);
				else if (colType.colWidth == 4)
					value = (int) number_int_value(data, colType, pushWarning, noRoundup);
				else if (colType.colWidth == 8)
					value = (long long) number_int_value(data, colType, pushWarning, noRoundup);
			break;
			case execplan::CalpontSystemCatalog::FLOAT:
			{
				string::size_type x = data.find('(');
				if (x < string::npos)
					data.erase(x, 1);
				x = data.find(')');
				if (x < string::npos)
					data.erase(x, 1);
				if ( number_value ( data ) )
				{
					float floatvalue;
					errno = 0;
#ifdef _MSC_VER
					floatvalue = (float)strtod(data.c_str(), 0);
#else
					floatvalue = strtof(data.c_str(), 0);
#endif
					if (errno == ERANGE)
					{
						pushWarning = true;
#ifdef _MSC_VER
						if ( abs(floatvalue) == HUGE_VAL )
#else
						if ( abs(floatvalue) == HUGE_VALF )
#endif
						{
							if ( floatvalue > 0 )
								floatvalue = MAX_FLOAT;
							else
								floatvalue = MIN_FLOAT;
						}
						else
							floatvalue = 0;
					}

					value = floatvalue;
				}
				else
					throw QueryDataExcept("range, valid value or conversion error on FLOAT type.", formatErr);
			}
			break;

			case execplan::CalpontSystemCatalog::DOUBLE:
			{
				string::size_type x = data.find('(');
				if (x < string::npos)
					data.erase(x, 1);
				x = data.find(')');
				if (x < string::npos)
					data.erase(x, 1);
				if ( number_value ( data ) )
				{
					double doublevalue;
					errno = 0;
					doublevalue = strtod(data.c_str(), 0);
					if (errno == ERANGE)
					{
						pushWarning = true;
#ifdef _MSC_VER
						if ( abs(doublevalue) == HUGE_VAL )
#else
						if ( abs(doublevalue) == HUGE_VALL )
#endif
						{
							if ( doublevalue > 0 )
								value = MAX_DOUBLE;
							else
								value = MIN_DOUBLE;
						}
						else
							value = 0;
					}
					else
						value = doublevalue;
				}
				else
				{
					throw QueryDataExcept("range, valid value or conversion error on DOUBLE type.", formatErr);
				}
			}
			break;

			case execplan::CalpontSystemCatalog::CHAR:
			case execplan::CalpontSystemCatalog::VARCHAR:
			{
				//check data length
				if ( data.length() > (unsigned int)colType.colWidth )
				{
					data = data.substr(0, colType.colWidth);
					pushWarning = true;
				}
				else
				{
					if ( (unsigned int)colType.colWidth > data.length())
					{
						//Pad null character to the string
						data.resize(colType.colWidth, 0);
					}
				}
				value = data;
			}
			break;

			case execplan::CalpontSystemCatalog::DATE:
			{
				if (data == "0000-00-00")  //@Bug 3210 Treat blank date as null
				{
					uint32_t d = joblist::DATENULL;
					value = d;
					break;
				}

				Date aDay;
				if (stringToDateStruct(data, aDay))
				{
					value = (*(reinterpret_cast<uint32_t *> (&aDay)));
				}
				else
				{
					throw QueryDataExcept("Invalid date", formatErr);
				}
			}
			break;

			case execplan::CalpontSystemCatalog::DATETIME:
			{
				if (data == "0000-00-00 00:00:00")  //@Bug 3210 Treat blank date as null
				{
					uint64_t d = joblist::DATETIMENULL;
					value = d;
					break;
				}

				DateTime aDatetime;
				if (stringToDatetimeStruct(data, aDatetime, 0))
				{
					value = *(reinterpret_cast<uint64_t*>(&aDatetime));
				}
				else
				{
					throw QueryDataExcept("Invalid datetime", formatErr);
				}
			}
			break;
			case execplan::CalpontSystemCatalog::BLOB:
			case execplan::CalpontSystemCatalog::CLOB:
				value = data;
			break;
			case execplan::CalpontSystemCatalog::VARBINARY:
{
//const char* p = dataOrig.data();
//cerr << "dataOrig: ";
//for (size_t i = 0; i < dataOrig.size(); i++, p++)
//{
//	if (isprint(*p)) cerr << *p << ' ';
//	else {unsigned u = *p; u &= 0xff; cerr << "0x" << hex << u << dec << ' ';}
//}
//cerr << endl;
//p = data.data();
//cerr << "data: ";
//for (size_t i = 0; i < data.size(); i++, p++)
//{
//	if (isprint(*p)) cerr << *p << ' ';
//	else {unsigned u = *p; u &= 0xff; cerr << "0x" << hex << u << dec << ' ';}
//}
//cerr << endl;
				value = data;
}
			break;
			default:
				throw QueryDataExcept("convertColumnData: unknown column data type.", dataTypeErr);
			break;
		}
	}
	else									//null
	{
			switch (type)
			{
				case execplan::CalpontSystemCatalog::BIT:
				{
					//TODO: How to communicate with write engine?
				}
				break;
				case execplan::CalpontSystemCatalog::TINYINT:
				{
					char tinyintvalue = joblist::TINYINTNULL;
					value = tinyintvalue;
				}
				break;
				case execplan::CalpontSystemCatalog::SMALLINT:
				{
					short smallintvalue = joblist::SMALLINTNULL;
					value = smallintvalue;
				}
				break;
				case execplan::CalpontSystemCatalog::MEDINT:
				case execplan::CalpontSystemCatalog::INT:
				{
					int intvalue = joblist::INTNULL;
					value = intvalue;
				}
				break;
				case execplan::CalpontSystemCatalog::BIGINT:
				{
					long long bigint = joblist::BIGINTNULL;
					value = bigint;
				}
				break;
				case execplan::CalpontSystemCatalog::DECIMAL:
				{
					if (colType.colWidth == execplan::CalpontSystemCatalog::ONE_BYTE)
					{
						char tinyintvalue = joblist::TINYINTNULL;
						value = tinyintvalue;
					}
					else if (colType.colWidth ==execplan::CalpontSystemCatalog::TWO_BYTE)
					{
						short smallintvalue = joblist::SMALLINTNULL;
						value = smallintvalue;
					}
					else if (colType.colWidth ==execplan::CalpontSystemCatalog::FOUR_BYTE)
					{
						int intvalue = joblist::INTNULL;
						value = intvalue;
					}
					else if (colType.colWidth ==execplan::CalpontSystemCatalog::EIGHT_BYTE)
					{
						long long eightbyte = joblist::BIGINTNULL;
						value = eightbyte;
					}
					else
					{
						WriteEngine::Token nullToken;
						value = nullToken;
					}
				}
				break;
				case execplan::CalpontSystemCatalog::FLOAT:
				{
					uint32_t tmp = joblist::FLOATNULL;
					float* floatvalue = (float*)&tmp;
					value = *floatvalue;
				}
				break;
				case execplan::CalpontSystemCatalog::DOUBLE:
				{
					uint64_t tmp = joblist::DOUBLENULL;
					double* doublevalue = (double*)&tmp;
					value = *doublevalue;
				}
				break;
				case execplan::CalpontSystemCatalog::DATE:
				{
					uint32_t d = joblist::DATENULL;
					value = d;
				}
				break;
				case execplan::CalpontSystemCatalog::DATETIME:
				{
					uint64_t d = joblist::DATETIMENULL;
					value = d;
				}
				break;
				case execplan::CalpontSystemCatalog::CHAR:
				{
					std::string charnull;
					if (colType.colWidth == 1)
					{
						//charnull = joblist::CHAR1NULL;
						charnull = '\376';
						value = charnull;
					}
					else if (colType.colWidth == 2)
					{
						//charnull = joblist::CHAR2NULL;
						charnull = "\377\376";
						value = charnull;
					}
					else if (( colType.colWidth < 5 ) && ( colType.colWidth > 2 ))
					{
						//charnull = joblist::CHAR4NULL;
						charnull = "\377\377\377\376";
						value = charnull;
					}
					else if (( colType.colWidth < 9 ) && ( colType.colWidth > 4 ))
					{
						//charnull = joblist::CHAR8NULL;
						charnull = "\377\377\377\377\377\377\377\376";
						value = charnull;
					}
					else
					{
						WriteEngine::Token nullToken;
						value = nullToken;
					}
				}
				break;
				case execplan::CalpontSystemCatalog::VARCHAR:
				{
					std::string charnull;
					if (colType.colWidth == 1 )
					{
						//charnull = joblist::CHAR2NULL;
						charnull = "\377\376";
						value = charnull;
					}
					else if ((colType.colWidth < 4)  && (colType.colWidth > 1))
					{
						//charnull = joblist::CHAR4NULL;
						charnull = "\377\377\377\376";
						value = charnull;
					}
					else if ((colType.colWidth < 8)  && (colType.colWidth > 3))
					{
						//charnull = joblist::CHAR8NULL;
						charnull = "\377\377\377\377\377\377\377\376";
						value = charnull;
					}
					else if ( colType.colWidth > 7 )
					{
						WriteEngine::Token nullToken;
						value = nullToken;
					}
				}
				break;
				case execplan::CalpontSystemCatalog::VARBINARY:
				{
					WriteEngine::Token nullToken;
					value = nullToken;
				}
				break;
				default:
					throw QueryDataExcept("convertColumnData: unknown column data type.", dataTypeErr);
				break;

			}

	}

	return value;
}

//------------------------------------------------------------------------------
// Convert date string to binary date.  Used by BulkLoad.
//------------------------------------------------------------------------------
uint32_t DataConvert::convertColumnDate(
	const char* dataOrg,
	CalpontDateTimeFormat dateFormat,
	int& status,
	unsigned int dataOrgLen )
{
	status = 0;
	const char* p;
	p = dataOrg;
	char fld[10];
	uint32_t value = 0;
	if ( dateFormat != CALPONTDATE_ENUM )
	{
		status = -1;
		return value;
	}

	if ( dataOrgLen < 10)
	{
		status = -1;
		return value;
	}

	int inYear, inMonth, inDay;
	memcpy( fld, p, 4);
	fld[4] = '\0';

	inYear = strtol(fld, 0, 10);

	memcpy( fld, p+5, 2);
	fld[2] = '\0';

	inMonth = strtol(fld, 0, 10);

	memcpy( fld, p+8, 2);
	fld[2] = '\0';

	inDay = strtol(fld, 0, 10);

	if ( isDateValid (inDay, inMonth, inYear))
	{
		Date aDay;
		aDay.year = inYear;
		aDay.month = inMonth;
		aDay.day = inDay;
		memcpy( &value, &aDay, 4);
	}
	else
	{
		status = -1;
	}

	return value;
}

//------------------------------------------------------------------------------
// Convert date/time string to binary date/time.  Used by BulkLoad.
//------------------------------------------------------------------------------
uint64_t DataConvert::convertColumnDatetime(
	const char* dataOrg,
	CalpontDateTimeFormat datetimeFormat,
	int& status,
	unsigned int dataOrgLen )
{
	status = 0;
	const char* p;
	p = dataOrg;
	char fld[10];
	uint64_t value = 0;
	if ( datetimeFormat != CALPONTDATETIME_ENUM )
	{
		status = -1;
		return value;
	}

	if ( dataOrgLen < 10)
	{
		status = -1;
		return value;
	}

	int inYear, inMonth, inDay, inHour, inMinute, inSecond, inMicrosecond;
	memcpy( fld, p, 4);
	fld[4] = '\0';

	inYear = strtol(fld, 0, 10);

	memcpy( fld, p+5, 2);
	fld[2] = '\0';

	inMonth = strtol(fld, 0, 10);

	memcpy( fld, p+8, 2);
	fld[2] = '\0';

	inDay = strtol(fld, 0, 10);

	int len = dataOrgLen;

	inHour        = 0;
	inMinute      = 0;
	inSecond      = 0;
	inMicrosecond = 0;
	if (len > 12)
	{
		// For backwards compatability we still allow leading blank
		if ((!isdigit(dataOrg[11]) && (dataOrg[11] != ' ')) ||
			 !isdigit(dataOrg[12]))
		{
			status = -1;
			return value;
		}

		memcpy( fld, p+11, 2);
		fld[2] = '\0';

		inHour = strtol(fld, 0, 10);

		if (len > 15)
		{
			if (!isdigit(dataOrg[14]) || !isdigit(dataOrg[15]))
			{
				status = -1;
				return value;
			}

			memcpy( fld, p+14, 2);
			fld[2] = '\0';

			inMinute = strtol(fld, 0, 10);

			if (len > 18)
			{
				if (!isdigit(dataOrg[17]) || !isdigit(dataOrg[18]))
				{
					status = -1;
					return value;
				}

				memcpy( fld, p+17, 2);
				fld[2] = '\0';

				inSecond = strtol(fld, 0, 10);

				if (len > 20)
				{
					unsigned int microFldLen = len - 20;
					if (microFldLen > (sizeof(fld)-1))
						microFldLen = sizeof(fld) - 1;
					memcpy( fld, p+20, microFldLen);
					fld[microFldLen] = '\0';
					inMicrosecond = strtol(fld, 0, 10);
				}
			}
		}
	}

	if ( isDateValid (inDay, inMonth, inYear) &&
		 isDateTimeValid (inHour, inMinute, inSecond, inMicrosecond) )
	{
		DateTime aDatetime;
		aDatetime.year = inYear;
		aDatetime.month = inMonth;
		aDatetime.day = inDay;
		aDatetime.hour = inHour;
		aDatetime.minute = inMinute;
		aDatetime.second = inSecond;
		aDatetime.msecond = inMicrosecond;

		memcpy( &value, &aDatetime, 8);
	}
	else
	{
		status = -1;
	}

	return value;
}

std::string DataConvert::dateToString( int  datevalue )
{
	// @bug 4703 abandon multiple ostringstream's for conversion
	Date d(datevalue);
	const int DATETOSTRING_LEN=12; // YYYY-MM-DD\0
	char buf[DATETOSTRING_LEN];

	sprintf(buf,"%04d-%02d-%02d",d.year,d.month,d.day);
	return buf;
}

std::string DataConvert::datetimeToString( long long  datetimevalue )
{
	// @bug 4703 abandon multiple ostringstream's for conversion
	DateTime dt(datetimevalue);
	const int DATETIMETOSTRING_LEN=21; // YYYY-MM-DD HH:MM:SS\0
	char buf[DATETIMETOSTRING_LEN];

	sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d",dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second);
	return buf;
}

std::string DataConvert::dateToString1( int  datevalue )
{
	// @bug 4703 abandon multiple ostringstream's for conversion
	Date d(datevalue);
	const int DATETOSTRING1_LEN=10; // YYYYMMDD\0
	char buf[DATETOSTRING1_LEN];

	sprintf(buf,"%04d%02d%02d",d.year,d.month,d.day);
	return buf;
}

std::string DataConvert::datetimeToString1( long long  datetimevalue )
{
	// @bug 4703 abandon multiple ostringstream's for conversion
	DateTime dt(datetimevalue);
	const int DATETIMETOSTRING1_LEN=22; // YYYYMMDDHHMMSSmmmmmm\0
	char buf[DATETIMETOSTRING1_LEN];

	sprintf(buf,"%04d%02d%02d%02d%02d%02d%06d",dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second,dt.msecond);
	return buf;
}

bool DataConvert::isNullData(ColumnResult* cr, int rownum, CalpontSystemCatalog::ColType colType)
{
	switch (colType.colDataType)
	{
		case execplan::CalpontSystemCatalog::TINYINT:
			if (cr->GetData(rownum) == joblist::TINYINTNULL)
				return true;
			return false;
		case execplan::CalpontSystemCatalog::SMALLINT:
			if (cr->GetData(rownum) == joblist::SMALLINTNULL)
				return true;
			return false;
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
			if (cr->GetData(rownum) == joblist::INTNULL)
				return true;
			return false;
		case execplan::CalpontSystemCatalog::BIGINT:
			if (cr->GetData(rownum) == static_cast<int64_t>(joblist::BIGINTNULL))
				return true;
			return false;
		case execplan::CalpontSystemCatalog::DECIMAL:
		{
			if (colType.colWidth <= execplan::CalpontSystemCatalog::FOUR_BYTE)
			{
				if (cr->GetData(rownum) == joblist::SMALLINTNULL)
					return true;
				return false;
			}
			else if (colType.colWidth <= 9)
			{
				if (cr->GetData(rownum) == joblist::INTNULL)
					return true;
				else return false;
			}
			else if (colType.colWidth <= 18)
			{
				if (cr->GetData(rownum) == static_cast<int64_t>(joblist::BIGINTNULL))
					return true;
				return false;
			}
			else
			{
				if (cr->GetStringData(rownum) == "\376\377\377\377\377\377\377\377")
					return true;
				return false;
			}
		}
		case execplan::CalpontSystemCatalog::FLOAT:
			//if (cr->GetStringData(rownum) == joblist::FLOATNULL)
			if (cr->GetStringData(rownum).compare("null") == 0 )
				return true;
			return false;
		case execplan::CalpontSystemCatalog::DOUBLE:
			//if (cr->GetStringData(rownum) == joblist::DOUBLENULL)
			if (cr->GetStringData(rownum).compare("null") == 0 )
				return true;
			return false;
		case execplan::CalpontSystemCatalog::DATE:
			if (cr->GetData(rownum) == joblist::DATENULL)
				return true;
			return false;
		case execplan::CalpontSystemCatalog::DATETIME:
			if (cr->GetData(rownum) == static_cast<int64_t>(joblist::DATETIMENULL))
				return true;
			return false;
		case execplan::CalpontSystemCatalog::CHAR:
		{
			std::string charnull;
			if ( cr->GetStringData(rownum) == "")
			{
				return true;
			}
			if (colType.colWidth == 1)
			{
				if (cr->GetStringData(rownum) == "\376")
					return true;
				return false;
			}
			else if (colType.colWidth == 2)
			{
				if (cr->GetStringData(rownum) == "\377\376")
					return true;
				return false;
			}
			else if (( colType.colWidth < 5 ) && ( colType.colWidth > 2 ))
			{
				if (cr->GetStringData(rownum) == "\377\377\377\376")
					return true;
				return false;
			}
			else if (( colType.colWidth < 9 ) && ( colType.colWidth > 4 ))
			{
				if (cr->GetStringData(rownum) == "\377\377\377\377\377\377\377\376")
					return true;
				return false;
			}
			else
			{
				if (cr->GetStringData(rownum) == "\376\377\377\377\377\377\377\377")
					return true;
				return false;
			}
		}
		case execplan::CalpontSystemCatalog::VARCHAR:
		{
			std::string charnull;
			if ( cr->GetStringData(rownum) == "")
			{
				return true;
			}
			if (colType.colWidth == 1)
			{
				if (cr->GetStringData(rownum) == "\377\376")
					return true;
				return false;
			}
			else if ((colType.colWidth < 4)  && (colType.colWidth > 1))
			{
				if (cr->GetStringData(rownum) == "\377\377\377\376")
					return true;
				return false;
			}
			else if ((colType.colWidth < 8)  && (colType.colWidth > 3))
			{
				if (cr->GetStringData(rownum) == "\377\377\377\377\377\377\377\376")
					return true;
				return false;
			}
			else
			{
				WriteEngine::Token nullToken;
				// bytes reversed
				if (cr->GetStringData(rownum) == "\376\377\377\377\377\377\377\377")
					return true;
				return false;
			}
		}
		default:
			throw QueryDataExcept("convertColumnData: unknown column data type.", dataTypeErr);
	}
}

int64_t DataConvert::dateToInt(const string& date)
{
	return stringToDate(date);
}

int64_t DataConvert::datetimeToInt(const string& datetime)
{
	return stringToDatetime(datetime);
}

int64_t DataConvert::stringToDate(const string& data)
{
	Date aDay;
	if( stringToDateStruct( data, aDay ) )
		return (*(reinterpret_cast<uint32_t *> (&aDay))) & 0xFFFFFFC0;
	else
		return -1;
}

int64_t DataConvert::stringToDatetime(const string& data, bool* date)
{
	DateTime dtime;
	if( stringToDatetimeStruct( data, dtime, date ) )
		return *(reinterpret_cast<uint64_t*>(&dtime));
	else
		return -1;
}

int64_t DataConvert::intToDate(int64_t data)
{
	//char buf[10] = {0};
	//snprintf( buf, 10, "%llu", (long long unsigned int)data);
	//string date = buf;
	char buf[21] = {0};
	snprintf( buf, 15, "%llu", (long long unsigned int)data);

	string year, month, day, hour, min, sec, msec;
	int64_t y = 0, m = 0, d = 0, h = 0, minute = 0, s = 0, ms = 0;
	switch (strlen(buf))
	{
		case 14:
			year = string(buf, 4);
			month = string(buf+4, 2);
			day = string(buf+6, 2);
			hour = string(buf+8,2);
			min = string(buf+10,2);
			sec = string(buf+12,2);
			msec = string(buf+14,6);
			break;
		case 12:
			year = string(buf, 2);
			month = string(buf+2, 2);
			day = string(buf+4, 2);
			hour = string(buf+6,2);
			min = string(buf+8,2);
			sec = string(buf+10,2);
			msec = string(buf+12,6);
			break;
		case 10:
			month = string(buf, 2);
			day = string(buf+2, 2);
			hour = string(buf+4,2);
			min = string(buf+6,2);
			sec = string(buf+8,2);
			msec = string(buf+10,6);
			break;
		case 9:
			month = string(buf, 1);
			day = string(buf+1, 2);
			hour = string(buf+3,2);
			min = string(buf+5,2);
			sec = string(buf+7,2);
			msec = string(buf+9,6);
			break;
		case 8:
			year = string(buf, 4);
			month = string(buf+4, 2);
			day = string(buf+6, 2);
			break;
		case 6:
			year = string(buf, 2);
			month = string(buf+2, 2);
			day = string(buf+4, 2);
			break;
		case 4:
			month = string(buf, 2);
			day = string(buf+2, 2);
			break;
		case 3:
			month = string(buf, 1);
			day = string(buf+1, 2);
			break;
		default:
			return -1;
	}
	Date aday;
	if (year.empty())
	{
		// MMDD format. assume current year
		time_t calender_time;
		struct tm todays_date;
		calender_time = time(NULL);
		localtime_r(&calender_time, &todays_date);
		y = todays_date.tm_year+1900;
	}
	else
	{
		y = atoi(year.c_str());
	}
	m = atoi(month.c_str());
	d = atoi(day.c_str());
	h = atoi(hour.c_str());
	minute = atoi(min.c_str());
	s = atoi(sec.c_str());
	ms = atoi(msec.c_str());
	//if (!isDateValid(d, m, y))
	//	return -1;
	if (!isDateValid(d, m, y) || !isDateTimeValid(h, minute, s, ms))
		return -1;
	aday.year = y;
	aday.month = m;
	aday.day = d;
	return *(reinterpret_cast<uint32_t*>(&aday));
}

int64_t DataConvert::intToDatetime(int64_t data, bool* date)
{
	bool isDate = false;
	char buf[21] = {0};
	snprintf( buf, 15, "%llu", (long long unsigned int)data);
	//string date = buf;
	string year, month, day, hour, min, sec, msec;
	int64_t y = 0, m = 0, d = 0, h = 0, minute = 0, s = 0, ms = 0;
	switch (strlen(buf))
	{
		case 14:
			year = string(buf, 4);
			month = string(buf+4, 2);
			day = string(buf+6, 2);
			hour = string(buf+8,2);
			min = string(buf+10,2);
			sec = string(buf+12,2);
			msec = string(buf+14,6);
			break;
		case 12:
			year = string(buf, 2);
			month = string(buf+2, 2);
			day = string(buf+4, 2);
			hour = string(buf+6,2);
			min = string(buf+8,2);
			sec = string(buf+10,2);
			msec = string(buf+12,6);
			break;
		case 10:
			month = string(buf, 2);
			day = string(buf+2, 2);
			hour = string(buf+4,2);
			min = string(buf+6,2);
			sec = string(buf+8,2);
			msec = string(buf+10,6);
			break;
		case 9:
			month = string(buf, 1);
			day = string(buf+1, 2);
			hour = string(buf+3,2);
			min = string(buf+5,2);
			sec = string(buf+7,2);
			msec = string(buf+9,6);
			break;
		case 8:
			year = string(buf, 4);
			month = string(buf+4, 2);
			day = string(buf+6, 2);
			isDate = true;
			break;
		case 6:
			year = string(buf, 2);
			month = string(buf+2, 2);
			day = string(buf+4, 2);
			isDate = true;
			break;
		case 4:
			month = string(buf, 2);
			day = string(buf+2, 2);
			break;
		case 3:
			month = string(buf, 1);
			day = string(buf+1, 2);
			isDate = true;
			break;
		default:
			return -1;
	}
	DateTime adaytime;
	if (year.empty())
	{
		// MMDD format. assume current year
		time_t calender_time;
		struct tm todays_date;
		calender_time = time(NULL);
		localtime_r(&calender_time, &todays_date);
		y = todays_date.tm_year+1900;
	}
	else
	{
		y = atoi(year.c_str());
	}
	m = atoi(month.c_str());
	d = atoi(day.c_str());
	h = atoi(hour.c_str());
	minute = atoi(min.c_str());
	s = atoi(sec.c_str());
	ms = atoi(msec.c_str());

	if (!isDateValid(d, m, y) || !isDateTimeValid(h, minute, s, ms))
		return -1;

	adaytime.year = y;
	adaytime.month = m;
	adaytime.day = d;
	adaytime.hour = h;
	adaytime.minute = minute;
	adaytime.second = s;
	adaytime.msecond = ms;
	if (date)
		*date = isDate;
	return *(reinterpret_cast<uint64_t*>(&adaytime));
}

int64_t DataConvert::stringToTime(const string& data)
{
	// MySQL supported time value format 'D HHH:MM:SS.fraction'
	// -34 <= D <= 34
	// -838 <= H <= 838
	uint64_t min = 0, sec = 0, msec = 0;
	int64_t day = 0, hour = 0;
	string time, hms, ms;
	char* end = NULL;

	// Day
	size_t pos = data.find(" ");
	if (pos != string::npos)
	{
		day = strtol(data.substr(0,pos).c_str(), &end, 10);
		if (*end != '\0')
			return -1;
		time = data.substr(pos+1, data.length()-pos-1);
	}
	else
	{
		time = data;
	}

	// Fraction
	pos = time.find(".");
	if (pos != string::npos)
	{
		msec = atol(time.substr(pos+1, time.length()-pos-1).c_str());
		hms = time.substr(0, pos);
	}
	else
	{
		hms = time;
	}

	// HHH:MM:SS
	pos = hms.find(":");
	if (pos == string::npos)
	{
		hour = atoi(hms.c_str());
	}
	else
	{
		hour = atoi(hms.substr(0, pos).c_str());
		ms = hms.substr(pos+1, hms.length()-pos-1);
	}

	// MM:SS
	pos = ms.find(":");
	if (pos != string::npos)
	{
		min = atoi(ms.substr(0, pos).c_str());
		sec = atoi(ms.substr(pos+1, ms.length()-pos-1).c_str());
	}
	else
	{
		min = atoi(ms.c_str());
	}

	Time atime;
	atime.day = day;
	atime.hour = hour;
	atime.minute = min;
	atime.second = sec;
	atime.msecond = msec;
	return *(reinterpret_cast<int64_t*>(&atime));
}

CalpontSystemCatalog::ColType DataConvert::convertUnionColType(vector<CalpontSystemCatalog::ColType>& types)
{
	idbassert(types.size());

	CalpontSystemCatalog::ColType unionedType = types[0];
	for (uint64_t i = 1; i < types.size(); i++)
	{
		// limited support for VARBINARY, no implicit conversion.
		if (types[i].colDataType == CalpontSystemCatalog::VARBINARY ||
			unionedType.colDataType == CalpontSystemCatalog::VARBINARY)
		{
			if (types[i].colDataType != unionedType.colDataType ||
				types[i].colWidth != unionedType.colWidth)
				throw runtime_error("VARBINARY in UNION must be the same width.");
		}

		switch (types[i].colDataType)
		{
			case CalpontSystemCatalog::TINYINT:
			case CalpontSystemCatalog::SMALLINT:
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::BIGINT:
			case CalpontSystemCatalog::DECIMAL:
			{
				switch (unionedType.colDataType)
				{
					case CalpontSystemCatalog::TINYINT:
					case CalpontSystemCatalog::SMALLINT:
					case CalpontSystemCatalog::MEDINT:
					case CalpontSystemCatalog::INT:
					case CalpontSystemCatalog::BIGINT:
					case CalpontSystemCatalog::DECIMAL:
						if (types[i].colWidth > unionedType.colWidth)
						{
							unionedType.colDataType = types[i].colDataType;
							unionedType.colWidth = types[i].colWidth;
						}
						if (types[i].colDataType == CalpontSystemCatalog::DECIMAL)
						{
							unionedType.colDataType = CalpontSystemCatalog::DECIMAL;
						}
						unionedType.scale = (types[i].scale > unionedType.scale) ? types[i].scale : unionedType.scale;
						break;
					case CalpontSystemCatalog::DATE:
						unionedType.colDataType = CalpontSystemCatalog::CHAR;
						unionedType.colWidth = 20;
						break;
					case CalpontSystemCatalog::DATETIME:
						unionedType.colDataType = CalpontSystemCatalog::CHAR;
						unionedType.colWidth = 26;
						break;
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR:
						if (unionedType.colWidth < 20)
							unionedType.colWidth = 20;
						break;
					case CalpontSystemCatalog::FLOAT:
					case CalpontSystemCatalog::DOUBLE:
					default:
						break;
				}
				break;
			}

			case CalpontSystemCatalog::DATE:
			{
				switch (unionedType.colDataType)
				{
					case CalpontSystemCatalog::TINYINT:
					case CalpontSystemCatalog::SMALLINT:
					case CalpontSystemCatalog::MEDINT:
					case CalpontSystemCatalog::INT:
					case CalpontSystemCatalog::BIGINT:
					case CalpontSystemCatalog::DECIMAL:
					case CalpontSystemCatalog::FLOAT:
					case CalpontSystemCatalog::DOUBLE:
						unionedType.colDataType = CalpontSystemCatalog::CHAR;
						unionedType.scale = 0;
						unionedType.colWidth = 20;
						break;
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR:
						if (unionedType.colWidth < 10)
							unionedType.colWidth = 10;
						break;
					case CalpontSystemCatalog::DATE:
					case CalpontSystemCatalog::DATETIME:
					default:
						break;
				}
				break;
			}

			case CalpontSystemCatalog::DATETIME:
			{
				switch (unionedType.colDataType)
				{
					case CalpontSystemCatalog::TINYINT:
					case CalpontSystemCatalog::SMALLINT:
					case CalpontSystemCatalog::MEDINT:
					case CalpontSystemCatalog::INT:
					case CalpontSystemCatalog::BIGINT:
					case CalpontSystemCatalog::DECIMAL:
					case CalpontSystemCatalog::FLOAT:
					case CalpontSystemCatalog::DOUBLE:
						unionedType.colDataType = CalpontSystemCatalog::CHAR;
						unionedType.scale = 0;
						unionedType.colWidth = 26;
						break;
					case CalpontSystemCatalog::DATE:
						unionedType.colDataType = CalpontSystemCatalog::DATETIME;
						unionedType.colWidth = types[i].colWidth;
						break;
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR:
						if (unionedType.colWidth < 26)
							unionedType.colWidth = 26;
						break;
					case CalpontSystemCatalog::DATETIME:
					default:
						break;
				}
				break;
			}

			case CalpontSystemCatalog::FLOAT:
			case CalpontSystemCatalog::DOUBLE:
			{
				switch (unionedType.colDataType)
				{
					case CalpontSystemCatalog::DATE:
						unionedType.colDataType = CalpontSystemCatalog::CHAR;
						unionedType.scale = 0;
						unionedType.colWidth = 20;
						break;
					case CalpontSystemCatalog::DATETIME:
						unionedType.colDataType = CalpontSystemCatalog::CHAR;
						unionedType.scale = 0;
						unionedType.colWidth = 26;
						break;
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR:
						if (unionedType.colWidth < 20)
							unionedType.colWidth = 20;
						break;
					case CalpontSystemCatalog::TINYINT:
					case CalpontSystemCatalog::SMALLINT:
					case CalpontSystemCatalog::MEDINT:
					case CalpontSystemCatalog::INT:
					case CalpontSystemCatalog::BIGINT:
					case CalpontSystemCatalog::DECIMAL:
					case CalpontSystemCatalog::FLOAT:
					case CalpontSystemCatalog::DOUBLE:
						unionedType.colDataType = CalpontSystemCatalog::DOUBLE;
						unionedType.scale = 0;
						unionedType.colWidth = sizeof(double);
						break;
					default:
						break;
				}
				break;
			}

			case CalpontSystemCatalog::CHAR:
			case CalpontSystemCatalog::VARCHAR:
			{
				switch (unionedType.colDataType)
				{
					case CalpontSystemCatalog::TINYINT:
					case CalpontSystemCatalog::SMALLINT:
					case CalpontSystemCatalog::MEDINT:
					case CalpontSystemCatalog::INT:
					case CalpontSystemCatalog::BIGINT:
					case CalpontSystemCatalog::DECIMAL:
					case CalpontSystemCatalog::FLOAT:
					case CalpontSystemCatalog::DOUBLE:
						unionedType.scale = 0;
						unionedType.colWidth = (types[i].colWidth > 20) ? types[i].colWidth : 20;
						break;
					case CalpontSystemCatalog::DATE:
						unionedType.colWidth = (types[i].colWidth > 10) ? types[i].colWidth : 10;
						break;
					case CalpontSystemCatalog::DATETIME:
						unionedType.colWidth = (types[i].colWidth > 26) ? types[i].colWidth : 26;
						break;
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR:
						if (unionedType.colWidth < types[i].colWidth)
							unionedType.colWidth = types[i].colWidth;
					default:
						break;
				}
				unionedType.colDataType = CalpontSystemCatalog::VARCHAR;
				break;
			}

			default:
			{
				break;
			}
		} // switch
	} // for

	return unionedType;
}

} // namespace dataconvert
// vim:ts=4 sw=4:

