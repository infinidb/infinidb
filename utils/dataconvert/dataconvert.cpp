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
* $Id: dataconvert.cpp 3258 2012-09-07 19:58:09Z xlou $
*
*
****************************************************************************/

#include <string>
#include <cmath>
#include <errno.h>
#include <limits>
#include <time.h>
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
	assert(scale < 20);
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

int convertMonth (std::string month)
{
	int value = 0;
	if ( from_string<int>( value, month, std::dec ) )
	{
		return value;
	}
	else
	{
		boost::to_lower(month);

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
	}
	return value;

}

void tokenTime (std::string data, std::vector<std::string>& dataList)
{
	dataList.clear();
	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep( ",; ; -;/;.;:;_;|;+;-;{;};*;^;%;$;#;@;!;~;`");
	tokenizer tokens(data, sep);
	for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
	{
		dataList.push_back(*tok_iter);
	}

	switch ( dataList.size() )
		{
		case 1:
		{
			if ( !number_value ( data ) && strlen(data.c_str()) >= 9 && strlen(data.c_str()) <= 21 )
			{
				dataList.clear();
				//result.result = NO_ERROR;
				dataList.push_back(data.substr(0, 4)); //Year
				int stop;
				stop = data.find_last_of ( "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" ) + 1;
				dataList.push_back(data.substr(4, stop-4)); //Month
				dataList.push_back(data.substr(stop, 2)); //Day
				switch ( data.length()-stop-2 )
				{
					case 1:
					{
						dataList.push_back(data.substr(stop + 2, 1)); //Hour
					}
					break;
					case 2:
					{
						dataList.push_back(data.substr(stop + 2, 2)); //Hour
					}
					break;
					case 3:
					{
						dataList.push_back(data.substr(stop + 2, 2)); //Hour
						dataList.push_back(data.substr(stop + 4, 1)); //Minute
					}
					break;
					case 4:
					{
						dataList.push_back(data.substr(stop + 2, 2)); //Hour
						dataList.push_back(data.substr(stop + 4, 2)); //Minute
					}
					break;
					case 5:
					{
						dataList.push_back(data.substr(stop + 2, 2)); //Hour
						dataList.push_back(data.substr(stop + 4, 2)); //Minute
						dataList.push_back(data.substr(stop + 6, 1)); //Second
					}
					break;
					case 6:
					{
						dataList.push_back(data.substr(stop + 2, 2)); //Hour
						dataList.push_back(data.substr(stop + 4, 2)); //Minute
						dataList.push_back(data.substr(stop + 6, 2)); //Second
					}
					break;
				}
			}
			else if ( number_value ( data ) && strlen(data.c_str()) >= 8  && strlen(data.c_str()) <= 14 )
			{
				dataList.clear();
				//result.result = NO_ERROR;
				dataList.push_back(data.substr(0, 4));
				dataList.push_back(data.substr(4, 2));
				dataList.push_back(data.substr(6, 2));
				switch ( strlen(data.c_str()) )
				{
					case 9:
					{
						dataList.push_back(data.substr(8, 1));
					}
					break;
					case 10:
					{
						dataList.push_back(data.substr(8, 2));
					}
					break;
					case 11:
					{
						dataList.push_back(data.substr(8, 2));
						dataList.push_back(data.substr(10, 1));
					}
					break;
					case 12:
					{
						dataList.push_back(data.substr(8, 2));
						dataList.push_back(data.substr(10, 2));
					}
					break;
					case 13:
					{
						dataList.push_back(data.substr(8, 2));
						dataList.push_back(data.substr(10, 2));
						dataList.push_back(data.substr(12, 1));
					}
					break;
					case 14:
					{
						dataList.push_back(data.substr(8, 2));
						dataList.push_back(data.substr(10, 2));
						dataList.push_back(data.substr(12, 2));
					}
					break;
				}

			}
			else
			{
				dataList.clear();
				throw QueryDataExcept("invalid date or time.", formatErr);
			}
		}
		break;
		case 2:
		{
			std::string fPart ( dataList[0] );
			std::string sPart ( dataList[1] );
			dataList.clear();
			if ( number_value ( fPart )	&& number_value ( sPart ))
			{
				switch ( fPart.length() )
				{
					case 1:
					case 2:
					case 3:
					case 4: //First part: YYYY
					{
						dataList.push_back ( fPart ); //Year
						if ( sPart.length() >= 4 && sPart.length() <= 10 )
						{
							switch ( sPart.length() )
							{
								case 4:
								{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 2)); //Day
								}
								break;
								case 5:
								{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 2)); //Day
									dataList.push_back(sPart.substr(4, 1)); //Hour
								}
								break;
								case 6:
								{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 2)); //Day
									dataList.push_back(sPart.substr(4, 2)); //Hour
								}
								break;
								case 7:
								{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 2)); //Day
									dataList.push_back(sPart.substr(4, 2)); //Hour
									dataList.push_back(sPart.substr(6, 1)); //Minute
								}
								break;
								case 8:
								{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 2)); //Day
									dataList.push_back(sPart.substr(4, 2)); //Hour
									dataList.push_back(sPart.substr(6, 2)); //Minute
								}
								break;
								case 9:
								{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 2)); //Day
									dataList.push_back(sPart.substr(4, 2)); //Hour
									dataList.push_back(sPart.substr(6, 2)); //Minute
									dataList.push_back(sPart.substr(8, 1)); //Second
								}
								break;
								case 10:
								{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 2)); //Day
									dataList.push_back(sPart.substr(4, 2)); //Hour
									dataList.push_back(sPart.substr(6, 2)); //Minute
									dataList.push_back(sPart.substr(8, 2)); //Second
								}
								break;
							}
						}
						else
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
						}
					}
					break;
					case 6: //First part: YYYYMM
					{
						dataList.push_back(fPart.substr(0, 4)); //Year
						dataList.push_back(fPart.substr(4, 2)); //Month
						if ( sPart.length() >= 1 && sPart.length() <= 8 )
						{
							switch ( sPart.length() )
							{
								case 1:
								{
									dataList.push_back(sPart.substr(0, 1)); //Day
								}
								break;
								case 2:
								{
									dataList.push_back(sPart.substr(0, 2)); //Day
								}
								break;
								case 3:
								{
									dataList.push_back(sPart.substr(0, 2)); //Day
									dataList.push_back(sPart.substr(2, 1)); //Hour
								}
								break;
								case 4:
								{
									dataList.push_back(sPart.substr(0, 2)); //Day
									dataList.push_back(sPart.substr(2, 2)); //Hour
								}
								break;
								case 5:
								{
									dataList.push_back(sPart.substr(0, 2)); //Day
									dataList.push_back(sPart.substr(2, 2)); //Hour
									dataList.push_back(sPart.substr(4, 1)); //Minute
								}
								break;
								case 6:
								{
									dataList.push_back(sPart.substr(0, 2)); //Day
									dataList.push_back(sPart.substr(2, 2)); //Hour
									dataList.push_back(sPart.substr(4, 2)); //Minute
								}
								break;
								case 7:
								{
									dataList.push_back(sPart.substr(0, 2)); //Day
									dataList.push_back(sPart.substr(2, 2)); //Hour
									dataList.push_back(sPart.substr(4, 2)); //Minute
									dataList.push_back(sPart.substr(6, 1)); //Second
								}
								break;
								case 8:
								{
									dataList.push_back(sPart.substr(0, 2)); //Day
									dataList.push_back(sPart.substr(2, 2)); //Hour
									dataList.push_back(sPart.substr(4, 2)); //Minute
									dataList.push_back(sPart.substr(6, 2)); //Second
								}
								break;
							}
						}
						else
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
						}
					}
					break;
					case 8: //First part: YYYYMMDD
					{
						dataList.push_back(fPart.substr(0, 4)); //Year
						dataList.push_back(fPart.substr(4, 2)); //Month
						dataList.push_back(fPart.substr(6, 2)); //Day
						if ( sPart.length() >= 1 && sPart.length() <= 6 )
						{
							switch ( sPart.length() )
							{
								case 1:
								{
									dataList.push_back(sPart.substr(0, 1)); //Hour
								}
								break;
								case 2:
								{
									dataList.push_back(sPart.substr(0, 2)); //Hour
								}
								break;
								case 3:
								{
									dataList.push_back(sPart.substr(0, 2)); //Hour
									dataList.push_back(sPart.substr(2, 1)); //Minute
								}
								break;
								case 4:
								{
									dataList.push_back(sPart.substr(0, 2)); //Hour
									dataList.push_back(sPart.substr(2, 2)); //Minute
								}
								break;
								case 5:
								{
									dataList.push_back(sPart.substr(0, 2)); //Hour
									dataList.push_back(sPart.substr(2, 2)); //Minute
									dataList.push_back(sPart.substr(4, 1)); //Second
								}
								break;
								case 6:
								{
									dataList.push_back(sPart.substr(0, 2)); //Hour
									dataList.push_back(sPart.substr(2, 2)); //Minute
									dataList.push_back(sPart.substr(4, 2)); //Second
								}
								break;
							}
						}
						else
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
						}
					}
					break;
					case 10: //First part: YYYYMMDDHH
					{
						dataList.push_back(fPart.substr(0, 4)); //Year
						dataList.push_back(fPart.substr(4, 2)); //Month
						dataList.push_back(fPart.substr(6, 2)); //Day
						dataList.push_back(fPart.substr(8, 2)); //Hour
						if ( sPart.length() >= 1 && sPart.length() <= 4 )
						{
							switch ( sPart.length() )
							{
								case 1:
								{
									dataList.push_back(sPart.substr(0, 1)); //Minute
								}
								break;
								case 2:
								{
									dataList.push_back(sPart.substr(0, 2)); //Minute
								}
								break;
								case 3:
								{
									dataList.push_back(sPart.substr(0, 2)); //Minute
									dataList.push_back(sPart.substr(2, 1)); //Second
								}
								break;
								case 4:
								{
									dataList.push_back(sPart.substr(0, 2)); //Minute
									dataList.push_back(sPart.substr(2, 2)); //Second
								}
								break;
							}
						}
						else
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
						}
					}
					break;
					case 12: //First part: YYYYMMDDHHMI
					{
						dataList.push_back(fPart.substr(0, 4)); //Year
						dataList.push_back(fPart.substr(4, 2)); //Month
						dataList.push_back(fPart.substr(6, 2)); //Day
						dataList.push_back(fPart.substr(8, 2)); //Hour
						dataList.push_back(fPart.substr(10, 2)); //Minute
						if ( sPart.length() >= 1 && sPart.length() <= 2 )
						{
							switch ( sPart.length() )
							{
									case 1:
								{
									dataList.push_back(sPart.substr(0, 1)); //Second
								}
								break;
								case 2:
								{
									dataList.push_back(sPart.substr(0, 2)); //Second
								}
								break;
							}
						}
						else
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
							}
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
							}
					break;
				}
			}
			else //Month is non-numerical
			{
				//result.result = NO_ERROR;
				if ( number_value ( fPart ) ) //First part: YYYY
				{
					dataList.push_back(fPart); //Year
					int sStop;
					sStop = sPart.find_last_of ( "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" ) + 1;
					dataList.push_back ( sPart.substr( 0, sStop ) );
					if ( ( sPart.length() - sStop ) >= 1 && (( sPart.length() - sStop ) <= 8 ) )
					{
						switch ( sPart.length() - sStop )
						{
							case 1:
							{
								dataList.push_back(sPart.substr(sStop, 1)); //Day
							}
							break;
							case 2:
							{
								dataList.push_back(sPart.substr(sStop, 2)); //Day
							}
							break;
							case 3:
							{
								dataList.push_back( sPart.substr(sStop, 2) ); //Day
								dataList.push_back( sPart.substr(sStop + 2, 1) ); //Hour
							}
							break;
							case 4:
							{
								dataList.push_back( sPart.substr(sStop, 2) ); //Day
								dataList.push_back( sPart.substr(sStop + 2, 2) ); //Hour
							}
							break;
							case 5:
							{
								dataList.push_back( sPart.substr(sStop, 2) ); //Day
								dataList.push_back( sPart.substr(sStop + 2, 2) ); //Hour
								dataList.push_back( sPart.substr(sStop + 4, 1) ); //Minute
							}
							break;
							case 6:
							{
								dataList.push_back( sPart.substr(sStop, 2) ); //Day
								dataList.push_back( sPart.substr(sStop + 2, 2) ); //Hour
								dataList.push_back( sPart.substr(sStop + 4, 2) ); //Minute
							}
							break;
							case 7:
							{
								dataList.push_back( sPart.substr(sStop, 2) ); //Day
								dataList.push_back( sPart.substr(sStop + 2, 2) ); //Hour
								dataList.push_back( sPart.substr(sStop + 4, 2) ); //Minute
								dataList.push_back( sPart.substr(sStop + 6, 1) ); //Second
							}
							break;
							case 8:
							{
								dataList.push_back( sPart.substr(sStop, 2) ); //Day
								dataList.push_back( sPart.substr(sStop + 2, 2) ); //Hour
								dataList.push_back( sPart.substr(sStop + 4, 2) ); //Minute
								dataList.push_back( sPart.substr(sStop + 6, 2) ); //Second
							}
							break;
						}
					}
					else
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
				}
			}
		}
		break;
		case 3: //Three tokens
		{
			std::string fPart ( dataList[0] );
			std::string sPart ( dataList[1] );
			std::string tPart ( dataList[2] );
			dataList.clear();
			if ( number_value ( fPart ) && number_value ( sPart ) )
			{
				switch ( fPart.length() )
				{
					case 1:
					case 2:
					case 3:
					case 4: //First part: YYYY
					{
						dataList.push_back ( fPart ); //Year
						if ( sPart.length() >= 1 && sPart.length() <= 8 )
						{
							switch ( sPart.length() )
							{
								case 1:
								case 2:
								{
									dataList.push_back(sPart); //Month
									if ( strlen(tPart.c_str()) >=1 && strlen(tPart.c_str()) <= 6 )
									{
										switch ( strlen(tPart.c_str()) )
										{
											case 1:
											{
												dataList.push_back(tPart.substr(0, 1)); //Day
											}
											break;
											case 2:
											{
												dataList.push_back(tPart.substr(0, 2)); //Day
											}
											break;
											case 3:
											{
												dataList.push_back(tPart.substr(0, 2)); //Day
												dataList.push_back(tPart.substr(2, 1)); //Hour
											}
											break;
											case 4:
											{
												dataList.push_back(tPart.substr(0, 2)); //Day
												dataList.push_back(tPart.substr(2, 2)); //Hour
											}
											break;
											case 5:
											{
												dataList.push_back(tPart.substr(0, 2)); //Day
												dataList.push_back(tPart.substr(2, 2)); //Hour
												dataList.push_back(tPart.substr(4, 1)); //Minute
											}
											break;
											case 6:
											{
												dataList.push_back(tPart.substr(0, 2)); //Day
												dataList.push_back(tPart.substr(2, 2)); //Hour
												dataList.push_back(tPart.substr(4, 2)); //Minute
											}
											break;
										}
									}
									else
									{
										dataList.clear();
										throw QueryDataExcept("invalid date or time.", formatErr);
									}
								}
								break;
								case 3:
								{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 1)); //Day
									if ( strlen(tPart.c_str()) >=1 && strlen(tPart.c_str()) <= 4 )
									{
										switch ( strlen(tPart.c_str()) )
										{
											case 1:
											{
												dataList.push_back(tPart.substr(0, 1)); //Hour
											}
											break;
											case 2:
											{
												dataList.push_back(tPart.substr(0, 2)); //hour
											}
											break;
											case 3:
											{
												dataList.push_back(tPart.substr(0, 2)); //hour
												dataList.push_back(tPart.substr(2, 1)); //Minute
											}
											break;
											case 4:
											{
												dataList.push_back(tPart.substr(0, 2)); //Hour
												dataList.push_back(tPart.substr(2, 2)); //Minute
											}
											break;
										}
									}
									else
									{
										dataList.clear();
										throw QueryDataExcept("invalid date or time.", formatErr);
									}
								}
								break;
								case 4:
									{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 2)); //Day
									if ( tPart.length() >=1 && tPart.length() <= 4 )
									{
										switch ( tPart.length() )
										{
											case 1:
											{
												dataList.push_back(tPart.substr(0, 1)); //Hour
											}
											break;
											case 2:
											{
												dataList.push_back(tPart.substr(0, 2)); //hour
											}
											break;
											case 3:
											{
												dataList.push_back(tPart.substr(0, 2)); //hour
												dataList.push_back(tPart.substr(2, 1)); //Minute
											}
											break;
											case 4:
											{
												dataList.push_back(tPart.substr(0, 2)); //Hour
												dataList.push_back(tPart.substr(2, 2)); //Minute
											}
											break;
										}
									}
									else
									{
										dataList.clear();
										throw QueryDataExcept("invalid date or time.", formatErr);
									}
								}
								break;
								case 5:
									{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 2)); //Day
									dataList.push_back(sPart.substr(4, 1)); //Hour
									if ( strlen(tPart.c_str()) >=1 && strlen(tPart.c_str()) <= 2 )
									{
										switch ( strlen(tPart.c_str()) )
										{
											case 1:
											{
												dataList.push_back(tPart.substr(0, 1)); //Minute
											}
											break;
											case 2:
											{
												dataList.push_back(tPart.substr(0, 2)); //Minute
											}
											break;
										}
									}
									else
									{
										dataList.clear();
										throw QueryDataExcept("invalid date or time.", formatErr);
									}
								}
								break;
								case 6:
								{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 2)); //Day
									dataList.push_back(sPart.substr(4, 2)); //Hour
									if ( strlen(tPart.c_str()) >=1 && strlen(tPart.c_str()) <= 2 )
									{
										switch ( tPart.length() )
										{
											case 1:
											{
												dataList.push_back(tPart.substr(0, 1)); //Minute
											}
											break;
											case 2:
											{
												dataList.push_back(tPart.substr(0, 2)); //Minute
											}
											break;
										}
									}
									else
									{
										dataList.clear();
										throw QueryDataExcept("invalid date or time.", formatErr);
									}
								}
								break;
								case 8:
								{
									dataList.push_back(sPart.substr(0, 2)); //Month
									dataList.push_back(sPart.substr(2, 2)); //Day
									dataList.push_back(sPart.substr(4, 2)); //Hour
									dataList.push_back(sPart.substr(6, 2)); //Minute
									if ( tPart.length() >=1 && tPart.length() <= 2 )
									{
										switch ( tPart.length() )
										{
											case 1:
											{
												dataList.push_back(tPart.substr(0, 1)); //Second
											}
											break;
											case 2:
											{
												dataList.push_back(tPart.substr(0, 2)); //Second
											}
											break;
										}
									}
									else
									{
										dataList.clear();
										throw QueryDataExcept("invalid date or time.", formatErr);
									}
								}
								break;
							}
						}
						else
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
						}
					}
					break;
					case 5: // First part: YYYYMM
					{
						dataList.push_back ( fPart.substr( 0, 4 ) ); //Year
						dataList.push_back ( fPart.substr( 4, 1 ) ); //Month
					}
					case 6: // First part: YYYYMM
					{
						dataList.push_back ( fPart.substr( 0, 4 ) ); //Year
						dataList.push_back ( fPart.substr( 4, 2 ) ); //Month
					}
					if ( sPart.length() >= 1 && sPart.length() <= 6 )
					{
						switch ( sPart.length() )
						{
							case 1: //DD
							{
								dataList.push_back ( sPart.substr( 0, 1 ) ); //Day
								if ( tPart.length() >= 1 && tPart.length() <= 4 )
								{
									switch ( tPart.length() )
									{
										case 1:
										{
											dataList.push_back ( tPart.substr( 0, 1 ) ); // Hour
										}
										break;
										case 2:
										{
											dataList.push_back ( tPart.substr( 0, 2 ) ); // Hour
										}
										break;
										case 3:
										{
											dataList.push_back ( tPart.substr( 0, 2 ) ); // Hour
											dataList.push_back ( tPart.substr( 2, 1 ) ); // Minute
										}
										break;
										case 4:
										{
											dataList.push_back ( tPart.substr( 0, 2 ) ); // Hour
											dataList.push_back ( tPart.substr( 2, 2 ) ); // Minute
										}
										break;
									}
								}
								else
								{
									dataList.clear();
									throw QueryDataExcept("invalid date or time.", formatErr);
								}
							}
							break;
							case 2:	//DD
							{
								dataList.push_back ( sPart.substr( 0, 2 ) ); //Day
								if ( tPart.length() >= 1 && tPart.length() <= 4 )
								{
									switch ( tPart.length() )
									{
										case 1:
										{
											dataList.push_back ( tPart.substr( 0, 1 ) ); // Hour
										}
										break;
										case 2:
										{
											dataList.push_back ( tPart.substr( 0, 2 ) ); // Hour
										}
										break;
										case 3:
										{
											dataList.push_back ( tPart.substr( 0, 2 ) ); // Hour
											dataList.push_back ( tPart.substr( 2, 1 ) ); // Minute
										}
										break;
										case 4:
										{
											dataList.push_back ( tPart.substr( 0, 2 ) ); // Hour
											dataList.push_back ( tPart.substr( 2, 2 ) ); // Minute
										}
										break;
									}
								}
								else
								{
									dataList.clear();
									throw QueryDataExcept("invalid date or time.", formatErr);
								}
							}
							break;
							case 3: //DDMI
							{
								dataList.push_back ( sPart.substr( 0, 2 ) ); //Day
								dataList.push_back ( sPart.substr( 2, 1 ) ); //Hour
								if ( strlen(tPart.c_str()) >= 1 && strlen(tPart.c_str()) <= 2 )
								{
									switch ( tPart.length() )
									{
										case 1:
										{
											dataList.push_back ( tPart.substr( 0, 1 ) ); // Minute
										}
										break;
										case 2:
										{
											dataList.push_back ( tPart.substr( 0, 2 ) ); // Minute
										}
										break;
									}
								}
								else
								{
									dataList.clear();
									throw QueryDataExcept("invalid date or time.", formatErr);
								}
							}
							break;
							case 4: //DDMI
							{
								dataList.push_back ( sPart.substr( 0, 2 ) ); //Day
								dataList.push_back ( sPart.substr( 2, 2 ) ); //Hour
								if ( strlen(tPart.c_str()) >= 1 && strlen(tPart.c_str()) <= 2 )
								{
									switch ( tPart.length() )
									{
										case 1:
										{
											dataList.push_back ( tPart.substr( 0, 1 ) ); // Minute
										}
										break;
										case 2:
										{
											dataList.push_back ( tPart.substr( 0, 2 ) ); // Minute
										}
										break;
									}
								}
								else
								{
									dataList.clear();
									throw QueryDataExcept("invalid date or time.", formatErr);
								}
							}
							break;
							case 5:
							{
								dataList.push_back ( sPart.substr( 0, 2 ) ); //Day
								dataList.push_back ( sPart.substr( 2, 2 ) ); //Hour
								dataList.push_back ( sPart.substr( 4, 1 ) ); //Minute
								if ( strlen(tPart.c_str()) >= 1 && strlen(tPart.c_str()) <= 2 )
								{
									switch ( strlen(tPart.c_str()) )
									{
										case 1:
										{
											dataList.push_back ( tPart.substr( 0, 1 ) ); // Second
										}
										break;
										case 2:
										{
											dataList.push_back ( tPart.substr( 0, 2 ) ); // Second
										}
										break;
									}
								}
								else
								{
									dataList.clear();
									throw QueryDataExcept("invalid date or time.", formatErr);
								}
							}
							break;
							case 6:
							{
								dataList.push_back ( sPart.substr( 0, 2 ) ); //Day
								dataList.push_back ( sPart.substr( 2, 2 ) ); //Hour
								dataList.push_back ( sPart.substr( 4, 2 ) ); //Minute
								if ( strlen(tPart.c_str()) >= 1 && strlen(tPart.c_str()) <= 2 )
								{
									switch ( tPart.length() )
									{
										case 1:
										{
											dataList.push_back ( tPart.substr( 0, 1 ) ); // Second
										}
										break;
										case 2:
										{
											dataList.push_back ( tPart.substr( 0, 2 ) ); // Second
										}
										break;
									}
								}
								else
								{
									dataList.clear();
									throw QueryDataExcept("invalid date or time.", formatErr);
								}
							}
							break;
						}
					}
					else
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
				break;
				}
			}
			else // Month is non-numeracal, three tokens
			{
				if ( number_value( fPart ) )
				{
					dataList.push_back ( fPart ); //Year
					int monthEnd = sPart.find_last_of ( "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" ) + 1;
					dataList.push_back ( sPart.substr ( 0, monthEnd ));
					if ( (int)sPart.length() > monthEnd )
					{
						switch ( sPart.length() - monthEnd )
						{
							case 1:
							{
								dataList.push_back ( sPart.substr ( monthEnd, 1 ));	//Day
							}
							break;
							case 2:
							{
								dataList.push_back ( sPart.substr ( monthEnd, 2 ));	//Day
							}
							break;
							case 3:
							{
								dataList.push_back ( sPart.substr ( monthEnd, 2 ));	//Day
								dataList.push_back ( sPart.substr ( monthEnd+2, 1 )); //Hour
							}
							break;
							case 4:
							{
								dataList.push_back ( sPart.substr ( monthEnd, 2 ));	//Day
								dataList.push_back ( sPart.substr ( monthEnd+2, 2 )); //Hour
							}
							break;
							case 5:
							{
								dataList.push_back ( sPart.substr ( monthEnd, 2 ));	//Day
								dataList.push_back ( sPart.substr ( monthEnd+2, 2 )); //Hour
								dataList.push_back ( sPart.substr ( monthEnd+4, 1 )); //Minute
							}
							break;
							case 6:
							{
								dataList.push_back ( sPart.substr ( monthEnd, 2 ));	//Day
								dataList.push_back ( sPart.substr ( monthEnd+2, 2 )); //Hour
								dataList.push_back ( sPart.substr ( monthEnd+4, 2 )); //Minute
							}
							break;
							default:
							{
								dataList.clear();
								throw QueryDataExcept("invalid date or time.", formatErr);
							}
							break;
						}
					}
					switch ( strlen(tPart.c_str()) ) //Third part
					{
						case 1:
						{
							dataList.push_back ( tPart.substr ( 0, 1 ));
						}
						break;
						case 2:
						{
							dataList.push_back ( tPart.substr ( 0, 2 ));
						}
						break;
						case 3:
						{
							dataList.push_back ( tPart.substr ( 0, 2 ));
							dataList.push_back ( tPart.substr ( 2, 1 ));
						}
						break;
						case 4:
						{
							dataList.push_back ( tPart.substr ( 0, 2 ));
							dataList.push_back ( tPart.substr ( 2, 2 ));
						}
						break;
						case 5:
						{
							dataList.push_back ( tPart.substr ( 0, 2 ));
							dataList.push_back ( tPart.substr ( 2, 2 ));
							dataList.push_back ( tPart.substr ( 4, 1 ));
						}
						break;
						case 6:
						{
							dataList.push_back ( tPart.substr ( 0, 2 ));
							dataList.push_back ( tPart.substr ( 2, 2 ));
							dataList.push_back ( tPart.substr ( 4, 2 ));
						}
						break;
						default:
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
						}
						break;
					}
				}
				else // First part includes month
				{
					//result.result = NO_ERROR;
					int monthStart = fPart.find_first_of ( "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" );
					int monthEnd = fPart.find_last_of ( "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" ) + 1;
					dataList.push_back ( fPart.substr ( 0, monthStart ) ); //Year
					dataList.push_back ( fPart.substr ( monthStart, monthEnd-monthStart ) ); //Month
					switch ( fPart.length() - monthEnd )
					{
						case 0:
						break;
						case 1:
						{
							dataList.push_back ( fPart.substr ( monthEnd, 1 ));
						}
						break;
						case 2:
						{
							dataList.push_back ( fPart.substr ( monthEnd, 2 ));
						}
						break;
						default:
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
						}
						break;
					}
					switch ( sPart.length() ) //Numerical
					{
						case 1:
						{
							dataList.push_back ( sPart.substr ( 0, 1 ));
						}
						break;
						case 2:
						{
							dataList.push_back ( sPart.substr ( 0, 2 ));
						}
						break;
						case 4:
						{
							dataList.push_back ( sPart.substr ( 0, 2 ));
							dataList.push_back ( sPart.substr ( 2, 2 ));
						}
						break;
						case 6:
						{
							dataList.push_back ( sPart.substr ( 0, 2 ));
							dataList.push_back ( sPart.substr ( 2, 2 ));
							dataList.push_back ( sPart.substr ( 4, 2 ));
						}
						break;
						default:
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
						}
						break;

					}
					switch ( strlen(tPart.c_str()) ) //numerical
					{
						case 1:
						{
							dataList.push_back ( tPart.substr ( 0, 1 ));
						}
						break;
						case 2:
						{
							dataList.push_back ( tPart.substr ( 0, 2 ));
						}
						break;
						case 4:
						{
							dataList.push_back ( tPart.substr ( 0, 2 ));
							dataList.push_back ( tPart.substr ( 2, 2 ));
						}
						break;
						case 6:
						{
							dataList.push_back ( tPart.substr ( 0, 2 ));
							dataList.push_back ( tPart.substr ( 2, 2 ));
							dataList.push_back ( tPart.substr ( 4, 2 ));
						}
						break;
						default:
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
						}
						break;
					}
				}
			}
		} //end of second outer case
		break;
		case 4: //Four tokens
		{
			std::string fPart ( dataList[0] );
			std::string sPart ( dataList[1] );
			std::string tPart ( dataList[2] );
			std::string fourPart ( dataList[3] );
			dataList.clear();

			if ( number_value( fPart )  &&  !number_value( sPart ))
			{
				//result.result = NO_ERROR;
				dataList.push_back(fPart); //Year
				string::size_type monthEnd =
					sPart.find_last_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") + 1;
				dataList.push_back(sPart.substr(0, monthEnd));

				//TODO: what are we trying to accomplish here?
				if (sPart.length() > monthEnd)
				{
					switch (sPart.length() - monthEnd)
					{
					case 1:
						dataList.push_back(sPart.substr(monthEnd, 1));
						break;
					case 2:
						dataList.push_back(sPart.substr(monthEnd, 2));
						break;
					case 4:
						dataList.push_back(sPart.substr(monthEnd, 2));
						dataList.push_back(sPart.substr(monthEnd + 2, 2));
						break;
					default:
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
						break;
					}
				}

				switch ( tPart.length() ) //numerical
				{
					case 1:
					{
						dataList.push_back ( tPart.substr ( 0, 1 ));
					}
					break;
					case 2:
					{
						dataList.push_back ( tPart.substr ( 0, 2 ));
					}
					break;
					case 4:
					{
						dataList.push_back ( tPart.substr ( 0, 2 ));
						dataList.push_back ( tPart.substr ( 2, 2 ));
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				switch ( fourPart.length() )
				{
					case 1:
					{
						dataList.push_back ( fourPart.substr ( 0, 1 ));
					}
					break;
					case 2:
					{
						dataList.push_back ( fourPart.substr ( 0, 2 ));
					}
					break;
					case 4:
					{
						dataList.push_back ( fourPart.substr ( 0, 2 ));
						dataList.push_back ( fourPart.substr ( 2, 2 ));
					}
					break;
					case 6:
					{
						dataList.push_back ( fourPart.substr ( 0, 2 ));
						dataList.push_back ( fourPart.substr ( 2, 2 ));
						dataList.push_back ( fourPart.substr ( 4, 2 ));
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
			}
			else if ( !number_value( fPart ) )//Month is non-numerical in first token
			{
				//result.result = NO_ERROR;
				int monthStart = fPart.find_first_of ( "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" );
				int monthEnd = fPart.find_last_of ( "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" ) + 1;
				dataList.push_back ( fPart.substr ( 0, monthStart )); //Year
				dataList.push_back ( fPart.substr ( monthStart, monthEnd - monthStart )); //Month
				if ( (fPart.length() - monthEnd)  > 0 )
				{
					switch ( fPart.length() - monthEnd )
					{
						case 1:
						{
							dataList.push_back ( fPart.substr ( monthEnd, 1 ));
						}
						break;
						case 2:
						{
							dataList.push_back ( fPart.substr ( monthEnd, 2 ));
						}
						break;
						case 4:
						{
							dataList.push_back ( fPart.substr ( monthEnd, 2 ));
							dataList.push_back ( fPart.substr ( monthEnd+2, 2 ));
						}
						break;
						default:
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
						}
						break;
					}
				}
				switch ( sPart.length() )
				{
					case 1:
					{
						dataList.push_back ( sPart.substr ( 0, 1 ));
					}
					break;
					case 2:
					{
						dataList.push_back ( sPart.substr ( 0, 2 ));
					}
					break;
					case 4:
					{
						dataList.push_back ( sPart.substr ( 0, 2 ));
						dataList.push_back ( sPart.substr ( 2, 2 ));
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				switch ( tPart.length() )
				{
					case 1:
					{
						dataList.push_back ( tPart.substr ( 0, 1 ));
					}
					break;
					case 2:
					{
						dataList.push_back ( tPart.substr ( 0, 2 ));
					}
					break;
					case 4:
					{
						dataList.push_back ( tPart.substr ( 0, 2 ));
						dataList.push_back ( tPart.substr ( 2, 2 ));
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				switch ( fourPart.length() )
				{
					case 1:
					{
						dataList.push_back ( fourPart.substr ( 0, 1 ));
					}
					break;
					case 2:
					{
						dataList.push_back ( fourPart.substr ( 0, 2 ));
					}
					break;
					case 4:
					{
						dataList.push_back ( fourPart.substr ( 0, 2 ));
						dataList.push_back ( fourPart.substr ( 2, 2 ));
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
			}
			else // all numerical
			{
				if ( fPart.length() < 5 )
				{
					dataList.push_back ( fPart );	//Year
				}
				else
				{
					dataList.push_back ( fPart.substr( 0, 4 ) ); //Year
					switch ( fPart.length() - 4 )
					{
						case 1:
						{
							dataList.push_back ( fPart.substr( 4, 1 ) );  //Month
						}
						break;
						case 2:
						{
							dataList.push_back ( fPart.substr( 4, 2 ) );  //Month
						}
						break;
						case 3:
						{
							dataList.push_back ( fPart.substr( 4, 2 ) );  //Month
							dataList.push_back ( fPart.substr( 6, 1 ) );  // Day
						}
						break;
						case 4:
						{
							dataList.push_back ( fPart.substr( 4, 2 ) );  //Month
							dataList.push_back ( fPart.substr( 6, 2 ) );  // Day
						}
						break;
						default:
						{
							dataList.clear();
							throw QueryDataExcept("invalid date or time.", formatErr);
						}
						break;
					}

				}
				switch ( sPart.length() )
				{
					case 1:
					{
						dataList.push_back ( sPart.substr( 0, 1 ) );
					}
					break;
					case 2:
					{
						dataList.push_back ( sPart.substr( 0, 2 ) );
					}
					break;
					case 4:
					{
						dataList.push_back ( sPart.substr( 0, 2 ) );
						dataList.push_back ( sPart.substr( 2, 2 ) );
					}
					break;
					case 6:
					{
						dataList.push_back ( sPart.substr( 0, 2 ) );
						dataList.push_back ( sPart.substr( 2, 2 ) );
						dataList.push_back ( sPart.substr( 4, 2 ) );
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				switch ( tPart.length() )
				{
					case 1:
					{
						dataList.push_back ( tPart.substr( 0, 1 ) );
					}
					break;
					case 2:
					{
						dataList.push_back ( tPart.substr( 0, 2 ) );
					}
					break;
					case 4:
					{
						dataList.push_back ( tPart.substr( 0, 2 ) );
						dataList.push_back ( tPart.substr( 2, 2 ) );
					}
					break;
					case 6:
					{
						dataList.push_back ( tPart.substr( 0, 2 ) );
						dataList.push_back ( tPart.substr( 2, 2 ) );
						dataList.push_back ( tPart.substr( 4, 2 ) );
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				switch ( fourPart.length() )
				{
					case 1:
					{
						dataList.push_back ( fourPart.substr( 0, 1 ) );
					}
					break;
					case 2:
					{
						dataList.push_back ( fourPart.substr( 0, 2 ) );
					}
					break;
					case 4:
					{
						dataList.push_back ( fourPart.substr( 0, 2 ) );
						dataList.push_back ( fourPart.substr( 2, 2 ) );
					}
					break;
					case 6:
					{
						dataList.push_back ( fourPart.substr( 0, 2 ) );
						dataList.push_back ( fourPart.substr( 2, 2 ) );
						dataList.push_back ( fourPart.substr( 4, 2 ) );
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
			}
		}
		case 5: //Five tokens
		case 6: //Six tokens
		{
			std::string fPart ( dataList[0] );
			std::string sPart ( dataList[1] );
			std::string tPart ( dataList[2] );
			std::string fourPart ( dataList[3] );
			std::string fivePart ( dataList[4] );
			int size = dataList.size();
			std::string sixPart;
			if ( size == 6)
			{
				sixPart = dataList[5];
			}
			dataList.clear();

			if ( number_value( fPart )  &&  number_value( sPart )) //All numerical
			{
				if ( fPart.length() < 5 )
				{
					dataList.push_back ( fPart );
				}
				else
				{
					dataList.push_back ( fPart.substr ( 0, 4 ));
					dataList.push_back ( fPart.substr ( 4, fPart.length()-4 ));
				}

				switch ( sPart.length() )
				{
					case 1:
					{
						dataList.push_back ( sPart.substr( 0, 1 ) );
					}
					break;
					case 2:
					{
						dataList.push_back ( sPart.substr( 0, 2 ) );
					}
					break;
					case 4:
					{
						dataList.push_back ( sPart.substr( 0, 2 ) );
						dataList.push_back ( sPart.substr( 2, 2 ) );
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				switch ( tPart.length() )
				{
					case 1:
					{
						dataList.push_back ( tPart.substr( 0, 1 ) );
					}
					break;
					case 2:
					{
						dataList.push_back ( tPart.substr( 0, 2 ) );
					}
					break;
					case 4:
					{
						dataList.push_back ( tPart.substr( 0, 2 ) );
						dataList.push_back ( tPart.substr( 2, 2 ) );
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				switch ( fourPart.length() )
				{
					case 1:
					{
						dataList.push_back ( fourPart.substr( 0, 1 ) );
					}
					break;
					case 2:
					{
						dataList.push_back ( fourPart.substr( 0, 2 ) );
					}
					break;
					case 4:
					{
						dataList.push_back ( fourPart.substr( 0, 2 ) );
						dataList.push_back ( fourPart.substr( 2, 2 ) );
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				switch ( fivePart.length() )
				{
					case 1:
					{
						dataList.push_back ( fivePart.substr( 0, 1 ) );
					}
					break;
					case 2:
					{
						dataList.push_back ( fivePart.substr( 0, 2 ) );
					}
					break;
					case 4:
					{
						dataList.push_back ( fivePart.substr( 0, 2 ) );
						dataList.push_back ( fivePart.substr( 2, 2 ) );
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				if ( size == 6 )
				{
					dataList.push_back ( sixPart );
				}
			}
			else if ( number_value( fPart ) ) //Non-numerical month is on the second part
			{
				dataList.push_back ( fPart ); //Year
				int monthEnd = sPart.find_last_of ( "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" ) + 1;
				dataList.push_back ( sPart.substr ( 0, monthEnd )); //Month
				if ( (sPart.length() - monthEnd)  > 0 )
				{
					dataList.push_back ( sPart.substr( monthEnd, sPart.length() - monthEnd ));
				}
				switch ( tPart.length() )
				{
					case 1:
					{
						dataList.push_back ( tPart.substr( 0, 1 ) );
					}
					break;
					case 2:
					{
						dataList.push_back ( tPart.substr( 0, 2 ) );
					}
					break;
					case 4:
					{
						dataList.push_back ( tPart.substr( 0, 2 ) );
						dataList.push_back ( tPart.substr( 2, 2 ) );
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				switch ( fourPart.length() )
				{
					case 1:
					{
						dataList.push_back ( fourPart.substr( 0, 1 ) );
					}
					break;
					case 2:
					{
						dataList.push_back ( fourPart.substr( 0, 2 ) );
					}
					break;
					case 4:
					{
						dataList.push_back ( fourPart.substr( 0, 2 ) );
						dataList.push_back ( fourPart.substr( 2, 2 ) );
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				switch ( fivePart.length() )
				{
					case 1:
					{
						dataList.push_back ( fivePart.substr( 0, 1 ) );
					}
					break;
					case 2:
					{
						dataList.push_back ( fivePart.substr( 0, 2 ) );
					}
					break;
					case 4:
					{
						dataList.push_back ( fivePart.substr( 0, 2 ) );
						dataList.push_back ( fivePart.substr( 2, 2 ) );
					}
					break;
					default:
					{
						dataList.clear();
						throw QueryDataExcept("invalid date or time.", formatErr);
					}
					break;
				}
				if ( size == 6 )
				{
					dataList.push_back ( sixPart );
				}
			}
			else // non-numerical month is in the first part
			{
				//result.result = NO_ERROR;
				int monthStart = fPart.find_first_of ( "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" );
				dataList.push_back ( fPart.substr ( 0, monthStart )); //Year
				dataList.push_back ( fPart.substr ( monthStart, fPart.length() - monthStart )); //Month
				dataList.push_back ( sPart );
				dataList.push_back ( tPart );
				dataList.push_back ( fourPart );
				dataList.push_back ( fivePart );
				if ( size == 6 )
				{
					dataList.push_back ( sixPart );
				}
			}
		}
		break;
	} //end of outest case
	return;
}

} // namespace anon

namespace dataconvert
{

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
				std::vector<std::string> dataList;
				tokenTime ( data, dataList );
				int size, inYear, inMonth, inDay;
				size = dataList.size();
				if ( size > 0 && size <= 7 )
				{
					if (!from_string<int>( inYear, dataList[0], std::dec ))
						throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);

					inMonth = convertMonth ( dataList[1] );
					if (!from_string<int>( inDay, dataList[2], std::dec ))
						throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);

					if ( isDateValid ( inDay, inMonth, inYear))
					{
						Date aDay;
						aDay.year = inYear;
						aDay.month = inMonth;
						aDay.day = inDay;
						value = *(reinterpret_cast<uint32_t *> (&aDay));
					}
					else
					{
						throw QueryDataExcept("Invalid date", formatErr);
					}
				}
				else
				{
					throw QueryDataExcept("Invalid date format", formatErr);
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
				std::vector<std::string> dataList;
				tokenTime ( data, dataList);
				int size = dataList.size();
				if ( size >= 3 && size <= 7 )
				{
					int inYear, inMonth, inDay, inHour, inMinute, inSecond, inMicrosecond;
					if (!from_string<int>( inYear, dataList[0], std::dec ))
						throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);

					inMonth = convertMonth ( dataList[1] );
					if (!from_string<int>( inDay, dataList[2], std::dec ))
						throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
					if ( size < 4 ) //only year-month-day
					{
						inHour = 0;
						inMinute = 0;
						inSecond = 0;
						inMicrosecond = 0;
					}
					else
					{
						if (!from_string<int>( inHour, dataList[3], std::dec ))
							throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
						if ( size > 4 )
						{
							if (!from_string<int>( inMinute, dataList[4], std::dec ))
								throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
						}
						else
						{
							inMinute = 0;
						}

						if ( size > 5 )
						{
							if (!from_string<int>( inSecond, dataList[5], std::dec ))
								throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
						}
						else
						{
							inSecond = 0;
						}

						if ( size > 6 )
						{
							if (!from_string<int>( inMicrosecond, dataList[6], std::dec ))
								throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
						}
						else
						{
							inMicrosecond = 0;
						}
					}

					if ( isDateValid ( inDay, inMonth, inYear) && isDateTimeValid( inHour, inMinute, inSecond, inMicrosecond ) )
					{
						DateTime aDatetime;
						aDatetime.year = inYear;
						aDatetime.month = inMonth;
						aDatetime.day = inDay;
						aDatetime.hour = inHour;
						aDatetime.minute = inMinute;
						aDatetime.second = inSecond;
						aDatetime.msecond = inMicrosecond;
						value = *(reinterpret_cast<uint64_t*>(&aDatetime));
					}
					else
					{
						throw QueryDataExcept("Invalid datetime", formatErr);
					}
				}
				else //invalid data
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
const char* p = dataOrig.data();
cerr << "dataOrig: ";
for (size_t i = 0; i < dataOrig.size(); i++, p++)
{
	if (isprint(*p)) cerr << *p << ' ';
	else {unsigned u = *p; u &= 0xff; cerr << "0x" << hex << u << dec << ' ';}
}
cerr << endl;
p = data.data();
cerr << "data: ";
for (size_t i = 0; i < data.size(); i++, p++)
{
	if (isprint(*p)) cerr << *p << ' ';
	else {unsigned u = *p; u &= 0xff; cerr << "0x" << hex << u << dec << ' ';}
}
cerr << endl;
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
		//Check whether this column has DEFAULT constraint
		if ( colType.constraintType == execplan::CalpontSystemCatalog::DEFAULT_CONSTRAINT )
		{
			value = colType.defaultValue;
		}
		else
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
					memcpy( fld, p+20, len - 20);
					fld[len - 20] = '\0';
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
	ostringstream year, month, day;

	datevalue >>= 6;
	day << setw(2) << setfill('0') << (unsigned)(datevalue & 0x3f);
	datevalue >>= 6;
	month << setw(2) << setfill('0') << (unsigned)(datevalue & 0xf);
	datevalue >>= 4;
	year << setw(4) << setfill('0') << (unsigned)(datevalue & 0xffff);

	return year.str()+ "-" + month.str() + "-" + day.str();
}

std::string DataConvert::datetimeToString( long long  datetimevalue )
{
	ostringstream year, month, day, hour, minute, second;
	//@bug 481
	//@Bug 3415. Don't display microseconds.
	datetimevalue >>= 20;
	second << setw(2) << setfill('0') << (unsigned)(datetimevalue & 0x3f);
	datetimevalue >>= 6;
	minute << setw(2) << setfill('0') << (unsigned)(datetimevalue & 0x3f);
	datetimevalue >>= 6;
	hour << setw(2) << setfill('0') << (unsigned)(datetimevalue & 0x3f);
	datetimevalue >>= 6;
	day << setw(2) << setfill('0') << (unsigned)(datetimevalue & 0x3f);
	datetimevalue >>= 6;
	month << setw(2) << setfill('0') << (unsigned)(datetimevalue & 0xf);
	datetimevalue >>= 4;
	year << setw(4) << setfill('0') << (unsigned)(datetimevalue & 0xffff);
	return year.str()+ "-" + month.str() + "-" + day.str() + " " + hour.str()
		+ ":" + minute.str() + ":" + second.str();
}

std::string DataConvert::dateToString1( int  datevalue )
{
	ostringstream year, month, day;

	datevalue >>= 6;
	day << setw(2) << setfill('0') << (unsigned)(datevalue & 0x3f);
	datevalue >>= 6;
	month << setw(2) << setfill('0') << (unsigned)(datevalue & 0xf);
	datevalue >>= 4;
	year << setw(4) << setfill('0') << (unsigned)(datevalue & 0xffff);

	return year.str() + month.str() + day.str();
}

std::string DataConvert::datetimeToString1( long long  datetimevalue )
{
	ostringstream year, month, day, hour, minute, second, microsec;
	//@bug 481
	microsec << setw(6) << setfill('0') << (unsigned)( datetimevalue & 0xfffff);
	datetimevalue >>= 20;
	second << setw(2) << setfill('0') << (unsigned)(datetimevalue & 0x3f);
	datetimevalue >>= 6;
	minute << setw(2) << setfill('0') << (unsigned)(datetimevalue & 0x3f);
	datetimevalue >>= 6;
	hour << setw(2) << setfill('0') << (unsigned)(datetimevalue & 0x3f);
	datetimevalue >>= 6;
	day << setw(2) << setfill('0') << (unsigned)(datetimevalue & 0x3f);
	datetimevalue >>= 6;
	month << setw(2) << setfill('0') << (unsigned)(datetimevalue & 0xf);
	datetimevalue >>= 4;
	year << setw(4) << setfill('0') << (unsigned)(datetimevalue & 0xffff);
	return year.str()+ month.str() + day.str() + hour.str()
		+ minute.str() + second.str() + microsec.str();
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

int64_t DataConvert::dateToInt(std::string date)
{
	return stringToDate(date);
}

int64_t DataConvert::datetimeToInt(std::string datetime)
{
	return stringToDatetime(datetime);
}

int64_t DataConvert::stringToDate(string data)
{
	try {
		std::vector<std::string> dataList;
		tokenTime ( data, dataList );
		int inYear = 0,
			inMonth = 0,
			inDay = 0,
			inHour = 0,
			inMinute = 0,
			inSecond = 0,
			inMicrosecond = 0;
		int size = dataList.size();
		int64_t value;

		if (size == 0)
			return 0;
		
		if ( size > 0 && size <= 6 )
		{
			if (!from_string<int>( inYear, dataList[0], std::dec ))
				throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);

			inMonth = convertMonth ( dataList[1] );
			if (!from_string<int>( inDay, dataList[2], std::dec ))
				throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);

			if (size > 3)
			{
				if (!from_string<int>( inHour, dataList[3], std::dec ))
					throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
				if ( size > 4 )
				{
					if (!from_string<int>( inMinute, dataList[4], std::dec ))
						throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
				}
				else
				{
					inMinute = 0;
				}

				if ( size > 5 )
				{
					if (!from_string<int>( inSecond, dataList[5], std::dec ))
						throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
				}
				else
				{
					inSecond = 0;
				}

				if ( size > 6 )
				{
					if (!from_string<int>( inMicrosecond, dataList[6], std::dec ))
						throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
				}
				else
				{
					inMicrosecond = 0;
				}

			}
			if ( isDateValid ( inDay, inMonth, inYear) && isDateTimeValid( inHour, inMinute, inSecond, inMicrosecond ))
			{
				Date aDay;
				aDay.year = inYear;
				aDay.month = inMonth;
				aDay.day = inDay;
				value = (*(reinterpret_cast<uint32_t *> (&aDay))) & 0xFFFFFFC0;
			}
			else
			{
				throw QueryDataExcept("Invalid date", formatErr);
			}
		}
		else
		{
			throw QueryDataExcept("Invalid date format", formatErr);
		}
		return value;
	} catch (...)
	{
		return -1;
	}
}

int64_t DataConvert::stringToDatetime(string data, bool* date)
{
	bool isDate = false;
	try {
		std::vector<std::string> dataList;
		tokenTime ( data, dataList);
		int size = dataList.size();
		int64_t value;

		if ( size >= 3 && size <= 7 )
		{
			int inYear, inMonth, inDay, inHour, inMinute, inSecond, inMicrosecond;
			if (!from_string<int>( inYear, dataList[0], std::dec ))
				throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);

			inMonth = convertMonth ( dataList[1] );
			if (!from_string<int>( inDay, dataList[2], std::dec ))
				throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
			if ( size < 4 ) //only year-month-day
			{
				inHour = 0;
				inMinute = 0;
				inSecond = 0;
				inMicrosecond = 0;
				isDate = true;
			}
			else
			{
				if (!from_string<int>( inHour, dataList[3], std::dec ))
					throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
				if ( size > 4 )
				{
					if (!from_string<int>( inMinute, dataList[4], std::dec ))
						throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
				}
				else
				{
					inMinute = 0;
				}

				if ( size > 5 )
				{
					if (!from_string<int>( inSecond, dataList[5], std::dec ))
						throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
				}
				else
				{
					inSecond = 0;
				}

				if ( size > 6 )
				{
					if (dataList[6].length() > 6)
						dataList[6] = dataList[6].substr(0,6);

					if (!from_string<int>( inMicrosecond, dataList[6], std::dec ))
						throw QueryDataExcept("range, valid value or conversion error on INT type.", formatErr);
				}
				else
				{
					inMicrosecond = 0;
				}
			}

			if (isDateTimeValid( inHour, inMinute, inSecond, inMicrosecond))
			{
				DateTime aDatetime;
				aDatetime.hour = inHour;
				aDatetime.minute = inMinute;
				aDatetime.second = inSecond;
				aDatetime.msecond = inMicrosecond;
				// @bug 3584. functions like time() produces only time part of a datetime.
				if (isDateValid ( inDay, inMonth, inYear) || (inDay == 0 && inMonth == 0 && inYear == 0))
				{
					aDatetime.year = inYear;
					aDatetime.month = inMonth;
					aDatetime.day = inDay;
				}
				else
				{
					throw QueryDataExcept("Invalid datetime", formatErr);
				}
				value = *(reinterpret_cast<uint64_t*>(&aDatetime));
			}

			else
			{
				throw QueryDataExcept("Invalid datetime", formatErr);
			}
		}
		else //invalid data
		{
			throw QueryDataExcept("Invalid datetime", formatErr);
		}
		if (date)
			*date = isDate;

		return value;
	} catch (...)
	{
		return -1;
	}
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

int64_t DataConvert::stringToTime(string data)
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
	CalpontSystemCatalog::ColType unionedType;
	if (types.size() > 0)
		unionedType = types[0];

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

