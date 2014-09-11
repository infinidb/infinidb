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
* $Id: func_str_to_date.cpp 2578 2011-05-12 16:26:55Z chao $
*
*
****************************************************************************/

#include <unistd.h>
#include <cstdlib>
#include <string>
using namespace std;

#include "functor_dtm.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_str_to_date::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int extractTime (string valStr, string formatStr, dataconvert::DateTime & dateTime)
{
	const char *val = valStr.c_str();
	const char *val_end= val + strlen(val);
	const char *ptr= formatStr.c_str();
	const char *end= ptr + formatStr.length();
	//char *end = ptr + strlen(ptr);
	int yearday = 0;
	int  weekday = 0;
	int daypart= 0;
	int week_number= -1;
	bool usa_time= false;
	int error= 0;
	bool sunday_first_n_first_week_non_iso = false;
	bool strict_week_number_year_type = false;
	bool strict_week_number = false;
	int  strict_week_number_year= -1;
	char *ep = NULL;
	
	for (; ptr != end; ptr++)
	{
		/* Skip pre-space */
		while (val != val_end && (*val == ' '))
			val++;
		char *tmp = NULL;
		error= 0;
		if (*ptr == '%' && ptr+1 != end)
		{

			int val_len= (uint) (val_end - val);
			switch (*++ptr) {
			/* Year */
			case 'Y':
			{
				tmp= (char*) val + min(4, val_len);
				dateTime.year= infinidb_strtoll10(val, &tmp, &error);
				if ((int) (tmp-val) <= 2)
				{
					dateTime.year+=2000; 
					if (dateTime.year > 2069 )
						dateTime.year -= 100;
				}
				val= tmp;
				break;
			}
			case 'y':
			{
				tmp= (char*) val + min(2, val_len);
				dateTime.year= infinidb_strtoll10(val, &tmp, &error);
				val= tmp;
				dateTime.year+=2000; 
				if (dateTime.year > 2069 )
					dateTime.year -= 100;
				break;
			}
			/* Month */
			case 'm':
			case 'c':
			{
			tmp= (char*) val + min(2, val_len);
			dateTime.month= infinidb_strtoll10(val, &tmp, &error);
			val= tmp;
			break;
			}
			case 'M':
			{
				const char *tmpPtr;
				/* Find end of word */
				for (tmpPtr= val ;tmpPtr < val_end && (*tmpPtr <'z') && (*tmpPtr > 'A') ; tmpPtr++)
					;
				char* monthStr = static_cast<char*>(alloca(strlen(val)-strlen(tmpPtr)+1));
				strncpy(monthStr, val, strlen(val)-strlen(tmpPtr));
				monthStr[strlen(val)-strlen(tmpPtr)]='\0';
				
				if ((dateTime.month= convertMonth(monthStr)) <= 0)
					return -1;
				tmp= (char*) val + strlen(monthStr);
				val = tmp;	
				break;
			}
			case 'b':
			{
				tmp = (char*) val + 3;
				char monthName[4];
				strncpy(monthName, val, 3);
				 monthName[3] = '\0';
				if ((dateTime.month= convertMonth(monthName)) <= 0)
						return -1;
				val = tmp;
				break;
			}
			/* Day */
			case 'd':
			case 'e':
			{
				tmp= (char*) val + min(2, val_len);
				dateTime.day= infinidb_strtoll10(val, &tmp, &error);
				val= tmp;
				break;
			}
			case 'D':
			{
				tmp= (char*) val + min(2, val_len);
				dateTime.day= infinidb_strtoll10(val, &tmp, &error);
				/* Skip 'st, 'nd, 'th .. */
				val= tmp + min((int) (val_end-tmp), 2);
				break;
			}
			/* Hour */
			case 'h':
			case 'I':
			case 'l':
			usa_time= 1;
			/* fall through */
			case 'k':
			case 'H':
			{
				tmp= (char*) val + min(2, val_len);
				dateTime.hour= infinidb_strtoll10(val, &tmp, &error);
				val= tmp;
				break;
			}

			/* Minute */
			case 'i':
			{
				tmp= (char*) val + min(2, val_len);
				dateTime.minute= infinidb_strtoll10(val, &tmp, &error);
				val= tmp;
				break;
			}
			/* Second */
			case 's':
			case 'S':
			{
				tmp= (char*) val + min(2, val_len);
				dateTime.second= infinidb_strtoll10(val, &tmp, &error);
				val= tmp;
				break;
			}
			/* micro second part */
			case 'f':
			{
				tmp= (char*) val_end;
				if (tmp - val > 6)
				tmp= (char*) val + 6;
				dateTime.second= infinidb_strtoll10(val, &tmp, &error);
				dateTime.msecond= 6 - (uint) (tmp - val);
				val= tmp;
				break;
			}
			/* AM / PM */
			case 'p':
			{
				if (val_len < 2 || ! usa_time)
					return -1;
		
				tmp= (char*) val + 2;
				if (strncmp(val, "PM", 2) == 0)
					daypart= 12;
				else if (strncmp(val, "AM", 2) != 0)
					return -1;
					
				val+= 2;
				break;
			}
			/* Exotic things */
			case 'W':
			{
				const char *tmpPtr;
				/* Find end of word */
				for (tmpPtr= val ;tmpPtr < val_end && (*tmpPtr <'z') && (*tmpPtr > 'A') ; tmpPtr++)
					;
				char* weekStr = static_cast<char*>(alloca(strlen(val)-strlen(tmpPtr)+1));
				strncpy(weekStr, val, strlen(val)-strlen(tmpPtr));
				weekStr[strlen(val)-strlen(tmpPtr)]='\0';
				
				if ((weekday = convertWeek(weekStr)) < 0)
					return -1;
				tmp= (char*) val + strlen(weekStr);
				val = tmp;				
				break;
			}
			case 'a':
			{
				tmp= (char*) val + 3;
				char wkname[4];
				strncpy(wkname, val, 3);
				 wkname[3] = '\0';
				if ((weekday = convertWeek(wkname)) < 0)
					return -1;
				val = tmp;	
				break;
			}
			case 'w':
			{
				tmp= (char*) val + 1;
				if ((weekday= infinidb_strtoll10(val, &tmp, &error)) < 0 ||
					weekday >= 7)
					return -1;
					
				val= tmp;
				break;
			}
			case 'j':
			{
				tmp= (char*) val + min(val_len, 3);
				yearday= (int) strtoll(val, &ep, 10);
				val= tmp;
				break;
			}

			/* Week numbers */
			case 'V':
			case 'U':
			case 'v':
			case 'u':
			{
				sunday_first_n_first_week_non_iso= (*ptr=='U' || *ptr== 'V');
				strict_week_number= (*ptr=='V' || *ptr=='v');
				tmp= (char*) val + min(val_len, 2);
				if ((week_number= (int) infinidb_strtoll10(val, &tmp, &error)) < 0 ||
					(strict_week_number && !week_number) || week_number > 53)
					return -1;
					
				val= tmp;
				break;
			}
			/* Year used with 'strict' %V and %v week numbers */
			case 'X':
			case 'x':
			{
				strict_week_number_year_type= (*ptr=='X');
				tmp= (char*) val + min(4, val_len);
				strict_week_number_year= infinidb_strtoll10(val, &tmp, &error);
				val= tmp;
				break;
			}
			/* Time in AM/PM notation */
			case 'r': 
			{
				string formatStr2 ("%h:%i:%s");
				tmp= (char*) val + val_len;
				if (extractTime(tmp, formatStr2, dateTime) < 0)
					return -1;	
					
				break;
			}
			/* Time in 24-hour notation */
			case 'T':
			{
				string formatStr3 ("%h:%i:%s");
				tmp= (char*) val + val_len;
				if (extractTime(tmp, formatStr3, dateTime) < 0)
					return -1;	
					
				break;
			}
			default:
			{
				ptr++;
				break;
			}
		  }
		}
		else if (*ptr != ' ')
		{
			if (*val != *ptr)
				return -1;
			val++;
			
		}
	}
	
	if (usa_time)
	{
		if (dateTime.hour > 12 || dateTime.hour < 1)
			return -1;
		dateTime.hour = dateTime.hour%12+daypart;
	}


	if (yearday > 0)
	{
		uint days;
		days= calc_daynr(dateTime.year,1,1) +  yearday - 1;
		if (days <= 0 || days > 3652424L)
			return -1;
		get_date_from_daynr(days,dateTime);
	}

	if (week_number >= 0 && weekday)
	{
		int days;
		uint weekday_b;

		/*
		%V,%v require %X,%x resprectively,
		%U,%u should be used with %Y and not %X or %x
		*/
		if ((strict_week_number &&
			(strict_week_number_year < 0 ||
			strict_week_number_year_type != sunday_first_n_first_week_non_iso)) ||
			(!strict_week_number && strict_week_number_year >= 0))
			return -1;

		/* Number of days since year 0 till 1st Jan of this year */
		days= calc_daynr((strict_week_number ? strict_week_number_year :
                       dateTime.year), 1, 1);
		/* Which day of week is 1st Jan of this year */
		weekday_b= calc_weekday(days, sunday_first_n_first_week_non_iso);

		/*
		Below we are going to sum:
		1) number of days since year 0 till 1st day of 1st week of this year
		2) number of days between 1st week and our week
		3) and position of our day in the week
		*/
		if (sunday_first_n_first_week_non_iso)
		{
			days+= ((weekday_b == 0) ? 0 : 7) - weekday_b +
				(week_number - 1) * 7 +
				weekday % 7;
		}
		else
		{
			days+= ((weekday_b <= 3) ? 0 : 7) - weekday_b +
				(week_number - 1) * 7 +
				(weekday - 1);
		}

		if (days <= 0 || days > 31)
			return -1;
		get_date_from_daynr(days,dateTime);
	}

	if (dateTime.month > 12 || dateTime.day > 31 || dateTime.hour > 23 || 
      dateTime.minute > 59 || dateTime.second > 59)
		return -1;

	if (val != val_end)
	{
		do
		{
			if ((*val != ' '))
			{
				break;
			}
		} while (++val != val_end);
	}
	
	return 0;
}

dataconvert::DateTime getDateTime (rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	dataconvert::DateTime dateTime;
	dateTime.year = 0;
	dateTime.month = 0;
	dateTime.day = 0;
	dateTime.hour = 0;
	dateTime.minute = 0;
	dateTime.second = 0;
	dateTime.msecond = 0;
	int64_t val = 0;
	string valStr;
	string formatStr;
	int rc = 0;
	switch (parm[0]->data()->resultType().colDataType)
	{
		case CalpontSystemCatalog::DATE:
		{
			val = parm[0]->data()->getIntVal(row, isNull);
			valStr = dataconvert::DataConvert::dateToString (val);
			formatStr = parm[1]->data()->getStrVal(row, isNull);
			rc = extractTime (valStr, formatStr, dateTime);
			if ( rc < 0)
			{
				isNull = true;
				return -1;
			}
			break;
		}
		case CalpontSystemCatalog::DATETIME:
		{
			val = parm[0]->data()->getIntVal(row, isNull);
			valStr = dataconvert::DataConvert::datetimeToString (val);
			formatStr = parm[1]->data()->getStrVal(row, isNull);
			rc = extractTime (valStr, formatStr, dateTime);
			if ( rc < 0)
			{
				isNull = true;
				return -1;
			}
			break;
		}
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		{
			valStr = parm[0]->data()->getStrVal(row, isNull);
			//decode with provided format
			formatStr = parm[1]->data()->getStrVal(row, isNull);
			rc = extractTime (valStr, formatStr, dateTime);
			if ( rc < 0)
			{
				isNull = true;
				return -1;
			}
			break;
		}
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::INT:	
		{
			val = parm[0]->data()->getIntVal(row, isNull);
			ostringstream oss;
			oss << val;
			//decode with provided format
			formatStr = parm[1]->data()->getStrVal(row, isNull);
			rc = extractTime (oss.str(), formatStr, dateTime);
			if ( rc < 0)
			{
				isNull = true;
				return -1;
			}
			break;	
		}
		case CalpontSystemCatalog::DECIMAL:
		{
			if (parm[0]->data()->resultType().scale == 0)
			{
				val = parm[0]->data()->getIntVal(row, isNull);
				
				ostringstream oss;
				oss << val;
				//decode with provided format
				formatStr = parm[1]->data()->getStrVal(row, isNull);
				rc = extractTime (oss.str(), formatStr, dateTime);
				if ( rc < 0)
				{
					isNull = true;
					return -1;
				}
			}
			break;
		}
		default:
			isNull = true;
			return -1;
	}
	return dateTime;
}

string Func_str_to_date::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	dataconvert::DateTime dateTime;
	dateTime = getDateTime(row, parm, isNull, ct);
	string convertedDate = dataconvert::DataConvert::datetimeToString(*((long long*) &dateTime));
	return convertedDate;
}

int32_t Func_str_to_date::getDateIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	dataconvert::DateTime dateTime;
	dateTime = getDateTime(row, parm, isNull, ct);
	int64_t time = *(reinterpret_cast<int64_t*>(&dateTime));
	return (((int32_t)(time >> 32)) & 0xFFFFFFC0);
}		

int64_t Func_str_to_date::getDatetimeIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	dataconvert::DateTime dateTime;
	dateTime = getDateTime(row, parm, isNull, ct);
	int64_t time = *(reinterpret_cast<int64_t*>(&dateTime));
	return time;
}

int64_t Func_str_to_date::getIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	dataconvert::DateTime dateTime;
	dateTime = getDateTime(row, parm, isNull, ct);
	int64_t time = *(reinterpret_cast<int64_t*>(&dateTime));
	return time;
}								


} // namespace funcexp
// vim:ts=4 sw=4:
