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
* $Id: func_date_format.cpp 3048 2012-04-04 15:33:45Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
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
const string IDB_date_format(const DateTime& dt, const string& format)
{
	// assume 256 is enough. assume not allowing incomplete date
	char buf[256];
	char* ptr = buf;
	uint32_t weekday = 0,
	         lyear = 0;
	
	for (uint i = 0; i < format.length(); i++)
	{
		if (format[i] != '%')
			*ptr++ = format[i];
		else
		{
			i++;
			switch (format[i]) 
			{
			case 'M':
				sprintf(ptr, "%s", monthFullNames[dt.month].c_str());
				ptr += monthFullNames[dt.month].length();
				break;
			case 'b':
				sprintf(ptr, "%s", monthAbNames[dt.month].c_str());
				ptr += monthAbNames[dt.month].length();
				break;
			case 'W':
			  weekday= calc_weekday(calc_daynr(dt.year, dt.month, dt.day), 0);
			  sprintf(ptr, "%s", weekdayFullNames[weekday].c_str());
				ptr += weekdayFullNames[weekday].length();
				break;
			case 'w':
			  	weekday= calc_weekday(calc_daynr(dt.year, dt.month, dt.day), 1);
				sprintf(ptr, "%01d", weekday);
				ptr += 1;
				break;
			case 'a':
				weekday=calc_weekday(calc_daynr(dt.year, dt.month, dt.day), 0);
				sprintf(ptr, "%s", weekdayAbNames[weekday].c_str());
				ptr += weekdayAbNames[weekday].length();
				break;
			case 'D':
				sprintf(ptr, "%s", dayOfMonth[dt.day].c_str());
				ptr += dayOfMonth[dt.day].length();
			break;
			case 'Y':
				sprintf(ptr, "%04d", dt.year);
				ptr += 4; 
				break;
			case 'y':
				sprintf(ptr, "%02d", dt.year % 100);
				ptr += 2;
				break;
			case 'm':
				sprintf(ptr, "%02d", dt.month);
				ptr += 2;
				break;
			case 'c':
				sprintf(ptr, "%d", dt.month);
				ptr = ptr + (dt.month >= 10 ? 2 : 1);
				break;
			case 'd':
				sprintf(ptr, "%02d", dt.day);
				ptr += 2;
				break;
			case 'e':
				sprintf(ptr, "%d", dt.day);
				ptr = ptr + (dt.day >= 10 ? 2 : 1);
				break;
			case 'f':
				sprintf(ptr, "%06d", dt.msecond);
				ptr += 6;
				break;
			case 'H':
				sprintf(ptr, "%02d", dt.hour);
				ptr += 2;
				break;
			case 'h':
			case 'I':
				sprintf(ptr, "%02d", (dt.hour%24 + 11)%12+1);
				ptr += 2;
				break;
			case 'i':					/* minutes */
				sprintf(ptr, "%02d", dt.minute);
				ptr += 2;
				break;
			case 'j':
				sprintf(ptr, "%03d", calc_daynr(dt.year, dt.month, dt.day) - calc_daynr(dt.year,1,1) + 1);
				ptr += 3;
				break;
			case 'k':
				sprintf(ptr, "%d", dt.hour);
				ptr += (dt.hour >= 10 ? 2 : 1);
				break;
			case 'l':
				sprintf(ptr, "%d", (dt.hour%24 + 11)%12+1);
				ptr += ((dt.hour%24 + 11)%12+1 >= 10 ? 2 : 1);
				break;
			case 'p':
				sprintf(ptr, "%s", (dt.hour % 24 < 12 ? "AM" : "PM"));
				ptr += 2;
				break;
			case 'r':
				sprintf(ptr, (dt.hour % 24 < 12 ? "%02d:%02d:%02d AM" : "%02d:%02d:%02d PM"), 
	             (dt.hour + 11) % 12 + 1, dt.minute, dt.second);
				ptr += 11;
				break;
			case 'S':
			case 's':
				sprintf(ptr, "%02d", dt.second);
				ptr += 2;
				break;
			case 'T':
				sprintf (ptr, "%02d:%02d:%02d", dt.hour, dt.minute, dt.second);
				ptr += 8;
				break;
			case 'U':
				sprintf(ptr, "%02d", calc_week(dt.year, dt.month, dt.day, week_first_weekday, &lyear));
				ptr += 2;
				break;
			case 'V':
				sprintf(ptr, "%02d", calc_week(dt.year, dt.month, dt.day, week_Year | week_first_weekday, &lyear));
				ptr += 2;
				break;
			case 'u':
				sprintf(ptr, "%02d", calc_week(dt.year, dt.month, dt.day, week_monday_first, &lyear));
				ptr += 2;
				break;
			case 'v':
				sprintf(ptr, "%02d", calc_week(dt.year, dt.month, dt.day, week_Year | week_monday_first, &lyear));
				ptr += 2;
				break;
			case 'x':
				(void) calc_week(dt.year, dt.month, dt.day, week_Year | week_monday_first, &lyear);
				sprintf(ptr, "%04d", lyear);
				ptr += 4;
				break;
			case 'X':
				(void) calc_week(dt.year, dt.month, dt.day, week_Year | week_first_weekday, &lyear);
				sprintf(ptr, "%04d", lyear);
				ptr += 4;
				break;
			default:
				*ptr++ = format[i];
	    }
	  }
	}
	*ptr = 0;
	return string(buf);
}

CalpontSystemCatalog::ColType Func_date_format::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	CalpontSystemCatalog::ColType ct;
	ct.colDataType = CalpontSystemCatalog::VARCHAR;
	ct.colWidth = 255;
	return ct;
}

string Func_date_format::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	int64_t val = 0;
	DateTime dt = 0;

	switch (parm[0]->data()->resultType().colDataType)
	{
		case CalpontSystemCatalog::DATE:
			val = parm[0]->data()->getIntVal(row, isNull);
			dt.year = (uint32_t)((val >> 16) & 0xffff);
			dt.month = (uint32_t)((val >> 12) & 0xf);
			dt.day = (uint32_t)((val >> 6) & 0x3f);
			break;
		case CalpontSystemCatalog::DATETIME:
			val = parm[0]->data()->getIntVal(row, isNull);
			dt.year = (uint32_t)((val >> 48) & 0xffff);
			dt.month = (uint32_t)((val >> 44) & 0xf);
			dt.day = (uint32_t)((val >> 38) & 0x3f);
			dt.hour = (uint32_t)((val >> 32) & 0x3f);
			dt.minute = (uint32_t)((val >> 26) & 0x3f);
			dt.second = (uint32_t)((val >> 20) & 0x3f);
			dt.msecond = (uint32_t)((val & 0xfffff));
			break;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
			val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));
			if (val == -1)
			{
				isNull = true;
				return "";
			}
			else
			{
				dt.year = (uint32_t)((val >> 48) & 0xffff);
				dt.month = (uint32_t)((val >> 44) & 0xf);
				dt.day = (uint32_t)((val >> 38) & 0x3f);
				dt.hour = (uint32_t)((val >> 32) & 0x3f);
				dt.minute = (uint32_t)((val >> 26) & 0x3f);
				dt.second = (uint32_t)((val >> 20) & 0x3f);
				dt.msecond = (uint32_t)((val & 0xfffff));
			}
			break;
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::INT:
			val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
			if (val == -1)
			{
				isNull = true;
				return "";
			}
			else
			{
				dt.year = (uint32_t)((val >> 48) & 0xffff);
				dt.month = (uint32_t)((val >> 44) & 0xf);
				dt.day = (uint32_t)((val >> 38) & 0x3f);
				dt.hour = (uint32_t)((val >> 32) & 0x3f);
				dt.minute = (uint32_t)((val >> 26) & 0x3f);
				dt.second = (uint32_t)((val >> 20) & 0x3f);
				dt.msecond = (uint32_t)((val & 0xfffff));
			}
			break;	
		case CalpontSystemCatalog::DECIMAL:
			if (parm[0]->data()->resultType().scale == 0)
			{
				val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
				if (val == -1)
				{
					isNull = true;
					return "";
				}
				else
				{
					dt.year = (uint32_t)((val >> 48) & 0xffff);
					dt.month = (uint32_t)((val >> 44) & 0xf);
					dt.day = (uint32_t)((val >> 38) & 0x3f);
					dt.hour = (uint32_t)((val >> 32) & 0x3f);
					dt.minute = (uint32_t)((val >> 26) & 0x3f);
					dt.second = (uint32_t)((val >> 20) & 0x3f);
					dt.msecond = (uint32_t)((val & 0xfffff));
				}
			}
			break;
		default:
			isNull = true;
			return "";
	}
	
	string format = parm[1]->data()->getStrVal(row, isNull);

	return IDB_date_format(dt, format);
}


int32_t Func_date_format::getDateIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return dataconvert::DataConvert::dateToInt(getStrVal(row, parm, isNull, ct));
}		


int64_t Func_date_format::getDatetimeIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return dataconvert::DataConvert::datetimeToInt(getStrVal(row, parm, isNull, ct));
}


} // namespace funcexp
// vim:ts=4 sw=4:
