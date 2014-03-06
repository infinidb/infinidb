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
* $Id: func_time_format.cpp 2477 2011-04-01 16:07:35Z rdempsey $
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

namespace funcexp
{

CalpontSystemCatalog::ColType Func_time_format::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	CalpontSystemCatalog::ColType ct;
	ct.colDataType = CalpontSystemCatalog::VARCHAR;
	ct.colWidth = 255;
	return ct;
}

string Func_time_format::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	// assume 256 is enough. assume not allowing incomplete date
	char buf[256];
	int64_t val = 0;
	uint32_t hour = 0, 
	         min = 0, 
	         sec = 0, 
	         msec = 0;

	switch (parm[0]->data()->resultType().colDataType)
	{
		case CalpontSystemCatalog::DATE:
			isNull = true;
			return "";
			break;
		case CalpontSystemCatalog::DATETIME:
			val = parm[0]->data()->getIntVal(row, isNull);
			hour = (uint32_t)((val >> 32) & 0x3f);
			min = (uint32_t)((val >> 26) & 0x3f);
			sec = (uint32_t)((val >> 20) & 0x3f);
			msec = (uint32_t)((val & 0xfffff));
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
				hour = (uint32_t)((val >> 32) & 0x3f);
				min = (uint32_t)((val >> 26) & 0x3f);
				sec = (uint32_t)((val >> 20) & 0x3f);
				msec = (uint32_t)((val & 0xfffff));
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
				hour = (uint32_t)((val >> 32) & 0x3f);
				min = (uint32_t)((val >> 26) & 0x3f);
				sec = (uint32_t)((val >> 20) & 0x3f);
				msec = (uint32_t)((val & 0xfffff));
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
					hour = (uint32_t)((val >> 32) & 0x3f);
					min = (uint32_t)((val >> 26) & 0x3f);
					sec = (uint32_t)((val >> 20) & 0x3f);
					msec = (uint32_t)((val & 0xfffff));
				}
			}
			break;
		default:
			isNull = true;
			return "";
	}
	
	const string& format = parm[1]->data()->getStrVal(row, isNull);

	char* ptr = buf;
	
	for (uint32_t i = 0; i < format.length(); i++)
	{
		if (format[i] != '%')
			*ptr++ = format[i];
		else
		{
			i++;
			switch (format[i]) 
			{
			case 'f':
				sprintf(ptr, "%06d", msec);
				ptr += 6;
				break;
			case 'H':
				sprintf(ptr, "%02d", hour);
				ptr += 2;
				break;
			case 'h':
			case 'I':
				sprintf(ptr, "%02d", (hour%24 + 11)%12+1);
				ptr += 2;
				break;
			case 'i':					/* minutes */
				sprintf(ptr, "%02d", min);
				ptr += 2;
				break;
			case 'k':
				sprintf(ptr, "%d", hour);
				ptr += (hour >= 10 ? 2 : 1);
				break;
			case 'l':
				sprintf(ptr, "%d", (hour%24 + 11)%12+1);
				ptr += ((hour%24 + 11)%12+1 >= 10 ? 2 : 1);
				break;
			case 'p':
				sprintf(ptr, "%s", (hour % 24 < 12 ? "AM" : "PM"));
				ptr += 2;
				break;
			case 'r':
				sprintf(ptr, (hour % 24 < 12 ? "%02d:%02d:%02d AM" : "%02d:%02d:%02d PM"), 
	             (hour + 11) % 12 + 1, min, sec);
				ptr += 11;
				break;
			case 'S':
			case 's':
				sprintf(ptr, "%02d", sec);
				ptr += 2;
				break;
			case 'T':
				sprintf (ptr, "%02d:%02d:%02d", hour, min, sec);
				ptr += 8;
				break;
			default:
				isNull = true;
				return "";
	    }
	  }
	}
	*ptr = 0;
	return string(buf);
}


} // namespace funcexp
// vim:ts=4 sw=4:
