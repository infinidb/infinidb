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
* $Id: func_from_unixtime.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
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
using namespace dataconvert;


namespace
{
using namespace funcexp;

DateTime getDateTime(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull)
{
	int64_t val;
	switch (parm[0]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::DECIMAL:
		{
			double value = parm[0]->data()->getDoubleVal(row, isNull);
			if (value > 0)
				value += 0.5;
			else if (value < 0)
				value -= 0.5;
			val = (int64_t)value;
			break;
		}
		default:
			val = parm[0]->data()->getIntVal(row, isNull);
	}
	
	if (val < 0 || val > helpers::TIMESTAMP_MAX_VALUE)
		return 0;

	DateTime dt;

	struct tm tmp_tm;
  time_t tmp_t= (time_t)val;
  localtime_r(&tmp_t, &tmp_tm);

	//to->neg=0;
	dt.year = (int64_t) ((tmp_tm.tm_year+1900) % 10000);
	dt.month = (int64_t) tmp_tm.tm_mon+1;
	dt.day = (int64_t) tmp_tm.tm_mday;
	dt.hour = (int64_t) tmp_tm.tm_hour;
	dt.minute = (int64_t) tmp_tm.tm_min;
	dt.second = (int64_t) tmp_tm.tm_sec;
	dt.msecond = 0;
	return dt;
}
}

namespace funcexp
{

CalpontSystemCatalog::ColType Func_from_unixtime::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	CalpontSystemCatalog::ColType ct;
	ct.colDataType = CalpontSystemCatalog::VARCHAR;
	ct.colWidth = 255;
	return ct;
}

string Func_from_unixtime::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{	
	DateTime dt = getDateTime(row, parm, isNull);
	if (*reinterpret_cast<int64_t*>(&dt) == 0)
	{
		isNull = true;
		return "";
	}

	if (parm.size() == 2)
	{
		const string& format = parm[1]->data()->getStrVal(row, isNull);
		return helpers::IDB_date_format(dt, format);
	}
	
	char buf[256] = {0};
	DataConvert::datetimeToString(*(reinterpret_cast<int64_t*>(&dt)), buf, 255);
	return string(buf, 255);
}

int32_t Func_from_unixtime::getDateIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return (getDatetimeIntVal(row, parm, isNull, ct) >> 32) & 0xFFFFFFC0;
}		

int64_t Func_from_unixtime::getDatetimeIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	DateTime dt = getDateTime(row, parm, isNull);
	if (*reinterpret_cast<int64_t*>(&dt) == 0)
	{
		isNull = true;
		return 0;
	}
	return *reinterpret_cast<int64_t*>(&dt);	
}

int64_t Func_from_unixtime::getIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return getDatetimeIntVal(row, parm, isNull, ct);
}								

double Func_from_unixtime::getDoubleVal(rowgroup::Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& ct)
{
	if (parm.size() == 1)
	{
		DateTime dt = getDateTime(row, parm, isNull);
		if (*reinterpret_cast<int64_t*>(&dt) == 0)
		{
			isNull = true;
			return 0;
		}
        char buf[32];  // actual string guaranteed to be 22
        snprintf( buf, 32, "%04d%02d%02d%02d%02d%02d.%06d",
                  dt.year, dt.month, dt.day, dt.hour,
				  dt.minute, dt.second, dt.msecond ); 
		return atof(buf);
	}
	
	return (double) atoi(getStrVal(row, parm, isNull, ct).c_str());
}


} // namespace funcexp
// vim:ts=4 sw=4:
