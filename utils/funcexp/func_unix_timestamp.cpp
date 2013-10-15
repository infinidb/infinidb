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

/****************************************************************************
* $Id: func_unix_timestamp.cpp 2521 2011-05-02 19:36:52Z zzhu $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
#include "funchelpers.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_unix_timestamp::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}


int64_t Func_unix_timestamp::getIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	int64_t val = parm[0]->data()->getIntVal(row, isNull);
	if (isNull) { //no paramter, return current unix_timestamp
		// get current time in seconds
		time_t cal;
		time (&cal);
		return (int64_t) cal;
	}

	uint32_t year = 0,
	         month = 0,
	         day = 0,
	         hour = 0,
	         min = 0,
	         sec = 0;


	switch (parm[0]->data()->resultType().colDataType)
	{
		case CalpontSystemCatalog::DATE:
			val = parm[0]->data()->getIntVal(row, isNull);
			year = (uint32_t)((val >> 16) & 0xffff);
			month = (uint32_t)((val >> 12) & 0xf);
			day = (uint32_t)((val >> 6) & 0x3f);
			break;
		case CalpontSystemCatalog::DATETIME:
			val = parm[0]->data()->getIntVal(row, isNull);
			year = (uint32_t)((val >> 48) & 0xffff);
			month = (uint32_t)((val >> 44) & 0xf);
			day = (uint32_t)((val >> 38) & 0x3f);
			hour = (uint32_t)((val >> 32) & 0x3f);
			min = (uint32_t)((val >> 26) & 0x3f);
			sec = (uint32_t)((val >> 20) & 0x3f);
			break;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
			val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));
			if (val == -1)
			{
				isNull = true;
				return -1;
			}
			else
			{
				year = (uint32_t)((val >> 48) & 0xffff);
				month = (uint32_t)((val >> 44) & 0xf);
				day = (uint32_t)((val >> 38) & 0x3f);
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
				return -1;
			}
			else
			{
				year = (uint32_t)((val >> 48) & 0xffff);
				month = (uint32_t)((val >> 44) & 0xf);
				day = (uint32_t)((val >> 38) & 0x3f);
			}
			break;	
		case CalpontSystemCatalog::DECIMAL:
			if (parm[0]->data()->resultType().scale == 0)
			{
				val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
				if (val == -1)
				{
					isNull = true;
					return -1;
				}
				else
				{
					year = (uint32_t)((val >> 48) & 0xffff);
					month = (uint32_t)((val >> 44) & 0xf);
					day = (uint32_t)((val >> 38) & 0x3f);
				}
			}
			break;
		default:
			isNull = true;
			return -1;
	}

	// same algorithm as my_time.c:my_system_gmt_sec
	uint loop;
	time_t tmp_t= 0;
	int shift= 0;
	struct tm *l_time,tm_tmp;
	int64_t diff, my_time_zone = parm[1]->data()->getIntVal(row, isNull);

	if ((year == helpers::TIMESTAMP_MAX_YEAR) && (month == 1) && (day > 4))
	{
		day-= 2;
		shift= 2;
	}

	tmp_t= (time_t)(((helpers::calc_mysql_daynr(year,month,day) -
	               719528)* 86400L + (int64_t)hour*3600L +
	               (int64_t)(min*60 + sec)) - (time_t)my_time_zone);

	localtime_r(&tmp_t,&tm_tmp);
	l_time=&tm_tmp;
	for (loop=0; loop < 2 && (hour != (uint32_t) l_time->tm_hour || min != (uint32_t) l_time->tm_min ||
	     sec != (uint32_t)l_time->tm_sec); loop++)
	{
		int days= day - l_time->tm_mday;
		if (days < -1)
			days= 1; /* Month has wrapped */
		else if (days > 1)
			days= -1;
		diff=(3600L*(int64_t) (days*24+((int) hour - (int) l_time->tm_hour)) +
		      (int64_t) (60*((int) min - (int) l_time->tm_min)) +
		      (int64_t) ((int) sec - (int) l_time->tm_sec));
		tmp_t+= (time_t) diff;
		localtime_r(&tmp_t,&tm_tmp);
		l_time=&tm_tmp;
	}

	if (loop == 2 && hour != (uint32_t)l_time->tm_hour)
	{
		int days= day - l_time->tm_mday;
		if (days < -1)
			days=1; /* Month has wrapped */
		else if (days > 1)
			days= -1;
		diff=(3600L*(int64_t) (days*24+((int) hour - (int) l_time->tm_hour))+
		     (int64_t) (60*((int) min - (int) l_time->tm_min)) +
		     (int64_t) ((int) sec - (int) l_time->tm_sec));
		if (diff == 3600)
			tmp_t+=3600 - min*60 - sec;	/* Move to next hour */
		else if (diff == -3600)
			tmp_t-=min*60 + sec;		/* Move to previous hour */
	}


	/* shift back, if we were dealing with boundary dates */
	tmp_t += shift*86400L;

	/* make sure we have legit timestamps (i.e. we didn't over/underflow anywhere above) */
	if ((tmp_t < helpers::TIMESTAMP_MIN_VALUE) || (tmp_t > helpers::TIMESTAMP_MAX_VALUE))
		tmp_t = 0;

	return (int64_t)tmp_t;
}


string Func_unix_timestamp::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return dataconvert::DataConvert::datetimeToString(getIntVal(row, parm, isNull, ct));

}

} // namespace funcexp
// vim:ts=4 sw=4:
