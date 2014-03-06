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
* $Id: func_timestampdiff.cpp 3921 2013-06-19 18:59:56Z bwilkinson $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functioncolumn.h"
#include "constantcolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

#include "functor_dtm.h"
#include "funchelpers.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_timestampdiff::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_timestampdiff::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	int64_t val1 = parm[0]->data()->getDatetimeIntVal(row, isNull);
	int64_t val2 = parm[1]->data()->getDatetimeIntVal(row, isNull);
    IntervalColumn::interval_type unit = static_cast<IntervalColumn::interval_type>(parm[2]->data()->getIntVal(row, isNull));
	DateTime dt1, dt2;
	dt1.year = (val1 >> 48) & 0xffff;
	dt1.month = (val1 >> 44) & 0xf;
	dt1.day = (val1 >> 38) & 0x3f;
	dt1.hour = (val1 >> 32) & 0x3f;
	dt1.minute = (val1 >> 26) & 0x3f;
	dt1.second = (val1 >> 20) & 0x3f;
	dt1.msecond = val1 & 0xfffff;
	 
	dt2.year = (val2 >> 48) & 0xffff;
	dt2.month = (val2 >> 44) & 0xf;
	dt2.day = (val2 >> 38) & 0x3f;
	dt2.hour = (val2 >> 32) & 0x3f;
	dt2.minute = (val2 >> 26) & 0x3f;
	dt2.second = (val2 >> 20) & 0x3f;
	dt2.msecond = val2 & 0xfffff;
	
	int64_t diff = 0;
	
	// unit: MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR
	int64_t monthdiff = ((int64_t)dt2.month - (int64_t)dt1.month) + 
		       ((int64_t)dt2.year - (int64_t)dt1.year) * 12;
	if (unit == IntervalColumn::INTERVAL_YEAR)
	{
		diff = monthdiff / 12;
	}
	else if (unit == IntervalColumn::INTERVAL_MONTH)
	{
		diff = monthdiff;
		if (dt2.day < dt1.day && monthdiff >0)
			diff = monthdiff - 1;
		else if (dt1.day < dt2.day && monthdiff < 0)
			diff = monthdiff + 1;
	}
	else if (unit == IntervalColumn::INTERVAL_QUARTER)
	{
		diff = monthdiff / 3;
		int daydiff = monthdiff %3;
		if (daydiff == 0)
		{
			if (dt2.day < dt1.day && monthdiff > 0)
				diff --;
			else if (dt1.day < dt2.day && monthdiff < 0)
				diff ++;
		}
	}
	else
	{
		int64_t seconds = 0, mseconds = 0;
		int l_sign = 1;
		int l_sign3;

		l_sign3 = helpers::calc_time_diff(val2, val1, l_sign, (long long*)&seconds, (long long*)&mseconds);
		l_sign3 = (l_sign3 == 0 ? 1 : -1);
		if (unit == IntervalColumn::INTERVAL_SECOND)
			diff = l_sign3 * seconds;
		else if (unit == IntervalColumn::INTERVAL_MICROSECOND)
			diff = l_sign3 * (seconds * 1000000L + mseconds);
		else if (unit == IntervalColumn::INTERVAL_MINUTE)
			diff = l_sign3 * (seconds/60L);
		else if (unit == IntervalColumn::INTERVAL_HOUR)
			diff = l_sign3 * (seconds/3600L);
		else if (unit == IntervalColumn::INTERVAL_DAY)
			diff = l_sign3 * (seconds/(24L*3600L));
		else if (unit == IntervalColumn::INTERVAL_WEEK)
			diff = l_sign3 * (seconds/(24L*3600L)/7L); 
	}
	return diff;
}

string Func_timestampdiff::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return intToString(getIntVal(row, parm, isNull, ct));
}

int32_t Func_timestampdiff::getDateIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return getIntVal(row, parm, isNull, ct);
}

int64_t Func_timestampdiff::getDatetimeIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return getIntVal(row, parm, isNull, ct);
}


} // namespace funcexp
// vim:ts=4 sw=4:
