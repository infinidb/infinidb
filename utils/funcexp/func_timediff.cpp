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
* $Id: func_timediff.cpp 3614 2013-02-28 16:45:09Z dhall $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_dtm.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

namespace funcexp
{

CalpontSystemCatalog::ColType Func_timediff::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

string timediff( int64_t time1, int64_t time2)
{
	long long seconds;
	long long microseconds;
	int l_sign = 1;

	if ( (time1 < 0 && time2 >= 0) ||
			(time2 < 0 && time1 >= 0) )
		l_sign = -l_sign;

	if ( time1 > time2 )
		calc_time_diff(time1, time2, l_sign, &seconds, &microseconds);
	else
		calc_time_diff(time2, time1, l_sign, &seconds, &microseconds);

	long t_seconds;
	int hour= seconds/3600L;
	t_seconds= seconds%3600L;
	int minute= t_seconds/60L;
	int second= t_seconds%60L;

    // Bug 5099: Standardize to mysql behavior. No timediff may be > 838:59:59
    if (hour > 838)
    {
        hour = 838;
        minute = 59;
        second = 59;
    }
	int sign = 0;
	if ( time1 < time2 )
		sign = 1;

	char buf[256];
	char* ptr = buf;
	string time3;
	if ( microseconds == 0 ) 
		sprintf (ptr, "%s%02d:%02d:%02d", sign ? "-" : "", hour, minute, second);
	else
		sprintf (ptr, "%s%02d:%02d:%02d:%06lld", sign ? "-" : "",hour, minute, second, microseconds);

	time3 = ptr;

	return time3;
}

string Func_timediff::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	int16_t type1 = parm[0]->data()->resultType().colDataType;
	int16_t type2 = parm[1]->data()->resultType().colDataType;
	
	int64_t val1 = -1, val2 = -1;
	bool isDate1 = false, isDate2 = false;

	switch (type1)
	{
		case execplan::CalpontSystemCatalog::DATE:
			val1 = parm[0]->data()->getDatetimeIntVal(row, isNull);
			isDate1 = true;
			break;
		case execplan::CalpontSystemCatalog::DATETIME:
			val1 = parm[0]->data()->getDatetimeIntVal(row, isNull);
			break;
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
			val1 = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull), &isDate1);
			break;
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
			val1 = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull), &isDate1);
			break;
		case execplan::CalpontSystemCatalog::DECIMAL:
			if (parm[0]->data()->resultType().scale != 0)
			{
				isNull = true;
				break;
			}
			else
			{
				val1 = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull), &isDate1);
				break;
			}
		default:
			isNull = true;
	}
	
	switch (type2)
	{
		case execplan::CalpontSystemCatalog::DATE:
			val2 = parm[1]->data()->getDatetimeIntVal(row, isNull);
			isDate2 = true;
			break;
		case execplan::CalpontSystemCatalog::DATETIME:
			val2 = parm[1]->data()->getDatetimeIntVal(row, isNull);
			break;
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
			val2 = dataconvert::DataConvert::stringToDatetime(parm[1]->data()->getStrVal(row, isNull), &isDate2);
			break;
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
			val2 = dataconvert::DataConvert::intToDatetime(parm[1]->data()->getIntVal(row, isNull), &isDate2);
			break;
		case execplan::CalpontSystemCatalog::DECIMAL:
			if (parm[1]->data()->resultType().scale != 0)
			{
				isNull = true;
				break;
			}
			else
			{
				val2 = dataconvert::DataConvert::intToDatetime(parm[1]->data()->getIntVal(row, isNull), &isDate2);
				break;
			}
		default:
			isNull = true;
	}
	
	if (isNull || val1 == -1 || val2 == -1)
	{
		isNull = true;
		return "";
	}
	
	// both date format or both datetime format
	if ((isDate1 && isDate2) || (!isDate1 && !isDate2))
		return timediff( val1, val2);
	isNull = true;
	return "";
}

int64_t Func_timediff::getDatetimeIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return dataconvert::DataConvert::datetimeToInt(getStrVal(row, parm, isNull, ct));
}

int64_t Func_timediff::getIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	// @bug 3585
	//return dataconvert::DataConvert::datetimeToInt(getStrVal(row, parm, isNull, ct));
	return atol(getStrVal(row, parm, isNull, ct).c_str());
}

double Func_timediff::getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	// @bug 3585
	//return (double)dataconvert::DataConvert::datetimeToInt(getStrVal(row, fp, isNull, op_ct));
	return atof(getStrVal(row, fp, isNull, op_ct).c_str());
}



} // namespace funcexp
// vim:ts=4 sw=4:
