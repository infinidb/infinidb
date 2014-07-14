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
* $Id: func_from_days.cpp 3921 2013-06-19 18:59:56Z bwilkinson $
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
using namespace dataconvert;

namespace funcexp
{

CalpontSystemCatalog::ColType Func_from_days::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_from_days::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	return getDatetimeIntVal(row, parm, isNull, op_ct);
}

string Func_from_days::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return intToString(getIntVal(row, parm, isNull, ct));
}

int32_t Func_from_days::getDateIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return (((getDatetimeIntVal(row, parm, isNull, ct) >> 32) & 0xFFFFFFC0) | 0x3E);
}

int64_t Func_from_days::getDatetimeIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	double val1 = parm[0]->data()->getDoubleVal(row, isNull);
	int64_t daynr = (int64_t)(val1 > 0 ? val1 + 0.5 : val1 - 0.5);

	DateTime aDaytime;
	helpers::get_date_from_mysql_daynr( daynr, aDaytime );
	
	// to be safe
	aDaytime.hour = 0;
	aDaytime.minute = 0;
	aDaytime.second = 0;
	aDaytime.msecond = 0;

	return (*(reinterpret_cast<uint64_t *> (&aDaytime)));
}


} // namespace funcexp
// vim:ts=4 sw=4:
