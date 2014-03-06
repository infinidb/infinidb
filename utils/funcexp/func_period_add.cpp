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
* $Id: func_period_add.cpp 2477 2011-04-01 16:07:35Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_period_add::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_period_add::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	int64_t period = parm[0]->data()->getIntVal(row, isNull);

	if ( period < 10000 ) {
		//get first 2 digits of year
		time_t now;
		now = time(NULL);
		struct tm tm;
		localtime_r(&now, &tm);
		char timestamp[10];
		strftime (timestamp, 10, "%Y", &tm);
		string Syear = timestamp;
		Syear = Syear.substr(0,2);
		int topyear = atoi(Syear.c_str());
		period = (topyear * 10000) + period;
	}

	int64_t year = period / 100;

	int64_t month = period - (year * 100);

	int64_t months = parm[1]->data()->getIntVal(row, isNull);

	int64_t yearsAdd = months / 12;

	int64_t monthsAdd = months - (yearsAdd * 12) ;

	year = year + yearsAdd;
	month = month + monthsAdd;

	if ( month > 12 ) {
		year++;
		month = month - 12;
	}
	else
	{
		if ( month < 1 ) {
			year--;
			month = month + 12;
		}
	}


	int64_t value = (year * 100) + month;
	return value;
}


} // namespace funcexp
// vim:ts=4 sw=4:
