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
* $Id: func_period_diff.cpp 2477 2011-04-01 16:07:35Z rdempsey $
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
#define YY_PART_YEAR 70

inline int64_t convert_period_to_month(int64_t period)
{
  int64_t a,b;
  if (period == 0)
    return 0L;
  if ((a=period/100) < YY_PART_YEAR)
    a+=2000;
  else if (a < 100)
    a+=1900;
  b=period%100;
  return a*12+b-1;
}


CalpontSystemCatalog::ColType Func_period_diff::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_period_diff::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	int64_t period1 = 0;

	switch (parm[0]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::DATE:
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			period1 = parm[0]->data()->getIntVal(row, isNull);
			break;
		}
		case execplan::CalpontSystemCatalog::DECIMAL:
		{
			IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
			period1 = d.value / power(d.scale);
			break;
		}
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			period1 = atoi(parm[0]->data()->getStrVal(row, isNull).c_str());
			break;
		}
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
		{
			period1 = (int64_t) parm[0]->data()->getDoubleVal(row, isNull);
			break;
		}
		default:
		{
			isNull = true;
		}
	}

	if (isNull)
		return 0;

	int64_t period2 = 0;

	switch (parm[1]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::DATE:
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			period2 = parm[1]->data()->getIntVal(row, isNull);
			break;
		}
		case execplan::CalpontSystemCatalog::DECIMAL:
		{
			IDB_Decimal d = parm[1]->data()->getDecimalVal(row, isNull);
			period2 = d.value / power(d.scale);
			break;
		}
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			period2 = atoi(parm[1]->data()->getStrVal(row, isNull).c_str());
			break;
		}
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
		{
			period2 = (int64_t) parm[1]->data()->getDoubleVal(row, isNull);
			break;
		}
		default:
		{
			isNull = true;
		}
	}

	if (isNull)
		return 0;

	return convert_period_to_month(period1) - convert_period_to_month(period2);
}


} // namespace funcexp
// vim:ts=4 sw=4:
