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
* $Id: func_div.cpp 3048 2012-04-04 15:33:45Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_real.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_div::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_div::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	double val1 = parm[0]->data()->getDoubleVal(row, isNull);
	double val2 = parm[1]->data()->getDoubleVal(row, isNull);
	int64_t int_val2 = (int64_t)(val2 > 0 ? val2 + 0.5 : val2 - 0.5);
	if (int_val2 == 0)
	{
		isNull = true;
		return 0;
	}
	int64_t int_val1 = (int64_t)(val1 > 0 ? val1 + 0.5 : val1 - 0.5);
	return int_val1 / int_val2;
}


double Func_div::getDoubleVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& ct)
{
	return getIntVal(row, parm, isNull, ct);
}


string Func_div::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	ostringstream oss;
	oss << getIntVal(row, parm, isNull, ct);
	return oss.str();
}


} // namespace funcexp
// vim:ts=4 sw=4:
