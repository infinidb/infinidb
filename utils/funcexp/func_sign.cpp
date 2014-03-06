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
* $Id: func_sign.cpp 3495 2013-01-21 14:09:51Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_sign::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	CalpontSystemCatalog::ColType ct;
	ct.colDataType = CalpontSystemCatalog::BIGINT;
	ct.colWidth = 8;
	return ct;
}

int64_t Func_sign::getIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	double val = parm[0]->data()->getDoubleVal(row, isNull);
	if (isNull)
		return 0;
	else if (val > 0)
		return 1;
	else if (val < 0)
		return -1;
	else
		return 0;
}

string Func_sign::getStrVal(rowgroup::Row& row,
								FunctionParm& parm,
								bool& isNull,
								CalpontSystemCatalog::ColType& op_ct)
{
	int64_t sign = getIntVal(row, parm, isNull, op_ct);
	if (sign > 0)
		return string("1");
	else if (sign < 0)
		return string("-1");
	else
		return string("0");
}

} // namespace funcexp
// vim:ts=4 sw=4:
