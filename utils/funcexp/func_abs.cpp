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
* $Id: func_abs.cpp 3495 2013-01-21 14:09:51Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <unistd.h>
using namespace std;

#include "functor_real.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_abs::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}

int64_t Func_abs::getIntVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	// null value is indicated by isNull
	return llabs(parm[0]->data()->getIntVal(row, isNull));
}

uint64_t Func_abs::getUintVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	// null value is indicated by isNull
	return parm[0]->data()->getIntVal(row, isNull);
}

IDB_Decimal Func_abs::getDecimalVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
	d.value = llabs(d.value);
	return d;
}


double Func_abs::getDoubleVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	return fabs(parm[0]->data()->getDoubleVal(row, isNull));
}


} // namespace funcexp
// vim:ts=4 sw=4:
