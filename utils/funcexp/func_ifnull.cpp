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
* $Id: func_ifnull.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
*
*
****************************************************************************/

#include <string>
using namespace std;

#include "functor_all.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

namespace funcexp
{

// IFNULL(expression1, expression12)
//
// if parm order:
//   expression1 expression2
//
// If expression1 is not NULL, IFNULL() returns expression1;
// otherwise it returns expression2.
//

CalpontSystemCatalog::ColType Func_ifnull::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}


int64_t Func_ifnull::getIntVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	if (isNull)
		return 0;

	int64_t r = parm[0]->data()->getIntVal(row, isNull);
	if (isNull)
	{
		isNull = false;
		return parm[1]->data()->getIntVal(row, isNull);
	}

	return r;
}


string Func_ifnull::getStrVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	if (isNull)
		return string();

	const string& r = parm[0]->data()->getStrVal(row, isNull);
	if (isNull)
	{
		isNull = false;
		return parm[1]->data()->getStrVal(row, isNull);
	}

	return r;
}


IDB_Decimal Func_ifnull::getDecimalVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	if (isNull)
		return IDB_Decimal();

	IDB_Decimal r = parm[0]->data()->getDecimalVal(row, isNull);
	if (isNull)
	{
		isNull = false;
		return parm[1]->data()->getDecimalVal(row, isNull);
	}

	return r;
}


double Func_ifnull::getDoubleVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	if (isNull)
		return 0.0;

	double r = parm[0]->data()->getDoubleVal(row, isNull);
	if (isNull)
	{
		isNull = false;
		return parm[1]->data()->getDoubleVal(row, isNull);
	}

	return r;
}


int32_t Func_ifnull::getDateIntVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	if (isNull)
		return 0;

	int64_t r = parm[0]->data()->getDateIntVal(row, isNull);
	if (isNull)
	{
		isNull = false;
		return parm[1]->data()->getDateIntVal(row, isNull);
	}

	return r;
}

int64_t Func_ifnull::getDatetimeIntVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	if (isNull)
		return 0;

	int64_t r = parm[0]->data()->getDatetimeIntVal(row, isNull);
	if (isNull)
	{
		isNull = false;
		return parm[1]->data()->getDatetimeIntVal(row, isNull);
	}

	return r;
}

bool Func_ifnull::getBoolVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	int64_t ret = getIntVal(row, parm, isNull, ct);
	return (ret == 0 ? false : true);
}

} // namespace funcexp
// vim:ts=4 sw=4:
