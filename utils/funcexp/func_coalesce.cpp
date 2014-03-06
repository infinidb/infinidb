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
* $Id: func_coalesce.cpp 3495 2013-01-21 14:09:51Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

#include "functor_all.h"
#include "funchelpers.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_coalesce::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_coalesce::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	int64_t val = 0;
	for (uint32_t i = 0; i < parm.size(); i++)
	{
		val = parm[i]->data()->getIntVal(row, isNull);
		if (isNull)
		{
			isNull = false;
			continue;
		}
		return val;
	}
	isNull = true;
	return val;
}

string Func_coalesce::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	string val;
	for (uint32_t i = 0; i < parm.size(); i++)
	{
		val = parm[i]->data()->getStrVal(row, isNull);
		if (isNull)
		{
			isNull = false;
			continue;
		}
		return val;
	}
	isNull = true;
	return "";
}

int32_t Func_coalesce::getDateIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	int64_t val = 0;
	for (uint32_t i = 0; i < parm.size(); i++)
	{
		val = parm[i]->data()->getDateIntVal(row, isNull);
		if (isNull)
		{
			isNull = false;
			continue;
		}
		return val;
	}
	isNull = true;
	return val;
}

int64_t Func_coalesce::getDatetimeIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	int64_t val = 0;
	for (uint32_t i = 0; i < parm.size(); i++)
	{
		val = parm[i]->data()->getDatetimeIntVal(row, isNull);
		if (isNull)
		{
			isNull = false;
			continue;
		}
		return val;
	}
	isNull = true;
	return val;
}

double Func_coalesce::getDoubleVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& ct)
{
	double d = 0.0;
	for (uint32_t i = 0; i < parm.size(); i++)
	{
		d = parm[i]->data()->getDoubleVal(row, isNull);
		if (isNull)
		{
			isNull = false;
			continue;
		}
		return d;
	}
	isNull = true;
	return d;
}


execplan::IDB_Decimal Func_coalesce::getDecimalVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& ct)
{
	IDB_Decimal d;
	for (uint32_t i = 0; i < parm.size(); i++)
	{
		d = parm[i]->data()->getDecimalVal(row, isNull);
		if (isNull)
		{
			isNull = false;
			continue;
		}
		return d;
	}
	isNull = true;
	return d;
}


} // namespace funcexp
// vim:ts=4 sw=4:
