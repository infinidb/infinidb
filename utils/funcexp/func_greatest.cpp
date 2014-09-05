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
* $Id: func_greatest.cpp 3957 2013-07-08 21:20:36Z bpaul $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_all.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "utils_utf8.h"
using namespace funcexp;


class to_lower
{
    public:
        char operator() (char c) const            // notice the return type
        {
            return tolower(c);
        }
};


namespace funcexp
{

CalpontSystemCatalog::ColType Func_greatest::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	//return fp[0]->data()->resultType();
	return resultType;
}

int64_t Func_greatest::getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	double str = fp[0]->data()->getDoubleVal(row, isNull);

	double greatestStr = str;
	for (uint i = 1; i < fp.size(); i++)
	{
		double str1 = fp[i]->data()->getDoubleVal(row, isNull);

		if ( greatestStr < str1 )
			greatestStr = str1;
	}
    uint64_t tmp = (uint64_t)greatestStr;
	return (int64_t) tmp;
}

uint64_t Func_greatest::getUintVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	double str = fp[0]->data()->getDoubleVal(row, isNull);

	double greatestStr = str;
	for (uint i = 1; i < fp.size(); i++)
	{
		double str1 = fp[i]->data()->getDoubleVal(row, isNull);

		if ( greatestStr < str1 )
			greatestStr = str1;
	}

	return (uint64_t) greatestStr;
}

double Func_greatest::getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	double str = fp[0]->data()->getDoubleVal(row, isNull);

	double greatestStr = str;
	for (uint i = 1; i < fp.size(); i++)
	{
		double str1 = fp[i]->data()->getDoubleVal(row, isNull);

		if ( greatestStr < str1 )
			greatestStr = str1;
	}

	return (double) greatestStr;
}

std::string Func_greatest::getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	string str = fp[0]->data()->getStrVal(row, isNull);

	string greatestStr = str;
	for (uint i = 1; i < fp.size(); i++)
	{
		string str1 = fp[i]->data()->getStrVal(row, isNull);

		int tmp = utf8::idb_strcoll(greatestStr.c_str(), str1.c_str());
		if ( tmp < 0 )

//		if ( greatestStr < str1 )
			greatestStr = str1;
	}

	return greatestStr;
}

IDB_Decimal Func_greatest::getDecimalVal(Row& row,
							FunctionParm& fp,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
//	double str = fp[0]->data()->getDoubleVal(row, isNull);
	IDB_Decimal str = fp[0]->data()->getDecimalVal(row, isNull);

	IDB_Decimal greatestStr = str;
	for (uint i = 1; i < fp.size(); i++)
	{
		IDB_Decimal str1 = fp[i]->data()->getDecimalVal(row, isNull);

		if ( greatestStr < str1 )
			greatestStr = str1;
	}

	return greatestStr;
}

} // namespace funcexp
// vim:ts=4 sw=4:

