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
* $Id: func_rtrim.cpp 2675 2011-06-04 04:58:07Z xlou $
*
*
****************************************************************************/

#include <string>
using namespace std;

#include "functor_str.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

namespace funcexp
{

CalpontSystemCatalog::ColType Func_rtrim::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}


std::string Func_rtrim::getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType&)
{
	string str = fp[0]->data()->getStrVal(row, isNull);
	string trim = (fp.size() > 1 ? fp[1]->data()->getStrVal(row, isNull) : " ");

	for(;;)
	{
		string::size_type pos = str.rfind (trim,str.length()-1);
		if (pos == strlen(str.c_str())-1 && pos != string::npos)
		{
			str = str.substr (0,pos);
		}
		else
		{                                     // no more whitespace
			break;
		}
	}

	return str;
}							


} // namespace funcexp
// vim:ts=4 sw=4:

