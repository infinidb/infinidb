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
* $Id: func_instr.cpp 3645 2013-03-19 13:10:24Z rdempsey $
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
#include "utils_utf8.h"
using namespace execplan;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_instr::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	CalpontSystemCatalog::ColType ct;
	ct.colDataType = CalpontSystemCatalog::VARCHAR;
	ct.colWidth = 255;
	return ct;
}

size_t Func_instr::in_str(const string& str, const string& substr, size_t start)
{
	// convert both inputs to wide character strings
	std::wstring wcstr = utf8::utf8_to_wstring(str);
	std::wstring wcsubstr = utf8::utf8_to_wstring(substr);

	if ((str.length() && !wcstr.length()) ||
		(substr.length() && !wcsubstr.length()))
		// this means one or both of the strings had conversion errors to wide character
		return 0;

	size_t pos = wcstr.find(wcsubstr, start-1);
	return (pos != string::npos ? pos+1 : 0);
}

int64_t Func_instr::getIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	uint64_t start = 1;
	if (parm.size() == 3)
		start = parm[2]->data()->getIntVal(row, isNull);

	if (isNull || start == 0)
		return 0;

	//Bug 5110 : to support utf8 char type, we have to convert and search  
	return in_str(parm[0]->data()->getStrVal(row, isNull), parm[1]->data()->getStrVal(row, isNull), start);

}


} // namespace funcexp
// vim:ts=4 sw=4:
