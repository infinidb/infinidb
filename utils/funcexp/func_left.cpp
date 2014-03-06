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
* $Id: func_left.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
*
*
****************************************************************************/

#include <string>
using namespace std;

#include "functor_str.h"
#include "functioncolumn.h"
#include "utils_utf8.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

namespace funcexp
{

CalpontSystemCatalog::ColType Func_left::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}


std::string Func_left::getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType&)
{
	const string& tstr = fp[0]->data()->getStrVal(row, isNull);
	if (isNull)
		return "";

	size_t strwclen = utf8::idb_mbstowcs(0, tstr.c_str(), 0) + 1;
	wchar_t* wcbuf = (wchar_t*)alloca(strwclen * sizeof(wchar_t));
	strwclen = utf8::idb_mbstowcs(wcbuf, tstr.c_str(), strwclen);
	wstring str(wcbuf, strwclen);

	int64_t pos = fp[1]->data()->getIntVal(row, isNull) - 1;
	if (isNull)
		return "";

	if (pos == -1)  // pos == 0
		return "";

	wstring out = str.substr(0, pos+1);
	size_t strmblen = utf8::idb_wcstombs(0, out.c_str(), 0) + 1;
	char* outbuf = (char*)alloca(strmblen * sizeof(char));
	strmblen = utf8::idb_wcstombs(outbuf, out.c_str(), strmblen);
	return string(outbuf, strmblen);

//	return str.substr(0, pos+1);
}							


} // namespace funcexp
// vim:ts=4 sw=4:

