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
* $Id: func_substr.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
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

#define STRCOLL_ENH__

namespace funcexp
{

CalpontSystemCatalog::ColType Func_substr::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}


std::string Func_substr::getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType&)
{
#ifdef STRCOLL_ENH__
	const string& tstr = fp[0]->data()->getStrVal(row, isNull);
	if (isNull)
		return "";

	size_t strwclen = utf8::idb_mbstowcs(0, tstr.c_str(), 0) + 1;
	wchar_t* wcbuf = (wchar_t*)alloca(strwclen * sizeof(wchar_t));
	strwclen = utf8::idb_mbstowcs(wcbuf, tstr.c_str(), strwclen);
	wstring str(wcbuf, strwclen);

	int64_t start = fp[1]->data()->getIntVal(row, isNull) - 1;
	if (isNull)
		return "";

	if (start == -1)  // pos == 0
		return "";

	wstring::size_type n = wstring::npos;
	if (fp.size() == 3)
	{
		int64_t len = fp[2]->data()->getIntVal(row,isNull);
		if (isNull)
			return "";

		if (len < 1)
			return "";

		n = len;
	}

	int64_t strLen = static_cast<int64_t>(str.length());
	if (start < -1)  // negative pos, beginning from end
		start += strLen + 1;

	if (start < 0 || strLen <= start)
	{
		return "";
	}

	wstring out = str.substr(start, n);
	size_t strmblen = utf8::idb_wcstombs(0, out.c_str(), 0) + 1;
	char* outbuf = (char*)alloca(strmblen * sizeof(char));
	strmblen = utf8::idb_wcstombs(outbuf, out.c_str(), strmblen);
	return string(outbuf, strmblen);
#else
	const string& str = fp[0]->data()->getStrVal(row, isNull);
	if (isNull)
		return "";

	int64_t start = fp[1]->data()->getIntVal(row, isNull) - 1;
	if (isNull)
		return "";

	if (start == -1)  // pos == 0
		return "";

	size_t n = string::npos;
	if (fp.size() == 3)
	{
		int64_t len = fp[2]->data()->getIntVal(row,isNull);
		if (isNull)
			return "";

		if (len < 1)
			return "";

		n = len;
	}

	size_t strLen = strlen(str.c_str());
	if (start < -1)  // negative pos, beginning from end
		start += strLen + 1;

	if (start < 0 || (int64_t)strLen <= start)
	{
		return "";
	}

	return str.substr(start, n);
#endif
}							


} // namespace funcexp
// vim:ts=4 sw=4:

