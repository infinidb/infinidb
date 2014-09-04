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
* $Id: func_insert.cpp 2477 2011-04-01 16:07:35Z rdempsey $
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

CalpontSystemCatalog::ColType Func_insert::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}

std::string Func_insert::getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType&)
{	
	string tstr = stringValue(fp[0], row, isNull);
	if (isNull)
		return "";

	size_t strwclen = utf8::idb_mbstowcs(0, tstr.c_str(), 0) + 1;
	wchar_t* wcbuf = (wchar_t*)alloca(strwclen * sizeof(wchar_t));
	strwclen = utf8::idb_mbstowcs(wcbuf, tstr.c_str(), strwclen);
	wstring str(wcbuf, strwclen);

	int64_t strLen = static_cast<int64_t>(str.length());

	string tnewstr = stringValue(fp[3], row, isNull);
	if (isNull)
		return "";

	strwclen = utf8::idb_mbstowcs(0, tnewstr.c_str(), 0) + 1;
	wcbuf = (wchar_t*)alloca(strwclen * sizeof(wchar_t));
	strwclen = utf8::idb_mbstowcs(wcbuf, tnewstr.c_str(), strwclen);
	wstring newstr(wcbuf, strwclen);

	int64_t newstrLen = static_cast<int64_t>(newstr.length());

	int64_t pos = fp[1]->data()->getIntVal(row, isNull);
	if (isNull)
		return "";

	int64_t len = fp[2]->data()->getIntVal(row, isNull);
	if (isNull)
		return "";

	if ((pos < 0) || ((pos-1) > strLen))
		return tstr;

	if ((len < 0) || (len > strLen))
		len = strLen;
	
	/* Re-testing with corrected params */
	if ((pos-1) > strLen)
		return tstr;

	wstring out;
	if (len >= strLen || (strLen - (pos-1+len)) < 0)
		out = str.substr(0, pos-1) + newstr.substr(0, newstrLen);
	else
		out = str.substr(0, pos-1) + newstr.substr(0, newstrLen) + str.substr((pos-1+len), strLen);

	size_t strmblen = utf8::idb_wcstombs(0, out.c_str(), 0) + 1;
	char* outbuf = (char*)alloca(strmblen * sizeof(char));
	strmblen = utf8::idb_wcstombs(outbuf, out.c_str(), strmblen);
	return string(outbuf, strmblen);


}


} // namespace funcexp
// vim:ts=4 sw=4:

