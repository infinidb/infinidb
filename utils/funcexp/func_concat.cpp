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
* $Id: func_concat.cpp 3716 2013-04-18 16:35:52Z bpaul $
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

#include "dataconvert.h"
using namespace dataconvert;

#define STRCOLL_ENH__

namespace funcexp
{

CalpontSystemCatalog::ColType Func_concat::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}


// Returns the string that results from concatenating the arguments.
// concat() returns NULL if any argument is NULL.
//
string Func_concat::getStrVal(Row& row,
								FunctionParm& parm,
								bool& isNull,
								CalpontSystemCatalog::ColType&)
{
#ifdef STRCOLL_ENH__
	string tstr = stringValue(parm[0], row, isNull);

	size_t strwclen = utf8::idb_mbstowcs(0, tstr.c_str(), 0) + 1;
	wchar_t* wcbuf = (wchar_t*)alloca(strwclen * sizeof(wchar_t));
	strwclen = utf8::idb_mbstowcs(wcbuf, tstr.c_str(), strwclen);
	wstring str(wcbuf, strwclen);

	for ( unsigned int id = 1 ; id < parm.size() ; id++) {
		string tstr = stringValue(parm[id], row, isNull);

		size_t strwclen1 = utf8::idb_mbstowcs(0, tstr.c_str(), 0) + 1;
		wchar_t* wcbuf1 = (wchar_t*)alloca(strwclen1 * sizeof(wchar_t));
		strwclen1 = utf8::idb_mbstowcs(wcbuf1, tstr.c_str(), strwclen1);
		wstring str1(wcbuf1, strwclen1);

		str += str1;
	}

	wstring out = str;
	size_t strmblen = utf8::idb_wcstombs(0, out.c_str(), 0) + 1;
	char* outbuf = (char*)alloca(strmblen * sizeof(char));
	strmblen = utf8::idb_wcstombs(outbuf, out.c_str(), strmblen);
	return string(outbuf, strmblen);

#else
	string str = string(stringValue(*it++, row, isNull).c_str());

	while (!isNull && it != parm.end())
		str += string(stringValue(*it++, row, isNull).c_str());

	return str;
#endif
}


} // namespace funcexp
// vim:ts=4 sw=4:
