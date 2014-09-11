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
* $Id: func_rpad.cpp 2675 2011-06-04 04:58:07Z xlou $
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

#define STRCOLL_ENH__

namespace funcexp
{
CalpontSystemCatalog::ColType Func_rpad::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}


std::string Func_rpad::getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType&)
{
#ifdef STRCOLL_ENH__
	string tstr = fp[0]->data()->getStrVal(row, isNull);

	size_t strwclen = mbstowcs(0, tstr.c_str(), 0) + 1;
	wchar_t* wcbuf = (wchar_t*)alloca(strwclen * sizeof(wchar_t));
	strwclen = mbstowcs(wcbuf, tstr.c_str(), strwclen);
	wstring str(wcbuf, strwclen);

	unsigned int len = fp[1]->data()->getIntVal(row, isNull);
	if (len < 0)
		return "";

	string pad = fp[2]->data()->getStrVal(row, isNull);
	if (isNull)
		return "";

	unsigned int strSize = strwclen;
	for ( unsigned int i = 0 ; i < strwclen ; i++ )
	{
		if ( str[i] == '\0' ) {
			strSize = i;
			break;
		}
	}

	if ( strSize == len )
		return string(tstr.c_str(), len);

	int64_t pos = len - 1;
	if (isNull)
		return "";

	if (pos == -1)  // pos == 0
		return "";

	if ( strSize > len ) {
		wstring out = str.substr(0,len);
		size_t strmblen = wcstombs(0, out.c_str(), 0) + 1;
		char* outbuf = (char*)alloca(strmblen * sizeof(char));
		strmblen = wcstombs(outbuf, out.c_str(), strmblen);
	
		return string(outbuf, strmblen);
}

	string fullpad;
	for(unsigned int i = 0 ; i < len-strSize;) {
		for(unsigned int j = 0 ; j < pad.size() ; j++)
		{
			fullpad = fullpad + pad.substr (j,1);
			i++;
			if (i >= len-strSize)
				break;
		}
	}

	size_t strmblen2 = wcstombs(0, str.c_str(), 0) + 1;
	char* outbuf2 = (char*)alloca(strmblen2 * sizeof(char));
	strmblen2 = wcstombs(outbuf2, str.c_str(), strmblen2);

	string newStr = string(outbuf2, strmblen2) + fullpad;
	size_t strwclen1 = mbstowcs(0, newStr.c_str(), 0) + 1;
	wchar_t* wcbuf1 = (wchar_t*)alloca(strwclen1 * sizeof(wchar_t));
	strwclen1 = mbstowcs(wcbuf1, newStr.c_str(), strwclen1);
	wstring nstr(wcbuf1, strwclen1);

	wstring out = nstr.substr(0,len);
	size_t strmblen = wcstombs(0, out.c_str(), 0) + 1;
	char* outbuf = (char*)alloca(strmblen * sizeof(char));
	strmblen = wcstombs(outbuf, out.c_str(), strmblen);

	return string(outbuf, strmblen);
#else
	string str = fp[0]->data()->getStrVal(row, isNull);

	unsigned int len = fp[1]->data()->getIntVal(row, isNull);
	if (len < 0)
		return "";

	string pad = fp[2]->data()->getStrVal(row, isNull);
	if (isNull)
		return "";

	unsigned int strSize = str.size();
	for ( unsigned int i = 0 ; i < str.size() ; i++ )
	{
		if ( str[i] == '\0' ) {
			strSize = i;
			break;
		}
	}

	if ( strSize == len )
		return str;

	if ( strSize > len )
		return str.substr(0,len);

	string fullpad;
	for(unsigned int i = 0 ; i < len-strSize;) {
		for(unsigned int j = 0 ; j < pad.size() ; j++)
		{
			fullpad = fullpad + pad.substr (j,1);
			i++;
			if (i >= len-strSize)
				break;
		}
	}

	string newStr = str.substr(0,strSize) + fullpad;

	return newStr.substr(0,len);
#endif
}


} // namespace funcexp
// vim:ts=4 sw=4:

