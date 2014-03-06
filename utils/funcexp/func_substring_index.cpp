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
* $Id: func_substring_index.cpp 2477 2011-04-01 16:07:35Z rdempsey $
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

CalpontSystemCatalog::ColType Func_substring_index::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}


std::string Func_substring_index::getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType&)
{
	const string& str = fp[0]->data()->getStrVal(row, isNull);
	if (isNull)
		return "";

	const string& delim = fp[1]->data()->getStrVal(row, isNull);
	if (isNull)
		return "";

	int64_t count = fp[2]->data()->getIntVal(row,isNull);
	if (isNull)
		return "";

	if ( count == 0 )
		return "";

	size_t end = strlen(str.c_str());

	if ( count > (int64_t) end )
		return str;

	string value = str;

	if ( count > 0 ) {
		int pointer = 0;

		for ( int i = 0 ; i < count ; i ++ )
		{
			string::size_type pos = str.find(delim,pointer);
			if (pos != string::npos)
				pointer = pos+1;
				end = pos;
		}
		value = str.substr(0,end);
	}
	else
	{
		count = count *-1;
		size_t end = strlen(str.c_str());
		int pointer = end;
		int start = 0;

		for ( int i = 0 ; i < count ; i ++ )
		{
			string::size_type pos = str.rfind(delim,pointer);
			if (pos != string::npos) {
				if ( count > (int64_t) end )
					return "";
				pointer = pos-1;
				start = pos+1;
			}
			else
				start = 0;
		}
		value = str.substr(start,end);
	}

	return value;
}							


} // namespace funcexp
// vim:ts=4 sw=4:

