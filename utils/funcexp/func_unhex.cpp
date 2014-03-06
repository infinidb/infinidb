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
* $Id: func_unhex.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_str.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace
{
	int64_t hex_to_int(char c)
	{
	  if (c <= '9' && c >= '0')
	    return c-'0';
	  c = toupper(c);
	  if (c <= 'F' && c >= 'A')
	    return c-'A'+10;
	  return -1;
	}
}

namespace funcexp
{

CalpontSystemCatalog::ColType Func_unhex::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}


string Func_unhex::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	const string& from = parm[0]->data()->getStrVal(row, isNull);
	char* to = new char[2 + from.length()/2];
	if (!to)
	{
		isNull = true;
		return "";
	}
	
	uint64_t from_pos = 0, to_pos = 0;
	int64_t hex_char = 0;
	if (strlen(from.c_str()) % 2)
	{
		hex_char = hex_to_int(from[from_pos++]);
		to[to_pos++] = hex_char;
		if (hex_char == -1)
			goto nullHandling;
	}

	for (; from_pos < strlen(from.c_str()); from_pos+=2)
	{
		hex_char = hex_to_int(from[from_pos]) << 4;
		if (hex_char == -1)
			goto nullHandling;
		to[to_pos] = hex_char;
		hex_char = hex_to_int(from[from_pos+1]);
		if (hex_char == -1)
			goto nullHandling;
		to[to_pos++] |= hex_char;
	}
	to[to_pos] = 0;
	return string(to);

nullHandling:
	isNull = true;
	return "";
}


} // namespace funcexp
// vim:ts=4 sw=4:
