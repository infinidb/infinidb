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
* $Id: func_reverse.cpp 2477 2011-04-01 16:07:35Z rdempsey $
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

void reverse( char *start, char *end )
{
    while( start < end )
    {
        char c = *start;
        *start++ = *end;
        *end-- = c;
    }
}

char *reverse_char( char *start )
{
    char *end = start;
    while( (end[1] & 0xC0) == 0x80 ) end++;
    reverse( start, end );
    return( end+1 );
}

CalpontSystemCatalog::ColType Func_reverse::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}

std::string Func_reverse::getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType&)
{
	string str = stringValue(fp[0], row, isNull);

    char *end = (char*) str.c_str();
    while( *end ) end = reverse_char( end );
    reverse( (char*) str.c_str(), end-1 );

	return str;
}


} // namespace funcexp
// vim:ts=4 sw=4:

