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
* $Id: func_repeat.cpp 2477 2011-04-01 16:07:35Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_str.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

class to_lower
{
    public:
        char operator() (char c) const            // notice the return type
        {
            return tolower(c);
        }
};


namespace funcexp
{

CalpontSystemCatalog::ColType Func_repeat::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	//return fp[0]->data()->resultType();
	return resultType;
}

std::string Func_repeat::getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	string str = stringValue(fp[0], row, isNull);

	if (str.empty() || str == "")
		return "";

	int count = fp[1]->data()->getIntVal(row, isNull);
	if (isNull)
		return "";

	if ( count < 1  )
		return "";

	//calculate size of buffer to allocate

	int size = str.length() * count;

	//allocate memory
	char *result = NULL;
	result = (char*) alloca(size * sizeof(char) + 1);
 	if (result == NULL) {
		return "";
 	}

	memset( (char*) result, 0, size);

	for ( int i = 0 ; i < count ; i ++ )
	{
 		if(strcat(result, str.c_str()) == NULL)
		return "";
	}

	return result;
}


} // namespace funcexp
// vim:ts=4 sw=4:

