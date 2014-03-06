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
* $Id: func_replace.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
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

namespace funcexp
{

CalpontSystemCatalog::ColType Func_replace::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}


std::string Func_replace::getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType&)
{
	const string& str = fp[0]->data()->getStrVal(row, isNull);

	const string& fromstr = fp[1]->data()->getStrVal(row, isNull);

	const string& tostr = fp[2]->data()->getStrVal(row, isNull);

	string newstr;
	unsigned int i = 0;
	for(;;) 
	{
		size_t pos = str.find(fromstr, i);
		if ( pos != string::npos ) {
			//match
			if ( pos > i )
				newstr = newstr + str.substr(i,pos-i);

			newstr = newstr + tostr;
			i = pos + fromstr.size();
		}
		else {
			newstr = newstr + str.substr(i,1000);
			break;
		}
	}

	return newstr;
}


} // namespace funcexp
// vim:ts=4 sw=4:

