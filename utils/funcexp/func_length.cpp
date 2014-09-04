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
* $Id: func_length.cpp 3048 2012-04-04 15:33:45Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"

#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_length::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	CalpontSystemCatalog::ColType ct;
	ct.colDataType = CalpontSystemCatalog::VARCHAR;
	ct.colWidth = 255;
	return ct;
}

int64_t Func_length::getIntVal(rowgroup::Row& row,
							FunctionParm& fp,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	if (fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::VARBINARY)
		return fp[0]->data()->getStrVal(row, isNull).length();

	return strlen(fp[0]->data()->getStrVal(row, isNull).c_str());
}


} // namespace funcexp
// vim:ts=4 sw=4:
