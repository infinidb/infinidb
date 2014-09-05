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
* $Id: func_isnull.cpp 3648 2013-03-19 21:33:52Z dhall $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_bool.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;
using namespace rowgroup;

#include "dataconvert.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_isnull::operationType (FunctionParm& fp, 
                       CalpontSystemCatalog::ColType& resultType)
{
	// operation type of idb_isnull should be the same as the argument type
	assert (fp.size() == 1);
	return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 *
 * This would be the most commonly called API for idb_isnull function
 */
bool Func_isnull::getBoolVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	switch (op_ct.colDataType)
	{
		// For the purpose of this function, one does not need to get the value of
		// the argument. One only need to know if the argument is NULL. The passed
		// in parameter isNull will be set if the parameter is evaluated NULL. 
		// Please note that before this function returns, isNull should be set to 
		// false, otherwise the result of the function would be considered NULL,
		// which is not possible for idb_isnull().
        case CalpontSystemCatalog::DECIMAL:
		case CalpontSystemCatalog::UDECIMAL:
			parm[0]->data()->getDecimalVal(row, isNull);
			break;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
			parm[0]->data()->getStrVal(row, isNull);
			break;
		default:
			parm[0]->data()->getIntVal(row, isNull);
	}
	bool ret = isNull;
	// It's important to reset isNull indicator.
	isNull = false;
	return (fIsNotNull? !ret : ret);
}


} // namespace funcexp
// vim:ts=4 sw=4:
