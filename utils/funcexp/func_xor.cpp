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
* $Id$
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

CalpontSystemCatalog::ColType Func_xor::operationType (FunctionParm& fp, 
                       CalpontSystemCatalog::ColType& resultType)
{
	assert (fp.size() == 2);
	return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 *
 * This would be the most commonly called API for idb_xor function
 */
bool Func_xor::getBoolVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	// Logical XOR. Returns NULL if either operand is NULL. 
	// For non-NULL operands, evaluates to 1 if an odd number of operands is nonzero, 
	// otherwise 0 is returned. 
	bool lopv = parm[0]->getBoolVal(row, isNull);
	if (isNull)
		return false;
	bool ropv = parm[1]->getBoolVal(row, isNull);
	if (isNull)
		return false;
	if ((lopv && !ropv) || (ropv && !lopv))
		return true;
	else
		return false;
}


} // namespace funcexp
// vim:ts=4 sw=4:
