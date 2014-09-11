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
* $Id: func_instr.cpp 2675 2011-06-04 04:58:07Z xlou $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_instr::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	CalpontSystemCatalog::ColType ct;
	ct.colDataType = CalpontSystemCatalog::VARCHAR;
	ct.colWidth = 255;
	return ct;
}

int64_t Func_instr::getIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	uint64_t start = 1;
	if (parm.size() == 3)
		start = parm[2]->data()->getIntVal(row, isNull);

	if (isNull || start == 0)
		return 0;

	size_t pos = parm[0]->data()->getStrVal(row, isNull).find(parm[1]->data()->getStrVal(row, isNull),start-1);

	return (pos != string::npos ? pos+1 : 0);
}


} // namespace funcexp
// vim:ts=4 sw=4:
