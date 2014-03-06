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
using namespace std;

#include "functor_str.h"
#include "funchelpers.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "logicalpartition.h"
using namespace BRM;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_idbpartition::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// all integer
	CalpontSystemCatalog::ColType ct;
	ct.colDataType = CalpontSystemCatalog::BIGINT;
	ct.colWidth = 8;
	return ct;
}

string Func_idbpartition::getStrVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	LogicalPartition part(
		parm[0]->data()->getIntVal(row, isNull),
		parm[1]->data()->getIntVal(row, isNull),
		parm[2]->data()->getIntVal(row, isNull));

	return part.toString();
}


} // namespace funcexp
// vim:ts=4 sw=4:
