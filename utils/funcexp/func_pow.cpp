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
* $Id: func_pow.cpp 2675 2011-06-04 04:58:07Z xlou $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <cerrno>
using namespace std;

#include "functor_real.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

namespace funcexp
{

CalpontSystemCatalog::ColType Func_pow::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}


double Func_pow::getDoubleVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	// null value is indicated by isNull
	double base = parm[0]->data()->getDoubleVal(row, isNull);
	if (!isNull)
	{
		double exponent = parm[1]->data()->getDoubleVal(row, isNull); 
		if (!isNull)
		{
			errno = 0;
			double x = pow(base, exponent);

			// @bug3490, not set to NULL when x is underflow.
			if ((errno == EDOM) || (errno == ERANGE && exponent > 0)) // display NULL
				isNull = true;

			return x;
		}
	}

	return 0.0;
}


} // namespace funcexp
// vim:ts=4 sw=4:
