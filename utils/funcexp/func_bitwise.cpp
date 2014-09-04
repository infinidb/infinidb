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
* $Id: func_bitwise.cpp 3279 2012-09-13 15:28:57Z bwilkinson $
*
*
****************************************************************************/

#include <string>
using namespace std;

#include "functor_int.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "predicateoperator.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "dataconvert.h"
using namespace dataconvert;

namespace funcexp
{

// @bug 4703 - the actual bug was only in the DATETIME case
// part of this statement below, but instead of leaving 5 identical
// copies of this code, extracted into a single utility function
// here.  This same method is potentially useful in other methods
// and could be extracted into a utility class with its own header
// if that is the case - this is left as future exercise
bool getUIntValFromParm(
		Row&  row,
		const execplan::SPTP& parm,
		uint64_t& value,
		bool& isNull)
{
	switch (parm->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
		{
			value = parm->data()->getIntVal(row, isNull);
		}
		break;

		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			value = parm->data()->getIntVal(row, isNull);
			if (isNull)
			{
				isNull = true;
			}
			else
			{
				value = 0;
			}
		}
		break;

		case execplan::CalpontSystemCatalog::DECIMAL:
		{
			IDB_Decimal d = parm->data()->getDecimalVal(row, isNull);
			int64_t tmpval = d.value / power(d.scale);
			int lefto = (d.value - tmpval * power(d.scale)) / power(d.scale-1);
			if ( tmpval >= 0 && lefto > 4 )
				tmpval++;
			if ( tmpval < 0 && lefto < -4 )
				tmpval--;
			value = tmpval;
		}
		break;

		case execplan::CalpontSystemCatalog::DATE:
		{
			int32_t time = parm->data()->getDateIntVal(row, isNull);

			Date d(time);
			value = d.convertToMySQLint();
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			int64_t time = parm->data()->getDatetimeIntVal(row, isNull);

			// @bug 4703 - missing year when convering to int
			DateTime dt(time);
			value = dt.convertToMySQLint();
		}
		break;

		default:
		{
			return false;
		}
	}
	return true;
}


//
// BITAND
//


CalpontSystemCatalog::ColType Func_bitand::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_bitand::getIntVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	if ( parm.size() < 2 ) {
		isNull = true;
		return 0;
	}

	uint64_t val1 = 0;
	uint64_t val2 = 0;
	if (!getUIntValFromParm(row, parm[0], val1, isNull) ||
		!getUIntValFromParm(row, parm[1], val2, isNull))
	{
		std::ostringstream oss;
		oss << "bitand: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
		throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
	}

	return val1 & val2;
}


//
// LEFT SHIFT
//


CalpontSystemCatalog::ColType Func_leftshift::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_leftshift::getIntVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	if ( parm.size() < 2 ) {
		isNull = true;
		return 0;
	}

	uint64_t val1 = 0;
	uint64_t val2 = 0;
	if (!getUIntValFromParm(row, parm[0], val1, isNull) ||
		!getUIntValFromParm(row, parm[1], val2, isNull))
	{
		std::ostringstream oss;
		oss << "leftshift: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
		throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
	}

	return val1 << val2;
}


//
// RIGHT SHIFT
//


CalpontSystemCatalog::ColType Func_rightshift::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_rightshift::getIntVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	if ( parm.size() < 2 ) {
		isNull = true;
		return 0;
	}

	uint64_t val1 = 0;
	uint64_t val2 = 0;
	if (!getUIntValFromParm(row, parm[0], val1, isNull) ||
		!getUIntValFromParm(row, parm[1], val2, isNull))
	{
		std::ostringstream oss;
		oss << "rightshift: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
		throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
	}

	return val1 >> val2;
}


//
// BIT OR
//


CalpontSystemCatalog::ColType Func_bitor::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_bitor::getIntVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	if ( parm.size() < 2 ) {
		isNull = true;
		return 0;
	}

	uint64_t val1 = 0;
	uint64_t val2 = 0;
	if (!getUIntValFromParm(row, parm[0], val1, isNull) ||
		!getUIntValFromParm(row, parm[1], val2, isNull))
	{
		std::ostringstream oss;
		oss << "bitor: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
		throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
	}

	return val1 | val2;
}


//
// BIT XOR
//


CalpontSystemCatalog::ColType Func_bitxor::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_bitxor::getIntVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	if ( parm.size() < 2 ) {
		isNull = true;
		return 0;
	}

	uint64_t val1 = 0;
	uint64_t val2 = 0;
	if (!getUIntValFromParm(row, parm[0], val1, isNull) ||
		!getUIntValFromParm(row, parm[1], val2, isNull))
	{
		std::ostringstream oss;
		oss << "bitxor: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
		throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
	}

	return val1 ^ val2;
}


} // namespace funcexp
// vim:ts=4 sw=4:
