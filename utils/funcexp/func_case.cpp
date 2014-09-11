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
* $Id: func_case.cpp 2675 2011-06-04 04:58:07Z xlou $
*
*
****************************************************************************/

#include <string>
//#define NDEBUG
#include <cassert>
using namespace std;

#include "functor_all.h"
#include "functioncolumn.h"
#include "predicateoperator.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;


namespace
{
using namespace funcexp;

inline uint64_t simple_case_cmp(Row& row,
								FunctionParm& parm,
								bool& isNull,
								CalpontSystemCatalog::ColType& operationColType)
{
	uint64_t i = 0;                // index to the parm list
	uint64_t n = parm.size() - 1;  // remove expression from count of expression_i + result_i
	uint64_t hasElse = n % 2;      // if 1, then ELSE exist
	n -= hasElse;                  // index to expression

	switch (operationColType.colDataType)
	{
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::DATE:
		case execplan::CalpontSystemCatalog::DATETIME:
		{
   			int64_t ev = parm[n]->data()->getIntVal(row, isNull);
			if (isNull)
				break;

			for (; i < n; i += 2)
			{
				if (ev == parm[i]->data()->getIntVal(row, isNull) && !isNull)
					break;
				else
					isNull = false;
			}
			break;
		}

		case execplan::CalpontSystemCatalog::CHAR:
		case execplan::CalpontSystemCatalog::VARCHAR:
		{
   			string ev = parm[n]->data()->getStrVal(row, isNull);
			if (isNull)
				break;

			for (; i < n; i += 2)
			{
				if (strcoll(ev.c_str(), parm[i]->data()->getStrVal(row, isNull).c_str()) == 0 && !isNull)
					break;
				else
					isNull = false;
			}
			break;
		}

		case execplan::CalpontSystemCatalog::DECIMAL:
		{
   			IDB_Decimal ev = parm[n]->data()->getDecimalVal(row, isNull);
			if (isNull)
				break;

			for (; i < n; i += 2)
			{
				if (ev == parm[i]->data()->getDecimalVal(row, isNull)	&& !isNull)
					break;
				else
					isNull = false;
			}
			break;
		}

		case execplan::CalpontSystemCatalog::DOUBLE:
		{
   			double ev = parm[n]->data()->getDoubleVal(row, isNull);
			if (isNull)
				break;

			for (; i < n; i += 2)
			{
				if (ev == parm[i]->data()->getDoubleVal(row, isNull) && !isNull)
					break;
				else
					isNull = false;
			}
			break;
		}

		case execplan::CalpontSystemCatalog::FLOAT:
		{
   			float ev = parm[n]->data()->getFloatVal(row, isNull);
			if (isNull)
				break;

			for (; i < n; i += 2)
			{
				if (ev == parm[i]->data()->getFloatVal(row, isNull) && !isNull)
					break;
				else
					isNull = false;
			}
			break;
		}

		default:
		{
			std::ostringstream oss;
			oss << "case: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
			throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
		}
	}

	if (i == n && !hasElse)
		isNull = true;

	return i;
}


inline uint64_t searched_case_cmp(Row& row,
								FunctionParm& parm,
								bool& isNull)
{
	uint64_t i = 0;                // index to the parm list
	uint64_t n = parm.size();      // count of boolean_expression_i + result_i
	uint64_t hasElse = n % 2;      // if 1, then ELSE exist
	n -= hasElse;                  // index to expression

	for (; i < n; i += 2)
	{
		if (parm[i]->getBoolVal(row, isNull))
			break;
	}

	isNull = false;
	if (i == n && !hasElse)
		isNull = true;

	return (i == n ? i-1 : i);
}


CalpontSystemCatalog::ColType caseOperationType(FunctionParm& fp,
												CalpontSystemCatalog::ColType& resultType,
												bool simpleCase)
{
	// ... expression_i + result_i + ... [[expression] + result_N]
	FunctionParm::size_type n = fp.size();

	if (simpleCase)                   // simple case has an expression
		n -= 1;                       // remove expression from count of expression_i + result_i
	bool hasElse = ((n % 2) != 0);    // if 1, then ELSE exist
	if (hasElse)
		--n;                          // n now is an even number
	assert((n % 2) == 0);

	bool allStringO = true;
	bool allStringR = true;

	FunctionParm::size_type l = fp.size() - 1;  // last fp index
	assert(fp[l]->data());
	CalpontSystemCatalog::ColType oct = fp[l]->data()->resultType();
	CalpontSystemCatalog::ColType rct = resultType;
	bool operation = true;
	for (uint64_t i = 0; i <= n; i++)
	{
		// operation or result type
		operation = ((i % 2) == 0);

		// the result type of ELSE, if exists.
		if (i == n)
		{
			if (!hasElse)
				break;

			if (simpleCase)
			{
				// the case expression
				if (fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::CHAR &&
					fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::VARCHAR)
				{
					PredicateOperator op;
					op.setOpType(oct, fp[i]->data()->resultType());
					allStringO = false;
					oct = op.operationType();
				}

				i += 1;
			}

			operation = false;
		}

		if (fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::CHAR &&
			fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::VARCHAR)
		{
			// this is not a string column
			PredicateOperator op;
			if (operation)
			{
				op.setOpType(oct, fp[i]->data()->resultType());
				allStringO = false;
				oct = op.operationType();
			}

			// If any parm is of string type, the result type should be string. (same as if)
			else if (rct.colDataType != CalpontSystemCatalog::CHAR &&
						rct.colDataType != CalpontSystemCatalog::VARCHAR)
			{
				op.setOpType(rct, fp[i]->data()->resultType());
				allStringR = false;
				rct = op.operationType();
			}
		}
		else
		{
			// this is a string
			// If any parm is of string type, the result type should be string. (same as if)
			allStringR = true;
		}
	}

	if (allStringO)
	{
		oct.colDataType = CalpontSystemCatalog::VARCHAR;
		oct.colWidth = 255;
	}

	if (allStringR)
	{
		rct.colDataType = CalpontSystemCatalog::VARCHAR;
		rct.colWidth = 255;
	}

	if (rct.scale != 0 && rct.colDataType == CalpontSystemCatalog::BIGINT)
		rct.colDataType = CalpontSystemCatalog::DECIMAL;
	if (oct.scale != 0 && oct.colDataType == CalpontSystemCatalog::BIGINT)
		oct.colDataType = CalpontSystemCatalog::DECIMAL;

	resultType = rct;
	return oct;
}

}

namespace funcexp
{

// simple CASE:
// SELECT CASE ("expression")
// WHEN "condition1" THEN "result1"
// WHEN "condition2" THEN "result2"
// ...
// [ELSE "resultN"]
// END
//
// simple CASE parm order:
//   expression1 result1 expression2 result2 ... expression [resultN]
//

CalpontSystemCatalog::ColType Func_simple_case::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	return caseOperationType(fp, resultType, true);
}


int64_t Func_simple_case::getIntVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	uint64_t i = simple_case_cmp(row, parm, isNull, operationColType);

	if (isNull)
		return joblist::BIGINTNULL;

	return parm[i+1]->data()->getIntVal(row, isNull);
}


string Func_simple_case::getStrVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	uint64_t i = simple_case_cmp(row, parm, isNull, operationColType);

	if (isNull)
		return string("");

	return parm[i+1]->data()->getStrVal(row, isNull);
}


IDB_Decimal Func_simple_case::getDecimalVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	uint64_t i = simple_case_cmp(row, parm, isNull, operationColType);

	if (isNull)
		return IDB_Decimal();  // need a null value for IDB_Decimal??

	return parm[i+1]->data()->getDecimalVal(row, isNull);
}


double Func_simple_case::getDoubleVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	uint64_t i = simple_case_cmp(row, parm, isNull, operationColType);

	if (isNull)
		return doubleNullVal();

	return parm[i+1]->data()->getDoubleVal(row, isNull);
}


int32_t Func_simple_case::getDateIntVal(rowgroup::Row& row,
								FunctionParm& parm,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct)
{
	uint64_t i = simple_case_cmp(row, parm, isNull, op_ct);

	if (isNull)
		return joblist::DATENULL;

	return parm[i+1]->data()->getDateIntVal(row, isNull);
}									
	

int64_t Func_simple_case::getDatetimeIntVal(rowgroup::Row& row,
								FunctionParm& parm,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct)
{
	uint64_t i = simple_case_cmp(row, parm, isNull, op_ct);

	if (isNull)
		return joblist::DATETIMENULL;

	return parm[i+1]->data()->getDatetimeIntVal(row, isNull);
}									



// searched CASE:
// SELECT CASE
// WHEN "boolean_expression1" THEN "result1"
// WHEN "boolean_expression2" THEN "result2"
// ...
// [ELSE "resultN"]
// END
//
// searched CASE parm order:
//   boolean_expression1 result1 boolean_expression2 result2 ... [resultN]
//
CalpontSystemCatalog::ColType Func_searched_case::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type not used by this functor.
	// return fp[1]->data()->resultType(); 	
	return caseOperationType(fp, resultType, false);
}


int64_t Func_searched_case::getIntVal(Row& row,
										FunctionParm& parm,
										bool& isNull,
										CalpontSystemCatalog::ColType&)
{
	uint64_t i = searched_case_cmp(row, parm, isNull);

	if (isNull)
		return joblist::BIGINTNULL;

	return parm[i+1]->data()->getIntVal(row, isNull);
}


string Func_searched_case::getStrVal(Row& row,
										FunctionParm& parm,
										bool& isNull,
										CalpontSystemCatalog::ColType&)
{
	uint64_t i = searched_case_cmp(row, parm, isNull);

	if (isNull)
		return string("");

	return parm[i+1]->data()->getStrVal(row, isNull);
}


IDB_Decimal Func_searched_case::getDecimalVal(Row& row,
										FunctionParm& parm,
										bool& isNull,
										CalpontSystemCatalog::ColType&)
{
	uint64_t i = searched_case_cmp(row, parm, isNull);

	if (isNull)
		return IDB_Decimal();  // need a null value for IDB_Decimal??

	return parm[i+1]->data()->getDecimalVal(row, isNull);
}


double Func_searched_case::getDoubleVal(Row& row,
										FunctionParm& parm,
										bool& isNull,
										CalpontSystemCatalog::ColType&)
{
	uint64_t i = searched_case_cmp(row, parm, isNull);

	if (isNull)
		return doubleNullVal();

	return parm[i+1]->data()->getDoubleVal(row, isNull);
}


int32_t Func_searched_case::getDateIntVal(rowgroup::Row& row,
								FunctionParm& parm,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct)
{
	uint64_t i = simple_case_cmp(row, parm, isNull, op_ct);

	if (isNull)
		return joblist::DATENULL;

	return parm[i+1]->data()->getDateIntVal(row, isNull);
}									

	
int64_t Func_searched_case::getDatetimeIntVal(rowgroup::Row& row,
								FunctionParm& parm,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct)
{
	uint64_t i = simple_case_cmp(row, parm, isNull, op_ct);

	if (isNull)
		return joblist::DATETIMENULL;

	return parm[i+1]->data()->getDatetimeIntVal(row, isNull);
}				


} // namespace funcexp
// vim:ts=4 sw=4:
