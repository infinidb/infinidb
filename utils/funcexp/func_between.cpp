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
* $Id: func_between.cpp 3957 2013-07-08 21:20:36Z bpaul $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_bool.h"
#include "functioncolumn.h"
#include "predicateoperator.h"
#include "constantcolumn.h"
using namespace execplan;

#include "rowgroup.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "utils_utf8.h"
using namespace funcexp;


namespace
{
	template<typename result_t>
	inline bool numericGE(result_t op1, result_t op2)
	{
		return op1 >= op2;
	}

	template<typename result_t>
	inline bool numericLE(result_t op1, result_t op2)
	{
		return op1 <= op2;
	}

	inline bool strGE(string op1, string op2)
	{
		//return strcoll(op1.c_str(), op2.c_str()) >= 0;
		return utf8::idb_strcoll(op1.c_str(), op2.c_str()) >= 0;
	}

	inline bool strLE(string op1, string op2)
	{
		//return strcoll(op1.c_str(), op2.c_str()) <= 0;
		return utf8::idb_strcoll(op1.c_str(), op2.c_str()) <= 0;
	}

	inline bool getBool(rowgroup::Row& row,
							funcexp::FunctionParm& pm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct,
							bool notBetween)
	{
		switch (ct.colDataType)
		{
			case execplan::CalpontSystemCatalog::BIGINT:
			case execplan::CalpontSystemCatalog::INT:
			case execplan::CalpontSystemCatalog::MEDINT:
			case execplan::CalpontSystemCatalog::TINYINT:
			case execplan::CalpontSystemCatalog::SMALLINT:
			{
				int64_t val = pm[0]->data()->getIntVal(row, isNull);
				if (notBetween)
				{
					if (!numericGE(val, pm[1]->data()->getIntVal(row, isNull)) && !isNull)
						return true;
					isNull = false;
					return (!numericLE(val, pm[2]->data()->getIntVal(row, isNull)) && !isNull);
				}
				return !isNull &&
						numericGE(val, pm[1]->data()->getIntVal(row, isNull)) &&
						numericLE(val, pm[2]->data()->getIntVal(row, isNull)) && !isNull;
			}
            case execplan::CalpontSystemCatalog::UBIGINT:
            case execplan::CalpontSystemCatalog::UINT:
            case execplan::CalpontSystemCatalog::UMEDINT:
            case execplan::CalpontSystemCatalog::UTINYINT:
            case execplan::CalpontSystemCatalog::USMALLINT:
            {
                uint64_t val = pm[0]->data()->getUintVal(row, isNull);
                if (notBetween)
                {
                    if (!numericGE(val, pm[1]->data()->getUintVal(row, isNull)) && !isNull)
                        return true;
                    isNull = false;
                    return (!numericLE(val, pm[2]->data()->getUintVal(row, isNull)) && !isNull);
                }
                return !isNull &&
                        numericGE(val, pm[1]->data()->getUintVal(row, isNull)) &&
                        numericLE(val, pm[2]->data()->getUintVal(row, isNull)) && !isNull;
            }
			case execplan::CalpontSystemCatalog::DATE:
			{
				int32_t val = pm[0]->data()->getDateIntVal(row, isNull);
				if (notBetween)
				{
					if (!numericGE(val, pm[1]->data()->getDateIntVal(row, isNull)) && !isNull)
						return true;
					isNull = false;
					return (!numericLE(val, pm[2]->data()->getDateIntVal(row, isNull)) && !isNull);
				}
				return !isNull &&
						numericGE(val, pm[1]->data()->getDateIntVal(row, isNull)) &&
						numericLE(val, pm[2]->data()->getDateIntVal(row, isNull));
			}
			case execplan::CalpontSystemCatalog::DATETIME:
			{
				int64_t val = pm[0]->data()->getDatetimeIntVal(row, isNull);
				if (notBetween)
				{
					if (!numericGE(val, pm[1]->data()->getDatetimeIntVal(row, isNull)) && !isNull)
						return true;
					isNull = false;
					return (!numericLE(val, pm[2]->data()->getDatetimeIntVal(row, isNull)) && !isNull);
				}
				return !isNull &&
						numericGE(val, pm[1]->data()->getDatetimeIntVal(row, isNull)) &&
						numericLE(val, pm[2]->data()->getDatetimeIntVal(row, isNull));
			}
			case execplan::CalpontSystemCatalog::DOUBLE:
            case execplan::CalpontSystemCatalog::UDOUBLE:
			case execplan::CalpontSystemCatalog::FLOAT:
            case execplan::CalpontSystemCatalog::UFLOAT:
			{
				double val = pm[0]->data()->getDoubleVal(row, isNull);
				if (notBetween)
				{
					if (!numericGE(val, pm[1]->data()->getDoubleVal(row, isNull)) && !isNull)
						return true;
					isNull = false;
					return (!numericLE(val, pm[2]->data()->getDoubleVal(row, isNull)) && !isNull);
				}
				return !isNull &&
						numericGE(val, pm[1]->data()->getDoubleVal(row, isNull)) &&
						numericLE(val, pm[2]->data()->getDoubleVal(row, isNull));
			}
            case execplan::CalpontSystemCatalog::DECIMAL:
			case execplan::CalpontSystemCatalog::UDECIMAL:
			{
				IDB_Decimal val = pm[0]->data()->getDecimalVal(row, isNull);
				if (notBetween)
				{
					if (!numericGE(val, pm[1]->data()->getDecimalVal(row, isNull)) && !isNull)
						return true;
					isNull = false;
					return (!numericLE(val, pm[2]->data()->getDecimalVal(row, isNull)) && !isNull);
				}
				return !isNull &&
						numericGE(val, pm[1]->data()->getDecimalVal(row, isNull)) &&
						numericLE(val, pm[2]->data()->getDecimalVal(row, isNull));
			}
			case execplan::CalpontSystemCatalog::VARCHAR: // including CHAR'
			case execplan::CalpontSystemCatalog::CHAR:
			{
				string val = pm[0]->data()->getStrVal(row, isNull);
				if (notBetween)
				{
					if (!strGE(val, pm[1]->data()->getStrVal(row, isNull)) && !isNull)
						return true;
					isNull = false;
					return (!strLE(val, pm[2]->data()->getStrVal(row, isNull)) && !isNull);
				}
				return !isNull &&
						strGE(val, pm[1]->data()->getStrVal(row, isNull)) &&
						strLE(val, pm[2]->data()->getStrVal(row, isNull));
			}
			default:
			{
				std::ostringstream oss;
				oss << "between: datatype of " << execplan::colDataTypeToString(ct.colDataType);
				throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
			}
		}
	}

}

namespace funcexp
{

CalpontSystemCatalog::ColType Func_between::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	PredicateOperator op;
	CalpontSystemCatalog::ColType ct = fp[0]->data()->resultType();
	//op.operationType(fp[0]->data()->resultType());
	bool allString = true;

	for (uint i = 1; i < fp.size(); i++)
	{
		//op.setOpType(op.operationType(), fp[i]->data()->resultType());
		op.setOpType(ct, fp[i]->data()->resultType());
		ct = op.operationType();

		if ((fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::CHAR &&
			fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::VARCHAR) || 
			ct.colDataType == CalpontSystemCatalog::DATE ||
			ct.colDataType == CalpontSystemCatalog::DATETIME)
		{
			allString = false;
		}
	}

	if (allString)
	{
		ct.colDataType = CalpontSystemCatalog::VARCHAR;
		ct.colWidth = 255;
	}

	else if (op.operationType().colDataType == CalpontSystemCatalog::DATETIME)
	{
		ConstantColumn *cc = NULL;
		for (uint i = 1; i < fp.size(); i++)
		{
			cc = dynamic_cast<ConstantColumn*>(fp[i]->data());
			if (cc)
			{
				Result result = cc->result();
				result.intVal = dataconvert::DataConvert::datetimeToInt(result.strVal);
				cc->result(result);
			}
		}
	}
	return ct;
}

bool Func_between::getBoolVal(rowgroup::Row& row,
							FunctionParm& pm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return getBool(row, pm, isNull, ct, false) && !isNull;
}





CalpontSystemCatalog::ColType Func_notbetween::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	PredicateOperator *op = new PredicateOperator();
	CalpontSystemCatalog::ColType ct;
	op->setOpType(fp[0]->data()->resultType(), fp[1]->data()->resultType());
	op->setOpType(op->resultType(), fp[2]->data()->resultType());
	return op->operationType();
}

bool Func_notbetween::getBoolVal(rowgroup::Row& row,
							FunctionParm& pm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return getBool(row, pm, isNull, ct, true) && !isNull;
}


} // namespace funcexp
// vim:ts=4 sw=4:
