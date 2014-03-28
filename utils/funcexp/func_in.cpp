/* Copyright (C) 2013 Calpont Corp.

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
* $Id: func_in.cpp 3954 2013-07-08 16:30:15Z bpaul $
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

#include "dataconvert.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "utils_utf8.h"
using namespace funcexp;

namespace
{
	template<typename result_t>
	inline bool numericEQ(result_t op1, result_t op2)
	{
		return op1 == op2;
	}
	
	inline bool strEQ(string op1, string op2)
	{
		return utf8::idb_strcoll(op1.c_str(), op2.c_str()) == 0;
	}
	
	inline bool getBoolForIn(rowgroup::Row& row,
							funcexp::FunctionParm& pm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct,
							bool isNotIn)
	{
		IDB_Decimal d; // to be removed;
		switch (ct.colDataType)
		{
			case execplan::CalpontSystemCatalog::BIGINT:
			case execplan::CalpontSystemCatalog::INT:
			case execplan::CalpontSystemCatalog::MEDINT:
			case execplan::CalpontSystemCatalog::TINYINT:
			case execplan::CalpontSystemCatalog::SMALLINT:	
			{
				int64_t val = pm[0]->data()->getIntVal(row, isNull);
				if (isNull)
					return false;
				for (uint i = 1; i < pm.size(); i++)
				{
					isNull = false;
					if (val == pm[i]->data()->getIntVal(row, isNull) && !isNull )
						return true;
					if (isNull && isNotIn)
						return true; // will be reversed to false by the caller
				}
				return false;	
			}
            case execplan::CalpontSystemCatalog::UBIGINT:
            case execplan::CalpontSystemCatalog::UINT:
            case execplan::CalpontSystemCatalog::UMEDINT:
            case execplan::CalpontSystemCatalog::UTINYINT:
            case execplan::CalpontSystemCatalog::USMALLINT:	
            {
                uint64_t val = pm[0]->data()->getUintVal(row, isNull);
                if (isNull)
                    return false;
                for (uint i = 1; i < pm.size(); i++)
                {
                    isNull = false;
                    if (val == pm[i]->data()->getUintVal(row, isNull) && !isNull )
                        return true;
                    if (isNull && isNotIn)
                        return true; // will be reversed to false by the caller
                }
                return false;	
            }
			case execplan::CalpontSystemCatalog::DATE:
			{
				int32_t val = pm[0]->data()->getDateIntVal(row, isNull);
				if (isNull)
					return false;
				for (uint i = 1; i < pm.size(); i++)
				{
					isNull = false;
					if ( val == pm[i]->data()->getDateIntVal(row, isNull) && !isNull )
						return true;
					if (isNull && isNotIn)
						return true; // will be reversed to false by the caller
				}
				return false;	
			}
			case execplan::CalpontSystemCatalog::DATETIME:
			{
				int64_t val = pm[0]->data()->getDatetimeIntVal(row, isNull);
				if (isNull)
					return false;
				for (uint i = 1; i < pm.size(); i++)
				{
					isNull = false;
					if ( val == pm[i]->data()->getDatetimeIntVal(row, isNull) && !isNull )
						return true;
					if (isNull && isNotIn)
						return true; // will be reversed to false by the caller
				}
				return false;	
			}
			case execplan::CalpontSystemCatalog::DOUBLE:
            case execplan::CalpontSystemCatalog::UDOUBLE:
			case execplan::CalpontSystemCatalog::FLOAT:
            case execplan::CalpontSystemCatalog::UFLOAT:
			{
				double val = pm[0]->data()->getDoubleVal(row, isNull);
				if (isNull)
					return false;
				for (uint i = 1; i < pm.size(); i++)
				{
					isNull = false;
					if ( val == pm[i]->data()->getDoubleVal(row, isNull) && !isNull )
						return true;
					if (isNull && isNotIn)
						return true; // will be reversed to false by the caller
				}
				return false;	
			}
			case execplan::CalpontSystemCatalog::DECIMAL:
            case execplan::CalpontSystemCatalog::UDECIMAL:
			{				
				IDB_Decimal val = pm[0]->data()->getDecimalVal(row, isNull);
				if (isNull)
					return false;
				for (uint i = 1; i < pm.size(); i++)
				{
					isNull = false;
					if ( val == pm[i]->data()->getDecimalVal(row, isNull) && !isNull )
						return true;
					if (isNull && isNotIn)
						return true; // will be reversed to false by the caller
				}
				return false;			
			}
			case execplan::CalpontSystemCatalog::VARCHAR: // including CHAR'
			case execplan::CalpontSystemCatalog::CHAR:
			{
				const string& val = pm[0]->data()->getStrVal(row, isNull);
				if (isNull)
					return false;
				for (uint i = 1; i < pm.size(); i++)
				{
					isNull = false;
					if ( utf8::idb_strcoll(val.c_str(), pm[i]->data()->getStrVal(row, isNull).c_str()) == 0 && !isNull)
						return true;
					if (isNull && isNotIn)
						return true; // will be reversed to false by the caller
				}
				return false;	
			}
			default:
			{
				std::ostringstream oss;
				oss << "regexo: datatype of " << execplan::colDataTypeToString(ct.colDataType);
				throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
			}
		}
	}
	
}

namespace funcexp
{

CalpontSystemCatalog::ColType Func_in::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	PredicateOperator op;
	CalpontSystemCatalog::ColType ct;
	// @bug 4230. Initialize ct to be the first argument.
	if (!fp.empty())
		ct = fp[0]->data()->resultType();
	bool allString = true;
	bool allNonToken = true;
	
	for (uint i = 0; i < fp.size(); i++)
	{
		//op.setOpType(op.operationType(), fp[i]->data()->resultType());
		if (fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::CHAR &&
			fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::VARCHAR)
		{
			allString = false;
			op.setOpType(ct, fp[i]->data()->resultType());
			ct = op.operationType();
		}
		else
		{
			if ((fp[i]->data()->resultType().colDataType == CalpontSystemCatalog::CHAR && 
				  fp[i]->data()->resultType().colWidth > 8) || 
				  (fp[i]->data()->resultType().colDataType == CalpontSystemCatalog::VARCHAR &&
				  fp[i]->data()->resultType().colWidth >= 8))
				  allNonToken = false;
		}
	}
	
	if (allString && !allNonToken)
	{
		ct.colDataType = CalpontSystemCatalog::VARCHAR;
		ct.colWidth = 255;
	}
	
	else if (allString && allNonToken)
	{
		ct.colDataType = CalpontSystemCatalog::BIGINT;
		ct.colWidth = 8;
	}
		

	// convert date const value according to the compare type here.
	if (op.operationType().colDataType == CalpontSystemCatalog::DATE)
	{
		ConstantColumn *cc = NULL;
		for (uint i = 1; i < fp.size(); i++)
		{
			cc = dynamic_cast<ConstantColumn*>(fp[i]->data());
			if (cc)
			{
				Result result = cc->result();
				result.intVal = dataconvert::DataConvert::dateToInt(result.strVal);
				cc->result(result);
			}
		}
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

bool Func_in::getBoolVal(rowgroup::Row& row,
							FunctionParm& pm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return getBoolForIn(row, pm, isNull, ct, false) && !isNull;
}





CalpontSystemCatalog::ColType Func_notin::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	PredicateOperator *op = new PredicateOperator();
	CalpontSystemCatalog::ColType ct;
	op->setOpType(fp[0]->data()->resultType(), fp[1]->data()->resultType());
	return op->operationType();
}

bool Func_notin::getBoolVal(rowgroup::Row& row,
							FunctionParm& pm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return (!getBoolForIn(row, pm, isNull, ct, true) && !isNull);
}


} // namespace funcexp
// vim:ts=4 sw=4:
