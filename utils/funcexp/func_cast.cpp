/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

//  $Id: func_cast.cpp 3923 2013-06-19 21:43:06Z bwilkinson $


#include <string>
using namespace std;

#include "functor_dtm.h"
#include "functor_int.h"
#include "functor_real.h"
#include "functor_str.h"
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

namespace
{	
	struct lconv* convData = localeconv();
}

namespace funcexp
{

// Why isn't "return resultType" the base default behavior? 
CalpontSystemCatalog::ColType Func_cast_signed::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

CalpontSystemCatalog::ColType Func_cast_unsigned::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

CalpontSystemCatalog::ColType Func_cast_char::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

CalpontSystemCatalog::ColType Func_cast_date::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

CalpontSystemCatalog::ColType Func_cast_datetime::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

CalpontSystemCatalog::ColType Func_cast_decimal::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

//
//	Func_cast_signed
//
int64_t Func_cast_signed::getIntVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{

	switch (parm[0]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		{
			return (int64_t) parm[0]->data()->getIntVal(row, isNull);
		}
		break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            return (int64_t) parm[0]->data()->getUintVal(row, isNull);
        }
        break;

		case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
		case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
		{
			double value = parm[0]->data()->getDoubleVal(row, isNull);
			if (value > 0)
				value += 0.5;
			else if (value < 0)
				value -= 0.5;

			int64_t ret = (int64_t) value;
			if (value > (double) numeric_limits<int64_t>::max())
				ret = numeric_limits<int64_t>::max();
			else if (value < (double) (numeric_limits<int64_t>::min()+2))
				ret = numeric_limits<int64_t>::min() + 2; // IDB min for bigint

			return ret;
		}
		break;

		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			const string& value = parm[0]->data()->getStrVal(row, isNull);
			if (isNull)
			{
				isNull = true;
				return 0;
			}
			return atoll(value.c_str());
		}
		break;

		case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
			int64_t value = d.value / helpers::power(d.scale);
			int lefto = (d.value - value * helpers::power(d.scale)) / helpers::power(d.scale-1);
			if ( value >= 0 && lefto > 4 )
				value++;
			if ( value < 0 && lefto < -4 )
				value--;
			return value;
		}
		break;

		case execplan::CalpontSystemCatalog::DATE:
		{
			int64_t time = parm[0]->data()->getDateIntVal(row, isNull);

			Date d(time);
			return d.convertToMySQLint();
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			int64_t time = parm[0]->data()->getDatetimeIntVal(row, isNull);

			// @bug 4703 need to include year
			DateTime dt(time);
			return dt.convertToMySQLint();
		}
		break;

		default:
		{
			std::ostringstream oss;
			oss << "cast: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
			throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
		}
	}
	return 0;
}


//
//	Func_cast_unsigned
//
uint64_t Func_cast_unsigned::getUintVal(Row& row,
								  	  FunctionParm& parm,
									  bool& isNull,
									  CalpontSystemCatalog::ColType& operationColType)
{

	switch (parm[0]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		{
			return (int64_t) parm[0]->data()->getUintVal(row, isNull);
		}
		break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            return parm[0]->data()->getUintVal(row, isNull);
        }
        break;

		case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
		case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
		{
			double value = parm[0]->data()->getDoubleVal(row, isNull);
			if (value > 0)
				value += 0.5;
			else if (value < 0)
				value -= 0.5;

			uint64_t ret = (uint64_t) value;
			if (value > (double) numeric_limits<uint64_t>::max()-2)
				ret = numeric_limits<int64_t>::max();
			else if (value < 0)
				ret = 0;

			return ret;
		}
		break;

		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			const string& value = parm[0]->data()->getStrVal(row, isNull);
			if (isNull)
			{
				isNull = true;
				return 0;
			}
            uint64_t ret = strtoul(value.c_str(), 0, 0);
			return ret;
		}
		break;

		case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
            if (d.value < 0)
            {
                return 0;
            }
			uint64_t value = d.value / helpers::power(d.scale);
			int lefto = (d.value - value * helpers::power(d.scale)) / helpers::power(d.scale-1);
			if ( value >= 0 && lefto > 4 )
				value++;
			return value;
		}
		break;

		case execplan::CalpontSystemCatalog::DATE:
		{
			int64_t time = parm[0]->data()->getDateIntVal(row, isNull);

			Date d(time);
			return d.convertToMySQLint();
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			int64_t time = parm[0]->data()->getDatetimeIntVal(row, isNull);

			// @bug 4703 need to include year
			DateTime dt(time);
			return dt.convertToMySQLint();
		}
		break;

		default:
		{
			std::ostringstream oss;
			oss << "cast: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
			throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
		}
	}
	return 0;
}


//
//	Func_cast_char
//
string Func_cast_char::getStrVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{

	// check for convert with 1 arg, return the argument
	if ( parm.size() == 1 )
		return parm[0]->data()->getStrVal(row, isNull);;

	int64_t length = parm[1]->data()->getIntVal(row, isNull);

	// @bug3488, a dummy parm is appended even the optional N is not present.
	if ( length < 0 )
		return parm[0]->data()->getStrVal(row, isNull);;

	switch (parm[0]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		{
			return helpers::intToString(parm[0]->data()->getIntVal(row, isNull)).substr(0,length);
		}
		break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
			return helpers::uintToString(parm[0]->data()->getUintVal(row, isNull)).substr(0,length);
        }
        break;

        case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::UDOUBLE:
		{
			return helpers::doubleToString(parm[0]->data()->getDoubleVal(row, isNull)).substr(0,length);
		}
		break;

		case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
		{
			return doubleToString(parm[0]->data()->getFloatVal(row, isNull)).substr(0,length);
		}
		break;

		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			const string& value = parm[0]->data()->getStrVal(row, isNull);
			if (isNull)
			{
				isNull = true;
				return value;
			}
			return value.substr(0,length);
		}
		break;

		case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

			char buf[80];

			dataconvert::DataConvert::decimalToString( d.value, d.scale, buf, 80, parm[0]->data()->resultType().colDataType);

			string sbuf = buf;
			return sbuf.substr(0,length);
		}
		break;

		case execplan::CalpontSystemCatalog::DATE:
		{
			return dataconvert::DataConvert::dateToString(parm[0]->data()->getDateIntVal(row, isNull)).substr(0,length);
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			return  dataconvert::DataConvert::datetimeToString(parm[0]->data()->getDatetimeIntVal(row, isNull)).substr(0,length);
		}
		break;

		default:
		{
			std::ostringstream oss;
			oss << "cast: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
			throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
		}
	}
}


//
//	Func_cast_date
//

int64_t Func_cast_date::getIntVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	if (operationColType.colDataType == execplan::CalpontSystemCatalog::DATE)
		return Func_cast_date::getDateIntVal(row,
				parm,
				isNull,
				operationColType);
				
	return Func_cast_date::getDatetimeIntVal(row,
				parm,
				isNull,
				operationColType);

}

string Func_cast_date::getStrVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	int64_t value;
	if (operationColType.colDataType == execplan::CalpontSystemCatalog::DATE)
	{
		value = Func_cast_date::getDateIntVal(row,
				parm,
				isNull,
				operationColType);
		char buf[30] = {'\0'};
		dataconvert::DataConvert::dateToString(value, buf, sizeof(buf));
		return string(buf);
	}
				
	value = Func_cast_date::getDatetimeIntVal(row,
				parm,
				isNull,
				operationColType);

	char buf[30] = {'\0'};
	dataconvert::DataConvert::datetimeToString(value, buf, sizeof(buf));
	return string(buf);
}

IDB_Decimal Func_cast_date::getDecimalVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	IDB_Decimal decimal;

	decimal.value = Func_cast_date::getDatetimeIntVal(row,
				parm,
				isNull,
				operationColType);

	return decimal;
}

double Func_cast_date::getDoubleVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
return (double) Func_cast_date::getDatetimeIntVal(row,
				parm,
				isNull,
				operationColType);
}


int32_t Func_cast_date::getDateIntVal(rowgroup::Row& row,
								FunctionParm& parm,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct)
{
	int64_t val;
	switch (parm[0]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
		{
			val = dataconvert::DataConvert::intToDate(parm[0]->data()->getIntVal(row, isNull));
			if (val == -1)
				isNull = true;
			else
				return val;
			break;
		}
		case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			if (parm[0]->data()->resultType().scale != 0)
			{
				isNull = true;
				break;
			}
			else
			{
				val = dataconvert::DataConvert::intToDate(parm[0]->data()->getIntVal(row, isNull));
				if (val == -1)
					isNull = true;
				else
					return val;
				break;
			}
		}
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			val = dataconvert::DataConvert::stringToDate(parm[0]->data()->getStrVal(row, isNull));
			if (val == -1)
				isNull = true;
			else
				return val;
			break;
		}

		case execplan::CalpontSystemCatalog::DATE:
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			return parm[0]->data()->getDateIntVal(row, isNull);
		}
		default:
		{
			isNull = true;
		}
	}

	return 0;
}

int64_t Func_cast_date::getDatetimeIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							execplan::CalpontSystemCatalog::ColType& operationColType)
{
	int64_t val;
	switch (parm[0]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
		{
			val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
			if (val == -1)
				isNull = true;
			else
				return val;
			break;
		}
        case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			if (parm[0]->data()->resultType().scale != 0)
			{
				isNull = true;
				break;
			}
			else
			{
				val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
				if (val == -1)
					isNull = true;
				else
					return val;
				break;
			}
		}
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));
			if (val == -1)
				isNull = true;
			else
				return val;
			break;
		}

		case execplan::CalpontSystemCatalog::DATE:
		{
			return parm[0]->data()->getDatetimeIntVal(row, isNull);
		}
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			// @bug 4703 eliminated unnecessary conversion from datetime to string and back
			//           need to zero out the time portions since we are casting to date
			DateTime val1(parm[0]->data()->getDatetimeIntVal(row, isNull));
			val1.hour = 0;
			val1.minute = 0;
			val1.second = 0;
			val1.msecond = 0;
			return *(reinterpret_cast<uint64_t*>(&val1));
		}
		default:
		{
			isNull = true;
		}
	}

	return 0;
}

//
//	Func_cast_datetime
//

int64_t Func_cast_datetime::getIntVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
return Func_cast_datetime::getDatetimeIntVal(row,
				parm,
				isNull,
				operationColType);

}

string Func_cast_datetime::getStrVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	int64_t value = Func_cast_datetime::getDatetimeIntVal(row,
				parm,
				isNull,
				operationColType);

	char buf[30] = {'\0'};
	dataconvert::DataConvert::datetimeToString(value, buf, sizeof(buf));
	return string(buf);	
}

IDB_Decimal Func_cast_datetime::getDecimalVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	IDB_Decimal decimal;

	decimal.value = Func_cast_datetime::getDatetimeIntVal(row,
				parm,
				isNull,
				operationColType);

	return decimal;
}

double Func_cast_datetime::getDoubleVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	return (double) Func_cast_datetime::getDatetimeIntVal(row,
					parm,
					isNull,
					operationColType);
}

int64_t Func_cast_datetime::getDatetimeIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							execplan::CalpontSystemCatalog::ColType& operationColType)
{
	int64_t val;
	switch (parm[0]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
		{
			val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
			if (val == -1)
				isNull = true;
			else
				return val;
			break;
		}
		case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			if (parm[0]->data()->resultType().scale)
			{
				val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
				if (val == -1)
					isNull = true;
				else
					return val;
			}
			else
			{
				isNull = true;
			}
			break;
		}
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));
			if (val == -1)
				isNull = true;
			else
				return val;
			break;
		}
		case execplan::CalpontSystemCatalog::DATE:
		{
			return parm[0]->data()->getDatetimeIntVal(row, isNull);
		}
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			return parm[0]->data()->getDatetimeIntVal(row, isNull);
		}
		default:
		{
			isNull = true;
		}
	}

	return -1;
}

//
//	Func_cast_decimal
//

int64_t Func_cast_decimal::getIntVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{

	IDB_Decimal decimal = Func_cast_decimal::getDecimalVal(row,
				parm,
				isNull,
				operationColType);

	return (int64_t) decimal.value/helpers::powerOf10_c[decimal.scale];
}


string Func_cast_decimal::getStrVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	IDB_Decimal decimal = Func_cast_decimal::getDecimalVal(row,
				parm,
				isNull,
				operationColType);

	char buf[80];

	dataconvert::DataConvert::decimalToString( decimal.value, decimal.scale, buf, 80, operationColType.colDataType);

	string value = buf;
	return value;

}


IDB_Decimal Func_cast_decimal::getDecimalVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
    IDB_Decimal decimal;

	int32_t	decimals = parm[1]->data()->getIntVal(row, isNull);
	int64_t max_length = parm[2]->data()->getIntVal(row, isNull);

	// As of 2.0, max length infiniDB can support is 18
	// decimal(0,0) is valid, and no limit on integer number
	if (max_length > 18 || max_length <= 0)
		max_length = 18;

	int64_t max_number_decimal = helpers::maxNumber_c[max_length];

	switch (parm[0]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		{
			decimal.value = parm[0]->data()->getIntVal(row, isNull);
			decimal.scale = 0;
			int64_t value = decimal.value * helpers::powerOf10_c[decimals];

			if ( value > max_number_decimal )
			{
				decimal.value = max_number_decimal;
				decimal.scale = decimals;
			}
			else if ( value < -max_number_decimal )
			{
				decimal.value = -max_number_decimal;
				decimal.scale = decimals;
			}
		}
		break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            uint64_t uval = parm[0]->data()->getUintVal(row, isNull);
            if (uval > (uint64_t)numeric_limits<int64_t>::max())
            {
                uval = numeric_limits<int64_t>::max();
            }
            decimal.value = uval;
            decimal.scale = 0;
            int64_t value = decimal.value * helpers::powerOf10_c[decimals];

            if ( value > max_number_decimal )
            {
                decimal.value = max_number_decimal;
                decimal.scale = decimals;
            }
        }
        break;

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::UFLOAT:
		{
			double value = parm[0]->data()->getDoubleVal(row, isNull);
			if (value > 0)
				decimal.value = (int64_t) (value * helpers::powerOf10_c[decimals] + 0.5);
			else if (value < 0)
				decimal.value = (int64_t) (value * helpers::powerOf10_c[decimals] - 0.5);
			else
				decimal.value = 0;
			decimal.scale = decimals;

			if ( value > max_number_decimal )
				decimal.value = max_number_decimal;
			else if ( value < -max_number_decimal )
					decimal.value = -max_number_decimal;
		}
		break;

		case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			decimal = parm[0]->data()->getDecimalVal(row, isNull);
			
			
			if (decimals > decimal.scale)
				decimal.value *= helpers::powerOf10_c[decimals-decimal.scale];
			else 
				decimal.value = (int64_t)(decimal.value > 0 ? 
				            (double)decimal.value/helpers::powerOf10_c[decimal.scale-decimals] + 0.5 :
			              (double)decimal.value/helpers::powerOf10_c[decimal.scale-decimals] - 0.5);
			decimal.scale = decimals;



			//int64_t value = decimal.value;

			if ( decimal.value > max_number_decimal )
				decimal.value = max_number_decimal;
			else if ( decimal.value < -max_number_decimal )
					decimal.value = -max_number_decimal;
		}
		break;

		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
        {
            const string& strValue = parm[0]->data()->getStrVal(row, isNull);
            const char* str = strValue.c_str();
            const char* s;
            const char* firstInt = NULL;
            char*       endptr = NULL;
            char        fracBuf[20];
            int         fracChars;
            int         negate = 1;
            bool        bFoundSign = false;
            bool        bRound = false;
            double      floatValue;
            int64_t     value = 0;
            int64_t     frac = 0;

            if (strValue.empty())
            {
                isNull = true;
                return IDB_Decimal();  // need a null value for IDB_Decimal??
            }

            decimal.scale = decimals;
            decimal.value = 0;

            // Look for scientific notation. The existence of an 'e' indicates it probably is.
            for (s = str; *s; ++s)  // This is faster than two finds.
            {
                if (*s == 'e' || *s == 'E')
                {
                    floatValue = strtod(str, 0);
                    // If the float value is too large, the saturated result may end up with
                    // the wrong sign, so we just check first.
                    if ((int64_t)floatValue > max_number_decimal)
                        decimal.value = max_number_decimal;
                    else if ((int64_t)floatValue < -max_number_decimal)
                        decimal.value = -max_number_decimal;
                    else if (floatValue > 0)
                        decimal.value = (int64_t) (floatValue * helpers::powerOf10_c[decimals] + 0.5);
                    else if (floatValue < 0)
                        decimal.value = (int64_t) (floatValue * helpers::powerOf10_c[decimals] - 0.5);
                    else
                        decimal.value = 0;
                    if (decimal.value > max_number_decimal)
                        decimal.value = max_number_decimal;
                    else if (decimal.value < -max_number_decimal)
                        decimal.value = -max_number_decimal;
                    return decimal;
                }
            }

            // There are cases (such as "-.95" that should return that may not result in the desired rounding.
            // By stripping the sign and adding it back in later, we can get a more accurate answer.
            for (s = str; *s; ++s)
            {
                if (*s == '-')
                {
                    if (bFoundSign) // If we find a duplicate sign char, it's an error.
                    {
                        return decimal;
                    }
                    bFoundSign = true;
                    negate = -1;
                } 
                else if (*s == '+')
                {
                    if (bFoundSign)
                    {
                        return decimal;
                    }
                    bFoundSign = true;
                } 
                else if (*s == *convData->decimal_point || *s == '.')
                {
                    // If we find a decimal point, that means there's no leading integer. (like ".99")
                    // In this case we need to mark where we are.
                    endptr = const_cast<char*>(s);
                    break;
                } 
                else if (isdigit(*s))
                {
                    firstInt = s;
                    break;
                }
            }

            errno = 0;
            if (firstInt)   // Checking to see if we have a decimal point, but no previous digits.
            {
                value = strtoll(firstInt, &endptr, 10);
            }
            if (!errno && endptr)
            {
                // Scale the integer portion according to the DECIMAL description
                value *= helpers::powerOf10_c[decimals];    

                // Get the fractional part.
                if (endptr && (*endptr == *convData->decimal_point || *endptr == '.'))
                {
                    s = endptr + 1;
                    // Get the digits to the right of the decimal
                    // Only retrieve those that matter based on scale.
                    for (fracChars = 0;
                        *s && isdigit(*s) && fracChars < decimals;
                        ++fracChars, ++s)
                    {
                        // Save the frac characters to a side buffer. This way we can limit
                        // ourselves to the scale without modifying the original string.
                        fracBuf[fracChars] = *s;
                    }
                    fracBuf[fracChars] = 0;
                    // Check to see if we need to round
                    if (isdigit(*s) && *s > '4')
                    {
                        bRound = true;
                    }
                }

                frac = strtoll(fracBuf, &endptr, 10);
                value += frac + (bRound ? 1 : 0);
                value *= negate;
            }

            decimal.value = value;

            if (decimal.value > max_number_decimal)
                decimal.value = max_number_decimal;
            else if (decimal.value < -max_number_decimal)
                decimal.value = -max_number_decimal;
        }
		break;

		case execplan::CalpontSystemCatalog::DATE:
		{
			int32_t s = 0;

			string value = dataconvert::DataConvert::dateToString1(parm[0]->data()->getDateIntVal(row, isNull));
			int32_t x = atol(value.c_str());
			if (!isNull)
			{
				decimal.value = x;
				decimal.scale = s;
			}
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			int32_t s = 0;

			string value = dataconvert::DataConvert::datetimeToString1(parm[0]->data()->getDatetimeIntVal(row, isNull));

			//strip off micro seconds
			string date = value.substr(0,14);

			int64_t x = atoll(date.c_str());
			if (!isNull)
			{
				decimal.value = x;
				decimal.scale = s;
			}
		}
		break;

		default:
		{
			std::ostringstream oss;
			oss << "cast: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
			throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
		}
	}

	return decimal;
}

double Func_cast_decimal::getDoubleVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	IDB_Decimal decimal = Func_cast_decimal::getDecimalVal(row,
				parm,
				isNull,
				operationColType);

	return (double) decimal.value/helpers::powerOf10_c[decimal.scale];
}


} // namespace funcexp
// vim:ts=4 sw=4:
