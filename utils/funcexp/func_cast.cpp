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

//  $Id: func_cast.cpp 2847 2011-09-02 22:31:37Z zzhu $


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

namespace funcexp
{

CalpontSystemCatalog::ColType Func_cast_signed::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
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

		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::DOUBLE:
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
			string value = parm[0]->data()->getStrVal(row, isNull);
			if (isNull)
			{
				isNull = true;
				return 0;
			}
			return atoll(value.c_str());
		}
		break;

		case execplan::CalpontSystemCatalog::DECIMAL:
		{
			IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
			int64_t value = d.value / power(d.scale);
			int lefto = (d.value - value * power(d.scale)) / power(d.scale-1);
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

			int32_t year = 0, 
					month = 0, 
					day = 0;
			
			year = (uint32_t)((time >> 16) & 0xffff);
			month = (uint32_t)((time >> 12) & 0xf);
			day = (uint32_t)((time >> 6) & 0x3f);

			return (int64_t) (year*10000)+(month*100)+day;
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			 int64_t time = parm[0]->data()->getDatetimeIntVal(row, isNull);

			int32_t year = 0, 
					month = 0, 
					day = 0, 
					hour = 0, 
					min = 0, 
					sec = 0;
		
					year = (uint32_t)((time >> 48) & 0xffff);
					month = (uint32_t)((time >> 44) & 0xf);
					day = (uint32_t)((time >> 38) & 0x3f);
					hour = (uint32_t)((time >> 32) & 0x3f);
					min = (uint32_t)((time >> 26) & 0x3f);
					sec = (uint32_t)((time >> 20) & 0x3f);

//			return (int64_t) (year*1000000000000)+(month*100000000)+(day*1000000)+(hour*10000)+(min*100)+sec;
			return (int64_t) (month*100000000)+(day*1000000)+(hour*10000)+(min*100)+sec;		
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
			ostringstream oss;
			oss << parm[0]->data()->getIntVal(row, isNull);
			string value = oss.str();
			return value.substr(0,length);
		}
		break;

		case execplan::CalpontSystemCatalog::DOUBLE:
		{
			ostringstream oss;
			oss << parm[0]->data()->getDoubleVal(row, isNull);
			string value = oss.str();
			return value.substr(0,length);
		}
		break;

		case execplan::CalpontSystemCatalog::FLOAT:
		{
			ostringstream oss;
			oss << parm[0]->data()->getFloatVal(row, isNull);
			string value = oss.str();
			return value.substr(0,length);
		}
		break;

		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			string value = parm[0]->data()->getStrVal(row, isNull);
			if (isNull)
			{
				isNull = true;
				return value;
			}
			return value.substr(0,length);
		}
		break;

		case execplan::CalpontSystemCatalog::DECIMAL:
		{
			IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

			char buf[80];

			dataconvert::DataConvert::decimalToString( d.value, d.scale, buf, 80);

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
		{
			val = dataconvert::DataConvert::intToDate(parm[0]->data()->getIntVal(row, isNull));
			if (val == -1)
				isNull = true;
			else
				return val;
			break;
		}
		case execplan::CalpontSystemCatalog::DECIMAL:
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
		{
			val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
			if (val == -1)
				isNull = true;
			else
				return val;
			break;
		}
		case execplan::CalpontSystemCatalog::DECIMAL:
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
			int64_t val1  = parm[0]->data()->getDatetimeIntVal(row, isNull);
			string value = dataconvert::DataConvert::datetimeToString(val1);
			value = value.substr(0,10);
			return dataconvert::DataConvert::stringToDatetime(value);
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
		{
			val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));
			if (val == -1)
				isNull = true;
			else
				return val;
			break;
		}
		case execplan::CalpontSystemCatalog::DECIMAL:
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

	return (int64_t) decimal.value/powerOf10_c[decimal.scale];
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

	dataconvert::DataConvert::decimalToString( decimal.value, decimal.scale, buf, 80);

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

	int64_t max_number_decimal = maxNumber_c[max_length];

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
			int64_t value = decimal.value * powerOf10_c[decimals];

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

		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
		{
			double value = parm[0]->data()->getDoubleVal(row, isNull);
			if (value > 0)
				decimal.value = (int64_t) (value * powerOf10_c[decimals] + 0.5);
			else if (value < 0)
				decimal.value = (int64_t) (value * powerOf10_c[decimals] - 0.5);
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
		{
			decimal = parm[0]->data()->getDecimalVal(row, isNull);
			
			
			if (decimals > decimal.scale)
				decimal.value *= powerOf10_c[decimals-decimal.scale];
			else 
				decimal.value = (int64_t)(decimal.value > 0 ? 
				            (double)decimal.value/powerOf10_c[decimal.scale-decimals] + 0.5 :
			              (double)decimal.value/powerOf10_c[decimal.scale-decimals] - 0.5);
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
			string strValue = parm[0]->data()->getStrVal(row, isNull);
			if (strValue.empty()) {
				isNull = true;
				return IDB_Decimal();  // need a null value for IDB_Decimal??
			}

			double value = strtod(strValue.c_str(), 0);
			if (value > 0)
				decimal.value = (int64_t) (value * powerOf10_c[decimals] + 0.5);
			else if (value < 0)
				decimal.value = (int64_t) (value * powerOf10_c[decimals] - 0.5);
			else
				decimal.value = 0;
			decimal.scale = decimals;
			
			if ( decimal.value > max_number_decimal )
				decimal.value = max_number_decimal;
			else if ( decimal.value < -max_number_decimal )
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

	return (double) decimal.value/powerOf10_c[decimal.scale];
}


} // namespace funcexp
// vim:ts=4 sw=4:
