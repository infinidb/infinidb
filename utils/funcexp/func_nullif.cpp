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

/****************************************************************************
* $Id: func_nullif.cpp 2485 2011-04-05 19:01:13Z zzhu $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_all.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;
 
#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;
using namespace dataconvert;

#include "funchelpers.h"
#include "utils_utf8.h"
using namespace funcexp;

namespace funcexp
{

CalpontSystemCatalog::ColType Func_nullif::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_nullif::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	int64_t exp1 = parm[0]->data()->getIntVal(row, isNull);
	int64_t exp2 = 0;

	switch (parm[1]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			exp2 = parm[1]->data()->getIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            exp2 = parm[1]->data()->getUintVal(row, isNull);
            break;
        }
		case execplan::CalpontSystemCatalog::DATE:
		{
			exp2 = parm[1]->data()->getDateIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			exp2 = parm[1]->data()->getDatetimeIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		default:
		{
			isNull = true;
		}
	}

	if ( exp1 == exp2 ) {
		isNull = true;
		return 0;
	}

	return exp1;
}

uint64_t Func_nullif::getUintVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	uint64_t exp1 = parm[0]->data()->getUintVal(row, isNull);
	uint64_t exp2 = 0;

	switch (parm[1]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
		case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			int64_t iexp2 = parm[1]->data()->getIntVal(row, isNull);
			if (isNull || iexp2 < 0) {
				isNull = false;
				return exp1;
			}
            exp2 = iexp2;
			break;
		}
        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            exp2 = parm[1]->data()->getUintVal(row, isNull);
            break;
        }

        case execplan::CalpontSystemCatalog::DATE:
		{
			exp2 = parm[1]->data()->getDateIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			exp2 = parm[1]->data()->getDatetimeIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		default:
		{
			isNull = true;
		}
	}

	if ( exp1 == exp2 ) {
		isNull = true;
		return 0;
	}

	return exp1;
}

string Func_nullif::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
    string exp1 = parm[0]->data()->getStrVal(row, isNull);
    if (isNull) {
		isNull = false;		
        return "";
	}

    string exp2 = parm[1]->data()->getStrVal(row, isNull);
	if (isNull) {
		isNull = false;
		return exp1;
	}

	int datatype0 = parm[0]->data()->resultType().colDataType;
	int datatype1 = parm[1]->data()->resultType().colDataType;

	if ( datatype0 == execplan::CalpontSystemCatalog::DATE &&
			datatype1 == execplan::CalpontSystemCatalog::DATETIME )
	{
		exp1 = exp1 + " 00:00:00";
	}

	if ( datatype1 == execplan::CalpontSystemCatalog::DATE &&
			datatype0 == execplan::CalpontSystemCatalog::DATETIME )
	{
		exp2 = exp2 + " 00:00:00";
	}

	if ( utf8::idb_strcoll(exp1.c_str() , exp2.c_str()) == 0 ) {
		isNull = true;
		return "";
	}

	return parm[0]->data()->getStrVal(row, isNull);
}

int32_t Func_nullif::getDateIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	int64_t exp1 = parm[0]->data()->getDateIntVal(row, isNull);
	int64_t exp2 = 0;

	switch (parm[1]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			exp2 = parm[1]->data()->getIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		case execplan::CalpontSystemCatalog::DATE:
		{
			exp2 = parm[1]->data()->getDateIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			exp2 = parm[1]->data()->getDatetimeIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		default:
		{
			isNull = true;
		}
	}

	if ( exp1 == exp2 ) {
		isNull = true;
		return 0;
	}

	return exp1;
}		

int64_t Func_nullif::getDatetimeIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	int64_t exp1 = parm[0]->data()->getDatetimeIntVal(row, isNull);
	int64_t exp2 = 0;

	switch (parm[1]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			exp2 = parm[1]->data()->getIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		case execplan::CalpontSystemCatalog::DATE:
		{
			exp2 = parm[1]->data()->getDateIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			exp2 = parm[1]->data()->getDatetimeIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		default:
		{
			isNull = true;
		}
	}

	if ( exp1 == exp2 ) {
		isNull = true;
		return 0;
	}

	return exp1;
}

double Func_nullif::getDoubleVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	double exp1 = parm[0]->data()->getDoubleVal(row, isNull);
	double exp2 = 0;

	switch (parm[1]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			exp2 = parm[1]->data()->getDoubleVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		case execplan::CalpontSystemCatalog::DATE:
		{
			exp2 = parm[1]->data()->getDateIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			exp2 = parm[1]->data()->getDatetimeIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
			break;
		}
		default:
		{
			isNull = true;
		}
	}

	if ( exp1 == exp2 ) {
		isNull = true;
		return 0;
	}

	return exp1;
}							


execplan::IDB_Decimal Func_nullif::getDecimalVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	IDB_Decimal exp1 = parm[0]->data()->getDecimalVal(row, isNull);
	IDB_Decimal exp2;

	switch (parm[1]->data()->resultType().colDataType)
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
        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			exp2 = parm[1]->data()->getDecimalVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}
		}
		break;
	
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		{
			int64_t value = parm[1]->data()->getIntVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}

			exp2.value = value;
			exp2.scale = 0;
		}

		case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
		{
			double value = parm[1]->data()->getDoubleVal(row, isNull);
			if (isNull) {
				isNull = false;
				return exp1;
			}

			exp2.value = (int64_t) value;
			exp2.scale = 0;
		}
		break;

		case execplan::CalpontSystemCatalog::DATE:
		{
			int32_t s = 0;
			int64_t x = 0;

			string value = DataConvert::dateToString1(parm[1]->data()->getDateIntVal(row, isNull));
			if (!isNull)
			{
				x = atoll(value.c_str());

				if ( s > 11 ) 
					s = 0;
				if ( s > 0 )
				{
					x *= helpers::powerOf10_c[s];
				}
				else if (s < 0)
				{
					s = -s;
					if ( s >= (int32_t) value.size() )
					{
						x = 0;
					}
					else
					{
						x /= helpers::powerOf10_c[s];
						x *= helpers::powerOf10_c[s];
					}

					s = 0;
				}
			}
			else
			{
				isNull = false;
				return exp1;
			}

			exp2.value = x;
			exp2.scale = s;
		}
		break;

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			int32_t s = 0;
			int64_t x = 0;

			string value = 
				DataConvert::datetimeToString1(parm[1]->data()->getDatetimeIntVal(row, isNull));
			if (!isNull)
			{
				//strip off micro seconds
				value = value.substr(0,14);
				int64_t x = atoll(value.c_str());

				if ( s > 5 ) 
					s = 0;
				if ( s > 0 )
				{
					x *= helpers::powerOf10_c[s];
				}
				else if (s < 0)
				{
					s = -s;
					if ( s >= (int32_t) value.size() )
					{
						x = 0;
					}
					else
					{
						x /= helpers::powerOf10_c[s];
						x *= helpers::powerOf10_c[s];
					}

					s = 0;
				}
			}
			else
			{
				isNull = false;
				return exp1;
			}

			exp2.value = x;
			exp2.scale = s;
		}
		break;

		default:
		{
			std::ostringstream oss;
			oss << "truncate: datatype of " << execplan::colDataTypeToString(op_ct.colDataType);
			throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
		}
	}

	if ( exp1 == exp2 ) {
		isNull = true;
		IDB_Decimal decimal;
		decimal.value = 0;
		decimal.scale = 0;
		return decimal;
	}

	return exp1;
}


} // namespace funcexp
// vim:ts=4 sw=4:
