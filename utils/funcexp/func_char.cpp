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
* $Id: func_char.cpp 3650 2013-03-19 22:52:19Z bpaul $
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

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

namespace
{
inline bool getChar( int64_t value, ostringstream& oss )
{
	ostringstream oss1;

	if ( value < 0 )
		return false;

	if ( value < 256 ) {
		oss << char(value);
		return true;
	}

	int64_t num = 256;
	while ( num > 255 ) {
		num = value / 256;
		int newValue = value - (num*256);
		oss1 << char(newValue);
		value = num;
	}
	oss1 << char(num);

	//reverse order
	string s = oss1.str();

	for ( int i = s.size() ; i > 0 ; i--)
	{
		oss << s[i-1];
	} 
	oss << s[s.size()];

	return true;
}

inline bool getChar( uint64_t value, ostringstream& oss )
{
	ostringstream oss1;

	if ( value < 256 ) {
		oss << char(value);
		return true;
	}

	uint64_t num = 256;
	while ( num > 255 ) {
		num = value / 256;
		int newValue = value - (num*256);
		oss1 << char(newValue);
		value = num;
	}
	oss1 << char(num);

	//reverse order
	string s = oss1.str();

	for ( int i = s.size() ; i > 0 ; i--)
	{
		oss << s[i-1];
	} 
	oss << s[s.size()];

	return true;
}
}

namespace funcexp
{
CalpontSystemCatalog::ColType Func_char::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}

string Func_char::getStrVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	ostringstream oss;

	switch (ct.colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		{
			int64_t value = parm[0]->data()->getIntVal(row, isNull);

			if ( !getChar(value, oss) ) {
				isNull = true;
				return "";
			}
		}
		break;
	
        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            uint64_t value = parm[0]->data()->getUintVal(row, isNull);

            if ( !getChar(value, oss) ) {
                isNull = true;
                return "";
            }
        }
        break;

        case execplan::CalpontSystemCatalog::VARCHAR: // including CHAR'
		case execplan::CalpontSystemCatalog::CHAR:
		case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
		{
			double value = parm[0]->data()->getDoubleVal(row, isNull);
			if ( !getChar((int64_t)value, oss) ) {
				isNull = true;
				return "";
			}
		}
		break;
	
        case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::UFLOAT:
		{
			float value = parm[0]->data()->getFloatVal(row, isNull);
			if ( !getChar((int64_t)value, oss) ) {
				isNull = true;
				return "";
			}
		}
		break;

        case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
			// get decimal and round up
			int value = d.value / helpers::power(d.scale);
			int lefto = (d.value - value * helpers::power(d.scale)) / helpers::power(d.scale-1);
			if ( lefto > 4 )
				value++;
			if ( !getChar((int64_t)value, oss) ) {
				isNull = true;
				return "";
			}
		}
		break;

		case execplan::CalpontSystemCatalog::DATE:
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			isNull = true;
			return "";
		}
		break;

		default:
		{
			std::ostringstream oss;
			oss << "char: datatype of " << execplan::colDataTypeToString(ct.colDataType);
			throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
		}
			
	}
	// Bug 5110 : Here the data in col is null. But there might have other
	// non-null columns we processed before and we do not want entire value
	// to become null. Therefore we set isNull flag to false.
	if(isNull)
	{
		isNull = false;
		return "";
	}

	return oss.str();
	
}


} // namespace funcexp
// vim:ts=4 sw=4:
