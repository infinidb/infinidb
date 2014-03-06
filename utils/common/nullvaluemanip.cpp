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


#include "nullvaluemanip.h"
#include <sstream>

using namespace std;
using namespace execplan;

namespace utils {

uint64_t getNullValue(CalpontSystemCatalog::ColDataType t, uint32_t colWidth)
{
    switch (t) {
		case CalpontSystemCatalog::TINYINT:
			return joblist::TINYINTNULL;
		case CalpontSystemCatalog::SMALLINT:
			return joblist::SMALLINTNULL;
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			return joblist::INTNULL;
		case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
			return joblist::FLOATNULL;
		case CalpontSystemCatalog::DATE:
			return joblist::DATENULL;
		case CalpontSystemCatalog::BIGINT:
			return joblist::BIGINTNULL;
		case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
			return joblist::DOUBLENULL;
		case CalpontSystemCatalog::DATETIME:
			return joblist::DATETIMENULL;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		case CalpontSystemCatalog::STRINT: {
			switch (colWidth) {
				case 1: return joblist::CHAR1NULL;
				case 2: return joblist::CHAR2NULL;
				case 3:
				case 4: return joblist::CHAR4NULL;
				case 5:
				case 6:
				case 7:
				case 8: return joblist::CHAR8NULL;
				default:
					throw logic_error("getNullValue() Can't return the NULL string");
			}
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
        {
			switch (colWidth) {
				case 1 : return joblist::TINYINTNULL;
				case 2 : return joblist::SMALLINTNULL;
				case 4 : return joblist::INTNULL;
				default: return joblist::BIGINTNULL;
			}
			break;
		}
        case CalpontSystemCatalog::UTINYINT:
            return joblist::UTINYINTNULL;
        case CalpontSystemCatalog::USMALLINT:
            return joblist::USMALLINTNULL;
        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UINT:
            return joblist::UINTNULL;
        case CalpontSystemCatalog::UBIGINT:
            return joblist::UBIGINTNULL;
		case CalpontSystemCatalog::LONGDOUBLE:
			return -1;  // no NULL value for long double yet, this is a nan.
		case CalpontSystemCatalog::VARBINARY:
		default:
			ostringstream os;
			os << "getNullValue(): got bad column type (" << t <<
				").  Width=" << colWidth << endl;
			throw logic_error(os.str());
	}

}

int64_t getSignedNullValue(CalpontSystemCatalog::ColDataType t, uint32_t colWidth)
{
	switch (t) {
		case CalpontSystemCatalog::TINYINT:
			return (int64_t) ((int8_t) joblist::TINYINTNULL);
		case CalpontSystemCatalog::SMALLINT:
			return (int64_t) ((int16_t) joblist::SMALLINTNULL);
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			return (int64_t) ((int32_t) joblist::INTNULL);
		case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
			return (int64_t) ((int32_t) joblist::FLOATNULL);
		case CalpontSystemCatalog::DATE:
			return (int64_t) ((int32_t) joblist::DATENULL);
		case CalpontSystemCatalog::BIGINT:
			return joblist::BIGINTNULL;
		case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
			return joblist::DOUBLENULL;
		case CalpontSystemCatalog::DATETIME:
			return joblist::DATETIMENULL;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		case CalpontSystemCatalog::STRINT: {
			switch (colWidth) {
				case 1: return (int64_t) ((int8_t) joblist::CHAR1NULL);
				case 2: return (int64_t) ((int16_t) joblist::CHAR2NULL);
				case 3:
				case 4: return (int64_t) ((int32_t) joblist::CHAR4NULL);
				case 5:
				case 6:
				case 7:
				case 8: return joblist::CHAR8NULL;
				default:
					throw logic_error("getSignedNullValue() Can't return the NULL string");
			}
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL: {
			switch (colWidth) {
				case 1 : return (int64_t) ((int8_t)  joblist::TINYINTNULL);
				case 2 : return (int64_t) ((int16_t) joblist::SMALLINTNULL);
				case 4 : return (int64_t) ((int32_t) joblist::INTNULL);
				default: return joblist::BIGINTNULL;
			}
			break;
		}
        case CalpontSystemCatalog::UTINYINT:
            return (int64_t) ((int8_t) joblist::UTINYINTNULL);
        case CalpontSystemCatalog::USMALLINT:
            return (int64_t) ((int16_t) joblist::USMALLINTNULL);
        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UINT:
            return (int64_t) ((int32_t) joblist::UINTNULL);
        case CalpontSystemCatalog::UBIGINT:
            return (int64_t)joblist::UBIGINTNULL;
		case CalpontSystemCatalog::LONGDOUBLE:
			return -1;  // no NULL value for long double yet, this is a nan.
		case CalpontSystemCatalog::VARBINARY:
		default:
			ostringstream os;
			os << "getSignedNullValue(): got bad column type (" << t <<
				").  Width=" << colWidth << endl;
			throw logic_error(os.str());
	}

}


}
