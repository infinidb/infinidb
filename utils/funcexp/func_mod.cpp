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
* $Id: func_mod.cpp 3616 2013-03-04 14:56:29Z rdempsey $
*
*
****************************************************************************/

#include <string>
using namespace std;

#include "functor_real.h"
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

CalpontSystemCatalog::ColType Func_mod::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}


IDB_Decimal Func_mod::getDecimalVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{

	IDB_Decimal retValue;
	retValue.value = 0;
	retValue.scale = 0;

	if ( parm.size() < 2 ) {
		isNull = true;
		return retValue;
	}

	int64_t div = parm[1]->data()->getIntVal(row, isNull);

	if ( div == 0 ) {
		isNull = true;
		return retValue;
	}

	IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
	int64_t value = d.value / helpers::power(d.scale);
	int lefto = d.value % helpers::power(d.scale);

	int64_t mod = (value % div) * helpers::power(d.scale) + lefto;

	retValue.value = mod;
	retValue.scale = d.scale;

	return retValue;
}


double Func_mod::getDoubleVal(Row& row,
									FunctionParm& parm,
									bool& isNull,
									CalpontSystemCatalog::ColType& operationColType)
{
	if ( parm.size() < 2 ) {
		isNull = true;
		return 0;
	}

	int64_t div = parm[1]->data()->getIntVal(row, isNull);

	if ( div == 0 ) {
		isNull = true;
		return 0;
	}

	double mod = 0;
	
	switch (parm[0]->data()->resultType().colDataType)
	{
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		{
			int64_t value = parm[0]->data()->getIntVal(row, isNull);

			mod = value % div;
		}
		break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            uint64_t udiv = parm[1]->data()->getIntVal(row, isNull);
            uint64_t uvalue = parm[0]->data()->getUintVal(row, isNull);

            mod = uvalue % udiv;
        }
        break;

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
		{
			double value = parm[0]->data()->getDoubleVal(row, isNull);

			mod = fmod(value,div);
		}
		break;

		case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
		{
			float value = parm[0]->data()->getFloatVal(row, isNull);

			mod = fmod(value,div);
		}
		break;

		case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
			int64_t value = d.value / helpers::power(d.scale);

			mod = value % div;
		}
		break;
		
		case execplan::CalpontSystemCatalog::CHAR:
		case execplan::CalpontSystemCatalog::VARCHAR:
		{
			double value = parm[0]->data()->getDoubleVal(row, isNull);
			mod = fmod(value,div);
			break;
		}

		default:
		{
			std::ostringstream oss;
			oss << "mod: datatype of " << execplan::colDataTypeToString(parm[0]->data()->resultType().colDataType);
			throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
		}
	}

	return mod;
}


} // namespace funcexp
// vim:ts=4 sw=4:
