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
* $Id: func_to_days.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;


namespace funcexp
{

CalpontSystemCatalog::ColType Func_to_days::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_to_days::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	CalpontSystemCatalog::ColDataType type = parm[0]->data()->resultType().colDataType;

	uint32_t year = 0, 
			month = 0, 
			day = 0;

	switch (type)
	{
		case execplan::CalpontSystemCatalog::DATE:
		{
			int32_t val = parm[0]->data()->getDateIntVal(row, isNull);
			year = (uint32_t)((val >> 16) & 0xffff);
			month = (uint32_t)((val >> 12) & 0xf);
			day = (uint32_t)((val >> 6) & 0x3f);
			return helpers::calc_mysql_daynr(year, month, day);
			break;
		}

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			int64_t val = parm[0]->data()->getDatetimeIntVal(row, isNull);
			year = (uint32_t)((val >> 48) & 0xffff);
			month = (uint32_t)((val >> 44) & 0xf);
			day = (uint32_t)((val >> 38) & 0x3f);

			return helpers::calc_mysql_daynr(year, month, day);
			break;
		}

		case execplan::CalpontSystemCatalog::VARCHAR: // including CHAR'
		case execplan::CalpontSystemCatalog::CHAR:
		{
			const string& value = parm[0]->data()->getStrVal(row, isNull);
			int64_t val = 0;
			if ( value.size() == 10 ) {
				// date type
				val = dataconvert::DataConvert::dateToInt(value);
				year = (uint32_t)((val >> 16) & 0xffff);
				month = (uint32_t)((val >> 12) & 0xf);
				day = (uint32_t)((val >> 6) & 0x3f);
			}
			else
			{	// datetime type
				val = dataconvert::DataConvert::datetimeToInt(value);
				year = (uint32_t)((val >> 48) & 0xffff);
				month = (uint32_t)((val >> 44) & 0xf);
				day = (uint32_t)((val >> 38) & 0x3f);
			}

			return helpers::calc_mysql_daynr(year, month, day);
			break;
		}

		default:
		{
			std::ostringstream oss;
			oss << "to_days: datatype of " << execplan::colDataTypeToString(type);;
			throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
		}
	}

	return 0;
}


} // namespace funcexp
// vim:ts=4 sw=4:
