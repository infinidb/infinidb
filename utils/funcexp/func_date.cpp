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
* $Id: func_date.cpp 2675 2011-06-04 04:58:07Z xlou $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_dtm.h"
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

CalpontSystemCatalog::ColType Func_date::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_date::getIntVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	CalpontSystemCatalog::ColDataType type = parm[0]->data()->resultType().colDataType;

	string value = "";

	switch (type)
	{
		case execplan::CalpontSystemCatalog::DATE:
		{
			return parm[0]->data()->getDatetimeIntVal(row, isNull);
			break;
		}

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			int64_t val1  = parm[0]->data()->getDatetimeIntVal(row, isNull);
			value = dataconvert::DataConvert::datetimeToString(val1);
			value = value.substr(0,10);
			break;
		}

		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::CHAR:
		case execplan::CalpontSystemCatalog::DECIMAL:
		{
			isNull = true;
			return 0;
		}
		break;

		default:
		{
			std::ostringstream oss;
			oss << "date: datatype of " << execplan::colDataTypeToString(type);
			throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
		}
	}

	return dataconvert::DataConvert::datetimeToInt(value);
}

string Func_date::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	string val = parm[0]->data()->getStrVal(row, isNull);

	return val.substr(0,10);
}


} // namespace funcexp
// vim:ts=4 sw=4:
