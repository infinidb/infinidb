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
* $Id: func_sec_to_time.cpp 2477 2011-05-12 16:07:35Z chao $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_str.h"
#include "functioncolumn.h"
#include "rowgroup.h"
#include "funchelpers.h"
#include "predicateoperator.h"
using namespace execplan;

#include "dataconvert.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;
namespace funcexp
{

CalpontSystemCatalog::ColType Func_sec_to_time::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

string Func_sec_to_time::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	
	int64_t val = 0;
	CalpontSystemCatalog::ColType curCt = parm[0]->data()->resultType();
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
			val = parm[0]->data()->getIntVal(row, isNull);
		}
		break;

		case execplan::CalpontSystemCatalog::DOUBLE:
		{
			const string& valStr = parm[0]->data()->getStrVal(row, isNull);
			val = parm[0]->data()->getIntVal(row, isNull);
			size_t x = valStr.find(".");
			if ( x < string::npos)
			{
				string tmp = valStr.substr(x+1,1);
				char * ptr = &tmp[0];
				int i = atoi(ptr);
				if (i >= 5)
				{
					if (val > 0)
						val += 1;
					else 
						val -=1;
				}
			}
		}
		break;

		case execplan::CalpontSystemCatalog::FLOAT:
		{
			const string& valStr = parm[0]->data()->getStrVal(row, isNull);
			val = parm[0]->data()->getIntVal(row, isNull);
			size_t x = valStr.find(".");
			if ( x < string::npos)
			{
				string tmp = valStr.substr(x+1,1);
				char * ptr = &tmp[0];
				int i = atoi(ptr);
				if (i >= 5)
				{
					if (val > 0)
						val += 1;
					else 
						val -=1;
				}
			}
		}
		break;

		case execplan::CalpontSystemCatalog::DECIMAL:
		{
			const string& valStr = parm[0]->data()->getStrVal(row, isNull);
			
			val = parm[0]->data()->getIntVal(row, isNull);
			size_t x = valStr.find(".");
			if ( x < string::npos)
			{
				string tmp = valStr.substr(x+1,1);
				char * ptr = &tmp[0];
				int i = atoi(ptr);
				if (i >= 5)
				{
					if (val > 0)
						val += 1;
					else 
						val -=1;
				}
			}
		}
		break;
		
		case execplan::CalpontSystemCatalog::CHAR:
		case execplan::CalpontSystemCatalog::VARCHAR:
		{
			val = parm[0]->data()->getIntVal(row, isNull);
	        
			break;
		}

		default:
		{
			std::ostringstream oss;
			oss << "sec_to_time: datatype of " << execplan::colDataTypeToString(parm[0]->data()->resultType().colDataType);
			throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
		}
	}
	
	int64_t posVal = llabs(val);
	if (val >3020399)
		return ("838:59:59");
		
	if (val < -3020399)
		return ("-838:59:59");
		
	//Format the time
	uint32_t hour = 0;
	uint32_t minute = 0;
	uint32_t second = 0;
		
	hour = posVal / 3600;
	minute = (posVal - (hour * 3600)) / 60;
	second = posVal - (hour * 3600) - (minute * 60);
    
    const char* minus = "-";
    const char* nominus = "";
	
	const char* signstr = ( val < 0 ) ? minus : nominus;
   
	char buf[32]; // actual string either 9 or 10 characters
    snprintf( buf, 32, "%s%02d:%02d:%02d", signstr, hour, minute, second );
	return buf;
}


int64_t Func_sec_to_time::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	int64_t val = parm[0]->data()->getIntVal(row, isNull);
	if (val > 3020399)
		val = 8385959;
	else if (val < -3020399)
		val = 4286581337LL;
	else
	{
		string time = getStrVal(row, parm, isNull, op_ct);
		size_t x = time.find(":");
		
		while ( x < string::npos)
		{
			time.erase(x,1);
			x = time.find(":");
		}
		char *ep = NULL;
		const char *str = time.c_str();
		errno = 0;
		val= strtoll(str, &ep, 10);
	}
	return val;
}

double Func_sec_to_time::getDoubleVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	
	double val = parm[0]->data()->getDoubleVal(row, isNull);
	
	if (val > 3020399)
		val = 8385959;
	else if (val < -3020399)
		val = 4286581337LL;
	else
	{
		string time = getStrVal(row, parm, isNull, op_ct);
		size_t x = time.find(":");
		
		while ( x < string::npos)
		{
			time.erase(x, 1);
			x = time.find(":");
		}
		char *ep = NULL;
		const char *str = time.c_str();
		errno = 0;
		val= (double)strtoll(str, &ep, 10);
	}
	return val;
}							

execplan::IDB_Decimal Func_sec_to_time::getDecimalVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	IDB_Decimal d;
	int64_t val = parm[0]->data()->getIntVal(row, isNull);
	if (val > 3020399)
		d.value = 8385959;
	else if (val < -3020399)
		d.value = 4286581337LL;
	else
	{
		string time = getStrVal(row, parm, isNull, op_ct);
		size_t x = time.find(":");
		
		while ( x < string::npos)
		{
			time.erase(x,1);
			x = time.find(":");
		}
		char *ep = NULL;
		const char *str = time.c_str();
		errno = 0;
		d.value= strtoll(str, &ep, 10);
	}
	d.scale = 0;
	return d;
}




} // namespace funcexp
// vim:ts=4 sw=4:
