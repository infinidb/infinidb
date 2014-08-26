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

//  $Id: windowfunctiontype.cpp 3932 2013-06-25 16:08:10Z xlou $


//#define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>
using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/assign/list_of.hpp>                 // for map_list_of()
#include <boost/algorithm/string/case_conv.hpp>     // for to_upper_copy
using namespace boost;

#include "loggingid.h"
#include "errorcodes.h"
#include "idberrorinfo.h"
using namespace logging;

#include "idborderby.h"
using namespace ordering;

#include "calpontsystemcatalog.h"
#include "constantcolumn.h"
#include "dataconvert.h"                            // int64_t IDB_pow[19]
using namespace execplan;

#include "windowfunctionstep.h"
using namespace joblist;

#include "windowframe.h"
#include "windowfunctiontype.h"
#include "wf_count.h"
#include "wf_lead_lag.h"
#include "wf_min_max.h"
#include "wf_nth_value.h"
#include "wf_ntile.h"
#include "wf_percentile.h"
#include "wf_ranking.h"
#include "wf_row_number.h"
#include "wf_stats.h"
#include "wf_sum_avg.h"

namespace windowfunction
{

map<int, string> colType2String = assign::map_list_of
	(CalpontSystemCatalog::BIT, "BIT")
	(CalpontSystemCatalog::TINYINT, "TINYINT")
	(CalpontSystemCatalog::CHAR, "CHAR")
	(CalpontSystemCatalog::SMALLINT, "SMALLINT")
	(CalpontSystemCatalog::DECIMAL, "DECIMAL")
	(CalpontSystemCatalog::MEDINT, "MEDINT")
	(CalpontSystemCatalog::INT, "INT")
	(CalpontSystemCatalog::FLOAT, "FLOAT")
	(CalpontSystemCatalog::DATE, "DATE")
	(CalpontSystemCatalog::BIGINT, "BIGINT")
	(CalpontSystemCatalog::DOUBLE, "DOUBLE")
	(CalpontSystemCatalog::DATETIME, "DATETIME")
	(CalpontSystemCatalog::VARCHAR, "VARCHAR")
	(CalpontSystemCatalog::VARBINARY, "VARBINARY")
	(CalpontSystemCatalog::CLOB, "CLOB")
	(CalpontSystemCatalog::BLOB, "BLOB")
	(CalpontSystemCatalog::UTINYINT, "UNSIGNED TINYINT")
	(CalpontSystemCatalog::USMALLINT, "UNSIGNED SMALLINT")
	(CalpontSystemCatalog::UDECIMAL, "UNSIGNED DECIMAL")
	(CalpontSystemCatalog::UMEDINT, "UNSIGNED MEDINT")
	(CalpontSystemCatalog::UINT, "UNSIGNED INT")
	(CalpontSystemCatalog::UFLOAT, "UNSIGNED FLOAT")
	(CalpontSystemCatalog::UBIGINT, "UNSIGNED BIGINT")
	(CalpontSystemCatalog::UDOUBLE, "UNSIGNED DOUBLE")
	(CalpontSystemCatalog::LONGDOUBLE, "INTERNAL LONG DOUBLE")
	(CalpontSystemCatalog::STRINT, "INTERNAL SHORT STRING")
;


// function name to function id convert
map<string, int> WindowFunctionType::windowFunctionId = assign::map_list_of
	(string("COUNT(*)"),        WF__COUNT_ASTERISK)
	(string("COUNT"),           WF__COUNT)
	(string("SUM"),             WF__SUM)
	(string("AVG"),             WF__AVG)
	(string("MIN"),             WF__MIN)
	(string("MAX"),             WF__MAX)
	(string("COUNT_DISTINCT"),  WF__COUNT_DISTINCT)
	(string("SUM_DISTINCT"),    WF__SUM_DISTINCT)
	(string("AVG_DISTINCT"),    WF__AVG_DISTINCT)
	(string("MIN_DISTINCT"),    WF__MIN)
	(string("MAX_DISTINCT"),    WF__MAX)
	(string("STD"),             WF__STDDEV_POP)
	(string("STDDEV"),          WF__STDDEV_POP)
	(string("STDDEV_POP"),      WF__STDDEV_POP)
	(string("STDDEV_SAMP"),     WF__STDDEV_SAMP)
	(string("VARIANCE"),        WF__VAR_POP)
	(string("VAR_POP"),         WF__VAR_POP)
	(string("VAR_SAMP"),        WF__VAR_SAMP)
	(string("ROW_NUMBER"),      WF__ROW_NUMBER)
	(string("RANK"),            WF__RANK)
	(string("PERCENT_RANK"),    WF__PERCENT_RANK)
	(string("DENSE_RANK"),      WF__DENSE_RANK)
	(string("CUME_DIST"),       WF__CUME_DIST)
	(string("FIRST_VALUE"),     WF__FIRST_VALUE)
	(string("LAST_VALUE"),      WF__LAST_VALUE)
	(string("NTH_VALUE"),       WF__NTH_VALUE)
	(string("LAG"),             WF__LAG)
	(string("LEAD"),            WF__LEAD)
	(string("NTILE"),           WF__NTILE)
	(string("MEDIAN"),          WF__PERCENTILE_CONT)
	(string("PERCENTILE"),      WF__PERCENTILE_CONT)
	(string("PERCENTILE_CONT"), WF__PERCENTILE_CONT)
	(string("PERCENTILE_DISC"), WF__PERCENTILE_DISC)
	(string("REGR_SLOPE"),      WF__REGR_SLOPE)
	(string("REGR_INTERCEPT"),  WF__REGR_INTERCEPT)
	(string("REGR_COUNT"),      WF__REGR_COUNT)
	(string("REGR_R2"),         WF__REGR_R2)
	(string("REGR_AVGX"),       WF__REGR_AVGX)
	(string("REGR_AVGY"),       WF__REGR_AVGY)
	(string("REGR_SXX"),        WF__REGR_SXX)
	(string("REGR_SXY"),        WF__REGR_SXY)
	(string("REGR_SYY"),        WF__REGR_SYY)
;

boost::shared_ptr<WindowFunctionType>
	WindowFunctionType::makeWindowFunction(const string& name, int ct)
{
	boost::shared_ptr<WindowFunctionType> af;
	int functionId = windowFunctionId[algorithm::to_upper_copy(name)];
	switch (functionId)
	{
		case WF__COUNT_ASTERISK:
		case WF__COUNT:
		case WF__COUNT_DISTINCT:
			af = WF_count<int64_t>::makeFunction(functionId, name, ct);
			break;
		case WF__MIN:
		case WF__MAX:
			af = WF_min_max<int64_t>::makeFunction(functionId, name, ct);
			break;
		case WF__SUM:
		case WF__AVG:
		case WF__SUM_DISTINCT:
		case WF__AVG_DISTINCT:
			af = WF_sum_avg<int64_t>::makeFunction(functionId, name, ct);
			break;
		case WF__STDDEV_POP:
		case WF__STDDEV_SAMP:
		case WF__VAR_POP:
		case WF__VAR_SAMP:
			af = WF_stats<int64_t>::makeFunction(functionId, name, ct);
			break;
		case WF__ROW_NUMBER:
			af = WF_row_number::makeFunction(functionId, name, ct);
			break;
		case WF__RANK:
		case WF__DENSE_RANK:
		case WF__PERCENT_RANK:
		case WF__CUME_DIST:
			af = WF_ranking::makeFunction(functionId, name, ct);
			break;
		case WF__FIRST_VALUE:
		case WF__LAST_VALUE:
		case WF__NTH_VALUE:
			af = WF_nth_value<int64_t>::makeFunction(functionId, name, ct);
			break;
		case WF__LEAD:
		case WF__LAG:
			af = WF_lead_lag<int64_t>::makeFunction(functionId, name, ct);
			break;
		case WF__NTILE:
			af = WF_ntile::makeFunction(functionId, name, ct);
			break;
		case WF__PERCENTILE_CONT:
		case WF__PERCENTILE_DISC:
			af = WF_percentile<int64_t>::makeFunction(functionId, name, ct);
			break;
		case WF__REGR_SLOPE:
		case WF__REGR_INTERCEPT:
		case WF__REGR_COUNT:
		case WF__REGR_R2:
		case WF__REGR_AVGX:
		case WF__REGR_AVGY:
		case WF__REGR_SXX:
		case WF__REGR_SXY:
		case WF__REGR_SYY:
		case WF__UNDEFINED:
		default:
			throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_NOT_SUPPORT, name),
				ERR_WF_NOT_SUPPORT);
			break;
	}

	return af;
}


const string WindowFunctionType::toString() const
{
	ostringstream oss;
	oss << "Window Function Id: " << fFunctionId << ", field indices: ";
	for (uint64_t i = 0; i < fFieldIndex.size(); i++)
		oss << fFieldIndex[i] << " ";
	oss << endl;

	return oss.str();
}


template<typename T> void WindowFunctionType::getValue(uint64_t i, T& t)
{
}


template<> void WindowFunctionType::getValue<int64_t>(uint64_t i, int64_t& t)
{
	t = fRow.getIntField(i);
}


template<> void WindowFunctionType::getValue<uint64_t>(uint64_t i, uint64_t& t)
{
	t = fRow.getUintField(i);
}


template<> void WindowFunctionType::getValue<double>(uint64_t i, double& t)
{
	t = fRow.getDoubleField(i);
}


template<> void WindowFunctionType::getValue<float>(uint64_t i, float& t)
{
	t = fRow.getFloatField(i);
}


template<> void WindowFunctionType::getValue<string>(uint64_t i, string& t)
{
	t = fRow.getStringField(i);
}


template<typename T> void WindowFunctionType::setValue(uint64_t i, T& t)
{
}


template<> void WindowFunctionType::setValue<int64_t>(uint64_t i, int64_t& t)
{
	fRow.setIntField(t, i);
}


template<> void WindowFunctionType::setValue<uint64_t>(uint64_t i, uint64_t& t)
{
	fRow.setUintField(t, i);
}


template<> void WindowFunctionType::setValue<double>(uint64_t i, double& t)
{
	fRow.setDoubleField(t, i);
}


template<> void WindowFunctionType::setValue<float>(uint64_t i, float& t)
{
	fRow.setFloatField(t, i);
}


template<> void WindowFunctionType::setValue<string>(uint64_t i, string& t)
{
	fRow.setStringField(t, i);
}


template<typename T>
void WindowFunctionType::setValue(int ct, int64_t b, int64_t e, int64_t c, T* v)
{
	if (c != WF__BOUND_ALL)
		b = e = c;

	uint64_t i = fFieldIndex[0];
	if (v == NULL)
		v = (T*) getNullValueByType(ct, i);

	for (int64_t j = b; j <= e; j++)
	{
		if (j % 1000 == 0 && fStep->cancelled())
			break;

		fRow.setData(getPointer((*fRowData)[j]));
		setValue(i, *v);
	}
}


template<typename T>
void WindowFunctionType::implicit2T(uint64_t i, T& t, int s)
{
	int ct = fRow.getColType(i);
	int pw = 0;
	switch (ct)
	{
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::DECIMAL:
		{
			t = (T) fRow.getIntField(i);
			pw = s - fRow.getScale(i); // pw is difference of scales, will be in [-18, 18]
			if (pw > 0)
				t *= IDB_pow[pw];
			else if (pw < 0)
				t /= IDB_pow[-pw];
			break;
		}
		case CalpontSystemCatalog::UTINYINT:
		case CalpontSystemCatalog::USMALLINT:
		case CalpontSystemCatalog::UMEDINT:
		case CalpontSystemCatalog::UINT:
		case CalpontSystemCatalog::UBIGINT:
		case CalpontSystemCatalog::UDECIMAL:
		{
			t = (T) fRow.getUintField(i);
			pw = s - fRow.getScale(i); // pw is difference of scales, will be in [-18, 18]
			if (pw > 0)
				t *= IDB_pow[pw];
			else if (pw < 0)
				t /= IDB_pow[-pw];
			break;
		}
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
		{
			if (s == 0)
				t = (T) fRow.getDoubleField(i);
			else
				t = (T) (fRow.getDoubleField(i) * IDB_pow[s]); // s is scale, [0, 18]
			break;
		}
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
		{
			if (s == 0)
				t = (T) fRow.getFloatField(i);
			else
				t = (T) (fRow.getFloatField(i) * IDB_pow[s]); // s is scale, [0, 18]
			break;
		}
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		default:
		{
			string errStr = fFunctionName + "(" + colType2String[ct] + ")";
			errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_INVALID_PARM_TYPE, errStr);
			cerr << errStr << endl;
			throw IDBExcept(errStr, ERR_WF_INVALID_PARM_TYPE);

			break;
		}
	}
}


template<>
void WindowFunctionType::implicit2T<string>(uint64_t i, string& t, int)
{
	t = fRow.getStringField(i);
}


template<typename T>
void WindowFunctionType::getConstValue(ConstantColumn* cc, T& t, bool& b)
{
}


template<>
void WindowFunctionType::getConstValue<int64_t>(ConstantColumn* cc, int64_t& t, bool& b)
{
	t = cc->getIntVal(fRow, b);
}


template<>
void WindowFunctionType::getConstValue<uint64_t>(ConstantColumn* cc, uint64_t& t, bool& b)
{
	t = cc->getUintVal(fRow, b);
}


template<>
void WindowFunctionType::getConstValue<double>(ConstantColumn* cc, double& t, bool& b)
{
	t = cc->getDoubleVal(fRow, b);
}


template<>
void WindowFunctionType::getConstValue<float>(ConstantColumn* cc, float& t, bool& b)
{
	t = cc->getFloatVal(fRow, b);
}


template<>
void WindowFunctionType::getConstValue<string>(ConstantColumn* cc, string& t, bool& b)
{
	t = cc->getStrVal(fRow, b);
}


template void WindowFunctionType::implicit2T<int64_t>(uint64_t, int64_t&, int);
template void WindowFunctionType::implicit2T<uint64_t>(uint64_t, uint64_t&, int);
template void WindowFunctionType::implicit2T<float>(uint64_t, float&, int);
template void WindowFunctionType::implicit2T<double>(uint64_t, double&, int);

template void WindowFunctionType::setValue<int64_t>(int, int64_t, int64_t, int64_t, int64_t*);
template void WindowFunctionType::setValue<uint64_t>(int, int64_t, int64_t, int64_t, uint64_t*);
template void WindowFunctionType::setValue<float>(int, int64_t, int64_t, int64_t, float*);
template void WindowFunctionType::setValue<double>(int, int64_t, int64_t, int64_t, double*);
template void WindowFunctionType::setValue<string>(int, int64_t, int64_t, int64_t, string*);



void* WindowFunctionType::getNullValueByType(int ct, int pos)
{
	static uint64_t bigIntNull    = joblist::BIGINTNULL;
	static uint64_t intNull       = joblist::INTNULL;
	static uint64_t smallIntNull  = joblist::SMALLINTNULL;
	static uint64_t tinyIntNull   = joblist::TINYINTNULL;
	static uint64_t ubigIntNull   = joblist::UBIGINTNULL;
	static uint64_t uintNull      = joblist::UINTNULL;
	static uint64_t usmallIntNull = joblist::USMALLINTNULL;
	static uint64_t utinyIntNull  = joblist::UTINYINTNULL;
	static uint64_t floatNull     = joblist::FLOATNULL;
	static uint64_t doubleNull    = joblist::DOUBLENULL;
	static uint64_t dateNull      = joblist::DATENULL;
	static uint64_t datetimeNull  = joblist::DATETIMENULL;
	static uint64_t char1Null     = joblist::CHAR1NULL;
	static uint64_t char2Null     = joblist::CHAR2NULL;
	static uint64_t char4Null     = joblist::CHAR4NULL;
	static uint64_t char8Null     = joblist::CHAR8NULL;
	static string stringNull("");

	void* v = NULL;
	switch (ct) {
		case CalpontSystemCatalog::TINYINT:
			v = &tinyIntNull;
			break;
		case CalpontSystemCatalog::SMALLINT:
			v = &smallIntNull;
			break;
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			v = &intNull;
			break;
		case CalpontSystemCatalog::BIGINT:
			v = &bigIntNull;
			break;
		case CalpontSystemCatalog::DATE:
			v = &dateNull;
			break;
		case CalpontSystemCatalog::DATETIME:
			v = &datetimeNull;
			break;
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
			v = &floatNull;
			break;
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
			v = &doubleNull;
			break;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR: {
			uint64_t len = fRow.getColumnWidth(pos);
			switch (len) {
				case 1:
					v = &char1Null;
					break;
				case 2:
					v = &char2Null;
					break;
				case 3:
				case 4:
					v = &char4Null;
					break;
				case 5:
				case 6:
				case 7:
				case 8:
					v = &char8Null;
					break;
				default:
					v = &stringNull;
					break;
			}
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
		case CalpontSystemCatalog::UDECIMAL:
		{
			uint64_t len = fRow.getColumnWidth(pos);
			switch (len) {
				case 1:
					v = &tinyIntNull;
					break;
				case 2:
					v = &smallIntNull;
					break;
				case 4:
					v = &intNull;
					break;
				default:
					v = &bigIntNull;
					break;
			}
			break;
		}
		case CalpontSystemCatalog::UTINYINT:
			v = &utinyIntNull;
			break;
		case CalpontSystemCatalog::USMALLINT:
			v = &usmallIntNull;
			break;
		case CalpontSystemCatalog::UMEDINT:
		case CalpontSystemCatalog::UINT:
			v = &uintNull;
			break;
		case CalpontSystemCatalog::UBIGINT:
			v = &ubigIntNull;
			break;
		case CalpontSystemCatalog::LONGDOUBLE:
		case CalpontSystemCatalog::VARBINARY:
		default:
			std::ostringstream oss;
			oss << "not supported data type: " << colType2String[ct];
			throw logic_error(oss.str());
			break;
	}

	return v;
}


}   //namespace
// vim:ts=4 sw=4:

