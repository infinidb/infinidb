/* Copyright (C) 2013 Calpont Corp.

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
* $Id: funcexp.cpp 3954 2013-07-08 16:30:15Z bpaul $
*
*
****************************************************************************/

#include <boost/thread/mutex.hpp>

#include "funcexp.h"
#include "functor_all.h"
#include "functor_bool.h"
#include "functor_dtm.h"
#include "functor_int.h"
#include "functor_real.h"
#include "functor_str.h"
#include "functor_export.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "joblisttypes.h"
using namespace joblist;

#ifndef SKIP_UDF
#include "udfsdk.h"
#endif

bool cPo = false;		// extern-ed in utils_utf8.h


namespace funcexp
{
/* static */
FuncExp* FuncExp::fInstance = 0;

/* static */
boost::mutex FuncExp::fInstanceMutex;

FuncExp* FuncExp::instance()
{
	boost::mutex::scoped_lock lk(fInstanceMutex);
	if (!fInstance)
		fInstance = new FuncExp();
	return fInstance;
}


FuncExp::FuncExp()
{
	fFuncMap["<<"] = new Func_leftshift();
	fFuncMap[">>"] = new Func_rightshift();
	fFuncMap["|"] = new Func_bitor();
	fFuncMap["^"] = new Func_bitxor();
	fFuncMap["&"] = new Func_bitand();
	fFuncMap["abs"] = new Func_abs();
	fFuncMap["acos"] = new Func_acos();
	fFuncMap["add_time"] = new Func_add_time();
	fFuncMap["asin"] = new Func_asin();
	fFuncMap["ascii"] = new Func_ascii();
	fFuncMap["atan"] = new Func_atan();
	fFuncMap["atan2"] = new Func_atan();
	fFuncMap["between"] = new Func_between();
	fFuncMap["case_searched"] = new Func_searched_case();
	fFuncMap["case_simple"] = new Func_simple_case();
	fFuncMap["cast_as_signed"] = new Func_cast_signed();	//dlh
    fFuncMap["cast_as_unsigned"] = new Func_cast_unsigned();	//dch
	fFuncMap["cast_as_char"] = new Func_cast_char();	//dlh
	fFuncMap["cast_as_date"] = new Func_cast_date();	//dlh
	fFuncMap["cast_as_datetime"] = new Func_cast_datetime();	//dlh
	fFuncMap["decimal_typecast"] = new Func_cast_decimal();	//dlh
	fFuncMap["ceil"] = new Func_ceil();	//dlh
	fFuncMap["ceiling"] = new Func_ceil();	//dlh
	fFuncMap["char"] = new Func_char();	//dlh
	fFuncMap["char_length"] = new Func_char_length();	//dlh
	fFuncMap["character_length"] = new Func_char_length();	//dlh
	fFuncMap["coalesce"] = new Func_coalesce();
	fFuncMap["concat"] = new Func_concat();
	fFuncMap["concat_ws"] = new Func_concat_ws();
	fFuncMap["conv"] = new Func_conv();
	fFuncMap["cos"] = new Func_cos();
	fFuncMap["cot"] = new Func_cot();
	fFuncMap["convert"] = new Func_cast_char();	//dlh
	fFuncMap["crc32"] = new Func_crc32();
	fFuncMap["date_add_interval"] = new Func_date_add(); //dlh
	fFuncMap["date_format"] = new Func_date_format();
	fFuncMap["day"] = new Func_day();	//dlh
	fFuncMap["dayname"] = new Func_dayname();
	fFuncMap["dayofmonth"] = new Func_day();	//dlh
	fFuncMap["dayofweek"] = new Func_dayofweek();	//dlh
	fFuncMap["dayofyear"] = new Func_dayofyear();	//dlh
	fFuncMap["degrees"] = new Func_degrees();
	fFuncMap["DIV"] = new Func_div(); // MySQL use upper case for this function name
	fFuncMap["elt"] = new Func_elt();
	fFuncMap["exp"] = new Func_exp();
	fFuncMap["extract"] = new Func_extract();	//dlh
	fFuncMap["find_in_set"] = new Func_find_in_set();
	fFuncMap["floor"] = new Func_floor();	//dlh
	fFuncMap["format"] = new Func_format();	//dlh
	fFuncMap["from_days"] = new Func_from_days();
	fFuncMap["from_unixtime"] = new Func_from_unixtime();
	fFuncMap["get_format"] = new Func_get_format();	//dlh
	fFuncMap["greatest"] = new Func_greatest();	//dlh
	fFuncMap["hex"] = new Func_hex();
	fFuncMap["hour"] = new Func_hour();	//dlh
	fFuncMap["if"] = new Func_if();
	fFuncMap["ifnull"] = new Func_ifnull();
	fFuncMap["in"] = new Func_in();
	fFuncMap[" IN "] = new Func_in();
	fFuncMap["inet_aton"] = new Func_inet_aton();
	fFuncMap["inet_ntoa"] = new Func_inet_ntoa();
	fFuncMap["insert"] = new Func_insert();
	fFuncMap["instr"] = new Func_instr();
	fFuncMap["isnull"] = new Func_isnull(false);
	fFuncMap["isnotnull"] = new Func_isnull(true);
	fFuncMap["last_day"] = new Func_last_day();
	fFuncMap["lcase"] = new Func_lcase();	//dlh
	fFuncMap["least"] = new Func_least();	//dlh
	fFuncMap["left"] = new Func_left();		//dlh
	fFuncMap["length"] = new Func_length();
	fFuncMap["ln"] = new Func_log();
	fFuncMap["locate"] = new Func_instr();
	fFuncMap["log"] = new Func_log();
	fFuncMap["log2"] = new Func_log2();
	fFuncMap["log10"] = new Func_log10();
	fFuncMap["lower"] = new Func_lcase();	//dlh
	fFuncMap["lpad"] = new Func_lpad();	//dlh
	fFuncMap["ltrim"] = new Func_ltrim();	//dlh
	fFuncMap["makedate"] = new Func_makedate();
	fFuncMap["maketime"] = new Func_maketime();
	fFuncMap["microsecond"] = new Func_microsecond();
	fFuncMap["minute"] = new Func_minute();	//dlh
	fFuncMap["mod"] = new Func_mod();	//dlh
	fFuncMap["%"] = new Func_mod();	//dlh
	fFuncMap["md5"] = new Func_md5();
	fFuncMap["mid"] = new Func_substr();
	fFuncMap["month"] = new Func_month();	//dlh
	fFuncMap["monthname"] = new Func_monthname();
	fFuncMap["notin"] = new Func_notin();
	fFuncMap["not IN "] = new Func_notin();
	fFuncMap["notbetween"] = new Func_notbetween();
	fFuncMap["nullif"] = new Func_nullif();
	fFuncMap["period_add"] = new Func_period_add();	//dlh
	fFuncMap["period_diff"] = new Func_period_diff();	//dlh
	fFuncMap["position"] = new Func_instr();	//dlh
	fFuncMap["pow"] = new Func_pow();
	fFuncMap["power"] = new Func_pow();
	fFuncMap["quarter"] = new Func_quarter();
	fFuncMap["radians"] = new Func_radians();	//dlh
	fFuncMap["rand"] = new Func_rand();
	fFuncMap["regexp"] = new Func_regexp();	//dlh
	fFuncMap["repeat"] = new Func_repeat();	//dlh
	fFuncMap["replace"] = new Func_replace();	//dlh
	fFuncMap["reverse"] = new Func_reverse();	//dlh
	fFuncMap["right"] = new Func_right();	//dlh
	fFuncMap["round"] = new Func_round();
	fFuncMap["rpad"] = new Func_rpad();	//dlh
	fFuncMap["rtrim"] = new Func_rtrim();	//dlh
	fFuncMap["second"] = new Func_second();	//dlh
	fFuncMap["sec_to_time"] = new Func_sec_to_time();
	fFuncMap["sha"] = new Func_sha();
	fFuncMap["sha1"] = new Func_sha();
	fFuncMap["sign"] = new Func_sign();
	fFuncMap["sin"] = new Func_sin();
	fFuncMap["sqrt"] = new Func_sqrt();
	fFuncMap["str_to_date"] = new Func_str_to_date();
	fFuncMap["strcmp"] = new Func_strcmp();
	fFuncMap["substr"] = new Func_substr();
	fFuncMap["substring"] = new Func_substr();	//dlh
	fFuncMap["substring_index"] = new Func_substring_index();	//dlh
	fFuncMap["sysdate"] = new Func_sysdate();	//dlhFuncExp
	fFuncMap["cast_as_time"] = new Func_time();	//dlh
	fFuncMap["tan"] = new Func_tan();
	fFuncMap["timediff"] = new Func_timediff();	//dlh
	fFuncMap["timestampdiff"] = new Func_timestampdiff();
	fFuncMap["time_format"] = new Func_time_format();	//dlh
	fFuncMap["time_to_sec"] = new Func_time_to_sec();	//dlh
	fFuncMap["to_days"] = new Func_to_days();	//dlh
	fFuncMap["trim"] = new Func_trim();	//dlh
	fFuncMap["truncate"] = new Func_truncate();	//dlh
	fFuncMap["ucase"] = new Func_ucase();	//dlh
	//fFuncMap["unhex"] = new Func_unhex();
	fFuncMap["unix_timestamp"] = new Func_unix_timestamp();
	fFuncMap["upper"] = new Func_ucase();	//dlh
	fFuncMap["week"] = new Func_week();	//dlh
	fFuncMap["weekday"] = new Func_weekday();	
	fFuncMap["weekofyear"] = new Func_week();	//dlh
	fFuncMap["year"] = new Func_year();	//dlh
	fFuncMap["yearweek"] = new Func_yearweek();	//dlh
#ifndef SKIP_UDF
	udfsdk::UDFSDK sdk;
	FuncMap sdkfm = sdk.UDFMap();
	FuncMap::const_iterator iter = sdkfm.begin();
	FuncMap::iterator funcMapIter;
	for (; iter != sdkfm.end(); ++iter)
	{
		//add sdkfm to fFuncMap
		funcMapIter = fFuncMap.find(iter->first);
		if (funcMapIter != fFuncMap.end())
		{
			// silently ignore the udf function? log it?
		}
		else
		{
			fFuncMap[iter->first] = iter->second;
		}
	}
#endif
}

Func* FuncExp::getFunctor(std::string& funcName)
{
	FuncMap::iterator iter = fFuncMap.find(funcName);
	if (iter == fFuncMap.end())
		return NULL;
	else
		return (*iter).second;
}

void FuncExp::evaluate(rowgroup::Row& row, std::vector<execplan::SRCP>& expression)
{
	bool isNull;
	for (uint i = 0; i < expression.size(); i++)
	{
		isNull = false;
		switch (expression[i]->resultType().colDataType)
		{			
			case CalpontSystemCatalog::DATE:
			{
				int64_t val = expression[i]->getIntVal(row, isNull);
				if (isNull)
					row.setUintField<4>(DATENULL, expression[i]->outputIndex());
				else
					row.setUintField<4>(val, expression[i]->outputIndex());
				break;
			}
			case CalpontSystemCatalog::DATETIME:
			{
				int64_t val = expression[i]->getIntVal(row, isNull);
				if (isNull)
					row.setUintField<8>(DATETIMENULL, expression[i]->outputIndex());
				else
					row.setUintField<8>(val, expression[i]->outputIndex());
				break;
			}
			case CalpontSystemCatalog::CHAR:
			case CalpontSystemCatalog::VARCHAR:			
			{			
				const std::string& val = expression[i]->getStrVal(row, isNull);
				if (isNull)
					row.setStringField(CPNULLSTRMARK, expression[i]->outputIndex());
				else
					row.setStringField(val, expression[i]->outputIndex());
				break;
			}
			case CalpontSystemCatalog::BIGINT:
			{
				int64_t val = expression[i]->getIntVal(row, isNull);
				if (isNull)
					row.setIntField<8>(BIGINTNULL, expression[i]->outputIndex());
				else
					row.setIntField<8>(val, expression[i]->outputIndex());
				break;								
			}
            case CalpontSystemCatalog::UBIGINT:
            {
                uint64_t val = expression[i]->getUintVal(row, isNull);
                if (isNull)
                    row.setUintField<8>(UBIGINTNULL, expression[i]->outputIndex());
                else
                    row.setUintField<8>(val, expression[i]->outputIndex());
                break;								
            }
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::MEDINT:
			{
				int64_t val = expression[i]->getIntVal(row, isNull);
				if (isNull)
					row.setIntField<4>(INTNULL, expression[i]->outputIndex());
				else
					row.setIntField<4>(val, expression[i]->outputIndex());
				break;					
			}
			case CalpontSystemCatalog::UINT:
			case CalpontSystemCatalog::UMEDINT:
			{
				uint64_t val = expression[i]->getUintVal(row, isNull);
				if (isNull)
					row.setUintField<4>(UINTNULL, expression[i]->outputIndex());
				else
					row.setUintField<4>(val, expression[i]->outputIndex());
				break;					
			}
			case CalpontSystemCatalog::SMALLINT:
			{
				int64_t val = expression[i]->getIntVal(row, isNull);
				if (isNull)
					row.setIntField<2>(SMALLINTNULL, expression[i]->outputIndex());
				else
					row.setIntField<2>(val, expression[i]->outputIndex());
				break;	
			}
            case CalpontSystemCatalog::USMALLINT:
            {
                uint64_t val = expression[i]->getUintVal(row, isNull);
                if (isNull)
                    row.setUintField<2>(USMALLINTNULL, expression[i]->outputIndex());
                else
                    row.setUintField<2>(val, expression[i]->outputIndex());
                break;	
            }
			case CalpontSystemCatalog::TINYINT:
			{
				int64_t val = expression[i]->getIntVal(row, isNull);
				if (isNull)
					row.setIntField<1>(TINYINTNULL, expression[i]->outputIndex());
				else
					row.setIntField<1>(val, expression[i]->outputIndex());
				break;	
			}
            case CalpontSystemCatalog::UTINYINT:
            {
                uint64_t val = expression[i]->getUintVal(row, isNull);
                if (isNull)
                    row.setUintField<1>(UTINYINTNULL, expression[i]->outputIndex());
                else
                    row.setUintField<1>(val, expression[i]->outputIndex());
                break;	
            }
			//In this case, we're trying to load a double output column with float data. This is the
			// case when you do sum(floatcol), e.g.
			case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UDOUBLE:
			{
				double val = expression[i]->getDoubleVal(row, isNull);
				if (isNull)
					row.setIntField<8>(DOUBLENULL, expression[i]->outputIndex());
				else
					row.setDoubleField(val, expression[i]->outputIndex());
				break;
			}
			case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT:
			{
				float val = expression[i]->getFloatVal(row, isNull);
				if (isNull)
					row.setIntField<4>(FLOATNULL, expression[i]->outputIndex());
				else
					row.setFloatField(val, expression[i]->outputIndex());
				break;
			}
			case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
			{
				IDB_Decimal val = expression[i]->getDecimalVal(row, isNull);
				if (isNull)
					row.setIntField<8>(BIGINTNULL, expression[i]->outputIndex());
				else
					row.setIntField<8>(val.value, expression[i]->outputIndex());
				break;
			}
			default:	// treat as int64
			{
				throw std::runtime_error("funcexp::evaluate(): non support datatype to set field.");
			}
		}
	}
}

}
