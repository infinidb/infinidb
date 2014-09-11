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

/*
 * $Id: ha_calpont_impl.cpp 8910 2012-09-18 18:52:26Z zzhu $
 */

#include <string>
#include <iostream>
#include <stack>
#ifdef _MSC_VER
#include <unordered_map>
#include <unordered_set>
#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#endif
#include <fstream>
#include <sstream>
#include <cerrno>
#include <cstring>
#include <time.h>
//#define NDEBUG
#include <cassert>
#include <vector>
#include <map>
#include <limits>
using namespace std;

#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/regex.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "idb_mysql.h"

#define NEED_CALPONT_INTERFACE
#include "ha_calpont_impl.h"

#include "ha_calpont_impl_if.h"
using namespace cal_impl_if;

#include "calpontselectexecutionplan.h"
#include "logicoperator.h"
#include "parsetree.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

#include "sm.h"

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

#include "dmlpackage.h"
#include "calpontdmlpackage.h"
#include "insertdmlpackage.h"
#include "vendordmlstatement.h"
#include "calpontdmlfactory.h"
using namespace dmlpackage;

#include "dmlpackageprocessor.h"
using namespace dmlpackageprocessor;

#include "configcpp.h"
using namespace config;

#include "rowgroup.h"
using namespace rowgroup;

#include "brmtypes.h"
using namespace BRM;

#include "calpontselectexecutionplan.h"
#include "calpontsystemcatalog.h"
#include "simplecolumn_int.h"
#include "simplecolumn_decimal.h"
#include "aggregatecolumn.h"
#include "constantcolumn.h"
#include "simplefilter.h"
#include "constantfilter.h"
#include "functioncolumn.h"
#include "arithmeticcolumn.h"
#include "arithmeticoperator.h"
#include "logicoperator.h"
#include "predicateoperator.h"
#include "rowcolumn.h"
#include "selectfilter.h"
using namespace execplan;

#include "tableband.h"
#include "joblisttypes.h"
using namespace joblist;

#include "cacheutils.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "resourcemanager.h"

#include "funcexp.h"
#include "functor.h"
using namespace funcexp;

#include "versionnumber.h"

namespace cal_impl_if
{
	extern bool nonConstFunc(Item_func* ifp);
}

namespace
{
// Calpont vtable non-support error message													 
const string infinidb_autoswitch_warning = "The query includes syntax that is not supported by InfiniDB distributed mode. The execution was switched to standard mode with downgraded performance.";

// copied from item_timefunc.cc
static const string interval_names[]=
{
  "year", "quarter", "month", "week", "day",  
  "hour", "minute", "second", "microsecond",
  "year_month", "day_hour", "day_minute", 
  "day_second", "hour_minute", "hour_second",
  "minute_second", "day_microsecond",
  "hour_microsecond", "minute_microsecond",
  "second_microsecond"
};

const unsigned NONSUPPORTED_ERR_THRESH = 2000;

//TODO: make this session-safe (put in connMap?)
vector<RMParam> rmParms;

//convenience fcn
inline uint32_t tid2sid(const uint32_t tid)
{
	return CalpontSystemCatalog::idb_tid2sid(tid);
}

void storeNumericField(Field** f, int64_t value, CalpontSystemCatalog::ColType& ct)
{
	// unset null bit first
	if ((*f)->null_ptr)
		*(*f)->null_ptr &= ~(*f)->null_bit;		
	switch ((*f)->type())
	{
		case MYSQL_TYPE_NEWDECIMAL:
		{
			Field_new_decimal* f2 = (Field_new_decimal*)*f;
			char buf[256];
			dataconvert::DataConvert::decimalToString( value, (unsigned)ct.scale, buf, 256 );
			f2->store(buf, strlen(buf), f2->charset());
			break;
		}
		case MYSQL_TYPE_TINY: //TINYINT type
		{
			Field_tiny* f2 = (Field_tiny*)*f;
			longlong int_val = (longlong)value;
			f2->store(int_val, f2->unsigned_flag);
			break;
		}
		case MYSQL_TYPE_SHORT: //SMALLINT type
		{
			Field_short* f2 = (Field_short*)*f;
			longlong int_val = (longlong)value;
			f2->store(int_val, f2->unsigned_flag);
			break;
		}
		case MYSQL_TYPE_LONG: //INT type
		{
			Field_long* f2 = (Field_long*)*f;
			longlong int_val = (longlong)value;
			f2->store(int_val, f2->unsigned_flag);
			break;
		}
		case MYSQL_TYPE_LONGLONG: //BIGINT type
		{
			Field_longlong* f2 = (Field_longlong*)*f;
			longlong int_val = (longlong)value;
			f2->store(int_val, f2->unsigned_flag);
			break;
		}
		case MYSQL_TYPE_FLOAT: // FLOAT type
		{
			Field_float* f2 = (Field_float*)*f;
			float float_val = *(float*)(&value);
			f2->store(float_val);
			break;
		}
		case MYSQL_TYPE_DOUBLE: // DOUBLE type
		{
			Field_double* f2 = (Field_double*)*f;
			double double_val = *(double*)(&value);
			f2->store(double_val);
			break;
		}
		default:
		{
			Field_longlong* f2 = (Field_longlong*)*f;
			longlong int_val = (longlong)value;
			f2->store(int_val, f2->unsigned_flag);
			break;
		}
	} 
}

//
// @bug 2244. Log exception related to lost connection to ExeMgr.
// Log exception error from calls to sm::tpl_scan_fetch in fetchNextRow()
//
void tpl_scan_fetch_LogException( cal_table_info& ti, cal_connection_info* ci, std::exception* ex)
{
	time_t t = time(0);
	char datestr[50];
	ctime_r(&t, datestr);
	datestr[ strlen(datestr)-1 ] = '\0'; // strip off trailing newline

	u_int32_t sesID   = 0;
	string connHndl("No connection handle to use");
	
	if (ti.conn_hndl) {
		connHndl = "ti connection used";
		sesID = ti.conn_hndl->sessionID;
	}
	
	else if (ci->cal_conn_hndl) {
		connHndl = "ci connection used";
		sesID = ci->cal_conn_hndl->sessionID;
	}

	int64_t rowsRet = -1;
	if (ti.tpl_scan_ctx)
		rowsRet = ti.tpl_scan_ctx->rowsreturned;

	if (ex)
		cerr << datestr << ": sm::tpl_scan_fetch error getting rows for sessionID: " <<
			sesID << "; " << connHndl << "; rowsReturned: " << rowsRet <<
			"; reason-" << ex->what() << endl;
	else
		cerr << datestr << ": sm::tpl_scan_fetch unknown error getting rows for sessionID: " <<
			sesID << "; " << connHndl << "; rowsReturned: " << rowsRet << endl;
}

const char hexdig[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', };

int vbin2hex(const uint8_t* p, const unsigned l, char* o)
{
	for (unsigned i = 0; i < l; i++, p++)
	{
		*o++ = hexdig[*p >> 4];
		*o++ = hexdig[*p & 0xf];
	}
	return 0;
}

int fetchNextRow(uchar *buf, cal_table_info& ti, cal_connection_info* ci)
{
	int rc = HA_ERR_END_OF_FILE;
	int num_attr = ti.msTablePtr->s->fields;
	CalpontSystemCatalog* csc = 0;
	sm::status_t sm_stat;

	try {
		if (ti.conn_hndl)
		{
			sm_stat = sm::tpl_scan_fetch(ti.tpl_scan_ctx, NULL, 0, ti.conn_hndl);
			csc = ti.conn_hndl->csc;
		}
		else if (ci->cal_conn_hndl)
		{
			sm_stat = sm::tpl_scan_fetch(ti.tpl_scan_ctx, NULL, 0, ci->cal_conn_hndl, (int*)(&current_thd->killed));
			csc = ci->cal_conn_hndl->csc;
		}
		else
			throw runtime_error("internal error");
	} catch (std::exception& ex) {
// @bug 2244. Always log this msg for now, as we try to track down when/why we are
//            losing socket connection with ExeMgr
//#ifdef INFINIDB_DEBUG
		tpl_scan_fetch_LogException( ti, ci, &ex);
//#endif
		sm_stat = sm::CALPONT_INTERNAL_ERROR;
	} catch (...) {
// @bug 2244. Always log this msg for now, as we try to track down when/why we are
//            losing socket connection with ExeMgr
//#ifdef INFINIDB_DEBUG
		tpl_scan_fetch_LogException( ti, ci, 0 );
//#endif
		sm_stat = sm::CALPONT_INTERNAL_ERROR;
	}

	if (sm_stat == sm::STATUS_OK)
	{
		Field **f;
		f = ti.msTablePtr->field;
		//set all fields to null in null col bitmap
		memset(buf, -1, ti.msTablePtr->s->null_bytes);
		CalpontSystemCatalog::ColType* colTypes = ti.tpl_scan_ctx->ctp;
		int64_t intColVal = 0;
		string stringColVal;		
		char tmp[256];
		
		/** tuple-vtable mode */
		if (!(ti.tpl_scan_ctx->traceFlags & execplan::CalpontSelectExecutionPlan::TRACE_TUPLE_OFF))
		{
			RowGroup *rowGroup = ti.tpl_scan_ctx->rowGroup;
			rowgroup::Row row;
			rowGroup->initRow(&row);
			rowGroup->getRow(ti.tpl_scan_ctx->rowsreturned, &row);		
			for (int s = 0; s < num_attr; s++, f++)
			{
				//This col is going to be written
				bitmap_set_bit(ti.msTablePtr->write_set, (*f)->field_index);
							
				// get coltype if not there yet
				if (colTypes[0].colWidth == 0)
				{
					for (short s = 0; s < num_attr; s++)
					{
						colTypes[s].colPosition = s;
						colTypes[s].colWidth = rowGroup->getColumnWidth(s);
						colTypes[s].colDataType = rowGroup->getColTypes()[s];
						colTypes[s].columnOID = rowGroup->getOIDs()[s];
						colTypes[s].scale = rowGroup->getScale()[s];
						colTypes[s].precision = rowGroup->getPrecision()[s];
					}
				}
				CalpontSystemCatalog::ColType colType = colTypes[s];

				// precision == -16 is borrowed as skip null check indicator for bit ops.
				if (row.isNullValue(s) && colTypes[s].precision != -16)
				{
					// @2835. Handle empty string and null confusion. store empty string for string column 
					if (colType.colDataType == CalpontSystemCatalog::CHAR ||
						  colType.colDataType == CalpontSystemCatalog::VARCHAR ||
						  colType.colDataType == CalpontSystemCatalog::VARBINARY)
					{
						Field_varstring* f2 = (Field_varstring*)*f;
						f2->store(tmp, 0, f2->charset());
					}
					continue;
				}
					
				// fetch and store data
				switch (colType.colDataType)
				{
					case CalpontSystemCatalog::DATE:
					{
						if ((*f)->null_ptr)
							*(*f)->null_ptr &= ~(*f)->null_bit;
						intColVal = row.getUintField<4>(s);
						DataConvert::dateToString(intColVal, tmp, 255);
						Field_varstring* f2 = (Field_varstring*)*f;
						f2->store(tmp, strlen(tmp), f2->charset());
						break;
					}
					case CalpontSystemCatalog::DATETIME:
					{
						if ((*f)->null_ptr)
							*(*f)->null_ptr &= ~(*f)->null_bit;
						intColVal = row.getUintField<8>(s);
						DataConvert::datetimeToString(intColVal, tmp, 255);
						Field_varstring* f2 = (Field_varstring*)*f;
						f2->store(tmp, strlen(tmp), f2->charset());
						break;
					}
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR:
					{
						Field_varstring* f2 = (Field_varstring*)*f;
						switch (colType.colWidth)
						{
							case 1:
								intColVal = row.getUintField<1>(s);
								f2->store((char*)(&intColVal), strlen((char*)(&intColVal)), f2->charset());   
								break;
							case 2:
								intColVal = row.getUintField<2>(s);
								f2->store((char*)(&intColVal), strlen((char*)(&intColVal)), f2->charset()); 	
								break;
							case 4:
								intColVal = row.getUintField<4>(s);
								f2->store((char*)(&intColVal), strlen((char*)(&intColVal)), f2->charset()); 
								break;
							case 8:
								//make sure we don't send strlen off into the weeds...
								intColVal = row.getUintField<8>(s);
								memcpy(tmp, &intColVal, 8);
								tmp[8] = 0;
								f2->store(tmp, strlen(tmp), f2->charset()); 
								break;
							default:
								stringColVal = row.getStringField(s);
								f2->store(stringColVal.c_str(), strlen(stringColVal.c_str()), f2->charset());
						}
						if ((*f)->null_ptr)
							*(*f)->null_ptr &= ~(*f)->null_bit;
						break;
					}
					case CalpontSystemCatalog::VARBINARY:
					{
						Field_varstring* f2 = (Field_varstring*)*f;

						if (current_thd->variables.infinidb_varbin_always_hex)
						{
							uint l;
							const uint8_t* p = row.getVarBinaryField(l, s);
							uint ll = l * 2;
							boost::scoped_array<char> sca(new char[ll]);
							vbin2hex(p, l, sca.get());
							f2->store(sca.get(), ll, f2->charset());
						}
						else
							f2->store((const char*)row.getVarBinaryField(s), row.getVarBinaryLength(s), f2->charset());

						if ((*f)->null_ptr)
							*(*f)->null_ptr &= ~(*f)->null_bit;
						break;
					}
					case CalpontSystemCatalog::BIGINT:
					{
						intColVal = row.getIntField<8>(s);
						storeNumericField(f, intColVal, colType);
						break;
					}
					case CalpontSystemCatalog::INT:
					{
						intColVal = row.getIntField<4>(s);
						storeNumericField(f, intColVal, colType);
						break;
					}
					case CalpontSystemCatalog::SMALLINT:
					{
						intColVal = row.getIntField<2>(s);
						storeNumericField(f, intColVal, colType);
						break;
					}
					case CalpontSystemCatalog::TINYINT:
					{
						intColVal = row.getIntField<1>(s);
						storeNumericField(f, intColVal, colType);
						break;
					}
					//In this case, we're trying to load a double output column with float data. This is the
					// case when you do sum(floatcol), e.g.
					case CalpontSystemCatalog::FLOAT:
					{
						float dl = row.getFloatField(s);
						if (dl == std::numeric_limits<float>::infinity())
							continue;
						
						//int64_t* icvp = (int64_t*)&dl;
						//intColVal = *icvp;
						Field_float* f2 = (Field_float*)*f;
						// bug 3485, reserve enough space for the longest float value
						// -3.402823466E+38 to -1.175494351E-38, 0, and
						// 1.175494351E-38 to 3.402823466E+38.
						(*f)->field_length = 40;
						//float float_val = *(float*)(&value);
						//f2->store(float_val);
						f2->store(dl);
						if ((*f)->null_ptr)
							*(*f)->null_ptr &= ~(*f)->null_bit;	
						break;
						
						//storeNumericField(f, intColVal, colType);
						//break;
					}
					case CalpontSystemCatalog::DOUBLE:
					{
						double dl = row.getDoubleField(s);
						if (dl == std::numeric_limits<double>::infinity())
							continue;
						Field_double* f2 = (Field_double*)*f;
						// bug 3483, reserve enough space for the longest double value
						// -1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and
						// 2.2250738585072014E-308 to 1.7976931348623157E+308. 
						(*f)->field_length = 310;
						//double double_val = *(double*)(&value);
						//f2->store(double_val);
						f2->store(dl);
						if ((*f)->null_ptr)
							*(*f)->null_ptr &= ~(*f)->null_bit;	
						break;
						

						//int64_t* icvp = (int64_t*)&dl;
						//intColVal = *icvp;
						//storeNumericField(f, intColVal, colType);
						//break;
					}
					case CalpontSystemCatalog::DECIMAL:
					{
						intColVal = row.getIntField(s);
						storeNumericField(f, intColVal, colType);
						break;
					}
					default:	// treat as int64
					{
						intColVal = row.getUintField<8>(s);
						storeNumericField(f, intColVal, colType);
						break;
					}
				}
			}
		}
		/** table mode */
		else	
		{
			const TableBand::VBA& cols = ti.tpl_scan_ctx->rowData.getColumns();
			for (int s = 0; s < num_attr; s++, f++)
			{
				CalpontSystemCatalog::ColType colType = colTypes[s];
				//This col is going to be written
				bitmap_set_bit(ti.msTablePtr->write_set, (*f)->field_index);

				if(cols[s]->isNullColumn())
					continue;
								
				if (cols[s]->getColumnType() == TableColumn::UINT64)
				{
					bool isNull = true;
					intColVal = 0;
					try {
						intColVal = (*cols[s]->getIntValues())[ti.tpl_scan_ctx->rowsreturned]; //colU64Data.getValues().at(ntplsch->rowsreturned);
						isNull = false;
					} catch (...) {
					}
					
					// colType should already been filled in rnd_init			
					switch (colType.colDataType)
					{
						case (CalpontSystemCatalog::DATE):
						{
							//check if column is null
							if (isNull || intColVal == DATENULL)
								continue;
							if ((*f)->null_ptr)
								*(*f)->null_ptr &= ~(*f)->null_bit;		
							DataConvert::dateToString(intColVal, tmp, 255);
							Field_varstring* f2 = (Field_varstring*)*f;
							f2->store(tmp, strlen(tmp), f2->charset());		
							break;											
						}
						case (CalpontSystemCatalog::DATETIME):
						{
							//check if column is null
							if (isNull || (uint64_t)intColVal == DATETIMENULL)
								continue;
							if ((*f)->null_ptr)
								*(*f)->null_ptr &= ~(*f)->null_bit;			
							DataConvert::datetimeToString(intColVal, tmp, 255);
							Field_varstring* f2 = (Field_varstring*)*f;
							f2->store(tmp, strlen(tmp), f2->charset());					
							break;								
						}
						case (CalpontSystemCatalog::CHAR):
						{
							if (colType.colWidth == 1)
							{
								if (isNull || (uint64_t)intColVal == CHAR1NULL)
									continue;
							}
							else if ( colType.colWidth == 2 )
							{
								if (isNull || (uint64_t)intColVal == CHAR2NULL)
									continue;
							}
							else if ( ( colType.colWidth < 5 ) && ( colType.colWidth > 2 ) )
							{
								if (isNull || (uint64_t)intColVal == CHAR4NULL)
									continue;
							}
							else
							{
								if (isNull || (uint64_t)intColVal == CHAR8NULL)
									continue;
							}
							if ((*f)->null_ptr)
								*(*f)->null_ptr &= ~(*f)->null_bit;		
							
							if (intColVal == 0)	
							{
								//TODO: do we want to set NULL for emtpy string?
								continue;
							}
							else
							{
								Field_varstring* f2 = (Field_varstring*)*f;
								//make sure we don't send strlen off into the weeds...
								memcpy(tmp, &intColVal, 8);
								tmp[8] = 0;
								f2->store(tmp, strlen(tmp), f2->charset()); 
							}
							break;
						}	
						case	(CalpontSystemCatalog::VARCHAR):
						{
							if (colType.colWidth == 1)
							{
								if (isNull || (uint64_t)intColVal == CHAR2NULL)
									continue;
							}
							else if ( ( colType.colWidth < 4 ) && ( colType.colWidth > 1 ) )
							{
								if (isNull || (uint64_t)intColVal == CHAR4NULL)
									continue;
							}
							else
							{
								if (isNull || (uint64_t)intColVal == CHAR8NULL)
									continue;
							}
							if ((*f)->null_ptr)
								*(*f)->null_ptr &= ~(*f)->null_bit;		
							if (intColVal == 0)
							{
								//TODO: do we want to set NULL for emtpy string?
								if ((*f)->null_ptr)
									*(*f)->null_ptr |= (*f)->null_bit;
							}
							else
							{
								Field_varstring* f2 = (Field_varstring*)*f;
								//make sure we don't send strlen off into the weeds...
								memcpy(tmp, &intColVal, 8);
								tmp[8] = 0;
								f2->store(tmp, strlen(tmp), f2->charset()); 
							}
							break;						
						}						 
						case	(CalpontSystemCatalog::VARBINARY):
						{
							if (colType.colWidth == 1)
							{
								if (isNull || (uint64_t)intColVal == CHAR2NULL)
									continue;
							}
							else if ( ( colType.colWidth < 4 ) && ( colType.colWidth > 1 ) )
							{
								if (isNull || (uint64_t)intColVal == CHAR4NULL)
									continue;
							}
							else
							{
								if (isNull || (uint64_t)intColVal == CHAR8NULL)
									continue;
							}
							if ((*f)->null_ptr)
								*(*f)->null_ptr &= ~(*f)->null_bit;		
							if (intColVal == 0)
							{
								//TODO: do we want to set NULL for emtpy string?
								if ((*f)->null_ptr)
									*(*f)->null_ptr |= (*f)->null_bit;
							}
							else
							{
								Field_varstring* f2 = (Field_varstring*)*f;
								//FIXME: store the length, not strlen!
								//make sure we don't send strlen off into the weeds...
								memcpy(tmp, &intColVal, 8);
								tmp[8] = 0;
								f2->store(tmp, strlen(tmp), f2->charset()); 
							}
							break;						
						}						 
						case CalpontSystemCatalog::INT:
						{
							//check if column is null
							if (isNull || (uint64_t)intColVal == INTNULL)
									continue;
							int32_t ival = static_cast<int32_t>(intColVal);
							storeNumericField(f, ival, colType);
							break;																			
						}
						case CalpontSystemCatalog::SMALLINT:
						{
							//check if column is null
							if (isNull || (uint64_t)intColVal == SMALLINTNULL)
									continue;
							int16_t ival = static_cast<int16_t>(intColVal);
							storeNumericField(f, ival, colType);
							break;
						}
						case CalpontSystemCatalog::TINYINT:
						{
							//check if column is null
							if (isNull || (uint64_t)intColVal == TINYINTNULL)
									continue;
							int8_t ival = static_cast<int8_t>(intColVal);
							storeNumericField(f, ival, colType);
							break;				
						}
						// @bug 3563. add decimal handling
						case CalpontSystemCatalog::DECIMAL:
						{
							int64_t ival = 0;
							if (colType.colWidth == 1)
							{
								if (isNull || (uint64_t)intColVal == TINYINTNULL)
									continue;
								ival = static_cast<int8_t>(intColVal);
							}
							else if (colType.colWidth == 2)
							{
								if (isNull || (uint64_t)intColVal == SMALLINTNULL)
									continue;
								ival = static_cast<int16_t>(intColVal);
							}
							else if (colType.colWidth == 4)
							{
								if (isNull || (uint64_t)intColVal == INTNULL)
									continue;
								ival = static_cast<int32_t>(intColVal);
							}
							else
							{
								if (isNull || (uint64_t)intColVal == BIGINTNULL)
									continue;
								ival = static_cast<int64_t>(intColVal);
							}
							storeNumericField(f, ival, colType);
							break;
						}
						default:
						{
							//check if column is null
							if (isNull || (uint64_t)intColVal == BIGINTNULL)
								continue;
							storeNumericField(f, intColVal, colType);
							break;
						}
					}
				}
				else // (cols[colPos]->getColumnType() == TableColumn::STRING)
				{
					bool isNull = true;
					try {
						stringColVal = (*cols[s]->getStrValues())[ti.tpl_scan_ctx->rowsreturned]; //colStrData.getValues().at(ntplsch->rowsreturned);
						isNull = false;
					} catch (...) {
					}
					//TODO: check if column is null
					if (isNull || stringColVal == CPNULLSTRMARK || strlen(stringColVal.c_str()) == 0)
						continue;

					if ((*f)->null_ptr)
						*(*f)->null_ptr &= ~(*f)->null_bit;		
					Field_varstring* f2 = (Field_varstring*)*f;
					f2->store(stringColVal.c_str(), strlen(stringColVal.c_str()), f2->charset());
				}
			}
		}
	
		ti.tpl_scan_ctx->rowsreturned++;
		ti.c++;
#ifdef INFINIDB_DEBUG
		if ((ti.c % 1000000) == 0)
			cerr << "fetchNextRow so far table " << ti.msTablePtr->s->table_name.str << " rows = " << ti.c << endl;
#endif
		ti.moreRows = true;
		rc = 0;	
	} 
	else if (sm_stat == sm::SQL_NOT_FOUND)
	{
		IDEBUG( cerr << "fetchNextRow done for table " << ti.msTablePtr->s->table_name.str << " rows = " << ti.c << endl );
		ti.c = 0;
		ti.moreRows = false;
		rc = HA_ERR_END_OF_FILE;
	}
	else if (sm_stat == sm::CALPONT_INTERNAL_ERROR)
	{
		ti.moreRows = false;
		rc = HA_ERR_INTERNAL_ERROR;
		ci->rc = rc;
	}
	else if (sm_stat == sm::SQL_KILLED)
	{
		// query was aborted by the user. treat it the same as limit query. close
		// connection after rnd_close.
		ti.c = 0;
		ti.moreRows = false;
		rc = HA_ERR_END_OF_FILE;
		current_thd->infinidb_vtable.has_limit = true;
		ci->rc = rc;
	}
	else 
	{
		ti.moreRows = false;
		rc = sm_stat;
		ci->rc = rc;
	}

	return rc;	
}

void makeUpdateScalarJoin(const ParseTree* n, void* obj)
{
	TreeNode *tn = n->data();
	SimpleFilter *sf = dynamic_cast<SimpleFilter*>(tn);
	if (!sf)
		return;
	
	SimpleColumn *scLeft = dynamic_cast<SimpleColumn*>(sf->lhs());
	SimpleColumn *scRight = dynamic_cast<SimpleColumn*>(sf->rhs());
	CalpontSystemCatalog::TableAliasName* updatedTables = reinterpret_cast<CalpontSystemCatalog::TableAliasName*>(obj);
	if ( scLeft && scRight )
	{
		if ( (strcasecmp(scLeft->tableName().c_str(), updatedTables->table.c_str()) == 0 ) && (strcasecmp(scLeft->schemaName().c_str(), updatedTables->schema.c_str()) == 0) 
		&& (strcasecmp(scLeft->tableAlias().c_str(),updatedTables->alias.c_str()) == 0))
		{
			uint64_t lJoinInfo = sf->lhs()->joinInfo();
			lJoinInfo |= JOIN_SCALAR;
			//lJoinInfo |= JOIN_OUTER_SELECT;
			//lJoinInfo |= JOIN_CORRELATED;
			sf->lhs()->joinInfo(lJoinInfo);
		}
		else if ( (strcasecmp(scRight->tableName().c_str(),updatedTables->table.c_str()) == 0) && (strcasecmp(scRight->schemaName().c_str(),updatedTables->schema.c_str())==0)
		&& (strcasecmp(scRight->tableAlias().c_str(),updatedTables->alias.c_str())==0))
		{
			uint64_t rJoinInfo = sf->rhs()->joinInfo();
			rJoinInfo |= JOIN_SCALAR;
			//rJoinInfo |= JOIN_OUTER_SELECT;
			//rJoinInfo |= JOIN_CORRELATED;
			sf->rhs()->joinInfo(rJoinInfo);
		}
		else
			return;
	}
	else 
		return;
}

void makeUpdateSemiJoin(const ParseTree* n, void* obj)
{
	TreeNode *tn = n->data();
	SimpleFilter *sf = dynamic_cast<SimpleFilter*>(tn);
	if (!sf)
		return;
	
	SimpleColumn *scLeft = dynamic_cast<SimpleColumn*>(sf->lhs());
	SimpleColumn *scRight = dynamic_cast<SimpleColumn*>(sf->rhs());
	CalpontSystemCatalog::TableAliasName* updatedTables = reinterpret_cast<CalpontSystemCatalog::TableAliasName*>(obj);
	//@Bug 3279. Added a check for column filters. 
	if ( scLeft && scRight && (strcasecmp(scRight->tableAlias().c_str(),scLeft->tableAlias().c_str()) != 0))
	{
		if ( (strcasecmp(scLeft->tableName().c_str(), updatedTables->table.c_str()) == 0 ) && (strcasecmp(scLeft->schemaName().c_str(), updatedTables->schema.c_str()) == 0) 
		&& (strcasecmp(scLeft->tableAlias().c_str(),updatedTables->alias.c_str()) == 0))
		{
			uint64_t lJoinInfo = sf->lhs()->joinInfo();
			lJoinInfo |= JOIN_SEMI;
			//lJoinInfo |= JOIN_OUTER_SELECT;
			//lJoinInfo |= JOIN_CORRELATED;
			sf->lhs()->joinInfo(lJoinInfo);
		}
		else if ( (strcasecmp(scRight->tableName().c_str(),updatedTables->table.c_str()) == 0) && (strcasecmp(scRight->schemaName().c_str(),updatedTables->schema.c_str())==0)
		&& (strcasecmp(scRight->tableAlias().c_str(),updatedTables->alias.c_str())==0))
		{
			uint64_t rJoinInfo = sf->rhs()->joinInfo();
			rJoinInfo |= JOIN_SEMI;
			//rJoinInfo |= JOIN_OUTER_SELECT;
			//rJoinInfo |= JOIN_CORRELATED;
			sf->rhs()->joinInfo(rJoinInfo);
		}
		else
			return;
	}
	else 
		return;
}

uint doUpdateDelete(THD *thd)
{
	//@Bug 4387. Check BRM status before start statement.
	scoped_ptr<DBRM> dbrmp(new DBRM());
	int rc = dbrmp->isReadWrite();
	if (rc != 0 )
	{
		setError(current_thd, HA_ERR_GENERIC, "Cannot execute the statement. DBRM is read only!");
		return HA_ERR_UNSUPPORTED;
	}
			
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	LEX* lex = thd->lex;
	assert(lex != 0);
	
	// Error out DELETE on VIEW. It's currently not supported.
	// @note DELETE on VIEW works natually (for simple cases at least), but we choose to turn it off 
	// for now - ZZ.
	TABLE_LIST* tables = thd->lex->query_tables;
	for (; tables; tables= tables->next_local)
	{
		if (tables->view)
		{
			Message::Args args;
			if (((thd->lex)->sql_command == SQLCOM_UPDATE) || ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI))
				args.add("Update");
			else
				args.add("Delete");
			string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_VIEW, args);
			setError(current_thd, HA_ERR_UNSUPPORTED, emsg);
			return HA_ERR_UNSUPPORTED;
		}

#if (defined(_MSC_VER) && defined(_DEBUG)) || defined(SAFE_MUTEX)
		if ((strcmp((*tables->table->s->db_plugin)->name.str, "InfiniDB") != 0) && (strcmp((*tables->table->s->db_plugin)->name.str, "MEMORY") != 0) &&
				       (tables->table->s->table_category != TABLE_CATEGORY_TEMPORARY) )
#else
		if ((strcmp(tables->table->s->db_plugin->name.str, "InfiniDB") != 0) && (strcmp(tables->table->s->db_plugin->name.str, "MEMORY") != 0) &&
				       (tables->table->s->table_category != TABLE_CATEGORY_TEMPORARY) )
#endif
		{
			Message::Args args;
			args.add("Non Calpont table(s)");
			string emsg(IDBErrorInfo::instance()->errorMsg(ERR_DML_NOT_SUPPORT_FEATURE, args));
			setError(current_thd, HA_ERR_UNSUPPORTED, emsg);
			return HA_ERR_UNSUPPORTED;
		}
	}
	
	
	// @bug 1127. Re-construct update stmt using lex instead of using the original query.
	string dmlStmt="";
	string schemaName;
	string tableName("");
	string aliasName("");
	UpdateSqlStatement updateSqlStmt;
	ColumnAssignmentList* colAssignmentListPtr = new ColumnAssignmentList();
	bool anyFromCol = false;
	bool isFromCol = false;
	bool isFromSameTable = true;
	//@Bug 2753. the memory already freed by destructor of UpdateSqlStatement
	if (((thd->lex)->sql_command == SQLCOM_UPDATE) || ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI))
	{
		ColumnAssignment* columnAssignmentPtr;
		Item_field *item;
		TABLE_LIST* table_ptr = thd->lex->select_lex.get_table_list();
	  List_iterator_fast<Item> field_it(thd->lex->select_lex.item_list);
	  List_iterator_fast<Item> value_it(thd->lex->value_list);
	  dmlStmt += "update ";
	  uint cnt = 0;
  
	  for (; table_ptr; table_ptr= table_ptr->next_leaf)
	  {
		dmlStmt += string(table_ptr->table_name);
	  	if (table_ptr->next_leaf)
	  		dmlStmt += ", ";
	  }
  
  	dmlStmt += " set ";   
  
		while ((item= (Item_field *) field_it++))
	  {
	  	cnt++;
	  	dmlStmt += string(item->name) + "=";
		
		string tmpTableName = bestTableName(item);
		aliasName = item->table_name;
		if (strcasecmp(tableName.c_str(), "") == 0)
		{
			tableName = tmpTableName;
		}
		else if (strcasecmp(tableName.c_str(), tmpTableName.c_str()) != 0)
		{
			//@ Bug3326 error out for multi table update
			string emsg(IDBErrorInfo::instance()->errorMsg(ERR_UPDATE_NOT_SUPPORT_FEATURE));
			thd->main_da.set_error_status(thd, HA_ERR_UNSUPPORTED, emsg.c_str());
			ci->rc = -1;
			thd->row_count_func = 0;	
			return -1;
		}
		schemaName = string(item->db_name);
		columnAssignmentPtr = new ColumnAssignment();
		columnAssignmentPtr->fColumn = string(item->name);
		columnAssignmentPtr->fOperator = "=";
		columnAssignmentPtr->fFuncScale = 0;
	    Item *value= value_it++;
		if (value->type() ==  Item::STRING_ITEM)
		{
			//@Bug 2587 use val_str to replace value->name to get rid of 255 limit
            String val, *str;
            str = value->val_str(&val);
            dmlStmt += "'" + string(str->c_ptr()) + "'";
			columnAssignmentPtr->fScalarExpression =  string(str->c_ptr()) ;
			columnAssignmentPtr->fFromCol = false;
        }
		else if ( value->type() ==  Item::VARBIN_ITEM )
		{	
			String val, *str;
            str = value->val_str(&val);
            dmlStmt += "'" + string(str->c_ptr()) + "'";
			columnAssignmentPtr->fScalarExpression =  string(str->c_ptr()) ;
			columnAssignmentPtr->fFromCol = false;
		}
		else if ( value->type() ==  Item::FUNC_ITEM )
		{
			//Bug 2092 handle negative values
			Item_func* ifp = (Item_func*)value;
			if (ifp->result_type() == DECIMAL_RESULT)
				columnAssignmentPtr->fFuncScale = ifp->decimals; //decimal scale

			vector <Item_field*> tmpVec;
			bool hasNonSupportItem = false;
			uint16_t parseInfo = 0;
			parse_item(ifp, tmpVec, hasNonSupportItem, parseInfo);				
			// const f&e evaluate here. @bug3513. Rule out special functions that takes
			// no argument but needs to be sent to the back end to process. Like rand(),
			// sysdate() etc.
			if (!hasNonSupportItem && !cal_impl_if::nonConstFunc(ifp) && tmpVec.size() == 0)
			{
				gp_walk_info gwi;
				gwi.thd = thd;
				SRCP srcp(buildReturnedColumn(value, gwi, gwi.fatalParseError));
				ConstantColumn *constCol = dynamic_cast<ConstantColumn*>(srcp.get());
				if (constCol ) 
				{
					columnAssignmentPtr->fScalarExpression  = constCol->constval();
					isFromCol = false;
					columnAssignmentPtr->fFromCol = false;
				}
				else
				{
					isFromCol = true;
					anyFromCol = true;
					columnAssignmentPtr->fFromCol = true;
					
				}
			}
			else
			{
				isFromCol = true;
				anyFromCol = true;
				columnAssignmentPtr->fFromCol = true;
			}
			
			if ( isFromCol )
			{
				string sectableName("");
				string secschemaName ("");
				for ( unsigned i = 0; i < tmpVec.size(); i++ )
				{
					sectableName = bestTableName(tmpVec[i]);
					if ( tmpVec[i]->db_name )
					{
						secschemaName = string(tmpVec[i]->db_name);
					}
					if ( (strcasecmp(tableName.c_str(), sectableName.c_str()) != 0) ||
						(strcasecmp(schemaName.c_str(), secschemaName.c_str()) != 0))
					{
						isFromSameTable = false;
						break;
					}
				}
			}
		}
		else if ( value->type() ==  Item::INT_ITEM ) 
		{
			std::ostringstream oss;
			oss << value->val_int();
			dmlStmt += oss.str();
			columnAssignmentPtr->fScalarExpression = oss.str();
			columnAssignmentPtr->fFromCol = false;
		}
	    else if ( value->type() ==  Item::FIELD_ITEM)  
		{
			isFromCol = true;
			anyFromCol = true;
			columnAssignmentPtr->fFromCol = true;
			Item_field* setIt = reinterpret_cast<Item_field*> (value);
			string sectableName = string(setIt->table_name);
			if ( setIt->db_name ) //derived table
			{
				string secschemaName = string(setIt->db_name);
				if ( (strcasecmp(tableName.c_str(), sectableName.c_str()) != 0) || (strcasecmp(schemaName.c_str(), secschemaName.c_str()) != 0))
				{
					isFromSameTable = false;
				}
			}
			else
			{
				isFromSameTable = false;
			}		
		}
		else if ( value->type() ==  Item::NULL_ITEM )
		{
			dmlStmt += "NULL";	
			columnAssignmentPtr->fScalarExpression = "NULL";
			columnAssignmentPtr->fFromCol = false;
		}
		else if ( value->type() == Item::SUBSELECT_ITEM )
		{
			isFromCol = true;
			anyFromCol = true;
			columnAssignmentPtr->fFromCol = true;
			Item_field* setIt = reinterpret_cast<Item_field*> (value);
			string sectableName = string(setIt->table_name);
			string secschemaName = string(setIt->db_name);
			if ( (strcasecmp(tableName.c_str(), sectableName.c_str()) != 0) || (strcasecmp(schemaName.c_str(), secschemaName.c_str()) != 0))
			{
				isFromSameTable = false;
			}
		}
	    else 
		{
			String val, *str;
            str = value->val_str(&val);
			dmlStmt += string(str->c_ptr());
			columnAssignmentPtr->fScalarExpression = string(str->c_ptr());
			columnAssignmentPtr->fFromCol = false;
		}	
		
		colAssignmentListPtr->push_back ( columnAssignmentPtr );
	    if (cnt < thd->lex->select_lex.item_list.elements)
	    	dmlStmt += ", ";    
	  }	
	}
	else
	{
		dmlStmt = string( thd->query );
	}

	uint32_t sessionID = tid2sid(thd->thread_id);
	CalpontDMLPackage* pDMLPackage = 0;
	dmlStmt += ";";	
	IDEBUG( cout << "STMT: " << dmlStmt << " and sessionID " << sessionID <<  endl ); 
	VendorDMLStatement dmlStatement(dmlStmt, sessionID);
	if (( (thd->lex)->sql_command == SQLCOM_UPDATE ) || ( (thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) )
		dmlStatement.set_DMLStatementType( DML_UPDATE );
	else
		dmlStatement.set_DMLStatementType( DML_DELETE );
		
	TableName* qualifiedTablName = new TableName();
	
	UpdateSqlStatement updateStmt;
	//@Bug 2753. To make sure the momory is freed.
	updateStmt.fColAssignmentListPtr = colAssignmentListPtr;
	TABLE_LIST *first_table = 0;
	if (( (thd->lex)->sql_command == SQLCOM_UPDATE ) || ( (thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) )
	{
		qualifiedTablName->fName = tableName;
		qualifiedTablName->fSchema = schemaName;
		updateStmt.fNamePtr = qualifiedTablName;
		pDMLPackage = CalpontDMLFactory::makeCalpontUpdatePackageFromMysqlBuffer( dmlStatement, updateStmt );
	}
	else
	{
		first_table= (TABLE_LIST*) thd->lex->select_lex.table_list.first;
		schemaName = first_table->table->s->db.str;
		tableName = first_table->table->s->table_name.str;
		qualifiedTablName->fName = tableName;
		qualifiedTablName->fSchema = schemaName;
		pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(dmlStatement);
	}
	if (!pDMLPackage)
	{
		string emsg("Fatal parse error in vtable mode in DMLParser ");
		setError(thd, HA_ERR_UNSUPPORTED, emsg);
		return HA_ERR_UNSUPPORTED;
	}
	pDMLPackage->set_TableName(tableName);
		
	pDMLPackage->set_SchemaName(schemaName);
	
	//pDMLPackage->set_IsFromCol( anyFromCol );
	pDMLPackage->set_IsFromCol( true );
	//cout << " setting 	isFromCol to " << isFromCol << endl;
	string origStmt( thd->query );
	origStmt += ";"; 
	pDMLPackage->set_SQLStatement( origStmt );
	
	//Save the item list
	List<Item> items = (thd->lex->select_lex.item_list);
	thd->lex->select_lex.item_list = thd->lex->value_list;
	if ( anyFromCol )
	{
	//	List<Item> items = (thd->lex->select_lex.item_list); //For join to use
		thd->lex->select_lex.item_list = thd->lex->value_list;
		//save the item_list to value_list to set join.
	//	thd->lex->value_list = items;
		
	}
	SELECT_LEX select_lex = lex->select_lex;
	
	//@Bug 2808 Error out on order by or limit clause
	if (( select_lex.explicit_limit ) || ( select_lex.order_list.elements != 0 ) )
	{
		string emsg("DML Statement with a limit or order by clause is not currently supported.");
		thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, emsg.c_str());
		ci->rc = 1;
		thd->row_count_func = 0;	
		return 0;
	}
	
	execplan::CalpontSelectExecutionPlan *updateCP = pDMLPackage->get_ExecutionPlan();
       THD::infinidb_state origState = thd->infinidb_vtable.vtable_state;

	//if (( (thd->lex)->sql_command == SQLCOM_UPDATE ) || ( (thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) )
	{
		gp_walk_info gwi;
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_CREATE_VTABLE;
		gwi.thd = thd;
		//updateCP->subType (CalpontSelectExecutionPlan::SINGLEROW_SUBS); //set scalar
		updateCP->subType (CalpontSelectExecutionPlan::SELECT_SUBS);
		//@Bug 2975.
		SessionManager sm;
		BRM::TxnID txnID;
		txnID = sm.getTxnID(sessionID);
		if (!txnID.valid)
		{
			txnID.id = 0;
			txnID.valid = true;
		}
		CalpontSystemCatalog::SCN verID;
		verID = sm.verID();
		
		updateCP->txnID(txnID.id);
		updateCP->verID(verID);
		updateCP->sessionID(sessionID);
		
		gwi.clauseType = WHERE;
		if (getSelectPlan(gwi, select_lex, *updateCP) != 0) //@Bug 3030 Modify the error message for unsupported functions
		{
			if (thd->infinidb_vtable.isUpdateWithDerive)
	                {
	                        // @bug 4457. MySQL inconsistence! for some queries, some structures are only available
				// in the derived_tables_processing phase. So by pass the phase for DML only when the
				// execution plan can not be successfully generated. recover lex before returning;
                               thd->lex->select_lex.item_list = items;
                               thd->infinidb_vtable.vtable_state = origState;
                               return 0;
                       }

			//check different error code
			Message::Args args;
			args.add(gwi.parseErrorText);
			string emsg(IDBErrorInfo::instance()->errorMsg(ERR_DML_NOT_SUPPORT_FEATURE, args));
			thd->main_da.set_error_status(thd, HA_ERR_UNSUPPORTED, emsg.c_str());
			ci->rc = -1;
			thd->row_count_func = 0;	
			return -1;
			
		}
			
		//set the large side by putting the updated table at the first
		CalpontSelectExecutionPlan::TableList tbList = updateCP->tableList();
		
		CalpontSelectExecutionPlan::TableList::iterator iter = tbList.begin();
		bool notFirst = false;
		while ( iter != tbList.end() )
		{
			if ( ( iter != tbList.begin() ) && (iter->schema == schemaName) && ( iter->table == tableName ) && ( iter->alias == aliasName ) )
			{
				notFirst = true;
				tbList.erase(iter);
				break;
			}
			iter++;
		}
		if ( notFirst )
		{
			//error out for now. Wait for scalar join.
/*			Message::Args args;
			string emsg(IDBErrorInfo::instance()->errorMsg(ERR_COLUMN_EQ_DIFFTABLE_COLUMN, args));
			thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, emsg.c_str());
			ci->rc = 1;
			thd->row_count_func = 0;	
			return 0;
*/
			CalpontSystemCatalog::TableAliasName tn = make_aliastable(schemaName, tableName, aliasName);
			iter = tbList.begin();
			tbList.insert( iter, 1, tn ); 
		}
		updateCP->tableList( tbList );
		updateCP->overrideLargeSideEstimate( true );
		//loop through returnedcols to find out constant columns
		CalpontSelectExecutionPlan::ReturnedColumnList returnedCols = updateCP->returnedCols();
		CalpontSelectExecutionPlan::ReturnedColumnList::iterator coliter = returnedCols.begin();
		while ( coliter != returnedCols.end() )
		{
			ConstantColumn *returnCol = dynamic_cast<ConstantColumn*>((*coliter).get());
			if (returnCol ) 
			{
				returnedCols.erase(coliter); 
				coliter = returnedCols.begin();
				//cout << "constant column " << endl;
			}
			else
				coliter++;
		}
		if ((updateCP->columnMap()).empty())
			throw runtime_error ("column map is empty!");
		if (returnedCols.empty())
			returnedCols.push_back((updateCP->columnMap()).begin()->second);
		updateCP->returnedCols( returnedCols );
		if (( (thd->lex)->sql_command == SQLCOM_UPDATE ) || ( (thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) )
		{
			const ParseTree* ptsub = updateCP->filters();
			if ( !isFromSameTable )
			{
				//cout << "set scalar" << endl;
				//walk tree to set scalar
				if (ptsub)
					ptsub->walk(makeUpdateScalarJoin, &tbList[0] );
			}
			else
			{
				//cout << "set semi" << endl;
				if (ptsub)
					ptsub->walk(makeUpdateSemiJoin, &tbList[0] );
			}
			//cout<< "Plan is " << endl << *updateCP << endl;
			thd->lex->select_lex.item_list = items;
		}
		//cout<< "Plan is " << endl << *updateCP << endl;
	}
	
	pDMLPackage->HasFilter(true);
	
	ByteStream bytestream;
	bytestream << sessionID;
	pDMLPackage->write(bytestream);
	delete pDMLPackage;

	if ( !ci->dmlProc )
	{
		ci->dmlProc = new MessageQueueClient("DMLProc");
		//cout << "test007: doUpdateDelete use new DMLProc client " <<ci->dmlProc << " for session " << sessionID << endl;
	}
	ByteStream::byte b;
	ByteStream::octbyte rows;
	std::string errorMsg;
	long long dmlRowCount = 0;
	if ( thd->killed > 0 )
	{
		return 0;
	}
	try
	{
		ci->dmlProc->write(bytestream);
		//cout << "test007:update use dml client " << ci->dmlProc << endl;
		bytestream = ci->dmlProc->read();
		if ( bytestream.length() == 0 )
		{
			b = 1;
			errorMsg = "Lost connection to DMLProc";
			ci->dmlProc = new MessageQueueClient("DMLProc");
		}	
		else {
			bytestream >> b;
			bytestream >> rows;
			bytestream >> errorMsg;
		}		
		
		dmlRowCount = rows;
	}
	catch (runtime_error&)
	{
		thd->main_da.can_overwrite_status = true;

		errorMsg = "Lost connection to DMLProc";
		b = 1;
		ci->dmlProc = new MessageQueueClient("DMLProc");
	}
	catch ( ... )
	{
		b = 1;
		errorMsg =  "Unknown error caught";
	}
	
	// If autocommit is on then go ahead and tell the engine to commit the transaction
	//@Bug 1960 If error occurs, the commit is just to release the active transaction.
	//@Bug 2241. Rollback transaction when it failed
	//@Bug 2818 handle ctrl-c
	if ( thd->killed > 0 )
	{
		std::string command("ROLLBACK");
		VendorDMLStatement cmdStmt(command, DML_COMMAND, sessionID);
		CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
		ByteStream bytestream;
		bytestream << static_cast<u_int32_t>(sessionID);
		pDMLPackage->write(bytestream);
		delete pDMLPackage;
			
		ByteStream::byte bc;
		try
		{
				ci->dmlProc->write(bytestream);
				bytestream = ci->dmlProc->read();
				bytestream >> bc;
		}
		catch (runtime_error&)
		{
        	errorMsg = "Lost connection to DMLProc";
			b = 1;
			ci->dmlProc = new MessageQueueClient("DMLProc");
		}
		catch (...)
		{
            errorMsg = "Unknown error caught";
			b = 1;
		}
		return 0;
	}
	
	//@Bug 4605. If error, always rollback.
	if ( b != dmlpackageprocessor::DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR )
	{		
		std::string command;
		if ((!(current_thd->options & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) && (( b == 0 ) || (b == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING)) )
			command = "COMMIT";
		else if (( b != 0 ) && (b != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING) ) 
			command = "ROLLBACK";
		else 
			command = "";
			
		if ( command != "")
		{
			VendorDMLStatement cmdStmt(command, DML_COMMAND, sessionID);
			CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
			ByteStream bytestream;
			bytestream << static_cast<u_int32_t>(sessionID);
			pDMLPackage->write(bytestream);
			delete pDMLPackage;
			
			ByteStream::byte bc;
			try
			{
				ci->dmlProc->write(bytestream);
				//cout << "test007:commit use dml client " << ci->dmlProc << endl;
				bytestream = ci->dmlProc->read();
				bytestream >> bc;
			}
			catch (runtime_error&)
			{
				errorMsg = "Lost connection to DMLProc";
				b = 1;
				ci->dmlProc = new MessageQueueClient("DMLProc");
			}
			catch (...)
			{
				errorMsg = "Unknown error caught";
				b = 1;
			}
		}
	}
	
	//@Bug 2241 Display an error message to user
	
	if ( ( b != 0 ) && (b != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING))
	{	
		//@Bug 2540. Set error status instead of warning	
		thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, errorMsg.c_str());
		ci->rc = b;
		thd->row_count_func = 0;	
		//cout << " error status " << ci->rc << endl;
	}
	else
	{
		thd->row_count_func = dmlRowCount;
		//cout << " error status " << ci->rc << " and rowcount = " << dmlRowCount << endl;
	}
	if ( b == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING )
	{
		//string errmsg ("Out of range value detected. Please check Calpont Syntax Guide for supported data range." );
		push_warning(thd, MYSQL_ERROR::WARN_LEVEL_WARN, 9999, errorMsg.c_str());
	}

	// @bug 4027. comment out the following because this will cause mysql
	// kernel assertion failure. not sure why this is here in the first place.	
//	if (thd->derived_tables)
//	{
//		thd->main_da.can_overwrite_status= TRUE;
//		thd->main_da.set_ok_status( thd, dmlRowCount, 0, "");
//	}	
	
	return 0;
}

} //anon namespace

extern "C"
{
#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calgetstats(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	THD* thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	unsigned long l = ci->queryStats.size();
	if (l == 0)
	{
		*is_null = 1;
		return 0;
	}
	if (l > 255) l = 255;
	memcpy(result, ci->queryStats.c_str(), l);
	*length = l;
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calgetstats_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 0)
	{
		strcpy(message,"CALGETSTATS() takes no arguments");
		return 1;
	}
	initid->maybe_null = 1;
	initid->max_length = 255;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calgetstats_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long calsettrace(UDF_INIT* initid, UDF_ARGS* args,
							char* is_null, char* error)
{
	THD* thd = current_thd;

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	

	long long oldTrace = ci->traceFlags;
	ci->traceFlags = (uint32_t)(*((long long*)args->args[0]));
	// keep the vtablemode bit
	ci->traceFlags |= (oldTrace & CalpontSelectExecutionPlan::TRACE_TUPLE_OFF);
	ci->traceFlags |= (oldTrace & CalpontSelectExecutionPlan::TRACE_TUPLE_AUTOSWITCH);
	return oldTrace;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calsettrace_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1 || args->arg_type[0] != INT_RESULT)
	{
		strcpy(message,"CALSETTRACE() requires one INTEGER argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calsettrace_deinit(UDF_INIT* initid)
{
}

#if 0
my_bool calvtablemode_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1 || args->arg_type[0] != INT_RESULT)
	{
		strcpy(message,"CALVTABLEMODE() requires one INTEGER argument");
		return 1;
	}
	return 0;
}

long long calvtablemode(UDF_INIT* initid, UDF_ARGS* args,
							char* is_null, char* error)
{
	// deprecated function. replaced by infinidb_vtable_mode system variable.
	THD* thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	
	long long ret = 1;
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
		ret = 0;
	else if (thd->infinidb_vtable.autoswitch)
		ret = 2;		
	
	switch ((uint32_t)(*((long long*)args->args[0])))
	{
		case 0:
			// vtable off
			thd->infinidb_vtable.vtable_state = THD::INFINIDB_DISABLE_VTABLE;
			ci->traceFlags |= CalpontSelectExecutionPlan::TRACE_TUPLE_OFF;
			break;
		case 1:
			// vtable on
			if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
				thd->infinidb_vtable.vtable_state = THD::INFINIDB_INIT;
			thd->infinidb_vtable.autoswitch = false;
			ci->traceFlags &= ~CalpontSelectExecutionPlan::TRACE_TUPLE_OFF;
			ci->traceFlags &= ~CalpontSelectExecutionPlan::TRACE_TUPLE_AUTOSWITCH;			
			break;
		case 2:
			// auto switch
			if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
				thd->infinidb_vtable.vtable_state = THD::INFINIDB_INIT;
			thd->infinidb_vtable.autoswitch = true;
			ci->traceFlags |= CalpontSelectExecutionPlan::TRACE_TUPLE_AUTOSWITCH;
			break;
		default:
			*error = 1;		
	}
	
	return ret;
}

void calvtablemode_deinit(UDF_INIT* initid)
{
}
#endif

#define MAXSTRINGLENGTH 50

const char* PmSmallSideMaxMemory = "pmmaxmemorysmallside";

const char* SetParmsPrelude = "Updated ";
const char* SetParmsError = "Invalid parameter: ";
const char* InvalidParmSize = "Invalid parameter size: Input value cannot be larger than ";

const size_t Plen = strlen(SetParmsPrelude);
const size_t Elen = strlen(SetParmsError);

const char* invalidParmSizeMessage(uint64_t size, size_t& len)
{
	ostringstream os;
	os << InvalidParmSize << size;
	len = os.str().length();
	return os.str().c_str();
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calsetparms(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	char parameter[MAXSTRINGLENGTH];
	char valuestr[MAXSTRINGLENGTH];
	size_t plen = args->lengths[0];
	size_t vlen = args->lengths[1];

  	memcpy(parameter,args->args[0], plen);
  	memcpy(valuestr,args->args[1], vlen);

	parameter[plen] = '\0';
	valuestr[vlen] = '\0';

	uint64_t value = Config::uFromText(valuestr);

	THD* thd = current_thd;
	uint32_t sessionID = tid2sid(thd->thread_id);

	const char* msg = SetParmsError;
	size_t mlen = Elen;
	bool includeInput = true;

	string pstr(parameter);
	algorithm::to_lower(pstr);
	if (pstr == PmSmallSideMaxMemory)
	{
	  joblist::ResourceManager rm;
		if (rm.getHjTotalUmMaxMemorySmallSide() >= value)
		{
			rmParms.push_back(RMParam(sessionID, execplan::PMSMALLSIDEMEMORY, value));

			msg = SetParmsPrelude;
			mlen = Plen;
		}
		else
		{
			msg = invalidParmSizeMessage(rm.getHjTotalUmMaxMemorySmallSide(), mlen);
			includeInput = false;
		}
	}

	memcpy(result, msg, mlen);
	if (includeInput)
	{
		memcpy(result + mlen , parameter, plen);
		mlen += plen;
		memcpy(result + mlen++, " ", 1);
		memcpy(result + mlen, valuestr, vlen);
		*length = mlen + vlen;
	}
	else 
		*length = mlen;
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calsetparms_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 2 || args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT)
	{
		strcpy(message,"CALSETPARMS() requires two string arguments");
		return 1;
	}
	initid->max_length=MAXSTRINGLENGTH;
	
	char valuestr[MAXSTRINGLENGTH];
	size_t vlen = args->lengths[1];

  	memcpy(valuestr,args->args[1], vlen--);

	for (size_t i = 0; i < vlen; ++i)
		if (!isdigit(valuestr[i]))
		{
			strcpy(message,"CALSETPARMS() second argument must be numeric or end in G, M or K");
			return 1;
		}

	if (!isdigit(valuestr[vlen]))
	{
		switch (valuestr[vlen])
		{
		case 'G':
		case 'g':
		case 'M':
		case 'm':
		case 'K':
		case 'k':
		case '\0':
			break;
		default:
			strcpy(message,"CALSETPARMS() second argument must be numeric or end in G, M or K");
			return 1;
		}
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calsetparms_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calviewtablelock_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count == 2 && (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT))
	{
		strcpy(message,"CALVIEWTABLELOCK() requires two string arguments");
		return 1;
	}
	else if ((args->arg_count == 1) && (args->arg_type[0] != STRING_RESULT ) )
	{
		strcpy(message,"CALVIEWTABLELOCK() requires one string argument");
		return 1;
	}
	else if (args->arg_count > 2 )
	{
		strcpy(message,"CALVIEWTABLELOCK() takes one or two arguments only");
		return 1;
	}
	else if (args->arg_count == 0 )
	{
		strcpy(message,"CALVIEWTABLELOCK() requires at least one argument");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calviewtablelock(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	THD* thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);		CalpontSystemCatalog::TableName tableName;
	if ( args->arg_count == 2 )
	{
		tableName.schema = args->args[0];
		tableName.table = args->args[1];
	}
	else if ( args->arg_count == 1 )
	{
		tableName.table = args->args[0];
		if (thd->db)
			tableName.schema = thd->db;
		else
		{
			string msg("No schema information provided");
			memcpy(result,msg.c_str(), msg.length());
			*length = msg.length();
			return result;
		}
	}
	if ( !ci->dmlProc )
	{
		ci->dmlProc = new MessageQueueClient("DMLProc");
		//cout << "test007: calviewtablelock use new DMLProc client " <<ci->dmlProc << endl;		
	}

	string lockinfo = ha_calpont_impl_viewtablelock(*ci, tableName);
	
	memcpy(result,lockinfo.c_str(), lockinfo.length());
	*length = lockinfo.length();
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calviewtablelock_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calcleartablelock_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count == 2 && (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT))
	{
		strcpy(message,"CALCLEARTABLELOCK() requires two string arguments");
		return 1;
	}
	else if ((args->arg_count == 1) && (args->arg_type[0] != STRING_RESULT ))
	{
		strcpy(message,"CALCLEARTABLELOCK() requires one string argument");
		return 1;
	}
	else if (args->arg_count > 2 )
	{
		strcpy(message,"CALCLEARTABLELOCK() takes one or two arguments only");
		return 1;
	}
	else if (args->arg_count == 0 )
	{
		strcpy(message,"CALCLEARTABLELOCK() requires at least one argument");
		return 1;
	}
	initid->maybe_null = 1;
	initid->max_length = 255;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calcleartablelock(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	THD* thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
		
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	CalpontSystemCatalog::TableName tableName;
	if ( args->arg_count == 2 )
	{
		tableName.schema = args->args[0];
		tableName.table = args->args[1];
	}
	else
	{
		tableName.table = args->args[0];
		tableName.schema = thd->db;
	}
	
	if ( !ci->dmlProc )
		ci->dmlProc = new MessageQueueClient("DMLProc");
		
	string lockinfo = ha_calpont_impl_cleartablelock(*ci, tableName);
	
	memcpy(result,lockinfo.c_str(), lockinfo.length());
	*length = lockinfo.length();
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calcleartablelock_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool callastinsertid_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count == 2 && (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT))
	{
		strcpy(message,"CALLASTINSRTID() requires two string arguments");
		return 1;
	}
	else if ((args->arg_count == 1) && (args->arg_type[0] != STRING_RESULT ) )
	{
		strcpy(message,"CALLASTINSERTID() requires one string argument");
		return 1;
	}
	else if (args->arg_count > 2 )
	{
		strcpy(message,"CALLASTINSERTID() takes one or two arguments only");
		return 1;
	}
	else if (args->arg_count == 0 )
	{
		strcpy(message,"CALLASTINSERTID() requires at least one argument");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long callastinsertid(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	THD* thd = current_thd;
		
	CalpontSystemCatalog::TableName tableName;
	long long nextVal = 0;
	if ( args->arg_count == 2 )
	{
		tableName.schema = args->args[0];
		tableName.table = args->args[1];
	}
	else if ( args->arg_count == 1 )
	{
		tableName.table = args->args[0];
		if (thd->db)
			tableName.schema = thd->db;
		else
		{
			string msg("No schema information provided");
			memcpy(result,msg.c_str(), msg.length());
			*length = msg.length();
			return nextVal;
		}
	}

	CalpontSystemCatalog* csc = CalpontSystemCatalog::makeCalpontSystemCatalog(tid2sid(thd->thread_id));
	
	nextVal = csc->nextAutoIncrValue(tableName);	
	
	if (nextVal == -2)
	{
		string msg("No such table found");
		setError(thd, HA_ERR_UNSUPPORTED, msg);
		return nextVal;
	}
	
	if (nextVal < 0)
	{
		setError(thd, HA_ERR_UNSUPPORTED, IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT));
		return nextVal;
	}
	//@Bug 3559. Return a message for table without autoincrement column.
	if (nextVal == 0)
	{
		string msg("Autoincrement does not exist for this table.");
		setError(thd, HA_ERR_UNSUPPORTED, msg);	
		return nextVal;
	}
	
	return (nextVal-1);
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void callastinsertid_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calflushcache_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long calflushcache(UDF_INIT* initid, UDF_ARGS* args,
							char* is_null, char* error)
{
	return static_cast<long long>(cacheutils::flushPrimProcCache());
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calflushcache_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 0)
	{
		strcpy(message,"CALFLUSHCACHE() takes no arguments");
		return 1;
	}

	return 0;
}

static const unsigned long TraceSize = 16 * 1024;

//mysqld will call this with only 766 bytes available in result no matter what we asked for in calgettrace_init()
// if we return a pointer that is not result, mysqld will take our pointer and use it, freeing up result
#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calgettrace(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	THD* thd = current_thd;
	const string* msgp;
	int flags = 0;
	if (args->arg_count > 0)
	{
		if (args->arg_type[0] == INT_RESULT)
		{
			flags = *reinterpret_cast<int*>(args->args[0]);
		}
	}

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	
	if (flags > 0)
		//msgp = &connMap[sessionID].extendedStats;
		msgp = &ci->extendedStats;
	else
		//msgp = &connMap[sessionID].miniStats;
		msgp = &ci->miniStats;
	unsigned long l = msgp->size();
	if (l == 0)
	{
		*is_null = 1;
		return 0;
	}
	if (l > TraceSize) l = TraceSize;
	*length = l;
	return msgp->c_str();
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calgettrace_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
#if 0
	if (args->arg_count != 0)
	{
		strcpy(message,"CALGETTRACE() takes no arguments");
		return 1;
	}
#endif
	initid->maybe_null = 1;
	initid->max_length = TraceSize;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calgettrace_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calgetversion(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	string version(idb_version);
	*length = version.size();
	memcpy(result, version.c_str(), *length);
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calgetversion_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 0)
	{
		strcpy(message,"CALGETVERSION() takes no arguments");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calgetversion_deinit(UDF_INIT* initid)
{
}

} //extern "C"

int ha_calpont_impl_open(const char *name, int mode, uint test_if_locked)
{
	IDEBUG ( cout << "ha_calpont_impl_open: " << name << ", " << mode << ", " << test_if_locked << endl );
	Config::makeConfig();
	return 0;
}

int ha_calpont_impl_close(void)
{
	IDEBUG( cout << "ha_calpont_impl_close" << endl );
	return 0;
}

int ha_calpont_impl_rnd_init(TABLE* table)
{
#ifdef DEBUG_SETENV
   string home(getenv("HOME"));
   if (!getenv("CALPONT_HOME"))
   {
      string calpontHome(home + "/Calpont/etc/");
      setenv("CALPONT_HOME", calpontHome.c_str(), 1);
   }

   if (!getenv("CALPONT_CONFIG_FILE"))
   {
      string calpontConfigFile(home + "/Calpont/etc/Calpont.xml");
      setenv("CALPONT_CONFIG_FILE", calpontConfigFile.c_str(), 1);
   }

   if (!getenv("CALPONT_CSC_IDENT"))
      setenv("CALPONT_CSC_IDENT", "dm", 1);
#endif

	IDEBUG( cout << "rnd_init for table " << table->s->table_name.str << endl );
	THD* thd = current_thd;
	
	// @bug 3005. if the table is not $vtable, then this could be a UDF defined on the connector.
	// watch this for other complications
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE && 
		  string(table->s->table_name.str).find("$vtable") != 0)
		  return 0;

	// return error is error status is already set
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_ERROR)
		return HA_ERR_INTERNAL_ERROR;
		
	// by pass the extra union trips. return 0
	if (thd->infinidb_vtable.isUnion && thd->infinidb_vtable.vtable_state == THD::INFINIDB_CREATE_VTABLE)
		return 0;
	
	// @bug 2232. Basic SP support. Error out non support sp cases.
	// @bug 3939. Only error out for sp with select. Let pass for alter table in sp.
	if (thd->infinidb_vtable.call_sp && (thd->lex)->sql_command != SQLCOM_ALTER_TABLE)
	{
		setError(thd, HA_ERR_UNSUPPORTED, "This stored procedure syntax is not supported by InfiniDB in this version");
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_ERROR;
		return HA_ERR_INTERNAL_ERROR;
	}

#ifdef SKIP_INSERT_SELECT
	if (thd->infinidb_vtable.isInsertSelect)
	{
		Message::Args args;
		args.add("Insert with Select");
		setError(thd, HA_ERR_UNSUPPORTED, (IDBErrorInfo::instance()->errorMsg(ERR_ENTERPRISE_ONLY, args)));
		return HA_ERR_UNSUPPORTED;
	}
#endif

	// mysql reads table twice for order by
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_REDO_PHASE1 ||
		thd->infinidb_vtable.vtable_state == THD::INFINIDB_ORDER_BY)	 
		return 0;
	
	if ( (thd->lex)->sql_command == SQLCOM_ALTER_TABLE )
		return 0;
		
	//Update and delete code
	if ( ((thd->lex)->sql_command == SQLCOM_UPDATE)  || ((thd->lex)->sql_command == SQLCOM_DELETE) || ((thd->lex)->sql_command == SQLCOM_DELETE_MULTI) || ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI))
		return doUpdateDelete(thd);						
	
	uint32_t sessionID = tid2sid(thd->thread_id);
	CalpontSystemCatalog* csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);	
	csc->identity(CalpontSystemCatalog::FE);
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	

	assert(ci != 0);
	if(thd->killed == THD::KILL_QUERY)
	{
		if (ci->cal_conn_hndl)
		{
			// send ExeMgr a signal before cloing the connection
			ByteStream msg;
			ByteStream::quadbyte qb = 0;
			msg << qb;
			try {
				ci->cal_conn_hndl->exeMgr->write(msg);
			}
			catch (...)
			{
				// canceling query, ignore connection failure.
			}
			sm::sm_cleanup(ci->cal_conn_hndl);
			ci->cal_conn_hndl = 0;
		}
		thd->infinidb_vtable.has_limit = false;
		return 0;
	}

	sm::tableid_t tableid = 0;
	cal_table_info ti;
	sm::cpsm_conhdl_t* hndl;
	
	// update traceFlags according to the autoswitch state
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
		ci->traceFlags |= CalpontSelectExecutionPlan::TRACE_TUPLE_OFF;
	else
		ci->traceFlags = (ci->traceFlags | CalpontSelectExecutionPlan::TRACE_TUPLE_OFF)^
										CalpontSelectExecutionPlan::TRACE_TUPLE_OFF;
								
	// table mode
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
	{  
		ti = ci->tableMap[table];
      
    // get connection handle for this table handler
		if (ti.conn_hndl == 0)
		{
			
			char sessionIDstr[80];
			snprintf(sessionIDstr, 80, "%u", sessionID);
			sessionIDstr[79] = 0;
			sm::sm_init(0, 0, 0, sessionIDstr, (void**)&ti.conn_hndl);
			ti.conn_hndl->csc	= csc;
		}
		hndl = ti.conn_hndl;
		
		try {
			ti.conn_hndl->connect();
		} catch (...) {
			setError(thd, HA_ERR_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			goto error;
		}
		
		// get filter plan for table 
		if (ti.csep == 0)
		{
			ti.csep = new CalpontSelectExecutionPlan();

			SessionManager sm;
			BRM::TxnID txnID;
			txnID = sm.getTxnID(sessionID);
			if (!txnID.valid)
			{
				txnID.id = 0;
				txnID.valid = true;
			}
			CalpontSystemCatalog::SCN verID;
			verID = sm.verID();
	
			ti.csep->txnID(txnID.id);
			ti.csep->verID(verID);
			ti.csep->sessionID(sessionID);
	
			ti.csep->traceFlags(ci->traceFlags);
			ti.msTablePtr = table;
			
			// send plan whenever rnd_init is called
			cp_get_table_plan(thd, *ti.csep, ti);
		}
			
		IDEBUG( cerr << table->s->table_name.str << " send plan:" << endl );
		IDEBUG( cerr << *ti.csep << endl );
		ByteStream msg;
		ti.csep->rmParms(rmParms);	
		
		// for ExeMgr logging sqltext. only log once for the query although multi plans may be sent
		if (ci->tableMap.size() == 1)
			ti.csep->data(thd->query);		
		
		ti.csep->serialize(msg);		
		ti.conn_hndl->exeMgr->write(msg);	
		ci->tableMap[table] = ti;		
	} 
	
	// vtable mode
	else 
	{
		//if (!ci->cal_conn_hndl || thd->infinidb_vtable.vtable_state == THD::INFINIDB_CREATE_VTABLE)
		if ( thd->infinidb_vtable.vtable_state == THD::INFINIDB_CREATE_VTABLE)
		{
			if (thd->infinidb_vtable.has_limit)
			{
				sm::sm_cleanup(ci->cal_conn_hndl);
				ci->cal_conn_hndl = 0;
				thd->infinidb_vtable.has_limit = false;
			}
			
			// if the previous query has error, re-establish the connection
			if (ci->queryState != 0)
			{
				sm::sm_cleanup(ci->cal_conn_hndl);
				ci->cal_conn_hndl = 0;
			}
			
			if (!ci->cal_conn_hndl)
			{
				char sessionIDstr[80];
				snprintf(sessionIDstr, 80, "%u", sessionID);
				sessionIDstr[79] = 0;
				sm::sm_init(0, 0, 0, sessionIDstr, (void**)&ci->cal_conn_hndl);
				assert(ci->cal_conn_hndl != 0);
				ci->cal_conn_hndl->csc = csc;
			}
		}

		if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_INIT && !ci->cal_conn_hndl)
		{
			char sessionIDstr[80];
			snprintf(sessionIDstr, 80, "%u", sessionID);
			sessionIDstr[79] = 0;
			sm::sm_init(0, 0, 0, sessionIDstr, (void**)&ci->cal_conn_hndl);
			assert(ci->cal_conn_hndl != 0);
			ci->cal_conn_hndl->csc = csc;
		}

		assert(ci->cal_conn_hndl != 0);
		assert(ci->cal_conn_hndl->exeMgr != 0);
	
		try {
			ci->cal_conn_hndl->connect();
		} catch (...) {
			setError(thd, HA_ERR_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			goto error;
		}
		hndl = ci->cal_conn_hndl;
	
		if (thd->infinidb_vtable.vtable_state != THD::INFINIDB_SELECT_VTABLE)
		{
			CalpontSelectExecutionPlan csep;

			SessionManager sm;
			BRM::TxnID txnID;
			txnID = sm.getTxnID(sessionID);
			if (!txnID.valid)
			{
				txnID.id = 0;
				txnID.valid = true;
			}
			CalpontSystemCatalog::SCN verID;
			verID = sm.verID();
	
			csep.txnID(txnID.id);
			csep.verID(verID);
			csep.sessionID(sessionID);
	
			csep.traceFlags(ci->traceFlags);

			//get plan
			int status = cp_get_plan(thd, csep);
			//if (cp_get_plan(thd, csep) != 0)
			if (status > 0)
				return HA_ERR_INTERNAL_ERROR;
			else if (status < 0)
				return 0;
			
			// @bug 2547. don't need to send the plan if it's impossible where for all unions.
			if (thd->infinidb_vtable.impossibleWhereOnUnion) 
				return 0;
							
			csep.data(thd->infinidb_vtable.original_query.c_ptr());

#ifdef PLAN_HEX_FILE
			// plan serialization
			ifstream ifs("/tmp/li1-plan.hex");
			ByteStream bs1;
			ifs >> bs1;
			ifs.close();
			csep.unserialize(bs1);
#endif

			IDEBUG( cout << "---------------- EXECUTION PLAN ----------------" << endl );
			IDEBUG( cerr << csep << endl );
			IDEBUG( cout << "-------------- EXECUTION PLAN END --------------\n" << endl );
			
			// check for broken pipe
			
			ByteStream msg;
			ByteStream emsgBs;			
			
			while (true)
			{
				try {
					ByteStream::quadbyte qb = 4;
					msg << qb;
					ci->cal_conn_hndl->exeMgr->write(msg);
					msg.restart();
					csep.rmParms(rmParms);			
					
					//send plan			
					csep.serialize(msg);		
					ci->cal_conn_hndl->exeMgr->write(msg);

					//get ExeMgr status back to indicate a vtable joblist success or not
					msg.restart();
					emsgBs.restart();
					msg = ci->cal_conn_hndl->exeMgr->read();			
					emsgBs = ci->cal_conn_hndl->exeMgr->read();	
					string emsg;			
									
					if (msg.length() == 0 || emsgBs.length() == 0)
					{
						emsg = "Lost connection to ExeMgr. Please contact your administrator";
						setError(thd, HA_ERR_UNSUPPORTED, emsg);
						return HA_ERR_UNSUPPORTED;
					}
					string emsgStr;
					emsgBs >> emsgStr;
					bool err = false;
						
					if (msg.length() == 4)
					{
						msg >> qb;
						if (qb != 0) 
							err = true;
					}
					else
					{
						err = true;
					}
					if (err)
					{
						setError(thd, HA_ERR_UNSUPPORTED, emsgStr);
						return HA_ERR_UNSUPPORTED;
					}
					rmParms.clear();
					ci->queryState = 1;
					break;
				} catch (...)
				{
					sm::sm_cleanup(ci->cal_conn_hndl);
					ci->cal_conn_hndl = 0;
					char sessionIDstr[80];
					snprintf(sessionIDstr, 80, "%u", sessionID);
					sessionIDstr[79] = 0;
					sm::sm_init(0, 0, 0, sessionIDstr, (void**)&ci->cal_conn_hndl);
					try {
						ci->cal_conn_hndl->connect();
					} catch (...)
					{
						setError(thd, HA_ERR_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
						CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
						goto error;
					}	
					msg.restart();
				}
			}
		}
	}

	// common path for both vtable select phase and table mode -- open scan handle
	ti = ci->tableMap[table];
	ti.msTablePtr = table;
	if ((thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE)||
	  (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE) ||
	  (thd->infinidb_vtable.vtable_state == THD::INFINIDB_REDO_QUERY))
	{
		if (ti.tpl_ctx == 0)
		{		
			ti.tpl_ctx = new sm::cpsm_tplh_t();
			ti.tpl_scan_ctx = new sm::cpsm_tplsch_t();
		}
		
		try {
		if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE)
			tableid = execplan::IDB_VTABLE_ID;
		else
			tableid = tbname2id(table->s->db.str, table->s->table_name.str, hndl);
		} catch (...) {
			string emsg = "No table ID found for table " + string(table->s->table_name.str);
			setError(thd, HA_ERR_INTERNAL_ERROR, emsg);
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			goto internal_error;
		}
		
		try {
			sm::tpl_open(tableid, (void**)&ti.tpl_ctx, hndl);
			sm::tpl_scan_open(tableid, sm::TPL_FH_READ, (void**)&ti.tpl_scan_ctx, hndl);
		} catch (std::exception& e)
		{
			string emsg = "table can not be opened: " + string(e.what());
			setError(thd, HA_ERR_INTERNAL_ERROR, emsg);
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			goto internal_error;
		}
		catch (...)
		{
			string emsg = "table can not be opened";
			setError(thd, HA_ERR_INTERNAL_ERROR, emsg);
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			goto internal_error;
		}
		
		ti.tpl_scan_ctx->traceFlags = ci->traceFlags;
		if (ti.tpl_scan_ctx->ctp == NULL)
		{
			CalpontSystemCatalog::ColType* ct;
			int num_attr = table->s->fields;
			ct = (CalpontSystemCatalog::ColType*)calloc(num_attr, sizeof(CalpontSystemCatalog::ColType));
			assert(ct != 0);

			// populate coltypes here for table mode because tableband gives treeoid for dictionary column
			if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
			{
				CalpontSystemCatalog::RIDList oidlist = csc->columnRIDs(make_table(table->s->db.str, table->s->table_name.str), true);
				for (unsigned int j = 0; j < oidlist.size(); j++)
				{            
					CalpontSystemCatalog::ColType ctype = csc->colType(oidlist[j].objnum);
					ct[ctype.colPosition] = ctype;
				}
			}
			ti.tpl_scan_ctx->ctp = ct;
		}
	}
	
	ci->tableMap[table] = ti;
	return 0;

error:
	if (ci->cal_conn_hndl)
	{
		sm::sm_cleanup(ci->cal_conn_hndl);
		ci->cal_conn_hndl = 0;
	}
	return HA_ERR_INTERNAL_ERROR;
	
internal_error:
	if (ci->cal_conn_hndl)
	{
		sm::sm_cleanup(ci->cal_conn_hndl);
		ci->cal_conn_hndl = 0;
	}
	return HA_ERR_UNSUPPORTED;
}

int ha_calpont_impl_rnd_next(uchar *buf, TABLE* table)
{
	THD* thd = current_thd;

	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_ERROR)
		return HA_ERR_INTERNAL_ERROR;
	// @bug 3005
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE && 
		  string(table->s->table_name.str).find("$vtable") != 0)
		  return HA_ERR_END_OF_FILE;
	if ( ((thd->lex)->sql_command == SQLCOM_UPDATE)  || ((thd->lex)->sql_command == SQLCOM_DELETE) || ((thd->lex)->sql_command == SQLCOM_DELETE_MULTI) || ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI))	
		return HA_ERR_END_OF_FILE;
	
	// @bug 2547
	if (thd->infinidb_vtable.impossibleWhereOnUnion)
		return HA_ERR_END_OF_FILE;
	
	// @bug 2232. Basic SP support 
	// @bug 3939. Only error out for sp with select. Let pass for alter table in sp.
	if (thd->infinidb_vtable.call_sp && (thd->lex)->sql_command != SQLCOM_ALTER_TABLE)
	{ 
		setError(thd, HA_ERR_UNSUPPORTED, "This stored procedure syntax is not supported by InfiniDB in this version"); 
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_ERROR; 
		return HA_ERR_INTERNAL_ERROR; 
	} 
			
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	
	// @bug 3078
	if(thd->killed == THD::KILL_QUERY)
	{
		if (ci->cal_conn_hndl)
		{
			// send ExeMgr a signal before cloing the connection
			ByteStream msg;
			ByteStream::quadbyte qb = 0;
			msg << qb;
			try {
				ci->cal_conn_hndl->exeMgr->write(msg);
			} catch (...)
			{
				// canceling query, ignore connection failure.
			}
			
			sm::sm_cleanup(ci->cal_conn_hndl);
			ci->cal_conn_hndl = 0;
		}
		thd->infinidb_vtable.has_limit = false;
		return 0;
	}
	
	if (ci->alterTableState > 0) return HA_ERR_END_OF_FILE;
	
	cal_table_info ti;
	ti = ci->tableMap[table];
	int rc = HA_ERR_END_OF_FILE;
	
	if (!ti.tpl_ctx || !ti.tpl_scan_ctx)
	{
		// @bug 2135.  Changed wording of error message below.
		string emsg = "Cannot open table handle for " + string(table->s->table_name.str) + ".";
		setError(thd, HA_ERR_INTERNAL_ERROR, emsg);
		CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
		return HA_ERR_INTERNAL_ERROR;	
	}
	
	assert(ti.msTablePtr == table);
	
	try {
		rc = fetchNextRow(buf, ti, ci);
	} catch (...)
	{
		string emsg = "Lost connection to ExeMgr while fetching.";
		setError(thd, HA_ERR_INTERNAL_ERROR, emsg);
		CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
		return HA_ERR_INTERNAL_ERROR;	
	}
	ci->tableMap[table] = ti;
	
	if (rc != 0 && rc != HA_ERR_END_OF_FILE)
	{
		string emsg;
		// remove this check when all error handling migrated to the new framework.
		if (rc >= 1000)
			emsg = ti.tpl_scan_ctx->errMsg;
		else
		{
			logging::ErrorCodes errorcodes;
			emsg = errorcodes.errorString(rc);
		}
		setError(thd, HA_ERR_INTERNAL_ERROR, emsg);
		CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
		rc = HA_ERR_INTERNAL_ERROR;		
	}

	return rc;
}

int ha_calpont_impl_rnd_end(TABLE* table)
{
	int rc = 0;
	THD* thd = current_thd;
	cal_connection_info* ci = NULL;
	
	if (thd->infinidb_vtable.cal_conn_info)
		ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_ORDER_BY )
	{
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_SELECT_VTABLE;	// flip back to normal state
		return rc;
	}
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_REDO_PHASE1 && thd->infinidb_vtable.mysql_optimizer_off)
		return rc;
	
	if ( (thd->lex)->sql_command == SQLCOM_ALTER_TABLE )
		return rc;
	
	if ( ((thd->lex)->sql_command == SQLCOM_UPDATE)  ||
		   ((thd->lex)->sql_command == SQLCOM_DELETE) ||
		   ((thd->lex)->sql_command == SQLCOM_DELETE_MULTI) ||
		   ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI))
		   return rc;

	if ( ((thd->lex)->sql_command == SQLCOM_INSERT) ||
		   ((thd->lex)->sql_command == SQLCOM_INSERT_SELECT) )
	{
		// @bug 4022. error handling for select part of dml
		if (ci->cal_conn_hndl && ci->rc)
		{
			// send ExeMgr a signal before cloing the connection
			ByteStream msg;
			ByteStream::quadbyte qb = 0;
			msg << qb;
			try {
				ci->cal_conn_hndl->exeMgr->write(msg);
			} catch (...)
			{
				// end of query, ignore connection failure.
			}
			sm::sm_cleanup(ci->cal_conn_hndl);
			ci->cal_conn_hndl = 0;
			thd->infinidb_vtable.has_limit = false; // prevent connection re-established
			return rc;
		}
	}
	
	if (!ci)
	{
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
		ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	}
	
	// @bug 3078
	if(thd->killed == THD::KILL_QUERY)
	{
		if (ci->cal_conn_hndl)
		{
			// send ExeMgr a signal before cloing the connection
			ByteStream msg;
			ByteStream::quadbyte qb = 0;
			msg << qb;
			try {
				ci->cal_conn_hndl->exeMgr->write(msg);
			} catch (...)
			{
				// canceling query, ignore connection failure.
			}
			sm::sm_cleanup(ci->cal_conn_hndl);
			ci->cal_conn_hndl = 0;
		}
		thd->infinidb_vtable.has_limit = false;
		return 0;
	}
	
	IDEBUG( cerr << "rnd_end for table " << table->s->table_name.str << endl );

	cal_table_info ti = ci->tableMap[table];
	sm::cpsm_conhdl_t* hndl;
	
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
		hndl = ti.conn_hndl;
	else
		hndl = ci->cal_conn_hndl;		
	
	if (ti.tpl_ctx)
	{
		if (ti.tpl_scan_ctx)
		{
			if (ti.tpl_scan_ctx->ctp)
				free(ti.tpl_scan_ctx->ctp);
			ti.tpl_scan_ctx->ctp = 0;
			try {
				sm::tpl_scan_close(ti.tpl_scan_ctx, hndl);
				ti.tpl_scan_ctx = 0;
			} catch (...) {
				rc = HA_ERR_INTERNAL_ERROR;
			}
		}
		ti.tpl_scan_ctx = 0;
		try {
			sm::tpl_close(ti.tpl_ctx, hndl);
			ti.tpl_ctx = 0;
		} catch (...) {
			rc = HA_ERR_INTERNAL_ERROR;
		}
	}
	ti.tpl_ctx = 0;
	
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE && 
		  thd->infinidb_vtable.has_order_by)
	thd->infinidb_vtable.vtable_state = THD::INFINIDB_ORDER_BY;
	
	ci->tableMap[table] = ti;
	
	return rc;
}

int ha_calpont_impl_create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
	THD *thd = current_thd;

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	

	//@Bug 1948. Mysql calls create table to create a new table with new signature.
	if (ci->alterTableState > 0) return 0;

	int rc = ha_calpont_impl_create_(name, table_arg, create_info, *ci);

	return rc;
}

int ha_calpont_impl_delete_table(const char *name)
{	
	//if this is an InfiniDB tmp table ('#sql*.frm') just leave...
	if (!bcmp((uchar*)name, tmp_file_prefix, tmp_file_prefix_length)) return 0;

	THD *thd = current_thd;

	if (!thd) return 0;
	if (!thd->lex) return 0;
	if (!thd->query) return 0;

	// @bug 1700. 
	if (thd->lex->sql_command == SQLCOM_DROP_DB)
	{
  	 thd->main_da.can_overwrite_status = true;
	 	 thd->main_da.set_error_status(thd, HA_ERR_UNSUPPORTED, "Non-empty database can not be dropped. ");
	 	 return 1;
	}
	
	TABLE_LIST *first_table= (TABLE_LIST*) thd->lex->select_lex.table_list.first;
	
	// should never get in here
	if (!first_table)
	{
		setError(thd, HA_ERR_INTERNAL_ERROR, "Null table pointer detected when dropping table");
		return 1;
	}
	if (!(first_table->table && first_table->table->s && first_table->table->s->db.str))
		return 0;
		
	string db = first_table->table->s->db.str;

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	

	if (!ci) return 0;

	//@Bug 1948,2306. if alter table want to drop the old table, InfiniDB does not need to drop.
	if ( ci->isAlter ){
		 ci->isAlter=false;
		 return 0;
	}
	// @bug 1793. make vtable droppable in calpontsys. "$vtable" ==> "@0024vtable" passed in as name.
	if (db == "calpontsys" && string(name).find("@0024vtable") == string::npos) 
	{
		std::string stmt(thd->query);
		algorithm::to_upper(stmt);
		//@Bug 2432. systables can be dropped with restrict
		if (stmt.find(" RESTRICT") != string::npos)
		{
			return 0;
		}
		setError(thd, HA_ERR_INTERNAL_ERROR, "Calpont system tables can only be dropped with restrict.");
		return 1;
	}

	int rc = ha_calpont_impl_delete_table_(name, *ci);
	return rc;
}
int ha_calpont_impl_write_row(uchar *buf, TABLE* table)
{
	THD *thd = current_thd;
	
	// Error out INSERT on VIEW. It's currently not supported.
	// @note INSERT on VIEW works natually (for simple cases at least), but we choose to turn it off 
	// for now - ZZ.
	TABLE_LIST* tables = thd->lex->query_tables;
	for (; tables; tables= tables->next_local)
	{
		if (tables->view)
		{
			Message::Args args;
			args.add("Insert");
			string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_VIEW, args);
			setError(current_thd, HA_ERR_UNSUPPORTED, emsg);
			return HA_ERR_UNSUPPORTED;
		}
	}
	
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	
	if ( !ci->dmlProc )
	{
		ci->dmlProc = new MessageQueueClient("DMLProc");
		//cout << "test007: ha_calpont_impl_write_row use new DMLProc client " <<ci->dmlProc << " for session " << current_thd->thread_id << endl;
	}
	
	if (ci->alterTableState > 0) return 0;
	ha_rows rowsInserted = 0;
	int rc = ha_calpont_impl_write_row_(buf, table, *ci, rowsInserted);
	
	//@Bug 2438 Added a variable rowsHaveInserted to keep track of how many rows have been inserted already.
	if ( !ci->singleInsert && ( rc == 0 ) && ( rowsInserted > 0 ))
	{
		ci->rowsHaveInserted += rowsInserted;
		if ( ( ci->bulkInsertRows > 0 ) && ( ci->rowsHaveInserted  >= ci->bulkInsertRows ) )
			ci->rowsHaveInserted = 0; //reset this variable
	}
	return rc;
}

int ha_calpont_impl_update_row()
{
	//@Bug 2540. Return the correct error code.
	THD *thd = current_thd;
	
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);
	int rc = ci->rc;
	if ( rc != 0 )
		ci->rc = 0;
	return ( rc );
}

int ha_calpont_impl_delete_row()
{
	//@Bug 2540. Return the correct error code.
	THD *thd = current_thd;
		
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);
	int rc = ci->rc;
	if ( rc !=0 )
		ci->rc = 0;
		
	return ( rc );
}

void ha_calpont_impl_start_bulk_insert(ha_rows rows, TABLE* table)
{
	THD *thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	

	if (ci->alterTableState > 0) return;
	
	//@Bug 2515. 
	//Check command instead of vtable state
	if ((thd->lex)->sql_command == SQLCOM_INSERT) 
	{
		//@Bug 4387. Check BRM status before start statement.
		scoped_ptr<DBRM> dbrmp(new DBRM());
		int rc = dbrmp->isReadWrite();
		if (rc != 0 )
		{
			setError(current_thd, HA_ERR_GENERIC, "Cannot execute the statement. DBRM is read only!");
		}
		if ( rows > 1 )
		{
			ci->singleInsert = false;
		}
	}
	else if ( (thd->lex)->sql_command == SQLCOM_LOAD || (thd->lex)->sql_command == SQLCOM_INSERT_SELECT)
	{
		//@Bug 4387. Check BRM status before start statement.
		scoped_ptr<DBRM> dbrmp(new DBRM());
		int rc = dbrmp->isReadWrite();
		if (rc != 0 )
		{
			setError(current_thd, HA_ERR_GENERIC, "Cannot execute the statement. DBRM is read only!");
		}
		
		ci->singleInsert = false; 
		ci->isLoaddataInfile = true;
	}

	ci->bulkInsertRows = rows;
	if ( ( ((thd->lex)->sql_command == SQLCOM_INSERT) ||  ((thd->lex)->sql_command == SQLCOM_LOAD) || (thd->lex)->sql_command == SQLCOM_INSERT_SELECT) && !ci->singleInsert ) 
	{			
		if ( !ci->dmlProc )
		{
			ci->dmlProc = new MessageQueueClient("DMLProc");
			//cout << "test007: ha_calpont_impl_start_bulk_insert use new DMLProc client " <<ci->dmlProc << " for session " << current_thd->thread_id << endl;
		} 
	} 
	if ( ci->rc != 0 )
		ci->rc = 0;
}



int ha_calpont_impl_end_bulk_insert(bool abort, TABLE* table)
{	
	THD *thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	
	int rc = 0;
	
	// @bug 2378. do not enter for select, reset singleInsert flag after multiple insert. 
	//@bug 2515. Check command intead of vtable state
	if ( ( ((thd->lex)->sql_command == SQLCOM_INSERT) ||  ((thd->lex)->sql_command == SQLCOM_LOAD) || (thd->lex)->sql_command == SQLCOM_INSERT_SELECT) && !ci->singleInsert ) 
	{
		if ( !ci->dmlProc )
		{
			ci->dmlProc = new MessageQueueClient("DMLProc");
			//cout << "test007: ha_calpont_impl_end_bulk_insert use new DMLProc client " <<ci->dmlProc << " for session " << current_thd->thread_id << endl;
		}

		//@Bug 2438. Only load dta infile calls last batch process
		if ( ci->isLoaddataInfile ) {
			//cout << "calling ha_calpont_impl_write_last_batch" << endl;
			//@Bug 2829 Handle ctrl-C
			if ( thd->killed > 0 )
				abort = true;

			rc = ha_calpont_impl_write_last_batch(table, *ci, abort);
			ci->rowsHaveInserted = 0;
		}
		
		if ( (ci->rc != 0) && ((thd->lex)->sql_command == SQLCOM_INSERT) && !ci->singleInsert && ( ci->rowsHaveInserted < ci->bulkInsertRows ) )
		{
			if ( thd->killed > 0 )
				abort = true;
			rc = ha_calpont_impl_write_last_batch(table, *ci, abort);
		}
		ci->singleInsert = true; // reset the flag
	}
	return rc;	
}

int ha_calpont_impl_commit (handlerton *hton, THD *thd, bool all)
{
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	
	if (ci->isAlter)
		return 0;
	if ( !ci->dmlProc )
	{
		ci->dmlProc = new MessageQueueClient("DMLProc");
		//cout << "test007: ha_calpont_impl_commit use new DMLProc client " <<ci->dmlProc << " for session " << current_thd->thread_id << endl;
	}

	int rc = ha_calpont_impl_commit_(hton, thd, all, *ci);
	thd->server_status&= ~SERVER_STATUS_IN_TRANS;
	return rc;
}

int ha_calpont_impl_rollback (handlerton *hton, THD *thd, bool all)
{
	// @bug 1738. no need to rollback for select. This is to avoid concurrent session
	// conflict because DML is not thread safe.
	//comment out for bug 3874. Select should never come to rollback. If there is no active transaction, 
	//rollback in DMLProc is not doing anything anyway
	//if (!(current_thd->options & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
	//{
	//	return 0;
	//}

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	
	if ( !ci->dmlProc ) {
	
		ci->dmlProc = new MessageQueueClient("DMLProc");
		//cout << "test007: ha_calpont_impl_rollback use new DMLProc client " <<ci->dmlProc << " for session " << current_thd->thread_id << endl;
	}
		
	int rc = ha_calpont_impl_rollback_(hton, thd, all, *ci);
	
	thd->server_status&= ~SERVER_STATUS_IN_TRANS;
	return rc;
}

int ha_calpont_impl_close_connection (handlerton *hton, THD *thd)
{
	if (!thd) return 0;

	if (thd->thread_id == 0)
		return 0;

	execplan::CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	if (!ci) return 0;

	int rc = 0;
	if ( ci->dmlProc )
	{
		rc = ha_calpont_impl_close_connection_(hton, thd, *ci);
		delete ci->dmlProc;
		ci->dmlProc = 0; //@Bug 2959.
	}
	
	if (ci->cal_conn_hndl)
	{		
		sm::sm_cleanup(ci->cal_conn_hndl);
		ci->cal_conn_hndl = 0;
	}

	return rc;
}

int ha_calpont_impl_rename_table(const char* from, const char* to)
{
//#ifdef INFINIDB_DEBUG
//	cout << "ha_calpont_impl_rename_table: " << from << " => " << to << endl;
//#endif
	IDEBUG( cout << "ha_calpont_impl_rename_table: " << from << " => " << to << endl );
	THD *thd = current_thd;

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	
	//@Bug 1948. Alter table call rename table twice
	if ( ci->alterTableState == cal_connection_info::ALTER_FIRST_RENAME )
	{
		ci->alterTableState = cal_connection_info::ALTER_SECOND_RENAME;
		IDEBUG( cout << "ha_calpont_impl_rename_table: was in state ALTER_FIRST_RENAME, now in ALTER_SECOND_RENAME" << endl );
		return 0;
	}
	else if (ci->alterTableState == cal_connection_info::ALTER_SECOND_RENAME)
	{
		ci->alterTableState = cal_connection_info::NOT_ALTER;
		IDEBUG( cout << "ha_calpont_impl_rename_table: was in state ALTER_SECOND_RENAME, now in NOT_ALTER" << endl );
		return 0;
	}
	else if ( thd->infinidb_vtable.vtable_state == THD::INFINIDB_ALTER_VTABLE )
		return 0;
	else
	{
#if 0
		thd->main_da.can_overwrite_status = true;    
		thd->main_da.set_error_status(thd, HA_ERR_UNSUPPORTED, "Syntax is not supported in InfiniDB.");
		return 1;
#endif
	}
 
	int rc = ha_calpont_impl_rename_table_(from, to, *ci);
	return rc;
}

int ha_calpont_impl_delete_row(const uchar *buf)
{
	IDEBUG( cout << "ha_calpont_impl_delete_row" << endl );
	return 0;
}

COND* ha_calpont_impl_cond_push(COND *cond, TABLE* table)
{
	THD *thd = current_thd;
	if (thd->infinidb_vtable.vtable_state != THD::INFINIDB_DISABLE_VTABLE)
		return cond;
		
	if (((thd->lex)->sql_command == SQLCOM_UPDATE) || ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI) || ((thd->lex)->sql_command == SQLCOM_DELETE) || ((thd->lex)->sql_command == SQLCOM_DELETE_MULTI))
		return cond;
	IDEBUG( cout << "ha_calpont_impl_cond_push: " << table->alias << endl );
	
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	
	cal_table_info ti = ci->tableMap[table];		
	
	if (!ti.csep)
	{
		if (!ti.condInfo)
			ti.condInfo = new gp_walk_info();	
		gp_walk_info* gwi = ti.condInfo;
		gwi->dropCond = false;
		gwi->fatalParseError = false;
		gwi->condPush = true;
		gwi->sessionid = tid2sid(thd->thread_id);
		cond->traverse_cond(gp_walk, gwi, Item::POSTFIX);	
		ci->tableMap[table] = ti;
		
		if (gwi->fatalParseError)
		{
			IDEBUG( cout << gwi->parseErrorText << endl );
			if (ti.condInfo)
			{
				delete ti.condInfo;
				ti.condInfo = 0;
				ci->tableMap[table] = ti;
			}
			return cond;
		}
		if (gwi->dropCond)
			return cond;
		else
			return NULL;
	}	
	
	return cond;
}

int ha_calpont_impl_external_lock(THD *thd, TABLE* table, int lock_type)
{
	// @bug 3014. Error out locking table command. IDB does not support it now.
	if (thd->lex->sql_command == SQLCOM_LOCK_TABLES)
	{
		setError(current_thd, HA_ERR_UNSUPPORTED,
		         logging::IDBErrorInfo::instance()->errorMsg(ERR_LOCK_TABLE));
		return HA_ERR_UNSUPPORTED;
	}

	// @info called for every table at the beginning and at the end of a query.
	// used for cleaning up the tableinfo.
	IDEBUG( cout << "external_lock for " << table->alias << endl );
	assert((thd->infinidb_vtable.vtable_state >= THD::INFINIDB_INIT_CONNECT && thd->infinidb_vtable.vtable_state <= THD::INFINIDB_REDO_QUERY) ||
		thd->infinidb_vtable.vtable_state == THD::INFINIDB_ERROR);
	if ( thd->infinidb_vtable.vtable_state == THD::INFINIDB_INIT  ) //return if not select 
		return 0;
	
#ifdef SKIP_INSERT_SELECT
	if (thd->infinidb_vtable.isInsertSelect)
	{
		Message::Args args;
		args.add("Insert with Select");
		setError(thd, HA_ERR_UNSUPPORTED, (IDBErrorInfo::instance()->errorMsg(ERR_ENTERPRISE_ONLY, args)));
		return HA_ERR_UNSUPPORTED;
	}
#endif

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);	
	
	if(thd->killed == THD::KILL_QUERY)
	{
		if (ci->cal_conn_hndl)
		{
			// send ExeMgr a signal before cloing the connection
			ByteStream msg;
			ByteStream::quadbyte qb = 0;
			msg << qb;
			try {
				ci->cal_conn_hndl->exeMgr->write(msg);
			} catch (...)
			{
				// canceling query, ignore connection failure.
			}
			sm::sm_cleanup(ci->cal_conn_hndl);
			ci->cal_conn_hndl = 0;
		}
		thd->infinidb_vtable.has_limit = false;
		return 0;
	}
	
	CalTableMap::iterator mapiter = ci->tableMap.find(table);
#ifdef _MSC_VER
	//FIMEX: fix this! (must be related to F_UNLCK define in winport)
	if (mapiter != ci->tableMap.end() && lock_type == 0) // make sure it's the release lock (2nd) call
#else
	if (mapiter != ci->tableMap.end() && lock_type == 2) // make sure it's the release lock (2nd) call
#endif
	{
		// table mode
		if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
		{
			if (ci->traceFlags & 1)
				push_warning(thd, MYSQL_ERROR::WARN_LEVEL_NOTE, 9999, mapiter->second.conn_hndl->queryStats.c_str());
			if (mapiter->second.conn_hndl)
			{
				ci->queryStats = mapiter->second.conn_hndl->queryStats;
				ci->extendedStats = mapiter->second.conn_hndl->extendedStats;
				ci->miniStats = mapiter->second.conn_hndl->miniStats;
				sm::sm_cleanup((*mapiter).second.conn_hndl);
			}
			(*mapiter).second.conn_hndl = 0;
			if ((*mapiter).second.condInfo)
				delete (*mapiter).second.condInfo;
			if ((*mapiter).second.csep)
				delete (*mapiter).second.csep;	

			// only push this warning for once
			if (ci->tableMap.size() == 1 && 
				  thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE && thd->infinidb_vtable.autoswitch)
			{
				push_warning(thd, MYSQL_ERROR::WARN_LEVEL_WARN, 9999, infinidb_autoswitch_warning.c_str());
			}
		}
		else // vtable mode
		{				
			if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE)
			{
				if (ci->traceFlags & 1)
					push_warning(thd, MYSQL_ERROR::WARN_LEVEL_NOTE, 9999, ci->cal_conn_hndl->queryStats.c_str());
				
				if (!ci->cal_conn_hndl)
					return 0;
				ci->queryStats = ci->cal_conn_hndl->queryStats;
				ci->extendedStats = ci->cal_conn_hndl->extendedStats;
				ci->miniStats = ci->cal_conn_hndl->miniStats;
				ci->queryState = 0;
				thd->infinidb_vtable.override_largeside_estimate = false;
				if ( thd->infinidb_vtable.has_limit)
				{
					sm::sm_cleanup(ci->cal_conn_hndl);
					ci->cal_conn_hndl = 0;
					thd->infinidb_vtable.has_limit = false;
				}
			}
		}
		ci->tableMap.erase(table);
	}
		
	return 0;
}

// for sorting length exceeds blob limit. Just error out for now.
int ha_calpont_impl_rnd_pos(uchar *buf, uchar *pos)
{
	IDEBUG( cout << "ha_calpont_impl_rnd_pos" << endl);
	string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_ORDERBY_TOO_BIG);
	setError(current_thd, HA_ERR_UNSUPPORTED, emsg);
	return HA_ERR_UNSUPPORTED;
}
// vim:sw=4 ts=4:

