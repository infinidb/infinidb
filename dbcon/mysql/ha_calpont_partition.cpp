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
* $Id: ha_calpont_partition.cpp 8607 2012-06-05 21:28:40Z zzhu $
*/

#include <iostream>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
//#include <unistd.h>
#include <iomanip>
using namespace std;

#include "idb_mysql.h"

#include "blocksize.h"
#include "calpontsystemcatalog.h"
#include "objectidmanager.h"
using namespace execplan;

#include "mastersegmenttable.h"
#include "extentmap.h"
#include "dbrm.h"
using namespace BRM;

#include "dataconvert.h"
using namespace dataconvert;
#include "ddlpkg.h"
#include "sqlparser.h"
using namespace ddlpackage;

#include "bytestream.h"
using namespace messageqcpp;

#include "ddlpackageprocessor.h"
using namespace ddlpackageprocessor;

#include "errorids.h"
#include "idberrorinfo.h"
#include "exceptclasses.h"
using namespace logging;

namespace {

//convenience fcn
inline uint32_t tid2sid(const uint32_t tid)
{
	return CalpontSystemCatalog::idb_tid2sid(tid);
}

void CHECK( int rc)
{ 
	if (rc != 0) 
	{
		ostringstream oss;
		oss << "Error in DBRM call " << rc << endl;
		throw runtime_error(oss.str()); 
	}
}

const uint64_t ET_DISABLED = 0x0002;
const uint64_t CPINVALID = 0x0004;

struct PartitionInfo
{
	int64_t min;
	int64_t max;
	uint64_t status;
	PartitionInfo():min((uint64_t)0x8000000000000001ULL), max((uint64_t)-0x8000000000000001LL), status(0) {};
};

typedef map<uint64_t, PartitionInfo> PartitionMap;

const string charcolToString(int64_t v)
{
	ostringstream oss;
	char c;
	for (int i = 0; i < 8; i++)
	{
		c = v & 0xff;
		oss << c;
		v >>= 8;
	}
	return oss.str();
}

const string format(int64_t v, CalpontSystemCatalog::ColType ct)
{
	ostringstream oss;
	
	switch (ct.colDataType)
	{
		case CalpontSystemCatalog::DATE:
			oss << DataConvert::dateToString(v);
			break;
		case CalpontSystemCatalog::DATETIME:
			oss << DataConvert::datetimeToString(v);
			break;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		{
			char c;
			for (int i = 0; i < 8; i++)
			{
				c = v & 0xff;
				oss << c;
				v >>= 8;
			}
			break;
		}
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::DECIMAL:
		{
			if (ct.scale > 0)
			{
				double d = ((double)(v) / (double)pow((double)10, ct.scale));			
				oss << setprecision(ct.scale) << fixed << d;
			}
			else
			{
				oss << v;
			}
			break;
		}
		case CalpontSystemCatalog::VARBINARY:
			oss << "N/A";
			break;
		default:
			oss << v;
			break;
	}
	return oss.str();
}
int processPartition ( SqlStatement *stmt, std::string &msg)
{
	//cout << "Sending to DDLProc" << endl;
    ByteStream bytestream;
    bytestream << stmt->fSessionID;
    stmt->serialize(bytestream);
    MessageQueueClient mq("DDLProc");
    ByteStream::byte b=0;
	int rc = 0;
	THD *thd = current_thd;
	string emsg;
    try
    {
      mq.write(bytestream);
      bytestream = mq.read();
	  if ( bytestream.length() == 0 )
	  {
		rc = 1;
      	thd->main_da.can_overwrite_status = true;

      	thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, "Lost connection to DDLProc");	
	  }
	  else
	  {
      	bytestream >> b;
      	bytestream >> emsg;
      	rc = b;
	  }
    }
    catch (runtime_error&)
    {
      rc =1;
      thd->main_da.can_overwrite_status = true;

	  thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, "Lost connection to DDLProc");
    }
    catch (...)
    {
      rc = 1;
      thd->main_da.can_overwrite_status = true;

	  thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, "Unknown error caught");
    }

    if ((b != 0) && (b!=ddlpackageprocessor::DDLPackageProcessor::WARNING))
    {
      thd->main_da.can_overwrite_status = true;

	  thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, emsg.c_str());
    }

	if (b ==ddlpackageprocessor::DDLPackageProcessor::WARNING)
	{
		rc = 0;
		string errmsg ("Error occured during file deletion. Restart DDLProc or use command tool ddlcleanup to clean up. " );
		push_warning(thd, MYSQL_ERROR::WARN_LEVEL_WARN, 9999, errmsg.c_str());
	}
    return rc;
}

std::string  ha_calpont_impl_markpartition_ (execplan::CalpontSystemCatalog::TableName tableName, uint32_t partition)
{
	ddlpackage::QualifiedName qualifiedName;
	qualifiedName.fSchema = tableName.schema;
	qualifiedName.fName = tableName.table;
	MarkPartitionStatement * stmt = new MarkPartitionStatement(&qualifiedName);
	stmt->fSessionID = tid2sid(current_thd->thread_id);
    stmt->fSql = "caldisablepartition";
	stmt->fOwner = tableName.schema;
	stmt->fPartition = partition;
	string msg;
	int rc = processPartition( stmt, msg);
	if ( rc != 0 )
		return msg;
	msg = "Partition is disabled." ;
	return msg;
}

std::string  ha_calpont_impl_restorepartition_ (execplan::CalpontSystemCatalog::TableName tableName, uint32_t partition)
{
	ddlpackage::QualifiedName qualifiedName;
	qualifiedName.fSchema = tableName.schema;
	qualifiedName.fName = tableName.table;
	RestorePartitionStatement * stmt = new RestorePartitionStatement(&qualifiedName);
	stmt->fSessionID = tid2sid(current_thd->thread_id);
    stmt->fSql = "calenablepartition";
	stmt->fOwner = tableName.schema;
	stmt->fPartition = partition;
	string msg;
	int rc = processPartition( stmt, msg);
	if ( rc != 0 )
		return msg;
	msg = "Partition is enabled." ;
	return msg;
}

std::string  ha_calpont_impl_droppartition_ (execplan::CalpontSystemCatalog::TableName tableName, uint32_t partition)
{
	ddlpackage::QualifiedName qualifiedName;
	qualifiedName.fSchema = tableName.schema;
	qualifiedName.fName = tableName.table;
	DropPartitionStatement * stmt = new DropPartitionStatement(&qualifiedName);
	stmt->fSessionID = tid2sid(current_thd->thread_id);
    stmt->fSql = "caldroppartition";
	stmt->fOwner = tableName.schema;
	stmt->fPartition = partition;
	string msg;
	int rc = processPartition( stmt, msg);
	//delete stmt;
	if ( rc != 0 )
		return msg;
	msg = "Partition is dropped" ;
	return msg;
}

extern "C"
{

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calshowpartitions_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count < 2 || 
		  args->arg_count > 3 ||
		  args->arg_type[0] != STRING_RESULT ||
		  args->arg_type[1] != STRING_RESULT ||
		  (args->arg_count == 3 && args->arg_type[2] != STRING_RESULT))
	{
		strcpy(message,"usage: CALSHOWPARTITIONS ([schema], table, column)");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calshowpartitions_deinit(UDF_INIT* initid)
{
}	

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calshowpartitions(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	DBRM em;
	vector<struct EMEntry> entries;
	vector<struct EMEntry>::iterator iter;
	vector<struct EMEntry>::iterator end;
	PartitionMap partMap;
	PartitionMap::iterator mapit;
	int32_t seqNum;
	bool header;
	string schema, table, column;
	CalpontSystemCatalog::ColType ct;
	if (args->arg_count == 3)
	{
		schema = (char*)(args->args[0]);
		table = (char*)(args->args[1]);
		column = (char*)(args->args[2]);
	}
	else
	{
		if (current_thd->db)
		{
			schema = current_thd->db; 
		}
		else
		{
			string errMsg = "No schema name indicated.";
			memcpy(result, errMsg.c_str(), errMsg.length());
			*length = errMsg.length();
			return result;
		}
		table = (char*)(args->args[0]);
		column = (char*)(args->args[1]);
	}
	
	try
	{
		CalpontSystemCatalog* csc = CalpontSystemCatalog::makeCalpontSystemCatalog(tid2sid(current_thd->thread_id));
		csc->identity(CalpontSystemCatalog::FE);
		CalpontSystemCatalog::TableColName tcn = make_tcn(schema, table, column);
		OID_t oid = csc->lookupOID(tcn);
		ct = csc->colType(oid);
		if (oid == -1)
		{
			Message::Args args;
			args.add("'" + schema + string(".") + table + string(".") + column + "'");
			throw IDBExcept(ERR_TABLE_NOT_IN_CATALOG, args);
		}
		
		CHECK(em.getExtents(oid, entries, false, false, true));
		if (entries.size() > 0)
		{
			header = false;
			iter = entries.begin();
			end = entries.end();
			for (;iter != end; ++iter)
			{
				PartitionInfo partInfo;
				if (iter->status == EXTENTOUTOFSERVICE)
					partInfo.status |= ET_DISABLED;
				
				mapit = partMap.find(iter->partitionNum);
				int state = em.getExtentMaxMin(iter->range.start, partInfo.max, partInfo.min, seqNum);
				
				if (state != CP_VALID)
				{
					partInfo.status |= CPINVALID;
					partMap[iter->partitionNum] = partInfo;
					continue;
				}
				
				if (mapit == partMap.end())
				{
					partMap[iter->partitionNum] = partInfo;
				}
				else
				{
					if (mapit->second.status & CPINVALID)
						continue;
					mapit->second.min = (partInfo.min < mapit->second.min ? partInfo.min : mapit->second.min);
					mapit->second.max = (partInfo.max > mapit->second.max ? partInfo.max : mapit->second.max);
				}
			}
		}
	} catch (IDBExcept& ex)
	{
		current_thd->main_da.can_overwrite_status = true;
		current_thd->main_da.set_error_status(current_thd, HA_ERR_UNSUPPORTED, ex.what());	
		return result;
	}
	catch (...)
	{
		current_thd->main_da.can_overwrite_status = true;
		current_thd->main_da.set_error_status(current_thd, HA_ERR_UNSUPPORTED, "Error occured when calling CALSHOWPARTITIONS");	
		return result;
	}
	ostringstream output;
	output.setf(ios::left, ios::adjustfield);
	output << setw(8) << "Part#" << setw(30) << "Min" << setw(30) << "Max" << /*setw(10) <<*/ "Status";
	for (mapit = partMap.begin(); mapit != partMap.end(); ++mapit)
	{
		output << "\n  " << setw(8) << mapit->first;
		if (mapit->second.status & CPINVALID)
			output << setw(30) << "N/A" << setw(30) << "N/A";
		else if (mapit->second.min > mapit->second.max)
			output << setw(30) << "Empty/Null" << setw(30) << "Empty/Null";
		else
			output << setw(30) << format(mapit->second.min, ct) << setw(30) << format(mapit->second.max, ct);
		if (mapit->second.status & ET_DISABLED)
			output << "Disabled";
		else
			output << "Enabled";
	}
	
	// use our own buffer to make sure it fits.
	char* res = new char[output.str().length()+1];
	memcpy(res,output.str().c_str(),output.str().length());
	*length = output.str().length();
	return res;
}
	
	#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool caldisablepartition_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count == 3 && (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT || args->arg_type[2] != INT_RESULT))
	{
		strcpy(message,"CALDISABLEPARTITION() requires two STRING and one INTEGER arguments");
		return 1;
	}
	else if ((args->arg_count == 2) && ( args->arg_type[0] != STRING_RESULT || args->arg_type[1] != INT_RESULT) )
	{
		strcpy(message,"CALDISABLEPARTITION() requires one STRING and one INTEGER arguments");
		return 1;
	}
	else if (args->arg_count > 3 )
	{
		strcpy(message,"CALDISABLEPARTITION() takes two or three arguments only");
		return 1;
	}
	else if (args->arg_count < 2 )
	{
		strcpy(message,"CALDISABLEPARTITION() requires at least two arguments");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* caldisablepartition(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{	
	CalpontSystemCatalog::TableName tableName;
	uint32_t partition = 0;
	if ( args->arg_count == 3 )
	{
		tableName.schema = args->args[0];
		tableName.table = args->args[1];
		partition = *reinterpret_cast<int*>(args->args[2]);
	}
	else
	{
		tableName.table = args->args[0];
		tableName.schema = current_thd->db;
		partition = *reinterpret_cast<int*>(args->args[1]);
	}
	
	
		
	string msg = ha_calpont_impl_markpartition_(tableName,partition );
	
	memcpy(result,msg.c_str(), msg.length());
	*length = msg.length();
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void caldisablepartition_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calenablepartition_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count == 3 && (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT || args->arg_type[2] != INT_RESULT))
	{
		strcpy(message,"CALENABLEPARTITION() requires two STRING and one INTEGER arguments");
		return 1;
	}
	else if ((args->arg_count == 2) && ( args->arg_type[0] != STRING_RESULT || args->arg_type[1] != INT_RESULT) )
	{
		strcpy(message,"CALENABLEPARTITION() requires one STRING and one INTEGER arguments");
		return 1;
	}
	else if (args->arg_count > 3 )
	{
		strcpy(message,"CALENABLEPARTITION() takes two or three arguments only");
		return 1;
	}
	else if (args->arg_count < 2 )
	{
		strcpy(message,"CALENABLEPARTITION() requires at least two arguments");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calenablepartition(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{	
	CalpontSystemCatalog::TableName tableName;
	uint32_t partition = 0;
	if ( args->arg_count == 3 )
	{
		tableName.schema = args->args[0];
		tableName.table = args->args[1];
		partition = *reinterpret_cast<int*> (args->args[2]);
	}
	else
	{
		tableName.table = args->args[0];
		tableName.schema = current_thd->db;
		partition = *reinterpret_cast<int*> (args->args[1]);
	}
	
	
		
	string msg = ha_calpont_impl_restorepartition_(tableName,partition );
	
	memcpy(result,msg.c_str(), msg.length());
	*length = msg.length();
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calenablepartition_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool caldroppartition_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count == 3 && (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT || args->arg_type[2] != INT_RESULT))
	{
		strcpy(message,"CALDROPPARTITION() requires two STRING and one INTEGER arguments");
		return 1;
	}
	else if ((args->arg_count == 2) && ( args->arg_type[0] != STRING_RESULT || args->arg_type[1] != INT_RESULT) )
	{
		strcpy(message,"CALDROPPARTITION() requires one STRING and one INTEGER arguments");
		return 1;
	}
	else if (args->arg_count > 3 )
	{
		strcpy(message,"CALDROPPARTITION() takes two or three arguments only");
		return 1;
	}
	else if (args->arg_count < 2 )
	{
		strcpy(message,"CALDROPPARTITION() requires at least two arguments");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* caldroppartition(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{	
	CalpontSystemCatalog::TableName tableName;
	uint32_t partition = 0;
	if ( args->arg_count == 3 )
	{
		tableName.schema = args->args[0];
		tableName.table = args->args[1];
		partition = *reinterpret_cast<int*> (args->args[2]);
	}
	else
	{
		tableName.table = args->args[0];
		tableName.schema = current_thd->db;
		partition = *reinterpret_cast<int*> (args->args[1]);
	}
	
	
		
	string msg = ha_calpont_impl_droppartition_(tableName,partition );
	
	memcpy(result,msg.c_str(), msg.length());
	*length = msg.length();
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void caldroppartition_deinit(UDF_INIT* initid)
{
}

}

}
