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
* $Id: ha_calpont_partition.cpp 9642 2013-06-24 14:57:42Z rdempsey $
*/

#include <iostream>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
//#include <unistd.h>
#include <iomanip>
using namespace std;

#include <boost/tokenizer.hpp>


#include "idb_mysql.h"

#include "blocksize.h"
#include "calpontsystemcatalog.h"
#include "objectidmanager.h"
using namespace execplan;

#include "mastersegmenttable.h"
#include "extentmap.h"
#include "dbrm.h"
#include "brmtypes.h"
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

#include <boost/algorithm/string/case_conv.hpp>
using namespace boost;

namespace {
const uint8_t ROUND_POS = 0x01;
const uint8_t ROUND_NEG = 0x80;

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

// partition warnings are delimited by '\n'
void push_warnings(THD* thd, string& warnings)
{
	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep("\n");
	tokenizer tokens(warnings, sep);

	for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
	{
		push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, (*tok_iter).c_str());
	}
}

string name(CalpontSystemCatalog::ColType& ct)
{
	switch (ct.colDataType)
	{
		case CalpontSystemCatalog::INT:
			return "INT";
		case CalpontSystemCatalog::TINYINT:
			return "TINYINT";
		case CalpontSystemCatalog::MEDINT:
			return "MEDINT";
		case CalpontSystemCatalog::SMALLINT:
			return "SMALLINT";
		case CalpontSystemCatalog::BIGINT:
			return "BIGINT";
		case CalpontSystemCatalog::DATE:
			return "DATE";
		case CalpontSystemCatalog::DATETIME:
			return "DATETIME";
		case CalpontSystemCatalog::DECIMAL:
			return "DECIMAL";
		case CalpontSystemCatalog::CHAR:
		{
			ostringstream oss;
			oss << "CHAR(" << ct.colWidth << ")";
			return oss.str();
		}
		case CalpontSystemCatalog::VARCHAR:
		{
			ostringstream oss;
			oss << "VARCHAR(" << ct.colWidth << ")";
			return oss.str();
		}
		case CalpontSystemCatalog::FLOAT:
			return "FLOAT";
		case CalpontSystemCatalog::DOUBLE:
			return "DOUBLE";
		case CalpontSystemCatalog::BIT:
			return "BIT";
		case CalpontSystemCatalog::VARBINARY:
			return "VARBINARY";
		case CalpontSystemCatalog::BLOB:
			return "BLOB";
		case CalpontSystemCatalog::CLOB:
			return "CLOB";
		case CalpontSystemCatalog::UINT:
			return "UINT";
		case CalpontSystemCatalog::UTINYINT:
			return "UTINYINT";
		case CalpontSystemCatalog::UMEDINT:
			return "UMEDINT";
		case CalpontSystemCatalog::USMALLINT:
			return "USMALLINT";
		case CalpontSystemCatalog::UBIGINT:
			return "UBIGINT";
		case CalpontSystemCatalog::UDECIMAL:
			return "UDECIMAL";
		case CalpontSystemCatalog::UFLOAT:
			return "UFLOAT";
		case CalpontSystemCatalog::UDOUBLE:
			return "UDOUBLE";
		default:
			return "Unknown Type";
	}
}

bool CP_type(CalpontSystemCatalog::ColType& ct)
{
	if (ct.colDataType == CalpontSystemCatalog::INT ||
		ct.colDataType == CalpontSystemCatalog::TINYINT ||
		ct.colDataType == CalpontSystemCatalog::MEDINT ||
		ct.colDataType == CalpontSystemCatalog::SMALLINT ||
		ct.colDataType == CalpontSystemCatalog::BIGINT ||
		ct.colDataType == CalpontSystemCatalog::DATE ||
		ct.colDataType == CalpontSystemCatalog::DATETIME ||
		ct.colDataType == CalpontSystemCatalog::DECIMAL ||
		ct.colDataType == CalpontSystemCatalog::UTINYINT ||
		ct.colDataType == CalpontSystemCatalog::USMALLINT ||
		ct.colDataType == CalpontSystemCatalog::UMEDINT ||
		ct.colDataType == CalpontSystemCatalog::UINT ||
		ct.colDataType == CalpontSystemCatalog::UBIGINT ||
		ct.colDataType == CalpontSystemCatalog::UDECIMAL ||
		(ct.colDataType == CalpontSystemCatalog::CHAR && ct.colWidth <= 8) ||
		(ct.colDataType == CalpontSystemCatalog::VARCHAR && ct.colWidth <= 7))
	{
		return true;
	}
	return false;
}

const uint64_t ET_DISABLED = 0x0002;
const uint64_t CPINVALID = 0x0004;

struct PartitionInfo
{
	int64_t min;
	int64_t max;
	uint64_t status;
	PartitionInfo():min((uint64_t)0x8000000000000001ULL),
					  max((uint64_t)-0x8000000000000001LL),
					  status(0) {};
};

typedef map<LogicalPartition, PartitionInfo> PartitionMap;

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

const string format(int64_t v, CalpontSystemCatalog::ColType& ct)
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
			// swap again to retain the string byte order
			uint64_t tmp = uint64ToStr(v);
			oss << (char*)(&tmp);
			break;
		}
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::DECIMAL:
		case CalpontSystemCatalog::UDECIMAL:
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
		case CalpontSystemCatalog::UTINYINT:
		case CalpontSystemCatalog::USMALLINT:
		case CalpontSystemCatalog::UMEDINT:
		case CalpontSystemCatalog::UINT:
		case CalpontSystemCatalog::UBIGINT:
			oss << static_cast<uint64_t>(v);
			break;
		case CalpontSystemCatalog::VARBINARY:
			oss << "N/A";
			break;
		default:
			oss << v;
			break;
	}
	return oss.str();
}

const int64_t IDB_format(char* str, CalpontSystemCatalog::ColType& ct, uint8_t& rf)
{
	int64_t v = 0;
	bool pushWarning = false;
	rf = 0;
	boost::any anyVal = DataConvert::convertColumnData(ct, str, pushWarning, false, true);

	switch (ct.colDataType)
	{
	case CalpontSystemCatalog::BIT:
		v = boost::any_cast<bool>(anyVal);
		break;
	case CalpontSystemCatalog::TINYINT:
		v = boost::any_cast<char>(anyVal);
		break;
	case CalpontSystemCatalog::UTINYINT:
		v = boost::any_cast<uint8_t>(anyVal);
		break;
	case CalpontSystemCatalog::SMALLINT:
		v = boost::any_cast<int16_t>(anyVal);
		break;
	case CalpontSystemCatalog::USMALLINT:
		v = boost::any_cast<uint16_t>(anyVal);
		break;
	case CalpontSystemCatalog::MEDINT:
	case CalpontSystemCatalog::INT:
#ifdef _MSC_VER
		v = boost::any_cast<int>(anyVal);
#else
		v = boost::any_cast<int32_t>(anyVal);
#endif
		break;
	case CalpontSystemCatalog::UMEDINT:
	case CalpontSystemCatalog::UINT:
		v = boost::any_cast<uint32_t>(anyVal);
		break;
	case CalpontSystemCatalog::BIGINT:
		v = boost::any_cast<long long>(anyVal);
		break;
	case CalpontSystemCatalog::UBIGINT:
		v = boost::any_cast<uint64_t>(anyVal);
		break;
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::VARBINARY:
	case CalpontSystemCatalog::BLOB:
	case CalpontSystemCatalog::CLOB:
		{
			string i = boost::any_cast<string>(anyVal);
			// bug 1932, pad nulls up to the size of v
			i.resize(sizeof(v), 0);
		  	v = uint64ToStr(*((uint64_t *) i.data()));
			if (pushWarning)
				rf = ROUND_POS;
		}
		break;
	case CalpontSystemCatalog::DATE:
		v = boost::any_cast<uint32_t>(anyVal);
		break;
	case CalpontSystemCatalog::DATETIME:
		v = boost::any_cast<uint64_t>(anyVal);
		break;
	case CalpontSystemCatalog::DECIMAL:
	case CalpontSystemCatalog::UDECIMAL:
		if (ct.colWidth == execplan::CalpontSystemCatalog::ONE_BYTE)
			v = boost::any_cast<char>(anyVal);
		else if (ct.colWidth == execplan::CalpontSystemCatalog::TWO_BYTE)
			v = boost::any_cast<int16_t>(anyVal);
		else if (ct.colWidth == execplan::CalpontSystemCatalog::FOUR_BYTE)
#ifdef _MSC_VER
			v = boost::any_cast<int>(anyVal);
#else
			v = boost::any_cast<int32_t>(anyVal);
#endif
		else
			v = boost::any_cast<long long>(anyVal);
		break;
	default:
		break;
	}

	if ((ct.colDataType == CalpontSystemCatalog::TINYINT ||
		 ct.colDataType == CalpontSystemCatalog::SMALLINT ||
		 ct.colDataType == CalpontSystemCatalog::MEDINT ||
		 ct.colDataType == CalpontSystemCatalog::INT ||
		 ct.colDataType == CalpontSystemCatalog::BIGINT ||
		 ct.colDataType == CalpontSystemCatalog::DECIMAL ||
		 ct.colDataType == CalpontSystemCatalog::UDECIMAL) &&
		pushWarning)
	{
		// get rid of leading white spaces and parentheses
		string data(str);
		size_t fpos = data.find_first_of(" \t()");
		while (string::npos != fpos)
		{
			data.erase(fpos, 1);
			fpos = data.find_first_of(" \t()");
		}
		rf = (data[0] == '-') ? ROUND_NEG : ROUND_POS;
	}

	return v;
}


void parsePartitionString(UDF_ARGS* args,
						  int offset,
						  set<LogicalPartition>& partitionNums,
						  string& errMsg,
						  execplan::CalpontSystemCatalog::TableName tableName)
{
	//@Bug 4695
	algorithm::to_lower(tableName.schema);
	if (tableName.schema == "calpontsys")
	{
		errMsg = IDBErrorInfo::instance()->errorMsg(SYSTABLE_PARTITION);
		return;
	}

	partitionNums.clear();

	string partStr = args->args[offset];
	char* partStrNoSpace = (char*)alloca(partStr.length()+1);
	char* tmp = partStrNoSpace;

	// trim off space
	for (uint32_t i = 0; i < partStr.length(); i++)
	{
		if (partStr[i] == ' ' || partStr[i] == '\t')
			continue;
		*tmp = partStr[i];
		++tmp;
	}
	*tmp = 0;
	string parts(partStrNoSpace);

	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep(",");
	boost::char_separator<char> sep1(".");
	tokenizer tokens(parts, sep);
	stringstream ss (stringstream::in | stringstream::out);

	for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
	{
		//cout << "token " << (*tok_iter) << endl;
		LogicalPartition lp;

		tokenizer tokens1((*tok_iter), sep1);
		uint32_t ctn = 0;
		for (tokenizer::iterator tok_iter1 = tokens1.begin(); tok_iter1 != tokens1.end(); ++tok_iter1, ctn++)
		{
			ss << *tok_iter1;
			switch (ctn)
			{
			case 0:
				if (ss >> lp.pp && ss.eof())
					break;
				goto error;
			case 1:
				if (ss >> lp.seg && ss.eof())
					break;
				goto error;
			case 2:
				if (ss >> lp.dbroot && ss.eof())
					break;
				goto error;
			default:
				goto error;
			}
			ss.clear();
			ss.str("");
		}
		partitionNums.insert(lp);
	}

	if (partitionNums.empty())
	{
		goto error;
	}
	return;

error:
	errMsg = "Invalid partition identifier(s)";
	return;

	// debug
	set<LogicalPartition>::const_iterator it;
	cout << "Partition: ";
	for (it = partitionNums.begin(); it != partitionNums.end(); ++it)
		cout << (*it) << endl;
}

int processPartition ( SqlStatement* stmt)
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
			thd->get_stmt_da()->set_overwrite_status(true);
			thd->get_stmt_da()->set_error_status(HA_ERR_INTERNAL_ERROR, "Lost connection to DDLProc", mysql_errno_to_sqlstate(HA_ERR_INTERNAL_ERROR), 0);
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
		thd->get_stmt_da()->set_overwrite_status(true);
		thd->get_stmt_da()->set_error_status(HA_ERR_INTERNAL_ERROR, "Lost connection to DDLProc", mysql_errno_to_sqlstate(HA_ERR_INTERNAL_ERROR), 0);
	}
	catch (...)
	{
		rc = 1;
		thd->get_stmt_da()->set_overwrite_status(true);
		thd->get_stmt_da()->set_error_status(HA_ERR_INTERNAL_ERROR, "Unknown error caught", mysql_errno_to_sqlstate(HA_ERR_INTERNAL_ERROR), 0);
	}

	if (b == ddlpackageprocessor::DDLPackageProcessor::WARN_NO_PARTITION)
	{
		rc = b;
		push_warnings(thd, emsg);
	}
	else if (b == ddlpackageprocessor::DDLPackageProcessor::PARTITION_WARNING)
	{
		rc = 0;
		push_warnings(thd, emsg);
	}
	else if (b ==ddlpackageprocessor::DDLPackageProcessor::WARNING)
	{
		rc = 0;
		string errmsg ("Error occured during partitioning operation. Restart DMLProc or use command tool ddlcleanup to clean up. " );
		push_warnings(thd, errmsg);
	}
	else if (b != 0 && b != ddlpackageprocessor::DDLPackageProcessor::WARN_NO_PARTITION)
	{
		thd->get_stmt_da()->set_overwrite_status(true);
		thd->get_stmt_da()->set_error_status(HA_ERR_INTERNAL_ERROR, emsg.c_str(), mysql_errno_to_sqlstate(HA_ERR_INTERNAL_ERROR), 0);
	}
	return rc;
}

void partitionByValue_common(UDF_ARGS* args,								// input
                             string& errMsg,								// output
                             CalpontSystemCatalog::TableName& tableName,	// output
                             set<LogicalPartition>& partSet,				// output
                             string functionName)							// input
{
	// identify partitions by the range
	DBRM em;
	vector<struct EMEntry> entries;
	vector<struct EMEntry>::iterator iter;
	PartitionMap partMap;
	PartitionMap::iterator mapit;
	int32_t seqNum;
	string schema, table, column;
	CalpontSystemCatalog::ColType ct;
	int64_t startVal, endVal;
	uint8_t rfMin = 0, rfMax = 0;

	if (args->arg_count == 5)
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
			errMsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NO_SCHEMA);
			return;
		}
		table = (char*)(args->args[0]);
		column = (char*)(args->args[1]);
	}

	tableName.schema = schema;
	tableName.table = table;

	//@Bug 4695
	algorithm::to_lower(tableName.schema);
	if (tableName.schema == "calpontsys")
	{
		errMsg = IDBErrorInfo::instance()->errorMsg(SYSTABLE_PARTITION);
		return;
	}

	try
	{
		boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(tid2sid(current_thd->thread_id));
		csc->identity(execplan::CalpontSystemCatalog::FE);
		CalpontSystemCatalog::TableColName tcn = make_tcn(schema, table, column);
		csc->identity(CalpontSystemCatalog::FE);
		OID_t oid = csc->lookupOID(tcn);
		ct = csc->colType(oid);
		if (oid == -1)
		{
			Message::Args args;
			args.add("'" + schema + string(".") + table + string(".") + column + "'");
			errMsg = IDBErrorInfo::instance()->errorMsg(ERR_TABLE_NOT_IN_CATALOG, args);
			return;
		}

		// check casual partition data type
		if (!CP_type(ct))
		{
			Message::Args args;
			args.add(name(ct));
			args.add(functionName);
			errMsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_BY_RANGE, args);
			return;
		}

		if (args->arg_count == 4)
		{
			if (!args->args[2])
			{
				if (isUnsigned(ct.colDataType))
				{
					startVal = 0;
				}
				else
				{
					startVal = numeric_limits<int64_t>::min();
				}
			}
			else
			{
				startVal = IDB_format((char*) args->args[2], ct, rfMin);
			}
			if (!args->args[3])
			{
				if (isUnsigned(ct.colDataType))
				{
					endVal = static_cast<int64_t>(numeric_limits<uint64_t>::max());
				}
				else
				{
					endVal = numeric_limits<int64_t>::max();
				}
			}
			else
			{
				endVal = IDB_format((char*) args->args[3], ct, rfMax);
			}
		}
		else
		{
			if (!args->args[3])
			{
				if (isUnsigned(ct.colDataType))
				{
					startVal = 0;
				}
				else
				{
					startVal = numeric_limits<int64_t>::min();
				}
			}
			else
			{
				startVal = IDB_format((char*) args->args[3], ct, rfMin);
			}
			if (!args->args[4])
			{
				if (isUnsigned(ct.colDataType))
				{
					endVal = static_cast<int64_t>(numeric_limits<uint64_t>::max());
				}
				else
				{
					endVal = numeric_limits<int64_t>::max();
				}
			}
			else
			{
				endVal = IDB_format((char*) args->args[4], ct, rfMax);
			}
		}

		CHECK(em.getExtents(oid, entries, false, false, true));
		if (entries.size() > 0)
		{
			LogicalPartition logicalPartNum;

			for (iter = entries.begin(); iter != entries.end(); ++iter)
			{
				PartitionInfo partInfo;
				logicalPartNum.dbroot = (*iter).dbRoot;
				logicalPartNum.pp = (*iter).partitionNum;
				logicalPartNum.seg = (*iter).segmentNum;
				if (iter->status == EXTENTOUTOFSERVICE)
					partInfo.status |= ET_DISABLED;

				mapit = partMap.find(logicalPartNum);
				int state = em.getExtentMaxMin(iter->range.start, partInfo.max, partInfo.min, seqNum);

				// char column order swap
				if ((ct.colDataType == CalpontSystemCatalog::CHAR && ct.colWidth <= 8) ||
					  (ct.colDataType == CalpontSystemCatalog::VARCHAR && ct.colWidth <= 7))
				{
					partInfo.max = uint64ToStr(partInfo.max);
					partInfo.min = uint64ToStr(partInfo.min);
				}

				if (mapit == partMap.end())
				{
					if (state != CP_VALID)
						partInfo.status |= CPINVALID;
					partMap[logicalPartNum] = partInfo;
				}
				else
				{
					if (mapit->second.status & CPINVALID)
						continue;
					if (isUnsigned(ct.colDataType))
					{
						mapit->second.min = 
						(static_cast<uint64_t>(partInfo.min) < static_cast<uint64_t>(mapit->second.min) ? partInfo.min : mapit->second.min);
						mapit->second.max = 
						(static_cast<uint64_t>(partInfo.max) > static_cast<uint64_t>(mapit->second.max) ? partInfo.max : mapit->second.max);
					}
					else
					{
						mapit->second.min = (partInfo.min < mapit->second.min ? partInfo.min : mapit->second.min);
						mapit->second.max = (partInfo.max > mapit->second.max ? partInfo.max : mapit->second.max);
					}
				}
			}

			// check col value range
			for (mapit = partMap.begin(); mapit != partMap.end(); ++mapit)
			{
				// @bug 4595. check empty/null case
				if (isUnsigned(ct.colDataType))
				{
					if (!(mapit->second.status & CPINVALID) &&
						static_cast<uint64_t>(mapit->second.min) >= static_cast<uint64_t>(startVal) &&
						static_cast<uint64_t>(mapit->second.max) <= static_cast<uint64_t>(endVal) &&
						!(static_cast<uint64_t>(mapit->second.min) == numeric_limits<uint64_t>::max() &&
						  static_cast<uint64_t>(mapit->second.max == 0)))
					{
						if (rfMin == ROUND_POS && mapit->second.min == startVal)
							continue;
						if (rfMax == ROUND_NEG && mapit->second.max == endVal)
							continue;
						partSet.insert(mapit->first);
					}
				}
				else
				{
					if (!(mapit->second.status & CPINVALID) && mapit->second.min >= startVal && mapit->second.max <= endVal &&
					    !(mapit->second.min == numeric_limits<int64_t>::max() && mapit->second.max == numeric_limits<int64_t>::min()))
					{
						if (rfMin == ROUND_POS && mapit->second.min == startVal)
							continue;
						if (rfMax == ROUND_NEG && mapit->second.max == endVal)
							continue;
						partSet.insert(mapit->first);
					}
				}
			}
		}
	}
	catch (QueryDataExcept& ex)
	{
		Message::Args args;
		args.add(ex.what());
		errMsg = IDBErrorInfo::instance()->errorMsg(ERR_INVALID_FUNC_ARGUMENT, args);
		return;
	}
	catch (IDBExcept& ex)
	{
		errMsg = ex.what();
		return;
	}
	catch (...)
	{
		errMsg = string("Error occured when calling ") + functionName;
		return;
	}

	if (partSet.empty())
	{
		errMsg = IDBErrorInfo::instance()->errorMsg(WARN_NO_PARTITION_FOUND);
		return;
	}
}

std::string  ha_calpont_impl_markpartitions_(
     execplan::CalpontSystemCatalog::TableName tableName, set<LogicalPartition>& partitionNums)
{
	ddlpackage::QualifiedName *qualifiedName = new QualifiedName();
	qualifiedName->fSchema = tableName.schema;
	qualifiedName->fName = tableName.table;
	MarkPartitionStatement* stmt = new MarkPartitionStatement(qualifiedName);
	stmt->fSessionID = tid2sid(current_thd->thread_id);
	stmt->fSql = "caldisablepartitions";
	stmt->fOwner = tableName.schema;
	stmt->fPartitions = partitionNums;
	string msg = "Partitions are disabled successfully" ;

	int rc = processPartition( stmt); // warnings will be pushed in the function
	if (rc == ddlpackageprocessor::DDLPackageProcessor::WARN_NO_PARTITION)
		msg = "No partitions are disabled";
	delete stmt;
	return msg;
}

std::string  ha_calpont_impl_restorepartitions_(
     execplan::CalpontSystemCatalog::TableName tableName, set<LogicalPartition>& partitionNums)
{
	ddlpackage::QualifiedName *qualifiedName = new QualifiedName();
	qualifiedName->fSchema = tableName.schema;
	qualifiedName->fName = tableName.table;
	RestorePartitionStatement* stmt = new RestorePartitionStatement(qualifiedName);
	stmt->fSessionID = tid2sid(current_thd->thread_id);
	stmt->fSql = "calenablepartitions";
	stmt->fOwner = tableName.schema;
	stmt->fPartitions = partitionNums;
	string msg;
	int rc = processPartition( stmt);
	if ( rc != 0 )
		return msg;
	msg = "Partitions are enabled successfully." ;

	delete stmt;
	return msg;
}

std::string  ha_calpont_impl_droppartitions_(
     execplan::CalpontSystemCatalog::TableName tableName, set<LogicalPartition>& partitionNums)
{
	ddlpackage::QualifiedName *qualifiedName = new QualifiedName();
	qualifiedName->fSchema = tableName.schema;
	qualifiedName->fName = tableName.table;
	DropPartitionStatement* stmt = new DropPartitionStatement(qualifiedName);
	stmt->fSessionID = tid2sid(current_thd->thread_id);
	stmt->fSql = "caldroppartitions";
	stmt->fOwner = tableName.schema;
	stmt->fPartitions = partitionNums;
	string msg = "Partitions are dropped successfully" ;

	int rc = processPartition( stmt);
	if (rc == ddlpackageprocessor::DDLPackageProcessor::WARN_NO_PARTITION)
		msg = "No partitions are dropped";

	delete stmt;
	return msg;
}

extern "C"
{

/**
 * CalShowPartitions
 */

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

	for (uint32_t i = 0; i < args->arg_count; i++)
	{
		if (!args->args[i])
		{
			strcpy(message,"usage: CALSHOWPARTITIONS ([schema], table, column)");
			return 1;
		}
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calshowpartitions_deinit(UDF_INIT* initid)
{
	delete initid->ptr;
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
	int32_t seqNum;
	string schema, table, column;
	CalpontSystemCatalog::ColType ct;
	string errMsg;

	try
	{
		PartitionMap::iterator mapit;
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
				throw IDBExcept(ERR_PARTITION_NO_SCHEMA);
			}
			table = (char*)(args->args[0]);
			column = (char*)(args->args[1]);
		}

		boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(tid2sid(current_thd->thread_id));
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
			iter = entries.begin();
			end = entries.end();
			LogicalPartition logicalPartNum;
			for (;iter != end; ++iter)
			{
				PartitionInfo partInfo;
				logicalPartNum.dbroot = (*iter).dbRoot;
				logicalPartNum.pp = (*iter).partitionNum;
				logicalPartNum.seg = (*iter).segmentNum;

				if (iter->status == EXTENTOUTOFSERVICE)
					partInfo.status |= ET_DISABLED;

				mapit = partMap.find(logicalPartNum);
				int state = em.getExtentMaxMin(iter->range.start, partInfo.max, partInfo.min, seqNum);

				// char column order swap for compare
				if ((ct.colDataType == CalpontSystemCatalog::CHAR && ct.colWidth <= 8) ||
					  (ct.colDataType == CalpontSystemCatalog::VARCHAR && ct.colWidth <= 7))
				{
					partInfo.max = uint64ToStr(partInfo.max);
					partInfo.min = uint64ToStr(partInfo.min);
				}

				if (mapit == partMap.end())
				{
					if (state != CP_VALID)
						partInfo.status |= CPINVALID;
					partMap[logicalPartNum] = partInfo;
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
		current_thd->get_stmt_da()->set_overwrite_status(true);
		current_thd->get_stmt_da()->set_error_status(HA_ERR_UNSUPPORTED, ex.what(), mysql_errno_to_sqlstate(HA_ERR_UNSUPPORTED), 0);
		return result;
	}
	catch (...)
	{
		current_thd->get_stmt_da()->set_overwrite_status(true);
		current_thd->get_stmt_da()->set_error_status(HA_ERR_UNSUPPORTED, "Error occured when calling CALSHOWPARTITIONS", mysql_errno_to_sqlstate(HA_ERR_UNSUPPORTED), 0);
		return result;
	}

	ostringstream output;
	output.setf(ios::left, ios::adjustfield);
	output << setw(10) << "Part#"
	       << setw(30) << "Min"
	       << setw(30) << "Max" << "Status";

	int64_t maxLimit = numeric_limits<int64_t>::max();
	int64_t minLimit = numeric_limits<int64_t>::min();
	// char column order swap for compare in subsequent loop
	if ((ct.colDataType == CalpontSystemCatalog::CHAR && ct.colWidth <= 8) ||
		(ct.colDataType == CalpontSystemCatalog::VARCHAR && ct.colWidth <= 7))
	{
		maxLimit = uint64ToStr(maxLimit);
		minLimit = uint64ToStr(minLimit);
	}

	PartitionMap::const_iterator partIt;
	for (partIt = partMap.begin(); partIt != partMap.end(); ++partIt)
	{
		ostringstream oss;
		oss << partIt->first;
		output << "\n  " << setw(10) << oss.str();
		if (partIt->second.status & CPINVALID)
		{
			output << setw(30) << "N/A" << setw(30) << "N/A";
		}
		else
		{
			if ((isUnsigned(ct.colDataType)))
			{
				if (static_cast<uint64_t>(partIt->second.min) == numeric_limits<uint64_t>::max() 
				&&  static_cast<uint64_t>(partIt->second.max) == numeric_limits<uint64_t>::min())
					output << setw(30) << "Empty/Null" << setw(30) << "Empty/Null";
				else
					output << setw(30) << format(partIt->second.min, ct) << setw(30) << format(partIt->second.max, ct);
			}
			else
			{
				if (partIt->second.min == maxLimit && partIt->second.max == minLimit)
					output << setw(30) << "Empty/Null" << setw(30) << "Empty/Null";
				else
					output << setw(30) << format(partIt->second.min, ct) << setw(30) << format(partIt->second.max, ct);
			}
		}

		if (partIt->second.status & ET_DISABLED)
			output << "Disabled";
		else
			output << "Enabled";
	}

	// use our own buffer to make sure it fits.
	initid->ptr = new char[output.str().length()+1];
	memcpy(initid->ptr,output.str().c_str(),output.str().length());
	*length = output.str().length();
	return initid->ptr;
}


/**
 * CalDisablePartitions
 */

	#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool caldisablepartitions_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	bool err = false;

	if (args->arg_count < 2 || args->arg_count > 3)
		err = true;
	else if (args->arg_count == 3 && ((args->arg_type[0] != STRING_RESULT ||
				 args->arg_type[1] != STRING_RESULT ||
				 args->arg_type[2] != STRING_RESULT)))
			err = true;
	else if (args->arg_count == 2 && ((args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT)))
			err = true;

	for (uint32_t i = 0; i < args->arg_count; i++)
	{
		if (!args->args[i])
		{
			err = true;
			break;
		}
	}

	if (err)
	{
		strcpy(message,"\nusage: CALDISABLEPARTITIONS (['schemaName'], 'tableName', 'partitionList')");
		return 1;
	}
	initid->maybe_null = 1;
	initid->max_length = 255;
	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* caldisablepartitions(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	CalpontSystemCatalog::TableName tableName;
	set <LogicalPartition> partitionNums;
	string errMsg;
	if ( args->arg_count == 3 )
	{
		tableName.schema = args->args[0];
		tableName.table = args->args[1];
		parsePartitionString(args, 2, partitionNums, errMsg, tableName);
	}
	else
	{
		tableName.table = args->args[0];
		if (!current_thd->db)
		{
			errMsg = "No schema name indicated.";
			memcpy(result, errMsg.c_str(), errMsg.length());
			*length = errMsg.length();
			return result;
		}
		tableName.schema = current_thd->db;
		parsePartitionString(args, 1, partitionNums, errMsg, tableName);
	}

	if (errMsg.empty())
		errMsg = ha_calpont_impl_markpartitions_(tableName,partitionNums );

	memcpy(result,errMsg.c_str(), errMsg.length());
	*length = errMsg.length();
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void caldisablepartitions_deinit(UDF_INIT* initid)
{
}

/**
 * CalEnablePartitions
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calenablepartitions_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	bool err = false;
	if (args->arg_count < 2 || args->arg_count > 3)
		err = true;
	else if (args->arg_count == 3 && ((args->arg_type[0] != STRING_RESULT ||
				 args->arg_type[1] != STRING_RESULT ||
				 args->arg_type[2] != STRING_RESULT)))
			err = true;
	else if (args->arg_count == 2 && ((args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT)))
			err = true;

	for (uint32_t i = 0; i < args->arg_count; i++)
	{
		if (!args->args[i])
		{
			err = true;
			break;
		}
	}

	if (err)
	{
		strcpy(message,"\nusage: CALENABLEPARTITIONS (['schemaName'], 'tableName', 'partitionList')");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;
	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calenablepartitions(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	CalpontSystemCatalog::TableName tableName;
	string errMsg;
	set<LogicalPartition> partitionNums;
	if ( args->arg_count == 3 )
	{
		tableName.schema = args->args[0];
		tableName.table = args->args[1];
		parsePartitionString(args, 2, partitionNums, errMsg, tableName);
	}
	else
	{
		tableName.table = args->args[0];
		if (!current_thd->db)
		{
			current_thd->get_stmt_da()->set_overwrite_status(true);
			current_thd->get_stmt_da()->set_error_status(HA_ERR_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NO_SCHEMA).c_str(), mysql_errno_to_sqlstate(HA_ERR_INTERNAL_ERROR), 0);
			return result;
		}
		tableName.schema = current_thd->db;
		parsePartitionString(args, 1, partitionNums, errMsg, tableName);
	}

	if (errMsg.empty())
		errMsg = ha_calpont_impl_restorepartitions_(tableName,partitionNums );

	memcpy(result, errMsg.c_str(), errMsg.length());
	*length = errMsg.length();
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calenablepartitions_deinit(UDF_INIT* initid)
{
}

/**
 * CalDropPartitions
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool caldroppartitions_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	bool err = false;
	if (args->arg_count < 2 || args->arg_count > 3)
		err = true;
	else if (args->arg_count == 3 && ((args->arg_type[0] != STRING_RESULT ||
			 args->arg_type[1] != STRING_RESULT ||
			 args->arg_type[2] != STRING_RESULT)))
		err = true;
	else if (args->arg_count == 2 && ((args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT)))
		err = true;

	for (uint32_t i = 0; i < args->arg_count; i++)
	{
		if (!args->args[i])
		{
			err = true;
			break;
		}
	}

	if (err)
	{
		strcpy(message,"\nusage: CALDROPPARTITIONS (['schemaName'], 'tableName', 'partitionList')");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;
	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* caldroppartitions(UDF_INIT* initid, UDF_ARGS* args,
                              char* result, unsigned long* length,
                              char* is_null, char* error)
{
	CalpontSystemCatalog::TableName tableName;
	string errMsg;
	set<LogicalPartition> partSet;

	if ( args->arg_count == 3 )
	{
		tableName.schema = args->args[0];
		tableName.table = args->args[1];
		parsePartitionString(args, 2, partSet, errMsg, tableName);
	}
	else
	{
		tableName.table = args->args[0];
		if (!current_thd->db)
		{
			current_thd->get_stmt_da()->set_overwrite_status(true);
			current_thd->get_stmt_da()->set_error_status(HA_ERR_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NO_SCHEMA).c_str(), mysql_errno_to_sqlstate(HA_ERR_INTERNAL_ERROR), 0);
			return result;
		}
		tableName.schema = current_thd->db;
		parsePartitionString(args, 1, partSet, errMsg, tableName);
	}

	if (errMsg.empty())
		errMsg = ha_calpont_impl_droppartitions_(tableName,partSet);

	memcpy(result, errMsg.c_str(), errMsg.length());
	*length = errMsg.length();
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void caldroppartitions_deinit(UDF_INIT* initid)
{
}

/**
 * CalDropPartitionsByValue
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void caldroppartitionsbyvalue_deinit(UDF_INIT* initid)
{
}


#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool caldroppartitionsbyvalue_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	bool err = false;
	if (args->arg_count < 4 || args->arg_count > 5)
	{
		err = true;
	}
	else if (args->arg_count == 4)
	{
		if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT || args->arg_type[2] != STRING_RESULT)
			err = true;
	}
	else if (args->arg_count == 5)
	{
		if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
			  args->arg_type[2] != STRING_RESULT || args->arg_type[3] != STRING_RESULT || args->arg_type[4] != STRING_RESULT)
			err = true;
	}

	if (err)
	{
		string msg = "\nusage: CALDROPPARTITIONSBYVALUE (['schema'], 'table', 'column', 'min', 'max')";
		//message = new char[msg.length()+1];
		strcpy(message, msg.c_str());
		message[msg.length()] = 0;
		//*length = msg.length();
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;
	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* caldroppartitionsbyvalue(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	string msg;
	CalpontSystemCatalog::TableName tableName;
	set<LogicalPartition> partSet;
	partitionByValue_common(args, msg, tableName, partSet, "calDropPartitionsByValue");

	if (!msg.empty())
	{
		current_thd->get_stmt_da()->set_overwrite_status(true);
		current_thd->get_stmt_da()->set_error_status(HA_ERR_INTERNAL_ERROR, msg.c_str(), mysql_errno_to_sqlstate(HA_ERR_INTERNAL_ERROR), 0);
		return result;
	}

	msg = ha_calpont_impl_droppartitions_(tableName,partSet);

	memcpy(result, msg.c_str(), msg.length());
	*length = msg.length();
	return result;
}

/**
 * CalDisablePartitionsByValue
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void caldisablepartitionsbyvalue_deinit(UDF_INIT* initid)
{
}


#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool caldisablepartitionsbyvalue_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	bool err = false;
	if (args->arg_count < 4 || args->arg_count > 5)
	{
		err = true;
	}
	else if (args->arg_count == 4)
	{
		if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT || args->arg_type[2] != STRING_RESULT)
			err = true;
	}
	else if (args->arg_count == 5)
	{
		if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
			  args->arg_type[2] != STRING_RESULT || args->arg_type[3] != STRING_RESULT || args->arg_type[4] != STRING_RESULT)
			err = true;
	}

	if (err)
	{
		strcpy(message,"\nusage: CALDISABLEPARTITIONS (['schema'], 'table', 'column', 'min', 'max')");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;
	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* caldisablepartitionsbyvalue(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	string msg;
	set<LogicalPartition> partSet;
	CalpontSystemCatalog::TableName tableName;
	partitionByValue_common(args, msg, tableName, partSet, "calDisablePartitionsByValue");

	if (!msg.empty())
	{
		current_thd->get_stmt_da()->set_overwrite_status(true);
		current_thd->get_stmt_da()->set_error_status(HA_ERR_INTERNAL_ERROR, msg.c_str(), mysql_errno_to_sqlstate(HA_ERR_INTERNAL_ERROR), 0);
		return result;
	}

	msg = ha_calpont_impl_markpartitions_(tableName, partSet);

	memcpy(result, msg.c_str(), msg.length());
	*length = msg.length();
	return result;
}


/**
 * CalEnablePartitionsByValue
 */
#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calenablepartitionsbyvalue_deinit(UDF_INIT* initid)
{
}


#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calenablepartitionsbyvalue_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	bool err = false;
	if (args->arg_count < 4 || args->arg_count > 5)
	{
		err = true;
	}
	else if (args->arg_count == 4)
	{
		if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT || args->arg_type[2] != STRING_RESULT)
			err = true;
	}
	else if (args->arg_count == 5)
	{
		if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
			  args->arg_type[2] != STRING_RESULT || args->arg_type[3] != STRING_RESULT || args->arg_type[4] != STRING_RESULT)
			err = true;
	}

	if (err)
	{
		strcpy(message,"\nusage: CALENABLEPARTITIONSBYVALUE (['schema'], 'table', 'column', 'min', 'max')");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;
	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calenablepartitionsbyvalue(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	string msg;
	set<LogicalPartition> partSet;
	CalpontSystemCatalog::TableName tableName;
	partitionByValue_common(args, msg, tableName, partSet, "calEnablePartitionsByValue");

	if (!msg.empty())
	{
		current_thd->get_stmt_da()->set_overwrite_status(true);
		current_thd->get_stmt_da()->set_error_status(HA_ERR_INTERNAL_ERROR, msg.c_str(), mysql_errno_to_sqlstate(HA_ERR_INTERNAL_ERROR), 0);
		return result;
	}

	msg = ha_calpont_impl_restorepartitions_(tableName, partSet);

	memcpy(result, msg.c_str(), msg.length());
	*length = msg.length();
	return result;
}

/**
 * CalShowPartitionsByValue
 */
#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calshowpartitionsbyvalue_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	bool err = false;
	if (args->arg_count < 4 || args->arg_count > 5)
	{
		err = true;
	}
	else if (args->arg_count == 4)
	{
		if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT || args->arg_type[2] != STRING_RESULT)
			err = true;
	}
	else if (args->arg_count == 5)
	{
		if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
			  args->arg_type[2] != STRING_RESULT || args->arg_type[3] != STRING_RESULT || args->arg_type[4] != STRING_RESULT)
			err = true;
	}

	if (err)
	{
		strcpy(message,"\nusage: CALSHOWPARTITIONSBYVALUE (['schema'], 'table', 'column', 'min', 'max')");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;
	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calshowpartitionsbyvalue_deinit(UDF_INIT* initid)
{
	delete initid->ptr;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calshowpartitionsbyvalue(UDF_INIT* initid, UDF_ARGS* args,
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
	string schema, table, column;
	CalpontSystemCatalog::ColType ct;
	string errMsg;
	int64_t startVal, endVal;
	uint8_t rfMin = 0, rfMax = 0;

	try
	{
		if (args->arg_count == 5)
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
				throw IDBExcept(ERR_PARTITION_NO_SCHEMA);
			}
			table = (char*)(args->args[0]);
			column = (char*)(args->args[1]);
		}

		boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(tid2sid(current_thd->thread_id));
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

		// check casual partition data type
		if (!CP_type(ct))
		{
			Message::Args args;
			args.add(name(ct));
			args.add("calShowPartitionsByValue");
			throw IDBExcept(ERR_PARTITION_BY_RANGE, args);
		}

		if (args->arg_count == 4)
		{
			if (!args->args[2])
			{
				if (isUnsigned(ct.colDataType))
				{
					startVal = 0;
				}
				else
				{
					startVal = numeric_limits<int64_t>::min();
				}
			}
			else
			{
				startVal = IDB_format((char*) args->args[2], ct, rfMin);
			}
			if (!args->args[3])
			{
				if (isUnsigned(ct.colDataType))
				{
					endVal = static_cast<int64_t>(numeric_limits<uint64_t>::max());
				}
				else
				{
					endVal = numeric_limits<int64_t>::max();
				}
			}
			else
			{
				endVal = IDB_format((char*) args->args[3], ct, rfMax);
			}
		}
		else
		{
			if (!args->args[3])
			{
				if (isUnsigned(ct.colDataType))
				{
					startVal = 0;
				}
				else
				{
					startVal = numeric_limits<int64_t>::min();
				}
			}
			else
			{
				startVal = IDB_format((char*) args->args[3], ct, rfMin);
			}
			if (!args->args[4])
			{
				if (isUnsigned(ct.colDataType))
				{
					endVal = static_cast<int64_t>(numeric_limits<uint64_t>::max());
				}
				else
				{
					endVal = numeric_limits<int64_t>::max();
				}
			}
			else
			{
				endVal = IDB_format((char*) args->args[4], ct, rfMax);
			}
		}

		CHECK(em.getExtents(oid, entries, false, false, true));
		if (entries.size() > 0)
		{
			iter = entries.begin();
			end = entries.end();
			LogicalPartition logicalPartNum;
			for (;iter != end; ++iter)
			{
				PartitionInfo partInfo;
				logicalPartNum.dbroot = (*iter).dbRoot;
				logicalPartNum.pp = (*iter).partitionNum;
				logicalPartNum.seg = (*iter).segmentNum;
				if (iter->status == EXTENTOUTOFSERVICE)
					partInfo.status |= ET_DISABLED;

				mapit = partMap.find(logicalPartNum);
				int state = em.getExtentMaxMin(iter->range.start, partInfo.max, partInfo.min, seqNum);

				// char column order swap
				if ((ct.colDataType == CalpontSystemCatalog::CHAR && ct.colWidth <= 8) ||
					  (ct.colDataType == CalpontSystemCatalog::VARCHAR && ct.colWidth <= 7))
				{
					partInfo.max = uint64ToStr(partInfo.max);
					partInfo.min = uint64ToStr(partInfo.min);
				}
				if (mapit == partMap.end())
				{
					if (state != CP_VALID)
						partInfo.status |= CPINVALID;
					partMap[logicalPartNum] = partInfo;
				}
				else
				{
					if (mapit->second.status & CPINVALID)
						continue;
					if (isUnsigned(ct.colDataType))
					{
						mapit->second.min = 
						  (static_cast<uint64_t>(partInfo.min) < static_cast<uint64_t>(mapit->second.min) ? partInfo.min : mapit->second.min);
						mapit->second.max = 
						  (static_cast<uint64_t>(partInfo.max) > static_cast<uint64_t>(mapit->second.max) ? partInfo.max : mapit->second.max);
					}
					else
					{
						mapit->second.min = (partInfo.min < mapit->second.min ? partInfo.min : mapit->second.min);
						mapit->second.max = (partInfo.max > mapit->second.max ? partInfo.max : mapit->second.max);
					}
				}
			}
		}
	}
	catch (logging::QueryDataExcept& ex)
	{
		Message::Args args;
		args.add(ex.what());
		errMsg = IDBErrorInfo::instance()->errorMsg(ERR_INVALID_FUNC_ARGUMENT, args);
		current_thd->get_stmt_da()->set_overwrite_status(true);
		current_thd->get_stmt_da()->set_error_status(HA_ERR_UNSUPPORTED, errMsg.c_str(), mysql_errno_to_sqlstate(HA_ERR_UNSUPPORTED), 0);
		return result;
	}
	catch (IDBExcept& ex)
	{
		current_thd->get_stmt_da()->set_overwrite_status(true);
		current_thd->get_stmt_da()->set_error_status(HA_ERR_UNSUPPORTED, ex.what(), mysql_errno_to_sqlstate(HA_ERR_UNSUPPORTED), 0);
		return result;
	}
	catch (...)
	{
		current_thd->get_stmt_da()->set_overwrite_status(true);
		current_thd->get_stmt_da()->set_error_status(HA_ERR_UNSUPPORTED, "Error occured when calling CALSHOWPARTITIONS", mysql_errno_to_sqlstate(HA_ERR_UNSUPPORTED), 0);
		return result;
	}

	// create a set of partInfo for sorting.
	bool noPartFound = true;
	ostringstream output;

	for (mapit = partMap.begin(); mapit != partMap.end(); ++mapit)
	{
		// @bug 4595. check empty/null case
		if (!(mapit->second.status & CPINVALID) && mapit->second.min >= startVal && mapit->second.max <= endVal &&
			  !(mapit->second.min == numeric_limits<int64_t>::max() && mapit->second.max == numeric_limits<int64_t>::min()))
		{
			if (rfMin == ROUND_POS && mapit->second.min == startVal)
				continue;
			if (rfMax == ROUND_NEG && mapit->second.max == endVal)
				continue;
			// print header
			if (noPartFound)
			{
				output.setf(ios::left, ios::adjustfield);
				output << setw(10) << "Part#"
					<< setw(30) << "Min"
					<< setw(30) << "Max" << "Status";
			}
			noPartFound = false;

			// print part info
			ostringstream oss;
			oss << mapit->first;
			output << "\n  " << setw(10) << oss.str();
			if (mapit->second.status & CPINVALID)
			{
				output << setw(30) << "N/A" << setw(30) << "N/A";
			}
			else
			{
				if ((isUnsigned(ct.colDataType)))
				{
					if (static_cast<uint64_t>(mapit->second.min) > static_cast<uint64_t>(mapit->second.max))
						output << setw(30) << "Empty/Null" << setw(30) << "Empty/Null";
					else
						output << setw(30) << format(mapit->second.min, ct) << setw(30) << format(mapit->second.max, ct);
				}
				else
				{
					if (mapit->second.min > mapit->second.max)
						output << setw(30) << "Empty/Null" << setw(30) << "Empty/Null";
					else
						output << setw(30) << format(mapit->second.min, ct) << setw(30) << format(mapit->second.max, ct);
				}
			}

			if (mapit->second.status & ET_DISABLED)
				output << "Disabled";
			else
				output << "Enabled";
		}
	}

	if (noPartFound)
	{
		errMsg = IDBErrorInfo::instance()->errorMsg(WARN_NO_PARTITION_FOUND);
		current_thd->get_stmt_da()->set_overwrite_status(true);
		current_thd->get_stmt_da()->set_error_status(HA_ERR_UNSUPPORTED, errMsg.c_str(), mysql_errno_to_sqlstate(HA_ERR_UNSUPPORTED), 0);
		return result;
	}

	// use our own buffer to make sure it fits.
	initid->ptr = new char[output.str().length()+1];
	memcpy(initid->ptr, output.str().c_str(),output.str().length());
	*length = output.str().length();
	return initid->ptr;
}

}

}
