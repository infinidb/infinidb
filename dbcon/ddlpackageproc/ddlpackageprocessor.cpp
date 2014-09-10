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

//   $Id: ddlpackageprocessor.cpp 9627 2013-06-18 13:59:21Z rdempsey $

#include <fstream>
#include <iomanip>
#include <iostream>
//#define NDEBUG
#include <cassert>
using namespace std;

#include "ddlpackageprocessor.h"

#include "dataconvert.h"
using namespace dataconvert;
#include "calpontselectexecutionplan.h"
#include "simplecolumn.h"
#include "constantcolumn.h"
#include "simplefilter.h"
#include "constantfilter.h"
using namespace execplan;
#include "joblist.h"
#include "joblistfactory.h"
#include "distributedenginecomm.h"
using namespace joblist;

#include "we_messages.h"
using namespace WriteEngine;
#include <boost/algorithm/string/case_conv.hpp>
using namespace boost::algorithm;

#include <boost/date_time/gregorian/gregorian.hpp>
using namespace boost::gregorian;

#include "cacheutils.h"
using namespace cacheutils;

#include "brmtypes.h"
using namespace BRM;

#include "bytestream.h"
using namespace messageqcpp;

#include "oamcache.h"
using namespace oam;

namespace
{
using namespace execplan;

const SOP opeq(new Operator("="));
const SOP opne(new Operator("<>"));
const SOP opor(new Operator("or"));
const SOP opand(new Operator("and"));
const SOP opis(new Operator("is"));
}

#include <boost/lexical_cast.hpp>
using boost::lexical_cast;
using boost::any_cast;

using namespace std;
using namespace execplan;
using namespace ddlpackage;

namespace ddlpackageprocessor
{

DDLPackageProcessor::~DDLPackageProcessor()
{
	//cout << "in DDLPackageProcessor destructor " << this << endl;
	delete fWEClient;
}

void  DDLPackageProcessor::getColumnsForTable(uint32_t sessionID, std::string schema,std::string table,
		ColumnList& colList)
{

	CalpontSystemCatalog::TableName tableName;
	tableName.schema = schema;
	tableName.table = table;
	std::string err;
	try
	{
		boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
		systemCatalogPtr->identity(CalpontSystemCatalog::EC);

		const CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(tableName);

		CalpontSystemCatalog::RIDList::const_iterator rid_iterator = ridList.begin();
		while (rid_iterator != ridList.end())
		{
			CalpontSystemCatalog::ROPair roPair = *rid_iterator;

			DDLColumn column;
			column.oid = roPair.objnum;
			column.colType = systemCatalogPtr->colType(column.oid);
			column.tableColName = systemCatalogPtr->colName(column.oid);

			colList.push_back(column);

			++rid_iterator;
		}

	}
	catch (std::exception& ex)
	{

		err = "DDLPackageProcessor::getColumnsForTable: while reading columns for table " +  schema + '.' + table + ": " + ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "DDLPackageProcessor::getColumnsForTable: caught unkown exception!" ;
		throw std::runtime_error(err);
	}

}

execplan::CalpontSystemCatalog::ColDataType DDLPackageProcessor::convertDataType(int dataType)
{
	execplan::CalpontSystemCatalog::ColDataType colDataType;

	switch (dataType)
	{
		case ddlpackage::DDL_CHAR:
			colDataType = CalpontSystemCatalog::CHAR;
			break;

		case ddlpackage::DDL_VARCHAR:
			colDataType = CalpontSystemCatalog::VARCHAR;
			break;

		case ddlpackage::DDL_VARBINARY:
			colDataType = CalpontSystemCatalog::VARBINARY;
			break;

		case ddlpackage::DDL_BINARY:
			colDataType = CalpontSystemCatalog::BIN16;
			break;

		case ddlpackage::DDL_BIT:
			colDataType = CalpontSystemCatalog::BIT;
			break;

		case ddlpackage::DDL_REAL:
		case ddlpackage::DDL_DECIMAL:
		case ddlpackage::DDL_NUMERIC:
		case ddlpackage::DDL_NUMBER:
			colDataType = CalpontSystemCatalog::DECIMAL;
			break;

		case ddlpackage::DDL_FLOAT:
			colDataType = CalpontSystemCatalog::FLOAT;
			break;

		case ddlpackage::DDL_DOUBLE:
			colDataType = CalpontSystemCatalog::DOUBLE;
			break;

		case ddlpackage::DDL_INT:
		case ddlpackage::DDL_INTEGER:
			colDataType = CalpontSystemCatalog::INT;
			break;

		case ddlpackage::DDL_BIGINT:
			colDataType = CalpontSystemCatalog::BIGINT;
			break;

		case ddlpackage::DDL_MEDINT:
			colDataType = CalpontSystemCatalog::MEDINT;
			break;

		case ddlpackage::DDL_SMALLINT:
			colDataType = CalpontSystemCatalog::SMALLINT;
			break;

		case ddlpackage::DDL_TINYINT:
			colDataType = CalpontSystemCatalog::TINYINT;
			break;

        case ddlpackage::DDL_UNSIGNED_DECIMAL:
        case ddlpackage::DDL_UNSIGNED_NUMERIC:
            colDataType = CalpontSystemCatalog::UDECIMAL;
            break;

        case ddlpackage::DDL_UNSIGNED_FLOAT:
            colDataType = CalpontSystemCatalog::UFLOAT;
            break;

        case ddlpackage::DDL_UNSIGNED_DOUBLE:
            colDataType = CalpontSystemCatalog::UDOUBLE;
            break;

        case ddlpackage::DDL_UNSIGNED_INT:
            colDataType = CalpontSystemCatalog::UINT;
            break;

        case ddlpackage::DDL_UNSIGNED_BIGINT:
            colDataType = CalpontSystemCatalog::UBIGINT;
            break;

        case ddlpackage::DDL_UNSIGNED_MEDINT:
            colDataType = CalpontSystemCatalog::UMEDINT;
            break;

        case ddlpackage::DDL_UNSIGNED_SMALLINT:
            colDataType = CalpontSystemCatalog::USMALLINT;
            break;

        case ddlpackage::DDL_UNSIGNED_TINYINT:
            colDataType = CalpontSystemCatalog::UTINYINT;
            break;

		case ddlpackage::DDL_DATE:
			colDataType = CalpontSystemCatalog::DATE;
			break;

		case ddlpackage::DDL_DATETIME:
			colDataType = CalpontSystemCatalog::DATETIME;
			break;

		case ddlpackage::DDL_CLOB:
			colDataType = CalpontSystemCatalog::CLOB;
			break;

		case ddlpackage::DDL_BLOB:
			colDataType = CalpontSystemCatalog::BLOB;
			break;

		default:
			throw runtime_error("Unsupported datatype!");

	}

	return colDataType;
}

std::string DDLPackageProcessor::buildTableConstraintName(const int oid,
		ddlpackage::DDL_CONSTRAINTS type)
{
	std::stringstream oid_number;
	oid_number <<  oid;
	std::string indexName;
	std::string prefix;

	switch (type)
	{
		case ddlpackage::DDL_PRIMARY_KEY:
			//prefix = "pk_";
			// @note this name is supplied by the previous create index statement
			// generated by Oracle. Use Oracle's PK name instead of making up our own
			indexName = fPKName;
			break;
		case ddlpackage::DDL_FOREIGN_KEY:
		case ddlpackage::DDL_REFERENCES:
			prefix = "fk_";
			break;
		case ddlpackage::DDL_UNIQUE:
			prefix = "uk_";
			break;
		case ddlpackage::DDL_CHECK:
			prefix = "ck_";
			break;
		case ddlpackage::DDL_NOT_NULL:
			prefix = "nk_";
			break;

		default:
			throw runtime_error("Unsupported constraint type!");
			break;
	}

	if (type != ddlpackage::DDL_PRIMARY_KEY)
		indexName = prefix + oid_number.str();
	boost::to_lower(indexName);

	return indexName;
}

std::string DDLPackageProcessor::buildColumnConstraintName(const std::string& schema,
		const std::string& table,
		const std::string& column,
		ddlpackage::DDL_CONSTRAINTS type)
{
	std::string indexName;

	std::string prefix;

	switch (type)
	{
		case ddlpackage::DDL_PRIMARY_KEY:
			prefix = "pk_";
			break;
		case ddlpackage::DDL_FOREIGN_KEY:
		case ddlpackage::DDL_REFERENCES:
			prefix = "fk_";
			break;
		case ddlpackage::DDL_UNIQUE:
			prefix = "uk_";
			break;
		case ddlpackage::DDL_CHECK:
			prefix = "ck_";
			break;
		case ddlpackage::DDL_NOT_NULL:
			prefix = "nk_";
			break;

		default:
			throw runtime_error("Unsupported constraint type!");
			break;
	}

	indexName = prefix + schema + "_" +  table +  "_" + column;

	boost::to_lower(indexName);

	return indexName;
}

char DDLPackageProcessor::getConstraintCode(ddlpackage::DDL_CONSTRAINTS type)
{
	char constraint_type;

	switch(type)
	{
		case ddlpackage::DDL_PRIMARY_KEY:
			constraint_type = 'p';
			break;

		case ddlpackage::DDL_REFERENCES:
		case ddlpackage::DDL_FOREIGN_KEY:
			constraint_type = 'f';
			break;

		case ddlpackage::DDL_UNIQUE:
			constraint_type = 'u';
			break;

		case ddlpackage::DDL_CHECK:
			constraint_type = 'c';
			break;

		case ddlpackage::DDL_NOT_NULL:
			constraint_type = 'n';
			break;

		default:
			constraint_type = '0';
			break;
	}

	return constraint_type;
}

boost::any
DDLPackageProcessor::getNullValueForType(const execplan::CalpontSystemCatalog::ColType& colType)
{
	boost::any value;
	switch(colType.colDataType)
	{
		case execplan::CalpontSystemCatalog::BIT:
			break;

		case execplan::CalpontSystemCatalog::TINYINT:
			{
				char tinyintvalue = joblist::TINYINTNULL;
				value = tinyintvalue;

			}
			break;

		case execplan::CalpontSystemCatalog::SMALLINT:
			{
				short smallintvalue = joblist::SMALLINTNULL;
				value = smallintvalue;
			}
			break;

		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
			{
				int intvalue = joblist::INTNULL;
				value = intvalue;
			}
			break;

		case execplan::CalpontSystemCatalog::BIGINT:
			{
				long long bigint = joblist::BIGINTNULL;
				value = bigint;
			}
			break;

		case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
			{
				if (colType.colWidth <= 4)
				{
					short smallintvalue = joblist::SMALLINTNULL;
					value = smallintvalue;
				}
				else if (colType.colWidth <= 9)
				{
					int intvalue = joblist::INTNULL;
					value = intvalue;
				}
				else if (colType.colWidth <= 18)
				{
					long long eightbyte = joblist::BIGINTNULL;
					value = eightbyte;
				}
				else
				{
					WriteEngine::Token nullToken;
					value = nullToken;
				}
			}
			break;
		case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
			{
				uint32_t jlfloatnull = joblist::FLOATNULL;
				float* fp = reinterpret_cast<float*>(&jlfloatnull);
				value = *fp;
			}
			break;

		case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
			{
				uint64_t jldoublenull = joblist::DOUBLENULL;
				double* dp = reinterpret_cast<double*>(&jldoublenull);
				value = *dp;
			}
			break;

		case execplan::CalpontSystemCatalog::DATE:
			{
				int d = joblist::DATENULL;
				value = d;
			}
			break;

		case execplan::CalpontSystemCatalog::DATETIME:
			{
				long long d = joblist::DATETIMENULL;
				value = d;
			}
			break;

		case execplan::CalpontSystemCatalog::CHAR:
			{
				std::string charnull;
				if (colType.colWidth == execplan::CalpontSystemCatalog::ONE_BYTE)
				{
					//charnull = joblist::CHAR1NULL;
					charnull = "\376";
					value = charnull;
				}
				else if (colType.colWidth == execplan::CalpontSystemCatalog::TWO_BYTE)
				{
					//charnull = joblist::CHAR2NULL;
					charnull = "\377\376";
					value = charnull;
				}
				else if (colType.colWidth <= execplan::CalpontSystemCatalog::FOUR_BYTE)
				{
					//charnull = joblist::CHAR4NULL;
					charnull = "\377\377\377\376";
					value = charnull;
				}
				else
				{
					WriteEngine::Token nullToken;
					value = nullToken;
				}

			}
			break;

        case execplan::CalpontSystemCatalog::VARCHAR:
			{
				std::string charnull;
				if (colType.colWidth == execplan::CalpontSystemCatalog::ONE_BYTE)
				{
					//charnull = joblist::CHAR2NULL;
					charnull = "\377\376";
					value = charnull;
				}
				else if (colType.colWidth < execplan::CalpontSystemCatalog::FOUR_BYTE)
				{
					//charnull = joblist::CHAR4NULL;
					charnull = "\377\377\377\376";
					value = charnull;
				}
				else
				{
					WriteEngine::Token nullToken;
					value = nullToken;
				}

			}
			break;

        case execplan::CalpontSystemCatalog::VARBINARY:
			{
				std::string charnull;
				if (colType.colWidth == execplan::CalpontSystemCatalog::ONE_BYTE)
				{
					//charnull = joblist::CHAR2NULL;
					charnull = "\377\376";
					value = charnull;
				}
				else if (colType.colWidth < execplan::CalpontSystemCatalog::FOUR_BYTE)
				{
					//charnull = joblist::CHAR4NULL;
					charnull = "\377\377\377\376";
					value = charnull;
				}
				else
				{
					WriteEngine::Token nullToken;
					value = nullToken;
				}

			}
			break;

        case execplan::CalpontSystemCatalog::UTINYINT:
            {
                uint8_t utinyintvalue = joblist::UTINYINTNULL;
                value = utinyintvalue;

            }
            break;

        case execplan::CalpontSystemCatalog::USMALLINT:
            {
                uint16_t usmallintvalue = joblist::USMALLINTNULL;
                value = usmallintvalue;
            }
            break;

        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UINT:
            {
                uint32_t uintvalue = joblist::UINTNULL;
                value = uintvalue;
            }
            break;

        case execplan::CalpontSystemCatalog::UBIGINT:
            {
                uint64_t ubigint = joblist::UBIGINTNULL;
                value = ubigint;
            }
            break;

		default:
			throw std::runtime_error("getNullValueForType: unkown column data type");
			break;

	}

	return value;
}

bool DDLPackageProcessor::isIndexConstraint(ddlpackage::DDL_CONSTRAINTS type)
{
	bool indexConstraint = false;

	switch(type)
	{
		case ddlpackage::DDL_PRIMARY_KEY:
		// @bug fix for #418, #416. Do not insert into sysindex
		//case ddlpackage::DDL_REFERENCES:
		case ddlpackage::DDL_UNIQUE:
			indexConstraint = true;
			break;

		default:
			break;

	}

	return indexConstraint;
}

void DDLPackageProcessor::getColumnReferences(ddlpackage::TableConstraintDef& tableConstraint,
		ddlpackage::ColumnNameList& columns)
{

	switch(tableConstraint.fConstraintType)
	{
		case ddlpackage::DDL_PRIMARY_KEY:
			{
				ddlpackage::TablePrimaryKeyConstraintDef& pkConstraint =
					dynamic_cast<ddlpackage::TablePrimaryKeyConstraintDef&>(tableConstraint);

				columns = pkConstraint.fColumnNameList;
			}
			break;

		case ddlpackage::DDL_REFERENCES:
		case ddlpackage::DDL_FOREIGN_KEY:
			{
				ddlpackage::TableReferencesConstraintDef& fkConstraint =
					dynamic_cast<ddlpackage::TableReferencesConstraintDef&>(tableConstraint);

				columns = fkConstraint.fColumns;
			}
			break;

		case ddlpackage::DDL_UNIQUE:
			{
				ddlpackage::TableUniqueConstraintDef& ukConstraint =
					dynamic_cast<ddlpackage::TableUniqueConstraintDef&>(tableConstraint);
				columns = ukConstraint.fColumnNameList;
			}
			break;

		default:
			break;
	}

}

boost::any DDLPackageProcessor::tokenizeData(execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result,
		const execplan::CalpontSystemCatalog::ColType& colType,
		const boost::any& data)
{
	std::string err("DDLPackageProcessor::tokenizeData ");
	SUMMARY_INFO(err);
	boost::any value;
	if (result.result == NO_ERROR)
	{

		try
		{
			std::string str;

			if (data.type() == typeid(int))
				str = lexical_cast<std::string>(any_cast<int>(data));
			else if (data.type() == typeid(float))
				str = lexical_cast<std::string>(any_cast<float>(data));
			else if (data.type() == typeid(long long))
				str = lexical_cast<std::string>(any_cast<long long>(data));
			else if (data.type() == typeid(double))
				str = lexical_cast<std::string>(any_cast<double>(data));
			else if (data.type() == typeid(bool))
				str = lexical_cast<std::string>(any_cast<bool>(data));
			else if (data.type() == typeid(short))
				str = lexical_cast<std::string>(any_cast<short>(data));
			else if (data.type() == typeid(char))
				str = lexical_cast<std::string>(any_cast<char>(data));
			else
				str = any_cast<std::string>(data);

			//Tokenize the data value
			WriteEngine::DctnryStruct dictStruct;
			dictStruct.dctnryOid = colType.ddn.dictOID;
			//added for multifiles per oid
			dictStruct.columnOid = colType.columnOID;
			WriteEngine::DctnryTuple  dictTuple;
			memcpy(dictTuple.sigValue, str.c_str(), str.length());
			dictTuple.sigSize = str.length();
			int error = NO_ERROR;
			if (NO_ERROR != (error = fWriteEngine.tokenize(txnID, dictStruct, dictTuple, false))) // @bug 5572 HDFS tmp file
			{
		WErrorCodes ec;
				throw std::runtime_error("WE: Tokenization failed " + ec.errorString(error));
			}
			WriteEngine::Token aToken = dictTuple.token;

			value = aToken;

		}
		catch (std::exception& ex)
		{
			err += ex.what();
			throw std::runtime_error(err);
		}
		catch(...)
		{
		err += "Unknown exception caught, tokenizeData failed.";
				throw std::runtime_error(err);

		}
	}
	return value;
}

void DDLPackageProcessor::flushPrimprocCache(std::vector<execplan::CalpontSystemCatalog::OID>& oidList)
{
	SUMMARY_INFO("DDLPackageProcessor::flushPrimprocCache");

	int err = 0;
	CalpontSystemCatalog::ROPair roPair;
	std::vector<CalpontSystemCatalog::OID>::const_iterator iter = oidList.begin();
	std::string error;
	BRM::DBRM dbrm;
	BRM::LBIDRange_v lbidRanges;
	LBIDRange_v::iterator it;
	BRM::BlockList_t blockList;
	execplan::CalpontSystemCatalog::SCN verID = 0;
	try
	{
		while (iter != oidList.end())
		{
			WriteEngine::OID oid = *iter;
			if (oid < 3000)
			{
				++iter;
			   continue;
			}
			//@Bug 1708 Flush primproc cache for associated lbids.
			err = dbrm.lookup(oid, lbidRanges);
			if (err)
			{
				error = "DBRM lookUp error.";
				throw std::runtime_error(error);
			}
			blockList.clear();
			for (it = lbidRanges.begin(); it != lbidRanges.end(); it++)
			{
				for (LBID_t  lbid = it->start; lbid < (it->start + it->size); lbid++)
				{
					blockList.push_back(BRM::LVP_t(lbid, verID));
				}
			}
			//Need find a more efficient way to do this.
			err = cacheutils::flushPrimProcBlocks (blockList);
			//No need to handle this error as the error comes from timeout, not real error
			(void)err;

			++iter;
		}
	}
	catch (std::exception& ex)
	{
		error = ex.what();
		throw std::runtime_error(error);
	}
	catch (...)
	{
		error = "Unknown exception caught";
		throw std::runtime_error(error);
	}
}



void DDLPackageProcessor::removeFiles(const uint64_t uniqueId, std::vector<execplan::CalpontSystemCatalog::OID>& oidList)
{
	SUMMARY_INFO("DDLPackageProcessor::removeFiles");
	ByteStream bytestream;
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	fWEClient->addQueue(uniqueId);
	bytestream << (ByteStream::byte)WE_SVR_WRITE_DROPFILES;
	bytestream << uniqueId;
	bytestream << (uint32_t) oidList.size();
	for (unsigned i=0; i < oidList.size(); i++)
	{
		bytestream << (uint32_t) oidList[i];
	}
		
	uint32_t msgRecived = 0;
	ByteStream::byte rc = 0;
	ByteStream::byte tmp8;
	string errorMsg;
	try {
		fWEClient->write_to_all(bytestream);

		bsIn.reset(new ByteStream());
				
		while (1)
		{
			if (msgRecived == fWEClient->getPmCount())
				break;
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
						rc = NETWORK_ERROR;
						errorMsg = "Network error while deleting files.";
						fWEClient->removeQueue(uniqueId);
						break;
			}			
			else {
				*bsIn >> tmp8;
				rc = tmp8;
				if (rc != 0) {
					*bsIn >> errorMsg;
					fWEClient->removeQueue(uniqueId);
					break;
				}
				else
					msgRecived++;						
			}
		}
	}
	catch (std::exception& ex)
	{
		fWEClient->removeQueue(uniqueId);
		throw std::runtime_error(ex.what());
	}
	catch (...)
	{
		fWEClient->removeQueue(uniqueId);
		throw std::runtime_error("Unknown error caught while deleting files.");
	}
	fWEClient->removeQueue(uniqueId);
	if ( rc != 0)
	{
		throw std::runtime_error(errorMsg);
	}
}

void DDLPackageProcessor::createFiles(CalpontSystemCatalog::TableName aTableName, const int useDBRoot, 
	const uint64_t uniqueId, const uint32_t numOids)
{
	SUMMARY_INFO("DDLPackageProcessor::createFiles");
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(1);
	CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(aTableName);
	fWEClient->addQueue(uniqueId);
	CalpontSystemCatalog::ColType colType;
	ByteStream bytestream;
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bytestream << (ByteStream::byte)WE_SVR_WRITE_CREATETABLEFILES;
	bytestream << (uint32_t)1;
	bytestream << uniqueId;
	bytestream << numOids;
	for (unsigned col  = 0; col < ridList.size(); col++)
	{						
		colType = systemCatalogPtr->colType(ridList[col].objnum);
		bytestream << (uint32_t) ridList[col].objnum;
		bytestream << (uint8_t) colType.colDataType;
		bytestream << (uint8_t) false;
		bytestream << (uint32_t) colType.colWidth;
		bytestream << (uint16_t) useDBRoot;
		bytestream << (uint32_t) colType.compressionType;
		if (colType.ddn.dictOID > 3000)
		{
			bytestream << (uint32_t) colType.ddn.dictOID;
			bytestream << (uint8_t) colType.colDataType;
			bytestream << (uint8_t) true;
			bytestream << (uint32_t) colType.colWidth;
			bytestream << (uint16_t) useDBRoot;
			bytestream << (uint32_t) colType.compressionType;
		}
	}
	ByteStream::byte rc = 0;
	ByteStream::byte tmp8;
	string errorMsg;
	try {
		OamCache * oamcache = OamCache::makeOamCache();
		boost::shared_ptr<std::map<int, int> > dbRootPMMap = oamcache->getDBRootToPMMap();
		int pmNum = (*dbRootPMMap)[useDBRoot];
			
		fWEClient->write(bytestream, (uint32_t)pmNum);
		bsIn.reset(new ByteStream());
				
		while (1)
		{
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = NETWORK_ERROR;
				errorMsg = "Network error while creating files.";
				fWEClient->removeQueue(uniqueId);
				break;
			}			
			else {
				*bsIn >> tmp8;
				rc = tmp8;
				if (rc != 0) {
					*bsIn >> errorMsg;
				}
				break;						
			}
		}
	}
	catch (std::exception& ex)
	{
		fWEClient->removeQueue(uniqueId);
		throw std::runtime_error(ex.what());
	}
	catch (...)
	{
		fWEClient->removeQueue(uniqueId);
		throw std::runtime_error("Unknown error caught while creating files.");
	}
	fWEClient->removeQueue(uniqueId);
	if ( rc != 0)
		throw std::runtime_error(errorMsg);
}

void DDLPackageProcessor::removePartitionFiles(std::vector<execplan::CalpontSystemCatalog::OID>& oidList, 
	                                             const PartitionNums& partitions, 
	                                             uint64_t uniqueId)
{
	SUMMARY_INFO("DDLPackageProcessor::removeFiles");

	ByteStream::byte rc = 0;
	std::string errorMsg;
	
	//get a unique number.
	fWEClient->addQueue(uniqueId);
	// Write the tables metadata to the system catalog
	VERBOSE_INFO("Remove Partition Files");
	ByteStream bs;
	bs << (ByteStream::byte)WE_SVR_DROP_PARTITIONS;
	bs << uniqueId;
	bs << (uint32_t)oidList.size();
	PartitionNums::const_iterator partIt;
	vector<BRM::PartitionInfo> partInfos;

	for (uint32_t i = 0; i <  oidList.size(); i++)
	{
		bs << (uint32_t)oidList[i];
		// add oid to LogicalPartition to form PartitionInfo
		for (partIt = partitions.begin(); partIt != partitions.end(); ++partIt)
		{
			PartitionInfo pi;
			pi.lp = (*partIt);
			pi.oid = oidList[i];
			partInfos.push_back(pi);
		}
	}

	bs << (uint32_t)partInfos.size();
	for (uint32_t i = 0; i < partInfos.size(); i++)
		partInfos[i].serialize(bs);

	fWEClient->write_to_all(bs);
	uint32_t pmCount = fWEClient->getPmCount();
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bsIn.reset(new ByteStream());

	while (pmCount)
	{
		bsIn->restart();
		fWEClient->read(uniqueId, bsIn);
		if ( bsIn->length() == 0 ) //read error
		{
			rc = NETWORK_ERROR;
			errorMsg = "Lost connection to Write Engine Server while dropping partitions";
			break;
		}
		else 
		{
			*bsIn >> rc;
			if (rc != 0)
			{
				*bsIn >> errorMsg;
				break;
			}
			pmCount--;
		}
	}

	if (rc)
	{
		WErrorCodes ec;
		errorMsg = "WE: Error removing files " + ec.errorString(rc);
		rc = cacheutils::dropPrimProcFdCache();
		fWEClient->removeQueue(uniqueId);
		throw std::runtime_error(errorMsg);
	}
	//@Bug 2171,3327. Drop PrimProc fd cache
	rc = cacheutils::dropPrimProcFdCache();
	fWEClient->removeQueue(uniqueId);
}

void DDLPackageProcessor::removeExtents(std::vector<execplan::CalpontSystemCatalog::OID>& oidList)
{
	SUMMARY_INFO("DDLPackageProcessor::removeExtents");
	int err = 0;
	err = fDbrm->deleteOIDs(oidList);
	if (err)
	{
		string errMsg;
		BRM::errString(err, errMsg);
		throw std::runtime_error(errMsg);
	}

}

void DDLPackageProcessor::createWriteDropLogFile(execplan::CalpontSystemCatalog::OID tableOid,  
	uint64_t uniqueId, std::vector<execplan::CalpontSystemCatalog::OID>& oidList)
{
	SUMMARY_INFO("DDLPackageProcessor::createWriteDropLogFile");
	//For shared nothing, the meta files are created under data1 with controllernode.
	OamCache * oamcache = OamCache::makeOamCache();
	std::string OAMParentModuleName = oamcache->getOAMParentModuleName();
	OAMParentModuleName = OAMParentModuleName.substr(2, OAMParentModuleName.length());
	int parentId = atoi(OAMParentModuleName.c_str());
	ByteStream bytestream;
	uint8_t rc = 0;
	std::string errorMsg;
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bytestream << (ByteStream::byte)WE_SVR_WRITE_DROPTABLE;
	bytestream << uniqueId;
	bytestream << (uint32_t)tableOid;
	bytestream << (uint32_t) oidList.size();
	for (uint32_t i=0; i < oidList.size(); i++)
	{
		bytestream << (uint32_t)oidList[i];
	}
	try {
		fWEClient->write(bytestream, (uint32_t)parentId);
		while (1)
		{
			bsIn.reset(new ByteStream());
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = NETWORK_ERROR;
				errorMsg = "Lost connection to Write Engine Server while writting drop table Log";
				break;
			}			
			else {
				*bsIn >> rc;
				if (rc != 0) {
					*bsIn >> errorMsg;
				}
				break;
			}
		}
	}
	catch (runtime_error& ex) //write error
	{
		rc = NETWORK_ERROR;
		errorMsg = ex.what();
	}
	catch (...)
	{
		rc = NETWORK_ERROR;
		errorMsg = "Got unknown exception while writting drop table Log." ;
	}

	if ( rc != 0)
		throw std::runtime_error(errorMsg);
}

void DDLPackageProcessor::deleteLogFile(LogFileType fileType, execplan::CalpontSystemCatalog::OID tableOid, uint64_t uniqueId)
{
	SUMMARY_INFO("DDLPackageProcessor::deleteLogFile");
	OamCache * oamcache = OamCache::makeOamCache();
	std::string OAMParentModuleName = oamcache->getOAMParentModuleName();
	OAMParentModuleName = OAMParentModuleName.substr(2, OAMParentModuleName.length());
	int parentId = atoi(OAMParentModuleName.c_str());
	ByteStream bytestream;
	uint8_t rc = 0;
	std::string errorMsg;
	fWEClient->addQueue(uniqueId);
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bytestream << (ByteStream::byte)WE_SVR_DELETE_DDLLOG;
	bytestream << uniqueId;
	bytestream << (uint32_t) fileType;
	bytestream << (uint32_t)tableOid;
	try {
		fWEClient->write(bytestream, (uint32_t)parentId);
		while (1)
		{
			bsIn.reset(new ByteStream());
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = NETWORK_ERROR;
				errorMsg = "Lost connection to Write Engine Server while deleting DDL log";
				break;
			}			
			else {
				*bsIn >> rc;
				if (rc != 0) {
					*bsIn >> errorMsg;
				}
				break;
			}
		}
	}
	catch (runtime_error& ex) //write error
	{
		rc = NETWORK_ERROR;
		errorMsg = ex.what();
	}
	catch (...)
	{
		rc = NETWORK_ERROR;
		errorMsg = "Got unknown exception while deleting DDL Log." ;
	}
	fWEClient->removeQueue(uniqueId);
	if ( rc != 0)
	{
		throw std::runtime_error(errorMsg);
	}
}

void DDLPackageProcessor::fetchLogFile(TableLogInfo & tableLogInfos, uint64_t uniqueId)
{
	SUMMARY_INFO("DDLPackageProcessor::fetchLogFile");
	OamCache * oamcache = OamCache::makeOamCache();
	std::string OAMParentModuleName = oamcache->getOAMParentModuleName();
	//Use a sensible default so that substr doesn't throw...
	if (OAMParentModuleName.empty())
		OAMParentModuleName = "pm1";
	int parentId = atoi(OAMParentModuleName.substr(2, OAMParentModuleName.length()).c_str());
	ByteStream bytestream;
	uint8_t rc = 0;
	uint32_t tmp32, tableOid, numOids, numPartitions;
	LogFileType logFileType;
	std::string errorMsg;
	fWEClient->addQueue(uniqueId);
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bytestream << (ByteStream::byte)WE_SVR_FETCH_DDL_LOGS;
	bytestream << uniqueId;
	try {
		fWEClient->write(bytestream, (uint32_t)parentId);
		while (1)
		{
			bsIn.reset(new ByteStream());
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = NETWORK_ERROR;
				errorMsg = "Lost connection to Write Engine Server while deleting DDL log";
				break;
			}			
			else {
				*bsIn >> rc;
				*bsIn >> errorMsg;
				while ( bsIn->length() > 0 )
				{
					*bsIn >> tmp32;
					tableOid = tmp32;
					*bsIn >> tmp32;
					logFileType = (LogFileType)tmp32;
					*bsIn >> tmp32;
					numOids = tmp32;
					OidList oidsList;
					PartitionNums partitionNums;
					for (unsigned i=0; i < numOids; i++)
					{
						*bsIn >> tmp32;
						oidsList.push_back(tmp32);
					}
					*bsIn >> tmp32;
					numPartitions = tmp32;
					BRM::LogicalPartition lp;
					for (unsigned i=0; i < numPartitions; i++)
					{
						lp.unserialize(*bsIn);
						partitionNums.insert(lp);
					}
					//build the tableloginfo
					LogInfo aLog;
					aLog.fileType = logFileType;
					aLog.oids = oidsList;
					aLog.partitionNums = partitionNums;
					tableLogInfos[tableOid] = aLog;
				}
				break;
			}
		}
	}
	catch (runtime_error& ex) //write error
	{
		rc = NETWORK_ERROR;
		errorMsg = ex.what();
	}
	catch (...)
	{
		rc = NETWORK_ERROR;
		errorMsg = "Got unknown exception while fetching DDL Log." ;
	}
	fWEClient->removeQueue(uniqueId);
	if ( rc != 0)
		throw std::runtime_error(errorMsg);
		
}

void DDLPackageProcessor::createWritePartitionLogFile(execplan::CalpontSystemCatalog::OID tableOid,  
	                                                   const PartitionNums& partitionNums,
	                                                   std::vector<execplan::CalpontSystemCatalog::OID>& oidList, uint64_t uniqueId)
{
	SUMMARY_INFO("DDLPackageProcessor::createWritePartitionLogFile");
	fWEClient->addQueue(uniqueId);
	OamCache * oamcache = OamCache::makeOamCache();
	std::string OAMParentModuleName = oamcache->getOAMParentModuleName();
	OAMParentModuleName = OAMParentModuleName.substr(2, OAMParentModuleName.length());
	int parentId = atoi(OAMParentModuleName.c_str());
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	ByteStream bytestream;
	std::string errorMsg;
	uint8_t rc = 0;
	bytestream << (ByteStream::byte)WE_SVR_WRITE_DROPPARTITION;
	bytestream << uniqueId;
	bytestream << (uint32_t)tableOid;
	bytestream << (uint32_t) partitionNums.size();
	PartitionNums::const_iterator it;
	
	for (it = partitionNums.begin(); it != partitionNums.end(); ++it)
		(*it).serialize(bytestream);
		
	bytestream << (uint32_t) oidList.size();
	for (uint32_t i=0; i < oidList.size(); i++)
	{
		bytestream << (uint32_t)oidList[i];
	}
	try {
		fWEClient->write(bytestream, (uint32_t)parentId);
		while (1)
		{
			bsIn.reset(new ByteStream());
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = NETWORK_ERROR;
				errorMsg = "Lost connection to Write Engine Server while writing DDL drop partition log";
				break;
			}			
			else {
				*bsIn >> rc;
				if (rc != 0) {
					*bsIn >> errorMsg;
				}
				break;
			}
		}
	}
	catch (runtime_error& ex) //write error
	{
		rc = NETWORK_ERROR;
		errorMsg = ex.what();
	}
	catch (...)
	{
		rc = NETWORK_ERROR;
		errorMsg = "Got unknown exception while writting truncate Log." ;
	}
	fWEClient->removeQueue(uniqueId);
	if ( rc != 0)
		throw std::runtime_error(errorMsg);
}

void DDLPackageProcessor::createWriteTruncateTableLogFile(execplan::CalpontSystemCatalog::OID tableOid,  uint64_t uniqueId, std::vector<execplan::CalpontSystemCatalog::OID>& oidList)
{
	SUMMARY_INFO("DDLPackageProcessor::createWriteTruncateTableLogFile");
	//For shared nothing, the meta files are created under data1 with controllernode.
	OamCache * oamcache = OamCache::makeOamCache();
	std::string OAMParentModuleName = oamcache->getOAMParentModuleName();
	OAMParentModuleName = OAMParentModuleName.substr(2, OAMParentModuleName.length());
	int parentId = atoi(OAMParentModuleName.c_str());
	ByteStream bytestream;
	uint8_t rc = 0;
	std::string errorMsg;
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bytestream << (ByteStream::byte)WE_SVR_WRITE_TRUNCATE;
	bytestream << uniqueId;
	bytestream << (uint32_t)tableOid;
	bytestream << (uint32_t) oidList.size();
	for (uint32_t i=0; i < oidList.size(); i++)
	{
		bytestream << (uint32_t)oidList[i];
	}
	try {
		fWEClient->write(bytestream, (uint32_t)parentId);
		while (1)
		{
			bsIn.reset(new ByteStream());
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = NETWORK_ERROR;
				errorMsg = "Lost connection to Write Engine Server while writing truncate table log";
				break;
			}			
			else {
				*bsIn >> rc;
				if (rc != 0) {
					*bsIn >> errorMsg;
				}
				break;
			}
		}
	}
	catch (runtime_error& ex) //write error
	{
		rc = NETWORK_ERROR;
		errorMsg = ex.what();
	}
	catch (...)
	{
		rc = NETWORK_ERROR;
		errorMsg = "Got unknown exception while writting truncate table Log." ;
	}

	if ( rc != 0)
		throw std::runtime_error(errorMsg);
}

#if 0
void DDLPackageProcessor::createOpenTruncateTableLogFile(execplan::CalpontSystemCatalog::OID tableOid, execplan::CalpontSystemCatalog::TableName tableName)
{
	SUMMARY_INFO("DDLPackageProcessor::createOpenTruncateTableLogFile");
	//Build file name with tableOid. Currently, table oid is not returned and therefore not reused
	string prefix, error;
	config::Config *config = config::Config::makeConfig();
	prefix = config->getConfig("SystemConfig", "DBRMRoot");
	if (prefix.length() == 0) {
		error = "Need a valid DBRMRoot entry in Calpont configuation file";
		throw std::runtime_error(error);
	}

	uint64_t pos =  prefix.find_last_of ("/") ;
	if (pos != string::npos)
	{
		fDDLLogFileName = prefix.substr(0, pos+1); //Get the file path
	}
	else
	{
		error = "Cannot find the dbrm directory for the DDL log file";
		throw std::runtime_error(error);

	}
	std::ostringstream oss;
	oss << tableOid;
	fDDLLogFileName += "DDL_TRUNCATETABLE_Log_" + oss.str();
	fDDLLogFile.open(fDDLLogFileName.c_str(), ios::out);

	if (!fDDLLogFile)
	{
		error = "DDL truncate table log file cannot be created";
		throw std::runtime_error(error);
	}
}

void DDLPackageProcessor::removeIndexFiles(execplan::CalpontSystemCatalog::SCN txnID,
		DDLResult& result,
		execplan::CalpontSystemCatalog::IndexOIDList& idxOIDList)
{
/*	SUMMARY_INFO("DDLPackageProcessor::removeIndexFiles");

	if (result.result != NO_ERROR)
		return;

	int err = 0;
	CalpontSystemCatalog::IndexOID idxOID;
	CalpontSystemCatalog::IndexOIDList::const_iterator iter = idxOIDList.begin();
	std::string error;
	try
	{
		while(iter != idxOIDList.end())
		{
			idxOID = *iter;
			if (idxOID.objnum < 3000 || idxOID.listOID < 3000)
			{
				++iter;
				continue;
			}
			err = -1;
			if (err)
			{
		WErrorCodes ec;
				error = "WE: Error removing index files: " + getFileName(idxOID.objnum) + ", " + getFileName(idxOID.listOID) + ". error = " + ec.errorString(err);
				throw std::runtime_error(error);
			}

			++iter;
		}
	}
	catch (std::exception& ex)
	{
		error = ex.what();
		throw std::runtime_error(error);
	}
	catch (...)
	{
		error = "Unknown exception caught";
		throw std::runtime_error(error);
	}
*/
}



void DDLPackageProcessor::updateSyscolumns(execplan::CalpontSystemCatalog::SCN txnID,
		DDLResult& result, WriteEngine::RIDList& ridList,
		WriteEngine::ColValueList& colValuesList,
		WriteEngine::ColValueList& colOldValuesList)
{
	SUMMARY_INFO("DDLPackageProcessor::updateSyscolumns");

	if (result.result != NO_ERROR)
		return;

    WriteEngine::ColStructList  colStructs;
	//std::vector<ColStruct> colStructs;
	WriteEngine::ColStruct colStruct;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	//Build column structure for COLUMNPOS_COL
	colStruct.dataOid = OID_SYSCOLUMN_COLUMNPOS;
	colStruct.colWidth = 4;
	colStruct.tokenFlag = false;
	colStruct.colDataType = CalpontSystemCatalog::INT;
	colStructs.push_back(colStruct);
	int error;
	std::string err;
	std::vector<void *> colOldValuesList1;

	try
	{
		//@Bug 3051 use updateColumnRecs instead of updateColumnRec to use different value for diffrent rows.
		if (NO_ERROR != (error = fWriteEngine.updateColumnRecs( txnID, colStructs, colValuesList, ridList )))
		{
			// build the logging message
			WErrorCodes ec;
			err = "WE: Failed on update SYSCOLUMN table. " + ec.errorString(error);
			throw std::runtime_error(err);
		}
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "updateSyscolumns:Unknown exception caught";
		throw std::runtime_error(err);
	}

}

#endif
void DDLPackageProcessor::returnOIDs(execplan::CalpontSystemCatalog::RIDList& ridList,
									 execplan::CalpontSystemCatalog::DictOIDList& dictOIDList)
{
	CalpontSystemCatalog::ROPair roPair;
	CalpontSystemCatalog::RIDList::const_iterator col_iter = ridList.begin();
	std::string err;
	try
	{
		execplan::ObjectIDManager fObjectIDManager;
		while (col_iter != ridList.end())
		{
			roPair = *col_iter;
			if (roPair.objnum < 3000)
			{
				++col_iter;
				continue;
			}

			fObjectIDManager.returnOID(roPair.objnum);
			++col_iter;
		}

		CalpontSystemCatalog::DictOID dictOID;
		CalpontSystemCatalog::DictOIDList::const_iterator dict_iter = dictOIDList.begin();
		while (dict_iter != dictOIDList.end())
		{
			dictOID = *dict_iter;
			if (dictOID.dictOID < 3000)
			{
				++dict_iter;
				continue;
			}
			fObjectIDManager.returnOID(dictOID.dictOID);
			++dict_iter;
		}
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "returnOIDs:Unknown exception caught";
		throw std::runtime_error(err);
	}
}

void DDLPackageProcessor::findColumnData(uint32_t sessionID, execplan::CalpontSystemCatalog::TableName& systableName,
		const std::string& colName,
		DDLColumn& sysCol)
{
	ColumnList columns;
	ColumnList::const_iterator column_iterator;
	std::string err;
	try
	{
		getColumnsForTable(sessionID, systableName.schema,systableName.table, columns);
		column_iterator = columns.begin();
		while (column_iterator != columns.end())
		{
			sysCol = *column_iterator;
			boost::to_lower(sysCol.tableColName.column);

			if (colName == sysCol.tableColName.column)
			{
				break;
			}
			++column_iterator;
		}
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "findColumnData:Unknown exception caught";
		throw std::runtime_error(err);
	}
}

void DDLPackageProcessor::cleanString(string& s)
{
	string::size_type pos = s.find_first_not_of(" ");
	//stripe off space and ' or '' at beginning and end
	if (pos < s.length())
	{
		s = s.substr(pos, s.length()-pos);
		if ((pos = s.find_last_of(" ")) < s.length())
		{
			s = s.substr(0, pos);
		}

	}
	if  (s[0] == '\'')
	{
		s = s.substr(1, s.length()-2);
		if  (s[0] == '\'')
			s = s.substr(1, s.length()-2);
	}
}

void DDLPackageProcessor::convertRidToColumn(uint64_t& rid, unsigned& dbRoot, unsigned& partition,
				unsigned& segment, unsigned filesPerColumnPartition,
				unsigned  extentsPerSegmentFile, unsigned extentRows,
				unsigned startDBRoot, unsigned dbrootCnt)
{
	partition = rid / (filesPerColumnPartition * extentsPerSegmentFile * extentRows);

	segment = (((rid % (filesPerColumnPartition * extentsPerSegmentFile * extentRows)) / extentRows)) % filesPerColumnPartition;

	dbRoot = ((startDBRoot - 1 + segment) % dbrootCnt) + 1;

	//Calculate the relative rid for this segment file
	uint64_t relRidInPartition = rid - ((uint64_t)partition * (uint64_t)filesPerColumnPartition * (uint64_t)extentsPerSegmentFile * (uint64_t)extentRows);
	idbassert(relRidInPartition <= (uint64_t)filesPerColumnPartition * (uint64_t)extentsPerSegmentFile * (uint64_t)extentRows);
	uint32_t numExtentsInThisPart = relRidInPartition / extentRows;
	unsigned numExtentsInThisSegPart = numExtentsInThisPart / filesPerColumnPartition;
	uint64_t relRidInThisExtent = relRidInPartition - numExtentsInThisPart * extentRows;
	rid = relRidInThisExtent +  numExtentsInThisSegPart * extentRows;
}

int DDLPackageProcessor::rollBackTransaction(uint64_t uniqueId, BRM::TxnID txnID, uint32_t sessionID)
{
	ByteStream bytestream;
	bytestream << (ByteStream::byte) WE_SVR_ROLLBACK_BLOCKS;
	bytestream << uniqueId;
	bytestream << sessionID;
	bytestream << (uint32_t)txnID.id;
	uint32_t msgRecived = 0;
	fWEClient->write_to_all(bytestream);
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bsIn.reset(new ByteStream());
	int rc = 0;
	ByteStream::byte tmp8;
	std::string errorMsg;
	while (1)
	{
		if (msgRecived == fWEClient->getPmCount())
			break;
		fWEClient->read(uniqueId, bsIn);
		if ( bsIn->length() == 0 ) //read error
		{
			rc = NETWORK_ERROR;
			fWEClient->removeQueue(uniqueId);
			break;
		}			
		else {
			*bsIn >> tmp8;
			rc = tmp8;
			if (rc != 0) {
				*bsIn >> errorMsg;
				fWEClient->removeQueue(uniqueId);
				break;
			}
			else
				msgRecived++;						
		}
	}
					
	if ((msgRecived == fWEClient->getPmCount()) && (rc == 0))
	{
		bytestream.restart();
		bytestream << (ByteStream::byte) WE_SVR_ROLLBACK_VERSION;
		bytestream << uniqueId;
		bytestream << sessionID;
		bytestream << (uint32_t) txnID.id;
		fWEClient->write_to_all(bytestream);
		bsIn.reset(new ByteStream());
		msgRecived = 0;
		while (1)
		{
			if (msgRecived == fWEClient->getPmCount())
				break;
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				fWEClient->removeQueue(uniqueId);
				break;
			}			
			else {
				*bsIn >> tmp8;
				rc = tmp8;
				if (rc != 0) {
					*bsIn >> errorMsg;
					fWEClient->removeQueue(uniqueId);
					break;
				}
				else
					msgRecived++;
							
			}
		}
	}
	return rc;
}

int DDLPackageProcessor::commitTransaction(uint64_t uniqueId, BRM::TxnID txnID)
{
	int rc = fDbrm->vbCommit(txnID.id);
	return rc;	
}

} // namespace
// vim:ts=4 sw=4:

