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

// $Id: we_ddlcommandproc.cpp 3082 2011-09-26 22:00:38Z chao $

#include <unistd.h>
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/progress.hpp"
#include "boost/scoped_ptr.hpp"
using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "we_messages.h"
#include "we_message_handlers.h"
#include "we_ddlcommon.h"
#include "we_ddlcommandproc.h"
#include "ddlpkg.h"
using namespace ddlpackage;
#include <boost/date_time/gregorian/gregorian.hpp>
using namespace boost::gregorian;
#include "dataconvert.h"
using namespace dataconvert;
//#include "we_brm.h"
namespace fs = boost::filesystem;
#include "cacheutils.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"
using namespace idbdatafile;

using namespace execplan;

namespace WriteEngine
{
	WE_DDLCommandProc::WE_DDLCommandProc()
	{
		filesPerColumnPartition = 8;
		extentsPerSegmentFile = 1;
		dbrootCnt = 1;
		extentRows = 0x800000;
		config::Config* cf = config::Config::makeConfig();
		string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
		if (fpc.length() != 0)
			filesPerColumnPartition = cf->uFromText(fpc);
		string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
		if (epsf.length() != 0)
			extentsPerSegmentFile = cf->uFromText(epsf);
		string dbct = cf->getConfig("SystemConfig", "DBRootCount");
		if (dbct.length() != 0)
			dbrootCnt = cf->uFromText(dbct);
	}
	WE_DDLCommandProc::WE_DDLCommandProc(const WE_DDLCommandProc& rhs)
	{
	}
	WE_DDLCommandProc::~WE_DDLCommandProc()
	{
	}
	uint8_t WE_DDLCommandProc::updateSyscolumnNextval(ByteStream& bs, std::string & err)
	{
		uint32_t columnOid, sessionID;
		uint64_t nextVal;
		int rc = 0;
		bs >> columnOid;
		bs >> nextVal;
		bs >> sessionID;
		uint16_t dbRoot;
		BRM::OID_t oid = 1021;
		fDbrm.getSysCatDBRoot(oid, dbRoot);
		std::map<uint32_t,uint32_t> oids;
		//std::vector<BRM::OID_t>  oidsToFlush;
		oids[columnOid] = columnOid;
		//oidsToFlush.push_back(columnOid);
		if (idbdatafile::IDBPolicy::useHdfs())	
			fWEWrapper.startTransaction(sessionID);
		rc = fWEWrapper.updateNextValue(sessionID,columnOid, nextVal, sessionID, dbRoot);
		if (rc != 0)
		{
			err = "Error in WE::updateNextValue";
			rc =1;
		}
		if (idbdatafile::IDBPolicy::useHdfs())	
		{
			fWEWrapper.flushDataFiles(rc, sessionID, oids);
			fWEWrapper.confirmTransaction(sessionID);
			if ( rc == 0)
				fWEWrapper.endTransaction(sessionID, true);
			else
				fWEWrapper.endTransaction(sessionID, false);
		}
		purgeFDCache();
		//if (idbdatafile::IDBPolicy::useHdfs())
		//	cacheutils::flushOIDsFromCache(oidsToFlush);
		return rc;
	}

uint8_t WE_DDLCommandProc::writeSystable(ByteStream& bs, std::string &err)
{
		int rc = 0;
	u_int32_t sessionID, tmp32, columnSize, dictSize, i;
	u_int8_t tmp8;
	int txnID, tableOID, colpos;
	uint32_t tableWithAutoi;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> tmp32;
	tableOID = tmp32;
	bs >> tmp32;
	tableWithAutoi = tmp32;

	bs >> columnSize;

	//deserialize column Oid and dictionary oid
	vector<u_int32_t> coloids;
	vector<u_int32_t> dictoids;

	for (i = 0; i < columnSize; ++i) {
		bs >> tmp32;
		coloids.push_back(tmp32);
	}

	bs >> dictSize;
	for (i = 0; i < dictSize; ++i) {
		bs >> tmp32;
		dictoids.push_back(tmp32);
	}
	bool alterFlag = 0;
	bs >> tmp8;
	alterFlag = (tmp8 != 0);
	bs >> tmp32;
	colpos = tmp32;
	bs >> tmp32;
	u_int16_t dbroot = tmp32;
	ddlpackage::TableDef tableDef;
	tableDef.unserialize(bs);

	WriteEngine::ColTuple colTuple;
	WriteEngine::ColStruct colStruct;
	WriteEngine::ColStructList colStructs;
	WriteEngine::ColTupleList colTuples;
	WriteEngine::dictStr dctColTuples;
	WriteEngine::DctnryStruct dctnryStruct;
	WriteEngine::ColValueList colValuesList;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DictStrList dctnryValueList;
	WriteEngine::RIDList ridList;
	CalpontSystemCatalog::TableName tableName;
	CalpontSystemCatalog::ROPair sysTableROPair;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr;
	ColumnList columns;
	ColumnList::const_iterator column_iterator;
	DDLColumn column;
	int error = 0;

	tableName.schema = CALPONT_SCHEMA;
	tableName.table  = SYSTABLE_TABLE;

	systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	std::map<uint32_t,uint32_t> oids;
	int rc1 = 0;
	//std::vector<BRM::OID_t>  oidsToFlush;
	try
	{
		sysTableROPair = systemCatalogPtr->tableRID(tableName);

		getColumnsForTable(sessionID, tableName.schema,tableName.table, columns);

			column_iterator = columns.begin();
			std::string tmpStr("");
		while (column_iterator != columns.end())
		{

			column = *column_iterator;
			boost::to_lower(column.tableColName.column);

			if (TABLENAME_COL == column.tableColName.column)
			{
				std::string tablename = tableDef.fQualifiedName->fName;
				boost::to_lower(tablename);
				colTuple.data = tablename;
				tmpStr = tablename;
			}
			else if (SCHEMA_COL == column.tableColName.column)
			{
				std::string schema =  tableDef.fQualifiedName->fSchema;
				boost::to_lower(schema);
				colTuple.data = schema;
				tmpStr = schema;
			}
			else if (OBJECTID_COL == column.tableColName.column)
			{
				colTuple.data = tableOID;
			}
			else if (CREATEDATE_COL == column.tableColName.column)
			{
				date d(day_clock::universal_day());
				std::string date = to_iso_string(d);
				Date aDay;
				int intvalue;
				std::string s = date.substr(0, 4);
				if (from_string<int>(intvalue, s, std::dec))
				{
					aDay.year = intvalue;
				}
				s = date.substr(4, 2);
				if (from_string<int>(intvalue, s, std::dec))
				{
					aDay.month = intvalue;
				}
				s = date.substr(6, 2);
				if (from_string<int>(intvalue, s, std::dec))
				{
					aDay.day = intvalue;
				}

				colTuple.data = *(reinterpret_cast<int *> (&aDay));

			}
			else if (INIT_COL == column.tableColName.column)
			{

				colTuple.data = getNullValueForType(column.colType);
			}
			else if (NEXT_COL == column.tableColName.column)
			{

				colTuple.data = getNullValueForType(column.colType);
			}
			else if (AUTOINC_COL == column.tableColName.column)
			{
				colTuple.data = tableWithAutoi;
			}
			else
			{
				colTuple.data = getNullValueForType(column.colType);
			}

			colStruct.dataOid = column.oid;
			colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
			colStruct.tokenFlag = false;
			colStruct.tokenFlag = column.colType.colWidth > 8 ? true : false;
			colStruct.colDataType = column.colType.colDataType;
			colStruct.fColDbRoot = dbroot;
			if (idbdatafile::IDBPolicy::useHdfs())
			{
				colStruct.fCompressionType = 2;
				dctnryStruct.fCompressionType = 2;
			}
				
			dctnryStruct.fColDbRoot = dbroot;
			if (colStruct.tokenFlag)
			{
				dctnryStruct.dctnryOid = column.colType.ddn.dictOID;
				dctnryStruct.columnOid = column.oid;
			}
			else
			{
				dctnryStruct.dctnryOid = 0;
				dctnryStruct.columnOid = column.oid;
			}

			colStructs.push_back(colStruct);
			oids[colStruct.dataOid] = colStruct.dataOid;
			//oidsToFlush.push_back(colStruct.dataOid);
			if (dctnryStruct.dctnryOid  > 0)
			{
				oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
				//oidsToFlush.push_back(dctnryStruct.dctnryOid);
			}
				
			colTuples.push_back(colTuple);

			dctColTuples.push_back (tmpStr);

			colValuesList.push_back(colTuples);

			dctnryStructList.push_back (dctnryStruct);

			dctnryValueList.push_back (dctColTuples);


			colTuples.pop_back();
			dctColTuples.pop_back();

			++column_iterator;
		}
		//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
		fWEWrapper.setTransId(txnID);
		fWEWrapper.setIsInsert(true);
		fWEWrapper.setBulkFlag(false);
		fWEWrapper.startTransaction(txnID);
		if (0 != colStructs.size())
		{
			error = fWEWrapper.insertColumnRec_SYS(txnID, colStructs, colValuesList,
											dctnryStructList, dctnryValueList, SYSCOLUMN_BASE);

			if (error != WriteEngine::NO_ERROR)
			{
			if (error == ERR_BRM_WR_VB_ENTRY)
			{
				throw std::runtime_error("WE: Error writing to BRM.");
			}
			else
			{
				WErrorCodes ec;
				throw std::runtime_error("WE: Error updating calpont.systable:" + ec.errorString(error));
			}
			}
			//if (idbdatafile::IDBPolicy::useHdfs())
			//	fWEWrapper.flushDataFiles(error, txnID, oids);
		 }
	 }
	catch (exception& ex)
	{
		err += ex.what();
		rc = 1;
	}
	catch (...)
	{
		err += "Unknown exception caught";
		rc = 1;
	}

	if (rc != 0)
		return rc;


	colStructs.clear();
	colTuples.clear();
	dctColTuples.clear();
	colValuesList.clear();
	ridList.clear();
	dctnryStructList.clear();
	dctnryValueList.clear();
	columns.clear();
	//oids.clear();
	ColumnDef* colDefPtr = 0;
	ColumnDefList::const_iterator iter;

	int startPos = colpos;

	tableName.schema = CALPONT_SCHEMA;
	tableName.table  = SYSCOLUMN_TABLE;
	BRM::OID_t sysOid = 1021;
	//Find out where syscolumn is
	rc = fDbrm.getSysCatDBRoot(sysOid, dbroot);
	getColumnsForTable(sessionID, tableName.schema,tableName.table, columns);
	unsigned int numCols = columns.size();
	//WriteEngine::ColTupleList colList[numCols];
	//ColTupleList is NOT POD, so let's try this:
	std::vector<WriteEngine::ColTupleList> colList;
	//WriteEngine::dictStr dctColList[numCols];
	std::vector<WriteEngine::dictStr> dctColList;
	ColumnDefList tableDefCols = tableDef.fColumns;

	ddlpackage::QualifiedName qualifiedName = *(tableDef.fQualifiedName);
	iter = tableDefCols.begin();
	//colpos = 0;
	std::string tmpStr("");
	for (unsigned int ii = 0; ii < numCols; ii++)
	{
		colList.push_back(WriteEngine::ColTupleList());
		dctColList.push_back(WriteEngine::dictStr());
	}
	try
	{
		unsigned int col = 0;
		unsigned int dictcol = 0;
		while (iter != tableDefCols.end())
		{
			colDefPtr = *iter;

			DictOID dictOID = {0, 0, 0, 0, 0};

			int dataType;
			dataType = convertDataType(colDefPtr->fType->fType);
			if (dataType == CalpontSystemCatalog::DECIMAL ||
				dataType == CalpontSystemCatalog::UDECIMAL)
			{
				if (colDefPtr->fType->fPrecision > 18) //@Bug 5717 precision cannot be over 18.
				{
					ostringstream os;
					os << "Syntax error: The maximum precision (total number of digits) that can be specified is 18";
					throw std::runtime_error(os.str());
				}
				else if	 (colDefPtr->fType->fPrecision < colDefPtr->fType->fScale)
				{
					ostringstream os;
					os << "Syntax error: scale should be less than precision, precision: " << colDefPtr->fType->fPrecision << " scale: " << colDefPtr->fType->fScale;
					throw std::runtime_error(os.str());
				}
				colDefPtr->convertDecimal();
			}

			bool hasDict = false;
			if ( (dataType == CalpontSystemCatalog::CHAR && colDefPtr->fType->fLength > 8) ||
				 (dataType == CalpontSystemCatalog::VARCHAR && colDefPtr->fType->fLength > 7) ||
				 (dataType == CalpontSystemCatalog::VARBINARY && colDefPtr->fType->fLength > 7) )
			{
				hasDict = true;
				dictOID.compressionType = colDefPtr->fType->fCompressiontype;
				dictOID.colWidth = colDefPtr->fType->fLength;
				dictOID.dictOID = dictoids[dictcol];
				dictcol++;

				//@Bug 2534. Take away the limit of 255 and set the limit to 8000.
				if (colDefPtr->fType->fLength > 8000)
				{
					ostringstream os;
					os << "char, varchar and varbinary length may not exceed 8000";
					throw std::runtime_error(os.str());
				}
			}
			else if (dataType == CalpontSystemCatalog::VARBINARY && colDefPtr->fType->fLength <= 7)
			{
				ostringstream os;
				os << "varbinary length may not be less than 8";
				throw std::runtime_error(os.str());
			}

			unsigned int i = 0;
			column_iterator = columns.begin();
			while (column_iterator != columns.end())
			{
				column = *column_iterator;
				boost::to_lower(column.tableColName.column);

				if (SCHEMA_COL == column.tableColName.column)
				{
					boost::to_lower(qualifiedName.fSchema);
					colTuple.data = qualifiedName.fSchema;
					tmpStr = qualifiedName.fSchema;
				}
				else if (TABLENAME_COL == column.tableColName.column)
				{
					boost::to_lower(qualifiedName.fName);
					colTuple.data = qualifiedName.fName;
					tmpStr = qualifiedName.fName;
				}
				else if (COLNAME_COL == column.tableColName.column)
				{
					boost::to_lower(colDefPtr->fName);
					colTuple.data = colDefPtr->fName;
					tmpStr = colDefPtr->fName;
				}
				else if (OBJECTID_COL == column.tableColName.column)
				{
					if (alterFlag)
						colTuple.data = coloids[col];
					else
						colTuple.data = coloids[col];
				}
				else if (DATATYPE_COL == column.tableColName.column)
				{
					colTuple.data = dataType;
				}
				else if (COLUMNLEN_COL == column.tableColName.column)
				{
					//@Bug 2089 Disallow zero length char and varch column to be created
					if (dataType == CalpontSystemCatalog::CHAR ||
						dataType == CalpontSystemCatalog::VARCHAR ||
						dataType == CalpontSystemCatalog::VARBINARY)
					{
						if (colDefPtr->fType->fLength <= 0)
						{
							ostringstream os;
							os << "char, varchar and varbinary length must be greater than zero";
							throw std::runtime_error(os.str());
						}
					}
					colTuple.data = colDefPtr->fType->fLength;
				}
				else if (COLUMNPOS_COL == column.tableColName.column)
				{
					colTuple.data = colpos;
				}
				else if (DEFAULTVAL_COL == column.tableColName.column)
				{
					if (colDefPtr->fDefaultValue)
					{
						colTuple.data = colDefPtr->fDefaultValue->fValue;
						tmpStr = colDefPtr->fDefaultValue->fValue;
					}
					else
					{
						tmpStr="";
						//colTuple.data = getNullValueForType(column.colType);
					}

				}
				else if (NULLABLE_COL == column.tableColName.column)
				{
					int nullable = 1;
					ColumnConstraintList& colConstraints = colDefPtr->fConstraints;
					ColumnConstraintList::const_iterator constraint_iter = colConstraints.begin();
					while (constraint_iter != colConstraints.end())
					{
						ColumnConstraintDef* consDefPtr = *constraint_iter;
						if (consDefPtr->fConstraintType == ddlpackage::DDL_NOT_NULL)
						{
							nullable = 0;
							break;
						}
						++constraint_iter;
					}
					colTuple.data = nullable;
				}
				else if (SCALE_COL == column.tableColName.column)
				{
					colTuple.data = colDefPtr->fType->fScale;
				}
				else if (PRECISION_COL == column.tableColName.column)
				{
					colTuple.data = colDefPtr->fType->fPrecision;
				}
				else if (DICTOID_COL == column.tableColName.column)
				{
					if (hasDict)
					{
						colTuple.data = dictOID.dictOID;
					}
					else
					{
						colTuple.data = getNullValueForType(column.colType);
					}
				}
				else if (LISTOBJID_COL == column.tableColName.column)
				{
					colTuple.data = getNullValueForType(column.colType);
				}
				else if (TREEOBJID_COL == column.tableColName.column)
				{
					colTuple.data = getNullValueForType(column.colType);
				}
				else if (MINVAL_COL == column.tableColName.column)
				{
						tmpStr="";
				}
				else if (MAXVAL_COL == column.tableColName.column)
				{
						tmpStr="";
				}
				else if (COMPRESSIONTYPE_COL == column.tableColName.column)
				{
					colTuple.data = colDefPtr->fType->fCompressiontype;
				}
				else if (AUTOINC_COL == column.tableColName.column)
				{
					//cout << "autoincrement= " << colDefPtr->fType->fAutoincrement << endl;
					colTuple.data = colDefPtr->fType->fAutoincrement;

				}
				else if (NEXTVALUE_COL == column.tableColName.column)
				{
					colTuple.data = colDefPtr->fType->fNextvalue;
				}
				else
				{
					colTuple.data = getNullValueForType(column.colType);
				}

				colStruct.dataOid = column.oid;
				colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
				colStruct.tokenFlag = false;
				colStruct.tokenFlag = column.colType.colWidth > 8 ? true : false;
				colStruct.colDataType = column.colType.colDataType;
				colStruct.fColDbRoot = dbroot;
				dctnryStruct.fColDbRoot = dbroot;
				if (idbdatafile::IDBPolicy::useHdfs())
				{
					colStruct.fCompressionType = 2;
					dctnryStruct.fCompressionType = 2;
				}
				if (colStruct.tokenFlag)
				{
					dctnryStruct.dctnryOid = column.colType.ddn.dictOID;
					dctnryStruct.columnOid = column.oid;
				}
				else
				{
					dctnryStruct.dctnryOid = 0;
					dctnryStruct.columnOid = column.oid;
				}

				oids[colStruct.dataOid] = colStruct.dataOid;
				//oidsToFlush.push_back(colStruct.dataOid);
				if (dctnryStruct.dctnryOid  > 0)
				{
					oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
					//oidsToFlush.push_back(dctnryStruct.dctnryOid);
				}
				
				if (colpos == startPos)
				{
					colStructs.push_back(colStruct);
					dctnryStructList.push_back (dctnryStruct);
				}
				colList[i].push_back(colTuple);
				//colList.push_back(WriteEngine::ColTupleList());
				//colList.back().push_back(colTuple);
				dctColList[i].push_back(tmpStr);
				//dctColList.push_back(WriteEngine::dictStr());
				//dctColList.back().push_back(tmpStr);
				++i;
				++column_iterator;
			}

			++colpos;
			col++;
			++iter;
		}


		if (0 != colStructs.size())
		{
			for (unsigned int n = 0; n < numCols; n++)
			{
				colValuesList.push_back(colList[n]);
				dctnryValueList.push_back(dctColList[n]);
			}
			//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
			error = fWEWrapper.insertColumnRec_SYS(txnID, colStructs, colValuesList,
								dctnryStructList, dctnryValueList, SYSCOLUMN_BASE);

			if (idbdatafile::IDBPolicy::useHdfs())
			{		
				rc1 = fWEWrapper.flushDataFiles(error, txnID, oids);
				if ((error == 0) && ( rc1 == 0))
				{
				
					rc1 = fWEWrapper.confirmTransaction(txnID);
				
					if ( rc1 == NO_ERROR)
						rc1 = fWEWrapper.endTransaction(txnID, true);
					else
						fWEWrapper.endTransaction(txnID, false);
				}
				else
				{
					fWEWrapper.endTransaction(txnID, false);	
				}
			}
			if (error != WriteEngine::NO_ERROR)
			{
				if (error == ERR_BRM_WR_VB_ENTRY)
				{
					throw std::runtime_error(
						"writeSysColumnMetaData WE: Error writing to BRM.");
				}
				else
				{
					WErrorCodes ec;
					throw std::runtime_error(
						"WE: Error updating calpont.syscolumn. " + ec.errorString(error));
//					 err = "Error updating calpont.syscolumn. error number = " + error;
				}
			}
			else
				error = rc1;		
		}
	}
	catch (exception& ex)
	{
		err += ex.what();
		rc = 1;
	}
	catch (...)
	{
		err += "Unknown exception caught";
		rc = 1;
	}
	
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//	cacheutils::flushOIDsFromCache(oidsToFlush);
		
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	return rc;
}

uint8_t WE_DDLCommandProc::writeSyscolumn(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32, coloid, dictoid;
	int txnID, startPos;
	string schema, tablename;
	u_int8_t tmp8;
	bool isAlter = false;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> tablename;
	bs >> coloid;
	bs >> dictoid;
	bs >> tmp8; //alterFlag
	bs >> tmp32;
	startPos = tmp32;
	isAlter = tmp32;
	boost::scoped_ptr<ddlpackage::ColumnDef>  colDefPtr(new ddlpackage::ColumnDef());
	colDefPtr->unserialize(bs);

	WriteEngine::ColStruct colStruct;
	WriteEngine::ColTuple colTuple;
	WriteEngine::ColStructList colStructs;
	WriteEngine::ColTupleList colTuples;
	WriteEngine::DctColTupleList dctColTuples;
	WriteEngine::ColValueList colValuesList;
	WriteEngine::RIDList ridList;
	WriteEngine::DctnryStruct dctnryStruct;
	WriteEngine::dictStr dctnryTuple;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DictStrList dctnryValueList;

	CalpontSystemCatalog::TableName tableName;

	ColumnList columns;
	ColumnList::const_iterator column_iterator;

	DDLColumn column;
	int error = 0;

	tableName.schema = CALPONT_SCHEMA;
	tableName.table  = SYSCOLUMN_TABLE;

	getColumnsForTable(sessionID, tableName.schema,tableName.table, columns);
	unsigned int numCols = columns.size();
	//WriteEngine::ColTupleList colList[numCols];
	//ColTupleList is NOT POD, so let's try this:
	std::vector<WriteEngine::ColTupleList> colList;
	//WriteEngine::dictStr dctColList[numCols];
	std::vector<WriteEngine::dictStr> dctColList;
	std::map<uint32_t,uint32_t> oids;
	std::vector<BRM::OID_t>  oidsToFlush;
	//colpos = 0;
	std::string tmpStr("");
	for (unsigned int ii = 0; ii < numCols; ii++)
	{
		colList.push_back(WriteEngine::ColTupleList());
		dctColList.push_back(WriteEngine::dictStr());
	}

	try
	{
		DictOID dictOID = {0, 0, 0, 0, 0};

		int dataType = convertDataType(colDefPtr->fType->fType);
		if (dataType == CalpontSystemCatalog::DECIMAL ||
			dataType == CalpontSystemCatalog::UDECIMAL)
		{
			if (colDefPtr->fType->fPrecision > 18) //@Bug 5717 precision cannot be over 18.
			{
					ostringstream os;
					os << "Syntax error: The maximum precision (total number of digits) that can be specified is 18";
					throw std::runtime_error(os.str());
			}
			else if	 (colDefPtr->fType->fPrecision < colDefPtr->fType->fScale)
			{
				ostringstream os;
				os << "Syntax error: scale should be less than precision, precision: " << colDefPtr->fType->fPrecision << " scale: " << colDefPtr->fType->fScale;
				throw std::runtime_error(os.str());
			}
			colDefPtr->convertDecimal();
		}

		if (dictoid > 0)
		{
			dictOID.compressionType = colDefPtr->fType->fCompressiontype;
			dictOID.colWidth = colDefPtr->fType->fLength;
			dictOID.dictOID = dictoid;

			//@Bug 2534. Take away the limit of 255 and set the limit to 8000.
			if (colDefPtr->fType->fLength > 8000)
			{
				ostringstream os;
				os << "char, varchar and varbinary length may not exceed 8000";
				throw std::runtime_error(os.str());
			}
		}
		else if (dataType == CalpontSystemCatalog::VARBINARY && colDefPtr->fType->fLength <= 7)
		{
			ostringstream os;
			os << "varbinary length may not be less than 8";
			throw std::runtime_error(os.str());
		}

		unsigned int i = 0;
		u_int16_t  dbRoot;
		BRM::OID_t sysOid = 1021;
		//Find out where syscolumn is
		rc = fDbrm.getSysCatDBRoot(sysOid, dbRoot);
		column_iterator = columns.begin();
		while (column_iterator != columns.end())
		{
				column = *column_iterator;
				boost::to_lower(column.tableColName.column);

				if (SCHEMA_COL == column.tableColName.column)
				{
					boost::to_lower(schema);
					colTuple.data = schema;
					tmpStr = schema;
				}
				else if (TABLENAME_COL == column.tableColName.column)
				{
					boost::to_lower(tablename);
					colTuple.data = tablename;
					tmpStr = tablename;
				}
				else if (COLNAME_COL == column.tableColName.column)
				{
					boost::to_lower(colDefPtr->fName);
					colTuple.data = colDefPtr->fName;
					tmpStr = colDefPtr->fName;
				}
				else if (OBJECTID_COL == column.tableColName.column)
				{
					colTuple.data = coloid;
				}
				else if (DATATYPE_COL == column.tableColName.column)
				{
					colTuple.data = dataType;
				}
				else if (COLUMNLEN_COL == column.tableColName.column)
				{
					//@Bug 2089 Disallow zero length char and varch column to be created
					if (dataType == CalpontSystemCatalog::CHAR ||
						dataType == CalpontSystemCatalog::VARCHAR ||
						dataType == CalpontSystemCatalog::VARBINARY)
					{
						if (colDefPtr->fType->fLength <= 0)
						{
							ostringstream os;
							os << "char, varchar and varbinary length must be greater than zero";
							throw std::runtime_error(os.str());
						}
					}
					colTuple.data = colDefPtr->fType->fLength;
				}
				else if (COLUMNPOS_COL == column.tableColName.column)
				{
					colTuple.data = startPos;
				}
				else if (DEFAULTVAL_COL == column.tableColName.column)
				{
					if (colDefPtr->fDefaultValue)
					{
						colTuple.data = colDefPtr->fDefaultValue->fValue;
						tmpStr = colDefPtr->fDefaultValue->fValue;
					}
					else
					{
						tmpStr="";
						//colTuple.data = getNullValueForType(column.colType);
					}

				}
				else if (NULLABLE_COL == column.tableColName.column)
				{
					int nullable = 1;
					ColumnConstraintList& colConstraints = colDefPtr->fConstraints;
					ColumnConstraintList::const_iterator constraint_iter = colConstraints.begin();
					while (constraint_iter != colConstraints.end())
					{
						ColumnConstraintDef* consDefPtr = *constraint_iter;
						if (consDefPtr->fConstraintType == ddlpackage::DDL_NOT_NULL)
						{
							nullable = 0;
							break;
						}
						++constraint_iter;
					}
					colTuple.data = nullable;
				}
				else if (SCALE_COL == column.tableColName.column)
				{
					colTuple.data = colDefPtr->fType->fScale;
				}
				else if (PRECISION_COL == column.tableColName.column)
				{
					colTuple.data = colDefPtr->fType->fPrecision;
				}
				else if (DICTOID_COL == column.tableColName.column)
				{
					if (dictoid>0)
					{
						colTuple.data = dictOID.dictOID;
					}
					else
					{
						colTuple.data = getNullValueForType(column.colType);
					}
				}
				else if (LISTOBJID_COL == column.tableColName.column)
				{
					colTuple.data = getNullValueForType(column.colType);
				}
				else if (TREEOBJID_COL == column.tableColName.column)
				{
					colTuple.data = getNullValueForType(column.colType);
				}
				else if (MINVAL_COL == column.tableColName.column)
				{
						tmpStr="";
				}
				else if (MAXVAL_COL == column.tableColName.column)
				{
						tmpStr="";
				}
				else if (COMPRESSIONTYPE_COL == column.tableColName.column)
				{
					colTuple.data = colDefPtr->fType->fCompressiontype;
				}
				else if (AUTOINC_COL == column.tableColName.column)
				{
					//cout << "autoincrement= " << colDefPtr->fType->fAutoincrement << endl;
					colTuple.data = colDefPtr->fType->fAutoincrement;

				}
				else if (NEXTVALUE_COL == column.tableColName.column)
				{
					colTuple.data = colDefPtr->fType->fNextvalue;
				}
				else
				{
					colTuple.data = getNullValueForType(column.colType);
				}

				colStruct.dataOid = column.oid;
				oids[column.oid] = column.oid;
				oidsToFlush.push_back(column.oid);
				colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
				colStruct.tokenFlag = false;
				colStruct.fColDbRoot = dbRoot;
				colStruct.tokenFlag = column.colType.colWidth > 8 ? true : false;
				colStruct.colDataType = column.colType.colDataType;
				dctnryStruct.fColDbRoot = dbRoot;
				if (idbdatafile::IDBPolicy::useHdfs())
				{
					colStruct.fCompressionType = 2;
					dctnryStruct.fCompressionType = 2;
				}
				if (colStruct.tokenFlag)
				{
					dctnryStruct.dctnryOid = column.colType.ddn.dictOID;
					dctnryStruct.columnOid = column.oid;
				}
				else
				{
					dctnryStruct.dctnryOid = 0;
					dctnryStruct.columnOid = column.oid;
				}

				if (dctnryStruct.dctnryOid > 0) {
					oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
					oidsToFlush.push_back(dctnryStruct.dctnryOid);
				}

				colStructs.push_back(colStruct);
				dctnryStructList.push_back (dctnryStruct);
				colList[i].push_back(colTuple);
				//colList.push_back(WriteEngine::ColTupleList());
				//colList.back().push_back(colTuple);
				dctColList[i].push_back(tmpStr);
				//dctColList.push_back(WriteEngine::dictStr());
				//dctColList.back().push_back(tmpStr);
				++i;
				++column_iterator;
		}


		if (0 != colStructs.size())
		{
			//FIXME: Is there a cleaner way to do this? Isn't colValuesList the same as colList after this?
			for (unsigned int n = 0; n < numCols; n++)
			{
				colValuesList.push_back(colList[n]);
				dctnryValueList.push_back(dctColList[n]);
			}
			//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
			fWEWrapper.setTransId(txnID);
			fWEWrapper.setIsInsert(true);
			fWEWrapper.setBulkFlag(false);
			fWEWrapper.startTransaction(txnID);
			int rc1 = 0;
								
			error = fWEWrapper.insertColumnRec_SYS(txnID, colStructs, colValuesList,
								dctnryStructList, dctnryValueList, SYSCOLUMN_BASE);
			
			if (idbdatafile::IDBPolicy::useHdfs())
			{		
				rc1 = fWEWrapper.flushDataFiles(error, txnID, oids);
				if ((error == 0) && ( rc1 == 0))
				{
				
					rc1 = fWEWrapper.confirmTransaction(txnID);
				
					if ( rc1 == NO_ERROR)
						rc1 = fWEWrapper.endTransaction(txnID, true);
					else
						fWEWrapper.endTransaction(txnID, false);
				}
				else
				{
					fWEWrapper.endTransaction(txnID, false);	
				}
			}

			if (error != WriteEngine::NO_ERROR)
			{
				if (error == ERR_BRM_WR_VB_ENTRY)
				{
					throw std::runtime_error(
						"writeSysColumnMetaData WE: Error writing to BRM.");
				}
				else
				{
					WErrorCodes ec;
					throw std::runtime_error(
						"WE: Error updating calpont.syscolumn. " + ec.errorString(error));
//					 err = "Error updating calpont.syscolumn. error number = " + error;
				}
			}
			else
				error = rc1;
		}

	}
	catch (exception& ex)
	{
		err += ex.what();
		rc = 1;
	}
	catch (...)
	{
		err += "Unknown exception caught";
		rc = 1;
	}
	purgeFDCache();
	if (isAlter)
	{
		if (idbdatafile::IDBPolicy::useHdfs())
			cacheutils::flushOIDsFromCache(oidsToFlush);
	}
	return rc;
}



uint8_t WE_DDLCommandProc::createtablefiles(ByteStream& bs, std::string & err)
{
	int rc = 0;
	uint32_t size, i;
	uint16_t tmp16;
	uint32_t tmp32;
	uint8_t tmp8;
	OID	 dataOid;
	int  colWidth;
	bool tokenFlag;
	int txnID;
	CalpontSystemCatalog::ColDataType colDataType;
	u_int16_t colDbRoot;
	int compressionType;
	bs >> tmp32;
	txnID = tmp32;
	bs >> size;
	fWEWrapper.setTransId(txnID);
	fWEWrapper.setIsInsert(true);
	fWEWrapper.setBulkFlag(true);
	std::map<uint32_t,uint32_t> oids;
	for (i = 0; i < size; ++i) {
		bs >> tmp32;
		dataOid = tmp32;
		bs >> tmp8;
		colDataType = (CalpontSystemCatalog::ColDataType)tmp8;
		bs >> tmp8;
		tokenFlag = (tmp8 != 0);
		bs >> tmp32;
		colWidth = tmp32;
		bs >> tmp16;
		colDbRoot = tmp16;
		bs >> tmp32;
		compressionType = tmp32;
		oids[dataOid] = dataOid;
		if (tokenFlag)
		{
			rc = fWEWrapper.createDctnry(0, dataOid, colWidth, colDbRoot, 0,0, compressionType);
		}
		else
		{
			rc = fWEWrapper.createColumn(0, dataOid, colDataType,colWidth, colDbRoot, 0,compressionType);
		}
		if (rc != 0)
			break;
	}
	//cout << "creating column file got error code " << rc << endl;
	if (rc != 0)
	{
		WErrorCodes ec;
		ostringstream oss;
		oss << "WE: Error creating column file for oid "  << dataOid <<  "; " << ec.errorString(rc) << endl;
		err = oss.str();
	}
	
	//if (idbdatafile::IDBPolicy::useHdfs())
	fWEWrapper.flushDataFiles(rc, txnID, oids);
	purgeFDCache();
	return rc;
}

uint8_t WE_DDLCommandProc::commitVersion(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t tmp32;
	int txnID;

	bs >> tmp32;
	txnID = tmp32;

	rc = fWEWrapper.commit(txnID);
	if (rc != 0)
	{
		WErrorCodes ec;
		ostringstream oss;
		oss << "WE: Error commiting transaction; "  << txnID << ec.errorString(rc) << endl;
		err = oss.str();
	}
	return rc;
}

uint8_t WE_DDLCommandProc::rollbackBlocks(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32;;
	int txnID;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	fWEWrapper.setTransId(txnID);
	fWEWrapper.setIsInsert(true);
	fWEWrapper.setBulkFlag(true);
	rc = fWEWrapper.rollbackBlocks(txnID, sessionID);
	if (rc != 0)
	{
		WErrorCodes ec;
		ostringstream oss;
		oss << "WE: Error rolling back files "  << txnID << " for session " <<  sessionID << "; " << ec.errorString(rc) << endl;
		err = oss.str();
	}
	std::map<uint32_t,uint32_t> oids;
	if (idbdatafile::IDBPolicy::useHdfs())			
		fWEWrapper.flushDataFiles(rc, txnID, oids);
	purgeFDCache();
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	return rc;
}

uint8_t WE_DDLCommandProc::rollbackVersion(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32;
	int txnID;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;

	rc = fWEWrapper.rollbackVersion(txnID, sessionID);
	if (rc != 0)
	{
		WErrorCodes ec;
		ostringstream oss;
		oss << "WE: Error rolling back transaction "  << txnID << " for session " <<  sessionID << "; " << ec.errorString(rc) << endl;
		err = oss.str();
	}
	purgeFDCache();
	return rc;
}

uint8_t WE_DDLCommandProc::deleteSyscolumn(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32;;
	int txnID;
	string schema, tablename;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> tablename;

	ddlpackage::QualifiedName sysCatalogTableName;
	sysCatalogTableName.fSchema = CALPONT_SCHEMA;
	sysCatalogTableName.fName  = SYSCOLUMN_TABLE;

	CalpontSystemCatalog::TableName userTableName;
	userTableName.schema = schema;
	userTableName.table = tablename;

	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr;
	systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	u_int16_t  dbRoot;
	BRM::OID_t sysOid = 1021;
	//Find out where syscolumn is
	rc = fDbrm.getSysCatDBRoot(sysOid, dbRoot);
	fWEWrapper.setTransId(txnID);
	fWEWrapper.startTransaction(txnID);
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;
	
	try
	{
		CalpontSystemCatalog::RIDList colRidList = systemCatalogPtr->columnRIDs(userTableName);

		WriteEngine::ColStruct colStruct;
		WriteEngine::ColStructList colStructs;
		std::vector<WriteEngine::ColStructList> colExtentsStruct;
		std::vector<void *> colValuesList;
		WriteEngine::RIDList ridList;
		std::vector<WriteEngine::RIDList> ridLists;
		DDLColumn column;
		CalpontSystemCatalog::RIDList::const_iterator colrid_iterator = colRidList.begin();
		while (colrid_iterator != colRidList.end())
		{
			WriteEngine::RID rid = (*colrid_iterator).rid;
			ridList.push_back(rid);
			++colrid_iterator;
		}

		ColumnList columns;
		getColumnsForTable(sessionID, sysCatalogTableName.fSchema, sysCatalogTableName.fName, columns);

		ColumnList::const_iterator column_iterator = columns.begin();
		while (column_iterator != columns.end())
		{
			column = *column_iterator;
			colStruct.dataOid = column.oid;
			colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
			colStruct.colDataType = column.colType.colDataType;
			colStruct.fColDbRoot = dbRoot;
			if (idbdatafile::IDBPolicy::useHdfs())
			{
				colStruct.fCompressionType = 2;
			}
			oids[colStruct.dataOid] = colStruct.dataOid;
			//oidsToFlush.push_back(colStruct.dataOid);
			colStructs.push_back(colStruct);

			++column_iterator;
		}
		colExtentsStruct.push_back(colStructs);
		ridLists.push_back(ridList);


		if (0 != colStructs.size() && 0 != ridLists[0].size())
		{
			int error = fWEWrapper.deleteRow(txnID, colExtentsStruct, colValuesList, ridLists, SYSCOLUMN_BASE);
			
			int rc1 = 0;
			if (idbdatafile::IDBPolicy::useHdfs())
			{		
				rc1 = fWEWrapper.flushDataFiles(error, txnID, oids);
				if ((error == 0) && ( rc1 == 0))
				{
				
					rc1 = fWEWrapper.confirmTransaction(txnID);
				
					if ( rc1 == NO_ERROR)
						rc1 = fWEWrapper.endTransaction(txnID, true);
					else
						fWEWrapper.endTransaction(txnID, false);
				}
				else
				{
					fWEWrapper.endTransaction(txnID, false);	
				}
			}
			
			if ( error == NO_ERROR)
				rc = rc1;
			else rc = error;
		}
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
	}
	catch (...)
	{
		err = "Unknown exception caught";
		rc = 1;
	}
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//	cacheutils::flushOIDsFromCache(oidsToFlush);
	return rc;
}

uint8_t WE_DDLCommandProc::deleteSyscolumnRow(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32;;
	int txnID;
	string schema, tablename, columnname;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> tablename;
	bs >> columnname;

	ddlpackage::QualifiedName sysCatalogTableName;
	sysCatalogTableName.fSchema = CALPONT_SCHEMA;
	sysCatalogTableName.fName  = SYSCOLUMN_TABLE;

	CalpontSystemCatalog::TableColName tableColName;
	tableColName.schema = schema;
	tableColName.table = tablename;
	tableColName.column = columnname;

	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr;
	systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	u_int16_t  dbRoot;
	BRM::OID_t sysOid = 1021;
	//Find out where syscolumn is
	rc = fDbrm.getSysCatDBRoot(sysOid, dbRoot);
	fWEWrapper.setTransId(txnID);
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.startTransaction(txnID);
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;
	
	try
	{
		CalpontSystemCatalog::ROPair colRO = systemCatalogPtr->columnRID(tableColName);
		if (colRO.objnum < 0)
		{
			err = "Column not found:" + tableColName.table + "." + tableColName.column;
			throw std::runtime_error(err);
		}

		WriteEngine::ColStruct colStruct;
		WriteEngine::ColStructList colStructs;
		std::vector<WriteEngine::ColStructList> colExtentsStruct;
		std::vector<void *> colValuesList;
		WriteEngine::RIDList ridList;
		std::vector<WriteEngine::RIDList> ridLists;
		DDLColumn column;

		ridList.push_back(colRO.rid);

		ColumnList columns;
		getColumnsForTable(sessionID, sysCatalogTableName.fSchema, sysCatalogTableName.fName, columns);

		ColumnList::const_iterator column_iterator = columns.begin();
		while (column_iterator != columns.end())
		{
			column = *column_iterator;
			colStruct.dataOid = column.oid;
			colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
			colStruct.colDataType = column.colType.colDataType;
			colStruct.fColDbRoot = dbRoot;
			if (idbdatafile::IDBPolicy::useHdfs())
			{
				colStruct.fCompressionType = 2;
			}
			oids[colStruct.dataOid] = colStruct.dataOid;
			//oidsToFlush.push_back(colStruct.dataOid);
			colStructs.push_back(colStruct);

			++column_iterator;
		}
		colExtentsStruct.push_back(colStructs);
		ridLists.push_back(ridList);


		if (0 != colStructs.size() && 0 != ridLists[0].size())
		{
			int error = fWEWrapper.deleteRow(txnID, colExtentsStruct, colValuesList, ridLists, SYSCOLUMN_BASE);
			int rc1 = 0;
			if (idbdatafile::IDBPolicy::useHdfs())
			{		
				rc1 = fWEWrapper.flushDataFiles(error, txnID, oids);
				if ((error == 0) && ( rc1 == 0))
				{
				
					rc1 = fWEWrapper.confirmTransaction(txnID);
				
					if ( rc1 == NO_ERROR)
						rc1 = fWEWrapper.endTransaction(txnID, true);
					else
						fWEWrapper.endTransaction(txnID, false);
				}
				else
				{
					fWEWrapper.endTransaction(txnID, false);	
				}
			}
			if ( error == NO_ERROR)
				rc = rc1;
			else rc = error;
		}
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
	}
	catch (...)
	{
		err = "Unknown exception caught";
		rc = 1;
	}
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//	cacheutils::flushOIDsFromCache(oidsToFlush);
	
	return rc;
}

uint8_t WE_DDLCommandProc::deleteSystable(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32;;
	int txnID;
	string schema, tablename;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> tablename;

	WriteEngine::WriteEngineWrapper writeEngine;
	ddlpackage::QualifiedName sysCatalogTableName;
	sysCatalogTableName.fSchema = CALPONT_SCHEMA;
	sysCatalogTableName.fName = SYSTABLE_TABLE;

	CalpontSystemCatalog::TableName userTableName;
	userTableName.schema = schema;
	userTableName.table = tablename;

	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr;
	systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	u_int16_t  dbRoot;
	BRM::OID_t sysOid = 1021;
	//Find out where systcolumn is
	rc = fDbrm.getSysCatDBRoot(sysOid, dbRoot);
	fWEWrapper.setTransId(txnID);
	
	fWEWrapper.startTransaction(txnID);
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;
	
	try
	{
		CalpontSystemCatalog::ROPair userTableROPair = systemCatalogPtr->tableRID(userTableName);
		if (userTableROPair.rid == std::numeric_limits<WriteEngine::RID>::max())
		{
			err = "RowID is not valid ";
			throw std::runtime_error(err);
		}

		WriteEngine::ColStruct colStruct;
		WriteEngine::ColStructList colStructs;
		std::vector<WriteEngine::ColStructList> colExtentsStruct;
		std::vector<void *> colValuesList;
		WriteEngine::RIDList ridList;
		std::vector<WriteEngine::RIDList> ridLists;
		DDLColumn column;
		ridList.push_back(userTableROPair.rid);

		ColumnList columns;
		getColumnsForTable(sessionID, sysCatalogTableName.fSchema, sysCatalogTableName.fName, columns);

		ColumnList::const_iterator column_iterator = columns.begin();
		while (column_iterator != columns.end())
		{
			column = *column_iterator;
			colStruct.dataOid = column.oid;
			colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
			colStruct.colDataType = column.colType.colDataType;
			colStruct.fColDbRoot = dbRoot;
			if (idbdatafile::IDBPolicy::useHdfs())
			{
				colStruct.fCompressionType = 2;
			}
			oids[colStruct.dataOid] = colStruct.dataOid;
			//oidsToFlush.push_back(colStruct.dataOid);
			colStructs.push_back(colStruct);

			++column_iterator;
		}
		colExtentsStruct.push_back(colStructs);
		ridLists.push_back(ridList);


		if (0 != colStructs.size() && 0 != ridLists[0].size())
		{
			int error = fWEWrapper.deleteRow(txnID, colExtentsStruct, colValuesList, ridLists, SYSCOLUMN_BASE);
			int rc1 = 0;
			if (idbdatafile::IDBPolicy::useHdfs())
			{		
				rc1 = fWEWrapper.flushDataFiles(error, txnID, oids);
				if ((error == 0) && ( rc1 == 0))
				{
				
					rc1 = fWEWrapper.confirmTransaction(txnID);
				
					if ( rc1 == NO_ERROR)
						rc1 = fWEWrapper.endTransaction(txnID, true);
					else
						fWEWrapper.endTransaction(txnID, false);
				}
				else
				{
					fWEWrapper.endTransaction(txnID, false);	
				}
			}
			if ( error == NO_ERROR)
				rc = rc1;
			else rc = error;
		}
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
	}
	catch (...)
	{
		err = "Unknown exception caught";
		rc = 1;
	}
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//	cacheutils::flushOIDsFromCache(oidsToFlush);
	
	return rc;
}

uint8_t WE_DDLCommandProc::deleteSystables(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32;;
	int txnID;
	string schema, tablename;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> tablename;

	WriteEngine::WriteEngineWrapper writeEngine;
	ddlpackage::QualifiedName sysCatalogTableName;
	sysCatalogTableName.fSchema = CALPONT_SCHEMA;
	sysCatalogTableName.fName = SYSTABLE_TABLE;

	CalpontSystemCatalog::TableName userTableName;
	userTableName.schema = schema;
	userTableName.table = tablename;

	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr;
	systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	WriteEngine::ColStruct colStruct;
	WriteEngine::ColStructList colStructs;
	std::vector<WriteEngine::ColStructList> colExtentsStruct;
	std::vector<void *> colValuesList;
	WriteEngine::RIDList ridList;
	std::vector<WriteEngine::RIDList> ridLists;
	DDLColumn column;
	u_int16_t  dbRoot;
	BRM::OID_t sysOid = 1003;
	//Find out where systable is
	rc = fDbrm.getSysCatDBRoot(sysOid, dbRoot);
	fWEWrapper.setTransId(txnID);
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.startTransaction(txnID);
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;
	try
	{
		CalpontSystemCatalog::ROPair userTableROPair = systemCatalogPtr->tableRID(userTableName);
		if (userTableROPair.rid == std::numeric_limits<WriteEngine::RID>::max())
		{
			err = "RowID is not valid ";
			throw std::runtime_error(err);
		}

		ridList.push_back(userTableROPair.rid);

		ColumnList columns;
		getColumnsForTable(sessionID, sysCatalogTableName.fSchema, sysCatalogTableName.fName, columns);

		ColumnList::const_iterator column_iterator = columns.begin();
		while (column_iterator != columns.end())
		{
			column = *column_iterator;
			colStruct.dataOid = column.oid;
			colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
			colStruct.colDataType = column.colType.colDataType;
			colStruct.fColDbRoot = dbRoot;
			if (idbdatafile::IDBPolicy::useHdfs())
			{
				colStruct.fCompressionType = 2;
			}
			oids[colStruct.dataOid] = colStruct.dataOid;
			//oidsToFlush.push_back(colStruct.dataOid);
			colStructs.push_back(colStruct);

			++column_iterator;
		}
		colExtentsStruct.push_back(colStructs);
		ridLists.push_back(ridList);


		{
			int error = fWEWrapper.deleteRow(txnID, colExtentsStruct, colValuesList, ridLists, SYSCOLUMN_BASE);
			int rc1 = 0;
			if (idbdatafile::IDBPolicy::useHdfs())
			{		
				rc1 = fWEWrapper.flushDataFiles(error, txnID, oids);
				if ((error == 0) && ( rc1 == 0))
				{
				
					rc1 = fWEWrapper.confirmTransaction(txnID);
				
					if ( rc1 == NO_ERROR)
						rc1 = fWEWrapper.endTransaction(txnID, true);
					else
						fWEWrapper.endTransaction(txnID, false);
				}
				else
				{
					fWEWrapper.endTransaction(txnID, false);	
				}
			}
			if ( error == NO_ERROR)
				rc = rc1;
			else rc = error;
		}
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
	}
	catch (...)
	{
		err = "Unknown exception caught";
		rc = 1;
	}

	if (rc != 0)
		return rc;

	//deleting from SYSCOLUMN
	sysCatalogTableName.fSchema = CALPONT_SCHEMA;
	sysCatalogTableName.fName  = SYSCOLUMN_TABLE;
	sysOid = 1021;
	//Find out where syscolumn is
	rc = fDbrm.getSysCatDBRoot(sysOid, dbRoot);

	try
	{
		CalpontSystemCatalog::RIDList colRidList = systemCatalogPtr->columnRIDs(userTableName);

		colStructs.clear();
		colExtentsStruct.clear();
		colValuesList.clear();
		ridList.clear();
		ridLists.clear();
		oids.clear();
		DDLColumn column;
		CalpontSystemCatalog::RIDList::const_iterator colrid_iterator = colRidList.begin();
		while (colrid_iterator != colRidList.end())
		{
			WriteEngine::RID rid = (*colrid_iterator).rid;
			ridList.push_back(rid);
			++colrid_iterator;
		}

		ColumnList columns;
		getColumnsForTable(sessionID, sysCatalogTableName.fSchema, sysCatalogTableName.fName, columns);

		ColumnList::const_iterator column_iterator = columns.begin();
		while (column_iterator != columns.end())
		{
			column = *column_iterator;
			colStruct.dataOid = column.oid;
			colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
			colStruct.colDataType = column.colType.colDataType;
			colStruct.fColDbRoot = dbRoot;
			if (idbdatafile::IDBPolicy::useHdfs())
			{
				colStruct.fCompressionType = 2;
			}
			colStructs.push_back(colStruct);
			oids[colStruct.dataOid] = colStruct.dataOid;
			//oidsToFlush.push_back(colStruct.dataOid);
			++column_iterator;
		}
		colExtentsStruct.push_back(colStructs);
		ridLists.push_back(ridList);


		if (0 != colStructs.size() && 0 != ridLists[0].size())
		{
			int error = fWEWrapper.deleteRow(txnID, colExtentsStruct, colValuesList, ridLists, SYSCOLUMN_BASE);
			int rc1 = 0;
			if (idbdatafile::IDBPolicy::useHdfs())
			{		
				rc1 = fWEWrapper.flushDataFiles(error, txnID, oids);
				if ((error == 0) && ( rc1 == 0))
				{
				
					rc1 = fWEWrapper.confirmTransaction(txnID);
				
					if ( rc1 == NO_ERROR)
						rc1 = fWEWrapper.endTransaction(txnID, true);
					else
						fWEWrapper.endTransaction(txnID, false);
				}
				else
				{
					fWEWrapper.endTransaction(txnID, false);	
				}
			}
			if ( error == NO_ERROR)
				rc = rc1;
			else rc = error;
		}
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
	}
	catch (...)
	{
		err = "Unknown exception caught";
		rc = 1;
	}
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//	cacheutils::flushOIDsFromCache(oidsToFlush);
	return rc;
}

uint8_t WE_DDLCommandProc::dropFiles(ByteStream& bs, std::string & err)
{
	int rc = 0;
	uint32_t size, i;
	uint32_t tmp32;
	std::vector<int32_t>  dataOids;

	bs >> size;
	for (i = 0; i < size; ++i) {
		bs >> tmp32;
		dataOids.push_back(tmp32);
	}
	try {
		rc = fWEWrapper.dropFiles(0, dataOids);
	}
	catch (...)
	{
		err = "WE: Error removing files ";
		rc = 1;
	}
	purgeFDCache();
	return rc;
}

uint8_t WE_DDLCommandProc::updateSyscolumnAuto(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32;
	std::string schema, tablename;
	int txnID;
	u_int8_t tmp8;
	bool autoIncrement = false;

	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> tablename;
	bs >> tmp8;
	autoIncrement = true;

	CalpontSystemCatalog::TableName tableName;
	tableName.schema = schema;
	tableName.table = tablename;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	WriteEngine::DctColTupleList dctRowList;
	WriteEngine::DctnryTuple dctColList;

	uint16_t dbRoot=0;
	uint16_t segment;
	uint32_t partition;
	CalpontSystemCatalog::RIDList roList;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	try {
		roList = systemCatalogPtr->columnRIDs(tableName);
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}
	//Build colStructs for SYSTABLE
	std::vector<WriteEngine::RID> ridList;
	WriteEngine::ColValueList colValuesList;
	WriteEngine::ColTupleList aColList;
	WriteEngine::ColStructList colStructs;
	std::vector<void *> colOldValuesList;
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;
	
	tableName.schema = CALPONT_SCHEMA;
	tableName.table = SYSCOLUMN_TABLE;
	DDLColumn column;
	WriteEngine::ColTuple colTuple;

	findColumnData(sessionID, tableName, AUTOINC_COL, column);
	WriteEngine::ColStruct colStruct;
	WriteEngine::DctnryStruct dctnryStruct;
	colStruct.dataOid = column.oid;
	colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
	colStruct.tokenFlag = false;
	colStruct.colDataType = column.colType.colDataType;
	if (idbdatafile::IDBPolicy::useHdfs())
	{
		colStruct.fCompressionType = 2;
	}
	string s1("y"), s2("n");
	boost::any datavalue1 = s1;
	boost::any datavalue2 = s2;
	if (autoIncrement)
		colTuple.data = datavalue1;
	else
		colTuple.data = datavalue2;

	colStruct.colDataType = column.colType.colDataType;

	dctnryStruct.dctnryOid = 0;
	dctnryStruct.columnOid = colStruct.dataOid;

	colStructs.push_back(colStruct);
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	dctnryStructList.push_back(dctnryStruct);
	for (unsigned int i = 0; i < roList.size(); i++)
	{
		aColList.push_back(colTuple);
	}
	colValuesList.push_back(aColList);
	std::vector<WriteEngine::ColStructList> colExtentsStruct;
	std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
	std::vector<extentInfo> extentsinfo;
	extentInfo aExtentinfo;
	CalpontSystemCatalog::OID oid = 1021;
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.setTransId(txnID);
	for (unsigned int i = 0; i < roList.size(); i++)
	{
		convertRidToColumn(roList[i].rid, dbRoot, partition, segment, oid);

		aExtentinfo.dbRoot = dbRoot;
		aExtentinfo.partition = partition;
		aExtentinfo.segment = segment;

		if (extentsinfo.empty())
			extentsinfo.push_back(aExtentinfo);
		else if (extentsinfo.back() != aExtentinfo)
			extentsinfo.push_back(aExtentinfo);
		ridList.push_back(roList[i].rid);
	}

	std::vector<WriteEngine::RIDList> ridLists;
	ridLists.push_back(ridList);
	//build colExtentsStruct
	for (unsigned i=0; i < extentsinfo.size(); i++)
	{
		for (unsigned j=0; j < colStructs.size(); j++)
		{
			colStructs[j].fColPartition = extentsinfo[i].partition;
			colStructs[j].fColSegment = extentsinfo[i].segment;
			colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
			dctnryStructList[j].fColPartition = extentsinfo[i].partition;
			dctnryStructList[j].fColSegment = extentsinfo[i].segment;
			dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
		}
		colExtentsStruct.push_back(colStructs);
		dctnryExtentsStruct.push_back(dctnryStructList);
	}

	// call the write engine to update the row
	if (idbdatafile::IDBPolicy::useHdfs())	
		fWEWrapper.startTransaction(txnID);
	rc = fWEWrapper.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
			ridLists, dctnryExtentsStruct, dctnryValueList, SYSCOLUMN_BASE);
	if (rc != NO_ERROR)
	{
		// build the logging message
		err = "WE: Update failed on: " + tableName.table;
	}
	int rc1 = 0;
	if (idbdatafile::IDBPolicy::useHdfs())
	{		
		rc1 = fWEWrapper.flushDataFiles(rc, txnID, oids);
		if ((rc == 0) && ( rc1 == 0))
		{
				
			rc1 = fWEWrapper.confirmTransaction(txnID);
				
			if ( rc1 == NO_ERROR)
				rc1 = fWEWrapper.endTransaction(txnID, true);
			else
				fWEWrapper.endTransaction(txnID, false);
		}
		else
		{
			fWEWrapper.endTransaction(txnID, false);
		}
	}
			
		
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//	cacheutils::flushOIDsFromCache(oidsToFlush);

	return rc;
}

uint8_t WE_DDLCommandProc::updateSyscolumnNextvalCol(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32;
	std::string schema, tablename;
	int txnID;
	u_int8_t tmp8;
	bool autoIncrement = false;

	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> tablename;
	bs >> tmp8;
	autoIncrement = true;

	CalpontSystemCatalog::TableName tableName;
	tableName.schema = schema;
	tableName.table = tablename;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	WriteEngine::DctColTupleList dctRowList;
	WriteEngine::DctnryTuple dctColList;

	uint16_t dbRoot=0;
	uint16_t segment;
	uint32_t partition;

	CalpontSystemCatalog::RIDList roList;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	try {
		roList = systemCatalogPtr->columnRIDs(tableName);
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}
	//Build colStructs for SYSTABLE
	tableName.schema = CALPONT_SCHEMA;
	tableName.table = SYSCOLUMN_TABLE;
	DDLColumn column;
	WriteEngine::ColStruct colStruct;
	WriteEngine::DctnryStruct dctnryStruct;
	WriteEngine::ColTuple colTuple;
	std::vector<WriteEngine::RID> ridList;
	WriteEngine::ColValueList colValuesList;
	WriteEngine::ColTupleList aColList;
	WriteEngine::ColStructList colStructs;
	std::vector<void *> colOldValuesList;
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;

	boost::any datavalue;
	findColumnData(sessionID, tableName, AUTOINC_COL, column);
	colStruct.dataOid = column.oid;
	colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
	colStruct.tokenFlag = false;
	colStruct.colDataType = column.colType.colDataType;
	if (idbdatafile::IDBPolicy::useHdfs())
	{
		colStruct.fCompressionType = 2;
		dctnryStruct.fCompressionType = 2;
	}
	string ystr("y");
	string nstr("n");
	if (autoIncrement)
		colTuple.data = ystr;
	else
		colTuple.data = nstr;

	colStruct.colDataType = column.colType.colDataType;

	dctnryStruct.dctnryOid = 0;
	dctnryStruct.columnOid = colStruct.dataOid;
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	colStructs.push_back(colStruct);
	dctnryStructList.push_back(dctnryStruct);
	for (unsigned int i = 0; i < roList.size(); i++)
	{
		aColList.push_back(colTuple);
	}
	colValuesList.push_back(aColList);


	//get start dbroot for this PM.
	//int PMNum = Config::getLocalModuleID();
	std::vector<extentInfo> extentsinfo;
	extentInfo aExtentinfo;


	//oam.getDbroots(PMNum);
	//dbRoot will be the first dbroot on this pm. dbrootCnt will be how many dbroots on this PM.
	CalpontSystemCatalog::OID oid = 1021;
	for (unsigned int i = 0; i < roList.size(); i++)
	{
		convertRidToColumn(roList[i].rid, dbRoot, partition, segment, oid);

		aExtentinfo.dbRoot = dbRoot;
		aExtentinfo.partition = partition;
		aExtentinfo.segment = segment;

		if (extentsinfo.empty())
			extentsinfo.push_back(aExtentinfo);
		else if (extentsinfo.back() != aExtentinfo)
			extentsinfo.push_back(aExtentinfo);
		ridList.push_back(roList[i].rid);
	}

	std::vector<WriteEngine::RIDList> ridLists;
	std::vector<WriteEngine::ColStructList> colExtentsStruct;
	std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
	ridLists.push_back(ridList);
	//build colExtentsStruct
	for (unsigned i=0; i < extentsinfo.size(); i++)
	{
		for (unsigned j=0; j < colStructs.size(); j++)
		{
			colStructs[j].fColPartition = extentsinfo[i].partition;
			colStructs[j].fColSegment = extentsinfo[i].segment;
			colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
			dctnryStructList[j].fColPartition = extentsinfo[i].partition;
			dctnryStructList[j].fColSegment = extentsinfo[i].segment;
			dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
		}
		colExtentsStruct.push_back(colStructs);
		dctnryExtentsStruct.push_back(dctnryStructList);
	}

	// call the write engine to update the row
	fWEWrapper.setTransId(txnID);
	fWEWrapper.startTransaction(txnID);
	
	rc = fWEWrapper.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
			ridLists, dctnryExtentsStruct, dctnryValueList, SYSCOLUMN_BASE);
	
	if (rc != NO_ERROR)
	{
		// build the logging message
		err = "WE: Update failed on: " + tableName.table;
	}
	
	if (idbdatafile::IDBPolicy::useHdfs())	
	{
		fWEWrapper.flushDataFiles(rc, txnID, oids);
		fWEWrapper.confirmTransaction(txnID);
		if ( rc == 0)
			fWEWrapper.endTransaction(txnID, true);
		else
			fWEWrapper.endTransaction(txnID, false);
	}
	
	systemCatalogPtr->flushCache();
	purgeFDCache();
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	return rc;
}

uint8_t WE_DDLCommandProc::updateSyscolumnTablename(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32;
	std::string schema, oldTablename, newTablename;
	int txnID;

	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> oldTablename;
	bs >> newTablename;

	CalpontSystemCatalog::TableName tableName;
	tableName.schema = schema;
	tableName.table = oldTablename;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	WriteEngine::DctColTupleList dctRowList;
	WriteEngine::DctnryTuple dctColList;
	WriteEngine::ColTuple colTuple;
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;

	uint16_t dbRoot=0;
	uint16_t segment;
	uint32_t partition;

	CalpontSystemCatalog::RIDList roList;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);

	try {
		roList = systemCatalogPtr->columnRIDs(tableName);
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}
	//Build colStructs for SYSTABLE
	std::vector<WriteEngine::RID> ridList;
	WriteEngine::ColValueList colValuesList;
	WriteEngine::ColTupleList aColList;
	WriteEngine::ColStructList colStructs;
	std::vector<void *> colOldValuesList;
	tableName.schema = CALPONT_SCHEMA;
	tableName.table = SYSCOLUMN_TABLE;
	DDLColumn column;
	findColumnData(sessionID, tableName, TABLENAME_COL, column);
	WriteEngine::ColStruct colStruct;
	WriteEngine::DctnryStruct dctnryStruct;

	colStruct.dataOid = column.oid;
	colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
	colStruct.tokenFlag = false;
	if ( (column.colType.colDataType == CalpontSystemCatalog::CHAR
				&& column.colType.colWidth > 8)
			|| (column.colType.colDataType == CalpontSystemCatalog::VARCHAR
				&& column.colType.colWidth > 7)
			|| (column.colType.colDataType == CalpontSystemCatalog::VARBINARY
				&& column.colType.colWidth > 7)
			|| (column.colType.colDataType == CalpontSystemCatalog::DECIMAL
				&& column.colType.precision > 18)
			|| (column.colType.colDataType == CalpontSystemCatalog::UDECIMAL
				&& column.colType.precision > 18) )//token
	{
		colStruct.colWidth = 8;
		colStruct.tokenFlag = true;
	}
	else
	{
		colStruct.colWidth = column.colType.colWidth;
	}
	colStruct.colDataType = column.colType.colDataType;

	//Tokenize the data value
	WriteEngine::DctnryStruct dictStruct;
	dictStruct.dctnryOid = column.colType.ddn.dictOID;
	dictStruct.columnOid = column.colType.columnOID;
	WriteEngine::DctnryTuple  dictTuple;
	memcpy(dictTuple.sigValue, newTablename.c_str(), newTablename.length());
	dictTuple.sigSize = newTablename.length();
	int error = NO_ERROR;
	fWEWrapper.setTransId(txnID);
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.startTransaction(txnID);
	if (NO_ERROR != (error = fWEWrapper.tokenize(txnID, dictStruct, dictTuple)))
	{
		WErrorCodes ec;
		throw std::runtime_error("WE: Tokenization failed " + ec.errorString(error));
	}
	WriteEngine::Token aToken = dictTuple.token;
	colTuple.data = aToken;

	colStruct.colDataType = column.colType.colDataType;

	if (idbdatafile::IDBPolicy::useHdfs())
	{
		colStruct.fCompressionType = 2;
		dctnryStruct.fCompressionType = 2;
	}
	if (colStruct.tokenFlag)
	{
		dctnryStruct.dctnryOid = column.colType.ddn.dictOID;
		dctnryStruct.columnOid = colStruct.dataOid;
	}
	else
	{
		dctnryStruct.dctnryOid = 0;
		dctnryStruct.columnOid = colStruct.dataOid;
	}

	colStructs.push_back(colStruct);
	dctnryStructList.push_back(dctnryStruct);
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	if (dctnryStruct.dctnryOid > 0)
	{
		oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
		//oidsToFlush.push_back(dctnryStruct.dctnryOid);
	}
		
	for (unsigned int i = 0; i < roList.size(); i++)
	{
		aColList.push_back(colTuple);
	}
	colValuesList.push_back(aColList);


	//It's the same string for each column, so we just need one dictionary struct
	WriteEngine::DctnryTuple  dctnryTuple;
	memset(&dctnryTuple, 0, sizeof(dctnryTuple));
	memcpy(dctnryTuple.sigValue, newTablename.c_str(), newTablename.length());
	dctnryTuple.sigSize = newTablename.length();
	dctnryTuple.isNull = false;
	dctColList = dctnryTuple;
	dctRowList.push_back(dctColList);
	dctnryValueList.push_back(dctRowList);
	std::vector<extentInfo> extentsinfo;
	extentInfo aExtentinfo;
	std::vector<WriteEngine::ColStructList> colExtentsStruct;
	std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;

	CalpontSystemCatalog::OID oid = 1021;
	for (unsigned int i = 0; i < roList.size(); i++)
	{
		convertRidToColumn(roList[i].rid, dbRoot, partition, segment, oid);

		aExtentinfo.dbRoot = dbRoot;
		aExtentinfo.partition = partition;
		aExtentinfo.segment = segment;

		if (extentsinfo.empty())
			extentsinfo.push_back(aExtentinfo);
		else if (extentsinfo.back() != aExtentinfo)
			extentsinfo.push_back(aExtentinfo);
		ridList.push_back(roList[i].rid);
	}

	std::vector<WriteEngine::RIDList> ridLists;
	ridLists.push_back(ridList);
	//build colExtentsStruct
	for (unsigned i=0; i < extentsinfo.size(); i++)
	{
		for (unsigned j=0; j < colStructs.size(); j++)
		{
			colStructs[j].fColPartition = extentsinfo[i].partition;
			colStructs[j].fColSegment = extentsinfo[i].segment;
			colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
			dctnryStructList[j].fColPartition = extentsinfo[i].partition;
			dctnryStructList[j].fColSegment = extentsinfo[i].segment;
			dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
		}
		colExtentsStruct.push_back(colStructs);
		dctnryExtentsStruct.push_back(dctnryStructList);
	}

	// call the write engine to update the row
	rc = fWEWrapper.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
			ridLists, dctnryExtentsStruct, dctnryValueList, SYSCOLUMN_BASE);
	if (rc != NO_ERROR)
	{
		// build the logging message
		err = "WE: Update failed on: " + tableName.table;
	}
	int rc1 = 0;
	if (idbdatafile::IDBPolicy::useHdfs())
	{		
		rc1 = fWEWrapper.flushDataFiles(rc, txnID, oids);
		if ((rc == 0) && ( rc1 == 0))
		{
				
			rc1 = fWEWrapper.confirmTransaction(txnID);
				
			if ( rc1 == NO_ERROR)
				rc1 = fWEWrapper.endTransaction(txnID, true);
			else
				fWEWrapper.endTransaction(txnID, false);
		}
		else
		{
			fWEWrapper.endTransaction(txnID, false);
		}
	}
	
	if (rc ==0 )
		rc = rc1;
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//		cacheutils::flushOIDsFromCache(oidsToFlush);
	
	return rc;
}

uint8_t WE_DDLCommandProc::updateSystableAuto(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32, autoVal;
	std::string schema, tablename;
	int txnID;

	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> tablename;
	bs >> autoVal;

	CalpontSystemCatalog::TableName tableName;
	tableName.schema = schema;
	tableName.table = tablename;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	WriteEngine::DctColTupleList dctRowList;
	WriteEngine::DctnryTuple dctColList;

	uint16_t dbRoot=0;
	uint16_t segment;
	uint32_t partition;

	CalpontSystemCatalog::ROPair ropair;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);

	try {
		ropair = systemCatalogPtr->tableRID(tableName);
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}
	if (ropair.objnum < 0)
	{
		err = "No such table: " + tableName.table;
		rc = 1;
		return rc;
	}
	// now we have to prepare the various structures for the WE to update the column.

	std::vector<WriteEngine::RID> ridList;
	WriteEngine::ColValueList colValuesList;
	WriteEngine::ColTupleList aColList;
	WriteEngine::ColStructList colStructs;
	std::vector<void *> colOldValuesList;
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;
	boost::any datavalue;
	datavalue = autoVal;

	WriteEngine::ColTuple colTuple;

	//Build colStructs for SYSTABLE
	tableName.schema = CALPONT_SCHEMA;
	tableName.table = SYSTABLE_TABLE;
	DDLColumn column;
	findColumnData (sessionID, tableName, AUTOINC_COL, column);
	WriteEngine::ColStruct colStruct;
	WriteEngine::DctnryStruct dctnryStruct;
	colStruct.dataOid = column.oid;
	colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
	colStruct.tokenFlag = false;

	colStruct.colDataType = column.colType.colDataType;

	colTuple.data = datavalue;

	dctnryStruct.dctnryOid = 0;
	dctnryStruct.columnOid = colStruct.dataOid;

	if (idbdatafile::IDBPolicy::useHdfs())
	{
		colStruct.fCompressionType = 2;
		dctnryStruct.fCompressionType = 2;
	}
	colStructs.push_back(colStruct);
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	dctnryStructList.push_back(dctnryStruct);
	aColList.push_back(colTuple);
	colValuesList.push_back(aColList);
	std::vector<WriteEngine::ColStructList> colExtentsStruct;
	std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;


	WriteEngine::DctnryTuple dctnryTuple;
	dctColList = dctnryTuple;
	dctRowList.push_back(dctColList);
	dctnryValueList.push_back(dctRowList);

	//In this case, there's only 1 row, so only one one extent, but keep it generic...
	std::vector<extentInfo> extentsinfo;
	extentInfo aExtentinfo;
	CalpontSystemCatalog::OID oid = 1003;
	convertRidToColumn(ropair.rid, dbRoot, partition, segment, oid);

	ridList.push_back(ropair.rid);
	std::vector<WriteEngine::RIDList> ridLists;
	ridLists.push_back(ridList);
	aExtentinfo.dbRoot = dbRoot;
	aExtentinfo.partition = partition;
	aExtentinfo.segment = segment;

	extentsinfo.push_back(aExtentinfo);

	//build colExtentsStruct
	for (unsigned i=0; i < extentsinfo.size(); i++)
	{
		for (unsigned j=0; j < colStructs.size(); j++)
		{
			colStructs[j].fColPartition = extentsinfo[i].partition;
			colStructs[j].fColSegment = extentsinfo[i].segment;
			colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
			dctnryStructList[j].fColPartition = extentsinfo[i].partition;
			dctnryStructList[j].fColSegment = extentsinfo[i].segment;
			dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
		}
		colExtentsStruct.push_back(colStructs);
		dctnryExtentsStruct.push_back(dctnryStructList);
	}

	// call the write engine to update the row
	fWEWrapper.setTransId(txnID);
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.startTransaction(txnID);
	
	rc = fWEWrapper.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
				ridLists, dctnryExtentsStruct, dctnryValueList, SYSCOLUMN_BASE);

	if (rc != NO_ERROR)
	{
		// build the logging message
		err = "WE: Update failed on: " + tableName.table;
	}
	int rc1 = 0;
	if (idbdatafile::IDBPolicy::useHdfs())
	{		
		rc1 = fWEWrapper.flushDataFiles(rc, txnID, oids);
		if ((rc == 0) && ( rc1 == 0))
		{
				
			rc1 = fWEWrapper.confirmTransaction(txnID);
				
			if ( rc1 == NO_ERROR)
				rc1 = fWEWrapper.endTransaction(txnID, true);
			else
				fWEWrapper.endTransaction(txnID, false);
		}
		else
		{
			fWEWrapper.endTransaction(txnID, false);
		}
	}
	
	if (rc ==0 )
		rc = rc1;
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//		cacheutils::flushOIDsFromCache(oidsToFlush);
	return rc;
}

uint8_t WE_DDLCommandProc::updateSystableTablename(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32;
	std::string schema, oldTablename, newTablename;
	int txnID;

	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> oldTablename;
	bs >> newTablename;

	CalpontSystemCatalog::TableName tableName;
	tableName.schema = schema;
	tableName.table = oldTablename;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	WriteEngine::DctColTupleList dctRowList;
	WriteEngine::DctnryTuple dctColList;

	uint16_t dbRoot=0;
	uint16_t segment;
	uint32_t partition;

	CalpontSystemCatalog::ROPair ropair;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);

	try {
		ropair = systemCatalogPtr->tableRID(tableName);
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}
	if (ropair.objnum < 0)
	{
		err = "No such table: " + tableName.table;
		return 1;
	}
	// now we have to prepare the various structures for the WE to update the column.

	std::vector<WriteEngine::RID> ridList;
	WriteEngine::ColValueList colValuesList;
	WriteEngine::ColTupleList aColList;
	WriteEngine::ColStructList colStructs;
	std::vector<void *> colOldValuesList;
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;
	boost::any datavalue;
	datavalue = newTablename;

	WriteEngine::ColTuple colTuple;

	//Build colStructs for SYSTABLE
	tableName.schema = CALPONT_SCHEMA;
	tableName.table = SYSTABLE_TABLE;
	DDLColumn column;
	findColumnData (sessionID, tableName, TABLENAME_COL, column);
	WriteEngine::ColStruct colStruct;
	WriteEngine::DctnryStruct dctnryStruct;
	colStruct.dataOid = column.oid;
	colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
	colStruct.tokenFlag = true;

	colStruct.colDataType = column.colType.colDataType;
	//Tokenize the data value
	WriteEngine::DctnryStruct dictStruct;
	dictStruct.dctnryOid = column.colType.ddn.dictOID;
	dictStruct.columnOid = column.colType.columnOID;
	WriteEngine::DctnryTuple  dictTuple;
	memcpy(dictTuple.sigValue, newTablename.c_str(), newTablename.length());
	dictTuple.sigSize = newTablename.length();
	int error = NO_ERROR;
	fWEWrapper.setTransId(txnID);
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.startTransaction(txnID);
	
	if (NO_ERROR != (error = fWEWrapper.tokenize(txnID, dictStruct, dictTuple)))
	{
		WErrorCodes ec;
		throw std::runtime_error("WE: Tokenization failed " + ec.errorString(error));
	}
	WriteEngine::Token aToken = dictTuple.token;
	colTuple.data = aToken;

	if (idbdatafile::IDBPolicy::useHdfs())
	{
		colStruct.fCompressionType = 2;
		dctnryStruct.fCompressionType = 2;
	}
	
	if (colStruct.tokenFlag)
	{
		dctnryStruct.dctnryOid = column.colType.ddn.dictOID;
		dctnryStruct.columnOid = colStruct.dataOid;
	}
	else
	{
		dctnryStruct.dctnryOid = 0;
		dctnryStruct.columnOid = colStruct.dataOid;
	}

	colStructs.push_back(colStruct);
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	if (dctnryStruct.dctnryOid > 0)
		oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
		
	dctnryStructList.push_back(dctnryStruct);
	aColList.push_back(colTuple);
	colValuesList.push_back(aColList);
	std::vector<WriteEngine::ColStructList> colExtentsStruct;
	std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;


	WriteEngine::DctnryTuple dctnryTuple;
	dctColList = dctnryTuple;
	dctRowList.push_back(dctColList);
	dctnryValueList.push_back(dctRowList);

	//In this case, there's only 1 row, so only one one extent, but keep it generic...
	std::vector<extentInfo> extentsinfo;
	extentInfo aExtentinfo;
	CalpontSystemCatalog::OID oid = 1003;
	convertRidToColumn(ropair.rid, dbRoot, partition, segment, oid);

	ridList.push_back(ropair.rid);
	std::vector<WriteEngine::RIDList> ridLists;
	ridLists.push_back(ridList);
	aExtentinfo.dbRoot = dbRoot;
	aExtentinfo.partition = partition;
	aExtentinfo.segment = segment;

	extentsinfo.push_back(aExtentinfo);

	//build colExtentsStruct
	for (unsigned i=0; i < extentsinfo.size(); i++)
	{
		for (unsigned j=0; j < colStructs.size(); j++)
		{
			colStructs[j].fColPartition = extentsinfo[i].partition;
			colStructs[j].fColSegment = extentsinfo[i].segment;
			colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
			dctnryStructList[j].fColPartition = extentsinfo[i].partition;
			dctnryStructList[j].fColSegment = extentsinfo[i].segment;
			dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
		}
		colExtentsStruct.push_back(colStructs);
		dctnryExtentsStruct.push_back(dctnryStructList);
	}

	// call the write engine to update the row
	rc = fWEWrapper.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
				ridLists, dctnryExtentsStruct, dctnryValueList, SYSCOLUMN_BASE);

	if (rc != NO_ERROR)
	{
		// build the logging message
		err = "WE: Update failed on: " + tableName.table;
		throw std::runtime_error(err);
	}
	int rc1 = 0;
	if (idbdatafile::IDBPolicy::useHdfs())
	{		
		rc1 = fWEWrapper.flushDataFiles(rc, txnID, oids);
		if ((rc == 0) && ( rc1 == 0))
		{
				
			rc1 = fWEWrapper.confirmTransaction(txnID);
				
			if ( rc1 == NO_ERROR)
				rc1 = fWEWrapper.endTransaction(txnID, true);
			else
				fWEWrapper.endTransaction(txnID, false);
		}
		else
		{
			fWEWrapper.endTransaction(txnID, false);
		}
	}
	
	if (rc ==0 )
		rc = rc1;
		
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//		cacheutils::flushOIDsFromCache(oidsToFlush);
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	return rc;
}

uint8_t WE_DDLCommandProc::updateSystablesTablename(ByteStream& bs, std::string & err)
{
	int rc = 0;
	u_int32_t sessionID, tmp32;
	std::string schema, oldTablename, newTablename;
	int txnID;

	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> oldTablename;
	bs >> newTablename;

	CalpontSystemCatalog::TableName tableName;
	tableName.schema = schema;
	tableName.table = oldTablename;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	WriteEngine::DctColTupleList dctRowList;
	WriteEngine::DctnryTuple dctColList;

	uint16_t dbRoot=0;
	uint16_t segment;
	uint32_t partition;

	CalpontSystemCatalog::ROPair ropair;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);

	//@bug 4592 Error handling for syscat call

	try {
		ropair = systemCatalogPtr->tableRID(tableName);
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}

	if (ropair.objnum < 0)
	{
		err = "No such table: " + tableName.table;
		return 1;
	}
	// now we have to prepare the various structures for the WE to update the column.

	std::vector<WriteEngine::RID> ridList;
	WriteEngine::ColValueList colValuesList;
	WriteEngine::ColTupleList aColList;
	WriteEngine::ColStructList colStructs;
	std::vector<void *> colOldValuesList;
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;
	boost::any datavalue;
	datavalue = newTablename;

	WriteEngine::ColTuple colTuple;

	//Build colStructs for SYSTABLE
	tableName.schema = CALPONT_SCHEMA;
	tableName.table = SYSTABLE_TABLE;
	DDLColumn column;
	findColumnData (sessionID, tableName, TABLENAME_COL, column);
	WriteEngine::ColStruct colStruct;
	WriteEngine::DctnryStruct dctnryStruct;
	colStruct.dataOid = column.oid;
	colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
	colStruct.tokenFlag = true;

	colStruct.colDataType = column.colType.colDataType;
	//Tokenize the data value
	WriteEngine::DctnryStruct dictStruct;
	dictStruct.dctnryOid = column.colType.ddn.dictOID;
	dictStruct.columnOid = column.colType.columnOID;
	WriteEngine::DctnryTuple  dictTuple;
	dictTuple.isNull = false;
	memcpy(dictTuple.sigValue, newTablename.c_str(), newTablename.length());
	dictTuple.sigSize = newTablename.length();
	//int error = NO_ERROR;
	//if (NO_ERROR != (error = fWEWrapper.tokenize(txnID, dictStruct, dictTuple)))
	//{
	//	WErrorCodes ec;
	//	throw std::runtime_error("WE: Tokenization failed " + ec.errorString(error));
	//}
	//WriteEngine::Token aToken = dictTuple.token;

	//colTuple.data = aToken;
	//cout << "token value for new table name is op:fbo = " << aToken.op <<":" << aToken.fbo << " null flag = " << (uint)dictTuple.isNull<< endl;
	if (idbdatafile::IDBPolicy::useHdfs())
	{
		colStruct.fCompressionType = 2;
		dctnryStruct.fCompressionType = 2;
	}
	if (colStruct.tokenFlag)
	{
		dctnryStruct.dctnryOid = column.colType.ddn.dictOID;
		dctnryStruct.columnOid = colStruct.dataOid;
	}
	else
	{
		dctnryStruct.dctnryOid = 0;
		dctnryStruct.columnOid = colStruct.dataOid;
	}

	colStructs.push_back(colStruct);
	dctnryStructList.push_back(dctnryStruct);
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	if (dctnryStruct.dctnryOid>0)
	{
		oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
		//oidsToFlush.push_back(dctnryStruct.dctnryOid);
	}
	aColList.push_back(colTuple);
	colValuesList.push_back(aColList);
	std::vector<WriteEngine::ColStructList> colExtentsStruct;
	std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;

	dctColList = dictTuple;
	dctRowList.push_back(dctColList);
	dctnryValueList.push_back(dctRowList);

	//In this case, there's only 1 row, so only one one extent, but keep it generic...
	std::vector<extentInfo> extentsinfo;
	extentInfo aExtentinfo;
	CalpontSystemCatalog::OID oid = 1003;
	convertRidToColumn(ropair.rid, dbRoot, partition, segment, oid);

	ridList.push_back(ropair.rid);
	std::vector<WriteEngine::RIDList> ridLists;
	ridLists.push_back(ridList);
	aExtentinfo.dbRoot = dbRoot;
	aExtentinfo.partition = partition;
	aExtentinfo.segment = segment;

	extentsinfo.push_back(aExtentinfo);

	//build colExtentsStruct
	for (unsigned i=0; i < extentsinfo.size(); i++)
	{
		for (unsigned j=0; j < colStructs.size(); j++)
		{
			colStructs[j].fColPartition = extentsinfo[i].partition;
			colStructs[j].fColSegment = extentsinfo[i].segment;
			colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
			dctnryStructList[j].fColPartition = extentsinfo[i].partition;
			dctnryStructList[j].fColSegment = extentsinfo[i].segment;
			dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
		}
		colExtentsStruct.push_back(colStructs);
		dctnryExtentsStruct.push_back(dctnryStructList);
	}

	// call the write engine to update the row
	fWEWrapper.setTransId(txnID);
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.startTransaction(txnID);
	
	rc = fWEWrapper.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
				ridLists, dctnryExtentsStruct, dctnryValueList, SYSCOLUMN_BASE);
	
	if (rc != NO_ERROR)
	{
		// build the logging message
		err = "WE: Update failed on: " + tableName.table;
		int rc1 = 0;
		if (idbdatafile::IDBPolicy::useHdfs())
		{		
			rc1 = fWEWrapper.flushDataFiles(rc, txnID, oids);
			if ((rc == 0) && ( rc1 == 0))
			{
				
				rc1 = fWEWrapper.confirmTransaction(txnID);
				
			if ( rc1 == NO_ERROR)
				rc1 = fWEWrapper.endTransaction(txnID, true);
			else
				fWEWrapper.endTransaction(txnID, false);
			}
			else
			{
				fWEWrapper.endTransaction(txnID, false);
			}
		}
	
		if (rc ==0 )
			rc = rc1;
		if (rc != 0)
			return rc;
	}
	//cout << "rename:systable is updated to " << newTablename << " for rid " << ropair.rid << endl;
	//Update SYSCOLUMN table
	tableName.schema = schema;
	tableName.table = oldTablename;
	dctnryStructList.clear();
	dctnryValueList.clear();
	dctRowList.clear();

	CalpontSystemCatalog::RIDList roList;
	try {
		roList = systemCatalogPtr->columnRIDs(tableName);
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}

	//Build colStructs for SYSCOLUMN
	ridList.clear();
	colValuesList.clear();
	aColList.clear();
	colStructs.clear();
	colOldValuesList.clear();
	oids.clear();
	tableName.schema = CALPONT_SCHEMA;
	tableName.table = SYSCOLUMN_TABLE;
	findColumnData(sessionID, tableName, TABLENAME_COL, column);

	colStruct.dataOid = column.oid;
	colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
	colStruct.tokenFlag = false;
	if ( (column.colType.colDataType == CalpontSystemCatalog::CHAR
				&& column.colType.colWidth > 8)
			|| (column.colType.colDataType == CalpontSystemCatalog::VARCHAR
				&& column.colType.colWidth > 7)
			|| (column.colType.colDataType == CalpontSystemCatalog::VARBINARY
				&& column.colType.colWidth > 7)
			|| (column.colType.colDataType == CalpontSystemCatalog::DECIMAL
				&& column.colType.precision > 18)
			|| (column.colType.colDataType == CalpontSystemCatalog::UDECIMAL
				&& column.colType.precision > 18) )//token
	{
		colStruct.colWidth = 8;
		colStruct.tokenFlag = true;
	}
	else
	{
		colStruct.colWidth = column.colType.colWidth;
	}
	colStruct.colDataType = column.colType.colDataType;

	//Tokenize the data value
	dictStruct.dctnryOid = column.colType.ddn.dictOID;
	dictStruct.columnOid = column.colType.columnOID;
	memcpy(dictTuple.sigValue, newTablename.c_str(), newTablename.length());
	dictTuple.sigSize = newTablename.length();
	dictTuple.isNull = false;
	/*
	if (NO_ERROR != (error = fWEWrapper.tokenize(txnID, dictStruct, dictTuple)))
	{
		WErrorCodes ec;
		throw std::runtime_error("WE: Tokenization failed " + ec.errorString(error));
	}
	aToken = dictTuple.token;
	colTuple.data = aToken; */

	colStruct.colDataType = column.colType.colDataType;

	if (idbdatafile::IDBPolicy::useHdfs())
	{
		colStruct.fCompressionType = 2;
		dctnryStruct.fCompressionType = 2;
	}
			
	if (colStruct.tokenFlag)
	{
		dctnryStruct.dctnryOid = column.colType.ddn.dictOID;
		dctnryStruct.columnOid = colStruct.dataOid;
	}
	else
	{
		dctnryStruct.dctnryOid = 0;
		dctnryStruct.columnOid = colStruct.dataOid;
	}
	
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	if (dctnryStruct.dctnryOid>0)
	{
		oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
		//oidsToFlush.push_back(dctnryStruct.dctnryOid);
	}
		
	colStructs.push_back(colStruct);
	dctnryStructList.push_back(dctnryStruct);
	
	for (unsigned int i = 0; i < roList.size(); i++)
	{
		aColList.push_back(colTuple);
	}
	colValuesList.push_back(aColList);


	//It's the same string for each column, so we just need one dictionary struct
	memset(&dictTuple, 0, sizeof(dictTuple));
	memcpy(dictTuple.sigValue, newTablename.c_str(), newTablename.length());
	dictTuple.sigSize = newTablename.length();
	dictTuple.isNull = false;
	dctColList = dictTuple;
	dctRowList.push_back(dctColList);
	dctnryValueList.push_back(dctRowList);
	extentsinfo.clear();
	colExtentsStruct.clear();
	dctnryExtentsStruct.clear();
	oid = 1021;
	for (unsigned int i = 0; i < roList.size(); i++)
	{
		convertRidToColumn(roList[i].rid, dbRoot, partition, segment, oid);

		aExtentinfo.dbRoot = dbRoot;
		aExtentinfo.partition = partition;
		aExtentinfo.segment = segment;

		if (extentsinfo.empty())
			extentsinfo.push_back(aExtentinfo);
		else if (extentsinfo.back() != aExtentinfo)
			extentsinfo.push_back(aExtentinfo);
		ridList.push_back(roList[i].rid);
	}

	ridLists.clear();
	ridLists.push_back(ridList);
	//build colExtentsStruct
	for (unsigned i=0; i < extentsinfo.size(); i++)
	{
		for (unsigned j=0; j < colStructs.size(); j++)
		{
			colStructs[j].fColPartition = extentsinfo[i].partition;
			colStructs[j].fColSegment = extentsinfo[i].segment;
			colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
			dctnryStructList[j].fColPartition = extentsinfo[i].partition;
			dctnryStructList[j].fColSegment = extentsinfo[i].segment;
			dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
		}
		colExtentsStruct.push_back(colStructs);
		dctnryExtentsStruct.push_back(dctnryStructList);
	}

	// call the write engine to update the row
	rc = fWEWrapper.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
			ridLists, dctnryExtentsStruct, dctnryValueList, SYSCOLUMN_BASE);
	if (rc != NO_ERROR)
	{
		// build the logging message
		err = "WE: Update failed on: " + tableName.table;
	}
	int rc1 = 0;
	if (idbdatafile::IDBPolicy::useHdfs())
	{		
		rc1 = fWEWrapper.flushDataFiles(rc, txnID, oids);
		if ((rc == 0) && ( rc1 == 0))
		{
				
			rc1 = fWEWrapper.confirmTransaction(txnID);
				
			if ( rc1 == NO_ERROR)
				rc1 = fWEWrapper.endTransaction(txnID, true);
			else
				fWEWrapper.endTransaction(txnID, false);
		}
		else
		{
			fWEWrapper.endTransaction(txnID, false);
		}
	}
	
	if (rc ==0 )
		rc = rc1;
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//	cacheutils::flushOIDsFromCache(oidsToFlush);
	//cout << "rename:syscolumn is updated" << endl;
	return rc;
}

uint8_t WE_DDLCommandProc::updateSyscolumnColumnposCol(messageqcpp::ByteStream& bs, std::string & err)
{
	int rc = 0;
	int colPos;
	string schema, atableName;
	u_int32_t sessionID, tmp32;
	int txnID;

	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> atableName;
	bs >> tmp32;
	colPos = tmp32;

	WriteEngine::RIDList ridList;
	WriteEngine::ColValueList colValuesList;
	WriteEngine::ColValueList colOldValuesList;
	CalpontSystemCatalog::TableName tableName;
	tableName.table = atableName;
	tableName.schema = schema;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
	CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	CalpontSystemCatalog::RIDList rids;
	try {
		rids = systemCatalogPtr->columnRIDs(tableName);
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}
	CalpontSystemCatalog::RIDList::const_iterator rid_iter = rids.begin();
	boost::any value;
	WriteEngine::ColTupleList colTuples;
	CalpontSystemCatalog::ColType columnType;
	CalpontSystemCatalog::ROPair colRO;
	//cout << "colpos is " << colPos << endl;
	try {
		while (rid_iter != rids.end())
		{
			//look up colType
			colRO = *rid_iter;
			columnType = systemCatalogPtr->colType(colRO.objnum);
			if (columnType.colPosition < colPos)
			{
				++rid_iter;
				continue;
			}
			ridList.push_back(colRO.rid);
			value = columnType.colPosition - 1;
			WriteEngine::ColTuple colTuple;
			colTuple.data = value;
			colTuples.push_back(colTuple);
			++rid_iter;
		}
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}
	colValuesList.push_back(colTuples);
	u_int16_t  dbRoot;
	BRM::OID_t sysOid = 1021;
	//Find out where systable is
	rc = fDbrm.getSysCatDBRoot(sysOid, dbRoot);
	fWEWrapper.setTransId(txnID);
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.startTransaction(txnID);
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;
	if (colTuples.size() > 0)
	{
		WriteEngine::ColStructList  colStructs;
		WriteEngine::ColStruct colStruct;
		WriteEngine::DctnryStructList dctnryStructList;
		WriteEngine::DctnryValueList dctnryValueList;
		//Build column structure for COLUMNPOS_COL
		colStruct.dataOid = OID_SYSCOLUMN_COLUMNPOS;
		colStruct.colWidth = 4;
		colStruct.tokenFlag = false;
		colStruct.colDataType = CalpontSystemCatalog::INT;
		colStruct.fColDbRoot = dbRoot;
		if (idbdatafile::IDBPolicy::useHdfs())
		{
			colStruct.fCompressionType = 2;
		}
		colStructs.push_back(colStruct);
		oids[colStruct.dataOid] = colStruct.dataOid;
		//oidsToFlush.push_back(colStruct.dataOid);
		rc = fWEWrapper.updateColumnRecs( txnID, colStructs, colValuesList, ridList, SYSCOLUMN_BASE );
	}
	int rc1 = 0;
	if (idbdatafile::IDBPolicy::useHdfs())
	{		
		rc1 = fWEWrapper.flushDataFiles(rc, txnID, oids);
		if ((rc == 0) && ( rc1 == 0))
		{
				
			rc1 = fWEWrapper.confirmTransaction(txnID);
				
			if ( rc1 == NO_ERROR)
				rc1 = fWEWrapper.endTransaction(txnID, true);
			else
				fWEWrapper.endTransaction(txnID, false);
		}
		else
		{
			fWEWrapper.endTransaction(txnID, false);
		}
	}
	
	if (rc ==0 )
		rc = rc1;
		
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//	cacheutils::flushOIDsFromCache(oidsToFlush);
	return rc;
}

uint8_t WE_DDLCommandProc::fillNewColumn(ByteStream& bs, std::string& err)
{
	int rc = 0;
	u_int32_t tmp32;
	u_int8_t tmp8;
	int txnID;
	OID dataOid, dictOid, refColOID;
	CalpontSystemCatalog::ColDataType dataType, refColDataType;
	bool autoincrement;
	int dataWidth, scale, precision, compressionType, refColWidth, refCompressionType;
	string defaultValStr;
	ColTuple defaultVal;

	bs >> tmp32;
	txnID = tmp32;
	bs >> tmp32;
	dataOid = tmp32;
	bs >> tmp32;
	dictOid = tmp32;
	bs >> tmp8;
	dataType = (CalpontSystemCatalog::ColDataType) tmp8;
	bs >> tmp8;
	autoincrement = (tmp8 != 0);
	bs >> tmp32;
	dataWidth = tmp32;
	bs >> tmp32;
	scale = tmp32;
	bs >> tmp32;
	precision = tmp32;
	bs >> defaultValStr;
	bs >> tmp8;
	compressionType = tmp8;
	bs >> tmp32;
	refColOID = tmp32;
	bs >> tmp8;
	refColDataType = (CalpontSystemCatalog::ColDataType) tmp8;
	bs >> tmp32;
	refColWidth = tmp32;
	bs >> tmp8;
	refCompressionType = tmp8;
	//Find the fill in value
	bool isNULL = false;
	if (defaultValStr == "")
		isNULL = true;

	CalpontSystemCatalog::ColType colType;
	colType.colDataType = static_cast<CalpontSystemCatalog::ColDataType>(dataType);
	colType.colWidth = dataWidth;
	colType.scale = scale;
	colType.precision = precision;
	bool pushWarning = false;
	defaultVal.data = DataConvert::convertColumnData(colType, defaultValStr, pushWarning, isNULL);
	fWEWrapper.setTransId(txnID);
	fWEWrapper.setIsInsert(true);
	fWEWrapper.setBulkFlag(true);
	std::map<uint32_t,uint32_t> oids;
	oids[dataOid] = dataOid;
	oids[refColOID] = refColOID;
	rc = fWEWrapper.fillColumn(txnID, dataOid, dataType, dataWidth, defaultVal,refColOID, refColDataType,
			 refColWidth, refCompressionType, isNULL, compressionType, defaultValStr, dictOid, autoincrement);
	if ( rc != 0 )
	{
		WErrorCodes ec;
		err = ec.errorString(rc);
	}	
	purgeFDCache();
	return rc;
}

uint8_t WE_DDLCommandProc::writeTruncateLog(ByteStream& bs, std::string& err)
{
	int rc = 0;
	u_int32_t tableOid, numOid, tmp32;
	bs >> tableOid;
	bs >> numOid;
	std::vector<u_int32_t> oids;

	for (u_int32_t i=0; i < numOid; i++)
	{
		bs >> tmp32;
		oids.push_back(tmp32);
	}

	string prefix;
	config::Config *config = config::Config::makeConfig();
	prefix = config->getConfig("SystemConfig", "DBRMRoot");
	if (prefix.length() == 0) {
		err = "Need a valid DBRMRoot entry in Calpont configuation file";
		rc =1;
		return rc;
	}

	uint64_t pos =  prefix.find_last_of ("/") ;
	std::string DDLLogFileName;
	if (pos != string::npos)
	{
		DDLLogFileName = prefix.substr(0, pos+1); //Get the file path
	}
	else
	{
		err = "Cannot find the dbrm directory for the DDL log file";
		rc =1;
		return rc;
	}
	std::ostringstream oss;
	oss << tableOid;
	DDLLogFileName += "DDL_TRUNCATETABLE_Log_" + oss.str();
	boost::scoped_ptr<idbdatafile::IDBDataFile> DDLLogFile(IDBDataFile::open(
								IDBPolicy::getType(DDLLogFileName.c_str(), IDBPolicy::WRITEENG),
								DDLLogFileName.c_str(), "w", 0));

	if (!DDLLogFile)
	{
		err = "DDL truncate table log file cannot be created";
		rc =1;
		return rc;
	}

	std::ostringstream buf;
	for (unsigned i=0; i < oids.size(); i++)
		buf << oids[i] << std::endl;

	std::string tmp(buf.str());
	DDLLogFile->write(tmp.c_str(), tmp.size());

	// DDLLogFile is a scoped_ptr, will be closed after return.

	return rc;
}

uint8_t WE_DDLCommandProc::writeDropPartitionLog(ByteStream& bs, std::string& err)
{
	int rc = 0;
	u_int32_t tableOid, numParts, numOid, tmp32;
	bs >> tableOid;
	std::set<BRM::LogicalPartition> partitionNums;
	bs >> numParts;
	BRM::LogicalPartition lp;
	for (u_int32_t i=0; i < numParts; i++)
	{
		lp.unserialize(bs);
		partitionNums.insert(lp);
	}

	bs >> numOid;
	std::vector<u_int32_t> oids;

	for (u_int32_t i=0; i < numOid; i++)
	{
		bs >> tmp32;
		oids.push_back(tmp32);
	}

	string prefix;
	config::Config *config = config::Config::makeConfig();
	prefix = config->getConfig("SystemConfig", "DBRMRoot");
	if (prefix.length() == 0) {
		err = "Need a valid DBRMRoot entry in Calpont configuation file";
		rc =1;
		return rc;
	}

	uint64_t pos = prefix.find_last_of ("/") ;
	std::string	DDLLogFileName;
	if (pos != string::npos)
	{
		DDLLogFileName = prefix.substr(0, pos+1); //Get the file path
	}
	else
	{
		err = "Cannot find the dbrm directory for the DDL drop partitions log file";
		rc =1;
		return rc;
	}
	std::ostringstream oss;
	oss << tableOid;
	DDLLogFileName += "DDL_DROPPARTITION_Log_" + oss.str();
	boost::scoped_ptr<idbdatafile::IDBDataFile> DDLLogFile(IDBDataFile::open(
								IDBPolicy::getType(DDLLogFileName.c_str(), IDBPolicy::WRITEENG),
								DDLLogFileName.c_str(), "w", 0));
	if (!DDLLogFile)
	{
		err = "DDL drop partitions log file cannot be created";
		rc =1;
		return rc;
	}

	std::ostringstream buf;
	// @SN write partition numbers to the log file, separated by space
	set<BRM::LogicalPartition>::const_iterator it;
	for (it = partitionNums.begin(); it != partitionNums.end(); ++it)
		buf << (*it) << endl;


	// -1 indicates the end of partition list
	BRM::LogicalPartition end(-1,-1,-1);
	buf << end << endl;

	for (unsigned i=0; i < oids.size(); i++)
		buf << oids[i] << std::endl;

	std::string tmp(buf.str());
	DDLLogFile->write(tmp.c_str(), tmp.size());

	return rc;
}

uint8_t WE_DDLCommandProc::writeDropTableLog(ByteStream& bs, std::string& err)
{
	int rc = 0;
	u_int32_t tableOid, numOid, tmp32;
	bs >> tableOid;

	bs >> numOid;
	std::vector<u_int32_t> oids;

	for (u_int32_t i=0; i < numOid; i++)
	{
		bs >> tmp32;
		oids.push_back(tmp32);
	}

	string prefix;
	config::Config *config = config::Config::makeConfig();
	prefix = config->getConfig("SystemConfig", "DBRMRoot");
	if (prefix.length() == 0) {
		err = "Need a valid DBRMRoot entry in Calpont configuation file";
		rc =1;
		return rc;
	}

	uint64_t pos = prefix.find_last_of ("/") ;
	std::string DDLLogFileName;
	if (pos != string::npos)
	{
		DDLLogFileName = prefix.substr(0, pos+1); //Get the file path
	}
	else
	{
		err = "Cannot find the dbrm directory for the DDL drop partitions log file";
		rc =1;
		return rc;
	}
	std::ostringstream oss;
	oss << tableOid;
	DDLLogFileName += "DDL_DROPTABLE_Log_" + oss.str();
	boost::scoped_ptr<idbdatafile::IDBDataFile> DDLLogFile(IDBDataFile::open(
								IDBPolicy::getType(DDLLogFileName.c_str(), IDBPolicy::WRITEENG),
								DDLLogFileName.c_str(), "w", 0));
	if (!DDLLogFile)
	{
		err = "DDL drop table log file cannot be created";
		rc =1;
		return rc;
	}

	std::ostringstream buf;
	for (unsigned i=0; i < oids.size(); i++)
		buf << oids[i] << std::endl;

	std::string tmp(buf.str());
	DDLLogFile->write(tmp.c_str(), tmp.size());

	return rc;
}

uint8_t WE_DDLCommandProc::deleteDDLLog(ByteStream& bs, std::string& err)
{
	int rc = 0;
	u_int32_t tableOid, fileType;
	bs >> fileType;
	bs >> tableOid;
	string prefix;
	config::Config *config = config::Config::makeConfig();
	prefix = config->getConfig("SystemConfig", "DBRMRoot");
	if (prefix.length() == 0) {
		err = "Need a valid DBRMRoot entry in Calpont configuation file";
		rc =1;
		return rc;
	}

	uint64_t pos = prefix.find_last_of ("/") ;
	std::string DDLLogFileName;
	if (pos != string::npos)
	{
		DDLLogFileName = prefix.substr(0, pos+1); //Get the file path
	}
	else
	{
		err = "Cannot find the dbrm directory for the DDL drop partitions log file";
		rc =1;
		return rc;
	}
	std::ostringstream oss;
	oss << tableOid;
	switch (fileType)
	{
		case DROPTABLE_LOG:
		{
			DDLLogFileName += "DDL_DROPTABLE_Log_" + oss.str();
			break;
		}
		case DROPPART_LOG:
		{
			DDLLogFileName += "DDL_DROPPARTITION_Log_" + oss.str();
			break;
		}
		case TRUNCATE_LOG:
		{
			DDLLogFileName += "DDL_TRUNCATETABLE_Log_" + oss.str();
			break;
		}
		default:
			break;
	}

	IDBPolicy::remove(DDLLogFileName.c_str());

	return rc;
}

uint8_t WE_DDLCommandProc::fetchDDLLog(ByteStream& bs, std::string& err)
{
	int rc = 0;

	//Find the ddl log files under DBRMRoot directory
	string prefix,ddlLogDir;
	config::Config *config = config::Config::makeConfig();
	prefix = config->getConfig("SystemConfig", "DBRMRoot");
	if (prefix.length() == 0) {
		rc = 1;
		err = "Need a valid DBRMRoot entry in Calpont configuation file";
		return rc;
	}
	uint64_t pos = prefix.find_last_of ("/");
	if ( pos != string::npos )
	{
		ddlLogDir = prefix.substr(0, pos+1); //Get the file path
	}
	else
	{
		rc = 1;
		err = "Cannot find the dbrm directory for the DDL log file";
		return rc;
	}

	boost::filesystem::path filePath;
	filePath = fs::system_complete( fs::path( ddlLogDir ) );
	if ( !fs::exists( filePath ) )
	{
		rc = 1;
		err = "\nDDL log file path is Not found: ";
		return rc;
	}

	std::vector<string> fileNames;

	if ( fs::is_directory( filePath ) )
	{
		fs::directory_iterator end_iter;
		for ( fs::directory_iterator dir_itr( filePath ); dir_itr != end_iter; ++dir_itr )
		{
			try
			{
				if ( !fs::is_directory( *dir_itr ) )
				{
#if BOOST_VERSION >= 105200
					fileNames.push_back ( dir_itr->path().generic_string() );
#else
					fileNames.push_back ( dir_itr->string() );
#endif
				}
			}
			catch (std::exception& ex)
			{
				err = ex.what();
				rc = 1;
				return rc;
			}
		}
	}

	CalpontSystemCatalog::OID fileoid;
	string tableName;
	bs.restart();
	for ( unsigned i=0; i < fileNames.size(); i++ )
	{
		pos = fileNames[i].find ("DDL_DROPTABLE_Log_") ;
		if ( pos != string::npos )
		{
			//Read the file to get oids
			//cout << "Found file " << fileNames[i] << endl;
			boost::scoped_ptr<idbdatafile::IDBDataFile> ddlLogFile(IDBDataFile::open(
								IDBPolicy::getType(fileNames[i].c_str(), IDBPolicy::WRITEENG),
								fileNames[i].c_str(), "r", 0));
			if ( !ddlLogFile )
				continue;

			//find the table oid
			pos = fileNames[i].find_last_of( "_");
			string tableOidStr = fileNames[i].substr(pos+1, fileNames[i].length()-pos-1);
			char *ep = NULL;
			uint32_t tableOid = strtoll(tableOidStr.c_str(), &ep, 10);
			bs << tableOid;
			bs << (uint32_t)DROPTABLE_LOG;
			std::vector<CalpontSystemCatalog::OID> oidList;

			ssize_t fileSize = ddlLogFile->size();
			boost::scoped_array<char> buf(new char[fileSize]);
			if (ddlLogFile->read(buf.get(), fileSize) != fileSize)
				return (uint8_t) ERR_FILE_READ;

			std::istringstream strbuf(string(buf.get(), fileSize));
			while (strbuf >> fileoid)
				oidList.push_back(fileoid);

			bs << (uint32_t)oidList.size();
			for ( unsigned j=0; j < oidList.size(); j++ )
			{
				bs << (uint32_t)oidList[j];
			}
			bs << (uint32_t) 0;
		}
		else //Find drop partition log file
		{
			pos = fileNames[i].find ("DDL_DROPPARTITION_Log_") ;
			if ( pos != string::npos )
			{
				boost::scoped_ptr<idbdatafile::IDBDataFile> ddlLogFile(IDBDataFile::open(
								IDBPolicy::getType(fileNames[i].c_str(), IDBPolicy::WRITEENG),
								fileNames[i].c_str(), "r", 0));
				BRM::LogicalPartition partition;
				vector<BRM::LogicalPartition> partitionNums;
				//find the table oid
				pos = fileNames[i].find_last_of( "_");
				string tableOidStr = fileNames[i].substr(pos+1, fileNames[i].length()-pos-1);
				char *ep = NULL;
				uint32_t tableOid = strtoll(tableOidStr.c_str(), &ep, 10);
				bs << tableOid;
				bs << (uint32_t)DROPPART_LOG;

				ssize_t fileSize = ddlLogFile->size();
				boost::scoped_array<char> buf(new char[fileSize]);
				if (ddlLogFile->read(buf.get(), fileSize) != fileSize)
					return (uint8_t) ERR_FILE_READ;

				std::istringstream strbuf(string(buf.get(), fileSize));
				while (strbuf >> partition)
				{
					if (partition.dbroot == (uint16_t)-1)
						break;
					partitionNums.push_back(partition);
				}
				std::vector<CalpontSystemCatalog::OID> oidPartList;
				while (strbuf >> fileoid)
					oidPartList.push_back( fileoid );

				bs << (uint32_t)oidPartList.size();
				for ( unsigned j=0; j < oidPartList.size(); j++ )
				{
					bs << (uint32_t)oidPartList[j];
				}
				bs << (uint32_t) partitionNums.size();
				for ( unsigned j=0; j < partitionNums.size(); j++ )
				{
					partitionNums[j].serialize(bs);
				}
			}
			else //find truncate table log file
			{
				pos = fileNames[i].find ("DDL_TRUNCATETABLE_Log_") ;
				if ( pos != string::npos )
				{
					boost::scoped_ptr<idbdatafile::IDBDataFile> ddlLogFile(IDBDataFile::open(
								IDBPolicy::getType(fileNames[i].c_str(), IDBPolicy::WRITEENG),
								fileNames[i].c_str(), "r", 0));
					if ( !ddlLogFile )
					{
						continue;
					}
					//find the table oid
					pos = fileNames[i].find_last_of( "_");
					string tableOidStr = fileNames[i].substr(pos+1, fileNames[i].length()-pos-1);
					char *ep = NULL;
					uint32_t tableOid = strtoll(tableOidStr.c_str(), &ep, 10);
					bs << tableOid;
					bs << (uint32_t)TRUNCATE_LOG;
					std::vector<CalpontSystemCatalog::OID> oidList;

					ssize_t fileSize = ddlLogFile->size();
					boost::scoped_array<char> buf(new char[fileSize]);
					if (ddlLogFile->read(buf.get(), fileSize) != fileSize)
						return (uint8_t) ERR_FILE_READ;

					std::istringstream strbuf(string(buf.get(), fileSize));
					while (strbuf >> fileoid)
						oidList.push_back(fileoid);

					bs << (uint32_t)oidList.size();
					for ( unsigned j=0; j < oidList.size(); j++ )
					{
						bs << (uint32_t)oidList[j];
					}
					bs << (uint32_t) 0;
				}
			}
		}
	}
	return rc;
}

uint8_t WE_DDLCommandProc::updateSyscolumnSetDefault(messageqcpp::ByteStream& bs, std::string & err)
{
	//Will update five columns: columnname, defaultvalue, nullable, autoincrement, nextvalue.
	int rc = 0;
	uint32_t tmp32;
	string schema, tableName, colName, defaultvalue;
	int txnID;
	u_int32_t sessionID;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> tableName;
	bs >> colName;
	bs >> defaultvalue;

	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr;
	systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);

	CalpontSystemCatalog::TableName atableName;
	CalpontSystemCatalog::TableColName tableColName;
	tableColName.schema = schema;
	tableColName.table = tableName;
	tableColName.column = colName;
	CalpontSystemCatalog::ROPair ropair;

	try {
		ropair = systemCatalogPtr->columnRID(tableColName);
		if (ropair.objnum < 0)
		{
			ostringstream oss;
			oss << "No such column: " << tableColName;
			throw std::runtime_error(oss.str().c_str());
		}
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}
	catch (...)
	{
		err = "renameColumn:Unknown exception caught";
		rc = 1;
		return rc;
	}

	uint16_t dbRoot=0;
	uint16_t segment;
	uint32_t partition;

	std::vector<WriteEngine::RID> ridList;
	ridList.push_back(ropair.rid);
	WriteEngine::ColValueList colValuesList;
	WriteEngine::ColTupleList aColList1;
	WriteEngine::ColStructList colStructs;
	std::vector<void *> colOldValuesList;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	WriteEngine::DctColTupleList dctRowList;
	WriteEngine::DctnryTuple dctColList;
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;

	WriteEngine::ColTuple colTuple;

	//Build colStructs for SYSCOLUMN
	atableName.schema = CALPONT_SCHEMA;
	atableName.table = SYSCOLUMN_TABLE;
	DDLColumn column;
	findColumnData (sessionID, atableName, DEFAULTVAL_COL, column); //DEFAULTVAL_COL column

	WriteEngine::ColStruct colStruct;
	WriteEngine::DctnryStruct dctnryStruct;
	
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.setTransId(txnID);
	fWEWrapper.startTransaction(txnID);
	//Build DEFAULTVAL_COL structure
	WriteEngine::ColTupleList aColList;
	colStruct.dataOid = column.oid;
	colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
	colStruct.tokenFlag = false;
	if ( (column.colType.colDataType == CalpontSystemCatalog::CHAR
			&& column.colType.colWidth > 8)
		|| (column.colType.colDataType == CalpontSystemCatalog::VARCHAR
			&& column.colType.colWidth > 7)
		|| (column.colType.colDataType == CalpontSystemCatalog::VARBINARY
			&& column.colType.colWidth > 7)
		|| (column.colType.colDataType == CalpontSystemCatalog::DECIMAL
			&& column.colType.precision > 18)
		|| (column.colType.colDataType == CalpontSystemCatalog::UDECIMAL
			&& column.colType.precision > 18) )//token
	{
		colStruct.colWidth = 8;
		colStruct.tokenFlag = true;
	}
	else
	{
		colStruct.colWidth = column.colType.colWidth;
	}

	colStruct.colDataType = column.colType.colDataType;

	if (colStruct.tokenFlag)
	{
		WriteEngine::DctnryStruct dictStruct;
		dictStruct.dctnryOid = column.colType.ddn.dictOID;
		dictStruct.columnOid = column.colType.columnOID;
		if (defaultvalue.length() <= 0) //null token
		{
			WriteEngine::Token nullToken;
			colTuple.data = nullToken;
		}
		else
		{
			WriteEngine::DctnryTuple  dictTuple;
			memcpy(dictTuple.sigValue, defaultvalue.c_str(), defaultvalue.length());
			dictTuple.sigSize = defaultvalue.length();
			dictTuple.isNull = false;
			int error = NO_ERROR;
			if (NO_ERROR != (error = fWEWrapper.tokenize(txnID, dictStruct, dictTuple)))
			{
				WErrorCodes ec;
				throw std::runtime_error("WE: Tokenization failed " + ec.errorString(error));
			}
			WriteEngine::Token aToken = dictTuple.token;
			colTuple.data = aToken;
		}
	}

	colStruct.colDataType = column.colType.colDataType;
	if (idbdatafile::IDBPolicy::useHdfs())
	{
		colStruct.fCompressionType = 2;
		dctnryStruct.fCompressionType = 2;
	}
	
	if (colStruct.tokenFlag)
	{
		dctnryStruct.dctnryOid = column.colType.ddn.dictOID;
		dctnryStruct.columnOid = colStruct.dataOid;
	}
	else
	{
		dctnryStruct.dctnryOid = 0;
		dctnryStruct.columnOid = colStruct.dataOid;
	}

	colStructs.push_back(colStruct);
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	if (dctnryStruct.dctnryOid>0)
	{
		oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
		//oidsToFlush.push_back(dctnryStruct.dctnryOid);
	}
	dctnryStructList.push_back(dctnryStruct);
	aColList.push_back(colTuple);
	colValuesList.push_back(aColList);
	WriteEngine::DctnryTuple  dctnryTuple;
	if(defaultvalue.length() > 0)
	{
		memcpy(dctnryTuple.sigValue, defaultvalue.c_str(), defaultvalue.length());
		dctnryTuple.sigSize = defaultvalue.length();
		dctnryTuple.isNull = false;
	}
	else
	{
		dctnryTuple.isNull = true;
	}

	dctColList = dctnryTuple;
	dctRowList.clear();
	dctRowList.push_back(dctColList);
	dctnryValueList.push_back(dctRowList);

	std::vector<WriteEngine::ColStructList> colExtentsStruct;
	std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
	std::vector<WriteEngine::RIDList> ridLists;
	ridLists.push_back(ridList);

	//In this case, there's only 1 row, so only one one extent, but keep it generic...
	std::vector<extentInfo> extentsinfo;
	extentInfo aExtentinfo;

	convertRidToColumn(ropair.rid, dbRoot, partition, segment, 1021);

	aExtentinfo.dbRoot = dbRoot;
	aExtentinfo.partition = partition;
	aExtentinfo.segment = segment;

	extentsinfo.push_back(aExtentinfo);

	//build colExtentsStruct
	for (unsigned i=0; i < extentsinfo.size(); i++)
	{
		for (unsigned j=0; j < colStructs.size(); j++)
		{
			colStructs[j].fColPartition = extentsinfo[i].partition;
			colStructs[j].fColSegment = extentsinfo[i].segment;
			colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
			dctnryStructList[j].fColPartition = extentsinfo[i].partition;
			dctnryStructList[j].fColSegment = extentsinfo[i].segment;
			dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
		}
		colExtentsStruct.push_back(colStructs);
		dctnryExtentsStruct.push_back(dctnryStructList);
	}

	// call the write engine to update the row
	
	if (NO_ERROR != fWEWrapper.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
			ridLists, dctnryExtentsStruct, dctnryValueList, SYSCOLUMN_BASE))
	{
		err = "WE: Update failed on: " + atableName.table;
		rc = 1;
	}
	int rc1 = 0;
	if (idbdatafile::IDBPolicy::useHdfs())
	{		
		rc1 = fWEWrapper.flushDataFiles(rc, txnID, oids);
		if ((rc == 0) && ( rc1 == 0))
		{
				
			rc1 = fWEWrapper.confirmTransaction(txnID);
				
			if ( rc1 == NO_ERROR)
				rc1 = fWEWrapper.endTransaction(txnID, true);
			else
				fWEWrapper.endTransaction(txnID, false);
		}
		else
		{
			fWEWrapper.endTransaction(txnID, false);
		}
	}
	
	if (rc ==0 )
		rc = rc1;
	//flush syscat cahche
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//	cacheutils::flushOIDsFromCache(oidsToFlush);
	
	return rc;
}

uint8_t WE_DDLCommandProc::updateSyscolumnRenameColumn(messageqcpp::ByteStream& bs, std::string & err)
{
	//Will update five columns: columnname, defaultvalue, nullable, autoincrement, nextvalue.
	int rc = 0;
	uint64_t nextVal;
	uint32_t tmp32, nullable;
	string schema, tableName, colOldname, autoinc, colNewName, defaultvalue;
	int txnID;
	u_int32_t sessionID;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	bs >> schema;
	bs >> tableName;
	bs >> colOldname;
	bs >> colNewName;
	bs >> autoinc;
	bs >> nextVal;
	bs >> nullable;
	bs >> defaultvalue;

	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr;
	systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);

	CalpontSystemCatalog::TableName atableName;
	CalpontSystemCatalog::TableColName tableColName;
	tableColName.schema = schema;
	tableColName.table = tableName;
	tableColName.column = colOldname;
	CalpontSystemCatalog::ROPair ropair;

	try {
		ropair = systemCatalogPtr->columnRID(tableColName);
		if (ropair.objnum < 0)
		{
			ostringstream oss;
			oss << "No such column: " << tableColName;
			throw std::runtime_error(oss.str().c_str());
		}
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}
	catch (...)
	{
		err = "renameColumn:Unknown exception caught";
		rc = 1;
		return rc;
	}

	uint16_t dbRoot=0;
	uint16_t segment;
	uint32_t partition;

	std::vector<WriteEngine::RID> ridList;
	ridList.push_back(ropair.rid);
	WriteEngine::ColValueList colValuesList;
	WriteEngine::ColTupleList aColList1;
	WriteEngine::ColStructList colStructs;
	std::vector<void *> colOldValuesList;
	std::map<uint32_t,uint32_t> oids;
	//std::vector<BRM::OID_t>  oidsToFlush;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	WriteEngine::DctColTupleList dctRowList;
	WriteEngine::DctnryTuple dctColList;
	boost::any datavalue;
	datavalue = colNewName;

	WriteEngine::ColTuple colTuple;

	//Build colStructs for SYSCOLUMN
	atableName.schema = CALPONT_SCHEMA;
	atableName.table = SYSCOLUMN_TABLE;
	DDLColumn column1, column2, column3, column4, column5;
	findColumnData (sessionID, atableName, COLNAME_COL, column1); //COLNAME_COL column
	findColumnData (sessionID, atableName, AUTOINC_COL, column2); //AUTOINC_COL column
	findColumnData (sessionID, atableName, NEXTVALUE_COL, column3); //NEXTVALUE_COL column
	findColumnData (sessionID, atableName, NULLABLE_COL, column4); //NULLABLE_COL column
	findColumnData (sessionID, atableName, DEFAULTVAL_COL, column5); //DEFAULTVAL_COL column

	WriteEngine::ColStruct colStruct;
	WriteEngine::DctnryStruct dctnryStruct;
	if (idbdatafile::IDBPolicy::useHdfs())	
	{
		colStruct.fCompressionType = 2;
		dctnryStruct.fCompressionType = 2;
	}
	
	
	fWEWrapper.setTransId(txnID);
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.startTransaction(txnID);
	//Build COLNAME_COL structure
	colStruct.dataOid = column1.oid;
	colStruct.colWidth = column1.colType.colWidth > 8 ? 8 : column1.colType.colWidth;
	colStruct.tokenFlag = false;
	if ( (column1.colType.colDataType == CalpontSystemCatalog::CHAR
			&& column1.colType.colWidth > 8)
		|| (column1.colType.colDataType == CalpontSystemCatalog::VARCHAR
			&& column1.colType.colWidth > 7)
		|| (column1.colType.colDataType == CalpontSystemCatalog::VARBINARY
			&& column1.colType.colWidth > 7)
		|| (column1.colType.colDataType == CalpontSystemCatalog::DECIMAL
			&& column1.colType.precision > 18)
		|| (column1.colType.colDataType == CalpontSystemCatalog::UDECIMAL
			&& column1.colType.precision > 18) )//token
	{
		colStruct.colWidth = 8;
		colStruct.tokenFlag = true;
	}
	else
	{
		colStruct.colWidth = column1.colType.colWidth;
	}

	colStruct.colDataType = column1.colType.colDataType;
	if (colStruct.tokenFlag)
	{
		WriteEngine::DctnryStruct dictStruct;
		if (idbdatafile::IDBPolicy::useHdfs())	
		{
			dictStruct.fCompressionType = 2;
		}
		dictStruct.dctnryOid = column1.colType.ddn.dictOID;
		dictStruct.columnOid = column1.colType.columnOID;
		WriteEngine::DctnryTuple  dictTuple;
		memcpy(dictTuple.sigValue, colNewName.c_str(), colNewName.length());
		dictTuple.sigSize = colNewName.length();
		dictTuple.isNull = false;
		int error = NO_ERROR;

		if (NO_ERROR != (error = fWEWrapper.tokenize(txnID, dictStruct, dictTuple)))
		{
			WErrorCodes ec;
			err = ec.errorString(error);
			rc = error;
			return rc;
		}

		WriteEngine::Token aToken = dictTuple.token;
		colTuple.data = aToken;
	}
	else
		colTuple.data = datavalue;

	colStruct.colDataType = column1.colType.colDataType;

	if (idbdatafile::IDBPolicy::useHdfs())
	{
		colStruct.fCompressionType = 2;
		dctnryStruct.fCompressionType = 2;
	}
	if (colStruct.tokenFlag)
	{
		dctnryStruct.dctnryOid = column1.colType.ddn.dictOID;
		dctnryStruct.columnOid = colStruct.dataOid;
	}
	else
	{
		dctnryStruct.dctnryOid = 0;
		dctnryStruct.columnOid = colStruct.dataOid;
	}

	colStructs.push_back(colStruct);
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	if (dctnryStruct.dctnryOid > 0)
	{
		oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
		//oidsToFlush.push_back(dctnryStruct.dctnryOid);
	}
		
	dctnryStructList.push_back(dctnryStruct);
	aColList1.push_back(colTuple);
	colValuesList.push_back(aColList1);
	WriteEngine::DctnryTuple dctnryTuple;
	boost::to_lower(colNewName);
	memcpy(dctnryTuple.sigValue, colNewName.c_str(), colNewName.length());
	dctnryTuple.sigSize = colNewName.length();
	dctnryTuple.isNull = false;
	dctColList = dctnryTuple;
	dctRowList.clear();
	dctRowList.push_back(dctColList);
	dctnryValueList.push_back(dctRowList);

	//Build AUTOINC_COL structure
	WriteEngine::ColTupleList aColList2;
	colStruct.dataOid = column2.oid;
	colStruct.colWidth = column2.colType.colWidth > 8 ? 8 : column2.colType.colWidth;
	colStruct.tokenFlag = false;
	colStruct.colDataType = column2.colType.colDataType;

	colTuple.data = autoinc;

	colStruct.colDataType = column2.colType.colDataType;

	dctnryStruct.dctnryOid = 0;
	dctnryStruct.columnOid = colStruct.dataOid;

	colStructs.push_back(colStruct);
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	if (dctnryStruct.dctnryOid > 0)
	{
		oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
		//oidsToFlush.push_back(dctnryStruct.dctnryOid);
	}
	dctnryStructList.push_back(dctnryStruct);
	aColList2.push_back(colTuple);
	colValuesList.push_back(aColList2);
	dctnryTuple.isNull = true;
	dctColList = dctnryTuple;
	dctRowList.clear();
	dctRowList.push_back(dctColList);
	dctnryValueList.push_back(dctRowList);

	//Build NEXTVALUE_COL structure
	WriteEngine::ColTupleList aColList3;
	colStruct.dataOid = column3.oid;
	colStruct.colWidth = column3.colType.colWidth > 8 ? 8 : column3.colType.colWidth;
	colStruct.tokenFlag = false;
	colStruct.colDataType = column3.colType.colDataType;

	colTuple.data = nextVal;

	colStruct.colDataType = column3.colType.colDataType;

	dctnryStruct.dctnryOid = 0;
	dctnryStruct.columnOid = colStruct.dataOid;

	colStructs.push_back(colStruct);
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	if (dctnryStruct.dctnryOid > 0)
	{
		oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
		//oidsToFlush.push_back(dctnryStruct.dctnryOid);
	}
		
	dctnryStructList.push_back(dctnryStruct);
	aColList3.push_back(colTuple);
	colValuesList.push_back(aColList3);

	dctnryTuple.isNull = true;
	dctColList = dctnryTuple;
	dctRowList.clear();
	dctRowList.push_back(dctColList);
	dctnryValueList.push_back(dctRowList);

	//Build NULLABLE_COL structure
	WriteEngine::ColTupleList aColList4;
	colStruct.dataOid = column4.oid;
	colStruct.colWidth = column4.colType.colWidth > 8 ? 8 : column4.colType.colWidth;
	colStruct.tokenFlag = false;
	colStruct.colDataType = column4.colType.colDataType;

	colTuple.data = nullable;

	colStruct.colDataType = column4.colType.colDataType;

	dctnryStruct.dctnryOid = 0;
	dctnryStruct.columnOid = colStruct.dataOid;

	colStructs.push_back(colStruct);
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	if (dctnryStruct.dctnryOid > 0)
	{
		oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
		//oidsToFlush.push_back(dctnryStruct.dctnryOid);
	}
		
	dctnryStructList.push_back(dctnryStruct);
	aColList4.push_back(colTuple);
	colValuesList.push_back(aColList4);
	dctnryTuple.isNull = true;
	dctColList = dctnryTuple;
	dctRowList.clear();
	dctRowList.push_back(dctColList);
	dctnryValueList.push_back(dctRowList);

	//Build DEFAULTVAL_COL structure
	WriteEngine::ColTupleList aColList5;
	colStruct.dataOid = column5.oid;
	colStruct.colWidth = column5.colType.colWidth > 8 ? 8 : column5.colType.colWidth;
	colStruct.tokenFlag = false;
	if ( (column5.colType.colDataType == CalpontSystemCatalog::CHAR
			&& column5.colType.colWidth > 8)
		|| (column5.colType.colDataType == CalpontSystemCatalog::VARCHAR
			&& column5.colType.colWidth > 7)
		|| (column5.colType.colDataType == CalpontSystemCatalog::VARBINARY
			&& column5.colType.colWidth > 7)
		|| (column5.colType.colDataType == CalpontSystemCatalog::DECIMAL
			&& column5.colType.precision > 18)
		 || (column5.colType.colDataType == CalpontSystemCatalog::UDECIMAL
			 && column5.colType.precision > 18) )//token
	{
		colStruct.colWidth = 8;
		colStruct.tokenFlag = true;
	}
	else
	{
		colStruct.colWidth = column5.colType.colWidth;
	}

	colStruct.colDataType = column5.colType.colDataType;

	if (colStruct.tokenFlag)
	{
		WriteEngine::DctnryStruct dictStruct;
		if (idbdatafile::IDBPolicy::useHdfs())	
		{
			colStruct.fCompressionType = 2;
			dictStruct.fCompressionType = 2;
		}
		dictStruct.dctnryOid = column5.colType.ddn.dictOID;
		dictStruct.columnOid = column5.colType.columnOID;
		if (defaultvalue.length() <= 0) //null token
		{
			WriteEngine::Token nullToken;
			colTuple.data = nullToken;
		}
		else
		{
			WriteEngine::DctnryTuple  dictTuple;
			memcpy(dictTuple.sigValue, defaultvalue.c_str(), defaultvalue.length());
			dictTuple.sigSize = defaultvalue.length();
			dictTuple.isNull = false;
			int error = NO_ERROR;
			if (NO_ERROR != (error = fWEWrapper.tokenize(txnID, dictStruct, dictTuple)))
			{
				WErrorCodes ec;
				throw std::runtime_error("WE: Tokenization failed " + ec.errorString(error));
			}
			WriteEngine::Token aToken = dictTuple.token;
			colTuple.data = aToken;
		}
	}

	fWEWrapper.flushDataFiles(rc, txnID, oids);

	colStruct.colDataType = column5.colType.colDataType;
	if (idbdatafile::IDBPolicy::useHdfs())
	{
		colStruct.fCompressionType = 2;
		dctnryStruct.fCompressionType = 2;
	}

	if (colStruct.tokenFlag)
	{
		dctnryStruct.dctnryOid = column5.colType.ddn.dictOID;
		dctnryStruct.columnOid = colStruct.dataOid;
	}
	else
	{
		dctnryStruct.dctnryOid = 0;
		dctnryStruct.columnOid = colStruct.dataOid;
	}

	colStructs.push_back(colStruct);
	dctnryStructList.push_back(dctnryStruct);
	oids[colStruct.dataOid] = colStruct.dataOid;
	//oidsToFlush.push_back(colStruct.dataOid);
	if (dctnryStruct.dctnryOid > 0)
	{
		oids[dctnryStruct.dctnryOid] = dctnryStruct.dctnryOid;
		//oidsToFlush.push_back(dctnryStruct.dctnryOid);
	}
		
	aColList5.push_back(colTuple);
	colValuesList.push_back(aColList5);

	if(defaultvalue.length() > 0)
	{
		memcpy(dctnryTuple.sigValue, defaultvalue.c_str(), defaultvalue.length());
		dctnryTuple.sigSize = defaultvalue.length();
		dctnryTuple.isNull = false;
	}
	else
	{
		dctnryTuple.isNull = true;
	}

	dctColList = dctnryTuple;
	dctRowList.clear();
	dctRowList.push_back(dctColList);
	dctnryValueList.push_back(dctRowList);
	std::vector<WriteEngine::ColStructList> colExtentsStruct;
	std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
	std::vector<WriteEngine::RIDList> ridLists;
	ridLists.push_back(ridList);

	//In this case, there's only 1 row, so only one one extent, but keep it generic...
	std::vector<extentInfo> extentsinfo;
	extentInfo aExtentinfo;

	convertRidToColumn(ropair.rid, dbRoot, partition, segment, 1021);

	aExtentinfo.dbRoot = dbRoot;
	aExtentinfo.partition = partition;
	aExtentinfo.segment = segment;

	extentsinfo.push_back(aExtentinfo);

	//build colExtentsStruct
	for (unsigned i=0; i < extentsinfo.size(); i++)
	{
		for (unsigned j=0; j < colStructs.size(); j++)
		{
			colStructs[j].fColPartition = extentsinfo[i].partition;
			colStructs[j].fColSegment = extentsinfo[i].segment;
			colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
			dctnryStructList[j].fColPartition = extentsinfo[i].partition;
			dctnryStructList[j].fColSegment = extentsinfo[i].segment;
			dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
		}
		colExtentsStruct.push_back(colStructs);
		dctnryExtentsStruct.push_back(dctnryStructList);
	}
	
	// call the write engine to update the row
	if (NO_ERROR != fWEWrapper.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
			ridLists, dctnryExtentsStruct, dctnryValueList, SYSCOLUMN_BASE))
	{
		err = "WE: Update failed on: " + atableName.table;
		rc = 1;
	}
	int rc1 = 0;
	if (idbdatafile::IDBPolicy::useHdfs())
	{		
		rc1 = fWEWrapper.flushDataFiles(rc, txnID, oids);
		if ((rc == 0) && ( rc1 == 0))
		{
				
			rc1 = fWEWrapper.confirmTransaction(txnID);
				
			if ( rc1 == NO_ERROR)
				rc1 = fWEWrapper.endTransaction(txnID, true);
			else
				fWEWrapper.endTransaction(txnID, false);
		}
		else
		{
			fWEWrapper.endTransaction(txnID, false);
		}
	}
	
	if (rc ==0 )
		rc = rc1;
	//flush syscat cahche
	systemCatalogPtr->flushCache();
	purgeFDCache();
	//if (idbdatafile::IDBPolicy::useHdfs())
	//		cacheutils::flushOIDsFromCache(oidsToFlush);
	return rc;
}

uint8_t WE_DDLCommandProc::dropPartitions(ByteStream& bs, std::string& err)
{
	int rc = 0;
	uint32_t size, i;
	uint32_t tmp32;
	std::vector<OID>  dataOids;
	std::vector<BRM::PartitionInfo> partitions;

	bs >> size;
	for (i = 0; i < size; ++i) {
		bs >> tmp32;
		dataOids.push_back(tmp32);
	}
	bs >> size;
	BRM::PartitionInfo pi;
	for (i = 0; i < size; ++i)
	{
		pi.unserialize(bs);
		partitions.push_back(pi);
	}

	try {
		rc = fWEWrapper.deletePartitions(dataOids, partitions);
	}
	catch (...)
	{
		err = "WE: Error removing files ";
		rc = 1;
	}
	return rc;
}

void WE_DDLCommandProc::purgeFDCache()
{
	if (idbdatafile::IDBPolicy::useHdfs())
    {
		TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(SYSCOLUMN_BASE);
		ColsExtsInfoMap colsExtsInfoMap = aTbaleMetaData->getColsExtsInfoMap();
		ColsExtsInfoMap::iterator it = colsExtsInfoMap.begin();
		ColExtsInfo::iterator aIt;
		std::vector<BRM::FileInfo> files;
		BRM::FileInfo aFile;
		vector<BRM::LBID_t> lbidList;
    	BRM::LBID_t startLbid;

		while (it != colsExtsInfoMap.end())
		{
			aIt = (it->second).begin();
			aFile.oid = it->first;
			while (aIt != (it->second).end())
			{
				aFile.partitionNum = aIt->partNum;
				aFile.dbRoot =aIt->dbRoot;
				aFile.segmentNum = aIt->segNum;
				aFile.compType = aIt->compType;
				files.push_back(aFile);
				fDbrm.lookupLocalStartLbid(aFile.oid, aFile.partitionNum, aFile.segmentNum, aIt->hwm, startLbid);
            	lbidList.push_back(startLbid);
			//cout <<"Added to files oid:dbroot:part:seg:compType = " << aFile.oid<<":"<<aFile.dbRoot<<":"<<aFile.partitionNum<<":"<<aFile.segmentNum
			//<<":"<<aFile.compType <<endl;
				aIt++;
			}
			it++;
		}
		cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID());
		fDbrm.invalidateUncommittedExtentLBIDs(0, &lbidList);
	}
	TableMetaData::removeTableMetaData(SYSCOLUMN_BASE);
}

}
// vim:ts=4 sw=4:

