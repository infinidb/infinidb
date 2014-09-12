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

// $Id: we_dmlcommandproc.cpp 3082 2011-09-26 22:00:38Z chao $

#include <unistd.h>
using namespace std;
#include "bytestream.h"
using namespace messageqcpp;

#include "we_messages.h"
#include "we_message_handlers.h"
#include "dmlpkg.h"
#include "we_dmlcommandproc.h"
using namespace dmlpackage;
#include "dmlpackageprocessor.h"
using namespace dmlpackageprocessor;
#include <boost/date_time/gregorian/gregorian.hpp>
using namespace boost::gregorian;
#include "dataconvert.h"
using namespace dataconvert;
#include "calpontsystemcatalog.h"
#include "sessionmanager.h"
using namespace execplan;
#include "messagelog.h"
#include "stopwatch.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "brmtypes.h"
using namespace BRM;
#include "we_tablemetadata.h"
#include "we_dbrootextenttracker.h"
#include "we_bulkrollbackmgr.h"
#include "we_define.h"

namespace WriteEngine
{
//StopWatch timer;
WE_DMLCommandProc::WE_DMLCommandProc()
{
	fIsFirstBatchPm = true;
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

WE_DMLCommandProc::WE_DMLCommandProc(const WE_DMLCommandProc& rhs)
{
	fIsFirstBatchPm = rhs.fIsFirstBatchPm;
	fRBMetaWriter.reset(new RBMetaWriter("BatchInsert", NULL));
}

WE_DMLCommandProc::~WE_DMLCommandProc()
{
	dbRootExtTrackerVec.clear();
}

uint8_t WE_DMLCommandProc::processSingleInsert(messageqcpp::ByteStream& bs, std::string & err)
{
	uint8_t rc = 0;
	InsertDMLPackage insertPkg;
	ByteStream::quadbyte tmp32;
	bs >> tmp32;
	BRM::TxnID txnid;
	txnid.valid = true;
	txnid.id  = tmp32;	
	bs >> tmp32;
	uint32_t dbroot = tmp32;
	
	//cout << "processSingleInsert received bytestream length " << bs.length() << endl;
	
	messageqcpp::ByteStream::byte packageType;
	bs >> packageType;
	insertPkg.read( bs);
	uint32_t sessionId = insertPkg.get_SessionID();
	//cout << " processSingleInsert for session " << sessionId << endl;
	DMLTable *tablePtr = insertPkg.get_Table();
	RowList rows = tablePtr->get_RowList();
	
	WriteEngine::ColStructList colStructs;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	WriteEngine::ColValueList colValuesList;
	WriteEngine::DictStrList dicStringList ;
	CalpontSystemCatalog::TableName tableName;
	CalpontSystemCatalog::TableColName tableColName;
	tableName.table = tableColName.table = tablePtr->get_TableName();
	tableName.schema = tableColName.schema = tablePtr->get_SchemaName();

	CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionId);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	
	try {
	
	  if (rows.size())
	  {
		Row *rowPtr = rows.at(0);
		ColumnList columns = rowPtr->get_ColumnList();
		ColumnList::const_iterator column_iterator = columns.begin();
		while (column_iterator != columns.end())
		{
			DMLColumn *columnPtr = *column_iterator;
			tableColName.column = columnPtr->get_Name();
			CalpontSystemCatalog::ROPair roPair = systemCatalogPtr->columnRID(tableColName);

			CalpontSystemCatalog::OID oid = systemCatalogPtr->lookupOID(tableColName);

			CalpontSystemCatalog::ColType colType;
			colType = systemCatalogPtr->colType(oid);
			
			WriteEngine::ColStruct colStruct;
			colStruct.fColDbRoot = dbroot;
			WriteEngine::DctnryStruct dctnryStruct;
			dctnryStruct.fColDbRoot = dbroot;
			colStruct.dataOid = roPair.objnum;
			colStruct.tokenFlag = false;
			colStruct.fCompressionType = colType.compressionType;
			// Token
			if ( isDictCol(colType) )
			{
				colStruct.colWidth = 8;
				colStruct.tokenFlag = true;
			}
			else
			{
				colStruct.colWidth = colType.colWidth;
			}
			colStruct.colDataType = (WriteEngine::ColDataType) colType.colDataType;

			if (colStruct.tokenFlag)
			{
				dctnryStruct.dctnryOid = colType.ddn.dictOID;
				dctnryStruct.columnOid = colStruct.dataOid;
				dctnryStruct.fCompressionType = colType.compressionType;
				dctnryStruct.colWidth = colType.colWidth;
			}
			else
			{
				dctnryStruct.dctnryOid = 0;
				dctnryStruct.columnOid = colStruct.dataOid;
				dctnryStruct.fCompressionType = colType.compressionType;
				dctnryStruct.colWidth = colType.colWidth;
			}

			colStructs.push_back(colStruct);
			dctnryStructList.push_back(dctnryStruct);

			++column_iterator;
		}

		unsigned int numcols = rowPtr->get_NumberOfColumns();
		std::string tmpStr("");
		for (unsigned int i = 0; i < numcols; i++)
		{
			
			WriteEngine::ColTupleList colTuples;
			WriteEngine::DctColTupleList dctColTuples;
			RowList::const_iterator row_iterator = rows.begin();
			while (row_iterator != rows.end())
			{
				Row *rowPtr = *row_iterator;
				const DMLColumn *columnPtr = rowPtr->get_ColumnAt(i);

				tableColName.column = columnPtr->get_Name();
				CalpontSystemCatalog::OID oid = systemCatalogPtr->lookupOID(tableColName);

				CalpontSystemCatalog::ColType colType;
				colType = systemCatalogPtr->colType(oid);

				boost::any datavalue;
				bool isNULL = false;
				bool pushWarning = false;
				std::vector<std::string> origVals;
				origVals = columnPtr->get_DataVector();
				WriteEngine::dictStr dicStrings;
				// token
				if ( isDictCol(colType) )
				{
					for ( uint32_t i=0; i < origVals.size(); i++ )
					{
						tmpStr = origVals[i];
						if ( tmpStr.length() == 0 )
							isNULL = true;
						else
							isNULL = false;
							
						if (colType.constraintType == CalpontSystemCatalog::NOTNULL_CONSTRAINT)
						{
							if (isNULL && colType.defaultValue.empty()) //error out
							{
								Message::Args args;
								args.add(tableColName.column);
								err = IDBErrorInfo::instance()->errorMsg(ERR_NOT_NULL_CONSTRAINTS, args);
								rc = 1;
								return rc;
							}
							else if (isNULL && !(colType.defaultValue.empty()))
							{
								tmpStr = colType.defaultValue;
							}							
						}
						if ( tmpStr.length() > (unsigned int)colType.colWidth )
						{
							tmpStr = tmpStr.substr(0, colType.colWidth);
							if ( !pushWarning )
								pushWarning = true;
						}
						WriteEngine::ColTuple colTuple;
						colTuple.data = datavalue;

						colTuples.push_back(colTuple);
						//@Bug 2515. Only pass string values to write engine
						dicStrings.push_back( tmpStr );
					}
						colValuesList.push_back(colTuples);
						//@Bug 2515. Only pass string values to write engine
						dicStringList.push_back( dicStrings );
				}
				else
				{
					string x;
					std::string indata;
					for ( uint32_t i=0; i < origVals.size(); i++ )
					{
						indata = origVals[i];
						if ( indata.length() == 0 )
							isNULL = true;
						else
							isNULL = false;
						
						//check if autoincrement column and value is 0 or null
						uint64_t nextVal = 1;
						if (colType.autoincrement)
						{
							try {
								nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
								fDbrm.startAISequence(oid, nextVal, colType.colWidth);
							}
							catch (std::exception& ex)
							{
								err = ex.what();
								rc = 1;
								return rc;
							}
						}
						if (colType.autoincrement && ( isNULL || (indata.compare("0")==0)))
						{
							try {
								bool reserved = fDbrm.getAIRange(oid, 1, &nextVal);
								if (!reserved)
								{
									err = IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT);
									rc = 1;
									return rc;
								}
							}
							catch (std::exception& ex)
							{
								err = ex.what();
								rc = 1;
								return rc;
							}
							ostringstream oss;
							oss << nextVal;
							indata = oss.str();
							isNULL = false;
						}
						
						if (colType.constraintType == CalpontSystemCatalog::NOTNULL_CONSTRAINT)
						{
							if (((colType.colDataType == execplan::CalpontSystemCatalog::DATE) && (indata =="0000-00-00")) || 
								((colType.colDataType == execplan::CalpontSystemCatalog::DATETIME) && (indata =="0000-00-00 00:00:00")))
							{	
								isNULL = true;
							}
							if (isNULL && colType.defaultValue.empty()) //error out
							{
								Message::Args args;
								args.add(tableColName.column);
								err = IDBErrorInfo::instance()->errorMsg(ERR_NOT_NULL_CONSTRAINTS, args);
								rc = 1;
								return rc;
							}
							else if (isNULL && !(colType.defaultValue.empty()))
							{
								indata = colType.defaultValue;
								isNULL = false;
							} 
						}
						
						try
						{                    
							datavalue = DataConvert::convertColumnData(colType, indata, pushWarning, isNULL);
						}
						catch(exception&)
						{
							rc = 1;
							Message::Args args;
							args.add(string("'") + indata + string("'"));
							err = IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERIC_DATA, args);
						}
						//@Bug 1806
						if (rc != NO_ERROR && rc != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING)
						{
							return rc;
						}
						if ( pushWarning && ( rc != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING ) )
							rc = dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING;
							
						WriteEngine::ColTuple colTuple;
						colTuple.data = datavalue;
						
						colTuples.push_back(colTuple);
						//@Bug 2515. Only pass string values to write engine
						dicStrings.push_back( tmpStr );
					}
					colValuesList.push_back(colTuples);
					dicStringList.push_back( dicStrings );
				}
				++row_iterator;
			}
		}
	  }
	 }
	 catch(exception & ex)
	 {
		rc = 1;
		err = ex.what();
		return rc;
	 }

	// call the write engine to write the rows
	int error = NO_ERROR;
	//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
	//cout << "inserting a row with transaction id " << txnid.id << endl;
	fWEWrapper.setIsInsert(true);
	
	if (colValuesList[0].size() > 0)
	{
		if (NO_ERROR != 
		(error = fWEWrapper.insertColumnRec_Single(txnid.id, colStructs, colValuesList, dctnryStructList, dicStringList)))
		{
			if (error == ERR_BRM_DEAD_LOCK)
			{
				rc = dmlpackageprocessor::DMLPackageProcessor::DEAD_LOCK_ERROR;
				WErrorCodes ec;
				err = ec.errorString(error);
			}
			else if ( error == ERR_BRM_VB_OVERFLOW )
			{
				rc = dmlpackageprocessor::DMLPackageProcessor::VB_OVERFLOW_ERROR;
				err = IDBErrorInfo::instance()->errorMsg(ERR_VERSIONBUFFER_OVERFLOW);
			}
			else
			{
				rc = dmlpackageprocessor::DMLPackageProcessor::INSERT_ERROR;
				WErrorCodes ec;
				err = ec.errorString(error);
			}
		}
	}
	std::map<uint32_t,uint32_t> oids;
	for ( unsigned i=0; i < colStructs.size(); i++)
	{
		oids[colStructs[i].dataOid] = colStructs[i].dataOid;
	}

	for (unsigned i=0; i<dctnryStructList.size(); i++)
	{
		oids[dctnryStructList[i].dctnryOid] = dctnryStructList[i].dctnryOid;
	}
	//flush files
	fWEWrapper.setTransId(txnid.id);

	// @bug5333, up to here, rc may have an error code already, don't overwrite it.
	uint8_t tmp8 = fWEWrapper.flushChunks(0, oids);  // why not pass rc to flushChunks?
	if (rc == NO_ERROR && tmp8 != NO_ERROR)
		rc = 1;  // return hardcoded 1 as the above

	fWEWrapper.setIsInsert(false);

	return rc;
}
	
uint8_t WE_DMLCommandProc::commitVersion(ByteStream& bs, std::string & err)
{
	int rc = 0;
	uint32_t tmp32;
	int txnID;

	bs >> tmp32;
	txnID = tmp32;
	//cout << "processing commit txnid = " << txnID << endl;
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

uint8_t WE_DMLCommandProc::rollbackBlocks(ByteStream& bs, std::string & err)
{
	int rc = 0;
	uint32_t sessionID, tmp32;;
	int txnID;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	//cout << "processing rollbackBlocks txnid = " << txnID << endl;
	try {
		rc = fWEWrapper.rollbackBlocks(txnID, sessionID);
	}
	catch (std::exception& ex)
	{
		rc = 1;
		err = ex.what();
	}
	return rc;
}

uint8_t WE_DMLCommandProc::rollbackVersion(ByteStream& bs, std::string & err)
{
	int rc = 0;
	uint32_t sessionID, tmp32;
	int txnID;
	bs >> sessionID;
	bs >> tmp32;
	txnID = tmp32;
	//cout << "processing rollbackVersion txnid = " << txnID << endl;
	rc = fWEWrapper.rollbackVersion(txnID, sessionID);
	if (rc != 0)
	{
		WErrorCodes ec;
		ostringstream oss;
		oss << "WE: Error rolling back transaction "  << txnID << " for session " <<  sessionID << "; " << ec.errorString(rc) << endl;
		err = oss.str();
	}	
	return rc;
}

uint8_t WE_DMLCommandProc::processBatchInsert(messageqcpp::ByteStream& bs, std::string & err, ByteStream::quadbyte & PMId)
{
	int rc = 0;
	//cout << "processBatchInsert received bytestream length " << bs.length() << endl;
	
	InsertDMLPackage insertPkg;
	ByteStream::quadbyte tmp32;
	bs >> tmp32;
	//cout << "processBatchInsert got transaction id " << tmp32 << endl;	
	bs >> PMId;
	//cout << "processBatchInsert gor PMId " << PMId << endl;
	insertPkg.read( bs);
	uint32_t sessionId = insertPkg.get_SessionID();
	//cout << " processBatchInsert for session " << sessionId << endl;
	DMLTable *tablePtr = insertPkg.get_Table();
	bool isAutocommitOn = insertPkg.get_isAutocommitOn();
	//cout << "This session isAutocommitOn is " << isAutocommitOn << endl;
	BRM::TxnID txnid;
	txnid.id  = tmp32;	
	txnid.valid = true;
	RowList rows = tablePtr->get_RowList();
	bool isInsertSelect = insertPkg.get_isInsertSelect();
		
	WriteEngine::ColStructList colStructs;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	WriteEngine::ColValueList colValuesList;
    WriteEngine::DictStrList dicStringList ;
	CalpontSystemCatalog::TableName tableName;
	CalpontSystemCatalog::TableColName tableColName;
	tableName.table = tableColName.table = tablePtr->get_TableName();
	tableName.schema = tableColName.schema = tablePtr->get_SchemaName();
	CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionId);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	CalpontSystemCatalog::ROPair roPair;
	CalpontSystemCatalog::RIDList ridList;
	CalpontSystemCatalog::DictOIDList dictOids;
	try {	
		ridList = systemCatalogPtr->columnRIDs(tableName, true);
		roPair = systemCatalogPtr->tableRID( tableName);
	}
	catch (std::exception& ex)
	{
			err = ex.what();
			rc = 1;
			return rc;
	}
	
	std::vector<OID>    dctnryStoreOids(ridList.size()) ;
	std::vector<BRM::EmDbRootHWMInfo_v> dbRootHWMInfoColVec(ridList.size());
	
	uint32_t  tblOid = roPair.objnum;
	std::vector<Column> columns;
	CalpontSystemCatalog::ColType colType;	
	std::vector<DBRootExtentInfo> colDBRootExtentInfo;
	bool bFirstExtentOnThisPM = false;
	Convertor convertor;
	DctnryStructList dctnryList;
	if ( fIsFirstBatchPm )
	{
		dbRootExtTrackerVec.clear();
		fRBMetaWriter.reset(new RBMetaWriter("BatchInsert", NULL));
		fWEWrapper.setIsInsert(true);
		try {
		  for (unsigned i=0; i < ridList.size(); i++)
		  {
			rc = BRMWrapper::getInstance()->getDbRootHWMInfo(ridList[i].objnum, dbRootHWMInfoColVec[i]);	
			//need handle error
			// Find DBRoot/segment file where we want to start adding rows
			colType = systemCatalogPtr->colType(ridList[i].objnum);
			BRM::EmDbRootHWMInfo_v& dbRootHWMInfo = dbRootHWMInfoColVec[i];		
			DBRootExtentTracker* pDBRootExtentTracker = new DBRootExtentTracker(ridList[i].objnum, convertor.getCorrectRowWidth( static_cast<WriteEngine::ColDataType>(colType.colDataType), colType.colWidth), 0, dbRootHWMInfo );
			dbRootExtTrackerVec.push_back( pDBRootExtentTracker );
			DBRootExtentInfo dbRootExtent;
			std::string trkErrMsg;
			rc = pDBRootExtentTracker->selectFirstSegFile(dbRootExtent,bFirstExtentOnThisPM, trkErrMsg);
			colDBRootExtentInfo.push_back(dbRootExtent);
			Column aColumn;
			aColumn.colWidth = convertor.getCorrectRowWidth( static_cast<WriteEngine::ColDataType>(colType.colDataType), colType.colWidth);
			aColumn.colDataType = static_cast<WriteEngine::ColDataType>(colType.colDataType);
			aColumn.compressionType = colType.compressionType;
			aColumn.dataFile.oid = ridList[i].objnum;
			aColumn.dataFile.fPartition = dbRootExtent.fPartition;
			aColumn.dataFile.fSegment = dbRootExtent.fSegment;
			aColumn.dataFile.fDbRoot = dbRootExtent.fDbRoot;
			aColumn.dataFile.hwm = dbRootExtent.fLocalHwm;
			columns.push_back(aColumn);
			if ((colType.compressionType > 0) && (colType.ddn.dictOID > 0))
			{
				DctnryStruct aDctnry;
				aDctnry.dctnryOid = colType.ddn.dictOID;
				aDctnry.fColPartition = dbRootExtent.fPartition;
				aDctnry.fColSegment = dbRootExtent.fSegment;
				aDctnry.fColDbRoot = dbRootExtent.fDbRoot;
				dctnryList.push_back(aDctnry);
			}
			if (colType.ddn.dictOID > 0)
			{
				dctnryStoreOids[i] = colType.ddn.dictOID;
			}
			else
			{
				dctnryStoreOids[i] = 0;
			}
		  }
		}
		catch (std::exception& ex)
		{
			err = ex.what();
			rc = 1;
			return rc;
		}
	}
	std::vector<BRM::LBIDRange>   rangeList;	
	if ( fIsFirstBatchPm && isAutocommitOn)
	{
		//save meta data, version last block for each dbroot at the start of batch insert
		try {
			fRBMetaWriter->init(tblOid, tableName.table);
			fRBMetaWriter->saveBulkRollbackMetaData(columns, dctnryStoreOids, dbRootHWMInfoColVec);
			//cout << "Saved meta files" << endl;
			if (!bFirstExtentOnThisPM)
			{
				//cout << "Backing up hwm chunks" << endl;
				for (unsigned i=0; i < dctnryList.size(); i++) //back up chunks for compressed dictionary
				{
					fRBMetaWriter->backupDctnryHWMChunk(dctnryList[i].dctnryOid, dctnryList[i].fColDbRoot, dctnryList[i].fColPartition, dctnryList[i].fColSegment);
				}
			}
		}
		catch (WeException& ex) // catch exception to close file, then rethrow
		{
			rc = 1;
			err =  ex.what();
		}
		//Do versioning. Currently, we only version columns, not strings. If there is a design change, this will need to be re-visited
		if ( rc != 0)
			return rc;
			
		if (!bFirstExtentOnThisPM) //If no extent on this pm, there is no need to version it
		{
			//rc = fWEWrapper.processBatchVersions(txnid.id, columns, rangeList);
			if (rc != 0)
			{
				//Nothing need rollback.
				try {
					fRBMetaWriter->deleteFile();
				}
				catch (WeException ex)
				{
					rc = 1;
					err = ex.what();
				}
				//set error message
				WErrorCodes ec;
				err = ec.errorString(rc);
				return rc;
			}
		}
	}
	
	if (rows.size())
	{
		Row *rowPtr = rows.at(0);
		ColumnList columns = rowPtr->get_ColumnList();
		ColumnList::const_iterator column_iterator = columns.begin();
		try {
		  while (column_iterator != columns.end())
		  {
				DMLColumn *columnPtr = *column_iterator;
				tableColName.column = columnPtr->get_Name();
				CalpontSystemCatalog::ROPair roPair = systemCatalogPtr->columnRID(tableColName);

				CalpontSystemCatalog::OID oid = systemCatalogPtr->lookupOID(tableColName);

				CalpontSystemCatalog::ColType colType;
				colType = systemCatalogPtr->colType(oid);
				
				WriteEngine::ColStruct colStruct;
				WriteEngine::DctnryStruct dctnryStruct;
				colStruct.dataOid = roPair.objnum;
				colStruct.tokenFlag = false;
				colStruct.fCompressionType = colType.compressionType;
				// Token
				if ( isDictCol(colType) )
				{
					colStruct.colWidth = 8;
					colStruct.tokenFlag = true;
				}
				else
				{
					colStruct.colWidth = colType.colWidth;
				}
				colStruct.colDataType = (WriteEngine::ColDataType) colType.colDataType;

				if (colStruct.tokenFlag)
				{
					dctnryStruct.dctnryOid = colType.ddn.dictOID;
					dctnryStruct.columnOid = colStruct.dataOid;
					dctnryStruct.fCompressionType = colType.compressionType;
					dctnryStruct.colWidth = colType.colWidth;
				}
				else
				{
					dctnryStruct.dctnryOid = 0;
					dctnryStruct.columnOid = colStruct.dataOid;
					dctnryStruct.fCompressionType = colType.compressionType;
					dctnryStruct.colWidth = colType.colWidth;
				}

				colStructs.push_back(colStruct);
				dctnryStructList.push_back(dctnryStruct);

				++column_iterator;
		  }
		}
		catch (std::exception& ex)
		{
			err = ex.what();
			rc = 1;
			return rc;
		}

		unsigned int numcols = rowPtr->get_NumberOfColumns();
		std::string tmpStr("");
		try {
		  for (unsigned int i = 0; i < numcols; i++)
		  {
				
				WriteEngine::ColTupleList colTuples;
				WriteEngine::DctColTupleList dctColTuples;
				RowList::const_iterator row_iterator = rows.begin();
				while (row_iterator != rows.end())
				{
					Row *rowPtr = *row_iterator;
					const DMLColumn *columnPtr = rowPtr->get_ColumnAt(i);

					tableColName.column = columnPtr->get_Name();
					CalpontSystemCatalog::OID oid = systemCatalogPtr->lookupOID(tableColName);

					CalpontSystemCatalog::ColType colType;
					colType = systemCatalogPtr->colType(oid);

					boost::any datavalue;
					bool isNULL = false;
					bool pushWarning = false;
					std::vector<std::string> origVals;
					origVals = columnPtr->get_DataVector();
					WriteEngine::dictStr dicStrings;
					// token
					if ( isDictCol(colType) )
					{
						for ( uint32_t i=0; i < origVals.size(); i++ )
						{
							tmpStr = origVals[i];
							if ( tmpStr.length() == 0 )
								isNULL = true;
							else
								isNULL = false;
							
							if (colType.constraintType == CalpontSystemCatalog::NOTNULL_CONSTRAINT)
							{
								if (isNULL && colType.defaultValue.empty()) //error out
								{
									Message::Args args;
									args.add(tableColName.column);
									err = IDBErrorInfo::instance()->errorMsg(ERR_NOT_NULL_CONSTRAINTS, args);
									rc = 1;
									return rc;
								}
								else if (isNULL && !(colType.defaultValue.empty()))
								{
									tmpStr = colType.defaultValue;
								}							
							}

							if ( tmpStr.length() > (unsigned int)colType.colWidth )
							{
								tmpStr = tmpStr.substr(0, colType.colWidth);
								if ( !pushWarning )
									pushWarning = true;
							}
							WriteEngine::ColTuple colTuple;
							colTuple.data = datavalue;

							colTuples.push_back(colTuple);
							//@Bug 2515. Only pass string values to write engine
							dicStrings.push_back( tmpStr );
						}
							colValuesList.push_back(colTuples);
							//@Bug 2515. Only pass string values to write engine
							dicStringList.push_back( dicStrings );
					}
					else
					{
						string x;
						std::string indata;
						//scan once to check how many autoincrement value needed
						uint32_t nextValNeeded = 0;
						uint64_t nextVal = 1;
						if (colType.autoincrement)
						{
							try {
								nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
								fDbrm.startAISequence(oid, nextVal, colType.colWidth);
							}
							catch (std::exception& ex)
							{
								err = ex.what();
								rc = 1;
								return rc;
							}
							for ( uint32_t i=0; i < origVals.size(); i++ )
							{
								indata = origVals[i];
								if ( indata.length() == 0 )
									isNULL = true;
								else
									isNULL = false;
								if ( isNULL || (indata.compare("0")==0))
									nextValNeeded++;
							}
						}
						
						if (nextValNeeded>0) //reserve next value
						{
							try {
								bool reserved = fDbrm.getAIRange(oid, nextValNeeded, &nextVal);
								if (!reserved)
								{
									err = IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT);
									rc = 1;
									return rc;
								}
							}
							catch (std::exception& ex)
							{
								err = ex.what();
								rc = 1;
								return rc;
							}
						}
						
						for ( uint32_t i=0; i < origVals.size(); i++ )
						{
							indata = origVals[i];
							if ( indata.length() == 0 )
								isNULL = true;
							else
								isNULL = false;
							
							//check if autoincrement column and value is 0 or null
							if (colType.autoincrement && ( isNULL || (indata.compare("0")==0)))
							{		
								ostringstream oss;
								oss << nextVal++;
								indata = oss.str();
								isNULL = false;
							}
							
							if (colType.constraintType == CalpontSystemCatalog::NOTNULL_CONSTRAINT)
							{
								if (((colType.colDataType == execplan::CalpontSystemCatalog::DATE) && (indata =="0000-00-00")) || 
									((colType.colDataType == execplan::CalpontSystemCatalog::DATETIME) && (indata =="0000-00-00 00:00:00")))
								{	
									isNULL = true;
								}
								
								if (isNULL && colType.defaultValue.empty()) //error out
								{
									Message::Args args;
									args.add(tableColName.column);
									err = IDBErrorInfo::instance()->errorMsg(ERR_NOT_NULL_CONSTRAINTS, args);
									rc = 1;
									return rc;
								}
								else if (isNULL && !(colType.defaultValue.empty()))
								{
									indata = colType.defaultValue;
									isNULL = false;
								}							
							}
							
							try
							{                    
								datavalue = DataConvert::convertColumnData(colType, indata, pushWarning, isNULL);
							}
							catch(exception&)
							{
								rc = 1;
								Message::Args args;
								args.add(string("'") + indata + string("'"));
								err = IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERIC_DATA, args);
							}
							//@Bug 1806
							if (rc != NO_ERROR && rc != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING)
							{
								return rc;
							}
							if ( pushWarning && ( rc != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING ) )
								rc = dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING;
								
							WriteEngine::ColTuple colTuple;
							colTuple.data = datavalue;
							
							colTuples.push_back(colTuple);
							//@Bug 2515. Only pass string values to write engine
							dicStrings.push_back( tmpStr );
						}
						colValuesList.push_back(colTuples);
						dicStringList.push_back( dicStrings );
					}
					++row_iterator;
				}
		  }
		 }
		 catch (std::exception& ex)
		{
			err = ex.what();
			rc = 1;
			return rc;
		}
	}

	// call the write engine to write the rows
	int error = NO_ERROR;
		//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
	//cout << "Batch inserting a row with transaction id " << txnid.id << endl;
	if (colValuesList.size() > 0)
	{
		if (colValuesList[0].size() > 0)
		{
			if (NO_ERROR != 
			(error = fWEWrapper.insertColumnRecs(txnid.id, colStructs, colValuesList, dctnryStructList, dicStringList, 
						dbRootExtTrackerVec, fRBMetaWriter.get(), bFirstExtentOnThisPM, isInsertSelect, 0, roPair.objnum, fIsFirstBatchPm)))
			{
				if (error == ERR_BRM_DEAD_LOCK)
				{
					rc = dmlpackageprocessor::DMLPackageProcessor::DEAD_LOCK_ERROR;
					WErrorCodes ec;
					err = ec.errorString(error);
				}
				else if ( error == ERR_BRM_VB_OVERFLOW )
				{
					rc = dmlpackageprocessor::DMLPackageProcessor::VB_OVERFLOW_ERROR;
					err = IDBErrorInfo::instance()->errorMsg(ERR_VERSIONBUFFER_OVERFLOW);
				}
				else
				{
					rc = dmlpackageprocessor::DMLPackageProcessor::INSERT_ERROR;
					WErrorCodes ec;
					err = ec.errorString(error);
				}
			}
		}
	}
	if (fIsFirstBatchPm && isAutocommitOn)
	{
		//fWEWrapper.writeVBEnd(txnid.id, rangeList);
		fIsFirstBatchPm = false;
	}
	else if (fIsFirstBatchPm)
	{
		fIsFirstBatchPm = false;
	}
	//cout << "Batch insert return code " << rc << endl;
	return rc;
}

uint8_t WE_DMLCommandProc::commitBatchAutoOn(messageqcpp::ByteStream& bs, std::string & err)
{
	uint8_t rc = 0;
	//need to commit the versioned blocks, set hwm, update casual partition, send back to DMLProc to set them
	//cout << " in commiting autocommit on batch insert " << endl;
	uint32_t tmp32, tableOid, sessionId;
	int txnID;

	bs >> tmp32;
	txnID = tmp32;
	bs >> tmp32;
	tableOid = tmp32;
	bs >> tmp32;
	sessionId = tmp32;
	
	BRM::DBRM dbrm;
		
	std::vector<BRM::BulkSetHWMArg> setHWMArgs;
	TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);
	ColsExtsInfoMap colsExtsInfoMap = aTbaleMetaData->getColsExtsInfoMap();
	ColsExtsInfoMap::iterator it = colsExtsInfoMap.begin();
	ColExtsInfo::iterator aIt;
	BulkSetHWMArg aArg;
	while (it != colsExtsInfoMap.end())
	{
		aIt = (it->second).begin();
		aArg.oid = it->first;
		//cout << "OID:" << aArg.oid; 
		while (aIt != (it->second).end())
		{
			aArg.partNum = aIt->partNum;
			aArg.segNum = aIt->segNum;
			aArg.hwm = aIt->hwm;
			//cout << " part:seg:hwm = " << aArg.partNum <<":"<<aArg.segNum<<":"<<aArg.hwm<<endl;
			setHWMArgs.push_back(aArg);
			aIt++;
		}
		it++;
	}
	
	bs.restart();
	//cout << " serialized setHWMArgs size = " << setHWMArgs.size() << endl;
	serializeInlineVector (bs, setHWMArgs);
	
	//flush files
	//cout << "flush files when autocommit on" << endl;
	fWEWrapper.setIsInsert(true);
	
	std::map<uint32_t,uint32_t> oids;
	CalpontSystemCatalog* systemCatalogPtr =
       CalpontSystemCatalog::makeCalpontSystemCatalog(sessionId);
		
	CalpontSystemCatalog::TableName aTableName =  systemCatalogPtr->tableName(tableOid);
	CalpontSystemCatalog::RIDList ridList;
	try {
		ridList = systemCatalogPtr->columnRIDs(aTableName, true);
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}
	
	for (unsigned i=0; i < ridList.size(); i++)
	{
		oids[ridList[i].objnum] = ridList[i].objnum;
	}
	CalpontSystemCatalog::DictOIDList dictOids;
	try {
		dictOids = systemCatalogPtr->dictOIDs(aTableName);
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
	}
	
	for (unsigned i=0; i < dictOids.size(); i++)
	{
		oids[dictOids[i].dictOID] = dictOids[i].dictOID;
	}
		
	fWEWrapper.setTransId(txnID);

	// @bug5333, up to here, rc == 0, but flushchunk may fail.
	if (fWEWrapper.flushChunks(0, oids) != NO_ERROR)
		rc = 1;  // return hardcoded 1 as the above

	fWEWrapper.setIsInsert(false);

	fIsFirstBatchPm = true;
	TableMetaData::removeTableMetaData(tableOid);
	return rc;
}

uint8_t WE_DMLCommandProc::processBatchInsertHwm(messageqcpp::ByteStream& bs, std::string & err)
{
	uint8_t tmp8, rc = 0;
	//set hwm for autocommit off
	uint32_t tmp32, tableOid;
	int txnID;
	bool isAutoCommitOn;

	bs >> tmp32;
	txnID = tmp32;
	bs >> tmp8;
	isAutoCommitOn = (tmp8 != 0);
	bs >> tmp32;
	tableOid = tmp32;
	bs >> tmp8;
	CalpontSystemCatalog* systemCatalogPtr =
    CalpontSystemCatalog::makeCalpontSystemCatalog(txnID);
	try {
	if (isAutoCommitOn)
	{
		bs.restart();
		if (fWEWrapper.getIsInsert())
		{
			std::map<uint32_t,uint32_t> oids;		
			CalpontSystemCatalog::TableName aTableName;
			CalpontSystemCatalog::RIDList ridList;
			try {
				aTableName =  systemCatalogPtr->tableName(tableOid);
				ridList = systemCatalogPtr->columnRIDs(aTableName, true);
			}
			catch ( ... )
			{
				err = "Systemcatalog error for tableoid " + tableOid;
				rc = 1;
				return rc;
			
			}
			for (unsigned i=0; i < ridList.size(); i++)
			{
				oids[ridList[i].objnum] = ridList[i].objnum;
			}
			CalpontSystemCatalog::DictOIDList dictOids = systemCatalogPtr->dictOIDs(aTableName);
			for (unsigned i=0; i < dictOids.size(); i++)
			{
				oids[dictOids[i].dictOID] = dictOids[i].dictOID;
			}
		
			fWEWrapper.setTransId(txnID);
			// @bug5333, up to here, rc == 0, but flushchunk may fail.
			rc = fWEWrapper.flushChunks(0, oids);
			fWEWrapper.setIsInsert(false);

			fIsFirstBatchPm = true;

			if (rc != NO_ERROR)
				return 1;  // return hardcoded 1 as the above
		}
		if ( tmp8 != 0)
			TableMetaData::removeTableMetaData(tableOid);
		return rc; // will set hwm with version commit.
	  }
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
			
	}
	
	BRM::DBRM dbrm;
	//cout << " In processBatchInsertHwm setting hwm" << endl;	
	std::vector<BRM::BulkSetHWMArg> setHWMArgs;
	TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);
	ColsExtsInfoMap colsExtsInfoMap = aTbaleMetaData->getColsExtsInfoMap();
	ColsExtsInfoMap::iterator it = colsExtsInfoMap.begin();
	ColExtsInfo::iterator aIt;
	BulkSetHWMArg aArg;
	while (it != colsExtsInfoMap.end())
	{
		aIt = (it->second).begin();
		aArg.oid = it->first;
		//cout << "for oid " << aArg.oid << endl;
		while (aIt != (it->second).end())
		{
			aArg.partNum = aIt->partNum;
			aArg.segNum = aIt->segNum;
			aArg.hwm = aIt->hwm;
			//cout << " part:seg:hwm = " << aArg.partNum <<":"<<aArg.segNum<<":"<<aArg.hwm<<endl;
			setHWMArgs.push_back(aArg);
			aIt++;
		}
		it++;
	}
	bs.restart();
	serializeInlineVector (bs, setHWMArgs);
		
	TableMetaData::removeTableMetaData(tableOid);
	
	fIsFirstBatchPm = true;
	//cout << "flush files when autocommit off" << endl;
	fWEWrapper.setIsInsert(true);

	std::map<uint32_t,uint32_t> oids;
		
	CalpontSystemCatalog::TableName aTableName =  systemCatalogPtr->tableName(tableOid);
	CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(aTableName, true);
	for (unsigned i=0; i < ridList.size(); i++)
	{
		oids[ridList[i].objnum] = ridList[i].objnum;
	}
	CalpontSystemCatalog::DictOIDList dictOids = systemCatalogPtr->dictOIDs(aTableName);
	for (unsigned i=0; i < dictOids.size(); i++)
	{
		oids[dictOids[i].dictOID] = dictOids[i].dictOID;
	}
		
	fWEWrapper.setTransId(txnID);

	// @bug5333, up to here, rc == 0, but flushchunk may fail.
	if (fWEWrapper.flushChunks(0, oids) != NO_ERROR)
		rc = 1;  // return hardcoded 1 as the above
	fWEWrapper.setIsInsert(false);

	fIsFirstBatchPm = true;
	return rc;
}
uint8_t WE_DMLCommandProc::commitBatchAutoOff(messageqcpp::ByteStream& bs, std::string & err)
{
	uint8_t rc = 0;
	//commit all versioned blocks, set hwm, update casual partition
	return rc;
}

uint8_t WE_DMLCommandProc::rollbackBatchAutoOn(messageqcpp::ByteStream& bs, std::string & err)
{
	uint8_t rc = 0;
	uint32_t tmp32, tableOid, sessionID;
	uint64_t lockID;
	bs >> sessionID;
	bs >> lockID;
	bs >> tmp32;
	tableOid = tmp32;
	//Bulkrollback
	CalpontSystemCatalog* systemCatalogPtr =
        CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	CalpontSystemCatalog::TableName aTableName;
	try {
		aTableName = systemCatalogPtr->tableName(tableOid);
	}
	catch ( ... )
	{
		err = "No such table for oid " + tableOid;
		rc = 1;
		return rc;
	}

	string table = aTableName.schema + "." + aTableName.table;
	string applName ("BatchInsert");
	rc = fWEWrapper.bulkRollback(tableOid,lockID,table,applName, false, err);
	fIsFirstBatchPm = true;
	TableMetaData::removeTableMetaData(tableOid);
	return rc;
}

uint8_t WE_DMLCommandProc::rollbackBatchAutoOff(messageqcpp::ByteStream& bs, std::string & err)
{
	uint8_t rc = 0;
	//Rollbacked all versioned blocks
	return rc;
}
uint8_t WE_DMLCommandProc::processUpdate(messageqcpp::ByteStream& bs, 
	                                       std::string & err, 
	                                       ByteStream::quadbyte & PMId,
	                                       uint64_t& blocksChanged)
{
	uint8_t rc = 0;
	//cout << " In processUpdate" << endl;
	uint32_t tmp32, sessionID;
	TxnID txnId;
	bs >> PMId;
	bs >> tmp32;
	txnId = tmp32;
	if (!rowGroups[txnId]) //meta data
	{
		rowGroups[txnId] = new rowgroup::RowGroup();
		rowGroups[txnId]->deserialize(bs);
		uint8_t pkgType;
		bs >> pkgType;
		cpackages[txnId].read(bs);
		//cout << "Processed meta data in update" << endl;
		return rc;
	}

	fWEWrapper.setIsInsert(false);

	bool pushWarning = false;
	rowGroups[txnId]->setData(const_cast<uint8_t*>(bs.buf()));
	//get rows and values
	rowgroup::Row row;
	rowGroups[txnId]->initRow(&row);
	string value("");
	uint32_t rowsThisRowgroup = rowGroups[txnId]->getRowCount();
	uint columnsSelected = rowGroups[txnId]->getColumnCount();
	std::vector<execplan::CalpontSystemCatalog::ColDataType> fetchColTypes =  rowGroups[txnId]->getColTypes();
	std::vector<uint> fetchColScales = rowGroups[txnId]->getScale();
	std::vector<uint> fetchColColwidths;
	for (uint i=0; i < columnsSelected; i++)
	{
		fetchColColwidths.push_back(rowGroups[txnId]->getColumnWidth(i));
	}
	
	WriteEngine::ColTupleList aColList;
	WriteEngine::ColTuple colTuple;
	WriteEngine::ColStructList colStructList;
	WriteEngine::ColStruct colStruct;
	WriteEngine::ColValueList colValueList;
	WriteEngine::RIDList  rowIDLists;

	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryStruct dctnryStruct;
    WriteEngine::DctnryValueList dctnryValueList;
	
	CalpontSystemCatalog::TableName tableName;
	CalpontSystemCatalog::TableColName tableColName;
	DMLTable* tablePtr =  cpackages[txnId].get_Table();
	RowList rows = tablePtr->get_RowList();
	dmlpackage::ColumnList columnsUpdated = rows[0]->get_ColumnList();
	tableColName.table = tableName.table = tablePtr->get_TableName();
	tableColName.schema = tableName.schema = tablePtr->get_SchemaName();
	tableColName.column  = columnsUpdated[0]->get_Name();
	
	sessionID = cpackages[txnId].get_SessionID();
	
	CalpontSystemCatalog* systemCatalogPtr =
        CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	CalpontSystemCatalog::OID oid = 0;
	try {
		oid = systemCatalogPtr->lookupOID(tableColName);
	}
	catch  (std::exception& ex)
	{
		rc = 1;
		ostringstream oss;
		oss << "lookupOID got exception " << ex.what() << " with column " << tableColName.schema << "." + tableColName.table << "." << tableColName.column;
		err = oss.str();
	}
	catch ( ... )
	{
		rc = 1;
		ostringstream oss;
		oss << "lookupOID got unknown exception with column " << tableColName.schema << "." << tableColName.table << "." << tableColName.column;
		err =  oss.str();
	}
	
	if (rc != 0)
		return rc;
		
	rowGroups[txnId]->getRow(0, &row);
	CalpontSystemCatalog::RID rid = row.getRid();
	uint16_t dbRoot, segment;
	uint32_t partition;
	//get file info		
	dbRoot = rowGroups[txnId]->getDBRoot();
	convertRidToColumn (rid, partition, segment);
	colStruct.fColPartition = partition;
	colStruct.fColSegment = segment;
	colStruct.fColDbRoot = dbRoot;
	dctnryStruct.fColPartition = partition;
	dctnryStruct.fColSegment = segment;
	dctnryStruct.fColDbRoot = dbRoot;
	
	//Build to be updated column structure and values
	int error = 0;
	unsigned fetchColPos = 0;
	bool ridsFetched = false;
	bool isNull = false;
	boost::any datavalue;
	int64_t intColVal = 0;
	//timer.start("fetch values");
	std::vector<string> colNames;
	
	// for query stats
	boost::scoped_array<CalpontSystemCatalog::ColType> colTypes(new CalpontSystemCatalog::ColType[columnsUpdated.size()]);
	boost::scoped_array<int> preBlkNums(new int[columnsUpdated.size()]);
	boost::scoped_array<OID> oids(new OID[columnsUpdated.size()]);

	for (unsigned int j = 0; j < columnsUpdated.size(); j++)
	{
		//timer.start("lookupsyscat");
		tableColName.column  = columnsUpdated[j]->get_Name();
		try {
			oids[j] = systemCatalogPtr->lookupOID(tableColName);		
			colTypes[j] = systemCatalogPtr->colType(oids[j]);
		}
		catch  (std::exception& ex)
		{
			rc = 1;
			ostringstream oss;
			oss << "colType got exception " << ex.what() << " with column oid " << oid;
			err = oss.str();
		}
		catch ( ... )
		{
			rc = 1;
			ostringstream oss;
			oss << "colType got unknown exception with column oid " << oid;
			err = oss.str();
		}
		
		if (rc !=0)
			return rc;

		preBlkNums[j] = -1;
	}
		
	for (unsigned int j = 0; j < columnsUpdated.size(); j++)
	{
	/*		WriteEngine::ColTupleList colTupleList;
			//timer.start("lookupsyscat");
			tableColName.column  = columnsUpdated[j]->get_Name();
			try {
				oid = systemCatalogPtr->lookupOID(tableColName);
			}
			catch  (std::exception& ex)
			{
				rc = 1;
				ostringstream oss;
				oss << "lookupOID got exception " << ex.what() << " with column " << tableColName.schema << "." << tableColName.table << "." << tableColName.column;
				err = oss.str();
			}
			catch ( ... )
			{
				rc = 1;
				ostringstream oss;
				oss <<  "lookupOID got unknown exception with column " << tableColName.schema << "." << tableColName.table << "." << tableColName.column;
				err = oss.str();
			}
			
			if (rc != 0)
				return rc;
				
			CalpontSystemCatalog::ColType colType;
			try {
				colType = systemCatalogPtr->colType(oid);
			}
			catch  (std::exception& ex)
			{
				rc = 1;
				ostringstream oss;
				oss << "colType got exception " << ex.what() << " with column oid " << oid;
				err = oss.str();
			}
			catch ( ... )
			{
				rc = 1;
				ostringstream oss;
				oss << "colType got unknown exception with column oid " << oid;
				err = oss.str();
			}
			
			if (rc !=0)
				return rc;
			*/
			WriteEngine::ColTupleList colTupleList;
			CalpontSystemCatalog::ColType colType = colTypes[j];
			oid = oids[j];
			colStruct.dataOid = oid;
			colStruct.colDataType = (WriteEngine::ColDataType)colType.colDataType;
			colStruct.tokenFlag = false;
			colStruct.fCompressionType = colType.compressionType;
			
			if( !ridsFetched)
			{
				// querystats
				uint64_t relativeRID = 0;

				for (unsigned i = 0; i < rowsThisRowgroup; i++)
				{
					rowGroups[txnId]->getRow(i, &row);
					rid = row.getRid();
					relativeRID = rid - rowGroups[txnId]->getBaseRid();
					convertRidToColumn (rid, partition, segment);
					rowIDLists.push_back(rid);
					uint colWidth = (colTypes[j].colWidth > 8 ? 8 : colTypes[j].colWidth);

					// populate stats.blocksChanged
					for (unsigned int k = 0; k < columnsUpdated.size(); k++)
					{
						if ((int)(relativeRID/(BYTE_PER_BLOCK/colWidth)) > preBlkNums[j])
						{
							blocksChanged++;
							preBlkNums[j] = relativeRID/(BYTE_PER_BLOCK/colWidth);
						}
					}
				}
				ridsFetched = true;
			}
			bool pushWarn = false;
			bool nameNeeded = false;
			if (isDictCol(colType))
			{
				colStruct.colWidth = 8;
				colStruct.tokenFlag = true;
				dctnryStruct.dctnryOid = colType.ddn.dictOID;
				dctnryStruct.columnOid = colStruct.dataOid;
				dctnryStruct.fCompressionType = colType.compressionType;
				dctnryStruct.colWidth = colType.colWidth;
				if (NO_ERROR != (error = fWEWrapper.openDctnry (txnId, dctnryStruct)))
				{
					WErrorCodes ec;
					err = ec.errorString(error);
					rc = error;
					return rc;
				}
				if (columnsUpdated[j]->get_isFromCol())
				{
					for (unsigned i = 0; i < rowsThisRowgroup; i++)
					{
						rowGroups[txnId]->getRow(i, &row);
						if (row.isNullValue(fetchColPos))
						{
							if ((colType.defaultValue.length() <= 0) && (colType.constraintType == CalpontSystemCatalog::NOTNULL_CONSTRAINT))
							{
								rc = 1;
								Message::Args args;
								args.add(tableColName.column);
								err = IDBErrorInfo::instance()->errorMsg(ERR_NOT_NULL_CONSTRAINTS, args);
								return rc;
							}
							else if (colType.defaultValue.length() > 0)
							{
								value = colType.defaultValue;
								if (value.length() > (unsigned int)colType.colWidth)
								{
									value = value.substr(0, colType.colWidth);
									pushWarn = true;
									if (!pushWarning)
									{
										pushWarning = true;
									}
									if (pushWarn)
										nameNeeded = true;
								}
								WriteEngine::DctnryTuple dctTuple;
								memcpy(dctTuple.sigValue, value.c_str(), value.length());
								dctTuple.sigSize = value.length();
								dctTuple.isNull = false;
								error = fWEWrapper.tokenize(txnId, dctTuple, colType.compressionType);
								if (error != NO_ERROR)
								{
									fWEWrapper.closeDctnry(txnId, colType.compressionType);
									return false;
								}
								colTuple.data = dctTuple.token;
								colTupleList.push_back (colTuple);
							}
							else
							{
								WriteEngine::Token nullToken;
								colTuple.data = nullToken;
								colTupleList.push_back (colTuple);
							}
							continue;
						}
					
						switch (fetchColTypes[fetchColPos])
						{
							case CalpontSystemCatalog::DATE:
							{
								intColVal = row.getUintField<4>(fetchColPos);
								value = DataConvert::dateToString(intColVal);
								break;
							}
							case CalpontSystemCatalog::DATETIME:
							{
								intColVal = row.getUintField<8>(fetchColPos);
								value = DataConvert::datetimeToString(intColVal);
								break;
							}
							case CalpontSystemCatalog::CHAR:
							case CalpontSystemCatalog::VARCHAR:
							{
								value = row.getStringField(fetchColPos);
								unsigned i = strlen(value.c_str());
								value = value.substr(0, i);
								break;
							}
							case CalpontSystemCatalog::VARBINARY:
							{
								value = row.getVarBinaryStringField(fetchColPos);
								break;
							}
							case CalpontSystemCatalog::BIGINT:
							{
								intColVal = row.getIntField<8>(fetchColPos);
							
								if (fetchColScales[fetchColPos] <= 0)
								{
									ostringstream os;
									os << intColVal;
									value = os.str();
								}
								break;
							}
							case CalpontSystemCatalog::INT:
							{
								intColVal = row.getIntField<4>(fetchColPos);
								if (fetchColScales[fetchColPos] <= 0)
								{
									ostringstream os;
									os << intColVal;
									value = os.str();
								}
								break;
							}
							case CalpontSystemCatalog::SMALLINT:
							{
								intColVal = row.getIntField<2>(fetchColPos);
								if (fetchColScales[fetchColPos] <= 0)
								{
									ostringstream os;
									os << intColVal;
									value = os.str();
								}
								break;
							}
							case CalpontSystemCatalog::TINYINT:
							{
								intColVal = row.getIntField<1>(fetchColPos);
								if (fetchColScales[fetchColPos] <= 0)
								{
									ostringstream os;
									os << intColVal;
									value = os.str();
								}
								break;
							}
							case CalpontSystemCatalog::DECIMAL:
							{
								if (fetchColColwidths[fetchColPos] == execplan::CalpontSystemCatalog::ONE_BYTE)
								{
									intColVal = row.getIntField<1>(fetchColPos);
									ostringstream os;
									os << intColVal;
									value = os.str();
								}
								else if (fetchColColwidths[fetchColPos] == execplan::CalpontSystemCatalog::TWO_BYTE)
								{
									intColVal = row.getIntField<2>(fetchColPos);
									ostringstream os;
									os << intColVal;
									value = os.str();
								}
								else if (fetchColColwidths[fetchColPos] == execplan::CalpontSystemCatalog::FOUR_BYTE)
								{
									intColVal = row.getIntField<4>(fetchColPos);
									ostringstream os;
									os << intColVal;
									value = os.str();
								}
								else if (fetchColColwidths[fetchColPos] == execplan::CalpontSystemCatalog::EIGHT_BYTE)
								{
									intColVal = row.getIntField<8>(fetchColPos);
									ostringstream os;
									os << intColVal;
									value = os.str();
								}
								else
								{
									value = row.getStringField(fetchColPos);
									unsigned i = strlen(value.c_str());
									value = value.substr(0, i);
								}
								break;
							}
							//In this case, we're trying to load a double output column with float data. This is the
							// case when you do sum(floatcol), e.g.
							case CalpontSystemCatalog::FLOAT:
							{
								float dl = row.getFloatField(fetchColPos);
								if (dl == std::numeric_limits<float>::infinity())
									continue;
								ostringstream os;	
								//@Bug 3350 fix the precision.
								os << setprecision(7) << dl;
								value = os.str();
								break;
							}
							case CalpontSystemCatalog::DOUBLE:
							{								
								double dl = row.getDoubleField(fetchColPos);
								if (dl == std::numeric_limits<double>::infinity())
									continue;
								ostringstream os;	
								//@Bug 3350 fix the precision.
								os <<setprecision(16)<< dl;
								value = os.str();
								break;
							}
							default:	// treat as int64
							{
								ostringstream os;
								intColVal = row.getUintField<8>(fetchColPos);
								os << intColVal;
								value = os.str();
								break;
							}
						}
						
						if (fetchColScales[fetchColPos] > 0)
						{
							long long int_val = (long long)intColVal;
							const int ctmp_size = 65+1+1+1;
							char ctmp[ctmp_size] = {0};
							snprintf(ctmp, ctmp_size, "%lld", int_val);
							size_t l1 = strlen(ctmp);
							char* ptr = &ctmp[0];
							if (int_val < 0)
							{
								ptr++;
								idbassert(l1 >= 2);
								l1--;
							}

							if ((unsigned)fetchColScales[fetchColPos] > l1)
							{
								const char* zeros = "0000000000000000000"; //19 0's
								size_t diff = fetchColScales[fetchColPos] - l1; //this will always be > 0
								memmove((ptr + diff), ptr, l1 + 1); //also move null
								memcpy(ptr, zeros, diff);
								l1 = 0;
							}
							else
								l1 -= fetchColScales[fetchColPos];
							memmove((ptr + l1 + 1), (ptr + l1), fetchColScales[fetchColPos] + 1); //also move null
							*(ptr + l1) = '.';
							
							value = ctmp;
							string::size_type pos = value.find_first_of(".");
							if (pos == 0)
								value.insert(0,"0");
						}
						uint32_t funcScale = columnsUpdated[j]->get_funcScale();
                                                if (funcScale != 0)
                                                {
                                                        string::size_type pos = value.find_first_of("."); //decimal point
                                                        if ( pos >= value.length() )
                                                                value.insert(value.length(),".");
                                                        //padding 0 if needed
                                                        pos = value.find_first_of(".");
                                                        uint32_t digitsAfterPoint = value.length() - pos - 1;
                                                        for (uint32_t i=0; i < (funcScale-digitsAfterPoint); i++)
                                                        {
                                                                value += "0";
                                                        }
                                                }
                                                //check data length
						//trim the string if needed
                        if (value.length() > (unsigned int)colType.colWidth)
                        {
                              value = value.substr(0, colType.colWidth);
							  
							  if (!pushWarn)
                                 pushWarn = true;
								 
                              if (!pushWarning)
                                 pushWarning = true;
								
							  if (pushWarn)
								nameNeeded = true;
                        }

						WriteEngine::DctnryTuple dctTuple;
						memcpy(dctTuple.sigValue, value.c_str(), value.length());
						dctTuple.sigSize = value.length();
						dctTuple.isNull = false;
						error = fWEWrapper.tokenize(txnId, dctTuple, colType.compressionType);
						if (error != NO_ERROR)
						{
							fWEWrapper.closeDctnry(txnId, colType.compressionType);
							return false;
						}
						colTuple.data = dctTuple.token;
						colTupleList.push_back (colTuple);
					}
					fWEWrapper.closeDctnry(txnId, colType.compressionType);
					fetchColPos++;
				}
				else //constant
				{
					if (columnsUpdated[j]->get_isnull())
					{
						if ((colType.defaultValue.length() <= 0) && (colType.constraintType == CalpontSystemCatalog::NOTNULL_CONSTRAINT))
						{
							rc = 1;
							Message::Args args;
							args.add(tableColName.column);
							err = IDBErrorInfo::instance()->errorMsg(ERR_NOT_NULL_CONSTRAINTS, args);
							return rc;
						}
						else if (colType.defaultValue.length() > 0)
						{
							value = colType.defaultValue;
							if (value.length() > (unsigned int)colType.colWidth)
							{
								value = value.substr(0, colType.colWidth);
								pushWarn = true;
								if (!pushWarning)
								{
									pushWarning = true;
								}
								if (pushWarn)
									nameNeeded = true;
							}
							WriteEngine::DctnryTuple dctTuple;
							memcpy(dctTuple.sigValue, value.c_str(), value.length());
							dctTuple.sigSize = value.length();
							dctTuple.isNull = false;
							error = fWEWrapper.tokenize(txnId, dctTuple, colType.compressionType);
							if (error != NO_ERROR)
							{
								fWEWrapper.closeDctnry(txnId, colType.compressionType);
								return false;
							}
							colTuple.data = dctTuple.token;
							fWEWrapper.closeDctnry(txnId, colType.compressionType); // Constant only need to tokenize once.
						}
						else
						{
							WriteEngine::Token nullToken;
							colTuple.data = nullToken;
						}
					}
					else
					{
						value = columnsUpdated[j]->get_Data();
						
						if (value.length() > (unsigned int)colType.colWidth)
						{
							value = value.substr(0, colType.colWidth);
							pushWarn = true;
							if (!pushWarning)
							{
								pushWarning = true;
							}
							if (pushWarn)
								nameNeeded = true;
						}
						WriteEngine::DctnryTuple dctTuple;
						memcpy(dctTuple.sigValue, value.c_str(), value.length());
						dctTuple.sigSize = value.length();
						dctTuple.isNull = false;
						error = fWEWrapper.tokenize(txnId, dctTuple, colType.compressionType);
						if (error != NO_ERROR)
						{
							fWEWrapper.closeDctnry(txnId, colType.compressionType);
							return false;
						}
						colTuple.data = dctTuple.token;
						fWEWrapper.closeDctnry(txnId, colType.compressionType); // Constant only need to tokenize once.
					}

					for (unsigned row = 0; row < rowsThisRowgroup; row++)
						colTupleList.push_back (colTuple);
				}
			}
			else //Non dictionary column
			{
				colStruct.colWidth = colType.colWidth;
				if (columnsUpdated[j]->get_isFromCol())
				{
					for (unsigned i = 0; i < rowsThisRowgroup; i++)
					{
						rowGroups[txnId]->getRow(i, &row);
						if (row.isNullValue(fetchColPos))
						{
							isNull = true;
							value = "";
						}
						else
						{
							isNull = false;
							switch (fetchColTypes[fetchColPos])
							{
								case CalpontSystemCatalog::DATE:
								{
									intColVal = row.getUintField<4>(fetchColPos);
									value = DataConvert::dateToString(intColVal);
									break;
								}
								case CalpontSystemCatalog::DATETIME:
								{
									intColVal = row.getUintField<8>(fetchColPos);
									value = DataConvert::datetimeToString(intColVal);
									break;
								}
								case CalpontSystemCatalog::CHAR:
								case CalpontSystemCatalog::VARCHAR:
								{
									value = row.getStringField(fetchColPos);
									unsigned i = strlen(value.c_str());
									value = value.substr(0, i);
									break;
								}
								case CalpontSystemCatalog::VARBINARY:
								{
									value = row.getVarBinaryStringField(fetchColPos);
									break;
								}
								case CalpontSystemCatalog::BIGINT:
								{
									intColVal = row.getIntField<8>(fetchColPos);
									if (fetchColScales[fetchColPos] <= 0)
									{
										ostringstream os;
										os << intColVal;
										value = os.str();
									}
									break;
								}
								case CalpontSystemCatalog::INT:
								{
									intColVal = row.getIntField<4>(fetchColPos);
									if (fetchColScales[fetchColPos] <= 0)
									{
										ostringstream os;
										os << intColVal;
										value = os.str();
									}
									break;
								}
								case CalpontSystemCatalog::SMALLINT:
								{
									intColVal = row.getIntField<2>(fetchColPos);
									if (fetchColScales[fetchColPos] <= 0)
									{
										ostringstream os;
										os << intColVal;
										value = os.str();
									}
									break;
								}
								case CalpontSystemCatalog::TINYINT:
								{
									intColVal = row.getIntField<1>(fetchColPos);
									if (fetchColScales[fetchColPos] <= 0)
									{
										ostringstream os;
										os << intColVal;
										value = os.str();
									}
									break;
								}
								case CalpontSystemCatalog::DECIMAL:
								{
									if (fetchColColwidths[fetchColPos] == execplan::CalpontSystemCatalog::ONE_BYTE)
									{
										intColVal = row.getIntField<1>(fetchColPos);
										ostringstream os;
										os << intColVal;
										value = os.str();
									}
									else if (fetchColColwidths[fetchColPos] == execplan::CalpontSystemCatalog::TWO_BYTE)
									{
										intColVal = row.getIntField<2>(fetchColPos);
										ostringstream os;
										os << intColVal;
										value = os.str();
									}
									else if (fetchColColwidths[fetchColPos] == execplan::CalpontSystemCatalog::FOUR_BYTE)
									{
										intColVal = row.getIntField<4>(fetchColPos);
										ostringstream os;
										os << intColVal;
										value = os.str();
									}
									else if (fetchColColwidths[fetchColPos] == execplan::CalpontSystemCatalog::EIGHT_BYTE)
									{
										intColVal = row.getIntField<8>(fetchColPos);
										ostringstream os;
										os << intColVal;
										value = os.str();
									}
									else
									{
										value = row.getStringField(fetchColPos);
										unsigned i = strlen(value.c_str());
										value = value.substr(0, i);
									}
									break;
								}
								//In this case, we're trying to load a double output column with float data. This is the
								// case when you do sum(floatcol), e.g.
								case CalpontSystemCatalog::FLOAT:
								{
									float dl = row.getFloatField(fetchColPos);
									if (dl == std::numeric_limits<float>::infinity())
										continue;
									ostringstream os;	
									//@Bug 3350 fix the precision.
									os << setprecision(7) << dl;
									value = os.str();
									break;
								}
								case CalpontSystemCatalog::DOUBLE:
								{								
									double dl = row.getDoubleField(fetchColPos);
									if (dl == std::numeric_limits<double>::infinity())
										continue;
									ostringstream os;	
									//@Bug 3350 fix the precision.
									os <<setprecision(16)<< dl;
									value = os.str();
									break;
								}
								default:	// treat as int64
								{
									ostringstream os;
									intColVal = row.getUintField<8>(fetchColPos);
									os << intColVal;
									value = os.str();
									break;
								}
							}
						}
						
						if (fetchColScales[fetchColPos] > 0)
						{
							long long int_val = (long long)intColVal;
							const int ctmp_size = 65+1+1+1;
							char ctmp[ctmp_size] = {0};
							snprintf(ctmp, ctmp_size, "%lld", int_val);
							size_t l1 = strlen(ctmp);
							char* ptr = &ctmp[0];
							if (int_val < 0)
							{
								ptr++;
								idbassert(l1 >= 2);
								l1--;
							}

							if ((unsigned)fetchColScales[fetchColPos] > l1)
							{
								const char* zeros = "0000000000000000000"; //19 0's
								size_t diff = fetchColScales[fetchColPos] - l1; //this will always be > 0
								memmove((ptr + diff), ptr, l1 + 1); //also move null
								memcpy(ptr, zeros, diff);
								l1 = 0;
							}
							else
								l1 -= fetchColScales[fetchColPos];
							memmove((ptr + l1 + 1), (ptr + l1), fetchColScales[fetchColPos] + 1); //also move null
							*(ptr + l1) = '.';
							
							value = ctmp;
							string::size_type pos = value.find_first_of(".");
							if (pos == 0)
								value.insert(0,"0");
						}
						
						uint32_t funcScale = columnsUpdated[j]->get_funcScale();
						if (funcScale != 0)
						{
							string::size_type pos = value.find_first_of("."); //decimal point
							if ( pos >= value.length() )
								value.insert(value.length(),".");
							//padding 0 if needed
							pos = value.find_first_of(".");
							uint32_t digitsAfterPoint = value.length() - pos -1;
							for (uint32_t i=0; i < (funcScale-digitsAfterPoint); i++)
							{
								value += "0";
							}
						}
						
						//Check NOT NULL constraint and default value
						if ((isNull) && (colType.defaultValue.length() <= 0) && (colType.constraintType == CalpontSystemCatalog::NOTNULL_CONSTRAINT))
						{
							rc = 1;
							Message::Args args;
							args.add(tableColName.column);
							err = IDBErrorInfo::instance()->errorMsg(ERR_NOT_NULL_CONSTRAINTS, args);
							return rc;
						}
						else if ((isNull) && (colType.defaultValue.length() > 0))
						{
							isNull = false;
							bool oneWarn = false;
							
							try {
								datavalue = DataConvert::convertColumnData(colType, colType.defaultValue, pushWarn, isNull);
							}
							catch (exception&)
							{
								//@Bug 2624. Error out on conversion failure
								rc = 1;
								Message::Args args;
								args.add(string("'") + colType.defaultValue + string("'"));
								err = IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERIC_DATA, args);
							}
						
							if ((pushWarn) && (!oneWarn))
								oneWarn = true;
								
							colTuple.data = datavalue;
							colTupleList.push_back (colTuple);
							
							if (oneWarn)
								pushWarn = true;
							
							if (!pushWarning)
							{
								pushWarning = pushWarn;
							}
							if (pushWarn)
								nameNeeded = true;
						}
						else
						{		
							try {
								datavalue = DataConvert::convertColumnData(colType, value, pushWarn, isNull);
							}
							catch (exception&)
							{
								//@Bug 2624. Error out on conversion failure
								rc = 1;
								Message::Args args;
								args.add(string("'") + value + string("'"));
								err = IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERIC_DATA, args);
								return rc;
							}
							colTuple.data = datavalue;
							colTupleList.push_back (colTuple);
							if (!pushWarning)
							{
								pushWarning = pushWarn;
							}
						
							if (pushWarn)
								nameNeeded = true;
						}
					}
					fetchColPos++;
				}
				else //constant column
				{
					if (columnsUpdated[j]->get_isnull())
					{
						isNull = true;
					}
					else
					{
						isNull = false;
					}

					string inData (columnsUpdated[j]->get_Data());
					if (((colType.colDataType == execplan::CalpontSystemCatalog::DATE) && (inData =="0000-00-00")) || 
									((colType.colDataType == execplan::CalpontSystemCatalog::DATETIME) && (inData =="0000-00-00 00:00:00")))
					{	
						isNull = true;
					}
					
					uint64_t nextVal = 0;
					if (colType.autoincrement)
					{
						try {
							nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
							fDbrm.startAISequence(oid, nextVal, colType.colWidth);
						}
						catch (std::exception& ex)
						{
								err = ex.what();
								rc = 1;
								return rc;
						}
					}
					if (colType.autoincrement && ( isNull || (inData.compare("0")==0)))
					{
						//reserve nextVal
						try {
							bool reserved = fDbrm.getAIRange(oid, rowsThisRowgroup, &nextVal);
							if (!reserved)
							{
								err = IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT);
								rc = 1;
								return rc;
							}
						}
						catch (std::exception& ex)
						{
								err = ex.what();
								rc = 1;
								return rc;
						}
						isNull = false;
						bool oneWarn = false;
						for (unsigned row = 0; row < rowsThisRowgroup; row++)
						{
							
							ostringstream oss;
							oss << nextVal++;
							inData = oss.str();
							try {
								datavalue = DataConvert::convertColumnData(colType, inData, pushWarn, isNull);
							}
							catch (exception&)
							{
								//@Bug 2624. Error out on conversion failure
								rc = 1;
								Message::Args args;
								args.add(string("'") + inData + string("'"));
								err = IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERIC_DATA, args);
							}
							if ((pushWarn) && (!oneWarn))
								oneWarn = true;
							colTuple.data = datavalue;
							colTupleList.push_back (colTuple);
						}
						if (oneWarn)
							pushWarn = true;
							
						if (!pushWarning)
						{
							pushWarning = pushWarn;
						}
						if (pushWarn)
								nameNeeded = true;
					}
					else if (isNull && (colType.defaultValue.length() <= 0) && (colType.constraintType == CalpontSystemCatalog::NOTNULL_CONSTRAINT))
					{
						rc = 1;
						Message::Args args;
						args.add(tableColName.column);
						err = IDBErrorInfo::instance()->errorMsg(ERR_NOT_NULL_CONSTRAINTS, args);
						return rc;
					}
					else if (isNull && (colType.defaultValue.length() > 0))
					{
						isNull = false;
						bool oneWarn = false;
						for (unsigned row = 0; row < rowsThisRowgroup; row++)
						{
							try {
								datavalue = DataConvert::convertColumnData(colType, colType.defaultValue, pushWarn, isNull);
							}
							catch (exception&)
							{
								//@Bug 2624. Error out on conversion failure
								rc = 1;
								Message::Args args;
								args.add(string("'") + colType.defaultValue + string("'"));
								err = IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERIC_DATA, args);
							}
						
							if ((pushWarn) && (!oneWarn))
								oneWarn = true;
								
							colTuple.data = datavalue;
							colTupleList.push_back (colTuple);
						}
						if (oneWarn)
							pushWarn = true;
							
						if (!pushWarning)
						{
							pushWarning = pushWarn;
						}
						if (pushWarn)
							nameNeeded = true;
					}
					else
					{
						try {
							datavalue = DataConvert::convertColumnData(colType, inData, pushWarn, isNull);
						}
						catch (exception&)
						{
							//@Bug 2624. Error out on conversion failure
							rc = 1;
							Message::Args args;
							args.add(string("'") + inData + string("'"));
							err = IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERIC_DATA, args);
							return rc;
						}
						colTuple.data = datavalue;
						if (!pushWarning)
						{
							pushWarning = pushWarn;
						}
						if (pushWarn)
								nameNeeded = true;
						for (unsigned row = 0; row < rowsThisRowgroup; row++)
							colTupleList.push_back (colTuple);
					}
				}
			}
			if (nameNeeded) 
			{
				colNames.push_back(tableColName.column);
			}
				
			colStructList.push_back(colStruct);
			colValueList.push_back (colTupleList);
	} //end of bulding values and column structure.
	//timer.stop("fetch values");
	if (rowIDLists.size() > 0)	
	{		
		error = fWEWrapper.updateColumnRecs(txnId, colStructList, colValueList,  rowIDLists);	
	}
		
	if (error != NO_ERROR)
	{
			rc = error;
			WErrorCodes ec;
			err = ec.errorString(error);

			if (error == ERR_BRM_DEAD_LOCK)
			{
				rc = dmlpackageprocessor::DMLPackageProcessor::DEAD_LOCK_ERROR;
			}
			else if (error == ERR_BRM_VB_OVERFLOW)
			{
				rc = dmlpackageprocessor::DMLPackageProcessor::VB_OVERFLOW_ERROR;
				err = IDBErrorInfo::instance()->errorMsg(ERR_VERSIONBUFFER_OVERFLOW);
			}
	}	
	if (pushWarning)
	{
		rc = dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING;
		Message::Args args;
		string cols = "'" + colNames[0] + "'";

		for (unsigned i=1; i<colNames.size();i++)
		{
			cols = cols + ", " +  "'" + colNames[i] + "'";
		}
		args.add(cols);
		err = IDBErrorInfo::instance()->errorMsg(WARN_DATA_TRUNC,args);
	}
	//cout << "finished update" << endl;
	return rc;
}

uint8_t WE_DMLCommandProc::processUpdate1(messageqcpp::ByteStream& bs, std::string & err)
{
	uint8_t rc = 0;
	//cout << " In processUpdate" << endl;
	uint32_t tmp32, sessionID;
	TxnID txnId;
	bs >> tmp32;
	txnId = tmp32;
	if (!rowGroups[txnId]) //meta data
	{
		rowGroups[txnId] = new rowgroup::RowGroup();
		rowGroups[txnId]->deserialize(bs);
		uint8_t pkgType;
		bs >> pkgType;
		cpackages[txnId].read(bs);
		//cout << "Processed meta data in update" << endl;
		return rc;
	}
	fWEWrapper.setIsInsert(false);

	bool pushWarning = false;
	rowGroups[txnId]->setData(const_cast<uint8_t*>(bs.buf()));
	//get rows and values
	rowgroup::Row row;
	CalpontSystemCatalog::RID rid;
	rowGroups[txnId]->initRow(&row);
	string value("");
	uint32_t rowsThisRowgroup = rowGroups[txnId]->getRowCount();
	uint columnsSelected = rowGroups[txnId]->getColumnCount();
	std::vector<execplan::CalpontSystemCatalog::ColDataType> fetchColTypes =  rowGroups[txnId]->getColTypes();
	std::vector<uint> fetchColScales = rowGroups[txnId]->getScale();
	std::vector<uint> fetchColColwidths;
	std::vector<ColValues> colsVals(columnsSelected);
	int64_t intColVal = 0;
	WriteEngine::RIDList  rowIDLists;
	bool isNull = false;
	for (uint i=0; i < columnsSelected; i++)
	{
		fetchColColwidths.push_back(rowGroups[txnId]->getColumnWidth(i));
	}
	
	WriteEngine::ColTupleList aColList;
	WriteEngine::ColTuple colTuple;
	WriteEngine::ColStructList colStructList;
	WriteEngine::ColStruct colStruct;
	WriteEngine::ColValueList colValueList;

	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryStruct dctnryStruct;
    WriteEngine::DctnryValueList dctnryValueList;
	boost::any datavalue;
	
	CalpontSystemCatalog::TableName tableName;
	CalpontSystemCatalog::TableColName tableColName;
	DMLTable* tablePtr =  cpackages[txnId].get_Table();
	RowList rows = tablePtr->get_RowList();
	dmlpackage::ColumnList columnsUpdated = rows[0]->get_ColumnList();
	tableColName.table = tableName.table = tablePtr->get_TableName();
	tableColName.schema = tableName.schema = tablePtr->get_SchemaName();
	tableColName.column  = columnsUpdated[0]->get_Name();
	
	sessionID = cpackages[txnId].get_SessionID();
	
	CalpontSystemCatalog* systemCatalogPtr =
        CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	CalpontSystemCatalog::OID oid;
	try {
		oid = systemCatalogPtr->lookupOID(tableColName);
	}
	catch  (std::exception& ex)
	{
		rc = 1;
		ostringstream oss;
		oss << "lookupOID got exception " << ex.what() << " with column " << tableColName.schema << "." + tableColName.table << "." << tableColName.column;
		err = oss.str();
	}
	catch ( ... )
	{
		rc = 1;
		ostringstream oss;
		oss << "lookupOID got unknown exception with column " << tableColName.schema << "." << tableColName.table << "." << tableColName.column;
		err =  oss.str();
	}
	
	if (rc != 0)
		return rc;
		
	rowGroups[txnId]->getRow(0, &row);
	rid = row.getRid();
	uint16_t dbRoot, segment;
	uint32_t partition;
	//get file info		
	dbRoot = rowGroups[txnId]->getDBRoot();
	//fDbrm.getStartExtent(oid, startDBRoot, startPartitionNum, true);	
	convertRidToColumn (rid, partition, segment);
	colStruct.fColPartition = partition;
	colStruct.fColSegment = segment;
	colStruct.fColDbRoot = dbRoot;
	dctnryStruct.fColPartition = partition;
	dctnryStruct.fColSegment = segment;
	dctnryStruct.fColDbRoot = dbRoot;
	
	
	for (unsigned i = 0; i < rowsThisRowgroup; i++)
	{
		rowGroups[txnId]->getRow(i, &row);
		rid = row.getRid();
		//timer.start("convertrid");
		convertRidToColumn (rid, partition, segment);
		//timer.stop("convertrid");
		rowIDLists.push_back(rid);
		//Fetch values
		//timer.start("fetchval");
		for (unsigned col = 0; col < columnsSelected; col++)
		{
			// fetch and store data
			if (row.isNullValue(col))
			{
				value = "";
				//cout << "update got value " << value << endl;
				idbassert(col < colsVals.size());
				colsVals[col].push_back(value);
				continue;
			}
			switch (fetchColTypes[col])
			{
				case CalpontSystemCatalog::DATE:
				{
					intColVal = row.getUintField<4>(col);
					value = DataConvert::dateToString(intColVal);
					break;
				}
				case CalpontSystemCatalog::DATETIME:
				{
					intColVal = row.getUintField<8>(col);
					value = DataConvert::datetimeToString(intColVal);
					break;
				}
				case CalpontSystemCatalog::CHAR:
				case CalpontSystemCatalog::VARCHAR:
				{
					value = row.getStringField(col);
					unsigned i = strlen(value.c_str());
					value = value.substr(0, i);
					break;
				}
				case CalpontSystemCatalog::VARBINARY:
				{
					value = row.getVarBinaryStringField(col);
					break;
				}
				case CalpontSystemCatalog::BIGINT:
				{
					intColVal = row.getIntField<8>(col);
					if ( fetchColScales[col]<= 0)
					{
						ostringstream os;
						os << intColVal;
						value = os.str();
					}
					break;
				}
				case CalpontSystemCatalog::INT:
				{
					intColVal = row.getIntField<4>(col);
					if (fetchColScales[col] <= 0)
					{
						ostringstream os;
						os << intColVal;
						value = os.str();
					}
					break;
				}
				case CalpontSystemCatalog::SMALLINT:
				{
					intColVal = row.getIntField<2>(col);
					if (fetchColScales[col] <= 0)
					{
						ostringstream os;
						os << intColVal;
						value = os.str();
					}
					break;
				}
				case CalpontSystemCatalog::TINYINT:
				{
					intColVal = row.getIntField<1>(col);
					if (fetchColScales[col] <= 0)
					{
						ostringstream os;
						os << intColVal;
						value = os.str();
					}
					break;
				}
				case CalpontSystemCatalog::DECIMAL:
				{
					if (fetchColColwidths[col] == execplan::CalpontSystemCatalog::ONE_BYTE)
					{
									intColVal = row.getIntField<1>(col);
									ostringstream os;
									os << intColVal;
									value = os.str();
					}
					else if (fetchColColwidths[col] == execplan::CalpontSystemCatalog::TWO_BYTE)
					{
						intColVal = row.getIntField<2>(col);
						ostringstream os;
						os << intColVal;
						value = os.str();
					}
					else if (fetchColColwidths[col] == execplan::CalpontSystemCatalog::FOUR_BYTE)
					{
						intColVal = row.getIntField<4>(col);
						ostringstream os;
						os << intColVal;
						value = os.str();
					}
					else if (fetchColColwidths[col] == execplan::CalpontSystemCatalog::EIGHT_BYTE)
					{
						intColVal = row.getIntField<8>(col);
						ostringstream os;
						os << intColVal;
						value = os.str();
					}
					else
					{
						value = row.getStringField(col);
						unsigned i = strlen(value.c_str());
						value = value.substr(0, i);
					}
					break;
				}
				//In this case, we're trying to load a double output column with float data. This is the
				// case when you do sum(floatcol), e.g.
				case CalpontSystemCatalog::FLOAT:
				{
					float dl = row.getFloatField(col);
					if (dl == std::numeric_limits<float>::infinity())
						continue;
					ostringstream os;	
					//@Bug 3350 fix the precision.
					os << setprecision(7) << dl;
					value = os.str();
					break;
				}
				case CalpontSystemCatalog::DOUBLE:
				{								
					double dl = row.getDoubleField(col);
					if (dl == std::numeric_limits<double>::infinity())
						continue;
					ostringstream os;	
					//@Bug 3350 fix the precision.
					os <<setprecision(16)<< dl;
					value = os.str();
					break;
				}
				default:	// treat as int64
				{
					ostringstream os;
					intColVal = row.getUintField<8>(col);
					os << intColVal;
					value = os.str();
					break;
				}
			}
						
			if (fetchColScales[col] > 0)
			{
				long long int_val = (long long)intColVal;
				const int ctmp_size = 65+1+1+1;
				char ctmp[ctmp_size] = {0};
				snprintf(ctmp, ctmp_size, "%lld", int_val);
				size_t l1 = strlen(ctmp);
				char* ptr = &ctmp[0];
				if (int_val < 0)
				{
					ptr++;
					idbassert(l1 >= 2);
					l1--;
				}

				if ((unsigned)fetchColScales[col] > l1)
				{
					const char* zeros = "0000000000000000000"; //19 0's
					size_t diff = fetchColScales[col] - l1; //this will always be > 0
					memmove((ptr + diff), ptr, l1 + 1); //also move null
					memcpy(ptr, zeros, diff);
					l1 = 0;
				}
				else
					l1 -= fetchColScales[col];
				memmove((ptr + l1 + 1), (ptr + l1), fetchColScales[col] + 1); //also move null
				*(ptr + l1) = '.';
							
				value = ctmp;
				string::size_type pos = value.find_first_of(".");
				if (pos == 0)
				value.insert(0,"0");
			}
						
			//cout << "update got value " << value << endl;
			idbassert(col < colsVals.size());
			colsVals[col].push_back(value);
		}
		//timer.stop("fetchval");
	}
	
	//Build structures for wrapper function
	unsigned colSel = 0;
	int error = 0;
	//timer.start("buildval");
	for (unsigned int j = 0; j < columnsUpdated.size(); j++)
	{
		WriteEngine::ColTupleList colTupleList;
		tableColName.column  = columnsUpdated[j]->get_Name();
		try {
			oid = systemCatalogPtr->lookupOID(tableColName);
		}
		catch  (std::exception& ex)
		{
			rc = 1;
			ostringstream oss;
			oss << "lookupOID got exception " << ex.what() << " with column " << tableColName.schema << "." << tableColName.table << "." << tableColName.column;
			err = oss.str();
		}
		catch ( ... )
		{
			rc = 1;
			ostringstream oss;
			oss <<  "lookupOID got unknown exception with column " << tableColName.schema << "." << tableColName.table << "." << tableColName.column;
			err = oss.str();
		}
			
		if (rc != 0)
			return rc;
				
		CalpontSystemCatalog::ColType colType;
		try {
			colType = systemCatalogPtr->colType(oid);
		}
		catch  (std::exception& ex)
		{
			rc = 1;
			ostringstream oss;
			oss << "colType got exception " << ex.what() << " with column oid " << oid;
			err = oss.str();
		}
		catch ( ... )
		{
			rc = 1;
			ostringstream oss;
			oss << "colType got unknown exception with column oid " << oid;
			err = oss.str();
		}
			
		if (rc !=0)
			return rc;
				
		colStruct.dataOid = oid;
		colStruct.colDataType = (WriteEngine::ColDataType)colType.colDataType;
		colStruct.tokenFlag = false;
		colStruct.fCompressionType = colType.compressionType;
		if (isDictCol(colType))
		{
			colStruct.colWidth = 8;
			colStruct.tokenFlag = true;
			dctnryStruct.dctnryOid = colType.ddn.dictOID;
			dctnryStruct.columnOid = colStruct.dataOid;
			dctnryStruct.fCompressionType = colType.compressionType;
			dctnryStruct.colWidth = colType.colWidth;
			if (NO_ERROR != (error = fWEWrapper.openDctnry (txnId, dctnryStruct)))
			{
				WErrorCodes ec;
				err = ec.errorString(error);
				rc = error;
				return rc;
			}
			if (columnsUpdated[j]->get_isFromCol())
			{	
				std::vector < std::string > colValues = colsVals[colSel++];
				for (unsigned row = 0; row < colValues.size(); row++)
				{
					//@bug 3582. Check the real scale
					uint32_t funcScale = columnsUpdated[j]->get_funcScale();
					if (funcScale != 0)
					{
						string::size_type pos = colValues[row].find_first_of("."); //decimal point
						if ( pos >= colValues[row].length() )
							colValues[row].insert(colValues[row].length(),".");
						//padding 0 if needed
						pos = colValues[row].find_first_of(".");
						uint32_t digitsAfterPoint = colValues[row].length() - pos - 1;
						for (uint32_t i=0; i < (funcScale-digitsAfterPoint); i++)
						{
							colValues[row] += "0";
						}
					}
					//check data length

					if (colValues[row].length() > (unsigned int)colType.colWidth)
					{
						colValues[row] = colValues[row].substr(0, colType.colWidth);
						pushWarning = true;
					}
						 
						
					if (colValues[row].compare ("") == 0) //Null value is not stored in dictionary file.
					{
						WriteEngine::Token nullToken;
						colTuple.data = nullToken;
					}
					else
					{							
						WriteEngine::DctnryTuple dctTuple;
						memcpy(dctTuple.sigValue, colValues[row].c_str(), colValues[row].length());
						dctTuple.sigSize = colValues[row].length();
						dctTuple.isNull = false;
						error = fWEWrapper.tokenize(txnId,dctTuple, colType.compressionType);
						if (error != NO_ERROR)
						{
							fWEWrapper.closeDctnry(txnId, colType.compressionType);
							WErrorCodes ec;
							err = ec.errorString(error);
							rc = error;
							return rc;
						}
						colTuple.data = dctTuple.token;
					}
					colTupleList.push_back (colTuple);
				}
				fWEWrapper.closeDctnry(txnId, colType.compressionType);
			}	
			else //constant
			{
				if (columnsUpdated[j]->get_isnull())
				{
					WriteEngine::Token nullToken;
					colTuple.data = nullToken;
				}
				else
				{
					value = columnsUpdated[j]->get_Data();
						
					if (value.length() > (unsigned int)colType.colWidth)
					{
						value = value.substr(0, colType.colWidth);
						pushWarning = true;
						if (!pushWarning)
						{
							pushWarning = true;
						}
					}
					WriteEngine::DctnryTuple dctTuple;
					memcpy(dctTuple.sigValue, value.c_str(), value.length());
					dctTuple.sigSize = value.length();
					dctTuple.isNull = false;
					error = fWEWrapper.tokenize(txnId, dctTuple, colType.compressionType);
					if (error != NO_ERROR)
					{
						fWEWrapper.closeDctnry(txnId, colType.compressionType);
						WErrorCodes ec;
						err = ec.errorString(error);
						rc = error;
						return rc;
					}
					colTuple.data = dctTuple.token;
					fWEWrapper.closeDctnry(txnId, colType.compressionType); // Constant only need to tokenize once.
				}

				for (unsigned row = 0; row < rowsThisRowgroup; row++)
					colTupleList.push_back (colTuple);
			}
		}
		else
		{
			colStruct.colWidth = colType.colWidth;
			if (columnsUpdated[j]->get_isFromCol())
			{
				std::vector < std::string > colValues = colsVals[colSel++];
				for (unsigned row = 0; row < colValues.size(); row++)
				{
					if (colValues[row].compare ("") == 0)
						isNull = true;
					else
						isNull = false;
							
					//@bug 3582. Check the real scale
					uint32_t funcScale = columnsUpdated[j]->get_funcScale();
					if (funcScale != 0)
					{
						string::size_type pos = colValues[row].find_first_of("."); //decimal point
						if ( pos >= colValues[row].length() )
							colValues[row].insert(colValues[row].length(),".");
						//padding 0 if needed
						pos = colValues[row].find_first_of(".");
						uint32_t digitsAfterPoint = colValues[row].length() - pos -1;
						for (uint32_t i=0; i < (funcScale-digitsAfterPoint); i++)
						{
							colValues[row] += "0";
						}
					}

					try {
						datavalue = DataConvert::convertColumnData(colType, colValues[row], pushWarning, isNull);
					}
					catch (exception&)
					{
						//@Bug 2624. Error out on conversion failure
						rc = 1;
						Message::Args args;
						args.add(string("'") + colValues[row] + string("'"));
						err = IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERIC_DATA, args);
						return rc;
					}
					colTuple.data = datavalue;
					colTupleList.push_back (colTuple);
					if (!pushWarning)
					{
						pushWarning = true;
					}
				}
			}
			else //constant
			{
				if (columnsUpdated[j]->get_isnull())
				{
					isNull = true;
				}
				else
				{
					isNull = false;
				}

				string inData (columnsUpdated[j]->get_Data());
				uint64_t nextVal = 0;
				if (colType.autoincrement)
				{
					try {
						nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
						fDbrm.startAISequence(oid, nextVal, colType.colWidth);
					}
					catch (std::exception& ex)
					{
						err = ex.what();
						rc = 1;
						return rc;
					}
				}
				if (colType.autoincrement && ( isNull || (inData.compare("0")==0)))
				{
					//reserve nextVal
					try {
						bool reserved = fDbrm.getAIRange(oid, rowsThisRowgroup, &nextVal);
						if (!reserved)
						{
							err = IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT);
							rc = 1;
							return rc;
						}
					}
					catch (std::exception& ex)
					{
						err = ex.what();
						rc = 1;
						return rc;
					}
					isNull = false;
					for (unsigned row = 0; row < rowsThisRowgroup; row++)
					{
							
						ostringstream oss;
						oss << nextVal++;
						inData = oss.str();
						try {
							datavalue = DataConvert::convertColumnData(colType, inData, pushWarning, isNull);
						}
						catch (exception&)
						{
							//@Bug 2624. Error out on conversion failure
							rc = 1;
							Message::Args args;
							args.add(string("'") + inData + string("'"));
							err = IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERIC_DATA, args);
						}
						colTuple.data = datavalue;
						colTupleList.push_back (colTuple);
					}
				}
				else
				{
					bool pushWarn = false;
					try {
						datavalue = DataConvert::convertColumnData(colType, inData, pushWarn, isNull);
					}
					catch (exception&)
					{
						//@Bug 2624. Error out on conversion failure
						rc = 1;
						Message::Args args;
						args.add(string("'") + inData + string("'"));
						err = IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERIC_DATA, args);
						return rc;
					}
					colTuple.data = datavalue;
					if (!pushWarning)
					{
						pushWarning = pushWarn;
					}
					for (unsigned row = 0; row < rowsThisRowgroup; row++)
						colTupleList.push_back (colTuple);
				}
			}								
		}
		colStructList.push_back(colStruct);
		colValueList.push_back (colTupleList);
	}
	//timer.stop("buildval");
	if (rowIDLists.size() > 0)	
	{
		//timer.start("updateColumnRecs");
		error = fWEWrapper.updateColumnRecs(txnId, colStructList, colValueList,  rowIDLists);	
		//timer.stop("updateColumnRecs");
	}
		
	if (error != NO_ERROR)
	{
		rc = error;
		WErrorCodes ec;
		err = ec.errorString(error);

		if (error == ERR_BRM_DEAD_LOCK)
		{
			rc = dmlpackageprocessor::DMLPackageProcessor::DEAD_LOCK_ERROR;
		}
		else if (error == ERR_BRM_VB_OVERFLOW)
		{
			rc = dmlpackageprocessor::DMLPackageProcessor::VB_OVERFLOW_ERROR;
			err = IDBErrorInfo::instance()->errorMsg(ERR_VERSIONBUFFER_OVERFLOW);
		}
	}	
	return rc;
}

uint8_t WE_DMLCommandProc::processFlushFiles(messageqcpp::ByteStream& bs, std::string & err)
{
	uint8_t rc = 0;
	uint32_t flushCode, txnId;
	int error;
	bs >> flushCode;
	bs >> txnId;
	std::map<uint32_t,uint32_t> oids;

	fWEWrapper.setIsInsert(false);

	//timer.start("flushDataFiles");
	error = fWEWrapper.flushDataFiles(flushCode, txnId, oids);
	//timer.stop("flushDataFiles");

	if (error != NO_ERROR)
	{
			rc = error;
			WErrorCodes ec;
			err = ec.errorString(error);
	}
	//erase rowgroup from the rowGroup map
	if (rowGroups[txnId])
	{
		delete rowGroups[txnId];
		rowGroups[txnId] = 0;
	}
	//timer.finish();
	return rc;
}

uint8_t WE_DMLCommandProc::processDelete(messageqcpp::ByteStream& bs, 
	                                       std::string & err, 
	                                       ByteStream::quadbyte & PMId,
	                                       uint64_t& blocksChanged)
{
	uint8_t rc = 0;
	//cout << " In processDelete" << endl;
	uint32_t tmp32, sessionID;
	TxnID txnId;
	bs >> PMId;
	bs >> sessionID;
	bs >> tmp32;
	txnId = tmp32;
	string schema, tableName;
	bs >> schema;
	bs >> tableName;
	
	if (!rowGroups[txnId]) //meta data
	{
		rowGroups[txnId] = new rowgroup::RowGroup();
		rowGroups[txnId]->deserialize(bs);
		return rc;
	}
	rowGroups[txnId]->setData(const_cast<uint8_t*>(bs.buf()));
	//get row ids
	rowgroup::Row row;
	rowGroups[txnId]->initRow(&row);
	WriteEngine::RIDList  rowIDList;
	CalpontSystemCatalog::RID rid;
	uint32_t rowsThisRowgroup = rowGroups[txnId]->getRowCount();
	uint16_t dbRoot, segment;
	uint32_t partition;
	CalpontSystemCatalog::TableName aTableName;	
	aTableName.schema = schema;
	aTableName.table = tableName; 
	CalpontSystemCatalog* systemCatalogPtr =
    CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	CalpontSystemCatalog::RIDList tableRidList;
	try {
		tableRidList = systemCatalogPtr->columnRIDs(aTableName, true);
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
			
	}
	
	// querystats
	uint64_t relativeRID = 0;
	boost::scoped_array<int> preBlkNums(new int[row.getColumnCount()]);
	boost::scoped_array<uint> colWidth(new uint[row.getColumnCount()]);

	// initialize
	for (uint j = 0; j < row.getColumnCount(); j++)
	{
		preBlkNums[j] = -1;
		colWidth[j] = (row.getColumnWidth(j) >= 8 ? 8 : row.getColumnWidth(j));
	}


	for (unsigned i = 0; i < rowsThisRowgroup; i++)
	{
		rowGroups[txnId]->getRow(i, &row);
		rid = row.getRid();
		relativeRID = rid - rowGroups[txnId]->getBaseRid();
		convertRidToColumn (rid, partition, segment);
		rowIDList.push_back(rid);
		
		// populate stats.blocksChanged
		for (uint j = 0; j < row.getColumnCount(); j++)
		{
			if ((int)(relativeRID/(BYTE_PER_BLOCK/colWidth[j])) > preBlkNums[j])
			{
				blocksChanged++;
				preBlkNums[j] = relativeRID/(BYTE_PER_BLOCK/colWidth[j]);
			}
		}
	}

	//Find the partition, segment, dbroot info for this rowgroup.
	rowGroups[txnId]->getRow(0, &row);
	rid = row.getRid();
	dbRoot = rowGroups[txnId]->getDBRoot();
	WriteEngine::ColStructList colStructList;	
	WriteEngine::ColStruct colStruct;
	//get file info													
	convertRidToColumn (rid, partition, segment);
	colStruct.fColPartition = partition;
	colStruct.fColSegment = segment;
	colStruct.fColDbRoot = dbRoot;
	
	try {
	  for (unsigned i = 0; i < tableRidList.size(); i++)	
	  {
		CalpontSystemCatalog::ColType colType;
        colType = systemCatalogPtr->colType( tableRidList[i].objnum );
		colStruct.dataOid = tableRidList[i].objnum;
        colStruct.tokenFlag = false;
		colStruct.fCompressionType = colType.compressionType;
        if (colType.colWidth > 8)         //token
        {
          colStruct.colWidth = 8;
          colStruct.tokenFlag = true;
        }
        else
        {
          colStruct.colWidth = colType.colWidth;
        }

        colStruct.colDataType = (WriteEngine::ColDataType)colType.colDataType;

        colStructList.push_back( colStruct );
	  }
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
			
	}
		
	std::vector<ColStructList> colExtentsStruct;
	std::vector<void *> colOldValueList;
	std::vector<RIDList> ridLists;
	colExtentsStruct.push_back(colStructList);
	ridLists.push_back(rowIDList);
	int error = 0;

	fWEWrapper.setIsInsert(false);

	error = fWEWrapper.deleteRow( txnId, colExtentsStruct, colOldValueList, ridLists );
	if (error != NO_ERROR)
	{
			rc = error;
			//cout << "WE Error code " << error << endl;
			WErrorCodes ec;
			err = ec.errorString(error);

			if (error == ERR_BRM_DEAD_LOCK)
			{
				rc = dmlpackageprocessor::DMLPackageProcessor::DEAD_LOCK_ERROR;
			}
			else if (error == ERR_BRM_VB_OVERFLOW)
			{
				rc = dmlpackageprocessor::DMLPackageProcessor::VB_OVERFLOW_ERROR;
				err = IDBErrorInfo::instance()->errorMsg(ERR_VERSIONBUFFER_OVERFLOW);
			}
	}	
	//cout << "WES return rc " << (int)rc << " with msg " << err << endl;
	return rc;
}

uint8_t WE_DMLCommandProc::processRemoveMeta(messageqcpp::ByteStream& bs, std::string & err)
{
	uint8_t rc = 0;
	//cout << "In processRemoveMeta" << endl;
	if (!fRBMetaWriter.get())
		return rc;
		
	try 
	{
		fRBMetaWriter->deleteFile();
		fRBMetaWriter.reset();
	}
	catch(exception & ex)
	{
		err = ex.what();
		rc = 1;
	}
	return rc;
}

//------------------------------------------------------------------------------
// Process bulk rollback command
//------------------------------------------------------------------------------
uint8_t WE_DMLCommandProc::processBulkRollback(messageqcpp::ByteStream& bs,
	std::string& err)
{
	uint8_t rc = 0;
	err.clear();

	try {
		uint32_t    tableOID;
		uint64_t    tableLockID;
		std::string tableName;
		std::string appName;

		// May want to eventually comment out this logging to stdout,
		// but it shouldn't hurt to keep in here.
		std::cout << "processBulkRollback";
		bs >> tableLockID;
		std::cout << ": tableLock-"<< tableLockID;

		bs >> tableOID;
		std::cout << "; tableOID-" << tableOID;
		
		bs >> tableName;
		if (tableName.length() == 0)
		{
			CalpontSystemCatalog* systemCatalogPtr =
			CalpontSystemCatalog::makeCalpontSystemCatalog(0);
			CalpontSystemCatalog::TableName aTableName = systemCatalogPtr->tableName(tableOID);	
			tableName = aTableName.toString();
			
		}
		std::cout << "; table-"    << tableName;

		bs >> appName;
		std::cout << "; app-"      << appName << std::endl;

		int we_rc = fWEWrapper.bulkRollback(
			tableOID,
			tableLockID,
			tableName,
			appName,
			false, // no extra debug logging to the console
			err );

		if (we_rc != NO_ERROR)
			rc = 2;
	}
	catch(exception& ex)
	{
		std::cout << "processBulkRollback: exception-" << ex.what() <<std::endl;
		err = ex.what();
		rc  = 1;
	}

	return rc;
}

//------------------------------------------------------------------------------
// Process bulk rollback cleanup command (deletes bulk rollback meta data files)
//------------------------------------------------------------------------------
uint8_t WE_DMLCommandProc::processBulkRollbackCleanup(
	messageqcpp::ByteStream& bs,
	std::string& err)
{
	uint8_t rc = 0;
	err.clear();

    try {
        uint32_t tableOID;

		// May want to eventually comment out this logging to stdout,
		// but it shouldn't hurt to keep in here.
		std::cout << "processBulkRollbackCleanup";
        bs >> tableOID;
		std::cout << ": tableOID-" << tableOID << std::endl;

        BulkRollbackMgr::deleteMetaFile( tableOID );
    }
    catch(exception& ex)
    {
		std::cout << "processBulkRollbackCleanup: exception-" << ex.what() <<
			std::endl;
		err = ex.what();
		rc  = 1;
	}

	return rc;
}

	uint8_t WE_DMLCommandProc::updateSyscolumnNextval(ByteStream& bs, std::string & err) 
	{
		uint32_t columnOid, sessionID;
		uint64_t nextVal;
		int rc = 0;
		bs >> columnOid;
		bs >> nextVal;
		bs >> sessionID;
		
		rc = fWEWrapper.updateNextValue(columnOid, nextVal, sessionID); 	
		if (rc != 0)
		{
			err = "Error in WE::updateNextValue";
			rc =1;
		}
			
		return rc;
	}
}
