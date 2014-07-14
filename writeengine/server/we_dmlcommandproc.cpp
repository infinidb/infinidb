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
#include "we_confirmhdfsdbfile.h"
#include "cacheutils.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"
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
	err.clear();
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

	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionId);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	CalpontSystemCatalog::ROPair tableRoPair;
	std::vector<string> colNames;
	bool isWarningSet = false;
	try
	{
		tableRoPair = systemCatalogPtr->tableRID(tableName);

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
				colStruct.colDataType = colType.colDataType;
	
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
								{
									pushWarning = true;
									isWarningSet = true;
									if ((rc != NO_ERROR) && (rc != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING))
										rc = dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING;
										
									colNames.push_back(tableColName.column);
								}
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
								try
								{
									nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
									fDbrm.startAISequence(oid, nextVal, colType.colWidth, colType.colDataType);
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
								try
								{
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
							if ( pushWarning) 
							{
								if (!isWarningSet)
									isWarningSet = true;
								if ( rc != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING )
									rc = dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING;
									
								colNames.push_back(tableColName.column);
							}

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
	fWEWrapper.setBulkFlag(true);
	fWEWrapper.setTransId(txnid.id);
	//For hdfs use only
	uint32_t  tblOid = tableRoPair.objnum;
	if (idbdatafile::IDBPolicy::useHdfs())
	{

		std::vector<Column> columns;
		DctnryStructList dctnryList;
		CalpontSystemCatalog::ColType colType;
		std::vector<DBRootExtentInfo> colDBRootExtentInfo;
		Convertor convertor;
		dbRootExtTrackerVec.clear();
		fRBMetaWriter.reset(new RBMetaWriter("SingleInsert", NULL));
		CalpontSystemCatalog::RIDList ridList;
		try
		{
			ridList = systemCatalogPtr->columnRIDs(tableName, true);
			std::vector<OID>    dctnryStoreOids(ridList.size()) ;
			std::vector<BRM::EmDbRootHWMInfo_v> dbRootHWMInfoColVec(ridList.size());
			bool bFirstExtentOnThisPM = false;

			// First gather HWM BRM information for all columns
			std::vector<int> colWidths;
			for (unsigned i=0; i < ridList.size(); i++)
			{
			rc = BRMWrapper::getInstance()->getDbRootHWMInfo(ridList[i].objnum, dbRootHWMInfoColVec[i]);
			//need handle error

			CalpontSystemCatalog::ColType colType2 = systemCatalogPtr->colType(ridList[i].objnum);
			colWidths.push_back( convertor.getCorrectRowWidth(
				colType2.colDataType, colType2.colWidth) );
			}

			for (unsigned i=0; i < ridList.size(); i++)
			{
				// Find DBRoot/segment file where we want to start adding rows
				colType = systemCatalogPtr->colType(ridList[i].objnum);
				boost::shared_ptr<DBRootExtentTracker> pDBRootExtentTracker (new DBRootExtentTracker(ridList[i].objnum,
					colWidths, dbRootHWMInfoColVec, i, 0) );
				dbRootExtTrackerVec.push_back( pDBRootExtentTracker );
				DBRootExtentInfo dbRootExtent;
				std::string trkErrMsg;
				bool bEmptyPM;
				if (i == 0)
					rc = pDBRootExtentTracker->selectFirstSegFile(dbRootExtent,bFirstExtentOnThisPM, bEmptyPM, trkErrMsg);
				else
					pDBRootExtentTracker->assignFirstSegFile(*(dbRootExtTrackerVec[0].get()),dbRootExtent);
				colDBRootExtentInfo.push_back(dbRootExtent);

				Column aColumn;
				aColumn.colWidth = convertor.getCorrectRowWidth(colType.colDataType, colType.colWidth);
				aColumn.colDataType = colType.colDataType;
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
			fRBMetaWriter->init(tblOid, tableName.table);
			fRBMetaWriter->saveBulkRollbackMetaData(columns, dctnryStoreOids, dbRootHWMInfoColVec);
			//cout << "Backing up hwm chunks" << endl;
			for (unsigned i=0; i<dctnryList.size(); i++) //back up chunks for compressed dictionary
			{
				// @bug 5572 HDFS tmp file - Ignoring return flag, don't need in this context
				fRBMetaWriter->backupDctnryHWMChunk(
				dctnryList[i].dctnryOid, dctnryList[i].fColDbRoot, dctnryList[i].fColPartition, dctnryList[i].fColSegment);
			}
		}
		catch (std::exception& ex)
		{
			err = ex.what();
			rc = 1;
			return rc;
		}
	}

	if (colValuesList[0].size() > 0)
	{
		if (NO_ERROR !=
		(error = fWEWrapper.insertColumnRec_Single(txnid.id, colStructs, colValuesList, dctnryStructList, dicStringList, tableRoPair.objnum)))
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
	std::vector<BRM::OID_t>  oidsToFlush;
	for ( unsigned i=0; i < colStructs.size(); i++)
	{
		oids[colStructs[i].dataOid] = colStructs[i].dataOid;
		oidsToFlush.push_back(colStructs[i].dataOid);
	}

	for (unsigned i=0; i<dctnryStructList.size(); i++)
	{
		oids[dctnryStructList[i].dctnryOid] = dctnryStructList[i].dctnryOid;
		oidsToFlush.push_back(dctnryStructList[i].dctnryOid);
	}

	fWEWrapper.setTransId(txnid.id);
	vector<LBID_t> lbidList;
	if (idbdatafile::IDBPolicy::useHdfs())
	{
		//save the extent info to mark them invalid, after flush, the meta file will be gone.
		std::tr1::unordered_map<TxnID, SP_TxnLBIDRec_t>::iterator mapIter;
		std::tr1::unordered_map<TxnID, SP_TxnLBIDRec_t>	m_txnLBIDMap = fWEWrapper.getTxnMap();
		try
		{
			mapIter = m_txnLBIDMap.find(txnid.id);
			if (mapIter != m_txnLBIDMap.end())
			{
				SP_TxnLBIDRec_t spTxnLBIDRec = (*mapIter).second;
				std::tr1::unordered_map<BRM::LBID_t, uint32_t> ::iterator listIter = spTxnLBIDRec->m_LBIDMap.begin();
				while (listIter != spTxnLBIDRec->m_LBIDMap.end())
				{
					lbidList.push_back(listIter->first);
					listIter++;
				}
			}
		}
		catch(...) {}
	}

	//flush files
	// @bug5333, up to here, rc may have an error code already, don't overwrite it.
	int flushChunksRc = fWEWrapper.flushChunks(0, oids);  // why not pass rc to flushChunks?
	if (flushChunksRc != NO_ERROR)
	{
		WErrorCodes ec;
		std::ostringstream ossErr;
		ossErr << "Error flushing chunks for table " << tableName <<
			"; " << ec.errorString(flushChunksRc);
		// Append to errmsg in case we already have an error
		if (err.length() > 0)
			err += "; ";
		err += ossErr.str();
		if (error == NO_ERROR)
			error = flushChunksRc;
		if ((rc == NO_ERROR) || (rc == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING))
			rc = 1;  // return hardcoded 1 as the above
	}
	
	// Confirm HDFS DB file changes "only" if no error up to this point
	if (idbdatafile::IDBPolicy::useHdfs())
	{
		if (error == NO_ERROR)
		{
			std::string eMsg;
			ConfirmHdfsDbFile confirmHdfs;
			error = confirmHdfs.confirmDbFileListFromMetaFile(tblOid,eMsg);
			if (error != NO_ERROR)
			{
				ostringstream ossErr;
				ossErr << "Error confirming changes to table " <<
					tableName << "; " << eMsg;
				err = ossErr.str();
				rc  = 1;
			}
			else // Perform extra cleanup that is necessary for HDFS
			{
				std::string eMsg;
				ConfirmHdfsDbFile confirmHdfs;
				int confirmRc2 = confirmHdfs.endDbFileListFromMetaFile(
					tblOid, true, eMsg);
				if (confirmRc2 != NO_ERROR)
				{
					// Might want to log this error, but don't think we need
					// to report as fatal, since all changes were confirmed.
				}

				//flush PrimProc FD cache
				TableMetaData* aTableMetaData = TableMetaData::makeTableMetaData(tblOid);
				ColsExtsInfoMap colsExtsInfoMap = aTableMetaData->getColsExtsInfoMap();
				ColsExtsInfoMap::iterator it = colsExtsInfoMap.begin();
				ColExtsInfo::iterator aIt;
				std::vector<BRM::FileInfo> files;
				BRM::FileInfo aFile;
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
						aIt++;
					}
					it++;
				}
				if (files.size() > 0)
					cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID());

				cacheutils::flushOIDsFromCache(oidsToFlush);
				fDbrm.invalidateUncommittedExtentLBIDs(0, &lbidList);

				try
				{
					BulkRollbackMgr::deleteMetaFile( tblOid );
				}
				catch(exception & ex)
				{
					err = ex.what();
					rc = 1;
				}
			}
		} // (error == NO_ERROR) through call to flushChunks()

		if (error != NO_ERROR) // rollback
		{
			string applName ("SingleInsert");
			fWEWrapper.bulkRollback(tblOid,txnid.id,tableName.toString(),
				applName, false, err);	
			BulkRollbackMgr::deleteMetaFile( tblOid );	
		}
	} // extra hdfs steps
	
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	TableMetaData::removeTableMetaData(tblOid);
	if ((rc == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING) || isWarningSet)
	{
		if (rc == NO_ERROR)
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
	try
	{
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
	if (idbdatafile::IDBPolicy::useHdfs())
		isAutocommitOn = true;
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
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionId);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	CalpontSystemCatalog::ROPair roPair;
	CalpontSystemCatalog::RIDList ridList;
	CalpontSystemCatalog::DictOIDList dictOids;
	try
	{
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
	std::vector<Column> columns;
	DctnryStructList dctnryList;
	std::vector<BRM::EmDbRootHWMInfo_v> dbRootHWMInfoColVec(ridList.size());

	uint32_t  tblOid = roPair.objnum;
	CalpontSystemCatalog::ColType colType;
	std::vector<DBRootExtentInfo> colDBRootExtentInfo;
	bool bFirstExtentOnThisPM = false;
	Convertor convertor;
	if ( fIsFirstBatchPm )
	{
		dbRootExtTrackerVec.clear();
		if (isAutocommitOn || ((fRBMetaWriter.get() == NULL) && (!isAutocommitOn)))
			fRBMetaWriter.reset(new RBMetaWriter("BatchInsert", NULL));
		fWEWrapper.setIsInsert(true);
		fWEWrapper.setBulkFlag(true);
		fWEWrapper.setTransId(txnid.id);
		try
		{
			// First gather HWM BRM information for all columns
			std::vector<int> colWidths;
			for (unsigned i=0; i < ridList.size(); i++)
			{
			rc = BRMWrapper::getInstance()->getDbRootHWMInfo(ridList[i].objnum, dbRootHWMInfoColVec[i]);
			//need handle error

			CalpontSystemCatalog::ColType colType2 = systemCatalogPtr->colType(ridList[i].objnum);
			colWidths.push_back( convertor.getCorrectRowWidth(
				colType2.colDataType, colType2.colWidth) );
			}

			for (unsigned i=0; i < ridList.size(); i++)
			{
				// Find DBRoot/segment file where we want to start adding rows
				colType = systemCatalogPtr->colType(ridList[i].objnum);
				boost::shared_ptr<DBRootExtentTracker> pDBRootExtentTracker (new DBRootExtentTracker(ridList[i].objnum,
					colWidths, dbRootHWMInfoColVec, i, 0) );
				dbRootExtTrackerVec.push_back( pDBRootExtentTracker );
				DBRootExtentInfo dbRootExtent;
				std::string trkErrMsg;
				bool bEmptyPM;
				if (i == 0)
				{
					rc = pDBRootExtentTracker->selectFirstSegFile(dbRootExtent,bFirstExtentOnThisPM, bEmptyPM, trkErrMsg);
				/*	cout << "bEmptyPM = " << (int) bEmptyPM << " bFirstExtentOnThisPM= " << (int)bFirstExtentOnThisPM <<
					" oid:dbroot:hwm = " << ridList[i].objnum << ":"<<dbRootExtent.fDbRoot << ":"
					<<":"<<dbRootExtent.fLocalHwm << " err = " << trkErrMsg << endl; */
				}
				else
					pDBRootExtentTracker->assignFirstSegFile(*(dbRootExtTrackerVec[0].get()),dbRootExtent);
					
				
				colDBRootExtentInfo.push_back(dbRootExtent);

				Column aColumn;
				aColumn.colWidth = convertor.getCorrectRowWidth(colType.colDataType, colType.colWidth);
				aColumn.colDataType = colType.colDataType;
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
		//@Bug 5996 validate hwm before starts
		rc = validateColumnHWMs(ridList, systemCatalogPtr, colDBRootExtentInfo, "Starting");
		if ( rc != 0)
		{
			WErrorCodes ec;
			err = ec.errorString(rc);
			err += " Check err.log for detailed information.";
			fIsFirstBatchPm = false;
			rc = 1;
			return rc;
		}
	}
	std::vector<BRM::LBIDRange>   rangeList;

	// use of MetaFile for bulk rollback support
	if ( fIsFirstBatchPm && isAutocommitOn)
	{
		//save meta data, version last block for each dbroot at the start of batch insert
		try
		{
			fRBMetaWriter->init(tblOid, tableName.table);
			fRBMetaWriter->saveBulkRollbackMetaData(columns, dctnryStoreOids, dbRootHWMInfoColVec);
			//cout << "Saved meta files" << endl;
			if (!bFirstExtentOnThisPM)
			{
				//cout << "Backing up hwm chunks" << endl;
				for (unsigned i=0; i < dctnryList.size(); i++) //back up chunks for compressed dictionary
				{
					// @bug 5572 HDFS tmp file - Ignoring return flag, don't need in this context
					fRBMetaWriter->backupDctnryHWMChunk(
						dctnryList[i].dctnryOid, dctnryList[i].fColDbRoot, dctnryList[i].fColPartition, dctnryList[i].fColSegment);
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

	}

	std::vector<string> colNames;
	bool isWarningSet = false;
	if (rows.size())
	{
		Row *rowPtr = rows.at(0);
		ColumnList columns = rowPtr->get_ColumnList();
		ColumnList::const_iterator column_iterator = columns.begin();

		try
		{
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
				colStruct.colDataType = colType.colDataType;

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
		try
		{
			for (unsigned int i = 0; i < numcols; i++)
			{
				WriteEngine::ColTupleList colTuples;
				WriteEngine::DctColTupleList dctColTuples;
				RowList::const_iterator row_iterator = rows.begin();
				bool pushWarning = false;
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
							try
							{
								nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
								fDbrm.startAISequence(oid, nextVal, colType.colWidth, colType.colDataType);
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
							try
							{
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
				if (pushWarning)
				{
					colNames.push_back(tableColName.column);
					isWarningSet = true;
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
/* Begin-Disable use of MetaFile for bulk rollback support;
   Use alternate call below that passes 0 ptr for RBMetaWriter
			if (NO_ERROR !=
			(error = fWEWrapper.insertColumnRecs(txnid.id, colStructs, colValuesList, dctnryStructList, dicStringList,
						dbRootExtTrackerVec, fRBMetaWriter.get(), bFirstExtentOnThisPM, isInsertSelect, 0, roPair.objnum, fIsFirstBatchPm)))
End-Disable use of MetaFile for bulk rollback support
*/
				
			if (NO_ERROR !=
			(error = fWEWrapper.insertColumnRecs(txnid.id, colStructs, colValuesList, dctnryStructList, dicStringList,
						dbRootExtTrackerVec, 0, bFirstExtentOnThisPM, isInsertSelect, isAutocommitOn, roPair.objnum, fIsFirstBatchPm)))
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
	if ( isWarningSet && ( rc == NO_ERROR ) )
	{
		rc = dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING;
		//cout << "Got warning" << endl;
		Message::Args args;
		string cols = "'" + colNames[0] + "'";

		for (unsigned i=1; i<colNames.size();i++)
		{
			cols = cols + ", " +  "'" + colNames[i] + "'";
		}
		args.add(cols);
		err = IDBErrorInfo::instance()->errorMsg(WARN_DATA_TRUNC,args);
		
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
	std::vector<BRM::FileInfo> files;
	BRM::FileInfo aFile;
	while (it != colsExtsInfoMap.end())
	{
		aIt = (it->second).begin();
		aArg.oid = it->first;
		aFile.oid = it->first;
		//cout << "OID:" << aArg.oid;
		while (aIt != (it->second).end())
		{
			aArg.partNum = aIt->partNum;
			aArg.segNum = aIt->segNum;
			aArg.hwm = aIt->hwm;
			if (!aIt->isDict)
				setHWMArgs.push_back(aArg);
			aFile.partitionNum = aIt->partNum;
			aFile.dbRoot =aIt->dbRoot;
			aFile.segmentNum = aIt->segNum;
			aFile.compType = aIt->compType;
			//cout <<"Added to files oid:dbroot:part:seg:compType = " << aFile.oid<<":"<<aFile.dbRoot<<":"<<aFile.partitionNum<<":"<<aFile.segmentNum
			//<<":"<<aFile.compType <<endl;
			files.push_back(aFile);
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
	fWEWrapper.setBulkFlag(true);

	std::map<uint32_t,uint32_t> oids;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(sessionId);

	CalpontSystemCatalog::TableName aTableName =  systemCatalogPtr->tableName(tableOid);
	CalpontSystemCatalog::RIDList ridList;
	try
	{
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
	try
	{
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

	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);

	fIsFirstBatchPm = true;
	if ((idbdatafile::IDBPolicy::useHdfs()) && (files.size()>0) )
		cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID());
	TableMetaData::removeTableMetaData(tableOid);
	return rc;
}

uint8_t WE_DMLCommandProc::processBatchInsertHwm(messageqcpp::ByteStream& bs, std::string & err)
{
	uint8_t tmp8, rc = 0;
	err.clear();
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
	//cout << "processBatchInsertHwm: tranid:isAutoCommitOn = " <<txnID <<":"<< (int)isAutoCommitOn << endl;
	std::vector<BRM::FileInfo> files;
	std::vector<BRM::OID_t>  oidsToFlush;

	BRM::FileInfo aFile;
	//BRM::FileInfo curFile;
	TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);
	ColsExtsInfoMap colsExtsInfoMap = aTbaleMetaData->getColsExtsInfoMap();
	ColsExtsInfoMap::iterator it = colsExtsInfoMap.begin();
	ColExtsInfo::iterator aIt;
	CalpontSystemCatalog::RIDList ridList;
	CalpontSystemCatalog::ROPair roPair;
	std::vector<DBRootExtentInfo> colDBRootExtentInfo;
	DBRootExtentInfo aExtentInfo;
	while (it != colsExtsInfoMap.end())
	{
		aIt = (it->second).begin();
		aFile.oid = it->first;
		oidsToFlush.push_back(aFile.oid);
		roPair.objnum = aFile.oid;
		aExtentInfo.fPartition = 0;
		aExtentInfo.fDbRoot = 0;
		aExtentInfo.fSegment = 0;
		aExtentInfo.fLocalHwm = 0;
		bool isDict = false;
		while (aIt != (it->second).end())
		{
			aFile.partitionNum = aIt->partNum;
			aFile.dbRoot =aIt->dbRoot;
			aFile.segmentNum = aIt->segNum;
			aFile.compType = aIt->compType;
			files.push_back(aFile);
			if (!aIt->isDict)
			{
				if ((aIt->partNum > aExtentInfo.fPartition) || ((aIt->partNum == aExtentInfo.fPartition) && (aIt->segNum > aExtentInfo.fSegment)) || 
					((aIt->partNum == aExtentInfo.fPartition) && (aIt->segNum == aExtentInfo.fSegment) && (aIt->segNum >aExtentInfo.fLocalHwm )))
				{
					aExtentInfo.fPartition = aIt->partNum;
					aExtentInfo.fDbRoot = aIt->dbRoot;
					aExtentInfo.fSegment = aIt->segNum;
					aExtentInfo.fLocalHwm = aIt->hwm;
				}
			}
			else
			{
				isDict = true;
			}
			aIt++;
		}
		if (!isDict)
		{
			ridList.push_back(roPair);
			colDBRootExtentInfo.push_back(aExtentInfo);
		}
		it++;
	}

	//@Bug 5996. Validate hwm before set them
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(0);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	try {
		CalpontSystemCatalog::TableName tableName = systemCatalogPtr->tableName(tableOid);
		ridList = systemCatalogPtr->columnRIDs(tableName);		
	}
	catch(exception& ex)
	{
		err = ex.what();
		rc = 1;
		TableMetaData::removeTableMetaData(tableOid);

		fIsFirstBatchPm = true;
		//cout << "flush files when autocommit off" << endl;
		fWEWrapper.setIsInsert(true);
		fWEWrapper.setBulkFlag(true);
		return rc;
	}
	rc = validateColumnHWMs(ridList, systemCatalogPtr, colDBRootExtentInfo, "Ending");
	if ( rc != 0)
	{
		WErrorCodes ec;
		err = ec.errorString(rc);
		err += " Check err.log for detailed information.";
		TableMetaData::removeTableMetaData(tableOid);

		fIsFirstBatchPm = true;
		fWEWrapper.setIsInsert(true);
		fWEWrapper.setBulkFlag(true);
		rc = 1;
		return rc;
	}

	try
	{
		if (isAutoCommitOn)
		{
			bs.restart();
			if (fWEWrapper.getIsInsert())
			{
				// @bug5333, up to here, rc == 0, but flushchunk may fail.
				rc = processBatchInsertHwmFlushChunks(tableOid, txnID, files, oidsToFlush, err);
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

	// Handle case where isAutoCommitOn is false
		BRM::DBRM dbrm;
		//cout << " In processBatchInsertHwm setting hwm" << endl;
		std::vector<BRM::BulkSetHWMArg> setHWMArgs;
		it = colsExtsInfoMap.begin();
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
						//@Bug 6029 dictionary store files already set hwm.
						if (!aIt->isDict)
							setHWMArgs.push_back(aArg);
						aIt++;
				}
				it++;
		}

	TableMetaData::removeTableMetaData(tableOid);

	fIsFirstBatchPm = true;
	//cout << "flush files when autocommit off" << endl;
	fWEWrapper.setIsInsert(true);
	fWEWrapper.setBulkFlag(true);

	rc = processBatchInsertHwmFlushChunks(tableOid, txnID,
		files, oidsToFlush, err);
	bs.restart();
	try
	{
		serializeInlineVector (bs, setHWMArgs);
	}
	catch (exception& ex)
	{
		// Append to errmsg in case we already have an error
		if (err.length() > 0)
			err += "; ";
		err += ex.what();
		rc = 1;
		return rc;
	}
	//cout << "flush is called for transaction " << txnID << endl;

	return rc;
}

//------------------------------------------------------------------------------
// Flush chunks for the specified table and transaction.
// Also confirms changes to DB files (for hdfs).
// files vector represents list of files to be purged from PrimProc cache.
// oid2ToFlush  represents list of oids  to be flushed from PrimProc cache.
// Afterwords, the following attributes are reset as follows:
//   fWEWrapper.setIsInsert(false);
//   fWEWrapper.setBulkFlag(false);
//   fIsFirstBatchPm = true;
// returns 0 for success; returns 1 if error occurs
//------------------------------------------------------------------------------
uint8_t WE_DMLCommandProc::processBatchInsertHwmFlushChunks(
	uint32_t tblOid, int txnID,
	const std::vector<BRM::FileInfo>& files,
	const std::vector<BRM::OID_t>& oidsToFlush,
	std::string& err)
{
	uint8_t rc = 0;
	std::map<uint32_t,uint32_t>       oids;
	CalpontSystemCatalog::TableName   aTableName;
	CalpontSystemCatalog::RIDList     ridList;
	CalpontSystemCatalog::DictOIDList dictOids;

	try
	{
		boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
			CalpontSystemCatalog::makeCalpontSystemCatalog(txnID);
		aTableName = systemCatalogPtr->tableName(tblOid);
		ridList    = systemCatalogPtr->columnRIDs(aTableName, true);
		dictOids   = systemCatalogPtr->dictOIDs(aTableName);
	}
	catch (exception& ex)
	{
		std::ostringstream ossErr;
		ossErr << "System Catalog error for table OID " << tblOid;
		// Include tbl name in msg unless exception occurred before we got it
		if (aTableName.table.length() > 0)
			ossErr << '(' << aTableName << ')';
		ossErr << "; " << ex.what();
		err = ossErr.str();
		rc = 1;
		return rc;
	}

	for (unsigned i=0; i < ridList.size(); i++)
	{
		oids[ridList[i].objnum] = ridList[i].objnum;
	}
	for (unsigned i=0; i < dictOids.size(); i++)
	{
		oids[dictOids[i].dictOID] = dictOids[i].dictOID;
	}

	fWEWrapper.setTransId(txnID);

	// @bug5333, up to here, rc == 0, but flushchunk may fail.
	rc = fWEWrapper.flushChunks(0, oids);
	if (rc == NO_ERROR)
	{
		// Confirm changes to db files "only" if no error up to this point
		if (idbdatafile::IDBPolicy::useHdfs())
		{
			std::string eMsg;
			ConfirmHdfsDbFile confirmHdfs;
			int confirmDbRc = confirmHdfs.confirmDbFileListFromMetaFile(
				tblOid,eMsg);
			if (confirmDbRc == NO_ERROR)
			{
				int endDbRc = confirmHdfs.endDbFileListFromMetaFile(
					tblOid, true, eMsg);
				if (endDbRc != NO_ERROR)
				{
					// Might want to log this error, but don't think we
					// need to report as fatal, as all changes were confirmed.
				}

				if (files.size()>0)
					cacheutils::purgePrimProcFdCache(files,
						Config::getLocalModuleID());
				cacheutils::flushOIDsFromCache(oidsToFlush);
			}
			else
			{
				ostringstream ossErr;
				ossErr << "Error confirming changes to table " <<
					aTableName << "; " << eMsg;
				err = ossErr.str();
				rc = 1; // reset to 1
			}
		}
	}
	else // flushChunks error
	{
		WErrorCodes ec;
		std::ostringstream ossErr;
		ossErr << "Error flushing chunks for table " << aTableName <<
			"; " << ec.errorString(rc);
		err = ossErr.str();
		rc = 1; // reset to 1
	}

	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
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
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	CalpontSystemCatalog::TableName aTableName;
	try
	{
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
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.setTransId(txnId);
	if (!rowGroups[txnId]) //meta data
	{
		rowGroups[txnId] = new rowgroup::RowGroup();
		rowGroups[txnId]->deserialize(bs);
		uint8_t pkgType;
		bs >> pkgType;
		cpackages[txnId].read(bs);
		//cout << "Processed meta data in update" << endl;

		rc = fWEWrapper.startTransaction(txnId);
		if (rc != NO_ERROR)
		{
			WErrorCodes ec;
			err = ec.errorString(rc);
		}
		return rc;
	}

	bool pushWarning = false;
	rowgroup::RGData rgData;
	rgData.deserialize(bs);
	rowGroups[txnId]->setData(&rgData);
	//rowGroups[txnId]->setData(const_cast<uint8_t*>(bs.buf()));
	//get rows and values
	rowgroup::Row row;
	rowGroups[txnId]->initRow(&row);
	string value("");
	uint32_t rowsThisRowgroup = rowGroups[txnId]->getRowCount();
	uint32_t columnsSelected = rowGroups[txnId]->getColumnCount();
	std::vector<execplan::CalpontSystemCatalog::ColDataType> fetchColTypes =  rowGroups[txnId]->getColTypes();
	std::vector<uint32_t> fetchColScales = rowGroups[txnId]->getScale();
	std::vector<uint32_t> fetchColColwidths;
	for (uint32_t i=0; i < columnsSelected; i++)
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

	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	CalpontSystemCatalog::OID oid = 0;
	CalpontSystemCatalog::ROPair tableRO;
	try
	{
		tableRO =  systemCatalogPtr->tableRID(tableName);
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
	uint16_t dbRoot, segment, blockNum;
	uint32_t partition;
	uint8_t extentNum;
	//Get the file information from rowgroup
	dbRoot = rowGroups[txnId]->getDBRoot();
	rowGroups[txnId]->getLocation(&partition, &segment, &extentNum, &blockNum);
	colStruct.fColPartition = partition;
	colStruct.fColSegment = segment;
	colStruct.fColDbRoot = dbRoot;
	dctnryStruct.fColPartition = partition;
	dctnryStruct.fColSegment = segment;
	dctnryStruct.fColDbRoot = dbRoot;
	TableMetaData* aTableMetaData = TableMetaData::makeTableMetaData(tableRO.objnum);
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
		try
		{
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
			try
			{
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
			try
			{
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
			colStruct.colDataType = colType.colDataType;
			colStruct.tokenFlag = false;
			colStruct.fCompressionType = colType.compressionType;
			tableColName.column  = columnsUpdated[j]->get_Name();
			if( !ridsFetched)
			{
				// querystats
				uint64_t relativeRID = 0;

				for (unsigned i = 0; i < rowsThisRowgroup; i++)
				{
					rowGroups[txnId]->getRow(i, &row);
					rid = row.getRid();
					relativeRID = rid - rowGroups[txnId]->getBaseRid();
					rid = relativeRID;
					convertToRelativeRid (rid, extentNum, blockNum);
					rowIDLists.push_back(rid);
					uint32_t colWidth = (colTypes[j].colWidth > 8 ? 8 : colTypes[j].colWidth);

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

				if (NO_ERROR != (error = fWEWrapper.openDctnry (txnId, dctnryStruct, false))) // @bug 5572 HDFS tmp file
				{
					WErrorCodes ec;
					err = ec.errorString(error);
					rc = error;
					return rc;
				}

				ColExtsInfo aColExtsInfo = aTableMetaData->getColExtsInfo(dctnryStruct.dctnryOid);
				ColExtsInfo::iterator it = aColExtsInfo.begin();
				while (it != aColExtsInfo.end())
				{
					if ((it->dbRoot == dctnryStruct.fColDbRoot) && (it->partNum == dctnryStruct.fColPartition) && (it->segNum == dctnryStruct.fColSegment))
						break;
					it++;
				}

				if (it == aColExtsInfo.end()) //add this one to the list
				{
					ColExtInfo aExt;
					aExt.dbRoot = dctnryStruct.fColDbRoot;
					aExt.partNum = dctnryStruct.fColPartition;
					aExt.segNum = dctnryStruct.fColSegment;
					aExt.compType =dctnryStruct.fCompressionType;
					aExt.isDict = true;
					aColExtsInfo.push_back(aExt);
				}
				aTableMetaData->setColExtsInfo(dctnryStruct.dctnryOid, aColExtsInfo);


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
							case CalpontSystemCatalog::DECIMAL:
							case CalpontSystemCatalog::UDECIMAL:
							{
								// decimal width > 8 cannot be stored in an integer
								if (fetchColColwidths[fetchColPos] > 8)
								{
									value = row.getStringField(fetchColPos);
									unsigned i = strlen(value.c_str());
									value = value.substr(0, i);
									break;
								}
								// else
								//     fall through to integer cases
							}
							case CalpontSystemCatalog::BIGINT:
							case CalpontSystemCatalog::UBIGINT:
							case CalpontSystemCatalog::INT:
							case CalpontSystemCatalog::UINT:
							case CalpontSystemCatalog::SMALLINT:
							case CalpontSystemCatalog::USMALLINT:
							case CalpontSystemCatalog::TINYINT:
							case CalpontSystemCatalog::UTINYINT:
							{
								{
									intColVal = row.getIntField(fetchColPos);
									if (fetchColTypes[fetchColPos] == CalpontSystemCatalog::UDECIMAL
										&& intColVal < 0)
									{
										intColVal = 0;
									}

									if (fetchColScales[fetchColPos] <= 0)
									{
										ostringstream os;
										if (isUnsigned(fetchColTypes[fetchColPos]))
											os << static_cast<uint64_t>(intColVal);
										else
											os << intColVal;
										value = os.str();
									}
									else
									{
										const int ctmp_size = 65+1+1+1;
										char ctmp[ctmp_size] = {0};
										DataConvert::decimalToString(
											intColVal, fetchColScales[fetchColPos],
											ctmp, ctmp_size, fetchColTypes[fetchColPos]);
										value = ctmp;  // null termination by decimalToString
									}
								}
								break;
							}
							//In this case, we're trying to load a double output column with float data. This is the
							// case when you do sum(floatcol), e.g.
							case CalpontSystemCatalog::FLOAT:
							case CalpontSystemCatalog::UFLOAT:
							{
								float dl = row.getFloatField(fetchColPos);
								if (dl == std::numeric_limits<float>::infinity())
									continue;
								if (fetchColTypes[fetchColPos] == CalpontSystemCatalog::UFLOAT && dl < 0.0)
								{
									dl = 0.0;
								}
								ostringstream os;
								//@Bug 3350 fix the precision.
								os << setprecision(7) << dl;
								value = os.str();
								break;
							}
							case CalpontSystemCatalog::DOUBLE:
							case CalpontSystemCatalog::UDOUBLE:
							{
								double dl = row.getDoubleField(fetchColPos);
								if (dl == std::numeric_limits<double>::infinity())
									continue;
								if (fetchColTypes[fetchColPos] == CalpontSystemCatalog::UDOUBLE && dl < 0.0)
								{
									dl = 0.0;
								}
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

						uint32_t funcScale = columnsUpdated[j]->get_funcScale();
						if (funcScale != 0)
						{
							string::size_type pos = value.find_first_of("."); //decimal point
							if ( pos >= value.length() )
								value.insert(value.length(),".");
							//padding 0 if needed
							pos = value.find_first_of(".");
							uint32_t digitsAfterPoint = value.length() - pos - 1;
							if (digitsAfterPoint < funcScale)
							{
								for (uint32_t i=0; i < (funcScale-digitsAfterPoint); i++)
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
							rc = error;
							WErrorCodes ec;
							err = ec.errorString(error);
							return rc;
						}

						colTuple.data = dctTuple.token;
						colTupleList.push_back (colTuple);
					}
					if (colType.compressionType == 0)
						fWEWrapper.closeDctnry(txnId, colType.compressionType, true);
					else
						fWEWrapper.closeDctnry(txnId, colType.compressionType, false);
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
								rc = error;
								WErrorCodes ec;
								err = ec.errorString(error);
								return rc;
							}
							colTuple.data = dctTuple.token;
							if (colType.compressionType == 0)
								fWEWrapper.closeDctnry(txnId, colType.compressionType, true);
							else
								fWEWrapper.closeDctnry(txnId, colType.compressionType, false); // Constant only need to tokenize once.
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
							rc = error;
							WErrorCodes ec;
							err = ec.errorString(error);
							return rc;
						}
						colTuple.data = dctTuple.token;

						if(colType.compressionType == 0)
							fWEWrapper.closeDctnry(txnId, colType.compressionType, true);
						else
							fWEWrapper.closeDctnry(txnId, colType.compressionType, false); // Constant only need to tokenize once.
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
								case CalpontSystemCatalog::DECIMAL:
								case CalpontSystemCatalog::UDECIMAL:
								{
									// decimal width > 8 cannot be stored in an integer
									if (fetchColColwidths[fetchColPos] > 8)
									{
										value = row.getStringField(fetchColPos);
										unsigned i = strlen(value.c_str());
										value = value.substr(0, i);
										break;
									}
									// else
									//     fall through to integer cases
								}
								case CalpontSystemCatalog::BIGINT:
								case CalpontSystemCatalog::UBIGINT:
								case CalpontSystemCatalog::INT:
								case CalpontSystemCatalog::UINT:
								case CalpontSystemCatalog::SMALLINT:
								case CalpontSystemCatalog::USMALLINT:
								case CalpontSystemCatalog::TINYINT:
								case CalpontSystemCatalog::UTINYINT:
								{
									{
										intColVal = row.getIntField(fetchColPos);
										if (fetchColTypes[fetchColPos] ==
													CalpontSystemCatalog::UDECIMAL
											&& intColVal < 0)
										{
											intColVal = 0;
										}

										if (fetchColScales[fetchColPos] <= 0)
										{
											ostringstream os;
											if (isUnsigned(fetchColTypes[fetchColPos]))
												os << static_cast<uint64_t>(intColVal);
											else
												os << intColVal;
											value = os.str();
										}
										else
										{
											const int ctmp_size = 65+1+1+1;
											char ctmp[ctmp_size] = {0};
											DataConvert::decimalToString(
												intColVal, fetchColScales[fetchColPos],
												ctmp, ctmp_size, fetchColTypes[fetchColPos]);
											value = ctmp;  // null termination by decimalToString
										}
									}
									break;
								}
								//In this case, we're trying to load a double output column with float data. This is the
								// case when you do sum(floatcol), e.g.
								case CalpontSystemCatalog::FLOAT:
								case CalpontSystemCatalog::UFLOAT:
								{
									float dl = row.getFloatField(fetchColPos);
									if (dl == std::numeric_limits<float>::infinity())
										continue;
									if (fetchColTypes[fetchColPos] == CalpontSystemCatalog::UFLOAT && dl < 0.0)
									{
										dl = 0.0;
									}
									ostringstream os;
									//@Bug 3350 fix the precision.
									os << setprecision(7) << dl;
									value = os.str();
									break;
								}
								case CalpontSystemCatalog::DOUBLE:
								case CalpontSystemCatalog::UDOUBLE:
								{
									double dl = row.getDoubleField(fetchColPos);
									if (dl == std::numeric_limits<double>::infinity())
										continue;
									if (fetchColTypes[fetchColPos] == CalpontSystemCatalog::UDOUBLE && dl < 0.0)
									{
										dl = 0.0;
									}
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

						uint32_t funcScale = columnsUpdated[j]->get_funcScale();
						if (funcScale != 0)
						{
							string::size_type pos = value.find_first_of("."); //decimal point
							if ( pos >= value.length() )
								value.insert(value.length(),".");
							//padding 0 if needed
							pos = value.find_first_of(".");
							uint32_t digitsAfterPoint = value.length() - pos -1;
							if (digitsAfterPoint < funcScale)
							{
								for (uint32_t i=0; i < (funcScale-digitsAfterPoint); i++)
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

							try
							{
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
							try
							{
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
						try
						{
							nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
							fDbrm.startAISequence(oid, nextVal, colType.colWidth, colType.colDataType);
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
						try
						{
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
							try
							{
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
							try
							{
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
						try
						{
							datavalue = DataConvert::convertColumnData(colType, inData, pushWarn, isNull, false, true);
						}
						catch (exception& ex)
						{
							//@Bug 2624. Error out on conversion failure
							rc = 1;
							cout << ex.what() << endl;
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
		error = fWEWrapper.updateColumnRecs(txnId, colStructList, colValueList,  rowIDLists, tableRO.objnum);
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

uint8_t WE_DMLCommandProc::processFlushFiles(messageqcpp::ByteStream& bs, std::string & err)
{
	uint8_t rc = 0;
	uint32_t flushCode, txnId, tableOid;
	int error;
	bs >> flushCode;
	bs >> txnId;
	bs >> tableOid;
	std::map<uint32_t,uint32_t> oids;
	CalpontSystemCatalog::TableName aTableName;
	CalpontSystemCatalog::RIDList ridList;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
	CalpontSystemCatalog::makeCalpontSystemCatalog(txnId);
   // execplan::CalpontSystemCatalog::ColType colType;
	CalpontSystemCatalog::DictOIDList dictOids;

	if (tableOid >= 3000)
	{
		try
		{
			aTableName =  systemCatalogPtr->tableName(tableOid);
		}
		catch ( ... )
		{
			err = "Systemcatalog error for tableoid " + tableOid;
			rc = 1;
			return rc;
		}

		dictOids = systemCatalogPtr->dictOIDs(aTableName);
		for (unsigned i=0; i < dictOids.size(); i++)
		{
			oids[dictOids[i].dictOID] = dictOids[i].dictOID;
		}

		//if (dictOids.size() > 0)
		//	colType = systemCatalogPtr->colTypeDct(dictOids[0].dictOID);
	}


	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	vector<LBID_t> lbidList;
	if (idbdatafile::IDBPolicy::useHdfs())
	{
		//save the extent info to mark them invalid, after flush, the meta file will be gone.
		std::tr1::unordered_map<TxnID, SP_TxnLBIDRec_t>::iterator mapIter;
		std::tr1::unordered_map<TxnID, SP_TxnLBIDRec_t>  m_txnLBIDMap = fWEWrapper.getTxnMap();
		try
		{
			mapIter = m_txnLBIDMap.find(txnId);
			if (mapIter != m_txnLBIDMap.end())
			{
				SP_TxnLBIDRec_t spTxnLBIDRec = (*mapIter).second;
				std::tr1::unordered_map<BRM::LBID_t, uint32_t> ::iterator listIter = spTxnLBIDRec->m_LBIDMap.begin();
				while (listIter != spTxnLBIDRec->m_LBIDMap.end())
				{
					lbidList.push_back(listIter->first);
					listIter++;
				}
			}
		}
		catch(...) {}
	}

	error = fWEWrapper.flushDataFiles(flushCode, txnId, oids);

//No need to close files, flushDataFile will close them.
	//if (((colType.compressionType > 0 ) && (dictOids.size() > 0)) || (idbdatafile::IDBPolicy::useHdfs()))
	//	fWEWrapper.closeDctnry(txnId, colType.compressionType, true);
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
	TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);
	ColsExtsInfoMap colsExtsInfoMap = aTbaleMetaData->getColsExtsInfoMap();
	ColsExtsInfoMap::iterator it = colsExtsInfoMap.begin();
	ColExtsInfo::iterator aIt;
	std::vector<BRM::FileInfo> files;
	std::vector<BRM::OID_t>  oidsToFlush;
	BRM::FileInfo aFile;
	while (it != colsExtsInfoMap.end())
	{
		aIt = (it->second).begin();
		aFile.oid = it->first;
		oidsToFlush.push_back(aFile.oid);
		while (aIt != (it->second).end())
		{
			aFile.partitionNum = aIt->partNum;
			aFile.dbRoot =aIt->dbRoot;
			aFile.segmentNum = aIt->segNum;
			aFile.compType = aIt->compType;
			files.push_back(aFile);
			//cout <<"Added to files oid:dbroot:part:seg:compType = " << aFile.oid<<":"<<aFile.dbRoot<<":"<<aFile.partitionNum<<":"<<aFile.segmentNum
			//<<":"<<aFile.compType <<endl;
			aIt++;
		}
		it++;
	}

	if (idbdatafile::IDBPolicy::useHdfs())
	{
		rc = fWEWrapper.confirmTransaction(txnId);
		//@Bug 5700. Purge FD cache after file swap
		cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID());
		cacheutils::flushOIDsFromCache(oidsToFlush);
		fDbrm.invalidateUncommittedExtentLBIDs(0, &lbidList);
	}
	//cout << "Purged files.size:moduleId = " << files.size() << ":"<<Config::getLocalModuleID() << endl;
	//if (idbdatafile::IDBPolicy::useHdfs())
	//		cacheutils::dropPrimProcFdCache();
	TableMetaData::removeTableMetaData(tableOid);
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
	fWEWrapper.setIsInsert(false);
	fWEWrapper.setBulkFlag(false);
	fWEWrapper.setTransId(txnId);
	if (!rowGroups[txnId]) //meta data
	{
		rowGroups[txnId] = new rowgroup::RowGroup();
		rowGroups[txnId]->deserialize(bs);
		//If hdfs, call chunkmanager to set up

		rc = fWEWrapper.startTransaction(txnId);
		if (rc != NO_ERROR)
		{
			WErrorCodes ec;
			err = ec.errorString(rc);
		}
		return rc;
	}
	rowgroup::RGData rgData;
	rgData.deserialize(bs);
	rowGroups[txnId]->setData(&rgData);
	//rowGroups[txnId]->setData(const_cast<uint8_t*>(bs.buf()));
	//get row ids
	rowgroup::Row row;
	rowGroups[txnId]->initRow(&row);
	WriteEngine::RIDList  rowIDList;
	CalpontSystemCatalog::RID rid;
	uint32_t rowsThisRowgroup = rowGroups[txnId]->getRowCount();
	uint16_t dbRoot, segment, blockNum;
	uint32_t partition;
	uint8_t extentNum;
	CalpontSystemCatalog::TableName aTableName;
	aTableName.schema = schema;
	aTableName.table = tableName;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
	CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	CalpontSystemCatalog::ROPair roPair;

	CalpontSystemCatalog::RIDList tableRidList;

	try
	{
		roPair = systemCatalogPtr->tableRID( aTableName);
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
	boost::scoped_array<uint32_t> colWidth(new uint32_t[row.getColumnCount()]);

	// initialize
	for (uint32_t j = 0; j < row.getColumnCount(); j++)
	{
		preBlkNums[j] = -1;
		colWidth[j] = (row.getColumnWidth(j) >= 8 ? 8 : row.getColumnWidth(j));
	}

	//Get the file information from rowgroup
	dbRoot = rowGroups[txnId]->getDBRoot();
	rowGroups[txnId]->getLocation(&partition, &segment, &extentNum, &blockNum);
	WriteEngine::ColStructList colStructList;
	WriteEngine::ColStruct colStruct;
	colStruct.fColPartition = partition;
	colStruct.fColSegment = segment;
	colStruct.fColDbRoot = dbRoot;

	for (unsigned i = 0; i < rowsThisRowgroup; i++)
	{
		rowGroups[txnId]->getRow(i, &row);
		rid = row.getRid();
		relativeRID = rid - rowGroups[txnId]->getBaseRid();
		rid = relativeRID;
		convertToRelativeRid (rid, extentNum, blockNum);
		rowIDList.push_back(rid);

		// populate stats.blocksChanged
		for (uint32_t j = 0; j < row.getColumnCount(); j++)
		{
			if ((int)(relativeRID/(BYTE_PER_BLOCK/colWidth[j])) > preBlkNums[j])
			{
				blocksChanged++;
				preBlkNums[j] = relativeRID/(BYTE_PER_BLOCK/colWidth[j]);
			}
		}
	}

	try
	{
		for (unsigned i = 0; i < tableRidList.size(); i++)
		{
			CalpontSystemCatalog::ColType colType;
			colType = systemCatalogPtr->colType( tableRidList[i].objnum );
			colStruct.dataOid = tableRidList[i].objnum;
			colStruct.tokenFlag = false;
			colStruct.fCompressionType = colType.compressionType;
			if (colType.colWidth > 8)  //token
			{
				colStruct.colWidth = 8;
				colStruct.tokenFlag = true;
			}
			else
			{
				colStruct.colWidth = colType.colWidth;
			}

			colStruct.colDataType = colType.colDataType;

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

	error = fWEWrapper.deleteRow( txnId, colExtentsStruct, colOldValueList, ridLists, roPair.objnum );
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
	uint32_t	tableOID;

	try
	{
		bs >> tableOID;
		//std::cout << ": tableOID-" << tableOID << std::endl;

		BulkRollbackMgr::deleteMetaFile( tableOID );
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

	try
	{
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
			boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
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

	try
	{
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
		uint16_t dbRoot;
		std::map<uint32_t,uint32_t> oids;
		//std::vector<BRM::OID_t>  oidsToFlush;
		oids[columnOid] = columnOid;
		//oidsToFlush.push_back(columnOid);
		BRM::OID_t oid = 1021;
		fDbrm.getSysCatDBRoot(oid, dbRoot);
		fWEWrapper.setTransId(sessionID);
		fWEWrapper.setBulkFlag(false);
		fWEWrapper.startTransaction(sessionID);
		//cout << "updateSyscolumnNextval startTransaction id " << sessionID << endl;
		rc = fWEWrapper.updateNextValue(sessionID, columnOid, nextVal, sessionID, dbRoot);
		if (rc != 0)
		{
			err = "Error in WE::updateNextValue";
			rc =1;
		}
		if (idbdatafile::IDBPolicy::useHdfs())
		{
			cout << "updateSyscolumnNextval flushDataFiles  " <<  endl;
			int rc1 = fWEWrapper.flushDataFiles(rc, sessionID, oids);

			if ((rc == 0) && ( rc1 == 0))
			{
				cout << "updateSyscolumnNextval confirmTransaction rc =0  " << endl;
				rc1 = fWEWrapper.confirmTransaction(sessionID);
				cout << "updateSyscolumnNextval confirmTransaction return code is  " << rc1 << endl;
				if ( rc1 == NO_ERROR)
					rc1 = fWEWrapper.endTransaction(sessionID, true);
				else
					fWEWrapper.endTransaction(sessionID, false);
			}
			else
			{
				cout << "updateSyscolumnNextval endTransaction with error  " << endl;
				fWEWrapper.endTransaction(sessionID, false);

			}

			if ( rc == NO_ERROR)
				rc = rc1;
		}

		//if (idbdatafile::IDBPolicy::useHdfs())
		//	cacheutils::flushOIDsFromCache(oidsToFlush);
		return rc;
	}

uint8_t WE_DMLCommandProc::processPurgeFDCache(ByteStream& bs, std::string & err)
{
	int rc = 0;
	uint32_t tableOid;
	bs >> tableOid;
	TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);
	ColsExtsInfoMap colsExtsInfoMap = aTbaleMetaData->getColsExtsInfoMap();
	ColsExtsInfoMap::iterator it = colsExtsInfoMap.begin();
	ColExtsInfo::iterator aIt;
	std::vector<BRM::FileInfo> files;
	BRM::FileInfo aFile;
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
			//cout <<"Added to files oid:dbroot:part:seg:compType = " << aFile.oid<<":"<<aFile.dbRoot<<":"<<aFile.partitionNum<<":"<<aFile.segmentNum
			//<<":"<<aFile.compType <<endl;
			aIt++;
		}
		it++;
	}

	if ((idbdatafile::IDBPolicy::useHdfs()) && (files.size()>0) )
		cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID());
	TableMetaData::removeTableMetaData(tableOid);
	return rc;
}

uint8_t WE_DMLCommandProc::processFixRows(messageqcpp::ByteStream& bs, 
	                                       std::string & err, 
	                                       ByteStream::quadbyte & PMId)
{
	uint8_t rc = 0;
	//cout << " In processFixRows" << endl;
	uint32_t tmp32;
	uint64_t sessionID;
	uint16_t dbRoot, segment;
	uint32_t partition;
	string schema, tableName;
	TxnID txnId;
	uint8_t tmp8;
	bool firstBatch = false;
	WriteEngine::RIDList  rowIDList;
	bs >> PMId;
	bs >> tmp8;
	firstBatch = (tmp8 != 0);
	bs >> sessionID;
	bs >> tmp32;
	txnId = tmp32;
	
	bs >> schema;
	bs >> tableName;
	bs >> dbRoot;
	bs >> partition;
	bs >> segment;
	
	deserializeInlineVector(bs, rowIDList);
	
	//Need to identify whether this is the first batch to start transaction.
	if (firstBatch)
	{
		rc = fWEWrapper.startTransaction(txnId);
		if (rc != NO_ERROR)
		{
			WErrorCodes ec;
			err = ec.errorString(rc);
			return rc;
		}
		fWEWrapper.setIsInsert(false);
		fWEWrapper.setBulkFlag(false);
		fWEWrapper.setTransId(txnId);
		fWEWrapper.setFixFlag(true);
	}
	

	CalpontSystemCatalog::TableName aTableName;	
	aTableName.schema = schema;
	aTableName.table = tableName; 
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
			CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	
	CalpontSystemCatalog::ROPair roPair;
		
	CalpontSystemCatalog::RIDList tableRidList;

	try {
		roPair = systemCatalogPtr->tableRID( aTableName);
		tableRidList = systemCatalogPtr->columnRIDs(aTableName, true);
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
			
	}
	
	WriteEngine::ColStructList colStructList;	
	WriteEngine::ColStruct colStruct;
	WriteEngine::DctnryStructList dctnryStructList;
/*	colStruct.fColPartition = partition;
	colStruct.fColSegment = segment;
	colStruct.fColDbRoot = dbRoot; */
	colStruct.fColPartition = 0;
	colStruct.fColSegment = 0;
	colStruct.fColDbRoot = 3;
	//should we always scan dictionary store files?
	
	try {
	  for (unsigned i = 0; i < tableRidList.size(); i++)	
	  {
		CalpontSystemCatalog::ColType colType;
        colType = systemCatalogPtr->colType( tableRidList[i].objnum );
		colStruct.dataOid = tableRidList[i].objnum;
        colStruct.tokenFlag = false;
		colStruct.fCompressionType = colType.compressionType;
		WriteEngine::DctnryStruct dctnryStruct;
		dctnryStruct.fColDbRoot = colStruct.fColDbRoot;
		dctnryStruct.fColPartition = colStruct.fColPartition;
		dctnryStruct.fColSegment = colStruct.fColSegment;
		dctnryStruct.fCompressionType = colStruct.fCompressionType;
		dctnryStruct.dctnryOid = 0;
        if (colType.colWidth > 8)         //token
        {
			colStruct.colWidth = 8;
			colStruct.tokenFlag = true;
			dctnryStruct.dctnryOid = colType.ddn.dictOID;
        }
        else
        {
          colStruct.colWidth = colType.colWidth;
        }

        colStruct.colDataType = colType.colDataType;

        colStructList.push_back( colStruct );
        dctnryStructList.push_back(dctnryStruct);
	  }
	}
	catch (exception& ex)
	{
		err = ex.what();
		rc = 1;
		return rc;
			
	}
		
	int error = 0;
	try {
		error = fWEWrapper.deleteBadRows( txnId, colStructList, rowIDList, dctnryStructList);
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
	}	
	catch (std::exception& ex)
	{
		err = ex.what();
		rc = 1;
	}
	//cout << "WES return rc " << (int)rc << " with msg " << err << endl;
	return rc;
}

uint8_t WE_DMLCommandProc::processEndTransaction(ByteStream& bs, std::string & err)
{
	int rc = 0;
	ByteStream::byte tmp8;
	bool success;
	uint32_t txnid;
	bs >> txnid;
	bs >> tmp8;
	success = (tmp8 != 0);
	rc = fWEWrapper.endTransaction(txnid, success);
	if (rc != NO_ERROR)
	{
		WErrorCodes ec;
		err = ec.errorString(rc);
	}
	return rc;

}
//------------------------------------------------------------------------------
// Validates the correctness of the current HWMs for this table.
// The HWMs for all the 1 byte columns should be identical.  Same goes
// for all the 2 byte columns, etc.  The 2 byte column HWMs should be
// "roughly" (but not necessarily exactly) twice that of a 1 byte column.
// Same goes for the 4 byte column HWMs vs their 2 byte counterparts, etc.
// ridList - columns oids to be used to get column width on to use with validation.
// segFileInfo - Vector of File objects carrying current DBRoot, partition,
//            HWM, etc to be validated for the columns belonging to jobTable.
// stage    - Current stage we are validating.  "Starting" or "Ending".
//------------------------------------------------------------------------------
int WE_DMLCommandProc::validateColumnHWMs(
    CalpontSystemCatalog::RIDList& ridList,
    boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr,
    const std::vector<DBRootExtentInfo>& segFileInfo,
    const char* stage )
{
    int rc = NO_ERROR;
    if ((!fIsFirstBatchPm) && (strcmp(stage,"Starting") == 0))
		return rc;
    // Used to track first 1-byte, 2-byte, 4-byte, and 8-byte columns in table
    int byte1First = -1;
    int byte2First = -1;
    int byte4First = -1;
    int byte8First = -1;

    // Make sure the HWMs for all 1-byte columns match; same for all 2-byte,
    // 4-byte, and 8-byte columns as well.
    CalpontSystemCatalog::ColType colType;
    Convertor convertor;
    for (unsigned k=0; k<segFileInfo.size(); k++)
    {
        int k1 = 0;

        // Find out column width
        colType = systemCatalogPtr->colType(ridList[k].objnum);
        colType.colWidth = convertor.getCorrectRowWidth(colType.colDataType, colType.colWidth);
     
        // Find the first 1-byte, 2-byte, 4-byte, and 8-byte columns.
        // Use those as our reference HWM for the respective column widths.
        switch ( colType.colWidth )
        {
            case 1:
            {
                if (byte1First == -1)
                    byte1First = k;
                k1 = byte1First;
                break;
            }
            case 2:
            {
                if (byte2First == -1)
                    byte2First = k;
                k1 = byte2First;
                break;
            }
            case 4:
            {
                if (byte4First == -1)
                    byte4First = k;
                k1 = byte4First;
                break;
            }
            case 8:
            default:
            {
                if (byte8First == -1)
                    byte8First = k;
                k1 = byte8First;
                break;
            }
        } // end of switch based on column width (1,2,4, or 8)

//std::cout << "dbg: comparing0 " << stage << " refcol-" << k1 <<
//  "; wid-" << jobColK1.width << "; hwm-" << segFileInfo[k1].fLocalHwm <<
//  " <to> col-" << k <<
//  "; wid-" << jobColK.width << " ; hwm-"<<segFileInfo[k].fLocalHwm<<std::endl;

        // Validate that the HWM for this column (k) matches that of the
        // corresponding reference column with the same width.
        if ((segFileInfo[k1].fDbRoot    != segFileInfo[k].fDbRoot)    ||
            (segFileInfo[k1].fPartition != segFileInfo[k].fPartition) ||
            (segFileInfo[k1].fSegment   != segFileInfo[k].fSegment)   ||
            (segFileInfo[k1].fLocalHwm  != segFileInfo[k].fLocalHwm))
        {
			CalpontSystemCatalog::ColType colType2;
			colType2 = systemCatalogPtr->colType(ridList[k1].objnum);
            ostringstream oss;
            oss << stage << " HWMs do not match for"
                " OID1-"       << ridList[k1].objnum              <<
                "; DBRoot-"    << segFileInfo[k1].fDbRoot      <<
                "; partition-" << segFileInfo[k1].fPartition   <<
                "; segment-"   << segFileInfo[k1].fSegment     <<
                "; hwm-"       << segFileInfo[k1].fLocalHwm    <<
                "; width-"     << colType2.colWidth << ':'<<std::endl<<
                " and OID2-"   << ridList[k].objnum               <<
                "; DBRoot-"    << segFileInfo[k].fDbRoot       <<
                "; partition-" << segFileInfo[k].fPartition    <<
                "; segment-"   << segFileInfo[k].fSegment      <<
                "; hwm-"       << segFileInfo[k].fLocalHwm     <<
                "; width-"     << colType.colWidth;
            fLog.logMsg( oss.str(), ERR_UNKNOWN, MSGLVL_ERROR );
            return ERR_BRM_HWMS_NOT_EQUAL;
        }

        // HWM DBRoot, partition, and segment number should match for all
        // columns; so compare DBRoot, part#, and seg# with first column.
        if ((segFileInfo[0].fDbRoot    != segFileInfo[k].fDbRoot)    ||
            (segFileInfo[0].fPartition != segFileInfo[k].fPartition) ||
            (segFileInfo[0].fSegment   != segFileInfo[k].fSegment))
        {
			CalpontSystemCatalog::ColType colType2;
			colType2 = systemCatalogPtr->colType(ridList[0].objnum);
            ostringstream oss;
            oss << stage << " HWM DBRoot,Part#, or Seg# do not match for"
                " OID1-"       << ridList[0].objnum               <<
                "; DBRoot-"    << segFileInfo[0].fDbRoot       <<
                "; partition-" << segFileInfo[0].fPartition    <<
                "; segment-"   << segFileInfo[0].fSegment      <<
                "; hwm-"       << segFileInfo[0].fLocalHwm     <<
                "; width-"     << colType2.colWidth << ':'<<std::endl<<
                " and OID2-"   << ridList[k].objnum               <<
                "; DBRoot-"    << segFileInfo[k].fDbRoot       <<
                "; partition-" << segFileInfo[k].fPartition    <<
                "; segment-"   << segFileInfo[k].fSegment      <<
                "; hwm-"       << segFileInfo[k].fLocalHwm     <<
                "; width-"     << colType.colWidth;
            fLog.logMsg( oss.str(), ERR_UNKNOWN, MSGLVL_ERROR );
            return ERR_BRM_HWMS_NOT_EQUAL;
        }
    } // end of loop to compare all 1-byte HWMs, 2-byte HWMs, etc.

    // Validate/compare HWM for 1-byte column in relation to 2-byte column, etc.
    // Without knowing the exact row count, we can't extrapolate the exact HWM
    // for the wider column, but we can narrow it down to an expected range.
    int refCol = 0;
    int colIdx = 0;

    // Validate/compare HWMs given a 1-byte column as a starting point
    if (byte1First >= 0)
    {
        refCol = byte1First;

        if (byte2First >= 0)
        {
            HWM hwmLo = segFileInfo[byte1First].fLocalHwm * 2;
            HWM hwmHi = hwmLo + 1;
            if ((segFileInfo[byte2First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte2First].fLocalHwm > hwmHi))
            {
                colIdx = byte2First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
        if (byte4First >= 0)
        {
            HWM hwmLo = segFileInfo[byte1First].fLocalHwm * 4;
            HWM hwmHi = hwmLo + 3;
            if ((segFileInfo[byte4First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte4First].fLocalHwm > hwmHi))
            {
                colIdx = byte4First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
        if (byte8First >= 0)
        {
            HWM hwmLo = segFileInfo[byte1First].fLocalHwm * 8;
            HWM hwmHi = hwmLo + 7;
            if ((segFileInfo[byte8First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte8First].fLocalHwm > hwmHi))
            {
                colIdx = byte8First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
    }

    // Validate/compare HWMs given a 2-byte column as a starting point
    if (byte2First >= 0)
    {
        refCol = byte2First;

        if (byte4First >= 0)
        {
            HWM hwmLo = segFileInfo[byte2First].fLocalHwm * 2;
            HWM hwmHi = hwmLo + 1;
            if ((segFileInfo[byte4First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte4First].fLocalHwm > hwmHi))
            {
                colIdx = byte4First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
        if (byte8First >= 0)
        {
            HWM hwmLo = segFileInfo[byte2First].fLocalHwm * 4;
            HWM hwmHi = hwmLo + 3;
            if ((segFileInfo[byte8First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte8First].fLocalHwm > hwmHi))
            {
                colIdx = byte8First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
    }

    // Validate/compare HWMs given a 4-byte column as a starting point
    if (byte4First >= 0)
    {
        refCol = byte4First;

        if (byte8First >= 0)
        {
            HWM hwmLo = segFileInfo[byte4First].fLocalHwm * 2;
            HWM hwmHi = hwmLo + 1;
            if ((segFileInfo[byte8First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte8First].fLocalHwm > hwmHi))
            {
                colIdx = byte8First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
    }

// To avoid repeating this message 6 times in the preceding source code, we
// use the "dreaded" goto to branch to this single place for error handling.
errorCheck:
    if (rc != NO_ERROR)
    {
		CalpontSystemCatalog::ColType colType1, colType2;
		colType1 = systemCatalogPtr->colType(ridList[refCol].objnum);
		colType1.colWidth = convertor.getCorrectRowWidth(
				colType1.colDataType, colType1.colWidth);
     
		colType2 = systemCatalogPtr->colType(ridList[colIdx].objnum);
		colType2.colWidth = convertor.getCorrectRowWidth(
				colType2.colDataType, colType2.colWidth);
     
        ostringstream oss;
        oss << stage << " HWMs are not in sync for"
            " OID1-"       << ridList[refCol].objnum                <<
            "; DBRoot-"    << segFileInfo[refCol].fDbRoot      <<
            "; partition-" << segFileInfo[refCol].fPartition   <<
            "; segment-"   << segFileInfo[refCol].fSegment     <<
            "; hwm-"       << segFileInfo[refCol].fLocalHwm    <<
            "; width-"     << colType1.colWidth<< ':'<<std::endl<<
            " and OID2-"   << ridList[colIdx].objnum                 <<
            "; DBRoot-"    << segFileInfo[colIdx].fDbRoot      <<
            "; partition-" << segFileInfo[colIdx].fPartition   <<
            "; segment-"   << segFileInfo[colIdx].fSegment     <<
            "; hwm-"       << segFileInfo[colIdx].fLocalHwm    <<
            "; width-"     << colType2.colWidth;
        fLog.logMsg( oss.str(), ERR_UNKNOWN, MSGLVL_ERROR );
    }

    return rc;
}
}
