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

//   $Id: updatepackageprocessor.cpp 8797 2012-08-08 14:28:05Z chao $

#include <iostream>
#include <fstream>
#include <ctype.h>
#include <string>
//#define NDEBUG
#include <cassert>
#include <map>
using namespace std;
#define UPDATEPKGPROC_DLLEXPORT
#include "updatepackageprocessor.h"
#undef UPDATEPKGPROC_DLLEXPORT
#include "writeengine.h"
#include "joblistfactory.h"
#include "messagelog.h"
#include "simplecolumn.h"
#include "sqllogger.h"
#include "stopwatch.h"
#include "dbrm.h"
#include "idberrorinfo.h"
#include "errorids.h"
#include "rowgroup.h"
#include "bytestream.h"
#include "calpontselectexecutionplan.h"
#include "autoincrementdata.h"
#include "columnresult.h"
using namespace WriteEngine;
using namespace dmlpackage;
using namespace execplan;
using namespace logging;
using namespace dataconvert;
using namespace joblist;
using namespace rowgroup;
using namespace messageqcpp;
using namespace BRM;

//#define PROFILE 1
namespace dmlpackageprocessor
{
	//valueListsByExtent valueListsMap;
DMLPackageProcessor::DMLResult
UpdatePackageProcessor::processPackage(dmlpackage::CalpontDMLPackage& cpackage)
{
    //cout << "inside UpdatePackageProcessor::processPackage" << endl;

    SUMMARY_INFO("UpdatePackageProcessor::processPackage");

    std::string err;
    DMLResult result;
    result.result = NO_ERROR;
	result.rowCount = 0;
	BRM::TxnID txnid;
    fSessionID = cpackage.get_SessionID();
	VoidValuesList colOldValuesList;
	joblist::SJLP jbl;
    VERBOSE_INFO("Processing Update DML Package...");
//#ifdef PROFILE
	//StopWatch timer;
//#endif
    try
    {
        // set-up the transaction
        txnid.id  = cpackage.get_TxnID();		
		txnid.valid = true;
		
        SQLLogger sqlLogger(cpackage.get_SQLStatement(), DMLLoggingId, fSessionID, txnid.id);

        // get the table object from the package
        DMLTable* tablePtr =  cpackage.get_Table();
        VERBOSE_INFO("The table name is:");
        VERBOSE_INFO(tablePtr->get_TableName());
        if (0 != tablePtr)
        {
            // get the row(s) from the table
            RowList rows = tablePtr->get_RowList();
            if (rows.size() == 0)
            {
                SUMMARY_INFO("No row to update!");
                return result;
            }
            std::string schemaName = tablePtr->get_SchemaName();
            
			//Check whether dictionary column(s) in updated columns.
			bool needLockTable = false;
			dmlpackage::ColumnList columns = rows[0]->get_ColumnList();
			CalpontSystemCatalog::TableColName tableColName;
			tableColName.schema = schemaName;
			tableColName.table = tablePtr->get_TableName();
			CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
			AutoincrementData *autoincrementData = AutoincrementData::makeAutoincrementData(fSessionID);
			systemCatalogPtr->identity(CalpontSystemCatalog::EC);
			systemCatalogPtr->sessionID(fSessionID);

			for (unsigned int j = 0; j < columns.size(); j++)
			{
				tableColName.column  = columns[j]->get_Name();
				CalpontSystemCatalog::OID oid = systemCatalogPtr->lookupOID(tableColName);

				CalpontSystemCatalog::ColType colType;
				colType = systemCatalogPtr->colType(oid);
				if ((isDictCol(colType)) || colType.autoincrement || (colType.compressionType != 0))
				{
					needLockTable = true;
					break;
				}
			}

			CalpontSystemCatalog::TableInfo tableInfo;
			execplan::CalpontSystemCatalog::ROPair roPair;
			int rc = 0;
			CalpontSystemCatalog::TableName tableName;
			if (needLockTable) //check table lock
			{
				tableName.schema = tablePtr->get_SchemaName();
				tableName.table = tablePtr->get_TableName();
				roPair = systemCatalogPtr->tableRID(tableName);
				tableInfo = systemCatalogPtr->tableInfo(tableName);

				u_int32_t  processID = 0;
				std::string  processName("DMLProc");
				int i = 0;
				rc = fSessionManager.setTableLock(roPair.objnum, fSessionID, processID, processName, true);

				if ( rc  == BRM::ERR_TABLE_LOCKED_ALREADY )
				{
					int waitPeriod = 10;
					int sleepTime = 100; // sleep 100 milliseconds between checks
					int numTries = 10;  // try 10 times per second
					waitPeriod = Config::getWaitPeriod();
            		numTries = 	waitPeriod * 10;
					struct timespec rm_ts;

					rm_ts.tv_sec = sleepTime/1000;
					rm_ts.tv_nsec = sleepTime%1000 *1000000;

            		for (; i < numTries; i++)
            		{
#ifdef _MSC_VER
						Sleep(rm_ts.tv_sec * 1000);
#else
						struct timespec abs_ts;
            			do
						{
							abs_ts.tv_sec = rm_ts.tv_sec;
							abs_ts.tv_nsec = rm_ts.tv_nsec;
						}
						while(nanosleep(&abs_ts,&rm_ts) < 0);
#endif

						rc = fSessionManager.setTableLock(roPair.objnum, fSessionID, processID, processName, true);

						if (rc == 0)
							break;
					}

					if (i >= numTries) //error out
					{
						result.result = UPDATE_ERROR;
						result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_TABLE_LOCKED));
						return result;
					}

				}
				else if ( rc  == BRM::ERR_FAILURE)
				{
					logging::Message::Args args;
					logging::Message message(1);
					args.add("Insert Failed due to BRM failure");
					result.result = UPDATE_ERROR;
					result.message = message;
					return result;
				}

			}

			//std::vector<dicStrValues> dicStrValCols;
			WriteEngine::ColValueList colValuesList;
			//WriteEngine::RIDList rowIDList;
			std::vector<WriteEngine::RIDList>  rowIDLists;
			ridsUpdateListsByExtent ridsListsMap;
			//WriteEngine::ColValueList colOldValuesList;

			bool done = false;
			bool firstCall = true;
			std::vector<extentInfo> extentsinfo;
			//get next value if the table has autoincrement column
			long long nextVal = 0;		
			long long originalNextVal = 0;
			if (tableInfo.tablewithautoincr == 1)
			{
				nextVal = autoincrementData->getNextValue(roPair.objnum);
				
				if (rc != 0)
				{
					err = "DML cannot get autoincrement value.";
					SUMMARY_INFO(err);
					throw std::runtime_error(err);
				}
				if (nextVal <= 0)
				{
					//get from systables
					nextVal= systemCatalogPtr->nextAutoIncrValue(tableName); 
				}
			}
			CalpontSystemCatalog::ColType  autoColType;	
			if (cpackage.get_IsFromCol())
			{

				valueListsByExtent valueLists;
				uint64_t rowsProcessed = 0;
				originalNextVal = nextVal;
				while (! done)
				{
					//timer.start("fixUpRowsValues");
					done = fixUpRowsValues(cpackage.get_SessionID(), cpackage, schemaName, tablePtr->get_TableName(), valueLists, firstCall, jbl, extentsinfo, ridsListsMap, result) ;
					//cout << "fixUpRowsValues return done = " << (done ? 1 : 0) << endl;
					//timer.stop("fixUpRowsValues");
					//@Bug 3184. Check the warning code, too
					if ((result.result != 0) && (result.result != DMLPackageProcessor::IDBRANGE_WARNING))
						throw std::runtime_error(result.message.msg());

					std::vector<std::string> colNameList;
					std::vector<CalpontSystemCatalog::ColType> colTypes;
	//				timer.start("updateRowsValues");
	//cout << "processing a batch" << endl;
					if (updateRowsValues(cpackage.get_SessionID(), txnid.id, schemaName, tablePtr->get_TableName(), rows, rowsProcessed, valueLists, result, ridsListsMap, nextVal, autoColType))
					{
						result.rowCount += rowsProcessed;
						//cout << "rows processed " << rowsProcessed << endl;
					}
					else
					{
						err = result.message.msg();
						throw std::logic_error(err);
					}
	//				timer.stop("updateRowsValues");
					extentsinfo.clear();
					rowsProcessed = 0;
					valueLists.clear();
					ridsListsMap.clear();
				}			
			}
			else
			{
				originalNextVal = nextVal;
				while (! done)
				{
					// handle the case that no filter is supplied and all rows must be updated
	//				timer.start("fixUpRows");
					done = fixUpRows(cpackage.get_SessionID(), cpackage, schemaName, tablePtr->get_TableName(), rows, firstCall, jbl, extentsinfo, ridsListsMap, result) ;
	//				timer.stop("fixUpRows");
					//@Bug 3184. Check the warning code, too
					if ((result.result != 0) && (result.result != DMLPackageProcessor::IDBRANGE_WARNING))
						throw std::runtime_error(result.message.msg());

					std::vector<std::string> colNameList;
					std::vector<CalpontSystemCatalog::ColType> colTypes;
					//timer.start("updateRows");
					if (updateRows(cpackage.get_SessionID(), txnid.id, schemaName, tablePtr->get_TableName(), rows, rowIDLists, colValuesList, colOldValuesList, result, colNameList, colTypes, extentsinfo, ridsListsMap, nextVal))
					{
						for (unsigned i = 0; i < rowIDLists.size(); i++)
						{
							result.rowCount += rowIDLists[i].size();
						}
					}

					else
					{
						err = result.message.msg();
						throw std::logic_error(err);
					}
					//timer.stop("updateRows");
					rowIDLists.clear();
					extentsinfo.clear();
					ridsListsMap.clear();
					clearVoidValuesList(colOldValuesList);
				}
			}
			// save the next value
			if ((tableInfo.tablewithautoincr == 1) && (originalNextVal < nextVal))
			{
				//validate the next value
				bool offByOne = false;
				if (nextVal > 0)
				{
					bool valid = validateNextValue(autoColType, nextVal, offByOne);
					if (valid && !offByOne)
					{
						autoincrementData->setNextValue(roPair.objnum, nextVal);
					}
					else if (!valid && offByOne)
					{
						autoincrementData->setNextValue(roPair.objnum, -1);
					}
					else //fail this statement
					{
						throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT));
					}
				}
				else
				{
					throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT));
				}		
			}
		}
		// Log the update statement.
		logging::logDML(cpackage.get_SessionID(), txnid.id, cpackage.get_SQLStatement(), cpackage.get_SchemaName());
    }
    catch (exception& ex)
    {
        cerr << "UpdatePackageProcessor::processPackage:" << ex.what() << endl;

        result.result = UPDATE_ERROR;
        result.message = Message(ex.what());
    }
    catch (...)
    {
        cerr << "UpdatePackageProcessor::processPackage: caught unknown exception!" << endl;
        logging::Message::Args args;
        logging::Message message(7);
        args.add("Update Failed: ");
        args.add("encountered unkown exception");
        args.add("");
        args.add("");
        message.format(args);

        result.result = UPDATE_ERROR;
        result.message = message;
    }
   //timer.finish();
	//@Bug 1886,2870 Flush VM cache only once per statement.
	//WriteEngineWrapper writeEngine;
	fWriteEngine.flushVMCache();
	std::map<u_int32_t,u_int32_t> oids;
	if (result.result == NO_ERROR || result.result == IDBRANGE_WARNING)
	{
		if (fWriteEngine.flushDataFiles(NO_ERROR, oids) != NO_ERROR)
		{
        	cerr << "UpdatePackageProcessor::processPackage: write data to disk failed" << endl;
			logging::Message::Args args;
			logging::Message message(7);
			args.add("Update Failed: ");
			args.add("error when writing data to disk");
			args.add("");
			args.add("");
			message.format(args);

			result.result = UPDATE_ERROR;
			result.message = message;
		}
	}
	else
	{
		//@Bug 4563. Always flush.
		fWriteEngine.flushDataFiles(NO_ERROR, oids);
	}

    VERBOSE_INFO("Finished Processing Update DML Package");
    return result;
	//FIXME: (in Windows, at least), when jbl goes out of scope here, if the above
	// call to fixUpRows() caused the BRM to go readonly, the dtor will hang. This
	// will cause the update stmt to hang in the client.
}

void UpdatePackageProcessor::clearVoidValuesList(VoidValuesList& valuesList)
{
    int nCols = valuesList.size();
    for(int i=0; i < nCols; i++)
    {
     	if (valuesList[i]!=NULL) free(valuesList[i]);
    }
    valuesList.clear();
}

bool UpdatePackageProcessor::updateRows(u_int32_t sessionID, CalpontSystemCatalog::SCN txnID,
	const std::string& schema, const std::string& table, const dmlpackage::RowList& rows,
	std::vector<WriteEngine::RIDList>& rowidLists, WriteEngine::ColValueList& colValuesList,
	std::vector<void *>& colOldValuesList, DMLResult& result, std::vector<std::string>& colNameList,
	std::vector<CalpontSystemCatalog::ColType>& colTypes, std::vector<extentInfo>& extentsinfo, 
	ridsUpdateListsByExtent& ridsListsMap, long long & nextVal)
{
    SUMMARY_INFO("UpdatePackageProcessor::updateRows");

    bool retval = true;
    //WriteEngine::WriteEngineWrapper writeEngine;
	std::vector<WriteEngine::ColStructList> colExtentsStruct;
	std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
    WriteEngine::ColTupleList aColList;
	WriteEngine::ColStructList colStructs;
	WriteEngine::DctnryStructList dctnryStructList;
    WriteEngine::DctnryValueList dctnryValueList;

    CalpontSystemCatalog::TableName tableName;
    CalpontSystemCatalog::TableColName tableColName;
    tableName.table = tableColName.table = table;
    tableName.schema = tableColName.schema = schema;

    CalpontSystemCatalog* systemCatalogPtr =
        CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);

    VERBOSE_INFO("Updating Row(s)...");
    dmlpackage::ColumnList columns = rows[0]->get_ColumnList();
    unsigned int numCols = columns.size();
    WriteEngine::ColTupleList rowList;
    WriteEngine::DctColTupleList dctRowList;
	//WriteEngine::ColTuple colList[numCols];
	std::vector<WriteEngine::ColTuple> colList;
	//WriteEngine::DctnryTuple  dctColList[numCols];
	std::vector<WriteEngine::DctnryTuple> dctColList;
    bool pushWarning = false;
	bool transactionWarning = false;
	std::vector<string> colNames;
    std::string tmpStr("");

	//Build colStruct and a row value
	for (unsigned int j = 0; j < columns.size(); j++)
    {

		VERBOSE_INFO("Looking up oid for:");
		VERBOSE_INFO(columns[j]->get_Name());

		tableColName.column  = columns[j]->get_Name();
		colNameList.push_back(tableColName.column);
		CalpontSystemCatalog::OID oid = systemCatalogPtr->lookupOID(tableColName);

		CalpontSystemCatalog::ColType colType;
		colType = systemCatalogPtr->colType(oid);

		CalpontSystemCatalog::ROPair roPair = systemCatalogPtr->columnRID(tableColName);
		colTypes.push_back(colType);
        WriteEngine::ColStruct colStruct;
       	WriteEngine::DctnryStruct dctnryStruct;
        colStruct.dataOid = roPair.objnum;
        colStruct.tokenFlag = false;
		colStruct.fCompressionType = colType.compressionType;
        if (isDictCol(colType))
        {
            colStruct.colWidth = 8;
            colStruct.tokenFlag = true;
        }
        else
        {
            colStruct.colWidth = colType.colWidth;
        }
			
		colStruct.colDataType = (WriteEngine::ColDataType)colType.colDataType;
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

		// convert the columns data from a string to the appropriate data type
        boost::any datavalue;

		const bool isNULL = columns[j]->get_isnull();
/*
		if (isNULL && (colType.constraintType == NOTNULL_CONSTRAINT))
		{
			retval = false;
			result.result = UPDATE_ERROR;
			Message::Args args;
			args.add(tableColName.column);
			result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_VIOLATE_NOT_NULL, args));
			return retval;
		}
*/
        if (isDictCol(colType))
        {
			//check data length
			tmpStr = columns[j]->get_Data();
			if (tmpStr.length() > (unsigned int)colType.colWidth)
			{
				tmpStr = tmpStr.substr(0, colType.colWidth);
				pushWarning = true;
			}

        }
        else
        {
			std::string inData(columns[j]->get_Data());
			
            try {
				datavalue = DataConvert::convertColumnData(colType, inData, pushWarning, isNULL);
            }
            catch (exception& ex)
    		{
				//@Bug 2624. Error out on conversion failure
        		cerr << "UpdatePackageProcessor::processPackage::updateRows" << ex.what() << endl;
				Message::Args args;
				args.add(string("'") + inData + string("'"));
        		result.result = UPDATE_ERROR;
        		result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERCAL_DATA, args));
				return false;
        	}
		}

        WriteEngine::ColTuple colTuple;
        colTuple.data = datavalue;

        //colList[j] = colTuple;
		colList.push_back(colTuple);
        WriteEngine::DctnryTuple dctnryTuple;
        memcpy(dctnryTuple.sigValue, tmpStr.c_str(), tmpStr.length());
        dctnryTuple.sigSize = tmpStr.length();
        dctnryTuple.isNull = isNULL;
        //dctColList[j] = dctnryTuple;
		dctColList.push_back(dctnryTuple);
		if (pushWarning)
		{
			transactionWarning = true;
			colNames.push_back(tableColName.column);
		}
    }
	if (transactionWarning)
    {
		result.result = IDBRANGE_WARNING;
		Message::Args args;
		string cols = "'" + colNames[0] + "'";

		for (unsigned i=1; i<colNames.size();i++)
		{
			cols = cols + ", " +  "'" + colNames[i] + "'";
		}
		args.add(cols);
		result.message = Message(IDBErrorInfo::instance()->errorMsg(WARN_DATA_TRUNC,args));
    }
	assert(colList.size() >= numCols);
	assert(dctColList.size() >= numCols);

    for (unsigned int n = 0; n < numCols; n++)
    {
		rowList.push_back(colList[n]);
        colValuesList.push_back(rowList);
		//@bug 1856
	    dctRowList.push_back(dctColList[n]);
        dctnryValueList.push_back (dctRowList);
		//@bug 2071
		rowList.clear();
		dctRowList.clear();
    }

	//build colExtentsStruct
	ridsUpdateListsByExtent::const_iterator itor;
	for (itor = ridsListsMap.begin(); itor != ridsListsMap.end(); itor++)
	{

		for (unsigned j=0; j < colStructs.size(); j++)
		{
			colStructs[j].fColPartition = (itor->first).partition;
			colStructs[j].fColSegment = (itor->first).segment;
			colStructs[j].fColDbRoot = (itor->first).dbRoot;
			dctnryStructList[j].fColPartition = (itor->first).partition;
			dctnryStructList[j].fColSegment = (itor->first).segment;
			dctnryStructList[j].fColDbRoot = (itor->first).dbRoot;
		}
		colExtentsStruct.push_back(colStructs);
		dctnryExtentsStruct.push_back(dctnryStructList);
		rowidLists.push_back(itor->second);
	}
	long long  rowCount = 0;
	for (unsigned i = 0; i < rowidLists.size(); i++)
	{
		rowCount += rowidLists[i].size();
	}

	retval = true;
	if (rowCount == 0)
		return retval;

    // call the write engine to update all the rows
    int error;
    //cout << "calling writeengine with rowidList size " << rowCount << endl; // << " and extentinfo = dbroot:segment  " <<extentinfo.dbRoot << ":" << extentinfo.segment << endl;
	if (NO_ERROR != (error = fWriteEngine.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList, rowidLists, dctnryExtentsStruct, dctnryValueList)))
    {
        retval = false;
        // build the logging message
        logging::Message::Args args;
        logging::Message message(7);
        args.add("Update failed on ");
        args.add(tableName.table);
        args.add(" table. ");
		WErrorCodes ec;
        args.add(ec.errorString(error));
        message.format(args);

        if (error == ERR_BRM_DEAD_LOCK)
        {
        	result.result = DEAD_LOCK_ERROR;
			result.message = message;
        }
		else if (error == ERR_BRM_VB_OVERFLOW)
		{
			result.result = VB_OVERFLOW_ERROR;
			result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_VERSIONBUFFER_OVERFLOW));
		}
        else
        {
        	result.result = UPDATE_ERROR;
			result.message = message;
        }
    }
	nextVal += rowCount;
    VERBOSE_INFO("Finished Updating Row(s)");

    return retval;
}

bool UpdatePackageProcessor::updateRowsValues(u_int32_t sessionID,
	execplan::CalpontSystemCatalog::SCN txnID, const std::string& schema, const std::string& table,
	const dmlpackage::RowList& rows, uint64_t & rowsProcessed, valueListsByExtent& valueLists,
	DMLResult& result, ridsUpdateListsByExtent& ridsListsMap, long long & nextVal,
	CalpontSystemCatalog::ColType & autoColType)
{
    SUMMARY_INFO("UpdatePackageProcessor::updateRowsValues");
//StopWatch timer;
    bool retval = true;
    //WriteEngine::WriteEngineWrapper writeEngine;
    WriteEngine::ColTupleList aColList;
	WriteEngine::ColStructList colStructList;
	WriteEngine::ColValueList colValueList;

	WriteEngine::DctnryStructList dctnryStructList;
    WriteEngine::DctnryValueList dctnryValueList;

    CalpontSystemCatalog::TableName tableName;
    CalpontSystemCatalog::TableColName tableColName;
    tableName.table = tableColName.table = table;
    tableName.schema = tableColName.schema = schema;

    CalpontSystemCatalog* systemCatalogPtr =
        CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);

    VERBOSE_INFO("Updating Row(s)...");
    dmlpackage::ColumnList columns = rows[0]->get_ColumnList();
    WriteEngine::ColTupleList rowList;
    bool pushWarning = false;
	bool transactionWarning = false;
	std::vector<string> colNames;
	bool isNull = false;
    std::string tmpStr("");
	WriteEngine::ColStruct colStruct;
	WriteEngine::DctnryStruct dctnryStruct;
	//Process one extent a time. Build colStruct and row values for each extent
	ridsUpdateListsByExtent::const_iterator itor;
	valueListsByExtent::const_iterator valueitor;
	boost::any datavalue;
	string inData;
	WriteEngine::ColTuple colTuple;
	int error = 0;
	valueitor = valueLists.begin();
	unsigned volInValues = 0;
	for (itor = ridsListsMap.begin(); itor != ridsListsMap.end(); itor++)
	{
		colValueList.clear();
		colStructList.clear();
		unsigned rowsThisExtent = itor->second.size();
		rowsProcessed += rowsThisExtent;
		colStruct.fColPartition = (itor->first).partition;
		colStruct.fColSegment = (itor->first).segment;
		colStruct.fColDbRoot = (itor->first).dbRoot;
		dctnryStruct.fColPartition = (itor->first).partition;
		dctnryStruct.fColSegment = (itor->first).segment;
		dctnryStruct.fColDbRoot = (itor->first).dbRoot;
		std::vector<ColValues> valuesThisExtent = valueitor->second; //vector of each column
		//timer.start("prepare values");
		for (unsigned int j = 0; j < columns.size(); j++)
		{
			WriteEngine::ColTupleList colTupleList;
			tableColName.column  = columns[j]->get_Name();
			CalpontSystemCatalog::OID oid = systemCatalogPtr->lookupOID(tableColName);
			CalpontSystemCatalog::ColType colType;
			colType = systemCatalogPtr->colType(oid);
			CalpontSystemCatalog::ROPair roPair = systemCatalogPtr->columnRID(tableColName);
			colStruct.dataOid = roPair.objnum;
			colStruct.colDataType = (WriteEngine::ColDataType)colType.colDataType;
			colStruct.tokenFlag = false;
			colStruct.fCompressionType = colType.compressionType;
			bool notStored = true;
			if (isDictCol(colType))
			{
				colStruct.colWidth = 8;
				colStruct.tokenFlag = true;
				dctnryStruct.dctnryOid = colType.ddn.dictOID;
				dctnryStruct.columnOid = colStruct.dataOid;
				dctnryStruct.fCompressionType = colType.compressionType;
				dctnryStruct.colWidth = colType.colWidth;
				//Open the dictionary file
				if (NO_ERROR != (error = fWriteEngine.openDctnry (txnID, dctnryStruct)))
				{
					logging::Message::Args args;
					logging::Message message(7);
					args.add("Update failed on ");
					args.add(tableName.table);
					args.add(" table. ");
					WErrorCodes ec;
					args.add(ec.errorString(error));
					message.format(args);
					result.message = message;
					return false;
				}
				//Build token values
				std::string tmpStr;
				if (columns[j]->get_isFromCol())
				{
					std::vector < std::string > colValues = valuesThisExtent[volInValues];
					assert (colValues.size() == rowsThisExtent);
					for (unsigned row = 0; row < colValues.size(); row++)
					{
						//@bug 3582. Check the real scale
						uint32_t funcScale = columns[j]->get_funcScale();
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
							if (pushWarning && notStored)
							{
								transactionWarning = true;
								colNames.push_back(tableColName.column);
								notStored = false;
							}
						}
						 
						
						if (colValues[row].compare ("") == 0) //Null value is not stored in dictionary file.
						{
	/*						if (colType.constraintType == NOTNULL_CONSTRAINT)
							{
								retval = false;
								result.result = UPDATE_ERROR;
								Message::Args args;
								args.add(tableColName.column);
								result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_VIOLATE_NOT_NULL, args));
								writeEngine.closeDctnry(txnID, colType.compressionType);
								return retval;
							}
	*/
							WriteEngine::Token nullToken;
							colTuple.data = nullToken;
						}
						else
						{							
							WriteEngine::DctnryTuple dctTuple;
							memcpy(dctTuple.sigValue, colValues[row].c_str(), colValues[row].length());
							dctTuple.sigSize = colValues[row].length();
							dctTuple.isNull = false;
							error = fWriteEngine.tokenize(txnID,dctTuple, colType.compressionType);
							if (error != NO_ERROR)
							{
								fWriteEngine.closeDctnry(txnID, colType.compressionType);
								return false;
							}
							colTuple.data = dctTuple.token;
						}
						colTupleList.push_back (colTuple);
					}
					fWriteEngine.closeDctnry(txnID, colType.compressionType);
					++volInValues;
				}
				else
				{
					if (columns[j]->get_isnull())
					{
/*						if (colType.constraintType == NOTNULL_CONSTRAINT)
						{
							retval = false;
							result.result = UPDATE_ERROR;
							Message::Args args;
							args.add(tableColName.column);
							result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_VIOLATE_NOT_NULL, args));
							writeEngine.closeDctnry(txnID, colType.compressionType);
							return retval;
						}
*/
						WriteEngine::Token nullToken;
						colTuple.data = nullToken;
					}
					else
					{
						tmpStr = columns[j]->get_Data();
						
						if (tmpStr.length() > (unsigned int)colType.colWidth)
						{
							tmpStr = tmpStr.substr(0, colType.colWidth);
							pushWarning = true;
							if (pushWarning && notStored)
							{
								transactionWarning = true;
								colNames.push_back(tableColName.column);
								notStored = false;
							}

						}
						WriteEngine::DctnryTuple dctTuple;
						memcpy(dctTuple.sigValue, tmpStr.c_str(), tmpStr.length());
						dctTuple.sigSize = tmpStr.length();
						dctTuple.isNull = false;
						error = fWriteEngine.tokenize(txnID, dctTuple, colType.compressionType);
						if (error != NO_ERROR)
						{
							fWriteEngine.closeDctnry(txnID, colType.compressionType);
							return false;
						}
						colTuple.data = dctTuple.token;
						fWriteEngine.closeDctnry(txnID, colType.compressionType); // Constant only need to tokenize once.
					}

					for (unsigned row = 0; row < (itor->second.size()); row++)
						colTupleList.push_back (colTuple);

				}
			}
			else
			{
				colStruct.colWidth = colType.colWidth;
				if (columns[j]->get_isFromCol())
				{
					std::vector < std::string > colValues = valuesThisExtent[volInValues];
					//cout << "colValues.size() : rowsThisExtent " << colValues.size() << ":" << rowsThisExtent << endl;
					assert(colValues.size() == rowsThisExtent);
					for (unsigned row = 0; row < colValues.size(); row++)
					{
						if (colValues[row].compare ("") == 0)
							isNull = true;
						else
							isNull = false;
							
						//@bug 3582. Check the real scale
						uint32_t funcScale = columns[j]->get_funcScale();
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
/*						if (isNULL && (colType.constraintType == NOTNULL_CONSTRAINT))
						{
							retval = false;
							result.result = UPDATE_ERROR;
							Message::Args args;
							args.add(tableColName.column);
							result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_VIOLATE_NOT_NULL, args));
							return retval;
						}
*/
						try {
							datavalue = DataConvert::convertColumnData(colType, colValues[row], pushWarning, isNull);
						}
						catch (exception& ex)
						{
							//@Bug 2624. Error out on conversion failure
							cerr << "UpdatePackageProcessor::processPackage::updateRowsValues" << ex.what() << endl;
							Message::Args args;
							args.add(string("'") + colValues[row] + string("'"));
							result.result = UPDATE_ERROR;
							result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERCAL_DATA, args));
							return false;
						}
						colTuple.data = datavalue;
						colTupleList.push_back (colTuple);
						if (pushWarning && notStored)
						{
							transactionWarning = true;
							colNames.push_back(tableColName.column);
							notStored = false;
						}
					}
					++volInValues;
				}
				else
				{
					if (columns[j]->get_isnull())
					{
						isNull = true;
					}
					else
					{
						isNull = false;
					}
/*					if (isNULL && (colType.constraintType == NOTNULL_CONSTRAINT))
					{
						retval = false;
						result.result = UPDATE_ERROR;
						Message::Args args;
						args.add(tableColName.column);
						result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_VIOLATE_NOT_NULL, args));
						return retval;
					}
*/
					string inData (columns[j]->get_Data());
					if (colType.autoincrement && ( isNull || (inData.compare("0")==0)))
					{
						isNull = false;
						autoColType = colType;
						for (unsigned row = 0; row < (itor->second.size()); row++)
						{
							if (nextVal <= 0)
							{
								result.result = UPDATE_ERROR;
								result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT));
								return false;
							}	
							ostringstream oss;
							oss << nextVal++;
							inData = oss.str();
							try {
							datavalue = DataConvert::convertColumnData(colType, inData, pushWarning, isNull);
							}
							catch (exception& ex)
							{
								//@Bug 2624. Error out on conversion failure
								cerr << "UpdatePackageProcessor::processPackage::updateRowsValues" << ex.what() << endl;
								Message::Args args;
								args.add(string("'") + inData + string("'"));
								result.result = UPDATE_ERROR;
								result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERCAL_DATA, args));
								return false;
							}
							colTuple.data = datavalue;
							colTupleList.push_back (colTuple);
						}
					}
					else
					{
						try {
							datavalue = DataConvert::convertColumnData(colType, inData, pushWarning, isNull);
						}
						catch (exception& ex)
						{
							//@Bug 2624. Error out on conversion failure
							cerr << "UpdatePackageProcessor::processPackage::updateRowsValues" << ex.what() << endl;
							Message::Args args;
							args.add(string("'") + inData + string("'"));
							result.result = UPDATE_ERROR;
							result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_NON_NUMERCAL_DATA, args));
							return false;
						}
						colTuple.data = datavalue;
						if (pushWarning && notStored)
						{
							transactionWarning = true;
							colNames.push_back(tableColName.column);
							notStored = false;
						}
						for (unsigned row = 0; row < (itor->second.size()); row++)
							colTupleList.push_back (colTuple);
					}
				}
			}

			colStructList.push_back(colStruct);
			colValueList.push_back (colTupleList);
		}
/*		WriteEngine::ColTupleList colTupleList = colValueList[0];
		for (unsigned i=0; i < colTupleList.size(); i++)
		{
			string curStr;
			curStr = boost::any_cast<std::string>(colTupleList[i].data);
			cout << " input to WE: " << curStr << endl;
		}
*/
		bool rtn = true;
		if (itor->second.size() == 0)
			return rtn;
		//timer.stop("prepare values");
		//timer.start("updateColumnRecs");
//cout << "rows to be updated " << rowsProcessed << " colValueList.size() = " << colValueList.size() << " colValueList[0].size()=  " <<  colValueList[0].size() << endl;
		if (NO_ERROR != (error = fWriteEngine.updateColumnRecs(txnID, colStructList, colValueList,  itor->second)))
		{
			retval = false;
			// build the logging message
			logging::Message::Args args;
			logging::Message message(7);
			args.add("Update failed on ");
			args.add(tableName.table);
			args.add(" table. ");
			WErrorCodes ec;
			args.add(ec.errorString(error));
			message.format(args);

			if (error == ERR_BRM_DEAD_LOCK)
			{
				result.result = DEAD_LOCK_ERROR;
				result.message = message;
			}
			else if (error == ERR_BRM_VB_OVERFLOW)
			{
				result.result = VB_OVERFLOW_ERROR;
				result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_VERSIONBUFFER_OVERFLOW));
			}
			else
			{
				result.result = UPDATE_ERROR;
				result.message = message;
			}
			break;
		}
		valueitor++;
		//timer.stop("updateColumnRecs");
	}

	if (transactionWarning)
    {
		result.result = IDBRANGE_WARNING;
		Message::Args args;
		string cols = "'" + colNames[0] + "'";

		for (unsigned i=1; i<colNames.size();i++)
		{
			cols = cols + ", " +  "'" + colNames[i] + "'";
		}
		args.add(cols);
		result.message = Message(IDBErrorInfo::instance()->errorMsg(WARN_DATA_TRUNC,args));
    }

    VERBOSE_INFO("Finished Updating Row(s)");
	//timer.finish();
    return retval;
}

bool UpdatePackageProcessor::fixUpRows(u_int32_t sessionID, dmlpackage::CalpontDMLPackage& cpackage,
	const std::string& schema, const std::string& table, dmlpackage::RowList& rows,
	bool & firstCall, joblist::SJLP & jbl, std::vector<extentInfo>& extentsinfo,
	ridsUpdateListsByExtent& ridsListsMap, DMLResult& result)
{
	/*check which column has index. If the index is multicolumn index and not in the update list,
	we need select the value(s) back from database.*/
	//cout << "fMaxUpdateRows = " << fMaxUpdateRows << endl;
	WriteEngine::RIDList rowIDList;
	CalpontSystemCatalog * csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	execplan::CalpontSystemCatalog::TableName tableName;
	tableName.schema = schema;
	tableName.table = table;
	config::Config* cf = config::Config::makeConfig();
	unsigned filesPerColumnPartition = 32;
	unsigned extentsPerSegmentFile = 4;
	unsigned dbrootCnt = 2;
	unsigned extentRows = 0x800000;
	string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
	if (fpc.length() != 0)
		filesPerColumnPartition = cf->uFromText(fpc);
	string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
	if (epsf.length() != 0)
		extentsPerSegmentFile = cf->uFromText(epsf);
	string dbct = cf->getConfig("SystemConfig", "DBRootCount");
	if (dbct.length() != 0)
		dbrootCnt = cf->uFromText(dbct);

	DBRM brm;
	extentRows = brm.getExtentRows();
	unsigned dbRoot = 0;
	unsigned partition = 0;
	unsigned segment = 0;

	//@Bug 1341. Added useCache flag to take advantage of cache.
	CalpontSystemCatalog::RIDList colRidList = csc->columnRIDs (tableName, true);
	RidList rids;
	RowGroup rowGroup;
	//ridsListsMap.clear();

	bool done  = true;
	extentInfo aExtentinfo;
	uint64_t curRow = 0;
	uint64_t batchRows = 0;
	CalpontSystemCatalog::RID rid;
	CalpontSystemCatalog::ColType ct;
	uint16_t startDBRoot;
	uint32_t partitionNum;

	//CalpontSystemCatalog::NJLSysDataList valueList;
	vector<ColumnResult*>::const_iterator it;
    if (cpackage.HasFilter())
    {
		if (rowIDList.size() > 0)
		{
			rowIDList.clear();
		}

		execplan::CalpontSelectExecutionPlan *plan = cpackage.get_ExecutionPlan();
		//cout << "Plan: " << *plan << endl;
		ByteStream bs;
		if ((0 != plan) && (0 != fEC))
		{
			if (firstCall)
			{
				//plan->traceOn(true);
				jbl = joblist::JobListFactory::makeJobList(plan, *fRM, true);
				if (jbl->status() == 0)
				{
					TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jbl.get());
					//assert(tjlp);
					rowGroup = tjlp->getOutputRowGroup();
					try
					{
						if (jbl->putEngineComm(fEC) != 0)
							throw std::runtime_error(jbl->errMsg());

						jbl->doQuery();
					}
					catch (exception& ex)
					{
						cerr << "UpdatePackageProcessor::processPackage::fixupRowsValues" << ex.what() << endl;
						logging::Message::Args args;
						logging::Message message(2);
						args.add("Update Failed: ");
						args.add(ex.what());
						message.format(args);
						result.result = UPDATE_ERROR;
						result.message = message;
						return true;
					}
				}
				else
				{
					result.result = JOB_ERROR;
					result.message = jbl->errMsg();
					return true;
				}
				firstCall = false;
			}
			else
			{
				TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jbl.get());
				//assert(tjlp);
				rowGroup = tjlp->getOutputRowGroup();
			}

			CalpontSystemCatalog::OID tableOid = csc->tableRID(execplan::make_table(tableName.schema, tableName.table)).objnum;

			//Check whether there is dictionary columns
			for (unsigned int i = 0; i < colRidList.size(); i++)
			{
				ct = csc->colType (colRidList[i].objnum);
				if (ct.ddn.dictOID > 0)
				{
					colRidList[i].objnum = ct.ddn.dictOID;
				}
			}
			brm.getStartExtent(colRidList[0].objnum, startDBRoot, partitionNum);
			//cout << "startpartition is " << partitionNum << endl;
			rowgroup::Row row;
			rowGroup.initRow(&row);
			do {
				bs.reset();
				//assert(jbl.get());
				curRow = jbl->projectTable(tableOid, bs);
				//cout << "get rows " << curRow << endl;
				// Error handle
				rowGroup.setData((bs.buf()));
				uint err = rowGroup.getStatus();
				if ( err != 0 )
				{
					result.result = UPDATE_ERROR;
					result.message = Message(projectTableErrCodeToMsg(err));
					return true;
				}	

				if (rowGroup.getBaseRid() == (uint64_t) (-1 & ~0x1fff))
				{
					continue;  // @bug4247, not valid row ids, may from small side outer
				}

				batchRows += curRow;
				if (curRow == 0) //No more rows to fetch
				{
					jbl.reset();
					bs.reset();
					done = true;
					return done;
				}
				else
				{
					rowGroup.getRow(0, &row);
					rid = row.getRid();
					//cout <<"update get rid " << rid << endl;
					convertRidToColumn (rid, dbRoot, partition, segment, filesPerColumnPartition,
							extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt,partitionNum );
					//cout << "after convertion, rid:dbRoot:partition:segment = 	" << rid << ":" <<dbRoot<<":"<<	partition<<":"<<segment<<endl;
					aExtentinfo.dbRoot = dbRoot;
					aExtentinfo.partition = partition;
					aExtentinfo.segment = segment;
					uint32_t rowsThisRowgroup = rowGroup.getRowCount();
					//cout << "rowsThisRowgroup " << rowsThisRowgroup << endl;
					for (unsigned i = 0; i < rowsThisRowgroup; i++)
					{
						rowGroup.getRow(i, &row);
						rid = row.getRid();
						convertRidToColumn (rid, dbRoot, partition, segment, filesPerColumnPartition,
							extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt,partitionNum);
						ridsListsMap[aExtentinfo].push_back(rid);
					}

					if (batchRows >= fMaxUpdateRows)
					{
						done = false;
						return done;
					}
				}

			} while (0 != curRow);
		}
	}
    else
    {
		//cout << "In fixUpRows without filters" << endl;
		dmlpackage::Row* rowPtr = new dmlpackage::Row();

		dmlpackage::ColumnList colList;
		getColumnsForTable(sessionID, schema,table,colList);
		if (colList.size())
		{
			rowPtr->set_RowID(0);
			dmlpackage::ColumnList::const_iterator iter = colList.begin();
			while (iter != colList.end())
			{
				DMLColumn* columnPtr = *iter;
				rowPtr->get_ColumnList().push_back(columnPtr);
				++iter;
			}
		}

		/* Here we build a simple execution plan to scan all the columns which has index and at the same time,
		it is dictionary. Other wise just scan the first column of the table with no filter to effectively get back all the rids for the table */

		//if (valueListsMap.size() > 0)
		//	done = false;
		//else
		execplan::CalpontSelectExecutionPlan csep;
		SessionManager sm;
		BRM::TxnID txnID;
		ByteStream bs;

		txnID = sm.getTxnID(sessionID);
		CalpontSystemCatalog::SCN verID;
		verID = sm.verID();

		csep.txnID(txnID.id);
		csep.verID(verID);
		csep.sessionID(cpackage.get_SessionID());

		CalpontSelectExecutionPlan::ReturnedColumnList acolList;
		CalpontSelectExecutionPlan::ColumnMap colMap;
		CalpontSelectExecutionPlan::TableList tbList;

		CalpontSystemCatalog::TableAliasName tn = make_aliastable(schema, table, table);
		tbList.push_back(tn);
		csep.tableList(tbList);

		DMLColumn* columnPtr = rowPtr->get_ColumnList().at(0);
		std::string fullSchema = schema+"."+table+"."+columnPtr->get_Name();
		SimpleColumn* sc = new SimpleColumn(fullSchema, cpackage.get_SessionID());
		sc->tableAlias(table);
		execplan::SRCP srcp(sc);
		acolList.push_back(srcp);
		colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(fullSchema, srcp));
		CalpontSystemCatalog::OID oid = sc->oid();
		// @bug 1367
		CalpontSystemCatalog::ColType colType = csc->colType(oid);
		if (colType.colWidth > 8 ||
			(colType.colDataType == CalpontSystemCatalog::VARCHAR && colType.colWidth > 7))
			oid = colType.ddn.dictOID;

		csep.columnMapNonStatic (colMap);
		csep.returnedCols (acolList);

		// so now that we have a defined execution plan, go run it and get the ridlist back into rows
		if ((0 != fEC))
		{
			if (firstCall)
			{
				jbl = joblist::JobListFactory::makeJobList(&csep, *fRM, true);
				if (jbl->status() == 0)
				{
					TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jbl.get());
					//assert(tjlp);
					rowGroup = tjlp->getOutputRowGroup();
					try
					{
						if (jbl->putEngineComm(fEC) != 0)
							throw std::runtime_error(jbl->errMsg());

						jbl->doQuery();
					}
					catch (exception& ex)
					{
						cerr << "UpdatePackageProcessor::processPackage::fixupRowsValues" << ex.what() << endl;
						logging::Message::Args args;
						logging::Message message(2);
						args.add("Update Failed: ");
						args.add(ex.what());
						message.format(args);
						result.result = UPDATE_ERROR;
						result.message = message;
						return true;
					}
				}
				else
				{
					result.result = JOB_ERROR;
					result.message = jbl->errMsg();
					return true;
				}
				firstCall = false;

			}
			else
			{
				TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jbl.get());
				//assert(tjlp);
				rowGroup = tjlp->getOutputRowGroup();
			}

			CalpontSystemCatalog::OID tableoid = csc->tableRID(execplan::make_table(tableName.schema, tableName.table)).objnum;

			brm.getStartExtent(colRidList[0].objnum, startDBRoot, partitionNum);
			rowgroup::Row row;
			rowGroup.initRow(&row);
			do {
				bs.reset();
				//assert(jbl.get());
				curRow = jbl->projectTable(tableoid, bs);
				// Error handle
				rowGroup.setData((bs.buf()));
				uint err = rowGroup.getStatus();
				if ( err != 0 )
				{
					result.result = UPDATE_ERROR;
					result.message = Message(projectTableErrCodeToMsg(err));
					return true;
				}	

				if (rowGroup.getBaseRid() == (uint64_t) (-1 & ~0x1fff))
				{
					continue;  // @bug4247, not valid row ids, may from small side outer
				}

				batchRows += curRow;

				if (curRow == 0) //No more rows to fetch
				{
					jbl.reset();
					bs.reset();
					done = true;
					return done;
				}

				else
				{
					rowGroup.getRow(0, &row);
					rid = row.getRid();
					convertRidToColumn (rid, dbRoot, partition, segment, filesPerColumnPartition,
								extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt,partitionNum);

					aExtentinfo.dbRoot = dbRoot;
					aExtentinfo.partition = partition;
					aExtentinfo.segment = segment;
					uint32_t rowsThisRowgroup = rowGroup.getRowCount();
					//cout << "rowsThisRowgroup " << rowsThisRowgroup << endl;
					for (unsigned i = 0; i < rowsThisRowgroup; i++)
					{
						rowGroup.getRow(i, &row);
						rid = row.getRid();
						convertRidToColumn (rid, dbRoot, partition, segment, filesPerColumnPartition,
								extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt,partitionNum);
						ridsListsMap[aExtentinfo].push_back(rid);
					}

					if (batchRows >= fMaxUpdateRows)
					{
						done = false;
						return done;
					}
				}
			} while(0 != curRow);
		}
	}

    return done;
}

bool UpdatePackageProcessor::fixUpRowsValues(u_int32_t sessionID,
	dmlpackage::CalpontDMLPackage& cpackage, const std::string& schema, const std::string& table,
	valueListsByExtent& valueLists, bool & firstCall, joblist::SJLP & jbl,
	std::vector<extentInfo>& extentsinfo, ridsUpdateListsByExtent& ridsListsMap, DMLResult& result)
{
	/*use memory usage to limit rowGroups to process*/
	WriteEngine::RIDList rowIDList;
	CalpontSystemCatalog * csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	execplan::CalpontSystemCatalog::TableName tableName;
	tableName.schema = schema;
	tableName.table = table;
	//cout << " in fixUpRowsValues " << endl;
	config::Config* cf = config::Config::makeConfig();
	unsigned filesPerColumnPartition = 32;
	unsigned extentsPerSegmentFile = 4;
	unsigned dbrootCnt = 2;
	unsigned extentRows = 0x800000;
	string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
	if (fpc.length() != 0)
		filesPerColumnPartition = cf->uFromText(fpc);
	string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
	if (epsf.length() != 0)
		extentsPerSegmentFile = cf->uFromText(epsf);
	string dbct = cf->getConfig("SystemConfig", "DBRootCount");
	if (dbct.length() != 0)
		dbrootCnt = cf->uFromText(dbct);

	DBRM brm;
	extentRows = brm.getExtentRows();
	unsigned dbRoot = 0;
	unsigned partition = 0;
	unsigned segment = 0;

	//@Bug 1341. Added useCache flag to take advantage of cache.
	CalpontSystemCatalog::RIDList colRidList = csc->columnRIDs (tableName, true);
	RidList rids;
	RowGroup rowGroup;
	//ridsListsMap.clear();

	bool done  = true;
	extentInfo aExtentinfo;
	uint64_t curDataSize = 0;
	uint64_t curRow = 0;
	uint64_t totalDataSize = 0;
	CalpontSystemCatalog::RID rid;
	CalpontSystemCatalog::ColType ct;
	uint16_t startDBRoot;
	uint32_t partitionNum;
	int64_t intColVal = 0;
	std::vector <CalpontSystemCatalog::ColType> colTypes;
	execplan::CalpontSelectExecutionPlan *plan = cpackage.get_ExecutionPlan();
	uint columns = plan->returnedCols().size();

	// force the table being updated to join last on the streaming side.
	plan->overrideLargeSideEstimate(true);

	//cout << "Plan:" << endl << *plan << endl;
	ByteStream bs;
	if ((0 != plan) && (0 != fEC))
	{
		if (firstCall)
		{
			//plan->traceOn(true);
			jbl = joblist::JobListFactory::makeJobList(plan, *fRM, true);
			if (jbl->status() == 0)
			{
				TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jbl.get());
				//assert(tjlp);
				rowGroup = tjlp->getOutputRowGroup();
				try
				{
					if (jbl->putEngineComm(fEC) != 0)
						throw std::runtime_error(jbl->errMsg());

					jbl->doQuery();
				}
				catch (exception& ex)
				{
						cerr << "UpdatePackageProcessor::processPackage::fixupRowsValues" << ex.what() << endl;
						logging::Message::Args args;
						logging::Message message(2);
						args.add("Update Failed: ");
						args.add(ex.what());
						message.format(args);
						result.result = UPDATE_ERROR;
						result.message = message;
						return true;
				}
			}
			else
			{
				result.result = JOB_ERROR;
				result.message = jbl->errMsg();
				return true;
			}
			firstCall = false;
		}
		else
		{
			TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jbl.get());
			//assert(tjlp);
			rowGroup = tjlp->getOutputRowGroup();
		}

		CalpontSelectExecutionPlan::ReturnedColumnList returnedCols = plan->returnedCols();
		//Fill in column types
		for (unsigned col = 0; col < returnedCols.size(); col++)
		{
			ct.colPosition = col;
			ct.colWidth = rowGroup.getColumnWidth(col);
			ct.colDataType = rowGroup.getColTypes()[col];
			ct.columnOID = rowGroup.getOIDs()[col];
			ct.scale = rowGroup.getScale()[col];
			ct.precision = rowGroup.getPrecision()[col];
			colTypes.push_back (ct);
		}

		CalpontSystemCatalog::OID tableOid = csc->tableRID(execplan::make_table(tableName.schema, tableName.table)).objnum;

		brm.getStartExtent(colRidList[0].objnum, startDBRoot, partitionNum);
		rowgroup::Row row;
		rowGroup.initRow(&row);
		string value("");

		do {
			bs.reset();
			//assert(jbl.get());
			curRow = jbl->projectTable(tableOid, bs);
			//cout << "getting rows " << curRow << endl;
			// Error handle
			rowGroup.setData((bs.buf()));
			uint err = rowGroup.getStatus();
			if ( err != 0 )
			{
				result.result = UPDATE_ERROR;
				result.message = Message(projectTableErrCodeToMsg(err));
				return true;
			}	
			if (curRow == 0) //No more rows to fetch
			{
				jbl.reset();
				bs.reset();
				done = true;
				return done;
			}

			if (rowGroup.getBaseRid() == (uint64_t) (-1 & ~0x1fff))
			{
				continue;  // @bug4247, not valid row ids, may from small side outer
			}

			rowGroup.getRow(0, &row);
			rid = row.getRid();
			convertRidToColumn (rid, dbRoot, partition, segment, filesPerColumnPartition,
					extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt, partitionNum);

			aExtentinfo.dbRoot = dbRoot;
			aExtentinfo.partition = partition;
			aExtentinfo.segment = segment;
			uint32_t rowsThisRowgroup = rowGroup.getRowCount();
			curDataSize = rowGroup.getDataSize();
			totalDataSize += curDataSize;
			std::vector<ColValues> colsVals(columns);
			//cout << "extent dbroot:partition:seg = " <<  dbRoot << ":" << partition << ":" << segment << " got rows " << rowsThisRowgroup << endl;
			for (unsigned i = 0; i < rowsThisRowgroup; i++)
			{
				rowGroup.getRow(i, &row);
				rid = row.getRid();
				//cout << "update got rid " << rid << endl;
				convertRidToColumn (rid, dbRoot, partition, segment, filesPerColumnPartition,
					extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt,partitionNum);
				ridsListsMap[aExtentinfo].push_back(rid);

				//Fetch values
				for (unsigned col = 0; col < returnedCols.size(); col++)
				{
					// fetch and store data
					if (row.isNullValue(col))
					{
						value = "";
						//cout << "update got value " << value << endl;
						assert(col < colsVals.size());
						colsVals[col].push_back(value);
						continue;
					}
					ct = colTypes[col];
					switch (ct.colDataType)
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
							if (ct.scale <= 0)
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
							if (ct.scale <= 0)
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
							if (ct.scale <= 0)
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
							if (ct.scale <= 0)
							{
								ostringstream os;
								os << intColVal;
								value = os.str();
							}
							break;
						}
						case CalpontSystemCatalog::DECIMAL:
						{
							if (ct.colWidth == execplan::CalpontSystemCatalog::ONE_BYTE)
							{
								intColVal = row.getIntField<1>(col);
								ostringstream os;
								os << intColVal;
								value = os.str();
							}
							else if (ct.colWidth == execplan::CalpontSystemCatalog::TWO_BYTE)
							{
								intColVal = row.getIntField<2>(col);
								ostringstream os;
								os << intColVal;
								value = os.str();
							}
							else if (ct.colWidth == execplan::CalpontSystemCatalog::FOUR_BYTE)
							{
								intColVal = row.getIntField<4>(col);
								ostringstream os;
								os << intColVal;
								value = os.str();
							}
							else if (ct.colWidth == execplan::CalpontSystemCatalog::EIGHT_BYTE)
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
					
					if (ct.scale > 0)
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
							assert(l1 >= 2);
							l1--;
						}

						if ((unsigned)ct.scale > l1)
						{
							const char* zeros = "0000000000000000000"; //19 0's
							size_t diff = ct.scale - l1; //this will always be > 0
							memmove((ptr + diff), ptr, l1 + 1); //also move null
							memcpy(ptr, zeros, diff);
							l1 = 0;
						}
						else
							l1 -= ct.scale;
						memmove((ptr + l1 + 1), (ptr + l1), ct.scale + 1); //also move null
						*(ptr + l1) = '.';
						
						value = ctmp;
						string::size_type pos = value.find_first_of(".");
						if (pos == 0)
							value.insert(0,"0");
					}
					
					//cout << "update got value " << value << endl;
					assert(col < colsVals.size());
					colsVals[col].push_back(value);
				}
			}
			//Save the values
			std::vector<ColValues> values;
			for (unsigned i=0; i < columns; i++)
			{
				values.push_back (colsVals[i]);
			}
			valueLists[aExtentinfo] =  values ;
//			if (totalDataSize >= fMaxUpdateRows)
//			{
				//cout << " ridsListsMap.size() = " << ridsListsMap.size() << endl;
				done = false;
				return done;
//			}

		} while (0 != curRow);
	}

	//cout << " ridsListsMap.size() = " << ridsListsMap.size() << endl;
	return done;
}
} // namespace dmlpackageprocessor
