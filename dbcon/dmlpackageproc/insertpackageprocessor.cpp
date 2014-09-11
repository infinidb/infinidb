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

/***********************************************************************
 *   $Id: insertpackageprocessor.cpp 8902 2012-09-13 20:19:35Z chao $
 *
 *
 ***********************************************************************/
#include <iostream>
#define INSERTPKGPROC_DLLEXPORT
#include "insertpackageprocessor.h"
#include "autoincrementdata.h"
#undef INSERTPKGPROC_DLLEXPORT
#include <vector>
#include <algorithm>
#include <sstream>
#include "messagelog.h"
#include "sqllogger.h"
#include <boost/algorithm/string/case_conv.hpp>

using namespace boost::algorithm;
using namespace std;
using namespace WriteEngine;
using namespace dmlpackage;
using namespace execplan;
using namespace dataconvert;
using namespace logging;

namespace dmlpackageprocessor
{

	DMLPackageProcessor::DMLResult InsertPackageProcessor::processPackage(dmlpackage::CalpontDMLPackage & cpackage)
	{
		SUMMARY_INFO("InsertPackageProcessor::processPackage");

		std::string err;
		DMLResult result;
		result.result = NO_ERROR;
		BRM::TxnID txnid;

		fSessionID = cpackage.get_SessionID();
		WriteEngine::ChunkManager* cm = cpackage.get_ChunkManager();
		fWriteEngine.setChunkManager(cm);
		std::map<u_int32_t,u_int32_t> oids;
		VERBOSE_INFO("Processing Insert DML Package...");

		try
		{
			// set-up the transaction
			txnid.id  = cpackage.get_TxnID();	
			txnid.valid = true;
			// get the table object from the package
			DMLTable *tablePtr = cpackage.get_Table();
			CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
			CalpontSystemCatalog::TableName tableName;
			if (0 != tablePtr)
			{
				//check table lock
				AutoincrementData *autoincrementData = AutoincrementData::makeAutoincrementData(fSessionID);
				systemCatalogPtr->identity(CalpontSystemCatalog::EC);
				systemCatalogPtr->sessionID(fSessionID);
				tableName.schema = tablePtr->get_SchemaName();
				tableName.table = tablePtr->get_TableName();
				execplan::CalpontSystemCatalog::ROPair roPair;
				roPair = systemCatalogPtr->tableRID( tableName );
				CalpontSystemCatalog::TableInfo tableInfo = systemCatalogPtr->tableInfo(tableName);
				

				u_int32_t  processID = 0;
				std::string  processName("DMLProc");
				int rc = 0;
				int i = 0; 
				rc = fSessionManager.setTableLock( roPair.objnum, fSessionID, processID, processName, true );
					
				if ( rc == BRM::ERR_TABLE_LOCKED_ALREADY )
				{	
					int waitPeriod = 10;
					int sleepTime = 100; // sleep 100 milliseconds between checks
					int numTries = 10;  // try 10 times per second
					waitPeriod = Config::getWaitPeriod();	
            		numTries = 	waitPeriod * 10;
					struct timespec rm_ts;

					rm_ts.tv_sec = sleepTime/1000; 
					rm_ts.tv_nsec = sleepTime%1000 *1000000;

            		for ( ; i < numTries; i++ )
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
											
						rc = fSessionManager.setTableLock( roPair.objnum, fSessionID, processID, processName, true );
						
						if ( rc == 0 )
							break;
					}
    
					if ( i >= numTries ) //error out
					{
						result.result = INSERT_ERROR;
						result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_TABLE_LOCKED));
						return result;
					}					
					
				}
				else if ( rc  == BRM::ERR_FAILURE)
				{
					logging::Message::Args args;
					logging::Message message(1);
					args.add("Insert Failed due to BRM failure");
					result.result = INSERT_ERROR;
					result.message = message;
					return result;
				}
		
				// get the row(s) from the table
				RowList rows = tablePtr->get_RowList();
				if ( cpackage.get_Logging() )
				{
					LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
					logging::Message::Args args1;
					logging::Message msg(1);
					args1.add("Start SQL statement: ");
					args1.add(cpackage.get_SQLStatement());
					msg.format( args1 );
					Logger logger(logid.fSubsysID);
					logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
				}
				
				// add any columns not supplied in the insert statement
				if (!fixupColumns(fSessionID, tablePtr->get_SchemaName(), tablePtr->get_TableName(), rows, result))
				{
					err = "Fixup columns error.";
					SUMMARY_INFO(err);
					throw std::runtime_error(err);
				}

				std::vector < WriteEngine::RID > ridList;
			//	WriteEngine::ColValueList colValuesList;
				std::vector < std::string > colNameList;
				std::vector < dicStrValues > dicStrValCols;
				long long nextVal = 0;
				long long originalNextVal = 0;
				//get autoincrement column next value
				if (tableInfo.tablewithautoincr == 1)
				{
					nextVal = autoincrementData->getNextValue(roPair.objnum);
					if (nextVal <= 0)
					{
						//get from systables
						nextVal= systemCatalogPtr->nextAutoIncrValue(tableName); 					
					}
				}
				originalNextVal = nextVal;
				// insert the row(s)
				CalpontSystemCatalog::ColType  autoColType;	
				if (insertRows
					(fSessionID, txnid.id, tablePtr->get_SchemaName(),
					 tablePtr->get_TableName(), rows, result, nextVal, autoColType,
					 cpackage.get_isInsertSelect()))
				{
					// save the next value
					if ((tableInfo.tablewithautoincr == 1) && (originalNextVal < nextVal))
					{
						//validate next Value
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
				else
				{
					err = result.message.msg();
					SUMMARY_INFO(err);
					throw std::logic_error(err);
				}  
			}

			// Log the insert statement.
			if ( cpackage.get_Logending() )
			{
				LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
				logging::Message::Args args1;
				logging::Message msg(1);
				args1.add("End SQL statement");
				msg.format( args1 );
				Logger logger(logid.fSubsysID);
				logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
				logging::logDML(cpackage.get_SessionID(), txnid.id, cpackage.get_SQLStatement()+ ";", cpackage.get_SchemaName());
				
				fWriteEngine.flushVMCache();
				CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(tableName, true);
				for (unsigned i=0; i < ridList.size(); i++)
				{
					oids[ridList[i].objnum] = ridList[i].objnum;
				}
				CalpontSystemCatalog::DictOIDList dictOids = systemCatalogPtr->dictOIDs(tableName);
				for (unsigned i=0; i < dictOids.size(); i++)
				{
					oids[dictOids[i].dictOID] = dictOids[i].dictOID;
				}
				
				if (result.result != IDBRANGE_WARNING)
					fWriteEngine.flushDataFiles(result.result, oids);
				else
					fWriteEngine.flushDataFiles(0, oids);
				//cout << "insertpackageprocessor Logging to datamods " << cpackage.get_SQLStatement()+ ";" << endl;
			}
		}
		catch(exception & ex)
		{
			cerr << "InsertPackageProcessor::processPackage: " << ex.what() << endl;

			logging::Message::Args args;
			logging::Message message(1);
			args.add("Insert Failed: ");
			args.add(ex.what());
			args.add("");
			args.add("");
			message.format(args);
			if ( result.result != VB_OVERFLOW_ERROR )
			{
				result.result = INSERT_ERROR;
				result.message = message;
			}
			//@Bug 3895. Log the end of insert statement.
			LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
			logging::Message::Args args1;
			logging::Message msg(1);
			args1.add("End SQL statement with error");
			msg.format( args1 );
			Logger logger(logid.fSubsysID);
			logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
			
		}
		catch(...)
		{
			cerr << "InsertPackageProcessor::processPackage: caught unknown exception!" << endl;
			logging::Message::Args args;
			logging::Message message(1);
			args.add("Insert Failed: ");
			args.add("encountered unkown exception");
			args.add("");
			args.add("");
			message.format(args);

			result.result = INSERT_ERROR;
			result.message = message;
			//@Bug 3895. Log the end of insert statement.
			LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
			logging::Message::Args args1;
			logging::Message msg(1);
			args1.add("End SQL statement with error");
			msg.format( args1 );
			Logger logger(logid.fSubsysID);
			logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
		}

		//TODO:Handle autocommit

		VERBOSE_INFO("Finished Processing Insert DML Package");
		return result;
	}

	bool InsertPackageProcessor::insertRows(u_int32_t sessionID,
											CalpontSystemCatalog::SCN txnID,
											const std::string & schema,
											const std::string & table,
											const dmlpackage::RowList & rows,
											DMLResult & result, long long & nextVal, CalpontSystemCatalog::ColType & autoColType,
											bool insertSelect)
	{
		SUMMARY_INFO("InsertPackageProcessor::insertRows");

		bool retval = true;
		WriteEngine::ColStructList colStructs;
		WriteEngine::DctnryStructList dctnryStructList;
		WriteEngine::DctnryValueList dctnryValueList;
		WriteEngine::ColValueList colValuesList;
        WriteEngine::DictStrList dicStringList ;
		CalpontSystemCatalog::TableName tableName;
		CalpontSystemCatalog::TableColName tableColName;
		tableName.table = tableColName.table = table;
		tableName.schema = tableColName.schema = schema;

		CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
		systemCatalogPtr->identity(CalpontSystemCatalog::EC);

		VERBOSE_INFO("Inserting Row(s)...");

		if (rows.size())
		{
			Row *rowPtr = rows.at(0);
			ColumnList columns = rowPtr->get_ColumnList();
			ColumnList::const_iterator column_iterator = columns.begin();
			while (column_iterator != columns.end() && true == retval)
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
					VERBOSE_INFO("Looking up OID for:");
					VERBOSE_INFO(columnPtr->get_Name().c_str());

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
							
/*							if ( isNULL && (colType.constraintType == NOTNULL_CONSTRAINT))
							{
								retval = false;
								result.result = INSERT_ERROR;
								Message::Args args;
								args.add(tableColName.column);
								result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_VIOLATE_NOT_NULL, args));
								return retval;
							}
*/
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
							
							//Error outon null value if the column has not null constraint.
/*							if ( isNULL && (colType.constraintType == NOTNULL_CONSTRAINT))
							{
								retval = false;
								result.result = INSERT_ERROR;
								Message::Args args;
								args.add(tableColName.column);
								result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_VIOLATE_NOT_NULL, args));
								return retval;
							}
*/							
							//check if autoincrement column and value is 0 or null
							if (colType.autoincrement && ( isNULL || (indata.compare("0")==0)))
							{
								if (nextVal <= 0)
									throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT));
									
								ostringstream oss;
								oss << nextVal++;
								indata = oss.str();
								isNULL = false;
								autoColType = colType;
							}
							try
							{                    
								datavalue = DataConvert::convertColumnData(colType, indata, pushWarning, isNULL);
							}
							catch(exception & ex)
							{
								cerr << "InsertPackageProcessor::processPackage: " << ex.what() << endl;
								//cout << "The value is " << indata << " and coltype is " << colType.colDataType<< endl;
								logging::Message::Args args;
								logging::Message message(1);
								args.add("Insert Failed:");
								args.add(ex.what());
								args.add("");
								args.add("");
								message.format(args);
								x = ex.what();
								result.result = INSERT_ERROR;
								result.message = message;
							}
							//@Bug 1806
							if (result.result != NO_ERROR && result.result != IDBRANGE_WARNING)
							{
								//throw std::runtime_error("Invalid data");
								throw std::runtime_error(x);
							}
							if ( pushWarning && ( result.result != IDBRANGE_WARNING ) )
								result.result = IDBRANGE_WARNING;
								
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

		VERBOSE_INFO("Invoking writeEngine.insertColumnRec");
		// call the write engine to write all the rows
		int error = NO_ERROR;
		//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
		if (colValuesList[0].size() > 0)
		{
			if (NO_ERROR != 
			(error = fWriteEngine.insertColumnRec(txnID, colStructs, colValuesList, dctnryStructList, dicStringList,insertSelect)))
			{
				retval = false;
				// build the logging message
				logging::Message::Args args;
				logging::Message message(1);
				args.add("Insert failed on the ");
				args.add(tableName.table);
				args.add("table. It was due to: ");
				WErrorCodes ec;
				args.add(ec.errorString(error));
				message.format(args);
				if (error == ERR_BRM_DEAD_LOCK)
				{
					result.result = DEAD_LOCK_ERROR;
					result.message = message;
				}
				else if ( error == ERR_BRM_VB_OVERFLOW )
				{
					result.result = VB_OVERFLOW_ERROR;
					result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_VERSIONBUFFER_OVERFLOW));
				}
				else
				{
					result.result = INSERT_ERROR;
					result.message = message;
				}
			}
		}

		return retval;
	}

	bool InsertPackageProcessor::fixupColumns(u_int32_t sessionID,
											  const std::string & schema,
											  const std::string & table,
											  const dmlpackage::RowList & rows, DMLResult & result)
	{

		SUMMARY_INFO("InsertPackageProcessor::fixupColumns");

		VERBOSE_INFO("Add missing columns to row(s)...");
		bool retval = true;
		if (rows.size())
		{
			ColumnList addedColumns;

			CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
			systemCatalogPtr->identity(CalpontSystemCatalog::EC);
			systemCatalogPtr->sessionID(fSessionID);

			Row *rowPtr = rows.at(0);

			CalpontSystemCatalog::TableName tableName;
			tableName.schema = schema;
			tableName.table = table;

			// get the column count from the row and the system catalog
			// if they match we are done
			unsigned int colcount = rowPtr->get_NumberOfColumns();
			//@Bug 1341. Added useCache flag to take advantage of cache.
			CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(tableName, true);
			typedef std::vector < CalpontSystemCatalog::OID > objnumList;
			objnumList oidList;
			//@bug 1396 Sort oidlist according to column position
			CalpontSystemCatalog::ColType colType;
			oidList.resize(ridList.size());
			for (unsigned int i = 0; i < ridList.size(); i++)
			{
				colType = systemCatalogPtr->colType(ridList[i].objnum);
				oidList[colType.colPosition] = ridList[i].objnum;
			}
			//@Bug 1274 oid can be reused and will not be inorder
			//@Bug 1304. systables are hard coded.
			// if ( oidList[0] < 3000 )
			//       sort(oidList.begin(), oidList.end());
			if (ridList.size() > colcount)
			{
				// else we need to add null columns values to complete the row
				// for each column in the system catalog
				RowList::const_iterator row_iterator = rows.begin();
				while (row_iterator != rows.end())
				{
					Row *rowPtr = *row_iterator;
					ColumnList columns = rowPtr->get_ColumnList();

					objnumList::const_iterator rid_iterator = oidList.begin();
					while (rid_iterator != oidList.end())
					{
						//CalpontSystemCatalog::OID obj = *rid_iterator;
						CalpontSystemCatalog::OID oid = *rid_iterator;

						CalpontSystemCatalog::ColType colType;
						colType = systemCatalogPtr->colType(oid);

						CalpontSystemCatalog::TableColName tableColName;
						tableColName = systemCatalogPtr->colName(oid);

						ColumnList::const_iterator column_iterator = columns.begin();
						bool found = false;

						while (column_iterator != columns.end())
						{
							DMLColumn *columnPtr = *column_iterator;
							std::string columnName = columnPtr->get_Name();
							boost::algorithm::to_lower(columnName);
							if (tableColName.column == columnName)
							{
								VERBOSE_INFO("Found column:");
								VERBOSE_INFO(tableColName.column.c_str());

								found = true;
								break;
							}
							++column_iterator;
						}
						// if not add it
						if (!found)
						{
							VERBOSE_INFO("Column not found in row:");
							VERBOSE_INFO(tableColName.column.c_str());
							VERBOSE_INFO("Adding column:");
							VERBOSE_INFO(tableColName.column.c_str());
							bool isNULL = true;
							rowPtr->get_ColumnList().push_back(new DMLColumn(tableColName.column, "NULL", isNULL));
						}

						++rid_iterator;

					}

					++row_iterator;
				}
			}
			else if ((ridList.size() == colcount) && ((rowPtr->get_ColumnList()[0]->get_Name()) == ""))
			{
				VERBOSE_INFO("No column name in row:");
				//std::string value;
				RowList::const_iterator row_iterator = rows.begin();
				while (row_iterator != rows.end())
				{
					Row *rowPtr = *row_iterator;
					ColumnList columns = rowPtr->get_ColumnList();

					objnumList::const_iterator rid_iterator = oidList.begin();
					unsigned int j = 0;
					while (rid_iterator != oidList.end())
					{
						CalpontSystemCatalog::OID obj = *rid_iterator;
						CalpontSystemCatalog::OID oid = obj;

						CalpontSystemCatalog::ColType colType;
						colType = systemCatalogPtr->colType(oid);

						CalpontSystemCatalog::TableColName tableColName;
						tableColName = systemCatalogPtr->colName(oid);

						ColumnList::const_iterator column_iterator = columns.begin();
						//DMLColumn* columnPtr = *column_iterator;
						VERBOSE_INFO("Adding column:");
						VERBOSE_INFO(tableColName.column.c_str());
						//value = columnPtr->get_Data();
						rowPtr->get_ColumnList()[j]->set_Name(tableColName.column);
						++rid_iterator;
						j++;
					}

					++row_iterator;
				}

			}
		}
		return retval;
	}

} // namespace dmlpackageprocessor

// vim:ts=4 sw=4:
