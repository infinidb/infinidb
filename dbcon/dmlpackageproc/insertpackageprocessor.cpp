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
 *   $Id: insertpackageprocessor.cpp 9378 2013-04-04 14:20:11Z chao $
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
#include "oamcache.h"
#include "bytestream.h"
#include <map>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include "we_messages.h"
#include "tablelockdata.h"

using namespace boost::algorithm;
using namespace std;
using namespace WriteEngine;
using namespace dmlpackage;
using namespace execplan;
using namespace dataconvert;
using namespace logging;
using namespace oam;
using namespace messageqcpp;

namespace dmlpackageprocessor
{

	DMLPackageProcessor::DMLResult InsertPackageProcessor::processPackage(dmlpackage::CalpontDMLPackage & cpackage)
	{
		SUMMARY_INFO("InsertPackageProcessor::processPackage");

		DMLResult result;
		result.result = NO_ERROR;
		BRM::TxnID txnid;
		// set-up the transaction
		txnid.id  = cpackage.get_TxnID();	
		txnid.valid = true;
		fSessionID = cpackage.get_SessionID();
		LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
		logging::Message::Args args1;
		logging::Message msg(1);
		args1.add("Start SQL statement: ");
		args1.add(cpackage.get_SQLStatement());
		msg.format( args1 );
		Logger logger(logid.fSubsysID);
		logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
		//WriteEngine::ChunkManager* cm = cpackage.get_ChunkManager();
		//fWriteEngine.setChunkManager(cm);
		//std::map<u_int32_t,u_int32_t> oids;
		VERBOSE_INFO("Processing Insert DML Package...");
		uint64_t uniqueId = 0;
		//Bug 5070. Added exception handling
		try {
			uniqueId = fDbrm->getUnique64();
		}
		catch (std::exception& ex)
		{
			logging::Message::Args args;
			logging::Message message(9);
			args.add(ex.what());
			message.format(args);
			result.result = INSERT_ERROR;	
			result.message = message;
			fSessionManager.rolledback(txnid);
			return result;
		}
		catch ( ... )
		{
			logging::Message::Args args;
			logging::Message message(9);
			args.add("Unknown error occured while getting unique number.");
			message.format(args);
			result.result = INSERT_ERROR;	
			result.message = message;
			fSessionManager.rolledback(txnid);
			return result;
		}
		uint64_t tableLockId = 0;
		int rc = 0;
		std::string errorMsg;
		OamCache * oamcache = OamCache::makeOamCache();
		std::vector<int> moduleIds = oamcache->getModuleIds();
		std::vector<uint> pms;		
			
		try
		{

			for (unsigned int i=0; i <moduleIds.size(); i++)
			{			
				pms.push_back((uint)moduleIds[i]);
			}
			
			//cout << "single insert get transaction id " << txnid.id << endl;
			// get the table object from the package
			DMLTable *tablePtr = cpackage.get_Table();
			boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
			//cout << "DMLProc using syscatptr:sessionid = " << systemCatalogPtr <<":" << fSessionID<< endl;
			CalpontSystemCatalog::TableName tableName;
			execplan::CalpontSystemCatalog::ROPair roPair;
			TablelockData * tablelockData = TablelockData::makeTablelockData(fSessionID);
			if (0 != tablePtr)
			{
				//check table lock
				systemCatalogPtr->identity(CalpontSystemCatalog::EC);
				systemCatalogPtr->sessionID(fSessionID);
				tableName.schema = tablePtr->get_SchemaName();
				tableName.table = tablePtr->get_TableName();
				roPair = systemCatalogPtr->tableRID( tableName );
				CalpontSystemCatalog::TableInfo tableInfo = systemCatalogPtr->tableInfo(tableName);
				
				tableLockId = tablelockData->getTablelockId(roPair.objnum); //check whether this table is locked already for this session
				if (tableLockId == 0)
				{
					//cout << "tablelock is not found in cache, getting from dbrm" << endl;
					u_int32_t  processID = ::getpid();
					int32_t   txnId = txnid.id;
					int32_t sessionId = fSessionID;
					std::string  processName("DMLProc");
					int i = 0;
				
					try {
						tableLockId = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId, &txnId, BRM::LOADING );
					}
					catch (std::exception&)
					{
						throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
					}
				
					if ( tableLockId  == 0 )
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
							try {
								processID = ::getpid();
								txnId = txnid.id;
								sessionId = fSessionID;
								processName = "DMLProc";
								tableLockId = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId, &txnId, BRM::LOADING );
							}
							catch (std::exception&)
							{
								throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
							}

							if (tableLockId > 0)
								break;
						}
						if (i >= numTries) //error out
						{
							result.result = INSERT_ERROR;
							logging::Message::Args args;
							args.add(processName);
							args.add((uint64_t)processID);
							args.add(sessionId);
							throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_TABLE_LOCKED,args));
						}			
					}
				}
				//cout << " tablelock is obtained with id " << tableLockId << endl;
				tablelockData->setTablelock(roPair.objnum, tableLockId);
				
				int pmNum = 1;
				//find the shortest dbroot to send the row
                //How about we find the dbroot that would help casual partitioning most?
                //Or at least hurt it the least.
				CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(tableName, true);
				std::vector<BRM::EmDbRootHWMInfo_v> allInfo (pms.size());
				for (unsigned i = 0; i < pms.size(); i++)
				{
					rc = fDbrm->getDbRootHWMInfo((ridList[0].objnum), pms[i], allInfo[i]);
					if ( rc != 0)
						break;
				}
				
				if ( rc !=0 ) //@Bug 4760.
				{
					result.result = INSERT_ERROR;
					ostringstream oss;
					oss << "There is no extent information for table " << tableName.table;
					throw std::runtime_error(oss.str());
				}
				BRM::EmDbRootHWMInfo tmp;
				bool tmpSet = false;
				for (unsigned i=0; i < allInfo.size(); i++)
				{
					BRM::EmDbRootHWMInfo_v emDbRootHWMInfos = allInfo[i];
						
					for (unsigned j=0; j < emDbRootHWMInfos.size(); j++)
					{					
						if (emDbRootHWMInfos[j].totalBlocks == 0)
							continue;
							
						if (!tmpSet)
						{
							tmp = emDbRootHWMInfos[j];
							tmpSet = true;
							continue;
						}
						if ( tmp > emDbRootHWMInfos[j] )
						{
							tmp = emDbRootHWMInfos[j];					
						}
					}
				}
				
				uint32_t dbroot = tmp.dbRoot;
				boost::shared_ptr<std::map<int, int> > dbRootPMMap = oamcache->getDBRootToPMMap();
				pmNum = (*dbRootPMMap)[dbroot];
				
				//@Bug 4760. validate pm value
				if (pmNum == 0)
				{
					result.result = INSERT_ERROR;
					ostringstream oss;
					oss << "There is no extent information for table " << tableName.table;
					throw std::runtime_error(oss.str());
				}
				//This is for single insert only. Batch insert is handled in dmlprocessor.
				//get a unique number
				//cout << "fWEClient = " << fWEClient << endl;
				fWEClient->addQueue(uniqueId);
				ByteStream bytestream;
				bytestream << (uint8_t)WE_SVR_SINGLE_INSERT;
				bytestream << uniqueId;
				bytestream << (uint32_t)txnid.id;
				bytestream << dbroot;
				cpackage.write(bytestream);
				boost::shared_ptr<messageqcpp::ByteStream> bsIn;
				
				ByteStream::byte rc1;
				try
				{
					fWEClient->write(bytestream, (uint)pmNum);
#ifdef IDB_DML_DEBUG
cout << "Single insert sending WE_SVR_SINGLE_INSERT to pm " << pmNum << endl;
#endif	
					
					bsIn.reset(new ByteStream());
					fWEClient->read(uniqueId, bsIn);
					if ( bsIn->length() == 0 ) //read error
					{
						rc = NETWORK_ERROR;
						errorMsg = "Lost connection to Write Engine Server while updating SYSTABLES";
					}			
					else {
						*bsIn >> rc1;
						if (rc1 != 0) {
							*bsIn >> errorMsg;
							rc = rc1;
						}
					}
					
				}
				catch (runtime_error& ex) //write error
				{
#ifdef IDB_DML_DEBUG
cout << "Single insert got exception" << ex.what() << endl;
#endif			
					rc = NETWORK_ERROR;
					errorMsg = ex.what();
				}
				catch (...)
				{
					errorMsg = "Caught ... exception during single row insert";
					rc = NETWORK_ERROR;
#ifdef IDB_DML_DEBUG
cout << "Single insert got unknown exception" << endl;
#endif
				}
				// Log the insert statement.
				LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
				logging::Message::Args args1;
				logging::Message msg(1);
				args1.add("End SQL statement");
				msg.format( args1 );
				Logger logger(logid.fSubsysID);
				logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
				logging::logDML(cpackage.get_SessionID(), txnid.id, cpackage.get_SQLStatement()+ ";", cpackage.get_SchemaName());
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
				errorMsg = ex.what();
			}
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
		}
		if (( rc !=0) && (rc != IDBRANGE_WARNING))
		{
			logging::Message::Args args;
			logging::Message message(1);
			args.add("Insert Failed: ");
			args.add(errorMsg);
			args.add("");
			args.add("");
			message.format(args);
			result.result = INSERT_ERROR;
			result.message = message;
		}
		else if (rc == IDBRANGE_WARNING)
		{
			logging::Message::Args args;
			logging::Message message(1);
			args.add(errorMsg);
			args.add("");
			args.add("");
			message.format(args);
			result.result = IDBRANGE_WARNING;
			result.message = message;
		}
		fWEClient->removeQueue(uniqueId);
		VERBOSE_INFO("Finished Processing Insert DML Package");
		return result;
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

			boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
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
