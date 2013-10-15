/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/***********************************************************************
 *   $Id: commandpackageprocessor.cpp 9613 2013-06-12 15:44:24Z dhall $
 *
 *
 ***********************************************************************/
#include <ctime>
#include <iostream>
#include <sstream>
#include <set>
#include <vector>
#include <boost/scoped_ptr.hpp>

#define COMMANDPKGPROC_DLLEXPORT
#include "commandpackageprocessor.h"
#undef COMMANDPKGPROC_DLLEXPORT
#include "messagelog.h"
#include "dbrm.h"
#include "sqllogger.h"
#include "tablelockdata.h"
#include "we_messages.h"
#include "we_ddlcommandclient.h"
#include "oamcache.h"
#include "snmpglobal.h"
#include "snmpmanager.h"
#include "liboamcpp.h"
using namespace std;
using namespace WriteEngine;
using namespace dmlpackage;
using namespace execplan;
using namespace logging;
using namespace boost;
using namespace BRM;

namespace dmlpackageprocessor
{
	// Tracks active cleartablelock commands by storing set of table lock IDs
	/*static*/ std::set<uint64_t>
		CommandPackageProcessor::fActiveClearTableLockCmds;    
	/*static*/ boost::mutex
		CommandPackageProcessor::fActiveClearTableLockCmdMutex;

DMLPackageProcessor::DMLResult
CommandPackageProcessor::processPackage(dmlpackage::CalpontDMLPackage& cpackage)
{
    SUMMARY_INFO("CommandPackageProcessor::processPackage");

    DMLResult result;
    result.result = NO_ERROR;

    VERBOSE_INFO("Processing Command DML Package...");
	std::string stmt = cpackage.get_DMLStatement();
	boost::algorithm::to_upper(stmt);
	trim(stmt);
	fSessionID = cpackage.get_SessionID();
	BRM::TxnID txnid = fSessionManager.getTxnID(cpackage.get_SessionID());
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
		result.result = COMMAND_ERROR;	
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
		result.result = COMMAND_ERROR;	
		result.message = message;
		fSessionManager.rolledback(txnid);
		return result;
	}
	
	string errorMsg;
	bool queRemoved = false;
	logging::LoggingID lid(20);
    logging::MessageLog ml(lid);
	LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
	logging::Message::Args args1;
	logging::Message msg(1);
	Logger logger(logid.fSubsysID);
	if (stmt != "CLEANUP")
	{
		args1.add("Start SQL statement: ");
		args1.add(stmt);
		msg.format( args1 );
		logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
	}
	//fWEClient->addQueue(uniqueId);
    try
    {
        // set-up the transaction
		if ( (stmt == "COMMIT") || (stmt == "ROLLBACK") )
		{
			//SQLLogger sqlLogger(stmt, DMLLoggingId, cpackage.get_SessionID(), txnid.id);
			
			if ((txnid.valid))
			{
				vector<LBID_t> lbidList;
				fDbrm->getUncommittedExtentLBIDs(static_cast<VER_t>(txnid.id), lbidList);
				bool cpInvalidated = false;
				//cout << "get a valid txnid " << txnid.id << " and stmt is " << stmt << " and isBachinsert is " << cpackage.get_isBatchInsert() << endl;
				if ((stmt == "COMMIT") && (cpackage.get_isBatchInsert()))
				{
					//update syscolumn for the next value 
					CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
					CalpontSystemCatalog::TableName tableName;
					tableName = systemCatalogPtr->tableName(cpackage.getTableOid());
                    try
                    {
                        uint64_t nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
                        if (nextVal != AUTOINCR_SATURATED) //need to update syscolumn
                        {
                            //get autoincrement column oid
                            int32_t columnOid = systemCatalogPtr->autoColumOid(tableName);
                            //get the current nextVal from controller
							scoped_ptr<DBRM> aDbrm(new DBRM());
                            uint64_t nextValInController = 0;
                            bool validNextVal = false;
                            aDbrm->getAILock(columnOid);
                            nextVal = systemCatalogPtr->nextAutoIncrValue(tableName); //in case it has changed
                            validNextVal = aDbrm->getAIValue(columnOid, &nextValInController);
                            if ((validNextVal) && (nextValInController > nextVal))
                            {
                                fWEClient->removeQueue(uniqueId);
                                queRemoved = true;
                                WE_DDLCommandClient ddlClient;
                                uint8_t rc = ddlClient.UpdateSyscolumnNextval(columnOid, nextValInController);
                                if (rc != 0)
                                    throw std::runtime_error("Error in UpdateSyscolumnNextval");
                            }
                        }
                    }
                    catch (std::exception& ex)
                    {
                        //Rollback transaction
                        rollBackTransaction(uniqueId, txnid, fSessionID, errorMsg);
                        fSessionManager.rolledback( txnid );
                        throw std::runtime_error(ex.what());
                    }
					//systemCatalogPtr->updateColinfoCache(nextValMap);
					int weRc = 0;
					if (cpackage.get_isAutocommitOn())
					{
						weRc = commitBatchAutoOnTransaction(uniqueId, txnid, cpackage.getTableOid(), errorMsg);
						cpInvalidated = true;
					}
					else
					{
						weRc = fDbrm->vbCommit(txnid.id);
						if (weRc != 0)
							BRM::errString(weRc, errorMsg);
						//weRc = commitBatchAutoOffTransaction(uniqueId, txnid, cpackage.getTableOid(), errorMsg);
					}
					
					if (weRc != 0)
					{
						throw std::runtime_error(errorMsg);
					}
					logging::logCommand(cpackage.get_SessionID(), txnid.id, "COMMIT;");
					fSessionManager.committed( txnid );
					//cout << "releasing  transaction id for batchinsert" <<  txnid.id << endl;
				}
				else if (stmt == "COMMIT")
				{
					//cout << "success in commitTransaction" << endl;
					//update syscolumn for the next value 
					CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
					CalpontSystemCatalog::TableName tableName;
					uint32_t tableOid = cpackage.getTableOid();
					std::vector<TableLockInfo> tableLocks = fDbrm->getAllTableLocks();
					if (tableOid == 0) //special case: transaction commit for autocommit off and not following a dml statement immediately
					{
						TablelockData * tablelockData = TablelockData::makeTablelockData(fSessionID);
						TablelockData::OIDTablelock  tablelockMap = tablelockData->getOidTablelockMap();
						TablelockData::OIDTablelock::iterator iter;
						if (!tablelockMap.empty())
						{
							for ( unsigned k=0; k < tableLocks.size(); k++)
							{
								iter = tablelockMap.find(tableLocks[k].tableOID);
								if ( iter != tablelockMap.end() )
								{
									tableName = systemCatalogPtr->tableName(tableLocks[k].tableOID);
                                    try 
                                    {
    									uint64_t nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
    									if (nextVal != AUTOINCR_SATURATED) //neet to update syscolumn
    									{
    										//get autoincrement column oid
    										int32_t columnOid = systemCatalogPtr->autoColumOid(tableName);
    										//get the current nextVal from controller
											scoped_ptr<DBRM> aDbrm(new DBRM());
    										uint64_t nextValInController = 0;
    										bool validNextVal = false;
											aDbrm->getAILock(columnOid);
											nextVal = systemCatalogPtr->nextAutoIncrValue(tableName); //in case it has changed
											validNextVal = aDbrm->getAIValue(columnOid, &nextValInController);
										
    										if ((validNextVal) && (nextValInController > (uint64_t)nextVal))
    										{
    											fWEClient->removeQueue(uniqueId);
    							
    											queRemoved = true;
    											WE_DDLCommandClient ddlClient;
    											uint8_t rc = ddlClient.UpdateSyscolumnNextval(columnOid, nextValInController,fSessionID);
    											aDbrm->releaseAILock(columnOid);
    											if (rc != 0)
    											{
    												//for now 
    												fSessionManager.committed( txnid );
    												throw std::runtime_error("Error in UpdateSyscolumnNextval");
    											}
    										}
    										else
    											aDbrm->releaseAILock(columnOid);
                                        }
						
									}
                                    catch (std::exception& ex)
                                    {
                                        //Rollback transaction, release tablelock
                                        rollBackTransaction(uniqueId, txnid, fSessionID, errorMsg);
                                        fDbrm->releaseTableLock(iter->second);
                                        fSessionManager.rolledback( txnid );
                                        throw std::runtime_error(ex.what());
                                    }
								}
							}	
						}												
					}
					else
					{
						if (tableOid >= 3000)
						{
							tableName = systemCatalogPtr->tableName(tableOid);
                            try 
                            {
                                uint64_t nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
    							if (nextVal != AUTOINCR_SATURATED) //need to update syscolumn
    							{
    								//get autoincrement column oid
    								int32_t columnOid = systemCatalogPtr->autoColumOid(tableName);
    								//get the current nextVal from controller
    								scoped_ptr<DBRM> aDbrm(new DBRM());
    								uint64_t nextValInController = 0;
    								bool validNextVal = false;
                                    aDbrm->getAILock(columnOid);
                                    nextVal = systemCatalogPtr->nextAutoIncrValue(tableName); //in case it has changed
                                    validNextVal = aDbrm->getAIValue(columnOid, &nextValInController);
    								if ((validNextVal) && (nextValInController > (uint64_t)nextVal))
    								{
    									fWEClient->removeQueue(uniqueId);
    							
    									queRemoved = true;
    									WE_DDLCommandClient ddlClient;
    									uint8_t rc = ddlClient.UpdateSyscolumnNextval(columnOid, nextValInController,fSessionID);
    									aDbrm->releaseAILock(columnOid);
    									if (rc != 0)
    									{
    										//for now 
    										fSessionManager.committed( txnid );
    										throw std::runtime_error("Error in UpdateSyscolumnNextval");
    									}
    								}
    								else
    									aDbrm->releaseAILock(columnOid);		
                                }
							}
                            catch (std::exception& ex)
                            {
                                //Rollback transaction, release tablelock
                                rollBackTransaction(uniqueId, txnid, fSessionID, errorMsg);
                                for ( unsigned k=0; k < tableLocks.size(); k++)
                                {
                                    if ( tableLocks[k].tableOID == tableOid )
                                    {	
                                        try {
                                            fDbrm->releaseTableLock(tableLocks[k].id);
                                        }
                                        catch (std::exception&)
                                        {}
                                    }
                                }
                                fSessionManager.rolledback( txnid );
                                throw std::runtime_error(ex.what());
                            }
						}
					}
					int weRc = commitTransaction(uniqueId, txnid );
					logging::logCommand(cpackage.get_SessionID(), txnid.id, "COMMIT;");
					
					if (weRc != WriteEngine::NO_ERROR)
					{
						//cout << "Got an error in commitTransaction" << endl;
						WErrorCodes   ec;
						ostringstream oss;
						oss << "COMMIT failed: " << ec.errorString(weRc);
						throw std::runtime_error(oss.str());
					}
					fSessionManager.committed( txnid );
					//cout << "commit releasing  transaction id " <<  txnid.id << endl;
				}
				else if ((stmt == "ROLLBACK") && (cpackage.get_isBatchInsert()))
				{
					int weRc = 0;
					
					
					//version rollback, Bulkrollback
					weRc = rollBackTransaction(uniqueId, txnid, fSessionID, errorMsg);
					if (weRc == 0)
					{
						//@Bug 4560 invalidate cp first as bulkrollback will truncate the newly added lbids.
						fDbrm->invalidateUncommittedExtentLBIDs(0, &lbidList);
						cpInvalidated = true;
						weRc = rollBackBatchAutoOnTransaction(uniqueId, txnid, fSessionID, cpackage.getTableOid(), errorMsg);
							
					}
					else
					{
						throw std::runtime_error(errorMsg);
					}
					
					logging::logCommand(cpackage.get_SessionID(), txnid.id, "ROLLBACK;");
					if (weRc != 0)
					{
						//@Bug 4524. Don't set to readonly. Just error out.
						WErrorCodes   ec;
						ostringstream oss;
						oss << "ROLLBACK batch insert failed due to: " << "(" << weRc << ")" << ec.errorString(weRc);
						//Log to error log
						logging::Message::Args args1;
						logging::Message message1(2);
						args1.add(oss.str());
						message1.format( args1 );
						ml.logErrorMessage( message1 );
						throw std::runtime_error(oss.str());
					}
					fSessionManager.rolledback( txnid );
					//cout << "batch rollback releasing  transaction id " <<  txnid.id << endl;
				}
				else if (stmt == "ROLLBACK")
				{
					std::string errorMsg("");
					logging::logCommand(cpackage.get_SessionID(), txnid.id, "ROLLBACK;");
					int weRc = rollBackTransaction(uniqueId, txnid, fSessionID, errorMsg);
					if (weRc != 0)
					{
						//cout << "Rollback failed" << endl;
						//@Bug 4524. Don't set to readonly. Just error out.
						ostringstream oss;
						oss << "ROLLBACK failed due to: " << errorMsg;
						//Log to error log
						logging::Message::Args args2;
						logging::Message message2(2);
						args2.add(oss.str());
						message2.format( args2 );
						ml.logErrorMessage( message2 );
						throw std::runtime_error(oss.str());
					}
					fSessionManager.rolledback( txnid );
					//cout << "Rollback releasing  transaction id " <<  txnid.id << endl;
				}
				
				if (!cpInvalidated)
				{
					fDbrm->invalidateUncommittedExtentLBIDs(0, &lbidList);
				}
			}
		}
		else if (stmt == "CLEANUP")
		{
			execplan::CalpontSystemCatalog::removeCalpontSystemCatalog
				  (cpackage.get_SessionID());
			execplan::CalpontSystemCatalog::removeCalpontSystemCatalog
				  (cpackage.get_SessionID() | 0x80000000);
		}
		else if (stmt == "VIEWTABLELOCK")
		{
			viewTableLock( cpackage, result );
		}
		else if (stmt == "CLEARTABLELOCK")
		{
			clearTableLock( uniqueId, cpackage, result );
		}
		else if ( !cpackage.get_Logging())
		{
			BRM::TxnID txnid = fSessionManager.getTxnID(cpackage.get_SessionID());
			logging::logDML(cpackage.get_SessionID(), txnid.id, cpackage.get_DMLStatement()+ ";", cpackage.get_SchemaName());
			SQLLogger sqlLogger(cpackage.get_DMLStatement(), DMLLoggingId, fSessionID, txnid.id);
			//cout << "commandpackageprocessor Logging " << cpackage.get_DMLStatement()+ ";" << endl;
		}
        else
        {
            std::string err = "Unknown command.";
            SUMMARY_INFO(err);
            throw std::runtime_error(err);
        }

    }
	catch ( logging::IDBExcept& noTable) //@Bug 2606 catch no table found exception
	{
		cerr << "CommandPackageProcessor::processPackage: " << noTable.what() << endl;

        result.result = COMMAND_ERROR;
        result.message = Message(noTable.what());
	}
    catch (std::exception& ex)
    {
        cerr << "CommandPackageProcessor::processPackage: " << ex.what() << endl;

        logging::Message::Args args;
        logging::Message message(1);
        args.add( ex.what() );
        args.add("");
        args.add("");
        message.format( args );

        result.result = COMMAND_ERROR;
        result.message = message;
    }
    catch (...)
    {
        cerr << "CommandPackageProcessor::processPackage: caught unknown exception!" << endl;
        logging::Message::Args args;
        logging::Message message(1);
        args.add( "Command Failed: ");
        args.add( "encountered unkown exception" );
        args.add("");
        args.add("");
        message.format( args );

        result.result = COMMAND_ERROR;
        result.message = message;
    }
	if (!queRemoved)
		fWEClient->removeQueue(uniqueId);	
	
	//release table lock
	if ((result.result == NO_ERROR)  && ((stmt == "COMMIT") || (stmt == "ROLLBACK")) )
	{
		TablelockData * tablelockData = TablelockData::makeTablelockData(fSessionID);
		TablelockData::OIDTablelock  tablelockMap = tablelockData->getOidTablelockMap();
		bool lockReleased = true;
		
		if (!tablelockMap.empty())
		{
			TablelockData::OIDTablelock::iterator it = tablelockMap.begin();
			while (it != tablelockMap.end())
			{
		
				try {
					lockReleased = fDbrm->releaseTableLock(it->second);
					//cout << "releasing tablelock " << it->second << endl;
				}
				catch (std::exception& ex)
				{
					logging::Message::Args args;
					logging::Message message(1);
					args.add( ex.what() );
					args.add("");
					args.add("");
					message.format( args );

					result.result = COMMAND_ERROR;
					result.message = message;
				}
				if (!lockReleased) //log an error
				{
					ostringstream os;
					os << "tablelock for table oid " << it->first << " is not found";
					logging::Message::Args args;
					logging::Message message(1);
					args.add( os.str());
					args.add("");
					args.add("");
					message.format( args );
					logging::LoggingID lid(21);
					logging::MessageLog ml(lid);

					ml.logErrorMessage(message);
				}
				//cout << "tablelock " << it->second << " is released" << endl;
				it++;
			}
						//@Bug 3557. Clean tablelock cache after commit/rollback.
			TablelockData::removeTablelockData(cpackage.get_SessionID());
		}
						
	}
    VERBOSE_INFO("Finished processing Command DML Package");
	//LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
	logging::Message::Args args2;
	logging::Message msg1(1);
	if (stmt != "CLEANUP")
	{
		args2.add("End SQL statement");
		msg1.format( args2 );
		//Logger logger(logid.fSubsysID);
		logger.logMessage(LOG_TYPE_DEBUG, msg1, logid); 
	}
    return result;
}

//------------------------------------------------------------------------------
// Process viewtablelock command; return table lock information for the
// specified table.
// This function closely resembles printTableLocks in viewtablelock.cpp.
//------------------------------------------------------------------------------
void CommandPackageProcessor::viewTableLock(
	const dmlpackage::CalpontDMLPackage& cpackage,
	DMLPackageProcessor::DMLResult& result)
{
	// Initialize System Catalog object used to get table name
	CalpontSystemCatalog *systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	CalpontSystemCatalog::TableName tableName;
	tableName.schema = cpackage.get_SchemaName();
	tableName.table  = cpackage.get_TableName();
	execplan::CalpontSystemCatalog::ROPair roPair;
	
	roPair = systemCatalogPtr->tableRID( tableName );

	// Get list of table locks for the requested table
	std::vector<BRM::TableLockInfo> tableLocks;
	tableLocks = fDbrm->getAllTableLocks();

	// Make preliminary pass through the table locks in order to determine our
	// output column widths based on the data.  Min column widths are based on
	// the width of the column heading (except for the 'state' column).
	uint64_t maxLockID                = 0;
	uint32_t maxPID                   = 0;
	int32_t  maxSessionID             = 0;
	int32_t  minSessionID             = 0;
	int32_t  maxTxnID                 = 0;

	unsigned int lockIDColumnWidth    = 6; // "LockID"
	unsigned int ownerColumnWidth     = 7; // "Process"
	unsigned int pidColumnWidth       = 3; // "PID"
	unsigned int sessionIDColumnWidth = 7; // "Session"
	unsigned int txnIDColumnWidth     = 3; // "Txn"
	unsigned int createTimeColumnWidth= 12;// "CreationTime"
	unsigned int pmColumnWidth        = 7; // "DBRoots"
	std::vector<std::string> createTimes;
	char cTimeBuffer[1024];

	for (unsigned idx=0; idx<tableLocks.size(); idx++)
	{
		if (tableLocks[idx].id > maxLockID)
			maxLockID = tableLocks[idx].id;
		if (tableLocks[idx].ownerName.length() > ownerColumnWidth)
			ownerColumnWidth = tableLocks[idx].ownerName.length();
		if (tableLocks[idx].ownerPID > maxPID)
			maxPID = tableLocks[idx].ownerPID;
		if (tableLocks[idx].ownerSessionID > maxSessionID)
			maxSessionID = tableLocks[idx].ownerSessionID;
		if (tableLocks[idx].ownerSessionID < minSessionID)
			minSessionID = tableLocks[idx].ownerSessionID;
		if (tableLocks[idx].ownerTxnID > maxTxnID)
			maxTxnID = tableLocks[idx].ownerTxnID;

		ctime_r( &tableLocks[idx].creationTime, cTimeBuffer );
		cTimeBuffer[ strlen(cTimeBuffer)-1 ] = '\0'; // strip trailing '\n'
		std::string cTimeStr( cTimeBuffer );
		if (cTimeStr.length() > createTimeColumnWidth)
			createTimeColumnWidth = cTimeStr.length();
		createTimes.push_back( cTimeStr );

		std::ostringstream pms; //It is dbroots now
		for (unsigned k=0; k<tableLocks[idx].dbrootList.size(); k++)
		{
			if (k > 0)
				pms << ',';
			pms << tableLocks[idx].dbrootList[k];
		}
		if (pms.str().length() > pmColumnWidth)
			pmColumnWidth = pms.str().length();
	}
	ownerColumnWidth      += 2;
	pmColumnWidth         += 2;
	createTimeColumnWidth += 2;

	std::ostringstream idString;
	idString << maxLockID;
	if (idString.str().length() > lockIDColumnWidth)
		lockIDColumnWidth = idString.str().length();
	lockIDColumnWidth += 2;

	std::ostringstream pidString;
	pidString << maxPID;
	if (pidString.str().length() > pidColumnWidth)
		pidColumnWidth = pidString.str().length();
	pidColumnWidth += 2;

	const std::string sessionNoneStr("BulkLoad");
	std::ostringstream sessionString;
	sessionString << maxSessionID;
	if (sessionString.str().length() > sessionIDColumnWidth)
		sessionIDColumnWidth = sessionString.str().length();
	if ((minSessionID < 0) &&
		(sessionNoneStr.length() > sessionIDColumnWidth))
		sessionIDColumnWidth = sessionNoneStr.length();
	sessionIDColumnWidth += 2;

	const std::string txnNoneStr("n/a");
	std::ostringstream txnString;
	txnString << maxTxnID;
	if (txnString.str().length() > txnIDColumnWidth)
		txnIDColumnWidth = txnString.str().length();
	txnIDColumnWidth += 2;

	// Make second pass through the table locks to build our result.
	// Keep in mind the same table could have more than 1 lock
	// (on different PMs), so we don't exit loop after "first" match.
	bool found = false;
	ostringstream os;
	for (unsigned idx=0; idx<tableLocks.size(); idx++)
	{
		if (roPair.objnum == (CalpontSystemCatalog::OID)
			tableLocks[idx].tableOID)
		{
			std::ostringstream pms;
			for (unsigned k=0; k<tableLocks[idx].dbrootList.size(); k++)
			{
				if (k > 0)
					pms << ',';
				pms << tableLocks[idx].dbrootList[k];
			}

			if (found) // write newline between lines
			{
				os << endl;
			}
			else // write the column headers before the first entry
			{
				os.setf(ios::left, ios::adjustfield);
				os << setw(lockIDColumnWidth)  << "LockID"       <<
					setw(ownerColumnWidth)     << "Process"      <<
					setw(pidColumnWidth)       << "PID"          <<
					setw(sessionIDColumnWidth) << "Session"      <<
					setw(txnIDColumnWidth)     << "Txn"          <<
					setw(createTimeColumnWidth)<< "CreationTime" <<
					setw(9)                    << "State"        <<
					setw(pmColumnWidth)        << "DBRoots"          << endl;
			}

			os << "  " <<
				setw(lockIDColumnWidth)    << tableLocks[idx].id       <<
				setw(ownerColumnWidth)     << tableLocks[idx].ownerName<<
				setw(pidColumnWidth)       << tableLocks[idx].ownerPID;

			// Log session ID, or "BulkLoad" if session is -1
			if (tableLocks[idx].ownerSessionID < 0)
				os << setw(sessionIDColumnWidth) << sessionNoneStr;
			else
				os << setw(sessionIDColumnWidth) <<
					tableLocks[idx].ownerSessionID;

			// Log txn ID, or "n/a" if txn is -1
			if (tableLocks[idx].ownerTxnID < 0)
				os << setw(txnIDColumnWidth) << txnNoneStr;
			else
				os << setw(txnIDColumnWidth) <<
					tableLocks[idx].ownerTxnID;

			os << setw(createTimeColumnWidth)<<
					createTimes[idx]       <<
				setw(9) << ((tableLocks[idx].state==BRM::LOADING) ?
					"LOADING" : "CLEANUP") <<
				setw(pmColumnWidth)        << pms.str();

			found = true;
		} // end of displaying a table lock match
	} // end of loop through all table locks

	if (!found)
	{
		os << " Table " << tableName.schema << "." <<
			tableName.table << " is not locked by any process.";
	}

	result.tableLockInfo = os.str();
}

//------------------------------------------------------------------------------
// Process cleartablelock command; execute bulk rollback and release table lock
// for the specified table.
//------------------------------------------------------------------------------
void CommandPackageProcessor::clearTableLock( uint64_t uniqueId,
	const dmlpackage::CalpontDMLPackage& cpackage,
	DMLPackageProcessor::DMLResult& result)
{
	CalpontSystemCatalog::TableName tableName;
	tableName.schema = cpackage.get_SchemaName();
	tableName.table  = cpackage.get_TableName ();

	// Get the Table lock ID that is passed in the SQL statement attribute.
	// This is a kludge we may want to consider changing.  Could add a table
	// lock ID attribute to the CalpontDMLPackage object.
	std::istringstream lockIDString( cpackage.get_SQLStatement() );
	uint64_t tableLockID;
	lockIDString >> tableLockID;

	//----------------------------------------------------- start of syslog code
	//
	// Log initiation of cleartablelock to syslog
	//
	const std::string APPLNAME("cleartablelock SQL cmd");
	const int SUBSYSTEM_ID = 21; // dmlpackageproc
	const int INIT_MSG_NUM = logging::M0088;
	logging::Message::Args msgArgs;
	logging::Message logMsg1( INIT_MSG_NUM );
	msgArgs.add( APPLNAME );
	msgArgs.add( tableName.toString() );
	msgArgs.add( tableLockID );
	logMsg1.format( msgArgs );
	logging::LoggingID  lid( SUBSYSTEM_ID );
	logging::MessageLog ml( lid );
	ml.logInfoMessage( logMsg1 );
	//------------------------------------------------------- end of syslog code

	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	messageqcpp::ByteStream                    bsOut;
	bool lockGrabbed   = false;
	bool bErrFlag      = false;
	bool bRemoveMetaErrFlag      = false;
	std::ostringstream combinedErrMsg;

	try {
		// Make sure BRM is in READ-WRITE state before starting
		int brmRc = fDbrm->isReadWrite( );
		if (brmRc != BRM::ERR_OK)
		{
			std::string brmErrMsg;
			BRM::errString( brmRc, brmErrMsg );
			std::ostringstream oss;
			oss << "Failed BRM status check: " << brmErrMsg;
			throw std::runtime_error( oss.str() );
		}

		BRM::TableLockInfo lockInfo;
		establishTableLockToClear( tableLockID, lockInfo );
		lockGrabbed = true;

		oam::OamCache* oamCache = oam::OamCache::makeOamCache();
		oam::OamCache::dbRootPMMap_t dbRootPmMap = oamCache->getDBRootToPMMap();
		std::map<int,int>::const_iterator mapIter;
		std::set<int> pmSet;

		// Construct relevant list of PMs based on the DBRoots associated
		// with the tableLock.
		for (unsigned int k=0; k<lockInfo.dbrootList.size(); k++)
		{
			mapIter = dbRootPmMap->find( lockInfo.dbrootList[k] );
			if (mapIter != dbRootPmMap->end())
			{
				int pm = mapIter->second;
				pmSet.insert( pm );
			}
			else
			{
				std::ostringstream oss;
				oss << "DBRoot " << lockInfo.dbrootList[k] <<
					" does not map to a PM.  Cannot perform rollback";
				throw std::runtime_error( oss.str() );
			}
		}

		std::vector<int> pmList;
		for (std::set<int>::const_iterator setIter = pmSet.begin();
			setIter != pmSet.end();
			++setIter)
		{
			pmList.push_back( *setIter );
		}

		std::cout << "cleartablelock rollback for table lock " << tableLockID <<
			" being forwarded to PM(s): ";
		for (unsigned int k=0; k<pmList.size(); k++)
		{
			if (k > 0)
				std::cout << ", ";
			std::cout << pmList[k];
		}
		std::cout << std::endl;

		// Perform bulk rollback if state is in LOADING state
		if (lockInfo.state == BRM::LOADING)
		{
			fWEClient->addQueue(uniqueId);

			//------------------------------------------------------------------
			// Send rollback msg to the writeengine server for every PM.
			// We send to each PM, instead of just to PMs in the tablelock's
			// PM list, just in case a DBRoot has been moved to a different PM.
			//------------------------------------------------------------------
//			std::cout << "cleartablelock rollback: tableLock-" << tableLockID <<
//				": uniqueID-" << uniqueId             <<
//				": oid-"      << lockInfo.tableOID    <<
//				"; name-"     << tableName.toString() <<
//				"; app-"      << APPLNAME             << std::endl;

			bsOut << (messageqcpp::ByteStream::byte)WE_SVR_DML_BULKROLLBACK;
			bsOut << uniqueId;
			bsOut << tableLockID;
			bsOut << lockInfo.tableOID;
			bsOut << tableName.toString();
			bsOut << APPLNAME;
			for (unsigned j=0; j<pmList.size(); j++)
			{
				fWEClient->write(bsOut, pmList[j]);
			}

			// Wait for "all" the responses, and accumulate any/all errors
			unsigned int pmMsgCnt = 0;
			while (pmMsgCnt < pmList.size())
			{
				std::string rollbackErrMsg;
				bsIn.reset(new messageqcpp::ByteStream());
				fWEClient->read(uniqueId, bsIn);
				if (bsIn->length() == 0)
				{
					bRemoveMetaErrFlag = true; 
					if (combinedErrMsg.str().length() > 0)
						combinedErrMsg << std::endl;
					combinedErrMsg << "Network error, PM rollback; ";
				}
				else
				{
					messageqcpp::ByteStream::byte rc;
					uint16_t pmNum;
					*bsIn >> rc;
					*bsIn >> rollbackErrMsg;
					*bsIn >> pmNum;

//					std::cout << "cleartablelock rollback response from PM"<<
//						pmNum << "; rc-" << (int)rc <<
//						"; errMsg: {" << rollbackErrMsg << '}' << std::endl;

					if (rc != 0)
					{
						bRemoveMetaErrFlag = true; 
						if (combinedErrMsg.str().empty())
							combinedErrMsg << "Rollback error; ";
						else
							combinedErrMsg << std::endl;
						combinedErrMsg << "[PM" << pmNum << "] " <<
							rollbackErrMsg;
					}
				}
				pmMsgCnt++;
			} // end of while loop to process all responses to bulk rollback

			// If no errors so far, then change state to CLEANUP state.
			// We ignore the return stateChange flag.
			if (!bErrFlag)
			{
				fDbrm->changeState( tableLockID, BRM::CLEANUP );
			}
		} // end of (lockInfo.state == BRM::LOADING)

		// If no errors so far, then:
		// 1. delete meta data backup rollback files
		// 2. finally release table lock
		if (!bErrFlag)
		{
			bsOut.reset();

			//------------------------------------------------------------------
			// Delete meta data backup rollback files
			//------------------------------------------------------------------
//			std::cout << "cleartablelock cleanup: uniqueID-" << uniqueId <<
//				": oid-" << lockInfo.tableOID << std::endl;

			bsOut << (messageqcpp::ByteStream::byte)
				WE_SVR_DML_BULKROLLBACK_CLEANUP;
			bsOut << uniqueId;
			bsOut << lockInfo.tableOID;
			for (unsigned j=0; j<pmList.size(); j++)
			{
				fWEClient->write(bsOut, pmList[j]);
			}

			// Wait for "all" the responses, and accumulate any/all errors
			unsigned int pmMsgCnt = 0;
			while (pmMsgCnt < pmList.size())
			{
				std::string fileDeleteErrMsg;
				bsIn.reset(new messageqcpp::ByteStream());
				fWEClient->read(uniqueId, bsIn);
				if (bsIn->length() == 0)
				{
					bRemoveMetaErrFlag = true;
					if (combinedErrMsg.str().length() > 0)
						combinedErrMsg << std::endl;
					combinedErrMsg << "Network error, PM rollback cleanup; ";
				}
				else
				{
					messageqcpp::ByteStream::byte rc;
					uint16_t pmNum;
					*bsIn >> rc;
					*bsIn >> fileDeleteErrMsg;
					*bsIn >> pmNum;

//					std::cout << "cleartablelock cleanup response from PM" <<
//						pmNum << "; rc-" << (int)rc <<
//						"; errMsg: {" << fileDeleteErrMsg << '}' << std::endl;

					if (rc != 0)
					{
						bRemoveMetaErrFlag = true;
						if (combinedErrMsg.str().empty())
							combinedErrMsg << "Cleanup error; ";
						else
							combinedErrMsg << std::endl;
						combinedErrMsg << "[PM" << pmNum << "] " <<
							fileDeleteErrMsg;
					}
				}
				pmMsgCnt++;
			} // end of while loop to process all responses to rollback cleanup

			// We ignore return release flag from releaseTableLock().
			if (!bErrFlag)
			{
				fDbrm->releaseTableLock( tableLockID );
			}
		}
	}
	catch (std::exception& ex)
	{
		bErrFlag = true;
		combinedErrMsg << ex.what();
	}

	if (!bErrFlag)
	{
		std::ostringstream oss;
		oss << "Table lock " << tableLockID << " for table " <<
			tableName.toString() << " is cleared.";
		//@Bug 4517. Release tablelock if remove meta files failed.
		if (bRemoveMetaErrFlag)
		{
			oss << " Warning: " << combinedErrMsg.str();
		}
		result.tableLockInfo = oss.str();
	}
	else
	{
		std::ostringstream oss;
		oss << "Table lock " << tableLockID << " for table " <<
			tableName.toString() << " cannot be cleared.  "  <<
			combinedErrMsg.str();
		result.tableLockInfo = oss.str();
	}

	// Remove tableLockID out of the active cleartableLock command list
	if (lockGrabbed)
	{
		boost::mutex::scoped_lock lock( fActiveClearTableLockCmdMutex );
		fActiveClearTableLockCmds.erase( tableLockID );
	}

	//----------------------------------------------------- start of syslog code
	//
	// Log completion of cleartablelock to syslog
	//
	const int COMP_MSG_NUM = logging::M0089;
	msgArgs.reset();
	logging::Message logMsg2( COMP_MSG_NUM );
	msgArgs.add( APPLNAME );
	msgArgs.add( tableName.toString() );
	msgArgs.add( tableLockID );
	std::string finalStatus;
	if (!bErrFlag)
	{
		finalStatus = "Completed successfully";
	}
	else
	{
		finalStatus  = "Encountered errors: ";
		finalStatus += combinedErrMsg.str();
	}
	msgArgs.add( finalStatus );
	logMsg2.format( msgArgs );
	ml.logInfoMessage( logMsg2 );
	//------------------------------------------------------- end of syslog code
}

//------------------------------------------------------------------------------
// Get/return the current tablelock info for tableLockID, and reassign the table
// lock to ourselves if we can.  If the lock is still in use by another process
// or by another DML cleartablelock thread, then we error out with exception.
//------------------------------------------------------------------------------
void CommandPackageProcessor::establishTableLockToClear( uint64_t tableLockID,
	BRM::TableLockInfo& lockInfo )
{
	boost::mutex::scoped_lock lock( fActiveClearTableLockCmdMutex );

	// Get current table lock info
	bool getLockInfo = fDbrm->getTableLockInfo(tableLockID, &lockInfo);

	if (!getLockInfo)
	{
		throw std::runtime_error( std::string("Lock does not exist.") );
	}

	std::string processName("DMLProc clearTableLock");
	u_int32_t processID = ::getpid();

	// See if another thread is executing a cleartablelock cmd for this table
	if ((lockInfo.ownerName == processName) &&
		(lockInfo.ownerPID  == processID))
	{
		std::set<uint64_t>::const_iterator it =
			fActiveClearTableLockCmds.find( tableLockID );
		if (it != fActiveClearTableLockCmds.end())
		{
			throw std::runtime_error( std::string( "Lock in use.  "
				"DML is executing another cleartablelock MySQL cmd.") );
		}
	}
	else
	{
		// Take over ownership of stale lock.
		// Use "DMLProc clearTableLock" as process name to differentiate
		// from a DMLProc lock used for inserts, updates, etc.
		int32_t sessionID = fSessionID;
		int32_t txnid     = -1;
		bool ownerChanged = fDbrm->changeOwner(
			tableLockID, processName, processID, sessionID, txnid);
		if (!ownerChanged)
		{
			throw std::runtime_error( std::string(
				"Unable to grab lock; lock not found or still in use.") );
		}
	}

	// Add this cleartablelock command to the list of active cleartablelock cmds
	fActiveClearTableLockCmds.insert( tableLockID );
}

}                                               // namespace dmlpackageprocessor
