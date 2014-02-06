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

/***********************************************************************
*   $Id: dmlprocessor.cpp 1024 2013-07-26 16:23:59Z chao $
*
*
***********************************************************************/
/** @file */
#include "configcpp.h"
#include <signal.h>

//#define      SERIALIZE_DDL_DML_CPIMPORT    1
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/shared_ptr.hpp>
using namespace boost;

#include "cacheutils.h"
#include "vss.h"
#include "dbrm.h"
#include "brmtypes.h"
#include "idberrorinfo.h"
#include "errorids.h"
#include "batchinsertprocessor.h"
#include "tablelockdata.h"
#include "oamcache.h"
#include "messagelog.h"
#include "sqllogger.h"
#include "we_messages.h"
#include "dmlprocessor.h"
using namespace BRM;
using namespace config;
using namespace execplan;
using namespace std;
using namespace messageqcpp;
using namespace dmlpackage;
using namespace dmlpackageprocessor;
using namespace joblist;
using namespace logging;
using namespace oam;
using namespace WriteEngine;

extern boost::mutex mute;
extern boost::condition_variable cond;

namespace
{
const std::string myname = "DMLProc";
}

namespace dmlprocessor
{
// Map to store the package handler objects so we can set flags during execution
// for things like ctrl+c
DMLProcessor::PackageHandlerMap_t DMLProcessor::packageHandlerMap;
boost::mutex DMLProcessor::packageHandlerMapLock;

//Map to store the BatchInsertProc object
std::map<uint32_t, BatchInsertProc*> DMLProcessor::batchinsertProcessorMap;
boost::mutex DMLProcessor::batchinsertProcessorMapLock;

//------------------------------------------------------------------------------
// A thread to periodically call dbrm to see if a user is
// shutting down the system or has put the system into write
// suspend mode. DBRM has 2 flags to check in this case, the
// ROLLBACK flag, and the FORCE flag. These flags will be
// reported when we ask for the Shutdown Pending flag (which we
// ignore at this point). Even if the user is putting the system
// into write suspend mode, this call will return the flags we
// are interested in. If ROLLBACK is set, we cancel normally.
// If FORCE is set, we can't rollback.
struct CancellationThread
{
    CancellationThread(DBRM * aDbrm) : fDbrm(aDbrm)
    {}
    void operator()()
    {
        bool bDoingRollback = false;
        bool bRollback = false;
        bool bForce = false;
        ostringstream oss;
        std::vector<BRM::TableLockInfo> tableLocks;
        BRM::TxnID txnId;
        DMLProcessor::PackageHandlerMap_t::iterator phIter;
        uint32_t sessionID;
        int rc = 0;

        while (true)
        {
            usleep(1000000);    // 1 seconds
            // Check to see if someone has ordered a shutdown or suspend with rollback.
            (void)fDbrm->getSystemShutdownPending(bRollback, bForce);
            if (bDoingRollback && bRollback)
            {
                continue;
                // We've already started the rollbacks. Don't start again.
            }
            bDoingRollback = false;
            if (bRollback)
            {
                RollbackTransactionProcessor rollbackProcessor(fDbrm);
                SessionManager sessionManager;
                uint64_t uniqueId = fDbrm->getUnique64();
                std::string errorMsg;
                int activeTransCount = 0;
                int idleTransCount = 0;
                bDoingRollback = true;
                ostringstream oss;
                oss << "DMLProc has been told to rollback all DML transactions.";
                DMLProcessor::log(oss.str(), logging::LOG_TYPE_INFO);
                // Tell any active processors to stop working and return an error
                // The front end will respond with a ROLLBACK command.
                // Mark all active processors to rollback
				boost::mutex::scoped_lock lk2(DMLProcessor::packageHandlerMapLock);
                for (phIter = DMLProcessor::packageHandlerMap.begin();
                    phIter != DMLProcessor::packageHandlerMap.end();
                    ++phIter)
                {
                    ostringstream oss;
                    oss << "DMLProc will rollback active session " << phIter->second->getSessionID() << " Transaction " << phIter->second->getTxnid();
                    DMLProcessor::log(oss.str(), logging::LOG_TYPE_INFO);

                    ++activeTransCount;
                    phIter->second->rollbackPending();
                }
                if (activeTransCount > 0)
                {
                    ostringstream oss1;
                    oss1 << "DMLProc is rolling back back " << activeTransCount << " active transactions.";
                    DMLProcessor::log(oss1.str(), logging::LOG_TYPE_INFO);
                }
                if (fDbrm->isReadWrite())
                {
                    continue;
                }

                // Check for any open DML transactions that don't currently have
                // a processor
                tableLocks = fDbrm->getAllTableLocks();
                if (tableLocks.size() > 0)
                {
                    for (uint32_t i = 0; i < tableLocks.size(); ++i)
                    {
                        sessionID = tableLocks[i].ownerSessionID;
                        phIter = DMLProcessor::packageHandlerMap.find(sessionID);
                        if (phIter == DMLProcessor::packageHandlerMap.end())
                        {
                            // We have found an active transaction without a packagehandler.
                            // This means that a transaction is open with autocommit turned
                            // off, but there's no current activity on the transaction. We
                            // need to roll it back if it's a DML transaction.
                            // If ownerName == "DMLProc" then it's a DML transaction.
                            if (tableLocks[i].ownerName == "DMLProc")
                            {
                                // OK, we know this is an idle DML transaction, so roll it back.
                                ++idleTransCount;
                                txnId.id = tableLocks[i].ownerTxnID;
                                txnId.valid = true;
                                rc = rollbackProcessor.rollBackTransaction(uniqueId, txnId, sessionID, errorMsg);
                                if ( rc == 0 )
                                {
                                    fDbrm->invalidateUncommittedExtentLBIDs(txnId.id);

                                    //@Bug 4524. In case it is batchinsert, call bulkrollback.
                                    rc = rollbackProcessor.rollBackBatchAutoOnTransaction(uniqueId, txnId, sessionID, tableLocks[i].tableOID, errorMsg);
                                    if (rc == 0)
                                    {
                                        logging::logCommand(0, tableLocks[i].ownerTxnID, "ROLLBACK;");

                                        bool lockReleased = true;
                                        try
                                        {
                                            lockReleased = fDbrm->releaseTableLock(tableLocks[i].id);
                                            TablelockData::removeTablelockData(sessionID);
                                        }
                                        catch (std::exception&)
                                        {
                                            throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
                                        }
                                        if (lockReleased)
                                        {
                                            sessionManager.rolledback(txnId);
                                            ostringstream oss;
                                            oss << "DMLProc rolled back idle transaction " <<tableLocks[i].ownerTxnID << " and table lock id " << tableLocks[i].id << " is released.";
                                            DMLProcessor::log(oss.str(), logging::LOG_TYPE_INFO);
                                        }
                                        else
                                        {
                                            ostringstream oss;
                                            oss << "DMLProc rolled back idle transaction " <<tableLocks[i].ownerTxnID << " and tble lock id " << tableLocks[i].id << " is not released.";
                                            DMLProcessor::log(oss.str(), logging::LOG_TYPE_INFO);
                                        }
                                    }
                                    else
                                    {
                                        ostringstream oss;
                                        oss << " problem with bulk rollback of idle transaction " << tableLocks[i].ownerTxnID << "and DBRM is setting to readonly and table lock is not released: " << errorMsg;
                                        DMLProcessor::log(oss.str(), logging::LOG_TYPE_CRITICAL);
                                        rc = fDbrm->setReadOnly(true);
                                    }
                                }
                                else
                                {
                                    ostringstream oss;
                                    oss << " problem with rollback of idle transaction " << tableLocks[i].ownerTxnID << "and DBRM is setting to readonly and table lock is not released: " << errorMsg;
                                    DMLProcessor::log(oss.str(), logging::LOG_TYPE_CRITICAL);
                                    rc = fDbrm->setReadOnly(true);
                                }   
                            }
                        }
                    }
                }

                // If there are any abandonded transactions without locks
                // release them.
                int len;
                boost::shared_array<BRM::SIDTIDEntry> activeTxns = sessionManager.SIDTIDMap(len);

                for (int i=0; i < len; i++)
                {
                    // If there isn't a table lock for this transaction, roll it back. Otherwise, assume
                    // it has an active processor or is not DML initiated and leave it alone. It's someone 
                    // else's concern.
                    bool bFoundit = false;
                    for (uint32_t j = 0; j < tableLocks.size(); ++j)
                    {
                        if (tableLocks[j].ownerTxnID == activeTxns[i].txnid.id)
                        {
                            bFoundit = true;
                            break;
                        }
                    }
                    if (!bFoundit && activeTxns[i].txnid.valid)
                    {
                        rollbackProcessor.rollBackTransaction(uniqueId, activeTxns[i].txnid, activeTxns[i].sessionid, errorMsg);
                        sessionManager.rolledback(activeTxns[i].txnid);
                        ++idleTransCount;
                        ostringstream oss;
                        oss << "DMLProc rolled back idle transaction with no tablelock" <<tableLocks[i].ownerTxnID;
                        DMLProcessor::log(oss.str(), logging::LOG_TYPE_INFO);
                    }
                }
                if (idleTransCount > 0)
                {
                    ostringstream oss2;
                    oss2 << "DMLProc has rolled back " << idleTransCount << " idle transactions.";
                    DMLProcessor::log(oss2.str(), logging::LOG_TYPE_INFO);
                }
            }
        }
    }
	DBRM* fDbrm;
};

PackageHandler::PackageHandler(const messageqcpp::IOSocket& ios, 
							   boost::shared_ptr<messageqcpp::ByteStream> bs, 
							   uint8_t packageType,
							   joblist::DistributedEngineComm *ec, 
							   uint64_t maxDeleteRows,
							   uint32_t sessionID, 
							   execplan::CalpontSystemCatalog::SCN txnId,
							   DBRM * aDbrm) : 
		fIos(ios), fPackageType(packageType), fEC(ec), fMaxDeleteRows(maxDeleteRows), fSessionID(sessionID), fTxnid(txnId), fDbrm(aDbrm)
{
	fByteStream = bs;
}

PackageHandler::~PackageHandler()
{
	//cout << "In destructor" << endl;
}

void PackageHandler::run()
{
	ResourceManager frm;
	dmlpackageprocessor::DMLPackageProcessor::DMLResult result;
	result.result = dmlpackageprocessor::DMLPackageProcessor::NO_ERROR;
	//cout << "PackageHandler handling ";
	std::string stmt;
	unsigned DMLLoggingId = 21;
	try
	{

		switch( fPackageType )
		{
			case dmlpackage::DML_INSERT:
				{
					// build an InsertDMLPackage from the bytestream
					//cout << "an INSERT package" << endl;
					dmlpackage::InsertDMLPackage insertPkg;
					//boost::shared_ptr<messageqcpp::ByteStream> insertBs (new messageqcpp::ByteStream);
					messageqcpp::ByteStream bsSave = *(fByteStream.get());
					insertPkg.read(*(fByteStream.get()));
					//cout << "This is batch insert " << insertPkg->get_isBatchInsert() << endl;
					if (insertPkg.get_isBatchInsert())
					{
						//cout << "This is batch insert " << endl;
						//boost::shared_ptr<messageqcpp::ByteStream> insertBs (new messageqcpp::ByteStream(fByteStream));
						BatchInsertProc* batchProcessor = NULL;
						{
							boost::mutex::scoped_lock lk(DMLProcessor::batchinsertProcessorMapLock);
						
							std::map<uint32_t, BatchInsertProc*>::iterator batchIter = DMLProcessor::batchinsertProcessorMap.find(fSessionID);
	
							if (batchIter == DMLProcessor::batchinsertProcessorMap.end())
							{
								batchProcessor = new BatchInsertProc(insertPkg.get_isAutocommitOn(), insertPkg.getTableOid(), fTxnid, fDbrm);
								DMLProcessor::batchinsertProcessorMap[fSessionID] = batchProcessor;
							//cout << "batchProcessor is created " << batchProcessor << endl;
							}
							else
							{
								batchProcessor = batchIter->second;
							//cout << "Found batchProcessor " << batchProcessor << endl;
							}
						}
						
						if ( insertPkg.get_Logging() )
						{
							LoggingID logid( DMLLoggingId, insertPkg.get_SessionID(), fTxnid);
							logging::Message::Args args1;
							logging::Message msg(1);
							args1.add("Start SQL statement: ");
							ostringstream oss;
							oss << insertPkg.get_SQLStatement() << "; |" << insertPkg.get_SchemaName() <<"|";
							args1.add(oss.str());
							msg.format( args1 );
							logging::Logger logger(logid.fSubsysID);
							logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
							TablelockData * tablelockData = TablelockData::makeTablelockData(insertPkg.get_SessionID());
							uint64_t tableLockId = tablelockData->getTablelockId(insertPkg.getTableOid());
							//cout << "Processing table oid " << insertPkg.getTableOid() << " for transaction "<< (int)fTxnid << endl;
							if (tableLockId == 0)
							{
								//cout << "Grabing tablelock for batchProcessor " << batchProcessor << endl;
								tableLockId = batchProcessor->grabTableLock(insertPkg.get_SessionID());
								if (tableLockId == 0)
								{
									BRM::TxnID brmTxnID;
									brmTxnID.id = fTxnid;
									brmTxnID.valid = true;
									sessionManager.rolledback(brmTxnID);
									string errMsg;
									int rc = 0;
									batchProcessor->getError(rc, errMsg);
									result.result = DMLPackageProcessor::TABLE_LOCK_ERROR;
									logging::Message::Args args;
									logging::Message message(1);
									args.add("Insert Failed: ");
									args.add(errMsg);
									args.add("");
									args.add("");
									message.format(args);
									result.message = message;
									break;
								}
							
								if (tableLockId > 0)
									tablelockData->setTablelock(insertPkg.getTableOid(), tableLockId);	
							}			
						}	
						if (insertPkg.get_Logending() && insertPkg.get_Logging()) //only one batch need to be processed.
						{
							//cout << "dmlprocessor add last pkg" << endl; 
							//need to add error handling.
							batchProcessor->addPkg(bsSave);
							batchProcessor->sendFirstBatch();
							batchProcessor->receiveOutstandingMsg();
							//@Bug 5162. Get the correct error message before the last message.
							string errMsg;
							int rc = 0;
							batchProcessor->getError(rc, errMsg);
							batchProcessor->sendlastBatch();
							batchProcessor->receiveAllMsg();
						
							if (rc == DMLPackageProcessor::IDBRANGE_WARNING)
							{
								result.result = dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING;
								LoggingID logid( DMLLoggingId, insertPkg.get_SessionID(), fTxnid);
								logging::Message::Args args1;
								logging::Message msg(1);
								args1.add("End SQL statement with warnings");
								msg.format( args1 );
								logging::Logger logger(logid.fSubsysID);
								logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
								logging::logDML(insertPkg.get_SessionID(), fTxnid, insertPkg.get_SQLStatement()+ ";", insertPkg.get_SchemaName());
								logging::Message::Args args;
								logging::Message message(1);
								args.add(errMsg);
								args.add("");
								args.add("");
								message.format(args);
								result.message = message;
							}
							else if ( rc != 0)
							{
								result.result = dmlpackageprocessor::DMLPackageProcessor::INSERT_ERROR;
								logging::Message::Args args;
								logging::Message message(1);
								cout << "Got error in the end of one batchinsert." << endl;
								args.add("Insert Failed: ");
								args.add(errMsg);
								args.add("");
								args.add("");
								message.format(args);
								result.message = message;
								LoggingID logid( DMLLoggingId, insertPkg.get_SessionID(), fTxnid);
								logging::Message::Args args1;
								logging::Message msg(1);
								args1.add("End SQL statement with error");
								msg.format( args1 );
								logging::Logger logger(logid.fSubsysID);
								logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
								logging::logDML(insertPkg.get_SessionID(), fTxnid, insertPkg.get_SQLStatement()+ ";", insertPkg.get_SchemaName());
							}
							else
							{
							//	if (!insertPkg.get_isAutocommitOn())
							//	{
							//		batchProcessor->setHwm();
							//	}
								LoggingID logid( DMLLoggingId, insertPkg.get_SessionID(), fTxnid);
								logging::Message::Args args1;
								logging::Message msg(1);
								args1.add("End SQL statement");
								msg.format( args1 );
								logging::Logger logger(logid.fSubsysID);
								logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
								logging::logDML(insertPkg.get_SessionID(), fTxnid, insertPkg.get_SQLStatement()+ ";", insertPkg.get_SchemaName());
							}
							//remove the batch insert object
							{
								boost::mutex::scoped_lock lk(DMLProcessor::batchinsertProcessorMapLock);
							
								std::map<uint32_t, BatchInsertProc*>::iterator batchIter = DMLProcessor::batchinsertProcessorMap.find(fSessionID);
								if (batchIter != DMLProcessor::batchinsertProcessorMap.end())
								{
									delete batchIter->second;
									DMLProcessor::batchinsertProcessorMap.erase(fSessionID);
								}
							}
						}
						else if (insertPkg.get_Logending()) //Last batch
						{
							int rc = 0;
							string errMsg;
							batchProcessor->getError(rc, errMsg);
							//cout <<"dmlprocessor received last pkg from mysql rc == " << rc << endl;
							if (( rc == 0) || (rc == DMLPackageProcessor::IDBRANGE_WARNING))
							{
								//cout << " rc = " << rc << endl;
								batchProcessor->addPkg(bsSave);
								batchProcessor->sendNextBatch();
								batchProcessor->receiveOutstandingMsg();
								//@Bug 5162. Get the correct error message before the last message.
								batchProcessor->getError(rc, errMsg);
								batchProcessor->sendlastBatch();
								batchProcessor->receiveAllMsg();
								
								if (rc == DMLPackageProcessor::IDBRANGE_WARNING)
								{
									result.result = dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING;
									LoggingID logid( DMLLoggingId, insertPkg.get_SessionID(), fTxnid);
									logging::Message::Args args1;
									logging::Message msg(1);
									args1.add("End SQL statement with warnings");
									msg.format( args1 );
									logging::Logger logger(logid.fSubsysID);
									logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
									logging::logDML(insertPkg.get_SessionID(), fTxnid, insertPkg.get_SQLStatement()+ ";", insertPkg.get_SchemaName());
									logging::Message::Args args;
									logging::Message message(1);
									args.add(errMsg);
									args.add("");
									args.add("");
									message.format(args);
									result.message = message;
								}
								else if ( rc != 0)
								{
									//cout << "Got error in the end of last batchinsert. error message is " << errMsg << endl;
									result.result = dmlpackageprocessor::DMLPackageProcessor::INSERT_ERROR;
									logging::Message::Args args;
									logging::Message message(1);
									args.add("Insert Failed: ");
									args.add(errMsg);
									args.add("");
									args.add("");
									message.format(args);
									result.message = message;
									LoggingID logid( DMLLoggingId, insertPkg.get_SessionID(), fTxnid);
									logging::Message::Args args1;
									logging::Message msg(1);
									args1.add("End SQL statement with error");
									msg.format( args1 );
									logging::Logger logger(logid.fSubsysID);
									logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
									logging::logDML(insertPkg.get_SessionID(), fTxnid, insertPkg.get_SQLStatement()+ ";", insertPkg.get_SchemaName());
								}
								else
								{
									LoggingID logid( DMLLoggingId, insertPkg.get_SessionID(), fTxnid);
									logging::Message::Args args1;
									logging::Message msg(1);
									args1.add("End SQL statement");
									msg.format( args1 );
									logging::Logger logger(logid.fSubsysID);
									logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
									logging::logDML(insertPkg.get_SessionID(), fTxnid, insertPkg.get_SQLStatement()+ ";", insertPkg.get_SchemaName());
								}
								//cout << "finished batch insert" << endl;
							}
							else
							{
								//error occured. Receive all outstanding messages nefore erroring out.
								batchProcessor->receiveOutstandingMsg();
								batchProcessor->sendlastBatch(); //needs to flush files
								batchProcessor->receiveAllMsg();
								result.result = dmlpackageprocessor::DMLPackageProcessor::INSERT_ERROR;
								//cout << "Got error in the end of batchinsert2. error msg is " << errMsg<< endl;
								logging::Message::Args args;
								logging::Message message(1);
								args.add("Insert Failed: ");
								args.add(errMsg);
								args.add("");
								args.add("");
								message.format(args);
								result.message = message;
								LoggingID logid( DMLLoggingId, insertPkg.get_SessionID(), fTxnid);
								logging::Message::Args args1;
								logging::Message msg(1);
								args1.add("End SQL statement with error");
								msg.format( args1 );
								logging::Logger logger(logid.fSubsysID);
								logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
								logging::logDML(insertPkg.get_SessionID(), fTxnid, insertPkg.get_SQLStatement()+ ";", insertPkg.get_SchemaName());
							}
							//remove from map
							{
								boost::mutex::scoped_lock lk(DMLProcessor::batchinsertProcessorMapLock);
								std::map<uint32_t, BatchInsertProc*>::iterator batchIter = DMLProcessor::batchinsertProcessorMap.find(fSessionID);
								if (batchIter != DMLProcessor::batchinsertProcessorMap.end())
								{
								//cout << "Batchinsertprcessor is deleted. " << batchIter->second << endl;
									delete batchIter->second;
									DMLProcessor::batchinsertProcessorMap.erase(fSessionID);
								}
							}
							
						}
						else
						{
							int rc = 0;
							string errMsg;
							batchProcessor->getError(rc, errMsg);
							if (rc == DMLPackageProcessor::IDBRANGE_WARNING)
							{
								result.result = dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING;
							}
							else if ( rc !=0) 
							{
								result.result = dmlpackageprocessor::DMLPackageProcessor::INSERT_ERROR;
								//@Bug 
								//cout << "Got error during batchinsert. with message " << errMsg << endl;
								logging::Message::Args args;
								logging::Message message(6);
								args.add( errMsg );
								message.format( args );
								result.message = message;
								batchProcessor->receiveOutstandingMsg();
								batchProcessor->sendlastBatch(); //needs to flush files
								//cout << "Last batch is sent to WES." << endl;
								batchProcessor->receiveAllMsg();
								LoggingID logid( DMLLoggingId, insertPkg.get_SessionID(), fTxnid);
								logging::Message::Args args1;
								logging::Message msg(1);
								args1.add("End SQL statement with error");
								msg.format( args1 );
								logging::Logger logger(logid.fSubsysID);
								logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
								//remove from map
								{
									boost::mutex::scoped_lock lk(DMLProcessor::batchinsertProcessorMapLock);
									std::map<uint32_t, BatchInsertProc*>::iterator batchIter = DMLProcessor::batchinsertProcessorMap.find(fSessionID);
									if (batchIter != DMLProcessor::batchinsertProcessorMap.end())
									{
									//cout << "Batchinsertprcessor is deleted. " << batchIter->second << endl;
										delete batchIter->second;
										DMLProcessor::batchinsertProcessorMap.erase(fSessionID);
									}
								}
								break;
							}
							
							batchProcessor->addPkg(bsSave);	
							batchProcessor->sendNextBatch();
                            break;
						}
					}
					else
					{
						//insertPkg.readTable(*(fByteStream.get()));
						insertPkg.set_TxnID(fTxnid);
						fProcessor.reset(new dmlpackageprocessor::InsertPackageProcessor(fDbrm, insertPkg.get_SessionID()));
						result = fProcessor->processPackage(insertPkg);
					}
				}
				break;

			case dmlpackage::DML_UPDATE:
				{
					// build an UpdateDMLPackage from the bytestream
					//cout << "an UPDATE package" << endl;
					boost::scoped_ptr<dmlpackage::UpdateDMLPackage> updatePkg(new dmlpackage::UpdateDMLPackage());
					updatePkg->read(*(fByteStream.get()));
					updatePkg->set_TxnID(fTxnid);
					// process it
					//@Bug 1341. Don't remove calpontsystemcatalog from this 
					//session to take advantage of cache. 
					fProcessor.reset(new dmlpackageprocessor::UpdatePackageProcessor(fDbrm, updatePkg->get_SessionID()));
					fProcessor->setEngineComm(fEC);
					fProcessor->setRM( &frm);
					idbassert( fTxnid != 0);
					result = fProcessor->processPackage(*(updatePkg.get())) ;
				}
				break;

			case dmlpackage::DML_DELETE:
				{
					boost::scoped_ptr<dmlpackage::DeleteDMLPackage> deletePkg(new dmlpackage::DeleteDMLPackage());
					deletePkg->read(*(fByteStream.get()));
					deletePkg->set_TxnID(fTxnid);
					// process it
					//@Bug 1341. Don't remove calpontsystemcatalog from this session to take advantage of cache.
					fProcessor.reset(new dmlpackageprocessor::DeletePackageProcessor(fDbrm, deletePkg->get_SessionID()));
					fProcessor->setEngineComm(fEC);
					fProcessor->setRM( &frm);
					idbassert( fTxnid != 0);
					result = fProcessor->processPackage(*(deletePkg.get())) ;
				}
				break;
			case dmlpackage::DML_COMMAND:
				{
					// build a CommandDMLPackage from the bytestream
					//cout << "a COMMAND package" << endl;
					dmlpackage::CommandDMLPackage commandPkg;
					commandPkg.read(*(fByteStream.get()));
					stmt = commandPkg.get_DMLStatement();
					boost::algorithm::to_upper(stmt);
					trim(stmt);
					if (stmt == "CLEANUP")
					{
						execplan::CalpontSystemCatalog::removeCalpontSystemCatalog(commandPkg.get_SessionID());
						execplan::CalpontSystemCatalog::removeCalpontSystemCatalog(commandPkg.get_SessionID() | 0x80000000);
					}				
					else
					{
						// process it
						//@Bug 1341. Don't remove calpontsystemcatalog from this session to take advantage of cache.
						fProcessor.reset(new dmlpackageprocessor::CommandPackageProcessor(fDbrm, commandPkg.get_SessionID()));
					
						//cout << "got command " << stmt << " for session " << commandPkg.get_SessionID() << endl;
						result = fProcessor->processPackage(commandPkg);
					}
				}
				break;
		}
		//Log errors
		if (   (result.result != dmlpackageprocessor::DMLPackageProcessor::NO_ERROR) 
			&& (result.result != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING) 
            && (result.result != dmlpackageprocessor::DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR)
            && (result.result != dmlpackageprocessor::DMLPackageProcessor::VB_OVERFLOW_ERROR) )
		{
			logging::LoggingID lid(21);
			logging::MessageLog ml(lid);

			ml.logErrorMessage( result.message );
		}
		else if (result.result == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING)
		{
			logging::LoggingID lid(21);
			logging::MessageLog ml(lid);

			ml.logWarningMessage( result.message );
		}
		
		// send back the results
		messageqcpp::ByteStream results;
		messageqcpp::ByteStream::octbyte rowCount = result.rowCount;
		messageqcpp::ByteStream::byte retval = result.result;
		results << retval;
		results << rowCount;
		results << result.message.msg();
		results << result.tableLockInfo; // ? connector does not get
		// query stats
		results << result.queryStats;
		results << result.extendedStats;
		results << result.miniStats;
		result.stats.serialize(results);
		fIos.write(results);
		//Bug 5226. dmlprocessor thread will close the socket to mysqld.
		//if (stmt == "CLEANUP")
		//	fIos.close();
	}
	catch(...)
	{
		fIos.close();
	}
}

void PackageHandler::rollbackPending()
{
	if (fProcessor.get() == NULL)
	{
		// This happens when batch insert
		return;
	}

	fProcessor->setRollbackPending(true);

    ostringstream oss;
    oss << "PackageHandler::rollbackPending: Processing DMLPackage.";
    DMLProcessor::log(oss.str(), logging::LOG_TYPE_DEBUG);
}

void added_a_pm(int)
{
	DistributedEngineComm *dec;
	ResourceManager rm;
	dec = DistributedEngineComm::instance(rm);
	dec->Setup();
}

DMLServer::DMLServer(int packageMaxThreads, int packageWorkQueueSize, DBRM* dbrm) :
	fPackageMaxThreads(packageMaxThreads), fPackageWorkQueueSize(packageWorkQueueSize), fDbrm(dbrm)
{
	fMqServer.reset(new MessageQueueServer("DMLProc"));

	fDmlPackagepool.setMaxThreads(fPackageMaxThreads);
	fDmlPackagepool.setQueueSize(fPackageWorkQueueSize);
}

void DMLServer::start()
{
	messageqcpp::IOSocket ios;
	uint32_t nextID = 1;

	try
	{
		// CancellationThread is for telling all active transactions
		// to quit working because the system is either going down
		// or going into write suspend mode
		CancellationThread cancelObject(fDbrm);
		boost::thread cancelThread(cancelObject);

		cout << "DMLProc is ready..." << endl;
		for (;;)
		{
			ios = fMqServer->accept();
			ios.setSockID(nextID++);
			fDmlPackagepool.invoke(DMLProcessor(ios, fDbrm));
		}
		cancelThread.join();
	}
	catch (...)
	{
	}
}

DMLProcessor::DMLProcessor(messageqcpp::IOSocket ios, BRM::DBRM* aDbrm) :
	fIos(ios), fDbrm(aDbrm)
{
	csc = CalpontSystemCatalog::makeCalpontSystemCatalog();
	csc->identity(CalpontSystemCatalog::EC);
}

void DMLProcessor::operator()()
{
	bool bIsDbrmUp = true;

	try
    {
        boost::shared_ptr<messageqcpp::ByteStream> bs1	(new messageqcpp::ByteStream());
		//messageqcpp::ByteStream bs;
        uint8_t packageType;

		ResourceManager rm;
		DistributedEngineComm* fEC = DistributedEngineComm::instance(rm);
		
		uint64_t maxDeleteRows = rm.getDMLMaxDeleteRows();
		
		bool concurrentSupport = true;
		string concurrentTranStr = config::Config::makeConfig()->getConfig("SystemConfig", "ConcurrentTransactions");
		if ( concurrentTranStr.length() != 0 )
		{
			if ((concurrentTranStr.compare("N") == 0) || (concurrentTranStr.compare("n") == 0))
				concurrentSupport = false;
		}
		
#ifndef _MSC_VER
		struct sigaction ign;
		memset(&ign, 0, sizeof(ign));
		ign.sa_handler = added_a_pm;
		sigaction(SIGHUP, &ign, 0);
#endif
		fEC->Open();
	
        for (;;)
        {
			//cout << "DMLProc is waiting for a Calpont DML Package on " << fIos.getSockID() << endl;
			try
			{
				bs1.reset(new messageqcpp::ByteStream(fIos.read()));
				//cout << "received from mysql socket " << fIos.getSockID() << endl;
			}
			catch (std::exception&)
			{
				//This is an I/O error from InetStreamSocket::read(), just close and move on...
				//cout << "runtime error during read on " << fIos.getSockID() << " " << ex.what() << endl;
				bs1->reset();
			}
			catch (...)
			{
				//cout << "... error during read " << fIos.getSockID() << endl;
				// all this throw does is cause this thread to silently go away. I doubt this is the right
				//  thing to do...
				throw;
			}

			if (!bs1 || bs1->length() == 0)
			{
				//cout << "Read 0 bytes. Closing connection " << fIos.getSockID() << endl;
				fIos.close();
				break;
			}

            uint32_t sessionID;
            *bs1 >> sessionID;
            *bs1 >> packageType;
//cout << "DMLProc received pkg. sessionid:type = " << sessionID <<":"<<(int)packageType << endl;
			uint32_t stateFlags;
			messageqcpp::ByteStream::byte status=255;
			messageqcpp::ByteStream::octbyte rowCount = 0;
			if (fDbrm->getSystemState(stateFlags) > 0)		// > 0 implies succesful retrieval. It doesn't imply anything about the contents
			{
				messageqcpp::ByteStream results;
				const char* responseMsg=0;
				bool bReject = false;
				// Check to see if we're in write suspended mode
				// If so, we can't process the request.
				if (stateFlags & SessionManagerServer::SS_SUSPENDED)
				{
					status =  DMLPackageProcessor::NOT_ACCEPTING_PACKAGES;
					responseMsg = "Writing to the database is disabled.";
					bReject = true;
				}
				// Check to see if we're in write suspend or shutdown pending mode
				if (packageType != dmlpackage::DML_COMMAND) // Not a commit or rollback
				{
					if (stateFlags & SessionManagerServer::SS_SUSPEND_PENDING
					 || stateFlags & SessionManagerServer::SS_SHUTDOWN_PENDING)
					{
						if (stateFlags & SessionManagerServer::SS_SUSPEND_PENDING)
						{
							responseMsg = "Writing to the database is disabled.";
						}
						else
						{
							responseMsg = "The database is being shut down.";
						}
						// Refuse all non active tranasactions
						// Check the rollback flag
						// -- Set: Rollback active transactions.
						// -- Not set: Allow active transactions.
						if (sessionManager.isTransactionActive(sessionID, bIsDbrmUp))
						{
							if (stateFlags & SessionManagerServer::SS_ROLLBACK)
							{
								status =  DMLPackageProcessor::JOB_CANCELED;
								bReject = true;
							}
						}
						else
						{
							status =  DMLPackageProcessor::NOT_ACCEPTING_PACKAGES;
							bReject = true;
						}
					}
					if (bReject)
					{
						// For batch insert, we need to send a lastpkg message
						// to batchInsertProcessor so it can clean things up.
						if (packageType == dmlpackage::DML_INSERT)
						{
							// build an InsertDMLPackage from the bytestream
							// We need the flags from the package to know what
							// type of package we're dealing with before we can
							// take special action for the last package of a
							// batch insert.
							dmlpackage::InsertDMLPackage insertPkg;
							messageqcpp::ByteStream bsSave = *(bs1.get());
							insertPkg.read(*(bs1.get()));
							BatchInsertProc* batchInsertProcessor = NULL;
							if (insertPkg.get_isBatchInsert() && insertPkg.get_Logending())
							{
								{
									boost::mutex::scoped_lock lk(DMLProcessor::batchinsertProcessorMapLock);
									std::map<uint32_t, BatchInsertProc*>::iterator batchIter = DMLProcessor::batchinsertProcessorMap.find(sessionID);
	
									if (batchIter != DMLProcessor::batchinsertProcessorMap.end()) //The first batch, no need to do anything
									{
									
										batchInsertProcessor = batchIter->second;
										batchInsertProcessor->addPkg(bsSave);
								
										batchInsertProcessor->sendlastBatch();
										batchInsertProcessor->receiveAllMsg();
								
								
										if (!insertPkg.get_isAutocommitOn())
										{
											batchInsertProcessor->setHwm();
										}
									
										batchIter = DMLProcessor::batchinsertProcessorMap.find(sessionID);
										if (batchIter != DMLProcessor::batchinsertProcessorMap.end())
										{
										DMLProcessor::batchinsertProcessorMap.erase(sessionID);
										}
									}
								}
							}
						}
						results << status;
						results << rowCount;
						logging::Message::Args args;
						logging::Message message(2);
						args.add(responseMsg);
						message.format( args );
						results << message.msg();
						fIos.write(results);
						continue;
					}
				}
			}

			// This section is to check to see if the user hit CTRL+C while the
			// DML was processing If so, the sessionID will be found in
			// packageHandlerMap and we can set rollbackPending in the
			// associated packageHandler. Other than CTRL+C, we should never
			// find our own sessionID in the map.
			// This mechanism may prove useful for other things, so the above
			// comment may change.
			{
				boost::mutex::scoped_lock lk2(DMLProcessor::packageHandlerMapLock);
				DMLProcessor::PackageHandlerMap_t::iterator phIter = packageHandlerMap.find(sessionID);
				if (phIter != packageHandlerMap.end())
				{
					if (packageType == dmlpackage::DML_COMMAND)
					{
						dmlpackage::CommandDMLPackage commandPkg;
						commandPkg.read(*(bs1.get()));
						std::string stmt = commandPkg.get_DMLStatement();
						boost::algorithm::to_upper(stmt);
						trim(stmt);
						if (stmt == "CTRL+C")
						{
							phIter->second->rollbackPending();
							fIos.close();
							break;
						}
					}
					else
					{
						// If there's a PackageHandler already working for this 
						// sessionID, we have a problem. Reject this package
						messageqcpp::ByteStream results;
						ostringstream oss;
						oss << "Received a DML command for session " << sessionID <<
						" while still processing a command for the same sessionID";
						results << static_cast<messageqcpp::ByteStream::byte>(DMLPackageProcessor::DEAD_LOCK_ERROR);
						results << static_cast<messageqcpp::ByteStream::octbyte>(0);	// rowcount
						logging::Message::Args args;
						logging::Message message(2);
						args.add(oss.str());
						message.format( args );
						logging::LoggingID lid(20);
						logging::MessageLog ml(lid);
						ml.logErrorMessage(message);
						results << message.msg();
						fIos.write(results);
						continue;
					}
				}
			}
			//cout << "   got a ";
			switch (packageType)
			{
			case dmlpackage::DML_INSERT:
			   //cout << "DML_INSERT";
			   break;
			case dmlpackage::DML_UPDATE:
			   //cout << "DML_UPDATE";
			   break;
			case dmlpackage::DML_DELETE:
			   //cout << "DML_DELETE";
			   break;
			case dmlpackage::DML_COMMAND:
			   //cout << "DML_COMMAND";		
			   break;
			case dmlpackage::DML_INVALID_TYPE:
			   //cout << "DML_INVALID_TYPE";
			   break;
			default:
			   //cout << "UNKNOWN";
			   break;
			}
		    //cout << " package" << endl;
			
			BRM::TxnID txnid;
			if (!concurrentSupport) 
			{
				//Check if any other active transaction
				bool anyOtherActiveTransaction = true;
				BRM::SIDTIDEntry blockingsid;

				//For logout commit trigger
				if ( packageType == dmlpackage::DML_COMMAND )
				{
					anyOtherActiveTransaction = false;
				}           		
				int i = 0;
				int waitPeriod = 10;
				//@Bug 2487 Check transaction map every 1/10 second

				int sleepTime = 100; // sleep 100 milliseconds between checks
				int numTries = 10;  // try 10 times per second
			

				string waitPeriodStr = config::Config::makeConfig()->getConfig("SystemConfig", "WaitPeriod");
				if ( waitPeriodStr.length() != 0 )
					waitPeriod = static_cast<int>(config::Config::fromText(waitPeriodStr));
				
				numTries = 	waitPeriod * 10;
				struct timespec rm_ts;

				rm_ts.tv_sec = sleepTime/1000; 
				rm_ts.tv_nsec = sleepTime%1000 *1000000;
				//cout << "starting i = " << i << endl;
				//txnid = sessionManager.getTxnID(sessionID);	
				while (anyOtherActiveTransaction)
				{
					anyOtherActiveTransaction = sessionManager.checkActiveTransaction( sessionID, bIsDbrmUp,
						blockingsid );
					//cout << "session " << sessionID << " with package type " << (int)packageType << " got anyOtherActiveTransaction " << anyOtherActiveTransaction << endl;
					if (anyOtherActiveTransaction) 
					{
						for ( ; i < numTries; i++ )
						{
#ifdef _MSC_VER
							Sleep(rm_ts.tv_sec * 1000);
#else
							struct timespec abs_ts;
							//cout << "session " << sessionID << " nanosleep on package type " << (int)packageType << endl;
							do
							{
								abs_ts.tv_sec = rm_ts.tv_sec; 
								abs_ts.tv_nsec = rm_ts.tv_nsec;
							} 
							while(nanosleep(&abs_ts,&rm_ts) < 0);
#endif
							anyOtherActiveTransaction = sessionManager.checkActiveTransaction( sessionID, bIsDbrmUp,
								blockingsid );
							if ( !anyOtherActiveTransaction )
							{
								txnid = sessionManager.getTxnID(sessionID);
								//cout << "Ready to process type " << (int)packageType << " with txd " << txnid << endl;
								if ( !txnid.valid )
								{
									txnid = sessionManager.newTxnID(sessionID, true);
									if (txnid.valid) {
										//cout << "Ready to process type " << (int)packageType << " for session "<< sessionID << " with new txnid " << txnid.id << endl;
										anyOtherActiveTransaction = false;
										break;
									}
									else
									{
										anyOtherActiveTransaction = true;
									}
								}
								else
								{
									anyOtherActiveTransaction = false;
									//cout << "already have transaction to process type " << (int)packageType << " for session "<< sessionID <<" with existing txnid " << txnid.id << endl;
									break;
								}
							}
						}
						//cout << "ending i = " << i << endl;
					}
					else
					{
						//cout << "Ready to process type " << (int)packageType << endl;
						txnid = sessionManager.getTxnID(sessionID);
						if ( !txnid.valid )
						{
							txnid = sessionManager.newTxnID(sessionID, true);
							if (txnid.valid) {
							//cout << "later Ready to process type " << (int)packageType << " for session "<< sessionID << " with new txnid " << txnid.id << endl;
								anyOtherActiveTransaction = false;
							}
							else
							{
								anyOtherActiveTransaction = true;
							//cout << "Cannot get txnid for  process type " << (int)packageType << " for session "<< sessionID << endl;
							}
						}
						else
						{
							anyOtherActiveTransaction = false;
							//cout << "already have transaction to process type " << (int)packageType << " for session "<< sessionID <<" with txnid " << txnid.id << endl;
							break;
						}
					}
					
					if ((anyOtherActiveTransaction) && (i >= numTries))
					{
						//cout << " Erroring out on package type " << (int)packageType << " for session " << sessionID << endl;
						break;  
					}
				}

				if (anyOtherActiveTransaction && (i >= numTries))
				{
					//cout << " again Erroring out on package type " << (int)packageType << endl;
					messageqcpp::ByteStream results;
					//@Bug 2681 set error code for active transaction
					status =  DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR;
					rowCount = 0;
					results << status;
					results << rowCount;
					Message::Args args;
					args.add(static_cast<uint64_t>(blockingsid.sessionid));
					results << IDBErrorInfo::instance()->errorMsg(ERR_ACTIVE_TRANSACTION, args);
					//@Bug 3854 Log to debug.log
					LoggingID logid(20, 0, 0);
					logging::Message::Args args1;
					logging::Message msg(1);
					args1.add(IDBErrorInfo::instance()->errorMsg(ERR_ACTIVE_TRANSACTION, args));
					msg.format( args1 );
					logging::Logger logger(logid.fSubsysID);
					logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
				
					fIos.write(results);
				}
				else
				{
					//cout << "starting processing package type " << (int) packageType << " for session " << sessionID << " with id " << txnid.id << endl;
					shared_ptr<PackageHandler> php(new PackageHandler(fIos, bs1, packageType, fEC, maxDeleteRows, sessionID, txnid.id, fDbrm));
					// We put the packageHandler into a map so that if we receive a
					// message to affect the previous command, we can find it.
					boost::mutex::scoped_lock lk2(DMLProcessor::packageHandlerMapLock, defer_lock);

					lk2.lock();
					packageHandlerMap[sessionID] = php;
					lk2.unlock();

					php->run();		// Operates in this thread.

					lk2.lock();
					packageHandlerMap.erase(sessionID);
					lk2.unlock();
				}
			}
			else
			{
				if (packageType != dmlpackage::DML_COMMAND)
				{
					txnid = sessionManager.getTxnID(sessionID);
					if ( !txnid.valid )
					{
						txnid = sessionManager.newTxnID(sessionID, true);
						if (!txnid.valid) {
							throw std::runtime_error( std::string("Unable to start a transaction. Check critical log.") );
						}
					}
				}
				else
				{
					txnid = sessionManager.getTxnID(sessionID);
				}

				shared_ptr<PackageHandler> php(new PackageHandler(fIos, bs1, packageType, fEC, maxDeleteRows, sessionID, txnid.id, fDbrm));
				// We put the packageHandler into a map so that if we receive a
				// message to affect the previous command, we can find it.
				boost::mutex::scoped_lock lk2(DMLProcessor::packageHandlerMapLock, defer_lock);

				lk2.lock();
				packageHandlerMap[sessionID] = php;
				lk2.unlock();

				php->run();		// Operates in this thread.

				lk2.lock();
				packageHandlerMap.erase(sessionID);
				lk2.unlock();
			}
		}
    }
    catch (std::exception& ex)
    {
        ostringstream oss;
        oss << "DMLProcessor failed on: " << ex.what();
        DMLProcessor::log(oss.str(), logging::LOG_TYPE_DEBUG);
		fIos.close();
    }
    catch (...)
    {
        ostringstream oss;
        oss << "DMLProcessor failed on: processing DMLPackage.";
        DMLProcessor::log(oss.str(), logging::LOG_TYPE_DEBUG);
        cerr << "Caught unknown exception! " << oss.str();
		fIos.close();
    }
}

void RollbackTransactionProcessor::processBulkRollback (BRM::TableLockInfo lockInfo, BRM::DBRM * dbrm, uint64_t uniqueId, 
						OamCache::dbRootPMMap_t& dbRootPMMap, bool & lockReleased)
{
	// Take over ownership of stale lock.
	// Use "DMLProc" as process name, session id and transaction id -1 to distinguish from real DMLProc rollback
	int32_t sessionID = -1;
	int32_t txnid     = -1;
	std::string processName("DMLProc");
	uint32_t processID = ::getpid();
	bool ownerChanged = true;
	lockReleased = true;
	try {
		ownerChanged = dbrm->changeOwner(lockInfo.id, processName, processID, sessionID, txnid);
	}
	catch (std::exception&)
	{
		lockReleased = false;
		throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
	}
	
	if (!ownerChanged)
	{
		lockReleased = false;
		throw std::runtime_error( std::string("Unable to grab lock; lock not found or still in use.") );
	}
	
	//send to all PMs
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	messageqcpp::ByteStream                    bsOut;
	string tableName("");
	fWEClient->addQueue(uniqueId);
	//find the PMs need to send the message to
	std::set<int> pmSet;
	int pmId;
	for (uint i=0; i<lockInfo.dbrootList.size(); i++)
	{	
		pmId = (*dbRootPMMap)[lockInfo.dbrootList[i]]; 
		pmSet.insert(pmId);
	}
	
	if (lockInfo.state == BRM::LOADING)
	{
		bsOut << (messageqcpp::ByteStream::byte)WE_SVR_DML_BULKROLLBACK;
		bsOut << uniqueId;
		bsOut << lockInfo.id;
		bsOut << lockInfo.tableOID;
		bsOut << tableName;
		bsOut << processName;
		std::set<int>::const_iterator iter = pmSet.begin();
		while (iter != pmSet.end())
		{
			fWEClient->write(bsOut, *iter);
			iter++;
		}

		// Wait for "all" the responses, and accumulate any/all errors
		unsigned int pmMsgCnt = 0;
		while (pmMsgCnt < pmSet.size())
		{
			std::string rollbackErrMsg;
			bsIn.reset(new messageqcpp::ByteStream());
			fWEClient->read(uniqueId, bsIn);
			if (bsIn->length() == 0)
			{	
				fWEClient->removeQueue(uniqueId);
				lockReleased = false;
				throw  std::runtime_error("Network error, PM rollback; ");
			}
			else
			{
				messageqcpp::ByteStream::byte rc;
				uint16_t pmNum;
				*bsIn >> rc;
				*bsIn >> rollbackErrMsg;
				*bsIn >> pmNum;

				if (rc != 0)
				{
					fWEClient->removeQueue(uniqueId);
					lockReleased = false;
					throw  std::runtime_error(rollbackErrMsg);
				}
			}
			pmMsgCnt++;
		} // end of while loop to process all responses to bulk rollback

		// If no errors so far, then change state to CLEANUP state.
		// We ignore the return stateChange flag.
		dbrm->changeState( lockInfo.id, BRM::CLEANUP );	
	} // end of (lockInfo.state == BRM::LOADING)
	
	//delete meta data backup rollback files
	bsOut.reset();

	bsOut << (messageqcpp::ByteStream::byte)WE_SVR_DML_BULKROLLBACK_CLEANUP;
	bsOut << uniqueId;
	bsOut << lockInfo.tableOID;
	std::set<int>::const_iterator iter = pmSet.begin();
	while (iter != pmSet.end())
	{
		fWEClient->write(bsOut, *iter);
		iter++;
	}

	// Wait for "all" the responses, and accumulate any/all errors
	unsigned int pmMsgCnt = 0;
	//@Bug 4517 Release tablelock when it is in CLEANUP state
	uint rcCleanup = 0;
	std::string fileDeleteErrMsg;
	while (pmMsgCnt < pmSet.size())
	{
		bsIn.reset(new messageqcpp::ByteStream());
		fWEClient->read(uniqueId, bsIn);
		if (bsIn->length() == 0)
		{
			fWEClient->removeQueue(uniqueId);
			rcCleanup = 1;
			fileDeleteErrMsg = "Network error, PM clean up; ";
		}
		else
		{
			messageqcpp::ByteStream::byte rc;
			uint16_t pmNum;
			*bsIn >> rc;
			*bsIn >> fileDeleteErrMsg;
			*bsIn >> pmNum;

			if ((rc != 0) && (rcCleanup == 0))
			{
				fWEClient->removeQueue(uniqueId);
				rcCleanup = rc;
			}
		}
		pmMsgCnt++;
	} // end of while loop to process all responses to rollback cleanup
	fWEClient->removeQueue(uniqueId);
	// We ignore return release flag from releaseTableLock().	
	dbrm->releaseTableLock( lockInfo.id );
	
	if (rcCleanup != 0)
		throw  std::runtime_error(fileDeleteErrMsg);
}

void DMLProcessor::log(const std::string& msg, logging::LOG_TYPE level)
{
	logging::Message::Args args;
	logging::Message message(2);
	args.add(msg);
	message.format(args);
	logging::LoggingID lid(20);
	logging::MessageLog ml(lid);
	switch (level)
	{
		case LOG_TYPE_DEBUG:
			ml.logDebugMessage(message);
			break;
		case LOG_TYPE_INFO:
			ml.logInfoMessage(message);
			break;
		case LOG_TYPE_WARNING:
			ml.logWarningMessage(message);
			break;
		case LOG_TYPE_ERROR:
			ml.logErrorMessage(message);
			break;
		case LOG_TYPE_CRITICAL:
			ml.logCriticalMessage(message);
			break;
	}
}

} //namespace dmlprocessor
// vim:ts=4 sw=4:


