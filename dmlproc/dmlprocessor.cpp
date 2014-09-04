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
*   $Id: dmlprocessor.cpp 1003 2013-06-26 15:04:55Z dcathey $
*
*
***********************************************************************/
/** @file */
#include "configcpp.h"
#include <signal.h>

#define      SERIALIZE_DDL_DML_CPIMPORT    1
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
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
std::map<uint32_t, PackageHandler*> DMLProcessor::packageHandlerMap;
boost::mutex DMLProcessor::packageHandlerMapLock;

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
    CancellationThread()
    {}
    void operator()()
    {
        bool bDoingRollback = false;
        bool bRollback = false;
        bool bForce = false;
        int  iShutdown;
        ostringstream oss;
        DBRM dbrm;
        std::vector<BRM::TableLockInfo> tableLocks;
        BRM::TxnID txnId;
        std::map<uint32_t, PackageHandler*>::iterator phIter;
        uint32_t sessionID;
        int rc = 0;

        while (true)
        {
            usleep(1000000);    // 1 seconds
            // Check to see if someone has ordered a shutdown or suspend with rollback.
            iShutdown = dbrm.getSystemShutdownPending(bRollback, bForce);
            if (bDoingRollback && bRollback)
            {
                continue;
                // We've already started the rollbacks. Don't start again.
            }
            bDoingRollback = false;
            if (bRollback)
            {
                RollbackTransactionProcessor rollbackProcessor;
                SessionManager sessionManager;
                uint64_t uniqueId = dbrm.getUnique64();
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
                DMLProcessor::packageHandlerMapLock.lock();
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
                if (dbrm.isReadWrite())
                {
                    continue;
                }

                // Check for any open DML transactions that don't currently have
                // a processor
                tableLocks = dbrm.getAllTableLocks();
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
                                    vector<LBID_t> lbidList;
                                    dbrm.getUncommittedExtentLBIDs(static_cast<VER_t>(txnId.id), lbidList);
                                    vector<LBID_t>::const_iterator iter = lbidList.begin();
                                    vector<LBID_t>::const_iterator end = lbidList.end();
                                    while (iter != end)
                                    {
                                        dbrm.setExtentMaxMin(*iter, numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max(), -1);
                                        ++iter;
                                    }

                                    //@Bug 4524. In case it is batchinsert, call bulkrollback.
                                    rc = rollbackProcessor.rollBackBatchAutoOnTransaction(uniqueId, txnId, sessionID, tableLocks[i].tableOID, errorMsg);
                                    if (rc == 0)
                                    {
                                        logging::logCommand(0, tableLocks[i].ownerTxnID, "ROLLBACK;");

                                        bool lockReleased = true;
                                        try
                                        {
                                            lockReleased = dbrm.releaseTableLock(tableLocks[i].id);
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
                                        rc = dbrm.setReadOnly(true);
                                    }
                                }
                                else
                                {
                                    ostringstream oss;
                                    oss << " problem with rollback of idle transaction " << tableLocks[i].ownerTxnID << "and DBRM is setting to readonly and table lock is not released: " << errorMsg;
                                    DMLProcessor::log(oss.str(), logging::LOG_TYPE_CRITICAL);
                                    rc = dbrm.setReadOnly(true);
                                }   
                            }
                        }
                    }
                }
                DMLProcessor::packageHandlerMapLock.unlock();

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
};

PackageHandler::PackageHandler(const messageqcpp::IOSocket& ios, 
							   boost::shared_ptr<messageqcpp::ByteStream> bs, 
							   messageqcpp::ByteStream::quadbyte packageType,
							   joblist::DistributedEngineComm *ec, 
							   uint64_t maxDeleteRows,
							   uint32_t sessionID, 
							   execplan::CalpontSystemCatalog::SCN txnId) : 
		fIos(ios), fPackageType(packageType), fEC(ec), fMaxDeleteRows(maxDeleteRows), fSessionID(sessionID), fTxnid(txnId)
{
	fByteStream = bs;
}

PackageHandler::~PackageHandler()
{
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
					BatchInsertProc* batchInsertProcessor;
					if (insertPkg.get_isBatchInsert())
					{
						//cout << "This is batch insert " << endl;
						//boost::shared_ptr<messageqcpp::ByteStream> insertBs (new messageqcpp::ByteStream(fByteStream));
						if ( insertPkg.get_Logging() )
						{
							LoggingID logid( DMLLoggingId, insertPkg.get_SessionID(), fTxnid);
							logging::Message::Args args1;
							logging::Message msg(1);
							args1.add("Start SQL statement: ");
							args1.add(insertPkg.get_SQLStatement());
							msg.format( args1 );
							logging::Logger logger(logid.fSubsysID);
							logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
							TablelockData * tablelockData = TablelockData::makeTablelockData(insertPkg.get_SessionID());
							uint64_t tableLockId = tablelockData->getTablelockId(insertPkg.getTableOid());
							if (tableLockId == 0)
							{
								//cout << "tablelock is not found in cache " << endl;
								DBRM dbrm;
								uint32_t  processID = ::getpid();
								int32_t txnId = fTxnid;
								std::string  processName("DMLProc");
								int32_t sessionId = insertPkg.get_SessionID();
								uint32_t tableOid = insertPkg.getTableOid();
								int i = 0;
								OamCache * oamcache = OamCache::makeOamCache();
								std::vector<int> pmList = oamcache->getModuleIds();
								//cout << "PMList size is " << pmList.size() << endl;
								std::vector<uint> pms;
								for (unsigned i=0; i < pmList.size(); i++)
								{
									pms.push_back((uint)pmList[i]);
								}
			
								try {
									tableLockId = dbrm.getTableLock(pms, tableOid, &processName, &processID, &sessionId, &txnId, BRM::LOADING );
								}
								catch (std::exception&)
								{
									result.result = dmlpackageprocessor::DMLPackageProcessor::INSERT_ERROR;
									result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
									break;
								}
		
								if ( tableLockId  == 0 )
								{
									int waitPeriod = 10;
									int sleepTime = 100; // sleep 100 milliseconds between checks
									int numTries = 10;  // try 10 times per second
									string waitPeriodStr = config::Config::makeConfig()->getConfig("SystemConfig", "WaitPeriod");
									if ( waitPeriodStr.length() != 0 )
										waitPeriod = static_cast<int>(config::Config::fromText(waitPeriodStr));
								
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
										try 
										{
											processID = ::getpid();
											txnId = fTxnid;
											sessionId = insertPkg.get_SessionID();;
											processName = "DMLProc";
											tableLockId = dbrm.getTableLock(pms, tableOid, &processName, &processID, &sessionId, &txnId, BRM::LOADING );
										}
										catch (std::exception&)
										{
											result.result = dmlpackageprocessor::DMLPackageProcessor::INSERT_ERROR;
											result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
											break;
										}

										if (tableLockId > 0)
											break;
									}

									if (i >= numTries) //error out
									{
										result.result = dmlpackageprocessor::DMLPackageProcessor::TABLE_LOCK_ERROR;
										logging::Message::Args args;
										args.add(processName);
										args.add((uint64_t)processID);
										args.add(sessionId);
										BRM::TxnID brmTxnID;
										brmTxnID.id = txnId;
										brmTxnID.valid = true;
										sessionManager.rolledback(brmTxnID);
										result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_TABLE_LOCKED,args));
										break;
									}
								}
							
								if (tableLockId > 0)
									tablelockData->setTablelock(tableOid, tableLockId);	
							}
						
							if ((tableLockId == 0) || (result.result != dmlpackageprocessor::DMLPackageProcessor::NO_ERROR))
								break;
						}	
						if (insertPkg.get_Logending() && insertPkg.get_Logging())
						{
							batchInsertProcessor = BatchInsertProc::makeBatchInsertProc(fTxnid, insertPkg.getTableOid());
							//cout << "dmlprocessor add last pkg" << endl; 
							batchInsertProcessor->addPkg(bsSave, true, insertPkg.get_isAutocommitOn(), insertPkg.getTableOid());
							cond.notify_one();
							int rc = 0;
							string errMsg;
							BatchInsertProc::removeBatchInsertProc(rc, errMsg);
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
							}
							else if ( rc != 0)
							{
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
						}
						else if (insertPkg.get_Logending())
						{
							batchInsertProcessor = BatchInsertProc::makeBatchInsertProc(fTxnid, insertPkg.getTableOid());
							int rc = 0;
							string errMsg;
							batchInsertProcessor->getError(rc, errMsg);
							//cout <<"dmlprocessor received last pkg from mysql rc == " << rc << endl;
							if ( rc == 0)
							{
								//cout << " rc = " << rc << endl;
								batchInsertProcessor->addPkg(bsSave, true, insertPkg.get_isAutocommitOn(), insertPkg.getTableOid());
								//cout << " added last pkg to queue" << endl;
								cond.notify_one();
								//cout << "Going to remove batchinsertproc " << endl;
								BatchInsertProc::removeBatchInsertProc(rc, errMsg);
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
								}
								else if ( rc != 0)
								{
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
							else if (rc == DMLPackageProcessor::IDBRANGE_WARNING)
							{
								BatchInsertProc::removeBatchInsertProc(rc, errMsg);
								result.result = dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING;
								LoggingID logid( DMLLoggingId, insertPkg.get_SessionID(), fTxnid);
								logging::Message::Args args1;
								logging::Message msg(1);
								args1.add("End SQL statement with warnings");
								msg.format( args1 );
								logging::Logger logger(logid.fSubsysID);
								logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
								logging::logDML(insertPkg.get_SessionID(), fTxnid, insertPkg.get_SQLStatement()+ ";", insertPkg.get_SchemaName());
							}
							else
							{
								BatchInsertProc::removeBatchInsertProc(rc, errMsg);
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
						}
						else
						{
							batchInsertProcessor = BatchInsertProc::makeBatchInsertProc(fTxnid, insertPkg.getTableOid());
							boost::unique_lock<boost::mutex> lock(mute);
							while (batchInsertProcessor->getInsertQueue()->size() >= batchInsertProcessor->getNumDBRoots() )
							{
								cond.wait(lock);
							}
							int rc = 0;
							string errMsg;
							batchInsertProcessor->getError(rc, errMsg);
							if (rc == DMLPackageProcessor::IDBRANGE_WARNING)
							{
								result.result = dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING;
							}
							else if ( rc !=0) 
							{
								result.result = dmlpackageprocessor::DMLPackageProcessor::VB_OVERFLOW_ERROR;
								logging::Message::Args args;
								logging::Message message(1);
								args.add(errMsg);
								message.format(args);
								result.message = message;
								break;
							}
							
							batchInsertProcessor->addPkg(bsSave, false, insertPkg.get_isAutocommitOn());	
							cond.notify_one();
                            break;
						}
					}
					else
					{
						insertPkg.set_TxnID(fTxnid);
						fProcessor.reset(new dmlpackageprocessor::InsertPackageProcessor);
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
					fProcessor.reset(new dmlpackageprocessor::UpdatePackageProcessor());
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
					fProcessor.reset(new dmlpackageprocessor::DeletePackageProcessor());
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
					// process it
					//@Bug 1341. Don't remove calpontsystemcatalog from this session to take advantage of cache.
					fProcessor.reset(new dmlpackageprocessor::CommandPackageProcessor);
					stmt = commandPkg.get_DMLStatement();
					boost::algorithm::to_upper(stmt);
					trim(stmt);
					//cout << "got command " << stmt << " for session " << commandPkg.get_SessionID() << endl;
					result = fProcessor->processPackage(commandPkg);
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
		if (stmt == "CLEANUP")
			fIos.close();
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
    oss << "PackageHandler::rollbackPending: rocessing DMLPackage.";
    DMLProcessor::log(oss.str(), logging::LOG_TYPE_DEBUG);
}

void added_a_pm(int)
{
	DistributedEngineComm *dec;
	ResourceManager rm;
	dec = DistributedEngineComm::instance(rm);
	dec->Setup();
}

DMLServer::DMLServer(int packageMaxThreads, int packageWorkQueueSize) :
	fPackageMaxThreads(packageMaxThreads), fPackageWorkQueueSize(packageWorkQueueSize)
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
		CancellationThread cancelObject;
		boost::thread cancelThread(cancelObject);

		cout << "DMLProc is ready..." << endl;
		for (;;)
		{
			ios = fMqServer->accept();
			ios.setSockID(nextID++);
			fDmlPackagepool.invoke(DMLProcessor(ios));
		}
		cancelThread.join();
	}
	catch (...)
	{
	}
}

DMLProcessor::DMLProcessor(messageqcpp::IOSocket ios) :
	fIos(ios)
{
	csc = CalpontSystemCatalog::makeCalpontSystemCatalog();
	csc->identity(CalpontSystemCatalog::EC);
}

void DMLProcessor::operator()()
{
	DBRM dbrm;
	bool bIsDbrmUp = true;

	try
    {
        boost::shared_ptr<messageqcpp::ByteStream> bs1	(new messageqcpp::ByteStream());
		//messageqcpp::ByteStream bs;
        messageqcpp::ByteStream::byte packageType;

		ResourceManager rm;
		DistributedEngineComm* fEC = DistributedEngineComm::instance(rm);
		boost::scoped_ptr<WriteEngine::ChunkManager> fCM(NULL);
		
		uint64_t maxDeleteRows = rm.getDMLMaxDeleteRows();
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
			catch (runtime_error&)
			{
				//This is an I/O error from InetStreamSocket::read(), just close and move on...
				//cout << "runtime error during read on " << fIos.getSockID() << " " << ex.what() << endl;
				bs1->reset();
			}
			catch (...)
			{
				//cout << "... error during read " << fIos.getSockID() << endl;
				throw;
			}
			if (bs1->length() == 0)
			{
				//cout << "Read 0 bytes. Closing connection " << fIos.getSockID() << endl;
				fIos.close();
				break;
			}
            uint32_t sessionID;
            *(bs1.get()) >> sessionID;
            *(bs1.get()) >> packageType;

			uint32_t stateFlags;
			messageqcpp::ByteStream::byte status=255;
			messageqcpp::ByteStream::octbyte rowCount = 0;
			if (dbrm.getSystemState(stateFlags) > 0)		// > 0 implies succesful retrieval. It doesn't imply anything about the contents
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
							BatchInsertProc* batchInsertProcessor;
							if (insertPkg.get_isBatchInsert() && insertPkg.get_Logending())
							{
								// No need to send lastpkg msg "if" this is a single msg
								// transaction (both get_Logging() and get_Logending() are true)
								if (!insertPkg.get_Logging())
								{
									int rc;
									string errMsg;
									BRM::TxnID txnid = sessionManager.getTxnID(sessionID);
									batchInsertProcessor = BatchInsertProc::makeBatchInsertProc(txnid.id, insertPkg.getTableOid());
									batchInsertProcessor->addPkg(bsSave, true);
									cond.notify_one();
									BatchInsertProc::removeBatchInsertProc(rc, errMsg); // Causes a join. That is, we'll wait for BatchInsertProc to finish doing its thing.
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
			packageHandlerMapLock.lock();
			std::map<uint32_t, PackageHandler*>::iterator phIter = packageHandlerMap.find(sessionID);
			packageHandlerMapLock.unlock();
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

			//cout << "   got a ";
			switch (packageType)
			{
			case dmlpackage::DML_INSERT:
			   //cout << "DML_INSERT";
			   if (fCM.get() == NULL)
			   {
					fCM.reset(new WriteEngine::ChunkManager());
			   }
			   fCM->setIsInsert(true);
			   break;
			case dmlpackage::DML_UPDATE:
			   //cout << "DML_UPDATE";
			   break;
			case dmlpackage::DML_DELETE:
			   //cout << "DML_DELETE";
			   break;
			case dmlpackage::DML_COMMAND:
			   //cout << "DML_COMMAND";
			   //@Bug 3851. Delete the chunkmanager for insert.
			   if ( fCM.get() != NULL )
				{
					std::map<uint32_t,uint32_t> oids;
					fCM->setIsInsert(false);
					BRM::TxnID txnid = sessionManager.getTxnID(sessionID);
					if (txnid.valid)
					{
						fCM->setTransId(txnid.id);
						fCM->flushChunks(0, oids);
						fCM.reset();
					}
				}
			   break;
			case dmlpackage::DML_INVALID_TYPE:
			   //cout << "DML_INVALID_TYPE";
			   break;
			default:
			   //cout << "UNKNOWN";
			   break;
			}
		    //cout << " package" << endl;
			
#ifdef SERIALIZE_DDL_DML_CPIMPORT                
			//Check if any other active transaction
			bool anyOtherActiveTransaction = true;
			BRM::TxnID txnid;
			BRM::SIDTIDEntry blockingsid;

			//For logout commit trigger
			if ( packageType == dmlpackage::DML_COMMAND )
			{
				anyOtherActiveTransaction = false;
			}
#endif            		
			int i = 0;
			int waitPeriod = 10;
			//@Bug 2487 Check transaction map every 1/10 second

			int sleepTime = 100; // sleep 100 milliseconds between checks
			int numTries = 10;  // try 10 times per second
			
#ifdef SERIALIZE_DDL_DML_CPIMPORT 
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
#endif
			
#ifdef SERIALIZE_DDL_DML_CPIMPORT
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
				PackageHandler ph(fIos, bs1, packageType, fEC, maxDeleteRows, sessionID, txnid.id);
				// We put the packageHandler into a map so that if we receive a
				// message to affect the previous command, we can find it.
				packageHandlerMapLock.lock();
				packageHandlerMap[sessionID] = &ph;
				packageHandlerMapLock.unlock();
				ph.run();		// Operates in this thread.
				packageHandlerMapLock.lock();
				packageHandlerMap.erase(sessionID);
				packageHandlerMapLock.unlock();
			}
#endif
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

void RollbackTransactionProcessor::processBulkRollback (BRM::TableLockInfo lockInfo, BRM::DBRM & dbrm, uint64_t uniqueId, 
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
		ownerChanged = dbrm.changeOwner(lockInfo.id, processName, processID, sessionID, txnid);
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
		dbrm.changeState( lockInfo.id, BRM::CLEANUP );	
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
	dbrm.releaseTableLock( lockInfo.id );
	
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


