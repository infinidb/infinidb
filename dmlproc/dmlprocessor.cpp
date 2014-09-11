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
*   $Id: dmlprocessor.cpp 851 2012-09-12 15:11:01Z chao $
*
*
***********************************************************************/
/** @file */
#include "dmlprocessor.h"
#include "configcpp.h"
#include "calpontsystemcatalog.h"
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
#include "we_chunkmanager.h"

using namespace BRM;
using namespace config;
using namespace execplan;
using namespace std;
using namespace messageqcpp;
using namespace dmlpackage;
using namespace dmlpackageprocessor;
using namespace joblist;
using namespace logging;

namespace
{
const std::string myname = "DMLProc";
bool gAcceptPackages;
}

namespace dmlprocessor
{

struct PackageHandler
{
    PackageHandler(const IOSocket& ios, messageqcpp::ByteStream& bs, messageqcpp::ByteStream::quadbyte packageType,
		DistributedEngineComm *ec, uint64_t maxDeleteRows, execplan::CalpontSystemCatalog::SCN txnId, WriteEngine::ChunkManager *cm) : 
	    fIos(ios), fByteStream(bs), fPackageType(packageType), fEC(ec), fMaxDeleteRows(maxDeleteRows), fTxnid(txnId),
		fChunkManager(cm) {}
    
    void run()
    {

        dmlpackageprocessor::DMLPackageProcessor::DMLResult result;
        result.result = dmlpackageprocessor::DMLPackageProcessor::NO_ERROR;
		//cout << "PackageHandler handling ";
		std::string stmt;
        try
        {

            switch( fPackageType )
            {
                case dmlpackage::DML_INSERT:
                    {
                        // build an InsertDMLPackage from the bytestream
						//cout << "an INSERT package" << endl;
						dmlpackage::InsertDMLPackage insertPkg;
                        insertPkg.read(fByteStream);
                        // process it
                        //@Bug 1341. Don't remove calpontsystemcatalog fro this session to take advantage of cache.
                        dmlpackageprocessor::InsertPackageProcessor processor;
						processor.setEngineComm(fEC);
						insertPkg.set_TxnID(fTxnid);
						insertPkg.set_ChunkManager(fChunkManager);
						
						//assert( fTxnid != 0);
						result = processor.processPackage(insertPkg);
                    }
                    break;

                case dmlpackage::DML_UPDATE:
                    {
                        // build an UpdateDMLPackage from the bytestream
						//cout << "an UPDATE package" << endl;
						dmlpackage::UpdateDMLPackage updatePkg;
                        updatePkg.read(fByteStream);
                        // process it
                        //@Bug 1341. Don't remove calpontsystemcatalog fro this session to take advantage of cache.
                        dmlpackageprocessor::UpdatePackageProcessor processor;
						processor.setEngineComm(fEC);
						processor.setMaxUpdateRows(fMaxDeleteRows);
						processor.setRM( &frm);
						updatePkg.set_TxnID(fTxnid);
						//assert( fTxnid != 0);
						result = processor.processPackage(updatePkg) ;
                    }
                    break;

                case dmlpackage::DML_DELETE:
                    {
                        // build a DeleteDMLPackage from the bytestream
						//cout << "a DELETE package" << endl;
						dmlpackage::DeleteDMLPackage deletePkg;
                        deletePkg.read(fByteStream);
                        // process it
                        //@Bug 1341. Don't remove calpontsystemcatalog fro this session to take advantage of cache.
                        dmlpackageprocessor::DeletePackageProcessor processor;
						processor.setEngineComm(fEC);
						processor.setMaxDeleteRows(fMaxDeleteRows);
						//@Bug 2867. use one resource manager
						processor.setRM( &frm);
						deletePkg.set_TxnID(fTxnid);
						//assert( fTxnid != 0);
						result = processor.processPackage(deletePkg);
                    }
                    break;
                case dmlpackage::DML_COMMAND:
                    {
                        // build a CommandDMLPackage from the bytestream
						//cout << "a COMMAND package" << endl;
						dmlpackage::CommandDMLPackage commandPkg;
                        commandPkg.read(fByteStream);
                        // process it
                        //@Bug 1341. Don't remove calpontsystemcatalog fro this session to take advantage of cache.
                        dmlpackageprocessor::CommandPackageProcessor processor;
						stmt = commandPkg.get_DMLStatement();
						boost::algorithm::to_upper(stmt);
						trim(stmt);
						//cout << "got command " << stmt << " for session " << commandPkg.get_SessionID() << endl;
                        result = processor.processPackage(commandPkg);
					}
                    break;
            }
            //Log errors
            if ((result.result != dmlpackageprocessor::DMLPackageProcessor::NO_ERROR) 
            	&& ( result.result != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING ) && result.result != dmlpackageprocessor::DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR )
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
			results << result.tableLockInfo;
            fIos.write(results);
			if (stmt == "CLEANUP")
				fIos.close();
        }
        catch(...)
        {
            fIos.close();
        }
    }

    messageqcpp::IOSocket fIos;
    messageqcpp::ByteStream fByteStream;
    messageqcpp::ByteStream::quadbyte fPackageType;
    DistributedEngineComm *fEC;
    uint64_t fMaxDeleteRows;
	ResourceManager frm;
	execplan::CalpontSystemCatalog::SCN fTxnid;
	WriteEngine::ChunkManager *fChunkManager;
	
};

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
	fMqServer = new MessageQueueServer("DMLProc");

	fDmlPackagepool.setMaxThreads(fPackageMaxThreads);
	fDmlPackagepool.setQueueSize(fPackageWorkQueueSize);

	gAcceptPackages = true;
}

void DMLServer::start()
{
	messageqcpp::IOSocket ios;

	cout << "DMLProc is ready..." << endl;

	try
	{
		for (;;)
		{
			ios = fMqServer->accept();
//cout << "DMLProc accepted a connection..." << endl;
			fDmlPackagepool.invoke(DMLProcessor(ios));
		}
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
    try
    {
        messageqcpp::ByteStream bs;
        messageqcpp::ByteStream::byte packageType;

		ResourceManager rm;
		DistributedEngineComm* fEC = DistributedEngineComm::instance(rm);
		WriteEngine::ChunkManager* fCM = NULL;
		
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
//cout << "DMLProc is waiting for a Calpont DML Package..." << endl;
			try
			{
				bs = fIos.read();
			}
			catch (runtime_error&)
			{
				//This is an I/O error from InetStreamSocket::read(), just close and move on...
				bs.reset();
			}
			catch (...)
			{
				throw;
			}
			if (bs.length() == 0)
			{
				fIos.close();
				break;
			}
            u_int32_t sessionID;
            bs >> sessionID;
            bs >> packageType;

            if (packageType == dmlpackage::DML_STOP)
            {
                //cout << "   got a DML_STOP" << endl;
                stopAcceptingPackages(fIos);
            }
            else if (packageType == dmlpackage::DML_RESUME)
            {
                //cout << "   got a DML_RESUME" << endl;
                resumeAcceptingPackages(fIos);
            }
            else if (gAcceptPackages)
            {
                //cout << "   got a ";
                switch (packageType)
                {
                case dmlpackage::DML_INSERT:
                   //cout << "DML_INSERT";
				   if (fCM == NULL)
				   {
						fCM = new WriteEngine::ChunkManager();
						fCM->setIsInsert(true);
				   }
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
				   if ( fCM != NULL )
					{
						std::map<u_int32_t,u_int32_t> oids;
						fCM->setIsInsert(false);
						BRM::TxnID txnid = sessionManager.getTxnID(sessionID);
						if (txnid.valid)
						{
							fCM->setTransId(txnid.id);
							fCM->flushChunks(0, oids);
							delete fCM;
							fCM = NULL;
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
				bool bIsDbrmUp = true;
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
            	string waitPeriodStr = Config::makeConfig()->getConfig("SystemConfig", "WaitPeriod");
            	if ( waitPeriodStr.length() != 0 )
            		waitPeriod = static_cast<int>(Config::fromText(waitPeriodStr));
            		
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
                	messageqcpp::ByteStream::byte status =  DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR;
                	messageqcpp::ByteStream::octbyte rowCount = 0;
                	results << status;
                	results << rowCount;
					Message::Args args;
					args.add(static_cast<uint64_t>(blockingsid.sessionid));

					CalpontSystemCatalog::OID to = blockingsid.tableOID;
					if (to < 3000)
					{
						vector<SIDTIDEntry> st;
						sessionManager.getTableLocksInfo(st);
						vector<SIDTIDEntry>::iterator iter = st.begin();
						vector<SIDTIDEntry>::iterator end = st.end();
						to = 0;
						while (iter != end)
						{
							if (iter->sessionid == blockingsid.sessionid && iter->tableOID >= 3000)
							{
								to = iter->tableOID;
								break;
							}
							++iter;
						}
					}

					if (to < 3000)
						args.add("another table");
					else
						args.add(csc->tableName(to).toString());

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
                	PackageHandler ph(fIos, bs, packageType, fEC, maxDeleteRows, txnid.id, fCM);
					ph.run();
                }
#endif
	    	}
            else
            {
                messageqcpp::ByteStream results;
                messageqcpp::ByteStream::byte status =  DMLPackageProcessor::NOT_ACCEPTING_PACKAGES;
                messageqcpp::ByteStream::octbyte rowCount = 0;
                results << status;
                results << rowCount;
				//@bug 266
                logging::Message::Args args;
        		logging::Message message(2);
        		args.add("Writing to the database is disabled.");
        		message.format( args );
				results << message.msg();
                fIos.write(results);
            }
        }
    }
    catch (std::exception& ex)
    {
        cerr << ex.what() << endl;
        logging::Message::Args args;
        logging::Message message(8);
        args.add("DMLProcessor failed on: ");
        args.add(ex.what());
        message.format( args );
        logging::LoggingID lid(20);
        logging::MessageLog ml(lid);
        ml.logDebugMessage( message );
		fIos.close();
    }
    catch (...)
    {
        cerr << "Caught unknown exception!" << endl;
        logging::Message::Args args;
        logging::Message message(8);
        args.add("DMLProcessor failed on: ");
        args.add("processing DMLPackage.");
        message.format( args );
        logging::LoggingID lid(20);
        logging::MessageLog ml(lid);
        ml.logDebugMessage( message );
		fIos.close();
    }
}

void DMLProcessor::stopAcceptingPackages(messageqcpp::IOSocket& ios)
{
    gAcceptPackages = false;

    messageqcpp::ByteStream results;
    messageqcpp::ByteStream::byte status =  DMLPackageProcessor::NO_ERROR;
    results << status;

    ios.write(results);
}

void DMLProcessor::resumeAcceptingPackages(messageqcpp::IOSocket& ios)
{
    gAcceptPackages = true;

    messageqcpp::ByteStream results;
    messageqcpp::ByteStream::byte status =  DMLPackageProcessor::NO_ERROR;
    results << status;

    ios.write(results);
}

} //namespace dmlprocessor
// vim:ts=4 sw=4:

