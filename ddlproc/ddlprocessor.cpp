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
 *   $Id: ddlprocessor.cpp 6 2006-06-23 17:58:51Z rcraighead $
 *
 *
 ***********************************************************************/

#include <map>
#include <boost/scoped_ptr.hpp>
using namespace std;

#include "ddlpkg.h"
#include "ddlprocessor.h"
#include "createtableprocessor.h"
#include "altertableprocessor.h"
#include "droptableprocessor.h"
#include "calpontsystemcatalog.h"
#include "sqlparser.h"
#include "configcpp.h"
#include "markpartitionprocessor.h"
#include "restorepartitionprocessor.h"
#include "droppartitionprocessor.h"
#define 	SERIALIZE_DDL_DML_CPIMPORT 	1

#include "cacheutils.h"
#include "vss.h"
#include "dbrm.h"
#include "idberrorinfo.h"
#include "errorids.h"
#include "we_messages.h"
using namespace BRM;

using namespace config;
using namespace messageqcpp;
using namespace ddlpackageprocessor;
using namespace ddlpackage;
using namespace execplan;
using namespace logging;
using namespace WriteEngine;

namespace
{
typedef messageqcpp::ByteStream::quadbyte quadbyte;

const quadbyte UNRECOGNIZED_PACKAGE_TYPE = 100;
const quadbyte NO_PKNAME_AVAILABLE = 101;

const std::string DDLProcName = "DDLProc";

// primary key name supplied by Oracle. Oracle will issue
// two separate DDL statements for a create table with primary
// key DDL, with the 1st one being create index and the 2nd
// one being create table. This PK name will be stored here
// when creatindexprocessor gets the create index statement. This
// is to make sure Calpont use the same system primary key name as Oracle
typedef map<u_int32_t, string> PKNameMap;

//FIXME: when DDL is parallelized again, access to this map needs to be protected by a lock!
PKNameMap pkNameMap;

WriteEngine::WriteEngineWrapper writeEngine;

void cleanPMSysCache()
{
	vector<BRM::OID_t> oidList = getAllSysCatOIDs();
	cacheutils::flushOIDsFromCache ( oidList );
}

struct PackageHandler
{
    void operator ()()
    {

        DDLPackageProcessor::DDLResult result;
        result.result = DDLPackageProcessor::NO_ERROR;
        //CalpontSystemCatalog* systemCatalogPtr;
               
        try
        {
			//cout << "DDLProc received package " << fPackageType << endl;
            switch( fPackageType )
            {
                case ddlpackage::DDL_CREATE_TABLE_STATEMENT:
                    {
                        CreateTableStatement createTableStmt;
                        createTableStmt.unserialize(fByteStream);
						CalpontSystemCatalog* systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(createTableStmt.fSessionID );
                        CreateTableProcessor processor;
                        PKNameMap::iterator it = pkNameMap.find(createTableStmt.fSessionID);
                        if (it == pkNameMap.end())
                            //throw (NO_PKNAME_AVAILABLE);
                            // TODO: should check if this createtablestmt has primary key
                            processor.PKName("");
                        else
                            processor.PKName((*it).second);    
						processor.fTxnid.id = fTxnid.id;
						processor.fTxnid.valid = true;
						//cout << "create table using txnid " << fTxnid.id << endl;
                        result = processor.processPackage(createTableStmt);                        
                        if (it != pkNameMap.end())
                            pkNameMap.erase(it);
                        systemCatalogPtr->removeCalpontSystemCatalog( createTableStmt.fSessionID );
                        systemCatalogPtr->removeCalpontSystemCatalog( createTableStmt.fSessionID | 0x80000000);
                    }
                    break;
#if 0
                case ddlpackage::DDL_CREATE_INDEX:
                    {
                        CreateIndexStatement createIndexStmt;
                        createIndexStmt.unserialize(fByteStream);
						CalpontSystemCatalog* systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(createIndexStmt.fSessionID );
                        CreateIndexProcessor processor;
						processor.fTxnid.id = fTxnid.id;
						processor.fTxnid..valid = true;
                        result = processor.processPackage(createIndexStmt);
                        pkNameMap.insert(PKNameMap::value_type(createIndexStmt.fSessionID, processor.PKName()));
                        systemCatalogPtr->removeCalpontSystemCatalog( createIndexStmt.fSessionID );
                        systemCatalogPtr->removeCalpontSystemCatalog( createIndexStmt.fSessionID | 0x80000000);
                    }
                    break;
#endif
                case ddlpackage::DDL_ALTER_TABLE_STATEMENT:
                    {
                    	AlterTableStatement alterTableStmt;
                    	alterTableStmt.unserialize(fByteStream);
                    	CalpontSystemCatalog* systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(alterTableStmt.fSessionID );
                    	AlterTableProcessor processor;   
						processor.fTxnid.id = fTxnid.id;
						processor.fTxnid.valid = true;
                    	PKNameMap::iterator it = pkNameMap.find(alterTableStmt.fSessionID);
                    	if (it == pkNameMap.end())
                    	//throw (NO_PKNAME_AVAILABLE);
                    	// TODO: should check if this createtablestmt has primary key
                    	processor.PKName("");
                    	else
                    		processor.PKName((*it).second);  
                    	result = processor.processPackage(alterTableStmt);                        
                    	if (it != pkNameMap.end())
                    	    pkNameMap.erase(it);
                        systemCatalogPtr->removeCalpontSystemCatalog( alterTableStmt.fSessionID );
                        systemCatalogPtr->removeCalpontSystemCatalog( alterTableStmt.fSessionID | 0x80000000);
                    }
                    break;

                case ddlpackage::DDL_DROP_TABLE_STATEMENT:
                    {
                        DropTableStatement dropTableStmt;
                        dropTableStmt.unserialize(fByteStream);
						CalpontSystemCatalog* systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(dropTableStmt.fSessionID );
                        boost::scoped_ptr<DropTableProcessor> processor(new DropTableProcessor());
						(processor->fTxnid).id = fTxnid.id;
						(processor->fTxnid).valid = true;
						//cout << "Drop table using txnid " << fTxnid.id << endl;
                        result = processor->processPackage(dropTableStmt);
                        systemCatalogPtr->removeCalpontSystemCatalog( dropTableStmt.fSessionID );
                        systemCatalogPtr->removeCalpontSystemCatalog( dropTableStmt.fSessionID | 0x80000000);
                    }
                    break;
                case ddlpackage::DDL_TRUNC_TABLE_STATEMENT:
                    {
                        TruncTableStatement truncTableStmt;
                        truncTableStmt.unserialize(fByteStream);
                        CalpontSystemCatalog* systemCatalogPtr =
						CalpontSystemCatalog::makeCalpontSystemCatalog(truncTableStmt.fSessionID);
                        boost::scoped_ptr<TruncTableProcessor> processor(new TruncTableProcessor());
						(processor->fTxnid).id = fTxnid.id;
						(processor->fTxnid).valid = true;
                        result = processor->processPackage(truncTableStmt);
                        systemCatalogPtr->removeCalpontSystemCatalog(truncTableStmt.fSessionID );
                        systemCatalogPtr->removeCalpontSystemCatalog(truncTableStmt.fSessionID | 0x80000000);
                    }
                    break;
				case ddlpackage::DDL_MARK_PARTITION_STATEMENT:
                    {
                        MarkPartitionStatement markPartitionStmt;
                        markPartitionStmt.unserialize(fByteStream);
                        CalpontSystemCatalog* systemCatalogPtr =
                        CalpontSystemCatalog::makeCalpontSystemCatalog(markPartitionStmt.fSessionID);
                        boost::scoped_ptr<MarkPartitionProcessor> processor(new MarkPartitionProcessor());
						(processor->fTxnid).id = fTxnid.id;
						(processor->fTxnid).valid = true;
                        result = processor->processPackage(markPartitionStmt);
                        systemCatalogPtr->removeCalpontSystemCatalog(markPartitionStmt.fSessionID );
                        systemCatalogPtr->removeCalpontSystemCatalog(markPartitionStmt.fSessionID | 0x80000000);
                    }
                    break;
				case ddlpackage::DDL_RESTORE_PARTITION_STATEMENT:
                    {
                        RestorePartitionStatement restorePartitionStmt;
                        restorePartitionStmt.unserialize(fByteStream);
                        CalpontSystemCatalog* systemCatalogPtr =
						CalpontSystemCatalog::makeCalpontSystemCatalog(restorePartitionStmt.fSessionID);
                        boost::scoped_ptr<RestorePartitionProcessor> processor(new RestorePartitionProcessor());
						(processor->fTxnid).id = fTxnid.id;
						(processor->fTxnid).valid = true;
                        result = processor->processPackage(restorePartitionStmt);
                        systemCatalogPtr->removeCalpontSystemCatalog(restorePartitionStmt.fSessionID );
                        systemCatalogPtr->removeCalpontSystemCatalog(restorePartitionStmt.fSessionID | 0x80000000);
                    }
                    break;
				case ddlpackage::DDL_DROP_PARTITION_STATEMENT:
                    {
                        DropPartitionStatement dropPartitionStmt;
                        dropPartitionStmt.unserialize(fByteStream);
                        CalpontSystemCatalog* systemCatalogPtr =
						CalpontSystemCatalog::makeCalpontSystemCatalog(dropPartitionStmt.fSessionID);
                        boost::scoped_ptr<DropPartitionProcessor> processor(new DropPartitionProcessor());
						(processor->fTxnid).id = fTxnid.id;
						(processor->fTxnid).valid = true;
                        result = processor->processPackage(dropPartitionStmt);
                        systemCatalogPtr->removeCalpontSystemCatalog(dropPartitionStmt.fSessionID );
                        systemCatalogPtr->removeCalpontSystemCatalog(dropPartitionStmt.fSessionID | 0x80000000);
                    }
                    break;
#if 0
                case ddlpackage::DDL_DROP_INDEX_STATEMENT:
                    {
                        DropIndexStatement dropIndexStmt;
                        dropIndexStmt.unserialize(fByteStream);
						CalpontSystemCatalog* systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(dropIndexStmt.fSessionID );
                        DropIndexProcessor processor;
                        result = processor.processPackage(dropIndexStmt);
                        systemCatalogPtr->CalpontSystemCatalog::makeCalpontSystemCatalog(dropIndexStmt.fSessionID );
                        systemCatalogPtr->CalpontSystemCatalog::makeCalpontSystemCatalog(dropIndexStmt.fSessionID | 0x80000000);
                    }
                    break;
#endif
                default:
                    throw(UNRECOGNIZED_PACKAGE_TYPE);
            }

            //@Bug 3427. No need to log user rror, just return the message to user.
            //Log errors
            if ((result.result != DDLPackageProcessor::NO_ERROR) && (result.result != DDLPackageProcessor::USER_ERROR))
            {
                logging::LoggingID lid(23);
                logging::MessageLog ml(lid);

                ml.logErrorMessage( result.message );
            }
			cleanPMSysCache();
            messageqcpp::ByteStream results;
            messageqcpp::ByteStream::byte status =  result.result;
            results << status;
            results << result.message.msg();
		
            fIos.write(results);

            fIos.close();
        }
        catch(quadbyte& /*foo*/)
        {
            fIos.close();
            cout << "Unrecognized package type" << endl;
        }
		catch(logging::IDBExcept& idbEx)
		{
			cleanPMSysCache();
            messageqcpp::ByteStream results;
			messageqcpp::ByteStream::byte status = DDLPackageProcessor::CREATE_ERROR;
            results << status;
			results << string(idbEx.what());
		
            fIos.write(results);

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
	BRM::TxnID fTxnid;
	

};

}

namespace ddlprocessor
{
DDLProcessor::DDLProcessor( int packageMaxThreads, int packageWorkQueueSize )
        :fPackageMaxThreads(packageMaxThreads), fPackageWorkQueueSize(packageWorkQueueSize),
         fMqServer(DDLProcName)
{
    fDdlPackagepool.setMaxThreads(fPackageMaxThreads);
    fDdlPackagepool.setQueueSize(fPackageWorkQueueSize);
	csc = CalpontSystemCatalog::makeCalpontSystemCatalog();
	csc->identity(CalpontSystemCatalog::EC);
	fWEClient = WriteEngine::WEClients::instance(WriteEngine::WEClients::DDLPROC);
	fPMCount = fWEClient->getPmCount();
}

void DDLProcessor::process()
{
    DBRM dbrm;
    messageqcpp::IOSocket ios;
    messageqcpp::ByteStream bs;
    PackageHandler handler;
    messageqcpp::ByteStream::quadbyte packageType;
	
	cout << "DDLProc is ready..." << endl;

    try
    {
        for (;;)
        {
            ios = fMqServer.accept();
            bs = ios.read();
            u_int32_t sessionID;
            bs >> sessionID;
            bs >> packageType;

            uint32_t stateFlags;
			if (dbrm.getSystemState(stateFlags) > 0)		// > 0 implies succesful retrieval. It doesn't imply anything about the contents
            {
                messageqcpp::ByteStream results;
                const char* responseMsg=0;
                messageqcpp::ByteStream::byte status;
                bool bReject = false;
                // Check to see if we're in write suspended mode
                // If so, we can't process the request.
                if (stateFlags & SessionManagerServer::SS_SUSPENDED)
                {
                    status =  DDLPackageProcessor::NOT_ACCEPTING_PACKAGES;
                    responseMsg = "Writing to the database is disabled.";
                    bReject = true;
                }
                // Check to see if we're in write suspend or shutdown pending mode
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
                    status =  DDLPackageProcessor::NOT_ACCEPTING_PACKAGES;
                    bReject = true;
                }
                if (bReject)
                {
                    results << status;
                    //@bug 266
                    MessageLog logger(LoggingID(27));
                    logging::Message::Args args;
                    logging::Message message(2);
                    args.add(responseMsg);
                    message.format( args );
                    results << message.msg();
                    ios.write(results);
                    std::cout << responseMsg << endl;
                    std::cout << "Command rejected. Status " << (int)status << message.msg() << endl;
                    continue;
                }
            }

#ifdef SERIALIZE_DDL_DML_CPIMPORT                
            //Check if any other active transaction
            bool bIsDbrmUp = true;
            bool anyOtherActiveTransaction = true;
            execplan::SessionManager sessionManager; 
            BRM::SIDTIDEntry blockingsid;
#endif          
            //check whether the system is ready to process statement.
            if (dbrm.getSystemReady() < 1)
            {
                messageqcpp::ByteStream results;
                messageqcpp::ByteStream::byte status =  DDLPackageProcessor::NOT_ACCEPTING_PACKAGES;
            
                results << status;
                string msg ("System is not ready yet. Please try again." );

                results << msg;
                ios.write(results);

                ios.close();
                continue;
            }
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
            BRM::TxnID txnid;
            int rc = 0;
            //anyOtherActiveTransaction = sessionManager.checkActiveTransaction( sessionID, bIsDbrmUp );	
            while (anyOtherActiveTransaction)
            {
                anyOtherActiveTransaction = sessionManager.checkActiveTransaction( sessionID, bIsDbrmUp,
                    blockingsid );
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
                            //cout << "Ready to process type " << (int)packageType << endl;
                            txnid = sessionManager.getTxnID(sessionID);
                            if ( !txnid.valid )
                            {
                                txnid = sessionManager.newTxnID(sessionID, true, true);
                                if (txnid.valid) {
                                    //cout << "Ready to process type " << (int)packageType << " with txnid " << txnid.id << endl;
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
                                string errorMsg;
                                rc = commitTransaction(txnid.id, errorMsg);
                                if ( rc != 0)
                                    throw std::runtime_error(errorMsg);
                                //need unlock the table.
                                std::vector<TableLockInfo> tableLocks = dbrm.getAllTableLocks();
                                bool lockReleased = true;
                                for (unsigned k = 0; k < tableLocks.size(); k++)
                                {
                                    if (tableLocks[k].ownerTxnID == txnid.id)
                                    {
                                        lockReleased = dbrm.releaseTableLock(tableLocks[k].id);
                                        if (!lockReleased)
                                        {
                                            ostringstream os;
                                            os << "tablelock id " << tableLocks[k].id << " is not found";
                                            throw std::runtime_error(os.str());
                                        }
                                    }
                                }
                                sessionManager.committed(txnid);
                                txnid = sessionManager.newTxnID(sessionID, true, true);
                                if (txnid.valid) {
                                    //cout << "Ready to process type " << (int)packageType << " with txnid " << txnid.id << endl;
                                    anyOtherActiveTransaction = false;
                                    break;
                                }
                                else
                                {
                                    anyOtherActiveTransaction = true;
                                }
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
                        txnid = sessionManager.newTxnID(sessionID, true, true);
                        if (!txnid.valid) {
                            //cout << "cannot get txnid " << (int)packageType << " for session " << sessionID <<  endl;
                            anyOtherActiveTransaction = true;
                        }
                        else
                        {
                            anyOtherActiveTransaction = false;
                        }
                    }
                    else 
                    {
                        string errorMsg;
                        rc = commitTransaction(txnid.id, errorMsg);
                        if ( rc != 0)
                            throw std::runtime_error(errorMsg);
                        //need unlock the table.
                        std::vector<TableLockInfo> tableLocks = dbrm.getAllTableLocks();
                        bool lockReleased = true;
                        for (unsigned k = 0; k < tableLocks.size(); k++)
                        {
                            if (tableLocks[k].ownerTxnID == txnid.id)
                            {
                                lockReleased = dbrm.releaseTableLock(tableLocks[k].id);
                                if (!lockReleased)
                                {
                                    ostringstream os;
                                    os << "tablelock id " << tableLocks[k].id << " is not found";
                                    throw std::runtime_error(os.str());
                                }
                            }
                        }	
                        sessionManager.committed(txnid);
                        txnid = sessionManager.newTxnID(sessionID, true, true);
                        if (!txnid.valid) {
                            //cout << "cannot get txnid " << (int)packageType << " for session " << sessionID <<  endl;
                            anyOtherActiveTransaction = true;
                        }
                        else
                        {
                            anyOtherActiveTransaction = false;
                        }
                    }
                }
                    
                if ((anyOtherActiveTransaction) && (i >= numTries))
                {
                    //cout << " Erroring out on package type " << (int)packageType << endl;
                    break;  
                }
            }
#endif
#ifdef SERIALIZE_DDL_DML_CPIMPORT	
            if ((anyOtherActiveTransaction) && (i >= numTries))
            {
                messageqcpp::ByteStream results;
                messageqcpp::ByteStream::byte status =  DDLPackageProcessor::NOT_ACCEPTING_PACKAGES;
            
                results << status;
                Message::Args args;
                args.add(static_cast<uint64_t>(blockingsid.sessionid));

                results << IDBErrorInfo::instance()->errorMsg(ERR_ACTIVE_TRANSACTION, args);
				//@Bug 3854 Log to debug.log
				LoggingID logid(15, 0, 0);
				logging::Message::Args args1;
				logging::Message msg(1);
				args1.add(IDBErrorInfo::instance()->errorMsg(ERR_ACTIVE_TRANSACTION, args));
				msg.format( args1 );
				logging::Logger logger(logid.fSubsysID);
				logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
				
                ios.write(results);

                ios.close();
            }
            else
            {
                handler.fIos = ios;
                handler.fByteStream = bs;
                handler.fPackageType = packageType;
                handler.fTxnid = txnid;
                fDdlPackagepool.invoke(handler);
        
            }
#endif
        }
    }
    catch (exception& ex)
    {
        cerr << ex.what() << endl;
		messageqcpp::ByteStream results;
        messageqcpp::ByteStream::byte status =  DDLPackageProcessor::NOT_ACCEPTING_PACKAGES;
                
        results << status;
        results << ex.what();
        ios.write(results);

        ios.close();
    }
    catch(...)
    {
        cerr << "Caught unknown exception!" << endl;
		messageqcpp::ByteStream results;
        messageqcpp::ByteStream::byte status =  DDLPackageProcessor::NOT_ACCEPTING_PACKAGES;
                
        results << status;
        results << "Caught unknown exception!";
        ios.write(results);

        ios.close();
    }

    // wait for all the threads to exit
    fDdlPackagepool.wait();
}

int DDLProcessor::commitTransaction(uint32_t txnID, std::string & errorMsg)
{
	ByteStream bytestream;
	DBRM dbrm;
	uint64_t uniqueId = dbrm.getUnique64();
	fWEClient->addQueue(uniqueId);
	bytestream << (ByteStream::byte)WE_SVR_COMMIT_VERSION;
	bytestream << uniqueId;
	bytestream << txnID;
	uint msgRecived = 0;
	fWEClient->write_to_all(bytestream);
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bsIn.reset(new ByteStream());
	int rc = 0;
	ByteStream::byte tmp8;
	while (1)
	{
		if (msgRecived == fPMCount)
			break;
		fWEClient->read(uniqueId, bsIn);
		if ( bsIn->length() == 0 ) //read error
		{
			rc = 1;
			errorMsg = "DDL cannot communicate with WES"; 
			fWEClient->removeQueue(uniqueId);
			break;
		}			
		else {
			*bsIn >> tmp8;
			rc = tmp8;
			if (rc != 0) {
				*bsIn >> errorMsg;
				fWEClient->removeQueue(uniqueId);
				break;
			}
			else
				msgRecived++;						
		}
	}
	return rc;
}
}  // namespace ddlprocessor
// vim:ts=4 sw=4:

