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

/*******************************************************************************
* $Id: we_readthread.cpp 4609 2013-04-19 15:32:02Z chao $
*
*******************************************************************************/

#include <unistd.h>

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "threadpool.h"
using namespace threadpool;

#include "we_dataloader.h"
#include "we_readthread.h"
#include "we_messages.h"
#include "we_message_handlers.h"
#include "we_ddlcommandproc.h"
#include "we_dmlcommandproc.h"
#include "we_redistribute.h"
#include "we_config.h"
#include "stopwatch.h"
using namespace logging;
using namespace WriteEngine;
//StopWatch timer;
namespace WriteEngine
{

ReadThread::ReadThread(const IOSocket& ios): fIos(ios) 
{

}

ReadThread::~ReadThread(){

}

void ReadThread::operator ()()
{
    // We should never come here
}

//-----------------------------------------------------------------------------
//ctor
DmlReadThread::DmlReadThread(const messageqcpp::IOSocket& ios,
                             ByteStream& Ibs ): ReadThread(ios), fWeDMLprocessor(new WE_DMLCommandProc), fWeDDLprocessor(new WE_DDLCommandProc)
{
    fIbs = Ibs;
}
//dtor
DmlReadThread::~DmlReadThread()
{
    //cout << "in DmlReadThread destructor" << endl;
}

void DmlReadThread::operator()()
{
    // DCH Since fIbs is a class member, there's no reason to make a copy here.
    // Why waste the CPU? Note that Splitter thread doesn't make a copy.
    // The only reason I can think of to make such a copy is to guarantee a
    // strong exception, but that doesn't appear to be in play here.
    ByteStream ibs = fIbs;
    ByteStream obs;
    ByteStream::byte msgId;
    ByteStream::octbyte uniqueID;
    ByteStream::quadbyte PMId;
    ByteStream::byte rc = 0;
    std::string errMsg;
    //cout << "DmlReadThread created ..." << endl;
    // queryStats.blocksChanged for delete/update
    uint64_t blocksChanged = 0;
	
    while (ibs.length()>0)
    {
        try
        {
            errMsg.clear();

            //do work here...
            ibs >> msgId;
			if (msgId != WE_SVR_CLOSE_CONNECTION)
				ibs >> uniqueID;
            //cout << "DmlReadThread " << pthread_self () << " received message id " << (uint)msgId << " and bytestream length " << ibs.length() << endl;
            switch (msgId)
            {
            case WE_SVR_SINGLE_INSERT:
                {
                    rc = fWeDMLprocessor->processSingleInsert(ibs, errMsg);
                    break;
                }
            case WE_SVR_COMMIT_VERSION:
                {
                    rc = fWeDMLprocessor->commitVersion(ibs, errMsg);
                    break;
                }
            case WE_SVR_ROLLBACK_BLOCKS:
                {
                    rc = fWeDMLprocessor->rollbackBlocks(ibs, errMsg);
                    break;
                }
            case WE_SVR_ROLLBACK_VERSION:
                {
                    rc = fWeDMLprocessor->rollbackVersion(ibs, errMsg);
                    break;
                }
            case WE_SVR_COMMIT_BATCH_AUTO_ON:
                {
                    rc = fWeDMLprocessor->commitBatchAutoOn(ibs, errMsg);
                    break;
                }
            case WE_SVR_ROLLBACK_BATCH_AUTO_ON:
                {
                    rc = fWeDMLprocessor->rollbackBatchAutoOn(ibs, errMsg);
                    break;
                }
            case WE_SVR_COMMIT_BATCH_AUTO_OFF:
                {
                    rc = fWeDMLprocessor->commitBatchAutoOn(ibs, errMsg);
                    break;
                }
            case WE_SVR_ROLLBACK_BATCH_AUTO_OFF:
                {
                    rc = fWeDMLprocessor->rollbackBatchAutoOff(ibs, errMsg);
                    break;
                }
            case WE_SVR_BATCH_INSERT:
                {
					//timer.start("processBatchInsert");
                    rc = fWeDMLprocessor->processBatchInsert(ibs, errMsg, PMId);
					//timer.stop("processBatchInsert");
					//cout << "fWeDMLprocessor " << fWeDMLprocessor << " is processing batchinsert ..." << endl;
                    break;
                }
            case WE_SVR_BATCH_INSERT_END:
                {
                    fWeDMLprocessor->processBatchInsertHwm(ibs, errMsg);
					//timer.finish();
                    break;
                }
            case WE_SVR_UPDATE:
                {
                    rc = fWeDMLprocessor->processUpdate(ibs, errMsg, PMId, blocksChanged);
                    break;
                }
            case WE_SVR_FLUSH_FILES:
                {
                    rc = fWeDMLprocessor->processFlushFiles(ibs, errMsg);
                    break;  
                }
            case WE_SVR_DELETE:
                {
                    rc = fWeDMLprocessor->processDelete(ibs, errMsg, PMId, blocksChanged);
                    break;
                }
            case WE_SVR_BATCH_AUTOON_REMOVE_META:
                {
                    rc = fWeDMLprocessor->processRemoveMeta(ibs, errMsg);
                    break;
                }
            case WE_SVR_DML_BULKROLLBACK:
                {
                    rc = fWeDMLprocessor->processBulkRollback(ibs, errMsg);
                    break;
                }
            case WE_SVR_DML_BULKROLLBACK_CLEANUP:
                {
                    rc = fWeDMLprocessor->processBulkRollbackCleanup(ibs, errMsg);
                    break;
                }
            case WE_UPDATE_NEXTVAL:
                {
                    rc = fWeDMLprocessor->updateSyscolumnNextval(ibs, errMsg);
                    break;
                }
            case WE_SVR_WRITE_SYSTABLE:
                {
                    rc = fWeDDLprocessor->writeSystable(ibs, errMsg);
                    break;
                }
            case WE_SVR_WRITE_SYSCOLUMN:
                {
                    rc = fWeDDLprocessor->writeSyscolumn(ibs, errMsg);
                    break;
                }
            case WE_SVR_WRITE_CREATE_SYSCOLUMN:
                {
                    rc = fWeDDLprocessor->writeCreateSyscolumn(ibs, errMsg);
                    break;
                }
            case WE_SVR_WRITE_CREATETABLEFILES:
                {
                    rc = fWeDDLprocessor->createtablefiles(ibs, errMsg);
                    break;
                }
            case WE_SVR_DELETE_SYSCOLUMN:
                {
                    rc = fWeDDLprocessor->deleteSyscolumn(ibs, errMsg);
                    break;
                }
            case WE_SVR_DELETE_SYSCOLUMN_ROW:
                {
                    rc = fWeDDLprocessor->deleteSyscolumnRow(ibs, errMsg);
                    break;
                }
            case WE_SVR_DELETE_SYSTABLE:
                {
                    rc = fWeDDLprocessor->deleteSystable(ibs, errMsg);
                    break;
                }
            case WE_SVR_DELETE_SYSTABLES:
                {
                    rc = fWeDDLprocessor->deleteSystables(ibs, errMsg);
                    break;
                }
            case WE_SVR_WRITE_DROPFILES:
                {
                    rc = fWeDDLprocessor->dropFiles(ibs, errMsg);
                    break;
                }
            case WE_SVR_UPDATE_SYSCOLUMN_AUTO:
                {
                    rc = fWeDDLprocessor->updateSyscolumnAuto(ibs, errMsg);
                    break;
                }
            case WE_SVR_UPDATE_SYSCOLUMN_NEXTVAL:
                {
                    rc = fWeDDLprocessor->updateSyscolumnNextvalCol(ibs, errMsg);
                    break;
                }
            case WE_SVR_UPDATE_SYSCOLUMN_DEFAULTVAL:
                {
                    rc = fWeDDLprocessor->updateSyscolumnSetDefault(ibs, errMsg);
                    break;
                }
            case WE_SVR_UPDATE_SYSCOLUMN_TABLENAME:
                {
                    rc = fWeDDLprocessor->updateSyscolumnTablename(ibs, errMsg);
                    break;
                }
            case WE_SVR_UPDATE_SYSCOLUMN_RENAMECOLUMN:
                {
                    rc = fWeDDLprocessor->updateSyscolumnRenameColumn(ibs, errMsg);
                    break;
                }
            case WE_SVR_UPDATE_SYSCOLUMN_COLPOS:
                {
                    rc = fWeDDLprocessor->updateSyscolumnColumnposCol(ibs, errMsg);
                    break;
                }
            case WE_SVR_UPDATE_SYSTABLE_AUTO:
                {
                    rc = fWeDDLprocessor->updateSystableAuto(ibs, errMsg);
                    break;
                }
            case WE_SVR_UPDATE_SYSTABLE_TABLENAME:
                {
                    rc = fWeDDLprocessor->updateSystableTablename(ibs, errMsg);
                    break;
                }
            case WE_SVR_UPDATE_SYSTABLES_TABLENAME:
                {
                    rc = fWeDDLprocessor->updateSystablesTablename(ibs, errMsg);
                    break;
                }
            case WE_SVR_FILL_COLUMN:
                {
                    rc = fWeDDLprocessor->fillNewColumn(ibs, errMsg);
                    break;
                }
            case WE_SVR_DROP_PARTITIONS:
                {
                    rc = fWeDDLprocessor->dropPartitions(ibs, errMsg);
                    break;
                }
            case WE_SVR_WRITE_TRUNCATE:
                {
                    rc = fWeDDLprocessor->writeTruncateLog(ibs, errMsg);
                    break;
                }
            case WE_SVR_WRITE_DROPPARTITION:
                {
                    rc = fWeDDLprocessor->writeDropPartitionLog(ibs, errMsg);
                    break;
                }
            case WE_SVR_WRITE_DROPTABLE:
                {
                    rc = fWeDDLprocessor->writeDropTableLog(ibs, errMsg);
                    break;
                }
            case WE_SVR_DELETE_DDLLOG:
                {
                    rc = fWeDDLprocessor->deleteDDLLog(ibs, errMsg);
                    break;
                }
            case WE_SVR_FETCH_DDL_LOGS:
                {
                    rc = fWeDDLprocessor->fetchDDLLog(ibs, errMsg);
                    break;
                }
			case WE_SVR_PURGEFD:
				{
					rc = fWeDMLprocessor->processPurgeFDCache(ibs, errMsg);
					break;
				}
			case WE_END_TRANSACTION:
				{
					rc = fWeDMLprocessor->processEndTransaction(ibs, errMsg);
					break;
				}
			case WE_SVR_CLOSE_CONNECTION:
				{
					break;
				}
            default:
                break;
            }
        }
        catch (std::exception& ex)
        {
            logging::LoggingID logid(19, 0, 0);
            logging::Message::Args args;
            logging::Message msg(1);
            args.add("we_readthread caught exception ");
            args.add(ex.what());
            msg.format(args);
            logging::Logger logger(logid.fSubsysID);
            logger.logMessage(logging::LOG_TYPE_ERROR, msg, logid);
            rc = 1;
            errMsg = msg.msg();
        }
        catch (...)
        {
            logging::LoggingID logid(19, 0, 0);
            logging::Message::Args args;
            logging::Message msg(1);
            args.add("we_readthread caught ... exception ");
            msg.format(args);
            logging::Logger logger(logid.fSubsysID);
            logger.logMessage(logging::LOG_TYPE_ERROR, msg, logid);
            rc = 1;
            errMsg = msg.msg();
        }  
		
		if (msgId != WE_SVR_CLOSE_CONNECTION) 
		{
			//send response
			obs.restart();
			obs << uniqueID;
			obs << rc;
			obs << errMsg;
		}

        if ((msgId == WE_SVR_COMMIT_BATCH_AUTO_ON) || (msgId ==WE_SVR_BATCH_INSERT_END) || (msgId == WE_SVR_FETCH_DDL_LOGS))
        {
            obs += ibs;
            //cout << " sending back hwm info with ibs length " << endl;
        }
        else if ((msgId == WE_SVR_BATCH_INSERT) || (msgId == WE_SVR_UPDATE) || (msgId == WE_SVR_DELETE))
        {
            obs << PMId;
        }
        else if ((msgId == WE_SVR_DML_BULKROLLBACK) ||
                 (msgId == WE_SVR_DML_BULKROLLBACK_CLEANUP))
        {
            obs << Config::getLocalModuleID();
        }

        if (msgId == WE_SVR_UPDATE || msgId == WE_SVR_DELETE)
            obs << blocksChanged; // send stats back to DMLProc
        blocksChanged = 0; // reset
		if (msgId == WE_SVR_CLOSE_CONNECTION)
		{
			//cout << "received request. closing connection ..." << endl;
			break;
		}
		else
		{ 
			try
			{
				fIos.write(obs);
            //cout << "dmlthread sent back response for msgid " << (uint)msgId << " with uniqueID:rc= "
            //<< (uint)uniqueID<<":"<< (uint)rc<<" and error message is " << errMsg <<endl;
            //get next message
				ibs = fIos.read();
			}
			catch (...)
			{
				break;
			}
		}
	}
	//cout << "closing connection for thread " << pthread_self () << endl;
    fIos.close();
}


//-----------------------------------------------------------------------------
//ctor
SplitterReadThread::SplitterReadThread(const messageqcpp::IOSocket& ios,
                                       ByteStream& Ibs):ReadThread(ios), fWeDataLoader(*this)
{
    fIbs = Ibs;
}

//-----------------------------------------------------------------------------
//copy ctor
SplitterReadThread::SplitterReadThread(const SplitterReadThread& rhs): 
ReadThread(rhs.fIos), fWeDataLoader(*this)
{
    fIbs = rhs.fIbs;

}
//-----------------------------------------------------------------------------
// dtor
SplitterReadThread::~SplitterReadThread()
{
}
//-----------------------------------------------------------------------------
/**
* @brief Thread Function which process incoming messages
*
*/
void SplitterReadThread::operator()()
{
    ByteStream::byte msgId;
    while (fIbs.length()>0)
    {
        fWeDataLoader.updateRxBytes(fIbs.length());

        //do work here...
        fIbs >> msgId;

        //cout << (int)msgId << endl;

        switch (msgId)
        {
        case WE_CLT_SRV_KEEPALIVE:
            {
                fWeDataLoader.onReceiveKeepAlive(fIbs);
                break;
            }
        case WE_CLT_SRV_DATA:
            {
                fWeDataLoader.onReceiveData(fIbs);
                break;
            }
        case WE_CLT_SRV_EOD:
            {
                fWeDataLoader.onReceiveEod(fIbs);
                break;
            }
        case WE_CLT_SRV_MODE:
            {
                fWeDataLoader.onReceiveMode(fIbs);
                break;
            }
        case WE_CLT_SRV_IMPFILENAME:
            {
                fWeDataLoader.onReceiveImportFileName(fIbs);
                break;
            }
        case WE_CLT_SRV_CMDLINEARGS:
            {
                fWeDataLoader.onReceiveCmdLineArgs(fIbs);
                break;
            }
        case WE_CLT_SRV_CMD:
            {
                fWeDataLoader.onReceiveCmd(fIbs);   //fig out share_ptr on BS& is better
                break;
            }
        case WE_CLT_SRV_ACK:
            {
                fWeDataLoader.onReceiveAck(fIbs);
                break;
            }
        case WE_CLT_SRV_NAK:
            {
                fWeDataLoader.onReceiveNak(fIbs);
                break;
            }
        case WE_CLT_SRV_PM_ERROR:
            {
                fWeDataLoader.onReceiveError(fIbs);
                break;
            }
        case WE_CLT_SRV_STARTCPI:
            {
                fWeDataLoader.onReceiveStartCpimport();
                break;
            }
        case WE_CLT_SRV_BRMRPT:
            {
                fWeDataLoader.onReceiveBrmRptFileName(fIbs);
                break;
            }
        case WE_CLT_SRV_CLEANUP:
            {
                fWeDataLoader.onReceiveCleanup(fIbs);
                break;
            }
        case WE_CLT_SRV_ROLLBACK:
            {
                fWeDataLoader.onReceiveRollback(fIbs);
                break;
            }
        case WE_CLT_SRV_JOBID:
            {
                fWeDataLoader.onReceiveJobId(fIbs);
                break;
            }
        case WE_CLT_SRV_JOBDATA:
            {
                fWeDataLoader.onReceiveJobData(fIbs);
                break;
            }
        case WE_CLT_SRV_ERRLOG:
            {
                fWeDataLoader.onReceiveErrFileRqst(fIbs);
                break;
            }
        case WE_CLT_SRV_BADLOG:
            {
                fWeDataLoader.onReceiveBadFileRqst(fIbs);
                break;
            }
        default:
            break;

        }

        fIbs.restart();
        try
        {   //get next message
            fIbs = fIos.read();
        }
        catch (...)
        {
            fIbs.restart();     //setting length=0, get out of loop
            cout << "Broken Pipe" << endl;

            logging::LoggingID logid(19, 0, 0);
            logging::Message::Args args;
            logging::Message msg(1);
            args.add("SplitterReadThread::operator: Broken Pipe ");
            msg.format(args);
            logging::Logger logger(logid.fSubsysID);
            logger.logMessage(logging::LOG_TYPE_ERROR, msg, logid);
        }
    }

    fIos.close();

}

//------------------------------------------------------------------------------
// ClearTableLockReadThread constructor.
//------------------------------------------------------------------------------
ClearTableLockReadThread::ClearTableLockReadThread(
                                                  const messageqcpp::IOSocket& ios,
                                                  ByteStream& Ibs ): ReadThread(ios),
fWeClearTableLockCmd(new WE_ClearTableLockCmd("ClearTableLockTool"))
{
    fIbs = Ibs;
}

//------------------------------------------------------------------------------
// ClearTableLockReadThread destructor.
//------------------------------------------------------------------------------
ClearTableLockReadThread::~ClearTableLockReadThread()
{
}

//------------------------------------------------------------------------------
// Thread entry point to ClearTableLockReadThread object used to receive msgs
// from a cleartablelock tool client.
//------------------------------------------------------------------------------
void ClearTableLockReadThread::operator()()
{
    ByteStream::byte msgId;
    ByteStream obs;
    ByteStream::byte rc = 0;
    std::string errMsg;

    // Read msgid from ByteStream and forward to applicable processing function
    while (fIbs.length() > 0)
    {
        fIbs >> msgId;
        switch (msgId)
        {
        case WE_CLT_SRV_CLEAR_TABLE_LOCK:
            {
                rc = fWeClearTableLockCmd->processRollback(fIbs, errMsg);
                break;
            }
        case WE_CLT_SRV_CLEAR_TABLE_LOCK_CLEANUP:
            {
                rc = fWeClearTableLockCmd->processCleanup(fIbs, errMsg);
                break;
            }
        default:
            {
                break;
            }
        }

        // Send response
        obs.restart();
        obs << rc;
        obs << errMsg;

        try
        {
            fIos.write(obs);

            // Get next message
            fIbs = fIos.read();
        }
        catch (...)
        {
            logging::LoggingID logid(19, 0, 0);
            logging::Message::Args args;
            logging::Message msg(1);
            args.add("ClearTableLockReadThread::operator: Broken Pipe ");
            msg.format(args);
            logging::Logger logger(logid.fSubsysID);
            logger.logMessage(logging::LOG_TYPE_ERROR, msg, logid);
            break;
        }
    }

    fIos.close();
}


//------------------------------------------------------------------------------
// RedistributeReadThread constructor.
//------------------------------------------------------------------------------
RedistributeReadThread::RedistributeReadThread(const messageqcpp::IOSocket& ios, ByteStream& Ibs)
: ReadThread(ios)
{
    fIbs = Ibs;
}

//------------------------------------------------------------------------------
// RedistributeReadThread destructor.
//------------------------------------------------------------------------------
RedistributeReadThread::~RedistributeReadThread()
{
}

//------------------------------------------------------------------------------
// Thread entry point to RedistributeReadThread object used to receive msgs
// from a cleartablelock tool client.
//------------------------------------------------------------------------------
void RedistributeReadThread::operator()()
{
    try
    {
        redistribute::Redistribute::handleRedistributeMessage(fIbs, fIos);
    }
    catch (...)
    {
        logging::LoggingID logid(19, 0, 0);
        logging::Message::Args args;
        logging::Message msg(1);
        args.add("RedistributeReadThread::operator exception handled ");
        msg.format(args);
        logging::Logger logger(logid.fSubsysID);
        logger.logMessage(logging::LOG_TYPE_ERROR, msg, logid);
    }

    fIos.close();
}
//------------------------------------------------------------------------------
// GetFileSizeThread constructor.
//------------------------------------------------------------------------------
GetFileSizeThread::GetFileSizeThread(const messageqcpp::IOSocket& ios, ByteStream& Ibs, BRM::DBRM& dbrm)
: ReadThread(ios), fWeGetFileSizes(new WE_GetFileSizes())
{
    fIbs = Ibs;
    key = dbrm.getUnique32();
}

//------------------------------------------------------------------------------
// GetFileSizeThread destructor.
//------------------------------------------------------------------------------
GetFileSizeThread::~GetFileSizeThread()
{
}

//------------------------------------------------------------------------------
// Thread entry point to GetFileSizeThread object used to receive msgs
//------------------------------------------------------------------------------
void GetFileSizeThread::operator()()
{
    ByteStream::byte msgId;
    ByteStream obs;
    ByteStream::byte rc = 0;
    std::string errMsg;

    // Read msgid from ByteStream and forward to applicable processing function
    while (fIbs.length() > 0)
    {
        fIbs >> msgId;
        switch (msgId)
        {
        case WE_SVR_GET_FILESIZES:
            {
                rc = fWeGetFileSizes->processTable(fIbs, errMsg, key);
                break;
            }
        default:
            {
                break;
            }
        }

        // Send response
        obs.restart();
        obs << rc;
		obs << errMsg;
        obs += fIbs;

        try
        {
            fIos.write(obs);

            // Get next message
            fIbs = fIos.read();
        }
        catch (...)
        {
            logging::LoggingID logid(19, 0, 0);
            logging::Message::Args args;
            logging::Message msg(1);
            args.add("GetFileSizeThread::operator: Broken Pipe ");
            msg.format(args);
            logging::Logger logger(logid.fSubsysID);
            logger.logMessage(logging::LOG_TYPE_ERROR, msg, logid);
            break;
        }
    }

    fIos.close();
}

//-----------------------------------------------------------------------------

void ReadThreadFactory::CreateReadThread(ThreadPool& Tp, IOSocket& Ios, BRM::DBRM& dbrm)
{
    struct timespec rm_ts;
    int sleepTime = 20000; // wait for 20 seconds
    rm_ts.tv_sec = sleepTime/1000;
    rm_ts.tv_nsec = sleepTime%1000 *1000000;
    bool isTimeOut = false;

    ByteStream::byte msgId;
    ByteStream aBs;
    try
    {
        aBs = Ios.read(&rm_ts, &isTimeOut);
    }
    catch (std::exception& ex)
    {
        cout<< "Handled : " << ex.what() <<endl;
        logging::LoggingID logid(19, 0, 0);
        logging::Message::Args args;
        logging::Message msg(1);
        args.add("ReadThreadFactory::CreateReadThread: read() error");
        args.add(ex.what());
        msg.format(args);
        logging::Logger logger(logid.fSubsysID);
        logger.logMessage(logging::LOG_TYPE_ERROR, msg, logid);
    }
    if ((aBs.length()<=0)||(isTimeOut))
    {
        Ios.close();
        return;
    }

    aBs.peek(msgId);

    switch (msgId)
    {
    case WE_SVR_DDL_KEEPALIVE:
    case WE_SVR_DML_KEEPALIVE:
        {
            DmlReadThread dmlReadThread(Ios, aBs);
			boost::thread t(dmlReadThread);
			//cout << "starting DML thread id " << t.get_id() << endl;
        }
        break;
    case WE_CLT_SRV_KEEPALIVE:
    case WE_CLT_SRV_MODE:
    case WE_CLT_SRV_DATA:
    case WE_CLT_SRV_CMD:
    case WE_CLT_SRV_ACK:
    case WE_CLT_SRV_NAK:
    case WE_CLT_SRV_PM_ERROR:
    case WE_CLT_SRV_CMDLINEARGS:
        {
            //SplitterReadThread aSpReadThread(Ios, aBs);
            //fOwner.attach(reinterpret_cast<Observer*>(&(aSpReadThread.fWeDataLoader)));
            //Tp.invoke(aSpReadThread);
            Tp.invoke(SplitterReadThread(Ios,aBs));
        }
        break;

    case WE_CLT_SRV_CLEAR_TABLE_LOCK:
    case WE_CLT_SRV_CLEAR_TABLE_LOCK_CLEANUP:
        {
            ClearTableLockReadThread clearTableLockThread(Ios, aBs);
            Tp.invoke( clearTableLockThread );
        }
        break;

    case WE_SVR_REDISTRIBUTE:
        {
            RedistributeReadThread RedistributeReadThread(Ios, aBs);
            Tp.invoke(RedistributeReadThread);
        }
        break;
	case WE_SVR_GET_FILESIZES:
        {
            GetFileSizeThread getFileSizeThread(Ios, aBs, dbrm);
            Tp.invoke(getFileSizeThread);
        }
        break;
    default:
        {
            Ios.close();    // don't know who is this
        }
        break;

    }

}

}

