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

//   $Id: updatepackageprocessor.cpp 9138 2012-12-11 20:13:47Z chao $

#include <iostream>
#include <fstream>
#include <ctype.h>
#include <string>
//#define NDEBUG
#include <cassert>
#include <map>
#include <boost/scoped_ptr.hpp>
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
#include "we_messages.h"
#include "tablelockdata.h"
#include "oamcache.h"

using namespace WriteEngine;
using namespace dmlpackage;
using namespace execplan;
using namespace logging;
using namespace dataconvert;
using namespace joblist;
using namespace rowgroup;
using namespace messageqcpp;
using namespace BRM;
using namespace oam;

//#define PROFILE 1
namespace dmlpackageprocessor
{
std::map<unsigned, bool> pmState;

//StopWatch timer;
DMLPackageProcessor::DMLResult
UpdatePackageProcessor::processPackage(dmlpackage::CalpontDMLPackage& cpackage)
{
    SUMMARY_INFO("UpdatePackageProcessor::processPackage");

    std::string err;
    DMLResult result;
    result.result = NO_ERROR;
	result.rowCount = 0;
	BRM::TxnID txnid;
    fSessionID = cpackage.get_SessionID();
    VERBOSE_INFO("Processing Update DML Package...");
	TablelockData * tablelockData = TablelockData::makeTablelockData(fSessionID);
	uint64_t uniqueId = fDbrm.getUnique64();
	uint64_t tableLockId = 0;
	CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
	CalpontSystemCatalog::TableName tableName;		
	// get the table object from the package
    DMLTable* tablePtr =  cpackage.get_Table();
	tableName.schema = tablePtr->get_SchemaName();
	tableName.table = tablePtr->get_TableName();
	fWEClient->addQueue(uniqueId);
//#ifdef PROFILE
//	StopWatch timer;
//#endif
    try
    {
        // set-up the transaction
        txnid.id  = cpackage.get_TxnID();		
		txnid.valid = true;
		LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
		logging::Message::Args args1;
		logging::Message msg(1);
		args1.add("Start SQL statement: ");
		args1.add(cpackage.get_SQLStatement());
		msg.format( args1 );
		logging::Logger logger(logid.fSubsysID);
		logger.logMessage(LOG_TYPE_DEBUG, msg, logid);

        VERBOSE_INFO("The table name is:");
        VERBOSE_INFO(tablePtr->get_TableName());	
        if (0 != tablePtr)
        {
            // get the row(s) from the table
            RowList rows = tablePtr->get_RowList();
            if (rows.size() == 0)
            {
                SUMMARY_INFO("No row to update!");
				fWEClient->removeQueue(uniqueId);
                return result;
            }
         
			execplan::CalpontSystemCatalog::ROPair roPair;
			
			roPair = systemCatalogPtr->tableRID(tableName);
			tableLockId = tablelockData->getTablelockId(roPair.objnum); //check whether this table is locked already for this session
			if (tableLockId == 0)
			{
				//cout << "tablelock is not found in cache, getting from dbrm" << endl;
				u_int32_t  processID = ::getpid();
				int32_t   txnId = txnid.id;
				int32_t sessionId = fSessionID;
				std::string  processName("DMLProc");
				int i = 0;
				OamCache * oamcache = OamCache::makeOamCache();
				std::vector<int> pmList = oamcache->getModuleIds();
				std::vector<uint> pms;
				for (unsigned i=0; i < pmList.size(); i++)
				{
					pms.push_back((uint)pmList[i]);
				}
				
				try {
					tableLockId = fDbrm.getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId, &txnId, BRM::LOADING );
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
							tableLockId = fDbrm.getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId, &txnId, BRM::LOADING );
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
						result.result = UPDATE_ERROR;
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
			//@Bug 4491 start AI sequence for autoincrement column
			const CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(tableName);
			CalpontSystemCatalog::RIDList::const_iterator rid_iterator = ridList.begin();
			CalpontSystemCatalog::ColType colType;
			while (rid_iterator != ridList.end())
			{
				// If user hit ctrl+c in the mysql console, this will be true.
				if (fRollbackPending)
				{
					result.result = JOB_CANCELED;
					break;
				}
				CalpontSystemCatalog::ROPair roPair = *rid_iterator;
				colType = systemCatalogPtr->colType(roPair.objnum);
				if (colType.autoincrement)
				{
					uint64_t nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
					fDbrm.startAISequence(roPair.objnum, nextVal, colType.colWidth);
					break; //Only one autoincrement column per table
				}
				++rid_iterator;
			}
			
			uint64_t  rowsProcessed = 0;
			if (!fRollbackPending)
			{
				rowsProcessed = fixUpRows(cpackage, result, uniqueId);
			}
			
			//@Bug 4994 Cancelled job is not error
			if (result.result == JOB_CANCELED)
				throw std::runtime_error("Query execution was interrupted");
			
			if ((result.result != 0) && (result.result != DMLPackageProcessor::IDBRANGE_WARNING))
				throw std::runtime_error(result.message.msg());
					
			result.rowCount = rowsProcessed;
					
			// Log the update statement.
			LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
			logging::Message::Args args1;
			logging::Message msg(1);
			args1.add("End SQL statement");
			msg.format( args1 );
			logging::Logger logger(logid.fSubsysID);
			logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
			logging::logDML(cpackage.get_SessionID(), txnid.id, cpackage.get_SQLStatement(), cpackage.get_SchemaName());
		}
	}
    catch (std::exception& ex)
    {
        cerr << "UpdatePackageProcessor::processPackage:" << ex.what() << endl;

		if (result.result == 0)
		{
			result.result = UPDATE_ERROR;
		}
        result.message = Message(ex.what());
		result.rowCount = 0;
		LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
		logging::Message::Args args1;
		logging::Message msg(1);
		args1.add("End SQL statement with error");
		msg.format( args1 );
		logging::Logger logger(logid.fSubsysID);
		logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
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
		result.rowCount = 0;
		LoggingID logid( DMLLoggingId, fSessionID, txnid.id);
		logging::Message::Args args1;
		logging::Message msg(1);
		args1.add("End SQL statement with error");
		msg.format( args1 );
		logging::Logger logger(logid.fSubsysID);
		logger.logMessage(LOG_TYPE_DEBUG, msg, logid); 
    }
  // timer.finish();
	//@Bug 1886,2870 Flush VM cache only once per statement. send to all PMs.
	//WriteEngineWrapper writeEngine;
	std::map<u_int32_t,u_int32_t> oids;
    if (result.result == NO_ERROR || result.result == IDBRANGE_WARNING)
    {
        if (flushDataFiles(NO_ERROR, oids, uniqueId, txnid) != NO_ERROR)
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
            result.rowCount = 0;
        }
    }
    else
    {
        //@Bug 4563. Always flush.
        flushDataFiles(NO_ERROR, oids, uniqueId, txnid);
    }
	//timer.finish();

/*	if (result.result != IDBRANGE_WARNING)
		flushDataFiles(result.result, oids, uniqueId, txnid);
	else
		flushDataFiles(0, oids, uniqueId, txnid);
*/	
	if (fRollbackPending)
	{
		result.result = JOB_CANCELED;
		logging::Message::Args args1;
		args1.add("Query execution was interrupted");
		result.message.format(args1);
	}

	fWEClient->removeQueue(uniqueId);
    VERBOSE_INFO("Finished Processing Update DML Package");
    return result;
}

uint64_t UpdatePackageProcessor::fixUpRows(dmlpackage::CalpontDMLPackage& cpackage, DMLResult& result, const uint64_t uniqueId)
{
	ByteStream msg, msgBk, emsgBs;
	ByteStream::quadbyte qb = 4;
	msg << qb;
	boost::scoped_ptr<rowgroup::RowGroup> rowGroup;
	uint64_t  rowsProcessed = 0;
	uint dbroot = 1;
	bool metaData = false;
	oam::OamCache * oamCache = oam::OamCache::makeOamCache();
	std::vector<int> fPMs = oamCache->getModuleIds();
	
	try {
#if !defined(_MSC_VER) && !defined(SKIP_OAM_INIT)
		//@Bug 4495 check PM status first
		std::vector<int> tmpPMs;
		for (unsigned i=0; i<fPMs.size(); i++)
		{
			int opState = 0;
			bool aDegraded = false;
			ostringstream aOss;
			aOss << "pm" << fPMs[i];
			std::string aModName = aOss.str();
			try
			{
				fOam.getModuleStatus(aModName, opState, aDegraded);
			}
			catch(std::exception& ex)
			{
				ostringstream oss;
				oss << "Exception on getModuleStatus on module ";
				oss <<	aModName;
				oss <<  ":  ";
				oss <<  ex.what();
				throw runtime_error( oss.str() );
			}

			if(opState == oam::ACTIVE )
			{
				pmState[fPMs[i]] = true;
				tmpPMs.push_back(fPMs[i]);
			}
		}
		
		fPMs.swap(tmpPMs);
#else
		for (unsigned i=0; i<fPMs.size(); i++)
		{
			pmState[fPMs[i]] = true;
		}
#endif
	//timer.start("ExeMgr");
		fExeMgr->write(msg);
		fExeMgr->write(*(cpackage.get_ExecutionPlan()));	
		//cout << "sending to ExeMgr plan with length " << (cpackage.get_ExecutionPlan())->length() << endl;
		msg.restart();
		emsgBs.restart();
		msg = fExeMgr->read(); //error handling
		emsgBs = fExeMgr->read();
		string emsg;			
		ByteStream::quadbyte qb;
							
		if (emsgBs.length() == 0)
		{
			cerr << "UpdatePackageProcessor::processPackage::fixupRows" << endl;
			logging::Message::Args args;
			logging::Message message(2);
			args.add("Update Failed: ");
			args.add("Lost connection to ExeMgr");
			message.format(args);
			result.result = UPDATE_ERROR;
			result.message = message;
			//timer.finish();
			return rowsProcessed;
		}
		string emsgStr;
		emsgBs >> emsgStr;
		bool err = false;
				
		if (msg.length() == 4)
		{
			msg >> qb;
			if (qb != 0) 
				err = true;
		}
		else
			err = true;
		if (err)
		{
			cerr << "UpdatePackageProcessor::processPackage::fixupRows" << endl;
			logging::Message::Args args;
			logging::Message message(2);
			args.add("Update Failed: ");
			args.add(emsgStr);
			message.format(args);
			result.result = UPDATE_ERROR;
			result.message = message;
			//timer.finish();
			return rowsProcessed;
		}
		//timer.stop("ExeMgr");
		while (true)
		{
			if (fRollbackPending)
			{
				break;
			}
			msg.restart();
			msgBk.restart();
			msg = fExeMgr->read();
			msgBk = msg;
			if ( msg.length() == 0 )
			{
				cerr << "UpdatePackageProcessor::processPackage::fixupRows" << endl;
				logging::Message::Args args;
				logging::Message message(2);
				args.add("Update Failed: ");
				args.add("Lost connection to ExeMgr");
				message.format(args);
				result.result = UPDATE_ERROR;
				result.message = message;
				//timer.finish();
				//return rowsProcessed;
				break;
			}
			else
			{
				if (rowGroup.get() == NULL)
				{
					//This is mete data, need to send all PMs.
					metaData = true;
					//cout << "sending meta data" << endl;
					//timer.start("Meta");
					err = processRowgroup(msgBk, result, uniqueId, cpackage, metaData, dbroot);
					rowGroup.reset(new rowgroup::RowGroup());
					rowGroup->deserialize(msg);
					qb = 100;
					msg.restart();
					msg << qb;
					fExeMgr->write(msg);
					metaData = false;
					//timer.stop("Meta");
					continue;
				}

				rowGroup->setData(const_cast<uint8_t*>(msg.buf())); 
				err = (rowGroup->getStatus() != 0);
				if (err)
				{
					msgBk.advance(rowGroup->getDataSize());
					string errorMsg;
					msgBk >> errorMsg;
					logging::Message::Args args;
					logging::Message message(2);
					args.add("Update Failed: ");
					args.add(errorMsg);
					message.format(args);
					result.result = UPDATE_ERROR;
					result.message = message;
					DMLResult tmpResult;
					receiveAll( tmpResult, uniqueId, fPMs);
/*					qb = 100;
					//@Bug 4358 get rid of broken pipe error.
					msg.restart();
					msg << qb;
					fExeMgr->write(msg);
*/					//timer.finish();
					//return rowsProcessed;
					//err = true;
					break;
				}
				if (rowGroup->getData() == NULL)
				{
					msg.restart();			
				}
				if (rowGroup->getRowCount() == 0)  //done fetching
				{
					//timer.finish();
					//need to receive all response
					err = receiveAll( result, uniqueId, fPMs);
					//return rowsProcessed;
					break;
				}
				if (rowGroup->getBaseRid() == (uint64_t) (-1 & ~0x1fff))
				{
					continue;  // @bug4247, not valid row ids, may from small side outer
				}
				dbroot = rowGroup->getDBRoot();
				//cout << "dbroot in the rowgroup is " << dbroot << endl;
				//timer.start("processRowgroup");
				err = processRowgroup(msgBk, result, uniqueId, cpackage, metaData, dbroot);
				//timer.stop("processRowgroup");
				if (err) {
					//timer.finish();
					LoggingID logid( DMLLoggingId, fSessionID, cpackage.get_TxnID());
					logging::Message::Args args1;
					logging::Message msg1(1);
					args1.add("SQL statement erroring out, need to receive all messages from WES");
					msg1.format( args1 );
					logging::Logger logger(logid.fSubsysID);
					logger.logMessage(LOG_TYPE_DEBUG, msg1, logid); 
					DMLResult tmpResult;
					receiveAll( tmpResult, uniqueId, fPMs);
					logging::Message::Args args2;
					logging::Message msg2(1);
					args2.add("SQL statement erroring out, received all messages from WES");
					msg2.format( args2 );
					logger.logMessage(LOG_TYPE_DEBUG, msg2, logid); 
					//@Bug 4358 get rid of broken pipe error.
/*					msg.restart();
					msg << qb;
					fExeMgr->write(msg);
					return rowsProcessed;	
*/
					//err = true;
					break;
				}
				rowsProcessed += rowGroup->getRowCount();
			}
		}
		if (fRollbackPending)
		{
			err = true;
			// Response to user
			cerr << "UpdatePackageProcessor::processPackage::fixupRows Rollback Pending" << endl;
			//@Bug 4994 Cancelled job is not error
			result.result = JOB_CANCELED;

			// Log 
			LoggingID logid( DMLLoggingId, fSessionID, cpackage.get_TxnID());
			logging::Message::Args args1;
			logging::Message msg1(1);
			args1.add("SQL statement canceled by user");
			msg1.format( args1 );
			logging::Logger logger(logid.fSubsysID);
			logger.logMessage(LOG_TYPE_DEBUG, msg1, logid); 

			// Clean out the pipe;
			DMLResult tmpResult;
			receiveAll( tmpResult, uniqueId, fPMs);
		}
		// get stats from ExeMgr
		if (!err)
		{
			qb = 3;
			msg.restart();
			msg << qb;
			fExeMgr->write(msg);
			msg = fExeMgr->read();
			msg >> result.queryStats;
			msg >> result.extendedStats;
			msg >> result.miniStats;
			result.stats.unserialize(msg);
		}
		
		//@Bug 4358 get rid of broken pipe error by sending a dummy bs.
		if (err)
		{
			msg.restart();
			msg << qb;
			fExeMgr->write(msg);
		}
		return rowsProcessed;	
		//stats.insert();
	}
	catch (runtime_error& ex)
	{
		cerr << "UpdatePackageProcessor::processPackage::fixupRows" << ex.what() << endl;
		logging::Message::Args args;
		logging::Message message(2);
		args.add("Update Failed: ");
		args.add(ex.what());
		message.format(args);
		result.result = UPDATE_ERROR;
		result.message = message;
		return rowsProcessed;
	}
	catch ( ... )
	{
		cerr << "UpdatePackageProcessor::processPackage::fixupRows" << endl;
		logging::Message::Args args;
		logging::Message message(2);
		args.add("Update Failed: ");
		args.add("Unknown error caught when communicating with ExeMgr");
		message.format(args);
		result.result = UPDATE_ERROR;
		result.message = message;
		return rowsProcessed;
	}
	//timer.finish();
	return rowsProcessed;
}

bool UpdatePackageProcessor::processRowgroup(ByteStream & aRowGroup, DMLResult& result, const uint64_t uniqueId, 
			dmlpackage::CalpontDMLPackage& cpackage, bool isMeta, uint dbroot)
{
	bool rc = false;
	//cout << "Get dbroot " << dbroot << endl;
	int pmNum = (*fDbRootPMMap)[dbroot];
	
	ByteStream bytestream;
	bytestream << (ByteStream::byte)WE_SVR_UPDATE;
	bytestream << uniqueId;
	bytestream << (ByteStream::quadbyte) pmNum;
	bytestream << (ByteStream::quadbyte) cpackage.get_TxnID();
	bytestream += aRowGroup;
	//cout << "sending rows to pm " << pmNum << " with msg length " << bytestream.length() << endl;
	uint msgRecived = 0;
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bsIn.reset(new ByteStream());
	ByteStream::byte tmp8;
	string errorMsg;
	ByteStream::quadbyte tmp32;
	uint64_t blocksChanged = 0;
	
	if (isMeta) //send to all PMs
	{
		cpackage.write(bytestream);
		fWEClient->write_to_all(bytestream);
		while (1)
		{
			if (msgRecived == fWEClient->getPmCount())
				break;
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = true;
				break;
			}			
			else {
				*bsIn >> tmp8;
				if (tmp8 > 0)
				{
					*bsIn >> errorMsg;
					rc = true;
					logging::Message::Args args;
					logging::Message message(2);
					args.add("Update Failed: ");
					args.add(errorMsg);
					message.format(args);
					result.result = UPDATE_ERROR;
					result.message = message;
					break;
				}
				else
					msgRecived++;						
			}
		}
		return rc;
	}
	
	if (pmState[pmNum])
	{ 	
		try {
			//cout << "sending rows to pm " << pmNum << " with msg length " << bytestream.length() << endl;
			fWEClient->write(bytestream, (uint)pmNum);
			pmState[pmNum] = false;
		}
		catch (runtime_error& ex) //write error
		{
			rc = true;
			logging::Message::Args args;
			logging::Message message(2);
			args.add("Update Failed: ");
			args.add(ex.what());
			message.format(args);
			result.result = UPDATE_ERROR;
			result.message = message;
		}
		catch (...)
		{
			rc = true;
			logging::Message::Args args;
			logging::Message message(2);
			args.add("Update Failed: ");
			args.add("Unknown error caught when communicating with WES");
			message.format(args);
			result.result = UPDATE_ERROR;
			result.message = message;
		}
	}
	else
	{							
		while (1)
		{
			bsIn.reset(new ByteStream());
			try {
				fWEClient->read(uniqueId, bsIn);
				if ( bsIn->length() == 0 ) //read error
				{
					rc = true;
					errorMsg = "Lost connection to Write Engine Server while updating";
					throw std::runtime_error(errorMsg); 
				}			
				else {
					*bsIn >> tmp8;
					*bsIn >> errorMsg;
					if (tmp8 == IDBRANGE_WARNING)
					{
						result.result = IDBRANGE_WARNING;	
						logging::Message::Args args;
						logging::Message message(2);
						args.add(errorMsg);
						message.format(args);
						result.message = message;
					}
					else if (tmp8 > 0)
					{
						result.stats.fErrorNo = tmp8;
						rc = (tmp8 != 0);
					}
					*bsIn >> tmp32;
					//cout << "Received response from pm " << tmp32 << endl;
					pmState[tmp32] = true;
					*bsIn >> blocksChanged;
					result.stats.fBlocksChanged += blocksChanged;
					if (rc != 0) {
						throw std::runtime_error(errorMsg); 
					}
					if ( tmp32 == (uint)pmNum )
					{
						//cout << "sending rows to pm " << pmNum << " with msg length " << bytestream.length() << endl;
						fWEClient->write(bytestream, (uint)pmNum);
						pmState[pmNum] = false;
						break;
					}
				}
			}
			catch (runtime_error& ex) //write error
			{
				rc = true;
				logging::Message::Args args;
				logging::Message message(2);
				args.add("Update Failed: ");
				args.add(ex.what());
				message.format(args);
				result.result = UPDATE_ERROR;
				result.message = message;
				break;
			}
			catch (...)
			{
				rc = true;
				logging::Message::Args args;
				logging::Message message(2);
				args.add("Update Failed: ");
				args.add("Unknown error caught when communicating with WES");
				message.format(args);
				result.result = UPDATE_ERROR;
				result.message = message;
				break;
			}
		}
	}
	return rc;
}

bool UpdatePackageProcessor::receiveAll(DMLResult& result, const uint64_t uniqueId, std::vector<int>& fPMs)
{
	//check how many message we need to receive
	uint messagesNotReceived = 0;
	bool err = false;
	for (unsigned i=0; i<fPMs.size(); i++)
	{
		if (!pmState[fPMs[i]])
			messagesNotReceived++;
	}
	
	if (messagesNotReceived > 0)
	{
		LoggingID logid( DMLLoggingId, fSessionID, fSessionID);
		if ( messagesNotReceived > fWEClient->getPmCount())
		{
			logging::Message::Args args1;
			logging::Message msg(1);
			args1.add("Update outstanding messages exceed PM count , need to receive messages:PMcount = ");
			ostringstream oss;
			oss << messagesNotReceived <<":"<<fWEClient->getPmCount();
			args1.add(oss.str());
			msg.format( args1 );
			logging::Logger logger(logid.fSubsysID);
			logger.logMessage(LOG_TYPE_ERROR, msg, logid); 
			err = true;
			logging::Message::Args args;
			logging::Message message(2);
			args.add("Update Failed: ");
			args.add("One of WriteEngineServer went away.");
			message.format(args);
			result.result = UPDATE_ERROR;
			result.message = message;
			return err;
		}
		
		boost::shared_ptr<messageqcpp::ByteStream> bsIn;
		bsIn.reset(new ByteStream());
		ByteStream::byte tmp8;
		string errorMsg;
		ByteStream::quadbyte tmp32;
		uint msgReceived = 0;
		uint64_t blocksChanged = 0;

		while (1)
		{
			if (msgReceived == messagesNotReceived)
				break;
				
			bsIn.reset(new ByteStream());
			try {
				fWEClient->read(uniqueId, bsIn);
				if ( bsIn->length() == 0 ) //read error
				{
					err = true;
					errorMsg = "Lost connection to Write Engine Server while updating";
					throw std::runtime_error(errorMsg); 
				}			
				else {
					*bsIn >> tmp8;
					*bsIn >> errorMsg;
					if (tmp8 == IDBRANGE_WARNING)
					{
						result.result = IDBRANGE_WARNING;	
						logging::Message::Args args;
						logging::Message message(2);
						args.add(errorMsg);
						message.format(args);
						result.message = message;				
					}
					else
					{
						result.stats.fErrorNo = tmp8;
						err = (tmp8 != 0);
					}
					*bsIn >> tmp32;
					*bsIn >> blocksChanged;
					//cout << "Received response from pm " << tmp32 << endl;
					pmState[tmp32] = true;
					if (err) {
						throw std::runtime_error(errorMsg); 
					}
					msgReceived++;
					result.stats.fBlocksChanged += blocksChanged;
				}
			}
			catch (runtime_error& ex) //write error
			{
				err = true;
				logging::Message::Args args;
				logging::Message message(2);
				args.add("Update Failed: ");
				args.add(ex.what());
				message.format(args);
				result.result = UPDATE_ERROR;
				result.message = message;
				break;
			}
			catch (...)
			{
				err = true;
				logging::Message::Args args;
				logging::Message message(2);
				args.add("Update Failed: ");
				args.add("Unknown error caught when communicating with WES");
				message.format(args);
				result.result = UPDATE_ERROR;
				result.message = message;
				break;
			}
		}	
	}
	return err;
}
} // namespace dmlpackageprocessor
