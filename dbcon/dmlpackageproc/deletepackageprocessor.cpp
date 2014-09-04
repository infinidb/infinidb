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
 *   $Id: deletepackageprocessor.cpp 9138 2012-12-11 20:13:47Z chao $
 *
 *
 ***********************************************************************/

#include <iostream>
#include <boost/scoped_ptr.hpp>
using namespace std;
#define DELETEPKGPROC_DLLEXPORT
#include "deletepackageprocessor.h"
#undef DELETEPKGPROC_DLLEXPORT
#include "writeengine.h"
#include "joblistfactory.h"
#include "messagelog.h"
#include "dataconvert.h"
#include "simplecolumn.h"
#include "messagelog.h"
#include "sqllogger.h"
#include "stopwatch.h"
#include "dbrm.h"
#include "idberrorinfo.h"
#include "errorids.h"
#include "rowgroup.h"
#include "bytestream.h"
#include "columnresult.h"
#include "we_messages.h"
#include "oamcache.h"
#include "tablelockdata.h"
#include "bytestream.h"

using namespace WriteEngine;
using namespace dmlpackage;
using namespace execplan;
using namespace logging;
using namespace dataconvert;
using namespace joblist;
using namespace BRM;
using namespace rowgroup;
using namespace messageqcpp;
using namespace oam;

namespace dmlpackageprocessor
{
  std::map<unsigned, bool> pmStateDel;
  DMLPackageProcessor::DMLResult
  DeletePackageProcessor::processPackage(dmlpackage::CalpontDMLPackage& cpackage)
  {
    SUMMARY_INFO("DeletePackageProcessor::processPackage");

    DMLResult result;
    result.result = NO_ERROR;
    BRM::TxnID txnid;
    fSessionID = cpackage.get_SessionID();
	//StopWatch timer;
    VERBOSE_INFO("DeletePackageProcessor is processing CalpontDMLPackage ...");
	TablelockData * tablelockData = TablelockData::makeTablelockData(fSessionID);
	uint64_t uniqueId = fDbrm.getUnique64();
	uint64_t tableLockId = 0;
	// get the table object from the package
    DMLTable* tablePtr =  cpackage.get_Table();
	std::string schemaName = tablePtr->get_SchemaName();
    std::string tableName = tablePtr->get_TableName();
	boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog( fSessionID );
	CalpontSystemCatalog::TableName aTableName;
	aTableName.table = tableName;
	aTableName.schema = schemaName;
	fWEClient->addQueue(uniqueId);
    try
    {
      // set-up the transaction
      txnid.id  = cpackage.get_TxnID();		
	  txnid.valid = true;

      SQLLogger sqlLogger(cpackage.get_SQLStatement(), DMLLoggingId, fSessionID, txnid.id);

      if ( 0 != tablePtr )
      {
        
		
		execplan::CalpontSystemCatalog::ROPair roPair;
		roPair = csc->tableRID(aTableName);

		tableLockId = tablelockData->getTablelockId(roPair.objnum); //check whether this table is locked already for this session
		if (tableLockId == 0)
		{
			//cout << "tablelock is not found in cache " << endl;
			u_int32_t  processID = ::getpid();
			int32_t   txnId = txnid.id;
			std::string  processName("DMLProc");
			int32_t sessionId = fSessionID;
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
		const CalpontSystemCatalog::RIDList ridList = csc->columnRIDs(aTableName);
		CalpontSystemCatalog::RIDList::const_iterator rid_iterator = ridList.begin();
		CalpontSystemCatalog::ColType colType;
		while (rid_iterator != ridList.end())
		{
			// If user hit ctrl+c in the mysql console, fRollbackPending will be true.
			if (fRollbackPending)
			{
				result.result = JOB_CANCELED;
				break;
			}
			CalpontSystemCatalog::ROPair roPair = *rid_iterator;
			colType = csc->colType(roPair.objnum);
			if (colType.autoincrement)
			{
				uint64_t nextVal = csc->nextAutoIncrValue(aTableName);
				fDbrm.startAISequence(roPair.objnum, nextVal, colType.colWidth);
				break; //Only one autoincrement column per table
			}
			++rid_iterator;
		}
		
		uint64_t  rowsProcessed = 0;
		
		rowsProcessed = fixUpRows(cpackage, result, uniqueId);
		//@Bug 4994 Cancelled job is not error
		if (result.result == JOB_CANCELED)
			throw std::runtime_error("Query execution was interrupted");
					
		if ((result.result != 0) && (result.result != DMLPackageProcessor::IDBRANGE_WARNING))
			throw std::runtime_error(result.message.msg());
					
		result.rowCount = rowsProcessed;

		// Log the DML statement.
		logging::logDML(cpackage.get_SessionID(), txnid.id, cpackage.get_SQLStatement(), cpackage.get_SchemaName());
	  }
    }
    catch (exception& ex)
    {
      cerr << "DeletePackageProcessor::processPackage: " << ex.what() << endl;
      
	  //@Bug 4994 Cancelled job is not error
      if (result.result == 0)
      {
		  result.result = DELETE_ERROR;
      }
      result.message = Message(ex.what());
    }
    catch (...)
    {
      cerr << "DeletePackageProcessor::processPackage: caught unknown exception!" << endl;
      logging::Message::Args args;
      logging::Message message(6);
      args.add( "Delete Failed: ");
      args.add( "encountered unknown exception" );
      args.add(result.message.msg());
      args.add("");
      message.format( args );

      result.result = DELETE_ERROR;
      result.message = message;
    }
	//timer.finish();
	//@Bug 1886,2870 Flush VM cache only once per statement. 
	std::map<u_int32_t,u_int32_t> oids;
    flushDataFiles(0, oids, uniqueId, txnid);

	if (fRollbackPending)
	{
		result.result = JOB_CANCELED;
		logging::Message::Args args1;
		args1.add("Query execution was interrupted");
		result.message.format(args1);
	}
	
	fWEClient->removeQueue(uniqueId);

    VERBOSE_INFO("Finished Processing Delete DML Package");
    return result;
  }

  uint64_t DeletePackageProcessor::fixUpRows (dmlpackage::CalpontDMLPackage& cpackage, DMLResult& result, const uint64_t uniqueId)
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
				pmStateDel[fPMs[i]] = true;
				tmpPMs.push_back(fPMs[i]);
			}
		}
		
		fPMs.swap(tmpPMs);
#else
		for (unsigned i=0; i<fPMs.size(); i++)
		{
			pmStateDel[fPMs[i]] = true;
		}
#endif

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
			cerr << "DeletePackageProcessor::processPackage::fixupRows" << endl;
			logging::Message::Args args;
			logging::Message message(2);
			args.add("Update Failed: ");
			args.add("Lost connection to ExeMgr");
			message.format(args);
			result.result = UPDATE_ERROR;
			result.message = message;
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
			cerr << "DeletePackageProcessor::processPackage::fixupRows" << endl;
			logging::Message::Args args;
			logging::Message message(2);
			args.add("Delete Failed: ");
			args.add(emsgStr);
			message.format(args);
			result.result = UPDATE_ERROR;
			result.message = message;
			return rowsProcessed;
		}
		
		while (true)
		{
			if (fRollbackPending)
			{
				result.result = JOB_CANCELED;
				err = true;
				break;
			}
			msg.restart();
			msgBk.restart();
			msg = fExeMgr->read();
			msgBk = msg;
			if ( msg.length() == 0 )
			{
				cerr << "DeletePackageProcessor::processPackage::fixupRows" << endl;
				logging::Message::Args args;
				logging::Message message(2);
				args.add("Delete Failed: ");
				args.add("Lost connection to ExeMgr");
				message.format(args);
				result.result = UPDATE_ERROR;
				result.message = message;
				//return rowsProcessed;
				break;
			}
			else
			{
				if (rowGroup.get() == NULL)
				{
					//This is meta data, need to send to all PMs.
					metaData = true;
					//cout << "sending meta data" << endl;
					err = processRowgroup(msgBk, result, uniqueId, cpackage, metaData, dbroot);
					rowGroup.reset(new rowgroup::RowGroup());
					rowGroup->deserialize(msg);
					qb = 100;
					msg.restart();
					msg << qb;
					//cout << "Projecting rows" << endl;
					fExeMgr->write(msg);
					metaData = false;
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
					args.add("Delete Failed: ");
					args.add(errorMsg);
					message.format(args);
					result.result = UPDATE_ERROR;
					result.message = message;
					DMLResult tmpResult;
					receiveAll( tmpResult, uniqueId, fPMs);
					//@Bug 4358 get rid of broken pipe error.
					//msg.restart();
					//msg << qb;
					//fExeMgr->write(msg);
					//return rowsProcessed;
					break;
				}
				if (rowGroup->getData() == NULL)
				{
					msg.restart();			
				}
				if (rowGroup->getRowCount() == 0)  //done fetching
				{
					err = receiveAll( result, uniqueId, fPMs);
					//return rowsProcessed;
					break;
				}
				if (rowGroup->getBaseRid() == (uint64_t) (-1 & ~0x1fff))
				{
					continue;  // @bug4247, not valid row ids, may from small side outer
				}
				dbroot = rowGroup->getDBRoot();
				err = processRowgroup(msgBk, result, uniqueId, cpackage, metaData, dbroot);
				if (err)
				{
					DMLResult tmpResult;
					receiveAll( tmpResult, uniqueId, fPMs);
					//@Bug 4358 get rid of broken pipe error.
					//msg.restart();
					//msg << qb;
					//fExeMgr->write(msg);
					//return rowsProcessed;	
					break;
				}
				rowsProcessed += rowGroup->getRowCount();
			}
		}
		if (fRollbackPending)
		{
			err = true;
			// Response to user
			cerr << "DeletePackageProcessor::processPackage::fixupRows Rollback Pending" << endl;
			
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

		if (!err)
		{
			// get stats from ExeMgr
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
	}
	catch (runtime_error& ex)
	{
		cerr << "DeletePackageProcessor::processPackage::fixupRows" << ex.what() << endl;
		logging::Message::Args args;
		logging::Message message(2);
		args.add("Delete Failed: ");
		args.add(ex.what());
		message.format(args);
		result.result = UPDATE_ERROR;
		result.message = message;
		return rowsProcessed;
	}
	catch ( ... )
	{
		cerr << "DeletePackageProcessor::processPackage::fixupRows" << endl;
		logging::Message::Args args;
		logging::Message message(2);
		args.add("Update Failed: ");
		args.add("Unknown error caught when communicating with ExeMgr");
		message.format(args);
		result.result = UPDATE_ERROR;
		result.message = message;
		return rowsProcessed;
	}
	
	return rowsProcessed;
  }
  
bool DeletePackageProcessor::processRowgroup(ByteStream & aRowGroup, DMLResult& result, const uint64_t uniqueId, 
			dmlpackage::CalpontDMLPackage& cpackage, bool isMeta, uint dbroot)
{
	bool rc = false;
	//cout << "Get dbroot " << dbroot << endl;
	int pmNum = (*fDbRootPMMap)[dbroot];
	DMLTable* tablePtr =  cpackage.get_Table();
	ByteStream bytestream;
	bytestream << (ByteStream::byte)WE_SVR_DELETE;
	bytestream << uniqueId;
	bytestream << (ByteStream::quadbyte) pmNum;
	bytestream << u_int32_t(cpackage.get_SessionID());
	bytestream << (ByteStream::quadbyte) cpackage.get_TxnID();
	bytestream << tablePtr->get_SchemaName();
	bytestream << tablePtr->get_TableName();
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
		fWEClient->write_to_all(bytestream);
		while (1)
		{
			if (msgRecived == fPMCount)
				break;
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = true;
				break;
			}			
			else {
				*bsIn >> tmp8;
				rc = (tmp8 != 0);
				if (rc != 0) {
					*bsIn >> errorMsg;
					break;
				}
				else
					msgRecived++;						
			}
		}
		return rc;
	}
	
	if (pmStateDel[pmNum])
	{ 	
		try {
			fWEClient->write(bytestream, (uint)pmNum);
			//cout << "sent tp pm " << pmNum<<endl;
			pmStateDel[pmNum] = false;
		}
		catch (runtime_error& ex) //write error
		{
			rc = true;
			logging::Message::Args args;
			logging::Message message(2);
			args.add("Delete Failed: ");
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
			args.add("Delete Failed: ");
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
					errorMsg = "Lost connection to Write Engine Server while deleting";
					throw std::runtime_error(errorMsg); 
				}			
				else {
					*bsIn >> tmp8;
					rc = (tmp8 != 0);
					*bsIn >> errorMsg;
					*bsIn >> tmp32;
					*bsIn >> blocksChanged;
					result.stats.fBlocksChanged += blocksChanged;
					result.stats.fErrorNo = tmp8;
					
					//cout << "received from pm " << (uint)tmp32 << " and rc = " << rc << endl;
					pmStateDel[tmp32] = true;
					if (rc != 0) {
						throw std::runtime_error(errorMsg); 
					}
					if ( tmp32 == (uint)pmNum )
					{
						fWEClient->write(bytestream, (uint)pmNum);
						pmStateDel[pmNum] = false;
						break;
					}		
				}
			}
			catch (runtime_error& ex) //write error
			{
				rc = true;
				logging::Message::Args args;
				logging::Message message(2);
				args.add("Delete Failed: ");
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
				args.add("Delete Failed: ");
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

bool DeletePackageProcessor::receiveAll(DMLResult& result, const uint64_t uniqueId, std::vector<int>& fPMs)
{
	//check how many message we need to receive
	uint messagesNotReceived = 0;
	bool err = false;
	for (unsigned i=0; i<fPMs.size(); i++)
	{
		if (!pmStateDel[fPMs[i]])
			messagesNotReceived++;
	}
	
	if (messagesNotReceived > 0)
	{
		LoggingID logid( DMLLoggingId, fSessionID, fSessionID);
		if ( messagesNotReceived > fPMCount)
		{
			logging::Message::Args args1;
			logging::Message msg(1);
			args1.add("Delete outstanding messages exceed PM count , need to receive messages:PMcount = ");
			ostringstream oss;
			oss << messagesNotReceived <<":"<<fPMCount;
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
					errorMsg = "Lost connection to Write Engine Server while deleting";
					throw std::runtime_error(errorMsg); 
				}			
				else {
					*bsIn >> tmp8;
					err = (tmp8 != 0);
					*bsIn >> errorMsg;
					*bsIn >> tmp32;
					*bsIn >> blocksChanged;
					//cout << "Received response from pm " << tmp32 << endl;
					pmStateDel[tmp32] = true;
					
					if (err) {
						throw std::runtime_error(errorMsg); 
					}
					msgReceived++;
					result.stats.fBlocksChanged += blocksChanged;
					result.stats.fErrorNo = tmp8;
				}
			}
			catch (runtime_error& ex) //write error
			{
				err = true;
				logging::Message::Args args;
				logging::Message message(2);
				args.add("Delete Failed: ");
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
				args.add("Delete Failed: ");
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
