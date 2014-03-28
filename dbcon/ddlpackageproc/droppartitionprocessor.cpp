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
 *   $Id: droppartitionprocessor.cpp 6567 2010-04-27 19:45:29Z rdempsey $
 *
 *
 ***********************************************************************/
#define DROPPARTITIONPROC_DLLEXPORT
#include "droppartitionprocessor.h"
#undef DROPPARTITIONPROC_DLLEXPORT

#include "messagelog.h"
#include "sqllogger.h"
#include "cacheutils.h"
#include "oamcache.h"
#include "logicalpartition.h"

using namespace std;
using namespace execplan;
using namespace logging;
using namespace WriteEngine;
using namespace oam;

namespace ddlpackageprocessor
{

	DropPartitionProcessor::DDLResult DropPartitionProcessor::processPackage(ddlpackage::DropPartitionStatement& dropPartitionStmt)
	{
		SUMMARY_INFO("DropPartitionProcessor::processPackage");

		DDLResult result;
		result.result = NO_ERROR;   
		std::string err;
		VERBOSE_INFO(dropPartitionStmt);

		// Commit current transaction.
		// all DDL statements cause an implicit commit
		VERBOSE_INFO("Getting current txnID");
		
		int rc = 0;
		rc = fDbrm->isReadWrite();
		BRM::TxnID txnID;
		txnID.id= fTxnid.id;
		txnID.valid= fTxnid.valid;
		if (rc != 0 )
		{
			logging::Message::Args args;
			logging::Message message(9);
			args.add("Unable to execute the statement due to DBRM is read only");
			message.format(args);
			result.result = DROP_ERROR;	
			result.message = message;
			fSessionManager.rolledback(txnID);
			return result;
		}

	
		std::vector <CalpontSystemCatalog::OID> oidList;
		CalpontSystemCatalog::RIDList tableColRidList;
		CalpontSystemCatalog::DictOIDList dictOIDList;
		execplan::CalpontSystemCatalog::ROPair roPair;
		uint32_t processID = 0;
		u_int64_t uniqueID = 0;
		uint32_t sessionID = dropPartitionStmt.fSessionID;
		std::string  processName("DDLProc");
		u_int64_t uniqueId = 0;
		
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
			result.result = ALTER_ERROR;	
			result.message = message;
			fSessionManager.rolledback(txnID);
			return result;
		}
		catch ( ... )
		{
			logging::Message::Args args;
			logging::Message message(9);
			args.add("Unknown error occured while getting unique number.");
			message.format(args);
			result.result = ALTER_ERROR;	
			result.message = message;
			fSessionManager.rolledback(txnID);
			return result;
		}
		
		string stmt = dropPartitionStmt.fSql + "|" + dropPartitionStmt.fTableName->fSchema +"|";
		SQLLogger logger(stmt, fDDLLoggingId, sessionID, txnID.id);

		try 
		{
			//check table lock
			boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(dropPartitionStmt.fSessionID);
			systemCatalogPtr->identity(CalpontSystemCatalog::EC);
			systemCatalogPtr->sessionID(dropPartitionStmt.fSessionID);
			CalpontSystemCatalog::TableName tableName;
			tableName.schema = dropPartitionStmt.fTableName->fSchema;
			tableName.table = dropPartitionStmt.fTableName->fName;
			roPair = systemCatalogPtr->tableRID( tableName );
			//@Bug 3054 check for system catalog
			if ( roPair.objnum < 3000 )
			{
				throw std::runtime_error("Drop partition cannot be operated on Calpont system catalog.");
			}
			int i = 0;
			processID = ::getpid();
			oam::OamCache * oamcache = OamCache::makeOamCache();
			std::vector<int> pmList = oamcache->getModuleIds();
			std::vector<uint> pms;
			for (unsigned i=0; i < pmList.size(); i++)
			{
				pms.push_back((uint)pmList[i]);
			}
				
			try {
				uniqueID = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, (int32_t*)&sessionID, (int32_t*)&txnID.id, BRM::LOADING );
			}
			catch (std::exception&)
			{
				result.result = DROP_ERROR;
				result.message = IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE);
				// no need to release lock. dbrm un-hold the lock
				fSessionManager.rolledback(txnID);
				return result;
			}
			
			if ( uniqueID  == 0 )
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
					// reset
					sessionID = dropPartitionStmt.fSessionID;
					txnID.id= fTxnid.id;
					txnID.valid= fTxnid.valid;
					processID = ::getpid();
					processName = "DDLProc";
					try 
					{
						uniqueID = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, (int32_t*)&sessionID, (int32_t*)&txnID.id, BRM::LOADING );
					}
					catch (std::exception&)
					{
						result.result = DROP_ERROR;
						result.message = IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE);
						fSessionManager.rolledback(txnID);
						return result;
					}

					if (uniqueID > 0)
						break;
				}

				if (i >= numTries) //error out
				{
					result.result = DROP_ERROR;
					logging::Message::Args args;
					args.add(processName);
					args.add((uint64_t)processID);
					args.add((uint64_t)sessionID);
					result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_TABLE_LOCKED,args));
					fSessionManager.rolledback(txnID);
					return result;
				}
			}

			// 1. Get the OIDs for the columns
			// 2. Get the OIDs for the dictionaries
			// 3. Save the OIDs to a log file
			// 4. Disable the extents from extentmap for the partition     	
			// 5. Remove the column and dictionary  files for the partition
			// 6. Flush PrimProc Cache 
			// 7. Remove the extents from extentmap for the partition

			CalpontSystemCatalog::TableName userTableName;
			userTableName.schema = dropPartitionStmt.fTableName->fSchema;
			userTableName.table = dropPartitionStmt.fTableName->fName;
		
			tableColRidList = systemCatalogPtr->columnRIDs( userTableName );
		
			dictOIDList = systemCatalogPtr->dictOIDs( userTableName );

			//Save qualified tablename, all column, dictionary OIDs, and transaction ID into a file in ASCII format
			for ( unsigned i=0; i < tableColRidList.size(); i++ )
			{
				if ( tableColRidList[i].objnum > 3000 )
					oidList.push_back( tableColRidList[i].objnum );
			}
			for ( unsigned i=0; i < dictOIDList.size(); i++ )
			{
				if (  dictOIDList[i].dictOID > 3000 )
					oidList.push_back( dictOIDList[i].dictOID );
			}
		
			//Mark the partition disabled from extent map
			string emsg;
			rc = fDbrm->markPartitionForDeletion( oidList, dropPartitionStmt.fPartitions, emsg);
			if (rc != 0 && rc !=BRM::ERR_PARTITION_DISABLED &&
				  rc != BRM::ERR_INVALID_OP_LAST_PARTITION &&
				  rc != BRM::ERR_NOT_EXIST_PARTITION)
			{
				throw std::runtime_error(emsg);
			}
			
			set<BRM::LogicalPartition> markedPartitions;
			set<BRM::LogicalPartition> outOfServicePartitions;
			
			// only log partitions that are successfully marked disabled.
			rc = fDbrm->getOutOfServicePartitions(oidList[0], outOfServicePartitions);
			
			if (rc != 0)
			{
				string errorMsg;
				BRM::errString(rc, errorMsg);
				ostringstream oss;
				oss << "getOutOfServicePartitions failed  due to " << errorMsg;
				     throw std::runtime_error(oss.str());
			}

			set<BRM::LogicalPartition>::iterator it;
			for (it = dropPartitionStmt.fPartitions.begin(); it != dropPartitionStmt.fPartitions.end(); ++it)
			{
				if (outOfServicePartitions.find(*it) != outOfServicePartitions.end())
					markedPartitions.insert(*it);
			}
			
			//Save the oids to a file
			createWritePartitionLogFile( roPair.objnum, markedPartitions, oidList, uniqueId);

			VERBOSE_INFO("Removing files");
			removePartitionFiles( oidList, markedPartitions, uniqueId );
			//Flush PrimProc cache for those lbids
			rc = cacheutils::flushPartition( oidList, markedPartitions );
			
			//Remove the partition from extent map
			emsg.clear();
			rc = fDbrm->deletePartition( oidList, dropPartitionStmt.fPartitions, emsg);
			if ( rc != 0 )
				throw std::runtime_error(emsg);
		}
		catch (exception& ex)
		{
			cerr << "DropPartitionProcessor::processPackage: " << ex.what() << endl;

			logging::Message::Args args;
			logging::Message message(ex.what());

			if (rc == BRM::ERR_TABLE_NOT_LOCKED)
				result.result = USER_ERROR;
			else if (rc == BRM::ERR_NOT_EXIST_PARTITION || rc == BRM::ERR_INVALID_OP_LAST_PARTITION) 
				result.result = PARTITION_WARNING;
			else if (rc == BRM::ERR_NO_PARTITION_PERFORMED)
				result.result = WARN_NO_PARTITION;
			else
				result.result = DROP_ERROR;
			result.message = message;
			try {
				fDbrm->releaseTableLock(uniqueID);
			} catch (std::exception&)
			{
				result.result = DROP_ERROR;
				result.message = IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE);
			}
			fSessionManager.rolledback(txnID);
			return result;
		}
		catch (...)
		{
			cerr << "DropPartitionProcessor::processPackage: caught unknown exception!" << endl;

			logging::Message::Args args;
			logging::Message message(1);
			args.add("Drop partition failed: ");
			args.add( "encountered unkown exception" );
			args.add("");
			args.add("");
		 	message.format( args );
		
			result.result = DROP_ERROR;
			result.message = message;
			try {
				fDbrm->releaseTableLock(uniqueID);
			} catch (std::exception&)
			{
				result.result = DROP_ERROR;
				result.message = IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE);
			}
			fSessionManager.rolledback(txnID);
			return result;
		}
		// Log the DDL statement
		logging::logDDL(dropPartitionStmt.fSessionID, txnID.id, dropPartitionStmt.fSql, dropPartitionStmt.fOwner);
		//Remove the log file
		//release the transaction
		try {
			fDbrm->releaseTableLock(uniqueID);
			deleteLogFile(DROPPART_LOG, roPair.objnum, uniqueId);
		} catch (std::exception&)
		{
			result.result = DROP_ERROR;
			result.message = IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE);
			fSessionManager.rolledback(txnID);
			return result;
		}
		fSessionManager.committed(txnID);
		return result;
	}

}
