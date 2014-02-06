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
 *   $Id: droppartitionprocessor.cpp 6567 2010-04-27 19:45:29Z rdempsey $
 *
 *
 ***********************************************************************/
#define RESTOREPARTITIONPROC_DLLEXPORT
#include "restorepartitionprocessor.h"
#undef RESTOREPARTITIONPROC_DLLEXPORT

#include "messagelog.h"
#include "sqllogger.h"
#include "oamcache.h"

using namespace std;
using namespace execplan;
using namespace logging;
using namespace WriteEngine;

namespace ddlpackageprocessor
{

	RestorePartitionProcessor::DDLResult RestorePartitionProcessor::processPackage(ddlpackage::RestorePartitionStatement& restorePartitionStmt)
	{
		SUMMARY_INFO("RestorePartitionProcessor::processPackage");

		DDLResult result;
		result.result = NO_ERROR;   
		std::string err;
		VERBOSE_INFO(restorePartitionStmt);

		BRM::TxnID txnID;
		txnID.id= fTxnid.id;
		txnID.valid= fTxnid.valid;
		
		int rc = 0;
		rc = fDbrm->isReadWrite();
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
		std::string  processName("DDLProc");

		string stmt = restorePartitionStmt.fSql + "|" + restorePartitionStmt.fTableName->fSchema +"|";
		SQLLogger logger(stmt, fDDLLoggingId, restorePartitionStmt.fSessionID, txnID.id);
		
		uint32_t processID = 0;
		uint64_t uniqueID = 0;
		uint32_t sessionID = restorePartitionStmt.fSessionID;
		execplan::CalpontSystemCatalog::ROPair roPair;
	
		try 
		{
			//check table lock
			boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(restorePartitionStmt.fSessionID);
			systemCatalogPtr->identity(CalpontSystemCatalog::EC);
			systemCatalogPtr->sessionID(restorePartitionStmt.fSessionID);
			CalpontSystemCatalog::TableName tableName;
			tableName.schema = restorePartitionStmt.fTableName->fSchema;
			tableName.table = restorePartitionStmt.fTableName->fName;
			roPair = systemCatalogPtr->tableRID( tableName );
			//@Bug 3054 check for system catalog
			if ( roPair.objnum < 3000 )
			{
				throw std::runtime_error("Drop partition cannot be operated on Calpont system catalog.");
			}
			int i = 0;
			processID = ::getpid();
			oam::OamCache * oamcache = oam::OamCache::makeOamCache();
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
					sessionID = restorePartitionStmt.fSessionID;
					txnID.id= fTxnid.id;
					txnID.valid= fTxnid.valid;
					processID = ::getpid();
					processName = "DDLProc";
					
					try {
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
			// 4. Remove the extents from extentmap
			// 5. Flush PrimProc Cache        	
			// 6. Remove the column and dictionary  files for the partition

			CalpontSystemCatalog::TableName userTableName;
			userTableName.schema = restorePartitionStmt.fTableName->fSchema;
			userTableName.table = restorePartitionStmt.fTableName->fName;
			
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

			//Remove the partition from extent map
			string emsg;
			rc = fDbrm->restorePartition( oidList, restorePartitionStmt.fPartitions, emsg);
			if ( rc != 0 )
			{
				throw std::runtime_error(emsg);
			}
		}
		catch (exception& ex)
		{
			logging::Message::Args args;
			logging::Message message(ex.what());

			if (( rc == BRM::ERR_NOT_EXIST_PARTITION) || (rc == BRM::ERR_INVALID_OP_LAST_PARTITION) || 
				 (rc == BRM::ERR_PARTITION_DISABLED) || (rc == BRM::ERR_TABLE_NOT_LOCKED))
				result.result = USER_ERROR;
			else if (rc == BRM::ERR_PARTITION_ENABLED)
				result.result = PARTITION_WARNING;
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
			//cerr << "RestorePartitionProcessor::processPackage: caught unknown exception!" << endl;

			logging::Message::Args args;
			logging::Message message(1);
			args.add("Enable partition: ");
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
		logging::logDDL(restorePartitionStmt.fSessionID, txnID.id, restorePartitionStmt.fSql, restorePartitionStmt.fOwner);
		try {
			fDbrm->releaseTableLock(uniqueID);
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
