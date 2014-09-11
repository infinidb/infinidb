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

using namespace std;
using namespace execplan;
using namespace logging;
using namespace WriteEngine;

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
        // all DDL statements cause an implicut commit
        VERBOSE_INFO("Getting current txnID");
		int rc = 0;
		rc = fDbrm.isReadWrite();
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
		uint32_t partition = dropPartitionStmt.fPartition;
		u_int32_t  processID = 0;
        std::string  processName("DDLProc");
        try 
        {
			//check table lock
			CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(dropPartitionStmt.fSessionID);
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
			rc = fSessionManager.setTableLock( roPair.objnum, dropPartitionStmt.fSessionID, processID, processName, true );
					
			if ( rc == BRM::ERR_TABLE_LOCKED_ALREADY )
			{	
				int waitPeriod = 10 * 1000;
				waitPeriod = Config::getWaitPeriod() * 1000;				
				//retry until time out (microsecond)
            			
				for ( ; i < waitPeriod; i+=100 )
				{
					usleep(100);
						
					rc = fSessionManager.setTableLock( roPair.objnum, dropPartitionStmt.fSessionID, processID, processName, true );
						
					if ( rc == 0 )
						break;
				}
    
				if ( i >= waitPeriod ) //error out
				{
					bool  lockStatus;
					ostringstream oss;
					uint32_t sid;
					rc = fSessionManager.getTableLockInfo( roPair.objnum, processID, processName, lockStatus, sid);	
					if ( lockStatus )
					{
						oss << " table " << dropPartitionStmt.fTableName->fSchema << "." << dropPartitionStmt.fTableName->fName << " is still locked by " << processName << " with ProcessID " << processID;
						if ((processName == "DMLProc") && (processID > 0))
						{
							oss << " due to active bulkrollback.";
						}
						oss << endl;
					}
		
					logging::Message::Args args;
					logging::Message message(9);
					args.add(oss.str());
					message.format(args);
					result.result = DROP_ERROR;	
					result.message = message;
					fSessionManager.rolledback(txnID);
					return result;
				}					
					
			}
			else if ( rc  == BRM::ERR_FAILURE)
			{
				logging::Message::Args args;
				logging::Message message(1);
				args.add("Drop partition Failed due to BRM failure");
				result.result = DROP_ERROR;
				result.message = message;
				fSessionManager.rolledback(txnID);
				return result;
			}

			SQLLogger logger(dropPartitionStmt.fSql, fDDLLoggingId, dropPartitionStmt.fSessionID, txnID.id);

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
			
			//Save the oids to a file
			createOpenPartitionLogFile( roPair.objnum, userTableName,  partition);
			writeLogFile ( userTableName, oidList );
		
			//Remove the partition from extent map
			rc = fDbrm.markPartitionForDeletion( oidList, partition);
			if (( rc != 0 ) && ( rc !=BRM::ERR_PARTITION_DISABLED ))
			{
				string errorMsg;
				BRM::errString( rc, errorMsg );
				ostringstream oss;
				oss << errorMsg;
				//Remove the log file
				deleteLogFile();
				throw std::runtime_error(oss.str());
			}
			VERBOSE_INFO("Removing files");
			removePartitionFiles( oidList, partition );
			//Flush PrimProc cache for those lbids
			rc = cacheutils::flushPartition( oidList, partition );
			
			//Remove the partition from extent map
			rc = fDbrm.deletePartition( oidList, partition);
			if ( rc != 0 )
			{
				string errorMsg;
				BRM::errString( rc, errorMsg );
				ostringstream oss;
				oss << errorMsg;
				throw std::runtime_error(oss.str());
			}
//#ifdef _MSC_VER
//			systemCatalogPtr->flushCache();
//#endif
		}
		catch (exception& ex)
		{
			cerr << "DropPartitionProcessor::processPackage: " << ex.what() << endl;

        	logging::Message::Args args;
        	logging::Message message(1);
        	args.add("Drop partition failed: ");
        	args.add( ex.what() );
        	args.add("");
        	args.add("");
       	 	message.format( args );

			if (( rc == BRM::ERR_NOT_EXIST_PARTITION) || (rc == BRM::ERR_INVALID_OP_LAST_PARTITION) || 
				(rc == BRM::ERR_PARTITION_DISABLED) || (rc == BRM::ERR_PARTITION_ENABLED) || (rc == BRM::ERR_TABLE_NOT_LOCKED))
				result.result = USER_ERROR;
			else
				result.result = DROP_ERROR;
        	result.message = message;
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
        	fSessionManager.rolledback(txnID);
			return result;
		}
		// Log the DDL statement
		logging::logDDL(dropPartitionStmt.fSessionID, txnID.id, dropPartitionStmt.fSql, dropPartitionStmt.fOwner);		
		//Remove the log file
		deleteLogFile();
        //release the transaction
        fSessionManager.committed(txnID);
        return result;

    }

}
