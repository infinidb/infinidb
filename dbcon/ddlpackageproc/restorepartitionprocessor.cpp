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
#define RESTOREPARTITIONPROC_DLLEXPORT
#include "restorepartitionprocessor.h"
#undef RESTOREPARTITIONPROC_DLLEXPORT

#include "messagelog.h"
#include "sqllogger.h"

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
		rc = fDbrm.isReadWrite();
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
		u_int32_t  processID = 0;
        std::string  processName("DDLProc");
        try 
        {
			CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(restorePartitionStmt.fSessionID);
			systemCatalogPtr->identity(CalpontSystemCatalog::EC);
			systemCatalogPtr->sessionID(restorePartitionStmt.fSessionID);
			CalpontSystemCatalog::TableName tableName;
			tableName.schema = restorePartitionStmt.fTableName->fSchema;
			tableName.table = restorePartitionStmt.fTableName->fName;
			uint32_t partition = restorePartitionStmt.fPartition;
			execplan::CalpontSystemCatalog::ROPair roPair;
			roPair = systemCatalogPtr->tableRID( tableName );
			
			//@Bug 3054 check for system catalog
			if ( roPair.objnum < 3000 )
			{
				throw std::runtime_error("Enable partition cannot be operated on Calpont system catalog.");
			}
			
			int i = 0; 
			rc = fSessionManager.setTableLock( roPair.objnum, restorePartitionStmt.fSessionID, processID, processName, true );
					
			if ( rc == BRM::ERR_TABLE_LOCKED_ALREADY )
			{	
				int waitPeriod = 10 * 1000;
				waitPeriod = Config::getWaitPeriod() * 1000;				
				//retry until time out (microsecond)
            			
				for ( ; i < waitPeriod; i+=100 )
				{
					usleep(100);
						
					rc = fSessionManager.setTableLock( roPair.objnum, restorePartitionStmt.fSessionID, processID, processName, true );
						
					if ( rc == 0 )
						break;
				}
    
				if ( i >= waitPeriod ) //error out
				{
					bool  lockStatus;
					ostringstream oss;
					u_int32_t sid;
					rc = fSessionManager.getTableLockInfo( roPair.objnum, processID, processName, lockStatus, sid);	
					if ( lockStatus )
					{
						oss << " table " << restorePartitionStmt.fTableName->fSchema << "." << restorePartitionStmt.fTableName->fName << " is still locked by " << processName << " with ProcessID " << processID;
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
				args.add("Enable partition Failed due to BRM failure");
				result.result = DROP_ERROR;
				result.message = message;
				fSessionManager.rolledback(txnID);
				return result;
			}
			
			SQLLogger logger(restorePartitionStmt.fSql, fDDLLoggingId, restorePartitionStmt.fSessionID, txnID.id);

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
			rc = fDbrm.restorePartition( oidList, partition);
			if ( rc != 0 )
			{
				string errorMsg;
				BRM::errString( rc, errorMsg );
				ostringstream oss;
				oss << "Enable partition failed  due to " << errorMsg;
				throw std::runtime_error(oss.str());
			}
		}
		catch (exception& ex)
		{
			//cerr << "RestorePartitionProcessor::processPackage: " << ex.what() << endl;

        	logging::Message::Args args;
        	logging::Message message(1);
        	args.add("Enable partition failed: ");
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
			fSessionManager.rolledback(txnID);
			return result;
		}
		// Log the DDL statement
		logging::logDDL(restorePartitionStmt.fSessionID, txnID.id, restorePartitionStmt.fSql, restorePartitionStmt.fOwner);		
		fSessionManager.committed(txnID);
        return result;

    }

}
