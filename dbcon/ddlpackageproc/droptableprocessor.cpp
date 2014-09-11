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
 *   $Id: droptableprocessor.cpp 8672 2012-06-27 14:35:06Z chao $
 *
 *
 ***********************************************************************/
#define DROPTABLEPROC_DLLEXPORT
#include "droptableprocessor.h"
#undef DROPTABLEPROC_DLLEXPORT

#include "messagelog.h"
#include "sqllogger.h"
#include "cacheutils.h"
using namespace cacheutils;
using namespace std;
using namespace execplan;
using namespace logging;
using namespace WriteEngine;

namespace ddlpackageprocessor
{

    DropTableProcessor::DDLResult DropTableProcessor::processPackage(ddlpackage::DropTableStatement& dropTableStmt)
    {
        SUMMARY_INFO("DropTableProcessor::processPackage");

        DDLResult result;
        result.result = NO_ERROR;   
        std::string err;
        VERBOSE_INFO(dropTableStmt);
		
        // Commit current transaction.
        // all DDL statements cause an implicut commit
        VERBOSE_INFO("Getting current txnID");
		int rc = 0;
        BRM::TxnID txnID;
		txnID.id= fTxnid.id;
		txnID.valid= fTxnid.valid;
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
		//@Bug 3890
		SQLLogger logger(dropTableStmt.fSql, fDDLLoggingId, dropTableStmt.fSessionID, txnID.id);
		
		std::vector <CalpontSystemCatalog::OID> oidList;
		CalpontSystemCatalog::RIDList tableColRidList;
		CalpontSystemCatalog::DictOIDList dictOIDList;
		execplan::CalpontSystemCatalog::ROPair roPair;
		std::string  processName("DDLProc");
		u_int32_t  processID = 0;
        try 
        {	
			//check table lock
			CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(dropTableStmt.fSessionID);
			systemCatalogPtr->identity(CalpontSystemCatalog::EC);
			systemCatalogPtr->sessionID(dropTableStmt.fSessionID);
			CalpontSystemCatalog::TableName tableName;
			tableName.schema = dropTableStmt.fTableName->fSchema;
			tableName.table = dropTableStmt.fTableName->fName;
			roPair = systemCatalogPtr->tableRID( tableName );

			int i = 0; 
			rc = fSessionManager.setTableLock( roPair.objnum, dropTableStmt.fSessionID, processID, processName, true );
					
			if ( rc == BRM::ERR_TABLE_LOCKED_ALREADY )
			{	
				int waitPeriod = 10 * 1000;
				waitPeriod = Config::getWaitPeriod() * 1000;				
				//retry until time out (microsecond)
            			
				for ( ; i < waitPeriod; i+=100 )
				{
					usleep(100);
						
					rc = fSessionManager.setTableLock( roPair.objnum, dropTableStmt.fSessionID, processID, processName, true );
						
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
						oss << " table " << dropTableStmt.fTableName->fSchema << "." << dropTableStmt.fTableName->fName << " is locked by " << processName << " with  ProcessID " << processID;
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
					//release the transaction
					fSessionManager.rolledback(txnID);
					return result;
				}					
					
			}
			else if ( rc  == BRM::ERR_FAILURE)
			{
					logging::Message::Args args;
					logging::Message message(1);
					args.add("Drop table Failed due to BRM failure");
					result.result = DROP_ERROR;
					result.message = message;
					//release the transaction
					fSessionManager.rolledback(txnID);
					return result;
			}

        	// 1. Get the OIDs for the columns
        	// 2. Get the OIDs for the dictionaries
        	// 3. Save the OIDs to a log file
        	// 4. Remove the Table from SYSTABLE
        	// 5. Remove the columns from SYSCOLUMN
        	// 6. Commit the changes made to systables
        	// 7. Flush PrimProc Cache
        	// 8. Update extent map
        	// 9. Remove the column and dictionary files
        	// 10.Return the OIDs

        	CalpontSystemCatalog::TableName userTableName;
        	userTableName.schema = dropTableStmt.fTableName->fSchema;
        	userTableName.table = dropTableStmt.fTableName->fName;

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
			createOpenLogFile( roPair.objnum, userTableName );
			writeLogFile ( userTableName, oidList );
			
        	VERBOSE_INFO("Removing the SYSTABLE meta data");
        	removeSysTableMetaData( dropTableStmt.fSessionID, txnID.id, result, *dropTableStmt.fTableName );
        	if (result.result != NO_ERROR)
        	{
            	err = "removeSysTableMetaData failed";
            	throw std::runtime_error(err);
        	}

        	VERBOSE_INFO("Removing the SYSCOLUM meta data");
        	removeSysColMetaData( dropTableStmt.fSessionID, txnID.id, result, *dropTableStmt.fTableName );
        	if (result.result != NO_ERROR)
        	{
            	err = "removeSysColMetaData failed";
            	throw std::runtime_error(err);
        	}
		
			// register the changes
			rc = fWriteEngine.commit( txnID.id );
			if ( rc != WriteEngine::NO_ERROR)
			{
				WErrorCodes   ec;
				ostringstream oss;
				oss << "Commit failed: " << ec.errorString(rc);
				throw std::runtime_error(oss.str());
			}
//#ifdef _MSC_VER
//			systemCatalogPtr->flushCache();
//#endif
		}
		catch (exception& ex)
		{
			cerr << "DropTableProcessor::processPackage: " << ex.what() << endl;
			rc = fWriteEngine.rollbackTran(txnID.id, dropTableStmt.fSessionID);
			ostringstream oss;
			if (rc != 0 )
			{
				WriteEngine::WErrorCodes   ec;
				oss << " problem with rollback. " << ec.errorString(rc);
			}
        	logging::Message::Args args;
        	logging::Message message(1);
        	args.add("Drop table failed: ");
        	args.add( ex.what() );
        	args.add(oss.str());
        	args.add("");
       	 	message.format( args );

        	result.result = DROP_ERROR;
        	result.message = message;
        	
        	fSessionManager.rolledback(txnID);
			rc = fSessionManager.setTableLock( roPair.objnum, dropTableStmt.fSessionID, processID, processName, false );
			//Remove log file
			deleteLogFile();
			return result;
		}
		catch (...)
		{
			cerr << "DropTableProcessor::processPackage: caught unknown exception!" << endl;
			rc = fWriteEngine.rollbackTran(txnID.id, dropTableStmt.fSessionID);
			ostringstream oss;
			if (rc != 0 )
			{
				WriteEngine::WErrorCodes   ec;
				oss << " problem with rollback. " << ec.errorString(rc);
			}
        	logging::Message::Args args;
        	logging::Message message(1);
        	args.add("Drop table failed: ");
        	args.add( "encountered unkown exception" );
        	args.add(oss.str());
        	args.add("");
       	 	message.format( args );

        	result.result = DROP_ERROR;
        	result.message = message;
        	fSessionManager.rolledback(txnID);
			rc = fSessionManager.setTableLock( roPair.objnum, dropTableStmt.fSessionID, processID, processName, false );
			//Remove log file
			deleteLogFile();
			return result;
		}
		// Log the DDL statement
		logging::logDDL(dropTableStmt.fSessionID, txnID.id, dropTableStmt.fSql, dropTableStmt.fOwner);
		rc = fSessionManager.setTableLock( roPair.objnum, dropTableStmt.fSessionID, processID, processName, false );
		try {
			VERBOSE_INFO("Removing files");
			removeFiles( txnID.id, oidList );
			//Flush PrimProc cache for those oids
		    rc = cacheutils::flushOIDsFromCache( oidList );
			VERBOSE_INFO("Removing extents");
        	removeExtents( txnID.id, result, oidList );
        	
		}
		catch (exception& ex) 
        {
#ifdef _MSC_VER
			//FIXME: Windows can't delete a file that's still open by another process
#else
            logging::Message::Args args;
        	logging::Message message(1);
        	args.add( "Remove column files failed." );
        	args.add( ex.what() );
        	args.add("");
       	 	message.format( args );

        	result.result = WARNING;
        	result.message = message;
        	fSessionManager.rolledback(txnID);
			return result;
#endif
        }
		catch ( ... )
		{
			logging::Message::Args args;
        	logging::Message message(1);
        	args.add("Drop table failed: ");
        	args.add( "Remove column files failed." );
        	args.add("");
        	args.add("");
       	 	message.format( args );

        	result.result = WARNING;
        	result.message = message;
        	fSessionManager.rolledback(txnID);
			return result;
		}
		
		//Remove the log file
		deleteLogFile();
        //release the transaction
        fSessionManager.committed(txnID);
        returnOIDs( tableColRidList, dictOIDList );

        return result;

    }

    TruncTableProcessor::DDLResult TruncTableProcessor::processPackage(ddlpackage::TruncTableStatement& truncTableStmt)
    {
        SUMMARY_INFO("TruncTableProcessor::processPackage");
		// 1. Get the OIDs for the columns
        // 2. Get the OIDs for the dictionaries
        // 3. Save the OIDs
        // 4. Flush PrimProc Cache
        // 5. Update extent map
        // 6. Remove the column and dictionary files
        // 7. Use the OIDs to create new column and dictionary files with abbreviate extent
		// 8. Update next value if the table has autoincrement column
		
		DDLResult result;
        result.result = NO_ERROR;   
        std::string err;
        VERBOSE_INFO(truncTableStmt);
			
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
		//@Bug 3890.
		SQLLogger logger(truncTableStmt.fSql, fDDLLoggingId, truncTableStmt.fSessionID, txnID.id);
		std::vector <CalpontSystemCatalog::OID> columnOidList;
		std::vector <CalpontSystemCatalog::OID> allOidList;
		CalpontSystemCatalog::RIDList tableColRidList;
		CalpontSystemCatalog::DictOIDList dictOIDList;
		execplan::CalpontSystemCatalog::ROPair roPair;
		std::string  processName("DDLProc");
		u_int32_t  processID = 0;
		CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(truncTableStmt.fSessionID);
		systemCatalogPtr->identity(CalpontSystemCatalog::EC);
		systemCatalogPtr->sessionID(truncTableStmt.fSessionID);
		CalpontSystemCatalog::TableInfo tableInfo;
		CalpontSystemCatalog::TableName userTableName;
        userTableName.schema = truncTableStmt.fTableName->fSchema;
        userTableName.table = truncTableStmt.fTableName->fName;
        try 
        {	
			//check table lock
			
			CalpontSystemCatalog::TableName tableName;
			tableName.schema = truncTableStmt.fTableName->fSchema;
			tableName.table = truncTableStmt.fTableName->fName;
			roPair = systemCatalogPtr->tableRID( tableName );

			int i = 0; 
			rc = fSessionManager.setTableLock( roPair.objnum, truncTableStmt.fSessionID, processID, processName, true );
					
			if ( rc == BRM::ERR_TABLE_LOCKED_ALREADY )
			{	
				int waitPeriod = 10 * 1000;
				waitPeriod = Config::getWaitPeriod() * 1000;				
				//retry until time out (microsecond)
            			
				for ( ; i < waitPeriod; i+=100 )
				{
					usleep(100);
						
					rc = fSessionManager.setTableLock( roPair.objnum, truncTableStmt.fSessionID, processID, processName, true );
						
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
						oss << " table " << truncTableStmt.fTableName->fSchema << "." << truncTableStmt.fTableName->fName << " is locked by " << processName << "  with  ProcessID " << processID;
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
					//release the transaction
					fSessionManager.rolledback(txnID);
					return result;
				}					
					
			}
			else if ( rc  == BRM::ERR_FAILURE)
			{
					logging::Message::Args args;
					logging::Message message(1);
					args.add("Drop table Failed due to BRM failure");
					result.result = DROP_ERROR;
					result.message = message;
					//release the transaction
					fSessionManager.rolledback(txnID);
					return result;
			}

        	tableColRidList = systemCatalogPtr->columnRIDs( userTableName );

        	dictOIDList = systemCatalogPtr->dictOIDs( userTableName );
			for ( unsigned i=0; i < tableColRidList.size(); i++ )
			{
				if ( tableColRidList[i].objnum > 3000 )
				{
					columnOidList.push_back( tableColRidList[i].objnum );
					allOidList.push_back( tableColRidList[i].objnum );
				}
			}
			for ( unsigned i=0; i < dictOIDList.size(); i++ )
			{
				if (  dictOIDList[i].dictOID > 3000 )
					allOidList.push_back( dictOIDList[i].dictOID );
			}
			//Check whether the table has autoincrement column
			tableInfo = systemCatalogPtr->tableInfo(userTableName);
		}
		catch (exception& ex)
		{
			cerr << "DropTableProcessor::processPackage: " << ex.what() << endl;
		
        	logging::Message::Args args;
        	logging::Message message(1);
        	args.add("Truncate table failed: ");
        	args.add( ex.what() );
        	args.add("");
       	 	message.format( args );

        	result.result = TRUNC_ERROR;
        	result.message = message;
        	
        	fSessionManager.rolledback(txnID);
			rc = fSessionManager.setTableLock( roPair.objnum, truncTableStmt.fSessionID, processID, processName, false );
			return result;
		}
		catch (...)
		{
			cerr << "DropTableProcessor::processPackage: caught unknown exception!" << endl;
			
        	logging::Message::Args args;
        	logging::Message message(1);
        	args.add("Truncate table failed: ");
        	args.add( "encountered unkown exception" );
        	args.add("");
       	 	message.format( args );

        	result.result = TRUNC_ERROR;
        	result.message = message;
        	fSessionManager.rolledback(txnID);
			rc = fSessionManager.setTableLock( roPair.objnum, truncTableStmt.fSessionID, processID, processName, false );
			return result;
		}
		
		//Save the oids to a file
		createOpenTruncateTableLogFile( roPair.objnum, userTableName );
		writeLogFile ( userTableName, allOidList );
		
		try {
			VERBOSE_INFO("Removing files");
			uint16_t dbroot = 0;
			uint32_t partitionNum = 0;
			fDbrm.getStartExtent(columnOidList[0], dbroot, partitionNum, true);
			//Disable extents first
			rc = fDbrm.markAllPartitionForDeletion( allOidList);
			if (rc != 0)
			{
				string errMsg;
				BRM::errString(rc, errMsg);
				throw std::runtime_error(errMsg);
			}
			
			removeFiles( txnID.id, allOidList ); //will drop PrimProc FD cache
			
			rc = cacheutils::flushOIDsFromCache(allOidList);	
			
			VERBOSE_INFO("Removing extents");
        	removeExtents( txnID.id, result, allOidList );
			
			//Create all column and dictionary files
			for (unsigned col  = 0; col < columnOidList.size(); col++)
			{
				CalpontSystemCatalog::ColType colType = systemCatalogPtr->colType(columnOidList[col]);
				rc = fWriteEngine.createColumn(txnID.id, columnOidList[col], static_cast<WriteEngine::ColDataType>(colType.colDataType), colType.colWidth, dbroot, 0, colType.compressionType);	
			}
			for (unsigned col  = 0; col <dictOIDList.size(); col++)
			{
				CalpontSystemCatalog::ColType colType = systemCatalogPtr->colTypeDct(dictOIDList[col].dictOID);
				rc = fWriteEngine.createDctnry(txnID.id, dictOIDList[col].dictOID, colType.colWidth, dbroot, 0, 0, colType.compressionType );	
			}
			
		}
		catch (exception& ex) 
        {
#ifdef _MSC_VER
			//FIXME: Windows can't delete a file that's still open by another process
#else
            logging::Message::Args args;
        	logging::Message message(1);
        	args.add( "Truncate table failed." );
        	args.add( ex.what() );
        	args.add("");
       	 	message.format( args );

        	result.result = TRUNC_ERROR;
        	result.message = message;
			//rc = fSessionManager.setTableLock( roPair.objnum, truncTableStmt.fSessionID, processID, processName, false );
        	fSessionManager.rolledback(txnID);
			return result;
#endif
        }
		catch ( ... )
		{
			logging::Message::Args args;
        	logging::Message message(1);
        	args.add("Truncate table failed: ");
        	args.add( "Remove column files failed." );
        	args.add("");
        	args.add("");
       	 	message.format( args );

        	result.result = TRUNC_ERROR;
        	result.message = message;
			//rc = fSessionManager.setTableLock( roPair.objnum, truncTableStmt.fSessionID, processID, processName, false );
        	fSessionManager.rolledback(txnID);
			return result;
		}
		//Check whether the table has autoincrement column
		if (tableInfo.tablewithautoincr == 1)
		{
			//reset nextvalue to 1
			WriteEngineWrapper::AutoIncrementValue_t nextValueMap;
			nextValueMap[roPair.objnum] = 1;
			rc = fWriteEngine.updateNextValue (nextValueMap,truncTableStmt.fSessionID);			
		}
		// Log the DDL statement
		logging::logDDL(truncTableStmt.fSessionID, txnID.id, truncTableStmt.fSql, truncTableStmt.fOwner);
		rc = fSessionManager.setTableLock( roPair.objnum, truncTableStmt.fSessionID, processID, processName, false );
        //release the transaction
        fSessionManager.committed(txnID);
		//Remove the log file
		deleteLogFile();
        return result;
    }

}
