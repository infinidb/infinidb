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
 *   $Id: droptableprocessor.cpp 9744 2013-08-07 03:32:19Z bwilkinson $
 *
 *
 ***********************************************************************/
#include <unistd.h>
#include <string>
#include <vector>
using namespace std;

#define DROPTABLEPROC_DLLEXPORT
#include "droptableprocessor.h"
#undef DROPTABLEPROC_DLLEXPORT

#include "we_messages.h"
#include "we_ddlcommandclient.h"
using namespace WriteEngine;

#include "cacheutils.h"
using namespace cacheutils;

#include "bytestream.h"
using namespace messageqcpp;

#include "sqllogger.h"
#include "messagelog.h"
using namespace logging;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "oamcache.h"
using namespace oam;

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
	// all DDL statements cause an implicit commit
	VERBOSE_INFO("Getting current txnID");
	ByteStream::byte rc = 0;
	BRM::TxnID txnID;
	txnID.id= fTxnid.id;
	txnID.valid= fTxnid.valid;
	int rc1 = 0;
	rc1= fDbrm->isReadWrite();
	if (rc1 != 0 )
	{
		Message::Args args;
		Message message(9);
		args.add("Unable to execute the statement due to DBRM is read only");
		message.format(args);
		result.result = DROP_ERROR;	
		result.message = message;
		fSessionManager.rolledback(txnID);
		return result;
	}
	
	string stmt = dropTableStmt.fSql + "|" + dropTableStmt.fTableName->fSchema +"|";
	SQLLogger logger(stmt, fDDLLoggingId, dropTableStmt.fSessionID, txnID.id);
	
	std::vector <CalpontSystemCatalog::OID> oidList;
	CalpontSystemCatalog::RIDList tableColRidList;
	CalpontSystemCatalog::DictOIDList dictOIDList;
	execplan::CalpontSystemCatalog::ROPair roPair;
	std::string errorMsg;
	ByteStream bytestream;
	uint64_t uniqueId = 0;
	//Bug 5070. Added exception handling
	try {
		uniqueId = fDbrm->getUnique64();
	}
	catch (std::exception& ex)
	{
		Message::Args args;
		Message message(9);
		args.add(ex.what());
		message.format(args);
		result.result = DROP_ERROR;	
		result.message = message;
		fSessionManager.rolledback(txnID);
		return result;
	}
	catch ( ... )
	{
		Message::Args args;
		Message message(9);
		args.add("Unknown error occured while getting unique number.");
		message.format(args);
		result.result = DROP_ERROR;	
		result.message = message;
		fSessionManager.rolledback(txnID);
		return result;
	}
	
	fWEClient->addQueue(uniqueId);
	int pmNum = 1;
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	uint64_t tableLockId = 0;
	OamCache* oamcache = OamCache::makeOamCache();
	std::vector<int> moduleIds = oamcache->getModuleIds();
	try 
	{	
		//check table lock
		boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(dropTableStmt.fSessionID);
		systemCatalogPtr->identity(CalpontSystemCatalog::EC);
		systemCatalogPtr->sessionID(dropTableStmt.fSessionID);
		CalpontSystemCatalog::TableName tableName;
		tableName.schema = dropTableStmt.fTableName->fSchema;
		tableName.table = dropTableStmt.fTableName->fName;
		roPair = systemCatalogPtr->tableRID( tableName );

		u_int32_t  processID = ::getpid();
		int32_t   txnid = txnID.id;
		int32_t sessionId = dropTableStmt.fSessionID;
		std::string  processName("DDLProc");
		int i = 0;
			
		std::vector<uint> pms;
		for (unsigned i=0; i < moduleIds.size(); i++)
		{
			pms.push_back((uint)moduleIds[i]);
		}
			
		try {
			tableLockId = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId, &txnid, BRM::LOADING );
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
			waitPeriod = WriteEngine::Config::getWaitPeriod();
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
					txnid = txnID.id;
					sessionId = dropTableStmt.fSessionID;;
					processName = "DDLProc";
					tableLockId = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId, &txnid, BRM::LOADING );
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
				Message::Args args;
				args.add(processName);
				args.add((uint64_t)processID);
				args.add(sessionId);
				throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_TABLE_LOCKED,args));
			}			
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
		Oam oam;

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
		
		//get a unique number
		VERBOSE_INFO("Removing the SYSTABLE meta data");
#ifdef IDB_DDL_DEBUG
cout << "Removing the SYSTABLEs meta data" << endl;
#endif
		bytestream << (ByteStream::byte)WE_SVR_DELETE_SYSTABLE;
		bytestream << uniqueId;
		bytestream << (u_int32_t) dropTableStmt.fSessionID;
		bytestream << (u_int32_t)txnID.id;
		bytestream << dropTableStmt.fTableName->fSchema;
		bytestream << dropTableStmt.fTableName->fName;
		
		//Find out where systable is
		BRM::OID_t sysOid = 1001;
		ByteStream::byte rc = 0;
		
		u_int16_t  dbRoot;
		rc = fDbrm->getSysCatDBRoot(sysOid, dbRoot);  
		if (rc != 0)
		{
			result.result =(ResultCode) rc;
			Message::Args args;
			Message message(9);
			args.add("Error while calling getSysCatDBRoot");
			args.add(errorMsg);
			result.message = message;
			//release transaction
			fSessionManager.rolledback(txnID);
			return result;
		}
		
		boost::shared_ptr<std::map<int, int> > dbRootPMMap = oamcache->getDBRootToPMMap();
		pmNum = (*dbRootPMMap)[dbRoot];
		try
		{
			//cout << "deleting systable entries with txnid " << txnID.id << endl;
			fWEClient->write(bytestream, (uint)pmNum);
#ifdef IDB_DDL_DEBUG
cout << "Drop table sending WE_SVR_DELETE_SYSTABLES to pm " << pmNum << endl;
#endif				
			while (1)
			{
				bsIn.reset(new ByteStream());
				fWEClient->read(uniqueId, bsIn);
				if ( bsIn->length() == 0 ) //read error
				{
					rc = NETWORK_ERROR;
					errorMsg = "Lost connection to Write Engine Server while updating SYSTABLES";
					break;
				}			
				else {
					*bsIn >> rc;
					if (rc != 0) {
						*bsIn >> errorMsg;
					}
					break;
				}
			}
		}
		catch (runtime_error& ex) //write error
		{
#ifdef IDB_DDL_DEBUG
cout << "Drop table got exception" << endl;
#endif
			rc = NETWORK_ERROR;
			errorMsg = ex.what();
		}
		catch (...)
		{
			rc = NETWORK_ERROR;
#ifdef IDB_DDL_DEBUG
cout << "Drop table got unknown exception" << endl;
#endif
		}
	
		if (rc != 0)
		{
			Message::Args args;
			Message message(9);
			args.add("Error in dropping table from systables.");
			args.add(errorMsg);
			message.format(args);
			result.result = (ResultCode)rc;
			result.message = message;
			//release table lock and session
			fSessionManager.rolledback(txnID);
			(void)fDbrm->releaseTableLock(tableLockId);
			fWEClient->removeQueue(uniqueId);
			return result;				
		}

		//remove from syscolumn
		bytestream.restart();
		bytestream << (ByteStream::byte)WE_SVR_DELETE_SYSCOLUMN;
		bytestream << uniqueId;
		bytestream << (u_int32_t) dropTableStmt.fSessionID;
		bytestream << (u_int32_t)txnID.id;
		bytestream << dropTableStmt.fTableName->fSchema;
		bytestream << dropTableStmt.fTableName->fName;
		
		//Find out where syscolumn is
		sysOid = 1021;
		rc = fDbrm->getSysCatDBRoot(sysOid, dbRoot);  
		if (rc != 0)
		{
			result.result =(ResultCode) rc;
			Message::Args args;
			Message message(9);
			args.add("Error while calling getSysCatDBRoot");
			args.add(errorMsg);
			result.message = message;
			//release transaction
			fSessionManager.rolledback(txnID);
			return result;
		}
		
		pmNum = (*dbRootPMMap)[dbRoot];
		try
		{
			//cout << "deleting systable entries with txnid " << txnID.id << endl;
			fWEClient->write(bytestream, (uint)pmNum);
#ifdef IDB_DDL_DEBUG
cout << "Drop table sending WE_SVR_DELETE_SYSTABLES to pm " << pmNum << endl;
#endif				
			while (1)
			{
				bsIn.reset(new ByteStream());
				fWEClient->read(uniqueId, bsIn);
				if ( bsIn->length() == 0 ) //read error
				{
					rc = NETWORK_ERROR;
					errorMsg = "Lost connection to Write Engine Server while updating SYSTABLES";
					break;
				}			
				else {
					*bsIn >> rc;
					if (rc != 0) {
						*bsIn >> errorMsg;
					}
					break;
				}
			}
		}
		catch (runtime_error& ex) //write error
		{
#ifdef IDB_DDL_DEBUG
cout << "Drop table got exception" << endl;
#endif
			rc = NETWORK_ERROR;
			errorMsg = ex.what();
		}
		catch (...)
		{
			rc = NETWORK_ERROR;
#ifdef IDB_DDL_DEBUG
cout << "Drop table got unknown exception" << endl;
#endif
		}
	
		if (rc != 0)
		{
			Message::Args args;
			Message message(9);
			args.add("Error in dropping table from systables.");
			args.add(errorMsg);
			message.format(args);
			result.result = (ResultCode)rc;
			result.message = message;
			//release table lock and session
			fSessionManager.rolledback(txnID);
			(void)fDbrm->releaseTableLock(tableLockId);
			fWEClient->removeQueue(uniqueId);
			return result;				
		}
				
		rc = commitTransaction(uniqueId, txnID);
		//cout << "commiting transaction " << txnID.id << " and valid is " << txnID.valid << endl;
		if (rc != 0)
			fSessionManager.rolledback(txnID);
		else
			fSessionManager.committed(txnID);
						
		if (rc != 0) 
		{
			Message::Args args;
			Message message(9);
			ostringstream oss;
			oss << " Commit failed with error code " << rc;					
			args.add(oss.str());
			fSessionManager.rolledback(txnID);
			(void)fDbrm->releaseTableLock(tableLockId);
			message.format(args);
			result.result = (ResultCode)rc;
			result.message = message;
			fWEClient->removeQueue(uniqueId);
			return result;
		}
		
		// Log the DDL statement
		logDDL(dropTableStmt.fSessionID, txnID.id, dropTableStmt.fSql, dropTableStmt.fOwner);
	}
	catch (std::exception& ex)
	{
		result.result = DROP_ERROR;
		Message::Args args;
		Message message(9);
		args.add("Drop table failed due to ");
		args.add(ex.what());
		fSessionManager.rolledback(txnID);
		try {
			(void)fDbrm->releaseTableLock(tableLockId);
		}
		catch (std::exception&)
		{
			args.add(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
		}
		message.format( args );
		result.message = message;
		fWEClient->removeQueue(uniqueId);
		return result;
	}
	catch (...)
	{
		result.result = DROP_ERROR;
		errorMsg = "Error in getting information from system catalog or from dbrm.";
		Message::Args args;
		Message message(9);
		args.add("Drop table failed due to ");
		args.add(errorMsg);
		fSessionManager.rolledback(txnID);
		try {
			(void)fDbrm->releaseTableLock(tableLockId);
		}
		catch (std::exception&)
		{
			args.add(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
		}
		message.format( args );
		result.message = message;
		fWEClient->removeQueue(uniqueId);
		return result;
	}
	
	try {
			(void)fDbrm->releaseTableLock(tableLockId);
	}
	catch (std::exception&)
	{
		result.result = DROP_ERROR;
		Message::Args args;
		Message message(9);
		args.add("Drop table failed due to ");
		args.add(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
		fSessionManager.rolledback(txnID);
		message.format( args );
		result.message = message;
		fWEClient->removeQueue(uniqueId);
		return result;
	}		
	
	//Save the oids to a file
	try {
		createWriteDropLogFile( roPair.objnum, uniqueId, oidList );
	}
	catch (std::exception& ex)
	{
		result.result = WARNING;
		Message::Args args;
		Message message(9);
		args.add("Drop table failed due to ");
		args.add(ex.what());
		message.format(args);
		result.message = message;
		fSessionManager.rolledback(txnID);
		fWEClient->removeQueue(uniqueId);
		return result;
	}
    // Bug 4208 Drop the PrimProcFDCache before droping the column files
    // FOr Windows, this ensures (most likely) that the column files have
    // no open handles to hinder the deletion of the files.
	rc = cacheutils::dropPrimProcFdCache();

    //Drop files
	bytestream.restart();
	bytestream << (ByteStream::byte)WE_SVR_WRITE_DROPFILES;
	bytestream << uniqueId;
	bytestream << (uint32_t) oidList.size();
	for (unsigned i=0; i < oidList.size(); i++)
	{
		bytestream << (uint32_t) oidList[i];
	}
#ifdef IDB_DDL_DEBUG
cout << "Drop table removing column files" << endl;
#endif		
	uint msgRecived = 0;
	try {
		fWEClient->write_to_all(bytestream);

		bsIn.reset(new ByteStream());
		ByteStream::byte tmp8;
		while (1)
		{
			if (msgRecived == fWEClient->getPmCount())
				break;
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = NETWORK_ERROR;
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
	}
	catch (std::exception& ex)
	{
		result.result = WARNING;
		Message::Args args;
		Message message(9);
		args.add("Drop table failed due to ");
		args.add(ex.what());
		message.format(args);
		result.message = message;
		fSessionManager.rolledback(txnID);
		fWEClient->removeQueue(uniqueId);
		return result;
	}
	catch (...)
	{
		result.result = WARNING;
		errorMsg = "Error in getting information from system catalog or from dbrm.";
		Message::Args args;
		Message message(9);
		args.add("Drop table failed due to ");
		args.add(errorMsg);
		message.format(args);
		result.message = message;
		fSessionManager.rolledback(txnID);
		fWEClient->removeQueue(uniqueId);
		return result;
	}
	//Drop PrimProc FD cache
	rc = cacheutils::dropPrimProcFdCache();
	//Flush primProc cache
	rc = cacheutils::flushOIDsFromCache( oidList );
	//Delete extents from extent map
	rc = fDbrm->deleteOIDs(oidList);
	
	if (rc != 0)
	{
		Message::Args args;
		Message message(1);
		args.add("Table dropped with warning ");
		args.add( "Remove from extent map failed." );
		args.add("");
		args.add("");
		message.format( args );

		result.result = WARNING;
		result.message = message;
		fSessionManager.rolledback(txnID);
		fWEClient->removeQueue(uniqueId);
		return result;
	}
	
	//Remove the log file
	fWEClient->removeQueue(uniqueId);
	deleteLogFile(DROPTABLE_LOG, roPair.objnum, uniqueId);
	//release the transaction
	//fSessionManager.committed(txnID);
	returnOIDs( tableColRidList, dictOIDList );
	return result;

}

TruncTableProcessor::DDLResult TruncTableProcessor::processPackage(ddlpackage::TruncTableStatement& truncTableStmt)
{
	SUMMARY_INFO("TruncTableProcessor::processPackage");
	// 1. lock the table
	// 2. Get the OIDs for the columns
	// 3. Get the OIDs for the dictionaries
	// 4. Save the OIDs
	// 5. Disable all partitions
	// 6. Remove the column and dictionary files
	// 7. Flush PrimProc Cache
	// 8. Update extent map
	// 9. Use the OIDs to create new column and dictionary files with abbreviate extent
	// 10 Update next value if the table has autoincrement column
	
	DDLResult result;
	result.result = NO_ERROR;   
	std::string err;
	VERBOSE_INFO(truncTableStmt);

	// @Bug 4150. Check dbrm status before doing anything to the table.
	int rc = 0;
	rc = fDbrm->isReadWrite();
	BRM::TxnID txnID;
	txnID.id= fTxnid.id;
	txnID.valid= fTxnid.valid;
	if (rc != 0 )
	{
		Message::Args args;
		Message message(9);
		args.add("Unable to execute the statement due to DBRM is read only");
		message.format(args);
		result.result = DROP_ERROR;	
		result.message = message;
		fSessionManager.rolledback(txnID);
		return result;
	}

	//@Bug 5765 log the schema.
	string stmt = truncTableStmt.fSql + "|" + truncTableStmt.fTableName->fSchema +"|";
	SQLLogger logger(stmt, fDDLLoggingId, truncTableStmt.fSessionID, txnID.id);
	
	std::vector <CalpontSystemCatalog::OID> columnOidList;
	std::vector <CalpontSystemCatalog::OID> allOidList;
	CalpontSystemCatalog::RIDList tableColRidList;
	CalpontSystemCatalog::DictOIDList dictOIDList;
	execplan::CalpontSystemCatalog::ROPair roPair;
	std::string  processName("DDLProc");
	u_int32_t  processID = ::getpid();;
	int32_t   txnid = txnID.id;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(truncTableStmt.fSessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	systemCatalogPtr->sessionID(truncTableStmt.fSessionID);
	CalpontSystemCatalog::TableInfo tableInfo;
	uint64_t uniqueId = 0;
	//Bug 5070. Added exception handling
	try {
		uniqueId = fDbrm->getUnique64();
	}
	catch (std::exception& ex)
	{
		Message::Args args;
		Message message(9);
		args.add(ex.what());
		message.format(args);
		result.result = DROP_ERROR;	
		result.message = message;
		fSessionManager.rolledback(txnID);
		return result;
	}
	catch ( ... )
	{
		Message::Args args;
		Message message(9);
		args.add("Unknown error occured while getting unique number.");
		message.format(args);
		result.result = DROP_ERROR;	
		result.message = message;
		fSessionManager.rolledback(txnID);
		return result;
	}
	fWEClient->addQueue(uniqueId);
	int pmNum = 1;
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	string errorMsg;
	uint32_t autoIncColOid = 0;
	uint64_t tableLockId = 0;
	OamCache * oamcache = OamCache::makeOamCache();
	std::vector<int> moduleIds = oamcache->getModuleIds();
	try 
	{	
		//check table lock
		
		CalpontSystemCatalog::TableName tableName;
		tableName.schema = truncTableStmt.fTableName->fSchema;
		tableName.table = truncTableStmt.fTableName->fName;
		roPair = systemCatalogPtr->tableRID( tableName );
		int32_t sessionId = truncTableStmt.fSessionID;
		std::string  processName("DDLProc");
		int i = 0;
			
		std::vector<uint> pms;
		for (unsigned i=0; i < moduleIds.size(); i++)
		{
			pms.push_back((uint)moduleIds[i]);
		}
		try {
			tableLockId = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId, &txnid, BRM::LOADING );
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
					txnid = txnID.id;
					sessionId = truncTableStmt.fSessionID;
					processName = "DDLProc";
					tableLockId = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId, &txnid, BRM::LOADING );
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
				Message::Args args;
				args.add(processName);
				args.add((uint64_t)processID);
				args.add(sessionId);
				throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_TABLE_LOCKED,args));
			}			
		}
		CalpontSystemCatalog::TableName userTableName;
		userTableName.schema = truncTableStmt.fTableName->fSchema;
		userTableName.table = truncTableStmt.fTableName->fName;

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
	catch (std::exception& ex)
	{
		cerr << "TruncateTableProcessor::processPackage: " << ex.what() << endl;
	
		Message::Args args;
		Message message(9);
		args.add("Truncate table failed: ");
		args.add( ex.what() );
		args.add("");    	 	       	
		fSessionManager.rolledback(txnID);
		try {
			(void)fDbrm->releaseTableLock(tableLockId);
		}
		catch (std::exception&)
		{
			args.add(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
		}
		fWEClient->removeQueue(uniqueId);			
		message.format( args );

		result.result = TRUNC_ERROR;
		result.message = message;
		return result;
	}
	catch (...)
	{
		cerr << "TruncateTableProcessor::processPackage: caught unknown exception!" << endl;
		
		Message::Args args;
		Message message(1);
		args.add("Truncate table failed: ");
		args.add( "encountered unkown exception" );
		args.add("");
		try {
			(void)fDbrm->releaseTableLock(tableLockId);
		}
		catch (std::exception&)
		{
			args.add(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
		}
		fWEClient->removeQueue(uniqueId);			
		message.format( args );

		result.result = TRUNC_ERROR;
		result.message = message;
		return result;
	}
	
	//Save the oids to a file
	try {
		createWriteTruncateTableLogFile( roPair.objnum, uniqueId, allOidList);
	}
	catch (std::exception& ex)
	{
		Message::Args args;
		Message message(9);
		args.add("Truncate table failed due to ");
		args.add(ex.what());
		fSessionManager.rolledback(txnID);
		fWEClient->removeQueue(uniqueId);			
		message.format( args );
		//@bug 4515 Release the tablelock as nothing has done to this table.
		try {
			(void)fDbrm->releaseTableLock(tableLockId);
		}
		catch (std::exception&) {}
		result.result = TRUNC_ERROR;
		result.message = message;
		return result;
	} 
	
	ByteStream bytestream;
	ByteStream::byte tmp8;
	try {
		//Disable extents first
		int rc1 = fDbrm->markAllPartitionForDeletion( allOidList);
		if (rc1 != 0)
		{
			string errMsg;
			BRM::errString(rc, errMsg);
			throw std::runtime_error(errMsg);
		}

        // Bug 4208 Drop the PrimProcFDCache before droping the column files
        // FOr Windows, this ensures (most likely) that the column files have
        // no open handles to hinder the deletion of the files.
	    rc = cacheutils::dropPrimProcFdCache();

        VERBOSE_INFO("Removing files");
		bytestream << (ByteStream::byte)WE_SVR_WRITE_DROPFILES;
		bytestream << uniqueId;
		bytestream << (uint32_t) allOidList.size();
		for (unsigned i=0; i < allOidList.size(); i++)
		{
			bytestream << (uint32_t) allOidList[i];
		}
	
		uint msgRecived = 0;
		try {
			fWEClient->write_to_all(bytestream);

			bsIn.reset(new ByteStream());
			
			while (1)
			{
				if (msgRecived == fWEClient->getPmCount())
					break;
				fWEClient->read(uniqueId, bsIn);
				if ( bsIn->length() == 0 ) //read error
				{
					rc = NETWORK_ERROR;
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
		}
		catch (std::exception& ex)
		{
			Message::Args args;
			Message message(9);
			args.add("Truncate table failed due to ");
			args.add(ex.what());
			fSessionManager.rolledback(txnID);
			fWEClient->removeQueue(uniqueId);			
			message.format( args );

			result.result = TRUNC_ERROR;
			result.message = message;
			deleteLogFile(TRUNCATE_LOG, roPair.objnum, uniqueId);
			return result;
		}
		catch (...)
		{
			result.result = DROP_ERROR;
			errorMsg = "Error in getting information from system catalog or from dbrm.";
			Message::Args args;
			Message message(9);
			args.add("Truncate table failed due to ");
			args.add(errorMsg);
			fSessionManager.rolledback(txnID);
			fWEClient->removeQueue(uniqueId);			
			message.format( args );

			result.result = TRUNC_ERROR;
			result.message = message;
			deleteLogFile(TRUNCATE_LOG, roPair.objnum, uniqueId);
			return result;
		}
		//Drop PrimProc FD cache
		rc = cacheutils::dropPrimProcFdCache();
		//Flush primProc cache
		rc = cacheutils::flushOIDsFromCache( allOidList );
		//Delete extents from extent map
		rc = fDbrm->deleteOIDs(allOidList);
	
		if (rc != 0)
		{
			Message::Args args;
			Message message(1);
			args.add("Table truncated with warning ");
			args.add( "Remove from extent map failed." );
			args.add("");
			args.add("");
			message.format( args );

			result.result = WARNING;
			result.message = message;
			fSessionManager.rolledback(txnID);
			fWEClient->removeQueue(uniqueId);
			return result;
		}
		
		//Get the number of tables in the database, the current table is included.
		int tableCount = systemCatalogPtr->getTableCount();
		Oam oam;
		//Calculate which dbroot the columns should start
		DBRootConfigList dbRootList = oamcache->getDBRootNums();
	
		uint16_t useDBRootIndex = tableCount % dbRootList.size();
		//Find out the dbroot# corresponding the useDBRootIndex from oam
		uint16_t useDBRoot = dbRootList[useDBRootIndex];
		
		bytestream.restart();
		bytestream << (ByteStream::byte) WE_SVR_WRITE_CREATETABLEFILES;
		bytestream << uniqueId;
		bytestream << (u_int32_t)txnID.id;
		uint32_t numOids = columnOidList.size() + dictOIDList.size();
		bytestream << numOids;
		CalpontSystemCatalog::ColType colType;
		
		for (unsigned col  = 0; col < columnOidList.size(); col++)
		{
			colType = systemCatalogPtr->colType(columnOidList[col]);
			if (colType.autoincrement)
				autoIncColOid = colType.columnOID;
			bytestream << (uint32_t)columnOidList[col];
			bytestream << (u_int8_t) colType.colDataType;
			bytestream << (u_int8_t) false;
			bytestream << (uint32_t) colType.colWidth;
			bytestream << (u_int16_t) useDBRoot;
			bytestream << (uint32_t) colType.compressionType;			
		}
		
		for (unsigned col  = 0; col <dictOIDList.size(); col++)
		{
			colType = systemCatalogPtr->colTypeDct(dictOIDList[col].dictOID);
			bytestream << (uint32_t) dictOIDList[col].dictOID;
			bytestream << (u_int8_t) colType.colDataType;
			bytestream << (u_int8_t) true;
			bytestream << (uint32_t) colType.colWidth;
			bytestream << (u_int16_t) useDBRoot;
			bytestream << (uint32_t) colType.compressionType;
		}
		
		boost::shared_ptr<std::map<int, int> > dbRootPMMap = oamcache->getDBRootToPMMap();
		pmNum = (*dbRootPMMap)[useDBRoot];
		try
		{
#ifdef IDB_DDL_DEBUG
cout << "Truncate table sending We_SVR_WRITE_CREATETABLEFILES to pm " << pmNum << endl;
#endif
			fWEClient->write(bytestream, pmNum);
			while (1)
			{
				bsIn.reset(new ByteStream());
				fWEClient->read(uniqueId, bsIn);
				if ( bsIn->length() == 0 ) //read error
				{
					rc = NETWORK_ERROR;
					errorMsg = "Lost connection to Write Engine Server while updating SYSTABLES";
					break;
				}			
				else {
					*bsIn >> tmp8;
					rc = tmp8;
					if (rc != 0) {
						*bsIn >> errorMsg;
					}
					break;
				}
			}
		
			if (rc != 0) {
				//drop the newly created files
				bytestream.restart();
				bytestream << (ByteStream::byte) WE_SVR_WRITE_DROPFILES;
				bytestream << uniqueId;
				bytestream << (uint32_t)(allOidList.size());
				for (unsigned i = 0; i < (allOidList.size()); i++)
				{
					bytestream << (uint32_t)(allOidList[i]);
				}
				fWEClient->write(bytestream, pmNum);
				while (1)
				{
					bsIn.reset(new ByteStream());
					fWEClient->read(uniqueId, bsIn);
					if ( bsIn->length() == 0 ) //read error
					{	
						break;
					}			
					else {
						*bsIn >> tmp8;
						//rc = tmp8;
						break;
					}
				}
				Message::Args args;
				Message message(1);
				args.add( "Truncate table failed." );
				args.add( errorMsg);
				args.add("");
				message.format( args );

				result.result = TRUNC_ERROR;
				result.message = message;
		//rc = fSessionManager.setTableLock( roPair.objnum, truncTableStmt.fSessionID, processID, processName, false );
				fSessionManager.rolledback(txnID);
				return result;
			}		
		}
		catch (runtime_error&)
		{
			rc = NETWORK_ERROR;
			errorMsg = "Lost connection to Write Engine Server";
		}
	}
#ifdef _MSC_VER
	catch (std::exception&) 
	{
		//FIXME: Windows can't delete a file that's still open by another process
	}
#else
	catch (std::exception& ex) 
	{
		Message::Args args;
		Message message(1);
		args.add( "Truncate table failed." );
		args.add( ex.what() );
		args.add("");
		message.format( args );

		result.result = TRUNC_ERROR;
		result.message = message;
		//rc = fSessionManager.setTableLock( roPair.objnum, truncTableStmt.fSessionID, processID, processName, false );
		fSessionManager.rolledback(txnID);
		return result;
	}
#endif
	catch ( ... )
	{
		Message::Args args;
		Message message(1);
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
	if (rc != 0)
	{
		rollBackTransaction( uniqueId, txnID, truncTableStmt.fSessionID); //What to do with the error code
		fSessionManager.rolledback(txnID);
	}
	
	//Check whether the table has autoincrement column
	if (tableInfo.tablewithautoincr == 1)
	{
		//reset nextvalue to 1
		WE_DDLCommandClient commandClient;
		rc = commandClient.UpdateSyscolumnNextval(autoIncColOid,1);	
	}
	// Log the DDL statement
	logDDL(truncTableStmt.fSessionID, txnID.id, truncTableStmt.fSql, truncTableStmt.fOwner);
	try {
		(void)fDbrm->releaseTableLock(tableLockId);
	}
	catch (std::exception&)
	{
		Message::Args args;
		Message message(1);
		args.add("Table truncated with warning ");
		args.add( "Release table failed." );
		args.add(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
		args.add("");
		message.format( args );

		result.result = WARNING;
		result.message = message;
		fSessionManager.rolledback(txnID);
		fWEClient->removeQueue(uniqueId);
	}
	//release the transaction
	fSessionManager.committed(txnID);
	fWEClient->removeQueue(uniqueId);
	//Remove the log file
	try {
		deleteLogFile(TRUNCATE_LOG, roPair.objnum, uniqueId);
	}
	catch ( ... )
	{
	}

	return result;
}

} //namespace ddlpackageprocessor

// vim:ts=4 sw=4:

