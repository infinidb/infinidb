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

//   $Id: createtableprocessor.cpp 9066 2012-11-13 21:37:23Z chao $

#define DDLPKGCREATETABLEPROC_DLLEXPORT
#include "createtableprocessor.h"
#undef DDLPKGCREATETABLEPROC_DLLEXPORT
#include "sessionmanager.h"
#include "messagelog.h"
#include <boost/algorithm/string/case_conv.hpp>
#include "sqllogger.h"
#include "we_messages.h"
#include "bytestream.h"
#include "oamcache.h"
using namespace messageqcpp;
using namespace boost::algorithm;
using namespace std;
using namespace execplan;
using namespace ddlpackage;
using namespace logging;
using namespace BRM;
using namespace WriteEngine;
using namespace oam;
using namespace boost;

namespace ddlpackageprocessor
{

CreateTableProcessor::DDLResult CreateTableProcessor::processPackage(
										ddlpackage::CreateTableStatement& createTableStmt)
{
	SUMMARY_INFO("CreateTableProcessor::processPackage");

	DDLResult result;
	BRM::TxnID txnID;
	txnID.id= fTxnid.id;
	txnID.valid= fTxnid.valid;
	result.result = NO_ERROR;
	int rc1 = 0;
	rc1 = fDbrm.isReadWrite();
	if (rc1 != 0 )
	{
		logging::Message::Args args;
		logging::Message message(9);
		args.add("Unable to execute the statement due to DBRM is read only");
		message.format(args);
		result.result = CREATE_ERROR;	
		result.message = message;
		fSessionManager.rolledback(txnID);
		return result;
	}
	DETAIL_INFO(createTableStmt);
	ddlpackage::TableDef& tableDef = *createTableStmt.fTableDef;
	//If schema = CALPONTSYS, do not create table
	boost::algorithm::to_lower(tableDef.fQualifiedName->fSchema);
	if (tableDef.fQualifiedName->fSchema == CALPONT_SCHEMA)
	{
		//release the transaction
		fSessionManager.rolledback(txnID);
		return result;
	}
	// Commit current transaction.
	// all DDL statements cause an implicut commit
	VERBOSE_INFO("Getting current txnID");

	//Check whether the table is existed already
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(createTableStmt.fSessionID);
	execplan::CalpontSystemCatalog::TableName tableName;
	tableName.schema = tableDef.fQualifiedName->fSchema;
	tableName.table = tableDef.fQualifiedName->fName;
	execplan::CalpontSystemCatalog::ROPair roPair;
	roPair.objnum = 0;
	ByteStream::byte rc = 0;
	/** @Bug 217 */
	/** @Bug 225 */
	try
	{
		roPair = systemCatalogPtr->tableRID(tableName);
	}
    catch (IDBExcept &ie) 
    {
        // TODO: What is and is not an error here?
        if (ie.errorCode() == ERR_DATA_OFFLINE)
        {
            //release transaction
            fSessionManager.rolledback(txnID);
            // Return the error for display to user
            logging::Message::Args args;
            logging::Message message(9);
            args.add(ie.what());
            message.format(args);
            result.result = CREATE_ERROR;
            result.message = message;
            return result;
        }
        else if ( ie.errorCode() == ERR_TABLE_NOT_IN_CATALOG)
        {
            roPair.objnum = 0;
        }
		else //error out
		{
			//release transaction
            fSessionManager.rolledback(txnID);
            // Return the error for display to user
            logging::Message::Args args;
            logging::Message message(9);
            args.add(ie.what());
            message.format(args);
            result.result = CREATE_ERROR;
            result.message = message;
            return result;
		}
    }
	catch (std::exception& ex)  //error out
	{
		//release transaction
        fSessionManager.rolledback(txnID);
        // Return the error for display to user
        logging::Message::Args args;
        logging::Message message(9);
        args.add(ex.what());
        message.format(args);
        result.result = CREATE_ERROR;
        result.message = message;
        return result;
	}
	catch (...) //error out
	{
		//release transaction
        fSessionManager.rolledback(txnID);
        // Return the error for display to user
        logging::Message::Args args;
        logging::Message message(9);
        args.add("Unknown exception caught when checking if the table name is already in use.");
        message.format(args);
        result.result = CREATE_ERROR;
        result.message = message;
        return result;
	}

	//This is a current db bug, it should not turn OID is it cannot find
	if (roPair.objnum >= 3000)
	{
#ifdef _MSC_VER
		//FIXME: Why do we need to do this???
		systemCatalogPtr->flushCache();
		try { roPair = systemCatalogPtr->tableRID(tableName); }
		catch (...) { roPair.objnum = 0; }
		if (roPair.objnum < 3000)
			goto keepGoing;
#endif
		logging::Message::Args args;
		logging::Message message(9);
		args.add("Internal create table error for");
		args.add(tableName.toString());
		args.add(": table already exists");
		args.add("(your schema is probably out-of-sync)");
		message.format(args);

		result.result = CREATE_ERROR;
		result.message = message;
		//release the transaction
		fSessionManager.rolledback(txnID);
		return result;
	}
#ifdef _MSC_VER
keepGoing:
#endif
	// Start a new transaction
	VERBOSE_INFO("Starting a new transaction");

	SQLLogger logger(createTableStmt.fSql, fDDLLoggingId, createTableStmt.fSessionID, txnID.id);


	std::string err;
	execplan::ObjectIDManager fObjectIDManager; 
	OamCache * oamcache = OamCache::makeOamCache();
	string errorMsg;
	//get a unique number
	uint64_t uniqueId = fDbrm.getUnique64();
	fWEClient->addQueue(uniqueId);
	try
	{
		//Allocate tableoid table identification
		VERBOSE_INFO("Allocating object ID for table");	
		// Allocate a object ID for each column we are about to create
		VERBOSE_INFO("Allocating object IDs for columns");
		u_int32_t numColumns = tableDef.fColumns.size();
		u_int32_t numDictCols = 0;
		for (unsigned i=0; i < numColumns; i++)
		{
			int dataType;
			dataType = convertDataType(tableDef.fColumns[i]->fType->fType);
			if ( (dataType == CalpontSystemCatalog::CHAR && tableDef.fColumns[i]->fType->fLength > 8) ||
				 (dataType == CalpontSystemCatalog::VARCHAR && tableDef.fColumns[i]->fType->fLength > 7) ||
				 (dataType == CalpontSystemCatalog::VARBINARY && tableDef.fColumns[i]->fType->fLength > 7) )			 
				 numDictCols++;
		}
		fStartingColOID = fObjectIDManager.allocOIDs(numColumns+numDictCols+1); //include column, oids,dictionary oids and tableoid
#ifdef IDB_DDL_DEBUG
cout << "Create table allocOIDs got the stating oid " << fStartingColOID << endl;
#endif		
		if (fStartingColOID < 0)
		{
			result.result = CREATE_ERROR;
			errorMsg = "Error in getting objectid from oidmanager.";
			logging::Message::Args args;
			logging::Message message(9);
			args.add("Create table failed due to ");
			args.add(errorMsg);
			result.message = message;
			fSessionManager.rolledback(txnID);
			return result;
		}

		// Write the tables metadata to the system catalog
		VERBOSE_INFO("Writing meta data to SYSTABLES");
		ByteStream bytestream;
		bytestream << (ByteStream::byte)WE_SVR_WRITE_SYSTABLE;
		bytestream << uniqueId;
		bytestream << (u_int32_t) createTableStmt.fSessionID;
		bytestream << (u_int32_t)txnID.id;
		bytestream << (u_int32_t)fStartingColOID;
		bytestream << (u_int32_t)createTableStmt.fTableWithAutoi;
		
		bytestream << numColumns;
		for (unsigned i = 0; i <numColumns; ++i) {
			bytestream << (u_int32_t)(fStartingColOID+i+1);
		}	
		bytestream << numDictCols;
		for (unsigned i = 0; i <numDictCols; ++i) {
			bytestream << (u_int32_t)(fStartingColOID+numColumns+i+1);
		}	
		
		u_int8_t alterFlag = 0;
		int colPos = 0;
		bytestream << (ByteStream::byte)alterFlag;
		bytestream << (u_int32_t)colPos;
		
		u_int16_t  dbRoot;
		BRM::OID_t sysOid = 1001;
		//Find out where systable is
		rc = fDbrm.getSysCatDBRoot(sysOid, dbRoot); 
		if (rc != 0)
		{
			result.result =(ResultCode) rc;
			logging::Message::Args args;
			logging::Message message(9);
			args.add("Error while calling getSysCatDBRoot ");
			args.add(errorMsg);
			result.message = message;
			//release transaction
			fSessionManager.rolledback(txnID);
			return result;
		}
		int pmNum = 1;
		bytestream << (u_int32_t)dbRoot; 
		tableDef.serialize(bytestream);
		boost::shared_ptr<messageqcpp::ByteStream> bsIn;
		boost::shared_ptr<std::map<int, int> > dbRootPMMap = oamcache->getDBRootToPMMap();
		pmNum = (*dbRootPMMap)[dbRoot];
		try
		{			
			fWEClient->write(bytestream, (uint)pmNum);
#ifdef IDB_DDL_DEBUG
cout << "create table sending We_SVR_WRITE_SYSTABLE to pm " << pmNum << endl;
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
                        errorMsg.clear();
						*bsIn >> errorMsg;
#ifdef IDB_DDL_DEBUG
cout << "Create table We_SVR_WRITE_CREATETABLEFILES: " << errorMsg << endl;
#endif
					}
					break;
				}
			}
		}
		catch (runtime_error& ex) //write error
		{
#ifdef IDB_DDL_DEBUG
cout << "create table got exception" << ex.what() << endl;
#endif			
			rc = NETWORK_ERROR;
			errorMsg = ex.what();
		}
		catch (...)
		{
			rc = NETWORK_ERROR;
#ifdef IDB_DDL_DEBUG
cout << "create table got unknown exception" << endl;
#endif
		}
		
		if (rc != 0)
		{
			result.result =(ResultCode) rc;
			logging::Message::Args args;
			logging::Message message(9);
			args.add("Create table failed due to ");
			args.add(errorMsg);
			message.format( args );
			result.message = message;
			if (rc != NETWORK_ERROR)
			{
				rollBackTransaction( uniqueId, txnID, createTableStmt.fSessionID );	//What to do with the error code			
			}
			//release transaction
			fSessionManager.rolledback(txnID);
			return result;
		}
		
		
		//Get the number of tables in the database, the current table is included.
		int tableCount = systemCatalogPtr->getTableCount();

		//Calculate which dbroot the columns should start
		DBRootConfigList dbRootList = oamcache->getDBRootNums();
		
		uint16_t useDBRootIndex = tableCount % dbRootList.size();
		//Find out the dbroot# corresponding the useDBRootIndex from oam
		uint16_t useDBRoot = dbRootList[useDBRootIndex];
		
		VERBOSE_INFO("Creating column files");
		ColumnDef* colDefPtr;
		ddlpackage::ColumnDefList tableDefCols = tableDef.fColumns;
		ColumnDefList::const_iterator iter = tableDefCols.begin();
		bytestream.restart();
		bytestream << (ByteStream::byte)WE_SVR_WRITE_CREATETABLEFILES;
		bytestream << uniqueId;
		bytestream << (numColumns + numDictCols);
		unsigned colNum = 0;
		unsigned dictNum = 0;
		while (iter != tableDefCols.end())
		{
			colDefPtr = *iter;

			int dataType1 = convertDataType(colDefPtr->fType->fType);
			if (dataType1 == CalpontSystemCatalog::DECIMAL)
			{
				if (colDefPtr->fType->fPrecision == -1 || colDefPtr->fType->fPrecision == 0)
				{
					colDefPtr->fType->fLength = 8;
				}
				else if ((colDefPtr->fType->fPrecision > 0) && (colDefPtr->fType->fPrecision < 3))
				{
					colDefPtr->fType->fLength = 1;
				}

				else if (colDefPtr->fType->fPrecision < 5 && (colDefPtr->fType->fPrecision > 2))
				{
					colDefPtr->fType->fLength = 2;
				}
				else if (colDefPtr->fType->fPrecision > 4 && colDefPtr->fType->fPrecision < 10)
				{
					colDefPtr->fType->fLength = 4;
				}
				else if (colDefPtr->fType->fPrecision > 9 && colDefPtr->fType->fPrecision < 19)
				{
					colDefPtr->fType->fLength = 8;
				}	
			}
			WriteEngine::ColDataType dataType = static_cast<WriteEngine::ColDataType>(dataType1);
			bytestream << (fStartingColOID + (colNum++) + 1);
			bytestream << (u_int8_t) dataType;
			bytestream << (u_int8_t) false;

			bytestream << (uint32_t) colDefPtr->fType->fLength;
			bytestream << (u_int16_t) useDBRoot;
			bytestream << (uint32_t) colDefPtr->fType->fCompressiontype;
			if ( (dataType == WriteEngine::CHAR && colDefPtr->fType->fLength > 8) ||
				 (dataType == WriteEngine::VARCHAR && colDefPtr->fType->fLength > 7) ||
				 (dataType == WriteEngine::VARBINARY && colDefPtr->fType->fLength > 7) )
			{
				bytestream << (uint32_t) (fStartingColOID+numColumns+(dictNum++)+1);
				bytestream << (u_int8_t) dataType;
				bytestream << (u_int8_t) true;
				bytestream << (uint32_t) colDefPtr->fType->fLength;
				bytestream << (u_int16_t) useDBRoot;
				bytestream << (uint32_t) colDefPtr->fType->fCompressiontype;
			}
			++iter;
		}
		//@Bug 4176. save oids to a log file for cleanup after fail over.
		std::vector <CalpontSystemCatalog::OID> oidList;
		for (unsigned i = 0; i <numColumns; ++i) 
		{
			oidList.push_back(fStartingColOID+i+1);
		}	
		bytestream << numDictCols;
		for (unsigned i = 0; i <numDictCols; ++i) 
		{
			oidList.push_back(fStartingColOID+numColumns+i+1);
		}	
		
		try {
			createWriteDropLogFile( fStartingColOID, uniqueId, oidList );
		}
		catch (std::exception& ex)
		{
			result.result =(ResultCode) rc;
			logging::Message::Args args;
			logging::Message message(9);
			args.add("Create table failed due to ");
			args.add(ex.what());
			message.format( args );
			result.message = message;
			if (rc != NETWORK_ERROR)
			{
				rollBackTransaction( uniqueId, txnID, createTableStmt.fSessionID );	//What to do with the error code			
			}
			//release transaction
			fSessionManager.rolledback(txnID);
			return result;
		}
		
		pmNum = (*dbRootPMMap)[useDBRoot];
		try
		{
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
					*bsIn >> rc;
					if (rc != 0) {
                        errorMsg.clear();
						*bsIn >> errorMsg;
#ifdef IDB_DDL_DEBUG
cout << "Create table We_SVR_WRITE_CREATETABLEFILES: " << errorMsg << endl;
#endif
					}
					break;
				}
			}
			
			if (rc != 0) {
				//drop the newly created files
				bytestream.restart();
				bytestream << (ByteStream::byte) WE_SVR_WRITE_DROPFILES;
				bytestream << uniqueId;
				bytestream << (uint32_t)(numColumns+numDictCols);
				for (unsigned i = 0; i < (numColumns+numDictCols); i++)
				{
					bytestream << (uint32_t)(fStartingColOID + i + 1);
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
						break;
					}
				}
			}		
		}
		catch (runtime_error&)
		{
			errorMsg = "Lost connection to Write Engine Server";
		}
		
		if (rc != 0)
		{
			rollBackTransaction( uniqueId, txnID, createTableStmt.fSessionID); //What to do with the error code
			fSessionManager.rolledback(txnID);
		}
		else
		{
			commitTransaction(uniqueId, txnID);
			fSessionManager.committed(txnID);
			fWEClient->removeQueue(uniqueId);	
			deleteLogFile(DROPTABLE_LOG, fStartingColOID, uniqueId);
		}
		
		// Log the DDL statement.
		logging::logDDL(createTableStmt.fSessionID, txnID.id, createTableStmt.fSql, createTableStmt.fOwner);
	}
	catch (std::exception& ex)
	{
		result.result = CREATE_ERROR;
		logging::Message::Args args;
		logging::Message message(9);
		args.add("Create table failed due to ");
		args.add(ex.what());
		message.format( args );
		result.message = message;
		fSessionManager.rolledback(txnID);
		fWEClient->removeQueue(uniqueId);
		return result;
	}
	//fWEClient->removeQueue(uniqueId);	
	if (rc !=0)
	{
		result.result = CREATE_ERROR;
		logging::Message::Args args;
		logging::Message message(9);
		args.add("Create table failed due to ");
		args.add(errorMsg);
		message.format( args );
		result.message = message;
	}
	return result;
}

void CreateTableProcessor::rollBackCreateTable(const string& error, BRM::TxnID txnID, int sessionId,
												ddlpackage::TableDef& tableDef, DDLResult& result)
{
	cerr << "CreatetableProcessor::processPackage: " << error << endl;

	logging::Message::Args args;
	logging::Message message(1);
	args.add("Create table Failed: ");
	args.add(error);
	args.add("");
	args.add("");
	message.format(args);

	result.result = CREATE_ERROR;
	result.message = message;
	
	fWriteEngine.rollbackTran(txnID.id, sessionId);

	size_t size = tableDef.fColumns.size();
	for (size_t i = 0; i < size; ++i)
	{
		fWriteEngine.dropColumn(txnID.id, fStartingColOID + i);
	}

	try
	{
		execplan::ObjectIDManager fObjectIDManager;
		fObjectIDManager.returnOID(fTableOID);
		fObjectIDManager.returnOIDs(fStartingColOID,
									fStartingColOID + tableDef.fColumns.size() - 1);
	}
	catch (std::exception& ex)
	{
		logging::Message::Args args;
		logging::Message message(6);
		args.add(ex.what());
		message.format(args);
		result.message = message;
		result.result = CREATE_ERROR;
	}
	catch (...)
	{
        logging::Message::Args args;
        logging::Message message(6);
        args.add("Unknown exception");
        message.format(args);
        result.message = message;
        result.result = CREATE_ERROR;
		//cout << "returnOIDs error" << endl;
	}
	IndexOIDList::const_iterator idxoid_iter = fIndexOIDList.begin();
	while (idxoid_iter != fIndexOIDList.end())
	{
		IndexOID idxOID = *idxoid_iter;
		(void)0;

		++idxoid_iter;
	}
	DictionaryOIDList::const_iterator dictoid_iter = fDictionaryOIDList.begin();
	while (dictoid_iter != fDictionaryOIDList.end())
	{
		DictOID dictOID = *dictoid_iter;
		fWriteEngine.dropDctnry(txnID.id, dictOID.dictOID, dictOID.treeOID, dictOID.listOID);
		//fObjectIDManager.returnOID(dictOID.dictOID);

		++dictoid_iter;
	}
	fSessionManager.rolledback(txnID);
}

/*
void CreateTableProcessor::createIndexFiles(DDLResult& result, ddlpackage::TableDef& tableDef)
{
	SUMMARY_INFO("CreateTableProcessor::createIndexFiles");

	if (result.result != NO_ERROR)
		return;

	int error = NO_ERROR;

	IndexOIDList::const_iterator iter = fIndexOIDList.begin();
	while (iter != fIndexOIDList.end())
	{
		IndexOID idxOID = *iter;

		error = fWriteEngine.createIndex(idxOID.treeOID, idxOID.listOID);
		if (error != WriteEngine::NO_ERROR)
		{
			logging::Message::Args args;
			logging::Message message(9);
			args.add("Error creating index file: ");
			args.add(idxOID.treeOID);
			args.add("error number: ");
			args.add(error);
			message.format(args);

			result.result = CREATE_ERROR;
			result.message = message;

			break;
		}

		++iter;
	}

}
*/
}												 // namespace ddlpackageprocessor
// vim:ts=4 sw=4:
