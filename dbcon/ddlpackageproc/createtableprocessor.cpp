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

//   $Id: createtableprocessor.cpp 9068 2012-11-13 21:37:42Z chao $

#define DDLPKGCREATETABLEPROC_DLLEXPORT
#include "createtableprocessor.h"
#undef DDLPKGCREATETABLEPROC_DLLEXPORT
#include "sessionmanager.h"
#include "messagelog.h"
#include <boost/algorithm/string/case_conv.hpp>
#include "sqllogger.h"
using namespace boost::algorithm;
using namespace std;
using namespace execplan;
using namespace ddlpackage;
using namespace logging;
using namespace BRM;

namespace ddlpackageprocessor
{

CreateTableProcessor::DDLResult CreateTableProcessor::processPackage(
										ddlpackage::CreateTableStatement& createTableStmt)
{
	SUMMARY_INFO("CreateTableProcessor::processPackage");

	DDLResult result;
	TxnID txnID;
	txnID.id= fTxnid.id;
	txnID.valid= fTxnid.valid;
	result.result = NO_ERROR;
	int rc = 0;
	rc = fDbrm.isReadWrite();
	if (rc != 0 )
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
	to_lower(tableDef.fQualifiedName->fSchema);
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
	CalpontSystemCatalog* systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(createTableStmt.fSessionID);
	execplan::CalpontSystemCatalog::TableName tableName;
	tableName.schema = tableDef.fQualifiedName->fSchema;
	tableName.table = tableDef.fQualifiedName->fName;
	execplan::CalpontSystemCatalog::ROPair roPair;
	roPair.objnum = 0;
	/** @Bug 217 */
	/** @Bug 225 */
	try
	{
		roPair = systemCatalogPtr->tableRID(tableName);
	}
	catch (IDBExcept &ie) 
    {
        // TODO: What is and is not an error here?
        if ( ie.errorCode() == ERR_TABLE_NOT_IN_CATALOG)
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
		args.add("(probably your schema is out-of-sync)");
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
	try
	{
		//Allocate tableoid table identification
		execplan::ObjectIDManager fObjectIDManager;
		VERBOSE_INFO("Allocating object ID for table");
		fTableOID = fObjectIDManager.allocOID();
		// Allocate a object ID for each column we are about to create
		VERBOSE_INFO("Allocating object IDs for columns");
		fStartingColOID = fObjectIDManager.allocOIDs(tableDef.fColumns.size());

		// Write the tables metadata to the system catalog
		VERBOSE_INFO("Writing meta data to SYSTABLE");
		writeSysTableMetaData(createTableStmt.fSessionID, txnID.id, result,tableDef, createTableStmt.fTableWithAutoi);

		VERBOSE_INFO("Writing meta data to SYSCOL");
		writeSysColumnMetaData(createTableStmt.fSessionID, txnID.id, result,tableDef.fColumns,
								*(tableDef.fQualifiedName), 0);

		if (tableDef.fConstraints.size() != 0)
		{
			VERBOSE_INFO("Writing table constraint meta data to SYSCONSTRAINT");
			//writeTableSysConstraintMetaData(createTableStmt.fSessionID, txnID.id, result, tableDef.fConstraints, *(tableDef.fQualifiedName));
			if (result.result != NO_ERROR)
			{
				throw std::runtime_error("writeTableSysConstraintMetaData failed: " + result.message.msg());
			}

			VERBOSE_INFO("Writing table constraint meta data to SYSCONSTRAINTCOL");
			//writeTableSysConstraintColMetaData(createTableStmt.fSessionID, txnID.id, result, tableDef.fConstraints, *(tableDef.fQualifiedName));

		}
		if (tableDef.fColumns.size() != 0)
		{
			VERBOSE_INFO("Writing column constraint meta data to SYSCONSTRAINT");
			//writeColumnSysConstraintMetaData(createTableStmt.fSessionID, txnID.id, result,tableDef.fColumns, *(tableDef.fQualifiedName));

			VERBOSE_INFO("Writing column constraint meta data to SYSCONSTRAINTCOL");
			//writeColumnSysConstraintColMetaData(createTableStmt.fSessionID, txnID.id, result,tableDef.fColumns, *(tableDef.fQualifiedName));
		}

		//@Bug 4176 save oids to a log file for DDL cleanup after fail over.
		std::vector <CalpontSystemCatalog::OID> oidList;
		for (uint j=0; j < tableDef.fColumns.size(); j++)
		{
			oidList.push_back(fStartingColOID+j);
		}
		
		for (uint j=0; j < fDictionaryOIDList.size(); j++)
		{
			oidList.push_back(fDictionaryOIDList[j].dictOID);
		}
		
		createOpenLogFile( fTableOID, tableName );
		writeLogFile ( tableName, oidList );
		
		//Get the number of tables in the database, the current table is included.
		int tableCount = systemCatalogPtr->getTableCount();

		//Calculate which dbroot the columns should start
		int dbRootCnt = 1;
		int useDBRoot = 1;
	  	string DBRootCount = config::Config::makeConfig()->getConfig("SystemConfig", "DBRootCount");
	  	if (DBRootCount.length() != 0)
		 dbRootCnt = static_cast<int>(config::Config::fromText(DBRootCount));

		useDBRoot =  (tableCount % dbRootCnt) + 1;

		VERBOSE_INFO("Creating column files");
		createColumnFiles(txnID.id, result,tableDef.fColumns, useDBRoot);

		VERBOSE_INFO("Creating index files");
		createIndexFiles(txnID.id, result);

		VERBOSE_INFO("Creating dictionary files");
		createDictionaryFiles(txnID.id, result, useDBRoot);

		// Log the DDL statement.
		logging::logDDL(createTableStmt.fSessionID, txnID.id, createTableStmt.fSql, createTableStmt.fOwner);

		DETAIL_INFO("Commiting transaction");
		fWriteEngine.commit(txnID.id);
		deleteLogFile();
		fSessionManager.committed(txnID);
	}
	catch (exception& ex)
	{
		rollBackCreateTable(ex.what(), txnID, createTableStmt.fSessionID, tableDef, result);
		deleteLogFile();
		}
	catch (...)
	{
		rollBackCreateTable("CreatetableProcessor::processPackage: caught unknown exception!",
							txnID, createTableStmt.fSessionID, tableDef, result);
		deleteLogFile();
	}

	return result;
}

void CreateTableProcessor::rollBackCreateTable(const string& error, TxnID txnID, int sessionId,
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
	DETAIL_INFO("Rolling back transaction");
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
	catch (exception& ex)
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
