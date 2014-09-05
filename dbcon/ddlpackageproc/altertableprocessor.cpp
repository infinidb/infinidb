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

//   $Id: altertableprocessor.cpp 9717 2013-07-24 20:42:34Z dhall $

/** @file */

#define ALTERTABLEPROC_DLLEXPORT
#include "altertableprocessor.h"
#undef ALTERTABLEPROC_DLLEXPORT
#include "sessionmanager.h"
#include "brm.h"
#include <boost/algorithm/string/case_conv.hpp>
using namespace boost::algorithm;

#include "messagelog.h"
#include "sqllogger.h"
#include "cacheutils.h"
using namespace cacheutils;
#include <typeinfo>
#include <string>
using namespace std;
using namespace execplan;
using namespace ddlpackage;
using namespace logging;
using namespace BRM;
#include "we_messages.h"
#include "we_ddlcommandclient.h"
using namespace WriteEngine;
#include "oamcache.h"
using namespace oam;
#include "bytestream.h"
using namespace messageqcpp;

//TODO: this should be in a common header somewhere
struct extentInfo
{
	uint16_t dbRoot;
	uint32_t partition;
	uint16_t segment;
	bool operator==(const extentInfo& rhs) const { return (dbRoot == rhs.dbRoot && partition == rhs.partition && segment == rhs.segment); }
	bool operator!=(const extentInfo& rhs) const { return !(*this == rhs); }
};

namespace
{

bool typesAreSame(const CalpontSystemCatalog::ColType& colType, const ColumnType& newType)
{
	switch (colType.colDataType)
	{
	case (CalpontSystemCatalog::BIT):
		if (newType.fType == DDL_BIT) return true;
		break;
	case (CalpontSystemCatalog::TINYINT):
		if (newType.fType == DDL_TINYINT && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		//Not sure this is possible...
		if (newType.fType == DDL_DECIMAL && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	case (CalpontSystemCatalog::UTINYINT):
		if (newType.fType == DDL_UNSIGNED_TINYINT && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	case (CalpontSystemCatalog::CHAR):
		if (newType.fType == DDL_CHAR && colType.colWidth == newType.fLength) return true;
		break;
	case (CalpontSystemCatalog::SMALLINT):
		if (newType.fType == DDL_SMALLINT && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		if (newType.fType == DDL_DECIMAL && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	case (CalpontSystemCatalog::USMALLINT):
		if (newType.fType == DDL_UNSIGNED_SMALLINT && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	case (CalpontSystemCatalog::DECIMAL):
		if ((newType.fType == DDL_DECIMAL || newType.fType == DDL_NUMERIC) && 
            colType.precision == newType.fPrecision && colType.scale == newType.fScale) 
            return true;
		break;
    case (CalpontSystemCatalog::UDECIMAL):
        if ((newType.fType == DDL_UNSIGNED_DECIMAL || newType.fType == DDL_UNSIGNED_NUMERIC) && 
            colType.precision == newType.fPrecision && colType.scale == newType.fScale)
            return true;
        break;
	// Don't think there can be such a type in syscat right now...
	case (CalpontSystemCatalog::MEDINT):
		if (newType.fType == DDL_MEDINT && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		if (newType.fType == DDL_DECIMAL && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	case (CalpontSystemCatalog::INT):
		if (newType.fType == DDL_INT && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		if (newType.fType == DDL_DECIMAL && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	case (CalpontSystemCatalog::UINT):
		if (newType.fType == DDL_UNSIGNED_INT && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	case (CalpontSystemCatalog::FLOAT):
		if (newType.fType == DDL_FLOAT) return true;
		break;
	case (CalpontSystemCatalog::UFLOAT):
		if (newType.fType == DDL_UNSIGNED_FLOAT) return true;
		break;
	case (CalpontSystemCatalog::DATE):
		if (newType.fType == DDL_DATE) return true;
		break;
	case (CalpontSystemCatalog::BIGINT):
		if (newType.fType == DDL_BIGINT && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		//decimal is mapped to bigint in syscat
		if (newType.fType == DDL_DECIMAL && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	case (CalpontSystemCatalog::UBIGINT):
		if (newType.fType == DDL_UNSIGNED_BIGINT && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	case (CalpontSystemCatalog::DOUBLE):
		if (newType.fType == DDL_DOUBLE) return true;
		break;
	case (CalpontSystemCatalog::UDOUBLE):
		if (newType.fType == DDL_UNSIGNED_DOUBLE) return true;
		break;
	case (CalpontSystemCatalog::DATETIME):
		if (newType.fType == DDL_DATETIME) return true;
		break;
	case (CalpontSystemCatalog::VARCHAR):
		if (newType.fType == DDL_VARCHAR && colType.colWidth == newType.fLength) return true;
		break;
	case (CalpontSystemCatalog::VARBINARY):
		if (newType.fType == DDL_VARBINARY && colType.colWidth == newType.fLength) return true;
		break;
	case (CalpontSystemCatalog::CLOB):
		break;
	case (CalpontSystemCatalog::BLOB):
		break;
	default:
		break;
	}
	return false;
}


bool comptypesAreCompat(int oldCtype, int newCtype)
{
	switch (oldCtype)
	{
	case 1:
	case 2:
		return (newCtype == 1 || newCtype == 2);

	default:
		break;
	}

	return (oldCtype == newCtype);
}

}

namespace ddlpackageprocessor
{

AlterTableProcessor::DDLResult AlterTableProcessor::processPackage(ddlpackage::AlterTableStatement& alterTableStmt)
{
	SUMMARY_INFO("AlterTableProcessor::processPackage");

	DDLResult result;
	BRM::TxnID txnID;
	txnID.id= fTxnid.id;
	txnID.valid= fTxnid.valid;
	result.result = NO_ERROR;
	std::string err;
	uint64_t tableLockId = 0;
	DETAIL_INFO(alterTableStmt);
	int rc = 0;
	rc = fDbrm->isReadWrite();
	if (rc != 0 )
	{
		logging::Message::Args args;
		logging::Message message(9);
		args.add("Unable to execute the statement due to DBRM is read only");
		message.format(args);
		result.result = ALTER_ERROR;	
		result.message = message;
		fSessionManager.rolledback(txnID);
		return result;
	}
	//@Bug 4538. Log the sql statement before grabbing tablelock
	SQLLogger logger(alterTableStmt.fSql, fDDLLoggingId, alterTableStmt.fSessionID, txnID.id);
	
	VERBOSE_INFO("Getting current txnID");
	OamCache * oamcache = OamCache::makeOamCache();
	std::vector<int> moduleIds = oamcache->getModuleIds();
	uint64_t uniqueId = 0;
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
	
	fWEClient->addQueue(uniqueId);
	try
	{
		//check table lock
		boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(alterTableStmt.fSessionID);
		systemCatalogPtr->identity(CalpontSystemCatalog::EC);
		systemCatalogPtr->sessionID(alterTableStmt.fSessionID);
		CalpontSystemCatalog::TableName tableName;
		tableName.schema =  (alterTableStmt.fTableName)->fSchema;
		tableName.table = (alterTableStmt.fTableName)->fName;
		execplan::CalpontSystemCatalog::ROPair roPair;
		roPair = systemCatalogPtr->tableRID(tableName);

		u_int32_t  processID = ::getpid();
		int32_t   txnid = txnID.id;
		int32_t sessionId = alterTableStmt.fSessionID;
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
					sessionId = alterTableStmt.fSessionID;;
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
				logging::Message::Args args;
				args.add(processName);
				args.add((uint64_t)processID);
				args.add(sessionId);
				throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_TABLE_LOCKED,args));
			}			
		}
		
		ddlpackage::AlterTableActionList actionList = alterTableStmt.fActions;
		AlterTableActionList::const_iterator action_iterator = actionList.begin();

		while (action_iterator != actionList.end())
		{
			std::string s(typeid(*(*action_iterator)).name());

			if (s.find(AlterActionString[0]) != string::npos)
			{
				//bug 827:change AtaAddColumn to AtaAddColumns
				//Add a column
				//Bug 1192
				ddlpackage::ColumnDef* columnDefPtr = 0;
				ddlpackage::AtaAddColumn *addColumnPtr = dynamic_cast<AtaAddColumn*> (*action_iterator);
				if (addColumnPtr)
				{
					columnDefPtr = addColumnPtr->fColumnDef;
				}
				else
				{
					ddlpackage::AtaAddColumns& addColumns = *(dynamic_cast<AtaAddColumns*> (*action_iterator));
	  				columnDefPtr = addColumns.fColumns[0];
				}

				addColumn (alterTableStmt.fSessionID, txnID.id, result, columnDefPtr, *(alterTableStmt.fTableName), uniqueId);
				if (result.result != NO_ERROR)
				{
					err = "AlterTable: add column failed";
					throw std::runtime_error(err);
				}

			}
			else if (s.find(AlterActionString[6]) != string::npos)
			{
				//Drop Column Default
				dropColumnDefault (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaDropColumnDefault*> (*action_iterator)), *(alterTableStmt.fTableName), uniqueId);
				if (result.result != NO_ERROR)
				{
					err = "AlterTable: drop column default failed";
					throw std::runtime_error(err);
				}
			}
			else if (s.find(AlterActionString[3]) != string::npos)
			{
				//Drop Columns
				dropColumns (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaDropColumns*> (*action_iterator)), *(alterTableStmt.fTableName), uniqueId);
			}
			else if (s.find(AlterActionString[2]) != string::npos)
			{
				//Drop a column
				dropColumn (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaDropColumn*> (*action_iterator)), *(alterTableStmt.fTableName), uniqueId);

			}
#if 0
			else if (s.find(AlterActionString[4]) != string::npos)
			{
				//Add Table Constraint
				addTableConstraint (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaAddTableConstraint*> (*action_iterator)), *(alterTableStmt.fTableName));

			}
#endif
			else if (s.find(AlterActionString[5]) != string::npos)
			{
				//Set Column Default
				setColumnDefault (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaSetColumnDefault*> (*action_iterator)), *(alterTableStmt.fTableName), uniqueId);

			}

#if 0
			else if (s.find(AlterActionString[7]) != string::npos)
			{
				//Drop Table Constraint
				dropTableConstraint (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaDropTableConstraint*> (*action_iterator)), *(alterTableStmt.fTableName));

			}
#endif

			else if (s.find(AlterActionString[8]) != string::npos)
			{
				//Rename Table
				renameTable (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaRenameTable*> (*action_iterator)), *(alterTableStmt.fTableName), uniqueId);

	   		}

			else if (s.find(AlterActionString[10]) != string::npos)
			{
				//Rename a Column
				renameColumn (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaRenameColumn*> (*action_iterator)), *(alterTableStmt.fTableName), uniqueId);

			}
			else
			{
				throw std::runtime_error("Altertable: Error in the action type");

			}

			++action_iterator;

		}

		// Log the DDL statement.
		logging::logDDL(alterTableStmt.fSessionID, txnID.id, alterTableStmt.fSql, alterTableStmt.fOwner);

		DETAIL_INFO("Commiting transaction");
		commitTransaction(uniqueId, txnID);
		fSessionManager.committed(txnID);
	}
	catch (std::exception& ex)
	{
		rollBackAlter(ex.what(), txnID, alterTableStmt.fSessionID, result, uniqueId);
	}
	catch (...)
	{
		rollBackAlter("encountered unknown exception. ", txnID, alterTableStmt.fSessionID, result, uniqueId);
	}
	//release table lock
	try {
		bool lockReleased = true;
		lockReleased = fDbrm->releaseTableLock(tableLockId);
		//cout << "table lock " << tableLockId << " is released" << endl;
	}
	catch (std::exception&)
	{
		if (result.result == NO_ERROR)
		{
			logging::Message::Args args;
			logging::Message message(1);
			args.add("Table lock is not released due to ");
			args.add(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
			args.add("");
			args.add("");
			message.format(args);
			result.result = ALTER_ERROR;
			result.message = message;
		}
	}
	fWEClient->removeQueue(uniqueId);
	return result;

}

void AlterTableProcessor::rollBackAlter(const string& error, BRM::TxnID txnID,
										int sessionId, DDLResult& result, u_int64_t uniqueId)
{
	DETAIL_INFO("Rolling back transaction");
	cerr << "AltertableProcessor::processPackage: " << error << endl;

	logging::Message::Args args;
	logging::Message message(1);
	args.add("Alter table Failed: ");
	args.add(error);
	args.add("");
	args.add("");
	message.format(args);

	rollBackTransaction( uniqueId, txnID, sessionId);
	fSessionManager.rolledback(txnID);
   	result.result = ALTER_ERROR;
	result.message = message;
}

void AlterTableProcessor::addColumn (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, 
		ddlpackage::ColumnDef* columnDefPtr, ddlpackage::QualifiedName& inTableName, const uint64_t uniqueId)
{
	std::string err("AlterTableProcessor::addColumn ");
	SUMMARY_INFO(err);
	// Allocate an object ID for the column we are about to create, non systables only
	VERBOSE_INFO("Allocating object ID for a column");
	ByteStream bs;
	ByteStream::byte tmp8;
	int rc = 0;
	std::string errorMsg;
	u_int16_t  dbRoot;
	BRM::OID_t sysOid = 1001;
	bool isDict = false;
	//@Bug 4111. Check whether the column exists in calpont systable
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::FE);
	CalpontSystemCatalog::TableColName tableColName;
	tableColName.schema = inTableName.fSchema;
	tableColName.table = inTableName.fName;
	tableColName.column = columnDefPtr->fName;
	CalpontSystemCatalog::OID columnOid;
	try {
		columnOid = systemCatalogPtr->lookupOID(tableColName);
	}
	catch (std::exception& ex)
	{
		result.result = ALTER_ERROR;
		err += ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		result.result = ALTER_ERROR;
		err += "Unknown exception caught";
		throw std::runtime_error(err);
	}
	
	if (columnOid > 0) //Column exists already
	{
		err =err + "Internal add column error for " + tableColName.schema + "." + tableColName.table + "." + tableColName.column
			+ ". Column exists already. Your table is probably out-of-sync";
		
		throw std::runtime_error(err);
	}
		
	if ((columnDefPtr->fType->fType == CalpontSystemCatalog::CHAR && columnDefPtr->fType->fLength > 8) ||
				 (columnDefPtr->fType->fType == CalpontSystemCatalog::VARCHAR && columnDefPtr->fType->fLength > 7) ||
				 (columnDefPtr->fType->fType == CalpontSystemCatalog::VARBINARY && columnDefPtr->fType->fLength > 7))
	{
		isDict = true;
	}
	//Find out where systables are
	rc = fDbrm->getSysCatDBRoot(sysOid, dbRoot);
	if (rc != 0)
		throw std::runtime_error("Error while calling getSysCatDBRoot ");
			
	int pmNum = 1;
	OamCache * oamcache = OamCache::makeOamCache();
	boost::shared_ptr<std::map<int, int> > dbRootPMMap = oamcache->getDBRootToPMMap();
	pmNum = (*dbRootPMMap)[dbRoot];
	
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	//Will create files on each PM as needed.
	//BUG931
	//In order to fill up the new column with data an existing column is selected as reference
	ColumnList columns;
	getColumnsForTable(sessionID, inTableName.fSchema, inTableName.fName, columns);
	ColumnList::const_iterator column_iterator;
	column_iterator = columns.begin();

	if (inTableName.fSchema != CALPONT_SCHEMA)
	{
		try {
			execplan::ObjectIDManager fObjectIDManager;
			if (isDict)
			{
					fStartingColOID = fObjectIDManager.allocOIDs(2);
			}
			else
				fStartingColOID = fObjectIDManager.allocOIDs(1);
		}
		catch (std::exception& ex)
		{
			result.result = ALTER_ERROR;
			err += ex.what();
			throw std::runtime_error(err);
		}
		//cout << "new oid is " << fStartingColOID << endl;
	}
	else
	{
		// Add columns to SYSTABLE and SYSCOLUMN
		if ((inTableName.fName == SYSTABLE_TABLE) &&
			(columnDefPtr->fName == AUTOINC_COL))
		{
			fStartingColOID = OID_SYSTABLE_AUTOINCREMENT;
		}
		else if ((inTableName.fName == SYSCOLUMN_TABLE) &&
			(columnDefPtr->fName == COMPRESSIONTYPE_COL))
		{
			fStartingColOID = OID_SYSCOLUMN_COMPRESSIONTYPE;
		}
		else if ((inTableName.fName == SYSCOLUMN_TABLE) &&
			(columnDefPtr->fName == NEXTVALUE_COL))
		{
			fStartingColOID = OID_SYSCOLUMN_NEXTVALUE;
		}
		else
		{
			throw std::runtime_error("Error adding column to calpontsys table");
		}
		columnDefPtr->fType->fCompressiontype = 0;
		columnDefPtr->fType->fAutoincrement   = "n";
		columnDefPtr->fType->fNextvalue       = 0;
		cerr << "updating calpontsys...using static OID " << fStartingColOID << endl;
	}

	fColumnNum = 1;
	//Find the position for the last column
	//@Bug 1358
	CalpontSystemCatalog::TableName tableName;
	tableName.schema = inTableName.fSchema;
	tableName.table = inTableName.fName;
	std::set<BRM::LogicalPartition> outOfSerPar;
	CalpontSystemCatalog::ROPair ropair;
	bool autoincrement = false;
	try
	{
		ropair = systemCatalogPtr->tableRID(tableName);
		if (ropair.objnum < 0)
		{
			err = "No such table: " + tableName.table;
			throw std::runtime_error(err);
		}
		
		int totalColumns = systemCatalogPtr->colNumbers(tableName);
		ColumnDefList aColumnList;
		aColumnList.push_back(columnDefPtr);
		bool alterFlag = true;
		
		if (inTableName.fSchema != CALPONT_SCHEMA)
		{
			VERBOSE_INFO("Writing meta data to SYSCOL"); //send to WES to process
			bs.restart();
			bs << (ByteStream::byte) WE_SVR_WRITE_SYSCOLUMN;
			bs << uniqueId;
			bs << sessionID;
			bs << (uint32_t) txnID;
			bs << inTableName.fSchema;
			bs << inTableName.fName;
			bs << (uint32_t) fStartingColOID;
			if (isDict)
				bs << (uint32_t) (fStartingColOID+1);
			else
				bs << (uint32_t) 0;
				
			bs << (u_int8_t) alterFlag;
			bs << (uint32_t) totalColumns;
			columnDefPtr->serialize(bs);
			//send to WES to process
			try {
				fWEClient->write(bs, (uint)pmNum);
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
			}
			catch (runtime_error& ex) //write error
			{		
				rc = NETWORK_ERROR;
				errorMsg = ex.what();
			}
			catch (...)
			{
				rc = NETWORK_ERROR;
				errorMsg = " Unknown exception caught while updating SYSTABLE.";
			}
		
			if (rc != 0)
			throw std::runtime_error(errorMsg);		
			
		}

		if ((columnDefPtr->fType->fAutoincrement).compare("y") == 0)
		{
			//update systable autoincrement column
			bs.restart();
			bs << (ByteStream::byte) WE_SVR_UPDATE_SYSTABLE_AUTO;
			bs << uniqueId;
			bs << sessionID;
			bs << (uint32_t) txnID;
			bs << inTableName.fSchema;
			bs << inTableName.fName;
			bs << (uint32_t) 1;
			
			//send to WES to process
			try {
				fWEClient->write(bs, (uint)pmNum);
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
			}
			catch (runtime_error& ex) //write error
			{		
				rc = NETWORK_ERROR;
				errorMsg = ex.what();
			}
			catch (...)
			{
				rc = NETWORK_ERROR;
				errorMsg = " Unknown exception caught while updating SYSTABLE.";
			}
		
			if (rc != 0)
			throw std::runtime_error(errorMsg);	

			//start a sequence in controller 
			fDbrm->startAISequence(fStartingColOID, columnDefPtr->fType->fNextvalue, columnDefPtr->fType->fLength,
								  convertDataType(columnDefPtr->fType->fType));
		}
		
		//@Bug 4176. save oids to a log file for cleanup after fail over.
		std::vector <CalpontSystemCatalog::OID> oidList;
		if (isDict)
		{
			oidList.push_back(fStartingColOID);
			oidList.push_back(fStartingColOID+1);
		}
		else
			oidList.push_back(fStartingColOID);
		
		
		createWriteDropLogFile( ropair.objnum, uniqueId, oidList );
		
		//@Bug 1358,1427 Always use the first column in the table, not the first one in columnlist to prevent random result
		//@Bug 4182. Use widest column as reference column
		
		//Find the widest column
		unsigned int colpos = 0;
		int maxColwidth = 0;
		for (colpos = 0; colpos < columns.size(); colpos++)
		{
			if (columns[colpos].colType.colWidth > maxColwidth)
				maxColwidth = columns[colpos].colType.colWidth;	
		}
		
		while(column_iterator != columns.end()) {
			if (column_iterator->colType.colWidth == maxColwidth)
			{
				//If there is atleast one existing column then use that as a reference to initialize the new column rows.
				//get dbroot information

				//fDbrm->getStartExtent((*column_iterator).oid, dbroot, partitionNum, true);

				rc = fDbrm->getOutOfServicePartitions(column_iterator->oid, outOfSerPar);

				if (rc != 0)
				{
					string errorMsg;
					BRM::errString(rc, errorMsg);
					ostringstream oss;
					oss << "getOutOfServicePartitions failed  due to " << errorMsg;
					throw std::runtime_error(oss.str());
				}
				
			
				int dataType1;
				dataType1 = convertDataType(columnDefPtr->fType->fType);
				if (dataType1 == CalpontSystemCatalog::DECIMAL ||
                    dataType1 == CalpontSystemCatalog::UDECIMAL)
				{
					columnDefPtr->convertDecimal();
				}
				
				CalpontSystemCatalog::ColDataType dataType = convertDataType(columnDefPtr->fType->fType);
			
				if ((columnDefPtr->fType->fAutoincrement).compare("y") == 0)
				{
					autoincrement = true;
				}
				//send to all WES to add the new column
				bs.restart();
				bs << (ByteStream::byte) WE_SVR_FILL_COLUMN;
				bs << uniqueId;
				bs << (uint32_t) txnID;
				bs << (uint32_t) fStartingColOID;
				if (isDict)
					bs << (uint32_t) (fStartingColOID+1);
				else
					bs << (uint32_t) 0;
				//new column info
				bs << (ByteStream::byte) dataType;
				bs << (ByteStream::byte) autoincrement;
				bs << (uint32_t) columnDefPtr->fType->fLength;
				bs << (uint32_t) columnDefPtr->fType->fScale;
				bs << (uint32_t) columnDefPtr->fType->fPrecision;
				std::string tmpStr("");
				if (columnDefPtr->fDefaultValue)
				{
					tmpStr = columnDefPtr->fDefaultValue->fValue;
				}
				
				bs << tmpStr;
				bs << (ByteStream::byte) columnDefPtr->fType->fCompressiontype;
				//ref column info
				bs << (uint32_t) column_iterator->oid;
				bs << (ByteStream::byte) column_iterator->colType.colDataType;
				bs << (uint32_t) column_iterator->colType.colWidth;
				bs << (ByteStream::byte) column_iterator->colType.compressionType;
				//cout << "sending command fillcolumn " << endl;	 
                uint msgRecived = 0;
                fWEClient->write_to_all(bs);
                bsIn.reset(new ByteStream());
                while (1)
                {
                    if (msgRecived == fPMCount)
                        break;
                    fWEClient->read(uniqueId, bsIn);
                    if ( bsIn->length() == 0 ) //read error
                    {
                        rc = NETWORK_ERROR;
                        break;
                    }			
                    else {
                        *bsIn >> tmp8;
                        *bsIn >> errorMsg;
                        rc = tmp8;
                        //cout << "Got error code from WES " << rc << endl;
                        if (rc != 0) 				
                            break;	
                        else
                            msgRecived++;						
                    }
                }				
                if (rc != 0) //delete the newly created files before erroring out
                {					
                    bs.restart();
                    bs << (ByteStream::byte)WE_SVR_WRITE_DROPFILES;
                    bs << uniqueId;
                    bs << (uint32_t) oidList.size();
                    for (uint i=0; i < oidList.size(); i++)
                    {
                        bs << (uint32_t) oidList[i];
                    }
                    
                    uint msgRecived = 0;
                    try {
                        fWEClient->write_to_all(bs);
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
                                errorMsg = "Lost connection to Write Engine Server while dropping column files";
                                break;
                            }			
                            else {
                                *bsIn >> tmp8;
                                rc = tmp8;
                                if (rc != 0) {
                                    *bsIn >> errorMsg;
                                    break;
                                }
                                else
                                    msgRecived++;						
                            }
                        }
                    }
                    catch (runtime_error& ex) //write error
                    {		
                        rc = NETWORK_ERROR;
                        errorMsg = ex.what();
                    }
                    catch (...)
                    {
                        rc = NETWORK_ERROR;
                        errorMsg = " Unknown exception caught while dropping column files.";
                    }

                    if ( rc == 0 ) {
                        fWEClient->removeQueue(uniqueId);
                        deleteLogFile(DROPTABLE_LOG, ropair.objnum, uniqueId);
                        fWEClient->addQueue(uniqueId);
                    }
                    throw std::runtime_error(errorMsg);
                }
                    
                //Update nextVal
					break;
			}
				//Update nextVal
			column_iterator++;
		}
	}
	catch (std::exception& ex)
	{
		if (result.result != CREATE_ERROR)
			result.result = ALTER_ERROR;
		err += ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		result.result = ALTER_ERROR;
		err += "Unknown exception caught";
		throw std::runtime_error(err);
	}
	
	//update SYSCOLUMN of the new next value
	if (autoincrement)
	{
		DBRM aDbrm;
		aDbrm.getAILock(fStartingColOID);	
		uint64_t nextValInController;
		bool validNextVal = aDbrm.getAIValue(fStartingColOID, &nextValInController);	
		if (validNextVal)
		{
			WE_DDLCommandClient ddlClient;
			uint8_t rc = ddlClient.UpdateSyscolumnNextval(fStartingColOID, nextValInController,sessionID);
			aDbrm.releaseAILock(fStartingColOID);
			if (rc!=0)
				throw std::runtime_error("Update SYSCAT next value failed");
		}
		else
		{
			aDbrm.releaseAILock(fStartingColOID);
			throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT));
		}
		aDbrm.releaseAILock(fStartingColOID);
	}
	std::vector <CalpontSystemCatalog::OID> oidList;
	oidList.push_back(fStartingColOID);
	
	if (outOfSerPar.size() > 0)
		rc = fDbrm->markPartitionForDeletion(oidList, outOfSerPar, errorMsg);
		
	if (rc != 0)
	{
		ostringstream oss;
		oss << "Mark partition for deletition failed  due to " << errorMsg;
		throw std::runtime_error(oss.str());
	}
	fWEClient->removeQueue(uniqueId);
	deleteLogFile(DROPTABLE_LOG, ropair.objnum, uniqueId);
	fWEClient->addQueue(uniqueId);
}

void AlterTableProcessor::dropColumn (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, 
			ddlpackage::AtaDropColumn& ataDropColumn, ddlpackage::QualifiedName& fTableName, const uint64_t uniqueId)
{
	// 1. Get the OIDs for the column
	// 2. Get the OIDs for the dictionary
	// 3. Remove the column from SYSCOLUMN
	// 4. update systable if the dropped column is autoincrement column 
	// 5. update column position for affected columns
	// 6. Remove the files

	SUMMARY_INFO("AlterTableProcessor::dropColumn");
	VERBOSE_INFO("Finding object IDs for the column");
	CalpontSystemCatalog::TableColName tableColName;
	CalpontSystemCatalog::TableName tableName;
	tableName.schema = fTableName.fSchema;
	tableName.table = fTableName.fName;
	tableColName.schema = fTableName.fSchema;
	tableColName.table = fTableName.fName;
	tableColName.column = ataDropColumn.fColumnName;
	execplan::CalpontSystemCatalog::DictOIDList dictOIDList;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
	CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	//@Bug 1358
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	std::string err;
	execplan::CalpontSystemCatalog::ROPair roPair;
    CalpontSystemCatalog::OID oid;
	CalpontSystemCatalog::ColType colType;
	try {
		roPair = systemCatalogPtr->tableRID( tableName );

        oid = systemCatalogPtr->lookupOID(tableColName);
        colType = systemCatalogPtr->colType(oid);
	}
	catch (std::exception& ex)
	{
		throw std::runtime_error(ex.what());
	}
	int colPos = colType.colPosition;	
	ByteStream bytestream;
	bytestream << (ByteStream::byte)WE_SVR_DELETE_SYSCOLUMN_ROW;
	bytestream << uniqueId;
	bytestream << sessionID;
	bytestream << (u_int32_t)txnID;
	bytestream << fTableName.fSchema;
	bytestream << fTableName.fName;
	bytestream << ataDropColumn.fColumnName;
	
	std::string errorMsg;
	u_int16_t  dbRoot;
	BRM::OID_t sysOid = 1001;
	ByteStream::byte rc = 0;
	//Find out where systable is
	rc = fDbrm->getSysCatDBRoot(sysOid, dbRoot);  
	if (rc != 0)
		throw std::runtime_error("Error while calling getSysCatDBRoot ");
	
	int pmNum = 1;
		
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	OamCache * oamcache = OamCache::makeOamCache();
	boost::shared_ptr<std::map<int, int> > dbRootPMMap = oamcache->getDBRootToPMMap();
	pmNum = (*dbRootPMMap)[dbRoot];
	try
	{			
		fWEClient->write(bytestream, (uint)pmNum);
#ifdef IDB_DDL_DEBUG
cout << "Alter table drop column sending WE_SVR_DELETE_SYSCOLUMN_ROW to pm " << pmNum << endl;
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
				*bsIn >> errorMsg;
				break;
			}
		}
	}
	catch (runtime_error& ex) //write error
	{
#ifdef IDB_DDL_DEBUG
cout << "Alter table drop column got exception" << ex.what() << endl;
#endif			
		rc = NETWORK_ERROR;
		errorMsg = ex.what();
	}
	catch (...)
	{
		rc = NETWORK_ERROR;
		errorMsg = " Unknown exception caught while updating SYSTABLE.";
#ifdef IDB_DDL_DEBUG
cout << "Alter table drop column got unknown exception" << endl;
#endif
	}
		
	if (rc != 0)
		throw std::runtime_error(errorMsg);
		
	//Update SYSTABLE 
	if (colType.autoincrement)
	{
		bytestream.restart();
		bytestream << (ByteStream::byte)WE_SVR_UPDATE_SYSTABLE_AUTO;
		bytestream << uniqueId;
		bytestream << sessionID;
		bytestream << (u_int32_t)txnID;
		bytestream << fTableName.fSchema;
		bytestream << fTableName.fName;
		bytestream << (u_int32_t) 0; //autoincrement off
		
		try {		
			fWEClient->write(bytestream, (uint)pmNum);
#ifdef IDB_DDL_DEBUG
cout << "Alter table drop column sending WE_SVR_UPDATE_SYSTABLE_AUTO to pm " << pmNum << endl;
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
					*bsIn >> errorMsg;
					break;
				}
			}
		}
		catch (runtime_error& ex) //write error
		{
#ifdef IDB_DDL_DEBUG
cout << "Alter table drop column got exception" << ex.what() << endl;
#endif			
			rc = NETWORK_ERROR;
			errorMsg = ex.what();
		}
		catch (...)
		{
			rc = NETWORK_ERROR;
			errorMsg = " Unknown exception caught while updating SYSTABLE.";
#ifdef IDB_DDL_DEBUG
cout << "Alter table drop column got unknown exception" << endl;
#endif
		}
		
		if (rc != 0)
			throw std::runtime_error(errorMsg);		
	}

	//Update column position
	bytestream.restart();
	bytestream << (ByteStream::byte)WE_SVR_UPDATE_SYSCOLUMN_COLPOS;
	bytestream << uniqueId;
	bytestream << sessionID;
	bytestream << (u_int32_t)txnID;
	bytestream << fTableName.fSchema;
	bytestream << fTableName.fName;
	bytestream << (u_int32_t) colPos;
	try {		
		fWEClient->write(bytestream, (uint)pmNum);
#ifdef IDB_DDL_DEBUG
cout << "Alter table drop column sending WE_SVR_UPDATE_SYSTABLE_AUTO to pm " << pmNum << endl;
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
				*bsIn >> errorMsg;
				break;
			}
		}
	}
	catch (runtime_error& ex) //write error
	{
#ifdef IDB_DDL_DEBUG
cout << "Alter table drop column got exception" << ex.what() << endl;
#endif			
		rc = NETWORK_ERROR;
		errorMsg = ex.what();
	}
	catch (...)
	{
		rc = NETWORK_ERROR;
		errorMsg = " Unknown exception caught while updating SYSTABLE.";
#ifdef IDB_DDL_DEBUG
cout << "Alter table drop column got unknown exception" << endl;
#endif
	}
		
	if (rc != 0)
		throw std::runtime_error(errorMsg);		

	//commit the transaction.
	BRM::TxnID aTxnID;
	aTxnID.id= txnID;
	aTxnID.valid= true;
	commitTransaction(uniqueId, aTxnID);

    // Bug 4208 Drop the PrimProcFDCache before droping the column files
    // FOr Windows, this ensures (most likely) that the column files have
    // no open handles to hinder the deletion of the files.
	rc = cacheutils::dropPrimProcFdCache();

    VERBOSE_INFO("Removing column files");
	//Drop files
	std::vector <CalpontSystemCatalog::OID> oidList;
	bytestream.restart();
	bytestream << (ByteStream::byte)WE_SVR_WRITE_DROPFILES;
	bytestream << uniqueId;
	if (colType.ddn.dictOID > 3000)  //@bug 4847. need to take care varchar(8)
	{
		bytestream << (uint32_t) 2;
		bytestream << (uint32_t) oid;
		bytestream << (uint32_t) colType.ddn.dictOID;
		oidList.push_back(oid);
		oidList.push_back( colType.ddn.dictOID);
	}
	else
	{
		bytestream << (uint32_t) 1;
		bytestream << (uint32_t) oid;
		oidList.push_back(oid);
	}
	//Save the oids to a file
	uint msgRecived = 0;
	bool fileDropped = true;
	try {
		createWriteDropLogFile( roPair.objnum, uniqueId, oidList );
		//@Bug 4811. Need to send to all PMs
		fWEClient->write_to_all(bytestream);
#ifdef IDB_DDL_DEBUG
cout << "Alter table drop column sending WE_SVR_UPDATE_SYSTABLE_AUTO to pm " << pmNum << endl;
#endif	
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
				errorMsg = "Lost connection to Write Engine Server while dropping column files";
				break;
			}			
			else {
				*bsIn >> tmp8;
				rc = tmp8;
				if (rc != 0) {
					*bsIn >> errorMsg;
					fileDropped = false;
					break;
				}
				else
					msgRecived++;						
			}
		}
	}
	catch (runtime_error& ex) //write error
	{
#ifdef IDB_DDL_DEBUG
cout << "Alter table drop column got exception" << ex.what() << endl;
#endif			
		rc = NETWORK_ERROR;
		errorMsg = ex.what();
	}
	catch (...)
	{
		rc = NETWORK_ERROR;
		errorMsg = " Unknown exception caught while dropping column files.";
#ifdef IDB_DDL_DEBUG
cout << "create table got unknown exception" << endl;
#endif
	}							
	//@Bug 3860
	rc = cacheutils::dropPrimProcFdCache();
	//Flush primProc cache
	rc = cacheutils::flushOIDsFromCache( oidList );
	//Delete extents from extent map
	rc = fDbrm->deleteOIDs(oidList);
	if (fileDropped)
    {
        fWEClient->removeQueue(uniqueId);
        deleteLogFile(DROPTABLE_LOG, roPair.objnum, uniqueId);
        fWEClient->addQueue(uniqueId);
    }
}


void AlterTableProcessor::dropColumns (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, 
		ddlpackage::AtaDropColumns& ataDropColumns, ddlpackage::QualifiedName& fTableName, const uint64_t uniqueId)
{
	SUMMARY_INFO("AlterTableProcessor::dropColumns");
	ddlpackage::ColumnNameList colList = ataDropColumns.fColumns;
	ddlpackage::ColumnNameList::const_iterator col_iter = colList.begin();

	std::string err;
	try
	{
		while (col_iter != colList.end())
		{
		ddlpackage::AtaDropColumn ataDropColumn;

		ataDropColumn.fColumnName = *col_iter;

		dropColumn (sessionID,txnID, result,ataDropColumn,fTableName,uniqueId);

		if (result.result != NO_ERROR)
		{
			DETAIL_INFO("dropColumns::dropColumn failed");
			return;
		}
		col_iter++;
		}
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "dropColumns:Unknown exception caught";
		throw std::runtime_error(err);
	}
}

void AlterTableProcessor::addTableConstraint (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, ddlpackage::AtaAddTableConstraint& ataAddTableConstraint, ddlpackage::QualifiedName& fTableName)
{
	/*TODO: Check if existing row satisfy the constraint.
	If not, the constraint will not be added. */
	SUMMARY_INFO("AlterTableProcessor::addTableConstraint");
	ddlpackage::TableConstraintDefList constrainList;
	constrainList.push_back(ataAddTableConstraint.fTableConstraint);
	VERBOSE_INFO("Writing table constraint meta data to SYSCONSTRAINT");
	//bool alterFlag = true;
	std::string err;
	try
	{
		//writeTableSysConstraintMetaData(sessionID, txnID, result, constrainList, fTableName, alterFlag);
		VERBOSE_INFO("Writing table constraint meta data to SYSCONSTRAINTCOL");
		//writeTableSysConstraintColMetaData(sessionID, txnID, result,constrainList, fTableName, alterFlag);
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "addTableConstraint:Unknown exception caught";
		throw std::runtime_error(err);
	}
}

void AlterTableProcessor::setColumnDefault (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, 
		ddlpackage::AtaSetColumnDefault& ataSetColumnDefault, ddlpackage::QualifiedName& fTableName, const uint64_t uniqueId)
{
	SUMMARY_INFO("AlterTableProcessor::setColumnDefault");
	/*Steps:
	1. Update SYSCOLUMN for default value change
	*/
	SUMMARY_INFO("AlterTableProcessor::setColumnDefault");
	ByteStream bs;
	std::string errorMsg;
	u_int16_t  dbRoot;
	BRM::OID_t sysOid = 1001;
	ByteStream::byte rc = 0;
	//Find out where systable is
	rc = fDbrm->getSysCatDBRoot(sysOid, dbRoot);  
	if (rc != 0)
		throw std::runtime_error("Error while calling getSysCatDBRoot");
	
	int pmNum = 1;
	OamCache * oamcache = OamCache::makeOamCache();
	boost::shared_ptr<std::map<int, int> > dbRootPMMap = oamcache->getDBRootToPMMap();
	pmNum = (*dbRootPMMap)[dbRoot];
	
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	
	string err;

			
	//Update SYSCOLUMN
	bs.restart();
	bs << (ByteStream::byte) WE_SVR_UPDATE_SYSCOLUMN_DEFAULTVAL;
	bs << uniqueId;
	bs << sessionID;
	bs << (uint32_t) txnID;
	bs << fTableName.fSchema;
	bs << fTableName.fName;
	bs << ataSetColumnDefault.fColumnName;
	string defaultValue("");
	if (ataSetColumnDefault.fDefaultValue)
		defaultValue = ataSetColumnDefault.fDefaultValue->fValue;
			 
	bs << defaultValue;
		
	//send to WES to process
	try {
		fWEClient->write(bs, (uint)pmNum);
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
		rc = NETWORK_ERROR;
		errorMsg = ex.what();
	}
	catch (...)
	{
		rc = NETWORK_ERROR;
		errorMsg = " Unknown exception caught while updating SYSTABLE.";
	}
		
	if (rc != 0)
		throw std::runtime_error(errorMsg);	
}
void AlterTableProcessor::dropColumnDefault (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, 
		ddlpackage::AtaDropColumnDefault& ataDropColumnDefault, ddlpackage::QualifiedName& fTableName, const uint64_t uniqueId)
{
		SUMMARY_INFO("AlterTableProcessor::setColumnDefault");
	/*Steps:
	1. Update SYSCOLUMN for default value change
	*/
	SUMMARY_INFO("AlterTableProcessor::setColumnDefault");
	ByteStream bs;
	std::string errorMsg;
	u_int16_t  dbRoot;
	BRM::OID_t sysOid = 1001;
	ByteStream::byte rc = 0;
	//Find out where systable is
	rc = fDbrm->getSysCatDBRoot(sysOid, dbRoot);  
	if (rc != 0)
		throw std::runtime_error("Error while calling getSysCatDBRoot");
	
	int pmNum = 1;
	OamCache * oamcache = OamCache::makeOamCache();
	boost::shared_ptr<std::map<int, int> > dbRootPMMap = oamcache->getDBRootToPMMap();
	pmNum = (*dbRootPMMap)[dbRoot];
	
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	
	string err;

			
	//Update SYSCOLUMN
	bs.restart();
	bs << (ByteStream::byte) WE_SVR_UPDATE_SYSCOLUMN_DEFAULTVAL;
	bs << uniqueId;
	bs << sessionID;
	bs << (uint32_t) txnID;
	bs << fTableName.fSchema;
	bs << fTableName.fName;
	bs << ataDropColumnDefault.fColumnName;
	string defaultValue("");		 
	bs << defaultValue;
		
	//send to WES to process
	try {
		fWEClient->write(bs, (uint)pmNum);
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
		rc = NETWORK_ERROR;
		errorMsg = ex.what();
	}
	catch (...)
	{
		rc = NETWORK_ERROR;
		errorMsg = " Unknown exception caught while updating SYSTABLE.";
	}
		
	if (rc != 0)
		throw std::runtime_error(errorMsg);	
}

#if 0
void AlterTableProcessor::dropTableConstraint (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, ddlpackage::AtaDropTableConstraint& ataDropTableConstraint, ddlpackage::QualifiedName& fTableName)
{
	/*Steps tp drop table constraint
	 1. Delete the ConstraintName from SYSCONSTRAINT
	 2. Delete the corresponding row from SYSCONSTRAINTCOL
	 3. Delete the row from SYSINDEX if PK;
	 3. Delete the rows from SYSINDEXCOL if PK;
	 */
	SUMMARY_INFO("AlterTableProcessor::dropTableConstraint");
	ddlpackage::QualifiedName sysCatalogTableName;
	sysCatalogTableName.fSchema = CALPONT_SCHEMA;
	sysCatalogTableName.fName  = SYSCONSTRAINT_TABLE;
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr;
	systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	//@Bug 1358
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	VERBOSE_INFO("Removing constraint meta data from SYSCONSTRAINT");
	std::string err;
	try
	{
		execplan::CalpontSystemCatalog::RID constrainRid = systemCatalogPtr->constraintRID(ataDropTableConstraint.fConstraintName);
		if (constrainRid == std::numeric_limits<CalpontSystemCatalog::RID>::max())
		{
			// build the logging message
			err = "Alter Table failed: Constraint name not found";
			throw std::runtime_error(err);
		}
		removeRowFromSysCatalog(sessionID, txnID, result, sysCatalogTableName, constrainRid);

		VERBOSE_INFO("Removing constraint meta data from SYSCONSTRAINTCOL");
		sysCatalogTableName.fName  = SYSCONSTRAINTCOL_TABLE;
		execplan::CalpontSystemCatalog::RIDList ridlist = systemCatalogPtr->constraintColRID(ataDropTableConstraint.fConstraintName);
		if (ridlist.size() == 0)
		{
			// build the logging message
			err = "Alter Table failed: Constraint name not found";
			throw std::runtime_error(err);
		}
		removeRowsFromSysCatalog(sessionID, txnID, result, sysCatalogTableName, ridlist);

		execplan::CalpontSystemCatalog::IndexName idxName;
		idxName.schema = fTableName.fSchema;
		idxName.table = fTableName.fName;
		idxName.index = ataDropTableConstraint.fConstraintName;
		execplan::CalpontSystemCatalog::ROPair ropair = systemCatalogPtr->indexRID(idxName);
		if (ropair.rid >= 0)
		{
			sysCatalogTableName.fName  = SYSINDEX_TABLE;
			removeRowFromSysCatalog(sessionID, txnID, result, sysCatalogTableName, ropair.rid);
		}
		execplan::CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->indexColRIDs(idxName);

		if (ridList.size() > 0)
		{
			execplan::CalpontSystemCatalog::RIDList::const_iterator riditer;
			sysCatalogTableName.fName  = SYSINDEXCOL_TABLE;
			riditer = ridList.begin();
			while (riditer != ridList.end())
			{
				ropair = *riditer;
				removeRowFromSysCatalog(sessionID, txnID, result, sysCatalogTableName, ropair.rid);
				riditer++;
			}
		}
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "dropTableConstraint:Unknown exception caught";
		throw std::runtime_error(err);
	}

}
#endif

void AlterTableProcessor::renameTable (uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, ddlpackage::AtaRenameTable& ataRenameTable, 
			ddlpackage::QualifiedName& fTableName, const uint64_t uniqueId)
{
	/*Steps:
	1. Update SYSTABLE (table name)
	2. Update SYSCOLUMN (table name)
	*/
	SUMMARY_INFO("AlterTableProcessor::renameTable");
	
	//@Bug 4599. Check whether the new table exists in infinidb
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	execplan::CalpontSystemCatalog::TableName tableName;
	tableName.schema = fTableName.fSchema;
	tableName.table = ataRenameTable.fQualifiedName->fName;
	execplan::CalpontSystemCatalog::ROPair roPair;
	roPair.objnum = 0;
	try
	{
		roPair = systemCatalogPtr->tableRID(tableName);
	}
	catch (...)
	{
		roPair.objnum = 0;
	}
	
	if (roPair.objnum >= 3000)
		throw std::runtime_error("The new tablename is already in use.");
	
	ByteStream bytestream;
	bytestream << (ByteStream::byte)WE_SVR_UPDATE_SYSTABLES_TABLENAME;
	bytestream << uniqueId;
	bytestream << sessionID;
	bytestream << (u_int32_t)txnID;
	bytestream << fTableName.fSchema;
	bytestream << fTableName.fName;
	bytestream << ataRenameTable.fQualifiedName->fName;
	
	std::string errorMsg;
	u_int16_t  dbRoot;
	BRM::OID_t sysOid = 1001;
	ByteStream::byte rc = 0;
	//Find out where systable is
	rc = fDbrm->getSysCatDBRoot(sysOid, dbRoot);   
	if (rc != 0)
		throw std::runtime_error("Error while calling getSysCatDBRoot");
	
	int pmNum = 1;
		
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	OamCache * oamcache = OamCache::makeOamCache();
	boost::shared_ptr<std::map<int, int> > dbRootPMMap = oamcache->getDBRootToPMMap();
	pmNum = (*dbRootPMMap)[dbRoot];
	try
	{		
		fWEClient->write(bytestream, (uint)pmNum);
#ifdef IDB_DDL_DEBUG
cout << "Rename table sending WE_SVR_UPDATE_SYSTABLE_TABLENAME to pm " << pmNum << endl;
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
				*bsIn >> errorMsg;
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
		errorMsg = " Unknown exception caught while updating SYSTABLE.";
#ifdef IDB_DDL_DEBUG
cout << "create table got unknown exception" << endl;
#endif
	}
		
	if (rc != 0)
		throw std::runtime_error(errorMsg);
}

void AlterTableProcessor::renameColumn(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID,
	DDLResult& result, ddlpackage::AtaRenameColumn& ataRenameColumn, 
	ddlpackage::QualifiedName& fTableName, const uint64_t uniqueId)
{
	/*Steps:
	1. Update SYSCOLUMN for name, autoincrement, nextval change
	2. Update SYSTABLE if column is autoincrement column
	*/
	SUMMARY_INFO("AlterTableProcessor::renameColumn");
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
		
	ByteStream bs;
	std::string errorMsg;
	u_int16_t  dbRoot;
	BRM::OID_t sysOid = 1001;
	ByteStream::byte rc = 0;
	//Find out where systable is
	rc = fDbrm->getSysCatDBRoot(sysOid, dbRoot);  
	if (rc != 0)
		throw std::runtime_error("Error while calling getSysCatDBRoot");
	
	int pmNum = 1;
	OamCache * oamcache = OamCache::makeOamCache();
	boost::shared_ptr<std::map<int, int> > dbRootPMMap = oamcache->getDBRootToPMMap();
	pmNum = (*dbRootPMMap)[dbRoot];
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	

	CalpontSystemCatalog::TableName tableName;
	CalpontSystemCatalog::TableColName tableColName;
	tableColName.schema = fTableName.fSchema;
	tableColName.table = fTableName.fName;
	tableColName.column = ataRenameColumn.fName;
	CalpontSystemCatalog::ROPair ropair;

	string err;

	try
	{
		//This gives us the rid in syscolumn that we want to update
		tableName.schema = tableColName.schema;
		tableName.table = tableColName.table;
		ropair = systemCatalogPtr->tableRID(tableName);
		if (ropair.objnum < 0)
		{
			ostringstream oss;
			oss << "No such table: " << tableName;
			throw std::runtime_error(oss.str().c_str());
		}
		
		ropair = systemCatalogPtr->columnRID(tableColName);
		if (ropair.objnum < 0)
		{
			ostringstream oss;
			oss << "No such column: " << tableColName;
			throw std::runtime_error(oss.str().c_str());
		}
		CalpontSystemCatalog::ColType colType = systemCatalogPtr->colType(ropair.objnum);

		if (!typesAreSame(colType, *ataRenameColumn.fNewType))
		{
			ostringstream oss;
			oss << "Changing the datatype of a column is not supported";
			throw std::runtime_error(oss.str().c_str());
		}
		
		//@Bug 3746 Check whether the change is about the compression type
		if (!comptypesAreCompat(colType.compressionType, (*ataRenameColumn.fNewType).fCompressiontype))
		{
			ostringstream oss;
			oss << "The compression type of an existing column cannot be changed.";
			throw std::runtime_error(oss.str().c_str());
		}
		//Check whether SYSTABLE needs to be updated
		CalpontSystemCatalog::TableInfo tblInfo = systemCatalogPtr->tableInfo(tableName);	
		if (((tblInfo.tablewithautoincr == 1) && (colType.autoincrement) && (ataRenameColumn.fNewType->fAutoincrement.compare("n") == 0)) || 
			((tblInfo.tablewithautoincr == 0) && (ataRenameColumn.fNewType->fAutoincrement.compare("y") == 0)))
		{
			//update systable autoincrement column
			bs.restart();
			bs << (ByteStream::byte) WE_SVR_UPDATE_SYSTABLE_AUTO;
			bs << uniqueId;
			bs << sessionID;
			bs << (uint32_t) txnID;
			bs << fTableName.fSchema;
			bs << fTableName.fName;
			if (ataRenameColumn.fNewType->fAutoincrement.compare("y") == 0)
				bs << (uint32_t) 1;
			else
				bs << (uint32_t) 0;
			
			//send to WES to process
			try {
				fWEClient->write(bs, (uint)pmNum);
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
				rc = NETWORK_ERROR;
				errorMsg = ex.what();
			}
			catch (...)
			{
				rc = NETWORK_ERROR;
				errorMsg = " Unknown exception caught while updating SYSTABLE.";
			}
		
			if (rc != 0)
			throw std::runtime_error(errorMsg);	

			//change a sequence in controller 
			if (ataRenameColumn.fNewType->fAutoincrement.compare("y") == 0)
			{
				fDbrm->startAISequence(ropair.objnum, ataRenameColumn.fNewType->fNextvalue, ataRenameColumn.fNewType->fLength,
									  convertDataType(ataRenameColumn.fNewType->fType));	
				//Reset it in case there is a sequence already
				fDbrm->resetAISequence(ropair.objnum, ataRenameColumn.fNewType->fNextvalue);
			}
			
		}	
		else
		{
			fDbrm->resetAISequence(ropair.objnum, 0);
		}
		
		//Update SYSCOLUMN
		bs.restart();
		bs << (ByteStream::byte) WE_SVR_UPDATE_SYSCOLUMN_RENAMECOLUMN;
		bs << uniqueId;
		bs << sessionID;
		bs << (uint32_t) txnID;
		bs << fTableName.fSchema;
		bs << fTableName.fName;
		bs << ataRenameColumn.fName;	
		bs << ataRenameColumn.fNewName;	
		bs << ataRenameColumn.fNewType->fAutoincrement;
		bs << (uint64_t) ataRenameColumn.fNewType->fNextvalue;
		uint32_t nullable = 1;
		string defaultValue("");
		if (ataRenameColumn.fConstraints.size() > 0)
		{
			for (uint j=0; j < ataRenameColumn.fConstraints.size(); j++)
			{
				if (ataRenameColumn.fConstraints[j]->fConstraintType == DDL_NOT_NULL)
				{
					nullable = 0;
					break;
				}
			}
		}
		bs << nullable;
		if (ataRenameColumn.fDefaultValue)
			defaultValue = ataRenameColumn.fDefaultValue->fValue;
			 
		bs << defaultValue;
		
		//send to WES to process
		try {
			fWEClient->write(bs, (uint)pmNum);
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
			rc = NETWORK_ERROR;
			errorMsg = ex.what();
		}
		catch (...)
		{
			rc = NETWORK_ERROR;
			errorMsg = " Unknown exception caught while updating SYSTABLE.";
		}
		
		if (rc != 0)
			throw std::runtime_error(errorMsg);	
	}
	catch (std::exception& ex)
	{
		err = ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "renameColumn:Unknown exception caught";
		throw std::runtime_error(err);
	}
		
}

}
// vim:ts=4 sw=4:

