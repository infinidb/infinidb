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

//   $Id: altertableprocessor.cpp 9059 2012-11-07 15:36:33Z chao $

/** @file */

#define ALTERTABLEPROC_DLLEXPORT
#include "altertableprocessor.h"
#undef ALTERTABLEPROC_DLLEXPORT
#include "sessionmanager.h"
#include "brm.h"
#include <boost/algorithm/string/case_conv.hpp>
using namespace boost::algorithm;

#include "messagelog.h"
#include "ddlindexpopulator.h"
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
using namespace WriteEngine;

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
const int MAX_INT = numeric_limits<int32_t>::max();
const short MAX_TINYINT = numeric_limits<int8_t>::max(); //127;
const short MAX_SMALLINT = numeric_limits<int16_t>::max(); //32767;
const long long MAX_BIGINT = numeric_limits<int64_t>::max();//9223372036854775807LL

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
	case (CalpontSystemCatalog::CHAR):
		if (newType.fType == DDL_CHAR && colType.colWidth == newType.fLength) return true;
		break;
	case (CalpontSystemCatalog::SMALLINT):
		if (newType.fType == DDL_SMALLINT && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		if (newType.fType == DDL_DECIMAL && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	// Don't think there can be such a type in syscat right now...
	case (CalpontSystemCatalog::DECIMAL):
		if (newType.fType == DDL_DECIMAL && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	// Don't think there can be such a type in syscat right now...
	case (CalpontSystemCatalog::MEDINT):
		if (newType.fType == DDL_MEDINT && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		if (newType.fType == DDL_DECIMAL && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	case (CalpontSystemCatalog::INT):
		if (newType.fType == DDL_INTEGER && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		if (newType.fType == DDL_DECIMAL && colType.precision == newType.fPrecision &&
			colType.scale == newType.fScale) return true;
		break;
	case (CalpontSystemCatalog::FLOAT):
		if (newType.fType == DDL_FLOAT) return true;
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
	case (CalpontSystemCatalog::DOUBLE):
		if (newType.fType == DDL_DOUBLE) return true;
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
	DETAIL_INFO(alterTableStmt);
	int rc = 0;
	rc = fDbrm.isReadWrite();
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
	try
	{
		//check table lock
		CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(alterTableStmt.fSessionID);
		systemCatalogPtr->identity(CalpontSystemCatalog::EC);
		systemCatalogPtr->sessionID(alterTableStmt.fSessionID);
		CalpontSystemCatalog::TableName tableName;
		tableName.schema =  (alterTableStmt.fTableName)->fSchema;
		tableName.table = (alterTableStmt.fTableName)->fName;
		execplan::CalpontSystemCatalog::ROPair roPair;
		roPair = systemCatalogPtr->tableRID(tableName);

		u_int32_t  processID = 0;
		std::string  processName("DDLProc");
		int rc = 0;
		int i = 0;
		rc = fSessionManager.setTableLock(roPair.objnum, alterTableStmt.fSessionID, processID, processName, true);

		if ( rc == BRM::ERR_TABLE_LOCKED_ALREADY )
		{
			int waitPeriod = 10 * 1000;
			waitPeriod = Config::getWaitPeriod() * 1000;
			//retry until time out (microsecond)

			for (; i < waitPeriod; i+=100)
			{
				usleep(100);

				rc = fSessionManager.setTableLock(roPair.objnum, alterTableStmt.fSessionID, processID, processName, true);

				if (rc == 0)
					break;
			}

			if (i >= waitPeriod) //error out
			{
				bool  lockStatus;
				ostringstream oss;
				uint32_t sid;
				rc = fSessionManager.getTableLockInfo( roPair.objnum, processID, processName, lockStatus, sid);	
				if ( lockStatus )
				{
					oss << " table " << (alterTableStmt.fTableName)->fSchema << "." << (alterTableStmt.fTableName)->fName << " is still locked by " << processName << " with ProcessID " << processID;
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

				result.result = ALTER_ERROR;
				result.message = message;
				fSessionManager.rolledback(txnID);
				return result;
			}

		}
		else if ( rc  == BRM::ERR_FAILURE)
		{
				logging::Message::Args args;
				logging::Message message(1);
				args.add("Alter table Failed due to BRM failure");
				result.result = ALTER_ERROR;
				result.message = message;
				fSessionManager.rolledback(txnID);
				return result;
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

				addColumn (alterTableStmt.fSessionID, txnID.id, result, columnDefPtr, *(alterTableStmt.fTableName));
				if (result.result != NO_ERROR)
				{
					err = "AlterTable: add column failed";
					throw std::runtime_error(err);
				}

			}
			else if (s.find(AlterActionString[6]) != string::npos)
			{
				//Drop Column Default
				dropColumnDefault (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaDropColumnDefault*> (*action_iterator)), *(alterTableStmt.fTableName));
				if (result.result != NO_ERROR)
				{
					err = "AlterTable: drop column default failed";
					throw std::runtime_error(err);
				}
			}
			else if (s.find(AlterActionString[3]) != string::npos)
			{
				//Drop Columns
				dropColumns (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaDropColumns*> (*action_iterator)), *(alterTableStmt.fTableName));
			}
			else if (s.find(AlterActionString[2]) != string::npos)
			{
				//Drop a column
				dropColumn (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaDropColumn*> (*action_iterator)), *(alterTableStmt.fTableName));

			}

			else if (s.find(AlterActionString[4]) != string::npos)
			{
				//Add Table Constraint
				addTableConstraint (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaAddTableConstraint*> (*action_iterator)), *(alterTableStmt.fTableName));

			}

			else if (s.find(AlterActionString[5]) != string::npos)
			{
				//Set Column Default
				setColumnDefault (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaSetColumnDefault*> (*action_iterator)), *(alterTableStmt.fTableName));

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
				renameTable (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaRenameTable*> (*action_iterator)), *(alterTableStmt.fTableName));

	   		}

			else if (s.find(AlterActionString[10]) != string::npos)
			{
				//Rename a Column
				renameColumn (alterTableStmt.fSessionID, txnID.id, result, *(dynamic_cast<AtaRenameColumn*> (*action_iterator)), *(alterTableStmt.fTableName));

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
		fWriteEngine.commit(txnID.id);
		fSessionManager.committed(txnID);
		deleteLogFile();
	}
	catch (exception& ex)
	{
		rollBackAlter(ex.what(), txnID, alterTableStmt.fSessionID, result);
	}
	catch (...)
	{
		rollBackAlter("encountered unknown exception. ", txnID, alterTableStmt.fSessionID, result);
	}
	return result;

}

void AlterTableProcessor::rollBackAlter(const string& error, BRM::TxnID txnID,
										int sessionId, DDLResult& result)
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

		// special handling to alter table primary key case. if not null violation, transaction
		// already rollbacked in validation call. here we commit.
		if (result.result == PK_NOTNULL_ERROR)
		{
			fWriteEngine.commit(txnID.id);
			fSessionManager.committed(txnID);
		}
		else
		{
			fWriteEngine.rollbackTran(txnID.id, sessionId);
		if (result.result == CREATE_ERROR)
		{
			rollBackFiles(txnID);
		}
		fSessionManager.rolledback(txnID);
   	}
   	result.result = ALTER_ERROR;
		result.message = message;
}

void AlterTableProcessor::addColumn (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, ddlpackage::ColumnDef* columnDefPtr, ddlpackage::QualifiedName& inTableName)
{
	std::string err("AlterTableProcessor::addColumn ");
	SUMMARY_INFO(err);
	// Allocate an object ID for the column we are about to create
	VERBOSE_INFO("Allocating object ID for a column");
	ColumnList columns;
	getColumnsForTable(sessionID, inTableName.fSchema, inTableName.fName, columns);
	ColumnList::const_iterator column_iterator;
	column_iterator = columns.begin();
	if (inTableName.fSchema != CALPONT_SCHEMA)
	{
	try {
		execplan::ObjectIDManager fObjectIDManager;
		fStartingColOID = fObjectIDManager.allocOID();
	}
	catch (exception& ex)
	{
		result.result = ALTER_ERROR;
		err += ex.what();
		throw std::runtime_error(err);
	}
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
	CalpontSystemCatalog* systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	//@Bug 1358
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	CalpontSystemCatalog::TableName tableName;
	tableName.schema = inTableName.fSchema;
	tableName.table = inTableName.fName;
	std::vector<uint32_t> outOfSerPar;
	CalpontSystemCatalog::ROPair ropair;
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
		VERBOSE_INFO("Writing meta data to SYSCOL");
		writeSysColumnMetaData(sessionID, txnID, result, aColumnList, inTableName, totalColumns, alterFlag);
		}
		else
			cerr << "updating calpontsys...skipping writeSysColumnMetaData()" << endl;

		if ((columnDefPtr->fConstraints).size() != 0)
		{
			VERBOSE_INFO("Writing column constraint meta data to SYSCONSTRAINT");
			//writeColumnSysConstraintMetaData(sessionID, txnID, result,aColumnList, inTableName);

			VERBOSE_INFO("Writing column constraint meta data to SYSCONSTRAINTCOL");
			//writeColumnSysConstraintColMetaData(sessionID, txnID, result,aColumnList, inTableName);
		}

		if ((columnDefPtr->fType->fAutoincrement).compare("y") == 0)
		{
			//update systable autoincrement column
			//This gives us the rid in systable that we want to update
			WriteEngine::DctnryStructList dctnryStructList;
			WriteEngine::DctnryValueList dctnryValueList;
			WriteEngine::DctColTupleList dctRowList;
			WriteEngine::DctnryTuple dctColList;

			config::Config* cf = config::Config::makeConfig();
			unsigned filesPerColumnPartition = 32;
			unsigned extentsPerSegmentFile = 4;
			unsigned dbrootCnt = 2;
			unsigned extentRows = 0x800000;
			string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
			if (fpc.length() != 0)
				filesPerColumnPartition = cf->uFromText(fpc);
			string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
			if (epsf.length() != 0)
				extentsPerSegmentFile = cf->uFromText(epsf);
			string dbct = cf->getConfig("SystemConfig", "DBRootCount");
			if (dbct.length() != 0)
				dbrootCnt = cf->uFromText(dbct);

			DBRM brm;
			extentRows = brm.getExtentRows();
			unsigned dbRoot = 0;
			unsigned partition = 0;
			unsigned segment = 0;
			uint16_t startDBRoot;
			uint32_t partitionNum;

			// now we have to prepare the various structures for the WE to update the column. 
			
			std::vector<WriteEngine::RID> ridList;
			ridList.push_back(ropair.rid);
			WriteEngine::ColValueList colValuesList;
			WriteEngine::ColTupleList aColList;
			WriteEngine::ColStructList colStructs;
			std::vector<void *> colOldValuesList;
			boost::any datavalue;
			datavalue = 1;

			WriteEngine::ColTuple colTuple;

			//Build colStructs for SYSTABLE
			tableName.schema = CALPONT_SCHEMA;
			tableName.table = SYSTABLE_TABLE;
			DDLColumn column;
			findColumnData (sessionID, tableName, AUTOINC_COL, column);
			WriteEngine::ColStruct colStruct;
			WriteEngine::DctnryStruct dctnryStruct;
			colStruct.dataOid = column.oid;
			colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
			colStruct.tokenFlag = false;
		
			colStruct.colDataType = static_cast<WriteEngine::ColDataType>(column.colType.colDataType);

			colTuple.data = datavalue;
			
			dctnryStruct.dctnryOid = 0;
			dctnryStruct.columnOid = colStruct.dataOid;

			colStructs.push_back(colStruct);
			dctnryStructList.push_back(dctnryStruct);
			aColList.push_back(colTuple);
			colValuesList.push_back(aColList);
			std::vector<WriteEngine::ColStructList> colExtentsStruct;
			std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
			std::vector<WriteEngine::RIDList> ridLists;
			ridLists.push_back(ridList);

			WriteEngine::DctnryTuple dctnryTuple;
			dctColList = dctnryTuple;
			dctRowList.push_back(dctColList);
			dctnryValueList.push_back(dctRowList);

			//In this case, there's only 1 row, so only one one extent, but keep it generic...
			std::vector<extentInfo> extentsinfo;
			extentInfo aExtentinfo;

			brm.getStartExtent(column.oid, startDBRoot, partitionNum);
			convertRidToColumn(ropair.rid, dbRoot, partition, segment, filesPerColumnPartition,
				extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt);

			aExtentinfo.dbRoot = dbRoot;
			aExtentinfo.partition = partition;
			aExtentinfo.segment = segment;

			extentsinfo.push_back(aExtentinfo);

			//build colExtentsStruct
			for (unsigned i=0; i < extentsinfo.size(); i++)
			{
				for (unsigned j=0; j < colStructs.size(); j++)
				{
					colStructs[j].fColPartition = extentsinfo[i].partition;
					colStructs[j].fColSegment = extentsinfo[i].segment;
					colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
					dctnryStructList[j].fColPartition = extentsinfo[i].partition;
					dctnryStructList[j].fColSegment = extentsinfo[i].segment;
					dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
				}
				colExtentsStruct.push_back(colStructs);
				dctnryExtentsStruct.push_back(dctnryStructList);
			}

			// call the write engine to update the row
			//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
			if (NO_ERROR != fWriteEngine.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
				ridLists, dctnryExtentsStruct, dctnryValueList))
			{
				// build the logging message
				err = "WE: Update failed on: " + tableName.table;
				throw std::runtime_error(err);
			}

			ridList.clear();
			colStructs.clear();
			dctnryStructList.clear();
			aColList.clear();
			colValuesList.clear();
			ridLists.clear();
			dctRowList.clear();
			dctnryValueList.clear();
			extentsinfo.clear();
			colExtentsStruct.clear();
			dctnryExtentsStruct.clear();
			colOldValuesList.clear();
		}
		
		
		
		VERBOSE_INFO("Creating column file");
		//@Bug 4176 save oids to a log file for DDL cleanup after fail over.
		std::vector <CalpontSystemCatalog::OID> oidList;
		oidList.push_back(fStartingColOID);
		for (uint j=0; j < fDictionaryOIDList.size(); j++)
		{
			oidList.push_back(fDictionaryOIDList[j].dictOID);
		}	
		createOpenLogFile( ropair.objnum, tableName );
		writeLogFile ( tableName, oidList );
		
		uint16_t dbroot = 0;
   		uint32_t partitionNum;
		//BUG931
		//In order to fill up the new column with data an existing column is selected as reference

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

				fDbrm.getStartExtent((*column_iterator).oid, dbroot, partitionNum, true);

				std::vector<struct BRM::EMEntry> extents;
				int rc = fDbrm.getOutOfServicePartitions((*column_iterator).oid, outOfSerPar);

				if (rc != 0)
				{
					string errorMsg;
					BRM::errString(rc, errorMsg);
					ostringstream oss;
					oss << "getOutOfServicePartitions failed  due to " << errorMsg;
					throw std::runtime_error(oss.str());
				}

				createColumnFiles(txnID, result,aColumnList, dbroot, partitionNum);
				//Check column width to decide whether Dictionary files need to be created
				if ((columnDefPtr->fType->fType == CalpontSystemCatalog::CHAR && columnDefPtr->fType->fLength > 8) ||
				 (columnDefPtr->fType->fType == CalpontSystemCatalog::VARCHAR && columnDefPtr->fType->fLength > 7) ||
				 (columnDefPtr->fType->fType == CalpontSystemCatalog::VARBINARY && columnDefPtr->fType->fLength > 7))
				{
					createDictionaryFiles(txnID, result, dbroot);
				}
				long long nextVal = 0; 
				fillColumnWithDefaultVal(txnID, result, aColumnList, *column_iterator, dbroot, nextVal);
				if (nextVal != 0)
				{
					//validate next values
					bool validValue = true;
					switch (columnDefPtr->fType->fType)
					{
						case ddlpackage::DDL_BIGINT:
						{
							if (nextVal > MAX_BIGINT)
								validValue = false;
						}
						break;
						case ddlpackage::DDL_INT:
						case ddlpackage::DDL_INTEGER:
						case ddlpackage::DDL_MEDINT:
						{
							if (nextVal > MAX_INT)
								validValue = false;
						}
						break;
						case ddlpackage::DDL_SMALLINT:
						{
							if (nextVal > MAX_SMALLINT)
								validValue = false;
						}
						break;
						case ddlpackage::DDL_TINYINT:
						{
							if (nextVal > MAX_TINYINT)
								validValue = false;
						}
						break;
					}
					if (!validValue)
					{
						result.result = CREATE_ERROR;
						throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT));
					}
					std::map<uint32_t, long long> nextValMap;
					nextValMap[ropair.objnum] = nextVal;
					rc = fWriteEngine.updateNextValue (nextValMap, sessionID);
					nextValMap.clear();
					nextValMap[ropair.objnum] = nextVal;
				}
				break;
			}
			column_iterator++;
		}
		//VERBOSE_INFO("Creating index files");
		//createIndexFiles(txnID, result);
	}
	catch (exception& ex)
	{
		if (result.result != CREATE_ERROR)
			result.result = ALTER_ERROR;
		err += ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		result.result = CREATE_ERROR;
		err += "Unknown exception caught";
		throw std::runtime_error(err);
	}
	std::vector <CalpontSystemCatalog::OID> oidList;
	for (unsigned i = 0; i < outOfSerPar.size(); i++)
	{
		oidList.push_back(fStartingColOID);
		int rc = fDbrm.markPartitionForDeletion(oidList, outOfSerPar[i]);
		if (rc != 0)
		{
				string errorMsg;
				BRM::errString(rc, errorMsg);
				ostringstream oss;
				oss << "Mark partition for deletition failed  due to " << errorMsg;
				throw std::runtime_error(oss.str());
		}
	}
}

void AlterTableProcessor::rollBackFiles(BRM::TxnID txnID)
{
	fWriteEngine.dropColumn(txnID.id, fStartingColOID);
	std::string err;

	try {
		execplan::ObjectIDManager fObjectIDManager;
 		fObjectIDManager.returnOIDs(fStartingColOID, fStartingColOID);
 	}
 	catch (exception& ex)
 	{
		err += ex.what();
		throw std::runtime_error(err);
 	}
 	catch (...)
 		{}

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
			//fObjectIDManager.returnOIDs(dictOID.dictOID, dictOID.treeOID);

			++dictoid_iter;
		}
}

void AlterTableProcessor::dropColumn (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, ddlpackage::AtaDropColumn& ataDropColumn, ddlpackage::QualifiedName& fTableName)
{
	// 1. Get the OIDs for the column
	// 2. Get the OIDs for the dictionary
	// 3. Get the OIDs for the indexes
	// 4. Remove the column from SYSCOLUMN
	// 5. update systable if the dropped column is autoincrement column 
	// 6. update column position for affected columns
	// 7. Remove the index column from SYSINDEXCOL
	// 8. Remove the constraint columns from SYSCONSTRAINTCOL
	// 9. Remove the column file
	// 10. Remove the index files
	// 11. Remove the dictionary files
	// 12.Return the OIDs
	SUMMARY_INFO("AlterTableProcessor::dropColumn");
	VERBOSE_INFO("Finding object IDs for the column");
	CalpontSystemCatalog::TableColName tableColName;
	tableColName.schema = fTableName.fSchema;
	tableColName.table = fTableName.fName;
	tableColName.column = ataDropColumn.fColumnName;
	execplan::CalpontSystemCatalog::DictOIDList dictOIDList;
	CalpontSystemCatalog::TableName tableName;
	tableName.schema = fTableName.fSchema;
	tableName.table = fTableName.fName;
	CalpontSystemCatalog::ROPair tableRO;
	CalpontSystemCatalog* systemCatalogPtr =
	CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	//@Bug 1358
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	std::string err;
	CalpontSystemCatalog::OID oid;
	CalpontSystemCatalog::ColType colType;
	try
	{
		tableRO = systemCatalogPtr->tableRID(tableName);
		execplan::CalpontSystemCatalog::ROPair colRO = systemCatalogPtr->columnRID(tableColName);
		if (colRO.objnum < 0)
		{
			err = "Column not found:" + tableColName.table + "." + tableColName.column;
			throw std::runtime_error(err);
		}
		oid = systemCatalogPtr->lookupOID(tableColName);
		colType = systemCatalogPtr->colType(oid);
		int colPos = colType.colPosition;
		ddlpackage::QualifiedName columnInfo;
		columnInfo.fSchema = fTableName.fSchema;
		columnInfo.fName = ataDropColumn.fColumnName;
		columnInfo.fCatalog = fTableName.fName;

		VERBOSE_INFO("Removing the SYSCOLUM meta data");
		removeColSysColMetaData(sessionID, txnID, result, columnInfo);
		//Update SYSTABLE 
		if (colType.autoincrement)
		{
			config::Config* cf = config::Config::makeConfig();
			unsigned filesPerColumnPartition = 32;
			unsigned extentsPerSegmentFile = 4;
			unsigned dbrootCnt = 2;
			unsigned extentRows = 0x800000;
			string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
			if (fpc.length() != 0)
				filesPerColumnPartition = cf->uFromText(fpc);
			string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
			if (epsf.length() != 0)
				extentsPerSegmentFile = cf->uFromText(epsf);
			string dbct = cf->getConfig("SystemConfig", "DBRootCount");
			if (dbct.length() != 0)
				dbrootCnt = cf->uFromText(dbct);

			DBRM brm;
			extentRows = brm.getExtentRows();
			unsigned dbRoot = 0;
			unsigned partition = 0;
			unsigned segment = 0;
			uint16_t startDBRoot;
			uint32_t partitionNum;
			
			DDLColumn column;
			tableName.schema = CALPONT_SCHEMA;
			tableName.table = SYSTABLE_TABLE;
			findColumnData (sessionID, tableName, AUTOINC_COL, column); //AUTOINC_COL column
			WriteEngine::ColValueList colValuesList;
			WriteEngine::ColTupleList aColList;
			WriteEngine::ColStructList colStructs;
			std::vector<void *> colOldValuesList;
			WriteEngine::DctnryStructList dctnryStructList;
			WriteEngine::DctnryValueList dctnryValueList;
			WriteEngine::DctColTupleList dctRowList;
			WriteEngine::DctnryTuple dctColList;
			WriteEngine::ColStruct colStruct;
			WriteEngine::DctnryStruct dctnryStruct;
			WriteEngine::ColTuple colTuple;
			colStruct.dataOid = column.oid;
			colStruct.colWidth = column.colType.colWidth;
			colStruct.tokenFlag = false;
			colStruct.colDataType = static_cast<WriteEngine::ColDataType>(column.colType.colDataType);
			colTuple.data = 0;
				
			dctnryStruct.dctnryOid = 0;
			dctnryStruct.columnOid = colStruct.dataOid;
			colStructs.push_back(colStruct);
			dctnryStructList.push_back(dctnryStruct);
			aColList.push_back(colTuple);
			colValuesList.push_back(aColList);
			
			std::vector<WriteEngine::ColStructList> colExtentsStruct;
			std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
			std::vector<WriteEngine::RID> ridList;
			ridList.push_back(tableRO.rid);
			std::vector<WriteEngine::RIDList> ridLists;
			ridLists.push_back(ridList);

			WriteEngine::DctnryTuple dctnryTuple;
			dctnryTuple.isNull = true;
			dctColList = dctnryTuple;
			dctRowList.push_back(dctColList);
			dctnryValueList.push_back(dctRowList);
			std::vector<extentInfo> extentsinfo;
			extentInfo aExtentinfo;

			brm.getStartExtent(column.oid, startDBRoot, partitionNum);
			convertRidToColumn(tableRO.rid, dbRoot, partition, segment, filesPerColumnPartition,
				extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt);

			aExtentinfo.dbRoot = dbRoot;
			aExtentinfo.partition = partition;
			aExtentinfo.segment = segment;

			extentsinfo.push_back(aExtentinfo);

			//build colExtentsStruct
			for (unsigned i=0; i < extentsinfo.size(); i++)
			{
				for (unsigned j=0; j < colStructs.size(); j++)
				{
					colStructs[j].fColPartition = extentsinfo[i].partition;
					colStructs[j].fColSegment = extentsinfo[i].segment;
					colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
					dctnryStructList[j].fColPartition = extentsinfo[i].partition;
					dctnryStructList[j].fColSegment = extentsinfo[i].segment;
					dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
				}
				colExtentsStruct.push_back(colStructs);
				dctnryExtentsStruct.push_back(dctnryStructList);
			}

			// call the write engine to update the row
			//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
			if (NO_ERROR != fWriteEngine.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
				ridLists, dctnryExtentsStruct, dctnryValueList))
			{
				// build the logging message
				err = "WE: Update failed on: " + tableName.table;
				throw std::runtime_error(err);
			}

			//WE loves vectors!
			ridList.clear();
			colStructs.clear();
			dctnryStructList.clear();
			aColList.clear();
			colValuesList.clear();
			ridLists.clear();
			dctRowList.clear();
			dctnryValueList.clear();
			extentsinfo.clear();
			colExtentsStruct.clear();
			dctnryExtentsStruct.clear();
			colOldValuesList.clear();
		}
		
		VERBOSE_INFO("Updating the SYSCOLUM column position");
		WriteEngine::RIDList ridList;
		WriteEngine::ColValueList colValuesList;
		WriteEngine::ColValueList colOldValuesList;
		CalpontSystemCatalog::TableName tableName;
		tableName.table = fTableName.fName;
		tableName.schema = fTableName.fSchema;
		CalpontSystemCatalog::RIDList rids = systemCatalogPtr->columnRIDs(tableName);
		CalpontSystemCatalog::RIDList::const_iterator rid_iter = rids.begin();
		boost::any value;
		WriteEngine::ColTupleList colTuples;
		CalpontSystemCatalog::ColType columnType;
		
		while (rid_iter != rids.end())
		{
			//look up colType
			colRO = *rid_iter;
			columnType = systemCatalogPtr->colType(colRO.objnum);
			if (columnType.colPosition < colPos)
			{
				++rid_iter;
				continue;
			}
			ridList.push_back(colRO.rid);
			value = columnType.colPosition - 1;
			WriteEngine::ColTuple colTuple;
			colTuple.data = value;
			colTuples.push_back(colTuple);
			++rid_iter;
		}
		colValuesList.push_back(colTuples);

		if (colTuples.size() > 0)
		{
			updateSyscolumns(txnID, result, ridList,colValuesList, colOldValuesList);
			if (result.result != NO_ERROR)
			{
				DETAIL_INFO("updateSyscolumns failed");
				return;
			}
		}
	}
	catch (exception& ex)
	{
		err = ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "dropColumn:Unknown exception caught";
		throw std::runtime_error(err);
	}
	fWriteEngine.commit(txnID);
	CalpontSystemCatalog::RIDList tableColRidList; 
    execplan::CalpontSystemCatalog::ROPair roPair; 
	roPair.objnum = oid; 
	tableColRidList.push_back(roPair); 
	//@Bug 4176 save oids to a log file for DDL cleanup after fail over.
	std::vector <CalpontSystemCatalog::OID> oidList;
	oidList.push_back(oid);
	oidList.push_back(colType.ddn.dictOID);	
	bool fileDropped = true;
	try {
		createOpenLogFile( tableRO.objnum, tableName );
		writeLogFile ( tableName, oidList );
		
		removeColumnFiles(txnID, result, tableColRidList);

		if (colType.ddn.dictOID > 3000) //@bug 4847. need to take care varchar(8)
		{
			VERBOSE_INFO("Removing dictionary files");
			dictOIDList.push_back(colType.ddn);
			removeDictionaryFiles(txnID, result, dictOIDList);
		}
	}
	catch (...)
	{
		fileDropped = false;
	}

	//@Bug 3860
	cacheutils::dropPrimProcFdCache();
	returnOIDs(tableColRidList, dictOIDList);
	if (fileDropped)
		deleteLogFile();
}


void AlterTableProcessor::dropColumns (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, ddlpackage::AtaDropColumns& ataDropColumns, ddlpackage::QualifiedName& fTableName)
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

		dropColumn (sessionID,txnID, result,ataDropColumn,fTableName);

		if (result.result != NO_ERROR)
		{
			DETAIL_INFO("dropColumns::dropColumn failed");
			return;
		}
		col_iter++;
		}
	}
	catch (exception& ex)
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
	catch (exception& ex)
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

void AlterTableProcessor::setColumnDefault (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, ddlpackage::AtaSetColumnDefault& ataSetColumnDefault, ddlpackage::QualifiedName& fTableName)
{
	/*TODO: oracle use MODIFY, our parser use ALTER(need to verify) */
	/*Steps to set column default:
	  1. Find RID of this column in SYSCOLUMN
	  2. Find OID of column DefaultValue in SYSCOLUMN
	  3. Call Write Engine to update the new default value */
	SUMMARY_INFO("AlterTableProcessor::setColumnDefault");
	CalpontSystemCatalog* systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	//@Bug 1358
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	CalpontSystemCatalog::TableColName tableColName;
	CalpontSystemCatalog::TableName tableName;
	tableColName.schema = fTableName.fSchema;
	tableColName.table = fTableName.fName;
	tableColName.column = ataSetColumnDefault.fColumnName;
	std::string err;
	try
	{
		CalpontSystemCatalog::ROPair roPair = systemCatalogPtr->columnRID(tableColName);
		if (roPair.objnum < 0)
		{
			ostringstream oss;
			oss << "No such column: " << tableColName;
			throw std::runtime_error(oss.str().c_str());
		}
		std::vector<WriteEngine::RID> ridList;
		ridList.push_back(roPair.rid);
		WriteEngine::ColValueList colValuesList;
		WriteEngine::ColTupleList aColList;
		WriteEngine::ColStructList colStructs;
		std::vector<void *> colOldValuesList;
		WriteEngine::ColTuple colTuple;
		//Build colStructs for SYSCOLUM
		tableName.schema = CALPONT_SCHEMA;
		tableName.table = SYSCOLUMN_TABLE;
		DDLColumn column;
		findColumnData (sessionID, tableName, DEFAULTVAL_COL, column);
		WriteEngine::ColStruct colStruct;
		colStruct.dataOid = column.oid;
		colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
		colStruct.tokenFlag = false;
		colStruct.tokenFlag = column.colType.colWidth > 8 ? true : false;
		colStruct.colDataType = static_cast<WriteEngine::ColDataType>(column.colType.colDataType);

		colStruct.colDataType = (WriteEngine::ColDataType)column.colType.colDataType;
		colStructs.push_back(colStruct);
		boost::any datavalue;
		if (ataSetColumnDefault.fDefaultValue->fNull)
		{
			datavalue = getNullValueForType(column.colType);
		}
		else
		{
			datavalue = ataSetColumnDefault.fDefaultValue->fValue;
		}

		if (column.colType.colWidth > 8) //token
		{
			colTuple.data = tokenizeData(txnID, result, column.colType,datavalue);
		}
		else
		{
			colTuple.data = datavalue;
		}
		aColList.push_back(colTuple);
		colValuesList.push_back(aColList);
		WriteEngine::DctnryStructList dctnryStructList;
		WriteEngine::DctnryValueList dctnryValueList;
		std::vector<WriteEngine::ColStructList> colExtentsStruct;
		colExtentsStruct.push_back(colStructs);
		std::vector<WriteEngine::RIDList> ridLists;
		ridLists.push_back(ridList);
		std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
		dctnryExtentsStruct.push_back(dctnryStructList);

		// call the write engine to update the row
		if (NO_ERROR != fWriteEngine.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList, ridLists, dctnryExtentsStruct, dctnryValueList))
		{
			err = "WE: Update failed on: " + tableColName.table;
			throw std::runtime_error(err);
		}
	}
	catch (exception& ex)
	{
		err = ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "setColumnDefault:Unknown exception caught";
		throw std::runtime_error(err);
	}

}
void AlterTableProcessor::dropColumnDefault (u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, ddlpackage::AtaDropColumnDefault& ataDropColumnDefault, ddlpackage::QualifiedName& fTableName)
{
	/*Steps to drop column default:
	  1. Find RID of this column in SYSCOLUMN
	  2. Find OID of column DefaultValue in SYSCOLUMN
	  3. Call Write Engine to update the default value to null */
	SUMMARY_INFO("AlterTableProcessor::dropColumnDefault");
	CalpontSystemCatalog* systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	//@Bug 1358
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	CalpontSystemCatalog::TableColName tableColName;
	CalpontSystemCatalog::TableName tableName;
	tableColName.schema = fTableName.fSchema;
	tableColName.table = fTableName.fName;
	tableColName.column = ataDropColumnDefault.fColumnName;
	std::string err;
	try
	{
		CalpontSystemCatalog::ROPair roPair = systemCatalogPtr->columnRID(tableColName);
		if (roPair.objnum < 0)
		{
			ostringstream oss;
			oss << "No such column: " << tableColName;
			throw std::runtime_error(oss.str().c_str());
		}
		std::vector<WriteEngine::RID> ridList;
		ridList.push_back(roPair.rid);
		CalpontSystemCatalog::OID oid = systemCatalogPtr->lookupOID(tableColName);
		CalpontSystemCatalog::ColType colType;
		colType = systemCatalogPtr->colType(oid);
		WriteEngine::ColValueList colValuesList;
		WriteEngine::ColTupleList aColList;
		WriteEngine::ColStructList colStructs;
		std::vector<void *> colOldValuesList;
		boost::any datavalue;
		datavalue = getNullValueForType(colType);

		WriteEngine::ColTuple colTuple;
		colTuple.data = datavalue;
		aColList.push_back(colTuple);
		colValuesList.push_back(aColList);
		//Build colStructs for SYSCOLUM
		tableName.schema = CALPONT_SCHEMA;
		tableName.table = SYSCOLUMN_TABLE;
		DDLColumn column;
		findColumnData (sessionID, tableName, DEFAULTVAL_COL, column);
		WriteEngine::ColStruct colStruct;
		colStruct.dataOid = column.oid;
		colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
		colStruct.tokenFlag = false;
		colStruct.tokenFlag = column.colType.colWidth > 8 ? true : false;
		colStruct.colDataType = static_cast<WriteEngine::ColDataType>(column.colType.colDataType);

		colStruct.colDataType = (WriteEngine::ColDataType)column.colType.colDataType;
		colStructs.push_back(colStruct);
		WriteEngine::DctnryStructList dctnryStructList;
		WriteEngine::DctnryValueList dctnryValueList;
		std::vector<WriteEngine::ColStructList> colExtentsStruct;
		colExtentsStruct.push_back(colStructs);
		std::vector<WriteEngine::RIDList> ridLists;
		ridLists.push_back(ridList);
		std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
		dctnryExtentsStruct.push_back(dctnryStructList);
		// call the write engine to update the row
		if (NO_ERROR != fWriteEngine.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList, ridLists, dctnryExtentsStruct, dctnryValueList))
		{
			// build the logging message
			err = "Update failed on: " + tableColName.table;
			throw std::runtime_error(err);
		}
	}
	catch (exception& ex)
	{
		err = ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "dropColumnDefault:Unknown exception caught";
		throw std::runtime_error(err);
	}
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
	CalpontSystemCatalog* systemCatalogPtr;
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
	catch (exception& ex)
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

void AlterTableProcessor::renameTable (uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result, ddlpackage::AtaRenameTable& ataRenameTable, ddlpackage::QualifiedName& fTableName)
{
	/*Steps:
	1. Update SYSTABLE (table name)
	2. Update SYSCOLUMN (table name)
	*/
	SUMMARY_INFO("AlterTableProcessor::renameTable");
	CalpontSystemCatalog::ROPair ropair;
	CalpontSystemCatalog::RIDList roList;
	CalpontSystemCatalog::TableName tableName;
	CalpontSystemCatalog* systemCatalogPtr;
	systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	//@Bug 1358
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	VERBOSE_INFO("Updating table meta data to SYSTABLE");
	ddlpackage::QualifiedName newTableName;
	newTableName.fSchema  = ataRenameTable.fQualifiedName->fSchema;
	newTableName.fName = ataRenameTable.fQualifiedName->fName;
	CalpontSystemCatalog::TableName oldTableName;
	std::string newName= ataRenameTable.fQualifiedName->fName;
	boost::algorithm::to_lower(newName);
	std::string value;
	oldTableName.schema = fTableName.fSchema;
	oldTableName.table = fTableName.fName;
	std::string err;
	WriteEngine::DctnryStructList dctnryStructList;
	WriteEngine::DctnryValueList dctnryValueList;
	WriteEngine::DctColTupleList dctRowList;
	WriteEngine::DctnryTuple dctColList;

	config::Config* cf = config::Config::makeConfig();
	unsigned filesPerColumnPartition = 32;
	unsigned extentsPerSegmentFile = 4;
	unsigned dbrootCnt = 2;
	unsigned extentRows = 0x800000;
	string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
	if (fpc.length() != 0)
		filesPerColumnPartition = cf->uFromText(fpc);
	string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
	if (epsf.length() != 0)
		extentsPerSegmentFile = cf->uFromText(epsf);
	string dbct = cf->getConfig("SystemConfig", "DBRootCount");
	if (dbct.length() != 0)
		dbrootCnt = cf->uFromText(dbct);

	DBRM brm;
	extentRows = brm.getExtentRows();
	unsigned dbRoot = 0;
	unsigned partition = 0;
	unsigned segment = 0;
	uint16_t startDBRoot;
	uint32_t partitionNum;

	try
	{
		//This gives us the rid in systable that we want to update
		ropair = systemCatalogPtr->tableRID(oldTableName);
		if (ropair.objnum < 0)
		{
			err = "No such table: " + tableName.table;
			throw std::runtime_error(err);
		}
		// now we have to prepare the various structures for the WE to update the column. This is immensely
		//  complicated.
		std::vector<WriteEngine::RID> ridList;
		ridList.push_back(ropair.rid);
		WriteEngine::ColValueList colValuesList;
		WriteEngine::ColTupleList aColList;
		WriteEngine::ColStructList colStructs;
		std::vector<void *> colOldValuesList;
		boost::any datavalue;
		datavalue = ataRenameTable.fQualifiedName->fName;

		WriteEngine::ColTuple colTuple;

		//Build colStructs for SYSTABLE
		tableName.schema = CALPONT_SCHEMA;
		tableName.table = SYSTABLE_TABLE;
		DDLColumn column;
		findColumnData (sessionID, tableName, TABLENAME_COL, column);
		WriteEngine::ColStruct colStruct;
		WriteEngine::DctnryStruct dctnryStruct;
		colStruct.dataOid = column.oid;
		colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
		colStruct.tokenFlag = false;
		if ( (column.colType.colDataType == execplan::CalpontSystemCatalog::CHAR
				&& column.colType.colWidth > 8)
			|| (column.colType.colDataType == execplan::CalpontSystemCatalog::VARCHAR
				&& column.colType.colWidth > 7)
			|| (column.colType.colDataType == execplan::CalpontSystemCatalog::VARBINARY
				&& column.colType.colWidth > 7)
			|| (column.colType.colDataType == execplan::CalpontSystemCatalog::DECIMAL
				&& column.colType.precision > 18) )//token
		{
			colStruct.colWidth = 8;
			colStruct.tokenFlag = true;
		}
		else
		{
			colStruct.colWidth = column.colType.colWidth;
		}
		colStruct.colDataType = static_cast<WriteEngine::ColDataType>(column.colType.colDataType);

		if (colStruct.tokenFlag)
			colTuple.data = tokenizeData(txnID, result, column.colType, datavalue);
		else
			colTuple.data = datavalue;

		colStruct.colDataType = (WriteEngine::ColDataType)column.colType.colDataType;

		if (colStruct.tokenFlag)
		{
			dctnryStruct.dctnryOid = column.colType.ddn.dictOID;
			dctnryStruct.columnOid = colStruct.dataOid;
		}
		else
		{
			dctnryStruct.dctnryOid = 0;
			dctnryStruct.columnOid = colStruct.dataOid;
		}

		colStructs.push_back(colStruct);
		dctnryStructList.push_back(dctnryStruct);
		aColList.push_back(colTuple);
		colValuesList.push_back(aColList);
		std::vector<WriteEngine::ColStructList> colExtentsStruct;
		std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
		std::vector<WriteEngine::RIDList> ridLists;
		ridLists.push_back(ridList);

		WriteEngine::DctnryTuple dctnryTuple;
		memcpy(dctnryTuple.sigValue, ataRenameTable.fQualifiedName->fName.c_str(), ataRenameTable.fQualifiedName->fName.length());
		dctnryTuple.sigSize = ataRenameTable.fQualifiedName->fName.length();
		dctnryTuple.isNull = false;
		dctColList = dctnryTuple;
		dctRowList.push_back(dctColList);
		dctnryValueList.push_back(dctRowList);

		//In this case, there's only 1 row, so only one one extent, but keep it generic...
		std::vector<extentInfo> extentsinfo;
		extentInfo aExtentinfo;

		brm.getStartExtent(column.oid, startDBRoot, partitionNum);
		convertRidToColumn(ropair.rid, dbRoot, partition, segment, filesPerColumnPartition,
			extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt);

		aExtentinfo.dbRoot = dbRoot;
		aExtentinfo.partition = partition;
		aExtentinfo.segment = segment;

		extentsinfo.push_back(aExtentinfo);

		//build colExtentsStruct
		for (unsigned i=0; i < extentsinfo.size(); i++)
		{
			for (unsigned j=0; j < colStructs.size(); j++)
			{
				colStructs[j].fColPartition = extentsinfo[i].partition;
				colStructs[j].fColSegment = extentsinfo[i].segment;
				colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
				dctnryStructList[j].fColPartition = extentsinfo[i].partition;
				dctnryStructList[j].fColSegment = extentsinfo[i].segment;
				dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
			}
			colExtentsStruct.push_back(colStructs);
			dctnryExtentsStruct.push_back(dctnryStructList);
		}

		// call the write engine to update the row
		//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
		if (NO_ERROR != fWriteEngine.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
			ridLists, dctnryExtentsStruct, dctnryValueList))
		{
			// build the logging message
			err = "WE: Update failed on: " + tableName.table;
			throw std::runtime_error(err);
		}

		//WE loves vectors!
		ridList.clear();
		colStructs.clear();
		dctnryStructList.clear();
		aColList.clear();
		colValuesList.clear();
		ridLists.clear();
		dctRowList.clear();
		dctnryValueList.clear();
		extentsinfo.clear();
		colExtentsStruct.clear();
		dctnryExtentsStruct.clear();
		colOldValuesList.clear();

		//This gives us the rids in syscolumn that we want to update
		roList = systemCatalogPtr->columnRIDs(oldTableName);
		// now we have to prepare the various structures for the WE to update the column. This is immensely
		//  complicated.
		for (unsigned int i = 0; i < roList.size(); i++)
		{
			ridList.push_back(roList[i].rid);
		}

		//Build colStructs for SYSTABLE
		tableName.schema = CALPONT_SCHEMA;
		tableName.table = SYSCOLUMN_TABLE;
		findColumnData(sessionID, tableName, TABLENAME_COL, column);
		colStruct.dataOid = column.oid;
		colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
		colStruct.tokenFlag = false;
		if ( (column.colType.colDataType == execplan::CalpontSystemCatalog::CHAR
				&& column.colType.colWidth > 8)
			|| (column.colType.colDataType == execplan::CalpontSystemCatalog::VARCHAR
				&& column.colType.colWidth > 7)
			|| (column.colType.colDataType == execplan::CalpontSystemCatalog::VARBINARY
				&& column.colType.colWidth > 7)
			|| (column.colType.colDataType == execplan::CalpontSystemCatalog::DECIMAL
				&& column.colType.precision > 18) )//token
		{
			colStruct.colWidth = 8;
			colStruct.tokenFlag = true;
		}
		else
		{
			colStruct.colWidth = column.colType.colWidth;
		}
		colStruct.colDataType = static_cast<WriteEngine::ColDataType>(column.colType.colDataType);

		if (colStruct.tokenFlag)
			colTuple.data = tokenizeData(txnID, result, column.colType, datavalue);
		else
			colTuple.data = datavalue;

		colStruct.colDataType = (WriteEngine::ColDataType)column.colType.colDataType;

		if (colStruct.tokenFlag)
		{
			dctnryStruct.dctnryOid = column.colType.ddn.dictOID;
			dctnryStruct.columnOid = colStruct.dataOid;
		}
		else
		{
			dctnryStruct.dctnryOid = 0;
			dctnryStruct.columnOid = colStruct.dataOid;
		}

		colStructs.push_back(colStruct);
		dctnryStructList.push_back(dctnryStruct);
		for (unsigned int i = 0; i < roList.size(); i++)
		{
			aColList.push_back(colTuple);
		}
		colValuesList.push_back(aColList);
		ridLists.push_back(ridList);

		//It's the same string for each column, so we just need one dictionary struct
		memset(&dctnryTuple, 0, sizeof(dctnryTuple));
		memcpy(dctnryTuple.sigValue, ataRenameTable.fQualifiedName->fName.c_str(), ataRenameTable.fQualifiedName->fName.length());
		dctnryTuple.sigSize = ataRenameTable.fQualifiedName->fName.length();
		dctnryTuple.isNull = false;
		dctColList = dctnryTuple;
		dctRowList.push_back(dctColList);
		dctnryValueList.push_back(dctRowList);

		//We need a list of unique extents, and we need to keep them in order. We look at the previous entry
		// in the vector (if any) and, if this one is different, push it.
		// TODO: is this right?

		brm.getStartExtent(column.oid, startDBRoot, partitionNum);
		for (unsigned int i = 0; i < roList.size(); i++)
		{
			convertRidToColumn(roList[i].rid, dbRoot, partition, segment, filesPerColumnPartition,
				extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt);

			aExtentinfo.dbRoot = dbRoot;
			aExtentinfo.partition = partition;
			aExtentinfo.segment = segment;

			if (extentsinfo.empty())
				extentsinfo.push_back(aExtentinfo);
			else if (extentsinfo.back() != aExtentinfo)
				extentsinfo.push_back(aExtentinfo);
		}

		//build colExtentsStruct
		for (unsigned i=0; i < extentsinfo.size(); i++)
		{
			for (unsigned j=0; j < colStructs.size(); j++)
			{
				colStructs[j].fColPartition = extentsinfo[i].partition;
				colStructs[j].fColSegment = extentsinfo[i].segment;
				colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
				dctnryStructList[j].fColPartition = extentsinfo[i].partition;
				dctnryStructList[j].fColSegment = extentsinfo[i].segment;
				dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
			}
			colExtentsStruct.push_back(colStructs);
			dctnryExtentsStruct.push_back(dctnryStructList);
		}

		// call the write engine to update the row
		//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
		if (NO_ERROR != fWriteEngine.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
			ridLists, dctnryExtentsStruct, dctnryValueList))
		{
			// build the logging message
			err = "WE: Update failed on: " + tableName.table;
			throw std::runtime_error(err);
		}

		ridList.clear();
		colStructs.clear();
		dctnryStructList.clear();
		aColList.clear();
		colValuesList.clear();
		ridLists.clear();
		dctRowList.clear();
		dctnryValueList.clear();
		extentsinfo.clear();
		colExtentsStruct.clear();
		dctnryExtentsStruct.clear();

	}
	catch (exception& ex)
	{
		err = ex.what();
		throw std::runtime_error(err);
	}
	catch (...)
	{
		err = "renameTable:Unknown exception caught";
		throw std::runtime_error(err);
	}
}

void AlterTableProcessor::renameColumn(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID,
	DDLResult& result, ddlpackage::AtaRenameColumn& ataRenameColumn, ddlpackage::QualifiedName& fTableName)
{
	/*Steps:
	1. Update SYSCOLUMN for name change
	2. Update SYSTABLE if column is autoincrement column
	*/
	SUMMARY_INFO("AlterTableProcessor::renameColumn");

	config::Config* cf = config::Config::makeConfig();
	unsigned filesPerColumnPartition = 32;
	unsigned extentsPerSegmentFile = 4;
	unsigned dbrootCnt = 2;
	unsigned extentRows = 0x800000;
	string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
	if (fpc.length() != 0)
		filesPerColumnPartition = cf->uFromText(fpc);
	string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
	if (epsf.length() != 0)
		extentsPerSegmentFile = cf->uFromText(epsf);
	string dbct = cf->getConfig("SystemConfig", "DBRootCount");
	if (dbct.length() != 0)
		dbrootCnt = cf->uFromText(dbct);

	DBRM brm;
	extentRows = brm.getExtentRows();
	unsigned dbRoot = 0;
	unsigned partition = 0;
	unsigned segment = 0;
	uint16_t startDBRoot;
	uint32_t partitionNum;

	CalpontSystemCatalog::TableName tableName;
	CalpontSystemCatalog::TableColName tableColName;
	tableColName.schema = fTableName.fSchema;
	tableColName.table = fTableName.fName;
	tableColName.column = ataRenameColumn.fName;
	CalpontSystemCatalog::ROPair ropair;

	CalpontSystemCatalog* systemCatalogPtr;
	systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
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
		CalpontSystemCatalog::RID ridTable = ropair.rid;
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
		if (colType.compressionType != (*ataRenameColumn.fNewType).fCompressiontype)
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
			tableName.schema = CALPONT_SCHEMA;
			tableName.table = SYSTABLE_TABLE;
			DDLColumn column;
			findColumnData (sessionID, tableName, AUTOINC_COL, column); //AUTOINC_COL column
			WriteEngine::ColValueList colValuesList;
			WriteEngine::ColTupleList aColList;
			WriteEngine::ColStructList colStructs;
			std::vector<void *> colOldValuesList;
			WriteEngine::DctnryStructList dctnryStructList;
			WriteEngine::DctnryValueList dctnryValueList;
			WriteEngine::DctColTupleList dctRowList;
			WriteEngine::DctnryTuple dctColList;
			WriteEngine::ColStruct colStruct;
			WriteEngine::DctnryStruct dctnryStruct;
			WriteEngine::ColTuple colTuple;
			colStruct.dataOid = column.oid;
			colStruct.colWidth = column.colType.colWidth;
			colStruct.tokenFlag = false;
			colStruct.colDataType = static_cast<WriteEngine::ColDataType>(column.colType.colDataType);

			if (ataRenameColumn.fNewType->fAutoincrement.compare("y") == 0)
				colTuple.data = 1;
			else
				colTuple.data = 0;
				
			dctnryStruct.dctnryOid = 0;
			dctnryStruct.columnOid = colStruct.dataOid;
			colStructs.push_back(colStruct);
			dctnryStructList.push_back(dctnryStruct);
			aColList.push_back(colTuple);
			colValuesList.push_back(aColList);
			
			std::vector<WriteEngine::ColStructList> colExtentsStruct;
			std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
			std::vector<WriteEngine::RID> ridList;
			ridList.push_back(ridTable);
			std::vector<WriteEngine::RIDList> ridLists;
			ridLists.push_back(ridList);

			WriteEngine::DctnryTuple dctnryTuple;
			dctnryTuple.isNull = true;
			dctColList = dctnryTuple;
			dctRowList.push_back(dctColList);
			dctnryValueList.push_back(dctRowList);
			std::vector<extentInfo> extentsinfo;
			extentInfo aExtentinfo;

			brm.getStartExtent(column.oid, startDBRoot, partitionNum);
			convertRidToColumn(ridTable, dbRoot, partition, segment, filesPerColumnPartition,
				extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt);

			aExtentinfo.dbRoot = dbRoot;
			aExtentinfo.partition = partition;
			aExtentinfo.segment = segment;

			extentsinfo.push_back(aExtentinfo);

			//build colExtentsStruct
			for (unsigned i=0; i < extentsinfo.size(); i++)
			{
				for (unsigned j=0; j < colStructs.size(); j++)
				{
					colStructs[j].fColPartition = extentsinfo[i].partition;
					colStructs[j].fColSegment = extentsinfo[i].segment;
					colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
					dctnryStructList[j].fColPartition = extentsinfo[i].partition;
					dctnryStructList[j].fColSegment = extentsinfo[i].segment;
					dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
				}
				colExtentsStruct.push_back(colStructs);
				dctnryExtentsStruct.push_back(dctnryStructList);
			}

			// call the write engine to update the row
			//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
			if (NO_ERROR != fWriteEngine.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
				ridLists, dctnryExtentsStruct, dctnryValueList))
			{
				// build the logging message
				err = "WE: Update failed on: " + tableName.table;
				throw std::runtime_error(err);
			}

			//WE loves vectors!
			ridList.clear();
			colStructs.clear();
			dctnryStructList.clear();
			aColList.clear();
			colValuesList.clear();
			ridLists.clear();
			dctRowList.clear();
			dctnryValueList.clear();
			extentsinfo.clear();
			colExtentsStruct.clear();
			dctnryExtentsStruct.clear();
			colOldValuesList.clear();			
		}			
		
		// now we have to prepare the various structures for the WE to update the column. This is immensely
		//  complicated.
		std::vector<WriteEngine::RID> ridList;
		ridList.push_back(ropair.rid);
		WriteEngine::ColValueList colValuesList;
		WriteEngine::ColTupleList aColList;
		WriteEngine::ColStructList colStructs;
		std::vector<void *> colOldValuesList;
		WriteEngine::DctnryStructList dctnryStructList;
		WriteEngine::DctnryValueList dctnryValueList;
		WriteEngine::DctColTupleList dctRowList;
		WriteEngine::DctnryTuple dctColList;
		boost::any datavalue;
		datavalue = ataRenameColumn.fNewName;

		WriteEngine::ColTuple colTuple;

		//Build colStructs for SYSCOLUMN
		tableName.schema = CALPONT_SCHEMA;
		tableName.table = SYSCOLUMN_TABLE;
		DDLColumn column;
		findColumnData (sessionID, tableName, COLNAME_COL, column); //COLNAME_COL column
		DDLColumn column1;
		findColumnData (sessionID, tableName, AUTOINC_COL, column1); //AUTOINC_COL column
		DDLColumn column2;
		findColumnData (sessionID, tableName, NEXTVALUE_COL, column2); //NEXTVALUE_COL column
		WriteEngine::ColStruct colStruct;
		WriteEngine::DctnryStruct dctnryStruct;
		colStruct.dataOid = column.oid;
		colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
		colStruct.tokenFlag = false;
		if ( (column.colType.colDataType == execplan::CalpontSystemCatalog::CHAR
				&& column.colType.colWidth > 8)
			|| (column.colType.colDataType == execplan::CalpontSystemCatalog::VARCHAR
				&& column.colType.colWidth > 7)
			|| (column.colType.colDataType == execplan::CalpontSystemCatalog::VARBINARY
				&& column.colType.colWidth > 7)
			|| (column.colType.colDataType == execplan::CalpontSystemCatalog::DECIMAL
				&& column.colType.precision > 18) )//token
		{
			colStruct.colWidth = 8;
			colStruct.tokenFlag = true;
		}
		else
		{
			colStruct.colWidth = column.colType.colWidth;
		}

		colStruct.colDataType = static_cast<WriteEngine::ColDataType>(column.colType.colDataType);

		if (colStruct.tokenFlag)
			colTuple.data = tokenizeData(txnID, result, column.colType, datavalue);
		else
			colTuple.data = datavalue;

		colStruct.colDataType = (WriteEngine::ColDataType)column.colType.colDataType;

		if (colStruct.tokenFlag)
		{
			dctnryStruct.dctnryOid = column.colType.ddn.dictOID;
			dctnryStruct.columnOid = colStruct.dataOid;
		}
		else
		{
			dctnryStruct.dctnryOid = 0;
			dctnryStruct.columnOid = colStruct.dataOid;
		}

		colStructs.push_back(colStruct);
		dctnryStructList.push_back(dctnryStruct);
		aColList.push_back(colTuple);
		colValuesList.push_back(aColList);
		//Build AUTOINC_COL structure
		WriteEngine::ColTupleList aColList1;
		colStruct.dataOid = column1.oid;
		colStruct.colWidth = column1.colType.colWidth > 8 ? 8 : column1.colType.colWidth;
		colStruct.tokenFlag = false;
		colStruct.colDataType = static_cast<WriteEngine::ColDataType>(column1.colType.colDataType);

		colTuple.data = ataRenameColumn.fNewType->fAutoincrement;

		colStruct.colDataType = (WriteEngine::ColDataType)column1.colType.colDataType;

		dctnryStruct.dctnryOid = 0;
		dctnryStruct.columnOid = colStruct.dataOid;

		colStructs.push_back(colStruct);
		dctnryStructList.push_back(dctnryStruct);
		aColList1.push_back(colTuple);
		colValuesList.push_back(aColList1);
		
		//Build NEXTVALUE_COL structure
		WriteEngine::ColTupleList aColList2;
		colStruct.dataOid = column2.oid;
		colStruct.colWidth = column2.colType.colWidth > 8 ? 8 : column2.colType.colWidth;
		colStruct.tokenFlag = false;
		colStruct.colDataType = static_cast<WriteEngine::ColDataType>(column2.colType.colDataType);

		colTuple.data = ataRenameColumn.fNewType->fNextvalue;

		colStruct.colDataType = (WriteEngine::ColDataType)column2.colType.colDataType;

		dctnryStruct.dctnryOid = 0;
		dctnryStruct.columnOid = colStruct.dataOid;

		colStructs.push_back(colStruct);
		dctnryStructList.push_back(dctnryStruct);
		aColList2.push_back(colTuple);
		colValuesList.push_back(aColList2);
		
		std::vector<WriteEngine::ColStructList> colExtentsStruct;
		std::vector<WriteEngine::DctnryStructList> dctnryExtentsStruct;
		std::vector<WriteEngine::RIDList> ridLists;
		ridLists.push_back(ridList);

		WriteEngine::DctnryTuple dctnryTuple;
		boost::to_lower(ataRenameColumn.fNewName);
		memcpy(dctnryTuple.sigValue, ataRenameColumn.fNewName.c_str(), ataRenameColumn.fNewName.length());
		dctnryTuple.sigSize = ataRenameColumn.fNewName.length();
		dctnryTuple.isNull = false;
		dctColList = dctnryTuple;
		dctRowList.push_back(dctColList);
		dctnryValueList.push_back(dctRowList);

		//In this case, there's only 1 row, so only one one extent, but keep it generic...
		std::vector<extentInfo> extentsinfo;
		extentInfo aExtentinfo;

		brm.getStartExtent(column.oid, startDBRoot, partitionNum);
		convertRidToColumn(ropair.rid, dbRoot, partition, segment, filesPerColumnPartition,
			extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt);

		aExtentinfo.dbRoot = dbRoot;
		aExtentinfo.partition = partition;
		aExtentinfo.segment = segment;

		extentsinfo.push_back(aExtentinfo);

		//build colExtentsStruct
		for (unsigned i=0; i < extentsinfo.size(); i++)
		{
			for (unsigned j=0; j < colStructs.size(); j++)
			{
				colStructs[j].fColPartition = extentsinfo[i].partition;
				colStructs[j].fColSegment = extentsinfo[i].segment;
				colStructs[j].fColDbRoot = extentsinfo[i].dbRoot;
				dctnryStructList[j].fColPartition = extentsinfo[i].partition;
				dctnryStructList[j].fColSegment = extentsinfo[i].segment;
				dctnryStructList[j].fColDbRoot = extentsinfo[i].dbRoot;
			}
			colExtentsStruct.push_back(colStructs);
			dctnryExtentsStruct.push_back(dctnryStructList);
		}

		// call the write engine to update the row
		//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
		if (NO_ERROR != fWriteEngine.updateColumnRec(txnID, colExtentsStruct, colValuesList, colOldValuesList,
			ridLists, dctnryExtentsStruct, dctnryValueList))
		{
			// build the logging message
			err = "WE: Update failed on: " + tableName.table;
			throw std::runtime_error(err);
		}

			//WE loves vectors!
			ridList.clear();
			colStructs.clear();
			dctnryStructList.clear();
			aColList.clear();
			colValuesList.clear();
			ridLists.clear();
			dctRowList.clear();
			dctnryValueList.clear();
			extentsinfo.clear();
			colExtentsStruct.clear();
			dctnryExtentsStruct.clear();
			colOldValuesList.clear();

	}
	catch (exception& ex)
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

