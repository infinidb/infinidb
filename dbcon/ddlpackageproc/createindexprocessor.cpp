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
 *   $Id: createindexprocessor.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
#include <iostream>
using namespace std;
#include "createindexprocessor.h"
#include "ddlindexpopulator.h"
#include "we_define.h"
#include "messagelog.h"
#include "sqllogger.h"

#include <boost/algorithm/string/case_conv.hpp>
using namespace boost::algorithm;

#include <boost/date_time/gregorian/gregorian.hpp>
using namespace boost::gregorian;

using namespace execplan;
using namespace ddlpackage;
using namespace logging;
using namespace BRM;
namespace ddlpackageprocessor
{

CreateIndexProcessor::DDLResult CreateIndexProcessor::processPackage(ddlpackage::CreateIndexStatement& createIndexStmt)
{
    /*
      get OIDs for the list & tree files
      commit the current transaction
      start a new transaction
      create the index in the metadata
      create the index on the WE
      end the transaction
     */
    SUMMARY_INFO("CreateIndexProcesssor::processPackage");

    DDLResult result;
    result.result = NO_ERROR;

    DETAIL_INFO(createIndexStmt);

    BRM::TxnID txnID;
	txnID.id= fTxnid.id;
	txnID.valid= fTxnid.valid;
    /*Check whether the table exists already. If not, it is assumed from primary key creating.
    This is based on the assumption that Front end is already error out if the user trys to
    create index on non-existing table. */
    CalpontSystemCatalog::TableName tableName;
    tableName.schema = (createIndexStmt.fTableName)->fSchema;
    tableName.table = (createIndexStmt.fTableName)->fName;
    CalpontSystemCatalog::ROPair roPair;
    boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog( createIndexStmt.fSessionID );
    try {
    	roPair = systemCatalogPtr->tableRID( tableName );
    }
    catch (exception& ex)
    {
    	// store primary key name in fPKName
    	fPKName = createIndexStmt.fIndexName->fName;
        return result;

    }
    catch (...)
    {
        return result;
    }
    if ( roPair.objnum < 3000 )
    {
    	return result;
    }
    fPKName = createIndexStmt.fIndexName->fName;
    int err = 0;


    SQLLogger logger(createIndexStmt.fSql, fDDLLoggingId, createIndexStmt.fSessionID, txnID.id);

    VERBOSE_INFO("Allocating object IDs for columns");

//    int oidbase = fObjectIDManager.allocOIDs(2);
//    fIdxOID.listOID = oidbase;
//    fIdxOID.treeOID = ++oidbase;


    VERBOSE_INFO("Starting a new transaction");

    ddlpackage::DDL_CONSTRAINTS type = createIndexStmt.fUnique ?  ddlpackage::DDL_UNIQUE : ddlpackage::DDL_INVALID_CONSTRAINT;

    VERBOSE_INFO("Writing meta data to SYSINDEX");
    bool multicol = false;
    if ( createIndexStmt.fColumnNames.size() > 1 )
    {
    	multicol = true;
    }
    //validate index columns
    CalpontSystemCatalog::TableColName tableColName;
    tableColName.schema = (createIndexStmt.fTableName)->fSchema;
    tableColName.table = (createIndexStmt.fTableName)->fName;
    CalpontSystemCatalog::OID oid;
    CalpontSystemCatalog::ColType colType;
    ColumnNameList::const_iterator colIter;
    int totalWidth = 0;
    DDLIndexPopulator pop(&fWriteEngine, &fSessionManager, createIndexStmt.fSessionID, txnID.id, result,
		          fIdxOID, createIndexStmt.fColumnNames, *createIndexStmt.fTableName, 
			  type, getDebugLevel());
    if ( multicol)
    {
    	for ( colIter = createIndexStmt.fColumnNames.begin(); colIter != createIndexStmt.fColumnNames.end(); colIter++)
    	{
    		tableColName.column = *colIter;
    
    		roPair = systemCatalogPtr->columnRID( tableColName );
    		oid = systemCatalogPtr->lookupOID( tableColName );
    		colType = systemCatalogPtr->colType (oid );
    		totalWidth += (pop.isDictionaryType(colType)) ? 8 : colType.colWidth;
	}
	if ( totalWidth > 32 )
	{
    	    stringstream ss;
    	    ss << totalWidth;			
	    	DETAIL_INFO("Total indexed column width greater than 32: " + ss.str());
	    	logging::Message::Args args;
            logging::Message message(9);
            args.add("Error creating index: ");
            args.add("Total indexed column width");
            args.add("greater than 32. ");
            message.format( args );

            result.result = CREATE_ERROR;
            result.message = message;
            return result;
	}
     }
	
try
{
	//writeSysIndexMetaData(createIndexStmt.fSessionID, txnID.id, result, *createIndexStmt.fTableName, type, createIndexStmt.fIndexName->fName, multicol);
 
    //fIdxOID values are set in writeSysIndexMetaData.
    pop.setIdxOID(fIdxOID);

    VERBOSE_INFO("Writing meta data to SYSINDEXCOL");
    //writeSysIndexColMetaData(createIndexStmt.fSessionID, txnID.id, result,*createIndexStmt.fTableName, createIndexStmt.fColumnNames, createIndexStmt.fIndexName->fName );
    
    if (createIndexStmt.fUnique)
    {
    	VERBOSE_INFO("Writing column constraint meta data to SYSCONSTRAINT");
    	WriteEngine::ColStruct colStruct;
    	WriteEngine::ColTuple colTuple;
    	WriteEngine::ColStructList colStructs;
    	WriteEngine::ColTupleList colTuples;
    	WriteEngine::ColValueList colValuesList;
    	WriteEngine::RIDList ridList;

    	DDLColumn column;

    	CalpontSystemCatalog::TableName sysConsTableName;
    	sysConsTableName.schema = CALPONT_SCHEMA;
    	sysConsTableName.table = SYSCONSTRAINT_TABLE;

    	bool isNull = false;
    	int error = 0;

    	// get the columns for the SYSCONSTRAINT table
    	ColumnList sysConsColumns;
    	ColumnList::const_iterator sysCons_iterator;
    	getColumnsForTable(createIndexStmt.fSessionID, sysConsTableName.schema,sysConsTableName.table, sysConsColumns);
	sysCons_iterator = sysConsColumns.begin();
	    std::string idxData;
        while ( sysCons_iterator != sysConsColumns.end() )
        {
		column = *sysCons_iterator;
                boost::algorithm::to_lower(column.tableColName.column);
                isNull = false;
                if (CONSTRAINTNAME_COL == column.tableColName.column)
                {
                	idxData = createIndexStmt.fIndexName->fName;
                	boost::algorithm::to_lower(idxData);
                    colTuple.data = idxData;
                }
                else if (SCHEMA_COL == column.tableColName.column)
                {                  
                    idxData = (createIndexStmt.fTableName)->fSchema;
                	boost::algorithm::to_lower(idxData);
                    colTuple.data = idxData;
                }
                else if (TABLENAME_COL == column.tableColName.column)
                {
                    idxData = (createIndexStmt.fTableName)->fName;
                	boost::algorithm::to_lower(idxData);
                    colTuple.data = idxData;
                }
                else if (CONSTRAINTTYPE_COL == column.tableColName.column)
                {
                    std::string consType;
		    char constraint_type = getConstraintCode(type);
                    consType += constraint_type;
                    colTuple.data = consType;
                }
                else if (CONSTRAINTPRIM_COL == column.tableColName.column)
                {
                    colTuple.data = getNullValueForType(column.colType);
                    isNull = true;
                }
                else if (CONSTRAINTTEXT_COL == column.tableColName.column)
                {
                        colTuple.data = getNullValueForType(column.colType);
                        isNull = true;
                }
                else if (INDEXNAME_COL == column.tableColName.column)
                {
                        idxData = createIndexStmt.fIndexName->fName;
                		boost::algorithm::to_lower(idxData);
                    	colTuple.data = idxData;
                }
                else
                {
                    colTuple.data = getNullValueForType(column.colType);
                    isNull = true;
                }

                colStruct.dataOid = column.oid;
		
                colStruct.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
                colStruct.tokenFlag = false;
                colStruct.tokenFlag = column.colType.colWidth > 8 ? true : false;
                colStruct.colDataType = column.colType.colDataType;

                if (column.colType.colWidth > 8 && !isNull)
                {
                    colTuple.data = tokenizeData(txnID.id, result, column.colType, colTuple.data);
                }
                colStructs.push_back( colStruct );
		
                colTuples.push_back( colTuple );

                colValuesList.push_back( colTuples );

                colTuples.pop_back();
                ++sysCons_iterator;
            }

    	    if (colStructs.size() != 0)
            {
		//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
    		//error = fWriteEngine.insertColumnRec( txnID.id, colStructs, colValuesList, ridList );
    		if ( error != WriteEngine::NO_ERROR )
    		{

				return rollBackCreateIndex(errorString( "WE: Error inserting Column Record: ", error), txnID, createIndexStmt.fSessionID);
//         		logging::Message::Args args;
//         		logging::Message message(9);
//         		args.add("Error updating: ");
//         		args.add("calpont.sysconstraint");
//         		args.add("error number: ");
//         		args.add( error );
//         		message.format( args );
// 
//         		result.result = CREATE_ERROR;
//         		result.message = message;
    		}
    		else
    		{
        		result.result = NO_ERROR;
    		}
            }	    
 
    	VERBOSE_INFO("Writing column constraint meta data to SYSCONSTRAINTCOL");
   	WriteEngine::ColStruct colStructCol;
    	WriteEngine::ColTuple colTupleCol;
    	WriteEngine::ColStructList colStructsCol;
    	WriteEngine::ColTupleList colTuplesCol;
    	WriteEngine::ColValueList colValuesListCol;
	CalpontSystemCatalog::TableName sysConsColTableName;
    	sysConsColTableName.schema = CALPONT_SCHEMA;
    	sysConsColTableName.table = SYSCONSTRAINTCOL_TABLE;
	colValuesList.clear();
	colTuples.clear();
    	isNull = false;
    	error = 0;
    	// get the columns for the SYSCONSTRAINTCOL table
    	ColumnList sysConsColColumns;
    	ColumnList::const_iterator sysConsCol_iterator;
    	getColumnsForTable(createIndexStmt.fSessionID, sysConsColTableName.schema,sysConsColTableName.table, sysConsColColumns);
        // write sysconstraintcol
        sysConsCol_iterator = sysConsColColumns.begin();
        std::string colData;
        while ( sysConsCol_iterator != sysConsColColumns.end() )
        {
                column = *sysConsCol_iterator;
                boost::algorithm::to_lower(column.tableColName.column);

                isNull = false;

                if (SCHEMA_COL == column.tableColName.column)
                {
                    colData = (createIndexStmt.fTableName)->fSchema;
                    boost::algorithm::to_lower(colData);
                    colTupleCol.data = colData; 
                }
                else if (TABLENAME_COL == column.tableColName.column)
                {
                	colData = (createIndexStmt.fTableName)->fName;
                    boost::algorithm::to_lower(colData);
                    colTupleCol.data = colData; 
                }
                else if (COLNAME_COL == column.tableColName.column)
                {
                	colData = createIndexStmt.fColumnNames[0];
                	boost::algorithm::to_lower(colData);
                    colTupleCol.data = colData;
                    
                }
                else if (CONSTRAINTNAME_COL == column.tableColName.column)
                {
                    colData = createIndexStmt.fIndexName->fName;
                    boost::algorithm::to_lower(colData);
                    colTupleCol.data = colData;
                }
                else
                {
                    colTupleCol.data = getNullValueForType(column.colType);
                    isNull = true;
                }

                colStructCol.dataOid = column.oid;
                colStructCol.colWidth = column.colType.colWidth > 8 ? 8 : column.colType.colWidth;
                colStructCol.tokenFlag = false;
                colStructCol.tokenFlag = column.colType.colWidth > 8 ? true : false;
                colStructCol.colDataType = column.colType.colDataType;

                if (column.colType.colWidth > 8 && !isNull)
                {
                    colTupleCol.data = tokenizeData(txnID.id, result, column.colType, colTupleCol.data);
                }
		
                colStructsCol.push_back( colStructCol );
		
                colTuplesCol.push_back( colTupleCol );

                colValuesListCol.push_back( colTuplesCol );

                colTuplesCol.pop_back();

                ++sysConsCol_iterator;
            }
 
    	    if (colStructsCol.size() != 0)
            {
		//fWriteEngine.setDebugLevel(WriteEngine::DEBUG_3);
    			//error = fWriteEngine.insertColumnRec( txnID.id, colStructsCol, colValuesListCol, ridList );
    			if ( error != WriteEngine::NO_ERROR )
    			{
					return rollBackCreateIndex(errorString( "WE: Error inserting Column Record: ", error), txnID, createIndexStmt.fSessionID);
	
/*       				logging::Message::Args args;
        			logging::Message message(9);
        			args.add("Error updating: ");
        			args.add("calpont.sysconstraintcol");
        			args.add("error number: ");
        			args.add( error );
        			message.format( args );

        			result.result = CREATE_ERROR;
        			result.message = message;*/
			}			
    			else
    			{
        			result.result = NO_ERROR;
    			}
    	    }
    }
    
    VERBOSE_INFO("Creating index files");
    err = fWriteEngine.createIndex( txnID.id, fIdxOID.treeOID, fIdxOID.listOID );
    if (err)
    {
		return rollBackCreateIndex(errorString("Write engine failed to create the new index. ", err), txnID, createIndexStmt.fSessionID);
    }
	// new if BULK_LOAD close 
    err = pop.populateIndex(result); 
    if ( err )
    {
		return rollBackCreateIndex(errorString("Failed to populate index with current data. ", err), txnID, createIndexStmt.fSessionID);
    }


    // Log the DDL statement.
    logging::logDDL(createIndexStmt.fSessionID, txnID.id, createIndexStmt.fSql, createIndexStmt.fOwner);

    DETAIL_INFO("Commiting transaction");
    err = fWriteEngine.commit( txnID.id );
    if (err)
    {
		return rollBackCreateIndex(errorString("Failed to commit the create index transaction. ", err), txnID, createIndexStmt.fSessionID);
    }

    fSessionManager.committed(txnID);
// original if BULK_LOAD close	}
} // try

    catch (exception& ex)
    {
		result = rollBackCreateIndex(ex.what(), txnID, createIndexStmt.fSessionID);
    }
    catch (...)
    {
		string msg("CreateIndexProcessor::processPackage: caught unknown exception!");
		result = rollBackCreateIndex(msg, txnID, createIndexStmt.fSessionID);
    }

    return result;
}

string  CreateIndexProcessor::errorString(const string& msg, int error)
{
	WriteEngine::WErrorCodes ec;
	return string(msg + ec.errorString(error));
}


CreateIndexProcessor::DDLResult CreateIndexProcessor::rollBackCreateIndex(const string& error, BRM::TxnID& txnID, int sessionId)
{
        cerr << "CreatetableProcessor::processPackage: " << error << endl;
	DETAIL_INFO(error);
        logging::Message::Args args;
        logging::Message message(1);
        args.add("Create Index Failed: ");
        args.add( error );
        args.add("");
        args.add("");
        message.format( args );
	DDLResult result;
    result.result = CREATE_ERROR;
    result.message = message;
  	rollBackIndex(txnID, sessionId);
	return result;
}

void CreateIndexProcessor::rollBackIndex(BRM::TxnID& txnID, int sessionId)
{
	fWriteEngine.rollbackTran(txnID.id, sessionId);
	fWriteEngine.dropIndex(txnID.id,fIdxOID.listOID, fIdxOID.treeOID); 
	try {
		//execplan::ObjectIDManager fObjectIDManager;
		//fObjectIDManager.returnOIDs(fIdxOID.treeOID, fIdxOID.listOID);
	}
	catch ( exception& ex )
	{
		
	}
	catch (... )
	{ }
	fSessionManager.rolledback(txnID);
}


}
