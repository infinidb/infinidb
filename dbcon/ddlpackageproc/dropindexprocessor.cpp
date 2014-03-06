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
 *   $Id: dropindexprocessor.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/

#include "dropindexprocessor.h"
#include "messagelog.h"
#include "sqllogger.h"

using namespace execplan;
using namespace logging;

namespace ddlpackageprocessor
{
DropIndexProcessor::DDLResult DropIndexProcessor::processPackage(ddlpackage::DropIndexStatement& dropIndexStmt)
{
    SUMMARY_INFO("DropIndexProcessor::processPackage");

    boost::shared_ptr<CalpontSystemCatalog> sysCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog( dropIndexStmt.fSessionID );
    CalpontSystemCatalog::IndexName indexName;
    CalpontSystemCatalog::IndexOID indexOID;

    BRM::TxnID txnID;
	txnID.id= fTxnid.id;
	txnID.valid= fTxnid.valid;
	
    DDLResult result;
    result.result = NO_ERROR;

    int err = 0;

    VERBOSE_INFO(dropIndexStmt);

	SQLLogger logger(dropIndexStmt.fSql, fDDLLoggingId, dropIndexStmt.fSessionID, txnID.id);

	indexName.schema = dropIndexStmt.fIndexName->fSchema;
    indexName.index  = dropIndexStmt.fIndexName->fName;
    //Look up table name from indexname. Oracle will error out if same constraintname or indexname exists.
    CalpontSystemCatalog::TableName tableName = sysCatalogPtr->lookupTableForIndex (dropIndexStmt.fIndexName->fName, dropIndexStmt.fIndexName->fSchema );
    indexName.table = tableName.table;
    indexOID = sysCatalogPtr->lookupIndexNbr(indexName);
    
    VERBOSE_INFO("Removing the SYSINDEX meta data");
    removeSysIndexMetaData(dropIndexStmt.fSessionID, txnID.id, result, *dropIndexStmt.fIndexName);
    if (result.result != NO_ERROR)
    {
        DETAIL_INFO("writeSysIndexMetaData failed");
        goto rollback;
    }

    VERBOSE_INFO("Removing the SYSINDEXCOL meta data");
    removeSysIndexColMetaData(dropIndexStmt.fSessionID, txnID.id, result, *dropIndexStmt.fIndexName);
    if (result.result != NO_ERROR)
    {
        DETAIL_INFO("writeSysIndexMetaData failed");
        goto rollback;
    }


    VERBOSE_INFO("Removing the index files");
    err = fWriteEngine.dropIndex(txnID.id, indexOID.objnum,indexOID.listOID);
    if (err)
    {
        DETAIL_INFO("WriteEngine dropIndex failed");
        goto rollback;
    }

    // Log the DDL statement
    logging::logDDL(dropIndexStmt.fSessionID, txnID.id, dropIndexStmt.fSql, dropIndexStmt.fOwner);

    // register the changes
    err = fWriteEngine.commit( txnID.id );
    if (err)
    {
        DETAIL_INFO("Failed to commit the drop index transaction");
        goto rollback;
    }
    fSessionManager.committed(txnID);
    //fObjectIDManager.returnOID(indexOID.objnum);
    //fObjectIDManager.returnOID(indexOID.listOID);
    return result;

rollback:
    fWriteEngine.rollbackTran(txnID.id, dropIndexStmt.fSessionID);
    fSessionManager.rolledback(txnID);
    return result;
}



}                                                 // namespace ddlpackageprocessor
