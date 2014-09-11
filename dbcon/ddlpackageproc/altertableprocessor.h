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
 *   $Id: altertableprocessor.h 7409 2011-02-08 14:38:50Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */
#ifndef ALTERTABLEPROCESSOR_H
#define ALTERTABLEPROCESSOR_H

#include "ddlpackageprocessor.h"

#if defined(_MSC_VER) && defined(ALTERTABLEPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace ddlpackageprocessor
{
    /** @brief specialization of a DDLPackageProcessor
     * for interacting with the Write Engine
     * to process alter table ddl statements.
     */
    class AlterTableProcessor : public DDLPackageProcessor
    {
        public:
            /** @brief process an alter table statement
             *
             * @param alterTableStmt the AlterTableStatement
             */
            EXPORT DDLResult processPackage(ddlpackage::AlterTableStatement& alterTableStmt);
            /** @brief add a physical column file
             *
             * @param result the result of the operation
             * @param addColumn the AtaAddColumn object
             * @param fTableName the QualifiedName of the table
             */
            EXPORT void addColumn(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                ddlpackage::ColumnDef* columnDefPtr,
                ddlpackage::QualifiedName& fTableName);

            /** @brief drop a column
             *
             * @param result the result of the operation
             * @param ataDropColumn the AtaDropColumn object
             * @param fTableName the QualifiedName for the table
             */
            EXPORT void dropColumn(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                ddlpackage::AtaDropColumn& ataDropColumn,
                ddlpackage::QualifiedName& fTableName );

            /** @brief drop columns
             *
             * @param result the result of the operation
             * @param ataDropColumns the AtaDropColumn object
             * @param fTableName the QualifiedName for the table
             */
            EXPORT void dropColumns(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                ddlpackage::AtaDropColumns& ataDropColumns,
                ddlpackage::QualifiedName& fTableName );

            /** @brief add table constraint
             *
             * @param result the result of the operation
             * @param ataAddTableConstraint the AtaDropColumn object
             * @param fTableName the QualifiedName for the table
             */
            EXPORT void addTableConstraint(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                ddlpackage::AtaAddTableConstraint& ataAddTableConstraint,
                ddlpackage::QualifiedName& fTableName );

            /** @brief set column default
             *
             * @param result the result of the operation
             * @param ataSetColumnDefault the AtaSetColumnDefault object
             * @param fTableName the QualifiedName for the table
             */
            EXPORT void setColumnDefault(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                ddlpackage::AtaSetColumnDefault& ataSetColumnDefault,
                ddlpackage::QualifiedName& fTableName );

            /** @brief drop column default
             *
             * @param result the result of the operation
             * @param ataDropColumnDefault the AtaDropColumnDefault object
             * @param fTableName the QualifiedName for the table
             */
            EXPORT void dropColumnDefault(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                ddlpackage::AtaDropColumnDefault& ataDropColumnDefault,
                ddlpackage::QualifiedName& fTableName );

            /** @brief drop table constraint
             *
             * @param result the result of the operation
             * @param ataDropTableConstraint the AtaDropTableConstraint object
             * @param fTableName the QualifiedName for the table
             */
            EXPORT void dropTableConstraint(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                ddlpackage::AtaDropTableConstraint& ataDropTableConstraint,
                ddlpackage::QualifiedName& fTableName );
            /** @brief rename a table
             *
             * @param result the result of the operation
             * @param ataRenameTable the AtaRenameTable object
             * @param fTableName the QualifiedName for the table
             */
            EXPORT void renameTable(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID,
                DDLResult& result, ddlpackage::AtaRenameTable& ataRenameTable,
                ddlpackage::QualifiedName& fTableName );

            /** @brief rename a column
             *
             * @param result the result of the operation
             * @param ataRenameColumn the AtaRenameColumn object
             * @param fTableName the QualifiedName for the table
             */
            EXPORT void renameColumn(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                ddlpackage::AtaRenameColumn& ataRenameColumn,
                ddlpackage::QualifiedName& fTableName );

        protected:
	    void rollBackFiles(BRM::TxnID txnID);
	    void rollBackAlter(const std::string& error, BRM::TxnID txnID, int sessionId, DDLResult& result);

        private:

    };

}                                                 //namespace ddlpackageprocessor

#undef EXPORT

#endif                                            //ALTERTABLEPROCESSOR_H
