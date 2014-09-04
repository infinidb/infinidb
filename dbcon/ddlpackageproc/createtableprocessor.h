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
 *   $Id: createtableprocessor.h 8436 2012-04-04 18:18:21Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */
#ifndef CREATETABLEPROCESSOR_H
#define CREATETABLEPROCESSOR_H

#include "ddlpackageprocessor.h"

#if defined(_MSC_VER) && defined(DDLPKGCREATETABLEPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace ddlpackageprocessor
{

/** @brief specialization of a DDLPackageProcessor
 * for interacting with the Write Engine
 * to process create table ddl statements.
 */
class CreateTableProcessor : public DDLPackageProcessor
{
public:

    /** @brief process a create table statement
     *
     * @param createTableStmt the CreateTableStatement
     */
    EXPORT DDLResult processPackage(ddlpackage::CreateTableStatement& createTableStmt);

protected:
	void rollBackCreateTable(const std::string& error, BRM::TxnID txnID, int sessionId, ddlpackage::TableDef& tableDef, DDLResult& result);

private:

};

}                                                 // namespace ddlpackageprocessor

#undef EXPORT

#endif                                            // CREATETABLEPROCESSOR_H
