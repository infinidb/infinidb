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
 *   $Id: insertdmlpackage.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef INSERTDMLPACKAGE_H
#define INSERTDMLPACKAGE_H
#include <string>
#include "calpontdmlpackage.h"
#include "bytestream.h"

#if defined(_MSC_VER) && defined(xxxINSERTDMLPKG_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackage
{
/** @brief concrete implementation of a CalpontDMLPackage
  * Specifically for representing INSERT DML Statements
  */
class InsertDMLPackage : public CalpontDMLPackage
{

public:
    /** @brief ctor
      */
    EXPORT InsertDMLPackage();

    /** @brief ctor
      *
      * @param schemaName the schema of the table being operated on
      * @param tableName the table name of the table being operated on
      * @param dmlStatement the dml statement
      * @param sessionID the session id
      */
    EXPORT InsertDMLPackage(std::string schemaName, std::string tableName,
                     std::string dmlStatement, int sessionID );

    /** @brief dtor
      */
    EXPORT virtual ~InsertDMLPackage();

    /** @brief write a InsertDMLPackage to a ByteStream
      *
      *  @param bytestream the ByteStream to write to
      */
    EXPORT int write(messageqcpp::ByteStream& bytestream);

    /** @brief read InsertDMLPackage from bytestream
      *
      * @param bytestream the ByteStream to read from
      */
    EXPORT int read(messageqcpp::ByteStream& bytestream);

    /** @brief build a InsertDMLPackage from a string buffer
      *
      * @param buffer
      * @param columns the number of columns in the buffer
      * @param rows the number of rows in the buffer
      */
    EXPORT int buildFromBuffer(std::string& buffer, int columns, int rows);
	
	/** @brief build a InsertDMLPackage from MySQL buffer
      *
	  * @param tableValuesMap  the value list for each column in the table
	  * @param colNameList the column name for each column
	  * @param columns number of columns in the table
	  * @param rows  number of rows to be touched
      */
    EXPORT int buildFromMysqlBuffer(ColNameList& colNameList, TableValuesMap& tableValuesMap, int columns, int rows);

    /** @brief build a InsertDMLPackage from a InsertSqlStatement
      *
      * @param sqlStmt the InsertSqlStatement
      */
    EXPORT int buildFromSqlStatement(SqlStatement& sqlStatement);

    /** @brief Dump the InsertDMLPackage for debugging purposes
      */
    EXPORT void Dump();

protected:

private:

};

}

#undef EXPORT

#endif                                            //INSERTDMLPACKAGE_H
