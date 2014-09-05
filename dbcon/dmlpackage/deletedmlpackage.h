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
 *   $Id: deletedmlpackage.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef DELETEDMLPACKAGE_H
#define DELETEDMLPACKAGE_H
#include <string>
#include "calpontdmlpackage.h"
#include "bytestream.h"

#if defined(_MSC_VER) && defined(xxxDELETEDMLPKG_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackage
{
    /** @brief concrete implementation of a CalpontDMLPackage
     * Specifically for representing DELETE DML Statements
     */
    class DeleteDMLPackage : public CalpontDMLPackage
    {

        public:

            /** @brief ctor
             */
            EXPORT DeleteDMLPackage();

            /** @brief ctor
             *
             * @param schemaName the schema of the table being operated on
             * @param tableName the name of the table being operated on
             * @param dmlStatement the dml statement
             * @param sessionID the session ID
             */
            EXPORT DeleteDMLPackage( std::string schemaName, std::string tableName,
                std::string dmlStatement, int sessionID );

            /** @brief dtor
             */
            EXPORT virtual ~DeleteDMLPackage();

            /** @brief write a DeleteDMLPackage to a ByteStream
             *
             * @param bytestream the ByteStream to write to
             */
            EXPORT int write(messageqcpp::ByteStream& bytestream);

            /** @brief read a DeleteDMLPackage from a ByteStream
             *
             * @param bytestream the ByteStream to read from
             */
            EXPORT int read(messageqcpp::ByteStream& bytestream);

            /** @brief build a DeleteDMLPackage from a string buffer
             *
             * @param buffer [rowId, columnName, colValue]
             * @param columns the number of columns in the buffer
             * @param rows the number of rows in the buffer
             */
            EXPORT int buildFromBuffer(std::string& buffer, int columns, int rows);

            /** @brief build a DeleteDMLPackage from a parsed DeleteSqlStatement
             *
             * @param sqlStatement the parsed DeleteSqlStatement
             */
            EXPORT int buildFromSqlStatement(SqlStatement& sqlStatement);
			/** @brief build a InsertDMLPackage from MySQL buffer
			*
			* @param colNameList, tableValuesMap
			* @param rows the number of rows in the buffer
			*/
			EXPORT int buildFromMysqlBuffer(ColNameList& colNameList, TableValuesMap& tableValuesMap, int columns, int rows);

        protected:

        private:

    };
}

#undef EXPORT

#endif                                            //DELETEDMLPACKAGE_H
