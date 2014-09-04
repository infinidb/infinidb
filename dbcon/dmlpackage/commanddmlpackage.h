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
 *   $Id: commanddmlpackage.h 8436 2012-04-04 18:18:21Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef COMMANDDMLPACKAGE_H
#define COMMANDDMLPACKAGE_H
#include <string>
#include "calpontdmlpackage.h"
#include "bytestream.h"

#if defined(_MSC_VER) && defined(xxxCOMMANDDMLPKG_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackage
{
    /** @brief concrete implementation of a CalpontDMLPackage
     * Specifically for representing COMMAND DML Statements
     */
    class CommandDMLPackage : public CalpontDMLPackage
    {

        public:
            /** @brief ctor
             */
            EXPORT CommandDMLPackage();

            /** @brief ctor
             */
            EXPORT CommandDMLPackage( std::string dmlStatement, int sessionID );

            /** @brief dtor
             */
            EXPORT virtual ~CommandDMLPackage();

            /** @brief write a CommandDMLPackage to a ByteStream
             *
             *  @param bytestream the ByteStream to write to
             */
            EXPORT int write(messageqcpp::ByteStream& bytestream);

            /** @brief read CommandDMLPackage from bytestream
             *
             * @param bytestream the ByteStream to read from
             */
            EXPORT int read(messageqcpp::ByteStream& bytestream);
            /** @brief do nothing
             *
             * @param buffer
             * @param columns the number of columns in the buffer
             * @param rows the number of rows in the buffer
             */
            inline int buildFromBuffer(std::string& buffer, int columns=0, int rows=0)
            {
                return 1;
            };

            /** @brief build a CommandDMLPackage from a CommandSqlStatement
             */
            EXPORT int buildFromSqlStatement(SqlStatement& sqlStatement);
			
			/** @brief build a InsertDMLPackage from MySQL buffer
			*
			* @param colNameList, tableValuesMap
			* @param rows the number of rows in the buffer
			*/
			int buildFromMysqlBuffer(ColNameList& colNameList, TableValuesMap& tableValuesMap, int columns, int rows)
			{
				return 1;
			};

        protected:

        private:

    };

}

#undef EXPORT

#endif                                            //COMMANDDMLPACKAGE_H
