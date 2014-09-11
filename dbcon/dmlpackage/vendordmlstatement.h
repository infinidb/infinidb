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
 *   $Id: vendordmlstatement.h 7517 2011-03-07 22:02:48Z chao $
 *
 *
 ***********************************************************************/
/** @file */
#ifndef VENDORDMLSTATEMENT_H
#define VENDORDMLSTATEMENT_H
#include <string>
#include <vector>
#include <map>
#include <stdint.h>

#if defined(_MSC_VER) && defined(xxxVENDORDMLSTATEMENT_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif
namespace dmlpackage
{
	typedef std::vector<std::string> ColValuesList;
	typedef std::vector<std::string> ColNameList;
	typedef std::map<uint32_t, ColValuesList> TableValuesMap;

    /** @brief describes the general interface
     *  and implementation of a Vendor DML Statement
     */
    class VendorDMLStatement
    {

        public:
            /** @brief ctor
             */
            EXPORT VendorDMLStatement(std::string dmlstatement, int sessionID);
			
			/** @brief ctor
             */
            EXPORT VendorDMLStatement(std::string dmlstatement, int stmttype, int sessionID);

            /** @brief old ctor!
             */
            EXPORT VendorDMLStatement(std::string dmlstatement, int stmttype, std::string tName,
                std::string schema, int rows, int columns, std::string buf,
                int sessionID);
				
			/** @brief ctor for mysql
             */
			EXPORT VendorDMLStatement(std::string dmlstatement, int stmttype, std::string tName, std::string schema, int rows, int columns, 
				ColNameList& colNameList, TableValuesMap& tableValuesMap, int sessionID);
				
            /** @brief destructor
             */
            EXPORT ~VendorDMLStatement();

            /** @brief Get the table name
             */
            inline std::string get_TableName() const { return fTableName; }

            /** @brief Set the table name
             */
            inline void set_TableName( std::string value ) { fTableName = value; }

            /** @brief Get the schema name
             */
            inline std::string get_SchemaName() const { return fSchema; }

            /** @brief Set the schema name
             */
            inline void set_SchemaName( std::string value ) { fSchema = value; }

            /** @brief Get the DML statVendorDMLStatement classement type
             */
            inline int get_DMLStatementType() const { return fDMLStatementType; }

            /** @brief Set the DML statement type
             */
            inline void set_DMLStatementType( int value ) { fDMLStatementType = value; }

            /** @brief Get the DML statement
             */
            inline const std::string get_DMLStatement() const  { return fDMLStatement; }

            /** @brief Set the DML statVendorDMLStatement classement
             */
            inline void set_DMLStatement( std::string dmlStatement ) { fDMLStatement = dmlStatement; }

            /** @brief Get the number of rows
             */
            inline int get_Rows() const { return fRows; }

            /** @brief Set the number of rows
             */
            inline void set_Rows( int value ) { fRows = value; }

            /** @brief Get the number of columns
             */
            inline int get_Columns() const { return fColumns; }

            /** @brief Set the number of columns
             */
            inline void set_Columns( int value ) { fColumns = value; }

            /** @brief Get the data buffer
             */
            inline std::string& get_DataBuffer() { return fDataBuffer; }

            /** @brief Set the data buffer
             */
            inline void set_DataBuffer( std::string value ) { fDataBuffer= value; }
            /** @brief Get the session ID
             */
            inline int get_SessionID() { return fSessionID; }

            /** @brief Set the session ID
             */
            inline void set_SessionID( int value ) { fSessionID = value; }

			inline ColNameList& get_ColNames() { return fColNameList; }
			inline TableValuesMap& get_values() { return fTableValuesMap; }
			/** @brief get the logging flag 
             */			
			inline const bool get_Logging() const { return fLogging; }
			
			/** @brief set the logging flag 
             *
             * @param logging the logging flag to set
             */
			inline void set_Logging( bool logging )
			{
				fLogging = logging;
			}

			/** @brief get the logging flag 
             */			
			inline const bool get_Logending() const { return fLogending; }
			
			/** @brief set the logending flag 
             *
             * @param logending the logending flag to set
             */
			inline void set_Logending( bool logending )
			{
				fLogending = logending;
			}
			
        protected:

        private:
            std::string fDMLStatement;
            int fDMLStatementType;
            std::string fTableName;
            std::string fSchema;
            int fRows;
            int fColumns;
            std::string fDataBuffer;
			ColNameList fColNameList;
			TableValuesMap fTableValuesMap;
            int fSessionID;
			bool fLogging;
			bool fLogending;

    };

}

#undef EXPORT

#endif                                            //VENDORDMLSTATEMENT_H
