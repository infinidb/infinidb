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
 *   $Id: insertdmlpackage.cpp 7517 2011-03-07 22:02:48Z chao $
 *
 *
 ***********************************************************************/
#include <stdexcept>
#include <iostream>
#include <boost/tokenizer.hpp>
#include <string>
using namespace std;
#define INSERTDMLPKG_DLLEXPORT
#include "insertdmlpackage.h"
#undef INSERTDMLPKG_DLLEXPORT

#ifdef _MSC_VER
#define strcasecmp stricmp
#endif

namespace dmlpackage
{

InsertDMLPackage::InsertDMLPackage()
{}

InsertDMLPackage::InsertDMLPackage( std::string schemaName, std::string tableName,
                                    std::string dmlStatement, int sessionID )
        :CalpontDMLPackage( schemaName, tableName, dmlStatement, sessionID )
{}

InsertDMLPackage::~InsertDMLPackage()
{}

int InsertDMLPackage::write(messageqcpp::ByteStream& bytestream)
{
    int retval = 1;

    messageqcpp::ByteStream::byte package_type = DML_INSERT;
    bytestream << package_type;

    messageqcpp::ByteStream::quadbyte session_id = fSessionID;
    bytestream << session_id;

    bytestream << fDMLStatement;
    bytestream << fDMLStatement;
    bytestream << fSchemaName;
	bytestream << (uint8_t)fLogging;
	bytestream << (uint8_t)fLogending;
    if (fTable != 0)
    {
        retval = fTable->write(bytestream);
    }
	bytestream << static_cast<const messageqcpp::ByteStream::doublebyte>(fIsInsertSelect);

    return retval;
}

int InsertDMLPackage::read(messageqcpp::ByteStream& bytestream)
{
    int retval = 1;

    messageqcpp::ByteStream::quadbyte session_id;
    bytestream >> session_id;
    fSessionID = session_id;

    std::string dmlStatement;
    bytestream >> fDMLStatement;
    bytestream >> fSQLStatement; 
    bytestream >> fSchemaName;
	uint8_t logging;
	bytestream >> logging;
	fLogging = (logging != 0);
	uint8_t logending;
	bytestream >> logending;
	fLogending = (logending != 0);
	
    fTable = new DMLTable();
    retval = fTable->read(bytestream);
    bytestream >> reinterpret_cast< messageqcpp::ByteStream::doublebyte&>(fIsInsertSelect);	
    return retval;
}

int InsertDMLPackage::buildFromBuffer(std::string& buffer, int columns, int rows)
{
#ifdef DML_PACKAGE_DEBUG
   // cout << "The data buffer received: " << buffer << endl;
#endif
    int retval = 1;

    initializeTable();

    std::vector<std::string> dataList;
    typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
    boost::char_separator<char> sep(",");
    tokenizer tokens(buffer, sep);
    for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
    {
        dataList.push_back(StripLeadingWhitespace(*tok_iter));

    }

    int n = 0;
    for (int i=0; i < rows; i++)
    {
        //get a new row
        Row *aRowPtr = new Row();
        std::string colName;
        std::string colValue;
        for (int j = 0; j < columns; j++)
        {
            //Build a column list
            colName = dataList[n];
            n++;
            colValue = dataList[n];
            n++;
#ifdef DML_PACKAGE_DEBUG
            //cout << "The column data: " << colName << " " << colValue << endl;
#endif
            DMLColumn* aColumn = new DMLColumn(colName, colValue, false);
            (aRowPtr->get_ColumnList()).push_back(aColumn);
        }
        //build a row list for a table
        fTable->get_RowList().push_back(aRowPtr);
    }

    return retval;
}

int InsertDMLPackage::buildFromMysqlBuffer(ColNameList& colNameList, TableValuesMap& tableValuesMap, int columns, int rows )
{
	int retval = 1;

    initializeTable();
	Row *aRowPtr = new Row();
	std::string colName;
	std::vector<std::string> colValList;
	for (int j = 0; j < columns; j++)
    {
      //Build a column list
      colName = colNameList[j];
      
      colValList = tableValuesMap[j];

      DMLColumn* aColumn = new DMLColumn(colName, colValList, false);
      (aRowPtr->get_ColumnList()).push_back(aColumn);
    }
	//build a row list for a table
    fTable->get_RowList().push_back(aRowPtr);
	aRowPtr = NULL;
	delete aRowPtr;
	return retval;
}

int InsertDMLPackage::buildFromSqlStatement(SqlStatement& sqlStatement)
{

    int retval = 1;

    InsertSqlStatement& insertStmt = dynamic_cast<InsertSqlStatement&>(sqlStatement);

    if (!insertStmt.fValuesOrQueryPtr)
        throw runtime_error("insertStmt.fValuesOrQueryPtr == NULL");

    initializeTable();
	bool isNULL = false;
    // only if we don't have a select statement
    if (0 == insertStmt.fValuesOrQueryPtr->fQuerySpecPtr)
    {

        ColumnNameList columnNameList = insertStmt.fColumnList;

        if (columnNameList.size())
        {

            ValuesList valuesList = insertStmt.fValuesOrQueryPtr->fValuesList;
            if (columnNameList.size() != valuesList.size())
            {
                throw logic_error("Column names and values count mismatch!");
            }
            Row* aRow = new Row();
            for (unsigned int i = 0; i < columnNameList.size(); i++)
            {
                DMLColumn *aColumn = new DMLColumn(columnNameList[i],valuesList[i], isNULL);
                (aRow->get_ColumnList()).push_back(aColumn);
            }
            fTable->get_RowList().push_back(aRow);

        }
        else
        {
            ValuesList valuesList = insertStmt.fValuesOrQueryPtr->fValuesList;
            ValuesList::const_iterator iter = valuesList.begin();
            Row* aRow = new Row();
            std::string colName = "";
            std::string colValue;
            while (iter != valuesList.end())
            {
                colValue = *iter;
                if ( strcasecmp(colValue.c_str(), "NULL") == 0)
            	{
            		isNULL = true;
            	}
            	else
            	{
            		isNULL = false;
            	}
                DMLColumn *aColumn = new DMLColumn(colName,colValue, isNULL);
                (aRow->get_ColumnList()).push_back(aColumn);

                ++iter;
            }
            fTable->get_RowList().push_back(aRow);
        }

    }
    else
    {
        fHasFilter = true;
        fQueryString = insertStmt.getQueryString();
    }

    return retval;
}

}  // namespace dmlpackage
