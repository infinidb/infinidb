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
 *   $Id: deletedmlpackage.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
#include <stdexcept>
#include <iostream>
#include <boost/tokenizer.hpp>
#include <string>
#include <cstdlib>
using namespace std;
#define DELETEDMLPKG_DLLEXPORT
#include "deletedmlpackage.h"
#undef DELETEDMLPKG_DLLEXPORT

namespace dmlpackage
{

DeleteDMLPackage::DeleteDMLPackage()
{}

DeleteDMLPackage::DeleteDMLPackage(std::string schemaName, std::string tableName,
                                   std::string dmlStatement, int sessionID)
        :CalpontDMLPackage( schemaName, tableName, dmlStatement, sessionID )
{}

DeleteDMLPackage::~DeleteDMLPackage()
{}

int DeleteDMLPackage::write(messageqcpp::ByteStream& bytestream)
{
    int retval = 1;

    messageqcpp::ByteStream::byte package_type = DML_DELETE;
    bytestream << package_type;

    messageqcpp::ByteStream::quadbyte session_id = fSessionID;
    bytestream << session_id;

 /*   if(fPlan != 0)
        fHasFilter = true;
    else
        fHasFilter = false;
*/
    messageqcpp::ByteStream::quadbyte hasFilter = fHasFilter;
    bytestream << hasFilter;

    bytestream << fUuid;

    bytestream << fDMLStatement;
    bytestream << fSQLStatement;
    bytestream << fSchemaName;
    if (fTable != 0)
    {
        retval = fTable->write(bytestream);
    }
    if(fHasFilter)
    {
        bytestream += *(fPlan.get());
    }

    return retval;
}
/**
 *
 */
int DeleteDMLPackage::read(messageqcpp::ByteStream& bytestream)
{
    int retval = 1;

    messageqcpp::ByteStream::quadbyte session_id;
    messageqcpp::ByteStream::quadbyte hasFilter;

    bytestream >> session_id;
    fSessionID = session_id;

    bytestream >> hasFilter;
    fHasFilter = (hasFilter != 0);

    bytestream >> fUuid;

    std::string dmlStatement;
    bytestream >> fDMLStatement;
    bytestream >> fSQLStatement;
    bytestream >> fSchemaName;

    fTable = new DMLTable();
    retval = fTable->read(bytestream);
    if(fHasFilter)
    {
         fPlan.reset(new messageqcpp::ByteStream(bytestream));
    }

    return retval;
}

int DeleteDMLPackage::buildFromSqlStatement(SqlStatement& sqlStatement)
{
    int retval = 1;

    DeleteSqlStatement& deleteStmt = dynamic_cast<DeleteSqlStatement&>(sqlStatement);

    initializeTable();

    if (0 != deleteStmt.fWhereClausePtr)
    {
        fHasFilter = true;
        fQueryString = deleteStmt.getQueryString();
    }
    // else all rows are deleted

    return retval;
}

/**
 *
 */
int DeleteDMLPackage::buildFromBuffer(std::string& buffer, int columns, int rows)
{
#ifdef DML_PACKAGE_DEBUG
    //cout << "The data buffer received: " << buffer << endl;
#endif

    int retval = 1;

    initializeTable();

    std::vector<std::string> dataList;
    typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
    boost::char_separator<char> sep(":");
    tokenizer tokens(buffer, sep);
    for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
    {
        dataList.push_back(StripLeadingWhitespace(*tok_iter));

    }

    int n = 0;
    for (int i=0; i < rows; i++)
    {
        //get a new row
        Row aRow;
        //Row *aRowPtr = new Row();
        //std::string colName;
        //std::string colValue;
        //get row ID from the buffer
        std::string rowid  = dataList[n++];
        aRow.set_RowID(atoll(rowid.c_str()));
        //aRowPtr->set_RowID(atol(rowid.c_str()));
#ifdef DML_PACKAGE_DEBUG

        //cout << "The row ID is " << rowid << endl;
#endif
        /*
                for (int j = 0; j < columns; j++)
                {
                    //Build a column list
                    colName = dataList[n++];
                    colValue = dataList[n++];
        #ifdef DML_PACKAGE_DEBUG
                    cout << "The column data: " << colName << " " << colValue << endl;
        #endif
                    DMLColumn* aColumn = new DMLColumn(colName, colValue);
                    (aRowPtr->get_ColumnList()).push_back(aColumn);
        }
        //build a row list for a table
        fTable->get_RowList().push_back(aRowPtr);
        */
    }

    return retval;

}

int DeleteDMLPackage::buildFromMysqlBuffer(ColNameList& colNameList, TableValuesMap& tableValuesMap, int columns, int rows )
{
    int retval = 1;

    initializeTable();
    //The row already built from MySql parser.
/*  Row *aRowPtr = new Row();
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
    fTable->get_RowList().push_back(aRowPtr); */
    return retval;
}

}                                                 // namespace dmlpackage
