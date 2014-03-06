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
*   $Id: dmltable.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/

#include "dmltable.h"
using namespace std;

namespace dmlpackage
{
DMLTable::DMLTable()
{}

DMLTable::~DMLTable()
{
    
    try
    {
        RowList::iterator it = fRows.begin();
        while(it != fRows.end())
        {
            delete *it;
            it++;
        }
    }
    catch(...)
    {
        cout << "failed to delete the table rows" << endl;
    }
    
}

int DMLTable::read(messageqcpp::ByteStream& bytestream)
{
    int retval = 1;

    // read the table name
    bytestream >> fName;

    // read the schema name
    bytestream >> fSchema;

    messageqcpp::ByteStream::quadbyte rowNum;
    bytestream >> rowNum;

    for (unsigned int i = 0; i < rowNum; i++)
    {
        Row* aRow = new Row();
        retval = aRow->read(bytestream);
        fRows.push_back(aRow);
    }
    return retval;
}

int DMLTable::write(messageqcpp::ByteStream& bytestream)
{
    int retval = 1;
    //write table name and schma name to the bytestream
    bytestream << fName;
    bytestream << fSchema;
    messageqcpp::ByteStream::quadbyte rowNum;
    rowNum = fRows.size();
    bytestream << rowNum;
    //write the row list
    RowList::iterator rowListPtr;
    rowListPtr = fRows.begin();
    for (; rowListPtr != fRows.end(); ++rowListPtr)
    {
        retval = (*rowListPtr)->write(bytestream);
    }

    return retval;
}

} // namespace dmlpackage

