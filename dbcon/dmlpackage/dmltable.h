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
*   $Id: dmltable.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef DMLTABLE_H
#define DMLTABLE_H

#include <string>
#include <vector>
#include "dmlobject.h"
#include "bytestream.h"
#include "row.h"

namespace dmlpackage
{
/** @brief concrete implementation of a DMLObject
  * Specifically for representing a database table
  */
class DMLTable : public DMLObject
{

public:

    /** @brief ctor
      */
    DMLTable();

    /** @brief dtor
      */
    ~DMLTable();

    /** @brief get the schema name
      */
    inline const std::string get_SchemaName() const { return fSchema; }

    /** @brief set the schema name
      */
    inline void set_SchemaName( std::string& sName ) { fSchema = sName; }

    /** @brief get the table name
      */
    inline const std::string get_TableName() const { return fName; }

    /** @brief set the table name
      */
    inline void set_TableName( std::string& tName ) { fName = tName; }

    /** @brief get the row list
      */
    inline RowList& get_RowList() { return fRows; }

    /** @brief read a DMLTable from a ByteStream
      *
      * @param bytestream the ByteStream to read from
      */
    int read(messageqcpp::ByteStream& bytestream);


    /** @brief write a DMLTable to a ByteStream
      *
      * @param bytestream the ByteStream to write to
      */
    int write(messageqcpp::ByteStream& bytestream);


protected:

private:
    std::string fName;
    RowList fRows;
    std::string fSchema;
    /*Copy and copy assignment constructor */
    DMLTable(const DMLTable&);
    DMLTable& operator=(const DMLTable&);
};

}  // namespace dmlpackage

#endif //DMLTABLE_H

