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
*   $Id: row.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef ROW_H
#define ROW_H
#include <string>
#include "dmlobject.h"
#include "bytestream.h"
#include "dmlcolumn.h"
#include "we_typeext.h"

#if defined(_MSC_VER) && defined(xxxDMLPKGROW_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackage
{
/** @brief concrete implementation of a DMLObject
  * Specifically for representing a table row
  */
class Row : public DMLObject
{
public:

    /** @brief ctor
     */
    EXPORT Row();

    /** @brief dtor
      */
    EXPORT ~Row();

    /** @brief copy constructor
     */
    EXPORT Row(const Row&);
    
    /** @brief read a Row from a ByteStream
      *
      * @param bytestream the ByteStream to read from
      */
    EXPORT int read(messageqcpp::ByteStream& bytestream);

    /** @brief write a Row to a ByteStream
      * 
      * @param bytestream the ByteStream to write to
      */
    EXPORT int write(messageqcpp::ByteStream& bytestream);

    /** @brief get the list of columns in the row
      */
    inline ColumnList& get_ColumnList() { return fColumnList; }

    /** @brief get the row id
      */
    inline WriteEngine::RID get_RowID() const { return fRowID; }

    /** @brief set the row id
      */
    inline void set_RowID(WriteEngine::RID rowId) { fRowID = rowId; }

    /** @brief  get the number of columns
      */
    inline unsigned int get_NumberOfColumns() const { return static_cast<unsigned int>(fColumnList.size()); }

    /** @brief  get the column at the specified index
      *
      * @param index the index of the column to get
      */
    EXPORT const DMLColumn* get_ColumnAt( unsigned int index ) const;

protected:

private:
    WriteEngine::RID fRowID;
    ColumnList fColumnList;
    Row& operator=(const Row&);

};

/** @brief a vector of Rows
  */
typedef std::vector<Row*>RowList;
}

#undef EXPORT

#endif //ROW_H

