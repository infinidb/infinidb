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
*   $Id: row.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <limits>

#define DMLPKGROW_DLLEXPORT
#include "row.h"
#undef DMLPKGROW_DLLEXPORT

namespace dmlpackage
{

Row::Row()
        :fRowID(std::numeric_limits<WriteEngine::RID>::max())
{}

Row::~Row()
{
    for ( unsigned int i = 0; i < fColumnList.size(); i++)
    {
        delete fColumnList[i];
    }
    fColumnList.clear();
}

Row::Row(const Row& row)
{
    for(unsigned int i = 0; i < row.fColumnList.size(); i++)
    {
	const DMLColumn* aColumn = row.get_ColumnAt(i);
	DMLColumn* newColumn = new DMLColumn(aColumn->get_Name(), aColumn->get_Data());
	fColumnList.push_back(newColumn);
    }
    fRowID = row.fRowID;
}
int Row::read(messageqcpp::ByteStream& bytestream)
{
    int retval = 1;
    messageqcpp::ByteStream::octbyte rowID;
    bytestream >> rowID;
    set_RowID(rowID);
    messageqcpp::ByteStream::quadbyte col_count;
    bytestream >> col_count;
    for (unsigned int i = 0; i < col_count; i++)
    {
        DMLColumn* aColumn = new DMLColumn();
        retval = aColumn->read(bytestream);
        fColumnList.push_back(aColumn);
    }
    return retval;
}


int Row::write(messageqcpp::ByteStream& bytestream)
{
    int retval = 1;
    messageqcpp::ByteStream::octbyte rowID = fRowID;
    bytestream << rowID;
    ColumnList::iterator colListPtr;
    colListPtr = fColumnList.begin();
    messageqcpp::ByteStream::quadbyte col_count = fColumnList.size();
    bytestream << col_count;
    for (; colListPtr != fColumnList.end(); ++colListPtr)
    {
        retval = (*colListPtr)->write(bytestream);
    }

    return retval;
}

const DMLColumn* Row::get_ColumnAt( unsigned int index ) const
{
    const DMLColumn* columnPtr = 0;

    if ( index >= 0 && index < fColumnList.size() )
    {
        columnPtr = fColumnList[index];
    }

    return columnPtr;
}

} // namespace dmlpackage



