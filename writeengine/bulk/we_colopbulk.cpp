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

/*****************************************************************************
 * $Id: we_colopbulk.cpp 4726 2013-08-07 03:38:36Z bwilkinson $
 *
 ****************************************************************************/

/** @file
 * Implementation of the ColumnOpBulk class
 *
 */

#include "we_colopbulk.h"

#include <exception>

#include "we_type.h"
#include "we_log.h"

namespace WriteEngine
{

//------------------------------------------------------------------------------
// Default ColumnOpBulk constructor
//------------------------------------------------------------------------------
ColumnOpBulk::ColumnOpBulk()
{
    m_compressionType = 0;
}

//------------------------------------------------------------------------------
// Alternate ColumnOpBulk Constructor
//------------------------------------------------------------------------------
ColumnOpBulk::ColumnOpBulk(Log* logger, int compressionType) : ColumnOp(logger)
{
    m_compressionType = compressionType;
}

//------------------------------------------------------------------------------
// ColumnOpBulk Destructor
//------------------------------------------------------------------------------
ColumnOpBulk::~ColumnOpBulk()
{
}

//------------------------------------------------------------------------------
// Open specified column file
//------------------------------------------------------------------------------
// @bug 5572 - HDFS usage: add *.tmp file backup flag
IDBDataFile* ColumnOpBulk::openFile(const WriteEngine::Column& column,
    uint16_t     dbRoot,
    uint32_t     partition,
    uint16_t     segment,
    std::string& segFile,
    bool         useTmpSuffix,
    const char*  mode,
    int          ioBuffSize) const
{
    return FileOp::openFile(column.dataFile.fid, dbRoot, partition, segment,
        segFile, mode, column.colWidth, useTmpSuffix);
}

//------------------------------------------------------------------------------
// Stub for abbreviatedExtent
//------------------------------------------------------------------------------
bool  ColumnOpBulk::abbreviatedExtent(IDBDataFile*, int) const
{
    throw std::logic_error(
        "Unauthorized use of ColumnOpBulk::abbreviatedExtent");

    return false;
}

//------------------------------------------------------------------------------
// Stub for blocksInFile
//------------------------------------------------------------------------------
int   ColumnOpBulk::blocksInFile(IDBDataFile*) const
{
    throw std::logic_error(
        "Unauthorized use of ColumnOpBulk::blocksInFile");

    return 0;
}

//------------------------------------------------------------------------------
// Stub for readBlock
//------------------------------------------------------------------------------
int   ColumnOpBulk::readBlock(IDBDataFile*, unsigned char*, const uint64_t)
{
    throw std::logic_error(
        "Unauthorized use of ColumnOpBulk::readBlock");

    return 0;
}

//------------------------------------------------------------------------------
// Stub for writeBlock
//------------------------------------------------------------------------------
int   ColumnOpBulk::saveBlock(IDBDataFile*, const unsigned char*, const uint64_t)
{
    throw std::logic_error(
        "Unauthorized use of ColumnOpBulk::saveBlock");

    return 0;
}

} //end of namespace
