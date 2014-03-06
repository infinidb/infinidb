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

/******************************************************************************
 * $Id: we_colopbulk.h 4726 2013-08-07 03:38:36Z bwilkinson $
 *
 *****************************************************************************/

/** @file
 * class ColumnOpBulk
 */

#ifndef _WE_COLOP_BULK_H_
#define _WE_COLOP_BULK_H_

#include "we_colop.h"

#include <cstdio>

namespace WriteEngine
{
class Log;

/** @brief A "generic" implementation version of ColumnOp.
 *
 * This is minimal implementation of ColumnOp that is used by bulkload
 * for both compressed and uncompressed column files.
 */
class ColumnOpBulk : public ColumnOp
{
  public:
                  ColumnOpBulk();
                  ColumnOpBulk(Log* logger, int compressionType);
    virtual      ~ColumnOpBulk();

    virtual bool  abbreviatedExtent(IDBDataFile*, int) const;
    virtual int   blocksInFile(IDBDataFile*) const;
    virtual IDBDataFile* openFile(const WriteEngine::Column& column,
        uint16_t dbRoot, uint32_t partition, uint16_t segment,
        std::string& segFile, bool useTmpSuffix, const char* mode = "r+b",
        int ioBuffSize = DEFAULT_BUFSIZ) const;
    virtual int   readBlock(IDBDataFile*, unsigned char*, const uint64_t);
    virtual int   saveBlock(IDBDataFile*, const unsigned char*, const uint64_t);
};

} //end of namespace

#endif // _WE_COLOP_BULK_H_
