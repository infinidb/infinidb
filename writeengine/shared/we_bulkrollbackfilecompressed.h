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

/*
* $Id: we_bulkrollbackfilecompressed.h 4726 2013-08-07 03:38:36Z bwilkinson $
*/

/** @file
 * Contains class to restore compressed db files on behalf of BulkRollBackMgr.
 */

#ifndef WE_BULKROLLBACKFILECOMPRESSED_H_
#define WE_BULKROLLBACKFILECOMPRESSED_H_

#include <cstdio>
#include <cstring>

#include "we_define.h"
#include "we_type.h"
#include "we_bulkrollbackfile.h"

#include "idbcompress.h"

namespace WriteEngine
{
    class BulkRollbackMgr;

//------------------------------------------------------------------------------
/** @brief Class used by BulkRollbackMgr to restore compressed db files.
 */
//------------------------------------------------------------------------------
class BulkRollbackFileCompressed : public BulkRollbackFile
{
public:

    /** @brief BulkRollbackFile constructor
     * @param mgr The controlling BulkRollbackMgr object.
     */
    BulkRollbackFileCompressed(BulkRollbackMgr* mgr);

    /** @brief BulkRollbackFile destructor
     */
    virtual     ~BulkRollbackFileCompressed();

    /** @brief Do we reinit trailing blocks in the HWM extent for the specified
     * segment file
     *
     * @param columnOID OID of the segment file in question
     * @param dbRoot DBRoot for the segment file in question
     * @param partNum Partition number for the segment file in question
     * @param segNum Segment number for the segment file in question
     */
    virtual bool doWeReInitExtent( OID columnOID,
                        uint32_t   dbRoot,
                        uint32_t   partNum,
                        uint32_t   segNum) const;

    /** @brief Reinitialize the specified column segment file starting at
     * startOffsetBlk, and truncate trailing extents.
     * @param columnOID OID of the relevant segment file
     * @param dbRoot DBRoot of the relevant segment file
     * @param partNum Partition number of the relevant segment file
     * @param segNum Segment number of the relevant segment file
     * @param startOffsetBlk Starting block offset where file is to be
     *        reinitialized
     * @param nBlocks Number of blocks to be reinitialized
     * @param colType Column type of the relevant segment file
     * @param colWidth Width in bytes of column.
     * @param restoreHwmChk Restore HWM chunk
     */
    virtual void reInitTruncColumnExtent(OID columnOID,
                                uint32_t   dbRoot,
                                uint32_t   partNum,
                                uint32_t   segNum,
                                long long   startOffsetBlk,
                                int         nBlocks,
                                execplan::CalpontSystemCatalog::ColDataType colType,
                                uint32_t   colWidth,
                                bool        restoreHwmChk );

    /** @brief Reinitialize the specified dictionary store segment file starting
     * at startOffsetBlk, and truncate trailing extents.
     * @param columnOID OID of the relevant segment file
     * @param dbRoot DBRoot of the relevant segment file
     * @param partNum Partition number of the relevant segment file
     * @param segNum Segment number of the relevant segment file
     * @param startOffsetBlk Starting block offset where file is to be
     *        reinitialized
     * @param nBlocks Number of blocks to be reinitialized
     */
    virtual void reInitTruncDctnryExtent(OID columnOID,
                                uint32_t   dbRoot,
                                uint32_t   partNum,
                                uint32_t   segNum,
                                long long   startOffsetBlk,
                                int         nBlocks );

    /** @brief Truncate the specified segment file to a specified num of bytes
     * @param columnOID OID of the relevant segment file
     * @param dbRoot DBRoot of the relevant segment file
     * @param partNum Partition number of the relevant segment file
     * @param segNum Segment number of the relevant segment file
     * @param fileSizeBlocks Number of blocks to retain in the file
     */
    virtual void truncateSegmentFile( OID    columnOID,
                                uint32_t   dbRoot,
                                uint32_t   partNum,
                                uint32_t   segNum,
                                long long   filesSizeBlocks );

private:
    // Disable unnecessary copy constructor and assignment operator
    BulkRollbackFileCompressed(const BulkRollbackFileCompressed& rhs);
    BulkRollbackFileCompressed& operator=(const BulkRollbackFileCompressed& rhs);

    size_t readFillBuffer    (  IDBDataFile* pFile,
                                char*       buffer,
                                size_t      bytesReq) const;
    int restoreHWMChunk      (  IDBDataFile* pFile,
                                OID         columnOID,
                                uint32_t   partNum,
                                uint32_t   segNum,
                                uint64_t    fileOffsetByteForRestoredChunk,
                                uint64_t&   restoredChunkLen,
                                uint64_t&   restoredFileSize,
                                std::string& errMsg );
    int loadColumnHdrPtrs    (  IDBDataFile* pFile,
                                char*       hdrs,
                                compress::CompChunkPtrList& chunkPtrs,
                                std::string& errMsg) const;
    int loadDctnryHdrPtrs    (  IDBDataFile* pFile,
                                char*       controlHdr,
                                compress::CompChunkPtrList& chunkPtrs,
                                uint64_t&   ptrHdrSize,
                                std::string& errMsg ) const;

    compress::IDBCompressInterface fCompressor;
};

} //end of namespace

#endif // WE_BULKROLLBACKFILECOMPRESSED_H_
