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
* $Id: we_bulkrollbackfilecompressed.h 2865 2011-02-03 17:57:16Z rdempsey $
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
	 */
	virtual int reInitTruncColumnExtent(OID columnOID,
                                u_int32_t   dbRoot,
                                u_int32_t   partNum,
                                u_int32_t   segNum,
                                long long   startOffsetBlk,
                                int         nBlocks,
                                ColDataType colType,
                                u_int32_t   colWidth );

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
	virtual int reInitTruncDctnryExtent(OID columnOID,
                                u_int32_t   dbRoot,
                                u_int32_t   partNum,
                                u_int32_t   segNum,
                                long long   startOffsetBlk,
                                int         nBlocks );

	/** @brief Truncate the specified segment file to a specified num of bytes
	 * @param columnOID OID of the relevant segment file
	 * @param dbRoot DBRoot of the relevant segment file
	 * @param partNum Partition number of the relevant segment file
	 * @param segNum Segment number of the relevant segment file
	 * @param fileSizeBlocks Number of blocks to retain in the file
	 */
	virtual int truncateSegmentFile( OID    columnOID,
                                u_int32_t   dbRoot,
                                u_int32_t   partNum,
                                u_int32_t   segNum,
                                long long   filesSizeBlocks );

private:
	// Disable unnecessary copy constructor and assignment operator
	BulkRollbackFileCompressed(const BulkRollbackFileCompressed& rhs);
	BulkRollbackFileCompressed& operator=(const BulkRollbackFileCompressed& rhs);

	int restoreHWMChunk      (  FILE*       pFile,
                                OID         columnOID,
                                u_int32_t   partNum,
                                u_int32_t   segNum,
                                uint64_t    fileOffsetByteForRestoredChunk,
                                uint64_t&   restoredChunkLen,
                                uint64_t&   restoredFileSize,
                                std::string& errMsg );
	int loadColumnHdrPtrs    (  FILE*       pFile,
                                char*       hdrs,
                                compress::CompChunkPtrList& chunkPtrs,
                                std::string& errMsg) const;
	int loadDctnryHdrPtrs    (  FILE*       pFile,
                                char*       controlHdr,
                                compress::CompChunkPtrList& chunkPtrs,
                                uint64_t&   ptrHdrSize,
                                std::string& errMsg ) const;

	compress::IDBCompressInterface fCompressor;
};

} //end of namespace

#endif // WE_BULKROLLBACKFILECOMPRESSED_H_
