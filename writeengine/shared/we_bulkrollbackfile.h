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
* $Id: we_bulkrollbackfile.h 2865 2011-02-03 17:57:16Z rdempsey $
*/

/** @file
 * Contains class to restore db files on behalf of BulkRollBackMgr.
 */

#ifndef WE_BULKROLLBACKFILE_H_
#define WE_BULKROLLBACKFILE_H_

#include "we_define.h"
#include "we_type.h"
#include "we_fileop.h"

namespace WriteEngine
{
	class BulkRollbackMgr;

//------------------------------------------------------------------------------
/** @brief Class used by BulkRollbackMgr to restore db files.
 */
//------------------------------------------------------------------------------
class BulkRollbackFile
{
public:

	/** @brief BulkRollbackFile constructor
	 * @param mgr The controlling BulkRollbackMgr object.
	 */
	BulkRollbackFile(BulkRollbackMgr* mgr);

	/** @brief BulkRollbackFile destructor
	 */
	virtual     ~BulkRollbackFile();

	/** @brief Delete a segment file
	 * @param columnOID OID of the segment file to be deleted
	 * @param fileTypeFlag file type (true->column; false->dictionary)
	 * @param dbRoot DBRoot of the segment file to be deleted
	 * @param partNum Partition number of the segment file to be deleted
	 * @param segNum Segment number of the segment file to be deleted
	 * @param segFileExisted (out) Did specified segment file exist
	 */
	        int deleteSegmentFile(OID     columnOID,
                                bool      fileTypeFlag,
                                u_int32_t dbRoot,
                                u_int32_t partNum,
                                u_int32_t segNum,
                                bool&     segFileExisted );

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

protected:
	BulkRollbackMgr* fMgr;                        // Bulk Rollback controller
	FileOp           fDbFile;                     // interface to DB file
	unsigned char fDctnryHdr[DCTNRY_HEADER_SIZE]; // empty dctnry store blk

private:
	// Disable unnecessary copy constructor and assignment operator
	BulkRollbackFile(const BulkRollbackFile& rhs);
	BulkRollbackFile& operator=(const BulkRollbackFile& rhs);
};

} //end of namespace

#endif // WE_BULKROLLBACKFILE_H_
