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
* $Id: we_bulkrollbackfile.h 4675 2013-06-13 15:20:37Z dcathey $
*/

/** @file
 * Contains class to restore db files on behalf of BulkRollBackMgr.
 * In some cases this means restoring an extent to it's previous state.
 * In some cases, it may mean removing extent(s) that were added to a file.
 * In other cases it may mean completely deleting a db segment file, if that
 * file did not exist prior to an aborted bulk load.
 */

#ifndef WE_BULKROLLBACKFILE_H_
#define WE_BULKROLLBACKFILE_H_

#include <string>

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

    /** @brief Construct the relevant db filename.
     * Warning: This function may throw a WeException.
     *
     * @param columnOID OID of the segment file to be deleted
     * @param fileTypeFlag file type (true->column; false->dictionary)
     * @param dbRoot DBRoot of the segment file to be deleted
     * @param partNum Partition number of the segment file to be deleted
     * @param segNum Segment number of the segment file to be deleted
     * @param segFileName (out) Name of segment file
     */
    void buildSegmentFileName(OID columnOID,
                        bool      fileTypeFlag,
                        uint32_t dbRoot,
                        uint32_t partNum,
                        uint32_t segNum,
                        std::string& segFileName);

    /** @brief Delete a segment file.
     * Warning: This function may throw a WeException.
     *
     * @param columnOID OID of the segment file to be deleted
     * @param fileTypeFlag file type (true->column; false->dictionary)
     * @param dbRoot DBRoot of the segment file to be deleted
     * @param partNum Partition number of the segment file to be deleted
     * @param segNum Segment number of the segment file to be deleted
     * @param segFileName Name of segment file to be deleted
     */
    void deleteSegmentFile(OID    columnOID,
                        bool      fileTypeFlag,
                        uint32_t dbRoot,
                        uint32_t partNum,
                        uint32_t segNum,
                        const std::string& segFileName );

    /** @brief Construct a directory path.
     *
     * @param oid       (in) OID to use in constructing directory path
     * @param dbRoot    (in) DBRoot to use in constructing directory path
     * @param partition (in) Partition number to use in constructing dir path
     * @param dirName   (out)Directory path constructed from input arguments
     * @return returns NO_ERROR if success
     */
    int buildDirName(   OID      oid,
                        uint16_t dbRoot,
                        uint32_t partition,
                        std::string& dirName);

    /** @brief Do we reinit trailing blocks in the HWM extent for the specified
     * segment file
     *
     * The base behavior of this function always returns true, to reinit
     * any trailing blocks in the HWM extent (for uncompressed data) to
     * empty values.
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
     * Warning: This function may throw a WeException.
     *
     * @param columnOID OID of the relevant segment file
     * @param dbRoot DBRoot of the relevant segment file
     * @param partNum Partition number of the relevant segment file
     * @param segNum Segment number of the relevant segment file
     * @param startOffsetBlk Starting block offset where file is to be
     *        reinitialized
     * @param nBlocks Number of blocks to be reinitialized
     * @param colType Column type of the relevant segment file
     * @param colWidth Width in bytes of column.
     * @param restoreHwmChk Restore HWM chunk (n/a to uncompressed)
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
     * Warning: This function may throw a WeException.
     *
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
     * Warning: This function may throw a WeException.
     *
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

protected:
    BulkRollbackMgr* fMgr;                        // Bulk Rollback controller
    FileOp           fDbFile;                     // interface to DB file
    unsigned char fDctnryHdr[DCTNRY_HEADER_SIZE]; // empty dctnry store blk

private:
    // Disable unnecessary copy constructor and assignment operator
    BulkRollbackFile(const BulkRollbackFile& rhs);
    BulkRollbackFile& operator=(const BulkRollbackFile& rhs);
};

//------------------------------------------------------------------------------
// Inline functions
//------------------------------------------------------------------------------
inline int BulkRollbackFile::buildDirName( OID oid,
    uint16_t dbRoot,
    uint32_t partition,
    std::string& dirName)
{
    return fDbFile.getDirName( oid, dbRoot, partition, dirName );
}

} //end of namespace

#endif // WE_BULKROLLBACKFILE_H_
