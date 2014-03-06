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

#include "we_bulkrollbackfilecompressedhdfs.h"

#include <sstream>
#include <boost/scoped_array.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>

#include "we_define.h"
#include "we_fileop.h"
#include "we_bulkrollbackmgr.h"
#include "we_confirmhdfsdbfile.h"
#include "we_convertor.h"
#include "messageids.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"
using namespace idbdatafile;
using namespace compress;
using namespace execplan;

namespace
{
const char* DATA_DIR_SUFFIX = "_data";
const char* OLD_FILE_SUFFIX = ".old_bulk";
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// BulkRollbackFileCompressedHdfs constructor
//------------------------------------------------------------------------------
BulkRollbackFileCompressedHdfs::BulkRollbackFileCompressedHdfs(
    BulkRollbackMgr* mgr) :
    BulkRollbackFile(mgr)
{
}

//------------------------------------------------------------------------------
// BulkRollbackFileCompressedHdfs destructor
//------------------------------------------------------------------------------
BulkRollbackFileCompressedHdfs::~BulkRollbackFileCompressedHdfs()
{
}

//------------------------------------------------------------------------------
// Truncate the specified database segment file to the extent specified by
// the given file offset.  Also updates the header(s) as well.
//
// columnOID      - OID of segment file to be truncated
// dbRoot         - DBRoot of segment file to be truncated
// partNum        - Partition number of segment file to be truncated
// segNum         - Segment number of segment file to be truncated
// fileSizeBlocks - Number of raw data blocks to be left in the file.
//                  Remainder of file is to be truncated.
//------------------------------------------------------------------------------
void BulkRollbackFileCompressedHdfs::truncateSegmentFile(
    OID       columnOID,
    uint32_t dbRoot,
    uint32_t partNum,
    uint32_t segNum,
    long long fileSizeBlocks )
{
    std::ostringstream msgText;
    msgText << "Truncating compressed HDFS column file"
        ": dbRoot-"          << dbRoot         << 
        "; part#-"           << partNum        <<
        "; seg#-"            << segNum         <<
        "; rawTotBlks-"      << fileSizeBlocks;
    fMgr->logAMessage( logging::LOG_TYPE_INFO,
        logging::M0075, columnOID, msgText.str() );

    restoreFromBackup( "column", columnOID, dbRoot, partNum, segNum );
}

//------------------------------------------------------------------------------
// Reinitialize a column segment extent (in the db file) to empty values,
// following the HWM.  Remaining extents in the file are truncated.
// Also updates the header(s) as well.
//  
// columnOID      - OID of segment file to be reinitialized
// dbRoot         - DBRoot of segment file to be reinitialized
// partNum        - Partition number of segment file to be reinitialized
// segNum         - Segment number of segment file to be reinitialized
// startOffsetBlk - File offset (after the HWM block), at which the file is
//                  to be reinitialized.  Value is in raw data blocks.
// nBlocks        - Number of blocks to be reinitialized
// colType        - Data type of the applicable column
// colWidth       - Width in bytes, of the applicable column
// restoreHwmChk  - Specifies whether HWM chunk is to be restored.
//------------------------------------------------------------------------------
void BulkRollbackFileCompressedHdfs::reInitTruncColumnExtent(
    OID         columnOID,
    uint32_t   dbRoot,
    uint32_t   partNum,
    uint32_t   segNum,
    long long   startOffsetBlk,
    int         nBlocks,
    CalpontSystemCatalog::ColDataType colType,
    uint32_t   colWidth,
    bool        restoreHwmChk )
{
    long long startOffset = startOffsetBlk * BYTE_PER_BLOCK;

    std::ostringstream msgText;
    msgText << "Reinit HWM compressed column extent in HDFS db file" <<
        ": dbRoot-"          << dbRoot      << 
        "; part#-"           << partNum     <<
        "; seg#-"            << segNum      <<
        "; rawOffset(bytes)-"<< startOffset <<
        "; rawFreeBlks-"     << nBlocks;
    fMgr->logAMessage( logging::LOG_TYPE_INFO,
        logging::M0075, columnOID, msgText.str() );

    restoreFromBackup( "column", columnOID, dbRoot, partNum, segNum );
}

//------------------------------------------------------------------------------
// Reinitialize a dictionary segment extent (in the db file) to empty blocks,
// following the HWM.  Remaining extents in the file are truncated.
// Also updates the header(s) as well.
//  
// dStoreOID      - OID of segment store file to be reinitialized
// dbRoot         - DBRoot of segment file to be reinitialized
// partNum        - Partition number of segment file to be reinitialized
// segNum         - Segment number of segment file to be reinitialized
// startOffsetBlk - Starting block (after the HWM block), at which the file is
//                  to be reinitialized.  Value is in raw data blocks.
// nBlocks        - Number of blocks to be reinitialized
//------------------------------------------------------------------------------
void BulkRollbackFileCompressedHdfs::reInitTruncDctnryExtent(
    OID         dStoreOID,
    uint32_t   dbRoot,
    uint32_t   partNum,
    uint32_t   segNum,
    long long   startOffsetBlk,
    int         nBlocks )
{
    long long startOffset = startOffsetBlk * BYTE_PER_BLOCK;

    std::ostringstream msgText;
    msgText << "Reinit HWM compressed dictionary store extent in HDFS db file"
        ": dbRoot-"           << dbRoot      <<
        "; part#-"            << partNum     <<
        "; seg#-"             << segNum      <<
        "; rawOffset(bytes)-" << startOffset <<
        "; rawFreeBlks-"      << nBlocks;
    fMgr->logAMessage( logging::LOG_TYPE_INFO,
        logging::M0075, dStoreOID, msgText.str() );

    restoreFromBackup( "dictionary store", dStoreOID, dbRoot, partNum, segNum );
}

//------------------------------------------------------------------------------
// For HDFS system, just always return true.
// Let ConfirmHdfsDbFile later determine when/if/how to restore from any
// existing backup file.
//------------------------------------------------------------------------------
bool BulkRollbackFileCompressedHdfs::doWeReInitExtent( OID columnOID,
    uint32_t dbRoot,
    uint32_t partNum,
    uint32_t segNum) const
{
    return true;
}

//------------------------------------------------------------------------------
// Replace the currently specified db file with it's corresponding backup file.
// The backup file is a complete backup, not just a backup of a single chunk.
//
// The initial implementation for this function restored from a NNN.pNNN.sNNN
// file stored under the meta file directory.
// The latest  implementation for this function restores from a FILENNN.cdf.tmp
// or FILENNN.cdf.orig file stored in the same OID directory as the FILENNN.cdf
// file.
// However, this function still looks for the first backup file (NNN.pNNN.sNNN)
// in case the user did not upgrade cleanly, and we have to restore using an
// old leftover backup file.
//------------------------------------------------------------------------------
void BulkRollbackFileCompressedHdfs::restoreFromBackup(const char* colType,
    OID       columnOID,
    uint32_t dbRoot,
    uint32_t partNum,
    uint32_t segNum)
{
    // Construct file name for db file to be restored
    char dbFileName[FILE_NAME_SIZE];
    int rc = fDbFile.getFileName( columnOID, dbFileName,
        dbRoot, partNum, segNum );
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "Error restoring " << colType <<
            " HDFS file for OID " << columnOID <<
            "; Can't construct file name for DBRoot"  << dbRoot <<
            "; partition-"   << partNum <<
            "; segment-"     << segNum;
        throw WeException( oss.str(), rc );
    }

    // Construct file name for backup copy of db file
    std::ostringstream ossFile;
    ossFile << "/" << columnOID << ".p" << partNum << ".s" << segNum;
    std::string backupFileName( fMgr->getMetaFileName() );
    backupFileName += DATA_DIR_SUFFIX;
    backupFileName += ossFile.str();

    std::string dbFileNameTmp = dbFileName;
    dbFileNameTmp += OLD_FILE_SUFFIX;

    // For backwards compatibility...
    // Restore from backup file used in initial HDFS release, in case the user
    // upgraded without going down cleanly.  In that case we might need to
    // rollback using an old backup file left from previous release.
    if ( IDBPolicy::exists(backupFileName.c_str()) )
    {
        // Rename current db file to make room for restored file
        rc = IDBPolicy::rename( dbFileName, dbFileNameTmp.c_str() );
        if (rc != 0)
        {
            std::ostringstream oss;
            oss << "Error restoring " << colType <<
                " HDFS file for OID " << columnOID <<
                "; Can't move old file for DBRoot"  << dbRoot <<
                "; partition-"        << partNum <<
                "; segment-"          << segNum;
            throw WeException( oss.str(), ERR_COMP_RENAME_FILE );
        }

        // Rename backup file to replace current db file
        rc = IDBPolicy::rename( backupFileName.c_str(), dbFileName );
        if (rc != 0)
        {
            std::ostringstream oss;
            oss << "Error restoring " << colType <<
                " HDFS file for OID " << columnOID <<
                "; Can't rename backup file for DBRoot"  << dbRoot <<
                "; partition-"        << partNum <<
                "; segment-"          << segNum;
            throw WeException( oss.str(), ERR_METADATABKUP_COMP_RENAME );
        }

        // Delete db file we just replaced with backup
        IDBPolicy::remove( dbFileNameTmp.c_str() );
    }
    else // Restore from HDFS temp swap backup file; This is the normal case
    {
        std::string errMsg;
        ConfirmHdfsDbFile confirmHdfs;
        rc = confirmHdfs.endDbFileChange( std::string("tmp"),
            dbFileName,
            false,
            errMsg);
        if (rc != 0)
        {
            std::ostringstream oss;
            oss << "Error restoring " << colType   <<
                " HDFS file for OID " << columnOID <<
                "; DBRoot"            << dbRoot    <<
                "; partition-"        << partNum   <<
                "; segment-"          << segNum    <<
                "; "                  << errMsg;
            throw WeException( oss.str(), rc );
        }
    }
}

} //end of namespace
