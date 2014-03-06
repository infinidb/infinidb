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
* $Id: we_bulkrollbackfile.cpp 4737 2013-08-14 20:45:46Z bwilkinson $
*/

#include "we_bulkrollbackfile.h"
#include "we_bulkrollbackmgr.h"

#include <sstream>

#include "we_define.h"
#include "we_fileop.h"
#include "messageids.h"
#include "IDBDataFile.h"
using namespace idbdatafile;

using namespace execplan;

namespace WriteEngine
{

//------------------------------------------------------------------------------
// BulkRollbackFile constructor
//------------------------------------------------------------------------------
BulkRollbackFile::BulkRollbackFile(BulkRollbackMgr* mgr) : fMgr(mgr)
{
    // Initialize empty dictionary header block used when reinitializing
    // dictionary store extents.
    const uint16_t freeSpace  = BYTE_PER_BLOCK -
        (HDR_UNIT_SIZE + NEXT_PTR_BYTES + HDR_UNIT_SIZE + HDR_UNIT_SIZE);
    const uint64_t nextPtr    = NOT_USED_PTR;
    const uint16_t offSetZero = BYTE_PER_BLOCK;
    const uint16_t endHeader  = DCTNRY_END_HEADER;

    memcpy(fDctnryHdr,                &freeSpace,  HDR_UNIT_SIZE);
    memcpy(fDctnryHdr+ HDR_UNIT_SIZE, &nextPtr,   NEXT_PTR_BYTES);
    memcpy(fDctnryHdr+ HDR_UNIT_SIZE + NEXT_PTR_BYTES,
                                      &offSetZero, HDR_UNIT_SIZE);
    memcpy(fDctnryHdr+ HDR_UNIT_SIZE + NEXT_PTR_BYTES + HDR_UNIT_SIZE,
                                      &endHeader,  HDR_UNIT_SIZE);
}

//------------------------------------------------------------------------------
// BulkRollbackFile destructor
//------------------------------------------------------------------------------
BulkRollbackFile::~BulkRollbackFile()
{
}

//------------------------------------------------------------------------------
// Build the specified database segment file name.
//
// columnOID      - OID of segment file to be found
// fileTypeFlag   - true -> column file; false -> dictionary store file
// dbRoot         - DBRoot of segment file to be found
// partNum        - Partition number of segment file to be found
// segNum         - Segment number of segment file to be found
// segFileName (out)    - Name of segment file
//------------------------------------------------------------------------------
void BulkRollbackFile::buildSegmentFileName(
    OID          columnOID,
    bool         fileTypeFlag,
    uint32_t    dbRoot,
    uint32_t    partNum,
    uint32_t    segNum,
    std::string& segFileName )
{
    char fileName[FILE_NAME_SIZE];
    int rc = fDbFile.getFileName( columnOID, fileName,
        dbRoot, partNum, segNum );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error constructing "    <<
            (fileTypeFlag ? "column" : "dictionary store") <<
            " filename for deletion"    <<
            "; columnOID-" << columnOID <<
            "; dbRoot-"    << dbRoot    <<
            "; partNum-"   << partNum   <<
            "; segNum-"    << segNum    <<
            "; "           << ec.errorString(rc);

        throw WeException( oss.str(), rc );
    }

    segFileName = fileName;
}

//------------------------------------------------------------------------------
// Delete the specified database segment file.
//
// columnOID      - OID of segment file to be deleted
// fileTypeFlag   - true -> column file; false -> dictionary store file
// dbRoot         - DBRoot of segment file to be deleted
// partNum        - Partition number of segment file to be deleted
// segNum         - Segment number of segment file to be deleted
// segFileName    - Name of file to be deleted
//------------------------------------------------------------------------------
void BulkRollbackFile::deleteSegmentFile(
    OID       columnOID, 
    bool      fileTypeFlag,
    uint32_t dbRoot,
    uint32_t partNum, 
    uint32_t segNum,
    const std::string& segFileName )
{
    std::ostringstream msgText; 
    msgText << "Deleting " << (fileTypeFlag ? "column" : "dictionary store") <<
        " file: dbRoot-"   << dbRoot  <<
        "; part#-"         << partNum <<
        "; seg#-"          << segNum;
    fMgr->logAMessage( logging::LOG_TYPE_INFO,
        logging::M0075, columnOID, msgText.str() );

    // delete the db segment file if it exists
    int rc = fDbFile.deleteFile( segFileName.c_str() );
    if (rc != NO_ERROR)
    {
        if (rc != ERR_FILE_NOT_EXIST)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error deleting segment file"
                "; columnOID-" << columnOID <<
                "; dbRoot-"    << dbRoot    <<
                "; partNum-"   << partNum   <<
                "; segNum-"    << segNum    <<
                "; "           << ec.errorString(rc);

            throw WeException( oss.str(), rc );
        }
    }
}

//------------------------------------------------------------------------------
// Truncate the specified database segment file to the given file offset.
//
// columnOID      - OID of segment file to be truncated
// dbRoot         - DBRoot of segment file to be truncated
// partNum        - Partition number of segment file to be truncated
// segNum         - Segment number of segment file to be truncated
// fileSizeBlocks - Number of blocks to be left in the file.  Remainder of file
//                  is to be truncated.
//------------------------------------------------------------------------------
void BulkRollbackFile::truncateSegmentFile(
    OID       columnOID,
    uint32_t dbRoot,
    uint32_t partNum,
    uint32_t segNum,
    long long fileSizeBlocks )
{
    long long fileSizeBytes = fileSizeBlocks * BYTE_PER_BLOCK;

    std::ostringstream msgText;
    msgText << "Truncating column file"
        ": dbRoot-"          << dbRoot         <<
        "; part#-"           << partNum        <<
        "; seg#-"            << segNum         <<
        "; totBlks-"         << fileSizeBlocks <<
        "; fileSize(bytes)-" << fileSizeBytes;
    fMgr->logAMessage( logging::LOG_TYPE_INFO,
        logging::M0075, columnOID, msgText.str() );

    std::string segFile;
    IDBDataFile* pFile = fDbFile.openFile(columnOID, dbRoot, partNum, segNum, segFile);

    if (pFile == 0)
    {
        std::ostringstream oss;
        oss << "Error opening column segment file to rollback extents "
            "from DB for"  <<
            ": OID-"       << columnOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum;

        throw WeException( oss.str(), ERR_FILE_OPEN );
    }

    int rc = fDbFile.truncateFile( pFile, fileSizeBytes );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error truncating column extents from DB for" <<
            ": OID-"       << columnOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum    <<
            "; "           << ec.errorString(rc);

        fDbFile.closeFile( pFile );
        throw WeException( oss.str(), rc );
    }

    fDbFile.closeFile( pFile );
}

//------------------------------------------------------------------------------
// Reinitialize a column segment extent (in the db file) to empty values,
// following the HWM.  Remaining extents in the file are truncated.
//  
// columnOID      - OID of segment file to be reinitialized
// dbRoot         - DBRoot of segment file to be reinitialized
// partNum        - Partition number of segment file to be reinitialized
// segNum         - Segment number of segment file to be reinitialized
// startOffsetBlk - File offset (after the HWM block), at which the file is
//                  to be reinitialized.  Value is in blocks.
// nBlocks        - Number of blocks to be reinitialized
// colType        - Data type of the applicable column
// colWidth       - Width in bytes, of the applicable column
// restoreHwmChk  - Specifies whether HWM chunk is to be restored.  n/a for
//                  uncompressed, but defined in this base class for the
//                  compressed derived class.
//------------------------------------------------------------------------------
void BulkRollbackFile::reInitTruncColumnExtent(
    OID         columnOID,
    uint32_t    dbRoot,
    uint32_t    partNum,
    uint32_t    segNum,
    long long   startOffsetBlk,
    int         nBlocks,
    CalpontSystemCatalog::ColDataType colType,
    uint32_t  colWidth,
    bool        /*restoreHwmChk*/ )
{
    long long startOffset = startOffsetBlk * BYTE_PER_BLOCK;

    std::ostringstream msgText;
    msgText << "Reinit HWM column extent in db file"
        ": dbRoot-"        << dbRoot      <<
        "; part#-"         << partNum     <<
        "; seg#-"          << segNum      <<
        "; offset(bytes)-" << startOffset <<
        "; freeBlks-"      << nBlocks;
    fMgr->logAMessage( logging::LOG_TYPE_INFO,
        logging::M0075, columnOID, msgText.str() );

    std::string segFile;
    IDBDataFile* pFile = fDbFile.openFile(columnOID, dbRoot, partNum, segNum, segFile);
    if (pFile == 0)
    {
        std::ostringstream oss;
        oss << "Error opening HWM column segment file to rollback extents "
            "from DB for"  << 
            ": OID-"       << columnOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum;

        throw WeException( oss.str(), ERR_FILE_OPEN );
    }

    // nBlocks is based on full extents, but if the database file only has an
    // abbreviated extent, then we reset nBlocks to reflect the size of the file
    // (Only the 1st extent in part0, seg0 employs an abbreviated extent.)
// DMC-SHARED_NOTHING_NOTE: Is it safe to assume only part0 seg0 is abbreviated?
    if ((partNum == 0) && (segNum == 0))
    {
        long long nBytesInAbbrevExtent = INITIAL_EXTENT_ROWS_TO_DISK * colWidth;
        if (startOffset <= nBytesInAbbrevExtent)
        {
            // This check would prevent us from truncating back to an
            // abbreviated extent if the failed import expanded the initial
            // extent; but when adding compression, decided to go ahead and
            // truncate back to an abbreviated extent.
            //long long fileSizeBytes;
            //int rc = fDbFile.getFileSize2(pFile,fileSizeBytes);
            //if (fileSizeBytes == nBytesInAbbrevExtent)
            {
                nBlocks = (nBytesInAbbrevExtent-startOffset) / BYTE_PER_BLOCK;

                std::ostringstream msgText2;
                msgText2 << "Reinit (abbrev) HWM column extent in db file"
                    ": dbRoot-"        << dbRoot      <<
                    "; part#-"         << partNum     <<
                    "; seg#-"          << segNum      <<
                    "; offset(bytes)-" << startOffset <<
                    "; freeBlks-"      << nBlocks;
                fMgr->logAMessage( logging::LOG_TYPE_INFO,
                    logging::M0075, columnOID, msgText2.str() );
            }
        }
    }

    // Initialize the remainder of the extent after the HWM block
    uint64_t emptyVal = fDbFile.getEmptyRowValue( colType, colWidth );

    int rc = fDbFile.reInitPartialColumnExtent( pFile,
        startOffset,
        nBlocks,
        emptyVal,
        colWidth );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error rolling back HWM column extent from DB for" <<
            ": OID-"       << columnOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum    <<
            "; "           << ec.errorString(rc);

        fDbFile.closeFile( pFile );
        throw WeException( oss.str(), rc );
    }

    // Truncate the remainder of the file
    rc = fDbFile.truncateFile( pFile, pFile->tell() );

    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error truncating post-HWM column extents "
            "from HWM segment DB file for" <<
            ": OID-"       << columnOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum    <<
            "; "           << ec.errorString(rc);

        fDbFile.closeFile( pFile );
        throw WeException( oss.str(), rc );
    }

    fDbFile.closeFile( pFile );
}

//------------------------------------------------------------------------------
// Reinitialize a dictionary segment extent (in the db file) to empty blocks,
// following the HWM.  Remaining extents in the file are truncated.
//  
// dStoreOID      - OID of segment store file to be reinitialized
// dbRoot         - DBRoot of segment file to be reinitialized
// partNum        - Partition number of segment file to be reinitialized
// segNum         - Segment number of segment file to be reinitialized
// startOffsetBlk - Starting block (after the HWM block), at which the file is
//                  to be reinitialized.  Value is in raw data blocks.
// nBlocks        - Number of blocks to be reinitialized
//------------------------------------------------------------------------------
void BulkRollbackFile::reInitTruncDctnryExtent(
    OID         dStoreOID,
    uint32_t   dbRoot,
    uint32_t   partNum,
    uint32_t   segNum,
    long long   startOffsetBlk,
    int         nBlocks )
{
    long long startOffset = startOffsetBlk * BYTE_PER_BLOCK;

    std::ostringstream msgText;
    msgText << "Reinit dictionary store extent in db file"
        ": dbRoot-"        << dbRoot      <<
        "; part#-"         << partNum     <<
        "; seg#-"          << segNum      <<
        "; offset(bytes)-" << startOffset <<
        "; numblks-"       << nBlocks;
    fMgr->logAMessage( logging::LOG_TYPE_INFO,
        logging::M0075, dStoreOID, msgText.str() );

    std::string segFile;
    IDBDataFile* pFile = fDbFile.openFile(dStoreOID, dbRoot, partNum, segNum, segFile);
    if (pFile == 0)
    {
        std::ostringstream oss;
        oss << "Error opening dictionary store segment file to rollback extents"
            " from DB for" <<
            ": OID-"       << dStoreOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum;

        throw WeException( oss.str(), ERR_FILE_OPEN );
    }

    // nBlocks is based on full extents, but if the database file only has an
    // abbreviated extent, then we reset nBlocks to reflect the size of the file
    // (Unlike column files which only employ an abbreviated extent for the
    // 1st extent in part0, seg0, all new store files start with abbrev extent)
    const uint32_t PSEUDO_COL_WIDTH = 8; // simulated col width for dictionary
    long long nBytesInAbbrevExtent = INITIAL_EXTENT_ROWS_TO_DISK *
                                     PSEUDO_COL_WIDTH;
    if (startOffset <= nBytesInAbbrevExtent)
    {
        // This check would prevent us from truncating back to an
        // abbreviated extent if the failed import expanded the initial
        // extent; but when adding compression, decided to go ahead and
        // truncate back to an abbreviated extent.
        //long long fileSizeBytes;
        //int rc = fDbFile.getFileSize2(pFile,fileSizeBytes);
        //if (fileSizeBytes == nBytesInAbbrevExtent)
        {
            nBlocks = (nBytesInAbbrevExtent-startOffset) / BYTE_PER_BLOCK;

            std::ostringstream msgText2;
            msgText2 << "Reinit (abbrev) dictionary store extent in db file"
                ": dbRoot-"        << dbRoot      <<
                "; part#-"         << partNum     <<
                "; seg#-"          << segNum      <<
                "; offset(bytes)-" << startOffset <<
                "; numblks-"       << nBlocks;
            fMgr->logAMessage( logging::LOG_TYPE_INFO,
                logging::M0075, dStoreOID, msgText2.str() );
        }
    }

    // Initialize the remainder of the extent after the HWM block
    int rc = fDbFile.reInitPartialDctnryExtent( pFile,
        startOffset,
        nBlocks,
        fDctnryHdr,
        DCTNRY_HEADER_SIZE );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error rolling back HWM dictionary store extent from DB for" <<
            ": OID-"       << dStoreOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum    <<
            "; "           << ec.errorString(rc);

        fDbFile.closeFile( pFile );
        throw WeException( oss.str(), rc );
    }

    // Truncate the remainder of the file
    rc = fDbFile.truncateFile( pFile, pFile->tell() );

    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error truncating post-HWM dictionary store extents "
            "from DB file for"          <<
            ": OID-"       << dStoreOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum    <<
            "; "           << ec.errorString(rc);

        fDbFile.closeFile( pFile );
        throw WeException( oss.str(), rc );
    }

    fDbFile.closeFile( pFile );
}

//------------------------------------------------------------------------------
// For uncompressed data...
// Always return true, in order to always reInit the post-HWM blocks for the
// HWM extent to empty values.
// This function is defined as a stub, so that the derived compression
// class can override this functionality, and return true or false depending
// on whether the HWM chunk was modified and backed up to disk.
//------------------------------------------------------------------------------
bool BulkRollbackFile::doWeReInitExtent( OID columnOID,
    uint32_t dbRoot,
    uint32_t partNum,
    uint32_t segNum) const
{
    return true;
}

} //end of namespace
