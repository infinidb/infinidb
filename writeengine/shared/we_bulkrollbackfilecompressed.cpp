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
* $Id: we_bulkrollbackfilecompressed.cpp 4737 2013-08-14 20:45:46Z bwilkinson $
*/

#include "we_bulkrollbackfilecompressed.h"

#include <sstream>
#include <boost/scoped_array.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>

#include "we_define.h"
#include "we_fileop.h"
#include "we_bulkrollbackmgr.h"
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
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// BulkRollbackFileCompressed constructor
//------------------------------------------------------------------------------
BulkRollbackFileCompressed::BulkRollbackFileCompressed(BulkRollbackMgr* mgr) :
    BulkRollbackFile(mgr)
{
}

//------------------------------------------------------------------------------
// BulkRollbackFileCompressed destructor
//------------------------------------------------------------------------------
BulkRollbackFileCompressed::~BulkRollbackFileCompressed()
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
void BulkRollbackFileCompressed::truncateSegmentFile(
    OID       columnOID,
    uint32_t dbRoot,
    uint32_t partNum,
    uint32_t segNum,
    long long fileSizeBlocks )
{
    std::ostringstream msgText1;
    msgText1 << "Truncating compressed column file"
        ": dbRoot-"          << dbRoot         << 
        "; part#-"           << partNum        <<
        "; seg#-"            << segNum         <<
        "; rawTotBlks-"      << fileSizeBlocks;
    fMgr->logAMessage( logging::LOG_TYPE_INFO,
        logging::M0075, columnOID, msgText1.str() );

    std::string segFile;
    IDBDataFile* pFile = fDbFile.openFile(columnOID, dbRoot, partNum, segNum, segFile);
    if (pFile == 0)
    {
        std::ostringstream oss;
        oss << "Error opening compressed column segment file to rollback "
            "extents from DB for"  <<
            ": OID-"       << columnOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum;

        throw WeException( oss.str(), ERR_FILE_OPEN );
    }

    // Read and parse the header pointers
    char hdrs[ IDBCompressInterface::HDR_BUF_LEN * 2 ];;
    CompChunkPtrList chunkPtrs;
    std::string      errMsg;
    int rc = loadColumnHdrPtrs(pFile, hdrs, chunkPtrs, errMsg);
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "Error reading compressed column ptr headers from DB for" <<
            ": OID-"       << columnOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum    <<
            "; "           << errMsg;

        fDbFile.closeFile( pFile );
        throw WeException( oss.str(), rc );
    }

    // Locate the chunk containing the last block we intend to keep
    unsigned int blockOffset      = fileSizeBlocks - 1;
    unsigned int chunkIndex       = 0;
    unsigned int blkOffsetInChunk = 0;
    fCompressor.locateBlock( blockOffset, chunkIndex, blkOffsetInChunk );

    // Truncate the extra extents that are to be aborted
    if (chunkIndex < chunkPtrs.size())
    {
        long long fileSizeBytes = chunkPtrs[chunkIndex].first +
            chunkPtrs[chunkIndex].second;

        std::ostringstream msgText2;
        msgText2 << "Compressed column file"
            ": dbRoot-"        << dbRoot         << 
            "; part#-"        << partNum        <<
            "; seg#-"         << segNum         <<
            "; truncated to " << fileSizeBytes  << " bytes";
        fMgr->logAMessage( logging::LOG_TYPE_INFO,
            logging::M0075, columnOID, msgText2.str() );

        // Drop off any trailing pointers (that point beyond the last block)
        fCompressor.setBlockCount( hdrs, fileSizeBlocks );
        std::vector<uint64_t> ptrs;
        for (unsigned i=0; i<=chunkIndex; i++)
        {
            ptrs.push_back( chunkPtrs[i].first );
        }
        ptrs.push_back( chunkPtrs[chunkIndex].first +
            chunkPtrs[chunkIndex].second );
        fCompressor.storePtrs( ptrs, hdrs );

        rc = fDbFile.writeHeaders( pFile, hdrs );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error writing compressed column headers to DB for" <<
                ": OID-"       << columnOID <<
                "; DbRoot-"    << dbRoot    <<
                "; partition-" << partNum   <<
                "; segment-"   << segNum    <<
                "; "           << ec.errorString(rc);

            fDbFile.closeFile( pFile );
            throw WeException( oss.str(), rc );
        }

        // Finally, we truncate the data base column segment file
        rc = fDbFile.truncateFile( pFile, fileSizeBytes );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error truncating compressed column extents from DB for" <<
                ": OID-"       << columnOID <<
                "; DbRoot-"    << dbRoot    <<
                "; partition-" << partNum   <<
                "; segment-"   << segNum    <<
                "; "           << ec.errorString(rc);

            fDbFile.closeFile( pFile );
            throw WeException( oss.str(), rc );
        }
    } // end of (chunkIndex < chunkPtrs.size())

    fDbFile.closeFile( pFile );
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
void BulkRollbackFileCompressed::reInitTruncColumnExtent(
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

    std::ostringstream msgText1;
    msgText1 << "Reinit HWM compressed column extent in db file" <<
        ": dbRoot-"          << dbRoot      << 
        "; part#-"           << partNum     <<
        "; seg#-"            << segNum      <<
        "; rawOffset(bytes)-"<< startOffset <<
        "; rawFreeBlks-"     << nBlocks;
    fMgr->logAMessage( logging::LOG_TYPE_INFO,
        logging::M0075, columnOID, msgText1.str() );

    std::string segFile;
    IDBDataFile* pFile = fDbFile.openFile(columnOID, dbRoot, partNum, segNum, segFile);
    if (pFile == 0)
    {
        std::ostringstream oss;
        oss << "Error opening compressed column segment file to rollback "
            "extents from DB for"  <<
            ": OID-"       << columnOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum;

        throw WeException( oss.str(), ERR_FILE_OPEN );
    }

    // Read and parse the header pointers
    char hdrs[ IDBCompressInterface::HDR_BUF_LEN * 2 ];
    CompChunkPtrList     chunkPtrs;
    std::string          errMsg;
    int rc = loadColumnHdrPtrs(pFile, hdrs, chunkPtrs, errMsg);
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "Error reading compressed column ptr headers from DB for" <<
            ": OID-"       << columnOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum    <<
            "; "           << errMsg;

        fDbFile.closeFile( pFile );
        throw WeException( oss.str(), rc );
    }
        
    // Locate the chunk containing the last block we intend to keep
    unsigned int blockOffset      = startOffsetBlk - 1;
    unsigned int chunkIndex       = 0;
    unsigned int blkOffsetInChunk = 0;
    fCompressor.locateBlock( blockOffset, chunkIndex, blkOffsetInChunk );

    if (chunkIndex < chunkPtrs.size())
    {
        // Read backup copy of HWM chunk and restore it's contents
        uint64_t restoredChunkLen = 0;
        uint64_t restoredFileSize = 0;
        if (restoreHwmChk)
        {
            rc = restoreHWMChunk(pFile, columnOID, partNum, segNum,
                chunkPtrs[chunkIndex].first,
                restoredChunkLen, restoredFileSize, errMsg);
            if (rc != NO_ERROR)
            {
                std::ostringstream oss;
                oss << "Error restoring HWM chunk for" <<
                    ": OID-"       << columnOID   <<
                    "; DbRoot-"    << dbRoot      <<
                    "; partition-" << partNum     <<
                    "; segment-"   << segNum      <<
                    "; blkoff-"    << blockOffset <<
                    "; "           << errMsg;

                fDbFile.closeFile( pFile );
                throw WeException( oss.str(), rc );
            }
        }
        else
        {
            restoredChunkLen = chunkPtrs[chunkIndex].second;

            // leave truncated to last chunk if no extra blocks needed
            if (nBlocks == 0)
                restoredFileSize = chunkPtrs[chunkIndex].first +
                                   chunkPtrs[chunkIndex].second;
            else
                restoredFileSize = (chunkPtrs[chunkIndex].first   +
                                    chunkPtrs[chunkIndex].second) +
                                    (uint64_t)(nBlocks * BYTE_PER_BLOCK);
        }

        // nBlocks is based on full extents, but if database file only has an
        // abbreviated extent, then we reset nBlocks to reflect the size of a
        // file with a single abbreviated extent.
        // (Only the 1st extent in part0, seg0 employs an abbreviated extent.)
        bool bAbbreviatedExtent = false;
// DMC-SHARED_NOTHING_NOTE: Is it safe to assume only part0 seg0 is abbreviated?
        if ((partNum == 0) && (segNum == 0))
        {
            long long nBytesInAbbrevExtent = INITIAL_EXTENT_ROWS_TO_DISK *
                                             colWidth;
            if (startOffset <= nBytesInAbbrevExtent)
            {
                nBlocks = (nBytesInAbbrevExtent-startOffset) / BYTE_PER_BLOCK;
                bAbbreviatedExtent = true;
            }
        }

        long long fileSizeBytes = restoredFileSize;

        std::ostringstream msgText2;
        msgText2 << "HWM compressed column file"
            ": dbRoot-" << dbRoot  <<
            "; part#-"  << partNum <<
            "; seg#-"   << segNum;
        if (bAbbreviatedExtent) // log adjusted nBlock count for abbrev extent
            msgText2 << "; rawFreeBlks-"  << nBlocks << " (abbrev)";
        msgText2 << "; restoredChunk-" << restoredChunkLen << " bytes";
        if (!restoreHwmChk)
            msgText2 << " (no change)";
        msgText2 << "; truncated to "  << fileSizeBytes    << " bytes";
        fMgr->logAMessage( logging::LOG_TYPE_INFO,
            logging::M0075, columnOID, msgText2.str() );

        // Initialize the remainder of the extent after the HWM chunk.
        // Just doing an ftruncate() reinits the file to 0's, which may or may
        // not actually reserve disk space if ftruncate is growing the file.
        // So reinit the blocks by calling reInitPartialColumnExtent() to help
        // avoid disk fragmentation.  Be careful not to init > 1 extent, be-
        // cause that is the limit on what that function was intended to do.
        const unsigned BLKS_PER_EXTENT =
            (BRMWrapper::getInstance()->getExtentRows() * colWidth) /
            BYTE_PER_BLOCK;
        long long nBlocksToInit = (fileSizeBytes - 
            (chunkPtrs[chunkIndex].first + restoredChunkLen)) / BYTE_PER_BLOCK;
        if (nBlocksToInit > BLKS_PER_EXTENT)
            nBlocksToInit = BLKS_PER_EXTENT; // don't init > 1 full extent
        if (nBlocksToInit > 0)
        {
            uint64_t emptyVal = fDbFile.getEmptyRowValue( colType, colWidth );
            rc = fDbFile.reInitPartialColumnExtent( pFile,
                (chunkPtrs[chunkIndex].first + restoredChunkLen),
                nBlocksToInit,
                emptyVal,
                colWidth );
            if (rc != NO_ERROR)
            {
                WErrorCodes ec;
                std::ostringstream oss;
                oss << "Error clearing HWM column extent from DB for"
                    ": OID-"       << columnOID <<
                    "; DbRoot-"    << dbRoot    <<
                    "; partition-" << partNum   <<
                    "; segment-"   << segNum    <<
                    "; "           << ec.errorString(rc);

                fDbFile.closeFile( pFile );
                throw WeException( oss.str(), rc );
            }
        }

        // Drop off any trailing pointers (that point beyond the last block).
        // Watch for the special case where we are restoring a db file as an
        // empty file (chunkindex=0 and restoredChunkLen=0); in this case we
        // just restore the first pointer (set to 8192).
        fCompressor.setBlockCount( hdrs, (startOffsetBlk + nBlocks) );
        std::vector<uint64_t> newPtrs;
        if ((chunkIndex > 0) || (restoredChunkLen > 0))
        {
            for (unsigned int i=0; i<=chunkIndex; i++)
            {
                newPtrs.push_back( chunkPtrs[i].first );
            }
        }
        newPtrs.push_back( chunkPtrs[chunkIndex].first + restoredChunkLen );
        fCompressor.storePtrs( newPtrs, hdrs );

        rc = fDbFile.writeHeaders( pFile, hdrs );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error writing compressed column headers to DB for" <<
                ": OID-"       << columnOID <<
                "; DbRoot-"    << dbRoot    <<
                "; partition-" << partNum   <<
                "; segment-"   << segNum    <<
                "; "           << ec.errorString(rc);

            fDbFile.closeFile( pFile );
            throw WeException( oss.str(), rc );
        }

        // Finally, we truncate the data base column segment file
        rc = fDbFile.truncateFile( pFile, fileSizeBytes );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error truncating compressed column extents from DB for" <<
                ": OID-"       << columnOID <<
                "; DbRoot-"    << dbRoot    <<
                "; partition-" << partNum   <<
                "; segment-"   << segNum    <<
                "; "           << ec.errorString(rc);

            fDbFile.closeFile( pFile );
            throw WeException( oss.str(), rc );
        }
    } // end of (chunkIndex < chunkPtrs.size())

    fDbFile.closeFile( pFile );
}

//------------------------------------------------------------------------------
// Load header pointer data for compressed column file.
//
// pFile     - FILE ptr to be used in reading header data.
// hdrs      - (out) Raw header data.
// chunkPtrs - (out) Chunk ptrs extracted from raw header data.
// errMsg    - (out) Error message if applicable.
//------------------------------------------------------------------------------
int BulkRollbackFileCompressed::loadColumnHdrPtrs(
    IDBDataFile* pFile,
    char*        hdrs,
    CompChunkPtrList& chunkPtrs,
    std::string& errMsg) const
{
    // Read the header pointers
    int rc = fDbFile.readHeaders( pFile, hdrs );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Header read error: " << ec.errorString(rc);
        errMsg = oss.str();

        return rc;
    }

    // Parse the header pointers
    int rc1 = fCompressor.getPtrList( hdrs, chunkPtrs );
    if (rc1 != 0)
    {
        rc = ERR_METADATABKUP_COMP_PARSE_HDRS;

        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Header parsing error (" << rc1 << "): " << ec.errorString(rc);
        errMsg = oss.str();

        return rc;
    }

    return NO_ERROR;
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
void BulkRollbackFileCompressed::reInitTruncDctnryExtent(
    OID         dStoreOID,
    uint32_t   dbRoot,
    uint32_t   partNum,
    uint32_t   segNum,
    long long   startOffsetBlk,
    int         nBlocks )
{
    long long startOffset = startOffsetBlk * BYTE_PER_BLOCK;

    std::ostringstream msgText1;
    msgText1 << "Reinit HWM compressed dictionary store extent in db file"
        ": dbRoot-"           << dbRoot      <<
        "; part#-"            << partNum     <<
        "; seg#-"             << segNum      <<
        "; rawOffset(bytes)-" << startOffset <<
        "; rawFreeBlks-"      << nBlocks;
    fMgr->logAMessage( logging::LOG_TYPE_INFO,
        logging::M0075, dStoreOID, msgText1.str() );

    std::string segFile;
    IDBDataFile* pFile = fDbFile.openFile(dStoreOID, dbRoot, partNum, segNum, segFile);
    if (pFile == 0)
    {
        std::ostringstream oss;
        oss << "Error opening compressed dictionary store segment file to "
            "rollback extents from DB for" <<
            ": OID-"       << dStoreOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum;

        throw WeException( oss.str(), ERR_FILE_OPEN );
    }

    char controlHdr[ IDBCompressInterface::HDR_BUF_LEN ];
    CompChunkPtrList chunkPtrs;
    uint64_t         ptrHdrSize;
    std::string      errMsg;
    int rc = loadDctnryHdrPtrs(pFile, controlHdr, chunkPtrs, ptrHdrSize,errMsg);
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "Error reading compressed dctnry ptr headers from DB for" <<
            ": OID-"       << dStoreOID <<
            "; DbRoot-"    << dbRoot    <<
            "; partition-" << partNum   <<
            "; segment-"   << segNum    <<
            "; "           << errMsg;

        fDbFile.closeFile( pFile );
        throw WeException( oss.str(), rc );
    }
    
    // Locate the chunk containing the last block we intend to keep
    unsigned int blockOffset      = startOffsetBlk - 1;
    unsigned int chunkIndex       = 0;
    unsigned int blkOffsetInChunk = 0;
    fCompressor.locateBlock( blockOffset, chunkIndex, blkOffsetInChunk );

    if (chunkIndex < chunkPtrs.size())
    {
        // Read backup copy of HWM chunk and restore it's contents
        uint64_t restoredChunkLen = 0;
        uint64_t restoredFileSize = 0;
        rc = restoreHWMChunk(pFile, dStoreOID, partNum, segNum,
            chunkPtrs[chunkIndex].first,
            restoredChunkLen, restoredFileSize, errMsg);
        if (rc == ERR_FILE_NOT_EXIST)
        {
            std::ostringstream msgText3;
            msgText3 << "No restore needed to Compressed dictionary file" <<
                ": dbRoot-" << dbRoot  <<
                "; part#-"  << partNum <<
                "; seg#-"   << segNum;
            fMgr->logAMessage( logging::LOG_TYPE_INFO,
                logging::M0075, dStoreOID, msgText3.str() );

            fDbFile.closeFile( pFile );
            return;
        }

        if (rc != NO_ERROR)
        {
            std::ostringstream oss;
            oss << "Error restoring HWM chunk for" <<
                ": OID-"       << dStoreOID   <<
                "; DbRoot-"    << dbRoot      <<
                "; partition-" << partNum     <<
                "; segment-"   << segNum      <<
                "; blkoff-"    << blockOffset <<
                "; "           << errMsg;

            fDbFile.closeFile( pFile );
            throw WeException( oss.str(), rc );
        }

        // nBlocks is based on full extents, but if database file only has an
        // abbreviated extent, then we reset nBlocks to reflect the file size.
        // (Unlike column files which only employ an abbreviated extent for the
        // 1st extent in part0, seg0, all store files start with abbrev extent)
        bool bAbbreviatedExtent          = false;
        const uint32_t PSEUDO_COL_WIDTH = 8; // simulated col width for dctnry
        long long nBytesInAbbrevExtent   = INITIAL_EXTENT_ROWS_TO_DISK *
                                     PSEUDO_COL_WIDTH;
        if (startOffset <= nBytesInAbbrevExtent)
        {
            nBlocks = (nBytesInAbbrevExtent-startOffset) / BYTE_PER_BLOCK;
            bAbbreviatedExtent = true;
        }

        long long fileSizeBytes = restoredFileSize;

        std::ostringstream msgText2;
        msgText2 << "HWM compressed dictionary file"
            ": dbRoot-" << dbRoot  <<
            "; part#-"  << partNum <<
            "; seg#-"   << segNum;
        if (bAbbreviatedExtent) // log adjusted nBlock count for abbrev extent
            msgText2 << "; rawFreeBlks-"  << nBlocks << " (abbrev)";
        msgText2 << "; restoredChunk-" << restoredChunkLen << " bytes" <<
            "; truncated to "          << fileSizeBytes    << " bytes";
        fMgr->logAMessage( logging::LOG_TYPE_INFO,
            logging::M0075, dStoreOID, msgText2.str() );

        // Initialize the remainder of the extent after the HWM chunk
        // Just doing an ftruncate() reinits the file to 0's, which may or may
        // not actually reserve disk space if ftruncate is growing the file.
        // So reinit the blocks by calling reInitPartialDctnryExtent() to help
        // avoid disk fragmentation.  Be careful not to init > 1 extent, be-
        // cause that is the limit on what that function was intended to do.
        const unsigned BLKS_PER_EXTENT =
            (BRMWrapper::getInstance()->getExtentRows() * PSEUDO_COL_WIDTH) /
            BYTE_PER_BLOCK;
        long long nBlocksToInit = (fileSizeBytes - 
            (chunkPtrs[chunkIndex].first + restoredChunkLen)) / BYTE_PER_BLOCK;
        if (nBlocksToInit > BLKS_PER_EXTENT)
            nBlocksToInit = BLKS_PER_EXTENT; // don't init > 1 full extent
        if (nBlocksToInit > 0)
        {
            rc = fDbFile.reInitPartialDctnryExtent( pFile,
                (chunkPtrs[chunkIndex].first + restoredChunkLen),
                nBlocksToInit,
                fDctnryHdr,
                DCTNRY_HEADER_SIZE );
            if (rc != NO_ERROR)
            {
                WErrorCodes ec;
                std::ostringstream oss;
                oss << "Error clearing HWM dictionary store extent from DB for"
                    ": OID-"       << dStoreOID <<
                    "; DbRoot-"    << dbRoot    <<
                    "; partition-" << partNum   <<
                    "; segment-"   << segNum    <<
                    "; "           << ec.errorString(rc);

                fDbFile.closeFile( pFile );
                throw WeException( oss.str(), rc );
            }
        }

        // Drop off any trailing pointers (that point beyond the last block).
        // Watch for the special case where we are restoring a db file as an
        // empty file (chunkindex=0 and restoredChunkLen=0); in this case we
        // just restore the first pointer (set to 8192).
        fCompressor.setBlockCount( controlHdr, (startOffsetBlk + nBlocks) );
        std::vector<uint64_t> newPtrs;
        if ((chunkIndex > 0) || (restoredChunkLen > 0))
        {
            for (unsigned int i=0; i<=chunkIndex; i++)
            {
                newPtrs.push_back( chunkPtrs[i].first );
            }
        }
        newPtrs.push_back( chunkPtrs[chunkIndex].first + restoredChunkLen );
        char* pointerHdr = new char[ptrHdrSize];
        fCompressor.storePtrs( newPtrs, pointerHdr, ptrHdrSize );

        rc = fDbFile.writeHeaders( pFile, controlHdr, pointerHdr, ptrHdrSize );
        delete[] pointerHdr;
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error writing compressed dictionary headers to DB for" <<
                ": OID-"       << dStoreOID <<
                "; DbRoot-"    << dbRoot    <<
                "; partition-" << partNum   <<
                "; segment-"   << segNum    <<
                "; "           << ec.errorString(rc);

            fDbFile.closeFile( pFile );
            throw WeException( oss.str(), rc );
        }

        // Finally, we truncate the data base dictionary store segment file
        rc = fDbFile.truncateFile( pFile, fileSizeBytes );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error truncating compressed dictionary store extents "
                "from DB file for"          <<
                ": OID-"       << dStoreOID <<
                "; DbRoot-"    << dbRoot    <<
                "; partition-" << partNum   <<
                "; segment-"   << segNum    <<
                "; "           << ec.errorString(rc);

            fDbFile.closeFile( pFile );
            throw WeException( oss.str(), rc );
        }
    } // end of (chunkIndex < chunkPtrs.size())

    fDbFile.closeFile( pFile );
}

//------------------------------------------------------------------------------
// Load header pointer data for compressed dictionary file.
//
// pFile     - FILE ptr to be used in reading header data.
// controlHdr- (out) Raw data from control header.
// chunkPtrs - (out) Chunk ptrs extracted from raw header data.
// ptrHdrSize- (out) Size of pointer header.
// errMsg    - (out) Error message if applicable.
//------------------------------------------------------------------------------
int BulkRollbackFileCompressed::loadDctnryHdrPtrs(
    IDBDataFile* pFile,
    char*        controlHdr,
    CompChunkPtrList& chunkPtrs,
    uint64_t&    ptrHdrSize,
    std::string& errMsg) const
{
    int rc = fDbFile.readFile(
        pFile, (unsigned char*)controlHdr, IDBCompressInterface::HDR_BUF_LEN);
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Control header read error: " << ec.errorString(rc);
        errMsg = oss.str();

        return rc;
    }

    int rc1 = fCompressor.verifyHdr( controlHdr );
    if (rc1 != 0)
    {
        rc = ERR_METADATABKUP_COMP_VERIFY_HDRS;

        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Control header verify error (" << rc1 << "): " <<
            ec.errorString(rc);
        errMsg = oss.str();
    
        return rc;
    }
    
    uint64_t hdrSize = fCompressor.getHdrSize(controlHdr);
    ptrHdrSize       = hdrSize - IDBCompressInterface::HDR_BUF_LEN;
    char* pointerHdr = new char[ptrHdrSize];

    rc = fDbFile.readFile(pFile, (unsigned char*)pointerHdr, ptrHdrSize);
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Pointer header read error: " << ec.errorString(rc);
        errMsg = oss.str();
        delete[] pointerHdr;

        return rc;
    }

    // Parse the header pointers
    rc1 = fCompressor.getPtrList( pointerHdr, ptrHdrSize, chunkPtrs );
    delete[] pointerHdr;
    if (rc1 != 0)
    {
        rc = ERR_METADATABKUP_COMP_PARSE_HDRS;

        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Pointer header parsing error (" << rc1 << "): " <<
            ec.errorString(rc);
        errMsg = oss.str();

        return rc;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Restore the HWM chunk back to the contents saved in the backup file.
//
// pFile                          - FILE* to segment file being reinitialized
// columnOID                      - OID of segment file to be reinitialized
// partNum                        - Partition num of seg file to reinitialize
// segNum                         - Segment num of seg file to reinitialize
// fileOffsetByteForRestoredChunk - Offset to pFile where restored chunk is to
//                                  be written
// restoredChunkLen (out)         - Length of restored chunk (in bytes)
// restoredFileSize (out)         - Size of file (in bytes) when backup was made
// errMsg (out)                   - Error msg if error returned
//------------------------------------------------------------------------------
int BulkRollbackFileCompressed::restoreHWMChunk(
    IDBDataFile* pFile,
    OID          columnOID,
    uint32_t    partNum,
    uint32_t    segNum,
    uint64_t     fileOffsetByteForRestoredChunk,
    uint64_t&    restoredChunkLen,
    uint64_t&    restoredFileSize,
    std::string& errMsg)
{
    restoredChunkLen = 0;
    restoredFileSize = 0;

    // Open the backup HWM chunk file
    std::ostringstream oss;
    oss << "/" << columnOID << ".p" << partNum << ".s" << segNum;
    std::string bulkRollbackSubPath( fMgr->getMetaFileName() );
    bulkRollbackSubPath += DATA_DIR_SUFFIX;
    bulkRollbackSubPath += oss.str();

    if ( !IDBPolicy::exists( bulkRollbackSubPath.c_str() ) )
    {
        std::ostringstream oss;
        oss << "Backup file does not exist: " << bulkRollbackSubPath;
        errMsg = oss.str();

        return ERR_FILE_NOT_EXIST;
    }

    IDBDataFile* backupFile = IDBDataFile::open(
    							IDBPolicy::getType( bulkRollbackSubPath.c_str(), IDBPolicy::WRITEENG ),
    							bulkRollbackSubPath.c_str(),
    							"rb",
    							0,
                                pFile->colWidth() );
    if (!backupFile)
    {
        int errrc = errno;

        std::string eMsg;
        Convertor::mapErrnoToString(errrc, eMsg);
        std::ostringstream oss;
        oss << "Error opening backup file " << 
            bulkRollbackSubPath << "; " << eMsg;
        errMsg = oss.str();

        return ERR_METADATABKUP_COMP_OPEN_BULK_BKUP;
    }

    // Read the chunk length and file size
    uint64_t sizeHdr[2];
    size_t bytesRead = readFillBuffer(backupFile, (char*)sizeHdr,
        sizeof(uint64_t)*2);
    if (bytesRead != sizeof(uint64_t)*2)
    {
        int errrc = errno;

        std::string eMsg;
        Convertor::mapErrnoToString(errrc, eMsg);
        std::ostringstream oss;
        oss << "Error reading chunk length from backup file " <<
            bulkRollbackSubPath << "; " << eMsg;
        errMsg = oss.str();

        delete backupFile;
        return ERR_METADATABKUP_COMP_READ_BULK_BKUP;
    }
    restoredChunkLen = sizeHdr[0];
    restoredFileSize = sizeHdr[1];

    // Position the destination offset in the DB file
    int rc = fDbFile.setFileOffset(pFile, fileOffsetByteForRestoredChunk,
        SEEK_SET);
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error setting column file offset" <<
            "; offset-" << fileOffsetByteForRestoredChunk     <<
            "; "       << ec.errorString(rc);
        errMsg = oss.str();

        delete backupFile;
        return rc;
    }

    // Copy backup version of chunk back to DB, unless chunk length is 0
    // in which case we have nothing to copy.
    if (restoredChunkLen > 0)
    {
        // Read the HWM chunk to be restored
        unsigned char* chunk = new unsigned char[restoredChunkLen];
        boost::scoped_array<unsigned char> scopedChunk( chunk );
        bytesRead = readFillBuffer(backupFile, (char*)chunk, restoredChunkLen);
        if (bytesRead != restoredChunkLen)
        {
            int errrc = errno;

            std::string eMsg;
            Convertor::mapErrnoToString(errrc, eMsg);
            std::ostringstream oss;
            oss << "Error reading chunk data from backup file " <<
                bulkRollbackSubPath <<
                "; size-" << restoredChunkLen <<
                ": "      << eMsg;
            errMsg = oss.str();

            delete backupFile;
            return ERR_METADATABKUP_COMP_READ_BULK_BKUP;
        }

        // Write/restore the HWM chunk to the applicable database file
        rc = fDbFile.writeFile(pFile, chunk, restoredChunkLen);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error writing to column file" <<
                "; offset-" << fileOffsetByteForRestoredChunk <<
                "; bytes-"  << restoredChunkLen               <<
                "; "        << ec.errorString(rc);
            errMsg = oss.str();

            delete backupFile;
            return rc;
        }
    }

    delete backupFile;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Return true/false depending on whether the applicable backup chunk file can
// be found to restore a backed up compressed chunk back into a db file.  If
// the backup file is not found, we assume that it's because one was not created
// and thus not needed.
//------------------------------------------------------------------------------
bool BulkRollbackFileCompressed::doWeReInitExtent( OID columnOID,
    uint32_t dbRoot,
    uint32_t partNum,
    uint32_t segNum) const
{
    std::ostringstream oss;
    oss << "/" << columnOID << ".p" << partNum << ".s" << segNum;
    std::string bulkRollbackSubPath( fMgr->getMetaFileName() );
    bulkRollbackSubPath += DATA_DIR_SUFFIX;
    bulkRollbackSubPath += oss.str();

    if ( !IDBPolicy::exists( bulkRollbackSubPath.c_str() ) )
    {
        return false;
    }

    return true;
}

//------------------------------------------------------------------------------
// Read requested number of bytes from the specified pFile into "buffer".
// Added this function as part of hdfs port, because IDBDataFile::read()
// may not return all the requested data in the first call to read().
//------------------------------------------------------------------------------
size_t BulkRollbackFileCompressed::readFillBuffer(
    IDBDataFile* pFile,
    char*        buffer,
    size_t       bytesReq) const
{
    char*   pBuf = buffer;
    ssize_t nBytes;
    size_t  bytesToRead = bytesReq;
    size_t  totalBytesRead = 0;

    while (1)
    {
        nBytes = pFile->read(pBuf, bytesToRead);
        if (nBytes > 0)
            totalBytesRead += nBytes;
        else
            break;

        if ((size_t)nBytes == bytesToRead)
            break;

        pBuf        += nBytes;
        bytesToRead  =  bytesToRead - (size_t)nBytes;
    }

    return totalBytesRead;
}

} //end of namespace
