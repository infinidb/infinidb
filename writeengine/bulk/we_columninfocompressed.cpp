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
* $Id: we_columninfocompressed.cpp 4726 2013-08-07 03:38:36Z bwilkinson $
*
*******************************************************************************/

#include "we_columninfocompressed.h"

#include "we_define.h"
#include "we_log.h"
#include "we_type.h"
#include "we_rbmetawriter.h"
#include "we_stats.h"

#include "we_tableinfo.h"

#include "idbcompress.h"
using namespace compress;

#include "IDBFileSystem.h"

#include <iostream>

namespace WriteEngine
{

//------------------------------------------------------------------------------
// ColumnInfoCompressed constructor
//------------------------------------------------------------------------------
ColumnInfoCompressed::ColumnInfoCompressed(Log*             logger,
                                           int              idIn,
                                           const JobColumn& columnIn,
                                           DBRootExtentTracker* pDBRootExtTrk,
                                           TableInfo*		pTableInfo):
                                           //RBMetaWriter*    rbMetaWriter) :
    ColumnInfo(logger, idIn, columnIn, pDBRootExtTrk, pTableInfo),
    fRBMetaWriter(pTableInfo->rbMetaWriter())
{
}

//------------------------------------------------------------------------------
// ColumnInfoCompressed destructor
//------------------------------------------------------------------------------
ColumnInfoCompressed::~ColumnInfoCompressed()
{
}

//------------------------------------------------------------------------------
// Close the current compressed Column file after first compressing/flushing
// any remaining data, and re-writing the headers as well.
//------------------------------------------------------------------------------
int ColumnInfoCompressed::closeColumnFile(bool bCompletingExtent,bool bAbort)
{
    int rc = NO_ERROR;

    if ( curCol.dataFile.pFile )
    {
        if (!bAbort)
        {
            // If we are opening and closing a file in order to add an extent as
            // part of preliminary block skipping, then we won't have a Column-
            // BufferManger object yet.  One will be created when the file is
            // reopened to begin importing.
            if (fColBufferMgr)
            {
                rc = fColBufferMgr->finishFile( bCompletingExtent );
                if (rc != NO_ERROR)
                {
                    WErrorCodes ec;
                    std::ostringstream oss;
                    oss << "Error closing compressed file; OID-"  <<
                        curCol.dataFile.fid <<
                        "; DBRoot-" << curCol.dataFile.fDbRoot    <<
                        "; part-"   << curCol.dataFile.fPartition <<
                        "; seg-"    << curCol.dataFile.fSegment   <<
                        "; "        << ec.errorString(rc);
                    fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
                    bAbort = true;
                }
            }
        }

        ColumnInfo::closeColumnFile(bCompletingExtent, bAbort);
    }

    return rc;
}

//------------------------------------------------------------------------------
// Prepare the initial compressed column segment file for import.
//------------------------------------------------------------------------------
int ColumnInfoCompressed::setupInitialColumnFile( HWM oldHwm, HWM hwm )
{
    char hdr[ compress::IDBCompressInterface::HDR_BUF_LEN * 2 ];
    RETURN_ON_ERROR( colOp->readHeaders(curCol.dataFile.pFile, hdr) );

    // Initialize the output buffer manager for the column.
    WriteEngine::ColumnBufferManager *mgr;
    if (column.colType == COL_TYPE_DICT)
    {
        mgr = new ColumnBufferManagerDctnry(
            this, 8, fLog, column.compressionType);
        RETURN_ON_ERROR( mgr->setDbFile(curCol.dataFile.pFile, hwm, hdr) );
    }
    else
    {
        mgr = new ColumnBufferManager(
            this, column.width, fLog, column.compressionType);
        RETURN_ON_ERROR( mgr->setDbFile(curCol.dataFile.pFile, hwm, hdr) );
    }
    fColBufferMgr = mgr;

    IDBCompressInterface compressor;
    int abbrevFlag =
        ( compressor.getBlockCount(hdr) ==
            uint64_t(INITIAL_EXTENT_ROWS_TO_DISK*column.width/BYTE_PER_BLOCK) );
    setFileSize( hwm, abbrevFlag );

    // See if dealing with abbreviated extent that will need expanding.
    // This only applies to the first extent of the first segment file.
    setAbbrevExtentCheck();

    // If we are dealing with initial extent, see if block skipping has
    // exceeded disk allocation, in which case we expand to a full extent.
    if (isAbbrevExtent())
    {
        unsigned int numBlksForFirstExtent =
          (INITIAL_EXTENT_ROWS_TO_DISK*column.width) / BYTE_PER_BLOCK;
        if ( ((oldHwm+1) <= numBlksForFirstExtent) &&
             ((hwm+1   ) >  numBlksForFirstExtent) )
        {
            RETURN_ON_ERROR( expandAbbrevExtent(false) );
        }
    }

    // Store the current allocated file size in availFileSize.
    // Keep in mind, these are raw uncompressed offsets.
    // NOTE: We don't call setFileOffset() to set the file position in the
    // column segment file at this point; we wait till we load the compressed
    // buffer later on in ColumnBufferCompressed::initToBeCompressedBuffer()
    long long byteOffset = (long long)hwm * (long long)BYTE_PER_BLOCK;

    fSizeWritten      = byteOffset;
    fSizeWrittenStart = fSizeWritten;
    availFileSize     = fileSize - fSizeWritten;

    if (fLog->isDebug( DEBUG_1 ))
    {
        std::ostringstream oss;
        oss << "Init raw data offsets in compressed column file OID-" <<
                curCol.dataFile.fid <<
            "; DBRoot-" << curCol.dataFile.fDbRoot    <<
            "; part-"   << curCol.dataFile.fPartition <<
            "; seg-"    << curCol.dataFile.fSegment   <<
            "; abbrev-" << abbrevFlag   <<
            "; begByte-"<< fSizeWritten <<
            "; endByte-"<< fileSize     <<
            "; freeBytes-" << availFileSize;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Reinitializes ColBuf buffer, and resets
// file offset data member attributes where new extent will start.
//------------------------------------------------------------------------------
int ColumnInfoCompressed::resetFileOffsetsNewExtent(const char* hdr)
{
    setFileSize( curCol.dataFile.hwm, false );
    long long byteOffset = (long long)curCol.dataFile.hwm *
                           (long long)BYTE_PER_BLOCK;
    fSizeWritten      = byteOffset;
    fSizeWrittenStart = fSizeWritten;
    availFileSize     = fileSize - fSizeWritten;

    // If we are adding an extent as part of preliminary block skipping, then
    // we won't have a ColumnBufferManager object yet, but that's okay, because
    // we are only adding the empty extent at this point.
    if (fColBufferMgr)
    {
        RETURN_ON_ERROR( fColBufferMgr->setDbFile(curCol.dataFile.pFile,
                         curCol.dataFile.hwm, hdr) );

        // Reinitialize ColBuf for the next extent
        long long startFileOffset;
        RETURN_ON_ERROR( fColBufferMgr->resetToBeCompressedColBuf(
                         startFileOffset ) );

        // Set the file offset to point to the chunk we are adding or updating
        RETURN_ON_ERROR( colOp->setFileOffset(curCol.dataFile.pFile,
                         startFileOffset) );
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Save HWM chunk for compressed dictionary store files, so that the HWM chunk
// can be restored by bulk rollback if an error should occur.
//------------------------------------------------------------------------------
// @bug 5572 - HDFS usage: add flag used to control *.tmp file usage
int ColumnInfoCompressed::saveDctnryStoreHWMChunk(bool& needBackup)
{
#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_COMPRESS_DCT_BACKUP_CHUNK);
#endif
    needBackup = false;
    int rc = NO_ERROR;
    try
    {
        needBackup = fRBMetaWriter->backupDctnryHWMChunk(
            column.dctnry.dctnryOid,
            curCol.dataFile.fDbRoot,
            curCol.dataFile.fPartition,
            curCol.dataFile.fSegment );
    }
    catch (WeException& ex)
    {
        fLog->logMsg(ex.what(), ex.errorCode(), MSGLVL_ERROR);
        rc = ex.errorCode();
    }
#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_COMPRESS_DCT_BACKUP_CHUNK);
#endif
        
    return rc;
}

//------------------------------------------------------------------------------
// Truncate specified dictionary store file for this column.
// Only applies to compressed columns.
//
// This function may logically belong in a dictionary related class, but I did
// not particularly want to put a bulk import specific function in Dctnry-
// Compress1 (a wrapper class shared with DML/DDL) or Dctnry, so I put it here.
// May change my mind later.
//
// dmc-Not the most efficient implementation.  We are reopening
// the file to perform the truncation, instead of truncating the file before
// we close it.  This is done because we need to first flush the compressed
// chunks before we can determine the truncation file size.  But the Chunk-
// Manager flushChunks() function immediately closes the file and clears itself
// after if flushes the data.  So by the time we get back to the application
// code it's too late to truncate the file.  At some point, we could look at
// adding or changing the ChunkManager API to support a flush w/o a close.
// That would be more optimum than having to reopen the file for truncation.
//------------------------------------------------------------------------------
int ColumnInfoCompressed::truncateDctnryStore(
    OID dctnryOid, uint16_t root, uint32_t pNum, uint16_t sNum) const
{
    int rc = NO_ERROR;

    // @bug5769 Don't initialize extents or truncate db files on HDFS
    if (idbdatafile::IDBPolicy::useHdfs())
    {
        std::ostringstream oss1;
        oss1 << "Finished writing dictionary file"
            ": OID-"    << dctnryOid <<
            "; DBRoot-" << root      <<
            "; part-"   << pNum      <<
            "; seg-"    << sNum;

        // Have to rework this logging if we want to keep it.
        // Filesize is not correct when adding data to an "existing" file,
        // since in the case of HDFS, we are writing to a *.cdf.tmp file.
        //char dctnryFileName[FILE_NAME_SIZE];
        //if (colOp->getFileName(dctnryOid,dctnryFileName,
        //    root, pNum, sNum) == NO_ERROR)
        //{
        //    off64_t dctnryFileSize = idbdatafile::IDBFileSystem::getFs(
        //        IDBDataFile::HDFS).size(dctnryFileName);
        //    if (dctnryFileSize != -1)
        //    {
        //        oss1 << "; size-" << dctnryFileSize;
        //    }
        //}
        fLog->logMsg( oss1.str(), MSGLVL_INFO2 );
    }
    else
    {
        // See if the relevant dictionary store file can/should be truncated
        // (to the nearest extent)
        std::string segFile;
        IDBDataFile* dFile = fTruncateDctnryFileOp.openFile(dctnryOid,
            root, pNum, sNum, segFile);
        if (dFile == 0)
        {
            rc = ERR_FILE_OPEN;

            std::ostringstream oss;
            oss << "Error opening compressed dictionary store segment "
                "file for truncation" <<
                ": OID-"       << dctnryOid <<
                "; DbRoot-"    << root <<
                "; partition-" << pNum <<
                "; segment-"   << sNum;
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

            return rc;
        }

        char controlHdr[ IDBCompressInterface::HDR_BUF_LEN ];
        rc = fTruncateDctnryFileOp.readFile( dFile,
            (unsigned char*)controlHdr, IDBCompressInterface::HDR_BUF_LEN);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error reading compressed dictionary store control hdr "
                "for truncation" <<
                ": OID-"       << dctnryOid <<
                "; DbRoot-"    << root <<
                "; partition-" << pNum <<
                "; segment-"   << sNum <<
                "; "           << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            fTruncateDctnryFileOp.closeFile( dFile );

            return rc;
        }

        IDBCompressInterface compressor;
        int rc1 = compressor.verifyHdr( controlHdr );
        if (rc1 != 0)
        {
            rc = ERR_COMP_VERIFY_HDRS;

            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error verifying compressed dictionary store ptr hdr "
                "for truncation" <<
                ": OID-"       << dctnryOid <<
                "; DbRoot-"    << root <<
                "; partition-" << pNum <<
                "; segment-"   << sNum <<
                "; (" << rc1 << ")";
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            fTruncateDctnryFileOp.closeFile( dFile );

            return rc;
        }

    // No need to perform file truncation if the dictionary file just contains
    // a single abbreviated extent.  Truncating up to the nearest extent would
    // actually grow the file (something we don't want to do), because we have
    // not yet reserved a full extent (on disk) for this dictionary store file.
        const int PSEUDO_COL_WIDTH = 8;
        uint64_t numBlocks = compressor.getBlockCount( controlHdr );
        if ( numBlocks == uint64_t
            (INITIAL_EXTENT_ROWS_TO_DISK*PSEUDO_COL_WIDTH/BYTE_PER_BLOCK) )
        {
            std::ostringstream oss1;
            oss1 << "Skip truncating abbreviated dictionary file"
                ": OID-"    << dctnryOid <<
                "; DBRoot-" << root      <<
                "; part-"   << pNum      <<
                "; seg-"    << sNum      <<
                "; blocks-" << numBlocks;
            fLog->logMsg( oss1.str(), MSGLVL_INFO2 );
            fTruncateDctnryFileOp.closeFile( dFile );

            return NO_ERROR;
        }

        uint64_t hdrSize    = compressor.getHdrSize(controlHdr);
        uint64_t ptrHdrSize = hdrSize - IDBCompressInterface::HDR_BUF_LEN;
        char*    pointerHdr = new char[ptrHdrSize];

        rc = fTruncateDctnryFileOp.readFile(dFile,
            (unsigned char*)pointerHdr, ptrHdrSize);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error reading compressed dictionary store pointer hdr "
                "for truncation" <<
                ": OID-"       << dctnryOid <<
                "; DbRoot-"    << root <<
                "; partition-" << pNum <<
                "; segment-"   << sNum <<
                "; "           << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            fTruncateDctnryFileOp.closeFile( dFile );

            return rc;
        }

        CompChunkPtrList chunkPtrs;
        rc1 = compressor.getPtrList( pointerHdr, ptrHdrSize, chunkPtrs );
        delete[] pointerHdr;
        if (rc1 != 0)
        {
            rc = ERR_COMP_PARSE_HDRS;

            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error parsing compressed dictionary store ptr hdr "
                "for truncation" <<
                ": OID-"       << dctnryOid <<
                "; DbRoot-"    << root <<
                "; partition-" << pNum <<
                "; segment-"   << sNum <<
                "; (" << rc1 << ")";
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            fTruncateDctnryFileOp.closeFile( dFile );

            return rc;
        }

        // Truncate the relevant dictionary store file to the nearest extent
        if (chunkPtrs.size() > 0)
        {
            long long dataByteLength = chunkPtrs[chunkPtrs.size()-1].first  +
                                       chunkPtrs[chunkPtrs.size()-1].second -
                                       hdrSize;                      

            long long extentBytes =
                fRowsPerExtent * PSEUDO_COL_WIDTH;

            long long rem = dataByteLength % extentBytes;
            if (rem > 0)
            {
                dataByteLength = dataByteLength - rem + extentBytes;
            }
            long long truncateFileSize = dataByteLength + hdrSize;

            std::ostringstream oss1;
            oss1 << "Truncating dictionary file"
                ": OID-"    << dctnryOid <<
                "; DBRoot-" << root      <<
                "; part-"   << pNum      <<
                "; seg-"    << sNum      <<
                "; size-"   << truncateFileSize;
            fLog->logMsg( oss1.str(), MSGLVL_INFO2 );

            if (truncateFileSize > 0)
                rc = fTruncateDctnryFileOp.truncateFile(dFile,truncateFileSize);
            else
                rc = ERR_COMP_TRUNCATE_ZERO;//@bug3913-Catch truncate to 0 bytes
            if (rc != NO_ERROR)
            {
                WErrorCodes ec;
                std::ostringstream oss;
                oss << "Error truncating compressed dictionary store file"
                    ": OID-"       << dctnryOid <<
                    "; DbRoot-"    << root <<
                    "; partition-" << pNum <<
                    "; segment-"   << sNum <<
                    "; "           << ec.errorString(rc);
                fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
                fTruncateDctnryFileOp.closeFile( dFile );

                return rc;
            }
        }

        fTruncateDctnryFileOp.closeFile( dFile );
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Fill out existing partial extent to extent boundary, so that we can resume
// inserting rows on an extent boundary basis.  This use case should only take
// place when a DBRoot with a partial extent has been moved from one PM to
// another.
//------------------------------------------------------------------------------
int ColumnInfoCompressed::extendColumnOldExtent(
    uint16_t    dbRootNext,
    uint32_t    partitionNext,
    uint16_t    segmentNext,
    HWM         hwmNextIn )
{
    const unsigned int BLKS_PER_EXTENT =
        (fRowsPerExtent * column.width)/BYTE_PER_BLOCK;

    // Round up HWM to the end of the current extent
    unsigned int nBlks = hwmNextIn + 1;
    unsigned int nRem  = nBlks % BLKS_PER_EXTENT;
    HWM hwmNext        = 0;
    if (nRem > 0)
        hwmNext = nBlks - nRem + BLKS_PER_EXTENT - 1;
    else
        hwmNext = nBlks - 1;

    std::ostringstream oss;  
    oss << "Padding compressed partial extent to extent boundary in OID-" <<
           curCol.dataFile.fid <<
        "; DBRoot-" << dbRootNext    <<
        "; part-"   << partitionNext <<
        "; seg-"    << segmentNext   <<
        "; hwm-"    << hwmNext;
    fLog->logMsg( oss.str(), MSGLVL_INFO2 );

    curCol.dataFile.pFile        = 0;
    curCol.dataFile.fDbRoot      = dbRootNext;
    curCol.dataFile.fPartition   = partitionNext;
    curCol.dataFile.fSegment     = segmentNext;
    curCol.dataFile.hwm          = hwmNext;
    curCol.dataFile.fSegFileName.clear();

    std::string segFileName;
    std::string errTask;
    int rc = colOp->fillCompColumnExtentEmptyChunks(
        curCol.dataFile.fid,
        curCol.colWidth,
        column.emptyVal,
        curCol.dataFile.fDbRoot,
        curCol.dataFile.fPartition,
        curCol.dataFile.fSegment,
        curCol.dataFile.hwm,
        segFileName,
        errTask);
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "extendColumnOldExtent: error padding extent (" <<
            errTask << "); " <<
            "column OID-" << curCol.dataFile.fid               <<
            "; DBRoot-"   << curCol.dataFile.fDbRoot           <<
            "; part-"     << curCol.dataFile.fPartition        <<
            "; seg-"      << curCol.dataFile.fSegment          <<
            "; newHwm-"   << curCol.dataFile.hwm               <<
            "; "          << ec.errorString(rc);
        fLog->logMsg( oss.str(), rc, MSGLVL_CRITICAL );
        fpTableInfo->fBRMReporter.addToErrMsgEntry(oss.str());
        return rc;
    }

    addToSegFileList( curCol.dataFile, hwmNext );

    return NO_ERROR;
}

}
