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
 * $Id: we_colbufmgr.cpp 4726 2013-08-07 03:38:36Z bwilkinson $
 *
 ****************************************************************************/

/** @file
 * ColumnBufferManager methods used to coordinate writes to db column files.
 *
 * ColumnBufferManager methods collect data in an internal buffer from
 * multiple threads, and then periodically flushes the buffer to the relevant
 * DB column segment file.  However, if the output is to be compressed, then
 * instead of flushing the buffer to disk, the data is instead copied to a to-
 * be-compressed buffer, where it is collected and later flushed to disk by
 * ColumnBufferCompressed.
 */

#include "we_colbufmgr.h"
#include "we_colbuf.h"
#include "we_colbufcompressed.h"
#include "we_columninfo.h"
#include "we_bulkstatus.h"
#include "we_log.h"
#include "blocksize.h"
#include <sstream>
#include <boost/date_time/posix_time/posix_time_types.hpp>

namespace
{
   // Minimum time to wait for a condition, so as to periodically wake up and
   // check the global job status, to see if the job needs to terminate.
   const int COND_WAIT_SECONDS = 3;
}

namespace WriteEngine {

//------------------------------------------------------------------------------
// ColumnBufferManger constructor
//------------------------------------------------------------------------------
ColumnBufferManager::ColumnBufferManager(ColumnInfo* pColInfo,
    int width, Log* logger, int compressionType) :
    fBufWriteOffset(0), fBufFreeOffset(0), fResizePending(false),
    fColWidth(width),
    fMaxRowId(std::numeric_limits<WriteEngine::RID>::max()),
    fColInfo(pColInfo), fLog(logger) {
    if (compressionType)
        fCBuf = new ColumnBufferCompressed(pColInfo, logger);
    else
        fCBuf = new ColumnBuffer(pColInfo, logger);
}

//------------------------------------------------------------------------------
// ColumnBufferManger destructor
//------------------------------------------------------------------------------
ColumnBufferManager::~ColumnBufferManager() {
    if (fCBuf)
        delete fCBuf;
}

//------------------------------------------------------------------------------
// Reserve a section out of the output buffer for the application code to begin
// using to store data destined for the database.
// startRowId (in) - starting RID for the rows to be stored in this section
// nRowsIn    (in) - desired number of rows to be stored in this section
// secRowCnt (out) - number of rows that the returned ColumnBufferSection can
//                   hold.  This may be smaller than nRowsIn (for dictionary
//                   column) if the rows cross an extent boundary.
//                   The application code will need to make a second call
//                   later, to create a section to handle the remaining rows
//                   that this section could not hold.
// cbs (out)       - reserved section pointer
// lastInputRowInExtent(out) - last input Row number (relative to start of job)
//                   that can go into the current extent we are loading.
//
// Basic algorithm:
// 1. Wait to ensure that ColumnBufferSection allocations are made in input
//    row order
// 2. Wait for any pending output buffer expansion to be completed
// 3. If buffer does not have enough room to contain nRowsIn of data, then
//    a. If there are any ColumnBufferSections in use, then
//         wait for these ColumnBufferSections to be released
//    b. If there is any data in the output buffer, then
//         flush the data (rounded down to nearest 8192-byte block)
//    c. Expand the output buffer, and
//         copy the remaining data to the front of the newly expanded buffer
//
//    @bug 3456
//    Note that when determining whether the output buffer has enough room or
//    not, we are comparing to remainingSpace-1 and not remainingSpace.  We
//    don't want output buffer to get completely full, because we can't
//    distinguish between a buffer that is full, and one that is empty;
//    as both cases are indicated by fBufFreeOffset == fBufWriteOffset.
//    If we allow output buffer to get exactly full, then we end up
//    losing track of data because we will think the buffer is empty.
//
// 6. Allocate new ColumnBufferSection
// 7. Update fBufFreeOffset, fSectionsInUse, fMaxRowId
//------------------------------------------------------------------------------
int ColumnBufferManager::reserveSection(
    RID startRowId,
    uint32_t  nRowsIn,
    uint32_t& secRowCnt,
    ColumnBufferSection** cbs,
    RID&  lastInputRowInExtent) {
#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_WAIT_TO_RESERVE_OUT_BUF);
#endif
    *cbs = 0;
    boost::posix_time::seconds wait_seconds(COND_WAIT_SECONDS);

    boost::mutex::scoped_lock lock(fColInfo->colMutex());

    //..Ensure that ColumnBufferSection allocations are made in input row order
    bool bWaitedForInSequence = false;
    while (1) {
        RID startRowTest = (std::numeric_limits<WriteEngine::RID>::max() ==
                            fMaxRowId) ? 0 : fMaxRowId + 1;

        if (startRowTest == startRowId)  
            break;

        if (fLog->isDebug( DEBUG_3 )) {
            bWaitedForInSequence = true;
            std::ostringstream oss;
            oss << "OID-" << fColInfo->curCol.dataFile.fid <<
                "; Waiting for in-sequence";
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        fOutOfSequence.timed_wait(lock, wait_seconds);

        // See if JobStatus has been set to terminate by another thread
        if (BulkStatus::getJobStatus() == EXIT_FAILURE)
        {
            throw SecondaryShutdownException( "ColumnBufferManager::"
                "reserveSection(1) responding to job termination");
        }
    }

    if (fLog->isDebug( DEBUG_3 )) {
        if (bWaitedForInSequence) {
            std::ostringstream oss;
            oss << "OID-" << fColInfo->curCol.dataFile.fid <<
                "; Resume after waiting for in-sequence";
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
    }

    //..Check/wait for any pending output buffer expansion to be completed
    bool bWaitedForResize = false;
    while (fResizePending) {
        if (fLog->isDebug( DEBUG_3 )) {
            bWaitedForResize = true;
            std::ostringstream oss;
            oss << "OID-" << fColInfo->curCol.dataFile.fid <<
               "; Waiting for pending resize";
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        fResizeInProgress.timed_wait(lock, wait_seconds);

        // See if JobStatus has been set to terminate by another thread
        if (BulkStatus::getJobStatus() == EXIT_FAILURE)
        {
            throw SecondaryShutdownException( "ColumnBufferManager::"
                "reserveSection(2) responding to job termination");
        }
    }

    if (fLog->isDebug( DEBUG_3 )) {
        if (bWaitedForResize) {
            std::ostringstream oss;
            oss << "OID-" << fColInfo->curCol.dataFile.fid <<
                "; Resume after waiting for pending resize";
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
    }

#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_WAIT_TO_RESERVE_OUT_BUF);
#endif

    // Through the use of the mutex lock and the fResizePending flag, nobody
    // should be changing the buffer size out from under us; so okay to save in
    // local variable till we call resizeColumnBuffer() to expand the buffer.
    int bufferSize = fCBuf->getSize();
    int remainingSpace = 0;

    if(bufferSize > 0) {
        //Calculate remaining space
        remainingSpace = bufferSize -
            (fBufFreeOffset + bufferSize - fBufWriteOffset) % bufferSize;
    }

    //..Restrict the new section to the extent boundary if applicable.
    //  We assume here that the colMutex() lock will assure the integrity
    //  of the values used in evaluating or recalculating spaceRequired.
    int nRows = 0;
    RETURN_ON_ERROR( rowsExtentCheck( nRowsIn, nRows ) );

    int spaceRequired = nRows * fColWidth;

    if (nRows > 0) {
        //..If not enough room to add nRows to output buffer, wait for pending
        //  sections to be released, so that we can flush and resize the buffer.
        //..@bug 3456: compare to remainingSpace-1 and not remainingSpace.
        //  See note in function description that precedes this function.
        if (spaceRequired > (remainingSpace-1)) {
//#ifdef PROFILE
//          Stats::startParseEvent(WE_STATS_WAIT_TO_RESIZE_OUT_BUF);
//#endif
            fResizePending = true;
            bool bWaitedForSectionsInUse = false;

            // Wait for all other threads to finish writing pending sections
            // to the output buffer, before we resize the buffer
            while(fSectionsInUse.size() > 0) {
                if (fLog->isDebug( DEBUG_3 )) {
                    bWaitedForSectionsInUse = true;
                    std::ostringstream oss;
                    oss << "OID-" << fColInfo->curCol.dataFile.fid <<
                        "; Waiting to resize output buffer; "
                        "sections in-use: " <<
                        fSectionsInUse.size();
                    fLog->logMsg( oss.str(), MSGLVL_INFO2 );
                }
                fBufInUse.timed_wait(lock, wait_seconds);

                // See if JobStatus has been set to quit by another thread
                if (BulkStatus::getJobStatus() == EXIT_FAILURE)
                {
                    throw SecondaryShutdownException( "ColumnBufferManager::"
                        "reserveSection(3) responding to job termination");
                }
            }
//#ifdef PROFILE
//          Stats::stopParseEvent(WE_STATS_WAIT_TO_RESIZE_OUT_BUF);
//          Stats::startParseEvent(WE_STATS_RESIZE_OUT_BUF);
//#endif

            if (fLog->isDebug( DEBUG_3 )) {
                if (bWaitedForSectionsInUse) {
                    std::ostringstream oss;
                    oss << "OID-" << fColInfo->curCol.dataFile.fid <<
                        "; Resume after waiting to resize output buffer";
                    fLog->logMsg( oss.str(), MSGLVL_INFO2 );
                }
            }

            // @bug 1977 correct problem; writing extra blocks
            // Flush remaining data blocks to disk "if" buffer contains data
            if(bufferSize > 0) {
                if (fBufFreeOffset != fBufWriteOffset)
                    RETURN_ON_ERROR( writeToFile(
                        (fBufFreeOffset + bufferSize - 1)%bufferSize) );
            }

            resizeColumnBuffer(spaceRequired);
            bufferSize = fCBuf->getSize(); // update bufferSize after resize-
                                           // ColumnBuffer() expanded the buffer
            fResizePending = false;
            fResizeInProgress.notify_all();
//#ifdef PROFILE
//          Stats::stopParseEvent(WE_STATS_RESIZE_OUT_BUF);
//#endif
        } // (spaceRequired > remainingSpace-1)

        *cbs = new ColumnBufferSection(
            fCBuf, startRowId, startRowId + nRows - 1,
            fColWidth, fBufFreeOffset);
        fBufFreeOffset = (fBufFreeOffset + nRows * fColWidth) % bufferSize;
        fSectionsInUse.push_back(*cbs);

        fMaxRowId = startRowId + nRows - 1;
        fOutOfSequence.notify_all();
    } // (nRows > 0)

    secRowCnt = nRows;

    // Get/return last input Row number for the extent this buffer goes in.
    // If we determine this set of rows will cross over to the next extent,
    // then we tell ColumnInfo to bump the last Row to the end of the next
    // extent, in preparation for the next Read buffer going into the next
    // extent.  We do this even though we have not yet allocated the next
    // extent from the extent map.
    lastInputRowInExtent = fColInfo->lastInputRowInExtent( );
    if ((startRowId + nRowsIn) > lastInputRowInExtent)
        fColInfo->lastInputRowInExtentInc( );

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Release the data in the specified ColumnBufferSection, meaning the data in
// that section is ready to be written to the database.
//------------------------------------------------------------------------------
int ColumnBufferManager::releaseSection(ColumnBufferSection* cbs) {
#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_WAIT_TO_RELEASE_OUT_BUF);
#endif
    boost::mutex::scoped_lock lock(fColInfo->colMutex());
#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_WAIT_TO_RELEASE_OUT_BUF);
#endif
    cbs->setStatus(WRITE_COMPLETE);
     
    int lastWriteOffset = fBufWriteOffset;

    std::list<ColumnBufferSection*>::iterator it = fSectionsInUse.begin();
    if (it != fSectionsInUse.end())
    {
        ColumnBufferSection* cbs_temp = *it;
        while (WRITE_COMPLETE == cbs_temp->getStatus())
        {
            lastWriteOffset = cbs_temp->getStartOffset() +
                              cbs_temp->getSectionSize() - 1;

            delete cbs_temp;
            it = fSectionsInUse.erase(it);
            if (it == fSectionsInUse.end())
                break;
            cbs_temp = *it;
        }
    }
    fBufInUse.notify_all();

    RETURN_ON_ERROR( writeToFile(lastWriteOffset) );

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Expand the output buffer for the column this ColumBufferManager is managing.
// "spaceRequired" indicates the number of additional bytes that are needed.
//------------------------------------------------------------------------------
void ColumnBufferManager::resizeColumnBuffer(int spaceRequired) {
    int bufferSize = fCBuf->getSize();
    int bufferSizeOld = bufferSize;
    int dataRemaining = (bufferSize > 0) ?
        ((fBufFreeOffset - fBufWriteOffset + bufferSize) % bufferSize) : 0;

    int resizeAction = 0;

    if(0 == bufferSize) {
        bufferSize = (int)(spaceRequired * 1.2); //Additional 20% to account
                                                //for changes in number of rows
                                                //because of varying line-widths
        resizeAction = 1;
    } else {
        if(spaceRequired > bufferSize) {
            bufferSize = spaceRequired * 2;
            resizeAction = 2;
        } else {
            bufferSize *= 2; //Double the buffer size
            resizeAction = 3;
        }
    }

    //Round off the bufferSize to size of a disk block
    if(bufferSize % BLOCK_SIZE > 0) {
        bufferSize = (((int)(bufferSize/BLOCK_SIZE)) + 1) * BLOCK_SIZE;
    }
    if (resizeAction > 0) {
        if (fLog->isDebug( DEBUG_2 )) {
            RID numRowsInBuffer = dataRemaining / fColWidth;
            RID firstRid        = fMaxRowId - numRowsInBuffer + 1;

            std::ostringstream oss;
            oss << "Resizing out buffer (case"     <<
                   resizeAction << ") for OID-"    <<
                   fColInfo->curCol.dataFile.fid   <<
                   "; oldSize-"  << bufferSizeOld  <<
                   "; freeOff-"  << fBufFreeOffset <<
                   "; writeOff-" << fBufWriteOffset<<
                   "; startRID-" << firstRid       <<
                   "; rows-"     << numRowsInBuffer<<
                   "; reqBytes-" << spaceRequired  <<
                   "; newSize-"  << bufferSize;
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
    }
        
    // @bug 1977 correct problem; writing extra blocks
    // If we have no data in buffer, we still call resizeAndCopy()
    // to expand the buffer; we just pass -1 for the buffer offsets.
    if (fBufFreeOffset == fBufWriteOffset)
    {
        fCBuf->resizeAndCopy(bufferSize, -1, -1);
    }
    else
    {
        int endOffset = (fBufFreeOffset + bufferSize - 1) % bufferSize;
        fCBuf->resizeAndCopy(bufferSize, fBufWriteOffset, endOffset);
    }
    fBufFreeOffset = dataRemaining;
    fBufWriteOffset = 0;
}

//------------------------------------------------------------------------------
// Writes data from our output buffer starting at fBufWriteOffset, through the
// "endOffset" that is specified (rounded down to nearest 8192-byte block).
// Trailing data (less than 1 block) is left in the output buffer.
// Keep in mind that the output buffer is a circular buffer, so if the
// applicable bytes "wrap-around" in the buffer, then this function will
// perform 2 I/O operations to write the 2 noncontiguous chunks of data.
//------------------------------------------------------------------------------
int ColumnBufferManager::writeToFile(int endOffset) {
    int bufferSize = fCBuf->getSize();

    if (endOffset == fBufWriteOffset)
        return NO_ERROR;

    unsigned int writeSize =
        (endOffset - fBufWriteOffset + bufferSize)%bufferSize + 1;

    // Don't bother writing anything if we don't at least have a BLOCK_SIZE
    // set of bytes to write out; which means we need to be sure to flush
    // the buffer at the end, because we could have leftover bytes that we
    // have not yet written out.
    if (writeSize < BLOCK_SIZE)
        return NO_ERROR;

    writeSize = writeSize - writeSize%BLOCK_SIZE; //round down to mult of blksiz
    endOffset = (fBufWriteOffset + writeSize - 1) % bufferSize;

    if (fLog->isDebug( DEBUG_3 )) {
        std::ostringstream oss;
        oss << "Writing OID-" << fColInfo->curCol.dataFile.fid <<
            "; bufWriteOff-"      << fBufWriteOffset <<
            "; bufFreeOff-"       << fBufFreeOffset  <<
            "; endWrite-"         << endOffset <<
            "; bytesToWrite-"     << writeSize <<
            "; bufSize-"          << bufferSize;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    // Account for circular buffer by making 2 calls to write the data,
    // if we are wrapping around at the end of the buffer.
    if(endOffset < fBufWriteOffset) {
        RETURN_ON_ERROR( writeToFileExtentCheck(
            fBufWriteOffset, bufferSize - fBufWriteOffset) );
        fBufWriteOffset = 0;
    }
    RETURN_ON_ERROR( writeToFileExtentCheck(
        fBufWriteOffset, endOffset - fBufWriteOffset + 1) );
    fBufWriteOffset = (endOffset + 1)%bufferSize;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Writes the specified bytes from the internal buffer to the db column file.
// The data to be written, starts at "startOffset" in the internal buffer and
// is "writeSize" bytes long.
// This function also checks to see if an extent needs to be added to the db
// column file for this number of bytes.  If a second extent is required,
// then the current db file will be filled out with the 1st part of the buffer,
// and the remaining buffer data will be written to the next segment file in
// the DBRoot, partition, segement number sequence.
// This function also catches and handles the case where an abbreviated
// extent needs to be expanded to a full extent on disk.
//
// WARNING: This means this function may change the information in the
//          ColumnInfo struct that owns this ColumnBufferManager, if a
//          second db column file has to be opened to finish writing the
//          internal buffer, or if an abbreviated extent is expanded.
//------------------------------------------------------------------------------
int ColumnBufferManager::writeToFileExtentCheck(
    uint32_t startOffset, uint32_t writeSize) {

    if (fLog->isDebug( DEBUG_3 )) {
        std::ostringstream oss;
        oss << "Col extent check: OID-" <<
               fColInfo->curCol.dataFile.fid <<
               "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
               "; part-"   << fColInfo->curCol.dataFile.fPartition <<
               "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
               "; Wanting to write "       << writeSize <<
               " bytes, with avail space " << fColInfo->availFileSize;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    // Don't need a mutex lock here because if writeToFile() is calling
    // us, we already have a lock; and if flush() is calling us, then
    // all parsing is complete, so we should have no thread contention.

    // If extent out of space, see if this is an abbrev extent we can expand
    long long availableFileSize = fColInfo->availFileSize;
    if ((availableFileSize < writeSize) && (fColInfo->isAbbrevExtent())) {
        int rc = fColInfo->expandAbbrevExtent(true);
        if (rc != NO_ERROR) {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "writeToFileExtentCheck: expand extent failed: " <<
                   ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }

        availableFileSize = fColInfo->availFileSize;
    }

    if (availableFileSize >= writeSize) {
        int rc = fCBuf->writeToFile(startOffset, writeSize);
        if (rc != NO_ERROR) {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "writeToFileExtentCheck: write1 extent failed: " <<
                   ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }
        fColInfo->updateBytesWrittenCounts( writeSize );
    }
    else {
        // We use ColumnInfo to help us add an extent to the "next"
        // segment file, if needed.
        // Current extent does not have enough room for buffer, so we
        // have to break up the buffer into 2 extents; creating a new
        // extent and switching the db column file "on-the-fly".
        int writeSize1 = availableFileSize;
        if (writeSize1 > 0)
        {
            int rc = fCBuf->writeToFile(startOffset, writeSize1);
            if (rc != NO_ERROR) {
                WErrorCodes ec;
                std::ostringstream oss;
                oss << "writeToFileExtentCheck: write2 extent failed: " <<
                       ec.errorString(rc);
                fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
                return rc;
            }
            fColInfo->updateBytesWrittenCounts( writeSize1 );
        }

        int rc = fColInfo->extendColumn( true );
        if (rc != NO_ERROR) {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "writeToFileExtentCheck: extend column failed: " <<
                   ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }

        int writeSize2 = writeSize - writeSize1;
        fCBuf->writeToFile(startOffset+writeSize1, writeSize2);
        if (rc != NO_ERROR) {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "writeToFileExtentCheck: write3 extent failed: " <<
                   ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }
        fColInfo->updateBytesWrittenCounts( writeSize2 );
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Flush the contents of internal fCBuf (column buffer) to disk.
//------------------------------------------------------------------------------
int ColumnBufferManager::flush( ) {

    if(fBufFreeOffset == fBufWriteOffset) {
        if (fLog->isDebug( DEBUG_2 )) {
            std::ostringstream oss;
            oss << "Skipping write flush for: OID-" <<
                fColInfo->curCol.dataFile.fid <<
                "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
                "; part-"   << fColInfo->curCol.dataFile.fPartition <<
                "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
                "; both fBufFreeOffset and fBufWriteOffset = "      <<
                fBufFreeOffset;
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
        return NO_ERROR;
    }
    int bufferSize = fCBuf->getSize();

    // Account for circular buffer by making 2 calls to write the data,
    // if we are wrapping around at the end of the buffer.
    if(fBufFreeOffset < fBufWriteOffset) {
        RETURN_ON_ERROR( writeToFileExtentCheck(
            fBufWriteOffset, bufferSize - fBufWriteOffset) );
        fBufWriteOffset = 0;
    }
    RETURN_ON_ERROR( writeToFileExtentCheck(
        fBufWriteOffset, fBufFreeOffset - fBufWriteOffset) );
    fBufWriteOffset = fBufFreeOffset;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// In the middle of parsing a Read buffer, if we detect that the buffer will
// need to be split among 2 different extents, then this function should be
// called to flush the first part of the buffer into the first extent, before
// writing the second part of the buffer into the second extent.  This function
// will wait for the pending sections to be parsed so that they can all be
// flushed to the segment file containing the first extent.  This function is
// currently only needed for Dictionary column processing.
//------------------------------------------------------------------------------
int ColumnBufferManager::intermediateFlush() {
    boost::posix_time::seconds wait_seconds(COND_WAIT_SECONDS);
    boost::mutex::scoped_lock lock(fColInfo->colMutex());

    // Wait for all other threads which are currently parsing rows,
    // to finish parsing the data in those sections.
#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_WAIT_FOR_INTERMEDIATE_FLUSH);
#endif
    while(fSectionsInUse.size() > 0) {
        fBufInUse.timed_wait(lock, wait_seconds);

        // See if JobStatus has been set to terminate by another thread
        if (BulkStatus::getJobStatus() == EXIT_FAILURE)
        {
            throw SecondaryShutdownException( "ColumnBufferManager::"
                "intermediateFlush() responding to job termination");
        }
    }
#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_WAIT_FOR_INTERMEDIATE_FLUSH);
#endif

    RETURN_ON_ERROR( flush( ) );

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// This function is a no-op for non-dictionary columns, but it is provided as
// a hook for ColumnBufferManagerDctnry to provide a function that adds extents
// as sections from the output buffer are being copied to the column segment
// file(s).
//------------------------------------------------------------------------------
int ColumnBufferManager::rowsExtentCheck( int nRows, int& nRows2 ) {
    nRows2 = nRows;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// This wrapper function wraps a mutex lock around the call to extendColumn().
// For typical numeric column processing, extendColumn() is called from within
// ColumnBufferManager::writeToFileExtentCheck() which already has a mutex lock.
// But dictionary token processing detects/extends a token column outside of
// ColumnBufferManager; hence the need for this function to be called to employ
// a mutex lock around the call to extendColumn().
//------------------------------------------------------------------------------
int ColumnBufferManager::extendTokenColumn( )
{
    boost::mutex::scoped_lock lock(fColInfo->colMutex());

    return fColInfo->extendColumn( false );
}

}
