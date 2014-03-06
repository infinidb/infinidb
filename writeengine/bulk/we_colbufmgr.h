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
 * $Id: we_colbufmgr.h 4726 2013-08-07 03:38:36Z bwilkinson $
 *
 *****************************************************************************/

/** @file
 * class ColumnBufferManager
 */

#ifndef WRITEENGINE_COLBUFMGR_H
#define WRITEENGINE_COLBUFMGR_H

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <list>
#include <vector>

#include "we_colbuf.h"
#include "we_colbufsec.h"
#include "idbcompress.h"

namespace WriteEngine {
    class Log;

struct ColumnInfo;

/**
 * ColumnBufferManager class provides the functionality for multiple threads to
 * write the same column file concurrently. It assumes that all the data is
 * to be written sequentially and does not provide features that allow modi-
 * fication to a random column file location. The main object is to use in
 * bulk-load operations where data is loaded sequentially to a particular
 * column file.  If reserveSection() receives an allocation request for an
 * out-of-order RID, reserveSection() will wait for any earlier RID buffer
 * allocations to catch up.
 * If we are writing out compressed data, then the output buffer that is nor-
 * mally flushed to disk, is instead flushed to a to-be-compressed buffer (by
 * ColumnBufferCompressed), where it is eventually compressed and written to
 * disk.
 */

class ColumnBufferManager {
  public:

    //-------------------------------------------------------------------------
    // Public Functions
    //-------------------------------------------------------------------------

    /** @brief Constructor
     *
     * @param ColumnInfo object used to manage the addition of extents to
     * @param colWidth Width of the column
     * @param logger Logging object used for logging
     * @param compressionType Compression type
     */
    ColumnBufferManager(ColumnInfo* pColInfo, int colWidth,
                        Log* logger, int compressionType);

    /** @brief Default destructor
     */
    virtual ~ColumnBufferManager();

    /** @brief Reserve section of the buffer for this thread to write to
     *
     * @param startRowId (in) The starting row-id in the column that this
     *        section will write to
     * @param nRows (in) Requested size of the section (number of rows)
     * @param secRowCnt (out) Resulting size of the reserved section.
     *        May be less than nRows for the case where a group of
     *        dictionary rows crosses an extent boundary.
     * @param ColumnBufferSection (out) ptr with info on reserved section 
     * @param lastRIDInExtent (out) last RID in relevant extent.
     * @return success or fail status
     */
    int reserveSection(RID startRowId, uint32_t nRows,
                       uint32_t& secRowCnt,
                       ColumnBufferSection** cbs,
                       RID&  lastRIDInExtent);

    /** @brief Release a section that has been reserved earlier.
     * The ColumnBufferSection pointer will be invalid after
     * releaseSection() is called.  releaseSection() releases all memory
     * associated with the ColumnBufferSection and the client is not
     * expected to free any memory.
     *
     * @param cbs ColumnBufferSection pointer denoting the pointer that
     * needs to be released
     * @return success or fail status
     */
    int releaseSection(ColumnBufferSection* cbs);

    /** @brief Final flushing of data and headers prior to closing the file.
     * @param bTruncFile is file to be truncated
     * @return NO_ERROR or success
     */
    int finishFile(bool bTruncFile) { return fCBuf->finishFile(bTruncFile); }

    /** @brief Method to ensure that all the data in the buffer has been
     *  written to the file (for uncompressed), or the to-be-compressed buffer
     *  (for compressed).  In the case of compressed data, finishFile() is
     *  responsible for flushing any remaining data in the to-be-compressed
     *  buffer out to the db file.
     * @return NO_ERROR or success
     */
    int flush( );

    /** @brief Flush the buffer in the middle of parsing.
     *
     *  This is used when we want to split up a buffer that is about to
     *  cross an extent boundary.
     * @return NO_ERROR or success
     */
    int intermediateFlush();

    /** @brief Set the IDBDataFile** destination for the applicable col segment file.
     *
     * @param cFile IDBDataFile* of the output column segment file.
     * @param hwm Starting HWM for the file.
     * @param hdrs with ptr information (only applies to compressed files)
     */
    int setDbFile(IDBDataFile* const cFile, HWM hwm, const char* hdrs)
    { return fCBuf->setDbFile(cFile, hwm, hdrs); }

    /** @brief Reset the ColBuf to-be-compressed buffer prior to importing the
     *  next extent.
     */
    int resetToBeCompressedColBuf(long long& startFileOffset)
    { return fCBuf->resetToBeCompressedColBuf( startFileOffset ); }

    /** @brief Wrapper around extendColumn(), used for dictionary token columns.
     */
    int extendTokenColumn( );

  protected:

    //-------------------------------------------------------------------------
    // Protected Functions
    //-------------------------------------------------------------------------

    /** @brief Resize the internal column buffer
     *
     * @param spaceRequired Amount of additional space required
     */
    void resizeColumnBuffer(int spaceRequired);

    /** @brief See if buffer has room for "nRows" without filling up the
     * current extent.
     *
     * @param nRows The number of rows ready to add to the buffer.
     * @param nRows2 If there is not room, then nRows2 is the allowable
     *   number of rows to add, else nRows2 will be "nRows".
     * @return NO_ERROR or success
     */
    virtual int rowsExtentCheck( int nRows, int& nRows2 );

    /** @brief Write buffer data to file
     *
     * @param Highest buffer offset which data should be written to file
     * @return NO_ERROR or success
     */
    int writeToFile(int endOffset);

    /** @brief Write buffer data to file while checking for extent boundary.
     *
     * WARNING: This function will update contents of ColumnInfo struct that
     * owns this ColumnBufferManger if more than 1 extent is required to
     * write out the buffer.
     * @param startOffset The buffer offset where the write should begin
     * @param writeSize   The number of bytes to be written to the file
     * @return success or fail status
     */
    virtual int writeToFileExtentCheck(uint32_t startOffset, uint32_t writeSize);

    //-------------------------------------------------------------------------
    // Protected Data Members
    //-------------------------------------------------------------------------

    /** @brief Internal ColumnBuffer
     *
     * The ColumnBuffer is used by this class as a circular buffer. The
     * variables fBufWriteOffset and fBufFreeOffset are used to keep track
     * of free and allocated space.
     */
    ColumnBuffer* fCBuf;

    /** @brief Offset from where the next file write should start
     */
    int fBufWriteOffset;

    /** @brief Offset from where the next section allocation should start
     */
    int fBufFreeOffset;

    /** @brief List of currently in-use sections
     */
    std::list<ColumnBufferSection*> fSectionsInUse;

    /** @brief Flag indicating that an internal buffer resize is in progress
     */
    bool fResizePending;

    /** @brief Condition variable for threads waiting for resize to complete
     */
    boost::condition fResizeInProgress;

    /** @brief Condition variable for threads waiting for all buffer sections
     * to be released
     */
    boost::condition fBufInUse;

    /** @brief Condition variable for threads who have arrived out-of-
     * sequence with respect to their row-id
     */
    boost::condition fOutOfSequence;

    /** @brief Width of the column
     */
    int fColWidth;

    /** @brief Maximum row-id among all the alocated sections
     */
    RID fMaxRowId;

    /** @brief Parent ColumnInfo object used to manage the addition of
     * extents to the applicable database column segment files.
     */
    ColumnInfo* fColInfo;

    /** @brief Object used for logging
     */
    Log* fLog;
};

//------------------------------------------------------------------------------
// Specialization of ColumnBufferManager that is used for Dictionary columns.
//------------------------------------------------------------------------------
class ColumnBufferManagerDctnry : public ColumnBufferManager {

  public:
    ColumnBufferManagerDctnry(ColumnInfo* pColInfo, int colWidth,
                              Log* logger, int compressionType);
    virtual ~ColumnBufferManagerDctnry();

    virtual int rowsExtentCheck( int nRows, int& nRows2 );
    virtual int writeToFileExtentCheck(uint32_t startOffset, uint32_t writeSize);
};

}
#endif /*WRITEENGINE_COLBUFMGR_H*/
