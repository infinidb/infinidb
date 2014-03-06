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
 * $Id: we_colbufcompressed.h 4726 2013-08-07 03:38:36Z bwilkinson $
 *
 *****************************************************************************/

/** @file
 * class ColumnBufferCompressed
 */

#ifndef WRITEENGINE_COLUMNBUFCOMPRESSED_H
#define WRITEENGINE_COLUMNBUFCOMPRESSED_H

#include "we_colbuf.h"

#include <cstdio>
#include <vector>

#include "idbcompress.h"

namespace WriteEngine {
class Log;
struct ColumnInfo;

/** @brief A buffer class to store data written to compressed column files
 *
 * ColumnBufferCompressed is to be used as a temporary buffer to store
 * compressed column data before writing it out to the intended destination
 * (currently a file stream). The file stream should be initialized by
 * the client of this class
 */
class ColumnBufferCompressed : public ColumnBuffer {

  public:

    /** @brief default Constructor
     */
    ColumnBufferCompressed( ColumnInfo* pColInfo, Log* logger);

    /** @brief default Destructor
     */
    virtual ~ColumnBufferCompressed();

    /** @brief Final flushing of data and headers prior to closing the file.
     * @param bTruncFile is file to be truncated
     * @return NO_ERROR or success
     */
    virtual int finishFile(bool bTruncFile);

    /** @brief Reset the ColBuf to-be-compressed buffer prior to importing the
     * next extent.
     * @param startFileOffset Byte offset where next extent chunk will start
     */
    virtual int resetToBeCompressedColBuf(long long& startFileOffset );

    /** @brief file mutator
     *
     * @param cFile    Destination FILE stream where buffer data will be written
     * @param startHwm Starting HWM for cFile
     * @param hdrs     Headers with ptr information.
     */
    virtual int setDbFile(IDBDataFile * const cFile, HWM startHwm, const char* hdrs);

    /** @brief Write data to FILE
     *
     * @param startOffset The buffer offset from where the write should begin
     * @param writeSize   The number of bytes to be written to the file
     */
    virtual int writeToFile(int startOffset, int writeSize);

  private:

    // Disable copy constructor and assignment operator by declaring and
    // not defining.
    ColumnBufferCompressed(const ColumnBufferCompressed&);
    ColumnBufferCompressed& operator=(const ColumnBufferCompressed&);

    // Compress and flush the to-be-compressed buffer; updates header if needed
    int compressAndFlush(bool bFinishFile);
    int initToBeCompressedBuffer( long long& startFileOffset);
                                // Initialize the to-be-compressed buffer
    int saveCompressionHeaders(); // Saves compression headers to the db file

    unsigned char*       fToBeCompressedBuffer; // data waiting to be compressed
    size_t               fToBeCompressedCapacity;//size of comp buffer;
                                                // should always be 4MB, unless
                                                // working with abbrev extent.
    size_t               fNumBytes;             // num Bytes in comp buffer
    compress::IDBCompressInterface*
                         fCompressor;           // data compression object
    compress::CompChunkPtrList
                         fChunkPtrs;            // col file header information
    bool                 fPreLoadHWMChunk;      // preload 1st HWM chunk only
    unsigned int         fUserPaddingBytes;     // compressed chunk padding
    bool                 fFlushedStartHwmChunk; // have we rewritten the hdr
                                                //   for the starting HWM chunk
};

}

#endif //WRITEENGINE_COLUMNBUFCOMPRESSED_H
