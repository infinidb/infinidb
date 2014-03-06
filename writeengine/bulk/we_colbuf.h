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
 * $Id: we_colbuf.h 4726 2013-08-07 03:38:36Z bwilkinson $
 *
 *****************************************************************************/

/** @file
 * class ColumnBuffer
 */

#ifndef WRITEENGINE_COLUMNBUF_H
#define WRITEENGINE_COLUMNBUF_H

#include <cstdio>

#include "we_type.h"

namespace WriteEngine {
class Log;
struct ColumnInfo;

/** @brief A buffer class to store data written to column files
 *
 * ColumnBuffer is to be used as a temporary buffer to store Column data before
 * writing it out to the intended destination(currently a file stream). The
 * file stream should be initialized by the client of this class
 */
class ColumnBuffer {

  public:

    /** @brief default Constructor
     */
    ColumnBuffer(ColumnInfo* pColInfo, Log* logger);

    /** @brief default Destructor
     */
    virtual ~ColumnBuffer();

    /** @brief Final flushing of data and headers prior to closing the file.
     * This is a no-op for uncompressed columns.
     * @param bTruncFile is file to be truncated
     * @return NO_ERROR or success
     */
    virtual int finishFile( bool bTruncFile );

    /** @brief Returns size of the buffer
     */
    int getSize() const { return fBufSize; }

    /** @brief Reset the ColBuf to-be-compressed buffer prior to importing the
     *  next extent.  This is a no-op for uncompressed columns.
     * @param startFileOffset (output) File offset to start of active chunk
     */
    virtual int resetToBeCompressedColBuf( long long& startFileOffset );

    /** @brief Set the IDBDataFile* destination for the applicable col segment file.
     * 
     * @param The destination IDBDataFile stream to which buffer data will be written
     * @param Starting HWM for cFile
     * @param Headers with ptr information (only applies to compressed files)
     */
    virtual int setDbFile(IDBDataFile * const cFile, HWM startHwm, const char* hdrs);

    /** @brief Resize the buffer, also copying the section denoted by the
     * offsets to the new buffer.  If offsets are -1, then the buffer is
     * expanded, but no copy is performed.
     *
     * @param newSize The new size of the buffer
     * @param startOffset The start offset in the current buffer from where the
     * previous data need to be copied
     * @param endOffset The end offset upto which the data should be copied to
     * new buffer
     */
    void resizeAndCopy(int newSize, int startOffset, int endOffset);

    /** @brief Write data to buffer
     *
     * @param data
     * @param startOffset
     * @param bytes
     */
    void write(const void * const data, int startOffset, int bytes) const;

    /** @brief Write data to FILE
     *
     * @param startOffset The buffer offset from where the write should begin
     * @param writeSize   The number of bytes to be written to the file
     */
    virtual int writeToFile(int startOffset, int writeSize);

  protected:

    // Disable copy constructor and assignment operator by declaring and
    // not defining.
    ColumnBuffer(const ColumnBuffer&);
    ColumnBuffer& operator=(const ColumnBuffer&);

    unsigned char* fBuffer; // Internal buffer
    int   fBufSize;         // Size of the internal buffer
    IDBDataFile* fFile;            // The column file output stream
    ColumnInfo* fColInfo;   // parent ColumnInfo Object
    Log*  fLog;             // Logger
    HWM   fStartingHwm;     // Starting HWM for current column segment file
};

}

#endif //WRITEENGINE_COLUMNBUF_H
