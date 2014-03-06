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
 * $Id: we_colbuf.cpp 4737 2013-08-14 20:45:46Z bwilkinson $
 *
 ****************************************************************************/

/** @file
 * Implementation of the ColumnBuffer class
 *
 */

#include "we_colbuf.h"
#include "we_columninfo.h"
#include "we_log.h"
#include "we_stats.h"
#include <string.h>
#include "IDBDataFile.h"
using namespace idbdatafile;

namespace WriteEngine {

ColumnBuffer::ColumnBuffer(ColumnInfo* pColInfo, Log* logger) :
    fBuffer(0), fBufSize(0), fFile(0), fColInfo(pColInfo), fLog(logger)
{
}

ColumnBuffer::~ColumnBuffer()
{
    delete[] fBuffer;
}

int ColumnBuffer::finishFile(bool /*bTruncFile*/)
{
    return NO_ERROR;
}

int ColumnBuffer::resetToBeCompressedColBuf( long long& startFileOffset )
{
    return NO_ERROR;
}

int ColumnBuffer::setDbFile(IDBDataFile* f, HWM startHwm, const char* /*hdrs*/)
{
    fFile        = f;
    fStartingHwm = startHwm;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Resizes the output buffer based on the "newSize".
// startOffset and endOffset points to the data to be carried or copied over
// from the old buffer to the new expanded buffer.
// If offsets are -1, then the buffer is expanded, but no copy is performed.
// @bug 1977 rework; correct problem; writing extra blocks
//------------------------------------------------------------------------------
void ColumnBuffer::resizeAndCopy(int newSize, int startOffset, int endOffset)
{
    unsigned char *old_buffer = fBuffer;
    fBuffer = new unsigned char[newSize];

    if (startOffset != -1) {
        int destBufferOffset = 0;

        // If the data in "buffer" wraps around, then here's where we copy
        // the data at the end of the buffer, before copying the data
        // at the start of the bufffer.
        if(endOffset < startOffset) { 
            memcpy(fBuffer, old_buffer + startOffset, fBufSize - startOffset);
            destBufferOffset = fBufSize - startOffset;
            startOffset = 0;
        }

        memcpy(fBuffer + destBufferOffset,
               old_buffer + startOffset, endOffset - startOffset + 1);
    }

    fBufSize = newSize;
    delete[] old_buffer;
}

//------------------------------------------------------------------------------
// Write data stored up in the output buffer to the segment column file.
//------------------------------------------------------------------------------
int ColumnBuffer::writeToFile(int startOffset, int writeSize)
{
    if (writeSize == 0) // skip unnecessary write, if 0 bytes given
        return NO_ERROR;

#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_WRITE_COL);
#endif
    size_t nitems = fFile->write(fBuffer + startOffset, writeSize) / writeSize;
    if (nitems != 1)
        return ERR_FILE_WRITE;
#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_WRITE_COL);
#endif

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Add data to output buffer
//------------------------------------------------------------------------------
void ColumnBuffer::write(const void * const data, int startOffset, int bytes) const
{
    memcpy(fBuffer + startOffset, data, bytes);
}

}
