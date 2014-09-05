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
 * $Id: we_colbufsec.cpp 4450 2013-01-21 14:13:24Z rdempsey $
 *
 ****************************************************************************/

/** @file
 * Implementation of the ColumnBufferSection class
 */

#include "we_colbufsec.h"

namespace WriteEngine {

ColumnBufferSection::ColumnBufferSection(
    ColumnBuffer * const cb,
    RID sRowId,
    RID eRowId,
    int width,
    int sOffset)
        :fCBuf(cb),
        fStartRowId(sRowId),
        fEndRowId(eRowId),
        fColWidth(width),
        fBufStartOffset(sOffset),
        fCurrRowId(sRowId),
        fStatus(INIT_COMPLETE) {
} 

ColumnBufferSection::~ColumnBufferSection() {
}

void ColumnBufferSection::write(const void * const data, int nRows) {
//Casting void * to unsigned char * without modifying the constness
    const unsigned char * const tData =
        static_cast<const unsigned char * const>(data);

    if(fCurrRowId + nRows + 1> fEndRowId) {
    //TODO: Handle error (old-dmc)
    }

    int startOffset = (fBufStartOffset + (fCurrRowId-fStartRowId) *
                      fColWidth) % fCBuf->getSize();
    int nBytes = nRows * fColWidth;
    int bytesWritten = 0;
    if((startOffset + nBytes) > fCBuf->getSize()) {
        fCBuf->write(tData, startOffset, fCBuf->getSize() - startOffset);
        bytesWritten = fCBuf->getSize() - startOffset;
        startOffset = 0;
    }
    fCBuf->write(tData + bytesWritten, startOffset, nBytes - bytesWritten);
    fCurrRowId += nRows;
}
    
void ColumnBufferSection::setStatus(int s) {
    fStatus = s;
}

int ColumnBufferSection::getStatus() const {
    return fStatus;
}

int ColumnBufferSection::getStartOffset() const {
    return fBufStartOffset;
}

int ColumnBufferSection::getSectionSize() const {
    return (fEndRowId - fStartRowId + 1) * fColWidth;
}

}
