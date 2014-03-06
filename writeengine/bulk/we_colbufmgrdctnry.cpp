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
 * $Id: we_colbufmgrdctnry.cpp 4726 2013-08-07 03:38:36Z bwilkinson $
 *
 ****************************************************************************/

#include "we_colbufmgr.h"
#include "we_columninfo.h"
#include "we_log.h"
#include <iostream>
#include <sstream>

namespace WriteEngine {

//------------------------------------------------------------------------------
// ColumnBufferManagerDctnry constructor that takes a ColumnInfo, colWidth, and
// FILE*.
//------------------------------------------------------------------------------
ColumnBufferManagerDctnry::ColumnBufferManagerDctnry(
    ColumnInfo* pColInfo,  int colWidth, Log* logger, int compressionType) :
    ColumnBufferManager(pColInfo, colWidth, logger, compressionType)
{
}

//------------------------------------------------------------------------------
// ColumnBufferManagerDctnry destructor.
//------------------------------------------------------------------------------
ColumnBufferManagerDctnry::~ColumnBufferManagerDctnry()
{
}

//------------------------------------------------------------------------------
// If we wanted to intercept the row data being written out by
// ColumnBufferManagerDctnry in order to break up any buffer crossing an
// extent boundary, this is where that logic would reside.  However, for
// dictionary columns we perform this extent division up front.  So for a
// dictionary column, this function is a simple pass-thru to the ColumnBuffer's
// writeToFile() function.
// The data to be written, starts at "startOffset" in the internal buffer and
// is "writeSize" bytes long.
//------------------------------------------------------------------------------
int ColumnBufferManagerDctnry::writeToFileExtentCheck(
    uint32_t startOffset, uint32_t writeSize)
{
    if (fLog->isDebug( DEBUG_3 ))
    {
        std::ostringstream oss;
        oss << "Dctnry writeToFileExtentCheck"
               ": OID-"    << fColInfo->curCol.dataFile.fid        <<
               "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
               "; part-"   << fColInfo->curCol.dataFile.fPartition <<
               "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
               "; writeSize-"        << writeSize                  <<
               "; oldAvailFileSize-" << fColInfo->availFileSize    <<
               "; newAvailFileSize-" << (fColInfo->availFileSize - writeSize);
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    int rc = fCBuf->writeToFile(startOffset, writeSize);
    if (rc != NO_ERROR) {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "writeToFileExtentCheck: write token extent failed: " <<
               ec.errorString(rc);
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    fColInfo->updateBytesWrittenCounts( writeSize );

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Before a section is reserved to contain "nRows" of incoming data, this
// function can be called to determine whether this group of rows will cause
// the buffer to cross an extent boundary.  If there is not room for "nRows",
// the number of rows that will fit in the current extent are returned.
// This function also catches and handles the case where an abbreviated
// extent needs to be expanded to a full extent on disk.
//
// WARNING: If the extent is expanded, then this function will change the
//          information in the ColumnInfo struct that owns this
//          ColumnBufferManagerDctnry.
//------------------------------------------------------------------------------
int ColumnBufferManagerDctnry::rowsExtentCheck( int nRows, int& nRows2 )
{
    nRows2 = nRows;

    int bufferSize    = fCBuf->getSize();
    long long spaceRequired = nRows * fColWidth;
    long dataInBuffer  = 0;
    if (bufferSize > 0)
        dataInBuffer = (fBufFreeOffset - fBufWriteOffset + bufferSize) % bufferSize;

    // if extent is out of space, see if this is an abbrev extent we can expand
    if (((dataInBuffer + spaceRequired) > fColInfo->availFileSize) &&
         (fColInfo->isAbbrevExtent()))
    {
        RETURN_ON_ERROR( fColInfo->expandAbbrevExtent(true) )
    }

    if ((dataInBuffer + spaceRequired) > fColInfo->availFileSize)
    {
        spaceRequired = fColInfo->availFileSize - dataInBuffer;
        nRows2        = (int)(spaceRequired / fColWidth);

        if (fLog->isDebug( DEBUG_1 ))
        {
            std::ostringstream oss1;
            oss1 << "Dctnry rowsExtentCheck (filling extent): OID-"    <<
                   fColInfo->curCol.dataFile.fid <<
                   "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
                   "; part-"   << fColInfo->curCol.dataFile.fPartition <<
                   "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
                   "; spaceRequired-" << spaceRequired <<
                   "; dataInBuffer-"  << dataInBuffer <<
                   "; availSpace-"    << fColInfo->availFileSize;
            fLog->logMsg( oss1.str(), MSGLVL_INFO2 );

            std::ostringstream oss2;
            oss2 << "Dctnry rowsExtentCheck: OID-" <<
                     fColInfo->curCol.dataFile.fid <<
                   "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
                   "; part-"   << fColInfo->curCol.dataFile.fPartition <<
                   "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
                   "; Changing nRows from " << nRows << " to " << nRows2;
            fLog->logMsg( oss2.str(), MSGLVL_INFO2 );
        }
    }
    else
    {
        if (fLog->isDebug( DEBUG_2 ))
        {
            std::ostringstream oss;
            oss << "Dctnry rowsExtentCheck: OID-" <<
                    fColInfo->curCol.dataFile.fid <<
                   "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
                   "; part-"   << fColInfo->curCol.dataFile.fPartition <<
                   "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
                   "; spaceRequired-" << spaceRequired <<
                   "; dataInBuffer-"  << dataInBuffer <<
                   "; availSpace-"    << fColInfo->availFileSize;
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
    }

    return NO_ERROR;
}

}
