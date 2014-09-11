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
* $Id: we_columninfo.cpp 4318 2012-11-06 20:46:49Z rdempsey $
*
*******************************************************************************/

#include <cstdlib>
#include <sstream>
#include <unistd.h>
//#define NDEBUG
//#include <cassert>
#include <cctype>

#include "we_columninfo.h"
#include "we_log.h"
#include "we_stats.h"
#include "we_colopbulk.h"
#include "brmtypes.h"
#include "cacheutils.h"
#include "we_brmreporter.h"

namespace
{
//------------------------------------------------------------------------------
// Do a fast ascii-hex-string to binary data conversion. This is done in-place.
// We take bytes 1 and 2 and put them back into byte 1; 3 and 4 into 2; etc.
// The length is adjusted by 1/2 and returned to the caller as the new length.
// If any invalid hex characters are present in the string (not 0-9,A-F, or
// a-f), then the string is considered invalid, and a null token will be used.
//------------------------------------------------------------------------------
unsigned int compactVarBinary(char* charTmpBuf, int fieldLength)
{
    unsigned char* p = reinterpret_cast<unsigned char*>(charTmpBuf);
    char* f = charTmpBuf;
    char v = '\0';
    for (int i = 0; i < fieldLength / 2; i++, p++)
    {
        // Store even number byte in high order 4 bits of next output byte
        v = *f;
        if (!isxdigit(v))
            return WriteEngine::COLPOSPAIR_NULL_TOKEN_OFFSET;
        if (v <= '9')
            *p = v - '0';
        else if (v <= 'F')
            *p = v - 'A' + 10;
        else //if (v <= 'f')
            *p = v - 'a' + 10;
        *p <<= 4;
        f++;

        // Store odd number byte in low order 4 bite of next output byte
        v = *f;
        if (!isxdigit(v))
            return WriteEngine::COLPOSPAIR_NULL_TOKEN_OFFSET;
        if (v <= '9')
            *p |= v - '0';
        else if (v <= 'F')
            *p |= v - 'A' + 10;
        else //if (v <= 'f')
            *p |= v - 'a' + 10;
        f++;
    }

// Changed our mind and decided to have the read thread reject rows with
// incomplete (odd length) varbinary fields, so the following check is not
// necessary.  We should only get to this function with an even fieldLength.
#if 0
    // Handle case where input data field has "odd" byte length.
    // Store last input byte in high order 4 bits of additional output byte,
    // and leave the low order bits set to 0.
    if ((fieldLength & 1) == 1)
    {
        v = *f;
        if (!isxdigit(v))
            return WriteEngine::COLPOSPAIR_NULL_TOKEN_OFFSET;
        if (v <= '9')
            *p = v - '0';
        else if (v <= 'F')
            *p = v - 'A' + 10;
        else //if (v <= 'f')
            *p = v - 'a' + 10;
        *p <<= 4;

        fieldLength++;
    }
#endif

    return (fieldLength / 2);
}
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// ColumnInfo constructor
//------------------------------------------------------------------------------
ColumnInfo::ColumnInfo(Log*             logger,
                       int              idIn,
                       const JobColumn& columnIn) :
                       savedHWM(0),
                       sizeWrittenStart(0),
                       sizeWritten(0),
                       id(idIn),
                       lastProcessingTime(0),
#ifdef PROFILE
                       totalProcessingTime(0),
#endif
                       colBufferMgr(0),
                       availFileSize(0),
                       fileSize(0),
                       savedLbid(0),
                       fLog(logger),
                       fLastInputRowInCurrentExtent(0),
                       fLoadingAbbreviatedExtent(false),
                       fColExtInf(0),
                       fMaxNumRowsPerSegFile(0),
                       fStore(0),
                       fAutoIncLastValue(0),
                       fSaturatedRowCnt(0)
{
    column = columnIn;

    // Allocate a ColExtInfBase object for those types that won't track
    // min/max CasualPartition info; this is a stub class that won't do
    // anything.
    switch ( column.weType )
    {
        case WriteEngine::WR_FLOAT:
        case WriteEngine::WR_DOUBLE:
        case WriteEngine::WR_VARBINARY: // treat like char dictionary for now
        case WriteEngine::WR_TOKEN:
        {
            fColExtInf = new ColExtInfBase( );
            break;
        }

        case WriteEngine::WR_CHAR:
        {
            if (column.colType == COL_TYPE_DICT)
            {
                fColExtInf = new ColExtInfBase( );
            }
            else
            {
                fColExtInf = new ColExtInf(column.mapOid, logger);
            }
            break;
        }

        case WriteEngine::WR_SHORT:
        case WriteEngine::WR_BYTE:
        case WriteEngine::WR_LONGLONG:
        case WriteEngine::WR_INT:
        default:
        {
            fColExtInf = new ColExtInf(column.mapOid, logger);
            break;
        }
    }

    colOp.reset(new ColumnOpBulk(logger, column.compressionType));

    fMaxNumRowsPerSegFile = BRMWrapper::getInstance()->getExtentRows() *
                            Config::getExtentsPerSegmentFile();
}

//------------------------------------------------------------------------------
// ColumnInfo destructor
//------------------------------------------------------------------------------
ColumnInfo::~ColumnInfo()
{
    clearMemory();

    // Closing dictionary file also updates the extent map; which we
    // don't want to do if we are aborting the job.  Besides, the
    // application code should be closing the dictionary as needed,
    // instead of relying on the destructor, so disabled this code.
    //if(fStore != NULL)
    //{   
    //    fStore->closeDctnryStore();
    //    delete fStore;
    //    fStore = NULL;
    //}

    if (fColExtInf)
    {
        delete fColExtInf;
        fColExtInf = 0;
    }
}

//------------------------------------------------------------------------------
// Clear memory consumed by this ColumnInfo object.
//------------------------------------------------------------------------------
void ColumnInfo::clearMemory( )
{
    if (colBufferMgr)
    {
        delete colBufferMgr;
        colBufferMgr = 0;
    }

    fDictBlocks.clear();
}

//------------------------------------------------------------------------------
// Add an extent for this column.  The next segment file in the DBRoot,
// partition, segment number rotation will be selected for the extent.
//
// NOTE: no mutex lock is employed here.  It is assumed that the calling
//       application code is taking care of this, if it is needed.
//------------------------------------------------------------------------------
int ColumnInfo::extendColumn( bool saveLBIDForCP )
{
    //..We assume the applicable file is already open, so...
    //  the HWM of the current segment file should be set to reference the
    //  last block in the current file (as specified in curCol.dataFile.pFile).
    //
    //  Prior to adding compression, we used ftell() to set HWM, but that
    //  would not work for compressed data.  Code now assumes that if we
    //  are adding an extent, that sizeWritten is a multiple of blksize,
    //  which it should be.  If we are adding an extent, sizeWritten should
    //  point to the last byte of a full extent boundary.
    HWM hwm = (sizeWritten / BYTE_PER_BLOCK) - 1;

    //..Save info about the current segment column file, and close that file.
    addToSegFileList( curCol.dataFile, hwm );

    // Close current segment column file prior to adding extent to next seg file
    int rc = closeColumnFile( true, false );
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "extendColumn: error closing extent in "    <<
               "column OID-" << curCol.dataFile.fid        <<
               "; DBRoot-"   << curCol.dataFile.fDbRoot    <<
               "; part-"     << curCol.dataFile.fPartition <<
               "; seg-"      << curCol.dataFile.fSegment   <<
               "; hwm-"      << hwm;
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

        return rc;
    }

    //..Extend the column, by adding an extent to the next DBRoot, partition,
    //  and segment file in the rotation.
    FILE*       pFileNew     = 0;
    uint16_t    dbRootNew    = 0;
    uint32_t    partitionNew = 0;
    uint16_t    segmentNew   = 0;
    std::string segFileNew;
    HWM         hwmNew       = 0;
    bool        newFile      = false;

    BRM::LBID_t startLbid;
    char hdr[ compress::IDBCompressInterface::HDR_BUF_LEN * 2 ];
    int allocsize = 0;
    rc = colOp->extendColumn ( curCol, true,
        pFileNew,
        dbRootNew,
        partitionNew,
        segmentNew,
        segFileNew,
        hwmNew,
        startLbid,
        newFile,
        allocsize,
        hdr );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "extendColumn: error adding extent after " <<
            "column OID-" << curCol.dataFile.fid          <<
            "; DBRoot-"   << curCol.dataFile.fDbRoot      <<
            "; part-"     << curCol.dataFile.fPartition   <<
            "; seg-"      << curCol.dataFile.fSegment;

        oss << "; newDBRoot-" << dbRootNew                <<
               "; newpart-"   << partitionNew             <<
               "; newseg-"    << segmentNew               <<
               "; " << ec.errorString(rc);
        fLog->logMsg( oss.str(), rc, MSGLVL_CRITICAL );

        if (pFileNew)
            colOp->closeFile( pFileNew ); // clean up loose ends

        return rc;
    }

    std::ostringstream oss;
    oss << "Add column extent OID-" << curCol.dataFile.fid <<
        "; DBRoot-" << dbRootNew    <<
        "; part-"   << partitionNew <<
        "; seg-"    << segmentNew   <<
        "; hwm-"    << hwmNew       <<
        "; LBID-"   << startLbid    <<
        "; file-"   << segFileNew;
    fLog->logMsg( oss.str(), MSGLVL_INFO2 );

    // Save the LBID with our CP extent info, so that we can update extent map
    if (saveLBIDForCP)
    {
        int rcLBID = fColExtInf->updateEntryLbid( startLbid );

        // If error occurs, we log WARNING, but we don't fail the job.
        if (rcLBID != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "updateEntryLbid failed for OID-" << curCol.dataFile.fid <<
                   "; LBID-" << startLbid <<
                   "; CasualPartition info may become invalid; " <<
                   ec.errorString(rcLBID);
            fLog->logMsg( oss.str(), rcLBID, MSGLVL_WARNING );
        }
    }

    //..Reset data members to reflect where we are in the newly
    //  opened column segment file.  The file may be a new file, or we may
    //  be adding an extent to an existing column segment file.
    curCol.dataFile.hwm          = hwmNew;
    curCol.dataFile.pFile        = pFileNew;
    curCol.dataFile.fPartition   = partitionNew;
    curCol.dataFile.fSegment     = segmentNew;
    curCol.dataFile.fDbRoot      = dbRootNew;
    curCol.dataFile.fSegFileName = segFileNew;

    rc = resetFileOffsetsNewExtent(hdr);
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "extendColumn: error moving to new extent in " <<
               "column OID-" << curCol.dataFile.fid           <<
               "; DBRoot-"   << curCol.dataFile.fDbRoot       <<
               "; part-"     << curCol.dataFile.fPartition    <<
               "; seg-"      << curCol.dataFile.fSegment      <<
               "; hwm-"      << curCol.dataFile.hwm;
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

        if (pFileNew)
            closeColumnFile( false, true ); // clean up loose ends

        return rc;
    }

    if (fLog->isDebug( DEBUG_1 ))
    {
        std::ostringstream oss2;
        oss2 << "Extent added to column OID-" << curCol.dataFile.fid <<
            "; DBRoot-" << dbRootNew    <<
            "; part-"   << partitionNew <<
            "; seg-"    << segmentNew   <<
            "; begByte-"<< sizeWritten  <<
            "; endByte-"<< fileSize     <<
            "; freeBytes-" << availFileSize;
        fLog->logMsg( oss2.str(), MSGLVL_INFO2 );
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
//  Either add or update the File object, so that it has the updated HWM.
//  We will access this info to update the HWM in the ExtentMap at the end
//  of the import.
//  dmc-could optimize later by changing fSegFileUpdateList from a vector
//  to a map or hashtable with a key consisting of partition and segment.
//------------------------------------------------------------------------------
void ColumnInfo::addToSegFileList( File& dataFile, HWM hwm )
{
    bool foundFlag = false;
    for (unsigned int i=0; i<fSegFileUpdateList.size(); i++)
    {
        if ((fSegFileUpdateList[i].fPartition == dataFile.fPartition) &&
            (fSegFileUpdateList[i].fSegment   == dataFile.fSegment))
        {
            if (fLog->isDebug( DEBUG_1 ))
            {
                std::ostringstream oss3;
                oss3 << "Updating HWM list"
                        "; column OID-" << dataFile.fid    <<
                        "; DBRoot-" << dataFile.fDbRoot    <<
                        "; part-"   << dataFile.fPartition <<
                        "; seg-"    << dataFile.fSegment   <<
                        "; oldhwm-" << fSegFileUpdateList[i].hwm  <<
                        "; newhwm-" << hwm;
                fLog->logMsg( oss3.str(), MSGLVL_INFO2 );
            }

            fSegFileUpdateList[i].hwm = hwm;
            foundFlag = true;
            break;
        }
    }
    if (!foundFlag)
    {
        if (fLog->isDebug( DEBUG_1 ))
        {
            std::ostringstream oss3;
            oss3 << "Adding to HWM list" <<
                    "; column OID-" << dataFile.fid    <<
                    "; DBRoot-" << dataFile.fDbRoot    <<
                    "; part-"   << dataFile.fPartition <<
                    "; seg-"    << dataFile.fSegment   <<
                    "; hwm-"    << hwm;
            fLog->logMsg( oss3.str(), MSGLVL_INFO2 );
        }

        dataFile.hwm = hwm;
        fSegFileUpdateList.push_back( dataFile );
    }
}

//------------------------------------------------------------------------------
// Reset file offset data member attributes when we start working on the next
// extent.
//------------------------------------------------------------------------------
int ColumnInfo::resetFileOffsetsNewExtent(const char* /*hdr*/)
{
    savedHWM         = curCol.dataFile.hwm;
    setFileSize( curCol.dataFile.hwm, false );
    long long byteOffset = (long long)savedHWM * (long long)BYTE_PER_BLOCK;
    sizeWritten      = byteOffset;
    sizeWrittenStart = sizeWritten;
    availFileSize    = fileSize - sizeWritten;

    // If we are adding an extent as part of preliminary block skipping, then
    // we won't have a ColumnBufferManager object yet, but that's okay, because
    // we are only adding the empty extent at this point.
    if (colBufferMgr)
    {
        RETURN_ON_ERROR( colBufferMgr->setDbFile(
                         curCol.dataFile.pFile, curCol.dataFile.hwm, 0) );

        RETURN_ON_ERROR( colOp->setFileOffset(curCol.dataFile.pFile,
                         byteOffset) );
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Set current size of file in raw (uncompressed) bytes, given the specified
// hwm.  abbrevFlag indicates whether this is a fixed size abbreviated extent.
// For unabbreviated extents the "logical" file size is calculated by rounding
// the hwm up to the nearest multiple of the extent size.
//------------------------------------------------------------------------------
void ColumnInfo::setFileSize( HWM hwm, int abbrevFlag )
{
    // Must be an abbreviated extent if there is only 1 compressed chunk in
    // the db file.  Even a 1-byte column would have 2 4MB chunks for an 8M
    // row column extent.
    if (abbrevFlag)
    {
        fileSize = (INITIAL_EXTENT_ROWS_TO_DISK * curCol.colWidth);
    }
    else
    {
        const unsigned int ROWS_PER_EXTENT =
            BRMWrapper::getInstance()->getExtentRows();

        long long nRows = ((long long)(hwm+1) * (long long)BYTE_PER_BLOCK) /
                           (long long)curCol.colWidth;
        long long nRem  = nRows % ROWS_PER_EXTENT;
        if (nRem == 0)
        {
            fileSize = nRows * curCol.colWidth;
        }
        else
        {
            fileSize = (nRows - nRem + ROWS_PER_EXTENT) * curCol.colWidth;
        }
    }
}

//------------------------------------------------------------------------------
// If we are dealing with the first extent in the first segment file for this
// column, and the segment file is still equal to 256K rows, then we set the
// fLoadingAbbreviatedExtent flag.  This tells us (later on) that we are dealing
// with an abbreviated extent that still needs to be expanded and filled, before
// we start adding new extents.
//------------------------------------------------------------------------------
void ColumnInfo::setAbbrevExtentCheck( )
{
    if ((curCol.dataFile.fPartition == 0) &&
        (curCol.dataFile.fSegment   == 0))
    {
        if (fileSize == (INITIAL_EXTENT_ROWS_TO_DISK * curCol.colWidth))
        {
            fLoadingAbbreviatedExtent = true;

            if (fLog->isDebug( DEBUG_1 ))
            {
                std::ostringstream oss;
                oss << "Importing into abbreviated extent, column OID-" <<
                         curCol.dataFile.fid   <<
                    "; DBRoot-"   << curCol.dataFile.fDbRoot    <<
                    "; part-"     << curCol.dataFile.fPartition <<
                    "; seg-"      << curCol.dataFile.fSegment   <<
                    "; fileSize-" << fileSize;
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }
        }
    }
}

//------------------------------------------------------------------------------
// If this is an abbreviated extent, we expand the extent to a full extent on
// disk, by initializing the necessary number of remaining blocks.
// bRetainFilePos flag controls whether the current file position is retained
// upon return from this function; else the file will be positioned at the end
// of the file.
//------------------------------------------------------------------------------
int ColumnInfo::expandAbbrevExtent( bool bRetainFilePos )
{
    if (fLoadingAbbreviatedExtent)
    {
#ifdef _MSC_VER
        __int64 oldOffset = 0;
        if (bRetainFilePos)
            oldOffset = _ftelli64(curCol.dataFile.pFile);
        colOp->setFileOffset(curCol.dataFile.pFile, 0, SEEK_END);
#else
        off_t oldOffset = 0;
        if (bRetainFilePos)
            oldOffset = ftello( curCol.dataFile.pFile );
        colOp->setFileOffset( curCol.dataFile.pFile, 0, SEEK_END );
#endif
        std::ostringstream oss;
        oss << "Expanding first extent to column OID-" << curCol.dataFile.fid <<
            "; DBRoot-" << curCol.dataFile.fDbRoot    <<
            "; part-"   << curCol.dataFile.fPartition <<
            "; seg-"    << curCol.dataFile.fSegment   <<
            "; file-"   << curCol.dataFile.fSegFileName;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );

        int rc = colOp->expandAbbrevExtent ( curCol );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "expandAbbrevExtent: error expanding extent for " <<
                   "OID-"      << curCol.dataFile.fid        <<
                   "; DBRoot-" << curCol.dataFile.fDbRoot    <<
                   "; part-"   << curCol.dataFile.fPartition <<
                   "; seg-"    << curCol.dataFile.fSegment   <<
                   "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_CRITICAL );
            return rc;
        }

        // Update available file size to reflect disk space added by expanding
        // the extent.
#ifdef _MSC_VER
        __int64   fileSizeBeforeExpand = fileSize;
#else
        long long fileSizeBeforeExpand = fileSize;
#endif
        setFileSize( (fileSizeBeforeExpand/BYTE_PER_BLOCK), false );
        availFileSize += (fileSize - fileSizeBeforeExpand);

        // Restore offset back to where we were before expanding the extent
        if (bRetainFilePos)
        {
            rc = colOp->setFileOffset(curCol.dataFile.pFile,oldOffset,SEEK_SET);
            if (rc != NO_ERROR)
            {
                WErrorCodes ec;
                std::ostringstream oss;
                oss << "expandAbbrevExtent: error seeking to new extent for " <<
                       "OID-"      << curCol.dataFile.fid        <<
                       "; DBRoot-" << curCol.dataFile.fDbRoot    <<
                       "; part-"   << curCol.dataFile.fPartition <<
                       "; seg-"    << curCol.dataFile.fSegment   <<
                       "; " << ec.errorString(rc);
                fLog->logMsg( oss.str(), rc, MSGLVL_CRITICAL );
                return rc;
            }
        }

        // We only use abbreviated extents for the very first extent.  So after
        // expanding a col's abbreviated extent, we should disable this check.
        fLoadingAbbreviatedExtent = false;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Close the current Column file.
//------------------------------------------------------------------------------
int ColumnInfo::closeColumnFile(bool /*bCompletingExtent*/, bool /*bAbort*/)
{
    if ( curCol.dataFile.pFile )
    {
        colOp->closeFile( curCol.dataFile.pFile );
        curCol.dataFile.pFile = 0;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Close the current Dictionary store file.
//------------------------------------------------------------------------------
int ColumnInfo::closeDctnryStore(bool bAbort)
{
    int rc = NO_ERROR;

    if (fStore)
    {
        if (bAbort)
            rc = fStore->closeDctnryOnly();
        else
            rc = fStore->closeDctnry();

        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "closeDctnryStore: error closing store file for " <<
                   "OID-"      << column.dctnry.dctnryOid    <<
                   "; DBRoot-" << curCol.dataFile.fDbRoot    <<
                   "; part-"   << curCol.dataFile.fPartition <<
                   "; seg-"    << curCol.dataFile.fSegment   <<
                   "; file-"   << fStore->getFileName()      <<
                   "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        }

        delete fStore;
        fStore = 0;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Open a new or existing Dictionary store file based on the DBRoot,
// partition, and segment settings in curCol.dataFile.
//------------------------------------------------------------------------------
int ColumnInfo::openDctnryStore( bool bMustExist )
{
    int rc = NO_ERROR;

    if ( column.dctnry.fCompressionType != 0 )
    {
        DctnryCompress1* dctnryCompress1 = new DctnryCompress1;
        dctnryCompress1->setMaxActiveChunkNum(1);
        dctnryCompress1->setBulkFlag(true);
        fStore = dctnryCompress1;
    }
    else
    {
        fStore = new DctnryCompress0;
    }

    fStore->setLogger(fLog);
    fStore->setColWidth( column.dctnryWidth );

    // If we are in the process of adding an extent to this column,
    // and the extent we are adding is the first extent for the
    // relevant column segment file, then the corresponding dictionary
    // store file will not exist, in which case we must create
    // the store file, else we open the applicable store file.
    if ( (bMustExist) ||
       (colOp->exists(column.dctnry.dctnryOid,
        curCol.dataFile.fDbRoot,
        curCol.dataFile.fPartition,
        curCol.dataFile.fSegment)) )
    {
        // Save HWM chunk (for compressed files) if this seg file calls for it
        RETURN_ON_ERROR( saveDctnryStoreHWMChunk() );

        rc = fStore->openDctnry(
            column.dctnry.dctnryOid,
            curCol.dataFile.fDbRoot,
            curCol.dataFile.fPartition,
            curCol.dataFile.fSegment);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "openDctnryStore: error opening existing store file for " <<
                   "OID-"      << column.dctnry.dctnryOid    <<
                   "; DBRoot-" << curCol.dataFile.fDbRoot    <<
                   "; part-"   << curCol.dataFile.fPartition <<
                   "; seg-"    << curCol.dataFile.fSegment   <<
                   "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

            // Ignore return code from closing file; already in error state
            closeDctnryStore(true); // clean up loose ends
            return rc;
        }

        if (INVALID_LBID != fStore->getCurLbid())
            fDictBlocks.push_back(BRM::LVP_t(fStore->getCurLbid(), 0));

        std::ostringstream oss;
        oss << "Opening existing store file for " << column.colName <<
               "; OID-"    << column.dctnry.dctnryOid    <<
               "; DBRoot-" << curCol.dataFile.fDbRoot    <<
               "; part-"   << curCol.dataFile.fPartition <<
               "; seg-"    << curCol.dataFile.fSegment   <<
               "; hwm-"    << fStore->getHWM()           <<
               "; file-"   << fStore->getFileName();
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }
    else
    {
        BRM::LBID_t startLbid;
        rc = fStore->createDctnry(
            column.dctnry.dctnryOid,
            column.dctnryWidth,      //@bug 3313 - pass string col width
            curCol.dataFile.fDbRoot,
            curCol.dataFile.fPartition,
            curCol.dataFile.fSegment,
            startLbid);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "openDctnryStore: error creating new store file for " <<
                   "OID-"      << column.dctnry.dctnryOid    <<
                   "; DBRoot-" << curCol.dataFile.fDbRoot    <<
                   "; part-"   << curCol.dataFile.fPartition <<
                   "; seg-"    << curCol.dataFile.fSegment   <<
                   "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_CRITICAL );

            // Ignore return code from closing file; already in error state
            closeDctnryStore(true); // clean up loose ends
            return rc;
        }

        rc = fStore->openDctnry(
            column.dctnry.dctnryOid,
            curCol.dataFile.fDbRoot,
            curCol.dataFile.fPartition,
            curCol.dataFile.fSegment);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "openDctnryStore: error opening new store file for " <<
                   "OID-"      << column.dctnry.dctnryOid    <<
                   "; DBRoot-" << curCol.dataFile.fDbRoot    <<
                   "; part-"   << curCol.dataFile.fPartition <<
                   "; seg-"    << curCol.dataFile.fSegment   <<
                   "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

            // Ignore return code from closing file; already in error state
            closeDctnryStore(true); // clean up loose ends
            return rc;
        }

        std::ostringstream oss;
        oss << "Opening new store file for " << column.colName <<
               "; OID-"      << column.dctnry.dctnryOid    <<
               "; DBRoot-"   << curCol.dataFile.fDbRoot    <<
               "; part-"     << curCol.dataFile.fPartition <<
               "; seg-"      << curCol.dataFile.fSegment   <<
               "; file-"     << fStore->getFileName();
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    return rc;
}

//------------------------------------------------------------------------------
// Update dictionary store file with specified strings, and return the assigned
// tokens (tokenbuf) to be stored in the corresponding column token file.
//------------------------------------------------------------------------------
int ColumnInfo::updateDctnryStore(char* buf,
    ColPosPair ** pos,
    const int totalRow,
    char* tokenBuf)
{
    // If this is a VARBINARY column; convert the ascii hex string into binary
    //  data and fix the length (it's now only half as long).
    // Should be safe to modify pos and buf arrays outside a mutex, as no other
    // thread should be accessing the strings from the same buffer, for this
    // column.
    if (curCol.colType == WR_VARBINARY)
    {
#ifdef PROFILE
        Stats::startParseEvent(WE_STATS_COMPACT_VARBINARY);
#endif
        for (int i = 0; i < totalRow; i++)
        {
            pos[i][id].offset =
                compactVarBinary(buf + pos[i][id].start, pos[i][id].offset);
        }
#ifdef PROFILE
        Stats::startParseEvent(WE_STATS_COMPACT_VARBINARY);
#endif
    }

#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_WAIT_TO_PARSE_DCT);
#endif
    boost::mutex::scoped_lock lock(dictionaryMutex);
#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_WAIT_TO_PARSE_DCT);
#endif

    int rc = fStore->insertDctnry( buf, pos, totalRow, id, tokenBuf );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "updateDctnryStore: error adding rows to store file for " <<
               "OID-"      << column.dctnry.dctnryOid    <<
               "; DBRoot-" << curCol.dataFile.fDbRoot    <<
               "; part-"   << curCol.dataFile.fPartition <<
               "; seg-"    << curCol.dataFile.fSegment   <<
               "; " << ec.errorString(rc);
        fLog->logMsg( oss.str(), rc, MSGLVL_CRITICAL );
        return rc;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// No action necessary for uncompressed dictionary files
//------------------------------------------------------------------------------
int ColumnInfo::saveDctnryStoreHWMChunk()
{
    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Initialize fLastInputRowInCurrentExtent used in detecting when a Read Buffer
// is crossing an extent boundary, so that we can accurately track the min/max
// for each extent as the Read buffers are parsed.
//------------------------------------------------------------------------------
void ColumnInfo::lastInputRowInExtentInit( )
{
    // Reworked initial block skipping for compression:
    const unsigned int ROWS_PER_EXTENT =
        BRMWrapper::getInstance()->getExtentRows();
    RID numRowsLeftInExtent = 0;
    RID numRowsWritten = sizeWritten / curCol.colWidth;
    if ((numRowsWritten % ROWS_PER_EXTENT) != 0)
        numRowsLeftInExtent = ROWS_PER_EXTENT -
            (numRowsWritten % ROWS_PER_EXTENT);
    
    // Handle edge case; if numRowsLeftInExtent comes out to be 0, then
    // current extent is full.  In this case we first bump up row count by a
    // full extent before we subtract by 1 to get the last row number in extent.
    bool bRoomToAddToOriginalExtent = true;
    if (numRowsLeftInExtent == 0)
    {
        numRowsLeftInExtent += BRMWrapper::getInstance()->getExtentRows();
        bRoomToAddToOriginalExtent = false;
    }

    fLastInputRowInCurrentExtent = numRowsLeftInExtent - 1;

    // If we have a pre-existing extent that we are going to add rows to,
    // then we need to add that extent to our ColExtInf object, so that we
    // can update the CP min/max at the end of the bulk load job.
    if ( bRoomToAddToOriginalExtent )
    {
        fColExtInf->addFirstEntry(fLastInputRowInCurrentExtent,
                                  savedLbid );
    }
}

//------------------------------------------------------------------------------
// Increment fLastRIDInExtent to the end of the next extent.
//------------------------------------------------------------------------------
void ColumnInfo::lastInputRowInExtentInc( )
{
    fLastInputRowInCurrentExtent += BRMWrapper::getInstance()->getExtentRows();
}

//------------------------------------------------------------------------------
// Parsing is complete for this column.  Flush pending data.  Close the current
// segment file, and corresponding dictionary store file (if applicable).  Also
// flushes PrimProc cache, and clears memory taken up by this ColumnInfo object.
//------------------------------------------------------------------------------
int ColumnInfo::finishParsing( )
{
    int rc = NO_ERROR;

    // Close the dctnry file handle.
    if (fStore)
    {
        rc = closeDctnryStore(false);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "finishParsing: close dictionary file error with column " <<
                column.colName << "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR);
            return rc;
        }
    }

    // We don't need the mutex to protect against concurrent access by other
    // threads, since by the time we get to this point, this is the last
    // thread working on this column.  But, we use the mutex to insure that
    // we see the latest state that may have been set by another parsing thread
    // working with the same column.
    boost::mutex::scoped_lock lock(fColMutex);

    // Force the flushing of remaining data in the output buffer
    rc = colBufferMgr->flush( );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "finishParsing: flush error with column " << column.colName <<
            "; " << ec.errorString(rc);
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR);
        return rc;
    }

    // Close the column file
    rc = closeColumnFile(false,false);
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "finishParsing: close column file error with column " <<
            column.colName << "; " << ec.errorString(rc);
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR);
        return rc;
    }

    // After closing the column and dictionary store files,
    // flush any updated dictionary blocks in PrimProc
    if (fDictBlocks.size() > 0)
    {
#ifdef PROFILE
        Stats::startParseEvent(WE_STATS_FLUSH_PRIMPROC_BLOCKS);
#endif
        cacheutils::flushPrimProcBlocks ( fDictBlocks );
#ifdef PROFILE
        Stats::stopParseEvent(WE_STATS_FLUSH_PRIMPROC_BLOCKS);
#endif
    }

    clearMemory();

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Store updated column information in BRMReporter for this column at EOJ;
// so that Extent Map CP information and HWM's can be updated.
// Bug2117-Src code from this function was factored over from we_tableinfo.cpp.
//
// We use mutex because this function is called by "one" of the parsing threads
// when parsing is complete for all the columns from this column's table.
// We use the mutex to insure that this parsing thread, which ends up being
// responsible for updating BRM for this column, is getting the most up to
// date values in fSegFileUpdateList, sizeWritten, etc which may have been
// set by another parsing thread.
//------------------------------------------------------------------------------
void ColumnInfo::getBRMUpdateInfo( BRMReporter& brmReporter )
{
    boost::mutex::scoped_lock lock(fColMutex);
    // Useful for debugging
    //printCPInfo();

    int entriesAdded = getHWMInfoForBRM( brmReporter );

    // If we added any rows (HWM update count > 0), then update corresponding CP
    if (entriesAdded > 0)
        getCPInfoForBRM( brmReporter );
}

//------------------------------------------------------------------------------
// Get updated Casual Partition (CP) information for BRM for this column at EOJ.
//------------------------------------------------------------------------------
void ColumnInfo::getCPInfoForBRM( BRMReporter& brmReporter )
{
    fColExtInf->getCPInfoForBRM(
        ((column.weType  == WriteEngine::WR_CHAR) &&
         (column.colType != COL_TYPE_DICT)),
         brmReporter );
}

//------------------------------------------------------------------------------
// Get updated HWM information for BRM for this column at EOJ.
// Returns count of the number of HWM entries added to the BRMReporter.
//------------------------------------------------------------------------------
int ColumnInfo::getHWMInfoForBRM( BRMReporter& brmReporter )
{
    //..If we wrote out any data to the last segment file, then
    //  update HWM for the current (last) segment file we were writing to.

    //Bug1374 - Update HWM when data added to file
    if ( sizeWritten > sizeWrittenStart )
    {
        //Bug1372.
        HWM hwm = (sizeWritten - 1)/BYTE_PER_BLOCK;

        addToSegFileList( curCol.dataFile, hwm );
    }

    int entriesAdded = 0;

    //..Update HWM for each segment file we touched, including the last one
    for (unsigned int iseg=0;
         iseg<fSegFileUpdateList.size(); iseg++)
    {
        // Log for now; may control with debug flag later
        //if (fLog->isDebug( DEBUG_1 ))
        {
            std::ostringstream oss;
            oss << "Saving HWM update for OID-"                       <<
                fSegFileUpdateList[iseg].fid                          <<
                "; hwm-"       << fSegFileUpdateList[iseg].hwm        <<
                "; DBRoot-"    << fSegFileUpdateList[iseg].fDbRoot    <<
                "; partition-" << fSegFileUpdateList[iseg].fPartition <<
                "; segment-"   << fSegFileUpdateList[iseg].fSegment;

            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        BRM::BulkSetHWMArg hwmArg;
        hwmArg.oid     = fSegFileUpdateList[iseg].fid;
        hwmArg.partNum = fSegFileUpdateList[iseg].fPartition;
        hwmArg.segNum  = fSegFileUpdateList[iseg].fSegment;
        hwmArg.hwm     = fSegFileUpdateList[iseg].hwm;
        brmReporter.addToHWMInfo( hwmArg );
        entriesAdded++;
    }
    fSegFileUpdateList.clear(); // don't need vector anymore, so release memory

    return entriesAdded;
}

//------------------------------------------------------------------------------
// Prepare the initial column segment file for import.
//------------------------------------------------------------------------------
int ColumnInfo::setupInitialColumnFile( HWM oldHwm, HWM hwm )
{
    // Initialize the output buffer manager for the column.
    if (column.colType == COL_TYPE_DICT)
    {
        colBufferMgr = new ColumnBufferManagerDctnry(this, 8, fLog, 0);
    }
    else
    {
        colBufferMgr = new ColumnBufferManager(this, column.width, fLog, 0);
    }
    RETURN_ON_ERROR( colBufferMgr->setDbFile(curCol.dataFile.pFile,hwm,0) );

    RETURN_ON_ERROR( colOp->getFileSize2(curCol.dataFile.pFile, fileSize) );

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

    // Seek till the HWM lbid.
    // Store the current allocated file size in availFileSize.
    long long byteOffset = (long long)hwm * (long long)BYTE_PER_BLOCK;
    RETURN_ON_ERROR( colOp->setFileOffset(curCol.dataFile.pFile, byteOffset) );

    sizeWritten      = byteOffset;
    sizeWrittenStart = sizeWritten;
    availFileSize    = fileSize - sizeWritten;

    if (fLog->isDebug( DEBUG_1 ))
    {
        std::ostringstream oss;
        oss << "Init raw data offsets in column file OID-" <<
                curCol.dataFile.fid <<
            "; DBRoot-" << curCol.dataFile.fDbRoot    <<
            "; part-"   << curCol.dataFile.fPartition <<
            "; seg-"    << curCol.dataFile.fSegment   <<
            "; begByte-"<< sizeWritten  <<
            "; endByte-"<< fileSize     <<
            "; freeBytes-" << availFileSize;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Update the number of bytes in the file, and the free space still remaining.
//------------------------------------------------------------------------------
void ColumnInfo::updateBytesWrittenCounts( unsigned int numBytesWritten )
{
    availFileSize = availFileSize - numBytesWritten;
    sizeWritten   = sizeWritten   + numBytesWritten;
}

//------------------------------------------------------------------------------
// Tell whether the current column segment file being managed by ColumnInfo,
// has filled up all its extents with data.
//------------------------------------------------------------------------------
bool ColumnInfo::isFileComplete() const
{
    if ((sizeWritten / column.width) >= fMaxNumRowsPerSegFile)
        return true;

    return false;
}

//------------------------------------------------------------------------------
// Truncate specified dictionary store file for this column.
// Only applies to compressed columns.
//------------------------------------------------------------------------------
int ColumnInfo::truncateDctnryStore(
    OID /*dctnryOid*/, uint16_t /*root*/, uint32_t /*pNum*/, uint16_t /*sNum*/)
    const
{
    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Initialize last used auto-increment value from the current "next"
// auto-increment value taken from the system catalog.
// Don't need to use fAutoIncMutex in this function as long as we call it from
// the main thread, during preprocessing.  But we go ahead and use the mutex
// for completeness.  Using the mutex should not affect performance, since this
// function is only called once per table.
//------------------------------------------------------------------------------
void ColumnInfo::initAutoInc( long long nextValue )
{
    boost::mutex::scoped_lock lock(fAutoIncMutex);
    // nextValue is unusable if < 1; probably means we already reached max value
    if (nextValue < 1)
        fAutoIncLastValue = column.fMaxIntSat;
    else
        fAutoIncLastValue = nextValue - 1;
}

//------------------------------------------------------------------------------
// Return "next" auto-increment value, based on last used auto-increment
// value tracked by this ColumnInfo object.
//------------------------------------------------------------------------------
long long ColumnInfo::getAutoInc( )
{
    long long nextValue = -1;

    boost::mutex::scoped_lock lock(fAutoIncMutex);
    // nextValue is returned as -1 if we reached max value
    if (fAutoIncLastValue < column.fMaxIntSat)
        nextValue = fAutoIncLastValue + 1;

    return nextValue;
}

//------------------------------------------------------------------------------
// Reserves the requested number of auto-increment numbers (autoIncCount).
// The starting value of the reserved block of numbers is returned in nextValue.
//------------------------------------------------------------------------------
int ColumnInfo::reserveAutoIncNums(uint autoIncCount, long long& nextValue )
{
    boost::mutex::scoped_lock lock(fAutoIncMutex);
    if ((column.fMaxIntSat - autoIncCount) < fAutoIncLastValue)
    {
        return ERR_AUTOINC_GEN_EXCEED_MAX;
    }

    nextValue = fAutoIncLastValue + 1;
    fAutoIncLastValue += autoIncCount;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Get current dbroot, partition, segment, and HWM for this column.
//
// We use mutex because this function is called by "one" of the parsing threads
// when parsing is complete for all the columns from this column's table.
// We use the mutex to insure that this parsing thread, which ends up being
// responsible for wrapping up this column, is getting the most up to
// date values for dbroot, partition, segment, and HWM which may have been
// set by another parsing thread.
//------------------------------------------------------------------------------
void ColumnInfo::getSegFileInfo( File& fileInfo )
{
    boost::mutex::scoped_lock lock(fColMutex);
    fileInfo.fDbRoot    = curCol.dataFile.fDbRoot;
    fileInfo.fPartition = curCol.dataFile.fPartition;
    fileInfo.fSegment   = curCol.dataFile.fSegment;
    if (sizeWritten > 0)
        fileInfo.hwm    = (sizeWritten - 1)/BYTE_PER_BLOCK;
    else
        fileInfo.hwm    = 0;
}

//------------------------------------------------------------------------------
// utility to convert a Status enumeration to a string
//------------------------------------------------------------------------------
/* static */
void ColumnInfo::convertStatusToString(
    WriteEngine::Status status,
    std::string& statusString )
{
    static std::string statusStringParseComplete("PARSE_COMPLETE");
    static std::string statusStringReadComplete ("READ_COMPLETE");
    static std::string statusStringReadProgress ("READ_PROGRESS");
    static std::string statusStringNew          ("NEW");
    static std::string statusStringErr          ("ERR");
    static std::string statusStringUnknown      ("OTHER");

    switch (status)
    {
        case PARSE_COMPLETE:
            statusString = statusStringParseComplete;
            break;
        case READ_COMPLETE:
            statusString = statusStringReadComplete;
            break;
        case READ_PROGRESS:
            statusString = statusStringReadProgress;
            break;
        case NEW:
            statusString = statusStringNew;
            break;
        case ERR:
            statusString = statusStringErr;
            break;
        default:
            statusString = statusStringUnknown;
            break;
    }
}

}
