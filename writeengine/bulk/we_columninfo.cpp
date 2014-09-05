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
* $Id: we_columninfo.cpp 4684 2013-06-18 19:47:46Z dcathey $
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
#include "we_columnautoinc.h"
#include "we_dbrootextenttracker.h"
#include "we_brmreporter.h"

#include "we_tableinfo.h"


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
                       const JobColumn& columnIn,
                       DBRootExtentTracker* pDBRootExtTrk,
                       TableInfo* pTableInfo) :
                       id(idIn),
                       lastProcessingTime(0),
#ifdef PROFILE
                       totalProcessingTime(0),
#endif
                       fColBufferMgr(0),
                       availFileSize(0),
                       fileSize(0),
                       fLog(logger),
                       fSavedHWM(0),
                       fSavedLbid(0),
                       fSizeWrittenStart(0),
                       fSizeWritten(0),
                       fLastInputRowInCurrentExtent(0),
                       fLoadingAbbreviatedExtent(false),
                       fColExtInf(0),
                       fMaxNumRowsPerSegFile(0),
                       fStore(0),
                       fAutoIncLastValue(0),
                       fSaturatedRowCnt(0),
                       fpTableInfo(pTableInfo),
                       fAutoIncMgr(0),
                       fDbRootExtTrk(pDBRootExtTrk),
                       fColWidthFactor(1),
                       fDelayedFileCreation(INITIAL_DBFILE_STAT_FILE_EXISTS)
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
        case WriteEngine::WR_USHORT:
        case WriteEngine::WR_UBYTE:
        case WriteEngine::WR_ULONGLONG:
        case WriteEngine::WR_UINT:
        default:
        {
            fColExtInf = new ColExtInf(column.mapOid, logger);
            break;
        }
    }

    colOp.reset(new ColumnOpBulk(logger, column.compressionType));

    fMaxNumRowsPerSegFile = BRMWrapper::getInstance()->getExtentRows() *
                            Config::getExtentsPerSegmentFile();

    // Create auto-increment object to manage auto-increment next-value
    if (column.autoIncFlag)
    {
        fAutoIncMgr = new ColumnAutoIncIncremental(logger);
        // formerly used ColumnAutoIncJob for Shared Everything
        // fAutoIncMgr = new ColumnAutoIncJob(logger);
    }
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
    //}

    if (fColExtInf)
        delete fColExtInf;

    if (fAutoIncMgr)
        delete fAutoIncMgr;

    if (fDbRootExtTrk)
        delete fDbRootExtTrk;
}

//------------------------------------------------------------------------------
// Clear memory consumed by this ColumnInfo object.
//------------------------------------------------------------------------------
void ColumnInfo::clearMemory( )
{
    if (fColBufferMgr)
    {
        delete fColBufferMgr;
        fColBufferMgr = 0;
    }

    fDictBlocks.clear();
}

//------------------------------------------------------------------------------
// If at the start of the job, We have encountered a PM that has no DB file for
// this column, then this function is called to setup delayed file creation.
// A starting DB file will be created if/when we determine that we have rows
// to be processed.
//------------------------------------------------------------------------------
void ColumnInfo::setupDelayedFileCreation(
    u_int16_t dbRoot,
    u_int32_t partition,
    u_int16_t segment,
    HWM hwm )
{
    fDelayedFileCreation = INITIAL_DBFILE_STAT_CREATE_FILE;
    fSavedHWM            = hwm;
    fSavedLbid           = INVALID_LBID;

    colOp->initColumn ( curCol );
    colOp->setColParam( curCol, id,
        column.width,
        column.dataType,
        column.weType,
        column.mapOid,
        column.compressionType,
        dbRoot, partition, segment );
}

//------------------------------------------------------------------------------
// Create a DB file as part of delayed file creation.  See setupDelayedFile-
// Creation for an explanation.
//------------------------------------------------------------------------------
int ColumnInfo::createDelayedFileIfNeeded( const std::string& tableName )
{
    int rc = NO_ERROR;

    // For optimization sake, we use a separate mutex (fDelayedFileCreateMutex)
    // exclusively reserved to be used as the gatekeeper to this function.
    // No sense in waiting for a fColMutex lock, when 99.99% of the time,
    // all we need to do is check fDelayedFileCreate, see that it's value
    // is INITIAL_DBFILE_STAT_FILE_EXISTS, and exit the function.
    boost::mutex::scoped_lock lock(fDelayedFileCreateMutex);

    if (fDelayedFileCreation == INITIAL_DBFILE_STAT_FILE_EXISTS)
        return NO_ERROR;

    // Don't try creating extent again if we are already in error state with a
    // previous thread failing to create this extent.
    if (fDelayedFileCreation == INITIAL_DBFILE_STAT_ERROR_STATE)
    {
        rc = ERR_FILE_CREATE;
        std::ostringstream oss;
        oss << "Previous attempt failed to create initial dbroot" <<
            curCol.dataFile.fDbRoot <<
            " extent for column file OID-" << column.mapOid <<
            "; dbroot-"       << curCol.dataFile.fDbRoot     <<
            "; partition-"    << curCol.dataFile.fPartition;
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    // Once we get this far, we go ahead and acquire a fColMutex lock.  The
    // fDelayedFileCreateMutex lock might suffice, but better to explicitly
    // lock fColMutex since we are modifying attributes that we typically
    // change within the scope of a fColMutex lock.
    boost::mutex::scoped_lock lock2(fColMutex);

    uint16_t dbRoot      = curCol.dataFile.fDbRoot;
    uint32_t partition   = curCol.dataFile.fPartition;
    HWM      hwm         = fSavedHWM; // number of blks to skip at start of file

    // We don't have a file on this PM, so we create an initial file
    ColumnOpBulk tempColOp(fLog, column.compressionType);
    
    bool         createLeaveFileOpen = false;
    FILE*        createPFile      = 0;
    uint16_t     createDbRoot     = dbRoot;
    uint32_t     createPartition  = partition;
    uint16_t     createSegment    = 0;
    std::string  createSegFile;
    HWM          createHwm        = 0;    //output
    BRM::LBID_t  createStartLbid  = 0;    //output
    bool         createNewFile    = true; //output
    int          createAllocSize  = 0;    //output
    char*        createHdrs       = 0;    //output

    std::string allocErrMsg;
    rc = fpTableInfo->allocateBRMColumnExtent( curCol.dataFile.fid,
        createDbRoot,
        createPartition,
        createSegment,
        createStartLbid,
        createAllocSize,
        createHwm,
        allocErrMsg );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error creating initial dbroot" << dbRoot <<
            " BRM extent for OID-" << column.mapOid <<
            "; dbroot-"     << dbRoot    <<
            "; partition-"  << partition <<
            "; " << ec.errorString(rc)   <<
            "; " << allocErrMsg;
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        fDelayedFileCreation = INITIAL_DBFILE_STAT_ERROR_STATE;
        return rc;
    }
    uint16_t segment = createSegment;
    partition        = createPartition;  // update our partition variable in
                                         // case extent was added to a different
                                         // partition than we intended
    BRM::LBID_t lbid = createStartLbid;

    rc = tempColOp.extendColumn(
        curCol,
        createLeaveFileOpen,
        true, // this is first file on this PM, does extra init DMC_NEW_PM_FILE
        createHwm,
        createStartLbid,
        createAllocSize,
        createDbRoot,
        createPartition,
        createSegment,
        createSegFile,
        createPFile,
        createNewFile,
        createHdrs);

    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error adding initial dbroot" << dbRoot <<
            " extent to column file OID-" << column.mapOid <<
            "; dbroot-"       << dbRoot    <<
            "; partition-"    << partition <<
            "; segment-"      << segment   <<
            "; " << ec.errorString(rc);
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        fDelayedFileCreation = INITIAL_DBFILE_STAT_ERROR_STATE;
        return rc;
    }

    // We don't have a file on this PM, so we create an initial file
    std::ostringstream oss1;
    oss1 << "Creating initial column extent on DBRoot-" << createDbRoot <<
        " for OID-" << column.mapOid   <<
        "; part-"   << createPartition <<
        "; seg-"    << createSegment   <<
        "; hwm-"    << createHwm       <<
        "; LBID-"   << createStartLbid <<
        "; file-"   << createSegFile;
    fLog->logMsg( oss1.str(), MSGLVL_INFO2 );

    // Create corresponding dictionary store file if applicable
    if(column.colType == COL_TYPE_DICT)
    {
        std::ostringstream oss;
        oss << "Creating initial dictionary extent on dbroot"<<dbRoot <<
            " (segment " << segment << 
            ") for dictionary OID " << column.dctnry.dctnryOid;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        BRM::LBID_t dLbid;
        Dctnry* tempD = 0;
        if (column.dctnry.fCompressionType != 0)
        {
            DctnryCompress1* tempD1;
            tempD1 = new DctnryCompress1;
            tempD1->setMaxActiveChunkNum(1);
            tempD1->setBulkFlag(true);
            tempD = tempD1;
        }
        else
        {
            tempD = new DctnryCompress0;
        }

        boost::scoped_ptr<Dctnry> refDctnry(tempD);
        rc = tempD->createDctnry(
            column.dctnry.dctnryOid,
            column.dctnryWidth,
            dbRoot,
            partition,
            segment,
            dLbid,
            true); // creating the store file
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error creating initial dbroot" << dbRoot <<
                " extent for dictionary file OID-" <<
                column.dctnry.dctnryOid        <<
                "; dbroot-"       << dbRoot    <<
                "; partition-"    << partition <<
                "; segment-"      << segment   <<
                "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            fDelayedFileCreation = INITIAL_DBFILE_STAT_ERROR_STATE;
            return rc;
        }

        rc = tempD->closeDctnry();
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error creating/closing initial dbroot" <<
                dbRoot << " extent for dictionary file OID-" <<
                column.dctnry.dctnryOid        <<
                "; partition-"    << partition <<
                "; segment-"      << segment   <<
                "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            fDelayedFileCreation = INITIAL_DBFILE_STAT_ERROR_STATE;
            return rc;
        }
    } // end of dictionary column processing

    rc = setupInitialColumnExtent(
        dbRoot, partition, segment,
        tableName, lbid, hwm, hwm, false, true );

    if (rc == NO_ERROR)
        fDelayedFileCreation = INITIAL_DBFILE_STAT_FILE_EXISTS;
    else
        fDelayedFileCreation = INITIAL_DBFILE_STAT_ERROR_STATE;

    return rc;
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
    //  are adding an extent, that fSizeWritten is a multiple of blksize,
    //  which it should be.  If we are adding an extent, fSizeWritten should
    //  point to the last byte of a full extent boundary.
    HWM hwm = (fSizeWritten / BYTE_PER_BLOCK) - 1;

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

    // Call Config::initConfigCache() to force the Config class
    // to reload config cache "if" the config file has changed.
    Config::initConfigCache();

    bool bChangeFlag = Config::hasLocalDBRootListChanged();
    //if (fLog->isDebug( DEBUG_1 ))
    //{
    //  std::ostringstream oss;
    //  oss << "Checking DBRootListChangeFlag: " << bChangeFlag;
    //  fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    //}
    if (bChangeFlag)
    {
        rc = ERR_BULK_DBROOT_CHANGE;

        WErrorCodes ec;
        std::ostringstream oss;
        oss << "extendColumn: DBRoots changed; " <<
            ec.errorString( rc );
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

        return rc;
    }

    //..Declare variables used to advance to the next extent
    uint16_t    dbRootNext   = 0;
    uint32_t    partitionNext= 0;
    uint16_t    segmentNext  = 0;
    HWM         hwmNext      = 0;
    BRM::LBID_t startLbid;

    //..When we finish an extent, we typically should be advancing to the next
    //  DBRoot to create a "new" extent.  But "if" the user has moved a DBRoot
    //  from another PM to this PM, then we may have a partial extent that we
    //  need to fill up.  Here's where we just fill out such partially filled
    //  extents with empty values, until we can get back to a "normal" full
    //  extent boundary case.
    bool bAllocNewExtent = false;
    while (!bAllocNewExtent)
    {
        //..If we have a DBRoot Tracker, then use that to determine next DBRoot
        //  to rotate to, else the old legacy BRM extent allocator will assign,
        //  if we pass in a dbroot of 0.
        bAllocNewExtent = true;
        if (fDbRootExtTrk)
        {
            bAllocNewExtent = fDbRootExtTrk->nextSegFile(
                dbRootNext, partitionNext, segmentNext, hwmNext, startLbid );
        }

        // If our next extent is a partial extent, then fill out that extent
        // to the next full extent boundary, and round up HWM accordingly.
        if (!bAllocNewExtent)
        {
            rc = extendColumnOldExtent( dbRootNext,
                partitionNext, segmentNext, hwmNext );
            if (rc != NO_ERROR)
                return rc;
        }
    }

    // Once we are back on a "normal" full extent boundary, we add a new extent
    // to resume adding rows.
    rc = extendColumnNewExtent( saveLBIDForCP, dbRootNext, partitionNext );

    return rc;
}

//------------------------------------------------------------------------------
// Add a new extent to this column, at the specified DBRoot.  Partition may be
// used if DBRoot is empty.
//------------------------------------------------------------------------------
int ColumnInfo::extendColumnNewExtent(
    bool     saveLBIDForCP,
    uint16_t dbRootNew,
    uint32_t partitionNew )
{
    //..Declare variables used to advance to the next extent
    FILE*       pFileNew     = 0;
    HWM         hwmNew       = 0;
    bool        newFile      = false;
    std::string segFileNew;

    uint16_t    segmentNew   = 0;
    BRM::LBID_t startLbid;

    char hdr[ compress::IDBCompressInterface::HDR_BUF_LEN * 2 ];

    // Extend the column by adding an extent to the next
    // DBRoot, partition, and segment file in the rotation
    int allocsize = 0;
    std::string allocErrMsg;
    int rc = fpTableInfo->allocateBRMColumnExtent( curCol.dataFile.fid,
        dbRootNew,
        partitionNew,
        segmentNew,
        startLbid,
        allocsize,
        hwmNew,
        allocErrMsg );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "extendColumnNewExtent: error creating BRM extent after " <<
            "column OID-" << curCol.dataFile.fid          <<
            "; DBRoot-"   << curCol.dataFile.fDbRoot      <<
            "; part-"     << curCol.dataFile.fPartition   <<
            "; seg-"      << curCol.dataFile.fSegment;

        oss << "; newDBRoot-" << dbRootNew                <<
            "; newpart-"      << partitionNew             <<
            "; " << ec.errorString(rc)                    <<
            "; " << allocErrMsg;
        fLog->logMsg( oss.str(), rc, MSGLVL_CRITICAL );
        fpTableInfo->fBRMReporter.addToErrMsgEntry(oss.str());

        return rc;
    }

    rc = colOp->extendColumn ( curCol,
        true,  // leave file open
        false, // don't treat as first file on this PM; if it "were", our pre-
               // processing would have created by the time we reached this pt
        hwmNew,
        startLbid,
        allocsize,
        dbRootNew,
        partitionNew,
        segmentNew,
        segFileNew,
        pFileNew,
        newFile,
        hdr );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "extendColumnNewExtent: error adding file extent after " <<
            "column OID-" << curCol.dataFile.fid          <<
            "; DBRoot-"   << curCol.dataFile.fDbRoot      <<
            "; part-"     << curCol.dataFile.fPartition   <<
            "; seg-"      << curCol.dataFile.fSegment;

        oss << "; newDBRoot-" << dbRootNew                <<
            "; newpart-"      << partitionNew             <<
            "; newseg-"       << segmentNew               <<
            "; fbo-"          << hwmNew                   <<
            "; " << ec.errorString(rc);
        fLog->logMsg( oss.str(), rc, MSGLVL_CRITICAL );
        fpTableInfo->fBRMReporter.addToErrMsgEntry(oss.str());

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
        oss << "extendColumnNewExtent: error moving to new extent in " <<
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
            "; begByte-"<< fSizeWritten <<
            "; endByte-"<< fileSize     <<
            "; freeBytes-" << availFileSize;
        fLog->logMsg( oss2.str(), MSGLVL_INFO2 );
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Fill out existing partial extent to extent boundary, so that we can resume
// inserting rows on an extent boundary basis.  This use case should only take
// place when a DBRoot with a partial extent has been moved from one PM to
// another.
//------------------------------------------------------------------------------
int ColumnInfo::extendColumnOldExtent(
    uint16_t    dbRootNext,
    uint32_t    partitionNext,
    uint16_t    segmentNext,
    HWM         hwmNext )
{
    const unsigned int BLKS_PER_EXTENT =
    (BRMWrapper::getInstance()->getExtentRows() * column.width)/BYTE_PER_BLOCK;
    HWM hwmNextExtentBoundary = hwmNext;

    // Round up HWM to the end of the current extent
    unsigned int nBlks = hwmNext + 1;
    unsigned int nRem  = nBlks % BLKS_PER_EXTENT;
    if (nRem > 0)
        hwmNextExtentBoundary = nBlks - nRem + BLKS_PER_EXTENT - 1;
    else
        hwmNextExtentBoundary = nBlks - 1;

    std::ostringstream oss;
    oss << "Padding partial extent to extent boundary in OID-" <<
           curCol.dataFile.fid <<
        "; DBRoot-" << dbRootNext    <<
        "; part-"   << partitionNext <<
        "; seg-"    << segmentNext   <<
        "; oldhwm-" << hwmNext       <<
        "; newhwm-" << hwmNextExtentBoundary;
    fLog->logMsg( oss.str(), MSGLVL_INFO2 );

    long long fileSizeBytes;
    int rc = colOp->getFileSize3( curCol.dataFile.fid,
        dbRootNext, partitionNext, segmentNext, fileSizeBytes);
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "extendColumnOldExtent: error padding partial extent for " <<
               "column OID-" << curCol.dataFile.fid           <<
               "; DBRoot-"   << curCol.dataFile.fDbRoot       <<
               "; part-"     << curCol.dataFile.fPartition    <<
               "; seg-"      << curCol.dataFile.fSegment      <<
               "; hwm-"      << curCol.dataFile.hwm;
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

        return rc;
    }

    curCol.dataFile.pFile        = 0;
    curCol.dataFile.fDbRoot      = dbRootNext;
    curCol.dataFile.fPartition   = partitionNext;
    curCol.dataFile.fSegment     = segmentNext;
    curCol.dataFile.hwm          = hwmNextExtentBoundary;
    curCol.dataFile.fSegFileName.clear();

    // See if we have an abbreviated extent that needs to be expanded on disk
    if (fileSizeBytes == (long long)INITIAL_EXTENT_ROWS_TO_DISK * column.width)
    {
        std::string segFile;

        FILE* pFile = colOp->openFile( curCol,
            dbRootNext, partitionNext, segmentNext, segFile );
        if ( !pFile )
        {
            std::ostringstream oss;
            rc = ERR_FILE_OPEN;
            oss << "extendColumnOldExtent: error padding partial extent for " <<
                   "column OID-" << curCol.dataFile.fid           <<
                   "; DBRoot-"   << curCol.dataFile.fDbRoot       <<
                   "; part-"     << curCol.dataFile.fPartition    <<
                   "; seg-"      << curCol.dataFile.fSegment      <<
                   "; hwm-"      << curCol.dataFile.hwm;
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

            return rc;
        }

        rc = colOp->expandAbbrevColumnExtent( pFile, dbRootNext,
            column.emptyVal, column.width);
        if (rc != NO_ERROR)
        {
            std::ostringstream oss;
            oss << "extendColumnOldExtent: error padding partial extent for " <<
                   "column OID-" << curCol.dataFile.fid           <<
                   "; DBRoot-"   << curCol.dataFile.fDbRoot       <<
                   "; part-"     << curCol.dataFile.fPartition    <<
                   "; seg-"      << curCol.dataFile.fSegment      <<
                   "; hwm-"      << curCol.dataFile.hwm;
            fLog->logMsg( oss.str(), rc, MSGLVL_CRITICAL );
            fpTableInfo->fBRMReporter.addToErrMsgEntry(oss.str());

            colOp->closeFile( pFile );

            return rc;
        }

        colOp->closeFile( pFile );
    }

    addToSegFileList( curCol.dataFile, hwmNextExtentBoundary );

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
    fSavedHWM         = curCol.dataFile.hwm;
    setFileSize( curCol.dataFile.hwm, false );
    long long byteOffset = (long long)fSavedHWM * (long long)BYTE_PER_BLOCK;
    fSizeWritten      = byteOffset;
    fSizeWrittenStart = fSizeWritten;
    availFileSize     = fileSize - fSizeWritten;

    // If we are adding an extent as part of preliminary block skipping, then
    // we won't have a ColumnBufferManager object yet, but that's okay, because
    // we are only adding the empty extent at this point.
    if (fColBufferMgr)
    {
        RETURN_ON_ERROR( fColBufferMgr->setDbFile(
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
// DMC-SHARED_NOTHING_NOTE: Is it safe to assume only part0 seg0 is abbreviated?
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
            fpTableInfo->fBRMReporter.addToErrMsgEntry(oss.str());
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
                fpTableInfo->fBRMReporter.addToErrMsgEntry(oss.str());
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
// Initialize fLastInputRowInCurrentExtent used in detecting when a Read Buffer
// is crossing an extent boundary, so that we can accurately track the min/max
// for each extent as the Read buffers are parsed.
//------------------------------------------------------------------------------
void ColumnInfo::lastInputRowInExtentInit( bool bIsNewExtent )
{
    // Reworked initial block skipping for compression:
    const unsigned int ROWS_PER_EXTENT =
        BRMWrapper::getInstance()->getExtentRows();
    RID numRowsLeftInExtent = 0;
    RID numRowsWritten = fSizeWritten / curCol.colWidth;
    if ((numRowsWritten % ROWS_PER_EXTENT) != 0)
        numRowsLeftInExtent = ROWS_PER_EXTENT -
            (numRowsWritten % ROWS_PER_EXTENT);
    
    bool bRoomToAddToOriginalExtent = true;
    if (fSizeWritten > 0)
    {
        // Handle edge case; if numRowsLeftInExtent comes out to be 0, then
        // current extent is full.  In this case we first bump up row count
        // by a full extent before we subtract by 1 to get the last row number
        // in extent.
        if (numRowsLeftInExtent == 0)
        {
            numRowsLeftInExtent = ROWS_PER_EXTENT;;
            bRoomToAddToOriginalExtent = false;
        }
    }
    else
    {
        // Starting new file with empty extent, so set row count to full extent
        numRowsLeftInExtent = ROWS_PER_EXTENT;
    }

    fLastInputRowInCurrentExtent = numRowsLeftInExtent - 1;

    // If we have a pre-existing extent that we are going to add rows to,
    // then we need to add that extent to our ColExtInf object, so that we
    // can update the CP min/max at the end of the bulk load job.
    if ( bRoomToAddToOriginalExtent )
    {
        fColExtInf->addFirstEntry(fLastInputRowInCurrentExtent,
                                  fSavedLbid,
                                  bIsNewExtent );
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
    if (fColBufferMgr)
    {
        rc = fColBufferMgr->flush( );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "finishParsing: flush error with column " << column.colName<<
                "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR);
            return rc;
        }
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
        cacheutils::flushPrimProcAllverBlocks ( fDictBlocks );
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
// date values in fSegFileUpdateList, fSizeWritten, etc which may have been
// set by another parsing thread.
//------------------------------------------------------------------------------
void ColumnInfo::getBRMUpdateInfo( BRMReporter& brmReporter )
{
    boost::mutex::scoped_lock lock(fColMutex);
    // Useful for debugging
    //printCPInfo(column);

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
    fColExtInf->getCPInfoForBRM(column, brmReporter);
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
    if ( fSizeWritten > fSizeWrittenStart )
    {
        //Bug1372.
        HWM hwm = (fSizeWritten - 1)/BYTE_PER_BLOCK;

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
                 fSegFileUpdateList[iseg].fid                         <<
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
// Setup initial extent we will begin loading at start of import.
// DBRoot, partition, segment, etc for the starting extent are specified.
// If block skipping is causing us to advance to the next extent, then we
// set things up to point to the last block in the current extent.  When we
// start adding rows, we will automatically advance to the next extent.
//------------------------------------------------------------------------------
int ColumnInfo::setupInitialColumnExtent(
    u_int16_t   dbRoot,               // dbroot of starting extent
    u_int32_t   partition,            // partition number of starting extent
    u_int16_t   segment,              // segment number of starting extent
    const std::string& tblName,       // name of table containing this column
    BRM::LBID_t lbid,                 // starting LBID for starting extent
    HWM         oldHwm,               // original HWM
    HWM         hwm,                  // new projected HWM after block skipping
    bool        bSkippedToNewExtent,  // blk skipping to next extent
    bool        bIsNewExtent )        // treat as new extent (for CP updates)
{
    // Init the ColumnInfo object
    colOp->initColumn( curCol );
    colOp->setColParam( curCol, id,
        column.width,
        column.dataType,
        column.weType, 
        column.mapOid,
        column.compressionType,
        dbRoot, partition, segment );

    // Open the column file
    if(!colOp->exists(column.mapOid, dbRoot, partition, segment) )
    {
        std::ostringstream oss;
        oss << "Column file does not exist for OID-" << column.mapOid <<
           "; DBRoot-"    << dbRoot    <<
           "; partition-" << partition <<
           "; segment-"   << segment;
        fLog->logMsg( oss.str(), ERR_FILE_NOT_EXIST, MSGLVL_ERROR );
        return ERR_FILE_NOT_EXIST;
    }

    std::string segFile;
    int rc = colOp->openColumnFile( curCol, segFile );
    if(rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error opening column file for OID-" << column.mapOid <<
            "; DBRoot-"    << dbRoot    <<
            "; partition-" << partition <<
            "; segment-"   << segment   <<
            "; filename-"  << segFile   <<
            "; " << ec.errorString(rc);
        fLog->logMsg( oss.str(), ERR_FILE_OPEN, MSGLVL_ERROR );
        return ERR_FILE_OPEN;
    }

    std::ostringstream oss1;
    oss1 << "Initializing import: "    <<
        "Table-"      << tblName       <<
        "; Col-"      << column.colName;
    if (curCol.compressionType)
        oss1          << " (compressed)";
    oss1 <<  "; OID-" << column.mapOid <<
        "; hwm-"      << hwm;
    if (bSkippedToNewExtent)
        oss1          << " (full; load into next extent)";
    oss1 << "; file-" << curCol.dataFile.fSegFileName;
    fLog->logMsg( oss1.str(), MSGLVL_INFO2 );

    if(column.colType == COL_TYPE_DICT)
    {
        RETURN_ON_ERROR( openDctnryStore( true ) );
    }

    fSavedLbid = lbid;
    fSavedHWM  = hwm;

    if (bSkippedToNewExtent)
        oldHwm = hwm;
    rc = setupInitialColumnFile(oldHwm, hwm);
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error reading/positioning column file for OID-" <<
            column.mapOid <<
            "; DBRoot-"    << dbRoot    <<
            "; partition-" << partition <<
            "; segment-"   << segment   <<
            "; filename-"  << segFile   <<
            "; " << ec.errorString(rc);
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    // Reworked initial block skipping for compression:
    // Block skipping is causing us to wrap up this extent.  We consider
    // the current extent to be full, so we "pretend" to fill out the
    // last block by adding 8192 bytes to the bytes written count.
    // This will help trigger the addition of a new extent when we
    // try to store the first section of rows to the db.
    if (bSkippedToNewExtent)
    {
        updateBytesWrittenCounts( BYTE_PER_BLOCK );
        fSizeWrittenStart = fSizeWritten;
    }

    // Reworked initial block skipping for compression:
    // This initializes CP stats for first extent regardless of whether
    // we end up adding rows to this extent, or initial block skipping
    // ultimately causes us to start with a new extent.
    lastInputRowInExtentInit( bIsNewExtent );

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Prepare the initial column segment file for import.
//------------------------------------------------------------------------------
int ColumnInfo::setupInitialColumnFile( HWM oldHwm, HWM hwm )
{
    // Initialize the output buffer manager for the column.
    if (column.colType == COL_TYPE_DICT)
    {
        fColBufferMgr = new ColumnBufferManagerDctnry(this, 8, fLog, 0);
    }
    else
    {
        fColBufferMgr = new ColumnBufferManager(this, column.width, fLog, 0);
    }
    RETURN_ON_ERROR( fColBufferMgr->setDbFile(curCol.dataFile.pFile,hwm,0) );

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

    fSizeWritten      = byteOffset;
    fSizeWrittenStart = fSizeWritten;
    availFileSize     = fileSize - fSizeWritten;

    if (fLog->isDebug( DEBUG_1 ))
    {
        std::ostringstream oss;
        oss << "Init raw data offsets in column file OID-" <<
                curCol.dataFile.fid <<
            "; DBRoot-" << curCol.dataFile.fDbRoot    <<
            "; part-"   << curCol.dataFile.fPartition <<
            "; seg-"    << curCol.dataFile.fSegment   <<
            "; begByte-"<< fSizeWritten <<
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
    fSizeWritten  = fSizeWritten   + numBytesWritten;
}

//------------------------------------------------------------------------------
// Tell whether the current column segment file being managed by ColumnInfo,
// has filled up all its extents with data.
//------------------------------------------------------------------------------
bool ColumnInfo::isFileComplete() const
{
    if ((fSizeWritten / column.width) >= fMaxNumRowsPerSegFile)
        return true;

    return false;
}

//------------------------------------------------------------------------------
// Initialize last used auto-increment value from the current "next"
// auto-increment value taken from the system catalog (or BRM).
//------------------------------------------------------------------------------
int ColumnInfo::initAutoInc( const std::string& fullTableName )
{
    int rc = fAutoIncMgr->init( fullTableName, this );

    return rc;
}

//------------------------------------------------------------------------------
// Reserves the requested number of auto-increment numbers (autoIncCount).
// The starting value of the reserved block of numbers is returned in nextValue.
//------------------------------------------------------------------------------
int ColumnInfo::reserveAutoIncNums(uint autoIncCount, uint64_t& nextValue )
{
    int rc = fAutoIncMgr->reserveNextRange( autoIncCount, nextValue );

    return rc;
}

//------------------------------------------------------------------------------
// Finished using auto-increment.  Current value can be committed back to the
// system catalog (or BRM).
//------------------------------------------------------------------------------
int ColumnInfo::finishAutoInc( )
{
    int rc = fAutoIncMgr->finish( );

    return rc;
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
    if (fSizeWritten > 0)
        fileInfo.hwm    = (fSizeWritten - 1)/BYTE_PER_BLOCK;
    else
        fileInfo.hwm    = 0;
}

//------------------------------------------------------------------------------
// Open a new or existing Dictionary store file based on the DBRoot,
// partition, and segment settings in curCol.dataFile.
//------------------------------------------------------------------------------
int ColumnInfo::openDctnryStore( bool bMustExist )
{
    int rc = NO_ERROR;

    if ( column.dctnry.fCompressionType != 0)
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
    if (column.fWithDefault)
        fStore->setDefault( column.fDefaultChr );
    fStore->setImportDataMode( fpTableInfo->getImportDataMode() );

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
            fDictBlocks.push_back(fStore->getCurLbid());

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
            fpTableInfo->fBRMReporter.addToErrMsgEntry(oss.str());

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
                   "OID-"    << column.dctnry.dctnryOid <<
                   "; file-" << fStore->getFileName()   <<
                   "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        }

        delete fStore;
        fStore = 0;
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
    long long truncCount = 0;    // No. of rows with truncated values

    // If this is a VARBINARY column; convert the ascii hex string into binary
    //  data and fix the length (it's now only half as long).
    // Should be safe to modify pos and buf arrays outside a mutex, as no other
    // thread should be accessing the strings from the same buffer, for this
    // column.
    // This only applies to default text mode.  This step is bypassed for
    // binary imports, because in that case, the data is already true binary.
    if ((curCol.colType == WR_VARBINARY) &&
        (fpTableInfo->getImportDataMode() == IMPORT_DATA_TEXT))
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
    boost::mutex::scoped_lock lock(fDictionaryMutex);
#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_WAIT_TO_PARSE_DCT);
#endif

    int rc = fStore->insertDctnry( buf, pos, totalRow, id, tokenBuf, truncCount );
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
        fpTableInfo->fBRMReporter.addToErrMsgEntry(oss.str());
        return rc;
    }
    incSaturatedCnt( truncCount );

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
