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

/*******************************************************************************
* $Id: we_columninfo.h 3615 2012-03-09 16:41:14Z dcathey $
*
*******************************************************************************/

/** @file
 *  Contains main class used to manage column information.
 */

#ifndef _WE_COLUMNINFO_H
#define _WE_COLUMNINFO_H

#include "we_type.h"
#include "we_brm.h"
#include "we_colop.h"
#include "we_colbufmgr.h"
#include "we_colextinf.h"
#include "we_dctnrycompress.h"
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <sys/time.h>
#include <vector>

#if defined(_MSC_VER) && !defined(_WIN64)
#  ifndef InterlockedAdd
#    define InterlockedAdd64 InterlockedAdd
#    define InterlockedAdd(x, y) ((x) + (y))
#  endif
#endif

namespace WriteEngine
{
class Log;
class BRMReporter;

enum Status 
{
    PARSE_COMPLETE = 0,
    READ_COMPLETE,
    READ_PROGRESS,
    NEW,
    ERR
};

struct LockInfo
{
    int locker;
    Status status;
    LockInfo() : locker(-1), status(WriteEngine::NEW) { }
};

//------------------------------------------------------------------------------
// Note the following mutex lock usage using fColMutex within ColumnInfo.
// This is the mutex that is used to manage the state of the current segment
// column file associated with ColumnInfo.  The state of the column being
// managed by ColumnInfo is typically driven by calls from BulkLoadBuffer
// to ColumnBufferManager; so ColumnBufferManager frequently initiates the
// mutex lock.  However, when parsing is complete for a table, then some
// ColumnInfo functions are called outside of ColumnBufferManager scope (by
// TableInfo::setParseComplete), and these ColumnInfo functions (getSegFileInfo
// and getBRMUpdateInfo) must lock the mutex on their own.
//
// Explicit locks initiated by these ColumnBufferManager functions:
//   reserveSection()
//   releaseSection()
//   intermediateFlush()
//   extendTokenColumn()
//
// Implicit locks assumed by these ColumnBufferManager functions:
//   resizeColumnBuffer() (within scope of lock in reserveSection())
//   rowsExtentCheck()    (within scope of lock in reserveSection())
//   writeToFile()        (within scope of lock in reserveSection() or
//                                                 releaseSection())
//   writeToFileExtentCheck() (within scope of lock in writeToFile() or
//                                                 flush()
//   finishFile()         (within scope of lock in finishParsing()
//   flush()              (within scope of lock in intermediateFlush() or
//                                                 finishParsing())
//
// Other ColumnBufferManager functions:
//   setDbFile()   - called by main thread during preprocesing, or within scope
//                   of a lock when an extent is being added.
//   resetColBuf() - called within scope of a lock when an extent is being added
//
// Explicit locks in ColumnInfo:
//   getSegFileInfo()
//   getBRMUpdateInfo()
//   finishParsing()
//
//------------------------------------------------------------------------------

/** @brief Maintains information about a DB column.
 */
struct ColumnInfo
{
    //-------------------------------------------------------------------------
    // Public Data Members
    //-------------------------------------------------------------------------

    /** @brief Current column 
     */
    Column curCol;

    /** @brief ColumnOp instance
     */
    boost::scoped_ptr<ColumnOp> colOp;

    /** @brief Initial High Water Mark before we start adding data to the
     *  current column segment file.  For the first segment file of the
     *  import, saveHWM takes into account any block(s) that are skipped.
     */
    HWM savedHWM;

    /** @brief Size of a segment file (in bytes) when the file is opened
     *   to add the next extent.
     *   For compressed data files, this is the "raw" data byte count,
     *   not the compressed byte count.
     */
#ifdef _MSC_VER
    __int64 sizeWrittenStart;
#else
    long long sizeWrittenStart;
#endif

    /** @brief Tracks the size of a segment file (in bytes) as rows are added.
     *   For compressed data files, this is the "raw" data byte count,
     *   not the compressed byte count.
     */
#ifdef _MSC_VER
    __int64 sizeWritten;
#else
    long long sizeWritten;
#endif

    /** @brief Column information.
     */
    JobColumn column;

    /** @brief column id
     */
    int id;

    /** @brief Processing time for the column. (in milliseconds)
     */
    double lastProcessingTime;

#ifdef PROFILE
    /** @brief Total processing time for the column. (in milliseconds)
     */
    double totalProcessingTime;
#endif

    /** @brief Instance of the write buffer manager.
     */
    ColumnBufferManager *colBufferMgr;

    /** @brief Freespace (in bytes) at the end of the current db column file
     *   For compressed data files, this is the "raw" data byte count,
     *   not the compressed byte count.
     */
#ifdef _MSC_VER
    __int64 availFileSize;
#else
    long long availFileSize;
#endif

    /** @brief Total size capacity of current db column segment file.
     *   For compressed data files, this is the "raw" data byte count,
     *   not the compressed byte count.
     */
#ifdef _MSC_VER
    __int64 fileSize;
#else
    long long fileSize;
#endif

    /** @brief LBID corresponding to initial HWM saved in savedHWM at start
     *  of import.
     *
     * LBID is used, at the end of the import, to identify to DBRM, an
     * extent whose CasualPartition stats are to be cleared, because we will
     * have written additional rows to that extent as part of an import.
     */
    BRM::LBID_t savedLbid;

    //-------------------------------------------------------------------------
    // Public Functions
    //-------------------------------------------------------------------------

    /** @brief Constructor.
     */
    ColumnInfo(Log*             logger,
               int              id,
               const JobColumn& column);

    /** @brief Destructor
     */
    virtual ~ColumnInfo();

    /** @brief Returns last input Row num in current "logical" extent; used
     *  to track min/max value per extent, as the data is parsed.   0-based
     *  where Row 0 is first valid input row in the import.
     */
    RID lastInputRowInExtent( ) const {return fLastInputRowInCurrentExtent;}

    /** @brief Increment last input Row num in current "logical" extent, so
     *  that it references the last row of the next extent; used in tracking
     *  min/max value extent.  0-based where Row 0 is first valid input row
     *  in the import.  This function is called when a Read buffer crosses
     *  an extent boundary.
     */
    void lastInputRowInExtentInc ( );

    /** @brief Init last input Row number in current "logical" extent; used
     *  to track min/max value per extent. 0-based where Row 0 is first
     *  valid input row in the import.
     */
    void lastInputRowInExtentInit( );

    /** @brief Update dictionary method.
     *  Parses and stores specified strings into the store file, and
     *  returns the assigned tokens (tokenBuf) to be stored in the
     *  corresponding column token file.
     */
    int  updateDctnryStore(char* buf,
                           ColPosPair ** pos,
                           const int totalRow,
                           char* tokenBuf);

    /** @brief Close the current Column file.
     *  @param bCompletedExtent are we completing an extent
     *  @param bAbort indicates if job is aborting and file should be
     *  closed without doing extra work: flushing buffer, etc.
     */
    virtual int closeColumnFile(bool bCompletingExtent, bool bAbort);

    /** @brief Close the current Dictionary store file.
     *  @param bAbort Indicates if job is aborting and file should be
     *  closed without doing extra work: flushing buffer, updating HWM, etc
     */
    int closeDctnryStore(bool bAbort);

    /** @brief utility to convert a Status enumeration to a string
     */
    static void convertStatusToString( WriteEngine::Status status,
                                std::string& statusString );

    /** @brief Adds an extent to "this" column if needed to contain
     *  the specified number of rows. (New version, supplants checkAnd-
     *  ExtendColumn()).  Also saves the HWM associated with the current
     *  extent, and uses it to update the extentmap at the job's end.
     *
     *  The state of ColumnInfo is updated to reflect the new extent.
     *  For example, curCol is updated with the DBRoot, partition, and
     *  segment file corresponding to the new extent and segment file.
     *
     *  @param saveLBIDForCP (in) Should new extent's LBID be saved in the
     *         extent stats we are saving to update Casual Partition.
     */
    int extendColumn( bool saveLBIDForCP );

    /** @brief Get Extent Map updates to send to BRM at EOJ, for this column.
     *  @param brmReporter Reporter object where BRM updates are to be saved
     */
    void getBRMUpdateInfo( BRMReporter& brmReporter );

    /** @brief Get current dbroot, partition, segment and HWM for this column.
     */
    void getSegFileInfo( File& fileInfo );

    /** @brief Return current next available auto-increment value
     */
    long long getAutoInc( );

    /** @brief Initialize autoincrement value from the current "next" value
     *  taken from the system catalog.
     */
    void initAutoInc( long long currentNextValue );

    /** @brief Open a new Dictionary store file based on the setting of the
     *  DBRoot, partition, and segment settings in curCol.dataFile.
     *  @param bMustExist Indicates whether store file must already exist
     */
    int openDctnryStore( bool bMustExist );

    /** @brief dictionary blocks that will need to be flushed from cache */
    BRM::BlockList_t fDictBlocks;

    /** @brief Set abbreviated extent flag if this is an abbrev extent */
    void setAbbrevExtentCheck();

    /** @brief Is current extent we are loading, an "abbreviated" extent
     */
    bool isAbbrevExtent() { return fLoadingAbbreviatedExtent; }

    /** @brief Expand abbreviated extent in current column segment file.
     *  @param bRetainFilePos controls whether current file position is
     *         to be retained up return from the function.
     */
    int expandAbbrevExtent( bool bRetainFilePos );

    /** @brief Print extent CP information
     */
    void printCPInfo( )
    { fColExtInf->print( ((column.weType  == WriteEngine::WR_CHAR) &&
                          (column.colType != COL_TYPE_DICT)) ); }

    /** @brief Update extent CP information
     */
    void updateCPInfo( RID     lastInputRow,
                       int64_t minVal,
                       int64_t maxVal )
    { fColExtInf->addOrUpdateEntry( lastInputRow, minVal, maxVal ); }

    /** @brief Prepare initial column segment file for importing of data.
     *  @param oldHWM Current HWM prior to initial block skipping.  This is only
     *         used for abbreviated extents, to detect when block skipping has
     *         caused us to require a full expanded extent.
     *  @param newHWM Starting point for adding data after initial blockskipping
     */
    virtual int setupInitialColumnFile( HWM oldHWM, HWM newHWM );

    /** @brief Update how many bytes of data are in the column segment file and
     *  how much room remains in the file (till the current extent is full).
     *  @param numBytesWritten Number of bytes just added to the column file.
     */
    void updateBytesWrittenCounts( unsigned int numBytesWritten );

    /** @brief Returns the current file size in bytes
     */
    int64_t getFileSize() const { return fileSize; }

    /** @brief Has file filled up all its extents
     */
    bool isFileComplete() const;

    /** @brief Reserve block of auto-increment numbers to generate
     *  @param autoIncCount The number of autoincrement numbers to be reserved.
     *  @param nextValue    Value of the first reserved auto inc number.
     */
    int reserveAutoIncNums(uint autoIncCount, long long& nextValue );

    /** @brief Truncate specified dictionary file.  Only applies if compressed.
     * @param dctnryOid Dictionary store OID
     * @param root DBRoot of relevant dictionary store segment file.
     * @param pNum Partition number of relevant dictionary store segment file.
     * @param sNum Segment number of relevant dictionary store segment file.
     */
    virtual int truncateDctnryStore(OID dctnryOid,
        uint16_t root, uint32_t pNum, uint16_t sNum) const;

    /** @brief Increment saturated row count for this column in current import
     * @param satIncCnt Increment count to add to the total saturation count.
     */
    void incSaturatedCnt( long long satIncCnt ) {
#ifdef _MSC_VER
        InterlockedAdd64    (&fSaturatedRowCnt, satIncCnt); }
#else
        __sync_fetch_and_add(&fSaturatedRowCnt, satIncCnt); }
#endif

    /** @brief Get saturated row count for this column.
     */
    long long saturatedCnt( ) { return fSaturatedRowCnt; }

    /** @brief When parsing is complete for a column, this function is called
     * to finish flushing and closing the current segment file.
     */
    int finishParsing( );

    /** @brief Mutex used to manage access to the output buffers and files.
     * This was formerly the fMgrMutex in ColumnBufferManager.  See comments
     * that precede this class definition for more information.
     */
    boost::mutex& colMutex() { return fColMutex; }

  protected:

    //-------------------------------------------------------------------------
    // Protected Functions
    //-------------------------------------------------------------------------

    void addToSegFileList(File& dataFile,   // save HWM info per segment file
                          HWM hwm );
    void clearMemory();                     // clear memory used by this object
    void getCPInfoForBRM(BRMReporter& brmReporter);//Get updated CP info for BRM
    int getHWMInfoForBRM(BRMReporter& brmReporter);//Get updated HWM inf for BRM
    virtual int resetFileOffsetsNewExtent(const char* hdr);
                                            // Reset file; start new extent
    void setFileSize( HWM hwm, int abbrevFlag ); // Set fileSize data member
    virtual int saveDctnryStoreHWMChunk();  // Backup dctnry HWM chunk

    //-------------------------------------------------------------------------
    // Protected Data Members
    //-------------------------------------------------------------------------

    boost::mutex dictionaryMutex;           // Mutex for dicionary updates
    boost::mutex fColMutex;                 // Mutex for column changes
    boost::mutex fAutoIncMutex;             // Mutex to manage fAutoIncLastValue
    Log* fLog;                              // Object used for logging

    // Tracks last input Row number in the current "logical" extent,
    // where Row number is 0-based, with Row 0 being the first row in the
    // import.  Used by parsing thread to track when a read buffer crosses
    // an extent boundary.  We detect when a Read buffer crosses an ex-
    // pected extent boundary so that we can track a column's min/max for
    // each extent.
    RID fLastInputRowInCurrentExtent;

    bool fLoadingAbbreviatedExtent;         // Is current extent abbreviated
    ColExtInfBase* fColExtInf;              // Used to update CP at end of job
    long long      fMaxNumRowsPerSegFile;   // Max num rows per segment file
    Dctnry*        fStore;                  // Corresponding dctnry store file

    // For autoincrement column only... Tracks latest autoincrement value used
    long long fAutoIncLastValue;

#ifdef _MSC_VER
    volatile LONGLONG  fSaturatedRowCnt;    // No. of rows with saturated values
#else
    volatile long long fSaturatedRowCnt;    // No. of rows with saturated values
#endif

    // List of segment files updated during an import; used to track infor-
    // mation necessary to update the ExtentMap at the "end" of the import.
    std::vector<File> fSegFileUpdateList;
};

}

#endif
