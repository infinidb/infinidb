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
* $Id: we_tableinfo.cpp 4648 2013-05-29 21:42:40Z rdempsey $
*
*******************************************************************************/
/** @file */

#include "we_tableinfo.h"
#include "we_bulkstatus.h"
#include "we_bulkload.h"

#include <sstream>
#include <sys/time.h>
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <utility>
// @bug 2099+
#include <iostream>
#ifdef _MSC_VER
	#include <stdlib.h>
#else
    #include <string.h>
#endif
using namespace std;

// @bug 2099-
#include <boost/filesystem/path.hpp>
using namespace boost;

#include "we_config.h"
#include "we_simplesyslog.h"
#include "we_bulkrollbackmgr.h"
#include "we_confirmhdfsdbfile.h"

#include "querytele.h"
using namespace querytele;

#include "oamcache.h"
#include "cacheutils.h"

namespace
{
    const std::string  BAD_FILE_SUFFIX = ".bad"; // Reject data file suffix
    const std::string  ERR_FILE_SUFFIX = ".err"; // Job error file suffix
    const std::string  BOLD_START      = "\033[0;1m";
    const std::string  BOLD_STOP       = "\033[0;39m";
}

namespace WriteEngine
{
//------------------------------------------------------------------------------
// Puts the current thread to sleep for the specified number of milliseconds.
// (Ex: used to wait for a Read buffer to become available.)
//------------------------------------------------------------------------------
void TableInfo::sleepMS(long ms)
{
    struct timespec rm_ts;

    rm_ts.tv_sec = ms/1000; 
    rm_ts.tv_nsec = ms%1000 *1000000;
#ifdef _MSC_VER
    Sleep(ms);
#else
    struct timespec abs_ts;
    do
    {
        abs_ts.tv_sec = rm_ts.tv_sec; 
        abs_ts.tv_nsec = rm_ts.tv_nsec;
    } 
    while(nanosleep(&abs_ts,&rm_ts) < 0);
#endif
}

//------------------------------------------------------------------------------
// TableInfo constructor
//------------------------------------------------------------------------------
TableInfo::TableInfo(Log* logger, const BRM::TxnID txnID,
                     const string& processName,
                     OID   tableOID,
                     const string& tableName,
                     bool  bKeepRbMetaFile) :
    fTableId(-1), fBufferSize(0), fFileBufSize(0),
    fStatusTI(WriteEngine::NEW),
    fReadBufCount(0), fNumberOfColumns(0),
    fHandle(NULL), fCurrentReadBuffer(0), fTotalReadRows(0),
    fTotalErrRows(0), fMaxErrorRows(5),
    fLastBufferId(-1), fFileBuffer(NULL), fCurrentParseBuffer(0), 
    fNumberOfColsParsed(0), fLocker(-1), fTableName(tableName),
    fTableOID(tableOID), fJobId(0), fLog(logger), fTxnID(txnID),
    fRBMetaWriter(processName, logger),
    fProcessName(processName),
    fKeepRbMetaFile(bKeepRbMetaFile),
    fbTruncationAsError(false),
    fImportDataMode(IMPORT_DATA_TEXT),
    fTableLocked(false),
    fReadFromStdin(false),
    fNullStringMode(false),
    fEnclosedByChar('\0'),
    fEscapeChar('\\'),
    fProcessingBegun(false),
    fBulkMode(BULK_MODE_LOCAL),
    fBRMReporter(logger, tableName),
    fTableLockID(0),
    fRejectDataCnt(0),
    fRejectErrCnt(0),
    fExtentStrAlloc(tableOID, logger),
    fOamCachePtr(oam::OamCache::makeOamCache())
{
    fBuffers.clear();
    fColumns.clear();
    fStartTime.tv_sec  = 0;
    fStartTime.tv_usec = 0;
    string teleServerHost(config::Config::makeConfig()->getConfig("QueryTele", "Host"));
    if (!teleServerHost.empty())
    {
        int teleServerPort = config::Config::fromText(config::Config::makeConfig()->getConfig("QueryTele", "Port"));
        if (teleServerPort > 0)
        {
		fQtc.serverParms(QueryTeleServerParms(teleServerHost, teleServerPort));
        }
    }
}

//------------------------------------------------------------------------------
// TableInfo destructor
//------------------------------------------------------------------------------
TableInfo::~TableInfo()
{
	fBRMReporter.sendErrMsgToFile(fBRMRptFileName);
    freeProcessingBuffers();
}

//------------------------------------------------------------------------------
// Frees up processing buffer memory.  We don't reset fReadBufCount to 0,
// because BulkLoad::lockColumnForParse() is calling getNumberOfBuffers()
// and dividing by the return value.  So best not to risk returning 0.
// Once we get far enough to call this freeProcessingBuffers() function,
// the application code obviously better be completely through accessing
// fBuffers and fColumns.
//------------------------------------------------------------------------------
void TableInfo::freeProcessingBuffers()
{
    // fLog->logMsg(
    //    string("Releasing TableInfo Buffer for ")+fTableName,
    //    MSGLVL_INFO1);
    fBuffers.clear();
    fColumns.clear();
    fNumberOfColumns = 0;
}

//------------------------------------------------------------------------------
// Close any database column or dictionary store files left open for this table.
// Under "normal" circumstances, there should be no files left open when we
// reach the end of the job, but in some error cases, the parsing threads may
// bail out without closing a file.  So this function is called as part of
// EOJ cleanup for any tables that are still holding a table lock.
//
// Files will automatically get closed when the program terminates, but when
// we are preparing for a bulk rollback, we want to explicitly close the files
// before we "reopen" them and start rolling back the contents of the files.
//
// For mode1 and mode2 imports, cpimport.bin does not lock the table or perform
// a bulk rollback, and closeOpenDbFile() is not called.  We instead rely on
// the program to implicitly close the files.
//------------------------------------------------------------------------------
void TableInfo::closeOpenDbFiles()
{
    ostringstream oss;
    oss << "Closing DB files for table " << fTableName << 
        ", left open by abnormal termination.";
    fLog->logMsg( oss.str(), MSGLVL_INFO2 );

    for (unsigned int k=0; k<fColumns.size(); k++)
    {
        stringstream oss1;
        oss1 << "Closing DB column file for: " <<
            fColumns[k].column.colName <<
            " (OID-" << fColumns[k].column.mapOid << ")";
        fLog->logMsg( oss1.str(), MSGLVL_INFO2 );
        fColumns[k].closeColumnFile(false,true);

        if (fColumns[k].column.colType == COL_TYPE_DICT)
        {
            stringstream oss2;
            oss2 << "Closing DB store  file for: "  <<
                fColumns[k].column.colName <<
                " (OID-" << fColumns[k].column.dctnry.dctnryOid << ")";
            fLog->logMsg( oss2.str(), MSGLVL_INFO2 );
            fColumns[k].closeDctnryStore(true);
        }
    }
}

//------------------------------------------------------------------------------
// Locks this table for reading to the specified thread (locker) "if" the table
// has not yet been assigned to a read thread.
//------------------------------------------------------------------------------
bool TableInfo::lockForRead(const int & locker)
{
    boost::mutex::scoped_lock lock(fSyncUpdatesTI);
    if(fLocker == -1)
    {
        if(fStatusTI == WriteEngine::NEW )
        {
            fLocker = locker;
            return true;
        }
    }
    return false;
}

//------------------------------------------------------------------------------
// Loop thru reading the import file(s) assigned to this TableInfo object.
//------------------------------------------------------------------------------
int  TableInfo::readTableData( )
{
    RID validTotalRows        = 0;
    RID totalRowsPerInputFile = 0;
    int filesTBProcessed = fLoadFileList.size();
    int fileCounter = 0;
    unsigned long long qtSentAt = 0;

    if(fHandle == NULL) {

        fFileName = fLoadFileList[fileCounter]; 
        int rc = openTableFile();
        if(rc != NO_ERROR)
        {
            // Mark the table status as error and exit.
            boost::mutex::scoped_lock lock(fSyncUpdatesTI);
            fStatusTI = WriteEngine::ERR;
            return rc;
        }
        fileCounter++;
    }
        
    timeval readStart;
    gettimeofday(&readStart, NULL);
    ostringstream ossStartMsg;
    ossStartMsg << "Start reading and loading table " << fTableName;
    fLog->logMsg( ossStartMsg.str(), MSGLVL_INFO2 );
    fProcessingBegun = true;

    ImportTeleStats its;
    its.job_uuid = fJobUUID;
    its.import_uuid = QueryTeleClient::genUUID();
    its.msg_type = ImportTeleStats::IT_START;
    its.start_time = QueryTeleClient::timeNowms();
    its.table_list.push_back(fTableName);
    its.rows_so_far.push_back(0);
    its.system_name = fOamCachePtr->getSystemName();
    its.module_name = fOamCachePtr->getModuleName();
    string tn = getTableName();
    its.schema_name = string(tn, 0, tn.find('.'));
    fQtc.postImportTele(its);

    //
    // LOOP to read all the import data for this table
    //
    while(true)
    {
        // See if JobStatus has been set to terminate by another thread
        if (BulkStatus::getJobStatus() == EXIT_FAILURE)
        {
            boost::mutex::scoped_lock lock(fSyncUpdatesTI);
            fStartTime = readStart;
            fStatusTI  = WriteEngine::ERR;
            its.msg_type = ImportTeleStats::IT_TERM;
            its.rows_so_far.pop_back();
            its.rows_so_far.push_back(0);
            fQtc.postImportTele(its);
            throw SecondaryShutdownException( "TableInfo::"
                "readTableData(1) responding to job termination");
        }

// @bug 3271: Conditionally compile the thread deadlock debug logging
#ifdef DEADLOCK_DEBUG
        // @bug2099+.  Temp hack to diagnose deadlock.
        struct timeval tvStart;
        gettimeofday(&tvStart, 0);
        bool report = false;
        bool reported = false;
        // @bug2099-
#else
        const bool report = false;
#endif

#ifdef PROFILE
        Stats::startReadEvent(WE_STATS_WAIT_FOR_READ_BUF);
#endif
        //
        // LOOP to wait for, and read, the next avail BulkLoadBuffer object
        //
        while(!isBufferAvailable(report))
        {
            // See if JobStatus has been set to terminate by another thread
            if (BulkStatus::getJobStatus() == EXIT_FAILURE)
            {
                boost::mutex::scoped_lock lock(fSyncUpdatesTI);
                fStartTime = readStart;
                fStatusTI  = WriteEngine::ERR;
                its.msg_type = ImportTeleStats::IT_TERM;
                its.rows_so_far.pop_back();
                its.rows_so_far.push_back(0);
                fQtc.postImportTele(its);
                throw SecondaryShutdownException( "TableInfo::"
                    "readTableData(2) responding to job termination");
            }

            // Sleep and check the condition again.
            sleepMS(1);
#ifdef DEADLOCK_DEBUG
            // @bug2099+
            if(report) report = false; // report one time.
            if(!reported)
            {
                struct timeval tvNow;
                gettimeofday(&tvNow, 0);
                if((tvNow.tv_sec - tvStart.tv_sec) > 100)
                {
                    time_t t = time(0);
                    char timeString[50];
                    ctime_r(&t, timeString);
                    timeString[ strlen(timeString)-1 ] = '\0';
                    ostringstream oss;
                    oss << endl << timeString << ": " <<
                        "TableInfo::readTableData: " << fTableName <<
                        "; Diff is " << (tvNow.tv_sec - tvStart.tv_sec) <<
                        endl;
                    cout << oss.str();
                    cout.flush();
                    report = true;
                    reported = true;
                }
            }
            // @bug2099-
#endif
        }
#ifdef PROFILE
        Stats::stopReadEvent(WE_STATS_WAIT_FOR_READ_BUF);
        Stats::startReadEvent(WE_STATS_READ_INTO_BUF);
#endif

        int readBufNo   = fCurrentReadBuffer;
        int prevReadBuf = (fCurrentReadBuffer - 1);

        if(prevReadBuf < 0) 
            prevReadBuf = fReadBufCount + prevReadBuf;

        // We keep a running total of read errors;  fMaxErrorRows specifies
        // the error limit.  Here's where we see how many more errors we
        // still have below the limit, and we pass this to fillFromFile().
        unsigned allowedErrCntThisCall =
          ( (fMaxErrorRows > fTotalErrRows) ?
            (fMaxErrorRows-fTotalErrRows) : 0 );

        // Fill in the specified buffer.
        // fTotalReadRowsPerInputFile is ongoing total number of rows read,
        //   per input file.
        // validTotalRows is ongoing total of valid rows read for all files
        //   pertaining to this DB table.
        int readRc = fBuffers[readBufNo].fillFromFile(
            fBuffers[prevReadBuf], fHandle, totalRowsPerInputFile,
            validTotalRows, fColumns, allowedErrCntThisCall);
        if (readRc != NO_ERROR) 
        {
            // error occurred.
            // need to exit.
            // mark the table status as error and exit.
            {
                boost::mutex::scoped_lock lock(fSyncUpdatesTI);
                fStartTime = readStart;
                fStatusTI  = WriteEngine::ERR;
                fBuffers[readBufNo].setStatusBLB(WriteEngine::ERR);
            }
            closeTableFile();

            // Error occurred on next row not read, so increment
            // totalRowsPerInputFile row count for the error msg
            WErrorCodes ec;
            ostringstream oss;
            oss << "Error reading import file " << fFileName <<
                "; near line " << totalRowsPerInputFile+1 << "; " <<
                ec.errorString(readRc);
            fLog->logMsg( oss.str(), readRc, MSGLVL_ERROR);

            its.msg_type = ImportTeleStats::IT_TERM;
            its.rows_so_far.pop_back();
            its.rows_so_far.push_back(0);
            fQtc.postImportTele(its);

            return readRc;
        }
#ifdef PROFILE
        Stats::stopReadEvent(WE_STATS_READ_INTO_BUF);
#endif
        its.msg_type = ImportTeleStats::IT_PROGRESS;
        its.rows_so_far.pop_back();
        its.rows_so_far.push_back(totalRowsPerInputFile);
	unsigned long long thisRows = static_cast<unsigned long long>(totalRowsPerInputFile);
	thisRows /= 1000000;
	if (thisRows > qtSentAt)
	{
		fQtc.postImportTele(its);
		qtSentAt = thisRows;
	}

        // Check if there were any errors in the read data.
        // if yes, copy it to the error list.
        // if the number of errors is greater than the maximum error count
        // mark the table status as error and exit.
        // call the method to copy the errors
        writeErrorList( &fBuffers[readBufNo].getErrorRows(),
                        &fBuffers[readBufNo].getExactErrorRows(), false );
        fBuffers[readBufNo].clearErrRows();

        if(fTotalErrRows > fMaxErrorRows)
        {
            // flush the reject data file and output the rejected rows
            // flush err file and output the rejected row id and the reason.
            writeErrorList( 0, 0, true );

            // number of errors > maximum allowed. hence return error.
            {
                boost::mutex::scoped_lock lock(fSyncUpdatesTI);
                fStartTime = readStart;
                fStatusTI  = WriteEngine::ERR;
                fBuffers[readBufNo].setStatusBLB(WriteEngine::ERR);
            }
            closeTableFile();
            ostringstream oss5;
            oss5 << "Actual error row count("   << fTotalErrRows <<
                ") exceeds the max error rows(" << fMaxErrorRows <<
                ") allowed for table " << fTableName;
            fLog->logMsg(oss5.str(), ERR_BULK_MAX_ERR_NUM, MSGLVL_ERROR);

            // List Err and Bad files to report file (if applicable)
            fBRMReporter.rptMaxErrJob( fBRMRptFileName, fErrFiles, fBadFiles );

            its.msg_type = ImportTeleStats::IT_TERM;
            its.rows_so_far.pop_back();
            its.rows_so_far.push_back(0);
            fQtc.postImportTele(its);

            return ERR_BULK_MAX_ERR_NUM;
        }

        // mark the buffer status as read complete.
        {
#ifdef PROFILE
        Stats::startReadEvent(WE_STATS_WAIT_TO_COMPLETE_READ);
#endif
        boost::mutex::scoped_lock lock(fSyncUpdatesTI);
#ifdef PROFILE
        Stats::stopReadEvent(WE_STATS_WAIT_TO_COMPLETE_READ);
        Stats::startReadEvent(WE_STATS_COMPLETING_READ);
#endif

        fStartTime = readStart;
        fBuffers[readBufNo].setStatusBLB(WriteEngine::READ_COMPLETE);
            
        fCurrentReadBuffer = (fCurrentReadBuffer + 1) % fReadBufCount;

        // bufferCount++;
        if( feof(fHandle) )
        {
            timeval readFinished;
            gettimeofday(&readFinished, NULL);
                
            closeTableFile();

            if (fReadFromStdin)
            {
                fLog->logMsg( "Finished loading " + fTableName + " from STDIN" +
                    ", Time taken = " + Convertor::int2Str((int)
                    (readFinished.tv_sec - readStart.tv_sec)) +
                    " seconds",
                    //" seconds; bufferCount-"+Convertor::int2Str(bufferCount),
                    MSGLVL_INFO2 );
            }
            else
            {
                fLog->logMsg( "Finished reading file " + fFileName +
                    ", Time taken = " + Convertor::int2Str((int)
                    (readFinished.tv_sec - readStart.tv_sec)) +
                    " seconds",
                    //" seconds; bufferCount-"+Convertor::int2Str(bufferCount),
                    MSGLVL_INFO2 );
            }
             
            // flush the reject data file and output the rejected rows
            // flush err file and output the rejected row id and the reason.
            writeErrorList( 0, 0, true );

            // If > 1 file for this table, then open next file in the list
            if ( fileCounter < filesTBProcessed ) 
            {
                fFileName = fLoadFileList[fileCounter];
                int rc = openTableFile();
                if(rc != NO_ERROR) 
                {
                    // Mark the table status as error and exit.
                    fStatusTI   = WriteEngine::ERR;
                    return rc;
                }
                fileCounter++;
                fTotalReadRows += totalRowsPerInputFile;
                totalRowsPerInputFile = 0;
            }
            else  // All files read for this table; break out of read loop
            {
                fStatusTI     = WriteEngine::READ_COMPLETE;
                fLastBufferId = readBufNo;
                fTotalReadRows += totalRowsPerInputFile;
                break;
            }

            gettimeofday(&readStart, NULL);
        } // reached EOF
#ifdef PROFILE
        Stats::stopReadEvent(WE_STATS_COMPLETING_READ);
#endif
        } // mark buffer status as read-complete within scope of a mutex
    }     // loop to read all data for this table

    its.msg_type = ImportTeleStats::IT_SUMMARY;
    its.end_time = QueryTeleClient::timeNowms();
    its.rows_so_far.pop_back();
    its.rows_so_far.push_back(fTotalReadRows);
    fQtc.postImportTele(its);
    fQtc.waitForQueues();

    return NO_ERROR;    
}

//------------------------------------------------------------------------------
// writeErrorList()
//   errorRows    - vector of row numbers and corresponding error messages
//   errorDatRows - vector of bad rows that have been rejected
//
// Adds errors pertaining to a specific buffer, to the cumulative list of
// errors to be reported to the user.
//------------------------------------------------------------------------------
void TableInfo::writeErrorList(const std::vector< std::pair<RID,
                                                  std::string> >* errorRows,
                               const std::vector<std::string>* errorDatRows,
                               bool bCloseFile)
{
    size_t errorRowsCount    = 0;
    size_t errorDatRowsCount = 0;

    if (errorRows)
        errorRowsCount = errorRows->size();
    if (errorDatRows)
        errorDatRowsCount = errorDatRows->size();

    if ((errorRowsCount    > 0) ||
        (errorDatRowsCount > 0) ||
        (bCloseFile))
    {
        boost::mutex::scoped_lock lock(fErrorRptInfoMutex);

        if ((errorRowsCount > 0)    || (bCloseFile))
            writeErrReason(errorRows   , bCloseFile);

        if ((errorDatRowsCount > 0) || (bCloseFile))
            writeBadRows  (errorDatRows, bCloseFile);

        fTotalErrRows += errorRowsCount;
    }
}

//------------------------------------------------------------------------------
// Parse the specified column (columnId) in the specified buffer (bufferId).
//------------------------------------------------------------------------------
int  TableInfo::parseColumn(const int &columnId, const int & bufferId,
                            double& processingTime)
{
    // parse the column
    // note the time and update the column's last processing time
    timeval parseStart, parseEnd;
    gettimeofday(&parseStart, NULL);

    // Will need to check whether the column needs to extend.
    // If size of the file is less than the required size, extend the column
    int rc = fBuffers[bufferId].parse(fColumns[columnId]);
    gettimeofday(&parseEnd, NULL);

    processingTime = (parseEnd.tv_usec / 1000 + parseEnd.tv_sec * 1000) - 
                     (parseStart.tv_usec / 1000 + parseStart.tv_sec * 1000);
        
    return rc;
}

//------------------------------------------------------------------------------
// Mark the specified column (columnId) in the specified buffer (bufferId) as
// PARSE_COMPLETE.  If this is the last column to be parsed for this buffer,
// then mark the buffer as PARSE_COMPLETE.
// If the last buffer for this table has been read (fLastBufferId != -1), then 
// see if all the data for columnId has been parsed for all the buffers, in
// which case we are finished parsing columnId.
// If this is the last column to finish parsing for this table, then mark the
// table status as PARSE_COMPLETE.
//------------------------------------------------------------------------------
int TableInfo::setParseComplete(const int &columnId,
                                const int & bufferId,
                                double processingTime)
{
    boost::mutex::scoped_lock lock(fSyncUpdatesTI);

    // Check table status in case race condition results in this function
    // being called after fStatusTI was set to ERR by another thread.
    if (fStatusTI == WriteEngine::ERR)
        return ERR_UNKNOWN;

    fColumns[columnId].lastProcessingTime   = processingTime;
#ifdef PROFILE
    fColumns[columnId].totalProcessingTime += processingTime;
#endif
    // Set buffer status to complete if setColumnStatus indicates that
    // all the columns are complete
    if (fBuffers[bufferId].setColumnStatus(
        columnId,WriteEngine::PARSE_COMPLETE))
        fBuffers[bufferId].setStatusBLB( WriteEngine::PARSE_COMPLETE );

    // fLastBufferId != -1 means the Read thread has read the last
    // buffer for this table
    if(fLastBufferId != -1)
    {
        // check if the status of the column in all the fBuffers is parse
        // complete then update the column status as parse complete.
        bool allBuffersDoneForAColumn = true;
        for(int i=0; i<fReadBufCount; ++i)
        {
            // check the status of the column in this buffer.
            Status bufferStatus = fBuffers[i].getStatusBLB();
            if ( (bufferStatus == WriteEngine::READ_COMPLETE) ||
                 (bufferStatus == WriteEngine::PARSE_COMPLETE) )
            {
                if(fBuffers[i].getColumnStatus(columnId) !=
                   WriteEngine::PARSE_COMPLETE)
                {
                    allBuffersDoneForAColumn = false;
                    break;
                }
            }
        }

        // allBuffersDoneForAColumn==TRUE means we are finished parsing columnId
        if(allBuffersDoneForAColumn)
        {
            // Accumulate list of HWM dictionary blocks to be flushed from cache
            std::vector<BRM::LBID_t> dictBlksToFlush;
            fColumns[columnId].getDictFlushBlks( dictBlksToFlush );
            for (unsigned kk=0; kk<dictBlksToFlush.size(); kk++)
            {
                fDictFlushBlks.push_back( dictBlksToFlush[kk] );
            }

            int rc = fColumns[columnId].finishParsing( );
            if (rc != NO_ERROR)
            {
                WErrorCodes ec;
                ostringstream oss;
                oss << "setParseComplete completion error; "
                    "Failed to load table: " <<
                    fTableName << "; " << ec.errorString(rc);
                fLog->logMsg( oss.str(), rc, MSGLVL_ERROR);
                fStatusTI = WriteEngine::ERR;
                return rc;
            }

            fNumberOfColsParsed++;

            //
            // If all columns have been parsed, then finished with this tbl
            //
            if(fNumberOfColsParsed >= fNumberOfColumns)
            {
                // After closing the column and dictionary store files,
                // flush any updated dictionary blocks in PrimProc.
                // We only do this for non-HDFS.  For HDFS we don't want
                // to flush till "after" we have "confirmed" all the file
                // changes, which flushes the changes to disk.
                if (!idbdatafile::IDBPolicy::useHdfs())
                {
                    if (fDictFlushBlks.size() > 0)
                    {
#ifdef PROFILE
                        Stats::startParseEvent(WE_STATS_FLUSH_PRIMPROC_BLOCKS);
#endif
                        cacheutils::flushPrimProcAllverBlocks(fDictFlushBlks);
#ifdef PROFILE
                        Stats::stopParseEvent(WE_STATS_FLUSH_PRIMPROC_BLOCKS);
#endif
                        fDictFlushBlks.clear();
                    }
                }

                // Update auto-increment next value if applicable.
                rc = synchronizeAutoInc( );
                if (rc != NO_ERROR)
                {
                    WErrorCodes ec;
                    ostringstream oss;
                    oss << "setParseComplete: autoInc update error; "
                        "Failed to load table: " << fTableName <<
                        "; " << ec.errorString(rc);
                    fLog->logMsg(oss.str(), rc, MSGLVL_ERROR);
                    fStatusTI = WriteEngine::ERR;
                    return rc;
                }

                //..Validate that all the HWM's are consistent and in-sync
                std::vector<DBRootExtentInfo> segFileInfo;
                for(unsigned i=0; i < fColumns.size(); ++i)
                {
                    DBRootExtentInfo extentInfo;
                    fColumns[i].getSegFileInfo( extentInfo );
                    segFileInfo.push_back( extentInfo );
                }
                rc = validateColumnHWMs( 0, segFileInfo, "Ending" );
                if (rc != NO_ERROR)
                {
                    WErrorCodes ec;
                    ostringstream oss;
                    oss << "setParseComplete: HWM validation error; "
                        "Failed to load table: " << fTableName <<
                        "; " << ec.errorString(rc);
                    fLog->logMsg(oss.str(), rc, MSGLVL_ERROR);
                    fStatusTI = WriteEngine::ERR;

                    ostringstream oss2;
                    oss2 << "Ending HWMs for table " << fTableName << ": ";
                    for (unsigned int n=0; n<fColumns.size(); n++)
                    {
                        oss2 << std::endl;
                        oss2 << "  " << fColumns[n].column.colName <<
                            "; DBRoot/part/seg/hwm: "        <<
                            segFileInfo[n].fDbRoot           <<
                            "/" << segFileInfo[n].fPartition <<
                            "/" << segFileInfo[n].fSegment   <<
                            "/" << segFileInfo[n].fLocalHwm;
                    }
                    fLog->logMsg(oss2.str(), MSGLVL_INFO1);

                    return rc;
                }

                //..Confirm changes to DB files (necessary for HDFS)
                rc = confirmDBFileChanges( );
                if (rc != NO_ERROR)
                {
                    WErrorCodes ec;
                    ostringstream oss;
                    oss << "setParseComplete: Error confirming DB changes; "
                        "Failed to load table: " << fTableName <<
                        "; " << ec.errorString(rc);
                    fLog->logMsg(oss.str(), rc, MSGLVL_ERROR);
                    fStatusTI = WriteEngine::ERR;
                    return rc;
                }

                //..Update BRM with HWM and Casual Partition info, etc.
                rc = finishBRM( );
                if (rc != NO_ERROR)
                {
                    WErrorCodes ec;
                    ostringstream oss;
                    oss << "setParseComplete: BRM error; "
                        "Failed to load table: " << fTableName <<
                        "; " << ec.errorString(rc);
                    fLog->logMsg(oss.str(), rc, MSGLVL_ERROR);
                    fStatusTI = WriteEngine::ERR;
                    return rc;
                }

                // Change table lock state to CLEANUP
                rc = changeTableLockState( );
                if (rc != NO_ERROR)
                {
                    WErrorCodes ec;
                    ostringstream oss;
                    oss << "setParseComplete: table lock state change error; "
                        "Table load completed: " << fTableName << "; " <<
                        ec.errorString(rc);
                    fLog->logMsg(oss.str(), rc, MSGLVL_ERROR);
                    fStatusTI = WriteEngine::ERR;
                    return rc;
                }

                // Finished with this table, so delete bulk rollback
                // meta data file and release the table lock.
                deleteTempDBFileChanges();
                deleteMetaDataRollbackFile();

                rc = releaseTableLock( );
                if (rc != NO_ERROR)
                {
                    WErrorCodes ec;
                    ostringstream oss;
                    oss << "setParseComplete: table lock release error; "
                        "Failed to load table: " << fTableName << "; " <<
                        ec.errorString(rc);
                    fLog->logMsg(oss.str(), rc, MSGLVL_ERROR);
                    fStatusTI = WriteEngine::ERR;
                    return rc;
                }

#ifdef PROFILE
                // Loop through columns again to print out the elapsed
                // parse times
                for(unsigned i=0; i < fColumns.size(); ++i)
                {
                    ostringstream ossColTime;
                    ossColTime << "Column " << i << "; OID-" <<
                        fColumns[i].column.mapOid << "; parseTime-" <<
                        (fColumns[i].totalProcessingTime/1000.0) <<
                        " seconds";
                    fLog->logMsg(ossColTime.str(), MSGLVL_INFO1);
                }
#endif
                    
                timeval endTime;
                gettimeofday(&endTime, 0);
                double elapsedTime = 
                    (endTime.tv_sec    + (endTime.tv_usec    / 1000000.0)) -
                    (fStartTime.tv_sec + (fStartTime.tv_usec / 1000000.0));

                fStatusTI = WriteEngine::PARSE_COMPLETE;
                reportTotals(elapsedTime);

                // Reduce memory use by allocating and releasing as needed
                freeProcessingBuffers();

            } // end of if (fNumberOfColsParsed >= fNumberOfColumns)
        }     // end of if (allBuffersDoneForAColumn)
    }         // end of if (fLastBufferId != -1)
     
    // If we finished parsing the buffer associated with currentParseBuffer,
    // but have not finshed the entire table, then advance currentParseBuffer.
    if((fStatusTI != WriteEngine::PARSE_COMPLETE) &&
       (fBuffers[bufferId].getStatusBLB() == WriteEngine::PARSE_COMPLETE))
    {
        // Find the BulkLoadBuffer object that is next in line to be parsed
        // and assign fCurrentParseBuffer accordingly.  Break out of the
        // loop if we wrap all the way around and catch up with the current-
        // Read buffer.
        if(bufferId == fCurrentParseBuffer)
        {
            int currentParseBuffer = fCurrentParseBuffer;
            while (fBuffers[currentParseBuffer].getStatusBLB() ==
                   WriteEngine::PARSE_COMPLETE )
            {
                currentParseBuffer     = (currentParseBuffer + 1) %
                                          fReadBufCount;
                fCurrentParseBuffer    = currentParseBuffer;
                if(fCurrentParseBuffer == fCurrentReadBuffer) break;
            }
        }
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Report summary totals to applicable destination (stdout, cpimport.bin log
// file, BRMReport file (for mode1) etc).
// elapsedTime is number of seconds taken to import this table.
//------------------------------------------------------------------------------
void TableInfo::reportTotals(double elapsedTime)
{
    ostringstream oss1;
    oss1 << "For table " << fTableName <<
        ": " << fTotalReadRows << " rows processed and " <<
        (fTotalReadRows - fTotalErrRows) << " rows inserted.";

    fLog->logMsg(oss1.str(), MSGLVL_INFO1);

    ostringstream oss2;
    oss2 << "For table " << fTableName << ": " << 
        "Elapsed time to load this table: " <<
        elapsedTime << " secs";

    fLog->logMsg(oss2.str(), MSGLVL_INFO2);

    // @bug 3504: Loop through columns to print saturation counts
    std::vector<boost::tuple<CalpontSystemCatalog::ColDataType,std::string,uint64_t> > satCounts;
    for (unsigned i=0; i < fColumns.size(); ++i)
    {
        std::string colName(fTableName);
        colName += '.';
        colName += fColumns[i].column.colName;
        long long satCount = fColumns[i].saturatedCnt();

        satCounts.push_back(boost::make_tuple(fColumns[i].column.dataType, 
                                              colName, 
                                              satCount));

        if (satCount > 0)
        {   // @bug 3375: report invalid dates/times set to null
            ostringstream ossSatCnt;
            ossSatCnt << "Column " << fTableName << '.' <<
                fColumns[i].column.colName << "; Number of ";
            if (fColumns[i].column.dataType == CalpontSystemCatalog::DATE)
			{
				//bug5383
				if(!fColumns[i].column.fNotNull)
                	ossSatCnt <<
                    	"invalid dates replaced with null: ";
				else
					ossSatCnt <<
						"invalid dates replaced with minimum value : ";
			}
            else if (fColumns[i].column.dataType ==
                     CalpontSystemCatalog::DATETIME)
			{
				//bug5383
				if(!fColumns[i].column.fNotNull)
                	ossSatCnt <<
                    	"invalid date/times replaced with null: ";
				else
					ossSatCnt <<
						"invalid date/times replaced with minimum value : ";
			}
            else if (fColumns[i].column.dataType == CalpontSystemCatalog::CHAR)
                ossSatCnt <<
                    "character strings truncated: ";
            else if (fColumns[i].column.dataType ==
                     CalpontSystemCatalog::VARCHAR)
                ossSatCnt <<
                    "character strings truncated: ";
            else
                ossSatCnt <<
                    "rows inserted with saturated values: ";
            ossSatCnt << satCount;
            fLog->logMsg(ossSatCnt.str(), MSGLVL_WARNING);
        }
    }

    logging::Message::Args tblFinishedMsgArgs;
    tblFinishedMsgArgs.add( fJobId );
    tblFinishedMsgArgs.add( fTableName );
    tblFinishedMsgArgs.add( (fTotalReadRows - fTotalErrRows) );
    SimpleSysLog::instance()->logMsg(
        tblFinishedMsgArgs,
        logging::LOG_TYPE_INFO,
        logging::M0083);

    //Bug1375 - cpimport.bin did not add entries to the transaction
    //          log file: data_mods.log  
    if ((fTotalReadRows - fTotalErrRows) > 0 )
        logToDataMods(fjobFileName, oss1.str());

    // Log totals in report file if applicable
    fBRMReporter.reportTotals( fTotalReadRows,
        (fTotalReadRows - fTotalErrRows),
        satCounts );
}

//------------------------------------------------------------------------------
// Report BRM updates to a report file or to BRM directly.
//------------------------------------------------------------------------------
int TableInfo::finishBRM( )
{
    // Collect the CP and HWM information for all the columns
    for(unsigned i=0; i < fColumns.size(); ++i)
    {
        fColumns[i].getBRMUpdateInfo( fBRMReporter );
    }

    // We use mutex not to synchronize contention among parallel threads,
    // because we should be the only thread accessing the fErrFiles and
    // fBadFiles at this point.  But we do use the mutex as a memory barrier
    // to make sure we have the latest copy of the data.
    std::vector<std::string>* errFiles = 0;
    std::vector<std::string>* badFiles = 0;
    {
        boost::mutex::scoped_lock lock(fErrorRptInfoMutex);
        errFiles = &fErrFiles;
        badFiles = &fBadFiles;
    }

    // Save the info just collected, to a report file or send to BRM
    int rc = fBRMReporter.sendBRMInfo( fBRMRptFileName, *errFiles, *badFiles );

    return rc;
}

//------------------------------------------------------------------------------
// Update status of table to reflect an error.
// No need to update the buffer or column status, because we are not going to
// continue the job anyway.  Other threads should terminate when they see that
// the JobStatus has been set to EXIT_FAILURE and/or the table status has been
// set to WriteEngine::ERR.
//------------------------------------------------------------------------------
void TableInfo::setParseError( )
{
    boost::mutex::scoped_lock lock(fSyncUpdatesTI);
    fStatusTI = WriteEngine::ERR;
}

//------------------------------------------------------------------------------
// Locks a column from the specified buffer (bufferId) for the specified parse
// thread (id); and returns the column id.  A return value of -1 means no
// column could be locked for parsing.
//------------------------------------------------------------------------------
// @bug2099. Temporary hack to diagnose deadlock.
// Added report parm and couts below.
int  TableInfo::getColumnForParse(const int & id,
                                  const int & bufferId,
                                  bool report)
{
    boost::mutex::scoped_lock lock(fSyncUpdatesTI);
    double maxTime = 0;
    int columnId = -1;

    while(true)
    {
        // See if JobStatus has been set to terminate by another thread
        if (BulkStatus::getJobStatus() == EXIT_FAILURE)
        {
            fStatusTI = WriteEngine::ERR;
            throw SecondaryShutdownException( "TableInfo::"
                "getColumnForParse() responding to job termination");
        }

        if ( !bufferReadyForParse(bufferId,report) ) return -1;

        // @bug2099+
        ostringstream oss;
        if (report)
        {
#ifdef _MSC_VER
            oss << " ----- " << GetCurrentThreadId() << ":fBuffers[" << bufferId <<
#else
            oss<< " ----- " << pthread_self() << ":fBuffers[" << bufferId <<
#endif
                "]: (colLocker,status,lasttime)- ";
        }
        // @bug2099-

        for(unsigned k=0; k < fNumberOfColumns; ++k)
        {
            // @bug2099+
            if (report)
            {
                Status colStatus = fBuffers[bufferId].getColumnStatus(k);
                int colLocker = fBuffers[bufferId].getColumnLocker(k);

                string colStatusStr;
                ColumnInfo::convertStatusToString(colStatus, colStatusStr);

                oss << '(' << colLocker << ',' << colStatusStr << ',' <<
                    fColumns[k].lastProcessingTime << ") ";
            }
            // @bug2099-

            if(fBuffers[bufferId].getColumnLocker(k) == -1)
            {
                if(columnId == -1)
                    columnId = k;
                else if(fColumns[k].lastProcessingTime == 0)
                {
                    if(fColumns[k].column.width >=
                       fColumns[columnId].column.width)
                        columnId = k;
                }
                else if(fColumns[k].lastProcessingTime > maxTime)
                {
                    maxTime = fColumns[k].lastProcessingTime;
                    columnId = k;
                }
            }
        }

        // @bug2099+
        if (report)
        {
            oss << "; selected colId: " << columnId;
            if (columnId != -1)
                oss << "; maxTime: " << maxTime;
            oss << endl;
            cout << oss.str();
            cout.flush();
        }
        // @bug2099-
        
        if(columnId == -1) return -1;

        if(fBuffers[bufferId].tryAndLockColumn(columnId, id))
        {
            return columnId;
        }
    }
}

//------------------------------------------------------------------------------
// Check if the specified buffer is ready for parsing (status == READ_COMPLETE)
// @bug 2099.  Temporary hack to diagnose deadlock.  Added report parm
//             and couts below.
//------------------------------------------------------------------------------
bool TableInfo::bufferReadyForParse(const int &bufferId, bool report) const
{
    if (fBuffers.size() == 0)
        return false;

    Status stat = fBuffers[bufferId].getStatusBLB();
    if(report) {
        ostringstream oss;
        string bufStatusStr;
        ColumnInfo::convertStatusToString( stat,
                                           bufStatusStr );
#ifdef _MSC_VER
        oss << " --- " << GetCurrentThreadId() <<
#else
        oss << " --- " << pthread_self() <<
#endif
            ":fBuffers[" << bufferId << "]=" << bufStatusStr <<
            " (" << stat << ")" << std::endl;
        cout << oss.str();
    }

    return (stat == WriteEngine::READ_COMPLETE) ? true : false;
}

//------------------------------------------------------------------------------
// Create the specified number (noOfBuffer) of BulkLoadBuffer objects and store
// them in fBuffers.  jobFieldRefList lists the fields in this import.
// fixedBinaryRecLen is fixed record length for binary imports (it is n/a
// for text bulk loads).
//------------------------------------------------------------------------------
void TableInfo::initializeBuffers(int   noOfBuffers,
                                  const JobFieldRefList& jobFieldRefList,
                                  unsigned int fixedBinaryRecLen)
{
#ifdef _MSC_VER
    //@bug 3751
    //When reading from STDIN, Windows doesn't like the huge default buffer of
    //  1M, so turn it down.
    if (fReadFromStdin)
    {
        fBufferSize = std::min(10240, fBufferSize);
    }
#endif

    fReadBufCount = noOfBuffers;
    // initialize and populate the buffer vector.
    for(int i=0; i<fReadBufCount; ++i)
    {
        BulkLoadBuffer *buffer = new BulkLoadBuffer(fNumberOfColumns,
                                                    fBufferSize, fLog,
                                                    i, fTableName,
                                                    jobFieldRefList);
        buffer->setColDelimiter  (fColDelim);
        buffer->setNullStringMode(fNullStringMode);
        buffer->setEnclosedByChar(fEnclosedByChar);
        buffer->setEscapeChar    (fEscapeChar    );
        buffer->setTruncationAsError(getTruncationAsError());
        buffer->setImportDataMode(fImportDataMode,
                                  fixedBinaryRecLen);
        fBuffers.push_back(buffer);
    }
}

//------------------------------------------------------------------------------
// Add the specified ColumnInfo object (info) into this table's fColumns vector.
//------------------------------------------------------------------------------
void TableInfo::addColumn(ColumnInfo * info)
{
    fColumns.push_back(info);
    fNumberOfColumns = fColumns.size();

    fExtentStrAlloc.addColumn( info->column.mapOid,
                               info->column.width );
}

//------------------------------------------------------------------------------
// Open the file corresponding to fFileName so that we can import it's contents.
// A buffer is also allocated and passed to setvbuf().
// If fReadFromStdin is true, we just assign stdin to our fHandle for reading.
//------------------------------------------------------------------------------
int TableInfo::openTableFile()
{
    if(fHandle != NULL)
        return NO_ERROR;

    if (fReadFromStdin)
    {
        fHandle = stdin;

#ifdef _MSC_VER
		// If this is a binary import from STDIN, then set stdin to binary
		if (fImportDataMode != IMPORT_DATA_TEXT)
			_setmode(_fileno(stdin), _O_BINARY);

        fFileBuffer = 0;
#else
        // Not 100% sure that calling setvbuf on stdin does much, but in
        // some tests, it made a slight difference.
        fFileBuffer = new char[fFileBufSize];
        setvbuf(fHandle, fFileBuffer, _IOFBF, fFileBufSize);
#endif
        ostringstream oss;
        oss << BOLD_START << "Reading input from STDIN to import into table " <<
            fTableName << "..." << BOLD_STOP;
        fLog->logMsg( oss.str(), MSGLVL_INFO1 );
    }
    else
    {
        if (fImportDataMode == IMPORT_DATA_TEXT)
            fHandle = fopen( fFileName.c_str() , "r" );
        else
            fHandle = fopen( fFileName.c_str() , "rb" );
        if(fHandle == NULL)
        {
            int errnum = errno;
            ostringstream oss;
            oss << "Error opening import file " << fFileName << ". " <<
                strerror(errnum);
            fLog->logMsg( oss.str(), ERR_FILE_OPEN, MSGLVL_ERROR );

            // return an error; caller should set fStatusTI if needed
            return ERR_FILE_OPEN;
        }

        // now the input load file is available for reading the data.
        // read the data from the load file into the buffers.
        fFileBuffer = new char[fFileBufSize];
        setvbuf(fHandle, fFileBuffer, _IOFBF, fFileBufSize);

        ostringstream oss;
        oss << "Opening " << fFileName << " to import into table " <<fTableName;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Close the current open file we have been importing.
//------------------------------------------------------------------------------
void TableInfo::closeTableFile()
{
    if (fHandle)
    {
        // If reading from stdin, we don't delete the buffer out from under
        // the file handle, because stdin is still open.  This will cause a
        // memory leak, but when using stdin, we can only read in 1 table.
        // So it's not like we will be leaking multiple buffers for several
        // tables over the life of the job.
        if (!fReadFromStdin)
        {
            fclose(fHandle);
            delete [] fFileBuffer;
        }

        fHandle = 0;
    }
}

//------------------------------------------------------------------------------
// "Grabs" the current read buffer for TableInfo so that the read thread that
// is calling this function, can read the next buffer's set of data.
//------------------------------------------------------------------------------
// @bug2099. Temporary hack to diagnose deadlock.
// Added report parm and couts below.
bool TableInfo::isBufferAvailable(bool report)
{
    boost::mutex::scoped_lock lock(fSyncUpdatesTI);
    Status bufferStatus = fBuffers[fCurrentReadBuffer].getStatusBLB();
    if( (bufferStatus == WriteEngine::PARSE_COMPLETE) ||
        (bufferStatus == WriteEngine::NEW) )
    {
        // reset buffer status and column locks while we have
        // an fSyncUpdatesTI lock
        fBuffers[fCurrentReadBuffer].setStatusBLB(
                 WriteEngine::READ_PROGRESS);
        fBuffers[fCurrentReadBuffer].resetColumnLocks();
        return true;
    }
    if(report)
    {
        ostringstream oss;
        string bufferStatusStr;
        ColumnInfo::convertStatusToString( bufferStatus, bufferStatusStr );
        oss << "  Buffer status is " << bufferStatusStr << ". " << endl;
        oss << "  fCurrentReadBuffer is " << fCurrentReadBuffer << endl;
        cout << oss.str();
        cout.flush();
    }

    return false;
}

//------------------------------------------------------------------------------
// Report whether rows were rejected, and if so, then list them out into the
// reject file.
//------------------------------------------------------------------------------
void TableInfo::writeBadRows( const std::vector<std::string>* errorDatRows,
                              bool bCloseFile )
{
    size_t errorDatRowsCount = 0;
    if (errorDatRows)
        errorDatRowsCount = errorDatRows->size();

    if(errorDatRowsCount > 0)
    {
        if (!fRejectDataFile.is_open())
        {
            ostringstream rejectFileName;
			if (fErrorDir.size() > 0)
			{
#ifdef _MSC_VER
                char filename[_MAX_FNAME];
                char ext[_MAX_EXT];
				_splitpath(const_cast<char*>(getFileName().c_str()), 
						   NULL, NULL, filename, ext);
				rejectFileName << fErrorDir << "\\" << filename << ext;
#else
				rejectFileName << fErrorDir << basename(getFileName().c_str()); 
#endif
			}
			else
			{
				rejectFileName << getFileName();
			}
            rejectFileName << ".Job_" << fJobId <<
                '_' << ::getpid() << BAD_FILE_SUFFIX;
            fRejectDataFileName = rejectFileName.str();
            fRejectDataFile.open( rejectFileName.str().c_str(),
                                  ofstream::out );
            if( !fRejectDataFile )
            {
                ostringstream oss;
                oss << "Unable to create file: " << rejectFileName.str() <<
                    ";  Check permission.";
                fLog->logMsg(oss.str(), ERR_FILE_OPEN, MSGLVL_ERROR);

                return;
            }
        }

        for(std::vector<string>::const_iterator iter = errorDatRows->begin();
            iter != errorDatRows->end(); ++iter)
        {
            fRejectDataFile << *iter;
        }

        fRejectDataCnt += errorDatRowsCount;
    }

    if (bCloseFile)
    {
        if (fRejectDataFile.is_open())
            fRejectDataFile.close();
        fRejectDataFile.clear();

        if (fRejectDataCnt > 0)
        {
            ostringstream oss;
            std::string rejectFileNameToLog;

            // Construct/report complete file name and save in list of files
            boost::filesystem::path p(fRejectDataFileName);
            if (!p.has_root_path())
            {
                char cwdPath[4096];
                getcwd(cwdPath, sizeof(cwdPath));
                boost::filesystem::path rejectFileName2( cwdPath );
                rejectFileName2 /= fRejectDataFileName;
                fBadFiles.push_back( rejectFileName2.string() );

                rejectFileNameToLog = rejectFileName2.string();
            }
            else
            {
                fBadFiles.push_back( fRejectDataFileName );

                rejectFileNameToLog = fRejectDataFileName;
            }

            oss << "Number of rows with errors = " << fRejectDataCnt <<
                ".  Exact error rows are listed in file " <<
                rejectFileNameToLog;
            fLog->logMsg(oss.str(), MSGLVL_INFO1);

            fRejectDataCnt = 0;
        }
    }
}

//------------------------------------------------------------------------------
// Report whether rows were rejected, and if so, then list out the row numbers
// and error reasons into the error file.
//------------------------------------------------------------------------------
void  TableInfo::writeErrReason( const std::vector< std::pair<RID,
                                                    string> >* errorRows,
                                 bool bCloseFile )
{
    size_t errorRowsCount = 0;
    if (errorRows)
        errorRowsCount = errorRows->size();

    if (errorRowsCount > 0)
    {
        if (!fRejectErrFile.is_open())
        {
            ostringstream errFileName;
			if (fErrorDir.size() > 0)
			{
#ifdef _MSC_VER
				char filename[_MAX_FNAME];
				char ext[_MAX_EXT];
				_splitpath(const_cast<char*>(getFileName().c_str()), 
						   NULL, NULL, filename, ext);
				errFileName << fErrorDir << "\\" << filename << ext;
#else
				errFileName << fErrorDir << basename(getFileName().c_str()); 
#endif
			}
			else
			{
				errFileName << getFileName();
			}
            errFileName << ".Job_" << fJobId <<
                '_' << ::getpid() << ERR_FILE_SUFFIX;
            fRejectErrFileName = errFileName.str();
            fRejectErrFile.open( errFileName.str().c_str(),
                                 ofstream::out );
            if( !fRejectErrFile )
            {
                ostringstream oss;
                oss << "Unable to create file: " << errFileName.str() <<
                    ";  Check permission.";
                fLog->logMsg(oss.str(), ERR_FILE_OPEN, MSGLVL_ERROR);

                return;
            }
        }

        for(std::vector< std::pair<RID,std::string> >::const_iterator iter =
            errorRows->begin();
            iter != errorRows->end(); ++iter)
        {
            fRejectErrFile << "Line number " << iter->first <<
                ";  Error: " << iter->second<< endl;
        }

        fRejectErrCnt += errorRowsCount;
    }

    if (bCloseFile)
    {
        if (fRejectErrFile.is_open())
            fRejectErrFile.close();
        fRejectErrFile.clear();

        if (fRejectErrCnt > 0)
        {
            ostringstream oss;
            std::string errFileNameToLog;

            // Construct/report complete file name and save in list of files
            boost::filesystem::path p(fRejectErrFileName);
            if (!p.has_root_path())
            {
                char cwdPath[4096];
                getcwd(cwdPath, sizeof(cwdPath));
                boost::filesystem::path errFileName2( cwdPath );
                errFileName2 /= fRejectErrFileName;
                fErrFiles.push_back( errFileName2.string() );

                errFileNameToLog = errFileName2.string();
            }
            else
            {
                fErrFiles.push_back( fRejectErrFileName );

                errFileNameToLog = fRejectErrFileName;
            }

            oss << "Number of rows with errors = " << fRejectErrCnt <<
                ".  Row numbers with error reasons are listed in file " <<
                errFileNameToLog;
            fLog->logMsg(oss.str(), MSGLVL_INFO1);

            fRejectErrCnt = 0;
        }
    }
}

//------------------------------------------------------------------------------
// Logs "Bulkload |Job" message along with the specified message text
// (messageText) to the critical log.
//------------------------------------------------------------------------------
void TableInfo::logToDataMods(const string& jobFile, const string&  messageText)
{
        logging::Message::Args args;

        unsigned subsystemId = 19; // writeengine

        logging::LoggingID loggingId(subsystemId, 0, fTxnID.id, 0);
        logging::MessageLog messageLog(loggingId, LOG_LOCAL1);

        logging::Message m(8);
        args.add("Bulkload |Job: " + jobFile);
        args.add("|" + messageText);
        m.format(args);
        messageLog.logInfoMessage(m);
}

//------------------------------------------------------------------------------
// Acquires DB table lock for this TableInfo object.
// Function employs retry logic based on the SystemConfig/WaitPeriod.
//------------------------------------------------------------------------------
int TableInfo::acquireTableLock( bool disableTimeOut )
{
    // Save DBRoot list at start of job; used to compare at EOJ.
    Config::getRootIdList( fOrigDbRootIds );

    // If executing distributed (mode1) or central command (mode2) then
    // don't worry about table locks.  The client front-end will manage locks.
    if ((fBulkMode == BULK_MODE_REMOTE_SINGLE_SRC) ||
        (fBulkMode == BULK_MODE_REMOTE_MULTIPLE_SRC))
    {
        if (fLog->isDebug( DEBUG_1 ))
        {
            ostringstream oss;
            oss << "Bypass acquiring table lock in distributed mode, "
                "for table" << fTableName << "; OID-" << fTableOID;
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
        return NO_ERROR;
    }

    const int SLEEP_INTERVAL    = 100; // sleep 100 milliseconds between checks
    const int NUM_TRIES_PER_SEC = 10;  // try 10 times per second

    int waitSeconds     = Config::getWaitPeriod();
    const int NUM_TRIES = NUM_TRIES_PER_SEC * waitSeconds;
    std::string tblLockErrMsg;

    // Retry loop to lock the db table associated with this TableInfo object
    std::string processName;
    uint32_t   processId;
    int32_t     sessionId;
    int32_t     transId;
    ostringstream pmModOss;
    pmModOss << " (pm" << Config::getLocalModuleID() << ')';
    bool timeout = false;
    //for (int i=0; i<NUM_TRIES; i++)
    int try_count = 0;
    while (!timeout)
    {
        processName = fProcessName;
        processName+= pmModOss.str();
        processId   = ::getpid();
        sessionId   = -1;
        transId     = -1;
        int rc = BRMWrapper::getInstance()->getTableLock (
            fTableOID,
            processName,
            processId,
            sessionId,
            transId,
            fTableLockID,
            tblLockErrMsg);

        if ((rc == NO_ERROR) && (fTableLockID > 0))
        {
            fTableLocked = true;

            if (fLog->isDebug( DEBUG_1 ))
            {
                ostringstream oss;
                oss << "Table lock acquired for table " << fTableName <<
                    "; OID-" << fTableOID << "; lockID-" << fTableLockID;
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }

            return NO_ERROR;
        }
        else if (fTableLockID == 0)
        {
            // sleep and then go back and try getting table lock again
            sleepMS(SLEEP_INTERVAL);

            if (fLog->isDebug( DEBUG_1 ))
            {
                ostringstream oss;
                oss << "Retrying to acquire table lock for table " << 
                    fTableName << "; OID-" << fTableOID;
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }
        }
        else
        {
            ostringstream oss;
            oss << "Error in acquiring table lock for table " << fTableName <<
                "; OID-" << fTableOID << "; " << tblLockErrMsg;
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

            return rc;
        }
        
        // if disableTimeOut is set then no timeout for table lock. Forever wait....
        timeout = (disableTimeOut ? false : (++try_count >= NUM_TRIES));
    }

    ostringstream oss;
    oss << "Unable to acquire lock for table " << fTableName <<
        "; OID-" << fTableOID << "; table currently locked by process-" <<
        processName << "; pid-" << processId <<
        "; session-" << sessionId <<
        "; txn-" << transId;
    fLog->logMsg( oss.str(), ERR_TBLLOCK_GET_LOCK_LOCKED, MSGLVL_ERROR );

    return ERR_TBLLOCK_GET_LOCK_LOCKED;
}

//------------------------------------------------------------------------------
// Change table lock state (to cleanup)
//------------------------------------------------------------------------------
int TableInfo::changeTableLockState( )
{
    // If executing distributed (mode1) or central command (mode2) then
    // don't worry about table locks.  The client front-end will manage locks.
    if ((fBulkMode == BULK_MODE_REMOTE_SINGLE_SRC) ||
        (fBulkMode == BULK_MODE_REMOTE_MULTIPLE_SRC))
    {
        return NO_ERROR;
    }

    std::string tblLockErrMsg;
    bool bChanged = false;

    int rc = BRMWrapper::getInstance()->changeTableLockState (
        fTableLockID,
        BRM::CLEANUP,
        bChanged,
        tblLockErrMsg );   

    if (rc == NO_ERROR)
    {
        if (fLog->isDebug( DEBUG_1 ))
        {
            ostringstream oss;
            if (bChanged)
            {
                oss << "Table lock state changed to CLEANUP for table " <<
                    fTableName <<
                    "; OID-" << fTableOID <<
                    "; lockID-" << fTableLockID;
            }
            else
            {
                oss << "Table lock state not changed to CLEANUP for table " <<
                    fTableName <<
                    "; OID-"    << fTableOID <<
                    "; lockID-" << fTableLockID <<
                    ".  Table lot locked.";
            }
            
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
    }
    else
    {
        ostringstream oss;
        oss << "Error in changing table state for table " << fTableName <<
            "; OID-"    << fTableOID    <<
            "; lockID-" << fTableLockID << "; " <<tblLockErrMsg;
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Releases DB table lock assigned to this TableInfo object.
//------------------------------------------------------------------------------
int TableInfo::releaseTableLock( )
{
    // If executing distributed (mode1) or central command (mode2) then
    // don't worry about table locks.  The client front-end will manage locks.
    if ((fBulkMode == BULK_MODE_REMOTE_SINGLE_SRC) ||
        (fBulkMode == BULK_MODE_REMOTE_MULTIPLE_SRC))
    {
        if (fLog->isDebug( DEBUG_1 ))
        {
            ostringstream oss;
            oss << "Bypass releasing table lock in distributed mode, "
                "for table " << fTableName << "; OID-" << fTableOID;
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
        return NO_ERROR;
    }

    std::string tblLockErrMsg;
    bool bReleased = false;

    // Unlock the database table
    int rc = BRMWrapper::getInstance()->releaseTableLock (
        fTableLockID,
        bReleased,
        tblLockErrMsg );   

    if (rc == NO_ERROR)
    {
        fTableLocked = false;

        if (fLog->isDebug( DEBUG_1 ))
        {
            ostringstream oss;
            if (bReleased)
            {
                oss << "Table lock released for table " << fTableName <<
                    "; OID-" << fTableOID <<
                    "; lockID-" << fTableLockID;
            }
            else
            {
                oss << "Table lock not released for table " << fTableName <<
                    "; OID-"    << fTableOID <<
                    "; lockID-" << fTableLockID <<
                    ".  Table not locked.";
            }
            
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
    }
    else
    {
        ostringstream oss;
        oss << "Error in releasing table lock for table " << fTableName <<
            "; OID-"    << fTableOID <<
            "; lockID-" << fTableLockID << "; " <<tblLockErrMsg;
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Delete bulk rollback metadata file.
//------------------------------------------------------------------------------
void TableInfo::deleteMetaDataRollbackFile( )
{
    // If executing distributed (mode1) or central command (mode2) then
    // don't worry about table locks, or deleting meta data files.  The
    // client front-end will manage these tasks after all imports are finished.
    if ((fBulkMode == BULK_MODE_REMOTE_SINGLE_SRC) ||
        (fBulkMode == BULK_MODE_REMOTE_MULTIPLE_SRC))
    {
        return;
    }

    if (!fKeepRbMetaFile)
    {
        // Treat any error as non-fatal, though we log it.
        try {
            fRBMetaWriter.deleteFile();
        }
        catch (WeException& ex) {
            ostringstream oss;
            oss << "Error deleting meta file; " << ex.what();
            fLog->logMsg(oss.str(), ex.errorCode(), MSGLVL_ERROR);
        }
    }
}

//------------------------------------------------------------------------------
// Changes to "existing" DB files must be confirmed on HDFS system.
// This function triggers this action.
//------------------------------------------------------------------------------
// @bug 5572 - Add db file confirmation for HDFS
int TableInfo::confirmDBFileChanges( )
{
    // Unlike deleteTempDBFileChanges(), note that confirmDBFileChanges()
    // executes regardless of the import mode.  We go ahead and confirm
    // the file changes at the end of a successful cpimport.bin.
    if (idbdatafile::IDBPolicy::useHdfs())
    {
        ostringstream oss;
        oss << "Confirming DB file changes for " << fTableName;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );

        std::string errMsg;
        ConfirmHdfsDbFile confirmHdfs;
        int rc = confirmHdfs.confirmDbFileListFromMetaFile( fTableOID, errMsg );
        if (rc != NO_ERROR)
        {
            ostringstream ossErrMsg;
            ossErrMsg << "Unable to confirm changes to table " << fTableName <<
                "; " << errMsg;
            fLog->logMsg( ossErrMsg.str(), rc, MSGLVL_ERROR );

            return rc;
        }
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Temporary swap files must be deleted on HDFS system.
// This function triggers this action.
//------------------------------------------------------------------------------
// @bug 5572 - Add db file confirmation for HDFS
void TableInfo::deleteTempDBFileChanges( )
{
    // If executing distributed (mode1) or central command (mode2) then
    // no action necessary.  The client front-end will initiate the deletion
    // of the temp files, only after all the distributed imports have
    // successfully completed.
    if ((fBulkMode == BULK_MODE_REMOTE_SINGLE_SRC) ||
        (fBulkMode == BULK_MODE_REMOTE_MULTIPLE_SRC))
    {
        return;
    }

    if (idbdatafile::IDBPolicy::useHdfs())
    {
        ostringstream oss;
        oss << "Deleting DB temp swap files for " << fTableName;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );

        std::string errMsg;
        ConfirmHdfsDbFile confirmHdfs;
        int rc = confirmHdfs.endDbFileListFromMetaFile(fTableOID, true, errMsg);

        // Treat any error as non-fatal, though we log it.
        if (rc != NO_ERROR)
        {
            ostringstream ossErrMsg;
            ossErrMsg << "Unable to delete temp swap files for table " <<
                fTableName << "; " << errMsg;
            fLog->logMsg( ossErrMsg.str(), rc, MSGLVL_ERROR );
        }
    }
}

//------------------------------------------------------------------------------
// Validates the correctness of the current HWMs for this table.
// The HWMs for all the 1 byte columns should be identical.  Same goes
// for all the 2 byte columns, etc.  The 2 byte column HWMs should be
// "roughly" (but not necessarily exactly) twice that of a 1 byte column.
// Same goes for the 4 byte column HWMs vs their 2 byte counterparts, etc.
// jobTable - table/column information to use with validation.
//            We use jobTable.colList[] (if provided) instead of data memmber
//            fColumns, because this function is called during preprocessing,
//            before TableInfo.fColumns has been initialized with data from
//            colList.
// segFileInfo - Vector of File objects carrying current DBRoot, partition,
//            HWM, etc to be validated for the columns belonging to jobTable.
// stage    - Current stage we are validating.  "Starting" or "Ending".
//------------------------------------------------------------------------------
int TableInfo::validateColumnHWMs(
    const JobTable* jobTable,
    const std::vector<DBRootExtentInfo>& segFileInfo,
    const char* stage )
{
    int rc = NO_ERROR;

    // Used to track first 1-byte, 2-byte, 4-byte, and 8-byte columns in table
    int byte1First = -1;
    int byte2First = -1;
    int byte4First = -1;
    int byte8First = -1;

    // Make sure the HWMs for all 1-byte columns match; same for all 2-byte,
    // 4-byte, and 8-byte columns as well.
    for (unsigned k=0; k<segFileInfo.size(); k++)
    {
        int k1 = 0;

        // Validate HWMs in jobTable if we have it, else use fColumns.
        const JobColumn& jobColK =
            ( (jobTable != 0) ? jobTable->colList[k] : fColumns[k].column );

        // Find the first 1-byte, 2-byte, 4-byte, and 8-byte columns.
        // Use those as our reference HWM for the respective column widths.
        switch ( jobColK.width )
        {
            case 1:
            {
                if (byte1First == -1)
                    byte1First = k;
                k1 = byte1First;
                break;
            }
            case 2:
            {
                if (byte2First == -1)
                    byte2First = k;
                k1 = byte2First;
                break;
            }
            case 4:
            {
                if (byte4First == -1)
                    byte4First = k;
                k1 = byte4First;
                break;
            }
            case 8:
            default:
            {
                if (byte8First == -1)
                    byte8First = k;
                k1 = byte8First;
                break;
            }
        } // end of switch based on column width (1,2,4, or 8)

        // Validate HWMs in jobTable if we have it, else use fColumns.
        const JobColumn& jobColK1 =
            ( (jobTable != 0) ? jobTable->colList[k1] : fColumns[k1].column );

//std::cout << "dbg: comparing0 " << stage << " refcol-" << k1 <<
//  "; wid-" << jobColK1.width << "; hwm-" << segFileInfo[k1].fLocalHwm <<
//  " <to> col-" << k <<
//  "; wid-" << jobColK.width << " ; hwm-"<<segFileInfo[k].fLocalHwm<<std::endl;

        // Validate that the HWM for this column (k) matches that of the
        // corresponding reference column with the same width.
        if ((segFileInfo[k1].fDbRoot    != segFileInfo[k].fDbRoot)    ||
            (segFileInfo[k1].fPartition != segFileInfo[k].fPartition) ||
            (segFileInfo[k1].fSegment   != segFileInfo[k].fSegment)   ||
            (segFileInfo[k1].fLocalHwm  != segFileInfo[k].fLocalHwm))
        {
            ostringstream oss;
            oss << stage << " HWMs do not match for"
                " OID1-"       << jobColK1.mapOid              <<
                "; column-"    << jobColK1.colName             <<
                "; DBRoot-"    << segFileInfo[k1].fDbRoot      <<
                "; partition-" << segFileInfo[k1].fPartition   <<
                "; segment-"   << segFileInfo[k1].fSegment     <<
                "; hwm-"       << segFileInfo[k1].fLocalHwm    <<
                "; width-"     << jobColK1.width << ':'<<std::endl<<
                " and OID2-"   << jobColK.mapOid               <<
                "; column-"    << jobColK.colName              <<
                "; DBRoot-"    << segFileInfo[k].fDbRoot       <<
                "; partition-" << segFileInfo[k].fPartition    <<
                "; segment-"   << segFileInfo[k].fSegment      <<
                "; hwm-"       << segFileInfo[k].fLocalHwm     <<
                "; width-"     << jobColK.width;
            fLog->logMsg( oss.str(), ERR_BRM_HWMS_NOT_EQUAL, MSGLVL_ERROR );
            return ERR_BRM_HWMS_NOT_EQUAL;
        }

        // HWM DBRoot, partition, and segment number should match for all
        // columns; so compare DBRoot, part#, and seg# with first column.
        if ((segFileInfo[0].fDbRoot    != segFileInfo[k].fDbRoot)    ||
            (segFileInfo[0].fPartition != segFileInfo[k].fPartition) ||
            (segFileInfo[0].fSegment   != segFileInfo[k].fSegment))
        {
            const JobColumn& jobCol0 =
            ( (jobTable != 0) ? jobTable->colList[0] : fColumns[0].column );

            ostringstream oss;
            oss << stage << " HWM DBRoot,Part#, or Seg# do not match for"
                " OID1-"       << jobCol0.mapOid               <<
                "; column-"    << jobCol0.colName              <<
                "; DBRoot-"    << segFileInfo[0].fDbRoot       <<
                "; partition-" << segFileInfo[0].fPartition    <<
                "; segment-"   << segFileInfo[0].fSegment      <<
                "; hwm-"       << segFileInfo[0].fLocalHwm     <<
                "; width-"     << jobCol0.width << ':'<<std::endl<<
                " and OID2-"   << jobColK.mapOid               <<
                "; column-"    << jobColK.colName              <<
                "; DBRoot-"    << segFileInfo[k].fDbRoot       <<
                "; partition-" << segFileInfo[k].fPartition    <<
                "; segment-"   << segFileInfo[k].fSegment      <<
                "; hwm-"       << segFileInfo[k].fLocalHwm     <<
                "; width-"     << jobColK.width;
            fLog->logMsg( oss.str(), ERR_BRM_HWMS_NOT_EQUAL, MSGLVL_ERROR );
            return ERR_BRM_HWMS_NOT_EQUAL;
        }
    } // end of loop to compare all 1-byte HWMs, 2-byte HWMs, etc.

    // Validate/compare HWM for 1-byte column in relation to 2-byte column, etc.
    // Without knowing the exact row count, we can't extrapolate the exact HWM
    // for the wider column, but we can narrow it down to an expected range.
    int refCol = 0;
    int colIdx = 0;

//if (byte1First >= 0)
//  std::cout << "dbg: cross compare1 " << stage << " col-" << byte1First <<
//  "; wid-" << ( (jobTable != 0) ? jobTable->colList[byte1First].width :
//                                  fColumns[byte1First].column.width ) <<
//  "; hwm-" << segFileInfo[byte1First].fLocalHwm << std::endl;

//if (byte2First >= 0)
//  std::cout << "dbg: cross compare2 " << stage << " col-" << byte2First <<
//  "; wid-" << ( (jobTable != 0) ? jobTable->colList[byte2First].width :
//                                  fColumns[byte2First].column.width ) <<
//  "; hwm-" << segFileInfo[byte2First].fLocalHwm << std::endl;

//if (byte4First >= 0)
//  std::cout << "dbg: cross compare4 " << stage << " col-" << byte4First <<
//  "; wid-" << ( (jobTable != 0) ? jobTable->colList[byte4First].width :
//                                  fColumns[byte4First].column.width ) <<
//  "; hwm-" << segFileInfo[byte4First].fLocalHwm << std::endl; 

//if (byte8First >= 0)
//  std::cout << "dbg: cross compare8 " << stage << " col-" << byte8First <<
//  "; wid-" << ( (jobTable != 0) ? jobTable->colList[byte8First].width :
//                                  fColumns[byte8First].column.width ) <<
//  "; hwm-" << segFileInfo[byte8First].fLocalHwm << std::endl;

    // Validate/compare HWMs given a 1-byte column as a starting point
    if (byte1First >= 0)
    {
        refCol = byte1First;

        if (byte2First >= 0)
        {
            HWM hwmLo = segFileInfo[byte1First].fLocalHwm * 2;
            HWM hwmHi = hwmLo + 1;
            if ((segFileInfo[byte2First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte2First].fLocalHwm > hwmHi))
            {
                colIdx = byte2First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
        if (byte4First >= 0)
        {
            HWM hwmLo = segFileInfo[byte1First].fLocalHwm * 4;
            HWM hwmHi = hwmLo + 3;
            if ((segFileInfo[byte4First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte4First].fLocalHwm > hwmHi))
            {
                colIdx = byte4First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
        if (byte8First >= 0)
        {
            HWM hwmLo = segFileInfo[byte1First].fLocalHwm * 8;
            HWM hwmHi = hwmLo + 7;
            if ((segFileInfo[byte8First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte8First].fLocalHwm > hwmHi))
            {
                colIdx = byte8First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
    }

    // Validate/compare HWMs given a 2-byte column as a starting point
    if (byte2First >= 0)
    {
        refCol = byte2First;

        if (byte4First >= 0)
        {
            HWM hwmLo = segFileInfo[byte2First].fLocalHwm * 2;
            HWM hwmHi = hwmLo + 1;
            if ((segFileInfo[byte4First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte4First].fLocalHwm > hwmHi))
            {
                colIdx = byte4First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
        if (byte8First >= 0)
        {
            HWM hwmLo = segFileInfo[byte2First].fLocalHwm * 4;
            HWM hwmHi = hwmLo + 3;
            if ((segFileInfo[byte8First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte8First].fLocalHwm > hwmHi))
            {
                colIdx = byte8First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
    }

    // Validate/compare HWMs given a 4-byte column as a starting point
    if (byte4First >= 0)
    {
        refCol = byte4First;

        if (byte8First >= 0)
        {
            HWM hwmLo = segFileInfo[byte4First].fLocalHwm * 2;
            HWM hwmHi = hwmLo + 1;
            if ((segFileInfo[byte8First].fLocalHwm < hwmLo) ||
                (segFileInfo[byte8First].fLocalHwm > hwmHi))
            {
                colIdx = byte8First;
                rc     = ERR_BRM_HWMS_OUT_OF_SYNC;
                goto errorCheck;
            }
        }
    }

// To avoid repeating this message 6 times in the preceding source code, we
// use the "dreaded" goto to branch to this single place for error handling.
errorCheck:
    if (rc != NO_ERROR)
    {
        const JobColumn& jobColRef = ( (jobTable != 0) ?
            jobTable->colList[refCol] : fColumns[refCol].column );
        const JobColumn& jobColIdx = ( (jobTable != 0) ?
            jobTable->colList[colIdx] : fColumns[colIdx].column );

        ostringstream oss;
        oss << stage << " HWMs are not in sync for"
            " OID1-"       << jobColRef.mapOid                 <<
            "; column-"    << jobColRef.colName                <<
            "; DBRoot-"    << segFileInfo[refCol].fDbRoot      <<
            "; partition-" << segFileInfo[refCol].fPartition   <<
            "; segment-"   << segFileInfo[refCol].fSegment     <<
            "; hwm-"       << segFileInfo[refCol].fLocalHwm    <<
            "; width-"     << jobColRef.width << ':'<<std::endl<<
            " and OID2-"   << jobColIdx.mapOid                 <<
            "; column-"    << jobColIdx.colName                <<
            "; DBRoot-"    << segFileInfo[colIdx].fDbRoot      <<
            "; partition-" << segFileInfo[colIdx].fPartition   <<
            "; segment-"   << segFileInfo[colIdx].fSegment     <<
            "; hwm-"       << segFileInfo[colIdx].fLocalHwm    <<
            "; width-"     << jobColIdx.width;
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
    }

    return rc;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Initialize the bulk rollback metadata writer for this table.
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int TableInfo::initBulkRollbackMetaData( )
{
    int rc = NO_ERROR;

    try
    {
        fRBMetaWriter.init( fTableOID, fTableName );
    }
    catch (WeException& ex)
    {
        fLog->logMsg(ex.what(), ex.errorCode(), MSGLVL_ERROR);
        rc = ex.errorCode();
    }

    return rc;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Saves snapshot of extentmap into a bulk rollback meta data file, for
//    use in a bulk rollback, if the current cpimport.bin job should fail.
//    The source code in RBMetaWriter::saveBulkRollbackMetaData() used to
//    reside in this TableInfo function.  But much of the source code was
//    factored out to create RBMetaWriter::saveBulkRollbackMetaData(), so
//    that the function would reside in the shared library for reuse by DML.
// PARAMETERS:
//    job - current job
//    segFileInfo - Vector of File objects carrying starting DBRoot, partition,
//                  etc, for each column belonging to tableNo.
//    dbRootHWMInfoVecCol - vector of last local HWM info for each DBRoot
//        (asssigned to current PM) for each column in "this" table.
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int TableInfo::saveBulkRollbackMetaData( Job& job,
    const std::vector<DBRootExtentInfo>& segFileInfo,
    const std::vector<BRM::EmDbRootHWMInfo_v>& dbRootHWMInfoVecCol )
{
    int rc = NO_ERROR;

    std::vector<Column> cols;
    std::vector<OID>    dctnryOids;

    // Loop through the columns in the specified table
    for ( size_t i = 0; i < job.jobTableList[fTableId].colList.size(); i++ ) 
    {
        JobColumn& jobCol = job.jobTableList[fTableId].colList[i];

        Column col;
        col.colNo               = i;
        col.colWidth            = jobCol.width;
        col.colType             = jobCol.weType;
        col.colDataType         = jobCol.dataType;
        col.dataFile.oid        = jobCol.mapOid;
        col.dataFile.fid        = jobCol.mapOid;
        col.dataFile.hwm        = segFileInfo[i].fLocalHwm;   // starting HWM
        col.dataFile.pFile      = 0;
        col.dataFile.fPartition = segFileInfo[i].fPartition;  // starting Part#
        col.dataFile.fSegment   = segFileInfo[i].fSegment;    // starting seg#
        col.dataFile.fDbRoot    = segFileInfo[i].fDbRoot;     // starting DBRoot
        col.compressionType     = jobCol.compressionType;
        cols.push_back( col );

        OID dctnryOid = 0;
        if (jobCol.colType == COL_TYPE_DICT)
            dctnryOid = jobCol.dctnry.dctnryOid;
        dctnryOids.push_back( dctnryOid );

    }   // end of loop through columns

    try
    {
        fRBMetaWriter.saveBulkRollbackMetaData(
            cols,
            dctnryOids,
            dbRootHWMInfoVecCol );
    }
    catch (WeException& ex)
    {
        fLog->logMsg(ex.what(), ex.errorCode(), MSGLVL_ERROR);
        rc = ex.errorCode();
    }

    return rc;
}

//------------------------------------------------------------------------------
// Synchronize system catalog auto-increment next value with BRM.
// This function is called at the end of normal processing to get the system
// catalog back in line with the latest auto increment next value generated by
// BRM.
//------------------------------------------------------------------------------
int TableInfo::synchronizeAutoInc( )
{
    for(unsigned i=0; i < fColumns.size(); ++i)
    {
        if (fColumns[i].column.autoIncFlag)
        {
            // TBD: Do we rollback flush cache error for autoinc.
            // Not sure we should bail out and rollback on a
            // ERR_BLKCACHE_FLUSH_LIST error, but we currently
            // rollback for "any" updateNextValue() error
            int rc = fColumns[i].finishAutoInc( );
            if (rc != NO_ERROR)
            {
                return rc;
            }
            break; // okay to break; only 1 autoinc column per table
        }
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Rollback changes made to "this" table by the current import job, delete the
// meta-data files, and release the table lock.  This function only applies to
// mode3 import.  Table lock and bulk rollbacks are managed by parent cpimport
// (file splitter) process for mode1 and mode2.
//------------------------------------------------------------------------------
int TableInfo::rollbackWork( )
{
    // Close any column or store files left open by abnormal termination.
    // We want to do this before reopening the files and doing a bulk rollback.
    closeOpenDbFiles();

    // Abort "local" bulk rollback if a DBRoot from the start of the job, is
    // now missing.  User should run cleartablelock to execute a rollback on
    // this PM "and" the PM where the DBRoot was moved to.
    std::vector<uint16_t> dbRootIds;
    Config::getRootIdList( dbRootIds );
    for (unsigned int j=0; j<fOrigDbRootIds.size(); j++)
    {
        bool bFound = false;
        for (unsigned int k=0; k<dbRootIds.size(); k++)
        {
            if (fOrigDbRootIds[j] == dbRootIds[k])
            {
                bFound = true;
                break;
            }
        }

        if (!bFound)
        {
            ostringstream oss;
            oss << "Mode3 bulk rollback not performed for table " <<
                fTableName << "; DBRoot" << fOrigDbRootIds[j] <<
                " moved from this PM during bulk load. " <<
                " Run cleartablelock to rollback and release the table lock " <<
                "across PMs.";
            int rc = ERR_BULK_ROLLBACK_MISS_ROOT;
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }
    }

    // Restore/rollback the DB files if we got far enough to begin processing
    // this table.
    int rc = NO_ERROR;
    if (hasProcessingBegun())
    {
        BulkRollbackMgr rbMgr( fTableOID,
            fTableLockID,
            fTableName,
            fProcessName, fLog );

        rc = rbMgr.rollback( fKeepRbMetaFile );
        if (rc != NO_ERROR)
        {
            ostringstream oss;
            oss << "Error rolling back table " << fTableName <<
                "; " << rbMgr.getErrorMsg();
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }
    }
    
    // Delete the meta data files after rollback is complete
    deleteMetaDataRollbackFile( );

    // Release the table lock
    rc = releaseTableLock( );
    if (rc != NO_ERROR)
    {
        ostringstream oss;
        oss << "Table lock not cleared for table " << fTableName;
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Allocate extent from BRM (through the stripe allocator).
//------------------------------------------------------------------------------
int TableInfo::allocateBRMColumnExtent(OID columnOID,
    uint16_t     dbRoot,
    uint32_t&    partition,
    uint16_t&    segment,
    BRM::LBID_t& startLbid,
    int&         allocSize,
    HWM&         hwm,
    std::string& errMsg )
{
    int rc = fExtentStrAlloc.allocateExtent( columnOID,
        dbRoot,
        partition,
        segment,
        startLbid,
        allocSize,
        hwm,
        errMsg );
    //fExtentStrAlloc.print();

    return rc;
}

}
// end of namespace
