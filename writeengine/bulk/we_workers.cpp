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
 * $Id: we_workers.cpp 4450 2013-01-21 14:13:24Z rdempsey $
 *
 ****************************************************************************/

#include "we_bulkload.h"
#include "we_bulkstatus.h"
#include "we_stats.h"
#include <sys/time.h>
#include <cmath>
#include "dataconvert.h"
#include <we_convertor.h>
using namespace std;
using namespace dataconvert;

namespace WriteEngine
{

//------------------------------------------------------------------------------
// Puts the current thread to sleep for the specified number of milliseconds.
//------------------------------------------------------------------------------
void BulkLoad::sleepMS(long ms)
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
// This is the main entry point method for each Read thread.
// id is the one-up number (starting at 0) associated with each Read thread.
//------------------------------------------------------------------------------
void  BulkLoad::read(int id)
{
#ifdef PROFILE
    Stats::registerReadProfThread( );
#endif
    // First get a table to work on.     
    // Acquire the read mutex
    // Iterate over the table list
    // if the table's status is new, set the locker = id
    // and then exit.
    int tableId = -1;

    try
    {
        //
        // LOOP to select and read the next table
        //
        while(true)
        {
            tableId = -1;
#ifdef PROFILE
            Stats::startReadEvent(WE_STATS_WAIT_TO_SELECT_TBL);
#endif
            if((tableId = lockTableForRead(id)) == -1)
            {
                fLog.logMsg( "BulkLoad::ReadOperation No more tables "
                              "available for processing. Read thread " 
                               + Convertor::int2Str(id) + " exiting...",
                               MSGLVL_INFO2);
#ifdef PROFILE
                Stats::stopReadEvent(WE_STATS_WAIT_TO_SELECT_TBL);
#endif
                return;
            }
#ifdef PROFILE
            Stats::stopReadEvent(WE_STATS_WAIT_TO_SELECT_TBL);
#endif
            int rc = fTableInfo[tableId].readTableData( );
            if (rc != NO_ERROR)
            {
                // Error occurred while reading the data, break out of loop.
                BulkStatus::setJobStatus( EXIT_FAILURE );
                ostringstream oss;
                oss << "Bulkload Read (thread " << id <<
                    ") Failed for Table " <<
                    fTableInfo[tableId].getTableName() <<
                    ".  Terminating this job.";
                fTableInfo[tableId].fBRMReporter.addToErrMsgEntry(oss.str());
                fLog.logMsg( oss.str(), rc, MSGLVL_CRITICAL );
                break;
            }
        }
    }
    catch (SecondaryShutdownException& ex)
    {
        // We are bailing out because another thread set bad job status
        ostringstream oss;
        if (tableId != -1)
            oss << "Bulkload Read (thread " << id <<
                ") Stopped reading Table "  <<
                fTableInfo[tableId].getTableName() << ".  "  << ex.what();
        else
            oss << "Bulkload Read (thread " << id <<
                ") Stopped reading Tables. " << ex.what();
        fLog.logMsg( oss.str(), MSGLVL_INFO1 );
    }
    catch (exception& ex)
    {
        BulkStatus::setJobStatus( EXIT_FAILURE );
        ostringstream oss;
        if (tableId != -1)
            oss << "Bulkload Read (thread " << id <<
                ") Failed for Table " <<
                fTableInfo[tableId].getTableName() << ".  "  << ex.what() <<
                ".  Terminating this job.";
        else
            oss << "Bulkload Read (thread " << id <<
                ") Failed for Table. " << ex.what() <<
                ".  Terminating this job.";
        if(tableId != -1) fTableInfo[tableId].fBRMReporter.addToErrMsgEntry(oss.str());
        fLog.logMsg( oss.str(), ERR_UNKNOWN, MSGLVL_CRITICAL ); 
    }
    catch (...)
    {
        BulkStatus::setJobStatus( EXIT_FAILURE );
        ostringstream oss;
        if (tableId != -1)
            oss << "Bulkload Read (thread " << id <<
                ") Failed for Table " <<
                fTableInfo[tableId].getTableName() <<
                ".  Terminating this job.";
        else
            oss << "Bulkload Read (thread " << id <<
                ") Failed for Table.  Terminating this job.";
        if(tableId != -1) fTableInfo[tableId].fBRMReporter.addToErrMsgEntry(oss.str());
        fLog.logMsg( oss.str(), ERR_UNKNOWN, MSGLVL_CRITICAL ); 
    }
}

//------------------------------------------------------------------------------
// Search for an available table to be read.
// First available table that is found, is locked and assigned to the specified
// thread id.
// Return value is -1 if no table is available for reading.
//------------------------------------------------------------------------------
int BulkLoad::lockTableForRead(int id)
{
    boost::mutex::scoped_lock lock(fReadMutex);
        
    for(unsigned i=0; i<fTableInfo.size(); ++i)
    {
        if(fTableInfo[i].lockForRead(id))
        return i;
    }

    return -1;    
}

//------------------------------------------------------------------------------
// This is the main entry point method for each parsing thread.
// id is the one-up number (starting at 0) associated with each parsing thread.
//------------------------------------------------------------------------------
void BulkLoad::parse(int id)
{
#ifdef PROFILE
    Stats::registerParseProfThread( );
#endif
    // Get a column from a buffer to parse.
    // The currentParseBuffer will be the buffer to be worked on
    int tableId       = -1;
    int columnId      = -1;
    int myParseBuffer = -1;

    try
    {
        //
        // LOOP to parse BulkLoadBuffers as they're loaded by Read thread(s)
        //
        while(true)
        {
#ifdef PROFILE
            Stats::startParseEvent(WE_STATS_WAIT_TO_SELECT_COL);
#endif

// @bug 3271: Conditionally compile the thread deadlock debug logging 
#ifdef DEADLOCK_DEBUG
            // @bug2099+ Temporary hack.
            struct timeval tvStart;
            gettimeofday(&tvStart, 0);
            bool report = false;
            bool reported = false;
            // @bug2099-
#else
            const bool report = false;
#endif

            //
            // LOOP to wait and select table/column/buffers
            // (BulkLoadBuffers) as they are loaded by the Read buffer.
            //
            while(true)
            {
                tableId       = -1;
                columnId      = -1;
                myParseBuffer = -1;

                //See if JobStatus has been set to terminate by other thread
                if (BulkStatus::getJobStatus() == EXIT_FAILURE)
                {
                    throw SecondaryShutdownException( "BulkLoad::"
                        "parse() responding to job termination");
                }

                if(allTablesDone(WriteEngine::PARSE_COMPLETE))
                {
#ifdef PROFILE
                    Stats::stopParseEvent(WE_STATS_WAIT_TO_SELECT_COL);
#endif
                    // no column from any of the tables available for
                    // parsing, hence exit.
                    return;
                }

                if(lockColumnForParse(id, tableId, columnId,
                                      myParseBuffer, report))
                    break;

                // Sleep and check the condition again.
                sleepMS(1);
#ifdef DEADLOCK_DEBUG
                // @bug2099+
                if(report) report = false; // report one time.
                if(!reported)
                {
                    struct timeval tvNow;
                    gettimeofday(&tvNow, 0);
                    if((tvNow.tv_sec - tvStart.tv_sec) >= 100)
                    {
                        time_t t = time(0);
                        char timeString[50];
                        ctime_r(&t, timeString);
                        timeString[ strlen(timeString)-1 ] = '\0';
                        ostringstream oss;
                        oss << endl << endl << timeString <<
                            ": BulkLoad::parse(" << id << "); " <<
#ifdef _MSC_VER
                            " Worker Thread " << GetCurrentThreadId() <<
#else
                            " Worker Thread " << pthread_self() <<
#endif
                            ":" << endl <<
                            "---------------------------------------"
                            "-------------------"  << endl;
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
            Stats::stopParseEvent(WE_STATS_WAIT_TO_SELECT_COL);
#endif
            // Have obtained the table and column for parsing.
            // Start parsing the column data.
            double processingTime;
            int rc = fTableInfo[tableId].parseColumn(columnId,myParseBuffer,
                                                     processingTime);
            if(rc != NO_ERROR)
            {
                // Error occurred while parsing the data, break out of loop.
                BulkStatus::setJobStatus( EXIT_FAILURE );
                ostringstream oss;
                oss << "Bulkload Parse (thread " << id <<
                    ") Failed for Table " <<
                    fTableInfo[tableId].getTableName() <<
                    " during parsing.  Terminating this job.";
                fTableInfo[tableId].fBRMReporter.addToErrMsgEntry(oss.str());
                fLog.logMsg( oss.str(), rc, MSGLVL_CRITICAL );

                setParseErrorOnTable( tableId, true );
                return;
            }

            // Parsing is complete. Acquire the mutex and increment
            // the parsingComplete value for the buffer
            if(fTableInfo[tableId].getStatusTI() != WriteEngine::ERR) 
            {
#ifdef PROFILE
                Stats::startParseEvent(WE_STATS_WAIT_TO_COMPLETE_PARSE);
#endif
                boost::mutex::scoped_lock lock(fParseMutex);
#ifdef PROFILE
                Stats::stopParseEvent(WE_STATS_WAIT_TO_COMPLETE_PARSE);
                Stats::startParseEvent(WE_STATS_COMPLETING_PARSE);
#endif
                rc = fTableInfo[tableId].setParseComplete(columnId,
                                                          myParseBuffer,
                                                          processingTime);
                if (rc != NO_ERROR)
                {
                    BulkStatus::setJobStatus( EXIT_FAILURE );
                    ostringstream oss;
                    oss << "Bulkload Parse (thread " << id <<
                        ") Failed for Table " <<
                        fTableInfo[tableId].getTableName() <<
                        " during parse completion.  Terminating this job.";
                    fTableInfo[tableId].fBRMReporter.addToErrMsgEntry(oss.str());
                    fLog.logMsg( oss.str(), rc, MSGLVL_CRITICAL );

                    setParseErrorOnTable( tableId, false );
                    return;
                }
#ifdef PROFILE
                Stats::stopParseEvent(WE_STATS_COMPLETING_PARSE);
#endif
            }
        }
    }
    catch (SecondaryShutdownException& ex)
    {
        // We are bailing out because another thread set bad job status
        ostringstream oss;
        if (tableId != -1) {
            oss << "Bulkload Parse (thread " << id <<
                ") Stopped parsing Table "  <<
                fTableInfo[tableId].getTableName() << ".  "  << ex.what();

            setParseErrorOnTable( tableId, true );
        }
        else {
            oss << "Bulkload Parse (thread " << id <<
                ") Stopped parsing Tables. " << ex.what();
        }
        fLog.logMsg( oss.str(), MSGLVL_INFO1 );
    }
    catch (exception& ex)
    {
        BulkStatus::setJobStatus( EXIT_FAILURE );
        ostringstream oss;
        if (tableId != -1) {
            oss << "Bulkload Parse (thread " << id <<
                ") Failed for Table " <<
                fTableInfo[tableId].getTableName() << ".  "  << ex.what() <<
                ".  Terminating this job.";

            setParseErrorOnTable( tableId, true );
            fTableInfo[tableId].fBRMReporter.addToErrMsgEntry(oss.str());
        }
        else {
            oss << "Bulkload Parse (thread " << id <<
                ") Failed for Table. " << ex.what() <<
                ".  Terminating this job.";
        }
        fLog.logMsg( oss.str(), ERR_UNKNOWN, MSGLVL_CRITICAL ); 
    }
    catch (...)
    {
        BulkStatus::setJobStatus( EXIT_FAILURE );
        ostringstream oss;
        if (tableId != -1) {
            oss << "Bulkload Parse (thread " << id <<
                ") Failed for Table " <<
                fTableInfo[tableId].getTableName() <<
                ".  Terminating this job.";

            setParseErrorOnTable( tableId, true );
            fTableInfo[tableId].fBRMReporter.addToErrMsgEntry(oss.str());
        }
        else {
            oss << "Bulkload Parse (thread " << id <<
                ") Failed for Table.  Terminating this job.";
        }
        fLog.logMsg( oss.str(), ERR_UNKNOWN, MSGLVL_CRITICAL ); 
    }
}

//------------------------------------------------------------------------------
// Search for an available table/column/buffer to be parsed.
// First available table/column/buffer that is found, is locked and assigned to
// the specified thread id.
// Return value is -1 if no table/column/buffer is available for parsing.
//------------------------------------------------------------------------------
// @bug2099 - Temp hack to diagnose deadlock. Added report parm and couts below.
bool BulkLoad::lockColumnForParse(
               int   thrdId,
               int & tableId,
               int & columnId,
               int & myParseBuffer,
               bool  report)
{
    // Acquire mutex
    // Iterate on the table list
    // Check if the currentParseBuffer is available for parsing
    // If yes, put the locker and fill the tableId and columnId
    // else, go to the next table for checking if a column is available
    boost::mutex::scoped_lock lock(fParseMutex);

    for(unsigned i=0; i<fTableInfo.size(); ++i)
    {
        if (fTableInfo[i].getStatusTI() == WriteEngine::PARSE_COMPLETE)
            continue;

        int currentParseBuffer = fTableInfo[i].getCurrentParseBuffer();
        myParseBuffer = currentParseBuffer;
        do
        {
            // @bug2099+
            if(report)
            {
                ostringstream oss;
                std::string bufStatusStr;
                Status stat = fTableInfo[i].getStatusTI();
                ColumnInfo::convertStatusToString( stat,
                                                   bufStatusStr );
#ifdef _MSC_VER
                oss << " - " << GetCurrentThreadId() <<
#else
                oss << " - " << pthread_self() <<
#endif
                    ":fTableInfo[" << i << "]" << bufStatusStr << " (" <<
                    stat << ")";
                if ( stat != WriteEngine::PARSE_COMPLETE )
                {
                    oss << "; fCurrentParseBuffer is " << myParseBuffer;
                }
                oss << endl;
                cout << oss.str();
                cout.flush();
            }
            // @bug2099-

            // get a buffer and column to parse if available.
            if((columnId = fTableInfo[i].getColumnForParse(
                           thrdId,myParseBuffer,report )) != -1)
            {
                tableId  = i;
                return true;
            }
            myParseBuffer = (myParseBuffer + 1) %
                            fTableInfo[i].getNumberOfBuffers();
        } 
        while(myParseBuffer != currentParseBuffer);
    }

    return false;
}

//------------------------------------------------------------------------------
// Used by the parsing threads to check to see if all the tables match the
// specified status.  The only current use for the function is to check to
// see if all the tables have a PARSE_COMPLETE status, hence the name of the
// function.
// If a table has an ERR status, then all the tables are considered finished,
// because that basically means we had an import error, and we need to shut
// down the job.
// Returns TRUE if all the tables are considered completed, else returns FALSE.
//------------------------------------------------------------------------------
bool BulkLoad::allTablesDone(Status status)
{
    for(unsigned i=0; i<fTableInfo.size(); ++i)
    {
        if(fTableInfo[i].getStatusTI() == WriteEngine::ERR)
            return true;
        if(fTableInfo[i].getStatusTI() != status)
            return false;
    }

    return true;
}

//------------------------------------------------------------------------------
// Set status for specified table to reflect a parsing error.
// Optionally lock fParseMutex (if requested).
// May evaluate later whether we need to employ the fParseMutex for this call.
//------------------------------------------------------------------------------
void BulkLoad::setParseErrorOnTable( int tableId, bool lockParseMutex )
{
    if (lockParseMutex)
    {
        boost::mutex::scoped_lock lock(fParseMutex);
        fTableInfo[tableId].setParseError( );
    }
    else
    {
        fTableInfo[tableId].setParseError( );
    }
}

}
