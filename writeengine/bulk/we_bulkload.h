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
* $Id: we_bulkload.h 3778 2012-04-20 19:08:56Z dcathey $
*
*******************************************************************************/
/** @file */

#ifndef _WE_BULKLOAD_H_
#define _WE_BULKLOAD_H_
#ifndef _MSC_VER
#include <pthread.h>
#endif
#include <fstream>
#include <string>
#include <vector>
#include <sys/time.h>

#include <we_log.h>
#include <we_colop.h> 
#include <we_xmljob.h>
#include <we_convertor.h>
#include <writeengine.h>

#include <we_brm.h>

#include "we_tableinfo.h"
#include "brmtypes.h"
#include "boost/ptr_container/ptr_vector.hpp"
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>

/** Namespace WriteEngine */
namespace WriteEngine
{
/** Class BulkLoad */
class BulkLoad : public FileOp
{
public:

   /**
    * @brief Constructor
    */
    BulkLoad();

   /**
    * @brief Default Destructor
    */
    ~BulkLoad();

   /**
    * @brief Load job information
    */
    int         loadJobInfo( const std::string& fullFileName,
                             bool  bUseTempJobFile,
                             const std::string& systemLang,
                             int   argc,
                             char** argv,
                             bool  bLogInfo2ToConsole,
                             bool  bValidateColumnList );

   /**
    * @brief Pre process jobs to validate and assign values to the job structure
    */
    int         preProcess( Job& job, int tableNo, TableInfo* tableInfo );

   /**
    * @brief Print job information
    */
    void        printJob() { if (isDebug(DEBUG_1))
                                 m_jobInfo.printJobInfo(m_log);
                             else
                                 m_jobInfo.printJobInfoBrief(m_log); }

   /**
    * @brief Process job
    */
    int         processJob( );

   /**
    * @brief Set Debug level for this BulkLoad object and any data members
    */
    void        setAllDebug( DebugLevel level );

   /**
    * @brief Update next autoincrement value for specified OID.
    *  @param tableOID  table whose autoincrement column is to be updated
    *  @param nextValue next autoincrement value to assign to tableOID
    */
    static int  updateNextValue(OID tableOID, long long nextValue);

   /**
    * @brief Setter for the private members
    */
    const std::string& getAlternateImportDir() { return fAlternateImportDir; }
    int         setAlternateImportDir(const std::string& loadDir,
                                      std::string& errMsg);
    void        setColDelimiter( char delim ) { m_colDelim = delim; }
    const std::string& getJobDir() {return DIR_BULK_JOB;}
    const std::string& getTempJobDir() {return DIR_BULK_TEMP_JOB;}
    const std::string& getSchema() {return m_jobInfo.getJob().schema;}
    void        setKeepRbMetaFiles(bool keepMeta) {fKeepRbMetaFiles = keepMeta;}
    void        setMaxMemSize( long long& maxMemSize) {m_maxMemSize=maxMemSize;}
    void        setNoOfParseThreads(int parseThreads)
                { fNoOfParseThreads = parseThreads; }
    void        setNoOfReadThreads( int readThreads)
                { fNoOfReadThreads = readThreads; }
    void        setNullStringMode( bool bMode ) { fNullStringMode = bMode; }
    void        setEnclosedByChar( char enChar) { fEnclosedByChar = enChar;}
    void        setEscapeChar    ( char esChar) { fEscapeChar     = esChar;}
    void        setParserNum( int parser ) { m_numOfParser = parser; }
    void        setProcessName(const std::string& processName)
                { fProcessName = processName; }
    void        setTxnID(BRM::TxnID txnID) {fTxnID = txnID;}
    void        addToCmdLineImportFileList(const std::string& importFile)
                { fCmdLineImportFiles.push_back( importFile ); }

    // Timer functions
    void        startTimer() { gettimeofday( &fStartTime, 0 ); }
    void        stopTimer()  { gettimeofday( &fEndTime, 0 );
                    fTotalTime =
                    (fEndTime.tv_sec   + (fEndTime.tv_usec   / 1000000.0)) -
                    (fStartTime.tv_sec + (fStartTime.tv_usec / 1000000.0)); }
    double      getTotalRunTime() const { return fTotalTime; }

    std::vector<std::string> tokenizeStr(const std::string& location,
                                         const std::string& filename);

private:

    //--------------------------------------------------------------------------
    // Private Data Members
    //--------------------------------------------------------------------------
    XMLJob      m_jobInfo;                 // current job information

    boost::scoped_ptr<ColumnOp> m_colOp;   // column operation

    std::string m_rootDir;                 // job process root directory
    std::string m_jobFileName;             // job description file name

    Log         m_log;                     // logger

    int         m_numOfParser;             // total number of parser
    char        m_colDelim;                // delimits col values within a row
    long long   m_maxMemSize;

    int         fNoOfBuffers;              // Number of read buffers
    int         fBufferSize;               // Read buffer size
    int         fFileBufferSize;           // Internal file system buffer size
    std::string fAlternateImportDir;       // Alternate bulk import directory
    std::string fProcessName;              // Application process name
    boost::ptr_vector<TableInfo> fTableInfo;// Vector of Table information
    int         fNoOfParseThreads;         // Number of parse threads
    int         fNoOfReadThreads;          // Number of read threads
    boost::thread_group fReadThreads;      // Read thread group
    boost::thread_group fParseThreads;     // Parse thread group
    boost::mutex fReadMutex;               // Manages table selection by each
                                           //   read thread
    boost::mutex fParseMutex;              // Manages table/buffer/column
                                           //   selection by each parsing thread
    BRM::TxnID  fTxnID;                    // TransID acquired from SessionMgr
    bool        fKeepRbMetaFiles;          // Keep/delete bulkRB metadata files
    bool        fNullStringMode;           // Treat "NULL" as NULL value
    char        fEnclosedByChar;           // Char used to enclose column value
    char        fEscapeChar;               // Escape char within enclosed value
    timeval     fStartTime;                // job start time
    timeval     fEndTime;                  // job end time
    double      fTotalTime;                // elapsed time for current phase
    std::vector<std::string> fCmdLineImportFiles; // Import Files from cmd line

    static WriteEngineWrapper* fWEWrapper; // Access to WE Wrapper functions

    // WriteEngineWrapper functions were written for DML/DDL which is
    // not geared for thread safety, so we employ our own mutex around any
    // calls to WriteEngineWrapper functions.  Don't foresee using many Wrap-
    // per functions, but if we do, we should revisit the impact of this mutex.
    static boost::mutex*       fWEWrapperMutex;

    static const std::string   DIR_BULK_JOB;     // Bulk job directory
    static const std::string   DIR_BULK_TEMP_JOB;// Directory for temp job files
    static const std::string   DIR_BULK_IMPORT;  // Bulk job import dir
    static const std::string   DIR_BULK_LOG;     // Bulk job log directory

    //--------------------------------------------------------------------------
    // Private Functions
    //--------------------------------------------------------------------------

    /** @brief Spawn the worker threads.
     */
    void spawnWorkers();

    /** @brief Method checks if all tables have the status set
     *  @param Status to be checked
     */
    bool allTablesDone(Status status);

    /** @brief Lock the table for read. Called by the read thread.
     *  @param Thread id
     */
    int lockTableForRead(int id);

    /** @brief Get column for parsing. Called by the parse thread.
     * @param Thread id
     * @param Table id set by the method
     * @param Column id set by the method
     * @param Parse buffer id set by the method.
     */
    // @bug 2099 - Temporary hack to diagnose deadlock. Added report parm below.
    bool lockColumnForParse(int id,
                            int &tableId,
                            int &columnId,
                            int &myParseBuffer,
                            bool report);

    /** @brief The thread method for the read thread.
     *  @param Thread id.
     */
    void read(int id);

    /** @brief The thread method for the parse thread.
     *  @param Thread id.
     */
    void parse(int  id);

    /** @brief Sleep method
     */
    void sleepMS(long int ms);

    /** @brief Initialize auto-increment column for specified schema and table.
     *  @param fullTableName Schema and table name separated by a period.
     *  @param colInfo ColumnInfo associated with auto-increment column.
     */
    int preProcessAutoInc( const std::string& fullTableName,
        ColumnInfo* colInfo);

    /**
     * @brief Rollback any tables that are left in a locked state at EOJ.
     */
    int rollbackLockedTables( );

    /**
     * @brief Rollback a table left in a locked state.
     */
    int rollbackLockedTable( TableInfo& tableInfo );

    /**
     * @brief Save metadata info required for bulk rollback.
     *  @param job Current job
     *  @param tableNo Table number in current job
     *  @param tableInfo TableInfo associated with table of interest
     *  @param segFileInfo Vector of segment File info for columns in tableNo
     */
    int saveBulkRollbackMetaData( Job& job,
        int tableNo,
        TableInfo* tableInfo,
        const std::vector<File>& segFileInfo );

    /**
     * @brief Validate existence of import data files.
     *  @param job Current job
     *  @param tableNo Table number in current job
     *  @param tableInfo TableInfo associated with table of interest
     */
    int validateImportDataFiles(Job& job,
        int tableNo, TableInfo* tableInfo);
};

}//end of namespace

#endif // _WE_BULKLOAD_H_
