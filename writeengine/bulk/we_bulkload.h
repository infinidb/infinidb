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
* $Id: we_bulkload.h 4631 2013-05-02 15:21:09Z dcathey $
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
#include <boost/uuid/uuid.hpp>

#if 0 //defined(_MSC_VER) && defined(WE_BULKLOAD_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{

/** Class BulkLoad */
class BulkLoad : public FileOp
{
public:

   /**
    * @brief BulkLoad onstructor
    */
    EXPORT              BulkLoad();

   /**
    * @brief BulkLoad destructor
    */
    EXPORT              ~BulkLoad();

   /**
    * @brief Load job information
    */
    EXPORT int          loadJobInfo( const std::string& fullFileName,
                             bool  bUseTempJobFile,
                             const std::string& systemLang,
                             int   argc,
                             char** argv,
                             bool  bLogInfo2ToConsole,
                             bool  bValidateColumnList );

   /**
    * @brief Pre process jobs to validate and assign values to the job structure
    */
    int                 preProcess(Job& job, int tableNo, TableInfo* tableInfo);

   /**
    * @brief Print job information
    */
    void                printJob( );

   /**
    * @brief Process job
    */
    EXPORT int          processJob( );

   /**
    * @brief Set Debug level for this BulkLoad object and any data members
    */
    void                setAllDebug( DebugLevel level );

   /**
    * @brief Update next autoincrement value for specified OID.
    *  @param columnOID  oid of autoincrement column to be updated
    *  @param nextValue next autoincrement value to assign to tableOID
    */
    static int          updateNextValue(OID columnOID, uint64_t nextValue);

    // Accessors and mutators
    void                addToCmdLineImportFileList(const std::string& importFile);
    const std::string&  getAlternateImportDir( ) const;
    const std::string&  getErrorDir          ( ) const;
	const std::string&  getJobDir            ( ) const;
    const std::string&  getSchema            ( ) const;
    const std::string&  getTempJobDir        ( ) const;
    bool                getTruncationAsError ( ) const;
    BulkModeType        getBulkLoadMode      ( ) const;
    bool                getContinue          ( ) const;
    boost::uuids::uuid  getJobUUID           ( ) const { return fUUID; }

    EXPORT int          setAlternateImportDir( const std::string& loadDir,
                                               std::string& errMsg);
    void                setImportDataMode    ( ImportDataMode importMode );
    void                setColDelimiter      ( char delim );
    void                setBulkLoadMode      ( BulkModeType bulkMode,
                                               const std::string& rptFileName );
    void                setEnclosedByChar    ( char enChar);
    void                setEscapeChar        ( char esChar);
    void                setKeepRbMetaFiles   ( bool keepMeta );
    void                setMaxErrorCount     ( unsigned int maxErrors );
    void                setNoOfParseThreads  ( int parseThreads );
    void                setNoOfReadThreads   ( int readThreads );
    void                setNullStringMode    ( bool bMode );
    void                setParseErrorOnTable (int tableId, bool lockParseMutex);
    void                setParserNum         ( int parser );
    void                setProcessName       ( const std::string& processName );
    void                setReadBufferCount   ( int noOfReadBuffers );
    void                setReadBufferSize    ( int readBufferSize );
    void                setTxnID             ( BRM::TxnID txnID );
    void                setVbufReadSize      ( int vbufReadSize );
    void                setTruncationAsError ( bool bTruncationAsError );
    void                setJobUUID           ( const std::string& jobUUID );
	void                setErrorDir          ( const std::string& errorDir );
    // Timer functions
    void                startTimer           ( );
    void                stopTimer            ( );
    double              getTotalRunTime      ( ) const;
    
    void                disableTimeOut       ( const bool disableTimeOut);
    bool                disableTimeOut       ( ) const;
    
    static void 				disableConsoleOutput ( const bool noConsoleOutput)
	{ fNoConsoleOutput = noConsoleOutput; } 
	static bool 				disableConsoleOutput ( )
	{ return fNoConsoleOutput; }

	// Add error message into appropriate BRM updater 
    static bool         addErrorMsg2BrmUpdater(const std::string& tablename, const std::ostringstream& oss);
    void                setDefaultJobUUID     ( );

private:

    //--------------------------------------------------------------------------
    // Private Data Members
    //--------------------------------------------------------------------------
    XMLJob      fJobInfo;                 // current job information

    boost::scoped_ptr<ColumnOp> fColOp;   // column operation

    std::string fRootDir;                 // job process root directory
    std::string fJobFileName;             // job description file name

    Log         fLog;                     // logger

    int         fNumOfParser;             // total number of parser
    char        fColDelim;                // delimits col values within a row

    int         fNoOfBuffers;              // Number of read buffers
    int         fBufferSize;               // Read buffer size
    int         fFileVbufSize;             // Internal file system buffer size
    long long   fMaxErrors;                // Max allowable errors per job
    std::string fAlternateImportDir;       // Alternate bulk import directory
	std::string fErrorDir;                 // Opt. where error records record
    std::string fProcessName;              // Application process name
    static boost::ptr_vector<TableInfo> fTableInfo;// Vector of Table information
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
    BulkModeType fBulkMode;                // Distributed bulk mode (1,2, or 3)
    std::string fBRMRptFileName;           // Name of distributed mode rpt file
    bool        fbTruncationAsError;       // Treat string truncation as error
    ImportDataMode fImportDataMode;        // Importing text or binary data
    bool        fbContinue;                // true when read and parse r running
                                           //
    static boost::mutex*       fDDLMutex;  // Insure only 1 DDL op at a time

    EXPORT static const std::string   DIR_BULK_JOB;     // Bulk job directory
    EXPORT static const std::string   DIR_BULK_TEMP_JOB;// Dir for tmp job files
    static const std::string   DIR_BULK_IMPORT;  // Bulk job import dir
    static const std::string   DIR_BULK_LOG;     // Bulk job log directory
    bool        fDisableTimeOut;           // disable timeout when waiting for table lock
    boost::uuids::uuid fUUID;               // job UUID
    static bool		fNoConsoleOutput;		   // disable output to console

    //--------------------------------------------------------------------------
    // Private Functions
    //--------------------------------------------------------------------------

    // Spawn the worker threads.
    void spawnWorkers();

    // Checks if all tables have the status set
    bool allTablesDone(Status status);

    // Lock the table for read. Called by the read thread.
    int lockTableForRead(int id);

    // Get column for parsing. Called by the parse thread.
    // @bug 2099 - Temporary hack to diagnose deadlock. Added report parm below.
    bool lockColumnForParse(int id,             // thread id
                            int &tableId,       // selected table id
                            int &columnId,      // selected column id
                            int &myParseBuffer, // selected parse buffer
                            bool report);

    // Map specified DBRoot to it's first segment file number
    int mapDBRootToFirstSegment(OID columnOid,
                            uint16_t  dbRoot,
                            uint16_t& segment);

    // The thread method for the read thread.
    void read(int id);

    // The thread method for the parse thread.
    void parse(int  id);

    // Sleep method
    void sleepMS(long int ms);

    // Initialize auto-increment column for specified schema and table.
    int preProcessAutoInc(
        const std::string& fullTableName,// schema.table
        ColumnInfo* colInfo);            // ColumnInfo associated with AI column

    // Determine starting HWM and LBID after block skipping added to HWM
    int preProcessHwmLbid( const ColumnInfo* info,
                               int          minWidth,
                               uint32_t    partition,
                               uint16_t    segment,
                               HWM&         hwm,
                               BRM::LBID_t& lbid,
                               bool&        bSkippedToNewExtent);

    // Rollback any tables that are left in a locked state at EOJ.
    int rollbackLockedTables( );

    // Rollback a table left in a locked state.
    int rollbackLockedTable( TableInfo& tableInfo );

    // Save metadata info required for shared-nothing bulk rollback.
    int saveBulkRollbackMetaData( Job& job,   // current job
        TableInfo* tableInfo,                 // TableInfo for table of interest
        const std::vector<DBRootExtentInfo>& segFileInfo, //vector seg file info
        const std::vector<BRM::EmDbRootHWMInfo_v>& dbRootHWMInfoPM);

    // Manage/validate the list of 1 or more import data files
    int manageImportDataFileList(Job& job,    // current job
        int tableNo,                          // table number of current job
        TableInfo* tableInfo);                // TableInfo for table of interest

    // Break up list of file names into a vector of filename strings
    int buildImportDataFileList(
        const std::string& location,
        const std::string& filename,
        std::vector<std::string>& importFileNames);
};

//------------------------------------------------------------------------------
// Inline functions
//------------------------------------------------------------------------------
inline void BulkLoad::addToCmdLineImportFileList(const std::string& importFile){
    fCmdLineImportFiles.push_back( importFile ); }

inline const std::string& BulkLoad::getAlternateImportDir( ) const {
    return fAlternateImportDir; }

inline const std::string& BulkLoad::getErrorDir( ) const {
    return fErrorDir; }

inline const std::string& BulkLoad::getJobDir( ) const {
    return DIR_BULK_JOB; }

inline const std::string& BulkLoad::getSchema( ) const {
    return fJobInfo.getJob().schema; }

inline const std::string& BulkLoad::getTempJobDir( ) const {
    return DIR_BULK_TEMP_JOB; }

inline bool BulkLoad::getTruncationAsError ( ) const {
    return fbTruncationAsError; }

inline BulkModeType BulkLoad::getBulkLoadMode ( ) const {
    return fBulkMode; }

inline bool BulkLoad::getContinue ( ) const {
    return fbContinue; }

inline void BulkLoad::printJob() {
    if (isDebug(DEBUG_1))
        fJobInfo.printJobInfo(fLog);
    else
        fJobInfo.printJobInfoBrief(fLog); }

inline void BulkLoad::setAllDebug( DebugLevel level ) {
    setDebugLevel( level );
    fLog.setDebugLevel( level ); }

inline void BulkLoad::setColDelimiter( char delim ) {
    fColDelim = delim; }

inline void BulkLoad::setBulkLoadMode(
    BulkModeType       bulkMode,
    const std::string& rptFileName ) {
    fBulkMode       = bulkMode;
    fBRMRptFileName = rptFileName; }

inline void BulkLoad::setEnclosedByChar( char enChar ) {
    fEnclosedByChar = enChar; }

inline void BulkLoad::setEscapeChar( char esChar ) {
    fEscapeChar     = esChar; }

inline void BulkLoad::setImportDataMode(ImportDataMode importMode) {
    fImportDataMode = importMode; }

inline void BulkLoad::setKeepRbMetaFiles( bool keepMeta ) {
    fKeepRbMetaFiles = keepMeta; }

// Mutator takes an unsigned int, but we store in a long long, because...
// TableInfo which eventually needs this attribute, takes an unsigned int,
// but we want to be able to init to -1, to indicate when it has not been set.
inline void BulkLoad::setMaxErrorCount( unsigned int maxErrors ) {
    fMaxErrors = maxErrors; }

inline void BulkLoad::setNoOfParseThreads(int parseThreads ) {
    fNoOfParseThreads = parseThreads; }

inline void BulkLoad::setNoOfReadThreads( int readThreads ) {
    fNoOfReadThreads = readThreads; }

inline void BulkLoad::setNullStringMode( bool bMode ) {
    fNullStringMode = bMode; }

inline void BulkLoad::setParserNum( int parser ) {
    fNumOfParser = parser; }

inline void BulkLoad::setProcessName( const std::string& processName ) {
    fProcessName = processName; }

inline void BulkLoad::setReadBufferCount( int noOfReadBuffers ) {
    fNoOfBuffers = noOfReadBuffers; }

inline void BulkLoad::setReadBufferSize( int readBufferSize ) {
    fBufferSize = readBufferSize; }

inline void BulkLoad::setTxnID( BRM::TxnID txnID ) {
    fTxnID = txnID; }

inline void BulkLoad::setVbufReadSize( int vbufReadSize ) {
    fFileVbufSize = vbufReadSize; }

inline void BulkLoad::setTruncationAsError(bool bTruncationAsError) {
    fbTruncationAsError = bTruncationAsError; }

inline void BulkLoad::setErrorDir( const std::string& errorDir ) {
    fErrorDir = errorDir; }

inline void BulkLoad::startTimer( ) {
    gettimeofday( &fStartTime, 0 ); }

inline void BulkLoad::stopTimer() {
    gettimeofday( &fEndTime, 0 );
    fTotalTime = (fEndTime.tv_sec   + (fEndTime.tv_usec   / 1000000.0)) -
                 (fStartTime.tv_sec + (fStartTime.tv_usec / 1000000.0)); }

inline double BulkLoad::getTotalRunTime() const {
    return fTotalTime; }
    
inline void BulkLoad::disableTimeOut( const bool disableTimeOut) {
    fDisableTimeOut = disableTimeOut; }

inline bool BulkLoad::disableTimeOut() const {
  return fDisableTimeOut; }

} // end of namespace

#undef EXPORT

#endif // _WE_BULKLOAD_H_
