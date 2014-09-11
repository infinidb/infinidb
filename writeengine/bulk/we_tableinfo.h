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
* $Id: we_tableinfo.h 3826 2012-05-06 21:13:55Z dcathey $
*
*******************************************************************************/
#ifndef _WE_TABLEINFO_H
#define _WE_TABLEINFO_H
#include "we_type.h"
#include "we_colop.h"
#include "we_fileop.h"
#include "we_blockop.h"
#include "we_brm.h"
#include "we_colbufmgr.h"
#include <utility>
#include <vector>
#include <boost/thread/mutex.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include "we_columninfo.h"
#include "we_bulkloadbuffer.h"
#include "we_rbmetawriter.h"
#include "we_log.h"
#include "we_brmreporter.h"
#include <sys/time.h>
#include "messagelog.h"
#include "brmtypes.h"
#include <fstream>
// @bug 2099+
#include <iostream>
#include <sstream>
// @bug 2099-

namespace WriteEngine
{
    
/* @brief Class which maintains the information for a table.
 */
class TableInfo
{
private:

    //--------------------------------------------------------------------------
    // Private Data Members
    //--------------------------------------------------------------------------

    int fTableId;                       // Table id
    int fBufferSize;                    // Size of buffer used by BulkLoadBuffer
    int fFileBufSize;                   // Size of fFileBuffer passed to setvbuf
                                        //   to read import files.  Comes from
                                        //   writeBufferSize tag in job xml file
    char fColDelim;                     // Used to delimit col values in a row
    volatile Status fStatusTI;          // Status of table.  Made volatile to
                                        //   insure BulkLoad methods can access
                                        //   (thru getStatusTI()) correctly w/o
                                        //   having to go through a mutex lock.
    int fReadBufCount;                  // Number of read buffers
                                        //   (size of fBuffers vector)
    unsigned fNumberOfColumns;          // Number of ColumnInfo objs in this tbl
                                        //   (size of fColumns vector)
    FILE * fHandle;                     // Handle to the input load file
    int fCurrentReadBuffer;             // Id of current buffer being popu-
                                        //   lated by the read thread
    RID fTotalReadRows;                 // Total number of rows read
    volatile unsigned fTotalErrRows;    // Total error rows among all input
                                        //   for this table.  Is volatile to
                                        //   insure parser & reader threads
                                        //   see the latest value.
    unsigned fMaxErrorRows;             // Maximum error rows
    int fLastBufferId;                  // Id of the last buffer
    char* fFileBuffer;                  // File buffer passed to setvbuf()
    int fCurrentParseBuffer;            // Id of leading current buffer being
                                        //   parsed.  There can be more than 1
                                        //   buffer being parsed concurrently.
    unsigned fNumberOfColsParsed;       // Number of columns completed parsing
    boost::ptr_vector<ColumnInfo> fColumns; // Columns of the table
    boost::ptr_vector<BulkLoadBuffer> fBuffers; // Array of read buffers.  Used
                                        //   to pass data from the read
                                        //   thread to the write thread(s)

    /* fSyncUpdatesTI is the mutex used to synchronize updates to TableInfo
     * (excluding fErrorRows and fErrDataRows)
     *
     * This mutex is also used to coordinate access to fColumnLocks and
     * fParseComplete (in BulkLoadBuffer) for the buffers within a table.
     * See bulkloadBuffer.h for more information.
     *  
     * As the controlling class, TableInfo is the one that is always
     * getting/setting the status of the BulkLoadBuffer objects, so
     * fSyncUpdatesTI is also used to set/get the BulkLoadBuffer status.
     */
    boost::mutex fSyncUpdatesTI;

    boost::mutex fErrorRptInfoMutex;    // Used to synhronize access to
                                        //   fRejectDataFile & fRejectErrFile
    int fLocker;                        // Read thread id reading this table
    std::vector<std::string> fLoadFileList; // Load files
    std::string fFileName;              // Current load file
    std::string fTableName;             // File name of the table
    OID fTableOID;                      // OID of the table
    std::string fjobFileName;           // Job file name
    int fJobId;                         // Job ID number
    Log* fLog;                          // Object used for logging
    timeval fStartTime; // Time when reading and processing began for this table
    const BRM::TxnID fTxnID;            // Transaction id for the build load
    RBMetaWriter fRBMetaWriter;         // Manages the writing of bulk roll-
                                        //   back meta data for this table
    std::string fProcessName;           // Name of application process used
                                        //   in db table locks
    bool fKeepRbMetaFile;               // Keep or delete bulk rollback meta
                                        //   data file
    volatile bool fTableLocked;         // Do we have db table lock
    bool fReadFromStdin;                // Read import file from STDIN
    bool fNullStringMode;               // Treat "NULL" as a null value
    char fEnclosedByChar;               // Character to enclose col values
    char fEscapeChar;                   // Escape character used in conjunc-
                                        //   tion with fEnclosedByChar
    bool fProcessingBegun;              // Has processing begun on this tbl
    BRMReporter fBRMReporter;           // Object used to report BRM updates

    std::ofstream fRejectDataFile;      // File containing rejected rows
    std::ofstream fRejectErrFile;       // File containing errmsgs for bad rows
    std::string   fRejectDataFileName;  // Filename for current fRejectDataFile
    std::string   fRejectErrFileName;   // Filename for current fRejectErrFile
    unsigned int  fRejectDataCnt;       // Running row count in current bad file
    unsigned int  fRejectErrCnt;        // Running count in current err msg file

    //--------------------------------------------------------------------------
    // Private Functions
    //--------------------------------------------------------------------------

    void closeTableFile();              // Change state of table lock to cleanup
    int  finishBRM();                   // Finish reporting updates for BRM
    void freeProcessingBuffers();       // Free up Processing Buffers
    bool isBufferAvailable(bool report);// Is tbl buffer available for reading
    int openTableFile();                // Open data file and set the buffer
    void sleepMS(long int ms);          // Sleep method

    // Write the list of errors for this table
    void writeErrorList(const std::vector< std::pair<RID,
                                           std::string> >* errorRows,
                         const std::vector<std::string>* errorDatRows,
                         bool bCloseFile);

    // Write out rejected rows, and corresponding error messages
    void writeBadRows( const std::vector<std::string>* errorDatRows,
                       bool bCloseFile );
    void writeErrReason( const std::vector< std::pair<RID,
                                            std::string> >* errorRows,
                         bool bCloseFile );

    // Disable copy constructor and assignment operator
    TableInfo (const TableInfo &tableInfo);
    TableInfo & operator =(const TableInfo & info);

public:

    //-------------------------------------------------------------------------
    // Public Functions
    //-------------------------------------------------------------------------

    /** @brief Default constructor
     */
    TableInfo(Log* logger, const BRM::TxnID,
              const std::string& processName,
              OID   tableOID,
              const std::string& tableName,
              bool  bKeepRbMetaFile);

    /** @brief Default destructor
     */
    ~TableInfo();

    /** @brief Acquire the DB table lock for this table
     */
    int acquireTableLock( );

    /** @brief Release the DB table lock for this table
     */
    int releaseTableLock( );

    /** @brief Close any DB files left open at job's end
     */
    void closeOpenDbFiles( );

    /** @brief Delete the bulk rollback metadata file.
     */
    void deleteMetaDataRollbackFile( );

    /** @brief Get number of buffers
     */
    int getNumberOfBuffers() const {return fReadBufCount;}

    /** @brief Set the buffer size
     *  @param Buffer size
     */
    void setBufferSize(const int bufSize){fBufferSize = bufSize;}

    /** @brief Set the file buffer size.
     *  @param Buffer size
     */
    void setFileBufferSize(const int fileBufSize){fFileBufSize=fileBufSize;}
        
    /** @brief Set the delimiter used to delimit column values within a row
     */
    void setColDelimiter(const char delim){fColDelim = delim;}

    /** @brief Get table status
     */
    Status getStatusTI() const { return fStatusTI; }

    /** @brief Get current parse buffer
     */
    int getCurrentParseBuffer() const {return fCurrentParseBuffer;}    

    /** @brief Get the number of columns 
     */
    const int getNumberOfColumns() const {return fNumberOfColumns;} 

    /** @brief get the file name
     */
    std::string getFileName() const {return fFileName;}

    /** @brief Set the table load file name
     */
    void setFileName(const std::string & fileName) {fFileName= fileName;}
       
    /** @brief Get the number of maximum allowed error rows
     */
    unsigned getMaxErrorRows() const {return fMaxErrorRows;}

    /** @brief set the maximum number of error rows allowed
     */
    void setMaxErrorRows(const unsigned int maxErrorRows)
    { fMaxErrorRows = maxErrorRows; }

    /** @brief Set mode to treat "NULL" string as NULL value or not.
     */
    void setNullStringMode( bool bMode ) { fNullStringMode = bMode; }

    /** @brief Set character optionally used to enclose input column values.
     */
    void setEnclosedByChar( char enChar ) { fEnclosedByChar = enChar; }

    /** @brief Set escape char to use in conjunction with enclosed by char.
     */
    void setEscapeChar ( char esChar )    { fEscapeChar  = esChar; }

    /** @brief Has processing begun for this table.
     */
    bool hasProcessingBegun() { return fProcessingBegun; }

    /** @brief set the table id
     */
    void setTableId(const int & id) {fTableId = id;}

    /** @brief get the file name
     */
    std::string getTableName() const {return fTableName;}

    /** @brief get the table OID
     */
    OID getTableOID( ) { return fTableOID; }

    /** @brief get the bulk rollback meta data writer object for this table
     */
    RBMetaWriter* rbMetaWriter() { return &fRBMetaWriter; }

    /** @brief Add column information to the table
     */
    void addColumn(ColumnInfo * info);

    /** @brief Initialize the buffer list
     *  @param noOfBuffers Number of buffers to create for this table
     *  @param jobFieldRefList List of fields in this import
     */
    void initializeBuffers(const int &noOfBuffers,
                           const JobFieldRefList& jobFieldRefList);

    /** @brief Read the table data into the read buffer
     */
    int readTableData( );

    /** @brief parse method
     */
    int parseColumn(const int &columnId, const int &bufferId,
                    double& processingTime);

    /** @brief update the buffer status for column
     */
    int setParseComplete(const int &columnId,
                         const int & bufferId,
                         double processingTime);

    /** @brief update the status to reflect a parsing error
     */
    void setParseError ();

    /** @brief Check if buffer ready for parsing.
     */
    // @bug 2099.  Temporary hack to diagnose deadlock.  Added report parm
    //             and couts below.
    bool bufferReadyForParse(const int &bufferId, bool report) const 
    {
        if (fBuffers.size() == 0)
            return false;

        Status stat = fBuffers[bufferId].getStatusBLB();
        if(report) {
            std::ostringstream oss;
            std::string bufStatusStr;
            ColumnInfo::convertStatusToString( stat,
                                               bufStatusStr );
#ifdef _MSC_VER
            oss << " --- " << GetCurrentThreadId() <<
#else
            oss << " --- " << pthread_self() <<
#endif
            ":fBuffers[" << bufferId << "]=" << bufStatusStr <<
            " (" << stat << ")" << std::endl;
            std::cout << oss.str();
        }
        return (stat == WriteEngine::READ_COMPLETE)?true:false;
    }

    /** @brief Check if a column is available for parsing in the buffer
     *  and return the column id if available
     */
    int getColumnForParse(const int & id,const int & bufferId,bool report);

    /** @brief Do we have a db lock with the session manager for this table.
     */
    bool isTableLocked() { return fTableLocked; }

    /** @brief Lock the table for reading
     */
    bool lockForRead(const int & locker);

    /** @brief set list of import files and STDIN usage flag
     */
    void setLoadFilesInput(bool  bReadFromStdin,
                           const std::vector<std::string>& files)
    { fReadFromStdin = bReadFromStdin;
      fLoadFileList  = files; }

    /** @brief set job file name under process.
     */
    void setJobFileName(const std::string & jobFileName)
    { fjobFileName = jobFileName; }

    /** @brief set job ID for this import.
     */
    void setJobId(int jobId) { fJobId = jobId; }

    /** @brief log message to data_mods.log file.
     */
    void logToDataMods(const std::string& jobFile,
                       const std::string&  messageText);

    /** @brief Validate consistency of current HWMs for this table's columns.
     *  If jobTable argument is provided, then it will be used to get additional
     *  column info, else this table's fColumns vector is used.
     *  "stage" indicates validation stage ("Starting" or "Ending" HWMs).
     */
    int validateColumnHWMs( const JobTable* jobTable,
                            const std::vector<File>& segFileInfo,
                            const char* stage );
};

}
#endif

