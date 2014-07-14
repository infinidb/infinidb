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
* $Id: we_bulkload.cpp 4730 2013-08-08 21:41:13Z chao $
*
*******************************************************************************/
/** @file */

#define WE_BULKLOAD_DLLEXPORT
#include "we_bulkload.h"
#undef WE_BULKLOAD_DLLEXPORT

#include <cmath>
#include <cstdlib>
#include <climits>
#include <glob.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <vector>
#include <sstream>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "we_bulkstatus.h"
#include "we_rbmetawriter.h"
#include "we_colopbulk.h"
#include "we_columninfocompressed.h"
#include "we_config.h"
#include "we_dbrootextenttracker.h"
#include "writeengine.h"
#include "sys/time.h"
#include "sys/types.h"
#include "dataconvert.h"
#include "idbcompress.h"
#include "calpontsystemcatalog.h"
#include "we_ddlcommandclient.h"


using namespace std;
using namespace boost;
using namespace dataconvert;

namespace
{
    const std::string IMPORT_PATH_STDIN("STDIN");
    const std::string IMPORT_PATH_CWD  (".");
    const std::string LOG_SUFFIX       = ".log";     // Job log file suffix
    const std::string ERR_LOG_SUFFIX   = ".err";     // Job err log file suffix
}

//extern WriteEngine::BRMWrapper* brmWrapperPtr;
namespace WriteEngine
{
	/* static */ boost::ptr_vector<TableInfo> BulkLoad::fTableInfo;
    /* static */ boost::mutex*       BulkLoad::fDDLMutex = 0;

    /* static */ const std::string   BulkLoad::DIR_BULK_JOB("job");
    /* static */ const std::string   BulkLoad::DIR_BULK_TEMP_JOB("tmpjob");
    /* static */ const std::string   BulkLoad::DIR_BULK_IMPORT("/data/import/");
    /* static */ const std::string   BulkLoad::DIR_BULK_LOG("/log/");
    /* static */ bool     			 BulkLoad::fNoConsoleOutput = false;

//------------------------------------------------------------------------------
// A thread to periodically call dbrm to see if a user is
// shutting down the system or has put the system into write
// suspend mode. DBRM has 2 flags to check in this case, the
// ROLLBACK flag, and the FORCE flag. These flags will be
// reported when we ask for the Shutdown Pending flag (which we
// ignore at this point). Even if the user is putting the system
// into write suspend mode, this call will return the flags we
// are interested in. If ROLLBACK is set, we cancel normally.
// If FORCE is set, we can't rollback.
struct CancellationThread
{
    CancellationThread(BulkLoad* pBulkLoad) : fpBulkLoad(pBulkLoad)
    {}
	BulkLoad* fpBulkLoad;
    void operator()()
    {
        bool bRollback = false;
        bool bForce = false;
        int  iShutdown;
        while (fpBulkLoad->getContinue())
        {
            usleep(1000000);    // 1 seconds
            // Check to see if someone has ordered a shutdown or suspend with
            // rollback or force.
            iShutdown = BRMWrapper::getInstance()->isShutdownPending(bRollback, 
                                                                     bForce);
            if (iShutdown != ERR_BRM_GET_SHUTDOWN)
            {
                if (bRollback)
                {
                    if (iShutdown == ERR_BRM_SHUTDOWN)
                    {
						if (!BulkLoad::disableConsoleOutput())
			                cout << "System stop has been ordered. Rollback" 
				                 << endl;
                    }
                    else
                    {
						if (!BulkLoad::disableConsoleOutput())
							cout << "Database writes have been suspended. Rollback" 
						         << endl;
                    }
                    BulkStatus::setJobStatus( EXIT_FAILURE );
                }
                else
                if (bForce)
                {
					if (!BulkLoad::disableConsoleOutput())
		                cout << "Immediate system stop has been ordered. "
			                 << "No rollback" 
				             << endl;
                }
            }
        }
    }
};
 
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
BulkLoad::BulkLoad() :
    fColOp(new ColumnOpBulk()),
    fColDelim('\0'),
    fNoOfBuffers(-1), fBufferSize(-1), fFileVbufSize(-1), fMaxErrors(-1),
    fNoOfParseThreads(3), fNoOfReadThreads(1),
    fKeepRbMetaFiles(false),
    fNullStringMode(false),
    fEnclosedByChar('\0'), // not enabled unless user overrides enclosed by char
    fEscapeChar('\0'),
    fTotalTime(0.0),
    fBulkMode(BULK_MODE_LOCAL),
    fbTruncationAsError(false),
    fImportDataMode(IMPORT_DATA_TEXT),
    fbContinue(false),
    fDisableTimeOut(false),
    fUUID(boost::uuids::nil_generator()())
{
    fTableInfo.clear();
    setDebugLevel( DEBUG_0 );

    fDDLMutex = new boost::mutex();
    memset( &fStartTime, 0, sizeof(timeval) );
    memset( &fEndTime,   0, sizeof(timeval) );
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
BulkLoad::~BulkLoad()
{
    fTableInfo.clear();
    delete fDDLMutex;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Set alternate directory path for import data files.  If the specified
//    path is "STDIN", then the import data will be read from stdin.
//    Note that we check for read "and" write access to the import directory
//    path so that we can not only read the input files, but also write the
//    *.bad and *.err files to that directory.
// PARAMETERS:
//    loadDir - import directory path
//    errMsg  - return error msg if failed return code is returned
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::setAlternateImportDir(const std::string& loadDir,
    std::string& errMsg)
{
    if (loadDir == IMPORT_PATH_STDIN)
    {
        fAlternateImportDir = loadDir;
    }
    else
    {
        if ( access(loadDir.c_str(),R_OK|W_OK) < 0 )
        {
            int errnum = errno;
            ostringstream oss;
            oss << "Error gaining r/w access to import path " << loadDir <<
               ": " << strerror(errnum);
            errMsg = oss.str();
            return ERR_FILE_OPEN;
        }

        if (loadDir == IMPORT_PATH_CWD)
        {
            fAlternateImportDir = loadDir;
        }
        else
        {
            if ( loadDir.c_str()[loadDir.size() - 1 ] == '/' )
                fAlternateImportDir = loadDir;
            else
                fAlternateImportDir = loadDir + "/";
        }
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Load a job information
// PARAMETERS:
//    fullName - full filename for job description file
//    bUseTempJobFile - are we using a temporary job XML file
//    systemLang-SystemLang setting used to set locale.
//    argc     - command line arg count
//    argv     - command line arguments
//    bLogInfo2ToConsole - Log info2 msgs to the console
//    bValidateColumnList- Validate that all the columns for each table have
//                         a corresponding <Column> or <DefaultColumn> tag.
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::loadJobInfo(
    const string& fullName,
    bool          bUseTempJobFile,
    const string& systemLang,
    int argc,
    char** argv,
    bool  bLogInfo2ToConsole,
    bool  bValidateColumnList )
{
    fJobFileName = fullName;
    fRootDir     = Config::getBulkRoot();

    if( !exists( fullName.c_str() ) ) {
        fLog.logMsg( " file " + fullName + " does not exist",
                       ERR_FILE_NOT_EXIST, MSGLVL_ERROR );
        return ERR_FILE_NOT_EXIST;
    }

    std::string errMsg;
    int rc = fJobInfo.loadJobXmlFile( fullName, bUseTempJobFile,
                                      bValidateColumnList, errMsg );
    if( rc != NO_ERROR ) {
        std::ostringstream oss;
        oss << "Error loading job file " << fullName << "; " << errMsg;
        fLog.logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    const Job& curJob = fJobInfo.getJob();
    string     logFile, errlogFile;
    logFile = fRootDir + DIR_BULK_LOG + "Job_" +
              Convertor::int2Str( curJob.id ) + LOG_SUFFIX;
    errlogFile = fRootDir + DIR_BULK_LOG + "Job_" +
              Convertor::int2Str( curJob.id ) + ERR_LOG_SUFFIX;
              
    if (disableConsoleOutput())
		fLog.setLogFileName(logFile.c_str(),errlogFile.c_str(),false);
    else
		fLog.setLogFileName(logFile.c_str(),errlogFile.c_str(),(int)bLogInfo2ToConsole);

    std::ostringstream ossLocale;
    ossLocale << "Locale is : " << systemLang;
    
    if (!(disableConsoleOutput()))
    {
		fLog.logMsg( ossLocale.str(), MSGLVL_INFO2 );
		if (!BulkLoad::disableConsoleOutput())
			cout << "Log file for this job: " << logFile << std::endl;
		fLog.logMsg( "successfully loaded job file " + fullName, MSGLVL_INFO1 );
	}
    if (argc > 1)
    {
        std::ostringstream oss;
        oss << "Command line options: ";
        for (int k=1; k<argc; k++)
        {
            if (!strcmp(argv[k],"\t")) // special case to print a <TAB>
                oss << "'\\t'" << " ";
            else
                oss << argv[k] << " ";
        }
        fLog.logMsg( oss.str(), MSGLVL_INFO2 );
    }

    // Validate that each table has 1 or more columns referenced in the xml file
    for(unsigned i=0; i<curJob.jobTableList.size(); i++) 
    {
        if( curJob.jobTableList[i].colList.size() == 0)
        {
            rc = ERR_INVALID_PARAM;
            fLog.logMsg( "No column definitions in job description file for "
                "table " + curJob.jobTableList[i].tblName,
                rc, MSGLVL_ERROR );
            return rc;
        }
    }

    // Validate that the user's xml file has been regenerated since the
    // required tblOid attribute was added to the Table tag for table locking.
    for (unsigned i=0; i<curJob.jobTableList.size(); i++)
    {
        if (curJob.jobTableList[i].mapOid == 0)
        {
            rc = ERR_XML_PARSE;
            fLog.logMsg( "Outdated job file " + fullName +
                          "; missing required 'tblOid' table attribute." +
                          "  Please regenerate this xml file.",
                          rc, MSGLVL_ERROR );
            return rc;
        }
    }

    // Validate that specified compression type is available
    compress::IDBCompressInterface compressor;
    for (unsigned kT=0; kT<curJob.jobTableList.size(); kT++)
    {
        for (unsigned kC=0; kC<curJob.jobTableList[kT].colList.size(); kC++)
        {
            if ( !compressor.isCompressionAvail(
                curJob.jobTableList[kT].colList[kC].compressionType) )
            {
                std::ostringstream oss;
                oss << "Specified compression type (" <<
                    curJob.jobTableList[kT].colList[kC].compressionType <<
                    ") for table " << curJob.jobTableList[kT].tblName  <<
                    " and column " <<
                    curJob.jobTableList[kT].colList[kC].colName <<
                    " is not available for use.";
                rc = ERR_COMP_UNAVAIL_TYPE;
                fLog.logMsg( oss.str(), rc, MSGLVL_ERROR );
                return rc;
            }
        }
    }

    // If binary import, do not allow <IgnoreField> tags in the Job file
    if ((fImportDataMode == IMPORT_DATA_BIN_ACCEPT_NULL) ||
        (fImportDataMode == IMPORT_DATA_BIN_SAT_NULL))
    {
        for (unsigned kT=0; kT<curJob.jobTableList.size(); kT++)
        {
            if (curJob.jobTableList[kT].fIgnoredFields.size() > 0)
            {
                std::ostringstream oss;
                oss << "<IgnoreField> tag present in Job file for table " <<
                    curJob.jobTableList[kT].tblName <<
                    "; this is not allowed for binary imports.";
                rc = ERR_BULK_BINARY_IGNORE_FLD;
                fLog.logMsg( oss.str(), rc, MSGLVL_ERROR );
                return rc;
            }       
        }
    }

    stopTimer();

    std::ostringstream ossXMLTime;
    ossXMLTime << "Job file loaded, run time for this step : " <<
        getTotalRunTime() << " seconds";
    fLog.logMsg( ossXMLTime.str(), MSGLVL_INFO1 );

    return NO_ERROR;
}
   
//------------------------------------------------------------------------------
// DESCRIPTION:
//    Spawns and joins the Read and Parsing threads to import the data.
// PARAMETERS:
//    none
// RETURN:
//    none
//------------------------------------------------------------------------------
void BulkLoad::spawnWorkers()
{
    // We're fixin' to launch threads. This lets anybody who cares (i.e.
    // checkCancellation) know that read and parse threads are running.
    fbContinue = true; 

    // Spawn a thread to check for user cancellation via calpont console
    // But only in mode 3 (local mode)
    boost::thread cancelThread;
    CancellationThread cancelationThread(this);
    if (getBulkLoadMode() == BULK_MODE_LOCAL)
    {
        cancelThread = boost::thread(cancelationThread);
    }

    // Spawn read threads
    for(int i=0; i<fNoOfReadThreads; ++i)
    {
        fReadThreads.create_thread(boost::bind(&BulkLoad::read, this, (int)i));
    }    
    fLog.logMsg("No of Read Threads Spawned = " +
                 Convertor::int2Str( fNoOfReadThreads ), MSGLVL_INFO1 );

    // Spawn parse threads
    for(int i=0; i<fNoOfParseThreads; ++i)
    {
        fParseThreads.create_thread(boost::bind(&BulkLoad::parse,this,(int)i));
    }
    fLog.logMsg("No of Parse Threads Spawned = " +
                 Convertor::int2Str( fNoOfParseThreads ), MSGLVL_INFO1 );
              
    fReadThreads.join_all();
    fParseThreads.join_all();
    fbContinue = false;
    if (getBulkLoadMode() == BULK_MODE_LOCAL)
    {
        cancelThread.join();              
    }
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Pre process job.  Determine DBRoot/segment file, HWM etc where we are
//    to start adding rows, create ColumnInfo object for each column.  Create
//    initial segment file if necessary.  This could happen in shared-nothing
//    where CREATE TABLE only creates the initial segment file on one of the
//    PMs.  The first time rows are added on the other PMs, an initial segment
//    file must be created.  (This could also happen "if" we ever decide to
//    allow the user to drop all partitions for a DBRoot, including the last
//    partition.)
//    PreProcessing also includes creating the bulk rollback back up files,
//    initializing auto-increment, sanity checking the consistency of the HWM
//    across columns, and opening the starting column and dictionary store
//    files.
// PARAMETERS:
//    job - current job
//    tableNo - table no
//    tableInfo - TableInfo object corresponding to tableNo table.
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::preProcess( Job& job, int tableNo,
                          TableInfo* tableInfo )
{
    int         rc=NO_ERROR, minWidth=9999; // give a big number
    HWM         minHWM = 999999;  // rp 9/25/07 Bug 473
    ColStruct   curColStruct;
    CalpontSystemCatalog::ColDataType colDataType;

    // Initialize portions of TableInfo object
    tableInfo->setBufferSize(fBufferSize);
    tableInfo->setFileBufferSize(fFileVbufSize);
    tableInfo->setTableId(tableNo);
    tableInfo->setColDelimiter(fColDelim);
    tableInfo->setJobFileName(fJobFileName);
    tableInfo->setJobId(job.id);
    tableInfo->setNullStringMode(fNullStringMode);
    tableInfo->setEnclosedByChar(fEnclosedByChar);
    tableInfo->setEscapeChar(fEscapeChar);
    tableInfo->setImportDataMode(fImportDataMode);
    tableInfo->setJobUUID(fUUID);

    if (fMaxErrors != -1)
        tableInfo->setMaxErrorRows(fMaxErrors);
    else
        tableInfo->setMaxErrorRows(job.jobTableList[tableNo].maxErrNum);

    // @bug 3929: cpimport.bin error messaging using up too much memory.
    // Validate that max allowed error count is within valid range
    long long maxErrNum = tableInfo->getMaxErrorRows();
    if (maxErrNum > MAX_ALLOW_ERROR_COUNT)
    {
        ostringstream oss;
        oss << "Max allowed error count specified as " << maxErrNum <<
            " for table " << job.jobTableList[tableNo].tblName <<
            "; this exceeds limit of " << MAX_ALLOW_ERROR_COUNT <<
            "; resetting to " << MAX_ALLOW_ERROR_COUNT;
        fLog.logMsg( oss.str(), MSGLVL_INFO2 );
        maxErrNum = MAX_ALLOW_ERROR_COUNT;
    }
    tableInfo->setMaxErrorRows(maxErrNum);

    //------------------------------------------------------------------------
    // First loop thru the columns for the "tableNo" table in jobTableList[].
    // Get the HWM information for each column.
    //------------------------------------------------------------------------
    std::vector<int>                    colWidths;
    std::vector<DBRootExtentInfo>       segFileInfo;
    std::vector<DBRootExtentTracker*>   dbRootExtTrackerVec;
    std::vector<BRM::EmDbRootHWMInfo_v> dbRootHWMInfoColVec(
        job.jobTableList[tableNo].colList.size() );
    DBRootExtentTracker* pRefDBRootExtentTracker = 0;
    bool bNoStartExtentOnThisPM = false;
    bool bEmptyPM               = false;
    for( size_t i = 0; i < job.jobTableList[tableNo].colList.size(); i++ ) 
    {
        const JobColumn& curJobCol = job.jobTableList[tableNo].colList[i];

        // convert column data type
        if( curJobCol.typeName.length() >0 &&
            fColOp->getColDataType( curJobCol.typeName.c_str(), colDataType ))
        {
            job.jobTableList[tableNo].colList[i].dataType =
               curColStruct.colDataType = colDataType;
        }
        else
        {
            ostringstream oss;
            oss << "Column type " << curJobCol.typeName << " is not valid ";
            fLog.logMsg( oss.str(), ERR_INVALID_PARAM, MSGLVL_ERROR );
            return ERR_INVALID_PARAM;
        }
        curColStruct.colWidth = curJobCol.width;
        Convertor::convertColType( &curColStruct );

        job.jobTableList[tableNo].colList[i].weType   = curColStruct.colType;
        // set width to correct column width
        job.jobTableList[tableNo].colList[i].width    = curColStruct.colWidth;
        job.jobTableList[tableNo].colList[i].emptyVal = getEmptyRowValue(
            job.jobTableList[tableNo].colList[i].dataType,
            job.jobTableList[tableNo].colList[i].width );

        // check HWM for column file
        rc = BRMWrapper::getInstance()->getDbRootHWMInfo( curJobCol.mapOid,
            dbRootHWMInfoColVec[i]);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            ostringstream oss;
            oss << "Error getting last DBRoot/HWMs for column file " <<
                curJobCol.mapOid << "; " << ec.errorString(rc);
            fLog.logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }

        colWidths.push_back( job.jobTableList[tableNo].colList[i].width );
    } // end of 1st for-loop through the list of columns (get starting HWM)

    //--------------------------------------------------------------------------
    // Second loop thru the columns for the "tableNo" table in jobTableList[].
    // Create DBRootExtentTracker, and select starting DBRoot.
    // Determine the smallest width column(s), and save that as minHWM.
    // We save additional HWM information acquired from BRM, in segFileInfo,
    // for later use.
    //--------------------------------------------------------------------------
    for( size_t i = 0; i < job.jobTableList[tableNo].colList.size(); i++ )
    {
        const JobColumn& curJobCol = job.jobTableList[tableNo].colList[i];

        // Find DBRoot/segment file where we want to start adding rows
        DBRootExtentTracker* pDBRootExtentTracker = new DBRootExtentTracker(
            curJobCol.mapOid,
            colWidths,
            dbRootHWMInfoColVec,
            i,
            &fLog );
        if (i == 0)
            pRefDBRootExtentTracker = pDBRootExtentTracker;
        dbRootExtTrackerVec.push_back( pDBRootExtentTracker );

        // Start adding rows to DBRoot/segment file that is selected
        DBRootExtentInfo dbRootExtent;
        if (i == 0) // select starting DBRoot/segment for column[0]
        {
            std::string trkErrMsg;
            rc = pDBRootExtentTracker->selectFirstSegFile(dbRootExtent,
                bNoStartExtentOnThisPM, bEmptyPM, trkErrMsg);
            if (rc != NO_ERROR)
            {
                fLog.logMsg( trkErrMsg, rc, MSGLVL_ERROR );
                return rc;
            }
        }
        else // select starting DBRoot/segment based on column[0] selection
        {    // to ensure all columns start with the same DBRoot/segment
            pDBRootExtentTracker->assignFirstSegFile(
                *pRefDBRootExtentTracker, // reference column[0] tracker
                dbRootExtent);
        }

        if( job.jobTableList[tableNo].colList[i].width < minWidth )
        {
            // save the minimum hwm     --  rp 9/25/07 Bug 473
            minWidth = job.jobTableList[tableNo].colList[i].width;
            minHWM   = dbRootExtent.fLocalHwm;
        }

        // Save column segment file info for use in subsequent loop
        segFileInfo.push_back( dbRootExtent );
    }

    //--------------------------------------------------------------------------
    // Validate that the starting HWMs for all the columns are in sync
    //--------------------------------------------------------------------------
    rc = tableInfo->validateColumnHWMs( &job.jobTableList[tableNo],
                                        segFileInfo, "Starting" );
    if (rc != NO_ERROR)
    {
         return rc;
    }

    //--------------------------------------------------------------------------
    // Create bulk rollback meta data file
    //--------------------------------------------------------------------------
    ostringstream oss11;
    oss11 << "Initializing import: " <<
        "Table-" << job.jobTableList[tableNo].tblName << "...";
    fLog.logMsg( oss11.str(), MSGLVL_INFO2 );

    rc = saveBulkRollbackMetaData( job, tableInfo, segFileInfo,
                                   dbRootHWMInfoColVec );
    if (rc != NO_ERROR)
    {
         return rc;
    }

    //--------------------------------------------------------------------------
    // Third loop thru the columns for the "tableNo" table in jobTableList[].
    // In this pass through the columns we create the ColumnInfo object,
    // open the applicable column and dictionary store files, and seek to
    // the block where we will begin adding data.
    //--------------------------------------------------------------------------
    unsigned int fixedBinaryRecLen = 0;
    for( size_t i = 0; i < job.jobTableList[tableNo].colList.size(); i++ ) 
    {
        uint16_t dbRoot    = segFileInfo[i].fDbRoot;
        uint32_t partition = segFileInfo[i].fPartition;
        uint16_t segment   = segFileInfo[i].fSegment;
        HWM       oldHwm    = segFileInfo[i].fLocalHwm;

        DBRootExtentTracker* pDBRootExtentTracker = 0;
        if (dbRootExtTrackerVec.size() > 0)
            pDBRootExtentTracker = dbRootExtTrackerVec[i];

        // Create a ColumnInfo for the next column, and add to tableInfo
        ColumnInfo* info = 0;
        if (job.jobTableList[tableNo].colList[i].compressionType)
            info = new ColumnInfoCompressed(&fLog, i,
                job.jobTableList[tableNo].colList[i],
                pDBRootExtentTracker,
                tableInfo);
                //tableInfo->rbMetaWriter());
        else
            info = new ColumnInfo(&fLog, i,
                job.jobTableList[tableNo].colList[i],
                pDBRootExtentTracker,
                tableInfo);

        // For auto increment column, we need to get the starting value
        if (info->column.autoIncFlag)
        {
            rc = preProcessAutoInc( job.jobTableList[tableNo].tblName, info );
            if (rc != NO_ERROR)
            {
               return rc;
            }
        }

        // For binary input mode, sum up the columns widths to get fixed rec len
        if ((fImportDataMode == IMPORT_DATA_BIN_ACCEPT_NULL) ||
            (fImportDataMode == IMPORT_DATA_BIN_SAT_NULL))
        { 
            if (job.jobTableList[tableNo].fFldRefs[i].fFldColType ==
                BULK_FLDCOL_COLUMN_FIELD)
            {
                fixedBinaryRecLen += info->column.definedWidth;
            }
        }

        // Skip minimum blocks before starting import; minwidth columns skip to
        // next block.  Wider columns skip based on multiple of width. If this
        // skipping of blocks requires a new extent, then we extend the column.
        HWM hwm = (minHWM +1) * ( info->column.width/minWidth );
        info->relativeColWidthFactor( info->column.width/minWidth );

        if ((bEmptyPM) || (bNoStartExtentOnThisPM))
        {
            // HWM not found in prev loop; can't get LBID.  Will create initial
            // extent on this PM later in this job, if we have valid rows to add
            if (bEmptyPM)
            {
                // No starting DB file on this PM
                ostringstream oss3;
                oss3 << "Currently no extents on dbroot" << dbRoot <<
                    " for column OID " << info->column.mapOid <<
                    "; will create starting extent";
                fLog.logMsg( oss3.str(), MSGLVL_INFO2 );
            }
            // Skip to subsequent physical partition if current HWM extent
            // for this "dbroot" is disabled.
            else    // bNoStartExtentOnThisPM is true
            {
                // Starting DB file on this PM is disabled
                ostringstream oss3;
                oss3 << "Current HWM extent is disabled on dbroot" << dbRoot <<
                    " for column OID " << info->column.mapOid <<
                    "; will create starting extent";
                fLog.logMsg( oss3.str(), MSGLVL_INFO2 );
            }

            // Pass blocks to be skipped at start of file "if" we decide to
            // employ block skipping for the first extent.
            hwm = info->column.width/minWidth;

            // We don't have a starting DB file on this PM, or the starting HWM
            // extent is disabled.  In either case, we will wait and create a
            // new DB file to receive any new rows, only after we make sure we
            // have rows to insert.
            info->setupDelayedFileCreation(
                dbRoot, partition, segment, hwm, bEmptyPM );
        }
        else
        {
            // Establish starting HWM and LBID for this job.
            // Keep in mind we have initial block skipping to account for.
            bool bSkippedToNewExtent = false;
            BRM::LBID_t lbid;

            RETURN_ON_ERROR( preProcessHwmLbid( info,
                minWidth, partition, segment,
                hwm, lbid, bSkippedToNewExtent ) );

            // Setup import to start loading into starting HWM DB file
            RETURN_ON_ERROR( info->setupInitialColumnExtent(
                dbRoot, partition, segment,
                job.jobTableList[tableNo].tblName,
                lbid,
                oldHwm, hwm,
                bSkippedToNewExtent, false) );   
        }

        tableInfo->addColumn(info);

    } // end of 2nd for-loop through the list of columns

    if ((fImportDataMode == IMPORT_DATA_BIN_ACCEPT_NULL) ||
        (fImportDataMode == IMPORT_DATA_BIN_SAT_NULL))
    {
        ostringstream oss12;
        oss12 << "Table " << job.jobTableList[tableNo].tblName << " will be "
            "imported in binary mode with fixed record length: " <<
            fixedBinaryRecLen << " bytes; ";
        if (fImportDataMode == IMPORT_DATA_BIN_ACCEPT_NULL)
            oss12 << "NULL values accepted";
        else
            oss12 << "NULL values saturated";
        fLog.logMsg( oss12.str(), MSGLVL_INFO2 );
    }

    // Initialize BulkLoadBuffers after we have added all the columns
    tableInfo->initializeBuffers(fNoOfBuffers, 
                                 job.jobTableList[tableNo].fFldRefs,
                                 fixedBinaryRecLen);

    fTableInfo.push_back(tableInfo);

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Saves snapshot of extentmap into a bulk rollback meta data file, for
//    use in a bulk rollback, if the current cpimport.bin job should fail.
// PARAMETERS:
//    job - current job
//    tableInfo - TableInfo object corresponding to tableNo table.
//    segFileInfo - Vector of File objects carrying starting DBRoot, partition,
//                  etc, for the columns belonging to tableNo.
//    dbRootHWMInfoColVec - Vector of vectors carrying extent/HWM info for each
//                          dbroot for each column.
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::saveBulkRollbackMetaData( Job& job,
    TableInfo* tableInfo,
    const std::vector<DBRootExtentInfo>& segFileInfo,
    const std::vector<BRM::EmDbRootHWMInfo_v>& dbRootHWMInfoColVec)
{
    return tableInfo->saveBulkRollbackMetaData(
        job, segFileInfo, dbRootHWMInfoColVec );
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Initialize auto-increment column for specified schema and table.
// PARAMETERS:
//    fullTableName - Schema and table name separated by a period.
//    colInfo    - ColumnInfo associated with auto-increment column.
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::preProcessAutoInc( const std::string& fullTableName, 
    ColumnInfo* colInfo)
{
    int rc = colInfo->initAutoInc( fullTableName );

    return rc;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Determine starting HWM and LBID, after applying block skipping to HWM.
// PARAMETERS:
//    info               - ColumnInfo of column we are working with
//    minWidth           - minimum width among all columns for this table
//    partition          - partition of projected starting HWM
//    segment            - file segment number of projected starting HWM
//    hwm (input/output) - input:  projected starting HWM after block skipping
//                         output: adjusted starting HWM 
//    lbid                 output: LBID associated with adjusted HWM
//    bSkippedToNewExtent- output:
//                         true -> normal block skipping use case
//                         false-> block skipped crossed out of hwm extent
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::preProcessHwmLbid(
    const ColumnInfo* info,
    int          minWidth,
    uint32_t    partition,
    uint16_t    segment,
    HWM&         hwm,                  // input/output
    BRM::LBID_t& lbid,                 // output
    bool&        bSkippedToNewExtent)  // output
{
    int rc = NO_ERROR;
    bSkippedToNewExtent = false;

    // Get starting LBID for the HWM block; if we can't get the start-
    // ing LBID, it means initial block skipping crossed extent boundary
    rc = BRMWrapper::getInstance()->getStartLbid(
        info->column.mapOid,
        partition,
        segment,
        (int)hwm, lbid);

    // If HWM Lbid is missing, take alternative action to see what to do.
    // Block skipping has caused us to advance out of the current HWM extent.
    if (rc != NO_ERROR)
    {
        bSkippedToNewExtent = true;

        lbid = INVALID_LBID;

        int blocksPerExtent =
            (BRMWrapper::getInstance()->getExtentRows() *
            info->column.width) / BYTE_PER_BLOCK;

        // Look for LBID associated with block at end of current extent
        uint32_t numBlocks = (((hwm+1) / blocksPerExtent) * blocksPerExtent);

        hwm = numBlocks - 1;
        rc = BRMWrapper::getInstance()->getStartLbid(
            info->column.mapOid,
            partition,
            segment,
            (int)hwm, lbid);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            ostringstream oss;
            oss << "Error getting HWM start LBID "
                "for previous last extent in column file OID-" <<
                info->column.mapOid <<
                "; partition-" << partition <<
                "; segment-"   << segment   <<
                "; hwm-"       << hwm       <<
                "; " << ec.errorString(rc);
            fLog.logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }
    }

    return rc;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::processJob( )
{
#ifdef PROFILE
    Stats::enableProfiling( fNoOfReadThreads, fNoOfParseThreads );
#endif
    int         rc = NO_ERROR;
    Job         curJob;
    size_t      i;
      
    curJob = fJobInfo.getJob();

    // For the following parms, we use the value read from the Job XML file if
    // a cmd line override value was not already assigned by cpimport.cpp.
    if (fNoOfBuffers  == -1)
        fNoOfBuffers   = curJob.numberOfReadBuffers;
    if (fBufferSize   == -1)
        fBufferSize    = curJob.readBufferSize;
    if (fFileVbufSize == -1)
        fFileVbufSize  = curJob.writeBufferSize;
    if (fColDelim     == '\0')
        fColDelim      = curJob.fDelimiter;

    //std::cout << "bulkload::fEnclosedByChar<" << fEnclosedByChar << '>' <<
    //std::endl << "bulkload::fEscapeChar<" << fEscapeChar << '>' <<
    //std::endl << "job.fEnclosedByChar<" <<curJob.fEnclosedByChar<< '>' <<
    //std::endl << "job.fEscapeChar<" << curJob.fEscapeChar << '>' <<
    //std::endl;
    if (fEnclosedByChar == '\0')
    {
        // std::cout << "Using enclosed char from xml file" << std::endl;
        fEnclosedByChar = curJob.fEnclosedByChar;
    }
    if (fEscapeChar == '\0')
    {
        // std::cout << "Using escape char from xml file" << std::endl;
        fEscapeChar = curJob.fEscapeChar;
    }
    // If EnclosedBy char is given, then we need an escape character.
    // We default to '\' if we didn't get one from xml file or cmd line.
    if (fEscapeChar == '\0')
    {
        //std::cout << "Using default escape char" << std::endl;
        fEscapeChar = '\\';
    }
    //std::cout << "bulkload::fEnclosedByChar<" << fEnclosedByChar << '>' <<
    //std::endl << "bulkload::fEscapeChar<" << fEscapeChar << '>' << std::endl;

    //Bug1315 - check whether DBRoots are RW mounted.
    std::vector<std::string> dbRootPathList;
    Config::getDBRootPathList( dbRootPathList );
    for (unsigned int counter=0; counter<dbRootPathList.size(); counter++)
    {
        if ( access( dbRootPathList[counter].c_str(),R_OK|W_OK) < 0 )
        {
            rc = ERR_FILE_NOT_EXIST;
            ostringstream oss;
            oss << "Error accessing DBRoot[" << counter << "] " <<
                dbRootPathList[counter] << "; " <<
                strerror(errno);
            fLog.logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }
    }     

    // Init total cumulative run time with time it took to load xml file
    double totalRunTime = getTotalRunTime();
    fLog.logMsg( "PreProcessing check starts", MSGLVL_INFO1 ); 
    startTimer();

    //--------------------------------------------------------------------------
    // Validate that only 1 table is specified for import if using STDIN
    //--------------------------------------------------------------------------
    if ((fAlternateImportDir == IMPORT_PATH_STDIN) &&
        (curJob.jobTableList.size() > 1))
    {
        rc = ERR_INVALID_PARAM;
        fLog.logMsg("Only 1 table can be imported per job when using STDIN",
            rc, MSGLVL_ERROR );
        return rc;
    }

    //--------------------------------------------------------------------------
    // Validate the existence of the import data files
    //--------------------------------------------------------------------------
    std::vector<TableInfo*> tables;
    for( i = 0; i < curJob.jobTableList.size(); i++ )
    {
        TableInfo *tableInfo = new TableInfo(&fLog,
                                             fTxnID,
                                             fProcessName,
                                             curJob.jobTableList[i].mapOid,
                                             curJob.jobTableList[i].tblName,
                                             fKeepRbMetaFiles);
        if ((fBulkMode == BULK_MODE_REMOTE_SINGLE_SRC) ||
            (fBulkMode == BULK_MODE_REMOTE_MULTIPLE_SRC))
            tableInfo->setBulkLoadMode( fBulkMode, fBRMRptFileName );

        tableInfo->setErrorDir(getErrorDir());
        tableInfo->setTruncationAsError(getTruncationAsError());
        rc = manageImportDataFileList( curJob, i, tableInfo );
        if (rc != NO_ERROR)
		{
			tableInfo->fBRMReporter.sendErrMsgToFile(tableInfo->fBRMRptFileName);
            return rc;     
		}

        tables.push_back( tableInfo );
    }

    //--------------------------------------------------------------------------
    // Before we go any further, we lock all the tables
    //--------------------------------------------------------------------------
    for( i = 0; i < curJob.jobTableList.size(); i++ )
    {
        rc = tables[i]->acquireTableLock( fDisableTimeOut );
        if (rc != NO_ERROR)
        {
            // Try releasing the table locks we already acquired.
            // Note that loop is k<i since tables[i] lock failed.
            for ( unsigned k=0; k<i; k++)
            {
                tables[k]->releaseTableLock( );//ignore return code in this case
            }

            return rc;
        }

        // If we have a lock, then init MetaWriter, so that it can delete any
        // leftover backup meta data files that collide with the ones we are
        // going to create.
        rc = tables[i]->initBulkRollbackMetaData( );
        if (rc != NO_ERROR)
        {
            // Try releasing the table locks we already acquired.
            // Note that loop is k<i= since tables[i] lock worked
            for (unsigned k=0; k<=i; k++)
            {
                tables[k]->releaseTableLock( );//ignore return code in this case
            }

            return rc;
        }
    }

    //--------------------------------------------------------------------------
    // Perform necessary preprocessing for each table
    //--------------------------------------------------------------------------
    for( i = 0; i < curJob.jobTableList.size(); i++ ) 
    {
        // If table already marked as complete then we are skipping the
        // table because there were no input files to process.
        if( tables[i]->getStatusTI() == WriteEngine::PARSE_COMPLETE)
            continue;

        rc = preProcess( curJob, i, tables[i] );

        if( rc != NO_ERROR ) {
        	std::string errMsg =
                "Error in pre-processing the job file for table " +
                curJob.jobTableList[i].tblName;
        	tables[i]->fBRMReporter.addToErrMsgEntry(errMsg);
            fLog.logMsg( errMsg, rc, MSGLVL_CRITICAL );

            // Okay to release the locks for the tables we did not get to
            for ( unsigned k=i+1; k<tables.size(); k++)
            {
                tables[k]->releaseTableLock( );//ignore return code in this case
            }

            // Okay to release the locks for any tables we preprocessed.
            // We will not have done anything to change these tables yet,
            // so all we need to do is release the locks.
            for ( unsigned k=0; k<=i; k++)
            {
                tables[k]->deleteMetaDataRollbackFile( );
                tables[k]->releaseTableLock( ); //ignore return code
            }

            // Ignore the return code for now; more important to base rc on the
            // success or failure of the previous work

            // BUG 4398: distributed cpimport calls takeSnapshot for modes 1 & 2
            if ((fBulkMode != BULK_MODE_REMOTE_SINGLE_SRC) &&
                (fBulkMode != BULK_MODE_REMOTE_MULTIPLE_SRC))
            {
                BRMWrapper::getInstance()->takeSnapshot();
            }

            return rc;
        }
    }
      
    stopTimer();
    fLog.logMsg( "PreProcessing check completed", MSGLVL_INFO1 );

    std::ostringstream ossPrepTime;
    ossPrepTime << "preProcess completed, run time for this step : " <<
        getTotalRunTime() << " seconds";
    fLog.logMsg( ossPrepTime.str(), MSGLVL_INFO1 );
    totalRunTime += getTotalRunTime();

    startTimer();

    spawnWorkers();
    if (BulkStatus::getJobStatus() == EXIT_FAILURE)
    {
        rc = ERR_UNKNOWN;
    }

    // Regardless of JobStatus, we rollback any tables that are left locked
    int rollback_rc = rollbackLockedTables( );
    if ((rc == NO_ERROR) && (rollback_rc != NO_ERROR))
    {
        rc = rollback_rc;
    }

    // Ignore the return code for now; more important to base rc on the
    // success or failure of the previous work

	// BUG 4398: distributed cpimport now calls takeSnapshot for modes 1 & 2
    if ((fBulkMode != BULK_MODE_REMOTE_SINGLE_SRC) &&
        (fBulkMode != BULK_MODE_REMOTE_MULTIPLE_SRC))
    {
        BRMWrapper::getInstance()->takeSnapshot();
    }

    stopTimer();
    totalRunTime += getTotalRunTime();

    std::ostringstream ossTotalRunTime;
    ossTotalRunTime << "Bulk load completed, total run time : " <<
        totalRunTime << " seconds" << std::endl;
    fLog.logMsg( ossTotalRunTime.str(), MSGLVL_INFO1 );

#ifdef PROFILE
    Stats::printProfilingResults( );
#endif

    return rc;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Deconstruct the list of 1 or more import files for the specified table,
//    and validate the existence of the specified files.
// PARAMETERS:
//    job - current job
//    tableNo - table no
//    tableInfo - TableInfo object corresponding to tableNo table.
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::manageImportDataFileList(Job& job,
                                       int tableNo,
                                       TableInfo* tableInfo)
{
    std::vector<std::string> loadFilesList;
    bool bUseStdin = false;

    // Take loadFileName from command line argument override "if" one exists,
    // else we take from the Job xml file
    std::string loadFileName;
    if (fCmdLineImportFiles.size() > (unsigned)tableNo)
        loadFileName = fCmdLineImportFiles[tableNo];
    else
        loadFileName = job.jobTableList[tableNo].loadFileName;

    if (fAlternateImportDir == IMPORT_PATH_STDIN)
    {
        bUseStdin = true;
        fLog.logMsg( "Using STDIN for input data", MSGLVL_INFO2 );

		int rc = buildImportDataFileList(std::string(),
            loadFileName,
            loadFilesList);
        if (rc != NO_ERROR)
        {
            return rc;
        }
		// BUG 4737 - in Mode 1, all data coming from STDIN, ignore input files
        if ((loadFilesList.size() > 1) && (fBulkMode != BULK_MODE_REMOTE_SINGLE_SRC))
        {
            ostringstream oss;
            oss << "Table " << tableInfo->getTableName() <<
                " specifies multiple "
                "load files; This is not allowed when using STDIN";
            fLog.logMsg( oss.str(), ERR_INVALID_PARAM, MSGLVL_ERROR );
			tableInfo->fBRMReporter.addToErrMsgEntry(oss.str());
            return ERR_INVALID_PARAM;
        }
    }
    else
    {
        std::string importDir;
        if ( fAlternateImportDir == IMPORT_PATH_CWD )  // current working dir
        {
            char cwdBuf[4096];
            importDir  = ::getcwd(cwdBuf,sizeof(cwdBuf));
            importDir += '/';
        }
        else if ( fAlternateImportDir.size() > 0 )     // -f path
        {
            importDir = fAlternateImportDir;
        }
        else                                           // <BULKROOT>/data/import
        {
            importDir  = fRootDir;
            importDir += DIR_BULK_IMPORT;
        }

        // Break down loadFileName into vector of file names in case load-
        // FileName contains a list of files or 1 or more wildcards.
        int rc = buildImportDataFileList(importDir,
            loadFileName,
            loadFilesList);
        if (rc != NO_ERROR)
        {
            return rc;
        }

        // No filenames is considered a fatal error, except for remote mode2.
        // For remote mode2 we just mark the table as complete since we will
        // have no data to load, but we don't consider this as an error.
        if (loadFilesList.size() == 0)
        {
            ostringstream oss;
            oss << "No import files found.   " << "default dir: " << importDir<<
                "   importFileName: " << loadFileName;
            if (fBulkMode == BULK_MODE_REMOTE_MULTIPLE_SRC)
            {
                tableInfo->setLoadFilesInput(bUseStdin, loadFilesList);
                tableInfo->markTableComplete( );
                fLog.logMsg( oss.str(), MSGLVL_INFO1 );
                return NO_ERROR;
            }
            else
            {
                fLog.logMsg( oss.str(), ERR_FILE_NOT_EXIST, MSGLVL_ERROR );
                return ERR_FILE_NOT_EXIST;
            }
        }
      
        // Verify that input data files exist.
        // We also used to check to make sure the input file is not empty, and
        // if it were, we threw an error at this point, but we removed that
        // check.  With shared-nothing, an empty file is now acceptable.
        for (unsigned ndx=0; ndx<loadFilesList.size(); ndx++ ) 
        {
			// in addition to being more portable due to the use of boost, this change
			// actually fixes an inherent bug with cpimport reading from a named pipe.
			// Only the first open call gets any data passed through the pipe so the
			// here that used to do an open to test for existence meant cpimport would 
			// never get data from the pipe. 
        	boost::filesystem::path pathFile(loadFilesList[ndx]);
        	if ( !boost::filesystem::exists( pathFile ) )
        	{
                ostringstream oss;
                oss << "input data file " << loadFilesList[ndx] << " does not exist";
                fLog.logMsg( oss.str(), ERR_FILE_NOT_EXIST, MSGLVL_ERROR );
				tableInfo->fBRMReporter.addToErrMsgEntry(oss.str());
                return ERR_FILE_NOT_EXIST;
        	}
			else
			{
                ostringstream oss;
                oss << "input data file " << loadFilesList[ndx];
                fLog.logMsg( oss.str(), MSGLVL_INFO1 );
			}
        }
    }

    tableInfo->setLoadFilesInput(bUseStdin, loadFilesList);

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Break up the filename string (which may contain a list of file names)
//    into a vector of strings, with each non-fully-qualified string being
//    prefixed by the path specified by "location".
// PARAMETERS:
//    location - path prefix
//    filename - list of file names
//    loadFiles- vector of file names extracted from filename string
// RETURN:
//    NO_ERROR if success
//------------------------------------------------------------------------------
int BulkLoad::buildImportDataFileList(
    const std::string& location,
    const std::string& filename,
    std::vector<std::string>& loadFiles)
{
    char *filenames = new char[filename.size() + 1];
    strcpy(filenames,filename.c_str());

    char *str;
    char *token;
    for (str=filenames; ; str=NULL)
    {
#ifdef _MSC_VER
    //On Windows, only comma and vertbar can separate input files
        token = strtok(str,",|");
#else
        token = strtok(str,", |");
#endif
        if (token == NULL)
            break;

        // If the token (filename) is fully qualified, then use the filename
        // as-is, else prepend the location (path prefix)
        boost::filesystem::path p(token);
        std::string fullPath;
        if (p.has_root_path())
        {
            fullPath = token;
        }
        else
        {
            fullPath  = location;
            fullPath += token;
        }

#ifdef _MSC_VER
        loadFiles.push_back(fullPath);
#else
        // If running mode2, then support a filename with wildcards
        if (fBulkMode == BULK_MODE_REMOTE_MULTIPLE_SRC)
        {
            bool bExpandFileName = false;

            size_t fpos = fullPath.find_first_of( "[*?" );
            if (fpos != std::string::npos)
            {
                bExpandFileName = true;
            }
            else // expand a directory name
            {
                struct stat curStat;
                if ( (stat(fullPath.c_str(),&curStat) == 0) &&
                     (S_ISDIR(curStat.st_mode)) )
                {
                    bExpandFileName = true;
                    fullPath += "/*";
                }
            }

            // If wildcard(s) present use glob() function to expand into a list
            if (bExpandFileName)
            {
                glob_t globBuf;
                memset(&globBuf, 0, sizeof(globBuf));
                int globFlags = GLOB_ERR | GLOB_MARK;
                int rc        = glob(fullPath.c_str(), globFlags, 0, &globBuf);
                if (rc != 0)
                {
                    if (rc == GLOB_NOMATCH)
                    {
                        continue;
                    }
                    else
                    {
                        ostringstream oss;
                        oss << "Error expanding filename " << fullPath;
                        if (rc == GLOB_NOSPACE)
                            oss << "; out of memory";
                        else if (rc == GLOB_ABORTED)
                            oss << "; error reading directory";
                        else if (rc == GLOB_NOSYS)
                            oss << "; globbing not implemented";
                        else
                            oss << "; rc-" << rc;
                        fLog.logMsg(oss.str(), ERR_FILE_GLOBBING, MSGLVL_ERROR);

                        delete [] filenames;
                        return ERR_FILE_GLOBBING;
                    }
                }

                // Include all non-directory files in the import file list
                std::string fullPath2;
                for (unsigned int k=0; k<globBuf.gl_pathc; k++)
                {
                    fullPath2 = globBuf.gl_pathv[k];
                    if ( !fullPath2.empty() )
                    {
                        if ( fullPath2[ fullPath2.length()-1 ] != '/' )
                        {
                            loadFiles.push_back( fullPath2 );
                        }
                    }
                }
            } // wild card present
            else
            {
                loadFiles.push_back(fullPath);
            }
        }     // mode2
        else
        {
            loadFiles.push_back(fullPath);
        }     // not mode2
#endif
    }         // loop through filename tokens

    delete [] filenames;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Clear table locks, and rollback any tables that are
//    still locked through session manager.
// PARAMETERS:
//    none
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::rollbackLockedTables( )
{
    int rc = NO_ERROR;

    // See if there are any DB tables that were left in a locked state
    bool lockedTableFound = false;
    for( unsigned i = 0; i < fTableInfo.size(); i++ )
    {
        if (fTableInfo[i].isTableLocked())
        {
            lockedTableFound = true;
            break;
        }
    }

    // If 1 or more tables failed to load, then report the lock
    // state of each table we were importing.
    if (lockedTableFound)
    {
        // Report the tables that were successfully loaded
        for( unsigned i = 0; i < fTableInfo.size(); i++ )
        {
            if (!fTableInfo[i].isTableLocked())
            {
                ostringstream oss;
                oss << "Table " << fTableInfo[i].getTableName() <<
                    " was successfully loaded. ";
                fLog.logMsg( oss.str(), MSGLVL_INFO1 );
            }
        }

        // Report the tables that were not successfully loaded
        for( unsigned i = 0; i < fTableInfo.size(); i++ )
        {
            if (fTableInfo[i].isTableLocked())
            {
                if (fTableInfo[i].hasProcessingBegun())
                {
                    ostringstream oss;
                    oss << "Table " << fTableInfo[i].getTableName() <<
                        " (OID-" << fTableInfo[i].getTableOID() << ")" <<
                        " was not successfully loaded.  Rolling back.";
                    fLog.logMsg( oss.str(), MSGLVL_INFO1 );
                }
                else
                {
                    ostringstream oss;
                    oss << "Table " << fTableInfo[i].getTableName() <<
                        " (OID-" << fTableInfo[i].getTableOID() << ")" <<
                        " did not start loading.  No rollback necessary.";
                    fLog.logMsg( oss.str(), MSGLVL_INFO1 );
                }

                rc = rollbackLockedTable( fTableInfo[i] );
                if (rc != NO_ERROR)
                {
                    break;
                }
            }
        }
    }

    return rc;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Clear table lock, and rollback the specified table that is still locked.
//    This function only comes into play for a mode3, since the tablelock and
//    bulk rollbacks are managed by the parent (cpipmort file splitter) process
//    in the case of mode1 and mode2 bulk loads.
// PARAMETERS:
//    tableInfo - the table to be released and rolled back
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::rollbackLockedTable( TableInfo& tableInfo )
{
    return tableInfo.rollbackWork( );
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Update next autoincrement value for specified column OID.
// PARAMETERS:
//    columnOid      - column OID of interest
//    nextAutoIncVal - next autoincrement value to assign to tableOID
// RETURN:
//    0 if success
//    other if fail
//------------------------------------------------------------------------------
/* static */
int BulkLoad::updateNextValue(OID columnOid, uint64_t nextAutoIncVal)
{
    // The odds of us ever having 2 updateNextValue() calls going on in parallel
    // are slim and none.  But it's theoretically possible if we had an import
    // job for 2 tables; so we put a mutex here just in case the DDLClient code
    // won't work well with 2 competing WE_DDLCommandClient objects in the same
    // process (ex: if there is any static data in WE_DDLCommandClient).
    boost::mutex::scoped_lock lock( *fDDLMutex );
    WE_DDLCommandClient ddlCommandClt;
    unsigned int rc = ddlCommandClt.UpdateSyscolumnNextval(
        columnOid, nextAutoIncVal );

    return (int)rc;
}

//------------------------------------------------------------------------------

bool BulkLoad::addErrorMsg2BrmUpdater(const std::string& tablename, const ostringstream& oss)
{
	int size = fTableInfo.size();
	if(size == 0) return false;
	
	for(int tableId = 0; tableId < size; tableId++)
	{
		if(fTableInfo[tableId].getTableName() == tablename)
		{
			fTableInfo[tableId].fBRMReporter.addToErrMsgEntry(oss.str());	
			return true;
		}
	}
	return false;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Set job UUID. Used by Query Telemetry to identify a unique import
//    job across PMs
// PARAMETERS:
//    jobUUID - the job UUID
// RETURN:
//    void
//------------------------------------------------------------------------------
void BulkLoad::setJobUUID(const std::string& jobUUID)
{
	fUUID = boost::uuids::string_generator()(jobUUID);
}

void BulkLoad::setDefaultJobUUID()
{
	if (fUUID.is_nil())
		fUUID = boost::uuids::random_generator()();
}

} //end of namespace

