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
* $Id: we_bulkload.cpp 3792 2012-04-25 19:28:25Z dcathey $
*
*******************************************************************************/
/** @file */

#include <cmath>
#include <cstdlib>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <vector>
#include <sstream>
#include <boost/filesystem/path.hpp>
#include "we_bulkload.h"
#include "we_bulkstatus.h"
#include "we_rbmetawriter.h"
#include "we_bulkrollbackmgr.h"
#include "we_colopbulk.h"
#include "we_columninfocompressed.h"
#include "we_config.h"
#include "writeengine.h"
#include "sys/time.h"
#include "sys/types.h"
#include "dataconvert.h"
#include "idbcompress.h"
#include "calpontsystemcatalog.h"
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
    /* static */ WriteEngineWrapper* BulkLoad::fWEWrapper = 0;
    /* static */ boost::mutex*       BulkLoad::fWEWrapperMutex = 0;

    /* static */ const std::string   BulkLoad::DIR_BULK_JOB("job");
    /* static */ const std::string   BulkLoad::DIR_BULK_TEMP_JOB("tmpjob");
    /* static */ const std::string   BulkLoad::DIR_BULK_IMPORT("/data/import/");
    /* static */ const std::string   BulkLoad::DIR_BULK_LOG("/log/");

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
BulkLoad::BulkLoad() :
    m_colOp(new ColumnOpBulk()),
    m_colDelim('\0'),
    fNoOfBuffers(4), fBufferSize(81920), fFileBufferSize(10240), 
    fNoOfParseThreads(3), fNoOfReadThreads(1),
    fKeepRbMetaFiles(false),
    fNullStringMode(false),
    fEnclosedByChar('\0'), // not enabled unless user overrides enclosed by char
    fEscapeChar('\0'),
    fTotalTime(0.0)
{
    fTableInfo.clear();
    setDebugLevel( DEBUG_0 );

    fWEWrapper      = new WriteEngineWrapper();
    fWEWrapperMutex = new boost::mutex();
    memset( &fStartTime, 0, sizeof(timeval) );
    memset( &fEndTime,   0, sizeof(timeval) );
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
BulkLoad::~BulkLoad()
{
    fTableInfo.clear();
    delete fWEWrapper;
    delete fWEWrapperMutex;
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
    startTimer();

    m_jobFileName = fullName;
    m_rootDir     = Config::getBulkRoot();

    if( !exists( fullName.c_str() ) ) {
        m_log.logMsg( " file " + fullName + " does not exist",
                       ERR_FILE_NOT_EXIST, MSGLVL_ERROR );
        return ERR_FILE_NOT_EXIST;
    }

    std::string errMsg;
    int rc = m_jobInfo.loadJobXmlFile( fullName, bUseTempJobFile,
                                       bValidateColumnList, errMsg );
    if( rc != NO_ERROR ) {
        std::ostringstream oss;
        oss << "Error loading job file " << fullName << "; " << errMsg;
        m_log.logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    const Job& curJob = m_jobInfo.getJob();
    string     logFile, errlogFile;
    logFile = m_rootDir + DIR_BULK_LOG + "Job_" +
              Convertor::int2Str( curJob.id ) + LOG_SUFFIX;
    errlogFile = m_rootDir + DIR_BULK_LOG + "Job_" +
              Convertor::int2Str( curJob.id ) + ERR_LOG_SUFFIX;
    m_log.setLogFileName(logFile.c_str(),errlogFile.c_str(),bLogInfo2ToConsole);

    std::ostringstream ossLocale;
    ossLocale << "Locale is : " << systemLang;
    m_log.logMsg( ossLocale.str(), MSGLVL_INFO2 );
    cout << "Log file for this job: " << logFile << std::endl;
    m_log.logMsg( "successfully loaded job file " + fullName, MSGLVL_INFO1 );
    if (argc > 1)
    {
        std::ostringstream oss;
        oss << "Command line options: ";
        for (int k=1; k<argc; k++)
        {
            oss << argv[k] << " ";
        }
        m_log.logMsg( oss.str(), MSGLVL_INFO2 );
    }

    // Validate that the user's xml file has been regenerated since the
    // required tblOid attribute was added to the Table tag for table locking.
    for (unsigned i=0; i<curJob.jobTableList.size(); i++)
    {
        if (curJob.jobTableList[i].mapOid == 0)
        {
            rc = ERR_XML_PARSE;
            m_log.logMsg( "Outdated job file " + fullName +
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
                m_log.logMsg( oss.str(), rc, MSGLVL_ERROR );
                return rc;
            }
        }
    }

    stopTimer();

    std::ostringstream ossXMLTime;
    ossXMLTime << "Job file loaded, run time for this step : " <<
        getTotalRunTime() << " seconds";
    m_log.logMsg( ossXMLTime.str(), MSGLVL_INFO1 );

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
    for(int i=0; i<fNoOfReadThreads; ++i)
    {
        fReadThreads.create_thread(boost::bind(&BulkLoad::read, this, (int)i));
    }    
    m_log.logMsg("No of Read Threads Spawned = " +
                 Convertor::int2Str( fNoOfReadThreads ), MSGLVL_INFO1 );

    for(int i=0; i<fNoOfParseThreads; ++i)
    {
        fParseThreads.create_thread(boost::bind(&BulkLoad::parse,this,(int)i));
    }
    m_log.logMsg("No of Parse Threads Spawned = " +
                 Convertor::int2Str( fNoOfParseThreads ), MSGLVL_INFO1 );
              
    fReadThreads.join_all();
    fParseThreads.join_all();
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Pre process job, mainly validation and conversion
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
    int         rc=NO_ERROR, maxWidth=0, minWidth=9999; // give a big number
    HWM         minHWM = 999999;  // rp 9/25/07 Bug 473
    ColStruct   curColStruct;
    ColDataType colDataType;

    // Initialize portions of TableInfo object
    tableInfo->setBufferSize(fBufferSize);
    tableInfo->setFileBufferSize(fFileBufferSize);
    tableInfo->setTableId(tableNo);
    tableInfo->setColDelimiter(m_colDelim);
    tableInfo->setJobFileName(m_jobFileName);
    tableInfo->setJobId(job.id);
    tableInfo->setNullStringMode(fNullStringMode);
    tableInfo->setEnclosedByChar(fEnclosedByChar);
    tableInfo->setEscapeChar(fEscapeChar);

    // @bug 3929: cpimport error messaging using up too much memory.
    // Validate that max allowed error count is within valid range
    long long maxErrNum = job.jobTableList[tableNo].maxErrNum;
    if (maxErrNum > MAX_ALLOW_ERROR_COUNT)
    {
        ostringstream oss;
        oss << "Max allowed error count specified as " << maxErrNum <<
            " for table " << job.jobTableList[tableNo].tblName <<
            "; this exceeds limit of " << MAX_ALLOW_ERROR_COUNT <<
            "; resetting to " << MAX_ALLOW_ERROR_COUNT;
        m_log.logMsg( oss.str(), MSGLVL_INFO2 );
        maxErrNum = MAX_ALLOW_ERROR_COUNT;
    }
    tableInfo->setMaxErrorRows(maxErrNum);

    //------------------------------------------------------------------------
    // First loop thru the columns for the "tableNo" table in jobTableList[].
    // and determine the HWM for the smallest width column(s), and save that
    // as minHWM.  We save additional information acquired from
    // getLastLocalHWM for later use.
    //------------------------------------------------------------------------
    std::vector<File> segFileInfo;
    for( size_t i = 0; i < job.jobTableList[tableNo].colList.size(); i++ ) 
    {
        u_int16_t dbRoot;
        u_int32_t partition;
        u_int16_t segment;

        JobColumn curJobCol = job.jobTableList[tableNo].colList[i];

        // convert column data type
        if( curJobCol.typeName.length() >0 &&
            m_colOp->getColDataType( curJobCol.typeName.c_str(), colDataType ))
        {
            job.jobTableList[tableNo].colList[i].dataType =
               curColStruct.colDataType = colDataType;
        }
        else
        {
            ostringstream oss;
            oss << "Column type " << curJobCol.typeName << " is not valid ";
            m_log.logMsg( oss.str(), ERR_INVALID_PARAM, MSGLVL_ERROR );
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
        if( job.jobTableList[tableNo].colList[i].width > maxWidth )
            maxWidth = job.jobTableList[tableNo].colList[i].width;

        // check HWM for column file
        HWM hwm;
        rc = BRMWrapper::getInstance()->getLastLocalHWM_HWMt(
            curJobCol.mapOid, dbRoot, partition, segment, hwm );
        if( rc != NO_ERROR ) {
            WErrorCodes ec;
            ostringstream oss;
            oss << "Error getting last HWM for column file " <<
                curJobCol.mapOid << "; " << ec.errorString(rc);
            m_log.logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }

        if( job.jobTableList[tableNo].colList[i].width < minWidth )
        {
            // save the minimum hwm     --  rp 9/25/07 Bug 473
            minWidth = job.jobTableList[tableNo].colList[i].width;
            minHWM   = hwm;
        }

        // Save column segment file info for use in subsequent loop
        File fInfo;
        fInfo.fDbRoot    = dbRoot;
        fInfo.fPartition = partition;
        fInfo.fSegment   = segment;
        fInfo.hwm        = hwm;
        segFileInfo.push_back( fInfo );
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
    m_log.logMsg( oss11.str(), MSGLVL_INFO2 );

    rc = saveBulkRollbackMetaData( job, tableNo, tableInfo, segFileInfo );
    if (rc != NO_ERROR)
    {
         return rc;
    }

    //--------------------------------------------------------------------------
    // Second loop thru the columns for the "tableNo" table in jobTableList[].
    // In this pass through the columns we create the ColumnInfo object,
    // open the applicable column and dictionary store files, and seek to
    // the block where we will begin adding data.
    //--------------------------------------------------------------------------
    for( size_t i = 0; i < job.jobTableList[tableNo].colList.size(); i++ ) 
    {
        u_int16_t dbRoot    = segFileInfo[i].fDbRoot;
        u_int32_t partition = segFileInfo[i].fPartition;
        u_int16_t segment   = segFileInfo[i].fSegment;
        HWM       oldHwm    = segFileInfo[i].hwm;

        // Create a ColumnInfo for the next column, and add to tableInfo
        ColumnInfo* info = 0;
        if (job.jobTableList[tableNo].colList[i].compressionType)
            info = new ColumnInfoCompressed(&m_log, i,
                job.jobTableList[tableNo].colList[i],
                tableInfo->rbMetaWriter());
        else
            info = new ColumnInfo(&m_log, i,
                job.jobTableList[tableNo].colList[i]);

        // For auto increment column, we need to get the starting value
        if (info->column.autoIncFlag)
        {
            rc = preProcessAutoInc( job.jobTableList[tableNo].tblName, info );
            if (rc != NO_ERROR)
            {
               return rc;
            }
        }

        // Skip minimum blocks before starting import; minwidth columns skip to
        // next block.  Wider columns skip based on multiple of width. If this
        // skipping of blocks requires a new extent, then we extend the column.
        HWM hwm = (minHWM +1) * ( info->column.width/minWidth );

        BRM::LBID_t lbid;

        // Try to get LBID for our HWM.  If we can't find it, then
        // that means our block skipping has caused us to advance into
        // another extent, so we try to create a new extent.
        bool bSkippedToNewExtent = false;
        rc = BRMWrapper::getInstance()->getStartLbid(
            info->column.mapOid,
            partition,
            segment,
            (int)hwm, lbid);

        if (rc != NO_ERROR)
        {
            bSkippedToNewExtent = true;

            // Reworked initial block skipping for compression:
            // Block skipping is causing us to wrap up this extent.
            // Calculate (round down) the HWM to reference last block
            // in the current extent.
            int blocksPerExtent =
                (BRMWrapper::getInstance()->getExtentRows() *
                info->column.width) / BYTE_PER_BLOCK;
            hwm = (((hwm+1) / blocksPerExtent) * blocksPerExtent) - 1;
            rc = BRMWrapper::getInstance()->getStartLbid(
                info->column.mapOid,
                partition,
                segment,
                (int)hwm, lbid);
            if (rc != NO_ERROR)
            {
                WErrorCodes ec;
                ostringstream oss;
                oss << "Error getting HWM start LBID for last extent in column "
                       "file OID-" <<
                       info->column.mapOid <<
                       "; partition-" << partition <<
                       "; segment-"   << segment   <<
                       "; hwm-"       << hwm <<
                       "; " << ec.errorString(rc);
                m_log.logMsg( oss.str(), rc, MSGLVL_ERROR );
                return rc;
            }
        }

        // Init the ColumnInfo object
        info->colOp->initColumn( info->curCol );
        info->colOp->setColParam( info->curCol, i,
            info->column.width,
            info->column.dataType,
            info->column.weType,
            info->column.mapOid,
            job.jobTableList[tableNo].colList[i].compressionType,
            dbRoot, partition, segment );

        // Open the column file
        if(!info->colOp->exists(info->column.mapOid,
            dbRoot, partition, segment) )
        {
            ostringstream oss;
            oss << "Column file does not exist for OID-" <<
                   info->column.mapOid  <<
                   "; DBRoot-"    << dbRoot <<
                   "; partition-" << partition <<
                   "; segment-"   << segment;
            m_log.logMsg( oss.str(), ERR_FILE_NOT_EXIST, MSGLVL_ERROR );
            return ERR_FILE_NOT_EXIST;
        }

        std::string segFile;
        rc = info->colOp->openColumnFile( info->curCol, segFile );
        if(rc != NO_ERROR) 
        {
            WErrorCodes ec;
            ostringstream oss;
            oss << "Error opening column file for OID-" <<
                   info->column.mapOid <<
                   "; DBRoot-"    << dbRoot <<
                   "; partition-" << partition <<
                   "; segment-"   << segment <<
                   "; filename-"  << segFile <<
                   "; " << ec.errorString(rc);
            m_log.logMsg( oss.str(), ERR_FILE_OPEN, MSGLVL_ERROR );
            return ERR_FILE_OPEN;
        }

        ostringstream oss1;
        oss1 << "Initializing import: " <<
            "Table-"    << job.jobTableList[tableNo].tblName <<
            "; Col-"    << info->column.colName;
        if (info->curCol.compressionType)
            oss1        << " (compressed)";
        oss1 <<  "; OID-"    << info->column.mapOid <<
            "; hwm-"    << hwm;
        if (bSkippedToNewExtent)
            oss1        << " (full; load into next extent)";
        oss1 << "; file-"   << info->curCol.dataFile.fSegFileName;
        m_log.logMsg( oss1.str(), MSGLVL_INFO2 );

        if(info->column.colType == COL_TYPE_DICT)
        {
        	RETURN_ON_ERROR( info->openDctnryStore( true ) );
        }

        info->savedLbid = lbid;
        info->savedHWM = hwm;

        if (bSkippedToNewExtent)
            oldHwm  = hwm;
        rc = info->setupInitialColumnFile(oldHwm, hwm);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            ostringstream oss;
            oss << "Error reading/positioning column file for OID-" <<
                   info->column.mapOid <<
                   "; DBRoot-"    << dbRoot <<
                   "; partition-" << partition <<
                   "; segment-"   << segment <<
                   "; filename-"  << segFile <<
                   "; " << ec.errorString(rc);
            m_log.logMsg( oss.str(), rc, MSGLVL_ERROR );
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
            info->updateBytesWrittenCounts( BYTE_PER_BLOCK );
            info->sizeWrittenStart = info->sizeWritten;
        }

        // Reworked initial block skipping for compression:
        // This initializes CP stats for first extent regardless of whether
        // we end up adding rows to this extent, or initial block skipping
        // ultimately causes us to start with a new extent.
        info->lastInputRowInExtentInit( );

        tableInfo->addColumn(info);

    } // end of for loop through the list of columns

    // Initialize BulkLoadBuffers after we have added all the columns
    tableInfo->initializeBuffers(fNoOfBuffers, 
                                 job.jobTableList[tableNo].fFldRefs);

    fTableInfo.push_back(tableInfo);

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Saves snapshot of extentmap into a bulk rollback meta data file, for
//    use in a bulk rollback, if the current cpimport job should fail.
// PARAMETERS:
//    job - current job
//    tableNo - table no
//    tableInfo - TableInfo object corresponding to tableNo table.
//    segFileInfo - Vector of File objects carrying DBRoot, partition, etc
//                  for the columns belonging to tableNo.
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::saveBulkRollbackMetaData( Job& job,
    int tableNo,
    TableInfo* tableInfo,
    const std::vector<File>& segFileInfo )
{
    int rc = NO_ERROR;

    rc = tableInfo->rbMetaWriter()->openFile( );
    if (rc != NO_ERROR)
    {
        return rc;
    }

    for( size_t i = 0; i < job.jobTableList[tableNo].colList.size(); i++ ) 
    {
        u_int16_t dbRoot    = segFileInfo[i].fDbRoot;
        u_int32_t partition = segFileInfo[i].fPartition;
        u_int16_t segment   = segFileInfo[i].fSegment;

        JobColumn& jobCol   = job.jobTableList[tableNo].colList[i];

        // Save column meta-data info to support bulk rollback, if needed
        rc = tableInfo->rbMetaWriter()->writeColumnMetaData(
            jobCol.mapOid,
            dbRoot,
            partition,
            segment,
            segFileInfo[i].hwm,
            jobCol.dataType,
            jobCol.typeName,
            jobCol.width,
            jobCol.compressionType );
        if (rc != NO_ERROR)
        {
            tableInfo->rbMetaWriter()->closeFile( false );
            return rc;
        }

        if (jobCol.compressionType )
        {
            rc = tableInfo->rbMetaWriter()->backupColumnHWMChunk(
            jobCol.mapOid,
            dbRoot,
            partition,
            segment,
            segFileInfo[i].hwm );
            if (rc != NO_ERROR)
            {
                tableInfo->rbMetaWriter()->closeFile( false );
                return rc;
            }
        }

        // Save dctnry store meta-data info to support bulk rollback, if needed
        if ( jobCol.colType == COL_TYPE_DICT ) 
        {
            // See how many segment files we have in this partition.
            // If HWM shows we are working on first stripe in this partition
            // then we won't have HWM's to save for the trailing segments.
            unsigned blocksPerExtent =
               (BRMWrapper::getInstance()->getExtentRows() *
                jobCol.width) / BYTE_PER_BLOCK;
            bool bSaveTrailingHwmsInPartition = false;
            if ((segFileInfo[i].hwm + 1) > blocksPerExtent)
                bSaveTrailingHwmsInPartition = true;

            // Figure out DBRoot for segment 0 so we can loop thru segment files
            int dbRootKount = Config::DBRootCount();
            int firstDbRoot = (int)dbRoot - (segment % dbRootKount);
            if (firstDbRoot <= 0)
                firstDbRoot += dbRootKount;
            u_int16_t dbRootStore = firstDbRoot;
            const unsigned segsPerPart = Config::getFilesPerColumnPartition();

            // For rollback purposes, record the local HWM for each store file
            // in the current partition.
            for (unsigned k=0; k<segsPerPart; k++)
            {
                u_int16_t kSeg = k;

                // If the HWM from the corresponding token file indicates we are
                // still working on the first "stripe" in this partition, then
                // bSaveTrailingHwmsInPartition will be false; and we have no
                // extents in the trailing segments for this partition.
                // In this case we save NoData markers for the trailing segment
                // files in this partition.
                if ((kSeg > segment) && !bSaveTrailingHwmsInPartition)
                {
                    tableInfo->rbMetaWriter()->writeDictionaryStoreMetaNoDataMarker(
                        jobCol.dctnry.dctnryOid,
                        dbRootStore,
                        partition,
                        kSeg,
                        jobCol.compressionType );
                }
                else
                {
                    // check HWM for dictionary store file
                    HWM dictHWMStore;
                    rc = BRMWrapper::getInstance()->getLocalHWM_HWMt(
                        jobCol.dctnry.dctnryOid,
                        partition,
                        kSeg,
                        dictHWMStore );
                    if (rc != NO_ERROR)
                    {
                        WErrorCodes ec;
                        ostringstream oss;
                        oss << "Error1 getting rollback HWM for dictionary "
                               "file "<<
                            jobCol.dctnry.dctnryOid <<
                            "; partition-" << partition <<
                            "; segment-"   << kSeg <<
                            "; " << ec.errorString(rc);
                            m_log.logMsg(oss.str(), rc, MSGLVL_ERROR);
                        tableInfo->rbMetaWriter()->closeFile( false );
                        return rc;
                    }

                    tableInfo->rbMetaWriter()->writeDictionaryStoreMetaData(
                        jobCol.dctnry.dctnryOid,
                        dbRootStore,
                        partition,
                        kSeg,
                        dictHWMStore,
                        jobCol.compressionType );
                }

                dbRootStore++;
                if (dbRootStore > dbRootKount)
                    dbRootStore = 1;

            } // end of loop through segsPerPart
        }     // extra work for dictionary store file
    }         // end of loop through all columns in table

    rc = tableInfo->rbMetaWriter()->closeFile( true );

    return rc;
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
    string::size_type periodIdx = fullTableName.find('.');
    if (periodIdx == string::npos)
    {
        ostringstream oss;
        oss << "Error parsing full table name in order to "
            "get auto-increment value for " << fullTableName;
        m_log.logMsg( oss.str(), ERR_AUTOINC_TABLE_NAME, MSGLVL_ERROR );
        return ERR_AUTOINC_TABLE_NAME;
    }
    else
    {
        std::string sName;
        std::string tName;
        sName.assign(fullTableName, 0, periodIdx);
        tName.assign(fullTableName, periodIdx+1,
            fullTableName.length() - (periodIdx+1));
        execplan::CalpontSystemCatalog::TableName tbl(sName,tName);
        long long nextAuto = 0;
        try
        {
            execplan::CalpontSystemCatalog* systemCatPtr =
                execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(
                    BULK_SYSCAT_SESSION_ID);
            systemCatPtr->identity(execplan::CalpontSystemCatalog::EC);

            // Handle bad return code or thrown exception from
            // system catalog query.
            nextAuto = systemCatPtr->nextAutoIncrValue( tbl );
            if (nextAuto == 0) {
                throw std::runtime_error(
                    "Not an auto-increment column, or column not found");
            }
            else if (nextAuto == -2) {
                throw std::runtime_error(
                    "Not able to get current nextValue, table not found");
            }
            else if (nextAuto <   0) {
                throw std::runtime_error(
                    "auto-increment max value already reached");
            }
        }
        catch (std::exception& ex)
        {
            ostringstream oss;
            oss << "Unable to get current auto-increment value for " <<
                sName << "." << tName << "; " << ex.what();
            m_log.logMsg( oss.str(), ERR_AUTOINC_INIT1, MSGLVL_ERROR );
            return ERR_AUTOINC_INIT1;
        }
        catch (...)
        {
            ostringstream oss;
            oss << "Unable to get auto-increment value for " <<
                sName << "." << tName << "; unknown exception";
            m_log.logMsg( oss.str(), ERR_AUTOINC_INIT2, MSGLVL_ERROR );
            return ERR_AUTOINC_INIT2;
        }

        ostringstream oss2;
        oss2 << "Initializing next auto increment for table-" <<
            fullTableName << ", column-" << colInfo->column.colName <<
            "; autoincrement " << nextAuto;
        m_log.logMsg( oss2.str(), MSGLVL_INFO2 );

        colInfo->initAutoInc( nextAuto );
    }

    return NO_ERROR;
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
    bool        bHaveColumn = false;
    Job         curJob;
    size_t      i;
      
    curJob = m_jobInfo.getJob();

    fNoOfBuffers    = curJob.numberOfReadBuffers;
    fBufferSize     = curJob.readBufferSize;
    fFileBufferSize = curJob.writeBufferSize;

    // If cmd line override was not given for delimiter, then use the one
    // from the XML file.  Same goes for enclosedBy and escape characters.
    if (m_colDelim == '\0')
    {
        m_colDelim   = curJob.fDelimiter;
    }
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
    if (fEnclosedByChar != '\0')
    {
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
    }
    //std::cout << "bulkload::fEnclosedByChar<" << fEnclosedByChar << '>' <<
    //std::endl << "bulkload::fEscapeChar<" << fEscapeChar << '>' << std::endl;

    //Bug1315 - check whether DBRoots are RW mounted.
    int dbRootKount = Config::DBRootCount();
    for (int counter=0; counter<dbRootKount; counter++)
    {
        if ( access(Config::getDBRoot(counter),R_OK|W_OK) < 0 )
        {
            rc = ERR_FILE_NOT_EXIST;
            ostringstream oss;
            oss << "Error accessing DBRoot" << counter+1 << " " <<
                string(Config::getDBRoot(counter)) << "; " << strerror(errno);
            m_log.logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }
    }     

    // Init total cumulative run time with time it took to load xml file
    double totalRunTime = getTotalRunTime();
    m_log.logMsg( "PreProcessing check starts", MSGLVL_INFO1 ); 
    startTimer();

    //--------------------------------------------------------------------------
    // Validate that only 1 table is specified for import if using STDIN
    //--------------------------------------------------------------------------
    if ((fAlternateImportDir == IMPORT_PATH_STDIN) &&
        (curJob.jobTableList.size() > 1))
    {
        rc = ERR_INVALID_PARAM;
        m_log.logMsg("Only 1 table can be imported per job when using STDIN",
            rc, MSGLVL_ERROR );
        return rc;
    }

    //--------------------------------------------------------------------------
    // Validate the existence of the import data files
    //--------------------------------------------------------------------------
    std::vector<TableInfo*> tables;
    for( i = 0; i < curJob.jobTableList.size(); i++ )
    {
        TableInfo *tableInfo = new TableInfo(&m_log,
                                             fTxnID,
                                             fProcessName,
                                             curJob.jobTableList[i].mapOid,
                                             curJob.jobTableList[i].tblName,
                                             fKeepRbMetaFiles);
        rc = validateImportDataFiles( curJob, i, tableInfo );
        if (rc != NO_ERROR)
            return rc;     

        tables.push_back( tableInfo );
    }

    //--------------------------------------------------------------------------
    // Before we go any further, we lock all the tables
    //--------------------------------------------------------------------------
    for( i = 0; i < curJob.jobTableList.size(); i++ )
    {
        rc = tables[i]->acquireTableLock( );
        if (rc != NO_ERROR)
        {
            // Try releasing the table locks we already acquired.
            // Note that loop is k<i since tables[i] lock failed.
            for (unsigned k=0; k<i; k++)
            {
                tables[k]->releaseTableLock( );//ignore return code in this case
            }

            return rc;
        }

        // If we have a lock, then init MetaWriter, so that it can delete any
        // leftover backup meta data files that collide with the ones we are
        // going to create.
        rc = tables[i]->rbMetaWriter()->init(
            curJob.jobTableList[i].mapOid,
            curJob.jobTableList[i].tblName );
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
        if( curJob.jobTableList[i].colList.size() > 0 )
            bHaveColumn = true;
        
        rc = preProcess( curJob, i, tables[i] );
        if( rc != NO_ERROR ) {
            m_log.logMsg( "Error in pre-processing the job file for table " +
                curJob.jobTableList[i].tblName, rc, MSGLVL_CRITICAL );

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
            BRMWrapper::getInstance()->takeSnapshot();

            return rc;
        }
    }
      
    stopTimer();
    m_log.logMsg( "PreProcessing check completed", MSGLVL_INFO1 );

    std::ostringstream ossPrepTime;
    ossPrepTime << "preProcess completed, run time for this step : " <<
        getTotalRunTime() << " seconds";
    m_log.logMsg( ossPrepTime.str(), MSGLVL_INFO1 );
    totalRunTime += getTotalRunTime();

    if( !bHaveColumn )
    {
        rc = ERR_INVALID_PARAM;
        m_log.logMsg( "No column definitions in job description file",
            rc, MSGLVL_ERROR );
        return rc;
    }

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
    BRMWrapper::getInstance()->takeSnapshot();

    stopTimer();
    totalRunTime += getTotalRunTime();

    std::ostringstream ossTotalRunTime;
    ossTotalRunTime << "Bulk load completed, total run time : " <<
        totalRunTime << " seconds" << std::endl;
    m_log.logMsg( ossTotalRunTime.str(), MSGLVL_INFO1 );

#ifdef PROFILE
    Stats::printProfilingResults( );
#endif

    return rc;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Validate the existence of the import data files for the specified table
// PARAMETERS:
//    job - current job
//    tableNo - table no
//    tableInfo - TableInfo object corresponding to tableNo table.
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::validateImportDataFiles(Job& job,
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

        loadFilesList = tokenizeStr(std::string(),
                        loadFileName);
        if (loadFilesList.size() > 1)
        {
            ostringstream oss;
            oss << "Table " << tableInfo->getTableName() <<
                " specifies multiple "
                "load files; This is not allowed when using STDIN";
            m_log.logMsg( oss.str(), ERR_INVALID_PARAM, MSGLVL_ERROR );
            return ERR_INVALID_PARAM;
        }
    }
    else
    {
        std::string importDir;
        if ( fAlternateImportDir == IMPORT_PATH_CWD )  // current working dir
        {
            // do nothing
        }
        else if ( fAlternateImportDir.size() > 0 )     // -f path
        {
            importDir = fAlternateImportDir;
        }
        else                                           // <BULKROOT>/data/import
        {
            importDir  = m_rootDir;
            importDir += DIR_BULK_IMPORT;
        }
        loadFilesList = tokenizeStr(importDir, loadFileName);
      
        // Verify that input data files exist and have data
        for (unsigned ndx=0; ndx<loadFilesList.size(); ndx++ ) 
        {
            string fileName = loadFilesList[ndx]; 
            ifstream in(fileName.c_str());
            if (!in)
            {
                in.close();
                ostringstream oss;
                oss << "input data file " << fileName << " does not exist";
                m_log.logMsg( oss.str(), ERR_FILE_NOT_EXIST, MSGLVL_ERROR );
                return ERR_FILE_NOT_EXIST;
            }
            if(in.peek()==EOF)
            {
                in.close();
                ostringstream oss;
                oss << "input data file " << fileName << " is an empty file";
                m_log.logMsg( oss.str(), ERR_FILE_READ, MSGLVL_ERROR );
                return ERR_FILE_READ;
            }
            in.close();
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
// RETURN:
//    returns a vector of file file path strings
//------------------------------------------------------------------------------
std::vector<std::string> BulkLoad::tokenizeStr(const std::string& location,
                                               const std::string& filename)
{
    char *filenames = new char[filename.size() + 1];
    strcpy(filenames,filename.c_str());

    char *str;
    char *token;
    std::vector<std::string> loadFiles;
    for (str = filenames;;str=NULL)
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
        if (p.has_root_directory())
        {
            loadFiles.push_back(std::string(token));
        }
        else
        {
            std::string fullPath(location);
            fullPath += token;
            loadFiles.push_back(fullPath);
        }
    }
    delete [] filenames;
    return loadFiles;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Sets active debug level for logging.
// PARAMETERS:
//    level - requested debug level
// RETURN:
//    none
//------------------------------------------------------------------------------
void BulkLoad::setAllDebug( DebugLevel level )
{
    setDebugLevel( level );
    m_log.setDebugLevel( level );
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
                m_log.logMsg( oss.str(), MSGLVL_INFO1 );
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
                    m_log.logMsg( oss.str(), MSGLVL_INFO1 );
                }
                else
                {
                    ostringstream oss;
                    oss << "Table " << fTableInfo[i].getTableName() <<
                        " (OID-" << fTableInfo[i].getTableOID() << ")" <<
                        " did not start loading.  No rollback necessary.";
                    m_log.logMsg( oss.str(), MSGLVL_INFO1 );
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
//    Clear table lock, and rollback the specified table
//    still locked through session manager.
// PARAMETERS:
//    tableInfo - the table to be released and rolled back
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
int BulkLoad::rollbackLockedTable( TableInfo& tableInfo)
{
    // Close any column or store files left open by abnormal termination
    tableInfo.closeOpenDbFiles();

    // Restore/rollback the DB files if we got far enough to begin processing
    // this table.
    int rc = NO_ERROR;
    if (tableInfo.hasProcessingBegun())
    {
        const std::string APPLNAME("cpimport");
        BulkRollbackMgr rbMgr( tableInfo.getTableOID(),
            tableInfo.getTableName(),
            APPLNAME, &m_log );
        rc = rbMgr.rollback( true, fKeepRbMetaFiles );
        if (rc != NO_ERROR)
        {
            ostringstream oss;
            oss << "Error rolling back table " <<
                tableInfo.getTableName()    <<
                "; " << rbMgr.getErrorMsg();
            m_log.logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }
    }

    // Delete the meta data files after rollback is complete
    tableInfo.deleteMetaDataRollbackFile( );

    // Release the table lock
    rc = tableInfo.releaseTableLock( );
    if (rc != NO_ERROR)
    {
        ostringstream oss;
        oss << "Table lock not cleared for table " <<
            tableInfo.getTableName();
        m_log.logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    return rc;
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//    Update next autoincrement value for specified table OID.
// PARAMETERS:
//    tableOID       - table OID of interest
//    nextAutoIncVal - next autoincrement value to assign to tableOID
// RETURN:
//    NO_ERROR if success
//    other if fail
//------------------------------------------------------------------------------
/* static */
int BulkLoad::updateNextValue(OID tableOID, long long nextAutoIncVal)
{
    WriteEngineWrapper::AutoIncrementValue_t nextValueMap;
    nextValueMap[tableOID] = nextAutoIncVal;

    // Perform writeengine wrapper call within mutex to ensure thread safe
    boost::mutex::scoped_lock lock( *fWEWrapperMutex );
    int rc = fWEWrapper->updateNextValue( nextValueMap,
                                          BULK_SYSCAT_SESSION_ID );

    return rc;
}

} //end of namespace

