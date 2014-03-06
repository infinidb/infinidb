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

/*
* $Id: we_rbmetawriter.cpp 4737 2013-08-14 20:45:46Z bwilkinson $
*/

#include "we_rbmetawriter.h"

#include <cerrno>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>

#include "we_config.h"
#include "we_convertor.h"
#include "we_define.h"
#include "we_log.h"
#include "we_bulkrollbackmgr.h"
#include "idbcompress.h"
using namespace compress;
using namespace execplan;
#include "IDBDataFile.h"
#include "IDBFileSystem.h"
#include "IDBPolicy.h"
using namespace idbdatafile;

namespace
{
    const char* DATA_DIR_SUFFIX = "_data";
    const char* TMP_FILE_SUFFIX = ".tmp";

    const char* VERSION3_REC    = "# VERSION: 3";
    const int   VERSION3_REC_LEN= 12;
    const char* VERSION4_REC    = "# VERSION: 4";
    const int   VERSION4_REC_LEN= 12;
    const char* COLUMN1_REC     = "COLUM1"; // HWM extent for a DBRoot
    const int   COLUMN1_REC_LEN = 6;
    const char* COLUMN2_REC     = "COLUM2"; // Placeholder for empty DBRoot
    const int   COLUMN2_REC_LEN = 6;
    const char* DSTORE1_REC     = "DSTOR1"; // HWM extent for a DBRoot
    const int   DSTORE1_REC_LEN = 6;
    const char* DSTORE2_REC     = "DSTOR2"; // Placeholder for empty DBRoot
    const int   DSTORE2_REC_LEN = 6;

    //--------------------------------------------------------------------------
    // Local Function that prints contents of an RBChunkInfo object
    //--------------------------------------------------------------------------
    std::ostream& operator<<(std::ostream& os,
        const WriteEngine::RBChunkInfo& chk)
    {
        os << "OID-"    << chk.fOid       <<
            "; DBRoot-" << chk.fDbRoot    <<
            "; Part-"   << chk.fPartition <<
            "; Seg-"    << chk.fSegment   <<
            "; HWM-"    << chk.fHwm;

        return os;
    }
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// Compare function used for set of RBChunkInfo objects.
//------------------------------------------------------------------------------
bool RBChunkInfoCompare::operator()
    (const RBChunkInfo& lhs, const RBChunkInfo& rhs) const
{
    if (lhs.fOid < rhs.fOid) {
        return true;
    }

    if ((lhs.fOid==rhs.fOid) && (lhs.fSegment < rhs.fSegment)) {
        return true;
    }

    return false;
}

//------------------------------------------------------------------------------
// RBMetaWriter constructor
//------------------------------------------------------------------------------
RBMetaWriter::RBMetaWriter (
    const std::string& appDesc,
    Log* logger ) : fMetaDataFile(NULL), fAppDesc(appDesc), fLog(logger), fCreatedSubDir(false)
{
}

//------------------------------------------------------------------------------
// Initialize this meta data file object using the specified table OID and name.
// We assume the application code calling this function, was able to acquire a
// table lock, meaning if there should happen to be any leftoever metadata files
// from a previous job, they can be deleted.
//------------------------------------------------------------------------------
void RBMetaWriter::init (
    OID tableOID,
    const std::string& tableName )
{
    fTableOID  = tableOID;
    fTableName = tableName;

    std::vector<uint16_t> dbRoots;
    Config::getRootIdList( dbRoots );

    std::string metaFileName;
    std::ostringstream oss;
    oss << "/" << fTableOID;

    // Delete any files that collide with the file names we are going to need.
    // Construct the filenames; we will use a temporary file name until we are
    // finished creating, at which time we will rename the temp files.
    for (unsigned m=0; m<dbRoots.size(); m++)
    {
        std::string bulkRollbackPath( Config::getDBRootByNum( dbRoots[m] ) );
        bulkRollbackPath += '/';
        bulkRollbackPath += DBROOT_BULK_ROLLBACK_SUBDIR;
        metaFileName      = bulkRollbackPath;
        metaFileName     += oss.str();

        std::string tmpMetaFileName = metaFileName;
        tmpMetaFileName += TMP_FILE_SUFFIX;

        // Delete any files that collide with the filenames we intend to use
        IDBPolicy::remove( metaFileName.c_str() );
        IDBPolicy::remove( tmpMetaFileName.c_str() );

        // Clear out any data subdirectory
        deleteSubDir( metaFileName );
    }
}   

//------------------------------------------------------------------------------
// Saves snapshot of extentmap into a bulk rollback meta data file, for
// use in a bulk rollback.  Function was closely modeled after function
// of similar name in bulk/we_tableinfo.cpp.  API was modified to help
// facilitate its use by DML.
//
// columns - Column vector with information about column in table.
//           Includes information about the initial HWM extent, so that
//           the corresponding HWM chunk can be backed up.
// dctnryStoreOids - Dictionary store OIDs that correspond to columns.
// dbRootHWMInfoVecCol - vector of last local HWM info for each DBRoot
//     (asssigned to current PM) for each column in tblOid.
//------------------------------------------------------------------------------
void RBMetaWriter::saveBulkRollbackMetaData(
    const std::vector<Column>& columns,
    const std::vector<OID>&    dctnryStoreOids,
    const std::vector<BRM::EmDbRootHWMInfo_v>& dbRootHWMInfoVecCol )
{
    int rc = NO_ERROR;
    bool bOpenedFile = false;

    try
    {
        std::vector<uint16_t> dbRoots;
        Config::getRootIdList( dbRoots );

        // Loop through DBRoot HWMs for this PM
        for (unsigned m=0; m<dbRoots.size(); m++)
        {
            std::string metaFileName = openMetaFile( dbRoots[m] );
            bOpenedFile    = true;
            fCreatedSubDir = false;

            // Loop through the columns in the specified table
            for( size_t i = 0; i < columns.size(); i++ ) 
            {
                const BRM::EmDbRootHWMInfo_v& dbRootHWMInfo =
                    dbRootHWMInfoVecCol[i];

                // Select dbRootHWMInfo that matches DBRoot for this iteration
                unsigned k = 0;
                for (; k<dbRootHWMInfo.size(); k++)
                {
                    if (dbRoots[m] == dbRootHWMInfo[k].dbRoot)
                        break;
                }
                if (k >= dbRootHWMInfo.size()) // logic error; should not happen
                {
                    std::ostringstream oss;
                    oss << "Error creating meta file; DBRoot" << dbRoots[m] <<
                       " listed in Calpont config file, but not in extentmap"
                       " for OID " << columns[i].dataFile.oid;
                    throw WeException( oss.str(), ERR_INVALID_PARAM );
                }
                    
                uint16_t dbRoot    = dbRootHWMInfo[k].dbRoot;
                uint32_t partition = 0;
                uint16_t segment   = 0;
                HWM       localHWM  = 0;
                bool      bExtentWithData = false;

                // For empty DBRoot (totalBlocks == 0),
                // leave partition, segment, and HWM set to 0
                if ((dbRootHWMInfo[k].totalBlocks > 0) ||
                    (dbRootHWMInfo[k].status == BRM::EXTENTOUTOFSERVICE))
                {
                    partition = dbRootHWMInfo[k].partitionNum;
                    segment   = dbRootHWMInfo[k].segmentNum;
                    localHWM  = dbRootHWMInfo[k].localHWM;
                    bExtentWithData = true;
                }

                // Save column meta-data info to support bulk rollback
                writeColumnMetaData(
                    metaFileName,
                    bExtentWithData,
                    columns[i].dataFile.oid,
                    dbRoot,
                    partition,
                    segment,
                    localHWM,
                    columns[i].colDataType,
                    ColDataTypeStr[ columns[i].colDataType ],
                    columns[i].colWidth,
                    columns[i].compressionType );

                // Save dctnry store meta-data info to support bulk rollback
                if ( dctnryStoreOids[i] > 0 ) 
                {
                    std::vector<uint32_t> segList;
                    std::string segFileListErrMsg;

                    if (bExtentWithData)
                    {
                        std::string dirName;
                        FileOp fileOp(false);
                        rc = fileOp.getDirName( dctnryStoreOids[i],
                            dbRoot, partition, dirName );
                        if (rc != NO_ERROR)
                        {
                            WErrorCodes ec;
                            std::ostringstream oss;
                            oss << "Bulk rollback error constructing path "
                                "for dictionary " << dctnryStoreOids[i] <<
                                "; dbRoot-"    << dbRoot    <<
                                "; partition-" << partition <<
                                "; "           << ec.errorString(rc);

                            throw WeException( oss.str(), rc );
                        }

                        rc = BulkRollbackMgr::getSegFileList(dirName, false,
                            segList,
                            segFileListErrMsg);       
                        if (rc != NO_ERROR)
                        {
                            WErrorCodes ec;
                            std::ostringstream oss;
                            oss << "Bulk rollback error for dictionary " <<
                                dctnryStoreOids[i]        <<
                                "; directory-" << dirName <<
                                "; " << segFileListErrMsg <<
                                "; " << ec.errorString(rc);

                            throw WeException( oss.str(), rc );
                        }
                    }   // end of "if (bExtentWithData)"

                    if (segList.size() == 0)
                    {
                       writeDictionaryStoreMetaNoDataMarker(
                            columns[i].dataFile.oid,
                            dctnryStoreOids[i],
                            dbRoot,
                            partition,
                            0, // segment
                            columns[i].compressionType );
                    }
                    else
                    {
                        // Loop thru dictionary store seg files for this DBRoot
                        for (unsigned int kk=0; kk<segList.size(); kk++)
                        {
                            unsigned int segDictionary = segList[kk];

                            // check HWM for dictionary store file
                            HWM dictHWMStore;
                            int extState;
                            rc = BRMWrapper::getInstance()->getLocalHWM(
                                dctnryStoreOids[i],
                                partition,
                                segDictionary,
                                dictHWMStore,
                                extState );
                            if (rc != NO_ERROR)
                            {
                                WErrorCodes ec;
                                std::ostringstream oss;
                                oss << "Error getting rollback HWM for "
                                    "dictionary file "<< dctnryStoreOids[i] <<
                                    "; partition-" << partition <<
                                    "; segment-"   << segDictionary <<
                                    "; " << ec.errorString(rc);
                                throw WeException( oss.str(), rc );
                            }

                            writeDictionaryStoreMetaData(
                                columns[i].dataFile.oid,
                                dctnryStoreOids[i],
                                dbRoot,
                                partition,
                                segDictionary,
                                dictHWMStore,
                                columns[i].compressionType );

                        } // loop thru dictionary store seg files in this DBRoot
                    }     // dictionary OID has 1 or more seg files in partition
                }         // if dictionary column

                // For a compressed column, backup the starting HWM chunk if the
                // starting HWM block is not on an empty DBRoot (or outOfSrvc)
                if ( (columns[i].compressionType) &&
                     (columns[i].dataFile.fDbRoot == dbRootHWMInfo[k].dbRoot) &&
                     (dbRootHWMInfo[k].totalBlocks > 0) &&
                     (dbRootHWMInfo[k].status != BRM::EXTENTOUTOFSERVICE) )
                {
                    backupColumnHWMChunk(
                        columns[i].dataFile.oid,
                        columns[i].dataFile.fDbRoot,
                        columns[i].dataFile.fPartition,
                        columns[i].dataFile.fSegment,
                        columns[i].dataFile.hwm );
                }

            }         // End of loop through columns

            // time to dump the string stream to file
            std::string data(fMetaDataStream.str());

            // this is to cover partical writes
            // no need for retry if low layer takes care partial writes.
            const char* p = data.c_str(); // buffer contents
            size_t      s = data.size();  // buffer size
            size_t      w = 0;            // total bytes written so far
            ssize_t     n = 0;            // bytes written in one write
            for (int i = 0; i < 10 && w < s; i++)
            {
                n = fMetaDataFile->write(p+w, s-w);
                if (n < 0)
                break;

                w += n;
            }
            if (w != s)
            {
                int errRc = errno;
                std::ostringstream oss;
                oss << "Error writing bulk rollback meta-data file "
                << metaFileName << "; written/expect:" << w << "/" << s
                << "; err-" << errRc << "; " << strerror( errRc );
               throw WeException(oss.str(), ERR_FILE_WRITE);
            }

            fMetaDataStream.str("");
            closeMetaFile( );
            bOpenedFile = false;

        }   // End of loop through DBRoot HWMs for this PM

        renameMetaFile( ); // rename meta files from temporary filenames
    }
    catch (WeException& ex) // catch exception to close file, then rethrow
    {
        if (bOpenedFile)
            closeMetaFile( );

        // If any error occurred, then go back and try to delete all meta files.
        // We catch and drop any exception, and return the original exception,
        // since we are already in error-mode at this point.
        try
        {
            deleteFile( );
        }
        catch (...)
        {
        }
        throw WeException( ex.what(), ex.errorCode() );
    }
}

//------------------------------------------------------------------------------
// Open a meta data file to save info about the specified table OID.
//------------------------------------------------------------------------------
std::string RBMetaWriter::openMetaFile ( uint16_t dbRoot )
{
    std::string bulkRollbackPath( Config::getDBRootByNum( dbRoot ) );
    bulkRollbackPath += '/';
    bulkRollbackPath += DBROOT_BULK_ROLLBACK_SUBDIR;

    if( !IDBPolicy::exists( bulkRollbackPath.c_str() ) )
    {
        if( IDBPolicy::mkdir( bulkRollbackPath.c_str() ) != 0 )
        {
            std::ostringstream oss;
            oss << "Error creating bulk rollback directory " <<
                bulkRollbackPath << ";" << std::endl;
            throw WeException( oss.str(), ERR_DIR_CREATE );
        }
    }

    // Open the file
    std::ostringstream oss;
    oss << "/" << fTableOID;
    std::string metaFileName( bulkRollbackPath );
    metaFileName += oss.str();
    fMetaFileNames.insert( make_pair(dbRoot,metaFileName) );

    std::string tmpMetaFileName( metaFileName );
    tmpMetaFileName += TMP_FILE_SUFFIX;
    fMetaDataFile = IDBDataFile::open(IDBPolicy::getType(tmpMetaFileName.c_str(),
                                          IDBPolicy::WRITEENG),
                                      tmpMetaFileName.c_str(), "wb", 0);

    if ( !fMetaDataFile )
    {
        int errRc = errno;
        std::ostringstream oss;
        std::string eMsg;
        Convertor::mapErrnoToString(errRc, eMsg);
        oss << "Error opening bulk rollback file " <<
            tmpMetaFileName << "; " << eMsg;
        throw WeException( oss.str(), ERR_FILE_OPEN );
    }

    fMetaDataStream <<
        "# VERSION: 4"             << std::endl <<
        "# APPLICATION: " << fAppDesc<<std::endl<<
        "# PID:    " << ::getpid() << std::endl <<
        "# TABLE:  " << fTableName << std::endl <<
        "# COLUM1: coloid,"
            "dbroot,part,seg,lastLocalHWM,type,typename,width,comp" <<
            std::endl <<
        "# COLUM2: coloid,"
            "dbroot,part,seg,type,typename,width,comp" <<
            std::endl <<
        "# DSTOR1: coloid,dctoid,"
            "dbroot,part,seg,localHWM,comp" << std::endl <<
        "# DSTOR2: coloid,dctoid,"
            "dbroot,part,seg,comp" << std::endl;

    // Clear out any data subdirectory
    // This is redundant because init() also calls deleteSubDir(), but it can't
    // hurt to call twice.  We "really" want to make sure we start with a clean
    // slate (no leftover backup chunk files from a previous import job).
    deleteSubDir( metaFileName );

    return metaFileName;
}

//------------------------------------------------------------------------------
// Close the currently open "temporary named" meta data file used during
// construction.  We will rename all the meta data files (for the various
// dbroots) to their eventual file names later, in renameMetaFile().
//------------------------------------------------------------------------------
void RBMetaWriter::closeMetaFile ( )
{
    delete fMetaDataFile;
    fMetaDataFile = NULL;
}

//------------------------------------------------------------------------------
// Rename temporary metafile names to their permanent name, taking file names
// from fMetaFileNames.  In the normal case there will be one file name per
// DBRoot for the local PM we are running on.
//------------------------------------------------------------------------------
void RBMetaWriter::renameMetaFile ( )
{
    for(std::map<uint16_t,std::string>::const_iterator iter =
        fMetaFileNames.begin(); iter != fMetaFileNames.end(); ++iter)
    {
        const std::string& metaFileName = iter->second;
        if (!metaFileName.empty())
        {
            std::string tmpMetaFileName = metaFileName;
            tmpMetaFileName += TMP_FILE_SUFFIX;

            if ( IDBPolicy::rename(tmpMetaFileName.c_str(), metaFileName.c_str()) )
            {
                int errRc = errno;
                std::ostringstream oss;
                std::string eMsg;
                Convertor::mapErrnoToString(errRc, eMsg);
                oss << "Error renaming meta data file-" <<
                    tmpMetaFileName << "; will be deleted; " << eMsg;
                throw WeException( oss.str(), ERR_METADATABKUP_FILE_RENAME );
            }
        }
    }
}

//------------------------------------------------------------------------------
// Delete the meta data files for the specified table OID.  We loop through all
// the DBRoots for the local PM, deleting the applicable meta data files.
// If the call to deleteSubDir() should throw an exception, we might not want
// to consider that a fatal error, but we go ahead and let the exception
// go up the call-stack so that the caller can log the corresponding message.
// The application code can then decide whether they want to consider this
// condition as fatal or not.
//------------------------------------------------------------------------------
void RBMetaWriter::deleteFile ( )
{
    for(std::map<uint16_t,std::string>::const_iterator iter =
        fMetaFileNames.begin(); iter != fMetaFileNames.end(); ++iter)
    {
        const std::string& metaFileName = iter->second;
        if (!metaFileName.empty())
        {
            std::string tmpMetaFileName = metaFileName;
            tmpMetaFileName += TMP_FILE_SUFFIX;

            IDBPolicy::remove( metaFileName.c_str() );
            IDBPolicy::remove( tmpMetaFileName.c_str() );

            deleteSubDir( metaFileName ); // can throw an exception
        }
    }

    fMetaFileNames.clear( );
}

//------------------------------------------------------------------------------
// New version of writeColumnMetaData for Shared-Nothing
//------------------------------------------------------------------------------
void RBMetaWriter::writeColumnMetaData (
    const std::string& metaFileName,
    bool               withHWM,
    OID                columnOID,
    uint16_t           dbRoot,
    uint32_t           partition,
    uint16_t           segment,
    HWM                lastLocalHwm,
    CalpontSystemCatalog::ColDataType colType,
    const std::string& colTypeName,
    int                colWidth,
    int                compressionType )
{
    if (withHWM)
    {
        fMetaDataStream  << "COLUM1: " <<
            columnOID    << ' ' <<
            dbRoot       << ' ' <<
            partition    << ' ' <<
            segment      << ' ' <<
            lastLocalHwm << ' ' <<
            colType      << ' ' <<
            colTypeName  << ' ' <<
            colWidth;
    }
    else
    {
        fMetaDataStream  << "COLUM2: " <<
            columnOID    << ' ' <<
            dbRoot       << ' ' <<
            partition    << ' ' <<
            segment      << ' ' <<
            colType      << ' ' <<
            colTypeName  << ' ' <<
            colWidth;
    }
    if (compressionType)
        fMetaDataStream << ' ' << compressionType << ' ';
    fMetaDataStream << std::endl;

    // If column is compressed, then create directory for storing HWM chunks
    if (compressionType)
    {
        if (!fCreatedSubDir)
        {
            // @bug 5572 - Don't need db backup files for HDFS;
            //             use hdfs buffer file
            if (!IDBPolicy::useHdfs())
                createSubDir( metaFileName );
        }
    }
}

//------------------------------------------------------------------------------
// New version of writeDictionaryStoreMetaData for Shared-Nothing.
//------------------------------------------------------------------------------
void RBMetaWriter::writeDictionaryStoreMetaData (
    OID      columnOID,
    OID      dictionaryStoreOID,
    uint16_t dbRoot,
    uint32_t partition,
    uint16_t segment,
    HWM      localHwm,
    int      compressionType )
{
    fMetaDataStream        << "DSTOR1: " <<
        columnOID          << ' ' <<
        dictionaryStoreOID << ' ' <<
        dbRoot             << ' ' <<
        partition          << ' ' <<
        segment            << ' ' <<
        localHwm;
    if (compressionType)
        fMetaDataStream << ' ' << compressionType << ' ';
    fMetaDataStream << std::endl;

    // Save dictionary meta data for later use in backing up the HWM chunks
    if (compressionType)
    {
        RBChunkInfo chunkInfo(
            dictionaryStoreOID, dbRoot, partition, segment, localHwm);
        fRBChunkDctnrySet.insert( chunkInfo );

        if ( (fLog) && (fLog->isDebug(DEBUG_1)) )
            printDctnryChunkList(chunkInfo, "after adding ");
    }
}

//------------------------------------------------------------------------------
// New version of writeDictionaryStoreMetaNoDataMarker for Shared-Nothing.
//------------------------------------------------------------------------------
void RBMetaWriter::writeDictionaryStoreMetaNoDataMarker (
    OID      columnOID,
    OID      dictionaryStoreOID,
    uint16_t dbRoot,
    uint32_t partition,
    uint16_t segment,
    int      compressionType )
{
    fMetaDataStream        << "DSTOR2: " <<
        columnOID          << ' ' <<
        dictionaryStoreOID << ' ' <<
        dbRoot             << ' ' <<
        partition          << ' ' <<
        segment;
    if (compressionType)
        fMetaDataStream << ' ' << compressionType << ' ';
    fMetaDataStream << std::endl;
}

//------------------------------------------------------------------------------
// Create the subdirectory we will use to backup data needed for rollback.
//------------------------------------------------------------------------------
void RBMetaWriter::createSubDir( const std::string& metaFileName )
{
    std::string bulkRollbackSubPath( metaFileName );
    bulkRollbackSubPath += DATA_DIR_SUFFIX;

    if( IDBPolicy::mkdir( bulkRollbackSubPath.c_str() ) != 0 )
    {
        std::ostringstream oss;
        oss << "Error creating bulk rollback data subdirectory " <<
            bulkRollbackSubPath << ";";
        throw WeException( oss.str(), ERR_DIR_CREATE );
    }

    fCreatedSubDir = true;
}

//------------------------------------------------------------------------------
// Delete the subdirectory used to backup data needed for rollback.
//------------------------------------------------------------------------------
void RBMetaWriter::deleteSubDir( const std::string& metaFileName )
{
    std::string bulkRollbackSubPath( metaFileName );
    bulkRollbackSubPath += DATA_DIR_SUFFIX;

    if( IDBPolicy::remove( bulkRollbackSubPath.c_str() ) != 0 )
    {
        std::ostringstream oss;
        oss << "Error deleting bulk rollback data subdirectory " <<
            bulkRollbackSubPath << ";";
        throw WeException( oss.str(), ERR_FILE_DELETE );
    }
}

//------------------------------------------------------------------------------
// Backup the contents of the HWM chunk for the specified column OID extent,
// so that the chunk is available for bulk rollback.
// This operation is only performed for compressed columns.
//------------------------------------------------------------------------------
void RBMetaWriter::backupColumnHWMChunk(
    OID       columnOID,
    uint16_t  dbRoot,
    uint32_t  partition,
    uint16_t  segment,
    HWM       startingHWM)
{
    // @bug 5572 - Don't need db backup file for HDFS; we use hdfs buffer file
    if (!IDBPolicy::useHdfs())
    {
        backupHWMChunk( true,
            columnOID, dbRoot, partition, segment, startingHWM );
    }
}

//------------------------------------------------------------------------------
// Backup the contents of the HWM chunk for the specified dictionary store OID
// extent, so that the chunk is available for bulk rollback.
// This operation is only performed for compressed columns.  Once the chunk is
// saved, we remove that OID, partition, and segment from the internal list
// (fRBChunkDctnrySet) that is maintained.
// Return value indicates whether the specified file needs to be backed up or
// not.
//
// This function MUST be maintained to be thread-safe so that multiple threads
// can concurrently call this function, with each thread managing a different
// dictionary column.
//------------------------------------------------------------------------------
// @bug 5572 - HDFS usage: add return flag to indicate backup status
bool RBMetaWriter::backupDctnryHWMChunk(
    OID       dctnryOID,
    uint16_t  dbRoot,
    uint32_t  partition,
    uint16_t  segment)
{
    bool bBackupApplies = false;

    if (fRBChunkDctnrySet.size() > 0)
    {
        RBChunkInfo chunkInfo(
            dctnryOID, 0, partition, segment, 0);
        RBChunkInfo chunkInfoFound(0,0,0,0,0);
        bool bFound = false;

        { // Use scoped lock to perform "find"
            boost::mutex::scoped_lock lock( fRBChunkDctnryMutex );
            if ( (fLog) && (fLog->isDebug(DEBUG_1)) )
                printDctnryChunkList(chunkInfo, "when searching ");
            RBChunkSet::iterator iter = fRBChunkDctnrySet.find ( chunkInfo );
            if (iter != fRBChunkDctnrySet.end())
            {
                bFound = true;
                chunkInfoFound = *iter;
            }
        }

        if (bFound)
        {
            if (chunkInfoFound.fPartition == partition)
            {
                // @bug 5572 - Don't need db backup file for HDFS;
                //             we use hdfs buffer file.  Set backup flag
                //             so application knows to use tmp buffer file.
                bBackupApplies = true;
                if (!IDBPolicy::useHdfs())
                {
                    backupHWMChunk(false, dctnryOID,
                        dbRoot, partition, segment, chunkInfoFound.fHwm);
                }
            }
            else
            {
                // How could this happen?  Ended up asking for different
                // partition than expected for the first instance of this
                // OID and segment file.  Perhaps initial blockskipping
                // or something caused us to advance to another segment file
                // without ever changing the expected extent.  At any rate
                // we still fall through and delete our entry because we
                // apparently did not end up changing the chunk referenced
                // by this RBChunkInfo object.
            }

            { // Use scoped lock to perform "erase"
                boost::mutex::scoped_lock lock( fRBChunkDctnryMutex );
                fRBChunkDctnrySet.erase( chunkInfoFound );
                if ( (fLog) && (fLog->isDebug(DEBUG_1)) )
                    printDctnryChunkList(chunkInfoFound, "after deleting ");
            }
        }
    }

    return bBackupApplies;
}

//------------------------------------------------------------------------------
// Backup entire contents of HWM file for the specified columnOID,dbRoot,etc,
// so that the file is available for bulk rollback.  This function is used for
// HDFS files only.  This operation is only performed for compressed columns.
//
// This function MUST be kept thread-safe in support of backupDctnryHWMChunk().
// See that function description for more details.  This is the reason
// backupHWMChunk() has to have a local FileOp object.  We can't share/reuse
// a FileOp data member variable unless we want to employ a mutex.
//------------------------------------------------------------------------------
// @bug 5572 - Stopped using backupHWMFile().
// Don't need db backup file for HDFS; we use hdfs buffer file
void RBMetaWriter::backupHWMFile(
    bool      bColumnFile, // is this a column (vs dictionary) file
    OID       columnOID,   // OID of column or dictionary store
    uint16_t  dbRoot,      // DB Root for db segment file
    uint32_t  partition,   // partition for db segment file
    uint16_t  segment,     // segment for db segment file
    HWM       startingHWM) // starting HWM for db segment file
{
    std::string fileType("column");
    if (!bColumnFile)
        fileType = "dictionary";

    FileOp fileOp; // @bug 4960: to keep thread-safe, we use local FileOp

    // Construct file name for db file to be backed up
    char dbFileName[FILE_NAME_SIZE];
    int rc = fileOp.getFileName( columnOID, dbFileName,
        dbRoot, partition, segment );
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "Error creating backup "   << fileType <<
            " file for OID " << columnOID <<
            "; Can't construct file name for DBRoot"  << dbRoot <<
            "; partition-"   << partition <<
            "; segment-"     << segment;
        throw WeException( oss.str(), rc );
    }    

    // Construct file name for backup copy of db file
    std::ostringstream ossFile;
    ossFile << "/" << columnOID << ".p" << partition << ".s" << segment;
    std::string backupFileName;
    rc = getSubDirPath( dbRoot, backupFileName );
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "Error creating backup "   << fileType <<
            " file for OID " << columnOID <<
            "; Can't find matching meta file for DBRoot" << dbRoot;
        throw WeException( oss.str(), rc );
    }
    backupFileName += ossFile.str();

    std::string backupFileNameTmp = backupFileName;
    backupFileNameTmp += TMP_FILE_SUFFIX;

    //if ( (fLog) && (fLog->isDebug(DEBUG_1)) )
    if (fLog)
    {
        std::ostringstream oss;
        oss << "Backing up HWM file for "      << fileType <<
            " file for OID " << columnOID      <<
            "; file-"     << backupFileNameTmp <<
            "; HWM-"      << startingHWM;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    // Copy the db file to a temporary name
    IDBFileSystem& fs = IDBPolicy::getFs( backupFileNameTmp.c_str() );

    if ( !fs.exists(dbFileName) ) 
    {
        std::ostringstream oss; 
        oss << "Error creating backup "   << fileType <<
            " file for OID " << columnOID <<
            "; dbfile does not exist for DBRoot" << dbRoot <<
            "; partition-"   << partition <<
            "; segment-"     << segment;
        throw WeException( oss.str(), ERR_FILE_NOT_EXIST );
    }

    rc = fs.copyFile( dbFileName, backupFileNameTmp.c_str() );
    if (rc != 0)
    {
        std::ostringstream oss;
        oss << "Error copying backup for " << fileType <<
            " OID-"        << columnOID <<
            "; DBRoot-"    << dbRoot    <<
            "; partition-" << partition <<
            "; segment-"   << segment   <<
            "; rc-"        << rc;

        fs.remove( backupFileNameTmp.c_str() );
        throw WeException( oss.str(), ERR_METADATABKUP_COMP_WRITE_BULK_BKUP );
    }

    // Rename temporary named backup file to final name
    rc = fs.rename( backupFileNameTmp.c_str(), backupFileName.c_str() );
    if (rc != 0)
    {
        std::ostringstream oss;
        oss << "Error renaming temp backup for " << fileType <<
            " OID-"        << columnOID <<
            "; DBRoot-"    << dbRoot    <<
            "; partition-" << partition <<
            "; segment-"   << segment   <<
            "; rc-"        << rc;

        fs.remove( backupFileNameTmp.c_str() );
        fs.remove( backupFileName.c_str() );
        throw WeException( oss.str(), ERR_METADATABKUP_COMP_RENAME );
    }
}

//------------------------------------------------------------------------------
// Backup the contents of the HWM chunk for the specified columnOID,dbRoot,etc,
// so that the chunk is available for bulk rollback.  This function is used for
// non-hdfs files.  This operation is only performed for compressed columns.
//
// This function MUST be kept thread-safe in support of backupDctnryHWMChunk().
// See that function description for more details.  This is the reason
// backupHWMChunk() has to have a local FileOp object.  We can't share/reuse
// a FileOp data member variable unless we want to employ a mutex.
//------------------------------------------------------------------------------
void RBMetaWriter::backupHWMChunk(
    bool      bColumnFile, // is this a column (vs dictionary) file
    OID       columnOID,   // OID of column or dictionary store
    uint16_t  dbRoot,      // DB Root for db segment file
    uint32_t  partition,   // partition for db segment file
    uint16_t  segment,     // segment for db segment file
    HWM       startingHWM) // starting HWM for db segment file
{
    std::string fileType("column");
    if (!bColumnFile)
        fileType = "dictionary";

    // Open the applicable database column segment file
    std::string segFile;
    FileOp fileOp; // @bug 4960: to keep thread-safe, we use local FileOp
    IDBDataFile* dbFile = fileOp.openFile( columnOID,
        dbRoot,
        partition,
        segment,
        segFile,
        "rb" );
    if ( !dbFile )
    {
        std::ostringstream oss;
        oss << "Backup error opening " << fileType <<
            " file for OID-" << columnOID <<
            "; DBRoot-"      << dbRoot    <<
            "; partition-"   << partition <<
            "; segment-"     << segment;
        throw WeException( oss.str(), ERR_FILE_OPEN );
    }

    // Get the size of the file, so we know where to truncate back to.
    long long fileSizeBytes;
    int rc = fileOp.getFileSize( dbFile, fileSizeBytes);
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Backup error getting file size for " << fileType <<
            " OID-"        << columnOID <<
            "; DBRoot-"    << dbRoot    <<
            "; partition-" << partition <<
            "; segment-"   << segment   <<
            "; " << ec.errorString(rc);
        fileOp.closeFile( dbFile );
        throw WeException( oss.str(), rc );
    }

    // Read Control header
    char controlHdr[ IDBCompressInterface::HDR_BUF_LEN ];
    rc = fileOp.readFile( dbFile, (unsigned char*)controlHdr,
        IDBCompressInterface::HDR_BUF_LEN );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Backup error reading " << fileType <<
            " file control hdr for OID-" << columnOID <<
            "; DBRoot-"          << dbRoot    <<
            "; partition-"       << partition <<
            "; segment-"         << segment   <<
            "; " << ec.errorString(rc);
        fileOp.closeFile( dbFile );
        throw WeException( oss.str(), rc );
    }

    IDBCompressInterface compressor;
    int rc1 = compressor.verifyHdr( controlHdr );
    if (rc1 != 0)
    {
        rc = ERR_METADATABKUP_COMP_VERIFY_HDRS;

        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Backup error verifying " << fileType <<
            " file control hdr for OID-" << columnOID <<
            "; DBRoot-"          << dbRoot    <<
            "; partition-"       << partition <<
            "; segment-"         << segment   <<
            "; " << ec.errorString(rc)        <<
            "; rc: "             << rc1;
        fileOp.closeFile( dbFile );
        throw WeException( oss.str(), rc );
    }

    // Read Pointer header data
    uint64_t hdrSize    = compressor.getHdrSize(controlHdr);
    uint64_t ptrHdrSize = hdrSize - IDBCompressInterface::HDR_BUF_LEN;
    char* pointerHdr    = new char[ptrHdrSize];
    rc = fileOp.readFile( dbFile, (unsigned char*)pointerHdr, ptrHdrSize );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Backup error reading " << fileType <<
            " file pointer hdr for OID-" << columnOID <<
            "; DBRoot-"          << dbRoot    <<
            "; partition-"       << partition <<
            "; segment-"         << segment   <<
            "; " << ec.errorString(rc);
        delete[] pointerHdr;
        fileOp.closeFile( dbFile );
        throw WeException( oss.str(), rc );
    }

    CompChunkPtrList     chunkPtrs;
    rc = compressor.getPtrList(pointerHdr, ptrHdrSize, chunkPtrs );
    delete[] pointerHdr;
    if (rc != 0)
    {
        std::ostringstream oss;
        oss << "Backup error getting " << fileType <<
            " file hdr for OID-" << columnOID <<
            "; DBRoot-"          << dbRoot    <<
            "; partition-"       << partition <<
            "; segment-"         << segment;
        fileOp.closeFile( dbFile );
        throw WeException( oss.str(), ERR_METADATABKUP_COMP_PARSE_HDRS );
    }

    // Locate HWM chunk
    unsigned int chunkIndex             = 0;
    unsigned int blockOffsetWithinChunk = 0;
    unsigned char* buffer               = 0;
    uint64_t chunkSize                  = 0;
    compressor.locateBlock(startingHWM, chunkIndex, blockOffsetWithinChunk);

    if (chunkIndex < chunkPtrs.size())
    {
        chunkSize = chunkPtrs[chunkIndex].second;

        // Read the HWM chunk
        rc = fileOp.setFileOffset(dbFile,chunkPtrs[chunkIndex].first,SEEK_SET);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Backup error seeking in " << fileType <<
                " file for OID-" << columnOID <<
                "; DBRoot-"      << dbRoot    <<
                "; partition-"   << partition <<
                "; segment-"     << segment   <<
                "; " << ec.errorString(rc);
            fileOp.closeFile( dbFile );
            throw WeException( oss.str(), rc );
        }

        buffer = new unsigned char[ chunkPtrs[chunkIndex].second ];
        rc = fileOp.readFile( dbFile, buffer, chunkPtrs[chunkIndex].second );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Backup error reading in " << fileType <<
                " file for OID-" << columnOID <<
                "; DBRoot-"      << dbRoot    <<
                "; partition-"   << partition <<
                "; segment-"     << segment   <<
                "; " << ec.errorString(rc);
            delete []buffer;
            fileOp.closeFile( dbFile );
            throw WeException( oss.str(), rc );
        }
    }
    else if (startingHWM == 0)
    {
        // Okay to proceed.  Empty file with no chunks.  Save 0 length chunk.
    }
    else
    {
        rc = ERR_METADATABKUP_COMP_CHUNK_NOT_FOUND;

        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Backup error for " << fileType <<
            " file for OID-" << columnOID <<
            "; DBRoot-"      << dbRoot    <<
            "; partition-"   << partition <<
            "; segment-"     << segment   <<
            "; hwm-"         << startingHWM <<
            "; chunkIdx-"    << chunkIndex  <<
            "; numPtrs-"     << chunkPtrs.size() <<
            "; not in hdrPtrs"  <<
            "; " << ec.errorString(rc);
        fileOp.closeFile( dbFile );
        throw WeException( oss.str(), rc );
    }

    // Backup the HWM chunk
    std::string errMsg;
    rc = writeHWMChunk(bColumnFile, columnOID, dbRoot, partition, segment,
        buffer, chunkSize, fileSizeBytes, startingHWM, errMsg);
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "Backup error writing backup for " << fileType <<
            " OID-"        << columnOID <<
            "; DBRoot-"    << dbRoot    <<
            "; partition-" << partition <<
            "; segment-"   << segment   <<
            "; "           << errMsg;
        delete []buffer;
        fileOp.closeFile( dbFile );
        throw WeException( oss.str(), rc );
    }
    
    // Close the applicable database column segment file and free memory
    delete []buffer;
    fileOp.closeFile( dbFile );
}

//------------------------------------------------------------------------------
// Writes out the specified HWM chunk to disk, in case we need it for bulk
// rollback.  If an error occurs, errMsg will contain the error message.
// This function is careful not to create a corrupt file (should the system
// crash in the middle of writing the file for example).  It's imperative
// that during a failure of any kind, that we not "accidentally" create and
// leave around a corrupt or incomplete HWM backup file that could cause a
// bulk rollback to fail, and eventually corrupt a data base file.
// So this function first creates the HWM backup file to a temp file, and
// after it is successfully created, it is it renamed to the final destination.
// If anything goes wrong, we try to delete any files we were creating.
//
// This function MUST be kept thread-safe in support of backupDctnryHWMChunk().
// See that function description for more details.
//------------------------------------------------------------------------------
int RBMetaWriter::writeHWMChunk(
    bool                 bColumnFile, // is this a column (vs dictionary) file
    OID                  columnOID,   // OID of column or dictionary store
    uint16_t             dbRoot,      // dbroot for db segment file
    uint32_t             partition,   // partition for db segment file
    uint16_t             segment,     // segment for db segment file
    const unsigned char* compressedOutBuf, // compressed chunk to be written
    uint64_t             chunkSize,   // number of bytes in compressedOutBuf
    uint64_t             fileSize,    // size of file in bytes
    HWM                  chunkHWM,    // HWM in the chunk being written
    std::string&         errMsg) const// error msg if error occurs
{
    std::ostringstream ossFile;
    ossFile << "/" << columnOID << ".p" << partition << ".s" << segment;
    std::string fileName;
    int rc = getSubDirPath( dbRoot, fileName );
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "Error creating backup file for OID " << columnOID <<
            "; Can't find matching meta file for DBRoot" << dbRoot;
        errMsg = oss.str();
        return ERR_METADATABKUP_COMP_OPEN_BULK_BKUP;
    }
    fileName += ossFile.str();

    std::string fileNameTmp = fileName;
    fileNameTmp += TMP_FILE_SUFFIX;

    //if ( (fLog) && (fLog->isDebug(DEBUG_1)) )
    if (fLog)
    {
        std::string fileType("column");
        if (!bColumnFile)
            fileType = "dictionary";

        std::ostringstream oss;
        oss << "Backing up HWM chunk for " << fileType <<
            " OID-"       << columnOID <<
            "; file-"     << fileNameTmp <<
            "; HWM-"      << chunkHWM    <<
            "; bytes-"    << chunkSize   <<
            "; fileSize-" << fileSize;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    IDBDataFile*  backupFile = IDBDataFile::open(
        IDBPolicy::getType( fileNameTmp.c_str(), IDBPolicy::WRITEENG ),
        fileNameTmp.c_str(),
        "w+b",
        0 );
    if (!backupFile)
    {
        int errRc = errno;
        WErrorCodes ec;
        std::ostringstream oss;
        std::string eMsg;
        Convertor::mapErrnoToString(errRc, eMsg);
        oss << ec.errorString(ERR_METADATABKUP_COMP_OPEN_BULK_BKUP) <<
            "; " << eMsg;
        errMsg = oss.str();
        return ERR_METADATABKUP_COMP_OPEN_BULK_BKUP;
    }

    IDBFileSystem& fs = IDBPolicy::getFs( fileNameTmp.c_str() );

    // Format of backup compressed chunk file:
    //   8 byte unsigned int carrying chunk size
    //   8 byte unsigned int carrying original file size
    //   N bytes containing compressed chunk
    uint64_t sizeHdr[2];
    sizeHdr[0] = chunkSize;
    sizeHdr[1] = fileSize;
    size_t itemsWritten = backupFile->write(sizeHdr, sizeof(uint64_t)*2) / (sizeof(uint64_t)*2);
    if (itemsWritten != 1)
    {
        int errRc = errno;
        WErrorCodes ec;
        std::ostringstream oss;
        std::string eMsg;
        Convertor::mapErrnoToString(errRc, eMsg);
        oss << ec.errorString(ERR_METADATABKUP_COMP_WRITE_BULK_BKUP) <<
            "; " << eMsg;
        errMsg = oss.str();

        delete backupFile;
        fs.remove( fileNameTmp.c_str() );
        return ERR_METADATABKUP_COMP_WRITE_BULK_BKUP;
    }

    if (chunkSize > 0)
    {
        itemsWritten = backupFile->write(compressedOutBuf, chunkSize ) / chunkSize;
        if (itemsWritten != 1)
        {
            int errRc = errno;
            WErrorCodes ec;
            std::ostringstream oss;
            std::string eMsg;
            Convertor::mapErrnoToString(errRc, eMsg);
            oss << ec.errorString(ERR_METADATABKUP_COMP_WRITE_BULK_BKUP) <<
                "; " << eMsg;
            errMsg = oss.str();

            delete backupFile;
            fs.remove( fileNameTmp.c_str() );
            return ERR_METADATABKUP_COMP_WRITE_BULK_BKUP;
        }
    }

    backupFile->flush();
//   IDBDataFile flush() does a sync where appropriate
    delete backupFile;

#ifdef _MSC_VER
    //Windows rename() behaves differently from Linux: it will return an error
    // if the target exists
    //FIXME: The Linux version seems a bit safer, perhaps implement a better
    // Windows port?
    unlink(fileName.c_str());
#endif

    // Rename HWM backup file to final name.
    if ( fs.rename(fileNameTmp.c_str(), fileName.c_str()) )
    {
        int errRc = errno;
        WErrorCodes ec;
        std::ostringstream oss;
        std::string eMsg;
        Convertor::mapErrnoToString(errRc, eMsg);
        oss << ec.errorString(ERR_METADATABKUP_COMP_RENAME) << "; " << eMsg;
        errMsg = oss.str();

        fs.remove( fileNameTmp.c_str() );
        fs.remove( fileName.c_str() );
        return ERR_METADATABKUP_COMP_RENAME;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Returns the directory path to be used for storing any backup data files.
//
// This function MUST be kept thread-safe in support of backupDctnryHWMChunk().
// See that function description for more details.
//------------------------------------------------------------------------------
int RBMetaWriter::getSubDirPath( uint16_t dbRoot,
    std::string& bulkRollbackSubPath ) const
{
    std::map<uint16_t,std::string>::const_iterator iter =
        fMetaFileNames.find( dbRoot );
    if (iter == fMetaFileNames.end())
    {
        return ERR_INVALID_PARAM;
    }
    bulkRollbackSubPath  = iter->second;
    bulkRollbackSubPath += DATA_DIR_SUFFIX;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Prints list of compressed dictionary HWM chunks that we are tracking,
// in order to backup to disk as needed, before we start adding rows to a
// previously existing chunk.
//------------------------------------------------------------------------------
void RBMetaWriter::printDctnryChunkList(
    const RBChunkInfo& rbChk, 
    const char* assocAction)
{
    if (fLog)
    {
        std::ostringstream oss;
        oss << "Dumping metaDictHWMChunks " << assocAction <<
            rbChk << ":";

        if (fRBChunkDctnrySet.size() > 0)
        {
            RBChunkSet::iterator iter = fRBChunkDctnrySet.begin();
            int k = 1;
            while (iter != fRBChunkDctnrySet.end())
            {
                oss << std::endl;
                oss << '\t' << k << ". " << *iter;

                ++k;
                ++iter;
            }
        }
        else
        {
            oss << std::endl;
            oss << '\t' << "Empty list";
        }
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }
}

//------------------------------------------------------------------------------
// Verify that specified string represents Version 3 file format
//------------------------------------------------------------------------------
/* static */
bool RBMetaWriter::verifyVersion3(const char* versionRec)
{
    if (strncmp(versionRec, VERSION3_REC, VERSION3_REC_LEN) == 0)
        return true;
    else
        return false;
}

//------------------------------------------------------------------------------
// Verify that specified string represents Version 4 file format
//------------------------------------------------------------------------------
/* static */
bool RBMetaWriter::verifyVersion4(const char* versionRec)
{
    if (strncmp(versionRec, VERSION4_REC, VERSION4_REC_LEN) == 0)
        return true;
    else
        return false;
}

//------------------------------------------------------------------------------
// Verify that specified record type is a Column1 record
//------------------------------------------------------------------------------
/* static */
 bool RBMetaWriter::verifyColumn1Rec(const char* recType)
{
    if (strncmp(recType, COLUMN1_REC, COLUMN1_REC_LEN) == 0)
        return true;
    else
        return false;
}

//------------------------------------------------------------------------------
// Verify that specified record type is a Column2 record
//------------------------------------------------------------------------------
/* static */
bool RBMetaWriter::verifyColumn2Rec(const char* recType)
{
    if (strncmp(recType, COLUMN2_REC, COLUMN2_REC_LEN) == 0)
        return true;
    else
        return false;
}

//------------------------------------------------------------------------------
// Verify that specified record type is a DStore1 record
//------------------------------------------------------------------------------
/* static */
bool RBMetaWriter::verifyDStore1Rec(const char* recType)
{
    if (strncmp(recType, DSTORE1_REC, DSTORE1_REC_LEN) == 0)
        return true;
    else
        return false;
}

//------------------------------------------------------------------------------
// Verify that specified record type is a DStore2 record
//------------------------------------------------------------------------------
/* static */
bool RBMetaWriter::verifyDStore2Rec(const char* recType)
{
    if (strncmp(recType, DSTORE2_REC, DSTORE2_REC_LEN) == 0)
        return true;
    else
        return false;
}

} // end of namespace
