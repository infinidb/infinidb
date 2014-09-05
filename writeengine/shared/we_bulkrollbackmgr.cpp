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
* $Id: we_bulkrollbackmgr.cpp 4450 2013-01-21 14:13:24Z rdempsey $
*/

#include <sstream>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <boost/scoped_ptr.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>

#define WRITEENGINEBULKROLLMGR_DLLEXPORT
#include "we_bulkrollbackmgr.h"
#undef WRITEENGINEBULKROLLMGR_DLLEXPORT

#include "we_define.h"
#include "we_brm.h"
#include "we_config.h"
#include "we_fileop.h"
#include "we_log.h"
#include "we_bulkrollbackfile.h"
#include "we_bulkrollbackfilecompressed.h"
#include "messageids.h"
#include "cacheutils.h"

using namespace execplan;

namespace
{
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
    const char* DATA_DIR_SUFFIX = "_data";
    const char* DBROOT_BULK_ROLLBACK_SUBDIR = "bulkRollback";
    const char* TMP_FILE_SUFFIX = ".tmp";

    const int BUF_SIZE = 1024;  // size of buffer used to read meta data records

    const std::string DB_FILE_PREFIX   ("FILE");
    const std::string DB_FILE_EXTENSION(".cdf");
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// BulkRollbackMgr constructor
//
// tableOID - OID of the table to be rolled back.
//------------------------------------------------------------------------------
BulkRollbackMgr::BulkRollbackMgr ( OID tableOID,
    u_int64_t lockID,
    const std::string& tableName,
    const std::string& applName, Log* logger ) :
    fTableOID(tableOID),
    fLockID(lockID),
    fTableName(tableName),
    fProcessId(0),
    fPendingDctnryStoreOID(0),
    fPendingDctnryStoreDbRoot(0),
    fSysLogger( logging::LoggingID( SUBSYSTEM_ID_WE ) ),
    fDebugConsole( false ),
    fLog(logger),
    fApplName(applName),
    fVersion(4)
{
}

//------------------------------------------------------------------------------
// Rolls back the state of the extentmap and database files for the table OID
// specified to the constructor, using the previously written meta-data file.
// The requiredMetaFile flag indicates whether a missing bulk rollback file
// should be considered an error or not; should probably only be 'true' if
// rolling back during a failed mode3 import, where we know when a metadata
// backup file has been created.
//
// returns:
//   NO_ERROR if rollback completed successfully
//------------------------------------------------------------------------------
int BulkRollbackMgr::rollback ( bool keepMetaFile )
{
    logAMessage( logging::LOG_TYPE_INFO,
        logging::M0084, 0, fApplName );

    int rc = NO_ERROR;
    int dbRootRollbackCount = 0;
    try
    {
        // validate that BRM is in read/write mode so we can update extentmap
        rc = BRMWrapper::getInstance()->isReadWrite();
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Bulk rollback for table " << fTableName << " (OID-" <<
                fTableOID << ") not performed; " << ec.errorString(rc);

            throw WeException( oss.str(), rc );
        }

        std::vector<u_int16_t> dbRoots;
        Config::getRootIdList( dbRoots );
        
        std::string emptyText0072;
        logAMessage( logging::LOG_TYPE_INFO,
            logging::M0072, 0, emptyText0072 );

        // Loop through DBRoots for this PM
        for (unsigned m=0; m<dbRoots.size(); m++)
        {
            bool bPerformRollback = openMetaDataFile ( dbRoots[m] );

            // Call function to:
            // 1. read bulk rollback meta-data file
            // 2. rollback applicable extents from extentmap
            // 3. delete applicable extents from database files
            // 4. reinitialize blocks trailing the HWM block in the last extent
            //    of each segment file
            // ...
            if (bPerformRollback)
            {
                dbRootRollbackCount++;
                deleteExtents ( );
                closeMetaDataFile ( );
            }
            else // Skip any DBRoot not having a meta-data file
            {
                std::ostringstream msg0090Text;
                msg0090Text << dbRoots[m];
                logAMessage( logging::LOG_TYPE_INFO,
                    logging::M0090, 0, msg0090Text.str() );
            }
        }

        if (dbRootRollbackCount > 0)
        {
            // Notify PrimProc to flush it's cache.  If error occurs, we tell
            // the user but keep going but warn the user.
            int cache_rc = cacheutils::flushPrimProcCache();
            if (cache_rc != 0)
            {
                std::ostringstream oss;
                oss << "ClearTableLock: Error flushing "
                    "PrimProc cache after rolling back data for table " <<
                    fTableName <<
                    " (OID-" << fTableOID << ");  rc-" << cache_rc;

                // If we have a logger, then use it to log to syslog, etc
                if (fLog)
                {
                    fLog->logMsg( oss.str(), MSGLVL_ERROR );
                }
                else // log message ourselves
                {
                    std::cout << oss.str() << std::endl;

                    logging::Message m( logging::M0010 );
                    logging::Message::Args args;
                    args.add( oss.str() );
                    m.format( args );
                    fSysLogger.logErrorMessage( m );
                }
            }
        }
    }
    catch (WeException& ex)
    {
        std::string msgText(fApplName);
        msgText += ". (rollback failed; ";
        msgText += ex.what();
        msgText += ')';
        logAMessage( logging::LOG_TYPE_ERROR,
            logging::M0085, 0, msgText );

        fErrorMsg = ex.what();
        return ex.errorCode();
    }

    if (!keepMetaFile)
        deleteMetaDataFiles ( );

    if (dbRootRollbackCount > 0)
    {
        logAMessage( logging::LOG_TYPE_INFO,
            logging::M0085, 0, fApplName );
    }
    else
    {
        std::string msgText(fApplName);
        msgText += ". (Nothing to rollback)";
        logAMessage( logging::LOG_TYPE_INFO,
            logging::M0085, 0, msgText );
    }

    return rc;
}

//------------------------------------------------------------------------------
// Validate that all the bulk rollback meta files on all the local DBRoots
// exist.  This should apply for a cpimport.bin mode3 import for example.
// A mode1 distributed import on the other hand, might not have gotten far
// enough to have created a bulk rollback meta file on every PM, so
// validateAllMetaFilesExist() should probably only be called within the
// context of a mode3 import that has entered rollback mode.  In any other
// case, a missing bulk rollback meta file might be explainable.
//
// @bug 4496 3.0 Failover
// NOTE: Stopped using this function with 3.0, when it became possible for
//       DBRoots to move from 1 PM to another, in the middle of the job.
//       We simply perform a bulk rollback for any meta-data file we find,
//       and skip performing a bulk rollback for a dbroot that does not
//       have a meta-data file.  Kept the function around for the time being.
//------------------------------------------------------------------------------
void BulkRollbackMgr::validateAllMetaFilesExist (
    const std::vector<u_int16_t>& dbRoots ) const
{
    // Loop through DBRoots for this PM
    for (unsigned m=0; m<dbRoots.size(); m++)
    {
        std::string bulkRollbackPath( Config::getDBRootByNum(dbRoots[m]) );

        // Construct file name and check for it's existence
        std::ostringstream oss;
        oss << '/' << DBROOT_BULK_ROLLBACK_SUBDIR << '/' << fTableOID;
        std::string metaFileName  = bulkRollbackPath;
        metaFileName += oss.str();

        struct stat curStat;
        if ( stat(metaFileName.c_str(), &curStat) != 0 )
        {
            std::ostringstream oss;
            oss << "Error opening bulk rollback meta-data file " <<
                metaFileName << "; File does not exist.";
    
            throw WeException( oss.str(), ERR_FILE_OPEN );
        }
    }
}

//------------------------------------------------------------------------------
// Open the meta-data file for fTableOID.  File contains information used in
// rolling back the table to a previous state.
// Returns true/false to indicate whether execution should continue if the
// meta-data file is missing.
//------------------------------------------------------------------------------
bool BulkRollbackMgr::openMetaDataFile ( u_int16_t dbRoot )
{
    std::string bulkRollbackPath( Config::getDBRootByNum( dbRoot ) );

    // Construct file name and check for it's existence
    std::ostringstream oss;
    oss << '/' << DBROOT_BULK_ROLLBACK_SUBDIR << '/' << fTableOID;
    fMetaFileName  = bulkRollbackPath;
    fMetaFileName += oss.str();

    // Return if the meta-data file does not exist.  This could happen if we
    // are executing distributed rollback on several PMs, some of which may
    // have not even executed an import.
    // Also could happen if DBRoots are moved from 1 PM to another during a job.
    struct stat curStat;
    if ( stat(fMetaFileName.c_str(), &curStat) != 0 )
    {
        return false;
    }

    // Open the file
    fMetaFile.open( fMetaFileName.c_str() );
    if ( fMetaFile.fail() )
    {
        int errRc = errno;
        std::ostringstream oss;
        oss << "Error opening bulk rollback meta-data file " <<
            fMetaFileName << "; err-" <<
            errRc << "; " << strerror( errRc );
    
        throw WeException( oss.str(), ERR_FILE_OPEN );
    }

    fMetaFileNames.push_back( fMetaFileName );

    // First record in the file must be a Version record.
    char  inBuf[ BUF_SIZE ];
    fMetaFile.getline( inBuf, BUF_SIZE );
    if (strncmp(inBuf, VERSION3_REC, VERSION3_REC_LEN) == 0)
    {
        fVersion = 3;
    }
    else if (strncmp(inBuf, VERSION4_REC, VERSION4_REC_LEN) == 0)
    {
        fVersion = 4;
    }
    else
    {
        std::ostringstream oss;
        oss << "Invalid version record in meta-data file " << fMetaFileName
            << "; record-<" << inBuf << ">" <<std::endl;
    
        throw WeException( oss.str(), ERR_INVALID_PARAM );
    }

    return true;
}

//------------------------------------------------------------------------------
// Close the current meta-data file used in rolling back fTableOID.
//------------------------------------------------------------------------------
void BulkRollbackMgr::closeMetaDataFile ( )
{
    if (fMetaFile.is_open())
        fMetaFile.close( );
}

//------------------------------------------------------------------------------
// Delete all the local meta-data files used in rolling back fTableOID.
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteMetaDataFiles ( )
{
    for (unsigned k=0; k<fMetaFileNames.size(); k++)
    {
        unlink( fMetaFileNames[k].c_str() );

        // Unlink corresponding tmp file created by RBMetaWriter.
        std::string tmpMetaFileName = fMetaFileNames[k];
        tmpMetaFileName += TMP_FILE_SUFFIX;
        unlink( tmpMetaFileName.c_str() );

        deleteSubDir( fMetaFileNames[k] );
    }
}

//------------------------------------------------------------------------------
// Delete the subdirectory used to backup data needed for rollback.
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteSubDir( const std::string& metaFileName )
{
    std::string bulkRollbackSubPath( metaFileName );
    bulkRollbackSubPath += DATA_DIR_SUFFIX;
    boost::filesystem::path dirPath(bulkRollbackSubPath);

    // Delete bulk rollback data subdirectory
    try
    {
        boost::filesystem::remove_all(dirPath);
    }
    catch (std::exception& ex)
    {
        std::string boostErrString = ex.what();
        std::ostringstream oss;
        oss << "Warning: Error deleting bulk rollback data subdirectory " <<
            bulkRollbackSubPath << "; " << boostErrString;
        if (fLog)
            fLog->logMsg( oss.str(), MSGLVL_WARNING );
        else
            std::cout << oss.str() << std::endl;
    }
}

//------------------------------------------------------------------------------
// Function that drives the rolling back or deletion of extents for a given
// database table as specified in a meta-data bulk rollback file.
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteExtents ( )
{
    char  inBuf[ BUF_SIZE ];
    OID   columnOID     = 0;
    OID   storeOID      = 0;
    u_int32_t dbRoot    = 0;

    // Loop through the records in the meta-data file
    while (fMetaFile.getline( inBuf, BUF_SIZE ))
    {
        // Restore extents for a DBRoot
        if (strncmp(inBuf, COLUMN1_REC, COLUMN1_REC_LEN) == 0)
        {
            // Process any pending dictionary deletes
            if (fPendingDctnryExtents.size() > 0)
            {
                deleteDctnryExtents ( );
                deleteDbFiles( );
            }

            deleteColumn1Extents ( inBuf );
            deleteDbFiles( );
        }
        // Delete all extents from a formerly empty DBRoot
        else if (strncmp(inBuf, COLUMN2_REC, COLUMN2_REC_LEN) == 0)
        {
            // Process any pending dictionary deletes
            if (fPendingDctnryExtents.size() > 0)
            {
                deleteDctnryExtents ( );
                deleteDbFiles( );
            }

            deleteColumn2Extents ( inBuf );
            deleteDbFiles( );
        }
        else if ((strncmp(inBuf, DSTORE1_REC, DSTORE1_REC_LEN) == 0) ||
                 (strncmp(inBuf, DSTORE2_REC, DSTORE2_REC_LEN) == 0))
        {
            if (fPendingDctnryExtents.size() > 0)
            {
                char recType[100];
                int numFields = sscanf(
                    inBuf, "%s %u %u %d",
                    recType, &columnOID, &storeOID, &dbRoot );
                if (numFields != 4)
                {
                    std::ostringstream oss;
                    oss << "Invalid record in meta-data file " << fMetaFileName
                        << "; record-<" << inBuf << ">" <<std::endl;
    
                    throw WeException( oss.str(), ERR_INVALID_PARAM );
                }

                // Process any pending dictionary deletes
                if ((storeOID != fPendingDctnryStoreOID) ||
                    (dbRoot   != fPendingDctnryStoreDbRoot))
                {
                    deleteDctnryExtents ( );
                    deleteDbFiles( );
                }
            }

            readMetaDataRecDctnry ( inBuf );
        }
        else
        {
            // ignore unrecognized record type
        }
    } // end of loop through all the records in the meta-data input file

    // Process any pending dictionary deletes
    if (fPendingDctnryExtents.size() > 0)
    {
        deleteDctnryExtents ( );
        deleteDbFiles( );
    }
}

//------------------------------------------------------------------------------
// Read a meta-data dictionary record (DSTORE1 or DSTORE2) from meta-data file.
// Each record specifies the rollback point for a given dbroot, partition,
// segment number, and HWM for a certain dictionary store OID.
//
// inBuf - latest dictionary extent record to be parsed from meta-data file
//------------------------------------------------------------------------------
void BulkRollbackMgr::readMetaDataRecDctnry ( const char* inBuf )
{
    char      recType[100];
    OID       dColumnOID;
    OID       dStoreOID;
    u_int32_t dbRootHwm;
    u_int32_t partNumHwm;
    u_int32_t segNumHwm;
    HWM       localHwm;
    int       compressionType = 0; // optional parameter

    sscanf(inBuf, "%s", recType);
    RollbackData rbData;

    // Process DSTORE1 records representing segment files with an HWM
    if ((strncmp(recType, DSTORE1_REC, DSTORE1_REC_LEN) == 0))
    {
        int numFields = sscanf(inBuf, "%s %u %u %u %u %u %u %d",
            recType, &dColumnOID, &dStoreOID,
            &dbRootHwm, &partNumHwm, &segNumHwm, &localHwm, &compressionType );

        if (numFields < 7) // compressionType optional
        {
            std::ostringstream oss;
            oss << "Invalid DSTOR1 record in meta-data file " <<
                fMetaFileName << "; record-<" << inBuf << ">" << std::endl;

            throw WeException( oss.str(), ERR_INVALID_PARAM );
        }

        rbData.fWithHwm = true;
        rbData.fHwm     = localHwm;
    }

    // Process DSTORE2 records representing segment files w/o HWM; meaning that
    // segment file did not previously exist and can be deleted if it was added
    else
    {
        int numFields = sscanf(inBuf, "%s %u %u %u %u %u %d",
            recType, &dColumnOID, &dStoreOID,
            &dbRootHwm, &partNumHwm, &segNumHwm, &compressionType );

        if (numFields < 6) // compressionType optional
        {
            std::ostringstream oss;
            oss << "Invalid DSTOR2 record in meta-data file " <<
                fMetaFileName << "; record-<" << inBuf << ">" << std::endl;

            throw WeException( oss.str(), ERR_INVALID_PARAM );
        }

        rbData.fWithHwm = false;
        rbData.fHwm     = 0;
    }

    rbData.fDbRoot  = dbRootHwm;
    rbData.fPartNum = partNumHwm;
    rbData.fSegNum  = segNumHwm;

    fPendingDctnryExtents.push_back( rbData );

    // OID and compression type should be the same for all store files relating
    // to the same dictionary column, but they are set for each record nonethe-
    // less since the format of the meta data file is a flat file format.
    // Likewise, the DBRoot will be the same for each collection of dictionary
    // extents that are processed as a group.
    fPendingDctnryStoreOID             = dStoreOID;
    fPendingDctnryStoreCompressionType = compressionType;
    fPendingDctnryStoreDbRoot          = dbRootHwm;
}

//------------------------------------------------------------------------------
// Delete column extents based on COLUMN1 record input
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteColumn1Extents ( const char* inBuf )
{
    if (fVersion == 3)
        deleteColumn1ExtentsV3( inBuf );
    else
        deleteColumn1ExtentsV4( inBuf );
}

//------------------------------------------------------------------------------
// Delete column extents based on COLUMN2 record input
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteColumn2Extents ( const char* inBuf )
{
    if (fVersion == 3)
        deleteColumn2ExtentsV3( inBuf );
    else
        deleteColumn2ExtentsV4( inBuf );
}

//------------------------------------------------------------------------------
// Delete all the column extents (from the extent map and the db files) that
// logically follow the HWM extent contained in inBuf; where inBuf is a
// COLUMN1 record read from a meta-data bulk rollback file.  This function
// is limited to rolling back the extent changes to a specific DBRoot.
//
// inBuf - latest column extent record to be parsed from meta-data file
//
// This function exists to handle version3 metadata input files, in case a user:
// 1. ungracefully took their system down with a table lock in place
// 2. upgraded to a release that supported version4 (used to support the new
//    segment file numbering).
// 3. then brought infinidb up, and DMLProc triggered this function to execute
//    a bulk rollback (during system startup) using a leftover version3
//    formatted meta data file.
//
// In the case of a COLUMN1 record, V3 and V4 are handled the same, so this
// adaptor function is a pass-thru to the V4 function.
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteColumn1ExtentsV3 ( const char* inBuf )
{
    deleteColumn1ExtentsV4( inBuf );
}

//------------------------------------------------------------------------------
// Delete all the column extents (from the extent map and the db files) that
// fall in a certain DBRoot; restoring the DBRoot to an "empty" state. inBuf
// is a COLUMN2 record read from a meta-data bulk rollback file.  This function
// is limited to rolling back the extent changes to a specific DBRoot.
//
// inBuf - latest column extent record to be parsed from meta-data file
//
// This function exists to handle version3 metadata input files, in case a user:
// 1. ungracefully took their system down with a table lock in place
// 2. upgraded to a release that supported version4 (used to support the new
//    segment file numbering).
// 3. then brought infinidb up, and DMLProc triggered this function to execute
//    a bulk rollback (during system startup) using a leftover version3
//    formatted meta data file.
//
// With Version3, we always wrote the expected starting segment number for an
// empty DBRoot, in a COLUMN2 record.  With Version4, we always write a 0 for
// the starting segment number, because the starting segment number is undeter-
// mined.  So this adaptor function changes any segment number for the COLUMN2
// record to a 0, to be 100% compatible with the Version4 format.
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteColumn2ExtentsV3 ( const char* inBuf )
{
    char        recType[100];
    OID         columnOID;
    u_int32_t   dbRootHwm;
    u_int32_t   partNumHwm;
    u_int32_t   segNumHwm;
    int         colTypeInt;
    CalpontSystemCatalog::ColDataType colType;
    char        colTypeName[100];
    u_int32_t   colWidth;
    int         compressionType = 0; // optional parameter
    
    // Read meta-data record
    int numFields = sscanf(inBuf, "%s %u %u %u %u %d %s %u %d",
        recType, &columnOID,
        &dbRootHwm, &partNumHwm, &segNumHwm,
        &colTypeInt, colTypeName, &colWidth, &compressionType );
    colType = (CalpontSystemCatalog::ColDataType)colTypeInt;
    if (numFields < 8) // compressionType is optional
    {
        std::ostringstream oss;
        oss << "Invalid COLUM2 record in meta-data file " <<
            fMetaFileName << "; record-<" << inBuf << ">" << std::endl;

        throw WeException( oss.str(), ERR_INVALID_PARAM );
    }

    std::ostringstream revisedBuf;
    u_int32_t revisedSegNumHwm = 0;
    revisedBuf << recType     << ' ' <<
                  columnOID   << ' ' <<
                  dbRootHwm   << ' ' <<
                  partNumHwm  << ' ' <<
                  revisedSegNumHwm   << ' ' <<
                  colTypeInt  << ' ' <<
                  colTypeName << ' ' <<
                  colWidth    << ' ';
    if (numFields > 8)
        revisedBuf << compressionType;

    deleteColumn2ExtentsV4( revisedBuf.str().c_str() );
}

//@bug 4091: V4 support for adding DBRoot
//------------------------------------------------------------------------------
// Delete all the column extents (from the extent map and the db files) that
// logically follow the HWM extent contained in inBuf; where inBuf is a
// COLUMN1 record read from a meta-data bulk rollback file.  This function
// is limited to rolling back the extent changes to a specific DBRoot.
//
// inBuf - latest column extent record to be parsed from meta-data file
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteColumn1ExtentsV4 ( const char* inBuf )
{
    char        recType[100];
    OID         columnOID;
    u_int32_t   dbRootHwm;
    u_int32_t   partNumHwm;
    u_int32_t   segNumHwm;
    HWM         lastLocalHwm;
    int         colTypeInt;
    CalpontSystemCatalog::ColDataType colType;
    char        colTypeName[100];
    u_int32_t   colWidth;
    int         compressionType = 0; // optional parameter
    
    // Read meta-data record
    int numFields = sscanf(inBuf, "%s %u %u %u %u %u %d %s %u %d",
        recType, &columnOID,
        &dbRootHwm, &partNumHwm, &segNumHwm, &lastLocalHwm,
        &colTypeInt, colTypeName, &colWidth, &compressionType );
    colType = (CalpontSystemCatalog::ColDataType)colTypeInt;
    if (numFields < 9) // compressionType is optional
    {
        std::ostringstream oss;
        oss << "Invalid COLUM1 record in meta-data file " <<
            fMetaFileName << "; record-<" << inBuf << ">" << std::endl;

        throw WeException( oss.str(), ERR_INVALID_PARAM );
    }

    std::ostringstream msg0073Text;
    msg0073Text << "DBRoot-" << dbRootHwm << " (column extent)";
    logAMessage( logging::LOG_TYPE_INFO,
        logging::M0073, columnOID, msg0073Text.str() );

    // Delete extents from the extentmap
    std::ostringstream msg0074Text;
    msg0074Text << "Restoring HWM column extent: "
        "dbRoot-"  << dbRootHwm  <<
        "; part#-" << partNumHwm <<
        "; seg#-"  << segNumHwm  <<
        "; hwm-"   << lastLocalHwm;
    logAMessage( logging::LOG_TYPE_INFO,
        logging::M0074, columnOID, msg0074Text.str() );

    // Create the object responsible for restoring the extents in the db files.
    BulkRollbackFile* fileRestorer = 0;
    if (compressionType)
        fileRestorer = new BulkRollbackFileCompressed(this);
    else
        fileRestorer = new BulkRollbackFile(this);
    boost::scoped_ptr<BulkRollbackFile> refBulkRollbackFile(fileRestorer);

    int rc = BRMWrapper::getInstance()->rollbackColumnExtents_DBroot (
        columnOID,
        false,                  // false -> Don't delete all extents (rollback
        (u_int16_t)dbRootHwm,   //    to specified dbroot, partition, etc.)
        partNumHwm,
        (u_int16_t)segNumHwm,
        lastLocalHwm );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error rolling back column extents from extent map for " <<
            columnOID <<
            "; dbRoot-"    << dbRootHwm    <<
            "; partition-" << partNumHwm   <<
            "; segment-"   << segNumHwm    <<
            "; hwm-"       << lastLocalHwm <<
            "; "           << ec.errorString(rc);

        throw WeException( oss.str(), ERR_BRM_BULK_RB_COLUMN );
    }

    // Determine the exact rollback point for the extents we are rolling back to
    const unsigned BLKS_PER_EXTENT =
        (BRMWrapper::getInstance()->getExtentRows() * colWidth)/BYTE_PER_BLOCK;
    u_int32_t lastBlkOfCurrStripe = 0;
    u_int32_t lastBlkOfPrevStripe = 0;
    if ((lastLocalHwm + 1) <= BLKS_PER_EXTENT)
    {
        lastBlkOfCurrStripe = BLKS_PER_EXTENT - 1;
    }
    else
    {
        lastBlkOfPrevStripe = lastLocalHwm -
            (lastLocalHwm % BLKS_PER_EXTENT) - 1;
        lastBlkOfCurrStripe = lastBlkOfPrevStripe + BLKS_PER_EXTENT;
    }

    u_int32_t dbRoot  = dbRootHwm;
    u_int32_t partNum = partNumHwm;
    bool existsFlag   = true;
    std::string segFileListErrMsg;

    // Delete extents from the database files.
    // Loop through all partitions (starting with the HWM partition partNumHwm),
    // deleting or restoring applicable extents.  We stay in loop till we 
    // reach a partition that has no column segment files to roll back.
    while ( 1 )
    {
        std::vector<u_int32_t> segList;
        std::string dirName;
        rc = fileRestorer->buildDirName( columnOID, dbRoot, partNum, dirName );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Bulk rollback error constructing path for column " <<
                columnOID <<
                "; dbRoot-"    << dbRoot    <<
                "; partition-" << partNum   <<
                "; "           << ec.errorString(rc);

            throw WeException( oss.str(), rc );
        }

        rc = getSegFileList( dirName, segList, segFileListErrMsg );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Bulk rollback error for column " << columnOID <<
                "; directory-" << dirName <<
                "; " << segFileListErrMsg <<
                "; " << ec.errorString(rc);

            throw WeException( oss.str(), rc );
        }

        if (segList.size() == 0)
            break; // Exit loop when we reach empty partition

        for (unsigned int kk=0; kk<segList.size(); kk++)
        {
            u_int32_t segNum = segList[kk];

            if ( partNum == partNumHwm )
            {
                if ( segNum < segNumHwm )
                {
                    fileRestorer->truncateSegmentFile ( columnOID,
                        dbRoot,
                        partNum,
                        segNum,
                        (lastBlkOfCurrStripe + 1) );
                } // end of (segNum < segNumHwm)

                else if ( segNum > segNumHwm )
                {
                    if (lastBlkOfPrevStripe > 0)
                    {
                        fileRestorer->truncateSegmentFile ( columnOID,
                            dbRoot,
                            partNum,
                            segNum,
                            (lastBlkOfPrevStripe + 1) );
                    }
                    // lastBlkOfPrevStripe = 0, means there was no previous
                    // stripe in this partition.  The HWM block was in the
                    // first stripe.  In this case we can delete any segment
                    // files added to this partition that follow segNumHwm.
                    else
                    {
                        std::string segFileName;
                        fileRestorer->findSegmentFile ( columnOID,
                            true,    // column segment file
                            dbRoot,
                            partNum,
                            segNum,
                            existsFlag,
                            segFileName );
                        if (existsFlag)
                        {
                            createFileDeletionEntry( columnOID,
                                true, // column segment file
                                dbRoot, partNum, segNum, segFileName );
                        }
                    }
                } // end of (segNum > segNumHwm)

                else // segNum == segNumHwm
                {
                    if (lastBlkOfCurrStripe == lastLocalHwm)
                    { 
                        fileRestorer->truncateSegmentFile ( columnOID,
                            dbRoot,
                            partNum,
                            segNum,
                            (lastBlkOfCurrStripe + 1) );
                    }
                    else
                    {
                        bool restoreChunk =
                            fileRestorer->doWeReInitExtent(columnOID,
                            dbRoot, partNum, segNum);

                        // For compressed data, if there is no backup chunk to
                        // restore (restoreChunk is false), we still restore
                        // the compressed headers to their previous setting.
                        // This would happen if DBRoot HWM was not on full ex-
                        // tent boundary when it was moved.  If/when cpimport
                        // reaches this migrated DBRoot in the middle of an
                        // import, we do not create a backup chunk file.  We
                        // instead only fill-in the HWM extent with empty row
                        // markers.  So no backup and restore is necessary. Only
                        // need to truncate any extra extents that were added.

                        // Reinit last extent and truncate the remainder,
                        // starting with the next block following the HWM block.
                        fileRestorer->reInitTruncColumnExtent ( columnOID,
                            dbRoot,
                            partNum,
                            segNum,
                            (lastLocalHwm + 1),
                            (lastBlkOfCurrStripe - lastLocalHwm),
                            colType,
                            colWidth,
                            restoreChunk );
                    }
                } // end of (segNum == segNumHwm)
            }
            else // ( partNum > partNumHwm )
            {
                // Delete any files added to subsequent partitions
                std::string segFileName;
                fileRestorer->findSegmentFile ( columnOID,
                    true,    // column segment file
                    dbRoot,
                    partNum,
                    segNum,
                    existsFlag,
                    segFileName );
                if (existsFlag)
                {
                    createFileDeletionEntry( columnOID,
                        true, // column segment file
                        dbRoot, partNum, segNum, segFileName );
                }
            }
        } // loop thru all the potential segment files in a partition

        partNum++;

    } // end of loop to go thru all partitions till we find last segment file
}

//@bug 4091: V4 support for adding DBRoot
//------------------------------------------------------------------------------
// Delete all the column extents (from the extent map and the db files) that
// fall in a certain DBRoot; restoring the DBRoot to an "empty" state. inBuf
// is a COLUMN2 record read from a meta-data bulk rollback file.  This function
// is limited to rolling back the extent changes to a specific DBRoot.
//
// inBuf - latest column extent record to be parsed from meta-data file
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteColumn2ExtentsV4 ( const char* inBuf )
{
    char        recType[100];
    OID         columnOID;
    u_int32_t   dbRootHwm;
    u_int32_t   partNumHwm;
    u_int32_t   segNumHwm;
    HWM         lastLocalHwm = 0;
    int         colTypeInt;
    CalpontSystemCatalog::ColDataType colType;
    char        colTypeName[100];
    u_int32_t   colWidth;
    int         compressionType = 0; // optional parameter
    
    // Read meta-data record
    int numFields = sscanf(inBuf, "%s %u %u %u %u %d %s %u %d",
        recType, &columnOID,
        &dbRootHwm, &partNumHwm, &segNumHwm,
        &colTypeInt, colTypeName, &colWidth, &compressionType );
    colType = (CalpontSystemCatalog::ColDataType)colTypeInt;
    if (numFields < 8) // compressionType is optional
    {
        std::ostringstream oss;
        oss << "Invalid COLUM2 record in meta-data file " <<
            fMetaFileName << "; record-<" << inBuf << ">" << std::endl;

        throw WeException( oss.str(), ERR_INVALID_PARAM );
    }

    std::ostringstream msg0073Text;
    msg0073Text << "DBRoot-" << dbRootHwm << " (column extent)";
    logAMessage( logging::LOG_TYPE_INFO,
        logging::M0073, columnOID, msg0073Text.str() );

    // Delete extents from the extentmap
    std::ostringstream msg0074Text;
    msg0074Text << "Restoring empty DBRoot. "
        "dbRoot-"  << dbRootHwm  <<
        "; part#-" << partNumHwm <<
        "; seg#-"  << segNumHwm  <<
        "; hwm-"   << lastLocalHwm;
    logAMessage( logging::LOG_TYPE_INFO,
        logging::M0074, columnOID, msg0074Text.str() );

    // Create the object responsible for restoring the extents in the db files.
    BulkRollbackFile* fileRestorer = 0;
    if (compressionType)
        fileRestorer = new BulkRollbackFileCompressed(this);
    else
        fileRestorer = new BulkRollbackFile(this);
    boost::scoped_ptr<BulkRollbackFile> refBulkRollbackFile(fileRestorer);

    int rc = BRMWrapper::getInstance()->rollbackColumnExtents_DBroot (
        columnOID,
        true,           // true -> delete all extents (restore to empty DBRoot)
        (u_int16_t)dbRootHwm,
        partNumHwm,
        (u_int16_t)segNumHwm,
        lastLocalHwm );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error rolling back column extents from extent map for " <<
            columnOID <<
            "; dbRoot-"    << dbRootHwm    <<
            "; partition-" << partNumHwm   <<
            "; segment-"   << segNumHwm    <<
            "; hwm-"       << lastLocalHwm <<
            "; "           << ec.errorString(rc);

        throw WeException( oss.str(), ERR_BRM_BULK_RB_COLUMN );
    }

    u_int32_t dbRoot  = dbRootHwm;
    u_int32_t partNum = partNumHwm;
    bool existsFlag   = true;
    std::string segFileListErrMsg;

    // Delete extents from the database files.
    // Loop through all partitions (starting with the HWM partition partNumHwm),
    // deleting or restoring applicable extents.  We stay in loop till we 
    // reach a partition that has no column segment files to roll back.
    while ( 1 )
    {
        std::vector<u_int32_t> segList;
        std::string dirName;
        rc = fileRestorer->buildDirName( columnOID, dbRoot, partNum, dirName );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Bulk rollback error constructing path for column " <<
                columnOID <<
                "; dbRoot-"    << dbRoot    <<
                "; partition-" << partNum   <<
                "; "           << ec.errorString(rc);

            throw WeException( oss.str(), rc );
        }

        rc = getSegFileList( dirName, segList, segFileListErrMsg );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Bulk rollback error for column " << columnOID <<
                "; directory-" << dirName <<
                "; " << segFileListErrMsg <<
                "; " << ec.errorString(rc);

            throw WeException( oss.str(), rc );
        }

        if (segList.size() == 0)
            break; // Exit loop when we reach empty partition
        
        for (unsigned int kk=0; kk<segList.size(); kk++)
        {
            u_int32_t segNum = segList[kk];

            // Delete any files added to subsequent partitions
            std::string segFileName;
            fileRestorer->findSegmentFile ( columnOID,
                true,    // column segment file
                dbRoot,
                partNum,
                segNum,                
                existsFlag,
                segFileName );

            if (existsFlag)
            {
                createFileDeletionEntry( columnOID,
                    true, // column segment file
                    dbRoot, partNum, segNum, segFileName );
            }
        } // loop thru all the potential segment files in a partition

        partNum++;

    } // end of loop to go thru all partitions till we find last segment file
}

//------------------------------------------------------------------------------
// Delete dictionary store extents based on COLUMN1 record input
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteDctnryExtents ( )
{
    if (fVersion == 3)
        deleteDctnryExtentsV3( );
    else
        deleteDctnryExtentsV4( );
}

//------------------------------------------------------------------------------
// Delete all the dictionary store extents (from the extent map and db files)
// that logically follow the extents contained in fPendingDctnryExtents; where
// fPendingDctnryExtents is a vector of DSTORE1 and DSTORE2 records (for a
// specific column OID), read from a meta-data bulk rollback file.  This
// function is limited to rolling back the extent changes to a specific DBRoot.
//
// This function exists to handle version3 metadata input files, in case a user:
// 1. ungracefully took their system down with a table lock in place
// 2. upgraded to a release that supported ersion4 (used to support the new
//    segment file numbering).
// 3. then brought infinidb up, and DMLProc triggered this function to execute
//    a bulk rollback (during system startup) using a leftover version3
//    formatted meta data file.
//
// With Version3, we always wrote out DSTORE1 record for each existing store
// file, and a DSTORE2 record for each store file not present in the relevant
// partition.  For an empty DBRoot, a DSTORE2 record was written for all the
// store files not present in the relevant partition.
// With Version4, no trailing DSTORE2 records are written for the store files
// not present.  A Single DSTORE2 record is still written for an empty DBRoot.
// So this adaptor function strips out the unnecessary trailing DSTORE2
// records, to be compatible with the Version4 format.
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteDctnryExtentsV3 ( )
{
    for (unsigned i=0; i<fPendingDctnryExtents.size(); ++i)
    {
        if (!fPendingDctnryExtents[i].fWithHwm)
        {
            if (i == 0)
            {
                fPendingDctnryExtents[i].fSegNum = 0;
                fPendingDctnryExtents.resize(1);
            }
            else
            {
                fPendingDctnryExtents.resize(i);
            }

            break;
        }
    }

    deleteDctnryExtentsV4( );
}

//@bug 4091: V4 support for adding DBRoot
//------------------------------------------------------------------------------
// Delete all the dictionary store extents (from the extent map and db files)
// that logically follow the extents contained in fPendingDctnryExtents; where
// fPendingDctnryExtents is a vector of DSTORE1 and DSTORE2 records (for a
// specific column OID), read from a meta-data bulk rollback file.  This
// function is limited to rolling back the extent changes to a specific DBRoot.
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteDctnryExtentsV4 ( )
{
    std::ostringstream msg0073Text;
    msg0073Text << "DBRoot-" << fPendingDctnryStoreDbRoot <<
        " (dictionary extent)";
    logAMessage( logging::LOG_TYPE_INFO,
        logging::M0073, fPendingDctnryStoreOID, msg0073Text.str() );

    if (fPendingDctnryExtents.size() == 0)
        return;

    std::vector<u_int16_t>  segNums;
    std::vector<BRM::HWM_t> hwms;

    // Build up list of HWM's to be sent to DBRM for extentmap rollback
    for (unsigned i=0; i<fPendingDctnryExtents.size(); i++)
    {
        if ( !fPendingDctnryExtents[i].fWithHwm )
            break;

        segNums.push_back( fPendingDctnryExtents[i].fSegNum );
        hwms.push_back(    fPendingDctnryExtents[i].fHwm    );
    }

    // Delete extents from the extentmap using hwms vector
    std::ostringstream msg0074Text;
    msg0074Text << "Restoring HWM dictionary store extents: "
        "dbRoot-" << fPendingDctnryStoreDbRoot         <<
        "; part#-" << fPendingDctnryExtents[0].fPartNum <<
        "; HWM(s): ";
    for (unsigned int k=0; k<hwms.size(); k++)
    {
        if (k > 0)
            msg0074Text << ", ";
        msg0074Text << hwms[k];
    }

    logAMessage( logging::LOG_TYPE_INFO,
        logging::M0074, fPendingDctnryStoreOID, msg0074Text.str() );

    // Create the object responsible for restoring the extents in the db files.
    BulkRollbackFile* fileRestorer = 0;
    if (fPendingDctnryStoreCompressionType)
        fileRestorer = new BulkRollbackFileCompressed(this);
    else
        fileRestorer = new BulkRollbackFile(this);
    boost::scoped_ptr<BulkRollbackFile> refBulkRollbackFile(fileRestorer);

    int rc = BRMWrapper::getInstance()->rollbackDictStoreExtents_DBroot (
        fPendingDctnryStoreOID,
        (u_int16_t)fPendingDctnryStoreDbRoot,
        fPendingDctnryExtents[0].fPartNum,
        segNums,
        hwms );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss<< "Error rolling back dictionary extents from extent map for "<<
            fPendingDctnryStoreOID <<
            "; partNum-" << fPendingDctnryExtents[0].fPartNum <<
            "; "         << ec.errorString(rc);

        throw WeException( oss.str(), ERR_BRM_BULK_RB_DCTNRY );
    }

    // Assign constants used later in calculating exact rollback point
    const unsigned COL_WIDTH       = 8;
    const unsigned ROWS_PER_EXTENT = BRMWrapper::getInstance()->getExtentRows();
    const unsigned BLKS_PER_EXTENT =
        (ROWS_PER_EXTENT * COL_WIDTH)/BYTE_PER_BLOCK;

    u_int32_t dbRoot  = fPendingDctnryStoreDbRoot;
    u_int32_t partNum = fPendingDctnryExtents[0].fPartNum;
    bool existsFlag   = true;
    std::string segFileListErrMsg;

    // Delete extents from the database files.
    // Loop through all partitions (starting with the HWM partition fPartNum),
    // deleting or restoring applicable extents.  We stay in loop till we 
    // reach a partition that has no dctnry store segment files to roll back.
    while ( 1 )
    {
        std::vector<u_int32_t> segList;
        std::string dirName;
        rc = fileRestorer->buildDirName( fPendingDctnryStoreOID,
            dbRoot, partNum, dirName );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Bulk rollback error constructing path for dictionary " <<
                fPendingDctnryStoreOID <<
                "; dbRoot-"    << dbRoot    <<
                "; partition-" << partNum   <<
                "; "           << ec.errorString(rc);

            throw WeException( oss.str(), rc );
        }

        rc = getSegFileList( dirName, segList, segFileListErrMsg );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Bulk rollback error for dictionary " <<
                fPendingDctnryStoreOID    <<
                "; directory-" << dirName <<
                "; " << segFileListErrMsg <<
                "; " << ec.errorString(rc);

            throw WeException( oss.str(), rc );
        }

        if (segList.size() == 0)
            break;

        for (unsigned int kk=0; kk<segList.size(); kk++)
        {
            u_int32_t segNum = segList[kk];
            bool      reInit = false;
            u_int32_t segIdx = 0;

            // For each segment file found in dirName, see if the file is in
            // the list of pending dictionary files to be rolled back; if the
            // file is found, then roll it back, if not found, then delete it.
            for (unsigned nn=0; nn<fPendingDctnryExtents.size(); nn++)
            {
                if ((fPendingDctnryExtents[nn].fPartNum == partNum) &&
                    (fPendingDctnryExtents[nn].fSegNum  == segNum))
                {
                    if (fPendingDctnryExtents[nn].fWithHwm)
                    {
                        segIdx = nn; // found corresponding file in pending list
                        reInit = true;
                    }
                    break;
                }
            }
                
            if (reInit)
            {
                HWM hwm = fPendingDctnryExtents[segIdx].fHwm;

                // Determine the exact rollback point for the extent
                // we are rolling back to
                u_int32_t lastBlkOfCurrStripe = hwm - 
                    (hwm % BLKS_PER_EXTENT) + BLKS_PER_EXTENT - 1;

                // Reinit last extent and truncate the remainder,
                // starting with the next block following the HWM block.
                fileRestorer->reInitTruncDctnryExtent (
                    fPendingDctnryStoreOID,
                    dbRoot,
                    partNum,
                    segNum,
                    (hwm + 1),
                    (lastBlkOfCurrStripe - hwm));
            }
            else // don't keep this segment file
            {
                std::string segFileName;
                fileRestorer->findSegmentFile ( fPendingDctnryStoreOID,
                    false,    // not a column segment file
                    dbRoot,
                    partNum,
                    segNum,                
                    existsFlag,
                    segFileName );
                if (existsFlag)
                {
                    createFileDeletionEntry( fPendingDctnryStoreOID,
                        false, // not a column segment file
                        dbRoot, partNum, segNum,
                        segFileName );
                }
            }
        } // loop thru all the potential segment files in a partition

        partNum++;

    } //end of loop to go thru all partitions till we find last segment file

    fPendingDctnryExtents.clear ( );
}

//------------------------------------------------------------------------------
// Add specified segment file to the list of files to be deleted.  We are
// accumulating the list of file names so that they can be deleted in reverse
// order.  (See deleteDbFiles()).
//------------------------------------------------------------------------------
//@bug 4241 Delete files in reverse order
void BulkRollbackMgr::createFileDeletionEntry(
    OID       columnOID,
    bool      fileTypeFlag,
    u_int32_t dbRoot,
    u_int32_t partNum,
    u_int32_t segNum,
    const std::string& segFileName )
{
    File f;
    f.oid          = columnOID;
    f.fid          = ((fileTypeFlag) ? 1 : 0); // use fid for file type flag
    f.fPartition   = partNum;
    f.fSegment     = segNum;
    f.fDbRoot      = dbRoot;
    f.fSegFileName = segFileName;
    fPendingFilesToDelete.push_back( f );
}

//------------------------------------------------------------------------------
// Delete db files for a column and DBRoot that are waiting to be deleted.
// Files are deleted in reverse order, (see Bug 4241) to facilitate partial
// bulk rollback failures and retries.
//------------------------------------------------------------------------------
//@bug 4241 Delete files in reverse order
void BulkRollbackMgr::deleteDbFiles( )
{
    // Okay to use a BulkRollbackFile object, because we are only calling the
    // deleteSegmentFile() method which is a base class function with no
    // polymorphic behavior.  (In other words, we don't need to worry about
    // employing a BulkRollbackFileCompressed object for a compressed column.)
    BulkRollbackFile fileRestorer( this );

    unsigned int fileCount = fPendingFilesToDelete.size();
    for (int i=fileCount-1; i>=0; --i)
    {
        fileRestorer.deleteSegmentFile(
            fPendingFilesToDelete[i].oid,
            ((fPendingFilesToDelete[i].fid > 0) ? true : false),
            fPendingFilesToDelete[i].fDbRoot,
            fPendingFilesToDelete[i].fPartition,
            fPendingFilesToDelete[i].fSegment,
            fPendingFilesToDelete[i].fSegFileName );
    }

    fPendingFilesToDelete.clear();
}

//------------------------------------------------------------------------------
// Get list of segment files found in the specified db directory path.
//------------------------------------------------------------------------------
/* static */
int BulkRollbackMgr::getSegFileList(
    const std::string& dirName,
    std::vector<u_int32_t>& segList,
    std::string& errMsg )
{
    const unsigned int DB_FILE_PREFIX_LEN = DB_FILE_PREFIX.length();
    segList.clear();

    // Return no segment files if partition directory path does not exist
    struct stat curStat;
    if ( stat(dirName.c_str(), &curStat) != 0 )
        return NO_ERROR;

    try
    {
        boost::filesystem::path dirPath( dirName.c_str() );
        boost::filesystem::directory_iterator end_itr; // create EOD marker

        // Loop through all the files in the specified directory
        for ( boost::filesystem::directory_iterator itr( dirPath );
            itr != end_itr;
            ++itr )
        {
#if BOOST_VERSION >= 105200
            //@bug 4989 - stem() and extension() return a temp path object by 
            // value so be sure to store in a string and not a string reference.
            const std::string fileBase = itr->path().stem().generic_string();
            const std::string fileExt  = itr->path().extension().generic_string();
#else
            //const std::string& fileName = itr->path().filename();
            const std::string& fileBase = itr->path().stem();
            const std::string& fileExt  = itr->path().extension();
#endif
            // Select files of interest ("FILE*.cdf")
            if ((fileBase.compare(0, DB_FILE_PREFIX_LEN, DB_FILE_PREFIX) == 0)&&
                (fileExt == DB_FILE_EXTENSION) )
            {
                const std::string& fileSeg = fileBase.substr(
                    DB_FILE_PREFIX_LEN, (fileBase.length()-DB_FILE_PREFIX_LEN));
                bool bDbFile = true;

                const unsigned fileSegLen = fileSeg.length();
                if (fileSegLen >= 3)
                {
                    for (unsigned int k=0; k<fileSegLen; k++)
                    {
                        if ( !isdigit(fileSeg[k]) )
                        {
                            bDbFile = false;
                            break;
                        }
                    }

                    if (bDbFile)
                    {
                        u_int32_t segNum = atoi( fileSeg.c_str() );
                        segList.push_back( segNum );
                    }
                } // filename must have 3 or more digits representing seg number
            }     // look for filenames matching "FILE*.cdf"
        }         // loop through the files in the directory

        if (segList.size() > 1)
            std::sort( segList.begin(), segList.end() );
    }
    catch (std::exception& ex)
    {
        segList.clear();
        errMsg = ex.what();
        return ERR_BULK_ROLLBACK_SEG_LIST;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Log a message to syslog.  columnOID and text are used depending on the msgId.
//
// logType   - type of message (debug, critical, etc)
// msgId     - message ID
// columnOID - column OID associated with this rollback message
// text      - message text
//------------------------------------------------------------------------------
void BulkRollbackMgr::logAMessage (
    logging::LOG_TYPE           logType,
    logging::Message::MessageID msgId,
    OID                         columnOID,
    const std::string&          text )
{
    logging::Message m( msgId );
    logging::Message::Args args;

    std::ostringstream ossTbl;
    ossTbl << fTableName << " (OID-" << fTableOID << ")";
    args.add( ossTbl.str() );

    if (msgId >= logging::M0073)
    {
        switch (msgId)
        {
            case logging::M0073:
            case logging::M0074:
            case logging::M0075:
            {
                args.add( (uint64_t)columnOID );
                break;
            }
            case logging::M0084:
            {
                args.add( (uint64_t)fLockID );
                break;
            }
            case logging::M0085:
            {
                args.add( (uint64_t)fLockID );
                break;
            }
            case logging::M0090:
            {
                // no other arg applicable for this message
                break;
            }
        }
        args.add( text );
    }
    m.format( args );

    // Log to syslog
    // Note that WARNING, ERROR and CRITICAL are logged to INFO as well as
    // their respective log files, so that the message will appear in context
    // with all the other INFO msgs used to track the flow of the rollback.
    switch (logType)
    {
        case logging::LOG_TYPE_DEBUG:
        {
            fSysLogger.logDebugMessage( m );
            break;
        }

        case logging::LOG_TYPE_INFO:
        {
            fSysLogger.logInfoMessage( m );
            break;
        }

        case logging::LOG_TYPE_WARNING:
        {
            fSysLogger.logWarningMessage( m );
            fSysLogger.logInfoMessage   ( m );
            break;
        }

        case logging::LOG_TYPE_ERROR:
        {
            fSysLogger.logErrorMessage( m );
            fSysLogger.logInfoMessage ( m );
            break;
        }

        default: // LOG_TYPE_CRITICAL
        {
            fSysLogger.logCriticalMessage( m );
            fSysLogger.logInfoMessage    ( m );
            break;
        }
    }

    // If fLog is defined then log to there, else log to cout.
    // Currently log msg0074 and msg0075 to console only if debug is enabled
    switch (msgId)
    {
        // Log the name of the table to be rolled back or restored
        case logging::M0072:
        {
            if (fLog)
            {
                std::ostringstream oss;
                oss << "Rolling back extents for table " <<
                    fTableName << " (OID-" << fTableOID << ")";
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }
            else
            {
                std::cout << "Rolling back extents for table " <<
                    fTableName << " (OID-" << fTableOID << ")" << std::endl;
            }
            break;
        }

        // Log the name of the table and column to be rolled back or restored
        case logging::M0073:
        {
            if (fLog)
            {
                std::ostringstream oss;
                oss << "Rolling back extents for table " <<
                    fTableName << " (OID-" << fTableOID <<
                    "); column " << columnOID << "; " << text;
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }
            else
            {
                std::cout << "Rolling back extents for table " <<
                    fTableName << " (OID-" << fTableOID <<
                    "); column " << columnOID << "; " << text << std::endl;
            }
            break;
        }

        // Log the rolling back of extent(s) from the extent map
        case logging::M0074:
        {
            if (fLog)
            {
                std::ostringstream oss;
                oss << "Rolling back extent map for table " <<
                    fTableName << " (OID-" << fTableOID <<
                    "); column " << columnOID << "; " << text;
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }
            else
            {
                if ( fDebugConsole )
                {
                    std::cout << "Rolling back extent map for table " <<
                        fTableName << " (OID-" << fTableOID <<
                        "); column " << columnOID << "; " << text << std::endl;
                }
            }
            break;
        }

        // Log the rolling back of extent(s) from the DB 
        case logging::M0075:
        {
            if (fLog)
            {
                std::ostringstream oss;
                oss << "Rolling back db file for table " <<
                    fTableName << " (OID-" << fTableOID <<
                    "); column " << columnOID << "; " << text;
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }
            else
            {
                if ( fDebugConsole )
                {
                    std::cout << "Rolling back db file for table " <<
                        fTableName << " (OID-" << fTableOID <<
                        "); column " << columnOID << "; " << text << std::endl;
                }
            }
            break;
        }

        // Log the start of a bulk rollback
        case logging::M0084:
        {
            if (fLog)
            {
                std::ostringstream oss;
                oss << "Starting bulk rollback for table " <<
                    fTableName << " (OID-" << fTableOID <<
                    ", Lock-" << fLockID << ") in " << text;
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }
            else
            {
                std::cout << "Starting bulk rollback for table " <<
                    fTableName << " (OID-" << fTableOID <<
                    ", Lock-" << fLockID << ") in " << text << std::endl;
            }
            break;
        }

        // Log the end of a bulk rollback
        case logging::M0085:
        {
            if (fLog)
            {
                std::ostringstream oss;
                oss << "Ending bulk rollback for table " <<
                    fTableName << " (OID-" << fTableOID <<
                    ", Lock-" << fLockID << ") in " << text;
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }
            else
            {
                std::cout << "Ending bulk rollback for table " <<
                    fTableName << " (OID-" << fTableOID <<
                    ", Lock-" << fLockID << ") in " << text << std::endl;
            }
            break;
        }

        // Log skipping a DBRoot rollback because no meta-data file
        case logging::M0090:
        {
            if (fLog)
            {
                std::ostringstream oss;
                oss << "Nothing to rollback for table " <<
                    fTableName << " (OID-" << fTableOID <<
                    ") on DBRoot" << text;
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }
            else
            {
                std::cout << "Nothing to rollback for table " <<
                    fTableName << " (OID-" << fTableOID <<
                    ") on DBRoot" << text << std::endl;
            }
            break;
        }

        default:
        {
            break;
        }
    }
}

//------------------------------------------------------------------------------
// Standalone utility that can be used to delete bulk rollback files.
// WARNING: this function can return an exception (from call to boost
// remove_all() )
//------------------------------------------------------------------------------
/* static */
void BulkRollbackMgr::deleteMetaFile( OID tableOID )
{
    std::vector<u_int16_t> dbRoots;
    Config::getRootIdList( dbRoots );

    // Loop through DBRoots for this PM
    for (unsigned m=0; m<dbRoots.size(); m++)
    {
        std::string bulkRollbackPath( Config::getDBRootByNum(dbRoots[m]) );

        std::ostringstream oss;
        oss << '/' << DBROOT_BULK_ROLLBACK_SUBDIR << '/' << tableOID;
        std::string metaFileName = bulkRollbackPath;
        metaFileName            += oss.str();

        // Delete the main bulk rollback file
        unlink( metaFileName.c_str() );

        // Unlink corresponding tmp file created by RBMetaWriter.
        std::string tmpMetaFileName = metaFileName;
        tmpMetaFileName += TMP_FILE_SUFFIX;
        unlink( tmpMetaFileName.c_str() );

        // Recursively delete any HWM chunk backup files
        std::string bulkRollbackSubPath( metaFileName );
        bulkRollbackSubPath += DATA_DIR_SUFFIX;
        boost::filesystem::path dirPath(bulkRollbackSubPath);

        // Delete bulk rollback data subdirectory; caution, the call to
        // remove_all() can throw an exception
        boost::filesystem::remove_all(dirPath);
    }
}

} //end of namespace
