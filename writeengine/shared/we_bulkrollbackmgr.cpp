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
* $Id: we_bulkrollbackmgr.cpp 4737 2013-08-14 20:45:46Z bwilkinson $
*/

#include <sstream>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>

#include "we_bulkrollbackmgr.h"

#include "we_define.h"
#include "we_brm.h"
#include "we_config.h"
#include "we_fileop.h"
#include "we_log.h"
#include "we_bulkrollbackfile.h"
#include "we_bulkrollbackfilecompressed.h"
#include "we_bulkrollbackfilecompressedhdfs.h"
#include "we_rbmetawriter.h"
#include "messageids.h"
#include "cacheutils.h"

using namespace execplan;

#include "IDBPolicy.h"
using namespace idbdatafile;

using namespace std;

namespace
{
    const char* DATA_DIR_SUFFIX = "_data";
    const char* TMP_FILE_SUFFIX = ".tmp";

    const int BUF_SIZE = 1024;  // size of buffer used to read meta data records

    const std::string DB_FILE_PREFIX   ("FILE");
    const std::string DB_FILE_EXTENSION(".cdf");
    const std::string DB_FILE_EXTENSION_ORIG(".orig");
    const std::string DB_FILE_EXTENSION_TMP (".tmp" );
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// BulkRollbackMgr constructor
//
// tableOID - OID of the table to be rolled back.
//------------------------------------------------------------------------------
BulkRollbackMgr::BulkRollbackMgr ( OID tableOID,
    uint64_t lockID,
    const std::string& tableName,
    const std::string& applName, Log* logger ) :
    fTableOID(tableOID),
    fLockID(lockID),
    fTableName(tableName),
    fProcessId(0),
    fMetaFile(NULL),
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

        std::vector<uint16_t> dbRoots;
        Config::getRootIdList( dbRoots );
        
        std::string emptyText0072;
        logAMessage( logging::LOG_TYPE_INFO,
            logging::M0072, 0, emptyText0072 );

        // Loop through DBRoots for this PM
        for (unsigned m=0; m<dbRoots.size(); m++)
        {
            std::istringstream metaDataStream;
            bool bPerformRollback = openMetaDataFile ( dbRoots[m],
                metaDataStream );

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
                deleteExtents ( metaDataStream );
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
            // Notify PrimProc to flush FD cache.  If error occurs, we tell
            // the user but keep going.
            int flushFd_rc = cacheutils::dropPrimProcFdCache();
            if (flushFd_rc != 0)
            {
                std::ostringstream oss;
                oss << "ClearTableLock: Error flushing PrimProc "
                    "FD cache after rolling back data for table " <<
                    fTableName <<
                    " (OID-" << fTableOID << ");  rc-" << flushFd_rc;

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

            // Notify PrimProc to flush block cache.  If error occurs, we tell
            // the user but keep going.
            std::vector<BRM::OID_t> allOIDs;
            std::set<OID>::const_iterator iter=fAllColDctOIDs.begin();
            while (iter != fAllColDctOIDs.end())
            {
                //std::cout << "Flushing OID from PrimProc cache " << *iter <<
                //  std::endl;
                allOIDs.push_back(*iter);
                ++iter;
            }

            int cache_rc = cacheutils::flushOIDsFromCache( allOIDs );
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
    const std::vector<uint16_t>& dbRoots ) const
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

        if ( !IDBPolicy::exists( metaFileName.c_str() ) )
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
bool BulkRollbackMgr::openMetaDataFile ( uint16_t dbRoot,
    std::istringstream& metaDataStream )
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
    if ( !IDBPolicy::exists( fMetaFileName.c_str() ) )
    {
        return false;
    }

    // Open the file
    fMetaFile = IDBDataFile::open( IDBPolicy::getType(fMetaFileName.c_str(),
                                   IDBPolicy::WRITEENG),
                                   fMetaFileName.c_str(), "rb", 0);

    if ( !fMetaFile )
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
    char inBuf[ BUF_SIZE ];
    ssize_t metaFileSize = IDBPolicy::size( fMetaFileName.c_str() );
    boost::scoped_array<char> buf( new char[ metaFileSize ] );
    // retry 10 times for partial reads, just in case
    ssize_t readSofar = 0; // bytes read so far
    ssize_t bytes = 0;    // bytes read by one pread
    char* p = buf.get();
    for (int i = 0; i < 10 && readSofar < metaFileSize; i++)
    {
        bytes = fMetaFile->pread( p+readSofar, readSofar, metaFileSize-readSofar);
        if (bytes < 0)
            break;

        readSofar += bytes;
    }
    if ( readSofar != metaFileSize )
    {
        int errRc = errno;
        std::ostringstream oss;
        oss << "Error reading bulk rollback meta-data file "
            << fMetaFileName << "; read/expect:" << readSofar << "/" << metaFileSize
            << "; err-" << errRc << "; " << strerror( errRc );
    
        throw WeException( oss.str(), ERR_FILE_READ );
    }

    // put the data in a string stream
    metaDataStream.str( string( p, metaFileSize ) );
    buf.reset();

    // read data
    metaDataStream.getline( inBuf, BUF_SIZE );
    if (RBMetaWriter::verifyVersion3(inBuf))
    {
        fVersion = 3;
    }
    else if (RBMetaWriter::verifyVersion4(inBuf))
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
    delete fMetaFile;
    fMetaFile = NULL;
}

//------------------------------------------------------------------------------
// Delete all the local meta-data files used in rolling back fTableOID.
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteMetaDataFiles ( )
{
    for (unsigned k=0; k<fMetaFileNames.size(); k++)
    {
        IDBPolicy::remove( fMetaFileNames[k].c_str() ) ;

        // Unlink corresponding tmp file created by RBMetaWriter.
        std::string tmpMetaFileName = fMetaFileNames[k];
        tmpMetaFileName += TMP_FILE_SUFFIX;
        IDBPolicy::remove( tmpMetaFileName.c_str() );

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

    if( IDBPolicy::remove( bulkRollbackSubPath.c_str() ) != 0 )
    {
        std::ostringstream oss;
        oss << "Warning: Error deleting bulk rollback data subdirectory " <<
            bulkRollbackSubPath << ";";
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
void BulkRollbackMgr::deleteExtents ( std::istringstream& metaDataStream )
{
    char  inBuf[ BUF_SIZE ];
    OID   columnOID     = 0;
    OID   storeOID      = 0;
    uint32_t dbRoot    = 0;

    // Loop through the records in the meta-data file
    while (metaDataStream.getline( inBuf, BUF_SIZE ))
    {
        // Restore extents for a DBRoot
        if (RBMetaWriter::verifyColumn1Rec(inBuf))
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
        else if (RBMetaWriter::verifyColumn2Rec(inBuf))
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
        else if (RBMetaWriter::verifyDStore1Rec(inBuf) ||
                 RBMetaWriter::verifyDStore2Rec(inBuf))
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
    uint32_t dbRootHwm;
    uint32_t partNumHwm;
    uint32_t segNumHwm;
    HWM       localHwm;
    int       compressionType = 0; // optional parameter

    sscanf(inBuf, "%s", recType);
    RollbackData rbData;

    // Process DSTORE1 records representing segment files with an HWM
    if (RBMetaWriter::verifyDStore1Rec(recType))
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
    uint32_t   dbRootHwm;
    uint32_t   partNumHwm;
    uint32_t   segNumHwm;
    int         colTypeInt;
    char        colTypeName[100];
    uint32_t   colWidth;
    int         compressionType = 0; // optional parameter
    
    // Read meta-data record
    int numFields = sscanf(inBuf, "%s %u %u %u %u %d %s %u %d",
        recType, &columnOID,
        &dbRootHwm, &partNumHwm, &segNumHwm,
        &colTypeInt, colTypeName, &colWidth, &compressionType );
    if (numFields < 8) // compressionType is optional
    {
        std::ostringstream oss;
        oss << "Invalid COLUM2 record in meta-data file " <<
            fMetaFileName << "; record-<" << inBuf << ">" << std::endl;

        throw WeException( oss.str(), ERR_INVALID_PARAM );
    }

    std::ostringstream revisedBuf;
    uint32_t revisedSegNumHwm = 0;
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
    uint32_t   dbRootHwm;
    uint32_t   partNumHwm;
    uint32_t   segNumHwm;
    HWM         lastLocalHwm;
    int         colTypeInt;
    CalpontSystemCatalog::ColDataType colType;
    char        colTypeName[100];
    uint32_t   colWidth;
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
    msg0074Text << "Restoring HWM column extent: " <<
        "dbRoot-"  << dbRootHwm  <<
        "; part#-" << partNumHwm <<
        "; seg#-"  << segNumHwm  <<
        "; hwm-"   << lastLocalHwm;
    logAMessage( logging::LOG_TYPE_INFO,
        logging::M0074, columnOID, msg0074Text.str() );
    fAllColDctOIDs.insert( columnOID );

    // Create the object responsible for restoring the extents in the db files.
    BulkRollbackFile* fileRestorer = makeFileRestorer(compressionType);
    boost::scoped_ptr<BulkRollbackFile> refBulkRollbackFile(fileRestorer);

    // DMC-We should probably change this to build up a list of BRM changes,
    //     and wait to make the call(s) to rollback the BRM changes "after" we
    //     have restored the db files, and purged PrimProc FD and block cache.
    int rc = BRMWrapper::getInstance()->rollbackColumnExtents_DBroot (
        columnOID,
        false,                  // false -> Don't delete all extents (rollback
        (uint16_t)dbRootHwm,   //    to specified dbroot, partition, etc.)
        partNumHwm,
        (uint16_t)segNumHwm,
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
    uint32_t lastBlkOfCurrStripe = 0;
    uint32_t lastBlkOfPrevStripe = 0;
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

    uint32_t dbRoot  = dbRootHwm;
    uint32_t partNum = partNumHwm;
    std::string segFileListErrMsg;

    // Delete extents from the database files.
    // Loop through all partitions (starting with the HWM partition partNumHwm),
    // deleting or restoring applicable extents.  We stay in loop till we 
    // reach a partition that has no column segment files to roll back.
    bool useHdfs = IDBPolicy::useHdfs();
    while ( 1 )
    {
        std::vector<uint32_t> segList;
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

        rc = getSegFileList( dirName, useHdfs, segList, segFileListErrMsg );
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
            uint32_t segNum = segList[kk];

            if ( partNum == partNumHwm )
            {
                // Don't rollback an OutOfService extent in the HWM partition
                bool bFound;
                int extState;
                rc = BRMWrapper::getInstance()->getExtentState(
                    columnOID, partNum, segNum, bFound, extState );
                if (rc != NO_ERROR)
                {
                    WErrorCodes ec;
                    std::ostringstream oss;
                    oss << "Bulk rollback error for column " << columnOID <<
                        "; Unable to get extent state for part-" << partNum <<
                        "; seg-" << segNum <<
                        "; " << ec.errorString(rc);

                    throw WeException( oss.str(), rc );
                }
                if ((bFound) && (extState == BRM::EXTENTOUTOFSERVICE))
                    continue;

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
                        fileRestorer->buildSegmentFileName ( columnOID,
                            true,    // column segment file
                            dbRoot,
                            partNum,
                            segNum,
                            segFileName );

                        createFileDeletionEntry( columnOID,
                            true, // column segment file
                            dbRoot, partNum, segNum, segFileName );
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
                fileRestorer->buildSegmentFileName ( columnOID,
                    true,    // column segment file
                    dbRoot,
                    partNum,
                    segNum,
                    segFileName );

                createFileDeletionEntry( columnOID,
                    true, // column segment file
                    dbRoot, partNum, segNum, segFileName );
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
    uint32_t   dbRootHwm;
    uint32_t   partNumHwm;
    uint32_t   segNumHwm;
    HWM         lastLocalHwm = 0;
    int         colTypeInt;
    char        colTypeName[100];
    uint32_t   colWidth;
    int         compressionType = 0; // optional parameter
    
    // Read meta-data record
    int numFields = sscanf(inBuf, "%s %u %u %u %u %d %s %u %d",
        recType, &columnOID,
        &dbRootHwm, &partNumHwm, &segNumHwm,
        &colTypeInt, colTypeName, &colWidth, &compressionType );
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

    // @bug 5644 - If user dropped all partitions in a dbroot, partNumHwm will
    // be 0, but we may start importing into part# > 0 (to line up with other
    // DBRoots).  Use extent map to find first partition added by this import.
    std::vector<struct BRM::EMEntry> extEntries;
    int rc = BRMWrapper::getInstance()->getExtents_dbroot( columnOID,
        extEntries, dbRootHwm );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error getting extent list from extent map for " <<
            columnOID <<
            "; dbRoot-"    << dbRootHwm    <<
            "; "           << ec.errorString(rc);

        throw WeException( oss.str(), ERR_BRM_BULK_RB_COLUMN );
    }
    uint32_t part1 = partNumHwm; // lowest part# for column and DBRoot
    if (extEntries.size() > 0)
    {
        part1 = extEntries[0].partitionNum;
        for (unsigned int kk=0; kk<extEntries.size(); kk++)
        {
            if (extEntries[kk].partitionNum < part1)
                part1 = extEntries[kk].partitionNum;
        }
    }

    // Delete extents from the extentmap
    std::ostringstream msg0074Text;
    msg0074Text << "Restoring empty DBRoot. "
        "dbRoot-"  << dbRootHwm  <<
        "; part#-" << partNumHwm <<
        "; seg#-"  << segNumHwm  <<
        "; hwm-"   << lastLocalHwm <<
        "; delete starting at part#-" << part1;
    logAMessage( logging::LOG_TYPE_INFO,
        logging::M0074, columnOID, msg0074Text.str() );
    fAllColDctOIDs.insert( columnOID );

    // Reset partNumHwm to partNum taken from extent map
    partNumHwm = part1;

    // Create the object responsible for restoring the extents in the db files.
    BulkRollbackFile* fileRestorer = makeFileRestorer(compressionType);
    boost::scoped_ptr<BulkRollbackFile> refBulkRollbackFile(fileRestorer);

    // DMC-We should probably change this to build up a list of BRM changes,
    //     and wait to make the call(s) to rollback the BRM changes "after" we
    //     have restored the db files, and purged PrimProc FD and block cache.
    rc = BRMWrapper::getInstance()->rollbackColumnExtents_DBroot (
        columnOID,
        true,           // true -> delete all extents (restore to empty DBRoot)
        (uint16_t)dbRootHwm,
        partNumHwm,
        (uint16_t)segNumHwm,
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

    uint32_t dbRoot  = dbRootHwm;
    uint32_t partNum = partNumHwm;
    std::string segFileListErrMsg;

    // Delete extents from the database files.
    // Loop through all partitions (starting with the HWM partition partNumHwm),
    // deleting or restoring applicable extents.  We stay in loop till we 
    // reach a partition that has no column segment files to roll back.
    bool useHdfs = IDBPolicy::useHdfs();
    while ( 1 )
    {
        std::vector<uint32_t> segList;
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

        rc = getSegFileList( dirName, useHdfs, segList, segFileListErrMsg );
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
            uint32_t segNum = segList[kk];

            // Delete any files added to subsequent partitions
            std::string segFileName;
            fileRestorer->buildSegmentFileName ( columnOID,
                true,    // column segment file
                dbRoot,
                partNum,
                segNum,                
                segFileName );

            createFileDeletionEntry( columnOID,
                true, // column segment file
                dbRoot, partNum, segNum, segFileName );
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

    std::vector<uint16_t>  segNums;
    std::vector<BRM::HWM_t> hwms;

    // Build up list of HWM's to be sent to DBRM for extentmap rollback
    for (unsigned i=0; i<fPendingDctnryExtents.size(); i++)
    {
        if ( !fPendingDctnryExtents[i].fWithHwm )
            break;

        segNums.push_back( fPendingDctnryExtents[i].fSegNum );
        hwms.push_back(    fPendingDctnryExtents[i].fHwm    );
    }

    // @bug 5644 - If user dropped all partitions in a dbroot, fPartNum will
    // be 0, but we may start importing into part# > 0 (to line up with other
    // DBRoots).  Use extent map to find first partition added by this import.
    uint32_t part1 = fPendingDctnryExtents[0].fPartNum; // lowest part# for
                                                        // OID and DBRoot
    if (hwms.size() == 0) // empty DBRoot case
    {
        std::vector<struct BRM::EMEntry> extEntries;
        int rc = BRMWrapper::getInstance()->getExtents_dbroot(
            fPendingDctnryStoreOID,
            extEntries,
            fPendingDctnryStoreDbRoot );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error getting extent list from extent map for " <<
                fPendingDctnryStoreOID <<
                "; dbRoot-"    << fPendingDctnryStoreDbRoot <<
                "; "           << ec.errorString(rc);

            throw WeException( oss.str(), ERR_BRM_BULK_RB_COLUMN );
        }
        if (extEntries.size() > 0)
        {
            part1 = extEntries[0].partitionNum;
            for (unsigned int kk=0; kk<extEntries.size(); kk++)
            {
                if (extEntries[kk].partitionNum < part1)
                    part1 = extEntries[kk].partitionNum;
            }
        }
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
    if (hwms.size() == 0)
        msg0074Text << "; delete starting at part#-" << part1;

    logAMessage( logging::LOG_TYPE_INFO,
        logging::M0074, fPendingDctnryStoreOID, msg0074Text.str() );
    fAllColDctOIDs.insert( fPendingDctnryStoreOID );

    // Reset partNum to partNum taken from extent map
    uint32_t partNum = part1;

    // Create the object responsible for restoring the extents in the db files.
    BulkRollbackFile* fileRestorer = makeFileRestorer(
        fPendingDctnryStoreCompressionType);
    boost::scoped_ptr<BulkRollbackFile> refBulkRollbackFile(fileRestorer);

    // DMC-We should probably change this to build up a list of BRM changes,
    //     and wait to make the call(s) to rollback the BRM changes "after" we
    //     have restored the db files, and purged PrimProc FD and block cache.
    int rc = BRMWrapper::getInstance()->rollbackDictStoreExtents_DBroot (
        fPendingDctnryStoreOID,
        (uint16_t)fPendingDctnryStoreDbRoot,
        partNum,
        segNums,
        hwms );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss<< "Error rolling back dictionary extents from extent map for "<<
            fPendingDctnryStoreOID <<
            "; partNum-" << partNum <<
            "; "         << ec.errorString(rc);

        throw WeException( oss.str(), ERR_BRM_BULK_RB_DCTNRY );
    }

    // Assign constants used later in calculating exact rollback point
    const unsigned COL_WIDTH       = 8;
    const unsigned ROWS_PER_EXTENT = BRMWrapper::getInstance()->getExtentRows();
    const unsigned BLKS_PER_EXTENT =
        (ROWS_PER_EXTENT * COL_WIDTH)/BYTE_PER_BLOCK;

    uint32_t dbRoot  = fPendingDctnryStoreDbRoot;
    std::string segFileListErrMsg;

    // Delete extents from the database files.
    // Loop through all partitions (starting with the HWM partition fPartNum),
    // deleting or restoring applicable extents.  We stay in loop till we 
    // reach a partition that has no dctnry store segment files to roll back.
    bool useHdfs = IDBPolicy::useHdfs();
    while ( 1 )
    {
        std::vector<uint32_t> segList;
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

        rc = getSegFileList( dirName, useHdfs, segList, segFileListErrMsg );
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
            uint32_t segNum = segList[kk];
            bool      reInit = false;
            uint32_t segIdx = 0;

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
                // Don't rollback an OutOfService extent in the HWM partition
                bool bFound;
                int extState;
                rc = BRMWrapper::getInstance()->getExtentState(
                    fPendingDctnryStoreOID, partNum, segNum, bFound, extState );
                if (rc != NO_ERROR)
                {
                    WErrorCodes ec;
                    std::ostringstream oss;
                    oss << "Bulk rollback error for dctnry store " <<
                        fPendingDctnryStoreOID <<
                        "; Unable to get extent state for part-" << partNum <<
                        "; seg-" << segNum <<
                        "; " << ec.errorString(rc);

                    throw WeException( oss.str(), rc );
                }
                if ((bFound) && (extState == BRM::EXTENTOUTOFSERVICE))
                    continue;

                HWM hwm = fPendingDctnryExtents[segIdx].fHwm;

                // Determine the exact rollback point for the extent
                // we are rolling back to
                uint32_t lastBlkOfCurrStripe = hwm - 
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
                fileRestorer->buildSegmentFileName ( fPendingDctnryStoreOID,
                    false,    // not a column segment file
                    dbRoot,
                    partNum,
                    segNum,                
                    segFileName );

                createFileDeletionEntry( fPendingDctnryStoreOID,
                    false, // not a column segment file
                    dbRoot, partNum, segNum,
                    segFileName );
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
    uint32_t dbRoot,
    uint32_t partNum,
    uint32_t segNum,
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
// If bIncludeAlternateSegFileNames is false, then only "FILENNN.cdf" files
// will be considered.
// If bIncludeAlternateSegFileNames is true, then in addition to "FILENNN.cdf"
// files, "FILENNN.cdf.orig" and "FILENNN.cdf.tmp" files will be considered.
// The latter case (flag set to true) is used on connection with HDFS.
//------------------------------------------------------------------------------
/* static */
int BulkRollbackMgr::getSegFileList(
    const std::string& dirName,
    bool bIncludeAlternateSegFileNames,
    std::vector<uint32_t>& segList,
    std::string& errMsg )
{
    const unsigned int DB_FILE_PREFIX_LEN = DB_FILE_PREFIX.length();
    segList.clear();
    std::set<uint32_t> segSet;

    // Return no segment files if partition directory path does not exist
    if( !IDBPolicy::isDir( dirName.c_str() ) )
        return NO_ERROR;

    list<string> dircontents;
    if( IDBPolicy::listDirectory( dirName.c_str(), dircontents ) == 0 )
    {
        list<string>::iterator iend = dircontents.end();
        for( list<string>::iterator i = dircontents.begin(); i != iend; ++i )
        {
            boost::filesystem::path filepath( *i );

#if BOOST_VERSION >= 105200
            //@bug 4989 - stem() and extension() return a temp path object by 
            // value so be sure to store in a string and not a string reference.
            const std::string fileBase = filepath.stem().generic_string();
            const std::string fileExt  = filepath.extension().generic_string();
#else
            const std::string& fileBase = filepath.stem();
            const std::string& fileExt  = filepath.extension();
#endif
            //std::cout << "getSegFileList: " << fileBase << " / " <<
            //  fileExt << std::endl;

            // Select files of interest ("FILE*.cdf")
            bool bMatchFound = false;
            unsigned int segNumStrLen = 0;

            if (fileBase.compare(0, DB_FILE_PREFIX_LEN, DB_FILE_PREFIX) == 0)
            {
                segNumStrLen = fileBase.length() - DB_FILE_PREFIX_LEN;

                // Select primary "FILE*.cdf" files
                if (fileExt == DB_FILE_EXTENSION)
                {
                    bMatchFound  = true;
                    //std::cout << "getSegFileList: match *.cdf" << std::endl;
                }
                // Select alternate files of interest ("FILE*.cdf.orig" and
                // "FILE*.cdf.tmp") used for HDFS backup and rollback.
                else if (bIncludeAlternateSegFileNames)
                {
                    if ((fileExt == DB_FILE_EXTENSION_ORIG) ||
                        (fileExt == DB_FILE_EXTENSION_TMP))
                    {
                        //std::cout << "getSegFileList: match *.tmp or *.orig"<<
                        //    std::endl;
                        unsigned int extLen = DB_FILE_EXTENSION.length();
                        if ((fileBase.length() >= extLen) &&
                            (fileBase.compare((fileBase.length()-extLen),
                                extLen,
                                DB_FILE_EXTENSION) == 0))
                        {
                            //std::cout << "getSegFileList: match *cdf.tmp or "
                            //  "*cdf.orig" << std::endl;
                            bMatchFound   = true;
                            segNumStrLen -= extLen;
                        }
                    }
                }
            } // if fileBase.compare() shows filename starting with "FILE"

            if (bMatchFound)
            {
                const std::string& fileSeg = fileBase.substr(
                    DB_FILE_PREFIX_LEN, segNumStrLen);
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
                        uint32_t segNum = atoi( fileSeg.c_str() );
                        segSet.insert( segNum );
                    }
                } // filename must have 3 or more digits representing seg number
            } // found "FILE*.cdf", "FILE*.cdf.orig", or "FILE*.cdf.tmp" file
        } // for each file in directory

        if (segSet.size() > 0)
        {
            std::set<uint32_t>::const_iterator iter=segSet.begin();
            while (iter != segSet.end())
            {
                //std::cout << "getSegFileList: Adding segnum " << *iter <<
                //    " to the segment list" << std::endl;
                segList.push_back(*iter);
                ++iter;
            }
        }

        return NO_ERROR;

    } // if listDirectory() success
    else
    {
        segList.clear();
        errMsg = "Unable to listDirectory in getSegFileList()";
        return ERR_BULK_ROLLBACK_SEG_LIST;
    }
}

//------------------------------------------------------------------------------
// Make/return the applicable BulkRollbackFile object used to rollback/restore
// a db file, based on the compression type and HDFS setting.
//------------------------------------------------------------------------------
BulkRollbackFile* BulkRollbackMgr::makeFileRestorer(int compressionType)
{
    BulkRollbackFile* fileRestorer = 0;
    if (compressionType)
    {
        if (IDBPolicy::useHdfs())
            fileRestorer = new BulkRollbackFileCompressedHdfs(this);
        else
            fileRestorer = new BulkRollbackFileCompressed(this);
    }
    else
    {
        fileRestorer = new BulkRollbackFile(this);
    }

    return fileRestorer;
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
    std::vector<uint16_t> dbRoots;
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
        IDBPolicy::remove( metaFileName.c_str() );

        // Unlink corresponding tmp file created by RBMetaWriter.
        std::string tmpMetaFileName = metaFileName;
        tmpMetaFileName += TMP_FILE_SUFFIX;
        IDBPolicy::remove( tmpMetaFileName.c_str() );

        // Recursively delete any HWM chunk backup files
        std::string bulkRollbackSubPath( metaFileName );
        bulkRollbackSubPath += DATA_DIR_SUFFIX;

        IDBPolicy::remove( bulkRollbackSubPath.c_str() );
    }
}

} //end of namespace
