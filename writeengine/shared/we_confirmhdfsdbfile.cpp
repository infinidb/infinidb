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

#include "we_confirmhdfsdbfile.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <sstream>
#include <vector>

#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>

#include "we_define.h"
#include "we_config.h"
#include "we_fileop.h"
#include "we_rbmetawriter.h"
#include "IDBPolicy.h"
#include "IDBDataFile.h"

namespace
{
    const int BUF_SIZE = 1024;  // size of buffer used to read meta data records
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// Constructor
// This class should typically only be used on an HDFS system, so we could
// hardcode this to pass HDFS to getFsi(); but it comes in handy for testing,
// to be able to execute this class on a non-HDFS stack as well.  So I rely
// on useHdfs() to tell me which FileSystem reference to get.
//------------------------------------------------------------------------------
ConfirmHdfsDbFile::ConfirmHdfsDbFile() :
    fFs( (idbdatafile::IDBPolicy::useHdfs()) ?
        idbdatafile::IDBFileSystem::getFs(idbdatafile::IDBDataFile::HDFS) :
        idbdatafile::IDBFileSystem::getFs(idbdatafile::IDBDataFile::BUFFERED))
{
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ConfirmHdfsDbFile::~ConfirmHdfsDbFile()
{
}

//------------------------------------------------------------------------------
// Backup a cdf file and replace it with the updated tmp file.
//------------------------------------------------------------------------------
int ConfirmHdfsDbFile::confirmDbFileChange(
    const std::string& backUpFileType,
    const std::string& filename,
    std::string& errMsg) const
{
    // return value
    int rc = NO_ERROR;

    // This rlc file should be renamed if success, just skip it
    if (backUpFileType.compare("rlc") == 0)
    {
        return rc;
    }

    if (backUpFileType.compare("tmp") != 0 )
    {
        std::ostringstream oss;
        oss << backUpFileType << " is a bad type to confirm DbFile change: " <<
            filename;
        errMsg = oss.str();
        rc = ERR_HDFS_BACKUP;

        return rc;
    }

    // add safety checks, just in case
    std::string tmp(filename + ".tmp");
    if (!fFs.exists(tmp.c_str()))  // file already swapped
        return rc;

    if (fFs.size(tmp.c_str()) <= 0)
    {
        std::ostringstream oss;
        oss << "tmp file " << tmp << " has bad size" << fFs.size(tmp.c_str());
        errMsg = oss.str();
        rc = ERR_COMP_RENAME_FILE;

        return rc;
    }

    // remove the old orig if exists
    std::string orig(filename + ".orig");
    errno = 0;
    if ((fFs.exists(orig.c_str())) &&
        (fFs.remove(orig.c_str())) != 0)
    {
        int errNum = errno;
        std::ostringstream oss;
        oss << "remove old " << orig << " failed: " << strerror(errNum);
        errMsg = oss.str();
        rc = ERR_COMP_REMOVE_FILE;

        return rc;
    }

    // backup the original
    errno = 0;
    if (fFs.rename(filename.c_str(), orig.c_str()) != 0)
    {
        int errNum = errno;
        std::ostringstream oss;
        oss << "rename " << filename << " to " << orig << " failed: " <<
            strerror(errNum);
        errMsg = oss.str();
        rc = ERR_COMP_RENAME_FILE;

        return rc;
    }

    // rename the new file
    errno = 0;
    if (fFs.rename(tmp.c_str(), filename.c_str()) != 0)
    {
        int errNum = errno;
        std::ostringstream oss;
        oss << "rename " << tmp << " to " << filename << " failed: " <<
            strerror(errNum);
        errMsg = oss.str();
        rc = ERR_COMP_RENAME_FILE;

        return rc;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Finalize the changes to a db file.
// If success flag is true, then remove the orig
// otherwise, move the orig back to cdf
//------------------------------------------------------------------------------
int ConfirmHdfsDbFile::endDbFileChange(
    const std::string& backUpFileType,
    const std::string& filename,
    bool  success,
    std::string& errMsg) const
{
    // return value
    int rc = NO_ERROR;

    // This rlc file should be renamed if success, it is useless if failed.
    if (backUpFileType.compare("rlc") == 0)
    {
        std::string rlc(filename + ".rlc");
        if (fFs.exists(rlc.c_str()))
            fFs.remove(rlc.c_str()); // TBD-okay to ignore failed removal?

        return rc;
    }

    if (backUpFileType.compare("tmp") != 0)
    {
        std::ostringstream oss;
        oss << backUpFileType << " is a bad type to finalize DbFile change: " <<
            filename;
        errMsg = oss.str();
        rc = ERR_HDFS_BACKUP;

        return rc;
    }

    std::string orig(filename + ".orig");
    if (success)
    {
        // remove the orig file
        errno = 0;
        if ((fFs.exists(orig.c_str())) &&
            (fFs.remove(orig.c_str())) != 0)
        {
            int errNum = errno;
            std::ostringstream oss;
            oss << "remove " << orig << " failed: " << strerror(errNum);
            errMsg = oss.str();
            rc = ERR_COMP_REMOVE_FILE;

            return rc;
        }
    }
    else
    {
        // restore the orig file
        if (fFs.exists(orig.c_str()))
        {
            errno = 0;
            // Try to remove file only if it exists
            if ((fFs.exists(filename.c_str())) &&
                (fFs.remove(filename.c_str()) != 0))
            {
                int errNum = errno;
                std::ostringstream oss;
                oss << "failed restore; remove " << filename << " failed: " <<
                    strerror(errNum);
                errMsg = oss.str();
                rc = ERR_COMP_REMOVE_FILE;

                return rc;
            }

            errno = 0;
            if (fFs.rename(orig.c_str(), filename.c_str()) != 0)
            {
                int errNum = errno;
                std::ostringstream oss;
                oss << "failed restore; rename " << orig << " failed: " <<
                    strerror(errNum);
                errMsg = oss.str();
                rc = ERR_COMP_RENAME_FILE;

                return rc;
            }
        }

        // remove the tmp file
        std::string tmp(filename + ".tmp");
        errno = 0;
        if ((fFs.exists(tmp.c_str())) &&
            (fFs.remove(tmp.c_str())) != 0)
        {
            int errNum = errno;
            std::ostringstream oss;
            oss << "failed restore; remove " << tmp << " failed: " <<
                strerror(errNum);
            errMsg = oss.str();
            rc = ERR_COMP_REMOVE_FILE;

            return rc;
        }

        // remove the chunk shifting helper
        std::string rlc(filename + ".rlc");
        errno = 0;
        if ((fFs.exists(rlc.c_str())) &&
            (fFs.remove(rlc.c_str())) != 0)
        {
            int errNum = errno;
            std::ostringstream oss;
            oss << "failed restore; remove " << rlc << " failed: " <<
                strerror(errNum);
            errMsg = oss.str();
            rc = ERR_COMP_REMOVE_FILE;

            return rc;
        }
    }

    return rc;
}

//------------------------------------------------------------------------------
// Confirm the changes to the hwm DB files listed in the bulk rollback meta
// data file corresponding to the specified table OID.
//------------------------------------------------------------------------------
int ConfirmHdfsDbFile::confirmDbFileListFromMetaFile(
    OID tableOID,
    std::string& errMsg)
{
    int rc = NO_ERROR;

    try
    {
        std::vector<uint16_t> dbRoots;
        Config::getRootIdList( dbRoots );

        for (unsigned m=0; m<dbRoots.size(); m++)
        {
            std::istringstream metaDataStream;
            openMetaDataFile ( tableOID,
                dbRoots[m], metaDataStream );

            confirmDbFiles( metaDataStream );
        }
    }
    catch (WeException& ex)
    {
        std::ostringstream oss;
        oss << "Error confirming changes to table " << tableOID <<
            "; " << ex.what();
        errMsg = oss.str();
        rc = ex.errorCode();
    }
    catch (std::exception& ex)
    {
        std::ostringstream oss;
        oss << "Error confirming changes to table " << tableOID <<
            "; " << ex.what();
        errMsg = oss.str();
        rc = ERR_UNKNOWN;   
    }

    return rc;
}

//------------------------------------------------------------------------------
// Confirm the changes to the hwm DB files listed in the bulk rollback meta
// data file stream stored in metaDataStream.
//------------------------------------------------------------------------------
void ConfirmHdfsDbFile::confirmDbFiles(std::istringstream& metaDataStream) const
{
    char inBuf[ BUF_SIZE ];

    // Loop through the records in the meta-data file
    while (metaDataStream.getline( inBuf, BUF_SIZE ))
    {
        // Restore Files for current DBRoot
        if (RBMetaWriter::verifyColumn1Rec(inBuf))
        {
            confirmColumnDbFile(inBuf);
        }
        else if (RBMetaWriter::verifyDStore1Rec(inBuf))
        {
            confirmDctnryStoreDbFile(inBuf);
        }
    }
}

//------------------------------------------------------------------------------
// Confirm the changes to the hwm column DB file described in the bulk
// rollback meta data file record stored in inBuf.
//------------------------------------------------------------------------------
void ConfirmHdfsDbFile::confirmColumnDbFile(const char* inBuf) const
{
    char        recType[100];
    OID         columnOID;
    uint32_t    dbRootHwm;
    uint32_t    partNumHwm;
    uint32_t    segNumHwm;
    HWM         lastLocalHwm;
    int         colTypeInt;
    char        colTypeName[100];
    uint32_t    colWidth;
    int         compressionType = 0; // optional parameter

    // Read meta-data record
    int numFields = sscanf(inBuf, "%s %u %u %u %u %u %d %s %u %d",
        recType, &columnOID,
        &dbRootHwm, &partNumHwm, &segNumHwm, &lastLocalHwm,
        &colTypeInt, colTypeName, &colWidth, &compressionType );
    if (numFields < 9) // compressionType is optional
    {
        std::ostringstream oss;
        oss << "Invalid COLUM1 record in meta-data file " <<
            fMetaFileName << "; record-<" << inBuf << ">";

        throw WeException( oss.str(), ERR_INVALID_PARAM );
    }

    // Construct the DB file name
    char dbFileName[FILE_NAME_SIZE];
    FileOp dbFile(false);
    int rc = dbFile.getFileName( columnOID,
        dbFileName,
        dbRootHwm,
        partNumHwm,
        segNumHwm );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error constructing column filename to confirm changes" <<
            "; columnOID-" << columnOID  <<
            "; dbRoot-"    << dbRootHwm  <<
            "; partNum-"   << partNumHwm <<
            "; segNum-"    << segNumHwm  <<
            "; "           << ec.errorString(rc);

        throw WeException( oss.str(), rc );
    }

    // Confirm the changes to the DB file name
    std::string errMsg;
    rc = confirmDbFileChange( std::string("tmp"),
        dbFileName,
        errMsg );
    if (rc != NO_ERROR)
    {
        throw WeException( errMsg, rc );
    }
}

//------------------------------------------------------------------------------
// Confirm the changes to the hwm dctnry store DB file described in the bulk
// rollback meta data file record stored in inBuf.
//------------------------------------------------------------------------------
void ConfirmHdfsDbFile::confirmDctnryStoreDbFile(const char* inBuf) const
{
    char      recType[100];
    OID       dColumnOID;
    OID       dStoreOID;
    uint32_t  dbRootHwm;
    uint32_t  partNumHwm;
    uint32_t  segNumHwm;
    HWM       localHwm;
    int       compressionType = 0; // optional parameter

    // Read meta-data record
    int numFields = sscanf(inBuf, "%s %u %u %u %u %u %u %d",
        recType, &dColumnOID, &dStoreOID,
        &dbRootHwm, &partNumHwm, &segNumHwm, &localHwm, &compressionType );
    if (numFields < 7) // compressionType optional
    {
        std::ostringstream oss;
        oss << "Invalid DSTOR1 record in meta-data file " <<
            fMetaFileName << "; record-<" << inBuf << ">";

        throw WeException( oss.str(), ERR_INVALID_PARAM );
    }

    // Construct the DB file name
    char dbFileName[FILE_NAME_SIZE];
    FileOp dbFile(false);
    int rc = dbFile.getFileName( dStoreOID,
        dbFileName,
        dbRootHwm,
        partNumHwm,
        segNumHwm );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss<<"Error constructing dictionary store filename to confirm changes"<<
            "; columnOID-" << dStoreOID  <<
            "; dbRoot-"    << dbRootHwm  <<
            "; partNum-"   << partNumHwm <<
            "; segNum-"    << segNumHwm  <<
            "; "           << ec.errorString(rc);

        throw WeException( oss.str(), rc );
    }

    // Confirm the changes to the DB file name
    std::string errMsg;
    rc = confirmDbFileChange( std::string("tmp"),
        dbFileName,
        errMsg );
    if (rc != NO_ERROR)
    {
        throw WeException( errMsg, rc );
    }
}

//------------------------------------------------------------------------------
// End the changes to the hwm DB files listed in the bulk rollback meta
// data file corresponding to the specified table OID.  Delete temp files.
//------------------------------------------------------------------------------
int ConfirmHdfsDbFile::endDbFileListFromMetaFile(
    OID tableOID,
    bool success,
    std::string& errMsg)
{
    int rc = NO_ERROR;
    errMsg.clear();

    std::vector<uint16_t> dbRoots;
    Config::getRootIdList( dbRoots );

    for (unsigned m=0; m<dbRoots.size(); m++)
    {
        std::istringstream metaDataStream;
        try
        {
            std::istringstream metaDataStream;
            openMetaDataFile ( tableOID,
                dbRoots[m], metaDataStream );

            endDbFiles( metaDataStream, success );
        }
        // We catch any errors, but not deleting a temp file is not fatal,
        // so we capture the error msg and keep going if a problem occurs.
        // We return a concatenated list of error msgs if multiple errors
        // take place.
        catch (WeException& ex)
        {
            if (errMsg.size() == 0)
            {
                std::ostringstream oss;
                oss << "Error deleting temp files for table " << tableOID <<
                    "; " << ex.what();
                errMsg = oss.str();
                rc = ex.errorCode();
            }
            else
            {
                errMsg += "; ";
                errMsg += ex.what();
            }
        }
        catch (std::exception& ex)
        {
            if (errMsg.size() == 0)
            {
                std::ostringstream oss;
                oss << "Error deleting temp files for table " << tableOID <<
                    "; " << ex.what();
                errMsg = oss.str();
                rc = ERR_UNKNOWN;   
            }
            else
            {
                errMsg += "; ";
                errMsg += ex.what();
            }
        }
    }

    return rc;
}

//------------------------------------------------------------------------------
// End the changes to the hwm DB files listed in the bulk rollback meta
// data file stream stored in metaDataStream.  Delete temp files.
//------------------------------------------------------------------------------
void ConfirmHdfsDbFile::endDbFiles(
    std::istringstream& metaDataStream,
    bool success) const
{
    char inBuf[ BUF_SIZE ];
    std::string errMsg;
    int rc = NO_ERROR;

    // Loop through the records in the meta-data file
    while (metaDataStream.getline( inBuf, BUF_SIZE ))
    {
        try
        {
            // Delete Temp Files for current DBRoot
            if (RBMetaWriter::verifyColumn1Rec(inBuf))
            {
                endColumnDbFile(inBuf, success);
            }
            else if (RBMetaWriter::verifyDStore1Rec(inBuf))
            {
                endDctnryStoreDbFile(inBuf, success);
            }
        }
        // We catch any errors, but not deleting a temp file is not fatal,
        // so we capture the error msg and keep going if a problem occurs.
        // We return a concatenated list of error msgs if multiple errors
        // take place.
        catch (WeException& ex)
        {
            if (errMsg.size() == 0)
            {
                rc = ex.errorCode();
            }
            else
            {
                errMsg += "; ";
            }
            errMsg += ex.what();
        }
        catch (std::exception& ex)
        {
            if (errMsg.size() == 0)
            {
                rc = ERR_UNKNOWN;   
            }
            else
            {
                errMsg += "; ";
            }
            errMsg += ex.what();
        }
    }

    // Throw exception with cumulative list of any error msgs
    if (errMsg.size() > 0)
    {
        throw WeException( errMsg, rc );
    }
}

//------------------------------------------------------------------------------
// End the changes to the hwm column DB file described in the bulk
// rollback meta data file record stored in inBuf.  Delete the temp file.
//------------------------------------------------------------------------------
void ConfirmHdfsDbFile::endColumnDbFile(
    const char* inBuf,
    bool success) const
{
    char        recType[100];
    OID         columnOID;
    uint32_t    dbRootHwm;
    uint32_t    partNumHwm;
    uint32_t    segNumHwm;
    HWM         lastLocalHwm;
    int         colTypeInt;
    char        colTypeName[100];
    uint32_t    colWidth;
    int         compressionType = 0; // optional parameter

    // Read meta-data record
    int numFields = sscanf(inBuf, "%s %u %u %u %u %u %d %s %u %d",
        recType, &columnOID,
        &dbRootHwm, &partNumHwm, &segNumHwm, &lastLocalHwm,
        &colTypeInt, colTypeName, &colWidth, &compressionType );
    if (numFields < 9) // compressionType is optional
    {
        std::ostringstream oss;
        oss << "Invalid COLUM1 record in meta-data file " <<
            fMetaFileName << "; record-<" << inBuf << ">";

        throw WeException( oss.str(), ERR_INVALID_PARAM );
    }

    // Construct the DB file name
    char dbFileName[FILE_NAME_SIZE];
    FileOp dbFile(false);
    int rc = dbFile.getFileName( columnOID,
        dbFileName,
        dbRootHwm,
        partNumHwm,
        segNumHwm );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error constructing column filename to end changes" <<
            "; columnOID-" << columnOID  <<
            "; dbRoot-"    << dbRootHwm  <<
            "; partNum-"   << partNumHwm <<
            "; segNum-"    << segNumHwm  <<
            "; "           << ec.errorString(rc);

        throw WeException( oss.str(), rc );
    }

    // Confirm the changes to the DB file name
    std::string errMsg;
    rc = endDbFileChange( std::string("tmp"),
        dbFileName,
        success,
        errMsg );
    if (rc != NO_ERROR)
    {
        throw WeException( errMsg, rc );
    }
}

//------------------------------------------------------------------------------
// End the changes to the hwm dctnry store DB file described in the bulk
// rollback meta data file record stored in inBuf.  Delete the temp file.
//------------------------------------------------------------------------------
void ConfirmHdfsDbFile::endDctnryStoreDbFile(
    const char* inBuf,
    bool success) const
{
    char      recType[100];
    OID       dColumnOID;
    OID       dStoreOID;
    uint32_t  dbRootHwm;
    uint32_t  partNumHwm;
    uint32_t  segNumHwm;
    HWM       localHwm;
    int       compressionType = 0; // optional parameter

    // Read meta-data record
    int numFields = sscanf(inBuf, "%s %u %u %u %u %u %u %d",
        recType, &dColumnOID, &dStoreOID,
        &dbRootHwm, &partNumHwm, &segNumHwm, &localHwm, &compressionType );
    if (numFields < 7) // compressionType optional
    {
        std::ostringstream oss;
        oss << "Invalid DSTOR1 record in meta-data file " <<
            fMetaFileName << "; record-<" << inBuf << ">";

        throw WeException( oss.str(), ERR_INVALID_PARAM );
    }

    // Construct the DB file name
    char dbFileName[FILE_NAME_SIZE];
    FileOp dbFile(false);
    int rc = dbFile.getFileName( dStoreOID,
        dbFileName,
        dbRootHwm,
        partNumHwm,
        segNumHwm );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss<<"Error constructing dictionary store filename to end changes"<<
            "; columnOID-" << dStoreOID  <<
            "; dbRoot-"    << dbRootHwm  <<
            "; partNum-"   << partNumHwm <<
            "; segNum-"    << segNumHwm  <<
            "; "           << ec.errorString(rc);

        throw WeException( oss.str(), rc );
    }

    // Confirm the changes to the DB file name
    std::string errMsg;
    rc = endDbFileChange( std::string("tmp"),
        dbFileName,
        success,
        errMsg );
    if (rc != NO_ERROR)
    {
        throw WeException( errMsg, rc );
    }
}

//------------------------------------------------------------------------------
// Open and read the bulk rollback metadata file for the specified table OID
// and DBRoot.  The contents of the metadata file is returned in the meta-
// DataStream argument.
//------------------------------------------------------------------------------
void ConfirmHdfsDbFile::openMetaDataFile(OID tableOID,
    uint16_t dbRoot,
    std::istringstream& metaDataStream)
{
    std::string bulkRollbackPath( Config::getDBRootByNum( dbRoot ) );

    // Construct file name and check for it's existence
    std::ostringstream ossFileName;
    ossFileName << '/' << DBROOT_BULK_ROLLBACK_SUBDIR << '/' << tableOID;
    fMetaFileName  = bulkRollbackPath;
    fMetaFileName += ossFileName.str();

    // Return if the meta-data file does not exist.
    if ( !fFs.exists( fMetaFileName.c_str() ) )
    {
        std::ostringstream oss;
        oss << "Bulk rollback meta-data file " <<
            fMetaFileName << " does not exist.";

        throw WeException( oss.str(), ERR_FILE_NOT_EXIST );
    }

    // Open the file
    boost::scoped_ptr<IDBDataFile> metaFile;
    errno = 0;
    metaFile.reset(idbdatafile::IDBDataFile::open(
        idbdatafile::IDBPolicy::getType(fMetaFileName.c_str(),
            idbdatafile::IDBPolicy::WRITEENG),
        fMetaFileName.c_str(), "rb", 0) );

    if ( !metaFile )
    {
        int errRc = errno;
        std::ostringstream oss;
        oss << "Error opening bulk rollback meta-data file " <<
            fMetaFileName << "; err-" <<
            errRc << "; " << strerror( errRc );

        throw WeException( oss.str(), ERR_FILE_OPEN );
    }

    // First record in the file must be a Version record.
    char inBuf[ BUF_SIZE ];
    ssize_t metaFileSize = fFs.size( fMetaFileName.c_str() );
    boost::scoped_array<char> buf( new char[ metaFileSize ] );
    // retry 10 times for partial reads, just in case
    ssize_t readSofar = 0; // bytes read so far
    ssize_t bytes = 0;    // bytes read by one pread
    char* p = buf.get();
    for (int i = 0; i < 10 && readSofar < metaFileSize; i++)
    {
         errno = 0;
         bytes = metaFile->pread( p+readSofar,
             readSofar,
             metaFileSize-readSofar);
         if (bytes < 0)
             break;

         readSofar += bytes;
    }
    if ( readSofar != metaFileSize )
    {
        int errRc = errno;
        std::ostringstream oss;
        oss << "Error reading bulk rollback meta-data file "
            << fMetaFileName << "; read/expect:" << readSofar << "/"
            << metaFileSize
            << "; err-" << errRc << "; " << strerror( errRc );

        throw WeException( oss.str(), ERR_FILE_READ );
    }

    // put the data in a string stream
    metaDataStream.str( std::string( p, metaFileSize ) );
    buf.reset();

    // read data
    metaDataStream.getline( inBuf, BUF_SIZE );
    if (!RBMetaWriter::verifyVersion4(inBuf))
    {
        std::ostringstream oss;
        oss << "Invalid version record in meta-data file " << fMetaFileName
            << "; record-<" << inBuf << ">";

        throw WeException( oss.str(), ERR_INVALID_PARAM );
    }
}

} // end of namespace
