/* Copyright (C) 2013 Calpont Corp.

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

//  $Id: we_fileop.cpp 4737 2013-08-14 20:45:46Z bwilkinson $

#include "config.h"

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdexcept>
#if defined(__FreeBSD__)
#include <sys/param.h>
#include <sys/mount.h>
#elif !defined(_MSC_VER)
#include <sys/vfs.h>
#endif
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/scoped_array.hpp>
using namespace std;

#define WRITEENGINEFILEOP_DLLEXPORT
#include "we_fileop.h"
#undef WRITEENGINEFILEOP_DLLEXPORT
#include "we_convertor.h"
#include "we_log.h"
#include "we_config.h"
#include "we_stats.h"
#include "we_simplesyslog.h"

#include "idbcompress.h"
using namespace compress;

#include "messagelog.h"
using namespace logging;

#include "IDBDataFile.h"
#include "IDBFileSystem.h"
#include "IDBPolicy.h"
using namespace idbdatafile;

namespace WriteEngine
{

   /*static*/ boost::mutex               FileOp::m_createDbRootMutexes;
   /*static*/ boost::mutex               FileOp::m_mkdirMutex;
   /*static*/ std::map<int,boost::mutex*> FileOp::m_DbRootAddExtentMutexes;
   const int MAX_NBLOCKS = 8192; // max number of blocks written to an extent
                                 // in 1 call to fwrite(), during initialization

//StopWatch timer;

/**
 * Constructor
 */
FileOp::FileOp(bool doAlloc) : m_compressionType(0),
    m_transId((TxnID)INVALID_NUM), m_buffer(0)
{
   if (doAlloc)
   {
      m_buffer = new char[DEFAULT_BUFSIZ];
      memset(m_buffer, '\0', DEFAULT_BUFSIZ);
   }
}

/**
 * Default Destructor
 */
FileOp::~FileOp()
{
   if (m_buffer)
   {
       delete [] m_buffer;
   }
   m_buffer = 0;
}

/***********************************************************
 * DESCRIPTION:
 *    Close a file
 * PARAMETERS:
 *    pFile - file handle
 * RETURN:
 *    none
 ***********************************************************/
void FileOp::closeFile( IDBDataFile* pFile ) const
{
	delete pFile;
}

/***********************************************************
 * DESCRIPTION:
 *    Create directory
 *    Function uses mutex lock to prevent thread contention trying to create
 *    2 subdirectories in the same directory at the same time.
 * PARAMETERS:
 *    dirName - directory name
 *    mode - create mode
 * RETURN:
 *    NO_ERROR if success, otherwise if fail
 ***********************************************************/
int FileOp::createDir( const char* dirName, mode_t mode ) const
{
    boost::mutex::scoped_lock lk(m_mkdirMutex);
    int rc = IDBPolicy::mkdir( dirName );
    if ( rc != 0 )
    {
        int errRc = errno;
        if (errRc == EEXIST)
            return NO_ERROR; // ignore "File exists" error

        if ( getLogger() )
        {
            std::ostringstream oss;
            std::string errnoMsg;
            Convertor::mapErrnoToString(errRc, errnoMsg);
            oss << "Error creating directory " << dirName << "; err-" <<
               errRc << "; " << errnoMsg;
            getLogger()->logMsg( oss.str(), ERR_DIR_CREATE, MSGLVL_ERROR );
        }
        return ERR_DIR_CREATE;
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Create the "first" segment file for a column with a fixed file size
 *    Note: the file is created in binary mode
 * PARAMETERS:
 *    fileName - file name with complete path
 *    numOfBlock - the total number of blocks to be initialized (written out)
 *    compressionType - Compression Type
 *    emptyVal - empty value used to initialize column values
 *    width    - width of column in bytes
 *    dbRoot   - DBRoot of column file we are creating
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_EXIST if file exists
 *    ERR_FILE_CREATE if can not create the file
 ***********************************************************/
int FileOp::createFile( const char* fileName, int numOfBlock,
                              uint64_t emptyVal, int width,
                              uint16_t dbRoot )
{
	IDBDataFile* pFile =
    	IDBDataFile::open(
    					IDBPolicy::getType( fileName, IDBPolicy::WRITEENG ),
    					fileName,
    					"w+b",
    					IDBDataFile::USE_VBUF,
                        width);
    int rc = 0;
    if( pFile != NULL ) {

        // Initialize the contents of the extent.
        if (m_compressionType)
        {
            rc = initAbbrevCompColumnExtent( pFile,
                               dbRoot,
                               numOfBlock,
                               emptyVal,
                               width );
        }
        else
        {
            rc = initColumnExtent( pFile,
                               dbRoot,
                               numOfBlock,
                               emptyVal,
                               width,
                               true,    // new file
                               false,   // don't expand; add new extent
                               true );  // add abbreviated extent
        }

        closeFile( pFile );
    }
    else
        return ERR_FILE_CREATE;

    return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Create the "first" segment file for a column with a fixed file size
 *    Note: the file is created in binary mode
 * PARAMETERS:
 *    fid      - OID of the column file to be created
 *    allocSize (out) - number of blocks allocated to the first extent
 *    dbRoot   - DBRoot where file is to be located
 *    partition- Starting partition number for segment file path
 *    compressionType - Compression type
 *    colDataType - the column data type
 *    emptyVal - designated "empty" value for this OID
 *    width    - width of column in bytes
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_EXIST if file exists
 *    ERR_FILE_CREATE if can not create the file
 ***********************************************************/
int FileOp::createFile(FID fid,
    int&     allocSize,
    uint16_t dbRoot,
    uint32_t partition,
    execplan::CalpontSystemCatalog::ColDataType colDataType,
    uint64_t  emptyVal,
    int      width)
{
    //std::cout << "Creating file oid: " << fid <<
    //    "; compress: " << m_compressionType << std::endl;
    char  fileName[FILE_NAME_SIZE];
    int   rc;

    uint16_t segment = 0; // should always be 0 when starting a new column
    RETURN_ON_ERROR( ( rc = oid2FileName( fid, fileName, true,
        dbRoot, partition, segment ) ) );

    //@Bug 3196
    if( exists( fileName ) )
        return ERR_FILE_EXIST;
    // allocatColExtent() treats dbRoot and partition as in/out
    // arguments, so we need to pass in a non-const variable.
    uint16_t dbRootx    = dbRoot;
    uint32_t partitionx = partition;

    // Since we are creating a new column OID, we know partition
    // and segment are 0, so we ignore their output values.
//timer.start( "allocateColExtent" );

    BRM::LBID_t startLbid;
    u_int32_t startBlock;
    RETURN_ON_ERROR( BRMWrapper::getInstance()->allocateColExtentExactFile(
        (const OID)fid, (u_int32_t)width, dbRootx, partitionx, segment, colDataType,
        startLbid, allocSize, startBlock) );

    // We allocate a full extent from BRM, but only write an abbreviated 256K
    // rows to disk for 1st extent, to conserve disk usage for small tables.
    // One exception here is if we have rolled off partition 0, and we are
    // adding a column to an existing table, then we are adding a column
    // whose first partition is not 0.  In this case, we know we are not
    // dealing with a small table, so we init a full extent for 1st extent.
    int totalSize = 0;
    if (partition == 0)
        totalSize = (INITIAL_EXTENT_ROWS_TO_DISK/BYTE_PER_BLOCK) * width;
    else
        totalSize = allocSize; // full extent if starting partition > 0

    // Note we can't pass full file name to isDiskSpaceAvail() because the
    // file does not exist yet, but passing DBRoot directory should suffice.
    if ( !isDiskSpaceAvail(Config::getDBRootByNum(dbRoot), totalSize) )
    {
        return ERR_FILE_DISK_SPACE;
    }
//timer.stop( "allocateColExtent" );

    return createFile( fileName, totalSize, emptyVal, width, dbRoot );
}

/***********************************************************
 * DESCRIPTION:
 *    Delete a file
 * PARAMETERS:
 *    fileName - file name with complete path
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_NOT_EXIST if file does not exist
 *    ERR_FILE_DELETE if can not delete a file
 ***********************************************************/
int FileOp::deleteFile( const char* fileName ) const
{
    if( !exists( fileName ) )
        return ERR_FILE_NOT_EXIST;

    return ( IDBPolicy::remove( fileName ) == -1 ) ? ERR_FILE_DELETE : NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Deletes all the segment or dictionary store files associated with the
 *    specified fid.
 * PARAMETERS:
 *    fid - OID of the column being deleted.
 * RETURN:
 *    NO_ERROR if success
 *    ERR_DM_CONVERT_OID if error occurs converting OID to file name
 ***********************************************************/
int FileOp::deleteFile( FID fid ) const
{
    char tempFileName[FILE_NAME_SIZE];
    char oidDirName  [FILE_NAME_SIZE];
    char dbDir       [MAX_DB_DIR_LEVEL][MAX_DB_DIR_NAME_SIZE];

    RETURN_ON_ERROR((Convertor::oid2FileName(
        fid, tempFileName, dbDir, 0, 0)));
    sprintf(oidDirName, "%s/%s/%s/%s",
        dbDir[0], dbDir[1], dbDir[2], dbDir[3]);
    //std::cout << "Deleting files for OID " << fid <<
    //             "; dirpath: " << oidDirName << std::endl;
    //need check return code.
    RETURN_ON_ERROR(BRMWrapper::getInstance()->deleteOid(fid));

    std::vector<std::string> dbRootPathList;
    Config::getDBRootPathList( dbRootPathList );
    for (unsigned i = 0; i < dbRootPathList.size(); i++)
    {
        char rootOidDirName[FILE_NAME_SIZE];
        sprintf(rootOidDirName, "%s/%s", dbRootPathList[i].c_str(), oidDirName);

        if( IDBPolicy::remove( rootOidDirName ) != 0 )
        {
        	ostringstream oss;
        	oss << "Unable to remove " << rootOidDirName;
        	throw std::runtime_error( oss.str() );
        }
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Deletes all the segment or dictionary store files associated with the
 *    specified fid.
 * PARAMETERS:
 *    fid - OIDs of the column/dictionary being deleted.
 * RETURN:
 *    NO_ERROR if success
 *    ERR_DM_CONVERT_OID if error occurs converting OID to file name
 ***********************************************************/
int FileOp::deleteFiles( const std::vector<int32_t>& fids ) const
{
    char tempFileName[FILE_NAME_SIZE];
    char oidDirName  [FILE_NAME_SIZE];
    char dbDir       [MAX_DB_DIR_LEVEL][MAX_DB_DIR_NAME_SIZE];
    std::vector<std::string> dbRootPathList;
    Config::getDBRootPathList( dbRootPathList );
    for ( unsigned n=0; n<fids.size(); n++ )
    {
        RETURN_ON_ERROR((Convertor::oid2FileName(
            fids[n], tempFileName, dbDir, 0, 0)));
        sprintf(oidDirName, "%s/%s/%s/%s",
            dbDir[0], dbDir[1], dbDir[2], dbDir[3]);
      //std::cout << "Deleting files for OID " << fid <<
      //             "; dirpath: " << oidDirName << std::endl;

        for (unsigned i = 0; i < dbRootPathList.size(); i++)
        {
            char rootOidDirName[FILE_NAME_SIZE];
            sprintf(rootOidDirName, "%s/%s", dbRootPathList[i].c_str(),
                oidDirName);

            if( IDBPolicy::remove( rootOidDirName ) != 0 )
            {
            	ostringstream oss;
            	oss << "Unable to remove " << rootOidDirName;
            	throw std::runtime_error( oss.str() );
            }
        }
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Deletes all the segment or dictionary store files associated with the
 *    specified fid and partition.
 * PARAMETERS:
 *    fids - OIDs of the column/dictionary being deleted.
 *    partition - the partition number
 * RETURN:
 *    NO_ERROR if success
 *    ERR_DM_CONVERT_OID if error occurs converting OID to file name
 ***********************************************************/
int FileOp::deletePartitions( const std::vector<OID>& fids, 
    const std::vector<BRM::PartitionInfo>& partitions ) const
{
    char tempFileName[FILE_NAME_SIZE];
    char oidDirName  [FILE_NAME_SIZE];
    char dbDir       [MAX_DB_DIR_LEVEL][MAX_DB_DIR_NAME_SIZE];
    char rootOidDirName[FILE_NAME_SIZE];
    char partitionDirName[FILE_NAME_SIZE];

    for (uint i = 0; i < partitions.size(); i++)
    {
        RETURN_ON_ERROR((Convertor::oid2FileName(
            partitions[i].oid, tempFileName, dbDir,
            partitions[i].lp.pp, partitions[i].lp.seg)));
        sprintf(oidDirName, "%s/%s/%s/%s/%s",
            dbDir[0], dbDir[1], dbDir[2], dbDir[3], dbDir[4]);
        // config expects dbroot starting from 0
        std::string rt( Config::getDBRootByNum(partitions[i].lp.dbroot) );
        sprintf(rootOidDirName, "%s/%s",
            rt.c_str(), tempFileName);
        sprintf(partitionDirName, "%s/%s",
            rt.c_str(), oidDirName);

        if( IDBPolicy::remove( rootOidDirName ) != 0 )
        {
        	ostringstream oss;
        	oss << "Unable to remove " << rootOidDirName;
        	throw std::runtime_error( oss.str() );
        }

        list<string> dircontents;
        if( IDBPolicy::listDirectory( partitionDirName, dircontents ) == 0 )
		{
			// the directory exists, now check if empty
        	if( dircontents.size() == 0 )
        	{
        		// empty directory
                if( IDBPolicy::remove( partitionDirName ) != 0 )
                {
                	ostringstream oss;
                	oss << "Unable to remove " << rootOidDirName;
                	throw std::runtime_error( oss.str() );
                }
        	}
		}
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Deletes the specified db segment file.
 * PARAMETERS:
 *    fid       - column OID of file to be deleted.
 *    dbRoot    - DBRoot associated with segment file
 *    partition - partition number of associated segment file
 *    segment   - segment number of associated segment file
 * RETURN:
 *    NO_ERROR if success
 ***********************************************************/
int FileOp::deleteFile( FID fid, u_int16_t dbRoot,
    u_int32_t partition, u_int16_t segment ) const
{
    char fileName[FILE_NAME_SIZE];

    RETURN_ON_ERROR( getFileName( fid, fileName,
        dbRoot, partition, segment) );

    return ( deleteFile( fileName ) );
}

/***********************************************************
 * DESCRIPTION:
 *    Check whether a file exists or not
 * PARAMETERS:
 *    fileName - file name with complete path
 * RETURN:
 *    true if exists, false otherwise
 ***********************************************************/
bool FileOp::exists( const char* fileName ) const
{
    return IDBPolicy::exists( fileName );
}

/***********************************************************
 * DESCRIPTION:
 *    Check whether a file exists or not
 * PARAMETERS:
 *    fid       - OID of file to be checked
 *    dbRoot    - DBRoot associated with segment file
 *    partition - partition number of associated segment file
 *    segment   - segment number of associated segment file
 * RETURN:
 *    true if exists, false otherwise
 ***********************************************************/
bool FileOp::exists( FID fid, u_int16_t dbRoot,
    u_int32_t partition, u_int16_t segment ) const
{
    char fileName[FILE_NAME_SIZE];

    if (getFileName(fid, fileName, dbRoot, partition,
        segment) != NO_ERROR)
        return false;

    return exists( fileName );
}

/***********************************************************
 * DESCRIPTION:
 *    Check whether an OID directory exists or not
 * PARAMETERS:
 *    fid - column or dictionary store OID
 * RETURN:
 *    true if exists, false otherwise
 ***********************************************************/
bool FileOp::existsOIDDir( FID fid ) const
{
    char fileName[FILE_NAME_SIZE];

    if (oid2DirName( fid, fileName ) != NO_ERROR)
	{
        return false;
	}

    return exists( fileName );
}

/***********************************************************
 * DESCRIPTION:
 *    Adds an extent to the specified column OID and DBRoot.
 *    Function uses ExtentMap to add the extent and determine which
 *    specific column segment file the extent is to be added to. If
 *    the applicable column segment file does not exist, it is created.
 *    If this is the very first file for the specified DBRoot, then the
 *    partition and segment number must be specified, else the selected
 *    partition and segment numbers are returned.
 * PARAMETERS:
 *    oid       - OID of the column to be extended
 *    emptyVal  - Empty value to be used for oid
 *    width     - Width of the column (in bytes)
 *    hwm       - The HWM (or fbo) of the column segment file where the new
 *                extent begins.
 *    startLbid - The starting LBID for the new extent
 *    allocSize - Number of blocks allocated to the extent.
 *    dbRoot    - The DBRoot of the file with the new extent.
 *    partition - The partition number of the file with the new extent.
 *    segment   - The segment number of the file with the new extent.
 *    segFile   - The name of the relevant column segment file.
 *    pFile     - IDBDataFile ptr to the file where the extent is added.
 *    newFile   - Indicates if the extent was added to a new or existing file
 *    hdrs      - Contents of the headers if file is compressed.
 * RETURN:
 *    NO_ERROR if success
 *    else the applicable error code is returned
 ***********************************************************/
int FileOp::extendFile(
    OID          oid,
    uint64_t     emptyVal,
    int          width,
    HWM          hwm,
    BRM::LBID_t  startLbid,
    int          allocSize,
    uint16_t     dbRoot,
    uint32_t     partition,
    uint16_t     segment,
    std::string& segFile,
    IDBDataFile*& pFile,
    bool&        newFile,
    char*        hdrs) 
{
    int rc = NO_ERROR;
    pFile = 0;
    segFile.clear();
    newFile = false;
    char fileName[FILE_NAME_SIZE];

    // If starting hwm or fbo is 0 then this is the first extent of a new file,
    // else we are adding an extent to an existing segment file
    if (hwm > 0) // db segment file should exist
    {
        RETURN_ON_ERROR( oid2FileName(oid, fileName, false,
            dbRoot, partition, segment) );
        segFile = fileName;

        if (!exists(fileName))
        {
            ostringstream oss;
            oss << "oid: " << oid << " with path " << segFile;
            logging::Message::Args args;
            args.add("File not found ");
            args.add(oss.str());
            args.add("");
            args.add("");
            SimpleSysLog::instance()->logMsg(args,
                logging::LOG_TYPE_ERROR,
                logging::M0001);
            return ERR_FILE_NOT_EXIST;
        }

        pFile = openFile( oid, dbRoot, partition, segment,
            segFile, "r+b" );//old file
        if (pFile == 0)
        {
            ostringstream oss;
            oss << "oid: " << oid << " with path " << segFile;
            logging::Message::Args args;
            args.add("Error opening file ");
            args.add(oss.str());
            args.add("");
            args.add("");
            SimpleSysLog::instance()->logMsg(args,
                logging::LOG_TYPE_ERROR,
                logging::M0001);
            return ERR_FILE_OPEN;
        }

        if ( isDebug(DEBUG_1) && getLogger() )
        {
            std::ostringstream oss;
            oss << "Opening existing column file"  <<
                   ": OID-"    << oid       <<
                   "; DBRoot-" << dbRoot    <<
                   "; part-"   << partition <<
                   "; seg-"    << segment   <<
                   "; LBID-"   << startLbid <<
                   "; hwm-"    << hwm       <<
                   "; file-"   << segFile;
            getLogger()->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        // @bug 5349: check that new extent's fbo is not past current EOF
        if (m_compressionType)
        {
            char hdrsIn[ compress::IDBCompressInterface::HDR_BUF_LEN * 2 ];
            RETURN_ON_ERROR( readHeaders(pFile, hdrsIn) );

            IDBCompressInterface compressor;
            unsigned int ptrCount   = compressor.getPtrCount(hdrsIn);
            unsigned int chunkIndex = 0;
            unsigned int blockOffsetWithinChunk = 0;
            compressor.locateBlock((hwm-1), chunkIndex, blockOffsetWithinChunk);

            //std::ostringstream oss1;
            //oss1 << "Extending compressed column file"<<
            //   ": OID-"    << oid       <<
            //   "; LBID-"   << startLbid <<
            //   "; fbo-"    << hwm       <<
            //   "; file-"   << segFile   <<
            //   "; chkidx-" << chunkIndex<<
            //   "; numPtrs-"<< ptrCount;
            //getLogger()->logMsg( oss1.str(), MSGLVL_INFO2 );

            if (chunkIndex >= ptrCount)
            {
                ostringstream oss;
                oss << "oid: " << oid << " with path " << segFile <<
                    "; new extent fbo " << hwm << "; number of "
                    "compressed chunks " << ptrCount;
                logging::Message::Args args;
                args.add("compressed");
                args.add(oss.str());
                SimpleSysLog::instance()->logMsg(args,
                    logging::LOG_TYPE_ERROR,
                    logging::M0103);
                return ERR_FILE_NEW_EXTENT_FBO;
            }

            if (hdrs)
            {
                memcpy(hdrs, hdrsIn, sizeof(hdrsIn) );
            }
        }
        else
        {
            long long fileSize;
            RETURN_ON_ERROR( getFileSize(pFile, fileSize) );
            long long calculatedFileSize = ((long long)hwm) * BYTE_PER_BLOCK;

            //std::ostringstream oss2;
            //oss2 << "Extending uncompressed column file"<<
            //   ": OID-"    << oid       <<
            //   "; LBID-"   << startLbid <<
            //   "; fbo-"    << hwm       <<
            //   "; file-"   << segFile   <<
            //   "; filesize-"<<fileSize;
            //getLogger()->logMsg( oss2.str(), MSGLVL_INFO2 );

            if (calculatedFileSize > fileSize)
            {
                ostringstream oss;
                oss << "oid: " << oid << " with path " << segFile <<
                    "; new extent fbo " << hwm << "; file size (bytes) " <<
                    fileSize;
                logging::Message::Args args;
                args.add("uncompressed");
                args.add(oss.str());
                SimpleSysLog::instance()->logMsg(args,
                    logging::LOG_TYPE_ERROR,
                    logging::M0103);
                return ERR_FILE_NEW_EXTENT_FBO;
            }
        }
    }
    else // db segment file should not exist
    {
        RETURN_ON_ERROR( oid2FileName(oid, fileName, true,
            dbRoot, partition, segment) );
        segFile = fileName;

        // if obsolete file exists, "w+b" will truncate and write over
        pFile = openFile( fileName, "w+b" );//new file
        if (pFile == 0)
            return ERR_FILE_CREATE;

        newFile = true;
        if ( isDebug(DEBUG_1) && getLogger() )
        {
            std::ostringstream oss;
            oss << "Opening new column file"<<
                   ": OID-"    << oid       <<
                   "; DBRoot-" << dbRoot    <<
                   "; part-"   << partition <<
                   "; seg-"    << segment   <<
                   "; LBID-"   << startLbid <<
                   "; hwm-"    << hwm       <<
                   "; file-"   << segFile;
            getLogger()->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        if ((m_compressionType) && (hdrs))
        {
            IDBCompressInterface compressor;
            compressor.initHdr(hdrs, m_compressionType);
        }
    }
#ifdef _MSC_VER
    //Need to call the win version with a dir, not a file
    if (!isDiskSpaceAvail(Config::getDBRootByNum(dbRoot), allocSize))
#else
    if ( !isDiskSpaceAvail(segFile, allocSize) )
#endif
    {
        return ERR_FILE_DISK_SPACE;
    }

    // We set to EOF just before we start adding the blocks for the new extent.
    // At one time, I considered changing this to seek to the HWM block, but
    // with compressed files, this is murky; do I find and seek to the chunk
    // containing the HWM block?  So I left as-is for now, seeking to EOF.
    rc = setFileOffset(pFile, 0, SEEK_END);
    if (rc != NO_ERROR)
        return rc;

    // Initialize the contents of the extent.
    rc = initColumnExtent( pFile,
                           dbRoot,
                           allocSize,
                           emptyVal,
                           width,
                           newFile, // new or existing file
                           false,   // don't expand; new extent
                           false ); // add full (not abbreviated) extent

    return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Add an extent to the exact segment file specified by
 *    the designated OID, DBRoot, partition, and segment.
 * PARAMETERS:
 *    oid       - OID of the column to be extended
 *    emptyVal  - Empty value to be used for oid
 *    width     - Width of the column (in bytes)
 *    allocSize - Number of blocks allocated to the extent.
 *    dbRoot    - The DBRoot of the file with the new extent.
 *    partition - The partition number of the file with the new extent.
 *    segment   - The segment number of the file with the new extent.
 *    segFile   - The name of the relevant column segment file.
 *    startLbid - The starting LBID for the new extent
 *    newFile   - Indicates if the extent was added to a new or existing file
 *    hdrs      - Contents of the headers if file is compressed.
 * RETURN:
 *    none
 ***********************************************************/
int FileOp::addExtentExactFile(
    OID          oid,
    uint64_t     emptyVal,
    int          width,
    int&         allocSize,
    uint16_t     dbRoot,
    uint32_t     partition,
    uint16_t     segment,
    execplan::CalpontSystemCatalog::ColDataType colDataType,
    std::string& segFile,
    BRM::LBID_t& startLbid,
    bool&        newFile,
    char*        hdrs) 
{
    int rc = NO_ERROR;
    IDBDataFile* pFile = 0;
    segFile.clear();
    newFile = false;
    HWM         hwm;

    // Allocate the new extent in the ExtentMap
    RETURN_ON_ERROR( BRMWrapper::getInstance()->allocateColExtentExactFile(
        oid, width, dbRoot, partition, segment, colDataType, startLbid, allocSize, hwm));

    // Determine the existence of the "next" segment file, and either open
    // or create the segment file accordingly.
    if (exists(oid, dbRoot, partition, segment))
    {
        pFile = openFile( oid, dbRoot, partition, segment,
            segFile, "r+b" );//old file
        if (pFile == 0)
        {
            ostringstream oss;
            oss << "oid: " << oid << " with path " << segFile;
            logging::Message::Args args;
            args.add("Error opening file ");
            args.add(oss.str());
            args.add("");
            args.add("");
            SimpleSysLog::instance()->logMsg(args,
                logging::LOG_TYPE_ERROR,
                logging::M0001);
            return ERR_FILE_OPEN;
        }

        if ( isDebug(DEBUG_1) && getLogger() )
        {
            std::ostringstream oss;
            oss << "Opening existing column file"  <<
                   ": OID-"    << oid       <<
                   "; DBRoot-" << dbRoot    <<
                   "; part-"   << partition <<
                   "; seg-"    << segment   <<
                   "; LBID-"   << startLbid <<
                   "; hwm-"    << hwm       <<
                   "; file-"   << segFile;
            getLogger()->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        if ((m_compressionType) && (hdrs))
        {
            rc = readHeaders(pFile, hdrs);
            if (rc != NO_ERROR)
                return rc;
        }
    }
    else
    {
        char fileName[FILE_NAME_SIZE];
        RETURN_ON_ERROR( oid2FileName(oid, fileName, true,
            dbRoot, partition, segment) );
        segFile = fileName;

        pFile = openFile( fileName, "w+b" );//new file
        if (pFile == 0)
            return ERR_FILE_CREATE;

        newFile = true;
        if ( isDebug(DEBUG_1) && getLogger() )
        {
            std::ostringstream oss;
            oss << "Opening new column file"<<
                   ": OID-"    << oid       <<
                   "; DBRoot-" << dbRoot    <<
                   "; part-"   << partition <<
                   "; seg-"    << segment   <<
                   "; LBID-"   << startLbid <<
                   "; hwm-"    << hwm       <<
                   "; file-"   << segFile;
            getLogger()->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        if ((m_compressionType) && (hdrs))
        {
            IDBCompressInterface compressor;
            compressor.initHdr(hdrs, m_compressionType);
        }
    }
#ifdef _MSC_VER
    //Need to call the win version with a dir, not a file
    if (!isDiskSpaceAvail(Config::getDBRootByNum(dbRoot), allocSize))
#else
    if ( !isDiskSpaceAvail(segFile, allocSize) )
#endif
    {
        return ERR_FILE_DISK_SPACE;
    }

    // We set to EOF just before we start adding the blocks for the new extent.
    // At one time, I considered changing this to seek to the HWM block, but
    // with compressed files, this is murky; do I find and seek to the chunk
    // containing the HWM block?  So I left as-is for now, seeking to EOF.
    rc = setFileOffset(pFile, 0, SEEK_END);
    if (rc != NO_ERROR)
        return rc;

    // Initialize the contents of the extent.
    rc = initColumnExtent( pFile,
                           dbRoot,
                           allocSize,
                           emptyVal,
                           width,
                           newFile, // new or existing file
                           false,   // don't expand; new extent
                           false ); // add full (not abbreviated) extent

    closeFile( pFile );
    return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Write out (initialize) an extent in a column file.
 *    A mutex is used for each DBRoot, to prevent contention between
 *    threads, because if multiple threads are creating extents on
 *    the same DBRoot at the same time, the extents can become
 *    fragmented.  It is best to only create one extent at a time
 *    on each DBRoot.
 *    This function can be used to initialize an entirely new extent, or
 *    to finish initializing an extent that has already been started.
 *    nBlocks controls how many 8192-byte blocks are to be written out.
 * PARAMETERS:
 *    pFile   (in) - IDBDataFile* of column segment file to be written to
 *    dbRoot  (in) - DBRoot of pFile
 *    nBlocks (in) - number of blocks to be written for an extent
 *    emptyVal(in) - empty value to be used for column data values
 *    width   (in) - width of the applicable column
 *    bNewFile(in) - are we adding an extent to a new file, in which case
 *                   headers will be included "if" it is a compressed file.
 *    bExpandExtent (in) - Expand existing extent, or initialize a new one
 *    bAbbrevExtent(in) - if creating new extent, is it an abbreviated extent
 * RETURN:
 *    returns ERR_FILE_WRITE if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
int FileOp::initColumnExtent(
    IDBDataFile* pFile,
    uint16_t dbRoot,
    int      nBlocks,
    uint64_t emptyVal,
    int      width,
    bool     bNewFile,
    bool     bExpandExtent,
    bool     bAbbrevExtent )
{
    if ((bNewFile) && (m_compressionType))
    {
        char hdrs[IDBCompressInterface::HDR_BUF_LEN*2];
        IDBCompressInterface compressor;
        compressor.initHdr(hdrs, m_compressionType);
        if (bAbbrevExtent)
            compressor.setBlockCount(hdrs, nBlocks);
        RETURN_ON_ERROR(writeHeaders(pFile, hdrs));
    }

    // @bug5769 Don't initialize extents or truncate db files on HDFS
    if (idbdatafile::IDBPolicy::useHdfs())
    {
        //@Bug 3219. update the compression header after the extent is expanded.
        if ((!bNewFile) && (m_compressionType) && (bExpandExtent))
        {
            updateColumnExtent(pFile, nBlocks);
        }

        // @bug 2378. Synchronize here to avoid write buffer pile up too much,
        // which could cause controllernode to timeout later when it needs to
        // save a snapshot.
        pFile->flush();
    }
    else
    {
        // Create vector of mutexes used to serialize extent access per DBRoot
        initDbRootExtentMutexes( );

        // Determine the number of blocks in each call to fwrite(), and the
        // number of fwrite() calls to make, based on this.  In other words,
        // we put a cap on the "writeSize" so that we don't allocate and write
        // an entire extent at once for the 64M row extents.  If we are
        // expanding an abbreviated 64M extent, we may not have an even
        // multiple of MAX_NBLOCKS to write; remWriteSize is the number of
        // blocks above and beyond loopCount*MAX_NBLOCKS.
        int writeSize = nBlocks * BYTE_PER_BLOCK; // 1M and 8M row extent size
        int loopCount = 1;
        int remWriteSize = 0;
        if (nBlocks > MAX_NBLOCKS)                // 64M row extent size
        {
            writeSize = MAX_NBLOCKS * BYTE_PER_BLOCK;
            loopCount = nBlocks / MAX_NBLOCKS;
            remWriteSize = nBlocks - (loopCount * MAX_NBLOCKS);
        }

        // Allocate a buffer, initialize it, and use it to create the extent
        idbassert(dbRoot > 0);
#ifdef PROFILE
        if (bExpandExtent)
            Stats::startParseEvent(WE_STATS_WAIT_TO_EXPAND_COL_EXTENT);
        else
            Stats::startParseEvent(WE_STATS_WAIT_TO_CREATE_COL_EXTENT);
#endif
        boost::mutex::scoped_lock lk(*m_DbRootAddExtentMutexes[dbRoot]);
#ifdef PROFILE
        if (bExpandExtent)
            Stats::stopParseEvent(WE_STATS_WAIT_TO_EXPAND_COL_EXTENT);
        else
            Stats::stopParseEvent(WE_STATS_WAIT_TO_CREATE_COL_EXTENT);
        Stats::startParseEvent(WE_STATS_INIT_COL_EXTENT);
#endif

        // Allocate buffer, and store in scoped_array to insure it's deletion.
        // Create scope {...} to manage deletion of writeBuf.
        {
            unsigned char* writeBuf = new unsigned char[writeSize];
            boost::scoped_array<unsigned char> writeBufPtr( writeBuf );

            setEmptyBuf( writeBuf, writeSize, emptyVal, width );

#ifdef PROFILE
            Stats::stopParseEvent(WE_STATS_INIT_COL_EXTENT);
            if (bExpandExtent)
                Stats::startParseEvent(WE_STATS_EXPAND_COL_EXTENT);
            else
                Stats::startParseEvent(WE_STATS_CREATE_COL_EXTENT);
#endif

            //std::ostringstream oss;
            //oss << "initColExtent: width-" << width <<
            //"; loopCount-" << loopCount <<
            //"; writeSize-" << writeSize;
            //std::cout << oss.str() << std::endl;
            if (remWriteSize > 0)
            {
                if( pFile->write( writeBuf, remWriteSize ) != remWriteSize )
                {
                    return ERR_FILE_WRITE;
                }
            }
            for (int j=0; j<loopCount; j++)
            {
        	    if( pFile->write( writeBuf, writeSize ) != writeSize )
                {
                    return ERR_FILE_WRITE;
                }
            }
        }

        //@Bug 3219. update the compression header after the extent is expanded.
        if ((!bNewFile) && (m_compressionType) && (bExpandExtent))
        {
            updateColumnExtent(pFile, nBlocks);
        }

        // @bug 2378. Synchronize here to avoid write buffer pile up too much,
        // which could cause controllernode to timeout later when it needs to
        // save a snapshot.
        pFile->flush();

#ifdef PROFILE
        if (bExpandExtent)
            Stats::stopParseEvent(WE_STATS_EXPAND_COL_EXTENT);
        else
            Stats::stopParseEvent(WE_STATS_CREATE_COL_EXTENT);
#endif
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Write (initialize) an abbreviated compressed extent in a column file.
 *    nBlocks controls how many 8192-byte blocks are to be written out.
 * PARAMETERS:
 *    pFile   (in) - IDBDataFile* of column segment file to be written to
 *    dbRoot  (in) - DBRoot of pFile
 *    nBlocks (in) - number of blocks to be written for an extent
 *    emptyVal(in) - empty value to be used for column data values
 *    width   (in) - width of the applicable column
 * RETURN:
 *    returns ERR_FILE_WRITE or ERR_FILE_SEEK if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
int FileOp::initAbbrevCompColumnExtent(
    IDBDataFile* pFile,
    uint16_t dbRoot,
    int      nBlocks,
    uint64_t emptyVal,
    int      width) 
{
    // Reserve disk space for full abbreviated extent
    int rc = initColumnExtent( pFile,
        dbRoot,
        nBlocks,
        emptyVal,
        width,
        true,   // new file
        false,  // don't expand; add new extent
        true ); // add abbreviated extent
    if (rc != NO_ERROR)
    {
        return rc;
    }

#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_COMPRESS_COL_INIT_ABBREV_EXT);
#endif

    char hdrs[IDBCompressInterface::HDR_BUF_LEN*2];
    rc = writeInitialCompColumnChunk( pFile,
        nBlocks,
        INITIAL_EXTENT_ROWS_TO_DISK,
        emptyVal,
        width,
        hdrs );   
    if (rc != NO_ERROR)
    {
        return rc;
    }

#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_COMPRESS_COL_INIT_ABBREV_EXT);
#endif

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Write (initialize) the first extent in a compressed db file.
 * PARAMETERS:
 *    pFile    - IDBDataFile* of column segment file to be written to
 *    nBlocksAllocated - number of blocks allocated to the extent; should be
 *        enough blocks for a full extent, unless it's the abbreviated extent
 *    nRows    - number of rows to initialize, or write out to the file
 *    emptyVal - empty value to be used for column data values
 *    width    - width of the applicable column (in bytes)
 *    hdrs     - (in/out) chunk pointer headers
 * RETURN:
 *    returns NO_ERROR on success.
 ***********************************************************/
int FileOp::writeInitialCompColumnChunk(
    IDBDataFile* pFile,
    int      nBlocksAllocated,
    int      nRows,
    uint64_t emptyVal,
    int      width,
    char*    hdrs) 
{
    const int INPUT_BUFFER_SIZE     = nRows * width;
    char* toBeCompressedInput       = new char[INPUT_BUFFER_SIZE];
    unsigned int userPaddingBytes   = Config::getNumCompressedPadBlks() *
                                      BYTE_PER_BLOCK;
    const int OUTPUT_BUFFER_SIZE    = IDBCompressInterface::maxCompressedSize(INPUT_BUFFER_SIZE) +
        userPaddingBytes;
    unsigned char* compressedOutput = new unsigned char[OUTPUT_BUFFER_SIZE];
    unsigned int outputLen          = OUTPUT_BUFFER_SIZE;
    boost::scoped_array<char> toBeCompressedInputPtr( toBeCompressedInput );
    boost::scoped_array<unsigned char> compressedOutputPtr(compressedOutput);

    setEmptyBuf( (unsigned char*)toBeCompressedInput,
        INPUT_BUFFER_SIZE, emptyVal, width);

    // Compress an initialized abbreviated extent
    IDBCompressInterface compressor( userPaddingBytes );
    int rc = compressor.compressBlock(toBeCompressedInput,
        INPUT_BUFFER_SIZE, compressedOutput, outputLen );
    if (rc != 0)
    {
        return ERR_COMP_COMPRESS;
    }

    // Round up the compressed chunk size
    rc = compressor.padCompressedChunks( compressedOutput,
        outputLen, OUTPUT_BUFFER_SIZE );
    if (rc != 0)
    {
        return ERR_COMP_PAD_DATA;
    }

//  std::cout << "Uncompressed rowCount: " << nRows <<
//      "; colWidth: "      << width   <<
//      "; uncompByteCnt: " << INPUT_BUFFER_SIZE <<
//      "; blkAllocCnt: "   << nBlocksAllocated  <<
//      "; compressedByteCnt: "  << outputLen << std::endl;

    compressor.initHdr(hdrs, m_compressionType);
    compressor.setBlockCount(hdrs, nBlocksAllocated);

    // Store compression pointers in the header
    std::vector<uint64_t> ptrs;
    ptrs.push_back( IDBCompressInterface::HDR_BUF_LEN*2 );
    ptrs.push_back( outputLen + (IDBCompressInterface::HDR_BUF_LEN*2) );
    compressor.storePtrs(ptrs, hdrs);

    RETURN_ON_ERROR( writeHeaders(pFile, hdrs) );

    // Write the compressed data
    if( pFile->write( compressedOutput, outputLen ) != outputLen )
    {
        return ERR_FILE_WRITE;
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Fill specified compressed extent with empty value chunks.
 * PARAMETERS:
 *    oid        - OID for relevant column
 *    colWidth   - width in bytes of this column
 *    emptyVal   - empty value to be used in filling empty chunks
 *    dbRoot     - DBRoot of extent to be filled
 *    partition  - partition of extent to be filled
 *    segment    - segment file number of extent to be filled
 *    hwm        - proposed new HWM of filled in extent
 *    segFile    - (out) name of updated segment file
 *    failedTask - (out) if error occurs, this is the task that failed
 * RETURN:
 *    returns NO_ERROR if success.
 ***********************************************************/
int FileOp::fillCompColumnExtentEmptyChunks(OID oid,
    int          colWidth,
    uint64_t     emptyVal,
    uint16_t     dbRoot,   
    uint32_t     partition,
    uint16_t     segment,
    HWM          hwm,
    std::string& segFile,
    std::string& failedTask)
{
    int rc = NO_ERROR;
    segFile.clear();
    failedTask.clear();

    // Open the file and read the headers with the compression chunk pointers
    // @bug 5572 - HDFS usage: incorporate *.tmp file backup flag
    IDBDataFile* pFile = openFile( oid, dbRoot, partition, segment, segFile,
        "r+b", DEFAULT_COLSIZ, true );
    if (!pFile)
    {
        failedTask = "Opening file";
        ostringstream oss;
        oss << "oid: " << oid << " with path " << segFile;
        logging::Message::Args args;
        args.add("Error opening file ");
        args.add(oss.str());
        args.add("");
        args.add("");
        SimpleSysLog::instance()->logMsg(args,
            logging::LOG_TYPE_ERROR,
            logging::M0001);
        return ERR_FILE_OPEN;
    }

    char hdrs[ IDBCompressInterface::HDR_BUF_LEN * 2 ];
    rc = readHeaders( pFile, hdrs );
    if (rc != NO_ERROR)
    {
        failedTask = "Reading headers";
        closeFile ( pFile );
        return rc;
    }

    int userPadBytes = Config::getNumCompressedPadBlks() * BYTE_PER_BLOCK;
    IDBCompressInterface compressor( userPadBytes );
    CompChunkPtrList chunkPtrs;
    int rcComp = compressor.getPtrList( hdrs, chunkPtrs );
    if (rcComp != 0)
    {
        failedTask = "Getting header ptrs";
        closeFile ( pFile );
        return ERR_COMP_PARSE_HDRS;
    }

    // Nothing to do if the proposed HWM is < the current block count
    uint64_t blkCount = compressor.getBlockCount(hdrs);
    if (blkCount > (hwm + 1))
    {
        closeFile ( pFile );
        return NO_ERROR;
    }

    const unsigned int ROWS_PER_EXTENT   =
        BRMWrapper::getInstance()->getInstance()->getExtentRows(); 
    const unsigned int ROWS_PER_CHUNK    =
        IDBCompressInterface::UNCOMPRESSED_INBUF_LEN / colWidth;
    const unsigned int CHUNKS_PER_EXTENT = ROWS_PER_EXTENT / ROWS_PER_CHUNK;

    // If this is an abbreviated extent, we first expand to a full extent
    // @bug 4340 - support moving the DBRoot with a single abbrev extent
    if ( (chunkPtrs.size() == 1) &&
        ((blkCount * BYTE_PER_BLOCK) ==
         (uint64_t)(INITIAL_EXTENT_ROWS_TO_DISK * colWidth)) )
    {
        if ( getLogger() )
        {
            std::ostringstream oss;
            oss << "Converting abbreviated partial extent to full extent for" <<
                   ": OID-"    << oid       <<
                   "; DBRoot-" << dbRoot    <<
                   "; part-"   << partition <<
                   "; seg-"    << segment   <<
                   "; file-"   << segFile   <<
                   "; wid-"    << colWidth  <<
                   "; oldBlkCnt-" << blkCount <<
                   "; newBlkCnt-" << 
                   ((ROWS_PER_EXTENT * colWidth) / BYTE_PER_BLOCK);
            getLogger()->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        off64_t   endHdrsOffset = pFile->tell();
        rc = expandAbbrevColumnExtent( pFile, dbRoot, emptyVal, colWidth );
        if (rc != NO_ERROR)
        {
            failedTask = "Expanding abbreviated extent";
            closeFile ( pFile );
            return rc;
        }

        CompChunkPtr chunkOutPtr;
        rc = expandAbbrevColumnChunk( pFile, emptyVal, colWidth,
            chunkPtrs[0], chunkOutPtr );
        if (rc != NO_ERROR)
        {
            failedTask = "Expanding abbreviated chunk";
            closeFile ( pFile );
            return rc;
        }
        chunkPtrs[0] = chunkOutPtr; // update chunkPtrs with new chunk size

        rc = setFileOffset( pFile, endHdrsOffset );
        if (rc != NO_ERROR)
        {
            failedTask = "Positioning file to end of headers";
            closeFile ( pFile );
            return rc;
        }

        // Update block count to reflect a full extent
        blkCount = (ROWS_PER_EXTENT * colWidth) / BYTE_PER_BLOCK;
        compressor.setBlockCount( hdrs, blkCount );
    }

    // Calculate the number of empty chunks we need to add to fill this extent
    unsigned numChunksToFill = 0;
    ldiv_t ldivResult = ldiv(chunkPtrs.size(), CHUNKS_PER_EXTENT);
    if (ldivResult.rem != 0)
    {
        numChunksToFill = CHUNKS_PER_EXTENT - ldivResult.rem;
    }

#if 0
    std::cout << "Number of allocated blocks:     " <<
        compressor.getBlockCount(hdrs) << std::endl;
    std::cout << "Pointer Header Size (in bytes): " <<
        (compressor.getHdrSize(hdrs) -
        IDBCompressInterface::HDR_BUF_LEN) << std::endl;
    std::cout << "Chunk Pointers (offset,length): " << std::endl;
    for (unsigned k=0; k<chunkPtrs.size(); k++)
    {
        std::cout << "  " << k << ". " << chunkPtrs[k].first <<
            " , " << chunkPtrs[k].second << std::endl;
    }
    std::cout << std::endl;
    std::cout << "Number of chunks to fill in: " << numChunksToFill <<
        std::endl << std::endl;
#endif

    off64_t   endOffset = 0;

    // Fill in or add necessary remaining empty chunks
    if (numChunksToFill > 0)
    {
        const int IN_BUF_LEN = IDBCompressInterface::UNCOMPRESSED_INBUF_LEN;
        const int OUT_BUF_LEN= IDBCompressInterface::maxCompressedSize(IN_BUF_LEN) + userPadBytes;
    
        // Allocate buffer, and store in scoped_array to insure it's deletion.
        // Create scope {...} to manage deletion of buffers
        {
            char*          toBeCompressedBuf = new char         [ IN_BUF_LEN  ];
            unsigned char* compressedBuf     = new unsigned char[ OUT_BUF_LEN ];
            boost::scoped_array<char> toBeCompressedInputPtr(toBeCompressedBuf);
            boost::scoped_array<unsigned char>
                                      compressedOutputPtr(compressedBuf);

            // Compress and then pad the compressed chunk
            setEmptyBuf( (unsigned char*)toBeCompressedBuf,
                IN_BUF_LEN, emptyVal, colWidth );
            unsigned int outputLen = OUT_BUF_LEN;
            rcComp = compressor.compressBlock( toBeCompressedBuf,
                IN_BUF_LEN, compressedBuf, outputLen );
            if (rcComp != 0)
            {
                failedTask = "Compressing chunk";
                closeFile ( pFile );
                return ERR_COMP_COMPRESS;
            }
            toBeCompressedInputPtr.reset(); // release memory

            rcComp = compressor.padCompressedChunks( compressedBuf,
                outputLen, OUT_BUF_LEN );
            if (rcComp != 0)
            {
                failedTask = "Padding compressed chunk";
                closeFile ( pFile );
                return ERR_COMP_PAD_DATA;
            }

            // Position file to write empty chunks; default to end of headers
            // in case there are no chunks listed in the header
            off64_t   startOffset = pFile->tell();
            if (chunkPtrs.size() > 0)
            {
                startOffset = chunkPtrs[chunkPtrs.size()-1].first +
                              chunkPtrs[chunkPtrs.size()-1].second;
                rc = setFileOffset( pFile, startOffset );
                if (rc != NO_ERROR)
                {
                    failedTask = "Positioning file to begin filling chunks";
                    closeFile ( pFile );
                    return rc;
                }
            }

            // Write chunks needed to fill out the current extent, add chunk ptr
            for (unsigned k=0; k<numChunksToFill; k++)
            {
                rc = writeFile( pFile,
                                (unsigned char*)compressedBuf,
                                outputLen );
                if (rc != NO_ERROR)
                {
                    failedTask = "Writing  a chunk";
                    closeFile ( pFile );
                    return rc;
                }
                CompChunkPtr compChunk( startOffset, outputLen );
                chunkPtrs.push_back( compChunk ); 
                startOffset = pFile->tell();
            }
        } // end of scope for boost scoped array pointers

        endOffset = pFile->tell();

        // Update the compressed chunk pointers in the header
        std::vector<uint64_t> ptrs;
        for (unsigned i=0; i<chunkPtrs.size(); i++)
        {
            ptrs.push_back( chunkPtrs[i].first );
        }
        ptrs.push_back( chunkPtrs[chunkPtrs.size()-1].first +
                        chunkPtrs[chunkPtrs.size()-1].second );
        compressor.storePtrs( ptrs, hdrs );
    
        rc = writeHeaders( pFile, hdrs );
        if (rc != NO_ERROR)
        {
            failedTask = "Writing headers";
            closeFile ( pFile );
            return rc;
        }
    }  // end of "numChunksToFill > 0"
    else
    {   // if no chunks to add, then set endOffset to truncate the db file
        // strictly based on the chunks that are already in the file
        if (chunkPtrs.size() > 0)
        {
            endOffset = chunkPtrs[chunkPtrs.size()-1].first +
                        chunkPtrs[chunkPtrs.size()-1].second;
        }
    }

    // Truncate the file to release unused space for the extent we just filled
    if (endOffset > 0)
    {
        rc = truncateFile(pFile, endOffset);
        if (rc != NO_ERROR)
        {
            failedTask = "Truncating file";
            closeFile ( pFile );
            return rc;
        }
    }

    closeFile ( pFile );

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Expand first chunk in pFile from an abbreviated chunk for an abbreviated
 *    extent to a full compressed chunk for a full extent.
 * PARAMETERS:
 *    pFile      - file to be updated
 *    colWidth   - width in bytes of this column
 *    emptyVal   - empty value to be used in filling empty chunks
 *    chunkInPtr - chunk pointer referencing first (abbrev) chunk
 *    chunkOutPtr- (out) updated chunk ptr referencing first (full) chunk
 * RETURN:
 *    returns NO_ERROR if success.
 ***********************************************************/
int FileOp::expandAbbrevColumnChunk(
    IDBDataFile* pFile,
    uint64_t emptyVal,
    int   colWidth,
    const CompChunkPtr& chunkInPtr,
    CompChunkPtr& chunkOutPtr )
{
    int userPadBytes = Config::getNumCompressedPadBlks() * BYTE_PER_BLOCK;
    const int IN_BUF_LEN = IDBCompressInterface::UNCOMPRESSED_INBUF_LEN;
    const int OUT_BUF_LEN= IDBCompressInterface::maxCompressedSize(IN_BUF_LEN) + userPadBytes;

    char* toBeCompressedBuf = new char[ IN_BUF_LEN  ];
    boost::scoped_array<char> toBeCompressedPtr(toBeCompressedBuf);

    setEmptyBuf( (unsigned char*)toBeCompressedBuf,
        IN_BUF_LEN, emptyVal, colWidth );

    RETURN_ON_ERROR( setFileOffset(pFile, chunkInPtr.first, SEEK_SET) );

    char* compressedInBuf = new char[ chunkInPtr.second ];
    boost::scoped_array<char> compressedInBufPtr(compressedInBuf);
    RETURN_ON_ERROR( readFile(pFile, (unsigned char*)compressedInBuf,
        chunkInPtr.second) );

    // Uncompress an "abbreviated" chunk into our 4MB buffer
    unsigned int outputLen = IN_BUF_LEN;
    IDBCompressInterface compressor( userPadBytes );
    int rc = compressor.uncompressBlock(
        compressedInBuf,
        chunkInPtr.second,
        (unsigned char*)toBeCompressedBuf,
        outputLen);
    if (rc != 0)
    {
        return ERR_COMP_UNCOMPRESS;
    }
    compressedInBufPtr.reset(); // release memory

    RETURN_ON_ERROR( setFileOffset(pFile, chunkInPtr.first, SEEK_SET) );

    unsigned char* compressedOutBuf = new unsigned char[ OUT_BUF_LEN ];
    boost::scoped_array<unsigned char> compressedOutBufPtr(compressedOutBuf);

    // Compress the data we just read, as a "full" 4MB chunk
    outputLen = OUT_BUF_LEN;
    rc = compressor.compressBlock(
        reinterpret_cast<char*>(toBeCompressedBuf),
        IN_BUF_LEN,
        compressedOutBuf,
        outputLen );
    if (rc != 0)
    {
        return ERR_COMP_COMPRESS;
    }

    // Round up the compressed chunk size
    rc = compressor.padCompressedChunks( compressedOutBuf,
        outputLen, OUT_BUF_LEN );
    if (rc != 0)
    {
        return ERR_COMP_PAD_DATA;
    }
    
    RETURN_ON_ERROR( writeFile(pFile, compressedOutBuf, outputLen) );

    chunkOutPtr.first  = chunkInPtr.first;
    chunkOutPtr.second = outputLen;

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Write headers to a compressed column file.
 * PARAMETERS:
 *    pFile   (in) - IDBDataFile* of column segment file to be written to
 *    hdr     (in) - header pointers to be written
 * RETURN:
 *    returns ERR_FILE_WRITE or ERR_FILE_SEEK if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
int FileOp::writeHeaders(IDBDataFile* pFile, const char* hdr) const
{
    RETURN_ON_ERROR( setFileOffset(pFile, 0, SEEK_SET) );

    // Write the headers
    if (pFile->write( hdr, IDBCompressInterface::HDR_BUF_LEN*2 ) != IDBCompressInterface::HDR_BUF_LEN*2)
    {
        return ERR_FILE_WRITE;
    }

   return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Write headers to a compressed column or dictionary file.
 * PARAMETERS:
 *    pFile   (in) - IDBDataFile* of column segment file to be written to
 *    controlHdr (in) - control header to be written
 *    pointerHdr (in) - pointer header to be written
 *    ptrHdrSize (in) - size (in bytes) of pointer header
 * RETURN:
 *    returns ERR_FILE_WRITE or ERR_FILE_SEEK if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
int FileOp::writeHeaders(IDBDataFile* pFile, const char* controlHdr,
    const char* pointerHdr, uint64_t ptrHdrSize) const
{
    RETURN_ON_ERROR( setFileOffset(pFile, 0, SEEK_SET) );

    // Write the control header
    if (pFile->write( controlHdr, IDBCompressInterface::HDR_BUF_LEN ) != IDBCompressInterface::HDR_BUF_LEN)
    {
        return ERR_FILE_WRITE;
    }

    // Write the pointer header
    if (pFile->write( pointerHdr, ptrHdrSize ) !=  (ssize_t) ptrHdrSize)
    {
        return ERR_FILE_WRITE;
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Write out (initialize) an extent in a dictionary store file.
 *    A mutex is used for each DBRoot, to prevent contention between
 *    threads, because if multiple threads are creating extents on
 *    the same DBRoot at the same time, the extents can become
 *    fragmented.  It is best to only create one extent at a time
 *    on each DBRoot.
 *    This function can be used to initialize an entirely new extent, or
 *    to finish initializing an extent that has already been started.
 *    nBlocks controls how many 8192-byte blocks are to be written out.
 * PARAMETERS:
 *    pFile   (in) - IDBDataFile* of column segment file to be written to
 *    dbRoot  (in) - DBRoot of pFile
 *    nBlocks (in) - number of blocks to be written for an extent
 *    blockHdrInit(in) - data used to initialize each block
 *    blockHdrInitSize(in) - number of bytes in blockHdrInit
 *    bExpandExtent (in) - Expand existing extent, or initialize a new one
 * RETURN:
 *    returns ERR_FILE_WRITE if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
int FileOp::initDctnryExtent(
    IDBDataFile*   pFile,
    uint16_t       dbRoot,
    int            nBlocks,
    unsigned char* blockHdrInit,
    int            blockHdrInitSize,
    bool           bExpandExtent )
{
    // @bug5769 Don't initialize extents or truncate db files on HDFS
    if (idbdatafile::IDBPolicy::useHdfs())
    {
        if (m_compressionType)
            updateDctnryExtent(pFile, nBlocks);

        // Synchronize to avoid write buffer pile up too much, which could cause
        // controllernode to timeout later when it needs to save a snapshot.
        pFile->flush();
    }
    else
    {
        // Create vector of mutexes used to serialize extent access per DBRoot
        initDbRootExtentMutexes( );

        // Determine the number of blocks in each call to fwrite(), and the
        // number of fwrite() calls to make, based on this.  In other words,
        // we put a cap on the "writeSize" so that we don't allocate and write
        // an entire extent at once for the 64M row extents.  If we are
        // expanding an abbreviated 64M extent, we may not have an even
        // multiple of MAX_NBLOCKS to write; remWriteSize is the number of
        // blocks above and beyond loopCount*MAX_NBLOCKS.
        int writeSize = nBlocks * BYTE_PER_BLOCK; // 1M and 8M row extent size
        int loopCount = 1;
        int remWriteSize = 0;
        if (nBlocks > MAX_NBLOCKS)                // 64M row extent size
        {
            writeSize = MAX_NBLOCKS * BYTE_PER_BLOCK;
            loopCount = nBlocks / MAX_NBLOCKS;
            remWriteSize = nBlocks - (loopCount * MAX_NBLOCKS);
        }

        // Allocate a buffer, initialize it, and use it to create the extent
        idbassert(dbRoot > 0);
#ifdef PROFILE
        if (bExpandExtent)
            Stats::startParseEvent(WE_STATS_WAIT_TO_EXPAND_DCT_EXTENT);
        else
            Stats::startParseEvent(WE_STATS_WAIT_TO_CREATE_DCT_EXTENT);
#endif
        boost::mutex::scoped_lock lk(*m_DbRootAddExtentMutexes[dbRoot]);
#ifdef PROFILE
        if (bExpandExtent)
            Stats::stopParseEvent(WE_STATS_WAIT_TO_EXPAND_DCT_EXTENT);
        else
            Stats::stopParseEvent(WE_STATS_WAIT_TO_CREATE_DCT_EXTENT);
        Stats::startParseEvent(WE_STATS_INIT_DCT_EXTENT);
#endif

        // Allocate buffer, and store in scoped_array to insure it's deletion.
        // Create scope {...} to manage deletion of writeBuf.
        {
            unsigned char* writeBuf = new unsigned char[writeSize];
            boost::scoped_array<unsigned char> writeBufPtr( writeBuf );

            memset(writeBuf, 0, writeSize);
            for (int i=0; i<nBlocks; i++)
            {
                memcpy( writeBuf+(i*BYTE_PER_BLOCK),
                        blockHdrInit,
                        blockHdrInitSize );
            }

#ifdef PROFILE
            Stats::stopParseEvent(WE_STATS_INIT_DCT_EXTENT);
            if (bExpandExtent)
                Stats::startParseEvent(WE_STATS_EXPAND_DCT_EXTENT);
            else
                Stats::startParseEvent(WE_STATS_CREATE_DCT_EXTENT);
#endif

            //std::ostringstream oss;
            //oss << "initDctnryExtent: width-8(assumed)" <<
            //"; loopCount-" << loopCount <<
            //"; writeSize-" << writeSize;
            //std::cout << oss.str() << std::endl;
            if (remWriteSize > 0)
            {
        	    if (pFile->write( writeBuf, remWriteSize ) != remWriteSize)
                {
                    return ERR_FILE_WRITE;
                }
            }
            for (int j=0; j<loopCount; j++)
            {
        	    if (pFile->write( writeBuf, writeSize ) != writeSize)
                {
                    return ERR_FILE_WRITE;
                }
            }
        }

        if (m_compressionType)
            updateDctnryExtent(pFile, nBlocks);

        // Synchronize to avoid write buffer pile up too much, which could cause
        // controllernode to timeout later when it needs to save a snapshot.
        pFile->flush();
#ifdef PROFILE
        if (bExpandExtent)
            Stats::stopParseEvent(WE_STATS_EXPAND_DCT_EXTENT);
        else
            Stats::stopParseEvent(WE_STATS_CREATE_DCT_EXTENT);
#endif
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Create a vector containing the mutexes used to serialize
 *    extent creation per DBRoot.  Serializing extent creation
 *    helps to prevent disk fragmentation.
 ***********************************************************/
/* static */
void FileOp::initDbRootExtentMutexes( )
{
    boost::mutex::scoped_lock lk(m_createDbRootMutexes);
    if ( m_DbRootAddExtentMutexes.size() == 0 )
    {
        std::vector<u_int16_t> rootIds;
        Config::getRootIdList( rootIds );

        for (size_t i=0; i<rootIds.size(); i++)
        {
            boost::mutex* pM = new boost::mutex;
            m_DbRootAddExtentMutexes[ rootIds[i] ] = pM;
        }
    }
}

/***********************************************************
 * DESCRIPTION:
 *    Cleans up memory allocated to the DBRoot extent mutexes.  Calling
 *    this function is not necessary, but it is provided for completeness,
 *    to complement initDbRootExtentMutexes(), and to provide a way to
 *    free up memory at the end of program execution.
 ***********************************************************/
/* static */
void FileOp::removeDbRootExtentMutexes( )
{
    boost::mutex::scoped_lock lk(m_createDbRootMutexes);

    std::map<int,boost::mutex*>::iterator k = m_DbRootAddExtentMutexes.begin();
    while (k != m_DbRootAddExtentMutexes.end() )
    {
        delete k->second;
        ++k;
    }
}

/***********************************************************
 * DESCRIPTION:
 *    Write out (reinitialize) a partial extent in a column file.
 *    A mutex is not used to prevent contention between threads,
 *    because the extent should already be in place on disk; so
 *    disk fragmentation is not an issue.
 * PARAMETERS:
 *    pFile   (in) - IDBDataFile* of column segment file to be written to
 *    startOffset(in)-file offset where we are to begin writing blocks
 *    nBlocks (in) - number of blocks to be written to the extent
 *    emptyVal(in) - empty value to be used for column data values
 *    width   (in) - width of the applicable column
 * RETURN:
 *    returns ERR_FILE_WRITE if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
int FileOp::reInitPartialColumnExtent(
    IDBDataFile* pFile,
    long long startOffset,
    int      nBlocks,
    uint64_t emptyVal,
    int      width )
{
    int rc = setFileOffset( pFile, startOffset, SEEK_SET );
    if (rc != NO_ERROR)
        return rc;

    if (nBlocks == 0)
        return NO_ERROR;

    // Determine the number of blocks in each call to fwrite(), and the
    // number of fwrite() calls to make, based on this.  In other words,
    // we put a cap on the "writeSize" so that we don't allocate and write
    // an entire extent at once for the 64M row extents.
    int writeSize = nBlocks * BYTE_PER_BLOCK; // 1M and 8M row extent size
    int loopCount = 0;
    int remainderSize = writeSize;
    if (nBlocks > MAX_NBLOCKS)                // 64M row extent size
    {
        writeSize = MAX_NBLOCKS * BYTE_PER_BLOCK;
        loopCount = nBlocks / MAX_NBLOCKS;
        remainderSize = nBlocks - (loopCount * MAX_NBLOCKS);
    }

    // Allocate a buffer, initialize it, and use it to initialize the extent
    // Store in scoped_array to insure it's deletion.
    // Create scope {...} to manage deletion of writeBuf.
    {
        unsigned char* writeBuf = new unsigned char[writeSize];
        boost::scoped_array<unsigned char> writeBufPtr( writeBuf );

        setEmptyBuf( writeBuf, writeSize, emptyVal, width );

        for (int j=0; j<loopCount; j++)
        {
        	if (pFile->write( writeBuf, writeSize ) != writeSize)
            {
                return ERR_FILE_WRITE;
            }
        }

        if (remainderSize > 0)
        {
        	if (pFile->write( writeBuf, remainderSize ) != remainderSize)
            {
                return ERR_FILE_WRITE;
            }
        }
    }

    // Synchronize here to avoid write buffer pile up too much, which could
    // cause controllernode to timeout later when it needs to save a snapshot.
    pFile->flush();
    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Write out (reinitialize) a partial extent in a dictionary store file.
 *    A mutex is not used to prevent contention between threads,
 *    because the extent should already be in place on disk; so
 *    disk fragmentation is not an issue.
 * PARAMETERS:
 *    pFile   (in) - IDBDataFile* of column segment file to be written to
 *    startOffset(in)-file offset where we are to begin writing blocks
 *    nBlocks (in) - number of blocks to be written to the extent
 *    blockHdrInit(in) - data used to initialize each block
 *    blockHdrInitSize(in) - number of bytes in blockHdrInit
 * RETURN:
 *    returns ERR_FILE_WRITE if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
int FileOp::reInitPartialDctnryExtent(
    IDBDataFile*   pFile,
    long long      startOffset,
    int            nBlocks,
    unsigned char* blockHdrInit,
    int            blockHdrInitSize )
{
    int rc = setFileOffset( pFile, startOffset, SEEK_SET );
    if (rc != NO_ERROR)
        return rc;

    if (nBlocks == 0)
        return NO_ERROR;

    // Determine the number of blocks in each call to fwrite(), and the
    // number of fwrite() calls to make, based on this.  In other words,
    // we put a cap on the "writeSize" so that we don't allocate and write
    // an entire extent at once for the 64M row extents.
    int writeSize = nBlocks * BYTE_PER_BLOCK; // 1M and 8M row extent size
    int loopCount = 0;
    int remainderSize = writeSize;
    if (nBlocks > MAX_NBLOCKS)                // 64M row extent size
    {
        writeSize = MAX_NBLOCKS * BYTE_PER_BLOCK;
        loopCount = nBlocks / MAX_NBLOCKS;
        remainderSize = nBlocks - (loopCount * MAX_NBLOCKS);
        nBlocks   = MAX_NBLOCKS;
    }

    // Allocate a buffer, initialize it, and use it to initialize the extent
    // Store in scoped_array to insure it's deletion.
    // Create scope {...} to manage deletion of writeBuf.
    {
        unsigned char* writeBuf = new unsigned char[writeSize];
        boost::scoped_array<unsigned char> writeBufPtr( writeBuf );

        memset(writeBuf, 0, writeSize);
        for (int i=0; i<nBlocks; i++)
        {
            memcpy( writeBuf+(i*BYTE_PER_BLOCK),
                    blockHdrInit,
                    blockHdrInitSize );
        }

        for (int j=0; j<loopCount; j++)
        {
            if (pFile->write( writeBuf, writeSize ) != writeSize)
            {
                return ERR_FILE_WRITE;
            }
        }

        if (remainderSize > 0)
        {
            if (pFile->write( writeBuf, remainderSize ) != remainderSize)
            {
                return ERR_FILE_WRITE;
            }
        }
    }

    // Synchronize here to avoid write buffer pile up too much, which could
    // cause controllernode to timeout later when it needs to save a snapshot.
    pFile->flush();
    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 * PARAMETERS:
 *    pFile - file handle
 *    fileSize (out) - file size in bytes
 * RETURN:
 *    error code
 ***********************************************************/
int FileOp::getFileSize( IDBDataFile* pFile, long long& fileSize ) const
{
    fileSize = 0;
    if ( pFile == NULL )
        return ERR_FILE_NULL;

    fileSize = pFile->size();
    if( fileSize < 0 )
    {
    	fileSize = 0;
    	return ERR_FILE_STAT;
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Get file size using file id
 * PARAMETERS:
 *    fid    - column OID
 *    dbroot    - DBRoot    of applicable segment file
 *    partition - partition of applicable segment file
 *    segment   - segment   of applicable segment file
 *    fileSize (out) - current file size for requested segment file
 * RETURN:
 *    NO_ERROR if okay, else an error return code.
 ***********************************************************/
int FileOp::getFileSize( FID fid, u_int16_t dbRoot,
    u_int32_t partition, u_int16_t segment,
    long long& fileSize ) const
{
    fileSize = 0;

    char fileName[FILE_NAME_SIZE];
    RETURN_ON_ERROR( getFileName(fid, fileName,
        dbRoot, partition, segment) );

    fileSize = IDBPolicy::size( fileName );

    if( fileSize < 0 )
    {
    	fileSize = 0;
    	return ERR_FILE_STAT;
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Check whether it is a directory
 * PARAMETERS:
 *    dirName - directory name
 * RETURN:
 *    true if it is, false otherwise
 ***********************************************************/
bool  FileOp::isDir( const char* dirName ) const
{
	return IDBPolicy::isDir( dirName );
}

/***********************************************************
 * DESCRIPTION:
 *    Convert an oid to a filename
 * PARAMETERS:
 *    fid - fid
 *    fullFileName - file name
 *    bCreateDir - whether need to create a directory
 *    dbRoot     - DBRoot where file is to be located; 1->DBRoot1,
 *                 2->DBRoot2, etc.  If bCreateDir is false, meaning we
 *                 are not creating the file but only searching for an
 *                 existing file, then dbRoot can be 0, and oid2FileName
 *                 will search all the DBRoots for the applicable filename.
 *    partition  - Partition number to be used in filepath subdirectory
 *    segment    - Segment number to be used in filename
 * RETURN:
 *    NO_ERROR if success, other if fail
 ***********************************************************/
int FileOp::oid2FileName( FID fid,
    char* fullFileName,
    bool bCreateDir,
    uint16_t dbRoot,
    uint32_t partition,
    uint16_t segment) const
{
#ifdef SHARED_NOTHING_DEMO_2
    if (fid >= 10000) {
        char root[FILE_NAME_SIZE];
        Config::getSharedNothingRoot(root);
        sprintf(fullFileName, "%s/FILE%d", root, fid);
        return NO_ERROR;
    }
#endif

    /* If is a version buffer file, the format is different. */
    if (fid < 1000) {
        /* Get the dbroot #
         * Get the root of that dbroot
         * Add "/versionbuffer.cdf"
         */
        BRM::DBRM dbrm;
        int _dbroot = dbrm.getDBRootOfVBOID(fid);
        if (_dbroot < 0)
            return ERR_INVALID_VBOID;
        snprintf(fullFileName, FILE_NAME_SIZE,
            "%s/versionbuffer.cdf", Config::getDBRootByNum(_dbroot).c_str());
        return NO_ERROR;
    }

//Get hashed part of the filename. This is the tail-end of the filename path,
//  excluding the DBRoot.
    char tempFileName[FILE_NAME_SIZE];
    char dbDir[MAX_DB_DIR_LEVEL][MAX_DB_DIR_NAME_SIZE];
    RETURN_ON_ERROR((Convertor::oid2FileName(
        fid, tempFileName, dbDir, partition, segment)));

    // see if file exists in specified DBRoot; return if found
    if (dbRoot > 0) {
        sprintf(fullFileName, "%s/%s", Config::getDBRootByNum(dbRoot).c_str(),
            tempFileName);

        //std::cout << "oid2FileName() OID: " << fid <<
        //   " searching for file: " << fullFileName <<std::endl;
       // if (access(fullFileName, R_OK) == 0) return NO_ERROR;
	   //@Bug 5397
		if (IDBPolicy::exists( fullFileName ))
			return NO_ERROR;
        //file wasn't found, user doesn't want dirs to be created, we're done
        if (!bCreateDir)
            return NO_ERROR;

        //std::cout << "oid2FileName() OID: " << fid <<
        //   " creating file: " << fullFileName <<std::endl;
    }
    else {
        //Now try to find the file in each of the DBRoots.
        std::vector<std::string> dbRootPathList;
        Config::getDBRootPathList( dbRootPathList );
        for (unsigned i = 0; i < dbRootPathList.size(); i++)
        {
            sprintf(fullFileName, "%s/%s", dbRootPathList[i].c_str(),
                tempFileName);
            //found it, nothing more to do, return
            //if (access(fullFileName, R_OK) == 0) return NO_ERROR;
			 //@Bug 5397
			if (IDBPolicy::exists( fullFileName ))
				return NO_ERROR;
        }

        //file wasn't found, user didn't specify DBRoot so we can't create
        return ERR_FILE_NOT_EXIST;
    }

    /*
    char dirName[FILE_NAME_SIZE];

    sprintf( dirName, "%s/%s", Config::getDBRootByNum(dbRoot).c_str(),
        dbDir[0] );
    if( !isDir( dirName ) )
        RETURN_ON_ERROR( createDir( dirName ));

    sprintf( dirName, "%s/%s", dirName, dbDir[1] );
    if( !isDir( dirName ) )
        RETURN_ON_ERROR( createDir( dirName ));

    sprintf( dirName, "%s/%s", dirName, dbDir[2] );
    if( !isDir( dirName ) )
        RETURN_ON_ERROR( createDir( dirName ));

    sprintf( dirName, "%s/%s", dirName, dbDir[3] );
    if( !isDir( dirName ) )
        RETURN_ON_ERROR( createDir( dirName ));

    sprintf( dirName, "%s/%s", dirName, dbDir[4] );
    if( !isDir( dirName ) )
        RETURN_ON_ERROR( createDir( dirName ));
    */

    std::stringstream aDirName;

    aDirName << Config::getDBRootByNum(dbRoot).c_str()<<"/" << dbDir[0];
    if(!isDir((aDirName.str()).c_str()))
        RETURN_ON_ERROR( createDir((aDirName.str()).c_str()) );

    aDirName << "/" << dbDir[1];
    if(!isDir(aDirName.str().c_str()))
        RETURN_ON_ERROR( createDir(aDirName.str().c_str()) );

    aDirName << "/" << dbDir[2];
    if(!isDir(aDirName.str().c_str()))
        RETURN_ON_ERROR( createDir(aDirName.str().c_str()) );

    aDirName << "/" << dbDir[3];
    if(!isDir(aDirName.str().c_str()))
        RETURN_ON_ERROR( createDir(aDirName.str().c_str()) );

    aDirName << "/" << dbDir[4];
    if(!isDir(aDirName.str().c_str()))
        RETURN_ON_ERROR( createDir(aDirName.str().c_str()) );

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Search for directory path associated with specified OID.
 *    If the OID is a version buffer file, it returns the whole
 *    filename.
 * PARAMETERS:
 *    fid   - (in)  OID to search for
 *    pFile - (out) OID directory path (including DBRoot) that is found
 * RETURN:
 *    NO_ERROR if OID dir path found, else returns ERR_FILE_NOT_EXIST
 ***********************************************************/
int FileOp::oid2DirName( FID fid, char* oidDirName ) const
{
    char tempFileName[FILE_NAME_SIZE];
    char dbDir[MAX_DB_DIR_LEVEL][MAX_DB_DIR_NAME_SIZE];

    /* If is a version buffer file, the format is different. */
    if (fid < 1000) {
        /* Get the dbroot #
         * Get the root of that dbroot
         */
        BRM::DBRM dbrm;
        int _dbroot = dbrm.getDBRootOfVBOID(fid);
        if (_dbroot < 0)
            return ERR_INVALID_VBOID;
        snprintf(oidDirName, FILE_NAME_SIZE, "%s",
            Config::getDBRootByNum(_dbroot).c_str());
        return NO_ERROR;
    }

    RETURN_ON_ERROR((Convertor::oid2FileName(
        fid, tempFileName, dbDir, 0, 0)));

    //Now try to find the directory in each of the DBRoots.
    std::vector<std::string> dbRootPathList;
    Config::getDBRootPathList( dbRootPathList );
    for (unsigned i = 0; i < dbRootPathList.size(); i++)
    {
        sprintf(oidDirName, "%s/%s/%s/%s/%s",
            dbRootPathList[i].c_str(),
            dbDir[0], dbDir[1], dbDir[2], dbDir[3]);

        //found it, nothing more to do, return
		//@Bug 5397. use the new way to check
		if (IDBPolicy::exists( oidDirName ))
			return NO_ERROR;
    }

    return ERR_FILE_NOT_EXIST;
}

/***********************************************************
 * DESCRIPTION:
 *    Construct directory path for the specified fid (OID), DBRoot, and
 *    partition number.  Directory path need not exist, nor is it created.
 * PARAMETERS:
 *    fid       - (in)  OID of interest
 *    dbRoot    - (in)  DBRoot of interest
 *    partition - (in)  partition of interest
 *    dirName   - (out) constructed directory path
 * RETURN:
 *    NO_ERROR if path is successfully constructed.
 ***********************************************************/
int FileOp::getDirName( FID fid, uint16_t dbRoot,
    uint32_t partition,
    std::string& dirName) const
{
    char tempFileName[FILE_NAME_SIZE];
    char dbDir[MAX_DB_DIR_LEVEL][MAX_DB_DIR_NAME_SIZE];

    RETURN_ON_ERROR((Convertor::oid2FileName(
        fid, tempFileName, dbDir, partition, 0)));

    std::string rootPath = Config::getDBRootByNum( dbRoot );
    std::ostringstream oss;
    oss << rootPath << '/' <<
        dbDir[0] << '/' <<
        dbDir[1] << '/' <<
        dbDir[2] << '/' <<
        dbDir[3] << '/' <<
        dbDir[4];
    dirName = oss.str();

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Open a file
 * PARAMETERS:
 *    fileName - file name with complete path
 *    pFile - file handle
 * RETURN:
 *    true if exists, false otherwise
 ***********************************************************/
// @bug 5572 - HDFS usage: add *.tmp file backup flag
IDBDataFile* FileOp::openFile( const char* fileName,
    const char* mode,
    const int ioColSize,
    bool useTmpSuffix ) const
{
    IDBDataFile* pFile;
    errno = 0;

    unsigned opts;
    if (ioColSize > 0)
        opts = IDBDataFile::USE_VBUF;
    else
        opts = IDBDataFile::USE_NOVBUF;
    if ((useTmpSuffix) && idbdatafile::IDBPolicy::useHdfs())
        opts |= IDBDataFile::USE_TMPFILE;
    pFile = IDBDataFile::open(
    						IDBPolicy::getType( fileName, IDBPolicy::WRITEENG ),
    						fileName,
    						mode,
    						opts,
                            ioColSize );
    if (pFile == NULL)
    {
        int errRc = errno;
        std::ostringstream oss;
        std::string errnoMsg;
        Convertor::mapErrnoToString(errRc, errnoMsg);
        oss << "FileOp::openFile(): fopen(" << fileName <<
               ", " << mode << "): errno = " << errRc <<
               ": " << errnoMsg;
        logging::Message::Args args;
        args.add(oss.str());
        SimpleSysLog::instance()->logMsg(args,
            logging::LOG_TYPE_CRITICAL,
            logging::M0006);
        SimpleSysLog::instance()->logMsg(args,
            logging::LOG_TYPE_ERROR,
            logging::M0006);
    }

    return pFile;
}

// @bug 5572 - HDFS usage: add *.tmp file backup flag
 IDBDataFile* FileOp::openFile( FID fid,
    uint16_t dbRoot,
    uint32_t partition,
    uint16_t segment,
    std::string&   segFile,
    const char* mode, int ioColSize,
    bool useTmpSuffix ) const
{
    char fileName[FILE_NAME_SIZE];
    int  rc;

    //fid2FileName( fileName, fid );
    RETURN_ON_WE_ERROR( ( rc = getFileName( fid, fileName,
        dbRoot, partition, segment ) ), NULL );

    // disable buffering for versionbuffer file
    if (fid < 1000)
        ioColSize = 0;

    IDBDataFile* pF = openFile( fileName, mode, ioColSize, useTmpSuffix );

    segFile = fileName;

    return pF;
}

/***********************************************************
 * DESCRIPTION:
 *    Read a portion of file to a buffer
 * PARAMETERS:
 *    pFile - file handle
 *    readBuf - read buffer
 *    readSize - the size to read
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_NULL if file handle is NULL
 *    ERR_FILE_READ if something wrong in reading the file
 ***********************************************************/
int FileOp::readFile( IDBDataFile* pFile, unsigned char* readBuf,
                            int readSize ) const
{
    if( pFile != NULL ) {
    	if( pFile->read( readBuf, readSize ) != readSize )
            return ERR_FILE_READ;
    }
    else
        return ERR_FILE_NULL;

    return NO_ERROR;
}

/***********************************************************
 * Reads contents of headers from "pFile" and stores into "hdrs".
 ***********************************************************/
int FileOp::readHeaders( IDBDataFile* pFile, char* hdrs ) const
{
    RETURN_ON_ERROR( setFileOffset(pFile, 0) );
    RETURN_ON_ERROR( readFile( pFile, reinterpret_cast<unsigned char*>(hdrs),
        (IDBCompressInterface::HDR_BUF_LEN * 2) ) );
    IDBCompressInterface compressor;
    int rc = compressor.verifyHdr( hdrs );
    if (rc != 0)
    {
        return ERR_COMP_VERIFY_HDRS;
    }

    return NO_ERROR;
}

/***********************************************************
 * Reads contents of headers from "pFile" and stores into "hdr1" and "hdr2".
 ***********************************************************/
int FileOp::readHeaders( IDBDataFile* pFile, char* hdr1, char* hdr2 ) const
{
    unsigned char* hdrPtr = reinterpret_cast<unsigned char*>(hdr1);
    RETURN_ON_ERROR( setFileOffset(pFile, 0) );
    RETURN_ON_ERROR( readFile( pFile, hdrPtr,
                     IDBCompressInterface::HDR_BUF_LEN ));

    IDBCompressInterface compressor;
    int ptrSecSize = compressor.getHdrSize(hdrPtr) -
                     IDBCompressInterface::HDR_BUF_LEN;
    return readFile( pFile, reinterpret_cast<unsigned char*>(hdr2),
                     ptrSecSize );
}

/***********************************************************
 * DESCRIPTION: No change Old signature
 *    Read a portion of file to a buffer
 * PARAMETERS:
 *    pFile - file handle
 *    offset - file offset
 *    origin - can be SEEK_SET, or SEEK_CUR, or SEEK_END
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_NULL if file handle is NULL
 *    ERR_FILE_SEEK if something wrong in setting the position
 ***********************************************************/
int FileOp::setFileOffset( IDBDataFile* pFile, long long offset, int origin ) const
{
    int rc;
    long long fboOffset = offset; // workaround solution to pass leakcheck error

    if( pFile == NULL )
         return ERR_FILE_NULL;
    if( offset < 0 )
         return ERR_FILE_FBO_NEG;
    rc = pFile->seek( fboOffset, origin );
    if (rc)
         return ERR_FILE_SEEK;

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Read a portion of file to a buffer
 * PARAMETERS:
 *    pFile - file handle
 *    offset - file offset
 *    origin - can be SEEK_SET, or SEEK_CUR, or SEEK_END
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_NULL if file handle is NULL
 *    ERR_FILE_SEEK if something wrong in setting the position
 ***********************************************************/
int FileOp::setFileOffsetBlock( IDBDataFile* pFile, uint64_t lbid, int origin) const
{
    long long  fboOffset = 0;
    int fbo = 0;

    // only when fboFlag is false, we get in here
    u_int16_t  dbRoot;
    u_int32_t  partition;
    u_int16_t  segment;
    RETURN_ON_ERROR( BRMWrapper::getInstance()->getFboOffset(
        lbid, dbRoot, partition, segment, fbo ) );
    fboOffset = ((long long)fbo) * (long)BYTE_PER_BLOCK;

    return setFileOffset( pFile, fboOffset, origin );
}

/***********************************************************
 * DESCRIPTION:
 *    Truncate file to the specified size.
 * PARAMETERS:
 *    pFile - file handle
 *    fileSize - size of file in bytes.
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_NULL if file handle is NULL
 *    ERR_FILE_SEEK if something wrong in setting the position
 ***********************************************************/
int FileOp::truncateFile( IDBDataFile* pFile, long long fileSize ) const
{
    if( pFile == NULL )
        return ERR_FILE_NULL;

    if ( pFile->truncate( fileSize ) != 0 )
        return ERR_FILE_TRUNCATE;

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Write a buffer to a file at at current location
 * PARAMETERS:
 *    pFile - file handle
 *    writeBuf - write buffer
 *    writeSize - the write size
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_NULL if file handle is NULL
 *    ERR_FILE_WRITE if something wrong in writing to the file
 ***********************************************************/
int FileOp::writeFile( IDBDataFile* pFile, const unsigned char* writeBuf,
                             int writeSize ) const
{
    if( pFile != NULL ) {
    	if( pFile->write( writeBuf, writeSize ) != writeSize )
            return ERR_FILE_WRITE;
    }
    else
        return ERR_FILE_NULL;
    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Determine whether the applicable filesystem has room to add the
 *    specified number of blocks (where the blocks contain BYTE_PER_BLOCK
 *    bytes).
 * PARAMETERS:
 *    fileName - file whose file system is to be checked.  Does not have to
 *               be a complete file name.  Dir path is sufficient.
 *    nBlock   - number of 8192-byte blocks to be added
 * RETURN:
 *    true if there is room for the blocks or it can not be determined;
 *    false if file system usage would exceed allowable threshold
 ***********************************************************/
bool FileOp::isDiskSpaceAvail(const std::string& fileName, int nBlocks) const
{
    bool bSpaceAvail = true;

    unsigned maxDiskUsage = Config::getMaxFileSystemDiskUsage();
    if (maxDiskUsage < 100) // 100% means to disable the check
    {
#ifdef _MSC_VER
        ULARGE_INTEGER freeBytesAvail;
        ULARGE_INTEGER totalBytesAvail;
        if (GetDiskFreeSpaceEx(fileName.c_str(), &freeBytesAvail,
            &totalBytesAvail, 0) != 0)
        {
            double avail = (double)freeBytesAvail.QuadPart;
            double total = (double)totalBytesAvail.QuadPart;
            double wanted = (double)nBlocks * (double)BYTE_PER_BLOCK;
            //If we want more than there is, return an error
            if (wanted > avail)
                bSpaceAvail = false;
            //If the remaining bytes would be too few, return an error
            else if ((total - (avail - wanted)) / total * 100.0 > maxDiskUsage)
                bSpaceAvail = false;
        }
#else
        struct statfs fStats;
        int rc = statfs( fileName.c_str(), &fStats );
        if (rc == 0)
        {
            double totalBlocks = fStats.f_blocks;
            double blksToAlloc = (double)(nBlocks*BYTE_PER_BLOCK) /
                                 fStats.f_bsize;
            double freeBlocks  = fStats.f_bavail - blksToAlloc;
            if ((((totalBlocks-freeBlocks)/totalBlocks)*100.0) > maxDiskUsage)
               bSpaceAvail = false;

            //std::cout         << "isDiskSpaceAvail"   <<
            //": totalBlocks: " << totalBlocks          <<
            //"; blkSize: "     << fStats.f_bsize       <<
            //"; nBlocks: "     << nBlocks              <<
            //"; freeBlks: "    << freeBlocks           <<
            //"; pctUsed: " << (((totalBlocks-freeBlocks)/totalBlocks)*100.0) <<
            //"; bAvail: "      << bSpaceAvail          << std::endl;
        }
#endif
    }
    return bSpaceAvail;
}

//------------------------------------------------------------------------------
// Virtual default functions follow; placeholders for derived class if they want
// to override (see ColumnOpCompress1 and DctnryCompress1 in /wrapper).
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Expand current abbreviated extent to a full extent for column segment file
// associated with pFile.  Function leaves fileposition at end of file after
// extent is expanded.
//------------------------------------------------------------------------------
int FileOp::expandAbbrevColumnExtent(
    IDBDataFile* pFile,   // FILE ptr to file where abbrev extent is to be expanded
    uint16_t dbRoot,  // The DBRoot of the file with the abbreviated extent
    uint64_t emptyVal,// Empty value to be used in expanding the extent
    int      width )  // Width of the column (in bytes)
{
    // Based on extent size, see how many blocks to add to fill the extent
    int blksToAdd = ( ((int)BRMWrapper::getInstance()->getExtentRows() -
        INITIAL_EXTENT_ROWS_TO_DISK)/BYTE_PER_BLOCK ) * width;

    // Make sure there is enough disk space to expand the extent.
    RETURN_ON_ERROR( setFileOffset( pFile, 0, SEEK_END ) );
    // TODO-will have to address this DiskSpaceAvail check at some point
    if ( !isDiskSpaceAvail(Config::getDBRootByNum(dbRoot), blksToAdd) )
    {
        return ERR_FILE_DISK_SPACE;
    }

    // Add blocks to turn the abbreviated extent into a full extent.
    int rc = initColumnExtent(pFile, dbRoot, blksToAdd, emptyVal, width,
        false,   // existing file
        true,    // expand existing extent
        false);  // n/a since not adding new extent

    return rc;
}

void FileOp::setTransId(const TxnID& transId)
{
    m_transId = transId;
}

void FileOp::setBulkFlag(bool isBulkLoad)
{
    m_isBulk = isBulkLoad;
}

int FileOp::flushFile(int rc, std::map<FID,FID> & oids)
{
    return NO_ERROR;
}

int FileOp::updateColumnExtent(IDBDataFile* pFile, int nBlocks)
{
    return NO_ERROR;
}

int FileOp::updateDctnryExtent(IDBDataFile* pFile, int nBlocks)
{
    return NO_ERROR;
}

} //end of namespace

