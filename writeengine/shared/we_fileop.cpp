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

//  $Id: we_fileop.cpp 4441 2013-01-11 13:10:46Z rdempsey $


#include "config.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sstream>
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

#include "idbcompress.h"
using namespace compress;

#define WRITEENGINEFILEOP_DLLEXPORT
#include "we_fileop.h"
#undef WRITEENGINEFILEOP_DLLEXPORT
#include "we_convertor.h"
#include "we_log.h"
#include "we_config.h"
#include "we_stats.h"

#ifdef _MSC_VER
#define snprintf _snprintf
#endif

namespace WriteEngine
{
   /*static*/ boost::mutex               FileOp::m_createDbRootMutexes;
   /*static*/ boost::mutex               FileOp::m_mkdirMutex;
   /*static*/ std::vector<boost::mutex*> FileOp::m_DbRootAddExtentMutexes;
   const int MAX_NBLOCKS = 8192; // max number of blocks written to an extent
                                 // in 1 call to fwrite(), during initialization


//StopWatch timer;

/**
 * Constructor
 */
FileOp::FileOp(bool doAlloc) : m_buffer(0), m_compressionType(0), m_transId((TxnID)INVALID_NUM)
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
   delete [] m_buffer;
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
void FileOp::closeFile( FILE* pFile ) const
{
    if( pFile != NULL )
        fclose( pFile );
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
const int FileOp::createDir( const char* dirName, mode_t mode ) const
{
    boost::mutex::scoped_lock lk(m_mkdirMutex);
#ifdef _MSC_VER
    int rc = ::mkdir(dirName);
#else
    int rc = ::mkdir( dirName, mode );
#endif
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
const int FileOp::createFile( const char* fileName, const int numOfBlock,
                              const i64 emptyVal, const int width,
                              const uint16_t dbRoot )
{
    FILE*          pFile;

    pFile = fopen( fileName, "w+b" );
    int rc = 0;
    if( pFile != NULL ) {
        setvbuf(pFile, (char*)m_buffer, _IOFBF, DEFAULT_BUFSIZ);

//timer.start( "fwrite");
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

//timer.stop( "fwrite");
        closeFile( pFile );
    }
    else
        return ERR_FILE_CREATE;

//timer.finish();
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
 *    emptyVal - designated "empty" value for this OID
 *    width    - width of column in bytes
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_EXIST if file exists
 *    ERR_FILE_CREATE if can not create the file
 ***********************************************************/
const int FileOp::createFile(const FID fid,
    int&      allocSize,
    const uint16_t dbRoot,
    const uint32_t partition,
    const i64 emptyVal,
    const int width)
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
    RETURN_ON_ERROR( (rc = BRMWrapper::getInstance()->allocateColExtent(
        (const OID)fid, (u_int32_t)width, dbRootx, partitionx, segment,
        startLbid, allocSize, startBlock ) ) );

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
    if ( !isDiskSpaceAvail(Config::getDBRoot(dbRoot-1), totalSize) )
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
const int FileOp::deleteFile( const char* fileName ) const
{
    if( !exists( fileName ) )
        return ERR_FILE_NOT_EXIST;

    return ( remove( fileName ) == -1 ) ? ERR_FILE_DELETE : NO_ERROR;
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
const int FileOp::deleteFile( const FID fid ) const
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

    BRMWrapper::getInstance()->deleteOid(fid);

    unsigned maxDBRoot = Config::DBRootCount();
    for (unsigned i = 0; i < maxDBRoot; i++)
    {
        char rootOidDirName[FILE_NAME_SIZE];
        sprintf(rootOidDirName, "%s/%s", Config::getDBRoot(i), oidDirName);
        boost::filesystem::path dirPath(rootOidDirName);
		try {
			boost::filesystem::remove_all(dirPath);
#ifdef _MSC_VER
		} catch (std::exception& ex) {
			//FIXME: alas, Windows cannot delete a file that is in use :-(
			std::string reason(ex.what());
			std::string ignore("The directory is not empty");
			if (reason.find(ignore) != std::string::npos)
				(void)0;
			else
				throw;
#endif
		} catch (...) {
			throw;
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
const int FileOp::deleteFiles( const std::vector<int32_t>& fids ) const
{
    char tempFileName[FILE_NAME_SIZE];
    char oidDirName  [FILE_NAME_SIZE];
    char dbDir       [MAX_DB_DIR_LEVEL][MAX_DB_DIR_NAME_SIZE];
    unsigned maxDBRoot = Config::DBRootCount();
    for ( unsigned n=0; n<fids.size(); n++ )
    {
        RETURN_ON_ERROR((Convertor::oid2FileName(
            fids[n], tempFileName, dbDir, 0, 0)));
        sprintf(oidDirName, "%s/%s/%s/%s",
            dbDir[0], dbDir[1], dbDir[2], dbDir[3]);
      //std::cout << "Deleting files for OID " << fid <<
      //             "; dirpath: " << oidDirName << std::endl;

        for (unsigned i = 0; i < maxDBRoot; i++)
        {
            char rootOidDirName[FILE_NAME_SIZE];
            sprintf(rootOidDirName, "%s/%s", Config::getDBRoot(i), oidDirName);
            boost::filesystem::path dirPath(rootOidDirName);
			try {
				boost::filesystem::remove_all(dirPath);
#ifdef _MSC_VER
			} catch (std::exception& ex) {
				//FIXME: alas, Windows cannot delete a file that is in use :-(
				std::string reason(ex.what());
				std::string ignore("The directory is not empty");
				if (reason.find(ignore) != std::string::npos)
					(void)0;
				else
					throw;
#endif
			} catch (...) {
				throw;
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
const int FileOp::deletePartition( const std::vector<int32_t>& fids, const uint32_t partition ) const
{
    char tempFileName[FILE_NAME_SIZE];
    char oidDirName  [FILE_NAME_SIZE];
    char dbDir       [MAX_DB_DIR_LEVEL][MAX_DB_DIR_NAME_SIZE];
    unsigned maxDBRoot = Config::DBRootCount();
    for ( unsigned n=0; n<fids.size(); n++ )
    {
        RETURN_ON_ERROR((Convertor::oid2FileName(
            fids[n], tempFileName, dbDir, partition, 0)));
        sprintf(oidDirName, "%s/%s/%s/%s/%s",
            dbDir[0], dbDir[1], dbDir[2], dbDir[3], dbDir[4]);
      //std::cout << "Deleting files for OID " << fid <<
      //             "; dirpath: " << oidDirName << std::endl;

        for (unsigned i = 0; i < maxDBRoot; i++)
        {
            char rootOidDirName[FILE_NAME_SIZE];
            sprintf(rootOidDirName, "%s/%s", Config::getDBRoot(i), oidDirName);
            boost::filesystem::path dirPath(rootOidDirName);
			try {
				boost::filesystem::remove_all(dirPath);
#ifdef _MSC_VER
			} catch (std::exception& ex) {
				//FIXME: alas, Windows cannot delete a file that is in use :-(
				std::string reason(ex.what());
				std::string ignore("The directory is not empty");
				if (reason.find(ignore) != std::string::npos)
					(void)0;
				else
					throw;
#endif
			} catch (...) {
				throw;
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
const int FileOp::deleteFile( const FID fid, const u_int16_t dbRoot,
    const u_int32_t partition, const u_int16_t segment ) const
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
const bool FileOp::exists( const char* fileName ) const
{
    struct stat curStat;
    return ( stat( fileName, &curStat) == 0 );
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
const bool FileOp::exists( const FID fid, const u_int16_t dbRoot,
    const u_int32_t partition, const u_int16_t segment ) const
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
const bool FileOp::existsOIDDir( const FID fid ) const
{
    char fileName[FILE_NAME_SIZE];

    if (getDirName( fid, fileName ) != NO_ERROR)
        return false;

    return exists( fileName );
}

/***********************************************************
 * DESCRIPTION:
 *    Adds an extent to a column segment file for the specified OID.
 *    Function uses ExtentMap to add the extent and determine which
 *    specific column segment file the extent is to be added to. If
 *    the applicable column segment file does not exist, it is created.
 * PARAMETERS:
 *    oid       - OID of the column to be extended
 *    emptyVal  - Empty value to be used for oid
 *    width     - Width of the column (in bytes)
 *    isBulkLoad- Is this extent being added during a bulk load.
 *                If true, only the last block is initialized in the
 *                file, else the entire extent will be initialized to disk.
 *    allocSize - Number of blocks allocated to the extent.
 *    pFile     - FILE ptr to the file where the extent is added.
 *    dbRoot    - The DBRoot of the file with the new extent.
 *    partition - The partition number of the file with the new extent.
 *    segment   - The segment number of the file with the new extent.
 *    segFile   - The name of the relevant column segment file.
 *    hwm       - The HWM (or fbo) of the column segment file where the new
 *                extent begins.
 *    startLbid - The starting LBID for the new extent
 *    newFile   - Indicates if the extent was added to a new or existing file
 *    hdrs      - Contents of the headers if file is compressed.
 * RETURN:
 *    NO_ERROR if success
 *    else the applicable error code is returned
 ***********************************************************/
const int FileOp::extendFile(
    const OID    oid,
    const i64    emptyVal,
    const int    width,
    bool         isbulkload,
    int&         allocSize,
    FILE*&       pFile,
    uint16_t&    dbRoot,
    uint32_t&    partition,
    uint16_t&    segment,
    std::string& segFile,
    HWM&         hwm,
    BRM::LBID_t& startLbid,
    bool&        newFile,
    char*        hdrs) 
{
    int rc = NO_ERROR;
    pFile = 0;
    segFile.clear();
    newFile = false;
    // Allocate the new extent in the ExtentMap
    RETURN_ON_ERROR( BRMWrapper::getInstance()->allocateColExtent(
        oid, width, dbRoot, partition, segment, startLbid, allocSize, hwm) );

    // Determine the existence of the "next" segment file, and either open
    // or create the segment file accordingly.
    if (exists(oid, dbRoot, partition, segment))
    {
        pFile = openFile( oid, dbRoot, partition, segment,
            segFile, "r+b" );//old file
        if (pFile == 0)
            return ERR_FILE_OPEN;

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
            {
                if (!isbulkload)
                    closeFile( pFile );
                return rc;
            }
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
    if (!isDiskSpaceAvail(Config::getDBRoot(dbRoot - 1), allocSize))
#else
    if ( !isDiskSpaceAvail(segFile, allocSize) )
#endif
    {
        if (!isbulkload)
            closeFile( pFile );
        return ERR_FILE_DISK_SPACE;
    }

    // We set to EOF just before we start adding the blocks for the new extent.
    // At one time, I considered changing this to seek to the HWM block, but
    // with compressed files, this is murky; do I find and seek to the chunk
    // containing the HWM block?  So I left as-is for now, seeking to EOF.
    rc = setFileOffset(pFile, 0, SEEK_END);
    if (rc)
    {
        if (!isbulkload)
            closeFile( pFile );
        return rc;
    }
    // Initialize the contents of the extent.
    rc = initColumnExtent( pFile,
                           dbRoot,
                           allocSize,
                           emptyVal,
                           width,
                           newFile, // new or existing file
                           false,   // don't expand; new extent
                           false ); // add full (not abbreviated) extent

    // Only close file for DML/DDL; leave file open for bulkload
    if (!isbulkload)
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
 *    pFile   (in) - FILE* of column segment file to be written to
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
const int FileOp::initColumnExtent(
    FILE*    pFile,
    uint16_t dbRoot,
    int      nBlocks,
    i64      emptyVal,
    int      width,
    bool     bNewFile,
    bool     bExpandExtent,
    bool     bAbbrevExtent )
{
    // Create vector of mutexes used to serialize extent access per DBRoot
    initDbRootExtentMutexes( );

    if ((bNewFile) && (m_compressionType))
    {
        char hdrs[IDBCompressInterface::HDR_BUF_LEN*2];
        IDBCompressInterface compressor;
        compressor.initHdr(hdrs, m_compressionType);
        if (bAbbrevExtent)
            compressor.setBlockCount(hdrs, nBlocks);
        RETURN_ON_ERROR(writeHeaders(pFile, hdrs));
    }

    // Determine the number of blocks in each call to fwrite(), and the
    // number of fwrite() calls to make, based on this.  In other words,
    // we put a cap on the "writeSize" so that we don't allocate and write
    // an entire extent at once for the 64M row extents.
    // If we are expanding an abbreviated 64M extent, we may not have an even
    // multiple of MAX_NBLOCKS to write; remWriteSize is the number of blocks
    // above and beyond loopCount*MAX_NBLOCKS.
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
    assert(dbRoot > 0);
#ifdef PROFILE
    if (bExpandExtent)
        Stats::startParseEvent(WE_STATS_WAIT_TO_EXPAND_COL_EXTENT);
    else
        Stats::startParseEvent(WE_STATS_WAIT_TO_CREATE_COL_EXTENT);
#endif
    boost::mutex::scoped_lock lk(*m_DbRootAddExtentMutexes[dbRoot-1]);
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
            if (fwrite( writeBuf, remWriteSize, 1, pFile ) != 1)
            {
                return ERR_FILE_WRITE;
            }
        }
        for (int j=0; j<loopCount; j++)
        {
            if (fwrite( writeBuf, writeSize, 1, pFile ) != 1)
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
#ifdef _MSC_VER
    _commit(_fileno(pFile));
#else
    fsync( fileno(pFile) );
#endif

#ifdef PROFILE
    if (bExpandExtent)
        Stats::stopParseEvent(WE_STATS_EXPAND_COL_EXTENT);
    else
        Stats::stopParseEvent(WE_STATS_CREATE_COL_EXTENT);
#endif

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Write (initialize) an abbreviated compressed extent in a column file.
 *    nBlocks controls how many 8192-byte blocks are to be written out.
 * PARAMETERS:
 *    pFile   (in) - FILE* of column segment file to be written to
 *    dbRoot  (in) - DBRoot of pFile
 *    nBlocks (in) - number of blocks to be written for an extent
 *    emptyVal(in) - empty value to be used for column data values
 *    width   (in) - width of the applicable column
 * RETURN:
 *    returns ERR_FILE_WRITE or ERR_FILE_SEEK if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
int FileOp::initAbbrevCompColumnExtent(
    FILE*    pFile,
    uint16_t dbRoot,
    int      nBlocks,
    i64      emptyVal,
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
    const int INPUT_BUFFER_SIZE     = INITIAL_EXTENT_ROWS_TO_DISK * width;
    char* toBeCompressedInput       = new char[INPUT_BUFFER_SIZE];
    unsigned int userPaddingBytes   = Config::getNumCompressedPadBlks() *
                                      BYTE_PER_BLOCK;
    const int OUTPUT_BUFFER_SIZE    = int( (double)INPUT_BUFFER_SIZE * 1.17 ) +
        userPaddingBytes;
    unsigned char* compressedOutput = new unsigned char[OUTPUT_BUFFER_SIZE];
    unsigned int outputLen          = OUTPUT_BUFFER_SIZE;
    boost::scoped_array<char> toBeCompressedInputPtr( toBeCompressedInput );
    boost::scoped_array<unsigned char> compressedOutputPtr(compressedOutput);

    setEmptyBuf( (unsigned char*)toBeCompressedInput,
        INPUT_BUFFER_SIZE, emptyVal, width);

    // Compress an initialized abbreviated extent
    IDBCompressInterface compressor( userPaddingBytes );
    rc = compressor.compressBlock(toBeCompressedInput,
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

    //std::cout << "Uncompressed byte count: " << INPUT_BUFFER_SIZE <<
    //    "; compressed byte count: " << outputLen << std::endl;

    char hdrs[IDBCompressInterface::HDR_BUF_LEN*2];
    compressor.initHdr(hdrs, m_compressionType);
    compressor.setBlockCount(hdrs, nBlocks);

    // Store compression pointers in the header
    std::vector<uint64_t> ptrs;
    ptrs.push_back( IDBCompressInterface::HDR_BUF_LEN*2 );
    ptrs.push_back( outputLen + (IDBCompressInterface::HDR_BUF_LEN*2) );
    compressor.storePtrs(ptrs, hdrs);

    RETURN_ON_ERROR( writeHeaders(pFile, hdrs) );

    // Write the compressed data
    if (fwrite( compressedOutput, outputLen, 1, pFile ) != 1)
    {
        return ERR_FILE_WRITE;
    }
#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_COMPRESS_COL_INIT_ABBREV_EXT);
#endif

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Write headers to a compressed column file.
 * PARAMETERS:
 *    pFile   (in) - FILE* of column segment file to be written to
 *    hdr     (in) - header pointers to be written
 * RETURN:
 *    returns ERR_FILE_WRITE or ERR_FILE_SEEK if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
int FileOp::writeHeaders(FILE* pFile, const char* hdr) const
{
    RETURN_ON_ERROR( setFileOffset(pFile, 0, SEEK_SET) );

    // Write the headers
    if (fwrite( hdr, IDBCompressInterface::HDR_BUF_LEN*2, 1, pFile ) != 1)
    {
        return ERR_FILE_WRITE;
    }

   return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Write headers to a compressed column or dictionary file.
 * PARAMETERS:
 *    pFile   (in) - FILE* of column segment file to be written to
 *    controlHdr (in) - control header to be written
 *    pointerHdr (in) - pointer header to be written
 *    ptrHdrSize (in) - size (in bytes) of pointer header
 * RETURN:
 *    returns ERR_FILE_WRITE or ERR_FILE_SEEK if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
int FileOp::writeHeaders(FILE* pFile, const char* controlHdr,
    const char* pointerHdr, uint64_t ptrHdrSize) const
{
    RETURN_ON_ERROR( setFileOffset(pFile, 0, SEEK_SET) );

    // Write the control header
    if (fwrite( controlHdr, IDBCompressInterface::HDR_BUF_LEN, 1, pFile ) != 1)
    {
        return ERR_FILE_WRITE;
    }

    // Write the pointer header
    if (fwrite( pointerHdr, ptrHdrSize, 1, pFile ) != 1)
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
 *    pFile   (in) - FILE* of column segment file to be written to
 *    dbRoot  (in) - DBRoot of pFile
 *    nBlocks (in) - number of blocks to be written for an extent
 *    blockHdrInit(in) - data used to initialize each block
 *    blockHdrInitSize(in) - number of bytes in blockHdrInit
 *    bExpandExtent (in) - Expand existing extent, or initialize a new one
 * RETURN:
 *    returns ERR_FILE_WRITE if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
const int FileOp::initDctnryExtent(
    FILE*          pFile,
    uint16_t       dbRoot,
    int            nBlocks,
    unsigned char* blockHdrInit,
    int            blockHdrInitSize,
    bool           bExpandExtent )
{
    // Create vector of mutexes used to serialize extent access per DBRoot
    initDbRootExtentMutexes( );

    // Determine the number of blocks in each call to fwrite(), and the
    // number of fwrite() calls to make, based on this.  In other words,
    // we put a cap on the "writeSize" so that we don't allocate and write
    // an entire extent at once for the 64M row extents.
    // If we are expanding an abbreviated 64M extent, we may not have an even
    // multiple of MAX_NBLOCKS to write; remWriteSize is the number of blocks
    // above and beyond loopCount*MAX_NBLOCKS.
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
    assert(dbRoot > 0);
#ifdef PROFILE
    if (bExpandExtent)
        Stats::startParseEvent(WE_STATS_WAIT_TO_EXPAND_DCT_EXTENT);
    else
        Stats::startParseEvent(WE_STATS_WAIT_TO_CREATE_DCT_EXTENT);
#endif
    boost::mutex::scoped_lock lk(*m_DbRootAddExtentMutexes[dbRoot-1]);
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
            if (fwrite( writeBuf, remWriteSize, 1, pFile ) != 1)
            {
                return ERR_FILE_WRITE;
            }
        }
        for (int j=0; j<loopCount; j++)
        {
            if (fwrite( writeBuf, writeSize, 1, pFile ) != 1)
            {
                return ERR_FILE_WRITE;
            }
        }
    }

    if (m_compressionType)
        updateDctnryExtent(pFile, nBlocks);

    // Synchronize here to avoid write buffer pile up too much, which could
    // cause controllernode to timeout later when it needs to save a snapshot.
#ifdef _MSC_VER
    _commit(_fileno(pFile));
#else
    fsync( fileno(pFile) );
#endif
#ifdef PROFILE
    if (bExpandExtent)
        Stats::stopParseEvent(WE_STATS_EXPAND_DCT_EXTENT);
    else
        Stats::stopParseEvent(WE_STATS_CREATE_DCT_EXTENT);
#endif

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Expand the current abbreviated extent to a full extent for the column
 *    segment file associated with pFile.  Function leaves fileposition at
 *    end of file after extent is expanded.
 * PARAMETERS:
 *    pFile     - FILE ptr to the file where abbrev extent is to be expanded
 *    dbRoot    - The DBRoot of the file with the abbreviated extent.
 *    emptyVal  - Empty value to be used in expanding the extent.
 *    width     - Width of the column (in bytes)
 * RETURN:
 *    NO_ERROR if success
 *    else the applicable error code is returned
 ***********************************************************/
const int FileOp::expandAbbrevColumnExtent(
    FILE*    pFile,
    uint16_t dbRoot,
    i64      emptyVal,
    int      width )
{
    // Based on extent size, see how many blocks to add to fill the extent
    int blksToAdd = ( ((int)BRMWrapper::getInstance()->getExtentRows() -
        INITIAL_EXTENT_ROWS_TO_DISK)/BYTE_PER_BLOCK ) * width;

    // Make sure there is enough disk space to expand the extent.
    RETURN_ON_ERROR( setFileOffset( pFile, 0, SEEK_END ) );
    if ( !isDiskSpaceAvail(Config::getDBRoot(dbRoot-1), blksToAdd) )
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
        size_t rootCnt = Config::DBRootCount();

        for (size_t i=0; i<rootCnt; i++)
        {
            m_DbRootAddExtentMutexes.push_back( new boost::mutex );
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
    size_t mutexCnt = m_DbRootAddExtentMutexes.size();

    for (size_t i=0; i<mutexCnt; i++)
    {
        delete m_DbRootAddExtentMutexes[i];
    }
}

/***********************************************************
 * DESCRIPTION:
 *    Write out (reinitialize) a partial extent in a column file.
 *    A mutex is not used to prevent contention between threads,
 *    because the extent should already be in place on disk; so
 *    disk fragmentation is not an issue.
 * PARAMETERS:
 *    pFile   (in) - FILE* of column segment file to be written to
 *    startOffset(in)-file offset where we are to begin writing blocks
 *    nBlocks (in) - number of blocks to be written to the extent
 *    emptyVal(in) - empty value to be used for column data values
 *    width   (in) - width of the applicable column
 * RETURN:
 *    returns ERR_FILE_WRITE if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
const int FileOp::reInitPartialColumnExtent(
    FILE*    pFile,
    long long startOffset,
    int      nBlocks,
    i64      emptyVal,
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
            if (fwrite( writeBuf, writeSize, 1, pFile ) != 1)
            {
                return ERR_FILE_WRITE;
            }
        }

        if (remainderSize > 0)
        {
            if (fwrite( writeBuf, remainderSize, 1, pFile ) != 1)
            {
                return ERR_FILE_WRITE;
            }
        }
    }

    // Synchronize here to avoid write buffer pile up too much, which could
    // cause controllernode to timeout later when it needs to save a snapshot.
#ifdef _MSC_VER
    _commit(_fileno(pFile));
#else
    fsync( fileno(pFile) );
#endif
    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Write out (reinitialize) a partial extent in a dictionary store file.
 *    A mutex is not used to prevent contention between threads,
 *    because the extent should already be in place on disk; so
 *    disk fragmentation is not an issue.
 * PARAMETERS:
 *    pFile   (in) - FILE* of column segment file to be written to
 *    startOffset(in)-file offset where we are to begin writing blocks
 *    nBlocks (in) - number of blocks to be written to the extent
 *    blockHdrInit(in) - data used to initialize each block
 *    blockHdrInitSize(in) - number of bytes in blockHdrInit
 * RETURN:
 *    returns ERR_FILE_WRITE if an error occurs,
 *    else returns NO_ERROR.
 ***********************************************************/
const int FileOp::reInitPartialDctnryExtent(
    FILE*          pFile,
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
            if (fwrite( writeBuf, writeSize, 1, pFile ) != 1)
            {
                return ERR_FILE_WRITE;
            }
        }

        if (remainderSize > 0)
        {
            if (fwrite( writeBuf, remainderSize, 1, pFile ) != 1)
            {
                return ERR_FILE_WRITE;
            }
        }
    }

    // Synchronize here to avoid write buffer pile up too much, which could
    // cause controllernode to timeout later when it needs to save a snapshot.
#ifdef _MSC_VER
    _commit(_fileno(pFile));
#else
    fsync( fileno(pFile) );
#endif
    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Get file size
 * PARAMETERS:
 *    pFile - file handle
 * RETURN:
 *    fileSize - the file size
 ***********************************************************/
//dmc-need to work on API; overloading return value as size and rc
const long FileOp::getFileSize( FILE* pFile ) const
{
    long fileSize = 0;
    if( pFile == NULL )
        return ERR_FILE_NULL;

    RETURN_ON_ERROR( setFileOffset(pFile, 0, SEEK_END) );

#ifdef _MSC_VER
    fileSize = _ftelli64(pFile);
#else
    fileSize = ftell( pFile );
#endif
    rewind ( pFile );

    return fileSize;
}

/***********************************************************
 * DESCRIPTION:
 *    Get file size.
 *    Would like to eventually remove getFileSize(FILE*) and replace
 *    with this version of getFileSize().  Calling fstat() should be more
 *    efficient than seeking to the end of a large file.  Plus
 *    getFileSize(FILE*) automatically rewinds to the start of the file,
 *    which the application code might not want.
 * PARAMETERS:
 *    pFile - file handle
 *    fileSize (out) - file size in bytes
 * RETURN:
 *    fileSize - the file size
 ***********************************************************/
int FileOp::getFileSize2( FILE* pFile, long long& fileSize ) const
{
    if ( pFile == NULL )
        return ERR_FILE_NULL;

    struct stat statBuf;
    int rc = fstat( fileno(pFile), &statBuf );
    if (rc != 0)
        return ERR_FILE_STAT;

    fileSize = statBuf.st_size;

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Get file size using file id
 * PARAMETERS:
 *    fid - file handle
 * RETURN:
 *    fileSize
 ***********************************************************/
const long FileOp::getFileSize( FID fid, const u_int16_t dbRoot,
    const u_int32_t partition, const u_int16_t segment ) const
{
    FILE* pFile;
    long fileSize = 0;

    std::string segFile;
    pFile = openFile( fid, dbRoot, partition, segment, segFile );
    if( pFile != NULL ) {
        fileSize = getFileSize( pFile );
        closeFile( pFile );
    }
    return fileSize;
}

const long FileOp::getFileSize( const char* fileName ) const
{
    struct stat curStat;
    long fileSize = 0;

    if( ::stat( fileName, &curStat ) == 0 )
        fileSize = (long) curStat.st_size;

    return fileSize;
}

/***********************************************************
 * DESCRIPTION:
 *    Check whether it is a directory
 * PARAMETERS:
 *    dirName - directory name
 * RETURN:
 *    true if it is, false otherwise
 ***********************************************************/
const bool  FileOp::isDir( const char* dirName ) const
{
    struct stat curStat;
    return ( stat( dirName, &curStat)!= 0 ) ? false : (( curStat.st_mode & S_IFDIR)!= 0);
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
const int FileOp::oid2FileName( const FID fid,
    char* fullFileName,
    const bool bCreateDir,
    const uint16_t dbRoot,
    const uint32_t partition,
    const uint16_t segment) const
{
#ifdef SHARED_NOTHING_DEMO_2
    if (fid >= 10000) {
        char root[FILE_NAME_SIZE];
        Config::getSharedNothingRoot(root);
        sprintf(fullFileName, "%s/FILE%d", root, fid);
        return NO_ERROR;
    }
#endif
//Get the hashed part of the filename. This is the tail-end of the filename path,
//  excluding the DBRoot.
    char tempFileName[FILE_NAME_SIZE];
    char dbDir[MAX_DB_DIR_LEVEL][MAX_DB_DIR_NAME_SIZE];
    RETURN_ON_ERROR((Convertor::oid2FileName(
        fid, tempFileName, dbDir, partition, segment)));

    // see if file exists in specified DBRoot; return if found
    if (dbRoot > 0) {
        sprintf(fullFileName, "%s/%s", Config::getDBRoot(dbRoot-1),
            tempFileName);

        //std::cout << "oid2FileName() OID: " << fid <<
        //   " searching for file: " << fullFileName <<std::endl;

        if (access(fullFileName, R_OK) == 0) return NO_ERROR;

        //file wasn't found, user doesn't want dirs to be created, we're done
        if (!bCreateDir)
            return NO_ERROR;

        //std::cout << "oid2FileName() OID: " << fid <<
        //   " creating file: " << fullFileName <<std::endl;
    }
    else {
        //Now try to find the file in each of the DBRoots.
        unsigned maxDBRoot = Config::DBRootCount();

        for (unsigned i = 0; i < maxDBRoot; i++)
        {
            sprintf(fullFileName, "%s/%s", Config::getDBRoot(i), tempFileName);
            //found it, nothing more to do, return
            if (access(fullFileName, R_OK) == 0) return NO_ERROR;
        }

        //file wasn't found, user didn't specify DBRoot so we can't create
        return ERR_FILE_NOT_EXIST;
    }

    char dirName[FILE_NAME_SIZE];

    sprintf( dirName, "%s/%s", Config::getDBRoot(dbRoot-1), dbDir[0] );
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

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION:
 *    Search for directory path associated with specified OID.
 * PARAMETERS:
 *    fid   - (in)  OID to search for
 *    pFile - (out) OID directory path (including DBRoot) that is found
 * RETURN:
 *    NO_ERROR if OID dir path found, else returns ERR_FILE_NOT_EXIST
 ***********************************************************/
const int FileOp::oid2DirName( const FID fid, char* oidDirName ) const
{
    char tempFileName[FILE_NAME_SIZE];
    char dbDir[MAX_DB_DIR_LEVEL][MAX_DB_DIR_NAME_SIZE];

    RETURN_ON_ERROR((Convertor::oid2FileName(
        fid, tempFileName, dbDir, 0, 0)));

    //Now try to find the directory in each of the DBRoots.
    unsigned maxDBRoot = Config::DBRootCount();

    for (unsigned i = 0; i < maxDBRoot; i++)
    {
        sprintf(oidDirName, "%s/%s/%s/%s/%s",
            Config::getDBRoot(i), dbDir[0], dbDir[1], dbDir[2], dbDir[3]);

        //found it, nothing more to do, return
        if (access(oidDirName, R_OK) == 0) return NO_ERROR;
    }

    return ERR_FILE_NOT_EXIST;
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
FILE* FileOp::openFile( const char* fileName, const char* mode,const int ioBuffSize ) const
{
    FILE* pFile;

    pFile = fopen( fileName, mode );
    if (pFile == NULL)
    {
        int errRc = errno;
        logging::MessageLog ml(logging::LoggingID(19));
        logging::Message m;
        logging::Message::Args args;
        std::ostringstream oss;
        std::string errnoMsg;
        Convertor::mapErrnoToString(errRc, errnoMsg);
        oss << "FileOp::openFile(): fopen(" << fileName <<
               ", " << mode << "): errno = " << errRc <<
               ": " << errnoMsg;
        args.add(oss.str());
        m.format(args);
        ml.logCriticalMessage(m);
    }
    else if (ioBuffSize > 0) {
        setvbuf(pFile, (char*)m_buffer, _IOFBF, ioBuffSize);
    }
    else { // ioBuffSize == 0, disable buffering with _IONBF
        setvbuf(pFile, NULL, _IONBF, 0);
    }

    return pFile;
}

 FILE* FileOp::openFile( const FID fid,
    const uint16_t dbRoot,
    const uint32_t partition,
    const uint16_t segment,
    std::string&   segFile,
    const char* mode, int ioBuffSize ) const
{
    char fileName[FILE_NAME_SIZE];
    int  rc;

    //fid2FileName( fileName, fid );
    RETURN_ON_WE_ERROR( ( rc = getFileName( fid, fileName,
        dbRoot, partition, segment ) ), NULL );

    // disable buffering for versionbuffer file
    if (fid < 1000)
        ioBuffSize = 0;

    FILE* pF = openFile( fileName, mode, ioBuffSize );

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
const int FileOp::readFile( FILE* pFile, unsigned char* readBuf,
                            const int readSize ) const
{
    size_t byteRead;

    if( pFile != NULL ) {
        byteRead = fread( readBuf, 1, readSize, pFile );
        if( (int) byteRead != readSize )
            return ERR_FILE_READ;
    }
    else
        return ERR_FILE_NULL;

    return NO_ERROR;
}

/***********************************************************
 * Reads contents of headers from "pFile" and stores into "hdrs".
 ***********************************************************/
int FileOp::readHeaders( FILE* pFile, char* hdrs ) const
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
int FileOp::readHeaders( FILE* pFile, char* hdr1, char* hdr2 ) const
{
    unsigned char* hdrPtr = reinterpret_cast<unsigned char*>(hdr1);
    RETURN_ON_ERROR( setFileOffset(pFile, 0) );
    RETURN_ON_ERROR( readFile( pFile, hdrPtr, IDBCompressInterface::HDR_BUF_LEN ));

    IDBCompressInterface compressor;
    int ptrSecSize = compressor.getHdrSize(hdrPtr) - IDBCompressInterface::HDR_BUF_LEN;
    return readFile( pFile, reinterpret_cast<unsigned char*>(hdr2), ptrSecSize );
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
const int FileOp::setFileOffset( FILE* pFile, const long long offset, const int origin ) const
{
    int rc;
    long long  fboOffset = offset;     // a workaround solution to pass leakcheck errors

    if( pFile == NULL )
         return ERR_FILE_NULL;
    if( offset < 0 )
         return ERR_FILE_FBO_NEG;
#ifdef _MSC_VER
    rc = _fseeki64(pFile, fboOffset, origin);
#else
    rc = fseeko( pFile, fboOffset, origin );
#endif
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
const int FileOp::setFileOffsetBlock( FILE* pFile,  const i64 lbid, const int origin) const
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
const int FileOp::truncateFile( FILE* pFile, const long long fileSize ) const
{
    if( pFile == NULL )
        return ERR_FILE_NULL;

#ifdef _MSC_VER
    if (_chsize_s(_fileno(pFile), fileSize) != 0)
#else
    if ( ftruncate(fileno(pFile), fileSize) != 0 )
#endif
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
const int FileOp::writeFile( FILE* pFile, const unsigned char* writeBuf,
                             const int writeSize ) const
{
    size_t byteWrite;

    if( pFile != NULL ) {
        byteWrite = fwrite( writeBuf, 1, writeSize, pFile );
        if( (int) byteWrite != writeSize )
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
        if (GetDiskFreeSpaceEx(fileName.c_str(), &freeBytesAvail, &totalBytesAvail, 0) != 0)
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

} //end of namespace

