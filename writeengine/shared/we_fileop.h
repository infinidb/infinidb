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

//  $Id: we_fileop.h 3792 2012-04-25 19:28:25Z dcathey $

/** @file */

#ifndef _WE_FILEOP_H_
#define _WE_FILEOP_H_
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <boost/thread.hpp>

#ifdef _MSC_VER
#include <direct.h>
#define S_IRWXU 0
#define S_IRWXG 0
#ifndef S_IROTH
#define S_IROTH 0
#endif
#define S_IXOTH 0
#endif

#include "we_blockop.h"
#include "we_brm.h"
#include "we_config.h"
#include "we_stats.h"

#if defined(_MSC_VER) && defined(WRITEENGINEFILEOP_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

#include "brmtypes.h"

/** Namespace WriteEngine */
namespace WriteEngine
{
  
/** Class FileOp */
class FileOp : public BlockOp
{
public:
   /**
    * @brief Constructor
    */
   EXPORT explicit FileOp(bool doAlloc=true);

   /**
    * @brief Default Destructor
    */
   EXPORT virtual ~FileOp();

   /**
    * @brief Close a file
    */
   EXPORT void           closeFile( FILE* pFile ) const;

   /**
    * @brief Create a directory
    */
   EXPORT const int      createDir( const char* dirName, mode_t mode ) const;
   const int      createDir( const char* dirName ) const
                     { return createDir( dirName, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH ); }

   /**
    * @brief Create a file with a fixed file size and file id
    */
   EXPORT const int      createFile( const FID fid,
                              int & allocSize,
                              const uint16_t dbRoot, const uint32_t partition,
                              const i64 emptyVal = 0, const int width = 1 ) ;

   /**
    * @brief Delete a file
    */
   EXPORT const int      deleteFile( const char* fileName ) const;

   /**
    * @brief Delete the db files corresponding to the specified file id
    */
   EXPORT const int      deleteFile( const FID fid ) const;
   
   /**
    * @brief Delete the db files corresponding to the specified file id
    */
   EXPORT const int      deleteFiles( const std::vector<int32_t>& fids ) const;

   /**
    * @brief Delete the db files corresponding to the specified file id and partition
    */
   EXPORT const int      deletePartition( const std::vector<int32_t>& fids, const uint32_t partition ) const;
   
   /**
    * @brief Delete a specific database segment file.
    */
   EXPORT const int      deleteFile( const FID fid, const u_int16_t dbRoot,
      const u_int32_t partition, const u_int16_t segment ) const;

   /**
    * @brief Check whether a file exists or not
    */
   EXPORT const bool     exists( const char* fileName ) const;

   /**
    * @brief @brief Check whether a file exists or not by using file id, DBRoot,
    * partition, and segment number.
    */
   EXPORT const bool     exists( const FID fid, const u_int16_t dbRoot,
      const u_int32_t partition, const u_int16_t segment ) const;

   /**
    * @brief Check whether a column exists or not by using file id.  Since this
    * is not enough to fully qualify a db filename, all it can do is to verify
    * that the OID directory exists on one or more of the DBRoots.
    */
   EXPORT const bool     existsOIDDir( const FID fid ) const;

   /**
    * @brief Expand current abbreviated extent for this column to a full extent
    *
    * @param pFile    FILE ptr of segment file we are updating.
    * @param dbRoot   DBRoot of the file being updated.
    * @param emptyVal Empty value used in initializing extents for this column
    * @param width    Width of this column (in bytes)
    */
   EXPORT virtual const int     expandAbbrevColumnExtent(
                             FILE*    pFile,
                             uint16_t dbRoot,
                             i64      emptyVal,
                             int      width );

   /**
    * @brief Add an extent to the column file associated with oid.
    *
    * The DBRoot, partition, segment, and HWM of the column file where the
    * extent is added is returned.  If needed, the applicable column segment
    * file will be created.  This extendFile function should supplant other
    * extendFile functions with Multiple-File-per-OID enhancement, "but" we
    * may want to rethink when we do Shared-Nothing.  When this function
    * returns, the file position will be located at the end of the file.
    *
    * @param oid OID of the column to be extended
    * @param emptyVal Empty value to be used for oid
    * @param width Width of the column
    * @param isBulkLoad Is this extent being added during a bulk load.
    *        If true, only the last block is initialized in the file,
    *        else the entire extent will be initialized to disk.
    * @param allocSize (out) Number of blocks allocated to the extent.
    * @param pFile (out) FILE ptr to the file where the extent is added.
    * @param dbRoot (out) The DBRoot of the file with the new extent.
    * @param partition (out) The partition num of the file with the new extent.
    * @param segment (out) The segment number of the file with the new extent.
    * @param segFile (out) Name of the segment file where extent was added.
    * @param hwm (out) The fbo of the column segment file where the new extent
    *        begins.
    * @param startLbid (out) The starting LBID for the new extent
    * @param newFile (out) Indicates if a new file was created for the extent
    * @param hdrs (in/out) Contents of headers, if file is compressed.
    * @return returns NO_ERROR if success.
    */
   EXPORT const int      extendFile(const OID oid, const i64 emptyVal, const int width,
                             bool         isBulkLoad,
                             int&         allocSize,
                             FILE*&       pFile,
                             uint16_t&    dbRoot,
                             uint32_t&    partition,
                             uint16_t&    segment,
                             std::string& segFile,
                             HWM&         hwm,
                             BRM::LBID_t& startLbid,
                             bool&        newFile,
                             char*        hdrs);

   /**
    * @brief Write the specified header info to compressed column file pFile.
    *
    * @param pFile Column file to be written to
    * @param hdr   Header info to be written
    */
   EXPORT int     writeHeaders(FILE* pFile, const char* hdr) const;

   /**
    * @brief Write the specified header info to compressed column or
    * dictionary file pFile.
    *
    * @param pFile Column file to be written to
    * @param controlHdr Control header info to be written
    * @param pointerHdr Pointer header info to be written
    * @param ptrHdrSize Size (in bytes) of pointerHdr
    */
   EXPORT int     writeHeaders(FILE* pFile,
                               const char* controlHdr,
                               const char* pointerHdr,
                               uint64_t ptrHdrSize) const;

   /**
    * @brief Get the filename for the specified fid (OID).
    *
    * This version of getFileName automatically uses 0 for the partition and
    * segment numbers.  If fid is < 4, it is assumed the caller is looking
    * for one of the version buffer files, and DBRoot will be based on the fid,
    * else the DBRoots will be searched for the appropriate file.
    *
    * @param fid (in) OID of the DB file of interest
    * @param fileName (out) the name of the pertinent file that was found
    *
    * @return returns NO_ERROR if success; ERR_FILE_NOT_EXIST if file not found
    */
   const int      getFileName( const FID fid, char* fileName ) const 
   { uint16_t dbRoot = 0;
     if ( fid < 1000 ) { // special case for version buffer files
        size_t rootCnt = Config::DBRootCount();
        dbRoot = (fid % rootCnt) + 1;
     }
     return oid2FileName( fid, fileName, true, dbRoot,0,0 ); 
   }

   /**
    * @brief Get the filename for the specified fid (OID). DBRoot, partition,
    * and segment number.
    *
    * @param fid (in) OID of the DB file of interest
    * @param fileName (out) the name of the pertinent file that was found
    * @param dbRoot (in) DBRoot of the file of interest.  If 0, then all the
    *        DBRoots will be searched.
    * @param partition (in) partition number of the file of interest
    * @param segment (in) segment number of the file of interest
    */
   const int      getFileName( const FID fid, char* fileName,
                     const uint16_t dbRoot,
                     const uint32_t partition, const uint16_t segment ) const
                  { return oid2FileName( fid, fileName, false, dbRoot, partition, segment ); }
   const int      getDirName( const FID fid, char* dirName ) const
                  { return oid2DirName( fid, dirName ); }

   /**
    * @brief Get the file size
    */
   EXPORT const long     getFileSize( FILE* pFile ) const;
   EXPORT int            getFileSize2( FILE* pFile, long long& fileSize ) const;
   EXPORT const long     getFileSize( FID fid, const uint16_t dbRoot,
      const uint32_t partition, const uint16_t segment ) const;
   EXPORT const long     getFileSize( const char* fileName ) const;

   /**
    * @brief Initialize an extent in a column segment file
    * @param pFile (in) FILE* of column segment file to be written to
    * @param dbRoot (in) - DBRoot of pFile
    * @param nBlocks (in) - number of blocks to be written for an extent
    * @param emptyVal(in) - empty value to be used for column data values
    * @param width (in) - width of the applicable column
    * @param bNewFile (in)      -  Adding extent to new file
    * @param bExpandExtent (in) -  Expand existing extent, or initialize new one
    * @param bAbbrevExtent (in) -  If adding new extent, is it abbreviated
    */
   EXPORT const int initColumnExtent( FILE*    pFile,
                                      uint16_t dbRoot,
                                      int      nBlocks,
                                      i64      emptyVal,
                                      int      width,
                                      bool     bNewFile,
                                      bool     bExpandExtent,
                                      bool     bAbbrevExtent );

   /**
    * @brief Initialize an extent in a dictionary store file
    * @param pFile (in) FILE* of dictionary store file to be written to
    * @param dbRoot (in) - DBRoot of pFile
    * @param nBlocks (in) - number of blocks to be written for an extent
    * @param blockHdrInit(in) - data used to initialize each block header
    * @param blockHdrInitSize(in) - number of bytes in blockHdrInit
    * @param bExpandExtent (in) -  Expand existing extent, or initialize new one
    */
   EXPORT const int initDctnryExtent( FILE*    pFile,
                                      uint16_t dbRoot,
                                      int      nBlocks,
                                      unsigned char* blockHdrInit,
                                      int      blockHdrInitSize,
                                      bool     bExpandExtent );

   /**
    * @brief Check whether it is an directory
    */
   EXPORT const bool     isDir( const char* dirName ) const;

   /**
    * @brief See if there is room in the file system for specific number of blks
    * @param fileName Name of file to extend (does not have to be full name)
    * @param nBlocks Number of 8192-byte blocks to be added
    * @return returns TRUE if file system has room for 'nBlocks', else FALSE
    */
   EXPORT bool           isDiskSpaceAvail(const std::string& fileName,
                                   int nBlocks) const;

   /**
    * @brief Convert an oid to a full file name
    */
   EXPORT const int      oid2FileName( const FID fid, char* fullFileName,
      const bool bCreateDir, const uint16_t dbRoot,
      const uint32_t partition, const uint16_t segment ) const;
   EXPORT const int      oid2DirName( const FID fid, char* oidDirName ) const;

   /**
    * @brief Open a file using a filename.
    * @param fileName Name of the file to open.
    * @param mode Mode to use in opening the file (ex: "r+b").
    * @param ioBuffSize Buffer size to be employed by setvbuf().
    * @return returns the FILE* of the opened file.
    */
   EXPORT FILE*          openFile( const char* fileName,
                                   const char* mode = "r+b",
                                   int ioBuffSize = DEFAULT_BUFSIZ) const;

   /**
    * @brief Open a file using an OID, dbroot, partition, and segment number.
    * @param fid OID of the file to be opened.
    * @param dbRoot DBRoot of the file to be opened.
    * @param partition Partition number of the file to be opened.
    * @param segment Segment number of the file to be opened.
    * @param mode Mode to use in opening the file (default of "r+b" will open
    *             an existing binary file as read/write.
    * @param ioBuffSize Buffer size to be employed by setvbuf().
    * @return returns the FILE* of the opened file.
    */
   EXPORT FILE*          openFile( const FID fid,
                                   const uint16_t dbRoot,
                                   const uint32_t partition,
                                   const uint16_t segment,
                                   std::string&   segFile,
                                   const char*    mode = "r+b",
                                   int ioBuffSize = DEFAULT_BUFSIZ) const;

   /**
    * @brief Read to a buffer from a file at current location
    */
   EXPORT const int      readFile( FILE* pFile, unsigned char* readBuf, 
                            const int readSize ) const;

   /**
    * @brief Reads in 2 compression header blocks from a column segment file.
    * FILE* points to start of data when function returns.
    * @param pFile (in) FILE* of column segment file to be read.
    * @param hdrs (out) Contents of headers that are read.
    */
   EXPORT int            readHeaders( FILE* pFile, char* hdrs ) const;
   EXPORT int            readHeaders( FILE* pFile, char* hdr1, char* hdr2 ) const;

   /**
    * @brief Reinitialize a partial extent in a column segment file
    * @param pFile (in) FILE* of column segment file to be written to
    * @param startOffset (in) - file offset where blocks are to be written
    * @param nBlocks (in) - number of blocks to be written to the extent
    * @param emptyVal(in) - empty value to be used for column data values
    * width (in) - width of the applicable column
    */
   EXPORT const int reInitPartialColumnExtent( FILE* pFile,
                            long long startOffset,
                            int       nBlocks,
                            i64       emptyVal,
                            int       width );

   /**
    * @brief Reinitialize an extent in a dictionary store file
    * @param pFile (in) FILE* of dictionary store file to be written to
    * @param startOffset (in) - file offset where blocks are to be written
    * @param nBlocks (in) - number of blocks to be written to the extent
    * @param blockHdrInit(in) - data used to initialize each block header
    * @param blockHdrInitSize(in) - number of bytes in blockHdrInit
    */
   EXPORT const int reInitPartialDctnryExtent( FILE* pFile,
                            long long      startOffset,
                            int            nBlocks,
                            unsigned char* blockHdrInit,
                            int            blockHdrInitSize );

   /**
    * @brief Remove a directory
    */
   const int      removeDir( const char* dir ) const { return ::rmdir( dir ) == 0 ? NO_ERROR : ERR_DIR_REMOVE; }

   /**
    * @brief Set the file to specified location based on the offset
    */
   EXPORT const int      setFileOffset( FILE* pFile, const long long offset, const int origin = SEEK_SET  ) const;
   EXPORT const int      setFileOffsetBlock( FILE* pFile, i64 lbid, const int origin = SEEK_SET ) const;

   /**
    * @brief Truncate the file to the specified file size
    */
   EXPORT const int      truncateFile( FILE* pFile, const long long fileSize ) const;

   /**
    * @brief Write a buffer to a file at current location
    */
   EXPORT const int      writeFile( FILE* pFile, const unsigned char* buf, const int bufSize ) const;

   /**
    * @brief set the flag to use the instance to access the brm wrapper class
    */ 

   virtual void           setTransId( const TxnID& transId){ m_transId = transId; }
   const TxnID    getTransId() const {return m_transId;}
   char*          m_buffer;

   void compressionType(int t) { m_compressionType = t; }
   int compressionType() const { return m_compressionType; }

   EXPORT virtual int flushFile(int rc, std::map<FID,FID> & oids) { return NO_ERROR; }

protected:
   virtual int    updateColumnExtent(FILE* pFile, int nBlocks)
                  { return NO_ERROR; }

   virtual int    updateDctnryExtent(FILE* pFile, int nBlocks)
                  { return NO_ERROR; }

   int            m_compressionType;  // compresssion type

private:
   //not copyable
   FileOp(const FileOp& rhs);
   FileOp& operator=(const FileOp& rhs);

   const int      createFile( const char* fileName, const int fileSize, 
                              const i64 emptyVal, const int width,
                              const uint16_t dbRoot );
   int            initAbbrevCompColumnExtent( FILE* pFile,
                              uint16_t dbRoot,
                              int      nBlocks,
                              i64      emptyVal,
                              int      width);
   static void    initDbRootExtentMutexes();
   static void    removeDbRootExtentMutexes();

   TxnID          m_transId;

   // protect creation of m_DbRootAddExtentMutexes
   static boost::mutex               m_createDbRootMutexes;

   // Mutexes used to serialize extent creation within each DBRoot
   static std::vector<boost::mutex*> m_DbRootAddExtentMutexes;

   // protect race condition in creating directories
   static boost::mutex               m_mkdirMutex;
};

} //end of namespace

#undef EXPORT

#endif // _WE_FILEOP_H_
