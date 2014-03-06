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

//  $Id: we_fileop.h 4737 2013-08-14 20:45:46Z bwilkinson $

/** @file */

#ifndef _WE_FILEOP_H_
#define _WE_FILEOP_H_
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <map>
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
#include "idbcompress.h"

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
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
    * @brief Destructor
    */
    EXPORT virtual ~FileOp();

   /**
    * @brief Close a file
    */
    EXPORT void         closeFile( IDBDataFile* pFile ) const;

   /**
    * @brief Create a directory
    */
    EXPORT int          createDir( const char* dirName, mode_t mode ) const;
    int                 createDir( const char* dirName ) const;

   /**
    * @brief Create a file with a fixed file size and file id
    */
    EXPORT int          createFile( FID fid,
                            int & allocSize,
                            uint16_t dbRoot, uint32_t partition,
                            execplan::CalpontSystemCatalog::ColDataType colDataType,
                            uint64_t emptyVal = 0, int width = 1 ) ;

   /**
    * @brief Delete a file
    */
    EXPORT int          deleteFile( const char* fileName ) const;

   /**
    * @brief Delete the db files corresponding to the specified file id
    */
    EXPORT int          deleteFile( FID fid ) const;
   
   /**
    * @brief Delete the db files corresponding to the specified file id
    */
    EXPORT int          deleteFiles( const std::vector<int32_t>& fids ) const;

   
   /**
    * @brief Delete db files corresponding to specified file id and partition
    */
    EXPORT int          deletePartitions( const std::vector<OID>& fids, 
                            const std::vector<BRM::PartitionInfo>& partitions )
                            const;
   
   /**
    * @brief Delete a specific database segment file.
    */
    EXPORT int          deleteFile( FID fid, uint16_t dbRoot,
                            uint32_t partition,
                            uint16_t segment ) const;

   /**
    * @brief Check whether a file exists or not
    */
    EXPORT bool         exists( const char* fileName ) const;

   /**
    * @brief @brief Check whether file exists or not by using file id, DBRoot,
    * partition, and segment number.
    */
    EXPORT bool         exists( FID fid, uint16_t dbRoot,
                            uint32_t partition, uint16_t segment ) const;

   /**
    * @brief Check whether a column exists or not by using file id.  Since this
    * is not enough to fully qualify a db filename, all it can do is to verify
    * that the OID directory exists on one or more of the DBRoots.
    */
    EXPORT bool         existsOIDDir( FID fid ) const;

   /**
    * @brief Expand current abbreviated extent for this column to a full extent
    *
    * @param pFile    FILE ptr of segment file we are updating.
    * @param dbRoot   DBRoot of the file being updated.
    * @param emptyVal Empty value used in initializing extents for this column
    * @param width    Width of this column (in bytes)
    */
    EXPORT virtual int  expandAbbrevColumnExtent(
                            IDBDataFile*    pFile,
                            uint16_t dbRoot,
                            uint64_t      emptyVal,
                            int      width );

   /**
    * @brief Add an extent to the specified Column OID and DBRoot.
    * The extent must already exist in the extentmap prior to calling this fctn.
    *
    * The partition, segment, and HWM of the column file where the
    * extent is added is returned.  If needed, the applicable column segment
    * file will be created.  This extendFile1 function should supplant other
    * extendFile functions with Multiple-File-per-OID enhancement, "but" we
    * may want to rethink when we do Shared-Nothing.  When this function
    * returns, the file position will be located at the end of the file.
    * For shared-everything DBRoot was an output argument, as BRM selected the
    * the DBRoot.  For shared-nothing DBRoot is an input argument, as the
    * application code must track/control the DBRoot selection.
    * If this is the very first file for the specified DBRoot, then the
    * partition and segment number must be specified, else the selected
    * partition and segment numbers are returned.
    *
    * @param oid OID of the column to be extended
    * @param emptyVal Empty value to be used for oid
    * @param width Width of the column
    * @param hwm The fbo of the column segment file where the new extent begins
    * @param startLbid The starting LBID for the new extent
    * @param allocSize Number of blocks allocated to the extent.
    * @param dbRoot The DBRoot of the file with the new extent.
    * @param partition The partnum of the file with the new extent.
    * @param segment The segnum of the file with the new extent.
    * @param segFile (out) Name of the segment file where extent was added.
    * @param pFile (out) FILE ptr to the file where the extent is added.
    * @param newFile (out) Indicates if a new file was created for the extent
    * @param hdrs (in/out) Contents of headers, if file is compressed.
    * @return returns NO_ERROR if success.
    */
    EXPORT int          extendFile(OID oid, uint64_t emptyVal,
                            int          width,
                            HWM          hwm,
                            BRM::LBID_t  startLbid,
                            int          allocSize,
                            uint16_t     dbRoot,
                            uint32_t     partition,
                            uint16_t     segment,
                            std::string& segFile,
                            IDBDataFile*&       pFile,
                            bool&        newFile,
                            char*        hdrs);

   /**
    * @brief  For alter table add column; add an extent to a specific file
    *
    * @param oid OID of the column to be extended
    * @param emptyVal Empty value to be used for oid
    * @param width Width of the column
    * @param allocSize (out) Number of blocks allocated to the extent.
    * @param dbRoot The DBRoot of the file with the new extent.
    * @param partition The partnum of the file with the new extent.
    * @param segment The segnum of the file with the new extent.
    * @param segFile (out) Name of the segment file where extent was added.
    * @param startLbid (out) The starting LBID for the new extent
    * @param newFile (out) Indicates if a new file was created for the extent
    * @param hdrs (in/out) Contents of headers, if file is compressed.
    */
    EXPORT int          addExtentExactFile(OID oid, uint64_t emptyVal,
                            int          width,
                            int&         allocSize,
                            uint16_t     dbRoot,
                            uint32_t     partition,
                            uint16_t     segment,
                            execplan::CalpontSystemCatalog::ColDataType colDataType,
                            std::string& segFile,
                            BRM::LBID_t& startLbid,
                            bool&        newFile,
                            char*        hdrs);

   /**
    * @brief Pad the specified compressed extent with empty chunks
    * @param oid OID of relevant column
    * @param width Width in bytes of this column
    * @param emptyVal Empty value to be employed in filling the chunks
    * @param dbRoot DBRoot of the extent to be filled
    * @param partition Partition of the extent to be filled
    * @param segment Segment file number of the extent to be filled
    * @param hwm New HWM blk setting for the segment file after extent is padded
    * @param segFile (out) Name of updated segment file
    * @param errTask (out) Task that failed if error occurs
    * @return returns NO_ERROR if success.
    */
    EXPORT int          fillCompColumnExtentEmptyChunks(OID oid,
                            int          colWidth,
                            uint64_t          emptyVal,
                            uint16_t     dbRoot,
                            uint32_t     partition,
                            uint16_t     segment,
                            HWM          hwm,
                            std::string& segFile,
                            std::string& errTask);

   /**
    * @brief Write the specified header info to compressed column file pFile.
    *
    * @param pFile Column file to be written to
    * @param hdr   Header info to be written
    */
    EXPORT int          writeHeaders(IDBDataFile* pFile, const char* hdr) const;

   /**
    * @brief Write the specified header info to compressed column or
    * dictionary file pFile.
    *
    * @param pFile Column file to be written to
    * @param controlHdr Control header info to be written
    * @param pointerHdr Pointer header info to be written
    * @param ptrHdrSize Size (in bytes) of pointerHdr
    */
    EXPORT int          writeHeaders(IDBDataFile* pFile,
                            const char* controlHdr,
                            const char* pointerHdr,
                            uint64_t ptrHdrSize) const;

   /**
    * @brief Get the Version Buffer filename for the specified fid (OID).
    *
    * This version of getFileName automatically uses 0 for the partition and
    * segment numbers.  The applicable DBRoot is assigned based on the OID.
    *
    * @param fid (in) OID of the Version Buffer DB file of interest
    * @param fileName (out) the name of the pertinent file that was found
    *
    * @return returns NO_ERROR if success; ERR_FILE_NOT_EXIST if file not found
    */
    int                 getVBFileName( FID fid, char* fileName ) const;

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
    int                 getFileName( FID fid, char* fileName,
                            uint16_t dbRoot,
                            uint32_t partition,
                            uint16_t segment ) const;

    /**
     * @brief Construct directory path for the specified fid (OID), DBRoot, and
     * partition number.  Directory does not have to exist, nor is it created.
     */
    int                 getDirName( FID fid, uint16_t dbRoot,
                            uint32_t partition,
                            std::string& dirName) const;

    /**
     * @brief Get the file size
     */
    EXPORT int          getFileSize( IDBDataFile* pFile, long long& fileSize ) const;
    EXPORT int          getFileSize( FID fid, uint16_t dbRoot,
                            uint32_t partition,
                            uint16_t segment,
                            long long& fileSize ) const;

   /**
    * @brief Initialize an extent in a dictionary store file
    * @param pFile (in) IDBDataFile* of dictionary store file to be written to
    * @param dbRoot (in) - DBRoot of pFile
    * @param nBlocks (in) - number of blocks to be written for an extent
    * @param blockHdrInit(in) - data used to initialize each block header
    * @param blockHdrInitSize(in) - number of bytes in blockHdrInit
    * @param bExpandExtent (in) -  Expand existing extent, or initialize new one
    */
    EXPORT int          initDctnryExtent( IDBDataFile*    pFile,
                            uint16_t dbRoot,
                            int      nBlocks,
                            unsigned char* blockHdrInit,
                            int      blockHdrInitSize,
                            bool     bExpandExtent );

   /**
    * @brief Check whether it is an directory
    */
    EXPORT bool         isDir( const char* dirName ) const;

   /**
    * @brief See if there is room in the file system for specific number of blks
    * @param fileName Name of file to extend (does not have to be full name)
    * @param nBlocks Number of 8192-byte blocks to be added
    * @return returns TRUE if file system has room for 'nBlocks', else FALSE
    */
    EXPORT bool         isDiskSpaceAvail(const std::string& fileName,
                            int nBlocks) const;

   /**
    * @brief Convert an oid to a full file name
    */
    EXPORT int          oid2FileName( FID fid, char* fullFileName,
                            bool bCreateDir, uint16_t dbRoot,
                            uint32_t partition, uint16_t segment ) const;
    EXPORT int          oid2DirName( FID fid, char* oidDirName ) const;

   /**
    * @brief Open a file using a filename.
    * @param fileName Name of the file to open.
    * @param mode Mode to use in opening the file (ex: "r+b").
    * @param ioBuffSize Buffer size to be employed by setvbuf().
    * @return returns the IDBDataFile* of the opened file.
    */
    EXPORT IDBDataFile*     openFile( const char* fileName,
                            const char* mode = "r+b",
                            int ioColSize = DEFAULT_COLSIZ,
                            bool useTmpSuffix = false) const;

   /**
    * @brief Open a file using an OID, dbroot, partition, and segment number.
    * @param fid OID of the file to be opened.
    * @param dbRoot DBRoot of the file to be opened.
    * @param partition Partition number of the file to be opened.
    * @param segment Segment number of the file to be opened.
    * @param mode Mode to use in opening the file (default of "r+b" will open
    *             an existing binary file as read/write.
    * @param ioBuffSize Buffer size to be employed by setvbuf().
    * @return returns the IDBDataFile* of the opened file.
    */
    EXPORT IDBDataFile*     openFile( FID fid,
                            uint16_t       dbRoot,
                            uint32_t       partition,
                            uint16_t       segment,
                            std::string&   segFile,
                            const char*    mode = "r+b",
                            int ioColSize = DEFAULT_COLSIZ,
                            bool useTmpSuffix = false) const;

   /**
    * @brief Read to a buffer from a file at current location
    */
    EXPORT int          readFile( IDBDataFile* pFile, unsigned char* readBuf,
                            int readSize ) const;

   /**
    * @brief Reads in 2 compression header blocks from a column segment file.
    * IDBDataFile* points to start of data when function returns.
    * @param pFile (in) IDBDataFile* of column segment file to be read.
    * @param hdrs (out) Contents of headers that are read.
    */
    EXPORT int          readHeaders( IDBDataFile* pFile, char* hdrs ) const;
    EXPORT int          readHeaders( IDBDataFile* pFile, char* hdr1, char* hdr2 )const;

   /**
    * @brief Reinitialize a partial extent in a column segment file
    * @param pFile (in) IDBDataFile* of column segment file to be written to
    * @param startOffset (in) - file offset where blocks are to be written
    * @param nBlocks (in) - number of blocks to be written to the extent
    * @param emptyVal(in) - empty value to be used for column data values
    * width (in) - width of the applicable column
    */
    EXPORT int          reInitPartialColumnExtent( IDBDataFile* pFile,
                            long long startOffset,
                            int       nBlocks,
                            uint64_t  emptyVal,
                            int       width );

   /**
    * @brief Reinitialize an extent in a dictionary store file
    * @param pFile (in) IDBDataFile* of dictionary store file to be written to
    * @param startOffset (in) - file offset where blocks are to be written
    * @param nBlocks (in) - number of blocks to be written to the extent
    * @param blockHdrInit(in) - data used to initialize each block header
    * @param blockHdrInitSize(in) - number of bytes in blockHdrInit
    */
    EXPORT int          reInitPartialDctnryExtent( IDBDataFile* pFile,
                            long long      startOffset,
                            int            nBlocks,
                            unsigned char* blockHdrInit,
                            int            blockHdrInitSize );

   /**
    * @brief Set the file to specified location based on the offset
    */
    EXPORT int          setFileOffset( IDBDataFile* pFile,
                            long long offset,
                            int origin = SEEK_SET  ) const;
    EXPORT int          setFileOffsetBlock( IDBDataFile* pFile,
                            uint64_t lbid,
                            int origin = SEEK_SET ) const;

   /**
    * @brief Truncate the file to the specified file size
    */
    EXPORT int          truncateFile( IDBDataFile* pFile,
                            long long fileSize ) const;

   /**
    * @brief Write a buffer to a file at current location
    */
    EXPORT int          writeFile( IDBDataFile* pFile,
                            const unsigned char* buf, int bufSize ) const;

   /**
    * @brief set the flag to use the instance to access the brm wrapper class
    */ 
    EXPORT virtual void        setTransId( const TxnID& transId);
	EXPORT virtual void        setBulkFlag(bool isBulkLoad);
	EXPORT virtual void 	   setFixFlag(bool isFix);
    TxnID               getTransId() const;

    void                compressionType(int t);
    int                 compressionType() const;

    EXPORT virtual int  flushFile(int rc, std::map<FID,FID> & oids);

protected:
    EXPORT virtual int         updateColumnExtent(IDBDataFile* pFile, int nBlocks);
    EXPORT virtual int         updateDctnryExtent(IDBDataFile* pFile, int nBlocks);

    int                 m_compressionType;  // compresssion type

private:
   //not copyable
    FileOp(const FileOp& rhs);
    FileOp& operator=(const FileOp& rhs);

    int                 createFile( const char* fileName, int fileSize, 
                            uint64_t emptyVal, int width,
                            uint16_t dbRoot );

    int                 expandAbbrevColumnChunk( IDBDataFile* pFile,
                            uint64_t   emptyVal,
                            int   colWidth,
                            const compress::CompChunkPtr& chunkInPtr,
                            compress::CompChunkPtr& chunkOutPt);

    int                 initAbbrevCompColumnExtent( IDBDataFile* pFile,
                            uint16_t dbRoot,
                            int      nBlocks,
                            uint64_t      emptyVal,
                            int      width);

    // Initialize an extent in a column segment file
    // pFile (in) IDBDataFile* of column segment file to be written to
    // dbRoot (in) - DBRoot of pFile
    // nBlocks (in) - number of blocks to be written for an extent
    // emptyVal(in) - empty value to be used for column data values
    // width (in) - width of the applicable column
    // bNewFile (in)      -  Adding extent to new file
    // bExpandExtent (in) -  Expand existing extent, or initialize new one
    // bAbbrevExtent (in) -  If adding new extent, is it abbreviated
    int                 initColumnExtent( IDBDataFile*    pFile,
                            uint16_t dbRoot,
                            int      nBlocks,
                            uint64_t emptyVal,
                            int      width,
                            bool     bNewFile,
                            bool     bExpandExtent,
                            bool     bAbbrevExtent );

    static void         initDbRootExtentMutexes();
    static void         removeDbRootExtentMutexes();

    int                 writeInitialCompColumnChunk( IDBDataFile* pFile,
                            int      nBlocksAllocated,
                            int      nRows,
                            uint64_t emptyVal,
                            int      width,
                            char*    hdrs);

    TxnID       m_transId;
	bool 		m_isBulk;
	bool 		m_isFix;

    // protect creation of m_DbRootAddExtentMutexes
    static boost::mutex               m_createDbRootMutexes;

    // Mutexes used to serialize extent creation within each DBRoot
    static std::map<int,boost::mutex*> m_DbRootAddExtentMutexes;

    // protect race condition in creating directories
    static boost::mutex               m_mkdirMutex;

    char*       m_buffer;             // buffer used with setvbuf()
};

//------------------------------------------------------------------------------
// Inline functions
//------------------------------------------------------------------------------
inline void FileOp::compressionType(int t)
{
    m_compressionType = t;
}

inline int FileOp::compressionType() const
{
    return m_compressionType;
}

inline int FileOp::createDir( const char* dirName ) const
{
    return createDir( dirName, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH );
}

inline int FileOp::getVBFileName( FID fid, char* fileName ) const 
{
    uint16_t dbRoot    = 0;
    uint32_t partition = 0;
    uint16_t segment   = 0;

    return oid2FileName( fid, fileName, true, dbRoot, partition, segment ); 
}

inline int FileOp::getFileName( FID fid, char* fileName,
    uint16_t dbRoot,
    uint32_t partition,
    uint16_t segment ) const
{
    return oid2FileName( fid, fileName, false, dbRoot, partition, segment );
}

inline TxnID FileOp::getTransId() const
{
    return m_transId;
}

} //end of namespace

#undef EXPORT

#endif // _WE_FILEOP_H_
