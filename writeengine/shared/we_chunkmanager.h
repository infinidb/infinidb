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

//  $Id: we_chunkmanager.h 4726 2013-08-07 03:38:36Z bwilkinson $


/** @file */

#ifndef CHUNK_MANAGER_H
#define CHUNK_MANAGER_H

#include <cstdio>
#include <map>
#include <list>
#include <string>
#include <boost/scoped_array.hpp>

#include "we_type.h"
#include "we_typeext.h"
#include "we_define.h"
#include "idbcompress.h"
#include "IDBFileSystem.h"

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

#ifdef _MSC_VER
#define WE_COMP_DBG(x) {}
#else
//#define IDB_COMP_DEBUG
#ifdef IDB_COMP_DEBUG
#define WE_COMP_DBG(x) {x}
#else
#define WE_COMP_DBG(x) {}
#endif
#endif

namespace logging
{
// use Logger (not we_log) for now.
class Logger;
}

namespace WriteEngine
{

// forward reference
class FileOp;

const int UNCOMPRESSED_CHUNK_SIZE = compress::IDBCompressInterface::UNCOMPRESSED_INBUF_LEN;
const int COMPRESSED_FILE_HEADER_UNIT = compress::IDBCompressInterface::HDR_BUF_LEN;

// assume UNCOMPRESSED_CHUNK_SIZE > 0xBFFF (49151), 8 * 1024 bytes padding
const int COMPRESSED_CHUNK_SIZE = compress::IDBCompressInterface::maxCompressedSize(UNCOMPRESSED_CHUNK_SIZE) + 64+3 + 8*1024;

const int BLOCKS_IN_CHUNK = UNCOMPRESSED_CHUNK_SIZE / BYTE_PER_BLOCK;
const int MAXOFFSET_PER_CHUNK = 511*BYTE_PER_BLOCK;

// chunk information
typedef int64_t ChunkId;
struct ChunkData
{
    ChunkId         fChunkId;
    unsigned int    fLenUnCompressed;
    char            fBufUnCompressed[UNCOMPRESSED_CHUNK_SIZE];
    bool            fWriteToFile;

    ChunkData(ChunkId id = 0) : fChunkId(id), fLenUnCompressed(0), fWriteToFile(false) {}
    bool operator < (const ChunkData& rhs) const { return fChunkId < rhs.fChunkId; }
};

// compressed DB file header information
struct CompFileHeader
{
    char fHeaderData[COMPRESSED_FILE_HEADER_UNIT * 2];
    char *fControlData;
    char *fPtrSection;
    boost::scoped_array<char> fLongPtrSectData;

    CompFileHeader() :
        fControlData(fHeaderData), fPtrSection(fHeaderData+COMPRESSED_FILE_HEADER_UNIT) {}
};


// unique ID of a DB file
struct FileID
{
    FID      fFid;
    uint32_t fDbRoot;
    uint32_t fPartition;
    uint32_t fSegment;

    FileID(FID f, uint32_t r, uint32_t p, uint32_t s) :
        fFid(f), fDbRoot(r), fPartition(p), fSegment(s) {}

    bool operator < (const FileID& rhs) const
    { return (
        (fFid < rhs.fFid) ||
        (fFid == rhs.fFid && fDbRoot < rhs.fDbRoot) ||
        (fFid == rhs.fFid && fDbRoot == rhs.fDbRoot && fPartition < rhs.fPartition) ||
        (fFid == rhs.fFid && fDbRoot == rhs.fDbRoot && fPartition == rhs.fPartition && fSegment < rhs.fSegment)); }

    bool operator == (const FileID& rhs) const
    { return (
        fFid == rhs.fFid && fDbRoot == rhs.fDbRoot && fPartition == rhs.fPartition && fSegment == rhs.fSegment); }

};


// compressed DB file information
class CompFileData
{
public:
    CompFileData(const FileID& id, const FID& fid, const execplan::CalpontSystemCatalog::ColDataType colDataType, int colWidth) :
       fFileID(id), fFid(fid), fColDataType(colDataType), fColWidth(colWidth), fDctnryCol(false),
       fFilePtr(NULL), fIoBSize(0) {}

    ChunkData* findChunk(int64_t cid) const;

protected:
    FileID          fFileID;
    FID             fFid;
    execplan::CalpontSystemCatalog::ColDataType fColDataType;
    int             fColWidth;
    bool            fDctnryCol;
    IDBDataFile*    fFilePtr;
    std::string     fFileName;
    CompFileHeader  fFileHeader;
    std::list<ChunkData*>   fChunkList;
    boost::scoped_array<char> fIoBuffer;
    size_t          fIoBSize;

    friend class ChunkManager;
};



class ChunkManager
{
public:
    // @brief constructor
    EXPORT ChunkManager();

    // @brief destructor
    EXPORT virtual ~ChunkManager();

    // @brief Retrieve a file pointer in the chunk manager.
    //        for column file
    IDBDataFile* getFilePtr(const Column& column,
                    uint16_t root,
                    uint32_t partition,
                    uint16_t segment,
                    std::string& filename,
                    const char* mode,
                    int size,
                    bool useTmpSuffix) const;

    // @brief Retrieve a file pointer in the chunk manager.
    //        for dictionary file
    IDBDataFile* getFilePtr(const FID& fid,
                    uint16_t root,
                    uint32_t partition,
                    uint16_t segment,
                    std::string& filename,
                    const char* mode,
                    int size,
                    bool useTmpSuffix) const;

    // @brief Create a compressed dictionary file with an appropriate header.
    IDBDataFile* createDctnryFile(const FID& fid,
                    int64_t  width,
                    uint16_t root,
                    uint32_t partition,
                    uint16_t segment,
                    const char* filename,
                    const char* mode,
                    int size);

    // @brief Read a block from pFile at offset fbo.
    //        The data may copied from memory if the chunk it belongs to is already available.
    int  readBlock(IDBDataFile* pFile, unsigned char* readBuf, uint64_t fbo);

    // @brief Save a block to a chunk in pFile.
    //        The block is not written to disk immediately, will be delayed until flush.
    int  saveBlock(IDBDataFile* pFile, const unsigned char* writeBuf, uint64_t fbo);

    // @brief Write all active chunks to disk, and reset all repository.
    EXPORT int  flushChunks(int rc, const std::map<FID, FID> & columOids);

    // @brief Reset all repository without writing anything to disk.
    void cleanUp(const std::map<FID, FID> & columOids);

    // @brief Expand an initial column, not dictionary, extent to a full extent.
    int expandAbbrevColumnExtent(IDBDataFile* pFile, uint64_t emptyVal, int width);

    // @brief Update column extent
    int updateColumnExtent(IDBDataFile* pFile, int addBlockCount);

    // @brief Update dictionary extent
    int updateDctnryExtent(IDBDataFile* pFile, int addBlockCount);

    // @brief Read in n continuous blocks to read buffer.
    //        for backing up blocks to version buffer
    int readBlocks(IDBDataFile* pFile, unsigned char* readBuf, uint64_t fbo, size_t n);

    // @brief Restore the data block at offset fbo from version buffer
    //        for rollback
    int restoreBlock(IDBDataFile* pFile, const unsigned char* writeBuf, uint64_t fbo);

    // @brief Retrieve the total block count of a DB file.
    int getBlockCount(IDBDataFile* pFile);

    // @brief Set FileOp pointer (for compression type, empty value, txnId, etc.)
    void fileOp(FileOp* fileOp);

    // @brief Control the number of active chunks being stored in memory
    void setMaxActiveChunkNum(unsigned int maxActiveChunkNum)
    { fMaxActiveChunkNum = maxActiveChunkNum; }

    // @brief Use this flag to avoid logging and backing up chunks, tmp files.
    void setBulkFlag(bool isBulkLoad)
    { fIsBulkLoad = isBulkLoad; }

    // @brief Use this flag to flush chunk when is full.
    void setIsInsert(bool isInsert) { fIsInsert = isInsert; }
    bool getIsInsert() { return fIsInsert; }

    void setTransId(const TxnID& transId) { fTransId = transId; }

    // @brief bug5504, Use non transactional DML for InfiniDB with HDFS
    EXPORT int startTransaction(const TxnID& transId) const;
    EXPORT int confirmTransaction(const TxnID& transId) const;
    EXPORT int endTransaction(const TxnID& transId, bool success) const;
	// @brief Use this flag to fix bad chunk.
    void setFixFlag(bool isFix)
    { fIsFix = isFix; }

	EXPORT int checkFixLastDictChunk(const FID& fid,
                    uint16_t root,
                    uint32_t partition,
                    uint16_t segment);

protected:
    // @brief Retrieve pointer to a compressed DB file.
    CompFileData* getFileData(const FID& fid,
                    uint16_t root,
                    uint32_t partition,
                    uint16_t segment,
                    std::string& filename,
                    const char* mode,
                    int size,
                    const execplan::CalpontSystemCatalog::ColDataType colDataType,
                    int colWidth,
                    bool useTmpSuffix,
                    bool dictnry = false) const;

    // @brief Retrieve a chunk of pFile from disk.
    int fetchChunkFromFile(IDBDataFile* pFile, int64_t id, ChunkData*& chunkData);

    // @brief Compress a chunk and write it to file.
    int writeChunkToFile(CompFileData* fileData, int64_t id);
    int writeChunkToFile(CompFileData* fileData, ChunkData* chunkData);

    // @brief Write the compressed data to file and log a recover entry.
    int writeCompressedChunk(CompFileData* fileData, int64_t offset, int64_t size);
    inline int writeCompressedChunk_(CompFileData* fileData, int64_t offset);

    // @brief Write the file header to disk.
    int writeHeader(CompFileData* fileData, int ln);
    inline int writeHeader_(CompFileData* fileData, int ptrSecSize);

    // @brief open a compressed DB file.
    int openFile(CompFileData* fileData, const char* mode, int colWidth,
        bool useTmpSuffix, int ln) const;

    // @brief set offset in a compressed DB file from beginning.
    int setFileOffset(IDBDataFile* pFile, const std::string& fileName, off64_t offset, int ln) const;

    // @brief read from a compressed DB file.
    int readFile(IDBDataFile* pFile, const std::string& fileName, void* buf, size_t size, int ln) const;

    // @brief write to a compressed DB file.
    int writeFile(IDBDataFile* pFile, const std::string& fileName, void* buf, size_t size, int ln) const;

    // @brief Close a compressed DB file.
    int closeFile(CompFileData* fileData);

    // @brief Set empty values to a chunk.
    void initializeColumnChunk(char* buf, CompFileData* fileData);
    void initializeDctnryChunk(char* buf, int size);

    // @brief Calculate the header size based on column width.
    int calculateHeaderSize(int width);

    // @brief Moving chunks as a result of expanding a chunk.
    int reallocateChunks(CompFileData* fileData);

    // @brief verify chunks in the file are OK
    int verifyChunksAfterRealloc(CompFileData* fileData);

    // @brief log a message to the syslog
    void logMessage(int code, int level, int lineNum, int fromLine=-1) const;
    void logMessage(const std::string& msg, int level) const;

    // @brief Write a DML recovery log
    int writeLog(TxnID txnId, std::string backUpFileType, std::string filename,
                 std::string &aDMLLogFileName, int64_t size=0, int64_t offset=0) const;

    // @brief remove DML recovery logs
    int removeBackups(TxnID txnId);

    // @brief swap the src file to dest file
    int swapTmpFile(const std::string& src, const std::string& dest);

    // @brief construnct a DML log file name
    int getDMLLogFileName(std::string& aDMLLogFileName, const TxnID& txnId) const;

    mutable std::map<FileID, CompFileData*>     fFileMap;
    mutable std::map<IDBDataFile*, CompFileData*> fFilePtrMap;
    std::list<std::pair<FileID, ChunkData*> >   fActiveChunks;
    unsigned int                                fMaxActiveChunkNum;  // max active chunks per file
    char*                                       fBufCompressed;
    unsigned int                                fLenCompressed;
    unsigned int                                fMaxCompressedBufSize;
    unsigned int                                fUserPaddings;
    bool                                        fIsBulkLoad;
    bool                                        fDropFdCache;
    bool                                        fIsInsert;
    bool                                        fIsHdfs;
    FileOp*                                     fFileOp;
    compress::IDBCompressInterface              fCompressor;
    logging::Logger*                            fSysLogger;
    TxnID                                       fTransId;
    int                                         fLocalModuleId;
    idbdatafile::IDBFileSystem&                 fFs;
	bool 										fIsFix;
	
private:
};

}

#undef EXPORT

#endif  // CHUNK_MANAGER_H

