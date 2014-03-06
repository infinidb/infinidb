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

//  $Id: we_chunkmanager.cpp 4737 2013-08-14 20:45:46Z bwilkinson $

#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <iostream>
#include <cstdio>
#include <ctime>
//#define NDEBUG
#include <cassert>
using namespace std;

#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>

#include "logger.h"
#include "cacheutils.h"

#include "we_chunkmanager.h"

#include "we_macro.h"
#include "we_brm.h"
#include "we_config.h"
#include "we_confirmhdfsdbfile.h"
#include "we_fileop.h"
#include "we_dctnry.h"
#include "we_stats.h"
using namespace execplan;

#include "IDBDataFile.h"
#include "IDBPolicy.h"
using namespace idbdatafile;

namespace
{

// Function to compare 2 ChunkData pointers.
bool chunkDataPtrLessCompare(WriteEngine::ChunkData* p1, WriteEngine::ChunkData* p2)
{
    return (p1->fChunkId) < (p2->fChunkId);
}

}

namespace WriteEngine
{

extern int NUM_BLOCKS_PER_INITIAL_EXTENT; // defined in we_dctnry.cpp
extern WErrorCodes ec;                    // defined in we_log.cpp

//------------------------------------------------------------------------------
// Search for the specified chunk in fChunkList.
//------------------------------------------------------------------------------
ChunkData* CompFileData::findChunk(int64_t id) const
{
    ChunkData* pChunkData = NULL;
    for (list<ChunkData*>::const_iterator lit = fChunkList.begin(); lit != fChunkList.end(); ++lit)
    {
        if ((*lit)->fChunkId == id)
        {
            pChunkData = *lit;
            break;
        }
    }

    return pChunkData;
}

//------------------------------------------------------------------------------
// ChunkManager constructor
//------------------------------------------------------------------------------
ChunkManager::ChunkManager() : fMaxActiveChunkNum(100), fLenCompressed(0), fIsBulkLoad(false),
                               fDropFdCache(false), fIsInsert(false), fIsHdfs(IDBPolicy::useHdfs()),
                               fFileOp(0), fSysLogger(NULL), fTransId(-1),
                               fLocalModuleId(Config::getLocalModuleID()),
                               fFs(fIsHdfs ?
                                   IDBFileSystem::getFs(IDBDataFile::HDFS) :
                                   IDBFileSystem::getFs(IDBDataFile::BUFFERED))
{
    fUserPaddings = Config::getNumCompressedPadBlks() * BYTE_PER_BLOCK;
    fCompressor.numUserPaddingBytes(fUserPaddings);
    fMaxCompressedBufSize = COMPRESSED_CHUNK_SIZE + fUserPaddings;
    fBufCompressed = new char[fMaxCompressedBufSize];
    fSysLogger = new logging::Logger(SUBSYSTEM_ID_WE);
    logging::MsgMap msgMap;
    msgMap[logging::M0080] = logging::Message(logging::M0080);
    fSysLogger->msgMap( msgMap );
}

//------------------------------------------------------------------------------
// ChunkManager destructor
//------------------------------------------------------------------------------
ChunkManager::~ChunkManager()
{
    std::map<FID,FID> columnOids;
    cleanUp(columnOids);

    delete [] fBufCompressed;
    fBufCompressed = NULL;

    delete fSysLogger;
    fSysLogger = NULL;
}


//------------------------------------------------------------------------------
// Log a message into the DML recovery log.
//------------------------------------------------------------------------------
int ChunkManager::writeLog(TxnID txnId, string backUpFileType, string filename,
                           string& aDMLLogFileName, int64_t size, int64_t offset) const
{
    //Get log file name
    if (getDMLLogFileName(aDMLLogFileName, txnId) != NO_ERROR)
        return ERR_DML_LOG_NAME;

    //Open file
    boost::scoped_ptr<IDBDataFile> aDMLLogFile;
    try
    {
        aDMLLogFile.reset(IDBDataFile::open(
                               IDBPolicy::getType(aDMLLogFileName.c_str(), IDBPolicy::WRITEENG),
                               aDMLLogFileName.c_str(), "a+b", 0));

        if (!aDMLLogFile)
        {
            ostringstream oss;
            oss << "trans " << txnId << ":File " << aDMLLogFileName
                << " can't be opened (no exception thrown)";
            logMessage(oss.str(), logging::LOG_TYPE_ERROR);
            return ERR_OPEN_DML_LOG;
        }
    }
    catch (exception& e)
    {
        ostringstream oss;
        oss << "trans " << txnId << ":File " << aDMLLogFileName
            << " can't be opened: " << e.what();
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_OPEN_DML_LOG;
    }

    //Write the log
    ostringstream entry;
    entry << backUpFileType << '\n' << filename << '\n' << size << '\n' << offset <<'\n';
    string tmp = entry.str();
    aDMLLogFile->seek(0, SEEK_END);
    aDMLLogFile->tell();
    aDMLLogFile->write(tmp.c_str(), tmp.size());

    return NO_ERROR;
}

int ChunkManager::removeBackups(TxnID txnId)
{
    // HDFS update/delete is handled differently
    if (fIsHdfs || fIsBulkLoad)
        return NO_ERROR;

    string aDMLLogFileName;
    if (getDMLLogFileName(aDMLLogFileName, txnId) != NO_ERROR)
        return ERR_DML_LOG_NAME;

    if (IDBPolicy::exists(aDMLLogFileName.c_str()))
    {
        boost::scoped_ptr<IDBDataFile> aDMLLogFile(IDBDataFile::open(
                                            IDBPolicy::getType(aDMLLogFileName.c_str(),
                                            IDBPolicy::WRITEENG),
                                            aDMLLogFileName.c_str(), "r", 0));

        if (aDMLLogFile) //need recover
        {
            ssize_t fileSize = aDMLLogFile->size();
            boost::scoped_array<char> buf(new char[fileSize]);
            if (aDMLLogFile->read(buf.get(), fileSize) != fileSize)
                return ERR_FILE_READ;

            std::istringstream strstream(string(buf.get(), fileSize));
            std::string backUpFileType;
            std::string filename;
            int64_t size;
            int64_t offset;
            while (strstream >> backUpFileType >> filename >> size >> offset)
            {
                if (backUpFileType.compare("tmp") == 0 )
                {
                    filename += ".tmp";
                    IDBPolicy::remove(filename.c_str());
                }
                else
                {
                    std::string backFileName(filename);
                    if (backUpFileType.compare("chk") == 0 )
                        backFileName += ".chk";
                    else
                        backFileName += ".hdr";

                    IDBPolicy::remove(backFileName.c_str());
                }
            }

            aDMLLogFile.reset();  // closes the file in IDBDataFile destructor.

            IDBPolicy::remove(aDMLLogFileName.c_str());
        }
        else
        {
            return ERR_OPEN_DML_LOG;
        }
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Get/Return IDBDataFile* for specified OID, root, partition, and segment.
// Function is to be used to open column files.
// If the IDBDataFile* is not found, then a segment file will be opened using the
// mode (mode) and I/O buffer size (size) that is given.  Name of the resulting
// file is returned in filename.
//
// For Bulk HDFS usage:
//  If useTmpSuffix flag is set, then IDBDataFile will use *.tmp for output.
//------------------------------------------------------------------------------
// @bug 5572 - HDFS usage: add *.tmp file backup flag
IDBDataFile* ChunkManager::getFilePtr(const Column& column,
                               uint16_t root,
                               uint32_t partition,
                               uint16_t segment,
                               string& filename,
                               const char* mode,
                               int size,
                               bool useTmpSuffix) const
{
    CompFileData* fileData = getFileData(column.dataFile.fid, root, partition, segment,
        filename, mode, size, column.colDataType, column.colWidth, useTmpSuffix);
    return (fileData ? fileData->fFilePtr : NULL);
}

//------------------------------------------------------------------------------
// Get/Return IDBDataFile* for specified OID, root, partition, and segment.
// Function is to be used to open dictionary store files.
// If the IDBDataFile* is not found, then a segment file will be opened using the
// mode (mode) and I/O buffer size (size) that is given.  Name of the resulting
// file is returned in filename.
//
// For Bulk HDFS usage:
//  If useTmpSuffix flag is set, then IDBDataFile will use *.tmp for output.
//------------------------------------------------------------------------------
// @bug 5572 - HDFS usage: add *.tmp file backup flag
IDBDataFile* ChunkManager::getFilePtr(const FID& fid,
                               uint16_t root,
                               uint32_t partition,
                               uint16_t segment,
                               string& filename,
                               const char* mode,
                               int size,
                               bool useTmpSuffix) const
{
    CompFileData* fileData = getFileData(fid, root, partition, segment, filename, mode, size,
        CalpontSystemCatalog::VARCHAR, 8, useTmpSuffix, true);  // hard code (varchar, 8) are dummy values for dictionary file
    return (fileData ? fileData->fFilePtr : NULL);
}

//------------------------------------------------------------------------------
// Get/Return CompFileData* for specified column OID, root, partition, and
// segment.  If the IDBDataFile* is not found, then a segment file will be opened
// using the mode (mode) and I/O buffer size (size) that is given.  Name of
// the resulting file is returned in filename.
// If the CompFileData* needs to be created, it will also be created and
// inserted into the fFileMap and fFilePtrMap for later use.
//
// For Bulk HDFS usage:
//  If useTmpSuffix flag is set, then IDBDataFile will use *.tmp for output.
//------------------------------------------------------------------------------
// @bug 5572 - HDFS usage: add *.tmp file backup flag
CompFileData* ChunkManager::getFileData(const FID& fid,
                                        uint16_t root,
                                        uint32_t partition,
                                        uint16_t segment,
                                        string& filename,
                                        const char* mode,
                                        int size,
                                        const CalpontSystemCatalog::ColDataType colDataType,
                                        int colWidth,
                                        bool useTmpSuffix,
                                        bool dctnry) const
{
    FileID fileID(fid, root, partition, segment);
    map<FileID, CompFileData*>::const_iterator mit = fFileMap.find(fileID);

    WE_COMP_DBG(cout << "getFileData: fid:" << fid << " root:" << root << " part:" << partition
                    << " seg:" << segment << " file* " << ((mit != fFileMap.end()) ? "" : "not ")
                    << "found." << endl;)

    // Get CompFileData pointer for existing Column or Dictionary store file
    if (mit != fFileMap.end())
    {
        filename = mit->second->fFileName;
        return mit->second;
    }

    // New CompFileData pointer needs to be created
    char name[FILE_NAME_SIZE];
    if (fFileOp->getFileName(fid, name, root, partition, segment) != NO_ERROR)
        return NULL;

    CompFileData* fileData = new CompFileData(fileID, fid, colDataType, colWidth);
    fileData->fFileName = filename = name;
    if (openFile(fileData, mode, colWidth, useTmpSuffix, __LINE__) != NO_ERROR)
    {
        WE_COMP_DBG(cout << "Failed to open " << fileData->fFileName << " ." << endl;)
        delete fileData;
        return NULL;
    }
    fileData->fIoBuffer.reset(new char[size]);
    fileData->fIoBSize = size;
// TODO-There is no current way to make this setvbuf call as IDBDataFile only
// accepts the USE_VBUF at construction time and then uses a buffer that it manages
// Can either propagate an option through the openFile() call above and let
// IDBDataFile manage it internally or expose a new setBuffer() option.
//  setvbuf(fileData->fFilePtr, fileData->fIoBuffer.get(), _IOFBF, size);
    fileData->fDctnryCol = dctnry;
    WE_COMP_DBG(cout << "open file* " << name << endl;)
    // get the control data in header.
    if (readFile(fileData->fFilePtr, fileData->fFileName, fileData->fFileHeader.fControlData,
                        COMPRESSED_FILE_HEADER_UNIT, __LINE__) != NO_ERROR)
    {
        WE_COMP_DBG(cout << "Failed to read control header." << endl;)
        delete fileData;
        return NULL;
    }

    // make sure the header is valid
    if (fCompressor.verifyHdr(fileData->fFileHeader.fControlData) != 0)
    {
        WE_COMP_DBG(cout << "Invalid header." << endl;)
        delete fileData;
        return NULL;
    }

    int headerSize = fCompressor.getHdrSize(fileData->fFileHeader.fControlData);
    int ptrSecSize = headerSize - COMPRESSED_FILE_HEADER_UNIT;
    if (ptrSecSize > COMPRESSED_FILE_HEADER_UNIT)
    {
        // >8K header, dictionary width > 128
        fileData->fFileHeader.fPtrSection = new char[ptrSecSize];
        fileData->fFileHeader.fLongPtrSectData.reset(fileData->fFileHeader.fPtrSection);
    }

    // read in the pointer section in header
    if (readFile(fileData->fFilePtr, fileData->fFileName, fileData->fFileHeader.fPtrSection,
                        ptrSecSize, __LINE__) != NO_ERROR)
    {
        WE_COMP_DBG(cout << "Failed to read pointer header." << endl;)
        delete fileData;
        return NULL;
    }
    fFileMap.insert(make_pair(fileID, fileData));
    //cout << "Insert into fFilemap root:partition:seg:fileID = " <<root<<":"<< partition<<":"<< segment<<":"<<fid<<endl;
    fFilePtrMap.insert(make_pair(fileData->fFilePtr, fileData));
    return fileData;
}

//------------------------------------------------------------------------------
// Return new IDBDataFile* for specified dictionary OID, root, partition, segment, and
// width.  A new segment file will be opened using the mode (mode) and I/O
// buffer size (size) that is given.  Name of the resulting file is returned
// in filename.
// A corresponding CompFileData* is created and inserted into fFileMap and
// fFilePtrMap for later use.
//------------------------------------------------------------------------------
IDBDataFile* ChunkManager::createDctnryFile(const FID& fid,
                                     int64_t width,
                                     uint16_t root,
                                     uint32_t partition,
                                     uint16_t segment,
                                     const char* filename,
                                     const char* mode,
                                     int size)
{
    FileID fileID(fid, root, partition, segment);
    CompFileData* fileData = new CompFileData(fileID, fid, CalpontSystemCatalog::VARCHAR, width);
    fileData->fFileName = filename;
    if (openFile(fileData, mode, width, false, __LINE__) != NO_ERROR) // @bug 5572 HDFS tmp file
    {
        WE_COMP_DBG(cout << "Failed to open " << fileData->fFileName << " ." << endl;)
        delete fileData;
        return NULL;
    }
    fileData->fIoBuffer.reset(new char[size]);
    fileData->fIoBSize = size;
//  see TODO- comment above
//  setvbuf(fileData->fFilePtr, fileData->fIoBuffer.get(), _IOFBF, size);
    fileData->fDctnryCol = true;
    WE_COMP_DBG(cout << "create file* " << filename << endl;)
    int hdrSize = calculateHeaderSize(width);
    int ptrSecSize = hdrSize - COMPRESSED_FILE_HEADER_UNIT;
    if (ptrSecSize > COMPRESSED_FILE_HEADER_UNIT)
    {
        // >8K header, dictionary width > 128
        fileData->fFileHeader.fPtrSection = new char[ptrSecSize];
        fileData->fFileHeader.fLongPtrSectData.reset(fileData->fFileHeader.fPtrSection);
    }

    fCompressor.initHdr(fileData->fFileHeader.fControlData, fileData->fFileHeader.fPtrSection,
                        fFileOp->compressionType(), hdrSize);
    if (writeHeader(fileData, __LINE__) != NO_ERROR)
    {
        WE_COMP_DBG(cout << "Failed to write header." << endl;)
        delete fileData;
        return NULL;
    }
    //@Bug 4977 remove log file
    removeBackups(fTransId);
    fFileMap.insert(make_pair(fileID, fileData));
    fFilePtrMap.insert(make_pair(fileData->fFilePtr, fileData));
    return fileData->fFilePtr;
}

//------------------------------------------------------------------------------
// Read the block for the specified fbo, from pFile's applicable chunk, and
// into readBuf.
//------------------------------------------------------------------------------
int ChunkManager::readBlock(IDBDataFile* pFile, unsigned char* readBuf, uint64_t fbo)
{
    map<IDBDataFile*, CompFileData*>::iterator fpIt = fFilePtrMap.find(pFile);
    if (fpIt == fFilePtrMap.end())
    {
        logMessage(ERR_COMP_FILE_NOT_FOUND, logging::LOG_TYPE_ERROR, __LINE__);
        return ERR_COMP_FILE_NOT_FOUND;
    }

    // find the chunk ID and offset in the chunk
    lldiv_t offset = lldiv(fbo * BYTE_PER_BLOCK, UNCOMPRESSED_CHUNK_SIZE);
    ChunkData* chunkData = (fpIt->second)->findChunk(offset.quot);

    WE_COMP_DBG(cout << "fbo:" << fbo << "  chunk id:" << offset.quot << " offset:" << offset.rem
                    << "  chunkData*:" << chunkData << endl;)

    int rc = NO_ERROR;
    // chunk is not already uncompressed
    if (chunkData == NULL)
        rc = fetchChunkFromFile(pFile, offset.quot, chunkData);

    if (rc == NO_ERROR)
    {
        // copy the data at fbo to readBuf
        memcpy(readBuf, chunkData->fBufUnCompressed + offset.rem, BYTE_PER_BLOCK);
    }

    return rc;
}

//------------------------------------------------------------------------------
// Write writeBuf to the block for the specified fbo, within pFile's applicable
// chunk.
//------------------------------------------------------------------------------
int ChunkManager::saveBlock(IDBDataFile* pFile, const unsigned char* writeBuf, uint64_t fbo)
{
    WE_COMP_DBG(cout << "save block fbo:" << fbo << endl;)
    map<IDBDataFile*, CompFileData*>::iterator fpIt = fFilePtrMap.find(pFile);
    if (fpIt == fFilePtrMap.end())
    {
        logMessage(ERR_COMP_FILE_NOT_FOUND, logging::LOG_TYPE_ERROR, __LINE__);
        return ERR_COMP_FILE_NOT_FOUND;
    }

    // find the chunk ID and offset in the chunk
    lldiv_t offset = lldiv(fbo * BYTE_PER_BLOCK, UNCOMPRESSED_CHUNK_SIZE);
    ChunkData* chunkData = (fpIt->second)->findChunk(offset.quot);

    int rc = NO_ERROR;
    // chunk is not already read in
    if ((chunkData == NULL) && ((rc = fetchChunkFromFile(pFile, offset.quot, chunkData)) != NO_ERROR))
        return rc;

    WE_COMP_DBG(cout << "fbo:" << fbo << "  chunk id:" << offset.quot << " offset:" << offset.rem
                    << " saved @" << (&(chunkData->fBufUnCompressed) + offset.rem) << endl;)

    memcpy(chunkData->fBufUnCompressed + offset.rem, writeBuf, BYTE_PER_BLOCK);
    chunkData->fWriteToFile = true;
// if the chunk is full for insert, flush it
//cout << "current offset.rem/8192 = " << offset.rem/8192 << endl;
    if (fIsInsert && (offset.rem == MAXOFFSET_PER_CHUNK))
    {
        if (((rc = writeChunkToFile(fpIt->second, chunkData)) == NO_ERROR) &&
            ((rc = writeHeader(fpIt->second, __LINE__)) == NO_ERROR))
        {
            //cout << "saveblock flushed the full chunk"<<endl;
            pFile->flush();

            //@Bug 4977 remove log file
            removeBackups(fTransId);
        }
    }

    return rc;
}

//------------------------------------------------------------------------------
// Flush all pending chunks to their corresponding segment files.
//------------------------------------------------------------------------------
int ChunkManager::flushChunks(int rc, const std::map<FID, FID> & columOids)
{
    // shall fail the the statement if failed here
    WE_COMP_DBG(cout << "flushChunks." << endl;)

    int k = fFilePtrMap.size();
    std::map<FID,FID>::const_iterator it;
    if ((rc == NO_ERROR) && fIsInsert)
    {
        while (k-- > 0 && rc == NO_ERROR)
        {
            map<IDBDataFile*, CompFileData*>::iterator i = fFilePtrMap.begin();
            // sort the chunk list first
            CompFileData* fileData = i->second;
            it = columOids.find (fileData->fFid);
            if (it != columOids.end())
            {
                list<ChunkData*>& chunkList = fileData->fChunkList;
                chunkList.sort(chunkDataPtrLessCompare);
                list<ChunkData*>::iterator j = chunkList.begin();
                while (j != chunkList.end())
                {
                    if ((rc = writeChunkToFile(fileData, *j)) != NO_ERROR)
                        break;

                    // write chunk to file removes the written chunk from the list
                    j = chunkList.begin();
                }

                if (rc != NO_ERROR)
                    break;

                // finally update the header
                if ((rc = writeHeader(fileData, __LINE__)) != NO_ERROR)
                    break;

                //@Bug 4977 remove log file
                removeBackups(fTransId);

                // closeFile invalidates the iterator
                closeFile(fileData);
            }
        }
    }
    else if (rc == NO_ERROR)
    {
        while (k-- > 0 && rc == NO_ERROR)
        {
            map<IDBDataFile*, CompFileData*>::iterator i = fFilePtrMap.begin();
            // sort the chunk list first
            CompFileData* fileData = i->second;

            list<ChunkData*>& chunkList = fileData->fChunkList;
            chunkList.sort(chunkDataPtrLessCompare);
            list<ChunkData*>::iterator j = chunkList.begin();
            while (j != chunkList.end())
            {
                if ((rc = writeChunkToFile(fileData, *j)) != NO_ERROR)
                    break;
                // write chunk to file removes the written chunk from the list
                j = chunkList.begin();
            }

            if (rc != NO_ERROR)
                break;

            // finally update the header
            if ((rc = writeHeader(fileData, __LINE__)) != NO_ERROR)
                break;

            //@Bug 4977 remove log file
            removeBackups(fTransId);

            // closeFile invalidates the iterator
            closeFile(fileData);
        }
    }

    if (rc != NO_ERROR)
    {
        cleanUp(columOids);
        return rc;
    }

    //fActiveChunks.clear();
    //fFileMap.clear();
    //fFilePtrMap.clear();

    if (fDropFdCache)
    {
        cacheutils::dropPrimProcFdCache();
        fDropFdCache = false;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Load and uncompress the requested chunk (id) for the specified file (pFile),
// into chunkData.
// id is: (fbo*BYTE_PER_BLOCK)/UNCOMPRESSED_CHUNK_SIZE
// If the active chunk list is already full, then we flush the oldest pending
// chunk to disk, to make room for fetching the requested chunk.
// If the header ptr for the requested chunk is 0 (or has length 0), then
// chunkData is initialized with a new empty chunk.
//------------------------------------------------------------------------------
int ChunkManager::fetchChunkFromFile(IDBDataFile* pFile, int64_t id, ChunkData*& chunkData)
{
    // return value
    int rc = NO_ERROR;

    // remove the oldest one if the max active chunk number is reached.
    WE_COMP_DBG(cout << "fActiveChunks.size:" << fActiveChunks.size() << endl;)
    //cout << "fetchChunkFromFile1: pFile = " << pFile << endl;
    map<IDBDataFile*, CompFileData*>::iterator fpIt = fFilePtrMap.find(pFile);
    if (fpIt == fFilePtrMap.end())
    {
        logMessage(ERR_COMP_FILE_NOT_FOUND, logging::LOG_TYPE_ERROR, __LINE__);
        return ERR_COMP_FILE_NOT_FOUND;
    }

    CompFileData* fileData = fpIt->second;

    if (fActiveChunks.size() >= fMaxActiveChunkNum)
    {
        list<std::pair<FileID, ChunkData*> >::iterator lIt = fActiveChunks.begin();
        if (!fIsBulkLoad && !(fpIt->second->fDctnryCol))
        {
            while ((lIt->first == fpIt->second->fFileID) && (lIt != fActiveChunks.end()))
                lIt++;
        }

        if (lIt != fActiveChunks.end())
        {
            map<FileID, CompFileData*>::iterator fIt = fFileMap.find(lIt->first);
            if (fIt == fFileMap.end())
            {
                logMessage(ERR_COMP_FILE_NOT_FOUND, logging::LOG_TYPE_ERROR, __LINE__);
                return ERR_COMP_FILE_NOT_FOUND;
            }

            if ((rc = writeChunkToFile(fIt->second, lIt->second)) != NO_ERROR)
            {
                ostringstream oss;
                oss << "write inactive chunk to file failed:" << fIt->second->fFileName << "@"
                    << __LINE__;
                logMessage(oss.str(), logging::LOG_TYPE_ERROR);
                return rc;
            }

            if ((rc = writeHeader(fIt->second, __LINE__)) != NO_ERROR)
            {
                // logged by writeHeader
                return rc;
            }

            //@Bug 4977 remove the log files
            removeBackups(fTransId);
        }
    }

#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_COMPRESS_DCT_INIT_BUF);
#endif
    // get a new ChunkData object
    chunkData = new ChunkData(id);
    pFile = fileData->fFilePtr; //update to get the reopened file ptr.
    fileData->fChunkList.push_back(chunkData);
    fActiveChunks.push_back(make_pair(fileData->fFileID, chunkData));

    // read the compressed chunk from file
    uint64_t* ptrs = reinterpret_cast<uint64_t*>(fileData->fFileHeader.fPtrSection);
    if (ptrs[id] && ptrs[id+1]) // compressed chunk data exists
    {
        // safety check
        if (ptrs[id] >= ptrs[id+1])
        {
            logMessage(ERR_COMP_WRONG_PTR, logging::LOG_TYPE_ERROR, __LINE__);
            return ERR_COMP_WRONG_PTR;
       }

        unsigned int chunkSize = (ptrs[id+1] - ptrs[id]);
        if ((rc = setFileOffset(pFile, fileData->fFileName, ptrs[id], __LINE__)) != NO_ERROR ||
            (rc = readFile(pFile, fileData->fFileName, fBufCompressed, chunkSize, __LINE__)) !=
             NO_ERROR)
        {
            // logged by setFileOffset/readFile
            return rc;
        }

        // uncompress the read in buffer
        unsigned int dataLen = sizeof(chunkData->fBufUnCompressed);
        if (fCompressor.uncompressBlock((char*)fBufCompressed, chunkSize,
                    (unsigned char*)chunkData->fBufUnCompressed, dataLen) != 0)
        {
			if (fIsFix)
			{
				uint64_t blocks = 512;
				if (id == 0)
				{
					char* hdr = fileData->fFileHeader.fControlData;
					if (fCompressor.getBlockCount(hdr) < 512)
						blocks = 256;
				}
				
				dataLen = 8192 * blocks;
				
				// load the uncompressed buffer with empty values.
				char* buf = chunkData->fBufUnCompressed;
				chunkData->fLenUnCompressed = UNCOMPRESSED_CHUNK_SIZE;
				if (fileData->fDctnryCol)
					initializeDctnryChunk(buf, UNCOMPRESSED_CHUNK_SIZE);
				else
					initializeColumnChunk(buf, fileData);					
			}
			else
			{
				logMessage(ERR_COMP_UNCOMPRESS, logging::LOG_TYPE_ERROR, __LINE__);
				return ERR_COMP_UNCOMPRESS;
			}
        }

//@bug 3313-Remove validation that incorrectly fails for long string store files
//      WE_COMP_DBG(cout << "chunk uncompressed to " << dataLen << endl;)
//      if (dataLen < (id+1) * BYTE_PER_BLOCK)
//      {
//          logMessage(ERR_COMP_UNCOMPRESS, logging::LOG_TYPE_ERROR, __LINE__);
//          return ERR_COMP_UNCOMPRESS;
//      }

        chunkData->fLenUnCompressed = dataLen;
    }
    else  // new chunk
    {
        if (id == 0 && ptrs[id] == 0) // if the 1st ptr is not set for new extent
        {
            ptrs[0] = fCompressor.getHdrSize(fileData->fFileHeader.fControlData);
        }

        // load the uncompressed buffer with empty values.
        char* buf = chunkData->fBufUnCompressed;
        chunkData->fLenUnCompressed = UNCOMPRESSED_CHUNK_SIZE;
        if (fileData->fDctnryCol)
            initializeDctnryChunk(buf, UNCOMPRESSED_CHUNK_SIZE);
        else
            initializeColumnChunk(buf, fileData);
    }
#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_COMPRESS_DCT_INIT_BUF);
#endif

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Initialize a column based chunk with the applicable empty values.
//------------------------------------------------------------------------------
void ChunkManager::initializeColumnChunk(char* buf, CompFileData* fileData)
{
    int size = UNCOMPRESSED_CHUNK_SIZE;
    uint64_t emptyVal = fFileOp->getEmptyRowValue(fileData->fColDataType, fileData->fColWidth);
    fFileOp->setEmptyBuf((unsigned char*)buf, size, emptyVal, fileData->fColWidth);
}

//------------------------------------------------------------------------------
// Initialize a dictionary based chunk with empty blocks.
//------------------------------------------------------------------------------
void ChunkManager::initializeDctnryChunk(char* buf, int size)
{
    Dctnry* dctnry = dynamic_cast<Dctnry*>(fFileOp);
    memset(buf, 0, size);
    char* end = buf + size;
    while (buf < end)
    {
        dctnry->copyDctnryHeader(buf);
        buf += BYTE_PER_BLOCK;
    }
}

//------------------------------------------------------------------------------
// Compress and write the requested chunk (id) for fileData, to disk.
// id is: (fbo*BYTE_PER_BLOCK)/UNCOMPRESSED_CHUNK_SIZE
//------------------------------------------------------------------------------
int ChunkManager::writeChunkToFile(CompFileData* fileData, int64_t id)
{
    ChunkData* chunkData = fileData->findChunk(id);
    if (!chunkData)
    {
        logMessage(ERR_COMP_CHUNK_NOT_FOUND, logging::LOG_TYPE_ERROR, __LINE__);
        return ERR_COMP_CHUNK_NOT_FOUND;
    }

    return writeChunkToFile(fileData, chunkData);
}

//------------------------------------------------------------------------------
// Compress and write the given chunk for fileData to disk.
// If the chunk has been flagged for writing (fWriteToFile is true),
// then subsequent chunks in the file are shifted down as needed, if the com-
// pressed chunk will not fit in the currently available embedded free space.
//------------------------------------------------------------------------------
int ChunkManager::writeChunkToFile(CompFileData* fileData, ChunkData* chunkData)
{
    WE_COMP_DBG(cout << "write chunk id=" << chunkData->fChunkId << " data "
                     << ((chunkData->fWriteToFile) ? "changed" : "NOT changed") << endl;)

    int rc = NO_ERROR; // return value
    bool needReallocateChunks = false;
    int64_t spaceAvl = 0;
    if (chunkData->fWriteToFile)
    {
#ifdef PROFILE
        Stats::startParseEvent(WE_STATS_COMPRESS_DCT_COMPRESS);
#endif
        // compress the chunk before writing it to file
        fLenCompressed = fMaxCompressedBufSize;
        if (fCompressor.compressBlock((char*)chunkData->fBufUnCompressed,
                                        chunkData->fLenUnCompressed,
                                        (unsigned char*)fBufCompressed,
                                        fLenCompressed) != 0)
        {
            logMessage(ERR_COMP_COMPRESS, logging::LOG_TYPE_ERROR, __LINE__);
            return ERR_COMP_COMPRESS;
        }
        WE_COMP_DBG(cout << "Chunk compressed from " << chunkData->fLenUnCompressed << " to "
                        << fLenCompressed;)

        // Removed padding code here, will add padding for the last chunk.
        // The existing chunks are already correctly aligned, use the padding to absort chunk
        // size increase when update.  This improves the performance with less chunk shifting.

#ifdef PROFILE
        Stats::stopParseEvent(WE_STATS_COMPRESS_DCT_COMPRESS);
#endif

        // need more work if the new compressed buffer is larger
        uint64_t* ptrs = reinterpret_cast<uint64_t*>(fileData->fFileHeader.fPtrSection);
        ChunkId chunkId = chunkData->fChunkId;
        if (ptrs[chunkId+1] > 0)
             spaceAvl = (ptrs[chunkId+1] - ptrs[chunkId]);
        WE_COMP_DBG(cout << ", available space:" << spaceAvl;)

        bool lastChunk = true;
        // usable chunkIds are 0 .. POINTERS_IN_HEADER-2
        // [chunkId+0] is the start offset of current chunk.
        // [chunkId+1] is the start offset of next chunk, the offset diff is current chunk size.
        // [chunkId+2] is 0 or not indicates if the next chunk exists.
        int headerSize = fCompressor.getHdrSize(fileData->fFileHeader.fControlData);
        int ptrSecSize = headerSize - COMPRESSED_FILE_HEADER_UNIT;
        int64_t usablePtrIds = (ptrSecSize / sizeof(uint64_t)) - 2;
        if (chunkId < usablePtrIds) // make sure [chunkId+2] has valid value
            lastChunk = (ptrs[(chunkId+2)] == 0);
        WE_COMP_DBG(cout << ", last chunk:" << (lastChunk?"true":"false") << endl;)

        if (spaceAvl < 0)
        {
            logMessage(ERR_COMP_WRONG_PTR, logging::LOG_TYPE_ERROR, __LINE__);
            return ERR_COMP_WRONG_PTR;
        }

        if ((int64_t)fLenCompressed <= spaceAvl)
        {
            // There is enough sapce.
            if ((rc = writeCompressedChunk(fileData, ptrs[chunkId], spaceAvl)) != NO_ERROR)
            {
                // log in writeCompressedChunk by setFileOffset and writeFile
                return rc;
            }
        }
        else if (lastChunk)
        {
            // add padding space if the chunk is written first time
            if (fCompressor.padCompressedChunks(
                    (unsigned char*)fBufCompressed, fLenCompressed, fMaxCompressedBufSize) != 0)
            {
                WE_COMP_DBG(cout << "Last chunk:" << chunkId << ", padding failed." << endl;)

                logMessage(ERR_COMP_PAD_DATA, logging::LOG_TYPE_ERROR, __LINE__);
                return ERR_COMP_PAD_DATA;
            }
            WE_COMP_DBG(cout << "Last chunk:" << chunkId << ", padded to " << fLenCompressed;)

            // This is the last chunk, safe to write any length of data.
            //@Bug 3888. Assign the error code
            if ((rc = writeCompressedChunk(fileData, ptrs[chunkId], spaceAvl)) != NO_ERROR)
            {
                // log in writeCompressedChunk by setFileOffset and writeFile
                return rc;
            }

            // Update the current chunk size.
            ptrs[chunkId+1] = ptrs[chunkId] + fLenCompressed;
        }
        else
        {
            needReallocateChunks = true;
        }
    }

    if (!needReallocateChunks)
    {
        fActiveChunks.remove(make_pair(fileData->fFileID, chunkData));
        fileData->fChunkList.remove(chunkData);
        delete chunkData;
    }
    else
    {
        ostringstream oss;
        oss << "Compressed data does not fit, caused a chunk shifting @line:" << __LINE__
            << " filename:" << fileData->fFileName << ", chunkId:" << chunkData->fChunkId
            << " data size:" << fLenCompressed << "/available:" << spaceAvl << " -- shifting ";
        if ((rc = reallocateChunks(fileData)) == NO_ERROR)
        {
            oss << "SUCCESS";
            logMessage(oss.str(), logging::LOG_TYPE_INFO);
        }
        else
        {
            oss << "FAILED";
            logMessage(oss.str(), logging::LOG_TYPE_CRITICAL);
        }
    }

    return rc;
}

//------------------------------------------------------------------------------
// Write the current compressed data in fBufCompressed to the specified segment
// file offset (offset) and file (fileData).  For DML usage, "size" specifies
// how many bytes to backup, for error recovery.  cpimport.bin does it's own
// backup and error recovery, so "size" is not applicable for bulk import usage.
//------------------------------------------------------------------------------
int ChunkManager::writeCompressedChunk(CompFileData* fileData, int64_t offset, int64_t size)
{
    int rc = NO_ERROR;

    if (!fIsBulkLoad && !fIsHdfs)
    {
        // backup current chunk to chk file
        string chkFileName(fileData->fFileName + ".chk");
        string aDMLLogFileName;
        unsigned char* buf = new unsigned char[size];
        if (((rc = setFileOffset(fileData->fFilePtr, fileData->fFileName, offset, __LINE__)) ==
             NO_ERROR)
            &&
            ((rc = readFile(fileData->fFilePtr, fileData->fFileName, buf, size, __LINE__)) ==
             NO_ERROR))
        {
            IDBDataFile*  chkFilePtr = IDBDataFile::open(
                                            IDBPolicy::getType(chkFileName.c_str(),
                                                IDBPolicy::WRITEENG),
                                            chkFileName.c_str(),
                                            "w+b",
                                            0 );
            if (chkFilePtr)
            {
                rc = writeFile(chkFilePtr, chkFileName, buf, size, __LINE__);
                delete chkFilePtr;
            }
            delete [] buf;

            if (rc != NO_ERROR)
            {
                IDBPolicy::remove(chkFileName.c_str());
                return rc;
            }

            // log the chunk information for recovery
            rc = writeLog(fTransId, "chk", fileData->fFileName, aDMLLogFileName, size, offset);
            if (rc != NO_ERROR)
            {
                ostringstream oss;
                oss << "log " << fileData->fFileName << ".chk to DML logfile failed.";
                logMessage(oss.str(), logging::LOG_TYPE_INFO);
                return rc;
            }
        }

        // write out the compressed data + padding
        if ((rc == NO_ERROR) && ((rc = writeCompressedChunk_(fileData, offset)) == NO_ERROR))
        {
            if ((fileData->fFilePtr)->flush() != 0) //@Bug3162.
            {
                rc = ERR_FILE_WRITE;
                ostringstream oss;
                oss << "Failed to flush " << fileData->fFileName << " @line: " << __LINE__;
                logMessage(oss.str(), logging::LOG_TYPE_ERROR);
            }
        }
    }
    else
    {
        // write out the compressed data + padding
        rc = writeCompressedChunk_(fileData, offset);
    }

    return rc;
}

//------------------------------------------------------------------------------
// Actually write the current compressed data in fBufCompressed to the specified
// segment file offset (offset) and file (fileData)
//------------------------------------------------------------------------------
inline int ChunkManager::writeCompressedChunk_(CompFileData* fileData, int64_t offset)
{
    int rc = setFileOffset(fileData->fFilePtr, fileData->fFileName, offset, __LINE__);
    if (rc != NO_ERROR)
        return rc;

    return writeFile(fileData->fFilePtr, fileData->fFileName,
                                  fBufCompressed, fLenCompressed, __LINE__);
}

//------------------------------------------------------------------------------
// Open the specified segment file (fileData) using the given mode.
// ln is the source code line number of the code invoking this operation
// (ex __LINE__); this is used for logging error messages.
//
// useTmpSuffix controls whether HDFS file is opened with USE_TMPFILE bit set.
// Typically set for bulk load, single insert, and batch insert, when adding
// rows to an "existing" file.
// Typically always set for DML update and delete.
//
// @bug 5572 - HDFS usage: add *.tmp file backup flag to API
//------------------------------------------------------------------------------
int ChunkManager::openFile(CompFileData* fileData, const char* mode, int colWidth,
    bool useTmpSuffix, int ln) const
{
    int rc = NO_ERROR;
    unsigned opts = IDBDataFile::USE_VBUF;
    if (fIsHdfs)
    {
        if (useTmpSuffix)
        {
            if (!fIsBulkLoad)
            {
                // keep a DML log for confirm or cleanup the .tmp file
                string aDMLLogFileName;
                if ((rc = writeLog(fTransId, "tmp", fileData->fFileName,
                    aDMLLogFileName, 0)) != NO_ERROR)
                {
                    ostringstream oss;
                    oss << "Failed to put " << fileData->fFileName << " into DML log.";
                    logMessage(oss.str(), logging::LOG_TYPE_ERROR);

                    return rc;
                }
            }

            opts |= IDBDataFile::USE_TMPFILE;
        }
    }

    fileData->fFilePtr = IDBDataFile::open(
                            IDBPolicy::getType( fileData->fFileName.c_str(), IDBPolicy::WRITEENG ),
                            fileData->fFileName.c_str(), mode, opts, colWidth);

    if (fileData->fFilePtr == NULL)
    {
        ostringstream oss;
        oss << "Failed to open compressed data file " << fileData->fFileName << " @line: " << ln;
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        rc = ERR_COMP_OPEN_FILE;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Set the file offset for the specified segment file (fileData) using the
// given offset.
// ln is the source code line number of the code invoking this operation
// (ex __LINE__); this is used for logging error messages.  Likewise, filename
// is used for logging any error message.
//------------------------------------------------------------------------------
int ChunkManager::setFileOffset(IDBDataFile* pFile, const string& fileName, off64_t offset, int ln) const
{
    int rc = NO_ERROR;
    if (pFile->seek(offset, SEEK_SET) != 0) rc = ERR_COMP_SET_OFFSET;

    if (rc != NO_ERROR)
    {
        ostringstream oss;
        oss << "Failed to set offset in compressed data file " << fileName
            << " @line: " << ln << " offset:" << offset;
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
    }

    return rc;
}

//------------------------------------------------------------------------------
// Read the requested number of bytes (size) from the specified file pFile.
// ln is the source code line number of the code invoking this operation
// (ex __LINE__); this is used for logging error messages.  Likewise, filename
// is used for logging any error message.
//------------------------------------------------------------------------------
int ChunkManager::readFile(IDBDataFile* pFile, const string& fileName, void* buf, size_t size, int ln) const
{
    size_t bytes = pFile->read(buf, size);
    if (bytes != size)
    {
        ostringstream oss;
        oss << "Failed to read from compressed data file " << fileName
            << " @line: " << ln << " read/expect:" << bytes << "/" << size;
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_COMP_READ_FILE;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Write the requested number of bytes (size) to the specified file pFile.
// ln is the source code line number of the code invoking this operation
// (ex __LINE__); this is used for logging error messages.  Likewise, filename
// is used for logging any error message.
//------------------------------------------------------------------------------
int ChunkManager::writeFile(IDBDataFile* pFile, const string& fileName, void* buf, size_t size, int ln) const
{
    size_t bytes = pFile->write(buf, size);
    if (bytes != size)
    {
        ostringstream oss;
        oss << "Failed to write to compressed data file " << fileName
            << " @line: " << ln << " written/expect:" << bytes << "/" << size;
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_COMP_WRITE_FILE;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Close the specified segment file (fileData), and remove the
// corresponding CompFileData reference from fFileMap and fFilePtrMap.
//------------------------------------------------------------------------------
int ChunkManager::closeFile(CompFileData* fileData)
{
    int rc = NO_ERROR;

    WE_COMP_DBG(cout << "closing file:" << fileData->fFileName << endl;)
    fFileMap.erase(fileData->fFileID);
    fFilePtrMap.erase(fileData->fFilePtr);

    if (fileData->fFilePtr)
        delete fileData->fFilePtr;

    delete fileData;
    fileData = NULL;

    return rc;
}

//------------------------------------------------------------------------------
// Write the chunk pointers headers for the specified file (fileData).
// ln is the source code line number of the code invoking this operation
// (ex __LINE__); this is used for logging error messages.  For DML usage,
// backup for recovery is also performed.  This step is skipped for cpimport.bin
// as bulk import performs its own backup and recovery operations.
//------------------------------------------------------------------------------
int ChunkManager::writeHeader(CompFileData* fileData, int ln)
{
    int rc = NO_ERROR;
    int headerSize = fCompressor.getHdrSize(fileData->fFileHeader.fControlData);
    int ptrSecSize = headerSize - COMPRESSED_FILE_HEADER_UNIT;

    if (!fIsHdfs && !fIsBulkLoad)
    {
        // write a backup header
        string hdrFileName(fileData->fFileName + ".hdr");
        string aDMLLogFileName;
        IDBDataFile*  hdrFilePtr = IDBDataFile::open(
                                        IDBPolicy::getType(hdrFileName.c_str(),
                                                           IDBPolicy::WRITEENG),
                                        hdrFileName.c_str(),
                                        "w+b",
                                        0,
                                        fileData->fColWidth );
        if (hdrFilePtr)
        {
            rc = writeFile(hdrFilePtr, hdrFileName, fileData->fFileHeader.fControlData,
                           COMPRESSED_FILE_HEADER_UNIT, __LINE__);

            if (rc == NO_ERROR)
                rc = writeFile(hdrFilePtr, hdrFileName, fileData->fFileHeader.fPtrSection,
                               ptrSecSize, __LINE__);

            delete hdrFilePtr;
        }

        if (rc == NO_ERROR)
        {
            // log the header information for recovery
            rc = writeLog(fTransId, "hdr", fileData->fFileName, aDMLLogFileName, headerSize);
            if (rc != NO_ERROR)
            {
                ostringstream oss;
                oss << "log " << fileData->fFileName << ".hdr to DML logfile failed.";
                logMessage(oss.str(), logging::LOG_TYPE_ERROR);
            }

            if ((rc == NO_ERROR) && (rc = writeHeader_(fileData, ptrSecSize)) == NO_ERROR)
            {
                (fileData->fFilePtr)->flush();
            }
        }
        else
        {
            IDBPolicy::remove(hdrFileName.c_str());
        }
    }
    else
    {
        if ((rc = writeHeader_(fileData, ptrSecSize)) == NO_ERROR)
        {
            (fileData->fFilePtr)->flush();
        }
    }

    if (rc != NO_ERROR)
    {
        ostringstream oss;
        oss << "write header failed: " << fileData->fFileName << "call from line:" << ln;
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
    }

    return rc;
}

//------------------------------------------------------------------------------
// Write the chunk pointers headers for the specified file (fileData).
// ln is the source code line number of the code invoking this operation
// (ex __LINE__); this is used for logging error messages.  For DML usage,
// backup for recovery is also performed.  This step is skipped for cpimport.bin
// as bulk import performs its own backup and recovery operations.
//------------------------------------------------------------------------------
inline int ChunkManager::writeHeader_(CompFileData* fileData, int ptrSecSize)
{
    int rc = setFileOffset(fileData->fFilePtr, fileData->fFileName, 0, __LINE__);
    if (rc == NO_ERROR)
        rc = writeFile(fileData->fFilePtr, fileData->fFileName,
                              fileData->fFileHeader.fControlData,
                              COMPRESSED_FILE_HEADER_UNIT, __LINE__);
    if (rc == NO_ERROR)
        rc = writeFile(fileData->fFilePtr, fileData->fFileName,
                              fileData->fFileHeader.fPtrSection,
                              ptrSecSize, __LINE__);
    return rc;
}

//------------------------------------------------------------------------------
// For the specified segment file (pFile), read in an abbreviated/compressed
// chunk extent, uncompress, and expand to a full chunk for a full extent.
//------------------------------------------------------------------------------
int ChunkManager::expandAbbrevColumnExtent(IDBDataFile* pFile, uint64_t emptyVal, int width)
{
    map<IDBDataFile*, CompFileData*>::iterator i = fFilePtrMap.find(pFile);
    if (i == fFilePtrMap.end())
    {
        logMessage(ERR_COMP_FILE_NOT_FOUND, logging::LOG_TYPE_ERROR, __LINE__);
        return ERR_COMP_FILE_NOT_FOUND;
    }

    int rc = NO_ERROR;
    // fetch the initial chunk if not already done.
    ChunkData* chunkData = (i->second)->findChunk(0);
    if ((chunkData == NULL) && ((rc = fetchChunkFromFile(pFile, 0, chunkData)) != NO_ERROR))
        return rc;

    // buf points to the end of existing data
    char* buf = chunkData->fBufUnCompressed + chunkData->fLenUnCompressed;
    int   size = UNCOMPRESSED_CHUNK_SIZE - chunkData->fLenUnCompressed;
    fFileOp->setEmptyBuf((unsigned char*)buf, size, emptyVal, width);
    chunkData->fLenUnCompressed = UNCOMPRESSED_CHUNK_SIZE;
    chunkData->fWriteToFile = true;
    //(writeChunkToFile(i->second, chunkData));
    //(writeHeader(i->second, __LINE__));
    return NO_ERROR;
}

//------------------------------------------------------------------------------
// For column segment file:
// Increment the block count stored in the chunk header used to track how many
// blocks are allocated to the corresponding segment file.
//------------------------------------------------------------------------------
int ChunkManager::updateColumnExtent(IDBDataFile* pFile, int addBlockCount)
{
    map<IDBDataFile*, CompFileData*>::iterator i = fFilePtrMap.find(pFile);
    if (i == fFilePtrMap.end())
    {
        logMessage(ERR_COMP_FILE_NOT_FOUND, logging::LOG_TYPE_ERROR, __LINE__);
        return ERR_COMP_FILE_NOT_FOUND;
    }

    CompFileData* pFileData = i->second;
    if (!pFileData)
    {
        logMessage(ERR_COMP_FILE_NOT_FOUND, logging::LOG_TYPE_ERROR, __LINE__);
        return ERR_COMP_FILE_NOT_FOUND;
    }
    int rc = NO_ERROR;
    char* hdr = pFileData->fFileHeader.fControlData;
    fCompressor.setBlockCount(hdr, fCompressor.getBlockCount(hdr) + addBlockCount);
    ChunkData* chunkData = (pFileData)->findChunk(0);
    if (chunkData != NULL)
    {
        if ((rc = writeChunkToFile(pFileData, chunkData)) == NO_ERROR)
        {
            rc = writeHeader(pFileData, __LINE__);
            if ( rc == NO_ERROR)
            {
                //@Bug 4977 remove log files
                removeBackups(fTransId);
            }
        }
        else
        {
            ostringstream oss;
            oss << "write chunk to file failed when updateColumnExtent: " << pFileData->fFileName;
            logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        }
    }

    pFile->flush();
    return rc;
}

//------------------------------------------------------------------------------
// For dictionary store segment file:
// Increment the block count stored in the chunk header used to track how many
// blocks are allocated to the corresponding segment file.
//------------------------------------------------------------------------------
int ChunkManager::updateDctnryExtent(IDBDataFile* pFile, int addBlockCount)
{
    map<IDBDataFile*, CompFileData*>::iterator i = fFilePtrMap.find(pFile);
    if (i == fFilePtrMap.end())
    {
        logMessage(ERR_COMP_FILE_NOT_FOUND, logging::LOG_TYPE_ERROR, __LINE__);
        return ERR_COMP_FILE_NOT_FOUND;
    }

    int rc = NO_ERROR;
    // fetch the initial chunk if not already done.
    ChunkData* chunkData = (i->second)->findChunk(0);
    if ((chunkData == NULL) && ((rc = fetchChunkFromFile(pFile, 0, chunkData)) != NO_ERROR))
        return rc;  // logged by fetchChunkFromFile

    char* hdr = i->second->fFileHeader.fControlData;
    char* uncompressedBuf = chunkData->fBufUnCompressed;
    int currentBlockCount = fCompressor.getBlockCount(hdr);
    // Bug 3203, write out the compressed initial extent.
    if (currentBlockCount == 0)
    {
        int initSize = NUM_BLOCKS_PER_INITIAL_EXTENT*BYTE_PER_BLOCK;
        initializeDctnryChunk(uncompressedBuf, initSize);
        chunkData->fWriteToFile = true;
        if ((rc = writeChunkToFile(i->second, chunkData)) == NO_ERROR)
        {
            rc = writeHeader(i->second, __LINE__);
            if ( rc == NO_ERROR)
            {
                //@Bug 4977 remove the log file
                removeBackups(fTransId);
            }
        }
        else
        {
            ostringstream oss;
            oss << "write chunk to file failed when updateDctnryExtent: " << i->second->fFileName;
            logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        }
    }
    else if (currentBlockCount == NUM_BLOCKS_PER_INITIAL_EXTENT)
    {
        int initSize = NUM_BLOCKS_PER_INITIAL_EXTENT*BYTE_PER_BLOCK;
        int incrSize = UNCOMPRESSED_CHUNK_SIZE - initSize;
        initializeDctnryChunk(uncompressedBuf + initSize, incrSize);
        uint64_t* ptrs = reinterpret_cast<uint64_t*>(i->second->fFileHeader.fPtrSection);
        ptrs[1] = 0;  // the compressed chunk size is unknown
    }

    if (rc == NO_ERROR)
        fCompressor.setBlockCount(hdr, fCompressor.getBlockCount(hdr) + addBlockCount);

    return rc;
}

//------------------------------------------------------------------------------
// Close any open segment files, and free up memory.
//------------------------------------------------------------------------------
void ChunkManager::cleanUp(const std::map<FID, FID> & columOids)
{
    WE_COMP_DBG(cout << "cleanUp with " << fActiveChunks.size() << " active chunk(s)." << endl;)
    std::map<FID,FID>::const_iterator it;
    map<IDBDataFile*, CompFileData*>::iterator i = fFilePtrMap.begin();
    while ( i != fFilePtrMap.end() )
    {
        CompFileData* fileData = i->second;

        it = columOids.find (fileData->fFid);
        if (fIsInsert && it != columOids.end())
        {
            list<ChunkData*>& chunks = fileData->fChunkList;
            for (list<ChunkData*>::iterator j = chunks.begin(); j != chunks.end(); ++j)
                delete *j;

            delete fileData->fFilePtr;
            fFileMap.erase(fileData->fFileID);
            fFilePtrMap.erase(i++);

            delete fileData;

        }
        else if (!fIsInsert || (columOids.size() == 0))
        {
            list<ChunkData*>& chunks = fileData->fChunkList;
            for (list<ChunkData*>::iterator j = chunks.begin(); j != chunks.end(); ++j)
                delete *j;

            delete fileData->fFilePtr;
            fFileMap.erase(fileData->fFileID);
            fFilePtrMap.erase(i++);

            delete fileData;
        }
        else
        {
            i++;
        }
    }

    if (fDropFdCache)
    {
        cacheutils::dropPrimProcFdCache();
        fDropFdCache = false;
    }
}

//------------------------------------------------------------------------------
// Read "n" blocks from pFile starting at fbo, into readBuf.
//------------------------------------------------------------------------------
int ChunkManager::readBlocks(IDBDataFile* pFile, unsigned char* readBuf, uint64_t fbo, size_t n)
{
    WE_COMP_DBG(cout << "backup blocks fbo:" << fbo << "  num:" << n << " file:" << pFile << endl;)

    // safety check
    if (pFile == NULL || n < 1)
    {
        return -1;
    }

    map<IDBDataFile*, CompFileData*>::iterator fpIt = fFilePtrMap.find(pFile);
    if (fpIt == fFilePtrMap.end())
    {
        return -1;
    }

    // the n blocks may cross more than one chunk
    // find the chunk ID and offset of the 1st fbo
    lldiv_t offset = lldiv(fbo * BYTE_PER_BLOCK, UNCOMPRESSED_CHUNK_SIZE);
    int idx = offset.quot;                      // current chunk id
    int rem = offset.rem;                       // offset in current chunk
    int num = UNCOMPRESSED_CHUNK_SIZE - rem;    // # of bytes available in current chunk
    int left = n * BYTE_PER_BLOCK;              // # of bytest to be read
    // # of bytes to be read from current chunk
    num = (left > num) ? num : left;
    do
    {
        ChunkData* chunkData = (fpIt->second)->findChunk(idx);

        WE_COMP_DBG(cout << "id:" << idx << " ofst:" << rem << " num:" << num <<
                            " left:" << left << endl;)
        // chunk is not already uncompressed
        if (chunkData == NULL)
        {
            if (fetchChunkFromFile(pFile, idx, chunkData) != NO_ERROR)
            {
                return -1;
            }
        }

        // copy the data at fbo to readBuf
        memcpy(readBuf, chunkData->fBufUnCompressed + rem, num);

        // prepare for the next read
        readBuf += num;
        rem = 0;
        left -= num;
        num = (left > UNCOMPRESSED_CHUNK_SIZE) ? UNCOMPRESSED_CHUNK_SIZE : left;
        idx++;
    }
    while (left > 0);

    return n;
}

//------------------------------------------------------------------------------
// Write the a block (writeBuf) into the fbo block of the specified file.
// Updated chunk is not flushed to disk but left pending in the applicable
// CompFileData object.
//------------------------------------------------------------------------------
int ChunkManager::restoreBlock(IDBDataFile* pFile, const unsigned char* writeBuf, uint64_t fbo)
{
    WE_COMP_DBG(cout << "restore blocks fbo:" << fbo << " file:" << pFile << endl;)
    // safety check
    if (pFile == NULL)
        return -1;

    map<IDBDataFile*, CompFileData*>::iterator fpIt = fFilePtrMap.find(pFile);
    if (fpIt == fFilePtrMap.end())
        return -1;

    // the n blocks may cross more than one chunk
    // find the chunk ID and offset of the 1st fbo
    lldiv_t offset = lldiv(fbo * BYTE_PER_BLOCK, UNCOMPRESSED_CHUNK_SIZE);
    ChunkData* chunkData = (fpIt->second)->findChunk(offset.quot);
    WE_COMP_DBG(cout << "id:" << offset.quot << " ofst:" << offset.rem << endl;)
    // chunk is not already uncompressed
    if (chunkData == NULL)
    {
        if (fetchChunkFromFile(pFile, offset.quot, chunkData) != NO_ERROR)
            return -1;
    }

    // copy the data to chunk buffer
    memcpy(chunkData->fBufUnCompressed + offset.rem, writeBuf, BYTE_PER_BLOCK);
    chunkData->fWriteToFile = true;

    return BYTE_PER_BLOCK;
}

//------------------------------------------------------------------------------
// Get the allocated block count from the header, for the specified file (pFile)
//------------------------------------------------------------------------------
int ChunkManager::getBlockCount(IDBDataFile* pFile)
{
    map<IDBDataFile*, CompFileData*>::iterator fpIt = fFilePtrMap.find(pFile);
    idbassert(fpIt != fFilePtrMap.end());

    return fCompressor.getBlockCount(fpIt->second->fFileHeader.fControlData);
}

//------------------------------------------------------------------------------
// Set the FileOp pointer and dictionary flag
//------------------------------------------------------------------------------
void ChunkManager::fileOp(FileOp* fileOp)
{
    fFileOp = fileOp;
    if (fileOp)
    {
        setTransId(fileOp->getTransId());
    }
}

//------------------------------------------------------------------------------
// Calculate and return the size of the chunk pointer header for a column of the
// specified width.
//------------------------------------------------------------------------------
int ChunkManager::calculateHeaderSize(int width)
{
    int headerUnits = 1;

    // dictionary columns may need variable length header
    if (width > 8)
    {
        int extentsPerFile = Config::getExtentsPerSegmentFile();
        int rowsPerExtent = BRMWrapper::getInstance()->getExtentRows();
        int rowsPerFile = rowsPerExtent * extentsPerFile;
        int stringsPerBlock = 8180 / (width + 2);  // 8180 = 8192 - 12
        int blocksNeeded = rowsPerFile / stringsPerBlock;
        int blocksPerChunk = UNCOMPRESSED_CHUNK_SIZE / BYTE_PER_BLOCK;
        lldiv_t chunks = lldiv(blocksNeeded, blocksPerChunk);
        int chunksNeeded = chunks.quot + (chunks.rem ? 1 : 0); // round up
        int ptrsNeeded = chunksNeeded + 1; // 1 more ptr for 0 ptr marking end
        int ptrsIn4K = (4 * 1024) / sizeof(uint64_t);
        lldiv_t hdrs = lldiv(ptrsNeeded, ptrsIn4K);
        headerUnits = hdrs.quot + (hdrs.rem ? 1 : 0); // round up

        // Always include odd number of 4K ptr headers, so that when we add the
        // single 4K control header, the cumulative header space will be an even
        // multiple of an 8K boundary.
        if ((headerUnits % 2) == 0)
            headerUnits++;
    }

    headerUnits++;  // add the control data block
    return (headerUnits*COMPRESSED_FILE_HEADER_UNIT);
}

//------------------------------------------------------------------------------
// Reallocate the chunks in a file to account for an expanding chunk that will
// not fit in the available embedded free space.
//------------------------------------------------------------------------------
int ChunkManager::reallocateChunks(CompFileData* fileData)
{
    WE_COMP_DBG(cout << "reallocate chunks in " << fileData->fFileName
                     << " (" << fileData->fFilePtr << ")" << endl;)

    // return value
    int rc = NO_ERROR;

    // original file info
    string origFileName = fileData->fFileName;
    IDBDataFile*  origFilePtr = fileData->fFilePtr;
    origFilePtr->flush();

    // back out the current pointers
    int headerSize = fCompressor.getHdrSize(fileData->fFileHeader.fControlData);
    int ptrSecSize = headerSize - COMPRESSED_FILE_HEADER_UNIT;
    compress::CompChunkPtrList origPtrs;
    if (fCompressor.getPtrList(fileData->fFileHeader.fPtrSection, ptrSecSize, origPtrs) != 0)
    {
        ostringstream oss;
        oss << "Chunk shifting failed, file:" << origFileName << " -- invalid header.";
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_COMP_PARSE_HDRS;
    }

    // get the chunks already in memory
    list<ChunkData*>& chunkList = fileData->fChunkList;
    chunkList.sort(chunkDataPtrLessCompare);
    list<ChunkData*>::iterator j = chunkList.begin();
    int numOfChunks = origPtrs.size();  // number of chunks that contain user data
    vector<ChunkData*> chunksTouched;   // chunk data is being modified, and in memory
    for (int i = 0; i < numOfChunks; i++)
        chunksTouched.push_back(NULL);

    // mark touched chunks
    while (j != chunkList.end())
    {
        chunksTouched[(*j)->fChunkId] = *j;
        j++;
    }

    // new file name and pointer
    string rlcFileName(fileData->fFileName + ".rlc");
    IDBDataFile*  rlcFilePtr = IDBDataFile::open(
                                    IDBPolicy::getType( rlcFileName.c_str(), IDBPolicy::WRITEENG ),
                                    rlcFileName.c_str(),
                                    "w+b",
                                    0,
                                    fileData->fColWidth );

    if (!rlcFilePtr)
    {
        ostringstream oss;
        oss << "Chunk shifting failed, file:" << origFileName << " -- cannot open rlc file.";
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_FILE_OPEN;
    }

    // log the recover information here
    string aDMLLogFileName;
    rc = writeLog(fTransId, "rlc", fileData->fFileName, aDMLLogFileName);
    if (rc != NO_ERROR)
    {
        ostringstream oss;
        oss << "log " << fileData->fFileName << ".rlc to DML logfile failed.";
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);

        delete rlcFilePtr;
        rlcFilePtr = NULL;
        IDBPolicy::remove(rlcFileName.c_str());
        return rc;
    }

    // !!! May conside to use mmap to speed up the copy !!!
    // !!! copy the whole file and update the shifting part !!!

    // store updated chunk pointers
    uint64_t* ptrs = reinterpret_cast<uint64_t*>(fileData->fFileHeader.fPtrSection);
    ptrs[0] = origPtrs[0].first;    // the first chunk offset has no change.

    // bug3913, file size 0 after reallocate.
    // write the header, to be updated later, make sure there is someing in the file
    if ((rc = writeFile(rlcFilePtr, rlcFileName, fileData->fFileHeader.fControlData,
                              COMPRESSED_FILE_HEADER_UNIT, __LINE__)) == NO_ERROR)
        rc = writeFile(rlcFilePtr, rlcFileName, fileData->fFileHeader.fPtrSection,
                              ptrSecSize, __LINE__);

    int k = 0;
    for (; k < numOfChunks && rc == NO_ERROR; k++)
    {
        uint64_t chunkSize = 0;                // size of current chunk
        unsigned char* buf = NULL;             // output buffer

        // Find the current chunk size, and allocate the data -- buf point to the data.
        if (chunksTouched[k] == NULL)
        {
            // Chunks not touched will be copied to new file without being uncompressed first.
            //cout << "reallocateChunks: chunk has not been updated" << endl;
            chunkSize = origPtrs[k].second;

            // read disk data into compressed data buffer
            buf = (unsigned char*)fBufCompressed;
            if ((rc = setFileOffset(origFilePtr, origFileName, origPtrs[k].first, __LINE__)) != NO_ERROR)
            {
                ostringstream oss;
                oss << "set file offset failed @line:" << __LINE__ << "with retCode:" << rc
                    << " filename:" << origFileName;
                logMessage(oss.str(), logging::LOG_TYPE_ERROR);
                continue;
            }

            if ((rc = readFile(origFilePtr, origFileName, buf, chunkSize, __LINE__)) != NO_ERROR)
            {
                ostringstream oss;
                oss << "readfile failed @line:" << __LINE__ << "with retCode:" << rc
                    << " filename:" << origFileName;
                logMessage(oss.str(), logging::LOG_TYPE_ERROR);
                continue;
            }
        }
        else // chunksTouched[k] != NULL
        {
            // chunk has been updated, and in memory.
            //cout << "reallocateChunks: chunk has been updated" << endl;
            ChunkData* chunkData = chunksTouched[k];
            fLenCompressed = fMaxCompressedBufSize;
            if ((rc = fCompressor.compressBlock((char*)chunkData->fBufUnCompressed,
                                            chunkData->fLenUnCompressed,
                                            (unsigned char*)fBufCompressed,
                                            fLenCompressed)) != 0)
            {
                ostringstream oss;
                oss << "Compress data failed @line:" << __LINE__ << "with retCode:" << rc
                    << " filename:" << rlcFileName;
                logMessage(oss.str(), logging::LOG_TYPE_ERROR);

                rc = ERR_COMP_COMPRESS;
                continue;
            }
            WE_COMP_DBG(cout << "Chunk compressed from " << chunkData->fLenUnCompressed << " to "
                            << fLenCompressed;)

            // shifting chunk, add padding space
            if ((rc = fCompressor.padCompressedChunks(
                    (unsigned char*)fBufCompressed, fLenCompressed, fMaxCompressedBufSize)) != 0)
            {
                WE_COMP_DBG(cout << ", but padding failed." << endl;)
                ostringstream oss;
                oss << "Compress data failed @line:" << __LINE__ << "with retCode:" << rc
                    << " filename:" << rlcFileName;
                logMessage(oss.str(), logging::LOG_TYPE_ERROR);

                rc = ERR_COMP_PAD_DATA;
                continue;
            }
            WE_COMP_DBG(cout << ", and padded to " << fLenCompressed;)

            buf = (unsigned char*)fBufCompressed;
            chunkSize = fLenCompressed;
        }

        // write is in sequence, no need to call setFileOffset
        //cout << "reallocateChunks: writing to temp file " << rlcFileName << " with fileptr " << rlcFilePtr << endl;
        rc = writeFile(rlcFilePtr, rlcFileName, buf, chunkSize, __LINE__);
        if (rc != NO_ERROR)
        {
            //cout << "reallocateChunks: writing to temp file " << rlcFileName << " with fileptr " << rlcFilePtr << " failed" << endl;
            ostringstream oss;
            oss << "write file failed @line:" << __LINE__ << "with retCode:" << rc
                << " filename:" << rlcFileName;
            logMessage(oss.str(), logging::LOG_TYPE_ERROR);
            continue;
        }

        // Update the current chunk size.
        ptrs[k+1] = ptrs[k] + chunkSize;
    }

    // up to now, everything OK: rc == NO_ERROR
    // remove all written chunks from active chunk list.
    j = chunkList.begin();
    while (j != chunkList.end())
    {
        ChunkData* chunkData = *j;
        fActiveChunks.remove(make_pair(fileData->fFileID, chunkData));
        fileData->fChunkList.remove(chunkData);
        delete chunkData;

        j = chunkList.begin();
    }

    // finally update the header
    if (rc == NO_ERROR)
        rc = setFileOffset(rlcFilePtr, rlcFileName, 0, __LINE__);
    if (rc == NO_ERROR)
        rc = writeFile(rlcFilePtr, rlcFileName, fileData->fFileHeader.fControlData,
                              COMPRESSED_FILE_HEADER_UNIT, __LINE__);
    if (rc == NO_ERROR)
        rc = writeFile(rlcFilePtr, rlcFileName, fileData->fFileHeader.fPtrSection,
                              ptrSecSize, __LINE__);

    if (rc != NO_ERROR)
    {
        struct timeval tv;
        gettimeofday(&tv, 0);
        struct tm ltm;
        localtime_r(reinterpret_cast<time_t*>(&tv.tv_sec), &ltm);
        char tmText[24];
        snprintf(tmText, sizeof(tmText), ".%04d%02d%02d%02d%02d%02d%06ld",
                ltm.tm_year+1900, ltm.tm_mon+1,
                ltm.tm_mday, ltm.tm_hour, ltm.tm_min,
                ltm.tm_sec, tv.tv_usec);
        string dbgFileName(rlcFileName + tmText);

        ostringstream oss;
        oss << "Chunk shifting failed, file:" << origFileName;
        if (IDBPolicy::rename(rlcFileName.c_str(), dbgFileName.c_str()) == 0)
            oss << ", rlc file is:" << dbgFileName;

        // write out the header for debugging in case the header in rlc file is bad or not updated.
        string rlcPtrFileName(dbgFileName + ".ptr");

        IDBDataFile*  rlcPtrFilePtr = IDBDataFile::open(
                                        IDBPolicy::getType(rlcPtrFileName.c_str(),
                                            IDBPolicy::WRITEENG),
                                        rlcPtrFileName.c_str(),
                                        "w+b",
                                        0,
                                        fileData->fColWidth);

        if (rlcPtrFilePtr &&
            (writeFile(rlcPtrFilePtr, rlcPtrFileName, fileData->fFileHeader.fControlData,
                              COMPRESSED_FILE_HEADER_UNIT, __LINE__) == NO_ERROR) &&
            (writeFile(rlcPtrFilePtr, rlcPtrFileName, fileData->fFileHeader.fPtrSection,
                              ptrSecSize, __LINE__) == NO_ERROR))
        {
            oss << ", rlc file header in memory: " << rlcPtrFileName;
        }
        else
        {
            oss << ", possible incomplete rlc file header in memory: " << rlcPtrFileName;
        }

        logMessage(oss.str(), logging::LOG_TYPE_ERROR);

        closeFile(fileData);
        delete rlcFilePtr;
        rlcFilePtr = NULL;

        if (rlcPtrFilePtr != NULL)
        {
            delete rlcPtrFilePtr;
            rlcPtrFilePtr = NULL;
        }

        return rc;
    }

    // update the file pointer map w/ new file pointer
    //cout << "realloc1: remove ptr = " << fileData->fFilePtr << endl;
    fFilePtrMap.erase(fileData->fFilePtr);
    delete fileData->fFilePtr;
    fileData->fFilePtr = NULL;
    delete rlcFilePtr;
    rlcFilePtr = NULL;

    // put reallocated file size here for logging purpose.
    uint64_t fileSize = 0;
    if (rc == NO_ERROR)
    {
#ifdef _MSC_VER
        //We need to do this early on so the ::rename() call below will work on Windows
        // we'll do it again later on, but that's life...
        //FIXME: there's a race here that a query will re-open the files before we can jigger
        // them around. We need to make sure PP is opening these files with the right perms
        // to allow another process to delete them.
        cacheutils::dropPrimProcFdCache();
#endif

        // @bug3913, keep the original file until the new file is properly renamed.
        // 1. check the new file size is NOT 0, matching ptr[k].
        // 2. mv the current to be backup.
        // 3. rename the rlc file.
        // 4. check the file size again.
        // 5. verify each chunk.
        // 5. rm the bak file or mv bak file back.

        // check the new file size using two methods mostly for curiosity on 0 size file.
        // They can be removed because all chunks are to be verified after rename.
        if ( IDBPolicy::size( rlcFileName.c_str() ) != (int64_t) ptrs[k] )
        {
            ostringstream oss;
            oss << "Incorrect file size, expect:" << ptrs[k] << ", stat:" << fileSize
                << ", filename:" << rlcFileName;
            logMessage(oss.str(), logging::LOG_TYPE_ERROR);
            rc = ERR_COMP_RENAME_FILE;
        }

        if (rc == NO_ERROR)
        {
            if (fIsHdfs)
                rc = swapTmpFile(rlcFileName, fileData->fFileName+".tmp");
            else
                rc = swapTmpFile(rlcFileName, fileData->fFileName);
        }

        if ((rc == NO_ERROR) &&
            (rc = openFile(fileData, "r+b", fileData->fColWidth, true, __LINE__)) == NO_ERROR) // @bug 5572 HDFS tmp file
        {
//          see TODO- above regarding setvbuf
//          setvbuf(fileData->fFilePtr, fileData->fIoBuffer.get(), _IOFBF, fileData->fIoBSize);
            fileSize = fileData->fFilePtr->size();
            if (fileSize == ptrs[k])
            {
               rc = verifyChunksAfterRealloc(fileData);
            }
            else
            {
                ostringstream oss;
                oss << "Incorrect file size, expect:" << ptrs[k] << ", stat:" << fileSize
                    << ", filename:" << fileData->fFileName;
                logMessage(oss.str(), logging::LOG_TYPE_ERROR);
                rc = ERR_COMP_RENAME_FILE;
            }

            if (rc == NO_ERROR)
            {
                fFilePtrMap.insert(make_pair(fileData->fFilePtr, fileData));
                //cout << "realloc2: insert ptr = " << fileData->fFilePtr << endl;
                // notify the PrimProc of unlinking original data file
                fDropFdCache = true;
            }
        }

        if (!fIsHdfs)
        {
            string bakFileName(fileData->fFileName + ".orig");
            if (rc == NO_ERROR)
            {
                // unlink the original file (remove is portable)
                if (fFs.remove(bakFileName.c_str()) != 0)
                {
                    ostringstream oss;
                    oss << "remove backup file " << bakFileName << " failed: " << strerror(errno);

                    // not much we can do, log an info message for manual cleanup
                    logMessage(oss.str(), logging::LOG_TYPE_INFO);
                }
            }
            else
            {
                // keep the bad file for debugging purpose
                if (fFs.rename(fileData->fFileName.c_str(), rlcFileName.c_str()) == 0)
                {
                    ostringstream oss;
                    oss << "data file after chunk shifting failed verification.";
                    logMessage(oss.str(), logging::LOG_TYPE_ERROR);
                }

                // roll back the bak file
                if (fFs.rename(bakFileName.c_str(), fileData->fFileName.c_str()) != 0)
                {
                    ostringstream oss;
                    oss << "rename " << bakFileName << " to " << fileData->fFileName << " failed: "
                        << strerror(errno);

                    // must manually move it back
                    logMessage(oss.str(), logging::LOG_TYPE_ERROR);
                }
            }
        }
    }

    if (!fIsHdfs)
    {
        if (rc == NO_ERROR)
        {
            // remove the log file
            fFs.remove(aDMLLogFileName.c_str());
        }
        else
        {
            struct timeval tv;
            gettimeofday(&tv, 0);
            struct tm ltm;
            localtime_r(reinterpret_cast<time_t*>(&tv.tv_sec), &ltm);
            char tmText[24];
            snprintf(tmText, sizeof(tmText), ".%04d%02d%02d%02d%02d%02d%06ld",
                    ltm.tm_year+1900, ltm.tm_mon+1,
                    ltm.tm_mday, ltm.tm_hour, ltm.tm_min,
                    ltm.tm_sec, tv.tv_usec);
            string dbgFileName(rlcFileName + tmText);

            ostringstream oss;
            oss << "Chunk shifting failed, file:" << origFileName;
            if (IDBPolicy::rename(rlcFileName.c_str(), dbgFileName.c_str()) == 0)
                oss << ", rlc file is:" << dbgFileName;

            // write out the header for debugging in case the header in rlc file is bad.
            string rlcPtrFileName(dbgFileName + ".hdr");
            IDBDataFile*  rlcPtrFilePtr = IDBDataFile::open(
                                            IDBPolicy::getType(rlcPtrFileName.c_str(),
                                                IDBPolicy::WRITEENG),
                                            rlcPtrFileName.c_str(),
                                            "w+b",
                                            0);
            if (rlcPtrFilePtr &&
                (writeFile(rlcPtrFilePtr, rlcPtrFileName, fileData->fFileHeader.fControlData,
                                  COMPRESSED_FILE_HEADER_UNIT, __LINE__) == NO_ERROR) &&
                (writeFile(rlcPtrFilePtr, rlcPtrFileName, fileData->fFileHeader.fPtrSection,
                                  ptrSecSize, __LINE__) == NO_ERROR))
            {
                oss << ", rlc file header in memory: " << rlcPtrFileName;
            }
            else
            {
                oss << ", possible incomplete rlc file header in memory: " << rlcPtrFileName;
            }

            logMessage(oss.str(), logging::LOG_TYPE_ERROR);

            closeFile(fileData);

            if (rlcFilePtr != NULL)
            {
                delete rlcFilePtr;
                rlcFilePtr = NULL;
            }

            if (rlcPtrFilePtr != NULL)
            {
                delete rlcPtrFilePtr;
                rlcPtrFilePtr = NULL;
            }
        }
    }

    return rc;
}

//------------------------------------------------------------------------------
// Verify chunks can be uncompressed after a chunk shift.
//------------------------------------------------------------------------------
int ChunkManager::verifyChunksAfterRealloc(CompFileData* fileData)
{
    int rc = NO_ERROR;

    // read in the header
    if ((rc = readFile(fileData->fFilePtr, fileData->fFileName, fileData->fFileHeader.fControlData,
                        COMPRESSED_FILE_HEADER_UNIT, __LINE__)) != NO_ERROR)
    {
        ostringstream oss;
        oss << "Failed to read control header from new " << fileData->fFileName << ", roll back";
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);

        return rc;
    }

    // make sure the header is valid
    if ((rc = fCompressor.verifyHdr(fileData->fFileHeader.fControlData)) != 0)
    {
        ostringstream oss;
        oss << "Invalid header in new " << fileData->fFileName << ", roll back";
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);

        return rc;
    }

    int headerSize = fCompressor.getHdrSize(fileData->fFileHeader.fControlData);
    int ptrSecSize = headerSize - COMPRESSED_FILE_HEADER_UNIT;

    // read in the pointer section in header
    if ((rc = readFile(fileData->fFilePtr, fileData->fFileName, fileData->fFileHeader.fPtrSection,
                        ptrSecSize, __LINE__)) != NO_ERROR)
    {
        ostringstream oss;
        oss << "Failed to read pointer header from new " << fileData->fFileName << "@" << __LINE__;
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return rc;
    }

    // get pointer list
    compress::CompChunkPtrList ptrs;
    if (fCompressor.getPtrList(fileData->fFileHeader.fPtrSection, ptrSecSize, ptrs) != 0)
    {
        ostringstream oss;
        oss << "Failed to parse pointer list from new " << fileData->fFileName << "@" << __LINE__;
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_COMP_PARSE_HDRS;
    }

    // now verify each chunk
    ChunkData chunkData;
    int numOfChunks = ptrs.size();  // number of chunks in the file
    for (int i = 0; i < numOfChunks && rc == NO_ERROR; i++)
    {
        unsigned int chunkSize = ptrs[i].second;
        if ((rc = setFileOffset(fileData->fFilePtr, fileData->fFileName, ptrs[i].first, __LINE__)))
        {
            ostringstream oss;
            oss << "Failed to setFileOffset new " << fileData->fFileName << "@" << __LINE__;
            logMessage(oss.str(), logging::LOG_TYPE_ERROR);
            continue;
        }

        if ((rc = readFile(fileData->fFilePtr, fileData->fFileName,
                          fBufCompressed, chunkSize, __LINE__)))
        {
            ostringstream oss;
            oss << "Failed to read chunk from new " << fileData->fFileName << "@" << __LINE__;
            logMessage(oss.str(), logging::LOG_TYPE_ERROR);
            continue;
        }

        // uncompress the read in buffer
        unsigned int dataLen = sizeof(chunkData.fBufUnCompressed);
        if (fCompressor.uncompressBlock((char*)fBufCompressed, chunkSize,
                    (unsigned char*)chunkData.fBufUnCompressed, dataLen) != 0)
        {
            ostringstream oss;
            oss << "Failed to uncompress chunk new " << fileData->fFileName << "@" << __LINE__;
            logMessage(oss.str(), logging::LOG_TYPE_ERROR);
            rc = ERR_COMP_UNCOMPRESS;
            continue;
        }
    }

    return rc;
}

//------------------------------------------------------------------------------
// Log an error message for the specified error code, and msg level.
//------------------------------------------------------------------------------
void ChunkManager::logMessage(int code, int level, int lineNum, int fromLine) const
{
    ostringstream oss;
    oss << ec.errorString(code) << " @line:" << lineNum;
    if (fromLine != -1)
        oss << " called from line:" << fromLine;

    logMessage(oss.str(), level);
}

//------------------------------------------------------------------------------
// Log the requested error message using the specified msg level.
//------------------------------------------------------------------------------
void ChunkManager::logMessage(const string& msg, int level) const
{
    logging::Message::Args args;
    args.add(msg);

    fSysLogger->logMessage((logging::LOG_TYPE) level, logging::M0080, args,
        //FIXME: store session id in class to pass on to LogginID...
        logging::LoggingID(SUBSYSTEM_ID_WE, 0, fTransId));
}

//------------------------------------------------------------------------------
// Replace the cdf file with the updated tmp file.
//------------------------------------------------------------------------------
int ChunkManager::swapTmpFile(const string& src, const string& dest)
{
    // return value
    int rc = NO_ERROR;

    // if no change to the cdf, the tmp may not exist, no need to swap.
    if (!fFs.exists(src.c_str()))
        return rc;

    ssize_t srcFileSize = IDBPolicy::size(src.c_str());
    if (srcFileSize <= 0)
    {
        ostringstream oss;
        oss << "swapTmpFile aborted. Source file size = " << srcFileSize;
        logMessage(oss.str(), logging::LOG_TYPE_CRITICAL);
        rc = ERR_COMP_RENAME_FILE;

        return rc;
    }

    errno = 0;
    // save the original file
    string orig(dest + ".orig");
    fFs.remove(orig.c_str());  // remove left overs
    if (fFs.rename(dest.c_str(), orig.c_str()) != 0)
    {
        ostringstream oss;
        oss << "rename " << dest << " to " << orig << " failed: " << strerror(errno);
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        rc = ERR_COMP_RENAME_FILE;
    }

    // rename the new file
    if (rc == NO_ERROR && fFs.rename(src.c_str(), dest.c_str()) != 0)
    {
        ostringstream oss;
        oss << "rename " << src << " to " << dest << " failed: " << strerror(errno);
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        rc = ERR_COMP_RENAME_FILE;
    }

    if (rc == NO_ERROR && fFs.remove(orig.c_str()) != 0)
        rc = ERR_COMP_REMOVE_FILE;

    return rc;
}


//------------------------------------------------------------------------------
// Construct a DML log file name based on transaction ID, etc.
//------------------------------------------------------------------------------
int ChunkManager::getDMLLogFileName(string& aDMLLogFileName, const TxnID& txnId) const
{
    config::Config *config = config::Config::makeConfig();
    string prefix = config->getConfig("SystemConfig", "DBRMRoot");
    if (prefix.length() == 0)
    {
        ostringstream oss;
        oss << "trans " << txnId << ":Need a valid DBRMRoot entry in Calpont configuation file";
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_DML_LOG_NAME;
    }

    uint64_t pos =  prefix.find_last_of ("/") ;
    if (pos != string::npos)
    {
        aDMLLogFileName = prefix.substr(0, pos+1); //Get the file path
    }
    else
    {
        ostringstream oss;
        oss << "trans " << txnId << ":Cannot find the dbrm directory ("
            << prefix.c_str() << ") for the DML log file";
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_DML_LOG_NAME;
    }

    ostringstream oss;
    oss << txnId << "_" << fLocalModuleId;
    aDMLLogFileName += "DMLLog_" + oss.str();

    return NO_ERROR;
}


//------------------------------------------------------------------------------
// clear the DML log file
//------------------------------------------------------------------------------
int ChunkManager::startTransaction(const TxnID& txnId) const
{
    // this step is for HDFS update/delete only.
    if (!fIsHdfs || fIsBulkLoad)
        return NO_ERROR;

    // Construct the DML log file name
    string aDMLLogFileName;
    if (getDMLLogFileName(aDMLLogFileName, txnId) != NO_ERROR)
        return ERR_DML_LOG_NAME;

    // truncate the existing file
    boost::scoped_ptr<IDBDataFile> aDMLLogFile(IDBDataFile::open(
                    IDBPolicy::getType(aDMLLogFileName.c_str(), IDBPolicy::WRITEENG),
                    aDMLLogFileName.c_str(), "wb", 0));

    if (!aDMLLogFile)
    {
        ostringstream oss;
        oss << "trans " << txnId << ":File " << aDMLLogFileName << " can't be opened.";
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_OPEN_DML_LOG;
    }

    return NO_ERROR;
}


//------------------------------------------------------------------------------
// Backup cdf file and replace the with the updated tmp file.
//------------------------------------------------------------------------------
int ChunkManager::confirmTransaction(const TxnID& txnId) const
{
    // return value
    int rc = NO_ERROR;
    // this step is for HDFS update/delete only.
    if (!fIsHdfs || fIsBulkLoad)
        return rc;

    string aDMLLogFileName;
    if (getDMLLogFileName(aDMLLogFileName, txnId) != NO_ERROR)
        return ERR_DML_LOG_NAME;

    //Open log file
    boost::scoped_ptr<IDBDataFile> aDMLLogFile;
    aDMLLogFile.reset(IDBDataFile::open(
                      IDBPolicy::getType(aDMLLogFileName.c_str(), IDBPolicy::WRITEENG),
                      aDMLLogFileName.c_str(), "r", 0));

    if (!aDMLLogFile)
    {
        ostringstream oss;
        oss << "trans " << txnId << ":File " << aDMLLogFileName << " can't be opened";
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_OPEN_DML_LOG;
    }

    ssize_t logSize = fFs.size(aDMLLogFileName.c_str());
    boost::scoped_array<char> buf(new char[logSize]);
    if (aDMLLogFile->read(buf.get(), logSize) != logSize)
                return ERR_FILE_READ;

    std::istringstream strstream(string(buf.get(), logSize));
    std::string backUpFileType;
    std::string filename;
    int64_t size;
    int64_t offset;
    ConfirmHdfsDbFile confirmHdfs;
    while (strstream >> backUpFileType >> filename >> size >> offset)
    {
        std::string confirmErrMsg;
        rc = confirmHdfs.confirmDbFileChange( backUpFileType,
            filename, confirmErrMsg );
        if (rc != NO_ERROR)
        {
            logMessage(confirmErrMsg, logging::LOG_TYPE_ERROR);
            break;
        }
    }

    return rc;
}


//------------------------------------------------------------------------------
// Finalize the chages
// if success, remove the orig
// otherwise,  move the orig back to cdf
//------------------------------------------------------------------------------
int ChunkManager::endTransaction(const TxnID& txnId, bool success) const
{
    // return value
    int rc = NO_ERROR;

    // this step is for HDFS update/delete only.
    if (!fIsHdfs || fIsBulkLoad)
        return rc;

    string aDMLLogFileName;
    if (getDMLLogFileName(aDMLLogFileName, txnId) != NO_ERROR)
        return ERR_DML_LOG_NAME;

    //Open log file
    boost::scoped_ptr<IDBDataFile> aDMLLogFile;
    aDMLLogFile.reset(IDBDataFile::open(
                      IDBPolicy::getType(aDMLLogFileName.c_str(), IDBPolicy::WRITEENG),
                      aDMLLogFileName.c_str(), "r", 0));

    if (!aDMLLogFile)
    {
        ostringstream oss;
        oss << "trans " << txnId << ":File " << aDMLLogFileName << " can't be opened";
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_OPEN_DML_LOG;
    }

    ssize_t logSize = fFs.size(aDMLLogFileName.c_str());
    ssize_t logRead = 0;
    boost::scoped_array<char> buf(new char[logSize]);
    if ((logRead = aDMLLogFile->read(buf.get(), logSize)) != logSize)
    {
        ostringstream oss;
        oss << "trans " << txnId << ":File " << aDMLLogFileName << " filed to read: "
            << logRead << "/" << logSize;
        logMessage(oss.str(), logging::LOG_TYPE_ERROR);
        return ERR_FILE_READ;
    }

    std::istringstream strstream(string(buf.get(), logSize));
    std::string backUpFileType;
    std::string filename;
    int64_t size;
    int64_t offset;
    ConfirmHdfsDbFile confirmHdfs;
    while (strstream >> backUpFileType >> filename >> size >> offset)
    {
        std::string finalizeErrMsg;
        rc = confirmHdfs.endDbFileChange( backUpFileType,
            filename, success, finalizeErrMsg );
        if (rc != NO_ERROR)
        {
            logMessage(finalizeErrMsg, logging::LOG_TYPE_ERROR);
            break;
        }
    }

    // final clean up or recover
    if (rc == NO_ERROR)
        rc = fFs.remove(aDMLLogFileName.c_str());

    return rc;
}

int ChunkManager::checkFixLastDictChunk(const FID& fid,
                    uint16_t root,
                    uint32_t partition,
                    uint16_t segment)
{
	
	int rc = 0;
	//Find the file info
	FileID fileID(fid, root, partition, segment);
    map<FileID, CompFileData*>::const_iterator mit = fFileMap.find(fileID);

    WE_COMP_DBG(cout << "getFileData: fid:" << fid << " root:" << root << " part:" << partition
                    << " seg:" << segment << " file* " << ((mit != fFileMap.end()) ? "" : "not ")
                    << "found." << endl;)

    // Get CompFileData pointer for existing Dictionary store file mit->second is CompFileData
    if (mit != fFileMap.end())
    {

		int headerSize = fCompressor.getHdrSize(mit->second->fFileHeader.fControlData);
		int ptrSecSize = headerSize - COMPRESSED_FILE_HEADER_UNIT;

		// get pointer list
		compress::CompChunkPtrList ptrs;
		if (fCompressor.getPtrList(mit->second->fFileHeader.fPtrSection, ptrSecSize, ptrs) != 0)
		{
			ostringstream oss;
			oss << "Failed to parse pointer list from new " << mit->second->fFileName << "@" << __LINE__;
			logMessage(oss.str(), logging::LOG_TYPE_ERROR);
			return ERR_COMP_PARSE_HDRS;
		}

		// now verify last chunk
		ChunkData* chunkData;
		int numOfChunks = ptrs.size();  // number of chunks in the file	
        unsigned int chunkSize = ptrs[numOfChunks-1].second;
        if ((rc = setFileOffset(mit->second->fFilePtr, mit->second->fFileName, ptrs[numOfChunks-1].first, __LINE__)))
        {
            ostringstream oss;
            oss << "Failed to setFileOffset new " << mit->second->fFileName << "@" << __LINE__;
            logMessage(oss.str(), logging::LOG_TYPE_ERROR);
            return rc;
        }

        if ((rc = readFile(mit->second->fFilePtr, mit->second->fFileName,
                          fBufCompressed, chunkSize, __LINE__)))
        {
            ostringstream oss;
            oss << "Failed to read chunk from new " << mit->second->fFileName << "@" << __LINE__;
            logMessage(oss.str(), logging::LOG_TYPE_ERROR);
            return rc;
        }

        // uncompress the read in buffer
        chunkData = new ChunkData(numOfChunks-1);
        unsigned int dataLen = sizeof(chunkData->fBufUnCompressed);
        if (fCompressor.uncompressBlock((char*)fBufCompressed, chunkSize,
                    (unsigned char*)chunkData->fBufUnCompressed, dataLen) != 0)
        {
			mit->second->fChunkList.push_back(chunkData);
			fActiveChunks.push_back(make_pair(mit->second->fFileID, chunkData));
            //replace this chunk with empty chunk
            uint64_t blocks = 512;
			if ((numOfChunks-1)== 0)
			{
					char* hdr = mit->second->fFileHeader.fControlData;
					if (fCompressor.getBlockCount(hdr) < 512)
						blocks = 256;
			}
				
			dataLen = 8192 * blocks;
				
			// load the uncompressed buffer with empty values.
			char* buf = chunkData->fBufUnCompressed;
			chunkData->fLenUnCompressed = UNCOMPRESSED_CHUNK_SIZE;
			initializeDctnryChunk(buf, UNCOMPRESSED_CHUNK_SIZE);	
			chunkData->fLenUnCompressed = dataLen;	
			chunkData->fWriteToFile = true;		
        }     
    }
	return rc;
}

}

// vim:ts=4 sw=4:
