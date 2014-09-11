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

/*******************************************************************************
* $Id: we_brm.cpp 4282 2012-10-29 16:31:57Z chao $
*
*******************************************************************************/
/** @file */

#include <cerrno>
#include <string>
#include <map>
//#define NDEBUG
#include <cassert>
#include <algorithm>
#include <unistd.h>
using namespace std;

#define WRITEENGINEBRM_DLLEXPORT
#include "we_brm.h"
#undef WRITEENGINEBRM_DLLEXPORT

#include "calpontsystemcatalog.h"
#include "we_dbfileop.h"
#include "we_convertor.h"
#include "we_chunkmanager.h"
#include "we_colopcompress.h"
#include "we_dctnrycompress.h"
#include "we_simplesyslog.h"

using namespace BRM;
//#include "stopwatch.h"
//using namespace logging;
/** Namespace WriteEngine */
namespace WriteEngine
{
BRMWrapper* BRMWrapper::m_instance = NULL;
boost::thread_specific_ptr<int> BRMWrapper::m_ThreadDataPtr;

#ifdef _MSC_VER
   __declspec(dllexport)
#endif

bool     BRMWrapper::m_useVb    = true;
OID      BRMWrapper::m_curVBOid  = INVALID_NUM;
FILE*    BRMWrapper::m_curVBFile = NULL;

struct fileInfoCompare // lt operator
{
    bool operator()(const File& lhs, const File& rhs) const
    {
        if (lhs.oid < rhs.oid) {
            return true;
        }
        if ((lhs.oid == rhs.oid) && (lhs.fDbRoot < rhs.fDbRoot)) {
            return true;
        }
        if ((lhs.oid == rhs.oid) &&(lhs.fDbRoot==rhs.fDbRoot) && (lhs.fPartition < rhs.fPartition)) {
            return true;
        }
        if ((lhs.oid == rhs.oid) && (lhs.fDbRoot==rhs.fDbRoot) && (lhs.fPartition==rhs.fPartition) && (lhs.fSegment < rhs.fSegment)) {
            return true;
        }

        return false;
    } // operator
}; // struct

typedef std::map< File, FILE*, fileInfoCompare > FileOpenMap;

//------------------------------------------------------------------------------
// Allocate an extent for the specified column OID.
// If this is the very first extent for the column, then dbRoot and partition
// must be specified, else the selected DBRoot and partition number is returned.
// The resulting segment number is always returned.
//------------------------------------------------------------------------------
int   BRMWrapper::allocateColExtent(
    const OID   oid,
    const u_int32_t colWidth,
    u_int16_t&  dbRoot,
    u_int32_t&  partition,
    u_int16_t&  segment,
    LBID_t&     startLbid,
    int&        allocSize,
    u_int32_t&  startBlock)
{
    int rc = blockRsltnMgrPtr->createColumnExtent(
        (int)oid, colWidth, dbRoot, partition, segment, startLbid,
        allocSize, startBlock);

    //std::ostringstream oss;
    //oss << "Allocated column extent: oid-" << oid <<
    //       "; wid-"       << colWidth     <<
    //       "; DBRoot-"    << dbRoot       <<
    //       "; part-"      << partition    <<
    //       "; seg-"       << segment      <<
    //       "; lbid-"      << startLbid    <<
    //       "; allocSize-" << allocSize    <<
    //       "; block-"     << startBlock;
    //std::cout << oss.str() << std::endl;

    return getRC(rc, ERR_BRM_ALLOC_EXTEND);
}

//------------------------------------------------------------------------------
// Allocate an extent for the specified dictionary store OID.
// If this is the very first extent for the column, then dbRoot must be
// specified, else the selected DBRoot is returned.  The selected partition
// and segment numbers are always returned.
//------------------------------------------------------------------------------
int   BRMWrapper::allocateDictStoreExtent(
    const OID   oid,
    u_int16_t   dbRoot,
    u_int32_t   partition,
    u_int16_t   segment,
    LBID_t&     startLbid,
    int&        allocSize)
{
    int rc = blockRsltnMgrPtr->createDictStoreExtent(
        (int)oid, dbRoot, partition, segment, startLbid, allocSize);

    //std::ostringstream oss;
    //oss << "Allocated dict extent: oid-" << oid <<
    //       "; DBRoot-"    << dbRoot    <<
    //       "; part-"      << partition <<
    //       "; seg-"       << segment   <<
    //       "; lbid-"      << startLbid <<
    //       "; allocSize-" << allocSize;
    //std::cout << oss.str() << std::endl;

    return getRC(rc, ERR_BRM_ALLOC_EXTEND);
}

//------------------------------------------------------------------------------
// Allocate an extent for the specified dictionary store OID
//------------------------------------------------------------------------------

#define MAX_VERSION_BUFFER_SIZE 1024

int BRMWrapper::copyVBBlock (FILE *pSourceFile, const OID sourceOid,
                FILE *pTargetFile, const OID targetOid,
                const std::vector < i32 > &fboList,
                const BRM::VBRange &freeList,
                size_t &nBlocksProcessed,
                DbFileOp* pFileOp,
                const size_t fboCurrentOffset)
{
    size_t bufferSize = MAX_VERSION_BUFFER_SIZE;
    if (freeList.size < bufferSize) bufferSize = freeList.size;
    if ((fboList.size()- fboCurrentOffset) < bufferSize) bufferSize = fboList.size()- fboCurrentOffset;

    unsigned char *buffer = (unsigned char *) malloc(bufferSize * BYTE_PER_BLOCK);
    if (buffer == NULL) {
        return ERR_NO_MEM;
    }

    size_t outputFileWritePointer = 0;

    while(outputFileWritePointer < freeList.size) {
        size_t numBlocksAvailableForWriting = freeList.size - outputFileWritePointer;
        if (bufferSize < numBlocksAvailableForWriting) numBlocksAvailableForWriting = bufferSize;

        size_t numBlocksToBeWritten = 0;
        //       size_t startOffsetInInput = nBlocksProcessed;
        size_t startOffsetInInput = fboCurrentOffset + nBlocksProcessed;
        // Consume whole of the freeList
        while((numBlocksToBeWritten < numBlocksAvailableForWriting)
            && (startOffsetInInput < fboList.size())) {

            // determine how many contiguous source blocks are availale
               size_t spaceAvailableInBuffer = numBlocksAvailableForWriting - numBlocksToBeWritten;

            size_t numContiguousBlocksAvaliableForReading = 1;
            size_t tmp = startOffsetInInput;

            while(1) {
                if (numContiguousBlocksAvaliableForReading == spaceAvailableInBuffer) break;
                if (tmp == (fboList.size() - 1)) break;
                if ((fboList[tmp] + 1) != fboList[tmp + 1]) break;

                tmp++;
                numContiguousBlocksAvaliableForReading++;
            }

            numContiguousBlocksAvaliableForReading = tmp - startOffsetInInput + 1;

            // determine how many contiguous blocks can be read from source file into buffer
            size_t numCopyBlocks = (numContiguousBlocksAvaliableForReading < spaceAvailableInBuffer)?
                numContiguousBlocksAvaliableForReading : spaceAvailableInBuffer;

            if (0 == numCopyBlocks) break;

            // read source blocks into buffer
            unsigned char *bufferOffset = buffer + (numBlocksToBeWritten * BYTE_PER_BLOCK);
			ColumnOp* colOp = dynamic_cast<ColumnOp*>(pFileOp);
			Dctnry* dctnry = dynamic_cast<Dctnry*>(pFileOp);
			if (colOp != NULL)
                pFileOp->chunkManager(colOp->chunkManager());
            else if (dctnry != NULL)
                pFileOp->chunkManager(dctnry->chunkManager());
            else
                pFileOp->chunkManager(NULL);
            size_t rwSize = pFileOp->readDbBlocks(pSourceFile, bufferOffset,
                                    fboList[startOffsetInInput], numCopyBlocks);

            if (rwSize != numCopyBlocks) {
                if (buffer) free(buffer);
                return ERR_BRM_VB_COPY_READ;
            }

            // update offsets and counters
            startOffsetInInput += numCopyBlocks;
            numBlocksToBeWritten += numCopyBlocks;
        }


        // write buffer to the target file if there is data read into it
        if (numBlocksToBeWritten > 0) {
            // Seek into target file
#ifdef _MSC_VER
            __int64 tgtOffset = (freeList.vbFBO + outputFileWritePointer) * BYTE_PER_BLOCK;
            int wc = _fseeki64(pTargetFile, tgtOffset, 0);
#else
            size_t tgtOffset = (freeList.vbFBO + outputFileWritePointer) * BYTE_PER_BLOCK;
            int wc = fseek(pTargetFile, tgtOffset, 0);
#endif
            if (wc != NO_ERROR) {
                std::string errMsgStr;
                Convertor::mapErrnoToString(errno, errMsgStr);
                logging::Message::Args args;
                args.add((uint64_t)targetOid);
                args.add((uint64_t)tgtOffset);
                args.add(std::string(errMsgStr));
                SimpleSysLog::instance()->logMsg(args, logging::LOG_TYPE_CRITICAL, logging::M0079);
                if (buffer) free(buffer);
                return ERR_BRM_VB_COPY_SEEK_VB;
            }

            size_t rwSize = fwrite(buffer, BYTE_PER_BLOCK,  numBlocksToBeWritten, pTargetFile);

            if (rwSize != numBlocksToBeWritten) {
                if (buffer) free(buffer);
                return ERR_BRM_VB_COPY_WRITE;
            }

            outputFileWritePointer += numBlocksToBeWritten;
            nBlocksProcessed += numBlocksToBeWritten;

        }
        else {  // There was nothing in the buffer, either source list or free list is finished
            if (buffer) free(buffer);
            return 0;
        }
    }

    if (buffer) free(buffer);
    return 0;
}

int BRMWrapper::copyVBBlock(FILE* pSourceFile, FILE* pTargetFile,
                                  const i64 sourceFbo, const i64 targetFbo,
                                  DbFileOp* fileOp, const Column& column)
{
    int rc, origin = 0;
    size_t rwSize;
    unsigned char buf[BYTE_PER_BLOCK];
    //add new error code for versioning error
#ifdef _MSC_VER
    rc = _fseeki64(pSourceFile, sourceFbo*BYTE_PER_BLOCK, origin);
#else
    rc = fseek(pSourceFile, sourceFbo*BYTE_PER_BLOCK, origin);
#endif
    if (rc != NO_ERROR)
        return ERR_BRM_VB_COPY_SEEK_VB;

    rwSize = fread(buf, 1, BYTE_PER_BLOCK, pSourceFile);
    if ((int) rwSize != BYTE_PER_BLOCK)
        return ERR_BRM_VB_COPY_READ;

    rwSize = fileOp->restoreBlock(pTargetFile, buf, targetFbo);
    if ((int) rwSize != BYTE_PER_BLOCK)
        return ERR_BRM_VB_COPY_WRITE;
    else
        return NO_ERROR;
}

// Not currently used
#if 0
int BRMWrapper::copyVBBlock(File&  sourceFileInfo, File& targetFileInfo, const i64 sourceFbo, const i64 targetFbo)
{
    FILE*          pSourceFile;
    FILE*          pTargetFile;
    int            rc;

    RETURN_ON_NULL((pSourceFile = openFile(sourceFileInfo, "r+b")), ERR_FILE_NOT_EXIST);
    RETURN_ON_NULL((pTargetFile = openFile(targetFileInfo, "r+b")), ERR_FILE_NOT_EXIST);

    rc = copyVBBlock(pSourceFile, pTargetFile, sourceFbo, targetFbo);
    fclose(pSourceFile);
    fclose(pTargetFile);

    return rc;
}

int BRMWrapper::copyVBBlock(FILE* pSourceFile, File& targetFileInfo, const i64 sourceFbo, const i64 targetFbo)
{
    FILE*          pTargetFile;
    int            rc;

    RETURN_ON_NULL((pTargetFile = openFile(targetFileInfo, "r+b")), ERR_FILE_NOT_EXIST);

    rc = copyVBBlock(pSourceFile, pTargetFile, sourceFbo, targetFbo);
    fclose(pTargetFile);

    return rc;
}
#endif

int BRMWrapper::commit(const VER_t transID)
{
    int rc = blockRsltnMgrPtr->vbCommit(transID);
    return getRC(rc, ERR_BRM_COMMIT);
}

void BRMWrapper::deleteInstance()
{
    delete m_instance;
    m_instance = NULL;
}

/**
 * @brief Inform BRM to delete certain oid
 */
int BRMWrapper::deleteOid(const OID oid)
{
    int rc = blockRsltnMgrPtr->deleteOID(oid);
    return getRC(rc, ERR_BRM_DEL_OID);
}

/**
 * @brief Inform BRM to delete certain oids
 */
int BRMWrapper::deleteOIDsFromExtentMap(const std::vector<int32_t>& oids)
{
    int rc = blockRsltnMgrPtr->deleteOIDs(oids);
    return getRC(rc, ERR_BRM_DEL_OID);
}

/**
 * @brief Get BRM information based on a specfic OID, DBRoot, partition,
 * and segment.
 */
int   BRMWrapper::getBrmInfo(const OID oid,
    const u_int32_t partition,
    const u_int16_t segment,
    const int       fbo,
    i64&            lbid)
{
    int rc = blockRsltnMgrPtr->lookupLocal((int)oid ,
        partition, segment,
        (u_int32_t)fbo,(LBID_t&)lbid);
     return getRC(rc, ERR_BRM_LOOKUP_LBID);
};

/**
 * @brief Get starting LBID from BRM for a specfic OID, partition, segment,
 * and block offset.
 */
int   BRMWrapper::getStartLbid(const OID oid,
    const u_int32_t partition,
    const u_int16_t segment,
    const int       fbo,
    LBID_t&         lbid)
{
    int rc = blockRsltnMgrPtr->lookupLocalStartLbid((int)oid ,
        partition, segment,
        (u_int32_t)fbo,lbid);
     return getRC(rc, ERR_BRM_LOOKUP_START_LBID);
}

/**
 * @brief Get the real physical offset based on the LBID
 */
int  BRMWrapper::getFboOffset(const i64 lbid,
    u_int16_t& dbRoot,
    u_int32_t& partition,
    u_int16_t& segment,
    int&       fbo)
{
    int oid;
    // according to Patric, extendmap don't need vbflag, thus verid =0
//  int rc = blockRsltnMgrPtr->lookup((u_int64_t)lbid, 0, false, oid, (u_int32_t&)fbo);
//  return getRC(rc, ERR_BRM_LOOKUP_FBO);
    return getFboOffset(lbid, oid, dbRoot, partition, segment, fbo);
}

int  BRMWrapper::getFboOffset(const i64 lbid, int& oid,
    u_int16_t& dbRoot,
    u_int32_t& partition,
    u_int16_t& segment,
    int&       fbo)
{
    // according to Patric, extendmap don't need vbflag, thus verid =0
    int rc = blockRsltnMgrPtr->lookupLocal((u_int64_t)lbid, 0, false, (BRM::OID_t&)oid,
        dbRoot, partition, segment,
        (u_int32_t&)fbo);
    return getRC(rc, ERR_BRM_LOOKUP_FBO);
}

BRMWrapper* BRMWrapper::getInstance()
{
    if (m_instance == NULL){
        m_instance = new BRMWrapper();

    // @bug 372
    // Create the VBF's. This should be smarter than this is...
  /*       size_t rootCnt = Config::DBRootCount();
    char fullFileName[FILE_NAME_SIZE];
    for (unsigned int i=0; i<4; i++)
    {
        uint16_t dbRoot = (i % rootCnt) + 1;
        fileOp.oid2FileName(i, fullFileName, true, dbRoot, 0, 0);
    }
*/
    }
    return m_instance;
}

int BRMWrapper::isReadWrite()
{
    int rc = blockRsltnMgrPtr->isReadWrite();
    if      (rc == BRM::ERR_OK)
        return NO_ERROR;
    else if (rc == BRM::ERR_READONLY)
        return ERR_BRM_READ_ONLY;
    else
        return ERR_BRM_GET_READ_WRITE;
}

FILE* BRMWrapper::openFile(File fileInfo, const char* mode, const bool bCache)
{
    FILE*    pFile;
    char     fileName[FILE_NAME_SIZE];

    if (bCache && fileInfo.oid == m_curVBOid && m_curVBFile != NULL)
        return m_curVBFile;

    FileOp fileOp;
    if (fileInfo.oid < 1000){ //Cannot have more than 999 version buffer files tp prevent oid collision
        RETURN_ON_WE_ERROR(fileOp.getFileName (fileInfo.oid, fileName),NULL);
    }
    else {
          RETURN_ON_WE_ERROR(fileOp.getFileName (fileInfo.oid, fileName,fileInfo.fDbRoot,
               fileInfo.fPartition, fileInfo.fSegment),NULL);
    }
    pFile = fopen(fileName, mode);

    // disable buffering for versionbuffer file
    if (pFile && fileInfo.oid < 1000 && setvbuf(pFile, NULL, _IONBF, 0)) {
        fclose(pFile);
        pFile = NULL;
	}

    if (pFile && bCache) {
        if (m_curVBOid != (OID)INVALID_NUM) {
            if (m_curVBOid != fileInfo.oid && m_curVBFile != NULL)
                fclose(m_curVBFile);
        }
        m_curVBOid = fileInfo.oid;
        m_curVBFile = pFile;
    }

    return pFile;
}

int BRMWrapper::rollBack(const VER_t transID, int sessionId)
{
    std::vector<LBID_t> lbidList;
    std::vector<LBIDRange> lbidRangeList;
    LBIDRange   range;
    OID_t       vbOid, weOid, currentVbOid;
    u_int32_t   vbFbo, weFbo;
    size_t      i;
    VER_t       verID = (VER_t) transID;
    bool        vbFlag;
    uint16_t   vbDbRoot, weDbRoot, vbSegmentNum, weSegmentNum;
    uint32_t   vbPartitionNum, wePartitionNum;
    File  sourceFileInfo;
    File  targetFileInfo;
    int rc = 0;
	std::map<FID,FID> columnOids;
	//Check BRM status before processing.
	rc = blockRsltnMgrPtr->isReadWrite();
	if (rc != 0 )
		return ERR_BRM_READ_ONLY;
	
	rc = blockRsltnMgrPtr->getUncommittedLBIDs(transID, lbidList);
	if ( rc != 0 )
	{
		if (rc == BRM::ERR_READONLY)
			return ERR_BRM_READ_ONLY;
		return rc;
		
	}
	
    //RETURN_ON_WE_ERROR(blockRsltnMgrPtr->getUncommittedLBIDs(transID, lbidList), ERR_BRM_GET_UNCOMM_LBID);

    if (isDebug(DEBUG_3)) {
        printf("\nIn rollBack, the transID is %d", transID);
        printf("\n\t the size of umcommittedLBIDs is "
#if __LP64__
            "%lu",
#else
            "%u",
#endif
            lbidList.size());
    }
    //@Bug 2314. Optimize the version buffer open times.
    currentVbOid = vbOid = 0;
    vbOid = currentVbOid;
    sourceFileInfo.oid = currentVbOid;
    sourceFileInfo.fPartition = 0;
    sourceFileInfo.fSegment = 0;
    size_t rootCnt = Config::DBRootCount();
    sourceFileInfo.fDbRoot = (vbOid % rootCnt) + 1;
    FILE*          pSourceFile;
    FILE*          pTargetFile;
    RETURN_ON_NULL((pSourceFile = openFile(sourceFileInfo, "r+b")), ERR_VB_FILE_NOT_EXIST);

    execplan::CalpontSystemCatalog* systemCatalogPtr =
        execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(sessionId);
    systemCatalogPtr->identity(execplan::CalpontSystemCatalog::EC);

    DbFileOp fileOp;
    fileOp.setTransId(transID);
	ChunkManager chunkManager;
	chunkManager.fileOp(&fileOp);
    FileOpenMap fileOpenList;
	//@bug3224, sort lbidList based on lbid
	sort(lbidList.begin(), lbidList.end());
	try {
      for(i = 0; i < lbidList.size(); i++) {
        verID = (VER_t) transID;
        range.start = lbidList[i];
        range.size = 1;
        lbidRangeList.push_back(range);
		//timer.start("vssLookup");
        // get version id
        RETURN_ON_WE_ERROR(
            blockRsltnMgrPtr->vssLookup(lbidList[i], verID, transID, vbFlag, true),
            ERR_BRM_LOOKUP_VERSION);
		//timer.stop("vssLookup");
        // copy buffer back
        //look for the block in extentmap
		//timer.start("lookupLocalEX");
        RETURN_ON_WE_ERROR(
            blockRsltnMgrPtr->lookupLocal(lbidList[i], /*transID*/verID, false, weOid,
            weDbRoot, wePartitionNum, weSegmentNum, weFbo), ERR_EXTENTMAP_LOOKUP);
		//timer.stop("lookupLocalEX");
        Column column;
        execplan::CalpontSystemCatalog::ColType colType = systemCatalogPtr->colType(weOid);
		columnOids[weOid] = weOid;
		//This must be a dict oid
		if (colType.columnOID == 0)
		{
			colType = systemCatalogPtr->colTypeDct(weOid);

			assert(colType.columnOID != 0);
			assert(colType.ddn.dictOID == weOid);
		}
        ColDataType colDataType = (ColDataType) colType.colDataType;
        ColType weColType;
        Convertor::convertColType(colDataType, weColType);
        column.colWidth = Convertor::getCorrectRowWidth(colDataType, colType.colWidth);
        column.colType = weColType;
        column.colDataType = colDataType;
        column.dataFile.fid = weOid;
        column.dataFile.fDbRoot = weDbRoot;
        column.dataFile.fPartition = wePartitionNum;
        column.dataFile.fSegment = weSegmentNum;
        column.compressionType = colType.compressionType;
        if (colType.compressionType == 0)
            fileOp.chunkManager(NULL);
        else
            fileOp.chunkManager(&chunkManager);

        if (isDebug(DEBUG_3))
#ifndef __LP64__
            printf(
                "\n\tuncommitted lbid - lbidList[i]=%lld weOid =%d weFbo=%d verID=%d, weDbRoot=%d",
                lbidList[i], weOid, weFbo, verID, weDbRoot);
#else
            printf(
                "\n\tuncommitted lbid - lbidList[i]=%ld weOid =%d weFbo=%d verID=%d, weDbRoot=%d",
                lbidList[i], weOid, weFbo, verID, weDbRoot);
#endif
        //look for the block in the version buffer
		//timer.start("lookupLocalVB");
        RETURN_ON_WE_ERROR(blockRsltnMgrPtr->lookupLocal(lbidList[i], verID, true, vbOid,
                            vbDbRoot, vbPartitionNum, vbSegmentNum, vbFbo), ERR_BRM_LOOKUP_FBO);
//timer.stop("lookupLocalVB");
        if (isDebug(DEBUG_3))
#ifndef __LP64__
        printf("\n\tuncommitted lbid - lbidList[i]=%lld vbOid =%d vbFbo=%d\n",
               lbidList[i], vbOid, vbFbo);
#else
        printf("\n\tuncommitted lbid - lbidList[i]=%ld vbOid =%d vbFbo=%d\n",
               lbidList[i], vbOid, vbFbo);
#endif

        //@Bug 2293 Version buffer file information cannot be obtained from lookupLocal
        if (vbOid != currentVbOid)
        {
            currentVbOid = vbOid;
            //cout << "VB file changed to " << vbOid << endl;
            fclose(pSourceFile);
            sourceFileInfo.oid = currentVbOid;
            sourceFileInfo.fPartition = 0;
            sourceFileInfo.fSegment = 0;
            sourceFileInfo.fDbRoot = (vbOid % rootCnt) + 1;
            RETURN_ON_NULL((pSourceFile = openFile(sourceFileInfo, "r+b")), ERR_VB_FILE_NOT_EXIST);
        }

        targetFileInfo.oid = weOid;
        targetFileInfo.fPartition = wePartitionNum;
        targetFileInfo.fSegment = weSegmentNum;
        targetFileInfo.fDbRoot = weDbRoot;
//      printf("\n\tsource file info - oid =%d fPartition=%d fSegment=%d, fDbRoot=%d", sourceFileInfo.oid, sourceFileInfo.fPartition, sourceFileInfo.fSegment, sourceFileInfo.fDbRoot);
//      printf("\n\ttarget file info - oid =%d fPartition=%d fSegment=%d, fDbRoot=%d", weOid, wePartitionNum, weSegmentNum, weDbRoot);
        if (column.compressionType != 0)
        {
            pTargetFile = fileOp.getFilePtr(column);
        }
        else if (fileOpenList.find(targetFileInfo) != fileOpenList.end())
        {
            pTargetFile = fileOpenList[targetFileInfo];
        }
        else
        {
            pTargetFile = openFile(targetFileInfo, "r+b");
            if (pTargetFile != NULL)
                fileOpenList[targetFileInfo] = pTargetFile;
        }

        if (pTargetFile == NULL)
        {
            rc = ERR_FILE_NOT_EXIST;
            goto cleanup;
        }

        std::vector<BRM::LBIDRange> lbidRangeList;
        BRM::LBIDRange   range;
        range.start = lbidList[i];
        range.size = 1;
        lbidRangeList.push_back(range);
        rc = blockRsltnMgrPtr->dmlLockLBIDRanges(lbidRangeList, transID);
        if (rc != 0 )
        {
            goto cleanup;
        }

//timer.start("copyVBBlock");
        rc =  copyVBBlock(pSourceFile, pTargetFile, vbFbo, weFbo, &fileOp, column);
//timer.stop("copyVBBlock");
        if (rc != NO_ERROR)
		{
			//@bug 4012, log an error to crit.log
			logging::Message::MessageID   msgId = 6;
			SimpleSysLog* slog = SimpleSysLog::instance();
			logging::Message m( msgId );
			logging::Message::Args args;

			std::ostringstream oss;
			WriteEngine::WErrorCodes   ec;
			oss << "Error in rolling back the block. lbid:oid:dbroot:partition:segment: " << lbidList[i] <<":" << weOid << ":" 
				<< weDbRoot <<":" << wePartitionNum<<":"<<weSegmentNum << " The error message is " << ec.errorString(rc);
			args.add( oss.str() );
			slog->logMsg(args, logging::LOG_TYPE_CRITICAL, msgId);
            goto cleanup;
		}
		fflush(pTargetFile);
        rc = blockRsltnMgrPtr->dmlReleaseLBIDRanges(lbidRangeList);
        if (rc != 0 )
        {
            goto cleanup;
        }
      }
	}
	catch ( runtime_error& ex )
	{
		rc = ERR_TBL_SYSCAT_ERROR;
	}
//timer.start("vbRollback");
   // rc =  blockRsltnMgrPtr->vbRollback(transID, lbidRangeList);
//	timer.stop("vbRollback");
    if (rc !=0)
    {
		if (rc == BRM::ERR_READONLY)
			return ERR_BRM_READ_ONLY;
		else
			return rc;
    }
    else
    {
        rc = NO_ERROR;
    }

cleanup:
    fclose(pSourceFile);

    //Close all target files
    //  -- chunkManager managed files
//	timer.start("flushChunks");
    if (rc == NO_ERROR)
	{
        rc = chunkManager.flushChunks(rc, columnOids); // write all active chunks to disk
		rc =  blockRsltnMgrPtr->vbRollback(transID, lbidRangeList);
	}
    else
        chunkManager.cleanUp(columnOids);          // close file w/o writing data to disk
		
//	timer.stop("flushChunks");
    //  -- other files
    FileOpenMap::const_iterator itor;
    for (itor = fileOpenList.begin(); itor != fileOpenList.end(); itor++)
    {
        fclose(itor->second);
    }
    return rc;
}

int BRMWrapper::writeVB(FILE* pFile, const VER_t transID, const OID oid, const i64 lbid,
    DbFileOp* pFileOp)
{
    int fbo;
    LBIDRange lbidRange;
    std::vector<i32> fboList;
    std::vector<LBIDRange> rangeList;

    lbidRange.start = lbid;
    lbidRange.size  = 1;
    rangeList.push_back(lbidRange);

    u_int16_t  dbRoot;
    u_int32_t  partition;
    u_int16_t  segment;
    RETURN_ON_ERROR(getFboOffset(lbid, dbRoot, partition, segment, fbo));

    fboList.push_back(fbo);
	int rc = writeVB(pFile, transID, oid, fboList, rangeList, pFileOp);
	//writeVBEnd(transID,rangeList);
    return rc;
}

// Eliminates blocks that have already been versioned by transaction transID
void BRMWrapper::pruneLBIDList(VER_t transID, vector<LBIDRange> *rangeList,
		vector<i32> *fboList) const
{
	vector<LBID_t> lbids;
    vector<BRM::VSSData> vssData;
    VER_t verID = (VER_t) transID;
    uint i;
    int rc;
	vector<LBIDRange> newrangeList;
	vector<i32> newfboList;

    for (i = 0; i < rangeList->size(); i++)
    	lbids.push_back((*rangeList)[i].start);

    rc = blockRsltnMgrPtr->bulkVSSLookup(lbids, verID, transID, &vssData);
    if (rc != 0)
    	return;   // catch the error in a more appropriate place

    for (i = 0; i < vssData.size(); i++) {
        BRM::VSSData &vd = vssData[i];
        // Check whether this transaction has already versioned this block
        if (vd.returnCode != 0 || vd.verID != transID) {
            newrangeList.push_back((*rangeList)[i]);
            newfboList.push_back((*fboList)[i]);
        }
    }
    newrangeList.swap(*rangeList);
    newfboList.swap(*fboList);
}

int BRMWrapper::writeVB(FILE* pSourceFile, const VER_t transID, const OID weOid,
    std::vector<i32>& fboList, std::vector<LBIDRange>& rangeList, DbFileOp* pFileOp)
{
    int rc;
    size_t i;
    size_t l;
    size_t rangeListCount;
    size_t k = 0;
    std::vector<VBRange> freeList;
    FILE* pTargetFile;
    int32_t vbOid;

    if (isDebug(DEBUG_3))
    {
        cout << "\nIn writeVB" << endl;
        cout << "\tTransId=" << transID << endl;
        cout << "\t weOid : " << weOid << endl;
        cout << "\trangeList size=" << rangeList.size() << endl;
        for (i = 0; i < rangeList.size(); i++)
        {
            cout << "\t weLBID start : " << rangeList[i].start;
            cout << " weSize : " << rangeList[i].size << endl;
        }

        cout << "\tfboList size=" << fboList.size() << endl;
        for (i = 0; i < fboList.size(); i++)
            cout << "\t weFbo : " << fboList[i] << endl;
    }

    pruneLBIDList(transID, &rangeList, &fboList);
    if (rangeList.empty())   // all blocks have already been versioned
    	return NO_ERROR;

	rc = blockRsltnMgrPtr->beginVBCopy(transID, rangeList, freeList);

	if (rc != NO_ERROR)
	{
		switch (rc)
		{
			case ERR_DEADLOCK: return ERR_BRM_DEAD_LOCK;
			case ERR_VBBM_OVERFLOW: return ERR_BRM_VB_OVERFLOW;
			case ERR_NETWORK: return ERR_BRM_NETWORK;
			case ERR_READONLY: return ERR_BRM_READ_ONLY;
			default: return ERR_BRM_BEGIN_COPY;
		}
	}

    if (isDebug(DEBUG_3))
    {
        cout << "\nAfter beginCopy and get a freeList=" << freeList.size() << endl;
        cout << "\tfreeList size=" << freeList.size() << endl;
        for (i = 0; i < freeList.size(); i++)
        {
            cout << "\t VBOid : " << freeList[i].vbOID ;
            cout << " VBFBO : " << freeList[i].vbFBO ;
            cout << " Size : " << freeList[i].size << endl;
        }
    }

    //@Bug 2371 The assumption of all entries in the freelist belong to the same version buffer file is wrong
    // Open the first version buffer file
    File fileInfo;
    size_t rootCnt = Config::DBRootCount();
    fileInfo.oid = freeList[0].vbOID;
    fileInfo.fPartition = 0;
    fileInfo.fSegment = 0;
    fileInfo.fDbRoot = (freeList[0].vbOID % rootCnt) + 1;

    pTargetFile = openFile(fileInfo, "r+b", true);
    if (pTargetFile == NULL)
    {
        pTargetFile = openFile(fileInfo, "w+b");
        if (pTargetFile == NULL)
        {
            rc = ERR_FILE_NOT_EXIST;
            goto cleanup;
        }
        else
        {
            fclose(pTargetFile);
            pTargetFile = openFile(fileInfo, "r+b",true);
            if (pTargetFile == NULL)
            {
                rc = ERR_FILE_NOT_EXIST;
                goto cleanup;
            }
        }
    }

    k = 0;
    vbOid = freeList[0].vbOID;
    rangeListCount = 0;
    for (i = 0; i < freeList.size(); i++)
    {
        rangeListCount += k;
        l = k; // store the number of blocks processed till now for this file
        if (vbOid == freeList[i].vbOID)
        {
            // This call to copyVBBlock will consume whole of the freeList[i]
            rc = copyVBBlock(pSourceFile, weOid, pTargetFile, fileInfo.oid,
                fboList, freeList[i], k, pFileOp, rangeListCount);

            if (rc != NO_ERROR)
                goto cleanup;

            for (; l < k; l++)
            {
                rc = blockRsltnMgrPtr->writeVBEntry(transID, rangeList[rangeListCount+l].start,
                    freeList[i].vbOID, freeList[i].vbFBO + l);

                if (rc != NO_ERROR)
				{
					switch (rc)
					{
						case ERR_DEADLOCK: rc = ERR_BRM_DEAD_LOCK;
						case ERR_VBBM_OVERFLOW: rc = ERR_BRM_VB_OVERFLOW;
						case ERR_NETWORK: rc = ERR_BRM_NETWORK;
						case ERR_READONLY: rc = ERR_BRM_READ_ONLY;
						default: rc = ERR_BRM_WR_VB_ENTRY;
					}
                    goto cleanup;
				}
            }
        }
        else
        {
            l = 0; //reset the begining of vb file offset
            k = 0;
            vbOid = freeList[i].vbOID;
            fileInfo.oid = freeList[i].vbOID;
            fileInfo.fPartition = 0;
            fileInfo.fSegment = 0;
            fileInfo.fDbRoot = (freeList[i].vbOID % rootCnt) + 1;
            pTargetFile = openFile(fileInfo, "r+b", true);
            if (pTargetFile == NULL)
            {
                pTargetFile = openFile(fileInfo, "w+b");
                if (pTargetFile == NULL)
                {
                    rc = ERR_FILE_NOT_EXIST;
                    goto cleanup;
                }
                else
                {
                    fclose(pTargetFile);
                    pTargetFile = openFile(fileInfo, "r+b", true);
                    if (pTargetFile == NULL)
                    {
                        rc = ERR_FILE_NOT_EXIST;
                        goto cleanup;
                    }
                }
            }

            // This call to copyVBBlock will consume whole of the freeList[i]
            rc = copyVBBlock(pSourceFile, weOid, pTargetFile, fileInfo.oid,
                fboList, freeList[i], k, pFileOp, rangeListCount);

            if (rc != NO_ERROR)
                goto cleanup;

            for (; l < k; l++)
            {
                rc = blockRsltnMgrPtr->writeVBEntry(transID, rangeList[rangeListCount+l].start,
                    freeList[i].vbOID, freeList[i].vbFBO + l);

                if (rc != NO_ERROR)
				{
					switch (rc)
					{
						case ERR_DEADLOCK: rc = ERR_BRM_DEAD_LOCK;
						case ERR_VBBM_OVERFLOW: rc = ERR_BRM_VB_OVERFLOW;
						case ERR_NETWORK: rc = ERR_BRM_NETWORK;
						case ERR_READONLY: rc = ERR_BRM_READ_ONLY;
						default: rc = ERR_BRM_WR_VB_ENTRY;
					}
                    goto cleanup;
				}
            }
        }
    }
    if (pTargetFile)
        fflush(pTargetFile);
    return rc;
cleanup:
    if (pTargetFile)
        fflush(pTargetFile);
    writeVBEnd(transID,rangeList);
    return rc;
}

void BRMWrapper::writeVBEnd(const VER_t transID, std::vector<LBIDRange>& rangeList)
{
	blockRsltnMgrPtr->endVBCopy(transID, rangeList);
}
	
int BRMWrapper::saveState()
{
    int rc=0;

    rc = blockRsltnMgrPtr->saveState();
    if (rc != NO_ERROR)
        rc = ERR_BRM_SAVE_STATE;

    return rc;

}

//------------------------------------------------------------------------------
// Save BRM return code in thread specific storage for later retrieval
//------------------------------------------------------------------------------
void BRMWrapper::saveBrmRc(int brmRc)
{
    int* dataPtr = m_ThreadDataPtr.get();
    if (dataPtr == 0)
    {
        dataPtr = new int(brmRc);
        m_ThreadDataPtr.reset(dataPtr);
    }
    else
    {
        *dataPtr = brmRc;
    }
}

//------------------------------------------------------------------------------
// Get latest BRM return code from thread specific storage and reset to OK,
// so that the "leftover" BRM return code will not erroneously get picked up
// and reported by subsequent calls to BRM.
//------------------------------------------------------------------------------
/* static */
int BRMWrapper::getBrmRc(bool reset)
{
    if (m_ThreadDataPtr.get() == 0)
       return BRM::ERR_OK;

    int brmRc = *m_ThreadDataPtr;
    if (reset)
        m_ThreadDataPtr.reset(new int(BRM::ERR_OK));

    return brmRc;
}

} //end of namespace
// vim:ts=4 sw=4:

