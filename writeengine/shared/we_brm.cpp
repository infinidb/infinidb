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
* $Id: we_brm.cpp 4737 2013-08-14 20:45:46Z bwilkinson $
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
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
using namespace boost;

#include "we_brm.h"

#include "calpontsystemcatalog.h"
#include "we_dbfileop.h"
#include "we_convertor.h"
#include "we_chunkmanager.h"
#include "we_colopcompress.h"
#include "we_dctnrycompress.h"
#include "we_simplesyslog.h"
#include "we_config.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"
using namespace idbdatafile;

using namespace BRM;
using namespace execplan;

#include "atomicops.h"
#include "cacheutils.h"
/** Namespace WriteEngine */
namespace WriteEngine
{
BRMWrapper* volatile BRMWrapper::m_instance = NULL;
boost::thread_specific_ptr<int> BRMWrapper::m_ThreadDataPtr;
boost::mutex BRMWrapper::m_instanceCreateMutex;

#ifdef _MSC_VER
   __declspec(dllexport)
#endif

bool     BRMWrapper::m_useVb    = true;
OID      BRMWrapper::m_curVBOid  = INVALID_NUM;
IDBDataFile* BRMWrapper::m_curVBFile = NULL;
boost::mutex vbFileLock;
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

typedef std::map< File, IDBDataFile*, fileInfoCompare > FileOpenMap;

//------------------------------------------------------------------------------
// Set up an Auto-increment sequence for specified Column OID, starting at
// startNextValue.  Column width is required to monitor if/when the sequence
// reaches the max integer value for the given column width.
//------------------------------------------------------------------------------
int BRMWrapper::startAutoIncrementSequence(
    OID          colOID,
    uint64_t    startNextValue,
    uint32_t    colWidth,
    execplan::CalpontSystemCatalog::ColDataType colDataType,
    std::string& errMsg)
{
    int rc = NO_ERROR;

    try
    {
        blockRsltnMgrPtr->startAISequence( colOID, startNextValue, colWidth, colDataType );
    }
    catch (std::exception& ex)
    {
        errMsg = ex.what();
        rc     = ERR_AUTOINC_START_SEQ;
    }
    
    return rc;
}

//------------------------------------------------------------------------------
// Reserve a range of auto-increment numbers for the specified column OID.
//------------------------------------------------------------------------------
int BRMWrapper::getAutoIncrementRange(
    OID          colOID,
    uint64_t    count,
    uint64_t&   firstNum,
    std::string& errMsg)
{
    int rc = NO_ERROR;

    try
    {
        uint64_t firstNumArg = 0;
        bool gotFullRange = blockRsltnMgrPtr->getAIRange(
            colOID, count, &firstNumArg );
        if (gotFullRange)
        {
            firstNum = firstNumArg;
        }
        else
        {
            rc = ERR_AUTOINC_GEN_EXCEED_MAX;
			WriteEngine::WErrorCodes ec;
            errMsg = ec.errorString(rc);
        }
    }
    catch (std::exception& ex)
    {
        errMsg = ex.what();
        rc     = ERR_AUTOINC_GET_RANGE;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Allocate a stripe of column extents for a table
//------------------------------------------------------------------------------
int   BRMWrapper::allocateStripeColExtents(
    const std::vector<BRM::CreateStripeColumnExtentsArgIn>& cols,
    uint16_t    dbRoot,
    uint32_t&   partition,
    uint16_t&   segmentNum,
    std::vector<BRM::CreateStripeColumnExtentsArgOut>& extents)
{
    int rc = blockRsltnMgrPtr->createStripeColumnExtents(
        cols, dbRoot, partition, segmentNum, extents );
    rc = getRC( rc, ERR_BRM_ALLOC_EXTEND );
    if (rc == NO_ERROR)
    {
        if (cols.size() != extents.size())
            rc = ERR_BRM_BAD_STRIPE_CNT;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Allocate an extent to the exact file specified by the column OID, DBRoot,
// partition, and segment.
//------------------------------------------------------------------------------
int   BRMWrapper::allocateColExtentExactFile(
    const OID   oid,
    const uint32_t colWidth,
    uint16_t   dbRoot,
    uint32_t   partition,
    uint16_t   segment,
    execplan::CalpontSystemCatalog::ColDataType colDataType,
    LBID_t&     startLbid,
    int&        allocSize,
    uint32_t&  startBlock)
{
    int rc = blockRsltnMgrPtr->createColumnExtentExactFile(
            (int)oid, colWidth, dbRoot, partition, segment, colDataType,
            startLbid, allocSize, startBlock);

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
    uint16_t   dbRoot,
    uint32_t   partition,
    uint16_t   segment,
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
// Inform BRM to delete certain oid
//------------------------------------------------------------------------------
int BRMWrapper::deleteOid(const OID oid)
{
    int rc = blockRsltnMgrPtr->deleteOID(oid);
    return getRC(rc, ERR_BRM_DEL_OID);
}

//------------------------------------------------------------------------------
// Inform BRM to delete certain oids
//------------------------------------------------------------------------------
int BRMWrapper::deleteOIDsFromExtentMap(const std::vector<int32_t>& oids)
{
    int rc = blockRsltnMgrPtr->deleteOIDs(oids);
    return getRC(rc, ERR_BRM_DEL_OID);
}

//------------------------------------------------------------------------------
// Get BRM information based on a specfic OID, DBRoot, partition, and segment.
//------------------------------------------------------------------------------
int   BRMWrapper::getBrmInfo(const OID oid,
    const uint32_t partition,
    const uint16_t segment,
    const int       fbo,
    LBID_t&         lbid)
{
    // SHARED_NOTHING: lookupLocal() usage okay if segNum unique.
    // If segment number is not unique across physical partition, then would
    // need to pass DBRoot to lookupLocal().
    int rc = blockRsltnMgrPtr->lookupLocal((int)oid ,
        partition, segment,
        (uint32_t)fbo,lbid);
     return getRC(rc, ERR_BRM_LOOKUP_LBID);
};

//------------------------------------------------------------------------------
// Get starting LBID from BRM for a specfic OID, partition, segment, and
// block offset.
//------------------------------------------------------------------------------
int   BRMWrapper::getStartLbid(const OID oid,
    const uint32_t partition,
    const uint16_t segment,
    const int       fbo,
    LBID_t&         lbid)
{
    // SHARED_NOTHING: lookupLocalStartLbid() usage okay if segNum unique.
    // If segment number is not unique across physical partition, then would
    // need to pass DBRoot to lookupLocalStartLbid().
    int rc = blockRsltnMgrPtr->lookupLocalStartLbid((int)oid ,
            partition, segment,
            (uint32_t)fbo,lbid);

    return getRC(rc, ERR_BRM_LOOKUP_START_LBID);
}

//------------------------------------------------------------------------------
// Get the real physical offset based on the LBID
//------------------------------------------------------------------------------
int  BRMWrapper::getFboOffset(const uint64_t lbid,
    uint16_t& dbRoot,
    uint32_t& partition,
    uint16_t& segment,
    int&       fbo)
{
    int oid;
    // according to Patric, extendmap don't need vbflag, thus verid =0
//  int rc = blockRsltnMgrPtr->lookup((uint64_t)lbid, 0, false, oid, (uint32_t&)fbo);
//  return getRC(rc, ERR_BRM_LOOKUP_FBO);
    return getFboOffset(lbid, oid, dbRoot, partition, segment, fbo);
}

//------------------------------------------------------------------------------
// Get the real physical offset based on the LBID
//------------------------------------------------------------------------------
int  BRMWrapper::getFboOffset(const uint64_t lbid, int& oid,
    uint16_t& dbRoot,
    uint32_t& partition,
    uint16_t& segment,
    int&       fbo)
{
    // according to Patric, extendmap don't need vbflag, thus verid =0
    int rc = blockRsltnMgrPtr->lookupLocal((uint64_t)lbid, 0, false, (BRM::OID_t&)oid,
        dbRoot, partition, segment,
        (uint32_t&)fbo);
    return getRC(rc, ERR_BRM_LOOKUP_FBO);
}

//------------------------------------------------------------------------------
// create singleton instance.
// @bug 5318 Add mutex lock to make more thread safe.
//------------------------------------------------------------------------------
BRMWrapper* BRMWrapper::getInstance()
{
    if (m_instance == 0) {
        boost::mutex::scoped_lock lock(m_instanceCreateMutex);
        if (m_instance == 0) {
            BRMWrapper* tmp = new BRMWrapper();

            // Memory barrier makes sure the m_instance assignment is not
            // mingled with the constructor code
			atomicops::atomicMb();
            m_instance = tmp;
        }
    }
    return m_instance;
}

//------------------------------------------------------------------------------
// Get HWM/extent information for each DBRoot in the specified column OID,
// for the current PM.
//------------------------------------------------------------------------------
int BRMWrapper::getDbRootHWMInfo( const OID oid,
    BRM::EmDbRootHWMInfo_v& emDbRootHwmInfos)
{
    int rc = NO_ERROR;
    uint16_t localModuleID = Config::getLocalModuleID();
    rc = blockRsltnMgrPtr->getDbRootHWMInfo(
        oid, localModuleID, emDbRootHwmInfos);

    // Temporary code for testing shared nothing
#if 0
    EmDbRootHWMInfo info;
    info.dbRoot         = 1;
    info.partitionNum   = 0;
    info.segmentNum     = 0;
    info.localHWM       = 8192;              // 8192, 16384;
    info.fbo            = 11;
    info.hwmExtentIndex = 101;
    info.totalBlocks    = 1001;
    emDbRootHwmInfos.push_back( info );

    info.dbRoot         = 2;
    info.partitionNum   = 0;
    info.segmentNum     = 1;
    info.localHWM       = 8192;              // 8192, 16384;
    info.fbo            = 22;
    info.hwmExtentIndex = 202;
    info.totalBlocks    = 2002;
    emDbRootHwmInfos.push_back( info );

    info.dbRoot         = 3;
    info.partitionNum   = 0;
    info.segmentNum     = 2;
    info.localHWM       = 4000;              // 4000, 10000;
    info.fbo            = 33;
    info.hwmExtentIndex = 303;
    info.totalBlocks    = 3003;
    emDbRootHwmInfos.push_back( info );

    info.dbRoot         = 4;
    info.partitionNum   = 0;
    info.segmentNum     = 3;
    info.localHWM       = 0;                 // 0, 8192;
    info.fbo            = 44;
    info.hwmExtentIndex = 404;
    info.totalBlocks    = 0;                 // 0, 1
    emDbRootHwmInfos.push_back( info );
#endif
    return getRC( rc, ERR_BRM_DBROOT_HWMS );
}

//------------------------------------------------------------------------------
// Is BRM in read/write state.
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
// Is the system being shutdown?
//------------------------------------------------------------------------------
int BRMWrapper::isShutdownPending(bool& bRollback, bool& bForce)
{
    int rc = blockRsltnMgrPtr->getSystemShutdownPending(bRollback, bForce);
    if (rc < 0)
        return ERR_BRM_GET_SHUTDOWN;
    else if (rc > 0)
        return ERR_BRM_SHUTDOWN;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Is the system int write suspend mode?
//------------------------------------------------------------------------------
int BRMWrapper::isSuspendPending()
{
    bool bRollback;
    int rc = blockRsltnMgrPtr->getSystemSuspendPending(bRollback);
    if (rc < 0)
        return ERR_BRM_GET_SUSPEND;
    else if (rc > 0)
        return ERR_BRM_SUSPEND;

    rc = blockRsltnMgrPtr->getSystemSuspended();
    if (rc < 0)
        return ERR_BRM_GET_SUSPEND;
    else if (rc > 0)
        return ERR_BRM_SUSPEND;

    return NO_ERROR;
}
// 
//------------------------------------------------------------------------------
// Request to BRM to save its current state.
//------------------------------------------------------------------------------
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
// Acquire a table lock for the specified table, owner, and pid.
// Resulting lock is returned in lockID.  If table is already locked, then the
// owner, pid, session id, and transaction of the current lock are returned.
//------------------------------------------------------------------------------
int BRMWrapper::getTableLock( OID tableOid,
    std::string& ownerName,
    uint32_t&   processID,
    int32_t&     sessionID,
    int32_t&     transID,
    uint64_t&   lockID,
    std::string& errMsg)
{
    int rc = NO_ERROR;
    lockID = 0;

    std::vector<uint32_t> pmList;
    pmList.push_back( Config::getLocalModuleID() );

    try
    {
        lockID = blockRsltnMgrPtr->getTableLock(
            pmList, tableOid, &ownerName, &processID, &sessionID, &transID,
            BRM::LOADING);
    }
    catch (std::exception& ex)
    {
        errMsg = ex.what();
        rc     = ERR_TBLLOCK_GET_LOCK;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Change the state of the specified lock to the indicated lock state.
//------------------------------------------------------------------------------
int BRMWrapper::changeTableLockState( uint64_t lockID,
    BRM::LockState lockState,
    bool& bChanged,
    std::string& errMsg )
{
    int rc = NO_ERROR;
    bChanged = false;

    try
    {
        bChanged = blockRsltnMgrPtr->changeState( lockID, lockState );
    }
    catch (std::exception& ex)
    {
        errMsg = ex.what();
        rc     = ERR_TBLLOCK_CHANGE_STATE;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Release the table lock associated with the specified lockID.
// bReleased will indicate whether the lock was released or not.
//------------------------------------------------------------------------------
int BRMWrapper::releaseTableLock( uint64_t lockID,
    bool& bReleased,
    std::string& errMsg )
{
    int rc = NO_ERROR;
    bReleased = false;

    try
    {
        bReleased = blockRsltnMgrPtr->releaseTableLock( lockID );
    }
    catch (std::exception& ex)
    {
        errMsg = ex.what();
        rc     = ERR_TBLLOCK_RELEASE_LOCK;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Get information about the specified table lock.
//------------------------------------------------------------------------------
int BRMWrapper::getTableLockInfo( uint64_t lockID,
    BRM::TableLockInfo* lockInfo,
    bool& bLockExists,
    std::string& errMsg )
{
    int rc = NO_ERROR;
    
    try
    {
        bLockExists = blockRsltnMgrPtr->getTableLockInfo( lockID, lockInfo );
    }
    catch (std::exception& ex)
    {
        errMsg = ex.what();
        rc     = ERR_TBLLOCK_GET_INFO;
    }

    return rc;
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

//------------------------------------------------------------------------------
//------------------------------------------------------------------------------
//------------------------------------------------------------------------------
// Versioning Functions Start Here and go to the end of the file
//------------------------------------------------------------------------------
//------------------------------------------------------------------------------
//------------------------------------------------------------------------------

#define MAX_VERSION_BUFFER_SIZE 1024

int BRMWrapper::copyVBBlock (IDBDataFile *pSourceFile, const OID sourceOid,
                IDBDataFile *pTargetFile, const OID targetOid,
                const std::vector < uint32_t > &fboList,
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
		//std::cout << "for oid " << sourceOid << " startOffsetInInput is " << startOffsetInInput << endl;
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
				//std::cout << " error when processing startOffsetInInput:fbo = " << startOffsetInInput <<":"<<fboList[startOffsetInInput]<<std::endl;
                return ERR_BRM_VB_COPY_READ;
            }

            // update offsets and counters
            startOffsetInInput += numCopyBlocks;
            numBlocksToBeWritten += numCopyBlocks;
        }


        // write buffer to the target file if there is data read into it
        if (numBlocksToBeWritten > 0) {
            // Seek into target file
            size_t tgtOffset = (freeList.vbFBO + outputFileWritePointer) * BYTE_PER_BLOCK;
            int wc = pTargetFile->seek(tgtOffset, 0);
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

            size_t rwSize = pTargetFile->write(buffer, BYTE_PER_BLOCK * numBlocksToBeWritten) / BYTE_PER_BLOCK;

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

int BRMWrapper::copyVBBlock(IDBDataFile* pSourceFile, IDBDataFile* pTargetFile,
                                  const uint64_t sourceFbo, const uint64_t targetFbo,
                                  DbFileOp* fileOp, const Column& column)
{
    size_t rwSize;
    unsigned char buf[BYTE_PER_BLOCK];
    //add new error code for versioning error
    rwSize = pSourceFile->pread(buf, sourceFbo*BYTE_PER_BLOCK, BYTE_PER_BLOCK);
    if ((int) rwSize != BYTE_PER_BLOCK)
        return ERR_BRM_VB_COPY_READ;

    rwSize = fileOp->restoreBlock(pTargetFile, buf, targetFbo);
    if ((int) rwSize != BYTE_PER_BLOCK)
        return ERR_BRM_VB_COPY_WRITE;
    else
        return NO_ERROR;
}

int BRMWrapper::commit(const VER_t transID)
{
    int rc = blockRsltnMgrPtr->vbCommit(transID);
    return getRC(rc, ERR_BRM_COMMIT);
}

IDBDataFile* BRMWrapper::openFile(const File& fileInfo, const char* mode, const bool bCache)
{
    IDBDataFile* pFile;
    char     fileName[FILE_NAME_SIZE];

    if (bCache && fileInfo.oid == m_curVBOid && m_curVBFile != NULL)
        return m_curVBFile;

    FileOp fileOp;
    if (fileInfo.oid < 1000){ //Cannot have more than 999 version buffer files tp prevent oid collision
        RETURN_ON_WE_ERROR(fileOp.getVBFileName (fileInfo.oid, fileName),NULL);
    }
    else {
          RETURN_ON_WE_ERROR(fileOp.getFileName (fileInfo.oid, fileName,fileInfo.fDbRoot,
               fileInfo.fPartition, fileInfo.fSegment),NULL);
    }
    // disable buffering for versionbuffer file by passing USE_NOVBUF
    pFile = IDBDataFile::open(
							IDBPolicy::getType( fileName, IDBPolicy::WRITEENG ),
							fileName,
							mode,
							IDBDataFile::USE_NOVBUF );

    if (pFile && bCache) {
        if (m_curVBOid != (OID)INVALID_NUM) {
            if (m_curVBOid != fileInfo.oid && m_curVBFile != NULL)
            {
            	delete m_curVBFile;
            	m_curVBFile = 0;
            }
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
    uint32_t   vbFbo, weFbo;
    size_t      i;
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
    IDBDataFile* pSourceFile;
    IDBDataFile* pTargetFile;
    RETURN_ON_NULL((pSourceFile = openFile(sourceFileInfo, "r+b")), ERR_VB_FILE_NOT_EXIST);

    boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatalogPtr =
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
    	QueryContext verID(transID);
    	VER_t outVer;
        range.start = lbidList[i];
        range.size = 1;
        lbidRangeList.push_back(range);
		//timer.start("vssLookup");
        // get version id
        RETURN_ON_WE_ERROR(
            blockRsltnMgrPtr->vssLookup(lbidList[i], verID, transID, &outVer, &vbFlag, true),
            ERR_BRM_LOOKUP_VERSION);
		//timer.stop("vssLookup");
        // copy buffer back
        //look for the block in extentmap
		//timer.start("lookupLocalEX");
        RETURN_ON_WE_ERROR(
            blockRsltnMgrPtr->lookupLocal(lbidList[i], outVer, false, weOid,
            weDbRoot, wePartitionNum, weSegmentNum, weFbo), ERR_EXTENTMAP_LOOKUP);
		//timer.stop("lookupLocalEX");
        Column column;
        execplan::CalpontSystemCatalog::ColType colType = systemCatalogPtr->colType(weOid);
		columnOids[weOid] = weOid;
		//This must be a dict oid
		if (colType.columnOID == 0)
		{
			colType = systemCatalogPtr->colTypeDct(weOid);

			idbassert(colType.columnOID != 0);
			idbassert(colType.ddn.dictOID == weOid);
		}
        CalpontSystemCatalog::ColDataType colDataType = colType.colDataType;
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
                lbidList[i], weOid, weFbo, outVer, weDbRoot);
#else
            printf(
                "\n\tuncommitted lbid - lbidList[i]=%ld weOid =%d weFbo=%d verID=%d, weDbRoot=%d",
                lbidList[i], weOid, weFbo, outVer, weDbRoot);
#endif
        //look for the block in the version buffer
		//timer.start("lookupLocalVB");
        RETURN_ON_WE_ERROR(blockRsltnMgrPtr->lookupLocal(lbidList[i], outVer, true, vbOid,
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
            delete pSourceFile;
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
            pTargetFile = fileOp.getFilePtr(column, false); // @bug 5572 HDFS tmp file
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
      }
	}
	catch ( runtime_error& )
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
	delete pSourceFile;

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
    	delete itor->second;
    }
    return rc;
}

int BRMWrapper::rollBackBlocks(const VER_t transID, int sessionId)
{
    if (idbdatafile::IDBPolicy::useHdfs())
        return 0;
		
    std::vector<LBID_t> lbidList;
    OID_t       vbOid;
    OID_t       weOid;
    OID_t       currentVbOid = static_cast<OID_t>(-1);
    uint32_t   vbFbo, weFbo;
    size_t      i;
    VER_t       verID = (VER_t) transID;

    uint16_t   vbDbRoot, weDbRoot, vbSegmentNum, weSegmentNum;
    uint32_t   vbPartitionNum, wePartitionNum;
    File  sourceFileInfo;
    File  targetFileInfo;
	Config config;
	config.initConfigCache();
	std::vector<uint16_t> rootList;
	config.getRootIdList( rootList );
	std::map<uint16_t, uint16_t> dbrootPmMap;
	
	for (i=0; i < rootList.size(); i++)
	{
		dbrootPmMap[rootList[i]] = rootList[i];
	}
	
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
	//std::cout << "rollBackBlocks get uncommited lbid " << lbidList.size() << std::endl;
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

    boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatalogPtr =
        execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(sessionId);
    systemCatalogPtr->identity(execplan::CalpontSystemCatalog::EC);

    DbFileOp fileOp;
    fileOp.setTransId(transID);
	ChunkManager chunkManager;
	chunkManager.fileOp(&fileOp);
    FileOpenMap fileOpenList;
	//@bug3224, sort lbidList based on lbid
	sort(lbidList.begin(), lbidList.end());
	
    IDBDataFile* pSourceFile = 0;
    IDBDataFile* pTargetFile = 0;
	std::map<uint16_t, uint16_t>::const_iterator dbrootPmMapItor;
	std::string errorMsg;
	
	std::vector<BRM::FileInfo> files;
    for(i = 0; i < lbidList.size(); i++) {
        verID = (VER_t) transID;
		//timer.start("vssLookup");
        // get version id

        verID = blockRsltnMgrPtr->getHighestVerInVB(lbidList[i], transID);
		if (verID < 0)
		{
			std::ostringstream oss;
			BRM::errString(verID, errorMsg);
			oss << "vssLookup error encountered while looking up lbid " << lbidList[i] << " and error code is " << verID << " with message " << errorMsg;
			throw std::runtime_error(oss.str());	
		}
		//timer.stop("vssLookup");
        // copy buffer back
        //look for the block in extentmap
		//timer.start("lookupLocalEX");
		rc = blockRsltnMgrPtr->lookupLocal(lbidList[i], /*transID*/verID, false, weOid,
            weDbRoot, wePartitionNum, weSegmentNum, weFbo);
		
		if ( rc != 0)
		{
			std::ostringstream oss;
			BRM::errString(rc, errorMsg);
			oss << "lookupLocal from extent map error encountered while looking up lbid:verID " << lbidList[i] << ":"
				<<(uint32_t)verID << " and error code is " << rc << " with message " << errorMsg;
			throw std::runtime_error(oss.str());	
		}
		
		//Check whether this lbid is on this PM.
		dbrootPmMapItor = dbrootPmMap.find(weDbRoot);
		
		if (dbrootPmMapItor == dbrootPmMap.end())
			continue;
		
		//timer.stop("lookupLocalEX");
        Column column;
        execplan::CalpontSystemCatalog::ColType colType = systemCatalogPtr->colType(weOid);
		columnOids[weOid] = weOid;
		//This must be a dict oid
		if (colType.columnOID == 0)
		{
			colType = systemCatalogPtr->colTypeDct(weOid);

			idbassert(colType.columnOID != 0);
			idbassert(colType.ddn.dictOID == weOid);
		}
        CalpontSystemCatalog::ColDataType colDataType = colType.colDataType;
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
		
		BRM::FileInfo aFile;	
		aFile.oid = weOid;		
		aFile.partitionNum = wePartitionNum;
		aFile.dbRoot = weDbRoot;
		aFile.segmentNum = weSegmentNum;
		aFile.compType = colType.compressionType;
		files.push_back(aFile);
		
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
		rc = blockRsltnMgrPtr->lookupLocal(lbidList[i], verID, true, vbOid,
                            vbDbRoot, vbPartitionNum, vbSegmentNum, vbFbo);
		if ( rc != 0)
		{
			std::ostringstream oss;
			BRM::errString(rc, errorMsg);
			oss << "lookupLocal from version buffer error encountered while looking up lbid:verID " << lbidList[i] << ":"
				<<(uint32_t)verID << " and error code is " << rc << " with message " << errorMsg;
			throw std::runtime_error(oss.str());	
		}
       
		if (pSourceFile == 0) //@Bug 2314. Optimize the version buffer open times.
		{
			currentVbOid = vbOid;
			sourceFileInfo.oid = currentVbOid;
			sourceFileInfo.fPartition = 0;
			sourceFileInfo.fSegment = 0;
			sourceFileInfo.fDbRoot = weDbRoot;
			errno = 0;
			pSourceFile = openFile(sourceFileInfo, "r+b");
			if (pSourceFile == NULL)
			{
				std::ostringstream oss;
				Convertor::mapErrnoToString(errno, errorMsg);
				oss << "Error encountered while opening version buffer file oid:dbroot = " << currentVbOid << ":"
					<<weDbRoot << " and error message:" << errorMsg;
				throw std::runtime_error(oss.str());
			}
		}
			
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
            delete pSourceFile;
            sourceFileInfo.oid = currentVbOid;
            sourceFileInfo.fPartition = 0;
            sourceFileInfo.fSegment = 0;
            sourceFileInfo.fDbRoot = weDbRoot;
			errno = 0;
			pSourceFile = openFile(sourceFileInfo, "r+b");
			if (pSourceFile == NULL)
			{
				std::ostringstream oss;
				Convertor::mapErrnoToString(errno, errorMsg);
				oss << "Error encountered while opening version buffer file oid:dbroot = " << currentVbOid << ":"
					<<weDbRoot << " and error message:" << errorMsg;
				throw std::runtime_error(oss.str());
			}
        }

        targetFileInfo.oid = weOid;
        targetFileInfo.fPartition = wePartitionNum;
        targetFileInfo.fSegment = weSegmentNum;
        targetFileInfo.fDbRoot = weDbRoot;
//      printf("\n\tsource file info - oid =%d fPartition=%d fSegment=%d, fDbRoot=%d", sourceFileInfo.oid, sourceFileInfo.fPartition, sourceFileInfo.fSegment, sourceFileInfo.fDbRoot);
//      printf("\n\ttarget file info - oid =%d fPartition=%d fSegment=%d, fDbRoot=%d", weOid, wePartitionNum, weSegmentNum, weDbRoot);
		//Check whether the file is on this pm.
			
        if (column.compressionType != 0)
        {
            pTargetFile = fileOp.getFilePtr(column, false); // @bug 5572 HDFS tmp file
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
				std::ostringstream oss;
				Convertor::mapErrnoToString(errno, errorMsg);
				oss << "Error encountered while opening source file oid:dbroot:partition:segment = " << weOid << ":"
					<<weDbRoot << ":"<<wePartitionNum <<":"<< weSegmentNum <<" and error message:" << errorMsg;
				errorMsg = oss.str();
				goto cleanup;	
		}
			
//timer.start("copyVBBlock");
		std::vector<BRM::LBIDRange> lbidRangeList;
		BRM::LBIDRange   range;
		range.start = lbidList[i];
        range.size = 1;
        lbidRangeList.push_back(range);
		rc = blockRsltnMgrPtr->dmlLockLBIDRanges(lbidRangeList, transID);
		if (rc != 0 )
		{
			BRM::errString(rc, errorMsg);
			goto cleanup;	
		}
        rc =  copyVBBlock(pSourceFile, pTargetFile, vbFbo, weFbo, &fileOp, column);
		
		//cout << "WES rolled block " << lbidList[i] << endl;
		if (rc != 0)
		{
			std::ostringstream oss;
			oss << "Error encountered while copying lbid " << lbidList[i]  << " to source file oid:dbroot:partition:segment = " << weOid << ":"
					<<weDbRoot << ":"<<wePartitionNum <<":"<< weSegmentNum;
			errorMsg = oss.str();
			goto cleanup;
		}
		pTargetFile->flush();
		rc = blockRsltnMgrPtr->dmlReleaseLBIDRanges(lbidRangeList);
		if (rc != 0 )
		{
			BRM::errString(rc, errorMsg);
			goto cleanup;	
		}
//timer.stop("copyVBBlock");
        if (rc != NO_ERROR)
            goto cleanup;
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
	if (pSourceFile)
	{
		delete pSourceFile;
	}

    //Close all target files
    //  -- chunkManager managed files
//	timer.start("flushChunks");
    if (rc == NO_ERROR)
	{
        rc = chunkManager.flushChunks(rc, columnOids); // write all active chunks to disk
	}
    else
        chunkManager.cleanUp(columnOids);          // close file w/o writing data to disk
	
	//@Bug 5466 need to purge PrimProc FD cache
	if ((idbdatafile::IDBPolicy::useHdfs()) && (files.size() > 0))
		cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID()); 	
//	timer.stop("flushChunks");
    //  -- other files
    FileOpenMap::const_iterator itor;
    for (itor = fileOpenList.begin(); itor != fileOpenList.end(); itor++)
    {
    	delete itor->second;
    }
	if ( rc != 0)
		throw std::runtime_error(errorMsg);
	
    return rc;
}

int BRMWrapper::rollBackVersion(const VER_t transID, int sessionId)
{
	std::vector<LBID_t> lbidList;
    std::vector<LBIDRange> lbidRangeList;
    LBIDRange   range;
	int rc = 0;
	
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
	//std::cout << "rollBackVersion get uncommited lbid " << lbidList.size() << std::endl;
	for(size_t i = 0; i < lbidList.size(); i++) {
        range.start = lbidList[i];
        range.size = 1;
        lbidRangeList.push_back(range);
	}
	rc =  blockRsltnMgrPtr->vbRollback(transID, lbidRangeList);
	return rc;
}


int BRMWrapper::writeVB(IDBDataFile* pFile, const VER_t transID, const OID oid, const uint64_t lbid,
    DbFileOp* pFileOp)
{
    int fbo;
    LBIDRange lbidRange;
    std::vector<uint32_t> fboList;
    std::vector<LBIDRange> rangeList;

    lbidRange.start = lbid;
    lbidRange.size  = 1;
    rangeList.push_back(lbidRange);

    uint16_t  dbRoot;
    uint32_t  partition;
    uint16_t  segment;
    RETURN_ON_ERROR(getFboOffset(lbid, dbRoot, partition, segment, fbo));

    fboList.push_back(fbo);
	std::vector<VBRange> freeList;
	int rc = writeVB(pFile, transID, oid, fboList, rangeList, pFileOp, freeList, dbRoot);
	//writeVBEnd(transID,rangeList);
    return rc;
}

// Eliminates blocks that have already been versioned by transaction transID
void BRMWrapper::pruneLBIDList(VER_t transID, vector<LBIDRange> *rangeList,
		vector<uint32_t> *fboList) const
{
	vector<LBID_t> lbids;
    vector<BRM::VSSData> vssData;
    BRM::QueryContext verID(transID);
    uint32_t i;
    int rc;
	vector<LBIDRange> newrangeList;
	vector<uint32_t> newfboList;

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
	
/*	if (newrangeList.size() != rangeList->size()) {
		cout << "Lbidlist is pruned, and the original list is: " << endl;
		for (uint32_t i = 0; i < rangeList->size(); i++)
		{
           cout << "lbid : " << (*rangeList)[i].start << endl;
		}
	} */
    newrangeList.swap(*rangeList);
    newfboList.swap(*fboList);
}

int BRMWrapper::writeVB(IDBDataFile* pSourceFile, const VER_t transID, const OID weOid,
    std::vector<uint32_t>& fboList, std::vector<LBIDRange>& rangeList, DbFileOp* pFileOp, std::vector<VBRange>& freeList, uint16_t dbRoot, bool skipBeginVBCopy)
{
	if (idbdatafile::IDBPolicy::useHdfs())
		return 0;
    int rc;
    size_t i;
    size_t processedBlocks;
    size_t rangeListCount;
    size_t k = 0;
    //std::vector<VBRange> freeList;
    IDBDataFile* pTargetFile;
    int32_t vbOid;

    if (isDebug(DEBUG_3))
    {
        cout << "\nIn writeVB" << endl;
        cout << "\n\tTransId=" << transID << endl;
        cout << "\t weOid : " << weOid << endl;
        cout << "\trangeList size=" << rangeList.size();
        for (i = 0; i < rangeList.size(); i++)
        {
            cout << "\t weLBID start : " << rangeList[i].start << endl;
            cout << " weSize : " << rangeList[i].size << endl;
        }

        cout << "\tfboList size=" << fboList.size() << endl;
        for (i = 0; i < fboList.size(); i++)
            cout << "\t weFbo : " << fboList[i] << endl;
    }
	
/*	cout << "\nIn writeVB" << endl;
    cout << "\n\tTransId=" << transID << endl;
    cout << "\t weOid : " << weOid << endl;
    cout << "\trangeList size=" << rangeList.size();
    for (i = 0; i < rangeList.size(); i++)
    {
            cout << "\t weLBID start : " << rangeList[i].start << endl;
    } 
*/
    if (!skipBeginVBCopy) {
		pruneLBIDList(transID, &rangeList, &fboList);
/*	cout << "\nIn writeVB" << endl;
    cout << "\n\tTransId=" << transID << endl;
    cout << "\t weOid : " << weOid << endl;
    cout << "\trangeList size=" << rangeList.size();
    for (i = 0; i < rangeList.size(); i++)
    {
            cout << "\t weLBID start : " << rangeList[i].start << endl;
    } 
*/
		if (rangeList.empty())   // all blocks have already been versioned
			return NO_ERROR;
		
	//Find the dbroot for a lbid
	//OID_t oid;
	//uint16_t segmentNum;
	//uint32_t partitionNum, fileBlockOffset;
	//rc = blockRsltnMgrPtr->lookupLocal(rangeList[0].start, transID, false, oid, dbRoot, partitionNum, segmentNum, fileBlockOffset);
	//if (rc != NO_ERROR)
	//	return rc;
		
	rc = blockRsltnMgrPtr->beginVBCopy(transID, dbRoot, rangeList, freeList);
		if (rc != NO_ERROR)
		{
			switch (rc)
			{
				case ERR_DEADLOCK: return ERR_BRM_DEAD_LOCK;
				case ERR_VBBM_OVERFLOW: return ERR_BRM_VB_OVERFLOW;
				case ERR_NETWORK: return ERR_BRM_NETWORK;
				case ERR_READONLY: return ERR_BRM_READONLY;
				default: return ERR_BRM_BEGIN_COPY;
			}
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
/*	for (i = 0; i < freeList.size(); i++)
        {
            cout << "\t VBOid : " << freeList[i].vbOID ;
            cout << " VBFBO : " << freeList[i].vbFBO ;
            cout << " Size : " << freeList[i].size << endl;
        }
*/
    //@Bug 2371 The assumption of all entries in the freelist belong to the same version buffer file is wrong
    // Open the first version buffer file
    File fileInfo;
 //   size_t rootCnt = Config::DBRootCount();
    fileInfo.oid = freeList[0].vbOID;
    fileInfo.fPartition = 0;
    fileInfo.fSegment = 0;
//    fileInfo.fDbRoot = (freeList[0].vbOID % rootCnt) + 1;
    fileInfo.fDbRoot = dbRoot;
	mutex::scoped_lock lk(vbFileLock);
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
        	delete pTargetFile;
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
	//cout << "writeVBEntry is putting the follwing lbids into VSS and freelist size is " << freeList.size() <<  endl;	 
    for (i = 0; i < freeList.size(); i++)
    {
        rangeListCount += k;
        processedBlocks = rangeListCount; // store the number of blocks processed till now for this file
        if (vbOid == freeList[i].vbOID)
        {
            // This call to copyVBBlock will consume whole of the freeList[i]
			k = 0;
            rc = copyVBBlock(pSourceFile, weOid, pTargetFile, fileInfo.oid,
                fboList, freeList[i], k, pFileOp, rangeListCount);
			//cout << "processedBlocks:k = " << processedBlocks <<":"<<k << endl;	

            if (rc != NO_ERROR)
                goto cleanup;

            for (; processedBlocks < (k+rangeListCount); processedBlocks++)
            {
                rc = blockRsltnMgrPtr->writeVBEntry(transID, rangeList[processedBlocks].start,
                    freeList[i].vbOID, freeList[i].vbFBO + (processedBlocks - rangeListCount));
				//cout << (uint64_t)rangeList[processedBlocks].start << endl;
                if (rc != NO_ERROR)
				{
					switch (rc)
					{
						case ERR_DEADLOCK: rc = ERR_BRM_DEAD_LOCK;
						case ERR_VBBM_OVERFLOW: rc = ERR_BRM_VB_OVERFLOW;
						case ERR_NETWORK: rc = ERR_BRM_NETWORK;
						case ERR_READONLY: rc = ERR_BRM_READONLY;
						default: rc = ERR_BRM_WR_VB_ENTRY;
					}
                    goto cleanup;
				}
            }
        }
    }
    if (pTargetFile)
    {
    	pTargetFile->flush();
    }
	return rc;
cleanup:
	if (pTargetFile)
	{
		pTargetFile->flush();
	}
	writeVBEnd(transID,rangeList);
    return rc;
}
void BRMWrapper::writeVBEnd(const VER_t transID, std::vector<LBIDRange>& rangeList)
{
	if (idbdatafile::IDBPolicy::useHdfs())
		return;

	blockRsltnMgrPtr->endVBCopy(transID, rangeList);
}

} //end of namespace
// vim:ts=4 sw=4:

