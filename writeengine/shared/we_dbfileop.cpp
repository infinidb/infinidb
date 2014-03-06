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

//  $Id: we_dbfileop.cpp 4737 2013-08-14 20:45:46Z bwilkinson $

/** @file */

#include <unistd.h>
#include <stdio.h>
#include <cstring>
using namespace std;

#include "we_chunkmanager.h"

#include "we_dbfileop.h"

#include "we_stats.h"
#include "IDBDataFile.h"
using namespace idbdatafile;

using namespace BRM;

namespace WriteEngine
{

/**
 * Constructor
 */
DbFileOp::DbFileOp() : m_chunkManager(NULL)
{}

/**
 * Default Destructor
 */
DbFileOp::~DbFileOp()
{}

/***********************************************************
 * DESCRIPTION: 
 *    flush the cache
 * PARAMETERS:
 *    none
 * RETURN:
 *    NO_ERROR if success, otherwise if fail
 ***********************************************************/
int DbFileOp::flushCache()
{
    BlockBuffer*   curBuf;

    if( !Cache::getUseCache() )
        return NO_ERROR;

    for( CacheMapIt it = Cache::m_writeList->begin();
                    it != Cache::m_writeList->end(); it++ ) {
        curBuf = it->second;
        RETURN_ON_ERROR( writeDBFile( (*curBuf).cb.file.pFile,
                                      (*curBuf).block.data,
                                      (*curBuf).block.lbid ) );
    }

    RETURN_ON_ERROR( Cache::flushCache() );
    return NO_ERROR;
}


/***********************************************************
 * DESCRIPTION: 
 *    get an entry within a sub block
 *    NOTE: the difference with readSubBlockEntry is that
 *          getSubBlockEntry only works for buffer while
 *          readSubBlockEntry works for file and block
 * PARAMETERS:
 *    blockBuf - the block buffer
 *    sbid - sub block id
 *    entryNo - entry no within sub block
 *    width - width in bytes
 *    pStruct - sturcture pointer
 * RETURN:
 *    none
 ***********************************************************/
void DbFileOp::getSubBlockEntry( unsigned char* blockBuf,
                                 const int sbid, const int entryNo, 
                                 const int width, void* pStruct )
{
    unsigned char*    pBlock;

    pBlock = blockBuf + BYTE_PER_SUBBLOCK * sbid + entryNo *MAX_COLUMN_BOUNDARY;
    memcpy( pStruct, pBlock, width );
}

/***********************************************************
 * DESCRIPTION: 
 *    Read a block from a file at specified location
 * PARAMETERS:
 *    pFile - file handle
 *    readBuf - read buffer
 *    fbo - file block offset
 * RETURN:
 *    NO_ERROR if success
 *    other number if something wrong
 ***********************************************************/
int DbFileOp::readDBFile( IDBDataFile* pFile,
                          unsigned char* readBuf,
                          const uint64_t lbid,
                          const bool isFbo )
{
    long long  fboOffset = 0;

    if( !isFbo ) {
        RETURN_ON_ERROR( setFileOffsetBlock( pFile, lbid ) );
    }
    else {  
        fboOffset = (lbid)*(long)BYTE_PER_BLOCK;                 
        RETURN_ON_ERROR( setFileOffset( pFile, fboOffset ) );
    }

    return readFile( pFile, readBuf, BYTE_PER_BLOCK );
}

int DbFileOp::readDBFile( IDBDataFile* pFile,
                          DataBlock* block,
                          const uint64_t lbid,
                          const bool isFbo )
{
     block->dirty = false;
     block->no = lbid;

     Stats::incIoBlockRead();

     return readDBFile( pFile, block->data, lbid, isFbo );
}

int DbFileOp::readDBFile( CommBlock& cb,
                          unsigned char* readBuf,
                          const uint64_t lbid ) 
{ 
    CacheKey key;

    if( Cache::getUseCache() )
    {
        if( Cache::cacheKeyExist( cb.file.oid, lbid ) ) {
            key = Cache::getCacheKey( cb.file.oid, lbid );
            RETURN_ON_ERROR( Cache::loadCacheBlock( key, readBuf ) );
            return NO_ERROR;
        }
    }

    RETURN_ON_ERROR( readDBFile( cb.file.pFile, readBuf, lbid ) ); 
    if( Cache::getUseCache() )
    {
        int  fbo = lbid;

        uint16_t  dbRoot;
        uint32_t  partition;
        uint16_t  segment;
        RETURN_ON_ERROR( BRMWrapper::getInstance()->getFboOffset(
            lbid, dbRoot, partition, segment, fbo ) );
      
        if( Cache::getListSize( FREE_LIST ) == 0 ) {
            if ( isDebug( DEBUG_1 ) ) {
                printf( "\nBefore flushing cache " );
                Cache::printCacheList();
            }

            // flush cache to give up more space
            RETURN_ON_ERROR( flushCache() );
            if ( isDebug( DEBUG_1 ) ) {
                printf( "\nAfter flushing cache " );
                Cache::printCacheList();
            }
        }
        RETURN_ON_ERROR( Cache::insertLRUList( cb, lbid, fbo, readBuf ) );
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION: No change, old signature 10/17/06
 *    Read an entry within a sub block from a file
 *    NOTE: the difference with getSubBlockEntry is that
 *          getSubBlockEntry only works for buffer while
 *          readSubBlockEntry works for file and block
 * PARAMETERS:
 *    pFile - file handler
 *    block - the block structure
 *    fbo - file block offset
 *    sbid - sub block id
 *    entryNo - entry no within sub block
 *    width - width in bytes
 *    pStruct - sturcture pointer
 * RETURN:
 *    NO_ERROR if success
 *    other number if something wrong
 ***********************************************************/
const int DbFileOp::readSubBlockEntry( IDBDataFile* pFile, DataBlock* block,
                                       const uint64_t lbid, const int sbid, 
                                       const int entryNo, const int width, 
                                       void* pStruct )
{
    RETURN_ON_ERROR( readDBFile( pFile, block->data, lbid ) );
    getSubBlockEntry( block->data, sbid, entryNo, width, pStruct );

    return NO_ERROR;
}


const int DbFileOp::readSubBlockEntry( CommBlock& cb, DataBlock* block, 
                                       const uint64_t lbid, const int sbid, 
                                       const int entryNo, const int width, 
                                       void* pStruct )
{
    RETURN_ON_ERROR( readDBFile( cb, block->data, lbid ) );
    getSubBlockEntry( block->data, sbid, entryNo, width, pStruct );

    return NO_ERROR;
}


/***********************************************************
 * DESCRIPTION: 
 *    Set an entry within a sub block
 *    NOTE: the difference with writeSubBlockEntry is that
 *          setSubBlockEntry only works for buffer while
 *          writeSubBlockEntry works for file and block
 * PARAMETERS:
 *    blockBuf - the block buffer
 *    sbid - sub block id
 *    entryNo - entry no within sub block
 *    width - width in bytes
 *    pStruct - sturcture pointer
 * RETURN:
 *    none
 ***********************************************************/
void DbFileOp::setSubBlockEntry( unsigned char* blockBuf, const int sbid, 
                                 const int entryNo, const int width, 
                                 const void* pStruct )
{
    unsigned char*    pBlock;

    pBlock = blockBuf + BYTE_PER_SUBBLOCK * sbid + entryNo *MAX_COLUMN_BOUNDARY;
    memcpy( pBlock, pStruct, width );
}

/***********************************************************
 * DESCRIPTION: 
 *    Write a number of blocks to the file at specified location
 * PARAMETERS:
 *    pFile - file handle
 *    writeBuf - write buffer
 *    fbo - file block offset
 *    numOfBlock - total number of file block offset
 * RETURN:
 *    NO_ERROR if success
 *    other number if something wrong
 ***********************************************************/
int DbFileOp::writeDBFile( CommBlock& cb, const unsigned char* writeBuf, 
                           const uint64_t lbid, const int numOfBlock ) 
{ 
    CacheKey key;
    int ret;

    if( Cache::getUseCache() )
    {
        if( Cache::cacheKeyExist( cb.file.oid, lbid ) ) {
            key = Cache::getCacheKey( cb.file.oid, lbid );
            RETURN_ON_ERROR( Cache::modifyCacheBlock( key, writeBuf ) );
            return NO_ERROR;
        }
    }
	if (BRMWrapper::getUseVb())
	{
		RETURN_ON_ERROR( writeVB( cb.file.pFile, cb.file.oid, lbid ) ); 
	}
    ret = writeDBFile( cb.file.pFile, writeBuf, lbid, numOfBlock );
	if (BRMWrapper::getUseVb())
	{
		LBIDRange_v ranges;
		LBIDRange range;
		range.start = lbid;
		range.size = 1;
		ranges.push_back(range);
		BRMWrapper::getInstance()->writeVBEnd(getTransId(), ranges);
	}

    return ret;
}

int DbFileOp::writeDBFileNoVBCache(CommBlock & cb,
                                   const unsigned char * writeBuf,
                                   const int fbo,
                                   const int numOfBlock)
{
    return writeDBFileNoVBCache( cb.file.pFile, writeBuf, fbo, numOfBlock ); 
}

/***********************************************************
 * DESCRIPTION: 
 *    Core function for writing data w/o using VB cache
 *    (bulk load dictionary store inserts)
 ***********************************************************/
int DbFileOp::writeDBFileNoVBCache( IDBDataFile* pFile,
                                    const unsigned char* writeBuf,
                                    const int fbo,
                                    const int numOfBlock  )
{
#ifdef PROFILE
    // This function is only used by bulk load for dictionary store files,
    // so we log as such.
    Stats::startParseEvent(WE_STATS_WRITE_DCT);
#endif

    for( int i = 0; i < numOfBlock; i++ ) {
        Stats::incIoBlockWrite();
        RETURN_ON_ERROR( writeFile( pFile, writeBuf, BYTE_PER_BLOCK ) );
    }

#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_WRITE_DCT);
#endif

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION: 
 *    Core function for writing data using VB cache
 ***********************************************************/
int DbFileOp::writeDBFile( IDBDataFile* pFile, const unsigned char* writeBuf,
                           const uint64_t lbid, const int numOfBlock  )
{
    RETURN_ON_ERROR( setFileOffsetBlock( pFile, lbid ) );

    for( int i = 0; i < numOfBlock; i++ ) {
        Stats::incIoBlockWrite();
        RETURN_ON_ERROR( writeFile( pFile, writeBuf, BYTE_PER_BLOCK ) );
    }

    return NO_ERROR;
}

// just don't have a good solution to consolidate with above functions
// Note: This is used with absolute FBO, no lbid involved
int DbFileOp::writeDBFileFbo(IDBDataFile* pFile, const unsigned char* writeBuf,
                             const uint64_t fbo, const int numOfBlock  )
{
    long long  fboOffset = 0;

    fboOffset = (fbo)*(long)BYTE_PER_BLOCK;
    RETURN_ON_ERROR( setFileOffset( pFile, fboOffset ) );

    for( int i = 0; i < numOfBlock; i++ ) {
        Stats::incIoBlockWrite();
        RETURN_ON_ERROR( writeFile( pFile, writeBuf, BYTE_PER_BLOCK ) );
    }

    return NO_ERROR;
}

/***********************************************************
 * DESCRIPTION: 
 *    Write an entry within a sub block to a file
 *    NOTE: the difference with getSubBlockEntry is that
 *          setSubBlockEntry only works for buffer while
 *          writeSubBlockEntry works for file and block
 * PARAMETERS:
 *    pFile - file handler
 *    block - the block structure
 *    fbo - file block offset
 *    sbid - sub block id
 *    entryNo - entry no within sub block
 *    width - width in bytes
 *    pStruct - sturcture pointer
 * RETURN:
 *    NO_ERROR if success
 *    other number if something wrong
 ***********************************************************/
const int DbFileOp::writeSubBlockEntry( IDBDataFile* pFile, DataBlock* block,
                                        const uint64_t lbid, const int sbid, 
                                        const int entryNo, const int width, 
                                        void* pStruct )
{
    setSubBlockEntry( block->data, sbid, entryNo, width, pStruct );
    block->dirty = false;

    return writeDBFile( pFile, block->data, lbid );
}

const int DbFileOp::writeSubBlockEntry( CommBlock& cb, DataBlock* block, 
                                        const uint64_t lbid, const int sbid, 
                                        const int entryNo, const int width, 
                                        void* pStruct )
{
    setSubBlockEntry( block->data, sbid, entryNo, width, pStruct );
    block->dirty = false;

    return writeDBFile( cb, block->data, lbid );
}

/***********************************************************
 * DESCRIPTION: 
 *    Write to version buffer
 * PARAMETERS:
 *    oid - file oid
 *    lbid - lbid
 * RETURN:
 *    NO_ERROR if success
 *    other number if something wrong
 ***********************************************************/
const int DbFileOp::writeVB( IDBDataFile* pFile, const OID oid, const uint64_t lbid )
{
    if( !BRMWrapper::getUseVb() )
        return NO_ERROR;

    int rc;
    TxnID transId = getTransId(); 

    if (transId !=((TxnID)INVALID_NUM))
    { 
        rc= BRMWrapper::getInstance()->writeVB( pFile,
                                                (const VER_t)transId,
                                                oid, lbid, this );
//@Bug 4671. The error is already logged by worker node.
/*        if (rc != NO_ERROR) 
        { 
            char msg[2048];
            snprintf(msg, 2048,
                     "we_dbfileop->BRMWrapper::getInstance()->writeVB "
                     "transId %i oid %i lbid "
#if __LP64__
                     "%lu"
#else
                     "%llu"
#endif
                     " Error Code %i", transId, oid, lbid, rc);
            puts(msg);
            {
                logging::MessageLog ml(logging::LoggingID(19));
                logging::Message m;
                logging::Message::Args args;
                args.add(msg);
                m.format(args);
                ml.logCriticalMessage(m);
            }
            return rc;
        } */
		return rc;
    }

    return NO_ERROR;
}

int DbFileOp::readDbBlocks(IDBDataFile* pFile,
                           unsigned char* readBuf,
                           uint64_t fbo,
                           size_t n)
{
    if (m_chunkManager) {
        return m_chunkManager->readBlocks(pFile, readBuf, fbo, n);
	}

    if (setFileOffset(pFile, fbo*BYTE_PER_BLOCK, SEEK_SET) != NO_ERROR)
        return -1;
    return pFile->read(readBuf, BYTE_PER_BLOCK * n) / BYTE_PER_BLOCK;
}

int DbFileOp::restoreBlock(IDBDataFile* pFile, const unsigned char* writeBuf, uint64_t fbo)
{
    if (m_chunkManager)
        return m_chunkManager->restoreBlock(pFile, writeBuf, fbo);

    if (setFileOffset(pFile, fbo*BYTE_PER_BLOCK, SEEK_SET) != NO_ERROR)
        return -1;

    return pFile->write(writeBuf, BYTE_PER_BLOCK);
}

// @bug 5572 - HDFS usage: add *.tmp file backup flag
IDBDataFile* DbFileOp::getFilePtr(const Column& column, bool useTmpSuffix)
{
    string filename;
    return m_chunkManager->getFilePtr(column,
                                      column.dataFile.fDbRoot,
                                      column.dataFile.fPartition,
                                      column.dataFile.fSegment,
                                      filename,
                                      "r+b",
                                      column.colWidth,
                                      useTmpSuffix);
}

} //end of namespace

