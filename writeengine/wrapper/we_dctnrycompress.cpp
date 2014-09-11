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

//  $Id: we_dctnrycompress.cpp 2962 2011-05-09 14:46:13Z chao $


/** @file */

#include <stdio.h>
#include <string.h>
#include <vector>
using namespace std;

#include "we_log.h"
#include "we_brm.h"
#include "we_chunkmanager.h"

#define WRITEENGINEDCTNRYCOMPRESS_DLLEXPORT
#include "we_dctnrycompress.h"
#undef WRITEENGINEDCTNRYCOMPRESS_DLLEXPORT


namespace WriteEngine
{
class ChunkManager;
// -----------------------------------------------------------------------------
// Dctnry with compression type 0
// -----------------------------------------------------------------------------

/**
 * Constructor
 */
DctnryCompress0::DctnryCompress0()
{
   m_compressionType = 0;
}

DctnryCompress0::DctnryCompress0(Log* logger)
{
   m_compressionType = 0;
   setDebugLevel( logger->getDebugLevel() );
   setLogger    ( logger );
}

/**
 * Default Destructor
 */
DctnryCompress0::~DctnryCompress0()
{}

// -----------------------------------------------------------------------------
// Dctnry with compression type 1
// -----------------------------------------------------------------------------

/**
 * Constructor
 */
DctnryCompress1::DctnryCompress1(Log* logger)
{
   m_compressionType = 1;
   fIsInsert = false;
   
   fChunkManager = new ChunkManager();
   
   DbFileOp::chunkManager(fChunkManager);
   if (logger)
   {
      setDebugLevel( logger->getDebugLevel() );
      setLogger    ( logger );
   }
   fChunkManager->fileOp(this);
}

/**
 * Default Destructor
 */
DctnryCompress1::~DctnryCompress1()
{
	if (!fIsInsert)
		delete fChunkManager;
}

void DctnryCompress1::chunkManager(ChunkManager * cm)
{
	if (fChunkManager != cm)
	{
		delete fChunkManager;
		fChunkManager = cm;
		fIsInsert = true;
		fChunkManager->fileOp(this);
	}
	
	DbFileOp::chunkManager(cm);
}

int DctnryCompress1::updateDctnryExtent(FILE* pFile, int nBlocks)
{
	return fChunkManager->updateDctnryExtent(pFile, nBlocks);
}


FILE* DctnryCompress1::createDctnryFile(const char *name,int width,const char *mode,int ioBuffSize)
{
   return fChunkManager->createDctnryFile(
       m_dctnryOID, width, m_dbRoot, m_partition, m_segment, name, mode, ioBuffSize);
}


FILE* DctnryCompress1::openDctnryFile()
{
   return fChunkManager->getFilePtr(
       m_dctnryOID, m_dbRoot, m_partition, m_segment, m_segFileName, "r+b", DEFAULT_BUFSIZ);
}


void DctnryCompress1::closeDctnryFile(bool doFlush, std::map<FID,FID> & columnOids)
{
    // return value??
	if (fIsInsert)
		return;
		
    if (doFlush)
        fChunkManager->flushChunks(NO_ERROR, columnOids);
    else
        fChunkManager->cleanUp(columnOids);
    m_dFile = NULL;
}


int DctnryCompress1::numOfBlocksInFile()
{
    return fChunkManager->getBlockCount(m_dFile);
}


int DctnryCompress1::readDBFile(FILE* pFile, unsigned char* readBuf, const i64 lbid,
                                const bool isFbo)
{
    int fbo = lbid;
    if (!isFbo)
        RETURN_ON_ERROR(lbidToFbo(lbid, fbo));

	return fChunkManager->readBlock(pFile, readBuf, fbo);
}


int DctnryCompress1::writeDBFile(FILE* pFile, const unsigned char* writeBuf, const i64 lbid,
                                 const int numOfBlock)
{
    int fbo = 0;
    RETURN_ON_ERROR(lbidToFbo(lbid, fbo));

    for (int i = 0; i < numOfBlock; i++)
	    RETURN_ON_ERROR(fChunkManager->saveBlock(pFile, writeBuf, fbo+i));

    return NO_ERROR;
}

int DctnryCompress1::writeDBFileNoVBCache(FILE *pFile,
                                           const unsigned char * writeBuf, const int fbo,
                                           const int numOfBlock)
{
    //int fbo = 0;
    //RETURN_ON_ERROR(lbidToFbo(lbid, fbo));

    for (int i = 0; i < numOfBlock; i++)
	    RETURN_ON_ERROR(fChunkManager->saveBlock(pFile, writeBuf, fbo+i));

    return NO_ERROR;
}

int DctnryCompress1::flushFile(int rc, std::map<FID,FID> & columnOids)
{
	return fChunkManager->flushChunks(rc, columnOids);
}


int DctnryCompress1::lbidToFbo(const i64 lbid, int& fbo)
{
    u_int16_t dbRoot;
    u_int16_t segment;
    u_int32_t partition;
    return BRMWrapper::getInstance()->getFboOffset(lbid, dbRoot, partition, segment, fbo);
}


} //end of namespace

