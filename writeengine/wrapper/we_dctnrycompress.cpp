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

//  $Id: we_dctnrycompress.cpp 4726 2013-08-07 03:38:36Z bwilkinson $


/** @file */

#include <stdio.h>
#include <string.h>
#include <vector>
using namespace std;

#include "we_log.h"
#include "we_brm.h"
#include "we_chunkmanager.h"

#include "we_dctnrycompress.h"


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
	m_chunkManager = new ChunkManager();

	if (logger)
	{
		setDebugLevel( logger->getDebugLevel() );
		setLogger    ( logger );
	}
	m_chunkManager->fileOp(this);
}

/**
 * Default Destructor
 */
DctnryCompress1::~DctnryCompress1()
{
	if (m_chunkManager)
		delete m_chunkManager;
}

int DctnryCompress1::updateDctnryExtent(IDBDataFile* pFile, int nBlocks)
{
	return m_chunkManager->updateDctnryExtent(pFile, nBlocks);
}


IDBDataFile* DctnryCompress1::createDctnryFile(const char *name,int width,const char *mode,int ioBuffSize)
{
   return m_chunkManager->createDctnryFile(
       m_dctnryOID, width, m_dbRoot, m_partition, m_segment, name, mode, ioBuffSize);
}


// @bug 5572 - HDFS usage: add *.tmp file backup flag
IDBDataFile* DctnryCompress1::openDctnryFile(bool useTmpSuffix)
{
   return m_chunkManager->getFilePtr(
       m_dctnryOID, m_dbRoot, m_partition, m_segment, m_segFileName, "r+b", DEFAULT_BUFSIZ, useTmpSuffix);
}


void DctnryCompress1::closeDctnryFile(bool doFlush, std::map<FID,FID> & columnOids)
{
    if (doFlush)
        m_chunkManager->flushChunks(NO_ERROR, columnOids);
    else
        m_chunkManager->cleanUp(columnOids);
    m_dFile = NULL;
}


int DctnryCompress1::numOfBlocksInFile()
{
    return m_chunkManager->getBlockCount(m_dFile);
}


int DctnryCompress1::readDBFile(IDBDataFile* pFile, unsigned char* readBuf, const uint64_t lbid,
                                const bool isFbo)
{
    int fbo = lbid;
    if (!isFbo)
        RETURN_ON_ERROR(lbidToFbo(lbid, fbo));

	return m_chunkManager->readBlock(pFile, readBuf, fbo);
}


int DctnryCompress1::writeDBFile(IDBDataFile* pFile, const unsigned char* writeBuf, const uint64_t lbid,
                                 const int numOfBlock)
{
    int fbo = 0;
    RETURN_ON_ERROR(lbidToFbo(lbid, fbo));

    for (int i = 0; i < numOfBlock; i++)
	    RETURN_ON_ERROR(m_chunkManager->saveBlock(pFile, writeBuf, fbo+i));

    return NO_ERROR;
}

int DctnryCompress1::writeDBFileNoVBCache(IDBDataFile* pFile,
                                           const unsigned char * writeBuf, const int fbo,
                                           const int numOfBlock)
{
    //int fbo = 0;
    //RETURN_ON_ERROR(lbidToFbo(lbid, fbo));

    for (int i = 0; i < numOfBlock; i++)
	    RETURN_ON_ERROR(m_chunkManager->saveBlock(pFile, writeBuf, fbo+i));

    return NO_ERROR;
}

int DctnryCompress1::flushFile(int rc, std::map<FID,FID> & columnOids)
{
	return m_chunkManager->flushChunks(rc, columnOids);
}


int DctnryCompress1::lbidToFbo(const uint64_t lbid, int& fbo)
{
    uint16_t dbRoot;
    uint16_t segment;
    uint32_t partition;
    return BRMWrapper::getInstance()->getFboOffset(lbid, dbRoot, partition, segment, fbo);
}


} //end of namespace

