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

//  $Id: we_colopcompress.cpp 2962 2011-05-09 14:46:13Z chao $


/** @file */

#include <stdio.h>
#include <string.h>
#include <vector>

#include "we_log.h"
#include "we_chunkmanager.h"
#include "idbcompress.h"

#define WRITEENGINECOLUMNOPCOMPRESS_DLLEXPORT
#include "we_colopcompress.h"
#undef WRITEENGINECOLUMNOPCOMPRESS_DLLEXPORT


namespace WriteEngine
{

class ChunkManager;

// --------------------------------------------------------------------------------------------
// ColumnOp with compression type 0
// --------------------------------------------------------------------------------------------

/**
 * Constructor
 */
ColumnOpCompress0::ColumnOpCompress0()
{
   m_compressionType = 0;
}


ColumnOpCompress0::ColumnOpCompress0(Log* logger)
{
   m_compressionType = 0;
   setDebugLevel( logger->getDebugLevel() );
   setLogger    ( logger );
}

/**
 * Default Destructor
 */
ColumnOpCompress0::~ColumnOpCompress0()
{}


FILE* ColumnOpCompress0::openFile(
   const Column& column, const uint16_t dbRoot, const uint32_t partition, const uint16_t segment,
   std::string& segFile, const char* mode, const int ioBuffSize) const
{
   return FileOp::openFile(column.dataFile.fid, dbRoot, partition, segment, segFile,
                     mode, ioBuffSize);
}


bool ColumnOpCompress0::abbreviatedExtent(FILE* pFile, int colWidth) const
{
   return (getFileSize(pFile) == INITIAL_EXTENT_ROWS_TO_DISK*colWidth);
}


int ColumnOpCompress0::blocksInFile(FILE* pFile) const
{
   return (getFileSize(pFile) / BYTE_PER_BLOCK);
}


int ColumnOpCompress0::readBlock(FILE* pFile, unsigned char* readBuf, const i64 fbo)
{
   return readDBFile(pFile, readBuf, fbo, true);
}


int ColumnOpCompress0::saveBlock(FILE* pFile, const unsigned char* writeBuf, const i64 fbo)
{
   return writeDBFileFbo(pFile, writeBuf, fbo, 1);
}


// --------------------------------------------------------------------------------------------
// ColumnOp with compression type 1
// --------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------

/**
 * Constructor
 */

ColumnOpCompress1::ColumnOpCompress1(Log* logger)
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
ColumnOpCompress1::~ColumnOpCompress1()
{
	if (!fIsInsert)
		delete fChunkManager;
}

void ColumnOpCompress1::chunkManager(ChunkManager* cm)
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

FILE* ColumnOpCompress1::openFile(
   const Column& column, const uint16_t dbRoot, const uint32_t partition, const uint16_t segment,
   std::string& segFile, const char* mode, const int ioBuffSize) const
{
   return fChunkManager->getFilePtr(column, dbRoot, partition, segment, segFile, mode, ioBuffSize);
}


bool ColumnOpCompress1::abbreviatedExtent(FILE* pFile, int colWidth) const
{
   return (blocksInFile(pFile) == INITIAL_EXTENT_ROWS_TO_DISK*colWidth/BYTE_PER_BLOCK);
}


int ColumnOpCompress1::blocksInFile(FILE* pFile) const
{
   CompFileHeader compFileHeader;
   readHeaders(pFile, compFileHeader.fControlData, compFileHeader.fPtrSection);

   compress::IDBCompressInterface compressor;
   return compressor.getBlockCount(compFileHeader.fControlData);
}


int ColumnOpCompress1::readBlock(FILE* pFile, unsigned char* readBuf, const i64 fbo)
{
   return fChunkManager->readBlock(pFile, readBuf, fbo);
}


int ColumnOpCompress1::saveBlock(FILE* pFile, const unsigned char* writeBuf, const i64 fbo)
{
   return fChunkManager->saveBlock(pFile, writeBuf, fbo);
}


int ColumnOpCompress1::flushFile(int rc, std::map<FID,FID> & columnOids)
{
   return fChunkManager->flushChunks(rc, columnOids);
}


const int ColumnOpCompress1::expandAbbrevColumnExtent(
   FILE* pFile, uint16_t dbRoot, i64 emptyVal, int width)
{
   // update the uncompressed initial chunk to full chunk
   RETURN_ON_ERROR(fChunkManager->expandAbbrevColumnExtent(pFile, emptyVal, width));

   // let the base to physically expand extent.
   return FileOp::expandAbbrevColumnExtent(pFile, dbRoot, emptyVal, width);
}


int ColumnOpCompress1::updateColumnExtent(FILE* pFile, int nBlocks)
{
   return fChunkManager->updateColumnExtent(pFile, nBlocks);
}


void ColumnOpCompress1::closeColumnFile(Column& column) const
{
   // Leave file closing to chunk manager.
   column.dataFile.pFile = NULL;
}

} //end of namespace

