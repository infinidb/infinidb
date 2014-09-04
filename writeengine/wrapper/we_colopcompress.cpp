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

//  $Id: we_colopcompress.cpp 4096 2012-08-07 20:06:09Z dhall $


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
ColumnOpCompress1::~ColumnOpCompress1()
{
	if (m_chunkManager)
	{
		delete m_chunkManager;
	}
}

FILE* ColumnOpCompress1::openFile(
   const Column& column, const uint16_t dbRoot, const uint32_t partition, const uint16_t segment,
   std::string& segFile, const char* mode, const int ioBuffSize) const
{
   return m_chunkManager->getFilePtr(column, dbRoot, partition, segment, segFile, mode, ioBuffSize);
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
   return m_chunkManager->readBlock(pFile, readBuf, fbo);
}


int ColumnOpCompress1::saveBlock(FILE* pFile, const unsigned char* writeBuf, const i64 fbo)
{
   return m_chunkManager->saveBlock(pFile, writeBuf, fbo);
}


int ColumnOpCompress1::flushFile(int rc, std::map<FID,FID> & columnOids)
{
   return m_chunkManager->flushChunks(rc, columnOids);
}


int ColumnOpCompress1::expandAbbrevColumnExtent(
   FILE* pFile, uint16_t dbRoot, i64 emptyVal, int width)
{
   // update the uncompressed initial chunk to full chunk
   RETURN_ON_ERROR(m_chunkManager->expandAbbrevColumnExtent(pFile, emptyVal, width));

   // let the base to physically expand extent.
   return FileOp::expandAbbrevColumnExtent(pFile, dbRoot, emptyVal, width);
}


int ColumnOpCompress1::updateColumnExtent(FILE* pFile, int nBlocks)
{
   return m_chunkManager->updateColumnExtent(pFile, nBlocks);
}


void ColumnOpCompress1::closeColumnFile(Column& column) const
{
   // Leave file closing to chunk manager.
   column.dataFile.pFile = NULL;
}

} //end of namespace

