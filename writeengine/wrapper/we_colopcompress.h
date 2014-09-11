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

//  $Id: we_colopcompress.h 3052 2011-08-31 18:22:45Z chao $


/** @file */

#ifndef _WE_COLOP_COMPRESS_H_
#define _WE_COLOP_COMPRESS_H_

#include <stdlib.h>

#include "we_colop.h"
#include "we_chunkmanager.h"

#if defined(_MSC_VER) && defined(WRITEENGINECOLUMNOPCOMPRESS_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{

/** Class ColumnOpCompress0 */
class ColumnOpCompress0 : public ColumnOp
{
public:
   /**
   * @brief Constructor
   */
   EXPORT ColumnOpCompress0();
   EXPORT ColumnOpCompress0(Log* logger);

   /**
   * @brief Default Destructor
   */
   EXPORT virtual ~ColumnOpCompress0();

   /**
   * @brief virtual method in ColumnOp
   */
   FILE*  openFile(const Column& column, uint16_t dbRoot, uint32_t partition,
      uint16_t segment, std::string& segFile, const char* mode = "r+b",
      int ioBuffSize = DEFAULT_BUFSIZ) const;

   /**
   * @brief virtual method in ColumnOp
   */
   bool abbreviatedExtent(FILE* pFile, int colWidth) const;

   /**
   * @brief virtual method in ColumnOp
   */
   int blocksInFile(FILE* pFile) const;


protected:

   /**
   * @brief virtual method in ColumnOp
   */
   int readBlock(FILE* pFile, unsigned char* readBuf, const i64 fbo);

   /**
   * @brief virtual method in ColumnOp
   */
   int saveBlock(FILE* pFile, const unsigned char* writeBuf, const i64 fbo);


private:
};



/** Class ColumnOpCompress1 */
class ColumnOpCompress1 : public ColumnOp
{
public:
   /**
   * @brief Constructor
   */
   EXPORT ColumnOpCompress1(Log* logger=0);

   /**
   * @brief Default Destructor
   */
   EXPORT virtual ~ColumnOpCompress1();

   /**
   * @brief virtual method in FileOp
   */
   EXPORT int flushFile(int rc, std::map<FID,FID> & columnOids);

   /**
   * @brief virtual method in FileOp
   */
   const int expandAbbrevColumnExtent(FILE* pFile, uint16_t dbRoot, i64 emptyVal, int width);

   /**
   * @brief virtual method in ColumnOp
   */
   FILE*  openFile(const Column& column, uint16_t dbRoot, uint32_t partition,
      uint16_t segment, std::string& segFile, const char* mode = "r+b",
      int ioBuffSize = DEFAULT_BUFSIZ) const;

   /**
   * @brief virtual method in ColumnOp
   */
   bool abbreviatedExtent(FILE* pFile, int colWidth) const;

   /**
   * @brief virtual method in ColumnOp
   */
   int blocksInFile(FILE* pFile) const;

   void chunkManager(ChunkManager* cm);
   void setTransId(const TxnID& transId) {ColumnOp::setTransId(transId); fChunkManager->setTransId(transId);}
   
protected:

   /**
   * @brief virtual method in FileOp
   */
   int updateColumnExtent(FILE* pFile, int nBlocks);

   /**
   * @brief virtual method in ColumnOp
   */
   void closeColumnFile(Column& column) const;

   /**
   * @brief virtual method in ColumnOp
   */
   int readBlock(FILE* pFile, unsigned char* readBuf, const i64 fbo);

   /**
   * @brief virtual method in ColumnOp
   */
   int saveBlock(FILE* pFile, const unsigned char* writeBuf, const i64 fbo);

private:
   ChunkManager* fChunkManager;
   bool fIsInsert;
};


} //end of namespace

#undef EXPORT

#endif // _WE_COLOP_COMPRESS_H_
