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

//  $Id: we_colopcompress.h 4726 2013-08-07 03:38:36Z bwilkinson $


/** @file */

#ifndef _WE_COLOP_COMPRESS_H_
#define _WE_COLOP_COMPRESS_H_

#include <stdlib.h>

#include "we_colop.h"
#include "we_chunkmanager.h"

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
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
   IDBDataFile*  openFile(const Column& column, uint16_t dbRoot, uint32_t partition,
      uint16_t segment, std::string& segFile, bool useTmpSuffix, const char* mode = "r+b",
      int ioBuffSize = DEFAULT_BUFSIZ) const;

   /**
   * @brief virtual method in ColumnOp
   */
   bool abbreviatedExtent(IDBDataFile* pFile, int colWidth) const;

   /**
   * @brief virtual method in ColumnOp
   */
   int blocksInFile(IDBDataFile* pFile) const;


protected:

   /**
   * @brief virtual method in ColumnOp
   */
   int readBlock(IDBDataFile* pFile, unsigned char* readBuf, const uint64_t fbo);

   /**
   * @brief virtual method in ColumnOp
   */
   int saveBlock(IDBDataFile* pFile, const unsigned char* writeBuf, const uint64_t fbo);


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
   int expandAbbrevColumnExtent(IDBDataFile* pFile, uint16_t dbRoot, uint64_t emptyVal, int width);

   /**
   * @brief virtual method in ColumnOp
   */
   IDBDataFile*  openFile(const Column& column, uint16_t dbRoot, uint32_t partition,
      uint16_t segment, std::string& segFile, bool useTmpSuffix, const char* mode = "r+b",
      int ioBuffSize = DEFAULT_BUFSIZ) const;

   /**
   * @brief virtual method in ColumnOp
   */
   bool abbreviatedExtent(IDBDataFile* pFile, int colWidth) const;

   /**
   * @brief virtual method in ColumnOp
   */
   int blocksInFile(IDBDataFile* pFile) const;

//   void chunkManager(ChunkManager* cm);

   /**
   * @brief virtual method in FileOp
   */
   void setTransId(const TxnID& transId) {ColumnOp::setTransId(transId); if (m_chunkManager) m_chunkManager->setTransId(transId);}
   
   void setBulkFlag(bool isBulkLoad) {m_chunkManager->setBulkFlag(isBulkLoad);};
   
   void setFixFlag(bool isFix) {m_chunkManager->setFixFlag(isFix);};
   
protected:

   /**
   * @brief virtual method in FileOp
   */
   int updateColumnExtent(IDBDataFile* pFile, int nBlocks);

   /**
   * @brief virtual method in ColumnOp
   */
   void closeColumnFile(Column& column) const;

   /**
   * @brief virtual method in ColumnOp
   */
   int readBlock(IDBDataFile* pFile, unsigned char* readBuf, const uint64_t fbo);

   /**
   * @brief virtual method in ColumnOp
   */
   int saveBlock(IDBDataFile* pFile, const unsigned char* writeBuf, const uint64_t fbo);

   /**
   * @brief Set the IsInsert flag in the ChunkManager. 
   * This forces flush at end of statement. Used only for bulk insert. 
   */
   void setIsInsert(bool isInsert) { m_chunkManager->setIsInsert(isInsert); }

private:
};


} //end of namespace

#undef EXPORT

#endif // _WE_COLOP_COMPRESS_H_
