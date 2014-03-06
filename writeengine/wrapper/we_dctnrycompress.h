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

//  $Id: we_dctnrycompress.h 4726 2013-08-07 03:38:36Z bwilkinson $


/** @file */

#ifndef _WE_DCTNRY_COMPRESS_H_
#define _WE_DCTNRY_COMPRESS_H_

#include <stdlib.h>

#include "we_dctnry.h"
#include "we_chunkmanager.h"
#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{

/** Class DctnryCompress */
class DctnryCompress0 : public Dctnry
{
public:
   /**
   * @brief Constructor
   */
   EXPORT DctnryCompress0();
   EXPORT DctnryCompress0(Log* logger);

   /**
   * @brief Default Destructor
   */
   EXPORT virtual ~DctnryCompress0();
};



/** Class DctnryCompress1 */
class DctnryCompress1 : public Dctnry
{
public:
   /**
   * @brief Constructor
   */
   EXPORT DctnryCompress1(Log* logger=0);

   /**
   * @brief Default Destructor
   */
   EXPORT virtual ~DctnryCompress1();

   /**
   * @brief virtual method in FileOp
   */
   EXPORT int flushFile(int rc, std::map<FID,FID> & columnOids);

   /**
   * @brief virtual method in DBFileOp
   */
   EXPORT int readDBFile(IDBDataFile* pFile, unsigned char* readBuf, const uint64_t lbid,
                         const bool isFbo = false );

   /**
   * @brief virtual method in DBFileOp
   */
   EXPORT int writeDBFile(IDBDataFile* pFile, const unsigned char* writeBuf, const uint64_t lbid,
                          const int numOfBlock = 1);

	/**
   * @brief virtual method in DBFileOp
   */
   EXPORT int writeDBFileNoVBCache(IDBDataFile* pFile,
                                           const unsigned char * writeBuf, const int fbo,
                                           const int numOfBlock = 1);


   /**
   * @brief virtual method in Dctnry
   */
   IDBDataFile* createDctnryFile(const char *name, int width, const char *mode, int ioBuffSize);

   /**
   * @brief virtual method in Dctnry
   */
   IDBDataFile* openDctnryFile(bool useTmpSuffix);

	/**
   * @brief virtual method in Dctnry
   */
   void closeDctnryFile(bool doFlush, std::map<FID,FID> & columnOids);
   
   /**
   * @brief virtual method in Dctnry
   */
   int numOfBlocksInFile();
   
   /**
   * @brief For bulkload to use
   */
   void setMaxActiveChunkNum(unsigned int maxActiveChunkNum) { m_chunkManager->setMaxActiveChunkNum(maxActiveChunkNum); };
   void setBulkFlag(bool isBulkLoad) {m_chunkManager->setBulkFlag(isBulkLoad);};
   void setFixFlag(bool isFix) {m_chunkManager->setFixFlag(isFix);};
   int checkFixLastDictChunk () { return m_chunkManager->checkFixLastDictChunk(m_dctnryOID, m_dbRoot, m_partition, m_segment); };
//   void chunkManager(ChunkManager* cm);

   /**
   * @brief virtual method in FileOp
   */
   void setTransId(const TxnID& transId) {Dctnry::setTransId(transId); if(m_chunkManager) m_chunkManager->setTransId(transId);}

   /**
   * @brief Set the IsInsert flag in the ChunkManager. 
   * This forces flush at end of statement. Used only for bulk insert. 
   */
   void setIsInsert(bool isInsert) { m_chunkManager->setIsInsert(isInsert); }

protected:

   /**
   * @brief virtual method in FileOp
   */
   int updateDctnryExtent(IDBDataFile* pFile, int nBlocks);

   /**
   * @brief convert lbid to fbo
   */
   int lbidToFbo(const uint64_t lbid, int& fbo);
};


} //end of namespace

#undef EXPORT

#endif // _WE_DCTNRY_COMPRESS_H_
