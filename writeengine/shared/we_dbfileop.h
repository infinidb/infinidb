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

//  $Id: we_dbfileop.h 4726 2013-08-07 03:38:36Z bwilkinson $

/** @file */

#ifndef _WE_DBFILEOP_H_
#define _WE_DBFILEOP_H_

#include "we_type.h"
#include "we_fileop.h"
#include "we_blockop.h"
#include "we_cache.h"


#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{

// forward reference
class ChunkManager;


/** Class DbFileOp */
class DbFileOp : public FileOp
{
public:
   /**
    * @brief Constructor
    */
    EXPORT DbFileOp();

   /**
    * @brief Default Destructor
    */
    EXPORT virtual ~DbFileOp();

    EXPORT virtual int   flushCache();

   /**
    * @brief Get an entry within a subblock
    */
    EXPORT void          getSubBlockEntry( unsigned char* blockBuf,
                                           const int sbid,
                                           const int entryNo,
                                           const int width,
                                           void* pStruct ) ;

   /**
    * @brief Get an entry within a subblock using block information
    */
    void                 getSubBlockEntry( DataBlock* block,
                                           const int sbid,
                                           const int entryNo,
                                           const int width,
                                           void* pStruct )
    { getSubBlockEntry( block->data, sbid, entryNo, width, pStruct );}

   /**
    * @brief Read DB file to a buffer
    */
    EXPORT virtual int   readDBFile(       IDBDataFile* pFile,
                                           unsigned char* readBuf,
                                           const uint64_t lbid,
                                           const bool isFbo = false );
    EXPORT int           readDBFile(       CommBlock& cb,
                                           unsigned char* readBuf,
                                           const uint64_t lbid );

    EXPORT int           readDBFile(       IDBDataFile* pFile,
                                           DataBlock* block,
                                           const uint64_t lbid,
                                           const bool isFbo = false );
    int                  readDBFile(       CommBlock& cb,
                                           DataBlock* block,
                                           const uint64_t lbid )
    { return readDBFile( cb, block->data, lbid );}

   /**
    * @brief Get an entry within a subblock and also populate block buffer
    *
    */
    EXPORT const int     readSubBlockEntry(IDBDataFile* pFile,
                                           DataBlock* block,
                                           const uint64_t lbid,
                                           const int sbid,
                                           const int entryNo,
                                           const int width,
                                           void* pStruct ) ;

    EXPORT const int     readSubBlockEntry(CommBlock& cb,
                                           DataBlock* block,
                                           const uint64_t lbid,
                                           const int sbid,
                                           const int entryNo,
                                           const int width,
                                           void* pStruct );

   /**
    * @brief Set an entry within a subblock
    */
    EXPORT void          setSubBlockEntry( unsigned char* blockBuf,
                                           const int sbid,
                                           const int entryNo,
                                           const int width,
                                           const void* pStruct ) ;

   /**
    * @brief Set an entry within a subblock using block information
    */
    void                 setSubBlockEntry( DataBlock* block,
                                           const int sbid,
                                           const int entryNo,
                                           const int width,
                                           const void* pStruct )
    { block->dirty = true;
      setSubBlockEntry( block->data, sbid, entryNo, width, pStruct ); }

   /**
    * @brief Lbid Write a buffer to a DB file
    */
    EXPORT virtual int   writeDBFile(      IDBDataFile* pFile,
                                           const unsigned char* writeBuf,
                                           const uint64_t lbid,
                                           const int numOfBlock = 1 );
    EXPORT int           writeDBFile(      CommBlock& cb,
                                           const unsigned char* writeBuf,
                                           const uint64_t lbid,
                                           const int numOfBlock = 1 );

   /**
    * @brief Write designated block(s) w/o writing to Version Buffer or cache.
    */
    EXPORT  int          writeDBFileNoVBCache(CommBlock & cb,
                                           const unsigned char * writeBuf,
                                           const int fbo,
                                           const int numOfBlock = 1);
    EXPORT virtual int   writeDBFileNoVBCache(IDBDataFile *pFile,
                                           const unsigned char * writeBuf,
                                           const int fbo,
                                           const int numOfBlock = 1);

    int                  writeDBFile(      IDBDataFile* pFile,
                                           DataBlock* block,
                                           const uint64_t lbid )
    { block->dirty=false; return writeDBFile( pFile, block->data, lbid ); }
    int                  writeDBFile(      CommBlock& cb,
                                           DataBlock* block,
                                           const uint64_t lbid )
    { return writeDBFile( cb, block->data, lbid ); }

    EXPORT virtual int   writeDBFileFbo(   IDBDataFile* pFile,
                                           const unsigned char* writeBuf,
                                           const uint64_t fbo,
                                           const int numOfBlock  );

    int                  writeDBFileNoVBCache(CommBlock & cb,
                                           DataBlock * block,
                                           const int fbo)
    { return writeDBFileNoVBCache(cb, block->data, fbo); }

   /**
    * @brief Write a sub block entry directly to a DB file
    */
    EXPORT const int     writeSubBlockEntry(IDBDataFile* pFile,
                                           DataBlock* block,
                                           const uint64_t lbid,
                                           const int sbid,
                                           const int entryNo,
                                           const int width,
                                           void* pStruct );

    EXPORT const int     writeSubBlockEntry(CommBlock& cb,
                                           DataBlock* block,
                                           const uint64_t lbid,
                                           const int sbid,
                                           const int entryNo,
                                           const int width,
                                           void* pStruct ) ;

   /**
    * @brief Write to version buffer
    */
    EXPORT const int     writeVB(          IDBDataFile* pFile,
                                           const OID oid,
                                           const uint64_t lbid );

    EXPORT virtual int   readDbBlocks(     IDBDataFile* pFile,
                                           unsigned char* readBuf,
                                           uint64_t fbo,
                                           size_t n);

    EXPORT virtual int   restoreBlock(     IDBDataFile* pFile,
                                           const unsigned char* writeBuf,
                                           uint64_t fbo);

    EXPORT virtual IDBDataFile* getFilePtr(const Column& column,
                                           bool useTmpSuffix);

    virtual void chunkManager(ChunkManager* ptr) { m_chunkManager = ptr;  }
    virtual ChunkManager* chunkManager()         { return m_chunkManager; }

protected:
    ChunkManager*        m_chunkManager;

private:

};

} //end of namespace

#undef EXPORT

#endif // _WE_DBFILEOP_H_
