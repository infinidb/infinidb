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

/******************************************************************************************
* $Id: we_cache.cpp 33 2006-10-30 13:45:13Z wzhou  $
*
******************************************************************************************/
/** @file */

#include <we_cache.h>

using namespace std;

namespace WriteEngine
{

   CacheControl*  Cache::m_cacheParam = NULL;
   FreeBufList*   Cache::m_freeList = NULL;
   CacheMap*      Cache::m_lruList = NULL;
   CacheMap*      Cache::m_writeList = NULL;
#ifdef _MSC_VER
   __declspec(dllexport)
#endif
   bool           Cache::m_useCache = false;
   /***********************************************************
    * DESCRIPTION:
    *    Clear all list and free memory
    * PARAMETERS:
    *    none
    * RETURN:
    *    NO_ERROR if success, other otherwise
    ***********************************************************/
   void  Cache::clear()
   {  
      CacheMapIt     it;
      BlockBuffer*   block;
      size_t         i;

      // free list
      if( m_freeList != NULL ) {
         for( i = 0; i < m_freeList->size(); i++ ) {
            block = m_freeList->at(i);
            block->clear();
         }
      }

      // LRU list
      if( m_lruList != NULL ) {
         for( it = m_lruList->begin(); it != m_lruList->end(); it++ ) {
            block = it->second;
            block->clear();
            m_freeList->push_back( block );
         }
         m_lruList->clear();
      }

      // Write list
      if( m_writeList != NULL ) {
         for( it = m_writeList->begin(); it != m_writeList->end(); it++ ) {
            block = it->second;
            block->clear();
            m_freeList->push_back( block );
         }
         m_writeList->clear();
      }
   }

   /***********************************************************
    * DESCRIPTION:
    *    Flush write cache
    * PARAMETERS:
    *    none
    * RETURN:
    *    NO_ERROR if success, other otherwise
    ***********************************************************/
   const int  Cache::flushCache()
   {  
      bool           bHasReadBlock = false;
      BlockBuffer*   curBuf;

      // add lock here
      if( m_lruList && m_lruList->size() > 0 ) {
         bHasReadBlock = true;
         for( CacheMapIt it = m_lruList->begin(); it != m_lruList->end(); it++ ) {
            curBuf = it->second;
            curBuf->clear();
            m_freeList->push_back( curBuf );
         }
         m_lruList->clear();
      }

      // must write to disk first
      if( m_writeList && m_writeList->size() > 0 ) {
         if( !bHasReadBlock ) 
            for( CacheMapIt it = m_writeList->begin(); it != m_writeList->end(); it++ ) {
               curBuf = it->second;
               curBuf->clear();
               m_freeList->push_back( curBuf );
            }
         else 
            for( CacheMapIt it = m_writeList->begin(); it != m_writeList->end(); it++ ) {
               curBuf = it->second;
               (*curBuf).block.dirty = false;
               processCacheMap( m_lruList, curBuf, INSERT );
            }
         m_writeList->clear();

      } // end of if( m_writeList->size()
      // add unlock here
      return NO_ERROR;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Free memory
    * PARAMETERS:
    *    none
    * RETURN:
    *    NO_ERROR if success, other otherwise
    ***********************************************************/
   void  Cache::freeMemory()
   {  
      CacheMapIt     it;
      BlockBuffer*   block;
      size_t         i;

      // free list
      if( m_freeList != NULL ) {
         for( i = 0; i < m_freeList->size(); i++ ) {
            block = m_freeList->at(i);
            block->freeMem();
            delete block;
         }
         m_freeList->clear();
         delete m_freeList;
         m_freeList = NULL;
      }

      // LRU list
      if( m_lruList != NULL ) {
         for( it = m_lruList->begin(); it != m_lruList->end(); it++ ) {
            block = it->second;
            block->freeMem();
            delete block;
         }
         m_lruList->clear();
         delete m_lruList;
         m_lruList = NULL;
      }

      // Write list
      if( m_writeList != NULL ) {
         for( it = m_writeList->begin(); it != m_writeList->end(); it++ ) {
            block = it->second;
            block->freeMem();
            delete block;
         }
         m_writeList->clear();
         delete m_writeList;
         m_writeList = NULL;
      }

      // param
      if( m_cacheParam != NULL ) {
         delete m_cacheParam;
         m_cacheParam = NULL;
      }
   }

   /***********************************************************
    * DESCRIPTION:
    *    get a list size
    * PARAMETERS:
    *    listType - List type
    * RETURN:
    *    NO_ERROR if success, other otherwise
    ***********************************************************/
   const int  Cache::getListSize( const CacheListType listType )
   {
      int size = 0;

      if( !m_useCache )
         return size;

      switch( listType ) {
         case FREE_LIST: size = m_freeList->size(); break;
         case LRU_LIST: size = m_lruList->size(); break;
         case WRITE_LIST: 
         default:
                        size = m_writeList->size(); break;
      }
      return size;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Init all parameters and list 
    * PARAMETERS:
    *    totalBlock - total blocks 
    *    chkPoint - checkpoint interval
    *    pctFree - percentage free
    * RETURN:
    *    NO_ERROR if success, other otherwise
    ***********************************************************/
   void  Cache::init( const int totalBlock, const int chkPoint, const int pctFree )
   {  
      BlockBuffer*   buffer;

      if( m_cacheParam && m_freeList && m_lruList && m_writeList )
         return;

      m_cacheParam = new CacheControl();
      m_cacheParam->totalBlock = totalBlock;
      m_cacheParam->checkInterval = chkPoint;
      m_cacheParam->pctFree = pctFree;

      m_freeList = new FreeBufList();
      m_lruList = new CacheMap();
      m_writeList = new CacheMap();

      for( int i = 0; i < m_cacheParam->totalBlock; i++ ) {
         buffer = new BlockBuffer();
         buffer->init();
         m_freeList->push_back( buffer );
      }
   }

   /***********************************************************
    * DESCRIPTION:
    *    Insert a buffer to LRU list
    * PARAMETERS:
    *    cb - Comm Block
    *    lbid - lbid value
    *    fbo - fbo
    *    buf - input buffer
    * RETURN:
    *    NO_ERROR if success, other otherwise
    ***********************************************************/
   const int  Cache::insertLRUList( CommBlock& cb, const uint64_t lbid, const uint64_t fbo, const unsigned char* buf  )
   {  
      BlockBuffer*      buffer;
      vector<BlockBuffer*>::iterator  it;

      if( m_freeList->size() == 0 )
         return ERR_FREE_LIST_EMPTY;

      // make sure flush first if necessary
      it = m_freeList->begin();
      buffer = *it;
      memcpy( (*buffer).block.data, buf, BYTE_PER_BLOCK );
      (*buffer).listType = LRU_LIST;
      (*buffer).block.lbid = lbid;
      (*buffer).block.fbo = fbo;
      (*buffer).block.dirty = false;
      (*buffer).block.hitCount = 1;
      (*buffer).cb.file.oid = cb.file.oid;
      (*buffer).cb.file.pFile = cb.file.pFile;

      RETURN_ON_ERROR( processCacheMap( m_lruList, buffer, INSERT ) );
      m_freeList->erase( it );

      return NO_ERROR;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Load cache block
    * PARAMETERS:
    *    key - Cache key
    *    buf - output buffer
    * RETURN:
    *    NO_ERROR if success, other otherwise
    ***********************************************************/
   const int  Cache::loadCacheBlock( const CacheKey& key, unsigned char* buf  )
   {  
      BlockBuffer*      buffer;
      CacheMapIt        iter;

      iter = m_lruList->find( key );
      if( iter != m_lruList->end() ) 
         buffer = iter->second;
      else {
         iter = m_writeList->find( key );
         if( iter != m_writeList->end() ) 
            buffer = iter->second;
         else
            return ERR_CACHE_KEY_NOT_EXIST;
      }
      memcpy( buf, (*buffer).block.data, BYTE_PER_BLOCK );
      (*buffer).block.hitCount++;

      return NO_ERROR;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Modify cache block
    * PARAMETERS:
    *    key - Cache key
    *    buf - output buffer
    * RETURN:
    *    NO_ERROR if success, other otherwise
    ***********************************************************/
   const int  Cache::modifyCacheBlock( const CacheKey& key, const unsigned char* buf  )
   {  
      BlockBuffer*      buffer;
      CacheMapIt        iter;

      iter = m_lruList->find( key );
      if( iter != m_lruList->end() ) {
         buffer = iter->second;
         (*buffer).listType = WRITE_LIST;
         (*buffer).block.dirty = true;

         (*m_writeList)[key] = iter->second;
         m_lruList->erase( iter );

      }
      else {
         iter = m_writeList->find( key );
         if( iter != m_writeList->end() ) 
            buffer = iter->second;
         else
            return ERR_CACHE_KEY_NOT_EXIST;
      }
      memcpy( (*buffer).block.data, buf, BYTE_PER_BLOCK );
      (*buffer).block.hitCount++;

      return NO_ERROR;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Print cache list
    * PARAMETERS:
    *    none
    * RETURN:
    *    none
    ***********************************************************/
   void  Cache::printCacheList()
   {  
      BlockBuffer*      buffer;
      int               i = 0;

      if( !m_useCache )
         return;

      cout << "\nFree List has " << m_freeList->size() << " elements" << endl;
      cout << "LRU List has " << m_lruList->size() << " elements" << endl;
      for( CacheMapIt it = m_lruList->begin(); it != m_lruList->end(); it++ ) {
         buffer = it->second;
         cout  << "\t[" << i++ << "] key=" << it->first << " listType=" << buffer->listType 
               << " oid=" << (*buffer).cb.file.oid << " fbo=" << (*buffer).block.fbo 
               << " dirty=" << (*buffer).block.dirty << " hitCount=" << (*buffer).block.hitCount << endl;
      }

      i = 0;
      cout << "Write List has " << m_writeList->size() << " elements" << endl;
      for( CacheMapIt it = m_writeList->begin(); it != m_writeList->end(); it++ ) {
         buffer = it->second;
         cout  << "\t[" << i++ << "] key=" << it->first << " listType=" << buffer->listType 
               << " oid=" << (*buffer).cb.file.oid << " fbo=" << (*buffer).block.fbo 
               << " dirty=" << (*buffer).block.dirty << " hitCount=" << (*buffer).block.hitCount << endl;
      }
   }

   /***********************************************************
    * DESCRIPTION:
    *    Process a buffer in a cache map
    * PARAMETERS:
    *    buffer - block buffer
    *    opType - insert or delete
    * RETURN:
    *    NO_ERROR if success, other otherwise
    ***********************************************************/
   const int  Cache::processCacheMap( CacheMap* map, BlockBuffer* buffer, OpType opType )
   {  
      RETURN_ON_NULL( buffer, ERR_NULL_BLOCK );
      CacheMapIt iter;

      CacheKey key = getCacheKey( buffer );
      iter = map->find( key );

      // only handle insert and delete
      if( iter == map->end() ) {
         if( opType == INSERT )
            (*map)[key] = buffer;
         else
            return ERR_CACHE_KEY_NOT_EXIST;
      }
      else {
         if( opType == INSERT )
            return ERR_CACHE_KEY_EXIST;
         else
            map->erase( iter );
      }

      return NO_ERROR;
   }



} //end of namespace

