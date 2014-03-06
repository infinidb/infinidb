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
* $Id: we_cache.h 33 2006-10-30 13:45:13Z wzhou $
*
******************************************************************************************/
/** @file */

#ifndef _WE_CACHE_H_
#define _WE_CACHE_H_
#include <iostream>
#ifdef _MSC_VER
#include <unordered_map>
#else
#if __GNUC__ == 4 && __GNUC_MINOR__ < 2
#include <ext/hash_map>
#else
#include <tr1/unordered_map>
#endif
#endif
#include <map>

#include <we_obj.h>
#include <we_convertor.h>

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif
/** Namespace WriteEngine */
namespace WriteEngine
{
typedef std::vector<BlockBuffer*>   FreeBufList;      /** @brief Free buffer list */
//typedef std::string                 CacheKey;         /** @brief Key definition */
typedef uint64_t                    CacheKey;         /** @brief Key definition */

//typedef std::map<CacheKey, BlockBuffer*, std::greater<CacheKey> >   CacheMap;      /** @brief Cache map */
//typedef CacheMap::iterator          CacheMapIt;       /** @brief CacheMap iterator */

template<class T>struct  hashCacheKey{ };
template<> struct hashCacheKey<CacheKey>
{
   size_t
   operator()(CacheKey __x) const
   { return __x; }
};

struct eqCacheKey
{
  bool operator() (const CacheKey k1, const CacheKey k2 ) const
  {
     return k1 == k2; 
  }
};

//typedef hash_map<Signature, TokenStruc, signatureHash<Signature>, eqSig> DCTNRYHASHMAP;
#if __GNUC__ == 4 && __GNUC_MINOR__ < 2
typedef __gnu_cxx::hash_map<CacheKey, BlockBuffer*, hashCacheKey<CacheKey>, eqCacheKey> CacheMap; 
#else
typedef std::tr1::unordered_map<CacheKey, BlockBuffer*, hashCacheKey<CacheKey>, eqCacheKey> CacheMap; 
#endif
//typedef __gnu_cxx::hash_map<CacheKey, BlockBuffer*> CacheMap; 
typedef CacheMap::iterator          CacheMapIt;

//typedef CacheMap                    LRUBufList;       /** @brief Least Recent Used Buffer list */
//typedef CacheMap                    WriteBufList;     /** @brief Write buffer list */

/** Class Cache */
class Cache 
{
public:
   /**
    * @brief Constructor
    */
   Cache() {}

   /**
    * @brief Default Destructor
    */
   ~Cache() {}

   /**
    * @brief Check whether cache key exists
    */
   static const bool    cacheKeyExist( CacheMap* map, const OID oid, const uint64_t lbid ) { CacheKey key = getCacheKey( oid, lbid ); return map->find(key) == map->end() ? false: true; }
   static const bool    cacheKeyExist( CacheMap* map, BlockBuffer* buffer ) { return cacheKeyExist( map, (*buffer).cb.file.oid, (*buffer).block.lbid ); }
   static const bool    cacheKeyExist( const OID oid, const uint64_t lbid ) { return cacheKeyExist( m_lruList, oid, lbid ) || cacheKeyExist( m_writeList, oid, lbid ); }

   /**
    * @brief Clear the buffer
    */
   EXPORT static void          clear();

   /**
    * @brief Free the buffer memory
    */
   EXPORT static void          freeMemory();

   /**
    * @brief Flush the write cache
    */
   EXPORT static const int     flushCache();

   /**
    * @brief Get the cache key
    */
   static CacheKey      getCacheKey( const OID oid, const uint64_t lbid ) { CacheKey key = lbid; /*Convertor::int2Str( oid ) + "|" + Convertor::int2Str(lbid)*/; return key; }
   static CacheKey      getCacheKey( const BlockBuffer* buffer ) { return getCacheKey( (*buffer).cb.file.oid, (*buffer).block.lbid ); }

   EXPORT static const int     getListSize( const CacheListType listType );

   /**
    * @brief Init the buffers
    */
   EXPORT static void          init( const int totalBlock, const int chkPoint, const int pctFree );
   static void          init() { init( DEFAULT_CACHE_BLOCK, DEFAULT_CHK_INTERVAL, DEFAULT_CACHE_PCT_FREE ); }

   /**
    * @brief Insert into LRU list
    */
   EXPORT static const int     insertLRUList( CommBlock& cb, const uint64_t lbid, const uint64_t fbo, const unsigned char* buf );
   static const int     insertLRUList( CommBlock& cb, const uint64_t lbid, const uint64_t fbo, const DataBlock& block  ) { return insertLRUList( cb, lbid, fbo, block.data ); }

   /**
    * @brief Insert into Write list
    */
//   static const int     insertWriteList( const CacheKey& key );

   /**
    * @brief Load cache block to a buffer
    */
   static const int     loadCacheBlock( const CacheKey& key, DataBlock& block  ) { return loadCacheBlock( key, block.data ); }
   EXPORT static const int     loadCacheBlock( const CacheKey& key, unsigned char* buf  );

   /**
    * @brief Modify a cache block
    */
   static const int     modifyCacheBlock( const CacheKey& key, const DataBlock& block  ) { return modifyCacheBlock( key, block.data ); }
   EXPORT static const int     modifyCacheBlock( const CacheKey& key, const unsigned char* buf  );

   /**
    * @brief Print
    */
   EXPORT static void          printCacheMapList( const CacheMap* map );
   EXPORT static void          printCacheList();

   /**
    * @brief Insert/Delete an element in cache map
    */
   EXPORT static const int     processCacheMap( CacheMap* map, BlockBuffer* buffer, OpType opType );

   // accessory
   static const int     getTotalBlock() { return m_cacheParam->totalBlock; }
   static const bool    getUseCache() { return m_useCache; }
   static void          setUseCache( const bool flag ) { m_useCache = flag; }

   static CacheControl* m_cacheParam;                 // Cache parameters
   static FreeBufList*  m_freeList;                   // free buffer list
   static CacheMap*     m_lruList;                    // LRU buffer list
   static CacheMap*     m_writeList;                  // Write buffer list

#if defined(_MSC_VER) && !defined(WRITEENGINE_DLLEXPORT)
   __declspec(dllimport)
#endif
   EXPORT static bool          m_useCache;                   // Use cache flag
private:

};

} //end of namespace

#undef EXPORT

#endif // _WE_CACHE_H_
