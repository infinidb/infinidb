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
* $Id: we_indextree.h 3720 2012-04-04 18:18:49Z rdempsey $
*
******************************************************************************************/
/** @file */

#ifndef _WE_INDEXTREE_H_
#define _WE_INDEXTREE_H_

#include <stdlib.h>

#include <we_dbfileop.h>
#include <we_index.h>
#include <we_freemgr.h>
#include <we_indexlist.h>

#if defined(_MSC_VER) && defined(WRITEENGINEINDEXTREE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{

/** Class ColumnOp */
class IndexTree : public DbFileOp
{
public:
   /**
    * @brief Constructor
    */
   EXPORT IndexTree();

   /**
    * @brief Default Destructor
    */
   EXPORT ~IndexTree();

   /**
    * @brief A wrapper for the call to free manager
    */
   EXPORT const int      assignSegment(  int segmentType, IdxEmptyListEntry* assignPtr, int no );

   /**
    * @brief Build empty index tree part
    */
   EXPORT const int      buildEmptyTreePart( const i64 key, const int width, const RID rid, const int startBitTestNo, const int offset = 0 );

   /**
    * @brief Build empty index branch
    */
   EXPORT const int      buildEmptyIndexTreeBranch( const i64 key, const int width, const RID rid, const int rootTestbitVal );

   /**
    * @brief Build exist index branch
    */
   EXPORT const int      buildExistIndexTreeBranch( const i64 key, const int width, const RID rid, const int rootTestbitVal, IdxBitmapPointerEntry bitmapEntry );

   /**
    * @brief Calculate bit test array
    */
   EXPORT const int      calculateBittestArray();

   /**
    * @brief Close index
    */
   EXPORT void           closeIndex();

   /**
    * @brief Clear index tree member variables
    */
   void           clear() { clearBlock( &m_rootBlock ); }

   /**
    * @brief Create index related files
    */
   EXPORT const int      createIndex( const FID treeFid, const FID listFid, const bool useFreeMgrFlag = true  );

   /**
    * @brief Delete a value from an index
    */
   EXPORT const int      deleteIndex( const i64 key, const int width, const RID rid );

   /**
    * @brief Drop index related files
    */
   const int      dropIndex( const FID treeFid, const FID listFid  )
#ifdef BROKEN_BY_MULTIPLE_FILES_PER_OID
                  { m_cbTree.file.pFile = m_fileopTree.openFile(treeFid);
                    m_cbList.file.pFile = m_fileopList.openFile(listFid); 
                    closeIndex(); m_useMultiCol = false; deleteFile( treeFid ); deleteFile( listFid ); return NO_ERROR; }
#endif
                  { std::string segFile;
                    m_cbTree.file.pFile = m_fileopTree.openFile(
                       treeFid,0,0,0, segFile);
                    m_cbList.file.pFile = m_fileopList.openFile(
                       listFid,0,0,0, segFile); 
                    closeIndex(); m_useMultiCol = false; deleteFile( treeFid ); deleteFile( listFid ); return NO_ERROR; }

   /**
    * @brief Get the test bit value
    */
   EXPORT const bool     getTestbitValue( const i64 key, const int width, const int curTestNo, int* bittestVal );

   /**
    * @brief Get the match entry in the tree
    */
   EXPORT const bool     getTreeMatchEntry( DataBlock* block, const i64 sbid, const i64 entry, const int width,
                                     const int allocCount, const bool* entryMap, int* matchEntry, IdxBitTestEntry* checkEntry );

   /**
    * @brief Get the tree node summary information
    */
   EXPORT const int      getTreeNodeInfo( DataBlock* block, const i64 sbid, const i64 entry, const int width,
                                   const IdxTreeGroupType group, int* allocCount, int* realCount, bool* entryMap );

   /**
    * @brief Check index address pointer is empty or not
    */
   EXPORT const bool     isAddrPtrEmpty( void* pStruct, const IdxTreeEntryType entryType ) const;

   /**
    * @brief Check whether the tree is empty
    */
   EXPORT const bool     isTreeEmpty();

   /**
    * @brief Init index
    */
   EXPORT const int      initIndex( const FID treeFid, const FID listFid );

   /**
    * @brief Move tree entries
    */
   EXPORT const int      moveEntry( const i64 oldFbo, const i64 oldSbid, const i64 oldEntry, const int width,  const i64 newFbo,
                             const i64 newSbid, const i64 newEntry, const int newGroup, const int allocCount, bool* entryMap, int* moveCount, const int newAllocCount = 0 );

   /**
    * @brief Open index related files
    */
   EXPORT const int      openIndex( const FID treeFid, const FID listFid  );

   /**
    * @brief Process index, including delete and search
    */
   EXPORT const int      processIndex( const i64 key, const int width, const RID rid, IdxEmptyListEntry& listHdrAddr, const bool bDelete = true );

   /**
    * @brief A wrapper for the call to free manager
    */
   EXPORT const int      releaseSegment( int segmentType, IdxEmptyListEntry* myPtr );

   /**
    * @brief Clean up index file
    */
   EXPORT const int      resetIndexFile( const FID treeFid, const FID listFid  );

   /**
    * @brief Setup bitset by column
    */
   EXPORT const int      setBitsetColumn( void* val, const int pos, const int width, const ColType colType );

   /**
    * @brief Set bit test entry
    */
   EXPORT void           setBittestEntry( IdxBitTestEntry* bittestEntry, const i64 testbitVal, const i64 group, const i64 fbo, const i64 sbid, const i64 entry, const i64 entryType = BIT_TEST ) const;

   /**
    * @brief Set blank entry
    */
   void           setBlankEntry( void* pStruct ) const { memset( pStruct, 0, 8 ); }

   /**
    * @brief Set empty list ptr entry
    */
   EXPORT void           setEmptyListEntry( IdxEmptyListEntry* myEntry, const i64 group, const i64 fbo, const i64 sbid, const i64 entry ) const;

   /**
    * @brief Set transaction Id
    */
   EXPORT void           setTransId( const TxnID txnId ) { FileOp::setTransId( txnId ); m_cbTree.session.txnid = m_cbList.session.txnid = txnId; m_freeMgr.setTransId(txnId); m_listMgr.setTransId(txnId);}

   /**
    * @brief Set use brm flag
    */

   /**
    * @brief Update a value in an index
    */
   EXPORT const int      updateIndex( const i64 key, const int width, const RID rid );

   EXPORT const int      updateListFile( const i64 key, const int width, const RID rid, const int curLevel, const i64 group, const int allocCount, const int useCount, const int offset, const bool addFlag = false );


   // internal use functions
   EXPORT void           clearTree( IdxTree* myTree );
   EXPORT void           clearTreeNode( IdxTreeNode* myNode );
   const IdxTree& getTree() { return m_tree; }
   const DataBlock& getRootBlock() { return m_rootBlock; }
   const bool     getUseMultiRid() { return m_useMultiRid; }

   EXPORT void           setTreeHeader( IdxTree* myTree, const i64 key, const RID rid, const int width,
                                  const int testbitVal, const IdxBitmapPointerEntry bitmapEntry );
   EXPORT void           setTreeNode( IdxTreeNode* myNode, const int level, const int allocCount, const int useCount,
                                 const int offset, const IdxBitTestEntry nextEntry, const IdxBitTestEntry curEntry  );

   EXPORT const int      updateIndexList( const i64 key, const int width, const RID rid, IdxEmptyListEntry* myEntry, const int no, const bool addFlag = false);

   EXPORT void           printMemSubBlock( DataBlock* curBlock, const int sbid, const bool bNoZero = false );
   EXPORT void           printSubBlock( const int fbo, const int sbid, const bool bNoZero = false );

   void           setAssignFbo( const int fbo ) { m_assignFbo = fbo; }     // internal testing purpose only
   void           setUseFreeMgr( const bool val ) { m_useFreeMgr = val; }
   void           setUseListMgr( const bool val ) { m_useListMgr = val;  }
   void           setUseMultiCol( const bool val ) { m_useMultiCol = val;  }
   void           setUseMultiRid( const bool val ) { m_useMultiRid = val;  }
   
   void           setCbTree(CommBlock& cb){ memcpy(&m_cbTree.session, &cb.session, 
                                                     sizeof(cb.session));
                                            memcpy(&m_cbTree.file, &cb.file, 
                                                     sizeof(cb.file)); }
   const CommBlock& getCbTree() const {return m_cbTree;}

   void           setCbList(CommBlock& cb){memcpy(&m_cbList.session, &cb.session, 
                                                     sizeof(cb.session));
                                           memcpy(&m_cbList.file, &cb.file, 
                                                         sizeof(cb.file)); }
   const CommBlock& getCbList() const {return m_cbList;}

   DataBlock      m_rootBlock;                     /** @brief This block contains sub block 0 and sub block 1 */
   CommBlock      m_cbTree;                        /** @brief index tree */
   CommBlock      m_cbList;                        /** @brief index list */

   IdxTree        m_tree;                          /** @brief index tree node */
   FreeMgr        m_freeMgr;                       /** @brief index free mgr */
   IndexList      m_listMgr;                       /** @brief index list mgr */

   IdxMultiColKey m_multiColKey;                   /** @brief index multi-column key */
   IdxMultiRid    m_multiRid;                      /** @brief index multi rids */
   FileOp         m_fileopTree;
   FileOp         m_fileopList;
private:
   // internal testing flags
   bool           m_useFreeMgr;
   bool           m_useListMgr;
   bool           m_useMultiCol;
   bool           m_useMultiRid;
   int            m_assignFbo;
};

} //end of namespace

#undef EXPORT

#endif // _WE_INDEXTREE_H_
