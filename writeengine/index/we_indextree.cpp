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
* $Id: we_indextree.cpp 4450 2013-01-21 14:13:24Z rdempsey $
*
******************************************************************************************/
/** @file */

#include <stdio.h>
#include <string.h>

#define WRITEENGINEINDEXTREE_DLLEXPORT
#include "we_indextree.h"
#undef WRITEENGINEINDEXTREE_DLLEXPORT

namespace WriteEngine
{ 

   /**
    * Constructor
    */
   IndexTree::IndexTree()
   : m_useFreeMgr( true ), m_useListMgr( true ), m_useMultiCol( false), m_useMultiRid( false ), m_assignFbo( 0 )
   {
      clearBlock( &m_rootBlock );
   }

   /**
    * Default Destructor
    */
   IndexTree::~IndexTree()
   {} 

/*
   const int IndexTree::getIndexTreeBitTestEntry( uint64_t entry, short* entryType, int* bitTest, int* group, int32_t* treePointer )
   {
      *treePointer = entry & IDX_PTR_MASK;
      entry = entry >> IDX_PTR_SIZE + 2; // skip one spare bit and bit-compare bit

      *group = entry & THREE_BIT_MASK;
      entry = entry >> IDX_GROUP_SIZE;

      *bitTest = entry & TEN_BIT_MASK;
      *entryType = entry >> IDX_BITTEST_SIZE;

      *entryType = entry & THREE_BIT_MASK;

      return NO_ERROR;
   }

   const void IndexTree::setIndexTreeBitTestEntry( uint64_t* entry, short entryType, int bitTest, int group, int32_t treePointer )
   {
      memset( entry, 0, ROW_PER_BYTE );

      *entry = treePointer | group << ( IDX_PTR_SIZE + 2 );
      *entry = *entry | bitTest << ( IDX_PTR_SIZE + 2 + IDX_GROUP_SIZE );
      *entry = *entry | entryType << ( IDX_PTR_SIZE + 2 + IDX_GROUP_SIZE + IDX_TYPE_SIZE );

      return NO_ERROR;
   }
*/
   /***********************************************************
    * DESCRIPTION:
    *    Clear the tree
    * PARAMETERS:
    *    myTree - tree pointer
    * RETURN:
    *    none
    ***********************************************************/
   void IndexTree::clearTree( IdxTree* myTree )
   {
      myTree->width = myTree->key = myTree->rid = myTree->maxLevel = 0;
      for( int i = 0; i < IDX_MAX_TREE_LEVEL; i++ )
         clearTreeNode( &myTree->node[i] );
   }

   /***********************************************************
    * DESCRIPTION:
    *    Clear the tree node
    * PARAMETERS:
    *    myNode - node pointer
    * RETURN:
    *    none
    ***********************************************************/
   void IndexTree::clearTreeNode( IdxTreeNode* myNode )
   {
      myNode->level = 0;
      myNode->allocCount = 0;
      myNode->useCount = 0;
//      myNode->group = 0;
      myNode->used = false;
      setBlankEntry( &myNode->next );
      setBlankEntry( &myNode->current );
   }


   /***********************************************************
    * DESCRIPTION:
    *    Assign segment from free manager
    * PARAMETERS:
    *    segmentType - group type
    *    assignPtr - the assigned ptr
    *    no - internal debug use flag
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::assignSegment( int segmentType, IdxEmptyListEntry* assignPtr, int no )
   {
      int rc = NO_ERROR;

      m_rootBlock.dirty = true;

      if( m_useFreeMgr ) {
         if( isDebug( DEBUG_3 )) {
            printf( "\n++++++ Before Assign"); 
            printMemSubBlock( &m_rootBlock, 0, true );
         }

         rc = m_freeMgr.assignSegment( /*m_pTreeFile*/m_cbTree, &m_rootBlock, TREE, (IdxTreeGroupType) segmentType, assignPtr /*, TREE */);
         if( isDebug( DEBUG_3 )) {
            printf( "\nAssign the pointer, entry segment=%d fbo=%2d sbid=%2d entry=%2d", segmentType, (int)assignPtr->fbo, (int)assignPtr->sbid, (int)assignPtr->entry ); 
            printMemSubBlock( &m_rootBlock, 0, true );
         }
      }
      else {
         assignPtr->fbo = 1 + m_assignFbo;
         assignPtr->sbid = 3 + no;
         assignPtr->entry = 4 ;
      }

      return rc;
   }


   /***********************************************************
    * DESCRIPTION:
    *    Build a complete empty tree branch
    * PARAMETERS:
    *    key - key value
    *    width - key width
    *    rid - row id
    *    rootTestbitVal - test bit test value at root level
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::buildEmptyIndexTreeBranch( const uint64_t key, const int width, const RID rid, const int rootTestbitVal )
   {
      int                     rc;
      IdxBitmapPointerEntry   bitmapEntry = {0};

      rc = buildEmptyTreePart( key, width, rid, 1 );

      // set the root level bitmapPointerMap
      bitmapEntry.type = BITMAP_PTR; //BIT_TEST;
      bitmapEntry.fbo = m_tree.node[0].next.fbo;
      bitmapEntry.sbid = m_tree.node[0].next.sbid;
      bitmapEntry.entry = m_tree.node[0].next.entry;

      setSubBlockEntry( m_rootBlock.data, IDX_BITMAP_SUBBLOCK_NO, rootTestbitVal, MAX_COLUMN_BOUNDARY, &bitmapEntry );
      m_rootBlock.dirty = true;

      return rc;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Build exist index tree branch
    * PARAMETERS:
    *    key - key value
    *    width - key width
    *    rid - row id
    *    rootTestbitVal - test bit at root level
    *    bitmapEntry - current bitmap entry
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::buildExistIndexTreeBranch( const uint64_t key, const int width, const RID rid, const int rootTestbitVal, IdxBitmapPointerEntry bitmapEntry )
   {
      int                     rc = NO_ERROR, loopCount, testbitVal, i, j, allocCount, realCount, matchPos, moveCount, parentLevel = 0, curLevel, curOffset = 0;
      bool                    bSuccess;
      IdxEmptyListEntry       assignPtrEntry, releasePtrEntry;
      IdxBitTestEntry         bittestEntry, matchBitTestEntry, parentBitTestEntry, curEntry;
      DataBlock               curBlock, parentBlock;
      bool                    bAddFlag = false, bDone = false, bFound, bExitOuterLoop, bExitInnerLoop, entryMap[ENTRY_PER_SUBBLOCK];

      bExitOuterLoop = false;
      loopCount = m_tree.maxLevel;
      for( i = 1; !bExitOuterLoop && i < loopCount; i++ ) {
         // load the block
         rc = readSubBlockEntry( m_cbTree, &curBlock, m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid,
                                 m_tree.node[parentLevel].next.entry, MAX_COLUMN_BOUNDARY, &bittestEntry );
         if( rc != NO_ERROR )
            return rc;

         if( i == 1 && isAddrPtrEmpty( &bittestEntry, BIT_TEST ))
            return ERR_STRUCT_EMPTY;

         rc = getTreeNodeInfo( &curBlock, m_tree.node[parentLevel].next.sbid, m_tree.node[parentLevel].next.entry, width,
                               (IdxTreeGroupType)bittestEntry.group, &allocCount, &realCount, entryMap );

         if( rc != NO_ERROR )
            return rc;


         matchBitTestEntry = bittestEntry;   // assign to the value of the first entry
         bSuccess = getTestbitValue( key, width, i, &testbitVal );

         setBittestEntry( &curEntry, testbitVal, bittestEntry.group, m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid, m_tree.node[parentLevel].next.entry );
         setTreeNode( &m_tree.node[i], i, allocCount, realCount, 0, bittestEntry, curEntry );

         matchBitTestEntry.bitTest = testbitVal;
         bFound = getTreeMatchEntry( &curBlock, m_tree.node[parentLevel].next.sbid, m_tree.node[parentLevel].next.entry,
                                     width, allocCount, entryMap, &matchPos, &matchBitTestEntry );

         if( !bFound ) {  // this testbit not exist at the current level
            bExitOuterLoop = true;    // tell to exit the outer loop
            bAddFlag = true;
            if( allocCount < realCount + 1 )  // we have enough space to take care of the extra one
            {  // we don't have space to take care of the extra one, have to reassign to a big block
               if( bittestEntry.group >= ENTRY_32 )             // it's impossible this condition holds true
                  return ERR_IDX_TREE_INVALID_GRP;

               m_tree.node[i].current.group++;
               rc = assignSegment( m_tree.node[i].current.group, &assignPtrEntry, i );
               if( rc != NO_ERROR )
                  return rc;

               m_tree.node[i].allocCount = 0x1 << m_tree.node[i].current.group;
               if( isDebug( DEBUG_3 )) {
                  printf( "\nEntry starting from %d:%d:%d (type %d ) will move to %d:%d:%d (type %d)", (int)m_tree.node[parentLevel].next.fbo, (int)m_tree.node[parentLevel].next.sbid, (int)m_tree.node[parentLevel].next.entry, (int)(m_tree.node[i].current.group-1),
                                                                                (int)assignPtrEntry.fbo, (int)assignPtrEntry.sbid, (int)assignPtrEntry.entry, (int)m_tree.node[i].current.group );
                  printf( "\nNew space capacity is %d", (int)m_tree.node[i].allocCount );
                  printf( "\nBefore the move" );
                  printSubBlock( m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid );
                  printSubBlock( assignPtrEntry.fbo, assignPtrEntry.sbid );
               }
               rc = moveEntry( m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid, m_tree.node[parentLevel].next.entry, width,
                               assignPtrEntry.fbo, assignPtrEntry.sbid, assignPtrEntry.entry, m_tree.node[i].current.group,
                               allocCount, entryMap, &moveCount, m_tree.node[i].allocCount );
               if( rc != NO_ERROR )
                  return rc;

               if(  isDebug( DEBUG_3 )) {
                  printf( "\nAfter the move" );
                  printSubBlock( m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid );
                  printSubBlock( assignPtrEntry.fbo, assignPtrEntry.sbid );
               }
               if( moveCount != realCount )
                  return ERR_IDX_TREE_MOVE_ENTRY;

               // set release entry
               setEmptyListEntry( &releasePtrEntry, bittestEntry.group, m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid, m_tree.node[parentLevel].next.entry );

               if( i == 1 ) {  // handle bitmap parent
                  bitmapEntry.fbo = m_tree.node[0].next.fbo = m_tree.node[i].current.fbo = assignPtrEntry.fbo;
                  bitmapEntry.sbid = m_tree.node[0].next.sbid = m_tree.node[i].current.sbid = assignPtrEntry.sbid;
                  bitmapEntry.entry = m_tree.node[0].next.entry = m_tree.node[i].current.entry = assignPtrEntry.entry;
                  setSubBlockEntry( m_rootBlock.data, IDX_BITMAP_SUBBLOCK_NO, rootTestbitVal, MAX_COLUMN_BOUNDARY, &bitmapEntry );
               }
               else { // handle parent for the rest of levels in the tree
                  rc = readSubBlockEntry( m_cbTree, &parentBlock, m_tree.node[parentLevel].current.fbo, m_tree.node[parentLevel].current.sbid,
                                          m_tree.node[parentLevel].current.entry, MAX_COLUMN_BOUNDARY, &parentBitTestEntry );
                  if( rc != NO_ERROR )
                     return rc;

                  parentBitTestEntry.fbo = m_tree.node[parentLevel].next.fbo = m_tree.node[i].current.fbo = assignPtrEntry.fbo;
                  parentBitTestEntry.sbid = m_tree.node[parentLevel].next.sbid = m_tree.node[i].current.sbid = assignPtrEntry.sbid;
                  parentBitTestEntry.entry = m_tree.node[parentLevel].next.entry = m_tree.node[i].current.entry = assignPtrEntry.entry;

                  rc = writeSubBlockEntry( m_cbTree, &parentBlock, m_tree.node[parentLevel].current.fbo, m_tree.node[parentLevel].current.sbid,
                                           m_tree.node[parentLevel].current.entry, MAX_COLUMN_BOUNDARY, &parentBitTestEntry );
                  if( rc != NO_ERROR )
                     return rc;
               }

               // here's the work to release the ptr
               rc = releaseSegment( bittestEntry.group, &releasePtrEntry );
               if( rc != NO_ERROR )
                  return rc;

            } // end of if( allocCount >=

            // take care of rest of empty part
            rc = readSubBlockEntry( m_cbTree, &curBlock, m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid,
                                     m_tree.node[parentLevel].next.entry, MAX_COLUMN_BOUNDARY, &bittestEntry );
            if( rc != NO_ERROR )
               return rc;

            m_tree.node[i].current.bitTest = testbitVal;
            matchBitTestEntry.group = m_tree.node[i].current.group;
            m_tree.node[i].allocCount = 0x1 << m_tree.node[i].current.group;

            bExitInnerLoop = false;
            for( j = 0; !bExitInnerLoop && j < m_tree.node[i].allocCount; j++ )
               if( !entryMap[j] ) {         // here's the empty spot

                  if( m_tree.maxLevel > 2 && i != loopCount -1 ) {
                     rc = buildEmptyTreePart( key, width, rid, i + 1, 0 );
                     if( rc != NO_ERROR )
                        return rc;
                     bDone = true;
                  }

                  // check out of bound
                  rc = readSubBlockEntry( m_cbTree, &curBlock, m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid,
                                        m_tree.node[parentLevel].next.entry + j, MAX_COLUMN_BOUNDARY, &bittestEntry );
                  if( rc != NO_ERROR )
                     return rc;

                  matchBitTestEntry.fbo = m_tree.node[i+1].current.fbo;
                  matchBitTestEntry.sbid = m_tree.node[i+1].current.sbid;
                  matchBitTestEntry.entry = m_tree.node[i+1].current.entry;

                  rc = writeSubBlockEntry( m_cbTree, &curBlock, m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid,
                                        m_tree.node[parentLevel].next.entry + j, MAX_COLUMN_BOUNDARY, &matchBitTestEntry );
                  if( rc != NO_ERROR )
                     return rc;

                  m_tree.node[i].useCount++;

                  m_tree.node[i].current.entry += j;
                  entryMap[j] = true;
                  curOffset = j;

                  bExitInnerLoop = true;
               } // end of if( !entryMap[j] )

         } // end of if( !bFound
         else {
            m_tree.node[i].current.entry += matchPos;
            m_tree.node[parentLevel].next.entry += matchPos;
         }

         m_tree.node[i].next = matchBitTestEntry;
         parentLevel++;
      } // end of for( i = 1;

      curLevel = m_tree.maxLevel-1;
      if( !bDone ) {
         rc = updateListFile( key, width, rid, curLevel, m_tree.node[curLevel].current.group, m_tree.node[curLevel].allocCount, m_tree.node[curLevel].useCount, curOffset, bAddFlag );
      }

      return rc;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Build a a part of empty tree branch
    * PARAMETERS:
    *    key - key value
    *    width - key width
    *    rid - row id
    *    startLevel - tree level
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    *    retBitTestEntry - return address pointer
    ***********************************************************/
   const int IndexTree::buildEmptyTreePart( const uint64_t key, const int width, const RID rid, const int startLevel, const int offset )
   {
      int                     rc, loopCount, testbitVal, i, parentLevel;
      bool                    bSuccess;
      IdxEmptyListEntry       assignPtrEntry, childPtrEntry;//, listEntry;
      IdxBitTestEntry         bittestEntry, curEntry;
      DataBlock               curBlock;

      if( startLevel <= 0 || m_tree.maxLevel < 2 )               // the start level must >= 1 and maxLevel >= 2
         return ERR_IDX_TREE_INVALID_LEVEL;

      loopCount = (m_tree.maxLevel - 1) > 1 ? m_tree.maxLevel - 1 : 0; //( width/5 ) - 1;

      rc = assignSegment( ENTRY_1, &assignPtrEntry, 0 );
      if( rc != NO_ERROR )
         return rc;

      if( isAddrPtrEmpty( &assignPtrEntry, EMPTY_LIST ) )
         return ERR_STRUCT_EMPTY;

      parentLevel = startLevel - 1;

      // assuming the parent take care of group, bit test value, and type
      m_tree.node[parentLevel].next.fbo = assignPtrEntry.fbo;
      m_tree.node[parentLevel].next.sbid = assignPtrEntry.sbid;
      m_tree.node[parentLevel].next.entry = assignPtrEntry.entry;

      // assign bit test for rest of levels
      for( i = startLevel; i < loopCount; i++ ) {
         // assign another one for child
         rc = assignSegment( ENTRY_1, &childPtrEntry, i );
         if( rc != NO_ERROR )
            return rc;

         rc = readSubBlockEntry( m_cbTree, &curBlock, m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid,
                                 m_tree.node[parentLevel].next.entry, /*width*/MAX_COLUMN_BOUNDARY, &bittestEntry );
         if( rc != NO_ERROR )
            return rc;

         bSuccess = getTestbitValue( key, width, i, &testbitVal );
         if( !bSuccess )
            return ERR_IDX_TREE_INVALID_LEVEL;

         setBittestEntry( &bittestEntry, testbitVal, ENTRY_1, childPtrEntry.fbo, childPtrEntry.sbid, childPtrEntry.entry );
         setSubBlockEntry( &curBlock, m_tree.node[parentLevel].next.sbid, m_tree.node[parentLevel].next.entry, MAX_COLUMN_BOUNDARY, &bittestEntry );

         writeDBFile( m_cbTree, &curBlock, m_tree.node[parentLevel].next.fbo );

         setBittestEntry( &curEntry, testbitVal, ENTRY_1, m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid, m_tree.node[parentLevel].next.entry );
         setTreeNode( &m_tree.node[i], i, 1, 1, 0, bittestEntry, curEntry );
         parentLevel++;
      }

      // assign the bit test for the last level
      // load the last piece
      rc = updateListFile( key, width, rid, i, ENTRY_1, 1, 1, offset, true );

      return rc;
   }


   /***********************************************************
    * DESCRIPTION:
    *    Close index files
    * PARAMETERS:
    *    none
    * RETURN:
    *    none
    ***********************************************************/
   void IndexTree::closeIndex()
   {
   
      if( m_rootBlock.dirty )
      {
         uint64_t lbid0 = 0;
#ifdef BROKEN_BY_MULTIPLE_FILES_PER_OID
         BRMWrapper::getInstance()->getBrmInfo( m_cbTree.file.oid, 0, lbid0 );
#endif
         writeDBFile( m_cbTree, m_rootBlock.data, lbid0); 
      }

      if( Cache::getUseCache() )
         flushCache();

      m_fileopTree.closeFile( m_cbTree.file.pFile );
      m_fileopList.closeFile( m_cbList.file.pFile );
      m_cbTree.file.pFile = NULL;
      m_cbList.file.pFile = NULL;
      clear();
   }

   /***********************************************************
    * DESCRIPTION:
    *    Create index related files
    * PARAMETERS:
    *    treeFid - the index tree file id
    *    listFid - the index list file id
    *    useFreeMgrFlag - internal use flag
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::createIndex( const FID treeFid, const FID listFid, const bool useFreeMgrFlag )
   {
#ifdef BROKEN_BY_MULTIPLE_FILES_PER_OID
      int allocSize;

      // init
 //     clear();

      RETURN_ON_ERROR( createFile( treeFid, DEFAULT_TOTAL_BLOCK/* * 10*/, allocSize ) );
      RETURN_ON_ERROR( createFile( listFid, DEFAULT_TOTAL_BLOCK/* * 10*/, allocSize ) );
#endif

      // load index files
//      RETURN_ON_ERROR( openIndex( treeFid, listFid ) );
//      rc = initIndex( treeFid, listFid );
//      closeIndex();

      return initIndex( treeFid, listFid );
   }

   /***********************************************************
    * DESCRIPTION:
    *    Delete a value from the tree
    * PARAMETERS:
    *    key - key value
    *    width - key width
    *    rid - row id
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::deleteIndex( const uint64_t key, const int width, const RID rid  )
   {
      IdxEmptyListEntry listHdrAddr;

      return processIndex( key, width, rid, listHdrAddr );
   }


   /***********************************************************
    * DESCRIPTION:
    *    Get test bit values
    * PARAMETERS:
    *    key - index key
    *    width - key width
    *    curTestNo - test bit iteration no
    * RETURN:
    *    True if success, otherwise if out of bound
    *    bittestVal - test bit value
    ***********************************************************/
   const bool IndexTree::getTestbitValue( const uint64_t key, const int width, const int curTestNo, int* bittestVal )
   {
      int   shiftPos, maskPos = 0;
      bool  bSuccess = true;
      
      if( !m_useMultiCol ) {
         *bittestVal = 0;
         shiftPos = width - ( curTestNo + 1 ) * 5;
         if( shiftPos > 0 )
            *bittestVal = getBitValue( key, shiftPos, BIT_MASK_ARRAY[5] );
         else {
            if( shiftPos >= -4 ) {
               maskPos = width - curTestNo  * 5 ;
               *bittestVal = key & BIT_MASK_ARRAY[maskPos];
            }
            else
               bSuccess = false;
         }
      }
      else 
         *bittestVal = m_multiColKey.testbitArray[curTestNo];

      return bSuccess;
   }


   /***********************************************************
    * DESCRIPTION:
    *    Get the matching entry within the current tree node
    * PARAMETERS:
    *    block - block data
    *    fbo, sbid, entry - pointer address
    *    width - key width
    *    allocCount - the total number of allocated entries
    *    entryMap - the entry availablibility map
    *    checkEntry - bittest value
    * RETURN:
    *    True if found, False otherwise
    *    checkEntry - if found the ptr got reset
    ***********************************************************/
   const bool IndexTree::getTreeMatchEntry( DataBlock* block, const uint64_t sbid, const uint64_t entry, const int width,
                                           const int allocCount, const bool* entryMap, int* matchEntry, IdxBitTestEntry* checkEntry )
   {
      IdxBitTestEntry   curEntry;
      bool              bFoundFlag = false;

      for( int i = 0; i < allocCount; i++ ) {
         getSubBlockEntry( block->data, sbid, entry + i, MAX_COLUMN_BOUNDARY, &curEntry );

         if( entryMap[i] && ( curEntry.type == checkEntry->type && curEntry.bitTest == checkEntry->bitTest ) ) {
            *checkEntry = curEntry;
            *matchEntry = i;
            bFoundFlag = true;
            break;
         }
      }

      return bFoundFlag;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Get tree node summary information
    * PARAMETERS:
    *    block - block data
    *    fbo, sbid, entry - pointer address
    *    width - key width
    *    group - entry group
    *    testbitVal - current bit test value
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    *    allocCount - the total number of allocated entries
    *    realCount - the total number of real entries
    *    entryMap - the entry availablibility map
    ***********************************************************/
   const int IndexTree::getTreeNodeInfo( DataBlock* block, const uint64_t sbid, const uint64_t entry, const int width,
                                          const IdxTreeGroupType group, int* allocCount, int* realCount, bool* entryMap )
   {
      IdxBitTestEntry   curEntry;
      int               rc = NO_ERROR;

      memset( entryMap, false, ENTRY_PER_SUBBLOCK );
      *realCount = 0;
      *allocCount = 0x1 << group;

      for( int i = 0; i < *allocCount; i++ ) {
         getSubBlockEntry( block->data, sbid, entry + i, MAX_COLUMN_BOUNDARY, &curEntry );
         if( curEntry.type == BIT_TEST || curEntry.type == LEAF_LIST ) {           // every guy here must have the same type
               entryMap[i] = true;
               *realCount = *realCount + 1;
         }
         else
         if( curEntry.type == EMPTY_ENTRY )
            entryMap[i] = false;
         else
            rc = ERR_IDX_TREE_INVALID_TYPE;

         if( rc != NO_ERROR )
            break;
      }

      return rc;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Check index entry address pointer is empty or not
    * PARAMETERS:
    *    pStruct - index entry pointer
    *    entryType - the entry type
    * RETURN:
    *    True if empty, False otherwise
    ***********************************************************/
   const bool IndexTree::isAddrPtrEmpty( void* pStruct, const IdxTreeEntryType entryType ) const
   {
      bool                    bStatus;
      IdxBitmapPointerEntry*  pBitmap;
      IdxBitTestEntry*        pBittest;
      IdxEmptyListEntry*      pEmptyList;


      switch( entryType ) {
         case BITMAP_PTR/*EMPTY_PTR*/ :  // this is the case for bitmap pointer address
                           pBitmap = (IdxBitmapPointerEntry*) pStruct;
                           bStatus = /*!pBitmap->oid &&*/ !pBitmap->fbo && !pBitmap->sbid && !pBitmap->entry;
                           break;
         case BIT_TEST :   // this is the case for bittest pointer address
                           pBittest = (IdxBitTestEntry*) pStruct;
                           bStatus = /*!pBittest->oid &&*/ !pBittest->fbo && !pBittest->sbid && !pBittest->entry;
                           break;
         case EMPTY_LIST :   // this is the case for bittest pointer address
                           pEmptyList = (IdxEmptyListEntry*) pStruct;
                           bStatus = !pEmptyList->fbo && !pEmptyList->sbid && !pEmptyList->entry;
                           break;
         default  :        bStatus = true;
      }

      return bStatus;
   }

   const int IndexTree::initIndex( const FID treeFid, const FID listFid )
   {
      int            rc=NO_ERROR;
      long           numOfBlock;
      unsigned char  writeBuf[BYTE_PER_BLOCK];

      bool oldUseVb = BRMWrapper::getUseVb();
      BRMWrapper::setUseVb( false );

      clear();
      RETURN_ON_ERROR( openIndex( treeFid, listFid ) );

      memset( writeBuf, 0, BYTE_PER_BLOCK );
      numOfBlock = getFileSize( m_cbTree.file.pFile )/BYTE_PER_BLOCK;
      for( int i = 0; i < numOfBlock; i++ )
         fwrite( writeBuf, sizeof( writeBuf ), 1, m_cbTree.file.pFile );

      numOfBlock = getFileSize( m_cbList.file.pFile )/BYTE_PER_BLOCK;
      for( int i = 0; i < numOfBlock; i++ )
         fwrite( writeBuf, sizeof( writeBuf ), 1, m_cbList.file.pFile );

      // very weird, have to close before we can call free mgr init
      closeIndex();
      RETURN_ON_ERROR( openIndex( treeFid, listFid ) );

      rc = m_freeMgr.init( m_cbTree, TREE );
      if( rc == NO_ERROR )
         rc = m_freeMgr.init( m_cbList, LIST );

      closeIndex();

      BRMWrapper::setUseVb( oldUseVb );

      if( isDebug( DEBUG_1 ) )
         printf( "\nEnd of the init for oid %d\n", m_cbTree.file.oid );

      return rc;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Move tree entries
    * PARAMETERS:
    *    fbo, sbid, entry - pointer address
    *    width - key width
    *    allocCount - the total number of allocated entries
    *    entryMap - the entry availablibility map
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::moveEntry( const uint64_t oldFbo, const uint64_t oldSbid, const uint64_t oldEntry, const int width, const uint64_t newFbo,
                                   const uint64_t newSbid, const uint64_t newEntry, const int newGroup, const int allocCount, bool* entryMap, int* moveCount, const int newAllocCount )
   {
      int               rc, i;
      DataBlock         oldBlock, newBlock;
      IdxBitTestEntry   curEntry, blankEntry;

      rc = readSubBlockEntry( m_cbTree, &oldBlock, oldFbo, oldSbid, oldEntry, /*width*/MAX_COLUMN_BOUNDARY, &curEntry );
      if( rc != NO_ERROR )
         return rc;

      rc = readSubBlockEntry( m_cbTree, &newBlock, newFbo, newSbid, newEntry, /*width*/MAX_COLUMN_BOUNDARY, &curEntry );
      if( rc != NO_ERROR )
         return rc;

      setBlankEntry( &blankEntry );
      for( i = 0; i < newAllocCount; i++ )
         setSubBlockEntry( newBlock.data, newSbid, newEntry + i, MAX_COLUMN_BOUNDARY, &blankEntry );

      *moveCount = 0;
      for( i = 0; i < allocCount; i++ ) {
         getSubBlockEntry( oldBlock.data, oldSbid, oldEntry + i, MAX_COLUMN_BOUNDARY, &curEntry );
         if( entryMap[i] ) {
            curEntry.group = newGroup;
            setSubBlockEntry( newBlock.data, newSbid, newEntry + *moveCount, MAX_COLUMN_BOUNDARY, &curEntry );
            *moveCount = *moveCount + 1;

         }
         setBlankEntry( &curEntry );
         if( newFbo != oldFbo )
            setSubBlockEntry( oldBlock.data, oldSbid, oldEntry + i, MAX_COLUMN_BOUNDARY, &curEntry );
         else
            setSubBlockEntry( newBlock.data, oldSbid, oldEntry + i, MAX_COLUMN_BOUNDARY, &curEntry );
      }
      rc = writeDBFile( m_cbTree, &newBlock, newFbo );
      if( rc != NO_ERROR )
         return rc;

      if( newFbo != oldFbo )
         rc = writeDBFile( m_cbTree, &oldBlock, oldFbo );

      // reset map
      memset( entryMap, false, ENTRY_PER_SUBBLOCK );
      for( i = 0; i < *moveCount; i++ )
         entryMap[i] = true;

      return rc;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Open index related files
    * PARAMETERS:
    *    treeFid - the index tree file id
    *    listFid - the index list file id
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::openIndex( const FID treeFid, const FID listFid  )
   {
      int rc =NO_ERROR;
      clear();

      // load index files
#ifdef BROKEN_BY_MULTIPLE_FILES_PER_OID
      m_cbTree.file.pFile = m_fileopTree.openFile( treeFid );
#endif
      if( m_cbTree.file.pFile == NULL )
         return ERR_FILE_OPEN;

#ifdef BROKEN_BY_MULTIPLE_FILES_PER_OID
      m_cbList.file.pFile = m_fileopList.openFile( listFid );
#endif
      if( m_cbList.file.pFile == NULL ) {
         m_fileopTree.closeFile( m_cbTree.file.pFile );       // close the one just open
         return ERR_FILE_OPEN;
      }
      m_cbTree.file.oid = treeFid;
      m_cbList.file.oid = listFid;

      uint64_t lbid0 = 0;
#ifdef BROKEN_BY_MULTIPLE_FILES_PER_OID
      rc = BRMWrapper::getInstance()->getBrmInfo( m_cbTree.file.oid, 0, lbid0 );
#endif
      if( rc != NO_ERROR ) {
         if( isDebug( DEBUG_1 ) ) {
            printf( "\nFor oid : %d block zero is %ld", m_cbTree.file.oid, (long)lbid0 );
            printf( "\nIn open index, have problem in get brmInfo, rc=%d", rc );
         }
         return rc;
      }
      rc =readDBFile( m_cbTree, m_rootBlock.data, lbid0 ); 

      return rc;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Process index, including search or delete
    * PARAMETERS:
    *    key - key value
    *    width - key width
    *    rid - row id
    *    listHdrAddr - list header address
    *    bDelete - delete operation flag
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::processIndex( const uint64_t key, const int width, const RID rid, IdxEmptyListEntry& listHdrAddr, const bool bDelete  )
   {
      int                     loopCount, testbitVal, i, allocCount, realCount, matchPos, curFbo, curSbid, curEntry;
      bool                    bSuccess;
      IdxEmptyListEntry       listEntry;
      IdxBitTestEntry         bittestEntry, matchBitTestEntry;
      DataBlock               curBlock;
      bool                    bFound, entryMap[ENTRY_PER_SUBBLOCK];

      int                     rootTestbitVal, rc = NO_ERROR;
      IdxBitmapPointerEntry   bitmapEntry;

      getTestbitValue( key, width, 0, &rootTestbitVal );
      getSubBlockEntry( m_rootBlock.data, 1, rootTestbitVal, MAX_COLUMN_BOUNDARY, &bitmapEntry );

      if( isAddrPtrEmpty( &bitmapEntry, /*EMPTY_PTR*/BITMAP_PTR ) )
         return bDelete ? ERR_IDX_LIST_INVALID_DELETE : NOT_FOUND ;

      // get the rootbittestEntry level bitmapPointerMap
      curFbo = bitmapEntry.fbo;
      curSbid = bitmapEntry.sbid;
      curEntry = bitmapEntry.entry;

      loopCount = width/5 + 1;
      for( i = 1; i < loopCount; i++ ) {
         // load the block
         rc = readSubBlockEntry( m_cbTree, &curBlock, curFbo, curSbid, curEntry, /*width*/MAX_COLUMN_BOUNDARY, &bittestEntry );
         if( rc != NO_ERROR )
            return rc;

         if( isDebug( DEBUG_3 ) ) {
 	         printf( "\nIn processIndex, level %d", i );
	         printSubBlock( curFbo, curSbid );
         }

         rc = getTreeNodeInfo( &curBlock, curSbid, curEntry, width, (IdxTreeGroupType)bittestEntry.group, &allocCount, &realCount, entryMap );
         if( rc != NO_ERROR )
            return rc;

         matchBitTestEntry = bittestEntry;   // assign to the value of the first entry
         bSuccess = getTestbitValue( key, width, i, &testbitVal );

         matchBitTestEntry.bitTest = testbitVal;
         bFound = getTreeMatchEntry( &curBlock, curSbid, curEntry, width, allocCount, entryMap, &matchPos, &matchBitTestEntry );
         if( bFound ) {  // this test bit exists at the current level
            if( i == loopCount - 1 ) {
//               if( loopCount == 2 )   // because of bitmap, it requires a special treatment
                   curEntry += matchPos;
               break;
            }
         }
         else
            return NOT_FOUND;

         getSubBlockEntry( &curBlock, curSbid, curEntry + matchPos, MAX_COLUMN_BOUNDARY, &matchBitTestEntry );

 	      curFbo = matchBitTestEntry.fbo;
	      curSbid = matchBitTestEntry.sbid;
	      curEntry = matchBitTestEntry.entry;

      } // end of for( i = 0;

      // here's the last level
      // load the last piece
      rc = readSubBlockEntry( m_cbTree, &curBlock, curFbo, curSbid, curEntry /*+ matchPos*/, MAX_COLUMN_BOUNDARY, &bittestEntry );
      if( rc != NO_ERROR )
         return rc;

      listEntry.fbo = bittestEntry.fbo;
      listEntry.sbid = bittestEntry.sbid;
      listEntry.entry = bittestEntry.entry;

      if( bDelete ) {  
         rc = m_listMgr.deleteIndexList( m_cbList, rid, key, &listEntry );
         if( rc != NO_ERROR )
            return rc;

         bSuccess = getTestbitValue( key, width, loopCount - 1, &testbitVal );
         setBittestEntry( &bittestEntry, testbitVal, bittestEntry.group, listEntry.fbo, listEntry.sbid, listEntry.entry, LEAF_LIST );
         setSubBlockEntry( &curBlock, curSbid, curEntry, MAX_COLUMN_BOUNDARY, &bittestEntry );

         rc = writeDBFile( m_cbTree, &curBlock, curFbo );

      }
      else 
         listHdrAddr = listEntry;

      return rc;
   }


   /***********************************************************
    * DESCRIPTION:
    *    Print a sub block
    * PARAMETERS:
    *    fbo - file block offset
    *    sbid - sub block id
    * RETURN:
    *    none
    ***********************************************************/
   void IndexTree::printSubBlock( const int fbo, const int sbid, const bool bNoZero )
   {
      DataBlock         curBlock;

      readDBFile( m_cbTree, &curBlock, fbo );
      printf( "\n lbid=%2d sbid=%2d", fbo, sbid );
      printMemSubBlock( &curBlock, sbid );
   }

   void IndexTree::printMemSubBlock( DataBlock* curBlock, const int sbid, const bool bNoZero )
   {
      int off;
      unsigned char*    curPos;
      IdxBitTestEntry   curEntry, testZero;

      setBlankEntry( &testZero );
      curPos = curBlock->data + BYTE_PER_SUBBLOCK * sbid;
      printf( "\n========================" );
      for( int i = 0; i < ENTRY_PER_SUBBLOCK; i++ ) {
         memcpy( &curEntry, curPos, MAX_COLUMN_BOUNDARY );
         off = memcmp( &testZero, &curEntry, MAX_COLUMN_BOUNDARY );
         printf( "\n Entry %2d : ", i );
         for( int j = 0; j < MAX_COLUMN_BOUNDARY; j++ )
            printf( " %2X", *(curPos + j) );

         printf( " fbo=%2d sbid=%2d entry=%2d group=%d bit=%2d type=%2d", (int)curEntry.fbo, (int)curEntry.sbid, (int)curEntry.entry, (int)curEntry.group, (int)curEntry.bitTest, (int)curEntry.type );

         curPos += MAX_COLUMN_BOUNDARY;
      }
      printf( "\n" );
   }

   /***********************************************************
    * DESCRIPTION:
    *    Assign segment from free manager
    * PARAMETERS:
    *    segmentType - group type
    *    assignPtr - the assigned ptr
    *    no - internal debug use flag
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::releaseSegment( int segmentType, IdxEmptyListEntry* myPtr )
   {
      int rc = NO_ERROR;

      m_rootBlock.dirty = true;
      if( m_useFreeMgr ) {
         if( isDebug( DEBUG_3 ) )
            printf( "\n----------Release the pointer, entry segment=%d fbo=%d sbid=%d entry=%d", segmentType, (int)myPtr->fbo, (int)myPtr->sbid, (int)myPtr->entry ); 
         rc = m_freeMgr.releaseSegment( m_cbTree, &m_rootBlock, TREE, (IdxTreeGroupType) segmentType, myPtr/*, TREE */);

         if( isDebug( DEBUG_3 ) )
            printf("\nReleased" );
      }

      return rc;
   }

   const int IndexTree::resetIndexFile( const FID treeFid, const FID listFid )
   {
/*      long numOfBlock;
      unsigned char  writeBuf[BYTE_PER_BLOCK];

      memset( writeBuf, 0, BYTE_PER_BLOCK );
      numOfBlock = getFileSize( m_cbTree.file.pFile )/BYTE_PER_BLOCK;
      for( int i = 0; i < numOfBlock; i++ )
         fwrite( writeBuf, sizeof( writeBuf ), 1, m_cbTree.file.pFile );

      numOfBlock = getFileSize( m_cbList.file.pFile )/BYTE_PER_BLOCK;
      for( int i = 0; i < numOfBlock; i++ )
         fwrite( writeBuf, sizeof( writeBuf ), 1, m_cbList.file.pFile );
*/
      return initIndex( treeFid, listFid );
   }

   /***********************************************************
    * DESCRIPTION:
    *    Calculate bit test array for multi column index
    * PARAMETERS:
    *    none
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::calculateBittestArray()
   {
      if( m_multiColKey.totalBit <= 0 )
         return ERR_INVALID_PARAM;

      m_multiColKey.maxLevel = m_multiColKey.totalBit/5 + 1;
      if( m_multiColKey.maxLevel > IDX_MAX_MULTI_COL_IDX_LEVEL ) 
         return ERR_VALUE_OUTOFRANGE;

      for( int i = 0; i < m_multiColKey.maxLevel - 1; i++ ) {
         m_multiColKey.curBitset = ( m_multiColKey.bitSet & m_multiColKey.curMask ) >> ( IDX_MAX_MULTI_COL_BIT - ( i + 1 ) * 5 );
         m_multiColKey.testbitArray[i] = m_multiColKey.curBitset.to_ulong();
         m_multiColKey.curMask = m_multiColKey.curMask >> 5;
      }

      m_multiColKey.curBitset = ( m_multiColKey.bitSet & m_multiColKey.curMask ) >> ( IDX_MAX_MULTI_COL_BIT - m_multiColKey.totalBit );
      m_multiColKey.testbitArray[m_multiColKey.maxLevel - 1] = m_multiColKey.curBitset.to_ulong();
      return NO_ERROR;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Setup bit test array for multi column index
    * PARAMETERS:
    *    keyArray - index key array
    *    totalByte - total byte in the key
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::setBitsetColumn( void* val, const int pos, const int width, const ColType colType )
   {
      int copyLen = width/8;

      switch( colType )
      {
         // No body is using float or double for indexing
/*         case WriteEngine::WR_FLOAT :  m_multiColKey.curBitset = *((float*) val); 
                                       break;
         case WriteEngine::WR_DOUBLE : m_multiColKey.curBitset = *((double*) val);
                                       break;
*/
         case WriteEngine::WR_CHAR :   m_multiColKey.curBitset.reset();
                                       for( int i = 0; i < width/8; i++ ) {
                                          uint64_t curChar = ((char*)val)[i];
                                          m_multiColKey.curBitset = m_multiColKey.curBitset << 8;
                                          m_multiColKey.curBitset |= curChar;
                                       } 
                                       memcpy( m_multiColKey.keyBuf + m_multiColKey.totalBit/8, (char*) val, copyLen );
                                       break;
         case WriteEngine::WR_SHORT :  m_multiColKey.curBitset = *((short*) val);
                                       memcpy( m_multiColKey.keyBuf + m_multiColKey.totalBit/8, (short*) val, copyLen );
                                       break;
         case WriteEngine::WR_BYTE :   m_multiColKey.curBitset = *((char *) val);
                                       memcpy( m_multiColKey.keyBuf + m_multiColKey.totalBit/8, (char*) val, copyLen );
                                       break;
         case WriteEngine::WR_LONGLONG:m_multiColKey.curBitset = *((long long*) val);
                                       memcpy( m_multiColKey.keyBuf + m_multiColKey.totalBit/8, (long long*) val, copyLen );
                                       break; 
         case WriteEngine::WR_INT :
         default  :                    memcpy( m_multiColKey.keyBuf + m_multiColKey.totalBit/8, (int*) val, copyLen );
                                       m_multiColKey.curBitset = *((int*) val);
                                       break;
      }

      m_multiColKey.totalBit += width;
      m_multiColKey.curBitset = m_multiColKey.curBitset << ( IDX_MAX_MULTI_COL_BIT - m_multiColKey.totalBit );
      m_multiColKey.bitSet |= m_multiColKey.curBitset;
      return NO_ERROR;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Set bit test entry
    * PARAMETERS:
    *    bittestEntry - the bit test entry
    *    testbitVal - current bit test value
    *    group - entry group
    *    fbo, sbid, entry - pointer address
    * RETURN:
    *    none
    ***********************************************************/
   void IndexTree::setBittestEntry( IdxBitTestEntry* bittestEntry, const uint64_t testbitVal, const uint64_t group, const uint64_t fbo, const uint64_t sbid, const uint64_t entry, const uint64_t entryType ) const
   {
      bittestEntry->type = entryType;
      bittestEntry->bitTest = testbitVal;
      bittestEntry->group = group;
      bittestEntry->bitCompare = BIT_5;
      bittestEntry->spare = 0;

      bittestEntry->fbo = fbo;
      bittestEntry->sbid = sbid;
      bittestEntry->entry = entry;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Set empty list entry for free manager
    * PARAMETERS:
    *    bittestEntry - the bit test entry
    *    testbitVal - current bit test value
    *    group - entry group
    *    fbo, sbid, entry - pointer address
    * RETURN:
    *    none
    ***********************************************************/
    // todo: need test case
   void IndexTree::setEmptyListEntry( IdxEmptyListEntry* myEntry, const uint64_t group, const uint64_t fbo, const uint64_t sbid, const uint64_t entry ) const
   {
      myEntry->type = EMPTY_PTR;
      myEntry->group = group;
      myEntry->spare = 0;
      myEntry->spare2 = 0;
      myEntry->fbo = fbo;
      myEntry->sbid = sbid;
      myEntry->entry = entry;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Set the tree header node
    * PARAMETERS:
    *    myTree - tree pointer
    *    key - index key
    *    width - key width
    *    rid - ROW ID
    *    testbitVal - bit test value at root level
    *    bitmapEntry - bitmap entry
    * RETURN:
    *    none
    ***********************************************************/
   void IndexTree::setTreeHeader( IdxTree* myTree, const uint64_t key, const RID rid, const int width,
                                  const int testbitVal, const IdxBitmapPointerEntry bitmapEntry )
   {
      IdxBitTestEntry nextEntry, curEntry;

      myTree->width = width;
      myTree->key = key;
      myTree->rid = rid;
      myTree->maxLevel = (width/5) + 1;

      setBlankEntry( &nextEntry );
      setBlankEntry( &curEntry );

//      myEntry.bitTest = testbitVal;
      nextEntry.fbo = bitmapEntry.fbo;
      nextEntry.sbid = bitmapEntry.sbid;
      nextEntry.entry = bitmapEntry.entry;
      nextEntry.type = BIT_TEST;

//      nextEntry.group = ENTRY_32;

      curEntry.fbo = 0;
      curEntry.sbid = IDX_BITMAP_SUBBLOCK_NO;
      curEntry.entry = testbitVal;
      curEntry.type = BITMAP_PTR;
      curEntry.group = ENTRY_32;
      curEntry.bitTest = testbitVal;


      // at this point useCount not known
      setTreeNode( &myTree->node[0], 0, ENTRY_PER_SUBBLOCK, 0, testbitVal, nextEntry, curEntry );
   }

   /***********************************************************
    * DESCRIPTION:
    *    Set the tree node
    * PARAMETERS:
    *    myNode - node pointer
    *    level - current tree level
    *    allocCount - allocated count
    *    useCount - used count
    *    offset - entry offset
    *    nextEntry - next entry
    *    curEntry - current entry
    * RETURN:
    *    none
    ***********************************************************/
   void IndexTree::setTreeNode( IdxTreeNode* myNode, const int level, const int allocCount, const int useCount,
                                 const int offset, const IdxBitTestEntry nextEntry, const IdxBitTestEntry curEntry )
   {
      myNode->level = level;
      myNode->allocCount = allocCount;
      myNode->useCount = useCount;
      myNode->offset = offset;
      myNode->next = nextEntry;
      myNode->current = curEntry;
      myNode->used = true;
   }


   const bool IndexTree::isTreeEmpty()
   {
      bool                    bEmpty = true;
      IdxBitmapPointerEntry   curBitmapPointer, emptyPointer;

      memset( &emptyPointer, 0, MAX_COLUMN_BOUNDARY );
      for( int i = 0; i < 32; i++ ) {
         getSubBlockEntry( m_rootBlock.data, IDX_BITMAP_SUBBLOCK_NO, i, MAX_COLUMN_BOUNDARY, &curBitmapPointer );
         if( memcmp( &curBitmapPointer, &emptyPointer, MAX_COLUMN_BOUNDARY ) ) {
            bEmpty = false;
            break;
         }
      }

//      printMemSubBlock( &m_rootBlock, IDX_BITMAP_SUBBLOCK_NO );
      return bEmpty;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Allocate Row ID
    * PARAMETERS:
    *    tableFid - the file id for table bitmap file
    *    totalRow - the total number of rows need to be allocated
    * RETURN:
    *    NO_ERROR if success
    *    rowIdArray - allocation of the row id left here
    ***********************************************************/
   const int IndexTree::updateIndex( const uint64_t key, const int width, const RID rid )
   {
      int                     rootTestbitVal, rc = NO_ERROR;
      IdxBitmapPointerEntry   curBitmapPointer;

      clearTree( &m_tree );

      getTestbitValue( key, width, 0, &rootTestbitVal );
      getSubBlockEntry( m_rootBlock.data, IDX_BITMAP_SUBBLOCK_NO, rootTestbitVal, MAX_COLUMN_BOUNDARY, &curBitmapPointer );

      setTreeHeader( &m_tree, key, rid, width, rootTestbitVal, curBitmapPointer );

      if( m_tree.maxLevel > IDX_MAX_TREE_LEVEL )
         return ERR_VALUE_OUTOFRANGE;

      if( isAddrPtrEmpty( &curBitmapPointer, (IdxTreeEntryType) curBitmapPointer.type ) )
         rc = buildEmptyIndexTreeBranch( key, width, rid, rootTestbitVal );
      else          // at least we have the root level pointer
         rc = buildExistIndexTreeBranch( key, width, rid, rootTestbitVal, curBitmapPointer );

      return rc;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Dummy list addr generator
    * PARAMETERS:
    *    key - index key
    *    width - key width
    *    rid - ROW ID
    *    myEntry - return entry with addr
    *    no - sequence no
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
    const int IndexTree::updateIndexList( const uint64_t key, const int width, const RID rid, IdxEmptyListEntry* myEntry, const int no, const bool addFlag )
   {
      int rc = NO_ERROR;

      if( m_useListMgr )
      {
         // doing something here
         if( !addFlag ) { // update list
            if( m_useMultiRid )
               rc = m_listMgr.updateIndexList( m_cbList, m_multiRid, key, myEntry );
            else
               rc = m_listMgr.updateIndexList( m_cbList, rid, key, myEntry );
         }
         else { // add list
            if( m_useMultiRid )
               rc = m_listMgr.addIndexListHdr( m_cbList, m_multiRid, key, myEntry );
            else
               rc = m_listMgr.addIndexListHdr( m_cbList, rid, key, myEntry );
         }

      }
      else {
         myEntry->fbo = 3;
         myEntry->sbid = 3;
         myEntry->entry = 3 + no;
      }

      return rc;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Update the entry in index list file
    * PARAMETERS:
    *    key - index key
    *    width - key width
    *    rid - ROW ID
    *    curLevel - current tree level
    *    group - current group
    *    allocCount - allocated count
    *    useCount - used count
    *    offset - entry offset
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    ***********************************************************/
   const int IndexTree::updateListFile( const uint64_t key, const int width, const RID rid, const int curLevel, const uint64_t group, const int allocCount, const int useCount, const int offset, const bool addFlag )
   {
      int                  rc, testbitVal, parentLevel = curLevel - 1;
      IdxEmptyListEntry    listEntry;
      IdxBitTestEntry      bittestEntry, curEntry;
      bool                 bSuccess;
      DataBlock            curBlock;

      rc = readSubBlockEntry( m_cbTree, &curBlock, m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid,
                              m_tree.node[parentLevel].next.entry + offset, MAX_COLUMN_BOUNDARY, &bittestEntry );
      if( rc != NO_ERROR )
         return rc;

      if( !addFlag ) {
         listEntry.fbo = bittestEntry.fbo;
         listEntry.sbid = bittestEntry.sbid;
         listEntry.entry = bittestEntry.entry;
      }

      rc = updateIndexList( key, width, rid, &listEntry, curLevel, addFlag );
      if( rc != NO_ERROR )
         return rc;

      bSuccess = getTestbitValue( key, width, curLevel, &testbitVal );
      setBittestEntry( &bittestEntry, testbitVal, group, listEntry.fbo, listEntry.sbid, listEntry.entry, LEAF_LIST );

      rc = writeSubBlockEntry( m_cbTree, &curBlock, m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid, m_tree.node[parentLevel].next.entry + offset, MAX_COLUMN_BOUNDARY, &bittestEntry );

      setBittestEntry( &curEntry, testbitVal, group, m_tree.node[parentLevel].next.fbo, m_tree.node[parentLevel].next.sbid, m_tree.node[parentLevel].next.entry, LEAF_LIST );
      setTreeNode( &m_tree.node[curLevel], curLevel,allocCount, useCount, offset, bittestEntry, curEntry );

      return rc;
   }

} //end of namespace

