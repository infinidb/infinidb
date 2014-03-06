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
* $Id: we_index.h 4450 2013-01-21 14:13:24Z rdempsey $
*
******************************************************************************************/
/** @file */

#ifndef _WE_INDEX_H_
#define _WE_INDEX_H_

#include <bitset>

#include "we_type.h"



/** Namespace WriteEngine */
namespace WriteEngine
{

   /*****************************************************
   * index definition
   ******************************************************/
   const int   IDX_BITTEST_SIZE         = 10;               /** @brief The bit size of bit test */
   const int   IDX_GROUP_SIZE           = 3;                /** @brief The bit size of group */
   const int   IDX_INSTRU_SIZE          = 4;                /** @brief The bit size of instruction */
   const int   IDX_PTR_SIZE             = 46;               /** @brief The bit size of address pointer */
   const int   IDX_TYPE_SIZE            = 3;                /** @brief The bit size of type */

   const int   IDX_BITMAP_SUBBLOCK_NO   = 1;                /** @brief Subblock 1 of root block is for bitmap pointer*/
   const int   IDX_MAX_TREE_LEVEL       = 128;              /** @brief The maximum depth of a tree */
   const int   IDX_MAX_MULTI_COL_BIT    = 256;              /** @brief The maximum bits of a multi-column tree (256 bit)*/
   const int   IDX_MAX_MULTI_COL_IDX_LEVEL = 52;            /** @brief The maximum depth of a multi-column tree */
   const int   IDX_MAX_MULTI_COL_IDX_NUM = 64;              /** @brief The maximum number of columns for a multi-column index */
   const int   MAX_IDX_RID              = 1024;             /** @brief Maximum index rids for one shot */
   const int   IDX_DEFAULT_READ_ROW     = 10000;            /** @brief Default number of rows for one read for index */

   // todo: need to move a higher level share file for dictionary
   const int   RID_SIZE                = 46;
//   const int   OID_SIZE                = 24;                /** @brief The bit size of object id */
   const int   FBO_SIZE                = 36;                /** @brief The bit size of file block offset */
   const int   SBID_SIZE               = 5;                 /** @brief The bit size of sub block id */
   const int   ENTRY_SIZE              = 5;                 /** @brief The bit size of entry location with a sub block  */
   
   const int   LIST_SIZE_TYPE          = 0;
   const int   LIST_RID_TYPE           = 3;
   const int   LIST_NOT_USED_TYPE      = 7;
   const int   LIST_HDR_SIZE           = 32;
   const int   LIST_SUBBLOCK_TYPE      = 4 ;
   const int   LIST_BLOCK_TYPE         = 5 ;
   const int   LIST_LLP_TYPE           = 6 ;
   const int   SUBBLOCK_TOTAL_BYTES    = 256;
   const int   LIST_SUB_LLP_POS        = 31;
   const int   LIST_LAST_LBID_POS      = 30;
   const int   LIST_BLOCK_LLP_POS      = 1023;
   const int   MAX_BLOCK_ENTRY         = 1024;
   const int   MAX_SUB_RID_CNT         = 30;
   const int   MAX_BLK_RID_CNT         = 1023;
   const int   MAX_BLK_NARRAY_RID_CNT  = 1018;
   const int   LBID_SBID_ENTRY         = 46;
   const int   RID_COUNT_SIZE          = 10;
   const int   CUR_BLK_POS_WIDTH       =  2;
   const int   LLP_STATUS_WIDTH        =  2;
   const int   LIST_ENTRY_WIDTH        =  8;
   const int   LIST_BLK_LLP_ENTRY_WIDTH=  48;
   const int   BEGIN_LIST_BLK_LLP_POS  = 1018;
   const int   NEXT_BLK_PTR_OFFSET     = 5;
   const int   PARENT_PTR_OFFSET       = 4;
   const int   TOTAL_NUM_ARRAY_PTR     = 4;
   const int   ARRAY_LLP_EXIST         = 1;
   const int   LLP_NOT_FULL            = 0;
   const int   LLP_FULL                = 1;
   const int   TOTAL_CUR_LEVEL         = 10;
   const int   CUR_LEVEL_POS_WIDTH     = 20;
   const uint64_t INVALID_KEY             = -1LL;                /** @brief Invalid number */

   /*****************************************************
   * mask definition
   ******************************************************/
   const int   BIT_MASK_ARRAY[]        = {   0x0,
                                             0x01,          /** @brief 1 bit mask */
                                             0x03,          /** @brief 2 bit mask */
                                             0x07,          /** @brief 3 bit mask */
                                             0x0F,          /** @brief 4 bit mask */
                                             0x1F,          /** @brief 5 bit mask */
                                             0x3F           /** @brief 6 bit mask */
                                          };

   /************************************************************************
    * Type enumerations
    ************************************************************************/
   enum IdxTreeEntryType {                   /** @brief Index tree entry types */
      EMPTY_ENTRY             = 0,           /** @brief Empty entry */
      UNIQUE_VAL              = 7,           /** @brief Unique value */
      EMPTY_LIST              = 1,           /** @brief Empty list pointer entry */
      EMPTY_PTR               = 2,           /** @brief Empty pointer entry */
      BIT_TEST                = 3,           /** @brief Bit test entry */
      LEAF_LIST               = 4,           /** @brief Leaf list pointer */
      BITMAP_PTR              = 5,           /** @brief Bitmap pointer */
//      SORT_LIST               = 5,           /** @brief Sorted list pointer */
      MULTI_COL               = 6            /** @brief Multi-column index pointer */
   };

   enum IdxTreeGroupType {                   /** @brief Index tree group types */
      ENTRY_1                 = 0,           /** @brief 1 entry per group */
      ENTRY_2                 = 1,           /** @brief 2 entry per group */
      ENTRY_4                 = 2,           /** @brief 4 entry per group */
      ENTRY_8                 = 3,           /** @brief 8 entry per group */
      ENTRY_16                = 4,           /** @brief 16 entry per group */
      ENTRY_32                = 5,           /** @brief 32 entry per group */
      ENTRY_BLK               = 6           /** @brief 1k entry per group */
   };

   enum IdxBitCompareType {                  /** @brief Index bit compare types */
      BIT_5                   = 0,           /** @brief 5-bit compare */
      BIT_10                  = 1            /** @brief 10-bit compare */
   };

   enum IdxFreeMgrType {                     /** @brief Index free manager types */
      TREE                    = 0,           /** @brief Index tree type */
      LIST                    = 1            /** @brief Index list type */
   };

   /************************************************************************
    * @brief index defintions
    ************************************************************************/
   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;  /** @brief entry type */
      uint64_t       spare       :  12;             /** @brief spare bits */
      uint64_t       group       :  IDX_GROUP_SIZE; /** @brief entry group type */
      // The following is related to ptr
      uint64_t       fbo         :  FBO_SIZE;       /** @brief file block offset */
      uint64_t       sbid        :  SBID_SIZE;      /** @brief sub block id */
      uint64_t       entry       :  ENTRY_SIZE;     /** @brief entry within sub block */
   } IdxStartSubBlockEntry;                        /** @brief Index start block entry structure */

   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;  /** @brief entry type */
      uint64_t       spare       :  2;             /** @brief spare bits */
      uint64_t       group       :  IDX_GROUP_SIZE; /** @brief entry group type */
      // The following is related to ptr
      uint64_t       spare2      :  10;             /** @brief spare bits */
      uint64_t       fbo         :  FBO_SIZE;       /** @brief file block offset */
      uint64_t       sbid        :  SBID_SIZE;      /** @brief sub block id */
      uint64_t       entry       :  ENTRY_SIZE;     /** @brief entry within sub block */
   } IdxEmptyListEntry;                            /** @brief Index empty list entry structure */

   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;  /** @brief entry type */
      uint64_t       spare       :  15;             /** @brief spare bits */
      // The following is related to ptr
      uint64_t       fbo         :  FBO_SIZE;       /** @brief file block offset */
      uint64_t       sbid        :  SBID_SIZE;      /** @brief sub block id */
      uint64_t       entry       :  ENTRY_SIZE;     /** @brief entry within sub block */
   } IdxBitmapPointerEntry;                        /** @brief Index bitmap pointer entry structure */

   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;  /** @brief entry type */
      uint64_t       bitTest     :  IDX_BITTEST_SIZE; /** @brief index bittest  */
      uint64_t       group       :  IDX_GROUP_SIZE; /** @brief entry group type */
      uint64_t       bitCompare  :  1;
      uint64_t       spare       :  1;              /** @brief spare bits */
      // The following is related to ptr
      uint64_t       fbo         :  FBO_SIZE;       /** @brief file block offset */
      uint64_t       sbid        :  SBID_SIZE;      /** @brief sub block id */
      uint64_t       entry       :  ENTRY_SIZE;     /** @brief entry within sub block */
   } IdxBitTestEntry;                              /** @brief Index bit test entry structure */

   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;  /** @brief entry type */
      uint64_t       spare       :  15;             /** @brief spare bits */
      // The following is related to ptr
      uint64_t       fbo         :  FBO_SIZE;       /** @brief file block offset */
      uint64_t       sbid        :  SBID_SIZE;      /** @brief sub block id */
      uint64_t       entry       :  ENTRY_SIZE;     /** @brief entry within sub block */
   } IdxTreePointerEntry;                          /** @brief Index tree pointer entry structure */
   /************************************************************************
    * @brief index list node defintions
    ************************************************************************/
   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;  /** @brief entry type 3 */
      uint64_t       spare       :  15;             /** @brief spare bits */
      RID            rid         :  RID_SIZE;       /** @brief row id */
   } IdxRidListEntry;                              /** @brief Index rid list entry structure */
                                 
   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;    /** @brief entry type */
      uint64_t       spare       :  5;
      uint64_t       count       :  RID_COUNT_SIZE;   /** the count of rids on the current blk */
      uint64_t       llp         :  LBID_SBID_ENTRY;  /** @brief size */
   } IdxRidListPtr;

   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;    /** @brief entry type */
      uint64_t       spare       :  5;
      uint64_t       count       :  RID_COUNT_SIZE;   /** the count of rids on the current blk */ 
      uint64_t       lbid        :  FBO_SIZE;         /** @brief size */
      uint64_t       sbid        :  SBID_SIZE;        /** @brief sub block id */
      uint64_t       entry       :  ENTRY_SIZE;       /** @brief entry within sub block */
   } IdxRidLastListPtr;

   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;    /** @brief entry type */
      uint64_t       spare       :  13;
      uint64_t       llpStat     :  LLP_STATUS_WIDTH; /** llp status */
      uint64_t       childLbid   :  FBO_SIZE;         /** @brief file block offset */
      uint64_t       spare2      :  10;
   } IdxRidChildListPtr;  

   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;    /** @brief entry type 0 or 6 */
      uint64_t       spare       :  5;
      uint64_t       count       :  RID_COUNT_SIZE;   /** the count of rids on the current blk */
      uint64_t       nextLbid    :  FBO_SIZE;       /** @brief file block offset */
      uint64_t       curLevel    :  TOTAL_CUR_LEVEL;     
    } IdxRidNextListPtr; 

   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;    /** @brief entry type 6*/
      uint64_t       spare       :  3;                /** @brief spare bits */
      uint64_t       curLevelPos :  CUR_LEVEL_POS_WIDTH;
      uint64_t       curBlkPos   :  CUR_BLK_POS_WIDTH;      /** the position of current blk */
      uint64_t       parentLbid  :  FBO_SIZE;       /** @brief file block offset */
   } IdxRidParentListPtr; 

   typedef struct {
      IdxRidChildListPtr      childIdxRidListPtr[4];
      IdxRidParentListPtr     parentIdxListPtr;
      IdxRidNextListPtr       nextIdxListPtr;
   } IdxRidListArrayPtr;
   
   /************************************************************************
    * @brief index list header defintions
    ************************************************************************/
   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;  /** @brief entry type */
      uint64_t       spare       :  15;             /** @brief spare bits */
      uint64_t       size        :  RID_SIZE;       /** @brief size */
   } IdxRidListHdrSize;

   typedef struct {
      uint64_t       type        :  IDX_TYPE_SIZE;  /** @brief entry type */
      uint64_t       spare       :  15;             /** @brief spare bits */
      uint64_t       llp         :  RID_SIZE;       /** @brief size */
   } IdxRidListHdrPtr;

   typedef struct {
      IdxRidListHdrSize idxRidListSize;
      uint64_t          key;
      IdxRidListEntry   firstIdxRidListEntry;
      IdxRidListHdrPtr  nextIdxRidListPtr;
   } IdxRidListHdr;

    typedef struct {
      uint64_t       part1        :  15;              /** @brief entry type */
      uint64_t       part2        :  15;             /** @brief spare bits */
      uint64_t       spare        :  34;             /** @brief size */
   } IdxRidListOffSet;
   /************************************************************************
    * @brief index tree node defintions
    ************************************************************************/
   typedef struct {
      IdxBitTestEntry   next;                      /** @brief next in the node */
      IdxBitTestEntry   current;                   /** @brief current addr */
      uint16_t          level;                     /** @brief tree level */
      uint16_t          allocCount;                /** @brief allocated entry cound from free mgr */
      uint16_t          useCount;                  /** @brief actual use entry count */
      uint16_t          offset;                    /** @brief entry offset */
      bool              used;                      /** @brief used flag */
   } IdxTreeNode;                                  /** @brief Index tree node */

   typedef struct {
      IdxTreeNode       node[IDX_MAX_TREE_LEVEL];  /** @brief node array */
      uint16_t          maxLevel;                  /** @brief max level */
      RID               rid;                       /** @brief current row id */
      uint64_t          key;                       /** @brief current key */
      uint16_t          width;                     /** @brief current width */
   } IdxTree;                                      /** @brief Index tree */

   struct IdxTreeCacheNode {
      RID               rid;                       /** @brief RID */
      uint64_t          key;                       /** @brief Key */
      IdxEmptyListEntry entry;                     /** @brief List pointer */
      bool              used;                      /** @brief Used flag */
      IdxTreeCacheNode() { used = false; }
   };

   struct IdxMultiColKey {
      std::bitset<IDX_MAX_MULTI_COL_BIT> bitSet;   /** @brief BitArray for all bits */
      std::bitset<IDX_MAX_MULTI_COL_BIT> curBitset;/** @brief Current working column */
      std::bitset<IDX_MAX_MULTI_COL_BIT> curMask;  /** @brief Current bitset mask */
      unsigned char     keyBuf[IDX_MAX_MULTI_COL_BIT/8];  /** @brief Key buffer */
      int               curLevel;                  /** @brief Current index level */
      int               maxLevel;                  /** @brief Maximum index level */
      int               totalBit;                  /** @brief Total bits */
      int               testbitArray[IDX_MAX_MULTI_COL_IDX_LEVEL]; /** @brief Test bit array */
      void              clear() { bitSet.reset(); curBitset.reset(); curMask.reset();
                                  curLevel = maxLevel = 0; totalBit = 0; 
                                  memset( testbitArray, 0, IDX_MAX_MULTI_COL_IDX_LEVEL); memset( keyBuf, 0, IDX_MAX_MULTI_COL_BIT/8 );
                                  curMask = 0x1F; curMask = curMask << (IDX_MAX_MULTI_COL_BIT - 5); 
                        }
      IdxMultiColKey()  { clear(); }
   };
   struct IdxMultiRid {
      RID*              ridArray;                  /** @brief RID array */
      int               totalRid;                  /** @brief Total number of row id */
      IdxMultiRid()     { totalRid = 0; ridArray = NULL; }
      void setMultiRid( RID* rids, const int size ) {
                        totalRid = size;
                        ridArray = rids;
/*                        ridArray = new RID[size];
                        memcpy( ridArray, rids, size * sizeof( RID ) ); */
      }
      void clearMultiRid()   { /*if( ridArray != NULL ) delete [] ridArray; ridArray = NULL;*/ }  // we don't want to get into this mem business
   };

   struct IdxLoadParam {
      File              sourceFile;                /** @brief Source file contatin values */

      OID               indexTreeOid;              /** @brief Target index tree oid */
      OID               indexListOid;              /** @brief Target index list oid */
      execplan::CalpontSystemCatalog::ColDataType indexColDataType;          /** @brief Target index column type */
      int               indexWidth;                /** @brief Target index width */

      int               maxLoadRow;                /** @brief Max rows for one load */

      void  setIdxLoadParam( const OID treeOid, const OID listOid, const execplan::CalpontSystemCatalog::ColDataType colDataType, const int width, const int maxRow ) 
                        { indexTreeOid = treeOid; indexListOid = listOid; indexColDataType = colDataType;
                          indexWidth = width; maxLoadRow = maxRow; }
      bool  isValid()   { return indexTreeOid && indexListOid && indexWidth && maxLoadRow; }
      IdxLoadParam()    { indexTreeOid = indexListOid = indexWidth = maxLoadRow = 0; }
   };

} //end of namespace
#endif // _WE_INDEX_H_
