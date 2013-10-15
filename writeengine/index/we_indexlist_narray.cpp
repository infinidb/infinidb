/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/* ========================================================================== */
/*                                                                            */
/*   we_indexlist_narray.cpp                                                       */
/*                                                                            */
/*                                                                            */
/*   Description                                                              */
/*                                                                            */
/* ========================================================================== */
#include <stdio.h>
//#include <pthread.h>
#include <string.h>
#ifndef _MSC_VER
#include <inttypes.h>
#endif
#include <math.h>
#include "we_indextree.h"
#include "we_indexlist.h"
using namespace std;

namespace WriteEngine
{

   /****************************************************************
    * DESCRIPTION:
    * Private Function for print the block trees
    * 
    ***************************************************************/ 
    const int  IndexList::printBlocks(uint64_t& lbid)
    {
        int rc;
        IdxRidListArrayPtr idxRidListArrayPtr;
        int curLevel = 0, curCount=0;
        
        memset(&idxRidListArrayPtr, 0, sizeof(idxRidListArrayPtr));
        m_lbid = lbid;
        rc = readCurBlk();
        getSubBlockEntry(m_curBlock.data, 0, 
                         BEGIN_LIST_BLK_LLP_POS, 
                         LIST_BLK_LLP_ENTRY_WIDTH, 
                         &idxRidListArrayPtr);
        curLevel     = idxRidListArrayPtr.nextIdxListPtr.curLevel;
        curCount     = idxRidListArrayPtr.nextIdxListPtr.count;
        for (int i=0; i< curLevel ; i++)
        {
          cout << "        ";
        }
        cout << "   Lbid->" << lbid << " curLevel->" << curLevel 
                                  << " curCount->" << curCount << endl;
        for (int i=0; i<TOTAL_NUM_ARRAY_PTR; i++)
        {
          uint64_t lbid;
          lbid = idxRidListArrayPtr.childIdxRidListPtr[i].childLbid;
          if (lbid != (uint64_t)INVALID_LBID)
              printBlocks(lbid);
        }
        return rc;
     }

    /****************************************************************
     * DESCRIPTION:
     * Private Function for getting the last Fbo on header
     * 
     ***************************************************************/ 
     const int  IndexList::setCurBlkNextPtr(uint64_t& nextLbid, int count)
     {
        int rc ;
        CommBlock cb;
        
        cb.file.oid = m_oid;     
        cb.file.pFile = m_pFile; 
        if (nextLbid== (uint64_t)INVALID_LBID)
         return ERR_IDX_LIST_SET_NEXT_LBID;
         
        m_idxRidListArrayPtr.nextIdxListPtr.type       = (int)LIST_BLOCK_TYPE;
        m_idxRidListArrayPtr.nextIdxListPtr.curLevel   = m_curLevel;
        m_idxRidListArrayPtr.nextIdxListPtr.count      = count;
        m_idxRidListArrayPtr.nextIdxListPtr.nextLbid   = nextLbid; 
        m_idxRidListArrayPtr.nextIdxListPtr.spare      = 0;

        if ((m_curBlock.lbid == m_lbid) && (m_curBlock.state>= BLK_READ))
        {
          rc = writeSubBlockEntry( cb, &m_curBlock,  m_lbid, 0, 
                                   BEGIN_LIST_BLK_LLP_POS+ NEXT_BLK_PTR_OFFSET, 
                                   LIST_ENTRY_WIDTH, 
                                   &m_idxRidListArrayPtr.nextIdxListPtr );
          m_curBlock.state = BLK_READ;
          if (rc!= NO_ERROR)
             return rc;
        }          
        else
          return ERR_IDX_LIST_SET_NEXT_LBID;
        
        return NO_ERROR;
     } 

    /****************************************************************
     * DESCRIPTION:
     * Private Function for setting the last Fbo on header
     * 
     ***************************************************************/    
     const int  IndexList::initCurBlock()
     {

      if ((m_curBlock.lbid == m_lbid) && (m_curBlock.state>= BLK_READ))
      {
        memset(&m_idxRidListArrayPtr, 0, sizeof(m_idxRidListArrayPtr));
        for (int i=0; i<TOTAL_NUM_ARRAY_PTR; i++)
        {
          m_idxRidListArrayPtr.childIdxRidListPtr[i].type = LIST_NOT_USED_TYPE;
          m_idxRidListArrayPtr.childIdxRidListPtr[i].llpStat = LLP_NOT_FULL;
          m_idxRidListArrayPtr.childIdxRidListPtr[i].spare =0;
          m_idxRidListArrayPtr.childIdxRidListPtr[i].childLbid= INVALID_LBID;
        }
        m_idxRidListArrayPtr.nextIdxListPtr.type       = LIST_SIZE_TYPE;
        m_idxRidListArrayPtr.nextIdxListPtr.spare      = 0;
        m_idxRidListArrayPtr.nextIdxListPtr.curLevel   = m_curLevel;
        m_idxRidListArrayPtr.nextIdxListPtr.count   = 0;
        m_idxRidListArrayPtr.nextIdxListPtr.nextLbid   = INVALID_LBID;
          
            
        m_idxRidListArrayPtr.parentIdxListPtr.type = LIST_LLP_TYPE;
        m_idxRidListArrayPtr.parentIdxListPtr.spare = 0;
        m_idxRidListArrayPtr.parentIdxListPtr.curLevelPos = m_curLevelPos; 
        m_idxRidListArrayPtr.parentIdxListPtr.curBlkPos = m_curBlkPos;        
        m_idxRidListArrayPtr.parentIdxListPtr.parentLbid = m_parentLbid;

        setSubBlockEntry(m_curBlock.data, 0, BEGIN_LIST_BLK_LLP_POS, 
                         LIST_BLK_LLP_ENTRY_WIDTH, &m_idxRidListArrayPtr);
      }
      else
        return ERR_IDX_LIST_INIT_LINK_BLKS;
      return NO_ERROR;
     }
    /****************************************************************
     * DESCRIPTION:
     * Private Function for setting the last Fbo on header
     * 
     ***************************************************************/    
     const int  IndexList::getNextInfoFromArray(IdxRidNextListPtr& nextIdxListPtr)
     {
       if ((m_curBlock.lbid == m_lbid) && (m_curBlock.state>= BLK_READ))
        getSubBlockEntry(m_curBlock.data, 0, 
                         BEGIN_LIST_BLK_LLP_POS+ NEXT_BLK_PTR_OFFSET, 
                         LIST_ENTRY_WIDTH, &nextIdxListPtr);
       else
        return ERR_IDX_LIST_GET_NEXT;
       return NO_ERROR;
     }
    /****************************************************************
     * DESCRIPTION:
     * Private Function for setting the last Fbo on header
     * 
     ***************************************************************/    
     const int  IndexList::getParentInfoFromArray(IdxRidParentListPtr& parentIdxListPtr)
     {
        int rc;
        CommBlock cb;
        
        cb.file.oid = m_oid;     
        cb.file.pFile = m_pFile;    
        if ((m_curBlock.lbid == m_lbid) && (m_curBlock.state>= BLK_READ))
           getSubBlockEntry(m_curBlock.data, 0, 
                            BEGIN_LIST_BLK_LLP_POS+ PARENT_PTR_OFFSET, 
                            LIST_ENTRY_WIDTH, &parentIdxListPtr);
        else
           return ERR_IDX_LIST_GET_PARENT;
        m_curLevelPos   = parentIdxListPtr.curLevelPos;
        m_curBlkPos     = parentIdxListPtr.curBlkPos;
        m_parentLbid    = parentIdxListPtr.parentLbid;
        if (m_parentLbid == (uint64_t)INVALID_LBID)
         return NO_ERROR;
        memset( m_parentBlock.data, 0, sizeof(m_parentBlock.data));
        rc = readDBFile(cb, &m_parentBlock, m_parentLbid );
        m_parentBlock.lbid  = m_parentLbid;
        m_parentBlock.state = BLK_READ;
        m_parentBlock.dirty = true;
      return NO_ERROR;
     }
    /****************************************************************
     * DESCRIPTION:
     * Private Function for setting the last Fbo on header
     * 
     ***************************************************************/    
     const int  IndexList::updateCurCountInArray(int insCnt)
     {
       int rc = NO_ERROR;
       IdxRidNextListPtr nextIdxListPtr;
       
       memset(&nextIdxListPtr, 0, sizeof(IdxRidNextListPtr));    
       if ((m_curBlock.lbid == m_lbid) && (m_curBlock.state>= BLK_READ))
       {
         getSubBlockEntry(m_curBlock.data, 0, BEGIN_LIST_BLK_LLP_POS+ NEXT_BLK_PTR_OFFSET, 
                          LIST_ENTRY_WIDTH, &nextIdxListPtr);

         nextIdxListPtr.count = nextIdxListPtr.count + insCnt;
         setSubBlockEntry(m_curBlock.data, 0, BEGIN_LIST_BLK_LLP_POS+ NEXT_BLK_PTR_OFFSET, 
                         LIST_ENTRY_WIDTH, &nextIdxListPtr);
         m_curBlock.state = BLK_WRITE;
       }
       else
         return ERR_IDX_LIST_GET_COUNT;
       return rc;
     }
   /****************************************************************
    * DESCRIPTION:
    * childLbid is the new child block and need to register it in 
    * the correct parent and assigned level        
    * Private Function for getting the header
    * 
    ***************************************************************/ 
   const int IndexList::updateParentStatus(uint64_t& childLbid)                                      
   {
      int rc;
      CommBlock cb;
        
      cb.file.oid = m_oid;     
      cb.file.pFile = m_pFile; 
        
      IdxRidListArrayPtr   idxRidListArrayPtr; 
      //Get the parent block read out or get the 4 children pointers out
      if ((m_parentBlock.state== BLK_INIT)||(m_parentBlock.lbid !=m_parentLbid))
      {    
                     rc = readSubBlockEntry(cb, &m_parentBlock, m_parentLbid,
                                            0, BEGIN_LIST_BLK_LLP_POS, 
                                            LIST_BLK_LLP_ENTRY_WIDTH, 
                                            &idxRidListArrayPtr);
                     m_parentBlock.dirty = true;
                     m_parentBlock.state = BLK_READ;
                     m_parentBlock.lbid  = m_parentLbid;
      }
      else
               getSubBlockEntry(m_parentBlock.data, 0, BEGIN_LIST_BLK_LLP_POS,                                             
                                LIST_BLK_LLP_ENTRY_WIDTH, 
                                &idxRidListArrayPtr);
      //update current Child Block to full
      // The reason to update parent is becuse CURRENT child link is full
      // The CURRENT child lbid is m_lbid, m_curBlkPos is where it was registered
      if (m_lbid != m_parentLbid)//The fulled child is not the parent itself
      { //normal case found the child block is full and set it full

        if (idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos].type == (int)LIST_BLOCK_TYPE)
             idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos].llpStat = LLP_FULL;
        //else if ((m_curBlkPos==0) && (m_flag)) //Next level parent, a new parent, no child
        else if (m_curBlkPos==0)//get here only when a brand new parent given, 
                                //it went up one level and the go back down to the same level, 
                                
        {
             if (!m_flag) //it is NOT the first block, this flag is useless, cannot be FALSE!
                return ERR_IDX_LIST_WRONG_TYPE;
             idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos].type = LIST_BLOCK_TYPE;
             idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos].llpStat = LLP_NOT_FULL;
             idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos].spare =0;
             idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos].childLbid= childLbid;
             setSubBlockEntry(m_parentBlock.data, 0, BEGIN_LIST_BLK_LLP_POS+m_curBlkPos, 
                   LIST_ENTRY_WIDTH, &idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos]);
             rc = writeDBFile( cb, m_parentBlock.data, m_parentLbid );
             m_parentBlock.state = BLK_READ;
             m_curLevelPos ++;
             return rc; 
        }
        else //m_flag cannot be false since it is not the first block
          return ERR_IDX_LIST_WRONG_TYPE;
      }
      else//This is the first block-> the current block's parent is itself
      {//only done once when first block was full
         m_curBlkPos = 0;
         idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos].type = (int)LIST_BLOCK_TYPE;
         idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos].llpStat = LLP_FULL;
         idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos].spare =0;
         idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos].childLbid= childLbid;
         setSubBlockEntry(m_parentBlock.data, 0, BEGIN_LIST_BLK_LLP_POS+m_curBlkPos, 
                   LIST_ENTRY_WIDTH, &idxRidListArrayPtr.childIdxRidListPtr[m_curBlkPos]);
         rc = writeDBFile( cb, m_parentBlock.data, m_parentLbid );
         m_parentBlock.state = BLK_READ;
         m_curLevel=1;
         m_curLevelPos = 0;
         return rc;
      }
      int i;
      for (i=0; i<TOTAL_NUM_ARRAY_PTR; i++)
      {
          if(idxRidListArrayPtr.childIdxRidListPtr[i].type != (int)LIST_BLOCK_TYPE)
          {
             idxRidListArrayPtr.childIdxRidListPtr[i].type = LIST_BLOCK_TYPE;
             idxRidListArrayPtr.childIdxRidListPtr[i].llpStat = LLP_NOT_FULL;
             idxRidListArrayPtr.childIdxRidListPtr[i].spare =0;
             idxRidListArrayPtr.childIdxRidListPtr[i].childLbid= childLbid;
             setSubBlockEntry(m_parentBlock.data, 0, BEGIN_LIST_BLK_LLP_POS+ i, 
                   LIST_ENTRY_WIDTH, &idxRidListArrayPtr.childIdxRidListPtr[i]);
             rc = writeDBFile( cb, m_parentBlock.data, m_parentLbid );
             m_parentBlock.state = BLK_READ;
             m_curBlkPos = i; //Need to figure out this where to put it
             m_curLevelPos ++;
             return rc;
          }
      }
      //Parent is full and all children are full Not found any child pointer is available to add
      //then go to sibling in the same level or down
      //Need to look for the next block on parent block, on level down
      //The level will be increment by 1 HERE
      //Change parent lbid go to next link
      if (m_curLevelPos == (pow(4.0, m_curLevel)-1))//if the last node of this level
      {
         m_curLevel++;
         m_curLevelPos = -1;
      }
      m_flag = true; // this need to go
      // A new parent
      m_curBlkPos = 0;
      m_parentLbid = idxRidListArrayPtr.nextIdxListPtr.nextLbid;
      memset( m_parentBlock.data, 0, sizeof(m_parentBlock.data));
      m_parentBlock.state = BLK_INIT;
      m_parentBlock.dirty = false;
     
      rc = updateParentStatus(childLbid);
      return rc;
   } 
  }
