/* Copyright (C) 2013 Calpont Corp.

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

/****************************************************************************/
/*                                                                          */
/*   we_indexlist_add_hdr.cpp                                               */
/*   (c) 2001 Author                                                        */
/*                                                                          */
/*   Description                                                            */
/*                                                                          */
/************************************************************************** */
#include <stdio.h>
#include <string.h>
#ifndef _MSC_VER
#include <inttypes.h>
#endif
#include "we_indexlist.h"
using namespace std;

namespace WriteEngine
{  
  /****************************************************************
   * DESCRIPTION:
   * RETURN:
   *    success    - successfully created the index list header
   *    failure    - it did not create the index list header    
   ***********************************************************/
    const int IndexList::updateHdrSub(const RID& newRid, const uint64_t &key)
    {             
       int rc = ERR_IDX_LIST_INVALID_UP_HDR;
       CommBlock cb;
       cb.file.oid = m_oid;     
       cb.file.pFile = m_pFile;
       
       if (m_curIdxRidListHdr.firstIdxRidListEntry.type
                      == (int)LIST_NOT_USED_TYPE)
       {
                m_curIdxRidListHdr.firstIdxRidListEntry.type =LIST_RID_TYPE;
                m_curIdxRidListHdr.firstIdxRidListEntry.rid = newRid;
                m_curIdxRidListHdr.idxRidListSize.size++;
                if (m_hdrBlock.lbid == m_hdrLbid)
                    rc = writeSubBlockEntry( cb, &m_hdrBlock,  m_hdrLbid, 
                                             m_hdrSbid, m_hdrEntry,
                                             LIST_HDR_SIZE,
                                             &m_curIdxRidListHdr );
                else
                    return ERR_IDX_LIST_WRONG_LBID_WRITE;
                m_hdrBlock.state = BLK_READ;
                return rc;       
        };//Done      
        //Check Header last entry's type and go to different next step 
        m_nextType = m_curIdxRidListHdr.nextIdxRidListPtr.type;   
        switch (m_nextType) 
        {
             case LIST_NOT_USED_TYPE://Header is not full
                //insert row id into header last enty 
                  m_curIdxRidListHdr.nextIdxRidListPtr.type=LIST_RID_TYPE;
                  m_curIdxRidListHdr.nextIdxRidListPtr.llp= newRid; 
                  m_curIdxRidListHdr.idxRidListSize.size++; 
                  if (m_hdrBlock.lbid == m_hdrLbid)                  
                   rc =writeSubBlockEntry( cb, &m_hdrBlock,m_hdrLbid, 
                                           m_hdrSbid, m_hdrEntry,                                            
                                           LIST_HDR_SIZE, &m_curIdxRidListHdr);
                  else 
                   return ERR_IDX_LIST_WRONG_LBID_WRITE;
                   
                  m_hdrBlock.state = BLK_READ;
                  m_lastLbid = INVALID_LBID;
                  break;
             case LIST_RID_TYPE://Header is full, need a new sub-block  	      
                  RID oldRid ;
                  IdxEmptyListEntry newIdxListEntry;
                  oldRid = m_curIdxRidListHdr.nextIdxRidListPtr.llp;    	         	      
                  //need a sub block                   
                  m_segType = LIST_SUBBLOCK_TYPE;
                  
                  rc= moveRidsToNewSub(m_pFile, oldRid,newRid,&newIdxListEntry);
                  if (rc!=NO_ERROR)
                    return rc;
                  //update header count twice
                  m_curIdxRidListHdr.nextIdxRidListPtr.type = m_segType;
                  m_curIdxRidListHdr.nextIdxRidListPtr.llp = 
                                   ((IdxRidListHdrPtr*)&newIdxListEntry)->llp;
                  m_curIdxRidListHdr.nextIdxRidListPtr.spare= 0;
                  m_curIdxRidListHdr.idxRidListSize.size++; 
                  setSubBlockEntry(m_hdrBlock.data, m_hdrSbid, 
                                   m_hdrEntry, LIST_HDR_SIZE, 
                                   &m_curIdxRidListHdr );
                  rc = writeDBFile( cb, m_hdrBlock.data, m_hdrLbid );

                  if (rc!=NO_ERROR)
                           return rc;
                  m_hdrBlock.state = BLK_READ;        
                  m_lastLbid = INVALID_LBID;
                  if (m_curBlock.state==BLK_WRITE)
                  {
                    rc = writeDBFile( cb, m_curBlock.data, m_lbid );
                    m_curBlock.state = BLK_READ;
                  }
                   
                  break;
             case LIST_SUBBLOCK_TYPE: //first one is a sub block
             		     
                    m_lbid= ((IdxEmptyListEntry*)
                              &(m_curIdxRidListHdr.nextIdxRidListPtr))->fbo;
                    m_sbid=((IdxEmptyListEntry*)
                              &(m_curIdxRidListHdr.nextIdxRidListPtr))->sbid;
                    m_entry=((IdxEmptyListEntry*)
                              &(m_curIdxRidListHdr.nextIdxRidListPtr))->entry;     
                    m_curType = m_nextType;  
                    m_segType = LIST_BLOCK_TYPE;        
                    rc =readCurBlk();      
                    rc = getNextInfoFromBlk(m_lastIdxRidListPtr);  
                    rc = addRidInSub(newRid, m_lastIdxRidListPtr);
                    break;                              	      
             default:
                    rc=ERR_IDX_LIST_INVALID_UP_HDR;
                    break;	
         }	//end of switch
      return rc;
   }
  /************************************************
   * No Change    
   * RETURN:
   *    success    - successfully created the index list header
   *    failure    - it did not create the index list header    
   ***********************************************************/  
   const int IndexList::addRidInSub(const RID& newRid, 
                                    IdxRidListPtr& lastIdxRidListPtr)
   {
       int rc = NO_ERROR;
       int maxCount;
       int count;
       CommBlock cb;
       
       cb.file.oid   = m_oid;
       cb.file.pFile = m_pFile;
    
       m_curType  = LIST_SUBBLOCK_TYPE;
       m_segType  = LIST_BLOCK_TYPE; 
       m_nextType = lastIdxRidListPtr.type;
       
       maxCount      = MAX_SUB_RID_CNT;
       count      = lastIdxRidListPtr.count;
       //For n-array
       m_curLevel =0;
       m_curLevelPos = 0;
       m_curBlkPos = 0;
       m_parentLbid = INVALID_LBID;
          
       if ((count ==maxCount) && (m_nextType==LIST_SIZE_TYPE))
       {//Full, need a new segment
      
           IdxEmptyListEntry newIdxListEntryPtr;
           memset(&newIdxListEntryPtr, 0, sizeof(newIdxListEntryPtr));
           rc = getSegment( m_pFile,  ENTRY_BLK, &newIdxListEntryPtr);
           if (rc!=NO_ERROR)
            return rc;
           lastIdxRidListPtr.type = LIST_BLOCK_TYPE;
           lastIdxRidListPtr.llp=((IdxRidListPtr*)&newIdxListEntryPtr)->llp;
           lastIdxRidListPtr.spare=0x0;
           rc = setNextInfoFromBlk( lastIdxRidListPtr);
           //New Block initialization 
           m_lbid = newIdxListEntryPtr.fbo; 
           m_sbid = 0;
           m_entry = 0;
           m_curType =  m_segType;
           rc = readCurBlk();
           //make sure no garbage in the new block last entry
           IdxRidListPtr idxRidListPtr;
           memset(&idxRidListPtr, 0, sizeof(idxRidListPtr));
           rc = setNextInfoFromBlk( idxRidListPtr);
           count = 0;
           if (m_useNarray)
             rc = initCurBlock();
           if (m_lastLbid != m_lbid)
           {
              rc = setLastLbid(m_lbid);    
           } 
       }//end if FULL 
       else if (count <maxCount)// if less than maxCount either type =7 or 0
       {
           if (m_lastLbid != INVALID_LBID)
           {
             uint64_t zlbid = INVALID_LBID;
             rc = setLastLbid(zlbid);  
           }
       } //endif count
       else if ((count ==maxCount) && (m_nextType==LIST_BLOCK_TYPE))
       {
         m_lbid = ((IdxEmptyListEntry*)&lastIdxRidListPtr)->fbo;
         m_sbid = 0;
         m_entry = 0;
         m_curType = LIST_BLOCK_TYPE;
         rc = addRidInBlk(newRid);
         return rc;
       }
       rc = insertRid(newRid, count);// count is the position
       rc = updateCurCount();
       rc = updateHdrCount();
           
       return rc;
   }
  /***********************************************************
   * DESCRIPTION:
   * RETURN:
   *    success NO_ERROR
   *    fail    
   ***********************************************************/  
   const int IndexList::moveRidsToNewSub(FILE* pFile, const RID& oldRid, 
                                         const RID& newRid,
                                         IdxEmptyListEntry* newIdxListEntryPtr)
   {    
         int rc;  
         //Write everything out in getSegment  
         m_segType = LIST_SUBBLOCK_TYPE;
                        
         rc =getSegment( m_pFile, ENTRY_32, newIdxListEntryPtr );         
         if (rc != NO_ERROR)
             return rc;
         //This is the new segment   
         m_curType = LIST_SUBBLOCK_TYPE;        
         m_lbid   = newIdxListEntryPtr->fbo;
         m_sbid   = newIdxListEntryPtr->sbid;
         m_entry  = newIdxListEntryPtr->entry;
         if  (m_lbid !=m_hdrLbid)
           rc = readCurBlk();
         rc = insertRid(oldRid, m_entry);
         m_entry++;
         rc = insertRid(newRid, m_entry);
         rc = updateCurCount(2);
                             
         return rc;                                              
   }
  /****************************************************************************
   * RETURN:
   *    success    - successfully created the index list header
   *    failure    - it did not create the index list header    
   ***************************************************************************/  
   const int IndexList::updateHdrCount()
   {    
      int rc=NO_ERROR; 
      
      if (m_hdrBlock.state == BLK_INIT)
       return ERR_IDX_LIST_UPDATE_HDR_COUNT;
      m_curIdxRidListHdr.idxRidListSize.size++; 
      setSubBlockEntry( m_hdrBlock.data, m_hdrSbid, 
                        m_hdrEntry, LIST_HDR_SIZE, 
                        &m_curIdxRidListHdr );
      m_hdrBlock.state = BLK_WRITE;
    
      return rc;
   }
 }
