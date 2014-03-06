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

/* ========================================================================== */
/*                                                                            */
/*   we_indexlist_multiple.cpp                                                */
/*   (c) 2001 Author                                                          */
/*                                                                            */
/*   Description                                                              */
/*                                                                            */
/* ========================================================================== */
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
    * Public Function for adding a header and more than one row id   
    *    
    * PARAMETERS:
    *    input 
    *        pFile      - File Handler
    *        rid        - Input row ID 
    *        key        - Input key value
    *    output 
    *        listHdrPtr - Output a pointer to the index list header
    *                     passed back to the caller
    *                     it will containlbid,sbid,entry    
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/   

    const int IndexList::addIndexListHdr( FILE* pFile, const RID* ridList, 
                                          const int size, const uint64_t& key, 
                                          IdxEmptyListEntry* newEmptyListPtr)
    {
      int rc;   
      CommBlock cb; 
      m_pFile       = pFile;       
      cb.file.oid   = m_oid;
      cb.file.pFile = m_pFile;
                                                
      if (size < 1)
       return ERR_IDX_LIST_INVALID_ADDHDR;
      //cout << "key =" << key << endl;      
      //Initialize three blokcs               
      rc = init();
      //The same as a single rowid 
      rc = addIndexListHdr( pFile, ridList[0], key, newEmptyListPtr);
      if (size == 1)
      {
        return rc;
      }
      //More than one row id
      //Set up the header structure     
      m_hdrLbid   = newEmptyListPtr->fbo;
      m_hdrSbid   = newEmptyListPtr->sbid;
      m_hdrEntry  = newEmptyListPtr->entry;
      rc = updateIndexList(pFile, ridList[1], key, newEmptyListPtr);
      if (rc!=NO_ERROR)
       return rc;
      
      if (size==2)
       return rc;
      rc = updateIndexList(pFile, ridList[2], key, newEmptyListPtr);
      if (rc!=NO_ERROR)
       return rc;
      
      if (size==3)
       return rc;         
      //DONE with the header   
      //add more row id into the subblock and block
      //first segment is a subblock
      m_curType =LIST_SUBBLOCK_TYPE ;
      m_type =LIST_SUBBLOCK_TYPE ;
      int startPos=3; 
      rc = getSubBlk();   
      if (size > 3)
       rc = addRidList(ridList, size,startPos); 
      if (rc != NO_ERROR)
       return rc;
    
      rc = updateIndexListWrite();   
      //rc = getSubBlk(); 
      //cout << "DEBUG INFO====================================="<< endl;
      //cout<< " m_oid=" << m_oid << endl;     
      //cout<< " key= " << key << " header lbid=" << newEmptyListPtr->fbo 
      //    << " header sbid= " << newEmptyListPtr->sbid 
      //    << " header entry= " << newEmptyListPtr->entry <<endl;                                     
      return rc;
     
   };
   /************************************************************
    * Description
    * Private Function, Internal use  
    * For adding a whole subblock or block
    * The segment is a clean slate and the lbid has been allocated         
    * Adding more rowid into the list, first time
    * without any delete action preceeding this
    * Fill up the subblock 31 entries first
    * Then blocks if needed        
    * input -- ridList - rowid List
    * input -- size    - total array size              
    *
    ************************************************************/            
   const int IndexList::addRidList(const RID* ridList, const int size, 
                                   int& startPos)
   {
     int remainder = size-startPos ;
     int insCnt, maxCount; 
     bool needNextBlk = false;
     int rc = NO_ERROR;   
     //int count =2;
     int count=0;
     CommBlock cb;
    
     cb.file.oid = m_oid;
     cb.file.pFile = m_pFile; 
     
     IdxEmptyListEntry newEmptyEntry;     
     rc = getSubBlk(m_lbid, m_sbid, m_entry);
     if (rc!=NO_ERROR)
       return rc;
     m_curType  =  ((IdxEmptyListEntry*)
                               &(m_curIdxRidListHdr.nextIdxRidListPtr))->type;      
     maxCount = MAX_SUB_RID_CNT; 
     m_curLevel = 0;
     m_curLevelPos = 0;
     m_curBlkPos = 0; 
     rc = readCurBlk(); 
     //startPos means how many has been inserted
     while (startPos < size)
     {
           rc = getNextInfoFromBlk();
           count = m_lastIdxRidListPtr.count;
           if (m_useNarray)
             m_curLevel = ((IdxRidNextListPtr*)&m_lastIdxRidListPtr)->curLevel;
             
           if (remainder > (maxCount-count))
           { 
               insCnt = maxCount-count;       
               remainder = remainder - insCnt;
               needNextBlk = true;
           }     
           else
           {
               insCnt = remainder;
               remainder = 0;
               needNextBlk = false;
           }  
           //Prepare to write the ridList 
           //write the current subblock or block
           for (int i=count; i< (count+insCnt) ; i++)
           {
             //cout << "startPos->" << startPos << endl;
             RID newRID= ridList[startPos];
	            rc = insertRid(newRID, i);
             startPos++;
             m_curIdxRidListHdr.idxRidListSize.size++; 
             m_hdrBlock.state =BLK_WRITE;
           }//end for
           setSubBlockEntry(m_hdrBlock.data, m_hdrSbid, 
                            m_hdrEntry, LIST_HDR_SIZE, 
                           &m_curIdxRidListHdr );
           m_hdrBlock.state =BLK_WRITE;
           //write out the last pointer and it depends on how much rid left
           if (!needNextBlk) 
           {//No more, the end. Just update the current block  
            //Not read from existing block, so initiate one
                if ((m_useNarray) && (m_curType == LIST_BLOCK_TYPE))
                    rc = updateCurCountInArray(insCnt);
                else
                {
                   m_lastIdxRidListPtr.count= count+insCnt;
                   rc = setNextInfoFromBlk( m_lastIdxRidListPtr);
                }
                if (m_lastLbid != m_lbid)
                  rc = setLastLbid(m_lbid);      
                if (rc!=NO_ERROR)
                {
                    return ERR_IDX_LIST_INVALID_ADD_LIST;
                } 
                return rc;//Done
           }
           else//new link
           {//take care the last entry with the new link
             int lastCount=0;
             m_segType = LIST_BLOCK_TYPE;
             memset(&newEmptyEntry, 0, sizeof(newEmptyEntry));
             rc = getSegment(m_pFile,ENTRY_BLK, &newEmptyEntry);   
             //handle current block update before move to the new block
             lastCount = count +insCnt;
             if ((m_curType == LIST_SUBBLOCK_TYPE ) || (!m_useNarray))
             {
               m_lastIdxRidListPtr.llp=((IdxRidListHdrPtr*)&newEmptyEntry)->llp;
               m_lastIdxRidListPtr.type= LIST_BLOCK_TYPE;
               m_lastIdxRidListPtr.spare=0x0;
               m_lastIdxRidListPtr.count= lastCount;
             }
             m_nextLbid = ((IdxEmptyListEntry*)&newEmptyEntry)->fbo;
             m_nextSbid =  0 ;
             m_nextEntry = 0 ;
             m_nextType = m_segType;
             if ((m_curType == LIST_SUBBLOCK_TYPE ) || (!m_useNarray))
             {//when current block is a subblock or single child link
                    rc = setNextInfoFromBlk( m_lastIdxRidListPtr);
             }
             else
             {
                     rc = updateLastPtrAndParent(lastCount);
             }
             //Move on to the new block for insertions
             m_lbid  = newEmptyEntry.fbo;
             m_sbid  = 0;
             m_entry = 0;
             m_curType = m_nextType;
                 
             rc = readCurBlk();
             //make sure no garbage
             IdxRidListPtr idxRidListPtr;
             memset(&idxRidListPtr, 0, sizeof(idxRidListPtr));
             rc = setNextInfoFromBlk( idxRidListPtr);

             if (m_useNarray)
             {
                    maxCount = MAX_BLK_NARRAY_RID_CNT;
                    rc = initCurBlock();
             }
             else
                    maxCount = MAX_BLK_RID_CNT;
             count =0;
           } //end else if needs next block
     }//end while
     return rc;
   }
   /****************************************************************
    * DESCRIPTION:
    * (0) THIS FUNCTION CAN ONLY BE CALLED WITH THE PUBLIC
    *           
    * (1) Given a key value and a row ID, update the link list           
    * Converted   
    * PARAMETERS:
    *    Input rid        
    *        --row ID
    *    Input key        
    *        -- key value
    *                 
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/
    const int IndexList::updateIndexList(FILE* pFile, const RID* ridList, 
                                         const int size, const uint64_t &key, 
                                         IdxEmptyListEntry* curIdxRidListHdrPtr)
    {
      int rc;   
      CommBlock cb;
      m_pFile = pFile;
      cb.file.oid = m_oid;
      cb.file.pFile = m_pFile; 
      
      m_pFile = pFile; 
      //cout << "key=" << key << endl;
      rc = init();
      rc = updateIndexList(pFile, ridList[0], key, curIdxRidListHdrPtr);      
      if (size == 1)
        return rc;
      rc = updateIndexList(pFile, ridList[1], key, curIdxRidListHdrPtr);
      if (size == 2)        
        return rc;                
      rc = getHdrInfo(curIdxRidListHdrPtr);
      if (rc != NO_ERROR)
      { 
              return rc;
      }
      if (key!= m_curIdxRidListHdr.key) 
      {
            //cout << "line 829->Error Key ->" << key << endl;
            //cout << "m_curIdxRidListHdr.key->"<<m_curIdxRidListHdr.key<< endl;
            //cout << "m_hdrLbid->" << m_hdrLbid << endl;
            //cout << "m_hdrSbid->" << m_hdrSbid << endl;
            //cout << "m_hdrEntry->" << m_hdrEntry << endl;
            //cout << "m_oid->" << m_oid << endl;
            return ERR_IDX_LIST_INVALID_KEY;
      }  
      int startPos=2;
      rc = updateIndexList(ridList, size, startPos);
      if (rc!=NO_ERROR)
      {
             return rc;
      }
      rc = updateIndexListWrite();
      if (rc!=NO_ERROR)
      {
            return rc;
      }
	     return rc;
      
    }
    
    /****************************************************************
    * DESCRIPTION:
    * (0) THIS FUNCTION CAN ONLY BE CALLED WITH THE PUBLIC
    *           
    * (1) Given a key value and a row ID, update the link list           
    * Converted   
    * PARAMETERS:
    *    Input rid        
    *        --row ID
    *    Input key        
    *        -- key value
    *                 
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/
    const int  IndexList::updateIndexList( const RID* ridList, const int size, 
                                           int& startPos)
    {
      IdxEmptyListEntry   newEmptyEntry;
      int rc = NO_ERROR, width=8;
      int pos = 0, totalbytes = 0, maxCount =0;      
      int remainder = size-startPos;
      int count=0;
      bool needNextBlk = false;
      
      IdxRidListEntry idxRidListEntry;
      int oldType;
       
      CommBlock cb;
      cb.file.oid = m_oid;
      cb.file.pFile = m_pFile;          
      if (m_pFile == NULL)      
       return ERR_IDX_LIST_INVALID_UPDATE;
       
      //Header should be available by now  
      if (((long long)m_hdrLbid==-1LL) || (m_hdrSbid==-1) || (m_hdrEntry==-1))
       return ERR_IDX_LIST_INVALID_UPDATE;
       
      width= LIST_ENTRY_WIDTH; 
      memset(&m_lastIdxRidListPtr, 0, sizeof(m_lastIdxRidListPtr));               
      if (m_lastLbid == (uint64_t)INVALID_LBID)
      {    
        rc = getSubBlk(m_lbid, m_sbid, m_entry);
        m_curType  =  ((IdxEmptyListEntry*)
                               &(m_curIdxRidListHdr.nextIdxRidListPtr))->type;                                      
        //First link           
        pos = LIST_SUB_LLP_POS;
        totalbytes = SUBBLOCK_TOTAL_BYTES;
        maxCount = MAX_SUB_RID_CNT;
        m_segType = LIST_SUBBLOCK_TYPE; //This is for next segment type 
        m_curType = LIST_SUBBLOCK_TYPE; 
      }
      else
      {   
        m_lbid = m_lastLbid;
        m_sbid =0;
        m_entry = 0;                
        pos = LIST_BLOCK_LLP_POS;
        totalbytes = BYTE_PER_BLOCK;
        if (m_useNarray)
               maxCount = MAX_BLK_NARRAY_RID_CNT;
        else
               maxCount = MAX_BLK_RID_CNT;
       
        
        m_curType = LIST_BLOCK_TYPE;
        m_segType = LIST_BLOCK_TYPE;
      }
      
      while (startPos < size)
      {                 
             rc = getNextInfoFromBlk();
             count = m_lastIdxRidListPtr.count;
             if (m_useNarray)
                m_curLevel=((IdxRidNextListPtr*)&m_lastIdxRidListPtr)->curLevel;
             int availCount = maxCount-count;
             int insCnt=0;
             m_nextType = m_lastIdxRidListPtr.type;
             if (availCount>0)
             {
                   if (remainder > availCount)
                   { 
                      insCnt = availCount;                             
                      needNextBlk = true;
                   }     
                   else
                   {
                       insCnt = remainder;
                       needNextBlk = false;
                   }              
                   for (int i = 0; i< insCnt; i++)
                   {
                      idxRidListEntry.type  =LIST_RID_TYPE;
                      idxRidListEntry.spare =0x0;
                      idxRidListEntry.rid   =ridList[startPos];
                      startPos++;
                      remainder--;
                      m_lastIdxRidListPtr.count++;
                      availCount--;
                      if (m_lbid!=m_hdrLbid)
                      { 
                        if ((m_curBlock.state != BLK_INIT) && 
                            (m_curBlock.lbid == m_lbid))
                           setSubBlockEntry( m_curBlock.data, m_sbid, 
                                          i+count, width, &idxRidListEntry );
                        else
                        {
                           //return ERR_IDX_LIST_INVALID_UPDATE;
                           rc =readCurBlk();
                           if (rc!=NO_ERROR)
                             return ERR_IDX_LIST_INVALID_BLK_READ;
                           setSubBlockEntry( m_curBlock.data, m_sbid, 
                                          i+count, width, &idxRidListEntry );
 
                        }
                        m_curBlock.dirty = true;
                        m_curBlock.state = BLK_WRITE;
                      }
                      else
                      {
                        setSubBlockEntry( m_hdrBlock.data, m_sbid, 
                                          i+count, width, &idxRidListEntry );
                        m_hdrBlock.dirty = true;
                        m_hdrBlock.state = BLK_WRITE;
                      } 
                       m_curIdxRidListHdr.idxRidListSize.size++; 
                       m_hdrBlock.state =BLK_WRITE;                            
                    }//end for
                    setSubBlockEntry(m_hdrBlock.data, m_hdrSbid, 
                                     m_hdrEntry, LIST_HDR_SIZE, 
                                     &m_curIdxRidListHdr );
                    m_hdrBlock.state = BLK_WRITE; 
             }//endif availCount>0
             else if (remainder >0)
             {
                needNextBlk = true;
             }
             else if (remainder==0)
             {
                needNextBlk = false;
                
             }
                  
             if (!needNextBlk) 
             {//No more, the end. Just update the current block  
               if (insCnt > 0)
               {
                       m_lastIdxRidListPtr.count= count+insCnt;
                       if (m_lbid!=m_hdrLbid)
                       { 
                          if ((m_curBlock.lbid == m_lbid) && 
                              (m_curBlock.state !=BLK_INIT))
                            setSubBlockEntry(m_curBlock.data, m_sbid, pos,width,
                                            &m_lastIdxRidListPtr ); 
                          else
                             return ERR_IDX_LIST_WRONG_LBID_WRITE; 
                       
                          m_curBlock.state = BLK_WRITE;         
                       }
                       else
                       {
                          setSubBlockEntry( m_hdrBlock.data, m_sbid, pos, width,
                                            &m_lastIdxRidListPtr );
                          m_hdrBlock.state =BLK_WRITE;
                       }
                       if (m_curType == LIST_SUBBLOCK_TYPE)
                       {
                           if (m_lastLbid != INVALID_LBID)
                           {
                             uint64_t zlbid = INVALID_LBID;
                             rc = setLastLbid(zlbid);
                           }
                       }
                       else
                       {
                           if (m_lastLbid != m_lbid)
                             rc = setLastLbid(m_lbid);
                       }
                       if (rc!=NO_ERROR)
                       {
                             return ERR_IDX_LIST_INVALID_UPDATE;
                       } 
                       return rc;//Done
                }
                else
                       return NO_ERROR;
             }//no new link
             else 
             {//take care the last entry with the new link
                    int lastCount=0;
                    lastCount = count + insCnt;
                    m_lastIdxRidListPtr.type= LIST_BLOCK_TYPE;
                    m_lastIdxRidListPtr.spare=0x0;
                    m_lastIdxRidListPtr.count= lastCount;
                    if (m_nextType != LIST_BLOCK_TYPE)
                    {
                        m_segType = LIST_BLOCK_TYPE;
                        rc = getSegment(m_pFile,ENTRY_BLK, &newEmptyEntry); 
                        if (rc != NO_ERROR) 
                        {
                          cout <<"Indexlist->Free mgr getSegment ERROR CODE rc=" << rc << endl;
                          return rc; 
                        }
                        //handle current block update before move to the new 
                        m_lastIdxRidListPtr.llp=
                                      ((IdxRidListHdrPtr*)&newEmptyEntry)->llp;
                        //For Narray
                        m_nextLbid = ((IdxEmptyListEntry*)&newEmptyEntry)->fbo;
                        m_nextSbid =  0 ;
                        m_nextEntry = 0 ;
                        oldType = m_nextType;
                        m_nextType = m_segType;
                        
                     } 
                    else
                    {
                     m_nextLbid=((IdxEmptyListEntry*)&m_lastIdxRidListPtr)->fbo;
                     m_nextSbid =  0 ;
                     m_nextEntry = 0 ;
                     oldType = m_nextType;
                     m_nextType = m_segType;
                    }                
                    if (m_curType == LIST_SUBBLOCK_TYPE )
                    {//when current block is a subblock
                            if (m_lbid == m_hdrLbid)
                            {//header should be read already
                               setSubBlockEntry( m_hdrBlock.data, m_sbid, 
                                                 pos, width,
                                                 &m_lastIdxRidListPtr );
                               m_hdrBlock.state = BLK_WRITE;
                             }
                             else
                             {
                               rc = readCurBlk();
                               setSubBlockEntry( m_curBlock.data, m_sbid, 
                                                 pos, width,
                                                 &m_lastIdxRidListPtr );
                               m_curBlock.state = BLK_WRITE;
                              }
                     }
                     else
                     {
                              if (m_useNarray)
                               rc = updateLastPtrAndParent(lastCount);
                              else
                               rc = updateLastPtr(lastCount);
                     }
                     writeCurBlk();
                     m_lbid  = m_nextLbid;
                     m_sbid  = 0;
                     m_entry = 0;
                     //m_lastLbid = m_lbid;
                     m_curType = m_nextType;
                     pos = LIST_BLOCK_LLP_POS;
                     totalbytes = BYTE_PER_BLOCK;
                     rc = readCurBlk();
                     if (m_useNarray)
                     {
                          maxCount = MAX_BLK_NARRAY_RID_CNT;
                          if (oldType != LIST_BLOCK_TYPE)
                            rc = initCurBlock();
                     }
                     else
                          maxCount = MAX_BLK_RID_CNT;
                        
             } //end else if needs next block
                 
      }//end while   
      
      return rc; 
    }                   
}//end namespace
    
 
