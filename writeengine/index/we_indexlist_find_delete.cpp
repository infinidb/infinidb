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

/* ========================================================================== */
/*                                                                            */
/*   Filename.c                                                               */
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
#include "we_indextree.h"
#include "we_indexlist.h"


using namespace std;

namespace WriteEngine
{  

   /************************************************
    * Description:
    * Find a entry for the given rowId and Key
    * Then Delete it from the list
    * Move the rest of the row id up in the same
    * sub block an decrement the count in that subblock
    * decrement the header size  
    * Converted                                        
    * input
    *     pFile       -- File Handler     
    *     rowId       -- row id
    *     key         -- value    
    *     curIdxRidListHdrPtr - point to the header    
    *     
    * return value
    *        Success -- 0
    *        Fail    -- ERR_IDX_LIST_INVALID_DELETE            
    ************************************************/        
    const int  IndexList::deleteInSub( const RID& rowId)                       
    {
     
     int rc =ERR_IDX_LIST_INVALID_DELETE;    
     DataBlock prevDataBlock;
     int pos =0, totalbytes=0;
     IdxRidListPtr* lastIdxRidListPtr;
     int type;
     
     CommBlock cb;
     cb.file.oid   = m_oid;
     cb.file.pFile = m_pFile;
      
      //get thelbid sbid and entry out from the header last entry
      //First Sub-block       
     m_lbid=((IdxEmptyListEntry*)&(m_curIdxRidListHdr.nextIdxRidListPtr))->fbo;
     m_sbid=((IdxEmptyListEntry*)&(m_curIdxRidListHdr.nextIdxRidListPtr))->sbid;
     m_entry=((IdxEmptyListEntry*)&(m_curIdxRidListHdr.nextIdxRidListPtr))->entry;                           
     //Read the pointer entry at LIST_SUB_LLP_POS location
     
     IdxRidListEntry rowIdArray[MAX_BLOCK_ENTRY];
     IdxRidListEntry newRowIdArray[MAX_BLOCK_ENTRY];                   
     memset(rowIdArray,0,BYTE_PER_BLOCK);
     memset(newRowIdArray,0,BYTE_PER_BLOCK);
     //First link                                             
     pos = LIST_SUB_LLP_POS;
     totalbytes = SUBBLOCK_TOTAL_BYTES;
     m_entryGroup = ENTRY_32;                
     if (m_lbid!=m_hdrLbid)
     {
         rc = readDBFile(cb, &m_curBlock, m_lbid );
         if (rc != NO_ERROR)
            return rc;
         rc = readSubBlockEntry(cb, &m_curBlock, m_lbid, 
                                m_sbid, 0, totalbytes,
                                rowIdArray); 
         if (rc != NO_ERROR)
            return rc;  
         m_curBlock.dirty=true; 
         m_curBlock.lbid = m_lbid;
         m_curBlock.state = BLK_READ;                                                      
     }
     else
     {
         if (m_hdrBlock.state >= BLK_READ)         
              getSubBlockEntry(m_hdrBlock.data, m_sbid, 
                               0, totalbytes, rowIdArray );                                
         else
              return ERR_IDX_LIST_INVALID_DELETE;              
     }           
     lastIdxRidListPtr=(IdxRidListPtr*)&rowIdArray[pos];
     int count;
     type  = lastIdxRidListPtr->type;  //next type
     count = lastIdxRidListPtr->count;//current count
     for (int i=0; i<count; i++)
     {
            if (rowIdArray[i].rid == rowId)
            {//found
                  m_dLbid = m_lbid;
                  m_dSbid = m_sbid;
                  m_dEntry= i;
                  rc = NO_ERROR;
                  memcpy(&newRowIdArray[0],
                                &rowIdArray[0], totalbytes); 
                  lastIdxRidListPtr->count--;
                  if (lastIdxRidListPtr->count==0)
                  {              
                     if (type == LIST_SIZE_TYPE)                                    
                     {
                       //header has no link
                          m_curIdxRidListHdr.nextIdxRidListPtr.type
                                         =LIST_NOT_USED_TYPE;
                          m_curIdxRidListHdr.nextIdxRidListPtr.llp= 0;
                         
                     }                                       
                   }//header's link block has nothing now
                   else //still have more
                   {
                     memcpy(&rowIdArray[i],&newRowIdArray[i+1],(count-(i+1))*LIST_ENTRY_WIDTH);
                   }
                   //last row id entry now moved up, so not used
                   rowIdArray[count-1].type  =LIST_NOT_USED_TYPE;
                   rowIdArray[count-1].rid   =0;
                   rowIdArray[count-1].spare =0;
                   //header update the size
                   m_curIdxRidListHdr.idxRidListSize.size--; 
                   if (m_lbid!=m_hdrLbid)
                   {
                            setSubBlockEntry( m_curBlock.data,  
                                              m_sbid,0, totalbytes, 
                                              rowIdArray ); 
                            rc = writeDBFile( cb, m_curBlock.data,  m_lbid);
                            if (rc != NO_ERROR)
                              return rc;
                            m_curBlock.state =BLK_READ;
                            rc = writeSubBlockEntry(cb,&m_hdrBlock,
                                                    m_hdrLbid,m_hdrSbid, 
                                                    m_hdrEntry, 
                                                    LIST_HDR_SIZE, 
                                                   &m_curIdxRidListHdr );
                            if (rc != NO_ERROR)
                              return rc;
                            
                            m_hdrBlock.state = BLK_READ;                 
                   }
                   else
                   {//m_lbid==m_hdrLbid
                            setSubBlockEntry( m_hdrBlock.data,  
                                              m_sbid,0, totalbytes, 
                                              rowIdArray );
                            setSubBlockEntry( m_hdrBlock.data, 
                                              m_hdrSbid,m_hdrEntry, 
                                              LIST_HDR_SIZE, 
                                              &m_curIdxRidListHdr);
                            m_hdrBlock.state = BLK_WRITE;
                            rc = writeDBFile( cb, m_hdrBlock.data, 
                                              m_hdrLbid);
                            if (rc != NO_ERROR)
                             return rc;                
                            m_hdrBlock.state = BLK_READ;
                   } //end if m_lbid==m_hdrHdrLbid
                  
                   m_dEntry = i; 
                   return rc;             
            }//endif  found
     }//end for 
      
     return rc;     
   }
 /************************************************
    * Description:
    * Find a entry for the given rowId and Key
    * Then Delete it from the list
    * Move the rest of the row id up in the same
    * sub block an decrement the count in that subblock
    * decrement the header size  
    * Converted                                        
    * input
    *     pFile       -- File Handler     
    *     rowId       -- row id
    *     key         -- value    
    *     curIdxRidListHdrPtr - point to the header    
    *     
    * return value
    *        Success -- 0
    *        Fail    -- ERR_IDX_LIST_INVALID_DELETE            
    ************************************************/    
    
    const int IndexList::deleteInBlock(const RID& rowId)
    {
       int width =LIST_ENTRY_WIDTH;
       int rc =ERR_IDX_LIST_INVALID_DELETE;    
       IdxRidListPtr* lastIdxRidListPtr;
       IdxRidListPtr lastSubIdxRidListPtr;
       bool found;
       int type, count;       
       IdxRidListPtr prevIdxRidListPtr;
       int  prevSbid, prevEntry, prevType;
       uint64_t prevLbid;
       DataBlock prevDataBlock;       
       int pos =0, totalbytes=0;
       int preTotalBytes, prevPos ;
       //IdxRidNextListPtr *nextIdxListPtr;
       
       IdxRidListEntry rowIdArray[MAX_BLOCK_ENTRY];
       IdxRidListEntry newRowIdArray[MAX_BLOCK_ENTRY];
     
       CommBlock cb;
       cb.file.oid   = m_oid;
       cb.file.pFile = m_pFile;
       //This is the sub block info
       prevLbid   = m_lbid;
       prevSbid   = m_sbid;
       prevEntry  = m_entry; 
       prevPos = LIST_SUB_LLP_POS;
       preTotalBytes = SUBBLOCK_TOTAL_BYTES;
       
       if (prevLbid == m_hdrLbid)  
       {
           if (m_hdrBlock.state >=BLK_READ)
             getSubBlockEntry(m_hdrBlock.data, m_sbid, 
                            prevPos, LIST_ENTRY_WIDTH, &lastSubIdxRidListPtr );
           else
             return ERR_IDX_LIST_INVALID_DELETE;
       }
       else
       {
           if (m_curBlock.state >=BLK_READ)
             getSubBlockEntry(m_curBlock.data, m_sbid, 
                              prevPos, LIST_ENTRY_WIDTH, &lastSubIdxRidListPtr );
           else
             return ERR_IDX_LIST_INVALID_DELETE;
       }   
       
       found = false;
       m_lbid    = ((IdxEmptyListEntry*)&lastSubIdxRidListPtr)->fbo;
       m_sbid    = 0;
       m_entry   = 0; 
       
       type  = lastSubIdxRidListPtr.type;
       count = lastSubIdxRidListPtr.count;
       pos = LIST_BLOCK_LLP_POS;
       totalbytes = BYTE_PER_BLOCK;
       
       //Not found in the first sub
       while ((!found) &&(type==LIST_BLOCK_TYPE))
       {
           rc = readSubBlockEntry(cb, &m_curBlock, m_lbid, 0, 0, 
                                   totalbytes, rowIdArray);
           if (rc != NO_ERROR)
            return rc;                                         
           m_curBlock.dirty = true; 
           m_curBlock.state =BLK_READ;
           m_curBlock.lbid = m_lbid;  
           prevType = type; //Save it just in case not found here                
           lastIdxRidListPtr =(IdxRidListPtr *) &rowIdArray[pos];
           type  = lastIdxRidListPtr->type;
           count = lastIdxRidListPtr->count;
                                         
           //prepared for not found in current block
           //find out what is the next type
           //Next Type is needed here
           for (int i=0; i<count; i++)
           {
              if (rowIdArray[i].rid == rowId)
              {//found the rowid
                 memcpy(&newRowIdArray[0],&rowIdArray[0],totalbytes);
                 found = true;
                 m_dLbid = m_lbid;
                 m_dSbid = m_sbid;
                 m_dEntry = i;
                 lastIdxRidListPtr->count--;                 
                 if (lastIdxRidListPtr->count==0)
                 {
                   if (!m_useNarray)
                   {
                     //get the previous value out, could be a sub block
                     if (prevLbid == m_hdrLbid)
                         getSubBlockEntry(m_hdrBlock.data, prevSbid, 
                                          prevPos, LIST_ENTRY_WIDTH, 
                                          &prevIdxRidListPtr);
                     else if (prevLbid == m_lbid)
                         getSubBlockEntry(m_curBlock.data, prevSbid, 
                                          prevPos, LIST_ENTRY_WIDTH, 
                                          &prevIdxRidListPtr);
                     else
                         rc = readSubBlockEntry(cb, &prevDataBlock, prevLbid, 
                                                prevSbid, prevPos, LIST_ENTRY_WIDTH,
                                                &prevIdxRidListPtr);
                         
                     if (rc != NO_ERROR)
                          return rc;
                     //check the type before set
                     if (type == LIST_BLOCK_TYPE)                        
                     {
                         ((IdxEmptyListEntry*)&prevIdxRidListPtr)->fbo
                            = ((IdxEmptyListEntry*)lastIdxRidListPtr)->fbo;
                         ((IdxEmptyListEntry*)&prevIdxRidListPtr)->sbid
                            = ((IdxEmptyListEntry*)lastIdxRidListPtr)->sbid;
                         ((IdxEmptyListEntry*)&prevIdxRidListPtr)->entry
                            = ((IdxEmptyListEntry*)lastIdxRidListPtr)->entry;
                          //safety check
                          prevIdxRidListPtr.type= type;
                     }
                     else // If no more links, the current one is gone also
                     {
                        if (prevIdxRidListPtr.count>0)
                        {
                              prevIdxRidListPtr.type =0;
                              prevIdxRidListPtr.llp = 0; 
                        }
                        else
                        {//In case it is a sub block, not released with 0 count
                              prevIdxRidListPtr.type =LIST_NOT_USED_TYPE;
                              prevIdxRidListPtr.llp = 0;
                        }
                      }//end if type =LIST_SUBBLOCK_TYPE,LIST_BLOCK_TYPE 
                      //;set to LIST_NOT_USED_TYPE--unused before release
                      lastIdxRidListPtr->type=LIST_NOT_USED_TYPE;
                      lastIdxRidListPtr->llp =0; 
                      if (prevPos == LIST_BLOCK_LLP_POS)
                      {
                         if (prevLbid<m_lastLbid)
                           rc = setLastLbid(prevLbid); 
                      }                                  
                    }
                 } //end if count==0
                 else
                 {
                        memcpy(&rowIdArray[i],&newRowIdArray[i+1],(count-(i+1))*LIST_ENTRY_WIDTH);
                        if (m_lastLbid > m_lbid)
                           rc = setLastLbid(m_lbid);
                            
                 }//count check
                  
                 //Found rowId
                 rowIdArray[count-1].type=LIST_NOT_USED_TYPE;
                 rowIdArray[count-1].rid =0;                   
                  
                 m_curIdxRidListHdr.idxRidListSize.size--;
                 //Write Out Put in another routine
                    
                 if ((prevLbid==m_hdrLbid) && (m_lbid != m_hdrLbid))
                 {// AAC --3
                         if (!m_useNarray)
                         {
                             if (lastIdxRidListPtr->count ==0)
                             {
                                setSubBlockEntry( m_hdrBlock.data, prevSbid, 
                                                  prevPos, width, 
                                                  &prevIdxRidListPtr ); 
                             }
                         }
                     
                         setSubBlockEntry( m_curBlock.data, m_sbid, 
                                           0, totalbytes, 
                                           rowIdArray );
                         setSubBlockEntry( m_hdrBlock.data, m_hdrSbid, 
                                           m_hdrEntry, LIST_HDR_SIZE, 
                                           &m_curIdxRidListHdr );
                         rc = writeDBFile( cb, m_hdrBlock.data,  m_hdrLbid);
                         if (rc != NO_ERROR)
                            return rc;
                         rc = writeDBFile( cb, m_curBlock.data,  m_lbid);
                         if (rc != NO_ERROR)
                          return rc;
                         m_hdrBlock.state = BLK_READ;
                         m_curBlock.state = BLK_READ;
                  }
                  else  
                  { //ABC -- 
                         if (!m_useNarray)
                         {
                            if (lastIdxRidListPtr->count ==0)
                            {
                               setSubBlockEntry( prevDataBlock.data, prevSbid, 
                                                 prevPos, LIST_ENTRY_WIDTH, 
                                                &prevIdxRidListPtr ); 
                               rc = writeDBFile( cb, prevDataBlock.data,prevLbid);
                               if (rc != NO_ERROR)
                                 return rc; 
                            }
                         }
                         setSubBlockEntry( m_curBlock.data, m_sbid, 
                                           0, totalbytes, 
                                           rowIdArray );
                         setSubBlockEntry( m_hdrBlock.data, m_hdrSbid, 
                                           m_hdrEntry, LIST_HDR_SIZE, 
                                          &m_curIdxRidListHdr );
                         rc = writeDBFile( cb, m_hdrBlock.data,  m_hdrLbid);
                         if (rc != NO_ERROR)
                              return rc;  
                         rc = writeDBFile( cb, m_curBlock.data,  m_lbid);
                                           memset(m_hdrBlock.data,0, 
                                           sizeof(m_hdrBlock.data));
                         if (rc != NO_ERROR)
                              return rc;
                          
                         m_hdrBlock.state = BLK_READ;
                         m_curBlock.state = BLK_READ;
                  } //last case A B C  --end 5                       
                  //Done with writing to disk
                  // Now we need to release the segment
                  if (!m_useNarray)
                  {
                        if (lastIdxRidListPtr->count ==0)
                        { 
                            rc = releaseSegment();
                            if (rc != NO_ERROR)
                              return rc;                      		                          
                        }// end release segment when count ==0
                  }     
                  m_entry =i; //for use in findRow ID 
                  return rc; //DONE !!!found then we return, no need to go on              
               }//FOUND THE ROWID returned !!!!
            }//for loop i not found continue to i++            
            //NOT FOUND in this block go to next block
            //assigning the current llp as previous llp:lbid, sbid, entry
            prevLbid    = m_lbid;
            prevSbid    = 0;
            prevEntry   = 0;
            prevPos     = pos;
            preTotalBytes = totalbytes;
            
            m_lbid     = ((IdxEmptyListEntry*)lastIdxRidListPtr)->fbo;
            m_sbid     = 0;
            m_entry    = 0;
       }// end while
       if (!found)
        rc = ERR_IDX_LIST_INVALID_DELETE;
       return rc;
    }


   /************************************************
    * Description:
    * Converted    
    * Find an entry for the given rowId and Key
    * Then Delete it from the list
    * Move the rest of the row id up in the same
    * sub block an decrement the count in that subblock
    * decrement the header size                                      
    * input
    *     pFile       -- File Handler     
    *     rowId       -- row id
    *     key         -- value    
    *     curIdxRidListHdrPtr - point to the header    
    *     
    * return value
    *        Success -- 0
    *        Fail    -- ERR_IDX_LIST_INVALID_DELETE            
    ************************************************/        
    const int      IndexList::deleteIndexList( FILE* pFile, const RID& rowId, 
                        const uint64_t& key, IdxEmptyListEntry* curIdxRidListHdrPtr,
                        uint64_t& lbid, int&sbid, int& entry)
    {
      int rc =ERR_IDX_LIST_INVALID_DELETE;    
      bool found = false;
                  
      m_pFile = pFile;
      getHdrInfo(curIdxRidListHdrPtr);
      if (key!= m_curIdxRidListHdr.key) 
      {
        return ERR_IDX_LIST_INVALID_KEY;
      }  
     
      uint64_t dlbid =-1LL;
      int dsbid = -1;
      int dentry = -1;  
      rc = deleteIndexList(rowId,key,dlbid,dsbid,dentry);
      
      if (rc!=NO_ERROR)
      {
       lbid=-1LL;
       sbid =-1;
       entry=-1;
       found = false;
       return rc;
      }
      else
      {
         lbid = dlbid;
         sbid = dsbid;
         entry = dentry;
      }
      
      return rc;
    }
   /************************************************
    * Description:
    * No change    
    * Find a entry for the given rowId and Key
    * Then Delete it from the list
    * Move the rest of the row id up in the same
    * sub block an decrement the count in that subblock
    * decrement the header size                                      
    * input
    *     pFile       -- File Handler     
    *     rowId       -- row id
    *     key         -- value    
    *     curIdxRidListHdrPtr - point to the header    
    *     
    * return value
    *        Success -- 0
    *        Fail    -- ERR_IDX_LIST_INVALID_DELETE            
    ************************************************/        
    const int      IndexList::deleteIndexList(const RID& rowId, 
                                              const uint64_t& key, 
                                              uint64_t& lbid, int&sbid, int& entry)
    {
      int rc =ERR_IDX_LIST_INVALID_DELETE;    
      rc = deleteIndexList(rowId,key);
      
      lbid   = m_dLbid;
      sbid   = m_dSbid;
      entry  = m_dEntry;
      return rc;
    }
   /************************************************
    * Description:
    * Converted    
    * Find all of the row Id or toke from list                               
    * input
    *     pFile       -- File Handler       
    *     curIdxRidListHdrPtr - point to the header    
    *     
    * return value
    *        Success -- 0
    *        Fail    -- ERR_IDX_LIST_INVALID_DELETE            
    ************************************************/ 
     const int    IndexList::getRIDArrayFromListHdr(FILE* pFile, uint64_t& key,
                                    IdxEmptyListEntry* curIdxRidListHdrPtr,
                                    RID* ridArray, int& size)
     {
      int rc=NO_ERROR;    
      int arrayCount = 0;
      IdxRidNextListPtr *nextIdxListPtr = NULL;
                  
      m_pFile = pFile;
      CommBlock cb;
      cb.file.oid = m_oid;
      cb.file.pFile = m_pFile;
      
      rc = getHdrInfo(curIdxRidListHdrPtr);
      if (m_curIdxRidListHdr.idxRidListSize.size==0)
      {size =0; return NO_ERROR;}
        
      if (key!= m_curIdxRidListHdr.key) 
      {
        return ERR_IDX_LIST_WRONG_KEY;
      }  
      // cout << "IndexList::getRIDArrayFromListHdr->KEY ------>" << key << endl;
      //Check the first row location, 3rd enty
      if (m_curIdxRidListHdr.firstIdxRidListEntry.type==(int)LIST_RID_TYPE)
      {        
        ridArray[arrayCount]= (RID)m_curIdxRidListHdr.firstIdxRidListEntry.rid;
        //cout<<" IndexList::getRIDArrayFromListHdr->header Lbid->" << m_hdrLbid <<" count->" << arrayCount <<endl;

        arrayCount++;
        //cout << "RID = " << (RID)m_curIdxRidListHdr.firstIdxRidListEntry.rid << endl;
      };
      //Check Header last entry's type 
      int type = m_curIdxRidListHdr.nextIdxRidListPtr.type;  
      switch (type) 
      {
           case LIST_RID_TYPE:// There is a row id here, Check!	               
             ridArray[arrayCount]=(RID)m_curIdxRidListHdr.nextIdxRidListPtr.llp;
             //cout<<"arrayCount->" << arrayCount << "rid->" << ridArray[arrayCount]<<endl;
             arrayCount++;	
             //cout << "RID = " << (RID)m_curIdxRidListHdr.nextIdxRidListPtr.llp << endl;
             
             size = arrayCount;
             return NO_ERROR;      
           case LIST_SUBBLOCK_TYPE://Not found in header, so go to the sub-block 
               //get thelbid sbid and entry out from the header last entry
     
   m_lbid=((IdxEmptyListEntry*)&(m_curIdxRidListHdr.nextIdxRidListPtr))->fbo;
   m_sbid=((IdxEmptyListEntry*)&(m_curIdxRidListHdr.nextIdxRidListPtr))->sbid;
   m_entry=((IdxEmptyListEntry*)&(m_curIdxRidListHdr.nextIdxRidListPtr))->entry;
          m_curType = type;
          //Read the pointer entry at LIST_SUB_LLP_POS location
          IdxRidListPtr *lastIdxRidListPtr;
          IdxRidListEntry rowIdArray[MAX_BLOCK_ENTRY];                   
          memset(rowIdArray,0,BYTE_PER_BLOCK);
          int  pos =0, totalbytes=0;
          pos = LIST_SUB_LLP_POS;
          totalbytes = SUBBLOCK_TOTAL_BYTES;
          
          if (m_lbid!=m_hdrLbid)
          {
              rc = readSubBlockEntry(cb, &m_curBlock, m_lbid, 
                                     m_sbid, 0, totalbytes, 
                                     rowIdArray);
              m_curBlock.lbid = m_lbid;
              m_curBlock.state = BLK_READ;
              m_curBlock.dirty = true;
          }
          else
             getSubBlockEntry(m_hdrBlock.data, m_sbid, 
                                0, totalbytes, 
                                rowIdArray );
          int type, count;
               
          lastIdxRidListPtr =(IdxRidListPtr *) &rowIdArray[pos];
          type  = lastIdxRidListPtr->type;
          count = lastIdxRidListPtr->count;

          //cout << "count->" << count << endl;
          //type should be LIST_BLOCK_TYPE from now on
          for (int i=0; i<count; i++)
          {
            ridArray[arrayCount]= (RID)(rowIdArray[i].rid); 
            //cout << "RID =" << (RID)(rowIdArray[i].rid) << endl;
            //cout<<"arrayCount->" << arrayCount << "rid->" << ridArray[arrayCount]<<endl;
            arrayCount++;
          }   
          //cout << "    Lbid->" << m_lbid ;
          //cout << "    count->" << count << endl;    
          m_lbid = ((IdxEmptyListEntry*)lastIdxRidListPtr)->fbo;
          
          while  (type ==LIST_BLOCK_TYPE)
          {
              //cout << "    Lbid->" << m_lbid ;
 
              pos = LIST_BLOCK_LLP_POS;
              totalbytes = BYTE_PER_BLOCK;
              rc = readSubBlockEntry(cb, &m_curBlock, m_lbid, 0 , 0, 
                                     totalbytes, rowIdArray);
              m_curBlock.lbid = m_lbid;
              m_curBlock.state = BLK_READ;             
              m_curBlock.dirty = true;
              
              if (!m_useNarray)  
              {                   
                    lastIdxRidListPtr =(IdxRidListPtr *) &rowIdArray[pos];
                    type  = lastIdxRidListPtr->type;
                    count = lastIdxRidListPtr->count;
              }
              else
              {
                     nextIdxListPtr = (IdxRidNextListPtr *)&rowIdArray[pos];
                     type  = nextIdxListPtr->type;
                     count = nextIdxListPtr->count;
              }
              
              //cout << "    count->" << count << endl; 
              for (int i=0; i<count; i++)
              {
                ridArray[arrayCount]=(RID)(rowIdArray[i].rid) ;
                //cout << "RID =" << (RID)(rowIdArray[i].rid) << endl;  
                //cout<<"arrayCount->" << arrayCount << "rid->" << ridArray[arrayCount]<<endl; 
                arrayCount++;             
              } 
              if (type ==LIST_BLOCK_TYPE)
              {             
                if (m_useNarray) 
                  m_lbid     = nextIdxListPtr->nextLbid;
                else
                  m_lbid     = ((IdxEmptyListEntry*)lastIdxRidListPtr)->fbo;
              }
              
           }//end while         
      };//end of switch
      size = arrayCount;
     return rc;       
    }//end getRIDArrayFromListHdr
  
     const int    IndexList::getRIDArrayFromListHdrNarray(FILE* pFile, uint64_t& key,
                                    IdxEmptyListEntry* curIdxRidListHdrPtr,
                                    RID* ridArray, int& size, bool flag)
     {
      int rc=NO_ERROR;    
      IdxRidNextListPtr *nextIdxListPtr;
      int  pos =0, totalbytes=0;
      IdxRidListPtr *lastIdxRidListPtr;
      IdxRidListEntry rowIdArray[MAX_BLOCK_ENTRY*10]; 
      int type=0, count=0;
               
  
                
      m_pFile = pFile;
      CommBlock cb;
      cb.file.oid = m_oid;
      cb.file.pFile = m_pFile;
      if (flag)
      {
        rc = getHdrInfo(curIdxRidListHdrPtr);
        if (m_curIdxRidListHdr.idxRidListSize.size==0)
          {size =0; return NO_ERROR;}
        
        if (key!= m_curIdxRidListHdr.key) 
        {
           return ERR_IDX_LIST_WRONG_KEY;
        }  
        // cout << "IndexList::getRIDArrayFromListHdr->KEY ------>" << key << endl;
        //Check the first row location, 3rd enty
        if (m_curIdxRidListHdr.firstIdxRidListEntry.type==(int)LIST_RID_TYPE)
        {        
           ridArray[size]= (RID)m_curIdxRidListHdr.firstIdxRidListEntry.rid;
           //cout<<" IndexList::getRIDArrayFromListHdr->header Lbid->" << m_hdrLbid <<" count->" << arrayCount <<endl;

           size++;
           //cout << "RID = " << (RID)m_curIdxRidListHdr.firstIdxRidListEntry.rid << endl;
        };
        //Check Header last entry's type 
        int type = m_curIdxRidListHdr.nextIdxRidListPtr.type;  
        switch (type) 
        {
           case LIST_RID_TYPE:// There is a row id here, Check!	               
             ridArray[size]=(RID)m_curIdxRidListHdr.nextIdxRidListPtr.llp;
             //cout<<"arrayCount->" << arrayCount << "rid->" << ridArray[arrayCount]<<endl;
             size++;	
             //cout << "RID = " << (RID)m_curIdxRidListHdr.nextIdxRidListPtr.llp << endl;
             return NO_ERROR;      
           case LIST_SUBBLOCK_TYPE://Not found in header, so go to the sub-block 
               //get thelbid sbid and entry out from the header last entry
     
              m_lbid=((IdxEmptyListEntry*)&(m_curIdxRidListHdr.nextIdxRidListPtr))->fbo;
              m_sbid=((IdxEmptyListEntry*)&(m_curIdxRidListHdr.nextIdxRidListPtr))->sbid;
              m_entry=((IdxEmptyListEntry*)&(m_curIdxRidListHdr.nextIdxRidListPtr))->entry;
              m_curType = type;
              //Read the pointer entry at LIST_SUB_LLP_POS location
                                
              memset(rowIdArray,0,BYTE_PER_BLOCK);
              
              pos = LIST_SUB_LLP_POS;
              totalbytes = SUBBLOCK_TOTAL_BYTES;
          
             if (m_lbid!=m_hdrLbid)
             {
                rc = readSubBlockEntry(cb, &m_curBlock, m_lbid, 
                                     m_sbid, 0, totalbytes, 
                                     rowIdArray);
                m_curBlock.lbid = m_lbid;
                m_curBlock.state = BLK_READ;
                m_curBlock.dirty = true;
             }
             else
                getSubBlockEntry(m_hdrBlock.data, m_sbid, 
                                0, totalbytes, 
                                rowIdArray );
            lastIdxRidListPtr =(IdxRidListPtr *) &rowIdArray[pos];
             type  = lastIdxRidListPtr->type;
             count = lastIdxRidListPtr->count;

            //cout << "count->" << count << endl;
            //type should be LIST_BLOCK_TYPE from now on
            for (int i=0; i<count; i++)
            {
              ridArray[size]= (RID)(rowIdArray[i].rid); 
              //cout << "RID =" << (RID)(rowIdArray[i].rid) << endl;
              //cout<<"arrayCount->" << arrayCount << "rid->" << ridArray[arrayCount]<<endl;
              size++;
            }   
            //cout << "    Lbid->" << m_lbid ;
            //cout << "    count->" << count << endl;    
            m_lbid = ((IdxEmptyListEntry*)lastIdxRidListPtr)->fbo;
            m_curType = type;
          }//end of switch
        }//end if flag
        if (m_curType ==LIST_BLOCK_TYPE)
        {
              pos = LIST_BLOCK_LLP_POS;
              totalbytes = BYTE_PER_BLOCK;
              rc = readSubBlockEntry(cb, &m_curBlock, m_lbid, 0 , 0, 
                                     totalbytes, rowIdArray);
              m_curBlock.lbid = m_lbid;
              m_curBlock.state = BLK_READ;             
              m_curBlock.dirty = true;
              
              nextIdxListPtr = (IdxRidNextListPtr *)&rowIdArray[pos];
              type  = nextIdxListPtr->type;
              
              count = nextIdxListPtr->count;
              
              for (int i=0; i<count; i++)
              {
                ridArray[size]=(RID)(rowIdArray[i].rid) ;
                size++;             
              } 
              IdxRidListArrayPtr idxRidListArrayPtr;
              int curLevel = 0, curCount=0;
        
              memset(&idxRidListArrayPtr, 0, sizeof(idxRidListArrayPtr));
              getSubBlockEntry(m_curBlock.data, 0, 
                         BEGIN_LIST_BLK_LLP_POS, 
                         LIST_BLK_LLP_ENTRY_WIDTH, 
                         &idxRidListArrayPtr);
              curLevel     = idxRidListArrayPtr.nextIdxListPtr.curLevel;
              curCount     = idxRidListArrayPtr.nextIdxListPtr.count;
              
              for (int i=0; i<TOTAL_NUM_ARRAY_PTR; i++)
              {
                m_lbid = idxRidListArrayPtr.childIdxRidListPtr[i].childLbid;
                int type = idxRidListArrayPtr.childIdxRidListPtr[i].type;
                
                if ((m_lbid != (uint64_t)INVALID_LBID) &&(type == LIST_BLOCK_TYPE))
                {
                     m_curType=LIST_BLOCK_TYPE;
                     getRIDArrayFromListHdrNarray(pFile,key,curIdxRidListHdrPtr,                                    
                                    ridArray, size, false);
                }
                    
              }
              
         }//end if block        
      return rc;       
    }//end getRIDArrayFromListHdrNarray
    
  
    /*---------------------------------------------------------------------
     * Description:  Output the info of this company to an abstract stream
     *
     * Input:        None
     *
     * Output:       None
     *
     * Comments:     None
     *
     * Return Code:  the abstract output stream
     *------------------------------------------------------------------*/
     ostream& operator<<(ostream& os, IndexList& rhs)
     {
            os   << rhs.m_curIdxRidListHdr.idxRidListSize.size << "\t"
	                << rhs.m_curIdxRidListHdr.key << "\t" 
		               << rhs.m_curIdxRidListHdr.firstIdxRidListEntry.type << "\t"
		               << rhs.m_curIdxRidListHdr.firstIdxRidListEntry.rid << "\t"
		               << rhs.m_curIdxRidListHdr.nextIdxRidListPtr.type << "\t"
		               << rhs.m_curIdxRidListHdr.nextIdxRidListPtr.llp << "\t"
		               <<endl;
	           return os;
    }
   /************************************************
    * Description:
    * Find a entry for the given rowId and Key 
    * Converted                         
    * input
    *     pFile       -- File Handler     
    *     rowId       -- row id
    *     key         -- value    
    *     curIdxRidListHdrPtr - point to the header 
    * output     
    *     lbid       -- File block id
    *     sbid      -- Sub Block id
    *     entry     -- Entry id
    *   
    *     
    * return value
    *        true --found
    *        false--not found                 
    ************************************************/                
    bool IndexList::findRowId(FILE* pFile, const RID& rowId, const uint64_t& key,
                              IdxEmptyListEntry* curIdxRidListHdrPtr,
                              uint64_t& lbid, int& sbid, int& entry)
    {
      bool found = false;
      int rc; 
  
      m_pFile = pFile;          
      rc = getHdrInfo(curIdxRidListHdrPtr);
      if (key!= m_curIdxRidListHdr.key) 
      {
         return false;
      }
      found = findRowId(rowId, key, lbid, sbid, entry);                                  
      return found;
    } 
    
   /************************************************
    * Description:
    * Find a entry for the given rowId and Key
    * Converted                          
    * input
    *     pFile       -- File Handler     
    *     rowId       -- row id
    *     key         -- value    
    *     curIdxRidListHdrPtr - point to the header 
    * output --    
    *     lbid       -- File block id
    *     sbid      -- Sub Block id
    *     entry     -- Entry id   
    *     
    * return value
    *        true --found
    *        false--not found                 
    ************************************************/                
    bool IndexList::findRowId(const RID& rowId, const int64_t& key,                                  
                                          int64_t& lbid, int& sbid, int& entry)
    {
      bool found = false;
      int rc;
      RID savedRid;
      CommBlock cb;
      int count;
      uint64_t prevLbid;
      int prevType;

      cb.file.oid = m_oid;
      cb.file.pFile = m_pFile;  
      //Check the first row location, 3rd enty--0,1,2
      
      if (m_curIdxRidListHdr.firstIdxRidListEntry.type== (int)LIST_RID_TYPE)
      {        
             if (m_curIdxRidListHdr.firstIdxRidListEntry.rid == rowId)
             {
                   lbid   = m_hdrLbid;
                   sbid   = m_hdrSbid;
                   entry  = m_hdrEntry+2;
                   found  = true;
                   return found;
              }         
      }; //endif type=LIST_RID_TYPE

      //Check Header last entry's type  
      int type = m_curIdxRidListHdr.nextIdxRidListPtr.type; 
      int pos =0, totalbytes =0;
      switch (type) 
      {
         case LIST_NOT_USED_TYPE://Header is not full, no sub-block linked
                  //No RowId here then on
                  lbid=-1LL;
                  sbid =-1;
                  entry =-1;
                  found =false;
                  return found;  //not found 	         
          case LIST_RID_TYPE:// There is a row id here, Check!
                       
                 savedRid = (RID)m_curIdxRidListHdr.nextIdxRidListPtr.llp;	      
                 if (savedRid == rowId)
                 {
                            lbid   = m_hdrLbid;
                            sbid  = m_hdrSbid;
                            entry = m_hdrEntry+3;
                            found =true;
                            return found;
                 }
                 else
                 {
                              lbid=-1LL;
                              sbid = -1;
                              entry =-1;
                              found = false;
                              return found;
                 } 
          case LIST_SUBBLOCK_TYPE://Not found in header
            //get the lbid sbid and entry out from the header last entry
        
                   m_lbid  =((IdxEmptyListEntry*)
                      &(m_curIdxRidListHdr.nextIdxRidListPtr))->fbo;   
                   m_sbid =((IdxEmptyListEntry*)
                      &(m_curIdxRidListHdr.nextIdxRidListPtr))->sbid;
                   m_entry=((IdxEmptyListEntry*)
                      &(m_curIdxRidListHdr.nextIdxRidListPtr))->entry;  
                   
                   //Read the pointer entry at LIST_SUB_LLP_POS
                   //reserve enough space for rowIdArray
                   IdxRidListPtr *lastIdxRidListPtr;
                   IdxRidListEntry rowIdArray[MAX_BLOCK_ENTRY];                   
                   memset(rowIdArray, 0, BYTE_PER_BLOCK);
                   //first link 
                   pos = LIST_SUB_LLP_POS;
                   totalbytes = SUBBLOCK_TOTAL_BYTES;
                   
                   //check if the sub block is on the header block          
                   if (m_lbid != m_hdrLbid)
                   {
                     rc = readSubBlockEntry( cb, &m_curBlock, m_lbid, m_sbid, 0, 
                                                   totalbytes, rowIdArray);                                  
                     m_curBlock.dirty = true;
                     m_curBlock.state = BLK_READ;
                   }
                   else
                   {
                           getSubBlockEntry(m_hdrBlock.data, m_sbid, 
                                           0, totalbytes,  rowIdArray ); 
                                                      
                   }                               
                   lastIdxRidListPtr =(IdxRidListPtr *) &rowIdArray[pos]; 
         
                   prevLbid = m_lbid; //sub block
                   prevType = type; //sub block       
                   count = lastIdxRidListPtr->count;  //current count  
                   type  = lastIdxRidListPtr->type;    //block 
                   found = false;
                   //look inside the first sub block
                   for (int i=0; i<count; i++)
                  {
                       if (rowIdArray[i].rid == rowId)
                       {
                                found = true;
                                lbid = m_lbid;
                                sbid = m_sbid;
                                entry = i;
                                return found;               
                        }
                  }         
                  while  ((!found) &&(type==LIST_BLOCK_TYPE))
                  {//There are more to check on the next link          
                       m_lbid  = ((IdxEmptyListEntry*)lastIdxRidListPtr)->fbo;
                       m_sbid  = 0;
                       m_entry = 0;

                       pos        = LIST_BLOCK_LLP_POS;
                       totalbytes = BYTE_PER_BLOCK;
                       
                       if ((m_lbid !=m_hdrLbid) && (m_lbid != prevLbid))
                       { // the only case for block
                               rc = readSubBlockEntry( cb, &m_curBlock, m_lbid, 
                                                        m_sbid, 0, totalbytes, 
                                                        rowIdArray);
                               m_curBlock.dirty=true;
                       }                        
				                  	else
					                  {
							                       printf("error in findRowID\n");
							                       return false;
					                  }
                       prevType = type;                             
                       lastIdxRidListPtr=(IdxRidListPtr *) &rowIdArray[pos];
                       type  = lastIdxRidListPtr->type;
                       count = lastIdxRidListPtr->count;
                       found = false;
                       for (int i=0; i<count; i++)
                       {
                                   if (rowIdArray[i].rid == rowId)
                                  {
                                        found = true;
                                        lbid = m_lbid;
                                        sbid = m_sbid;
                                        entry = i;                  
                                        return found;               
                                  }
                       }//end for i   
                       prevLbid = m_lbid;                       
                 } //end while  
                 break;
        default:
                 printf ("FIND ROWID got no where, error out !! \n"); 
                 break;  
      }; //end switch
      lbid =INVALID_LBID;
      sbid =INVALID_NUM;
      entry=INVALID_NUM;
      found = false;
      return found;
    } //end function
    
 } //end of namespace    
    
    
