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
/*   we_indexlist.cpp                                                               */
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
    /**
     * Constructor
     */
    IndexList::IndexList()
    :m_oid((OID) INVALID_NUM), m_useNarray(true),
     m_curLevel(INVALID_NUM),m_curBlkPos(0),
     m_curLevelPos(INVALID_NUM)
     
    {
      m_freemgr.setDebugLevel(DEBUG_0);
      init();
    }; 
   /****************************************************************
    * DESCRIPTION:
    * Public Function for adding a header    
    * (1) Given a key value and a row ID,                
    * (2) Return a pointer for insertion into the correct position
    *     in the Index Tree List Pointer group
    * (3) A return code should indicate success or failur            
    * PARAMETERS:
    *    input 
    *        pFile      - File Handler
    *        rid        - Input row ID 
    *        key        - Input key value
    *    output 
    *        listHdrPtr - Output a pointer to the index list header
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    *
    *******************************************************************/   
    const int IndexList::addIndexListHdr( FILE* pFile, const RID& rowId, 
                                          const uint64_t& key, 
                                          IdxEmptyListEntry* newEmptyListPtr)
    {
         int rc;   
         CommBlock cb;
         m_pFile       = pFile;
         cb.file.oid   = m_oid;
         cb.file.pFile = m_pFile;    
         //Set up the header structure 
         //Initialize header blokcs
         rc = resetBlk(&m_hdrBlock);
         m_hdrLbid    = INVALID_LBID; 
         m_hdrSbid    = INVALID_NUM;
         m_hdrEntry   = INVALID_NUM;

         //Initialize the new Index List header to null 
         memset( &m_curIdxRidListHdr, 0, LIST_HDR_SIZE );
         //Assign the bit fields for the first entry in the Index List Header
         m_curIdxRidListHdr.idxRidListSize.type  = LIST_SIZE_TYPE;
         m_curIdxRidListHdr.idxRidListSize.spare = 0x0;
         m_curIdxRidListHdr.idxRidListSize.size  = 1;
         //Assign the bit fields for the second entry of the Index List Header
         m_curIdxRidListHdr.key = key;
         //Assign bit fields for the third entry of the Index List Header
         m_curIdxRidListHdr.firstIdxRidListEntry.type  =LIST_RID_TYPE;
         m_curIdxRidListHdr.firstIdxRidListEntry.spare =0x0;
         m_curIdxRidListHdr.firstIdxRidListEntry.rid   =rowId ;
         //Assign bit fields for the fourth entry of the Index List Header
         m_curIdxRidListHdr.nextIdxRidListPtr.type  = LIST_NOT_USED_TYPE;
         m_curIdxRidListHdr.nextIdxRidListPtr.spare =0x0;
         m_curIdxRidListHdr.nextIdxRidListPtr.llp   =0x0; 
         /* Get assigned space for the header from free manager    
          * Get the new block for the new idx list header
          * The header needs LIST_HDR_SIZE bytes
          */
         rc = getSegment(pFile, ENTRY_4, newEmptyListPtr);
         if (rc != NO_ERROR)
                  return rc;
         m_hdrLbid   = newEmptyListPtr->fbo;
         m_hdrSbid   = newEmptyListPtr->sbid;
         m_hdrEntry  = newEmptyListPtr->entry;
      
         //Write Index List Header to the file block
         //Write LIST_HDR_SIZE bytes in one time.  
          
         rc = readDBFile( cb, m_hdrBlock.data, m_hdrLbid );  
         rc = writeSubBlockEntry( cb, &m_hdrBlock,  m_hdrLbid, m_hdrSbid, 
                                  m_hdrEntry,LIST_HDR_SIZE, &m_curIdxRidListHdr );
         if (rc!= NO_ERROR)
         {
             return rc;
         } 
         //Wrote Header Block Out already, Start Over next time
         //Update the flags to indicate there is data on the header block
         m_hdrBlock.dirty = true;            
         m_hdrBlock.lbid  = m_hdrLbid; 
         m_hdrBlock.state = BLK_READ;
         m_lastLbid = INVALID_LBID;
         //DONE 
         return rc;    
   };
   /****************************************************************
    * DESCRIPTION:
    *
    *                 
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/   
    const int IndexList::updateIndexList(FILE* pFile, const RID& newRid, 
                                         const uint64_t& key, 
                                         IdxEmptyListEntry* curIdxRidListHdrPtr)
    {      
         int rc; 
         m_pFile = pFile; 
         //Initialization
         if ( (key!= m_curIdxRidListHdr.key) || (m_hdrBlock.state==BLK_INIT))
         {
           rc = initGetHdr(key, curIdxRidListHdrPtr);
           if (key!= m_curIdxRidListHdr.key)
                  return ERR_IDX_LIST_INVALID_KEY;
         }
         rc = updateIndexList(newRid, key);
         if (rc!=NO_ERROR)
         {
              return rc;
         }
         //Write everything out
         rc = updateIndexListWrite();         
         return rc;
    };
   /****************************************************************
    * DESCRIPTION:
    * (0) THIS FUNCIION CAN ONLY BE CALLED WITH THE PUBLIC
    *
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/   
    const int IndexList::updateIndexList(const RID& newRid, const uint64_t& key)                                                                                   
    {      
       int rc = NO_ERROR;

       //m_lastLbid==0 or not determines if we can skip from the header,the first
       //subblock or go to the last inserted block
       if (m_lastLbid ==(uint64_t)INVALID_LBID)
       {
           rc = updateHdrSub(newRid, key);
       }
       else // get the lastLbid info from header
       {//m_lastLbid > 0, space is in some block now
           m_lbid  = m_lastLbid;
           m_sbid =  0;
           m_entry = 0;
           m_segType = LIST_BLOCK_TYPE;
           m_curType = LIST_BLOCK_TYPE;
           rc = addRidInBlk(newRid);       
       }
       return rc;
    };
  
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
    const int      IndexList::deleteIndexList( FILE* pFile, const RID& rowId, 
                        const uint64_t& key, IdxEmptyListEntry* curIdxRidListHdrPtr)
    {
      int rc =ERR_IDX_LIST_INVALID_DELETE;          
      m_pFile = pFile;
      
      getHdrInfo(curIdxRidListHdrPtr);
      if (key!= m_curIdxRidListHdr.key) 
      {
        memset( m_hdrBlock.data, 0, sizeof(m_hdrBlock.data));
        m_hdrBlock.dirty = false;
        m_hdrBlock.state = BLK_INIT;
        return ERR_IDX_LIST_INVALID_KEY;
      }                        
      rc = deleteIndexList(rowId,key);      
      return rc;
    }
    
   /************************************************
    * Description:
    * Converted - keep the first sub block    
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
    const int      IndexList::deleteIndexList( const RID& rowId,const uint64_t& key) 
    {
      int rc =ERR_IDX_LIST_INVALID_DELETE;    
      RID savedRid ;
      DataBlock prevDataBlock;
      
      CommBlock cb;
      cb.file.oid = m_oid;
      cb.file.pFile = m_pFile;
 
      //Check the first row location, 3rd entry
      //Because it may be deleted from the delete action
      //The header size cannot tell us the rowid size on header
      if (m_curIdxRidListHdr.firstIdxRidListEntry.type
                                     ==(int)LIST_RID_TYPE)
      {        
               if (m_curIdxRidListHdr.firstIdxRidListEntry.rid 
                      == rowId)
               {
                   m_curIdxRidListHdr.firstIdxRidListEntry.type 
                         =LIST_NOT_USED_TYPE; //not used type
                   m_curIdxRidListHdr.firstIdxRidListEntry.rid = 0;
                   m_curIdxRidListHdr.idxRidListSize.size--;
                   rc = writeSubBlockEntry(cb,&m_hdrBlock,m_hdrLbid,
                                                       m_hdrSbid, m_hdrEntry,
                                                        LIST_HDR_SIZE, 
                                                       &m_curIdxRidListHdr );  
                   memset(m_hdrBlock.data, 0, sizeof(m_hdrBlock.data));
                   m_hdrBlock.dirty = false; 
                   m_dLbid   = m_hdrLbid;
                   m_dSbid   = m_hdrSbid;
                   m_dEntry  = m_hdrEntry+2;
                   
                   return rc;
               }         
       };
       //Check Header last entry's type 
       int type = m_curIdxRidListHdr.nextIdxRidListPtr.type;    
       switch (type) 
       {
           case LIST_NOT_USED_TYPE://Header is not full, no sub-block linked
                  //No RowId here
                  memset(m_hdrBlock.data, 0, sizeof(m_hdrBlock.data));
                  m_hdrBlock.dirty = false; 
                  m_dLbid = -1LL;
                  m_dSbid = -1;
                  m_dEntry = -1;
                  return ERR_IDX_LIST_INVALID_DELETE;  //not found, failed   	         
           case LIST_RID_TYPE:// There is a row id here, Check!	               
                  savedRid = m_curIdxRidListHdr.nextIdxRidListPtr.llp;	      
                  if (savedRid == rowId)
                  {
                          m_curIdxRidListHdr.nextIdxRidListPtr.type 
                                       =LIST_NOT_USED_TYPE;
                          m_curIdxRidListHdr.nextIdxRidListPtr.llp = 0;
                          m_curIdxRidListHdr.idxRidListSize.size--;
                          rc = writeSubBlockEntry(cb, &m_hdrBlock,m_hdrLbid,
                                                  m_hdrSbid, m_hdrEntry, 
                                                  LIST_HDR_SIZE, 
                                                  &m_curIdxRidListHdr );           
                           m_hdrBlock.dirty = false;
                           memset(m_hdrBlock.data, 0, sizeof(m_hdrBlock.data));
                           m_dLbid = m_hdrLbid;
                           m_dSbid = m_hdrSbid;
                           m_dEntry = 3;
                                        
                           return rc;
                   } 
                   else 
                   {
                           m_hdrBlock.dirty = false;
                           memset(m_hdrBlock.data, 0, sizeof(m_hdrBlock.data));
                           m_dLbid = -1LL;
                           m_dSbid = -1;
                           m_dEntry = -1;
                           return ERR_IDX_LIST_INVALID_DELETE;
                   }
            case LIST_SUBBLOCK_TYPE://Not found in header, 
              rc = deleteInSub(rowId);
              if (rc == NO_ERROR)
                return rc;
              rc = deleteInBlock(rowId);
              return rc;                          
              break;
          default:
              break;       
        };//end of switch
        return ERR_IDX_LIST_INVALID_DELETE;
      }
 }
