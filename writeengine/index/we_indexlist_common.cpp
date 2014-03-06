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
#include "we_indexlist.h"


using namespace std;

namespace WriteEngine
{  
    /****************************************************************
    * DESCRIPTION:
    * Private Function for getting the last Fbo on header
    * 
    ***************************************************************/        
    const int IndexList::init()                                        
    {
        memset( m_hdrBlock.data, 0, sizeof(m_hdrBlock.data));
        memset( m_curBlock.data, 0, sizeof(m_curBlock.data));
        memset( m_nextBlock.data, 0, sizeof(m_nextBlock.data));                                 
        memset( m_blockZero.data, 0, sizeof(m_blockZero.data));
        memset( m_parentBlock.data, 0, sizeof(m_parentBlock.data));
           
        memset( &m_curIdxRidListHdr, 0, LIST_HDR_SIZE );
        m_curIdxRidListHdr.key = INVALID_KEY;
        
        memset( &m_idxRidListArrayPtr, 0, sizeof(IdxRidListArrayPtr) );
        memset( &m_lastIdxRidListPtr, 0, sizeof(IdxRidListPtr) );
        
        m_hdrBlock.dirty    = false;      
        m_curBlock.dirty    = false;
        m_nextBlock.dirty   = false;
        m_blockZero.dirty   = false;
        m_parentBlock.dirty = false;

        m_hdrBlock.state    = BLK_INIT;   
        m_curBlock.state    = BLK_INIT;
        m_nextBlock.state   = BLK_INIT;
        m_blockZero.state   = BLK_INIT;
        m_parentBlock.state = BLK_INIT;

        m_hdrBlock.lbid     = INVALID_LBID;
        m_curBlock.lbid     = INVALID_LBID;
        m_nextBlock.lbid    = INVALID_LBID;
        m_blockZero.lbid    = INVALID_LBID;
        m_parentBlock.lbid  = INVALID_LBID;

        m_lbid        =INVALID_LBID;
        m_sbid        =INVALID_NUM;
        m_entry       =INVALID_NUM;

        m_hdrLbid     =INVALID_LBID; 
        m_hdrSbid     =INVALID_NUM;
        m_hdrEntry    =INVALID_NUM;

        m_nextLbid    =INVALID_LBID;
        m_nextSbid    =INVALID_NUM; 
        m_nextEntry   =INVALID_NUM; 
           
        m_parentLbid = INVALID_LBID;
      
        m_lastLbid     = INVALID_LBID;

        return NO_ERROR;
   }
   /****************************************************************
    * DESCRIPTION:
    * Private Function for getting the last Fbo on header
    * 
    ***************************************************************/    
    const int IndexList::initBlksGetHdrBlk()                                         
    {
        int rc = NO_ERROR;
        CommBlock cb;
        cb.file.oid   = m_oid;
        cb.file.pFile = m_pFile;
        memset( m_hdrBlock.data, 0, sizeof(m_hdrBlock.data));
        memset( m_curBlock.data, 0, sizeof(m_curBlock.data));
        memset( m_nextBlock.data, 0, sizeof(m_nextBlock.data));                                 
        memset( m_blockZero.data, 0, sizeof(m_blockZero.data));
        memset( m_parentBlock.data, 0, sizeof(m_curBlock.data));

        m_hdrBlock.dirty    = false;      
        m_curBlock.dirty    = false;
        m_nextBlock.dirty   = false;
        m_blockZero.dirty   = false;
        m_parentBlock.dirty = false;

        m_hdrBlock.state    = BLK_INIT;           
        m_curBlock.state    = BLK_INIT;
        m_nextBlock.state   = BLK_INIT;
        m_blockZero.state   = BLK_INIT;
        m_parentBlock.state = BLK_INIT;
        //Get the header info if the header exist already
        if (m_hdrLbid != (uint64_t)INVALID_LBID)
        {                     
          memset( &m_curIdxRidListHdr, 0, LIST_HDR_SIZE );
          rc =readSubBlockEntry(cb,&m_hdrBlock,m_hdrLbid, m_hdrSbid, m_hdrEntry,                                               
                               LIST_HDR_SIZE, &m_curIdxRidListHdr );         
          m_hdrBlock.dirty   = true; 
          m_hdrBlock.lbid    = m_hdrLbid;
          m_hdrBlock.state   = BLK_READ;            
        }
       return rc;
   }     
   /****************************************************************
    * DESCRIPTION:
    * Private Function for getting the last Fbo on header
    * 
    ***************************************************************/    
    const int IndexList::initGetHdr(const uint64_t &key, 
                                   IdxEmptyListEntry* curIdxRidListHdrPtr)                                         
    {
        int rc;
        CommBlock cb;
        cb.file.oid   = m_oid;
        cb.file.pFile = m_pFile;
                
        rc = init();
        m_hdrLbid     =curIdxRidListHdrPtr->fbo; 
        m_hdrSbid     =curIdxRidListHdrPtr->sbid;
        m_hdrEntry    =curIdxRidListHdrPtr->entry;                     
        memset( &m_curIdxRidListHdr, 0, LIST_HDR_SIZE );
        rc =readSubBlockEntry(cb, &m_hdrBlock,m_hdrLbid, m_hdrSbid, m_hdrEntry,                                               
                              LIST_HDR_SIZE, &m_curIdxRidListHdr );         
        m_hdrBlock.dirty   = true; 
        m_hdrBlock.lbid    = m_hdrLbid;
        m_hdrBlock.state   = BLK_READ;            
        if (m_curIdxRidListHdr.nextIdxRidListPtr.type == (int)LIST_SUBBLOCK_TYPE)
           rc = getLastLbid();
        else
           m_lastLbid = INVALID_LBID;
        return rc;
    }
       /****************************************************************
    * DESCRIPTION:
    * Private Function for getting the header
    * 
    ***************************************************************/ 
  	 const int  IndexList::getHdrInfo(IdxEmptyListEntry* curIdxRidListHdrPtr)
    {
       int rc = NO_ERROR; 
       CommBlock cb; 

       cb.file.oid   = m_oid;
       cb.file.pFile = m_pFile;
       //Get the Header block, sub-block and entry info from Index Tree 

       m_hdrLbid   = curIdxRidListHdrPtr->fbo;      
       m_hdrSbid   = curIdxRidListHdrPtr->sbid;
       m_hdrEntry  = curIdxRidListHdrPtr->entry; 
       
       memset(m_hdrBlock.data, 0, sizeof(m_hdrBlock.data));
       m_hdrBlock.dirty = false;
       m_hdrBlock.state = BLK_INIT;
       m_hdrBlock.no    = INVALID_NUM;
       m_hdrBlock.lbid  = INVALID_LBID;
       //header is 4 entries LIST_HDR_SIZE bytes   
       memset( &m_curIdxRidListHdr, 0, LIST_HDR_SIZE );
       //Get the old header out       
       rc = readDBFile( cb, &m_hdrBlock, m_hdrLbid );

       m_hdrBlock.dirty = true;
       m_hdrBlock.state = BLK_READ;
       m_hdrBlock.lbid  = m_hdrLbid;
       getSubBlockEntry( m_hdrBlock.data, m_hdrSbid, m_hdrEntry, LIST_HDR_SIZE, 
                                  &m_curIdxRidListHdr ); 
       if (m_curIdxRidListHdr.nextIdxRidListPtr.type == (int)LIST_SUBBLOCK_TYPE)
           rc = getLastLbid();
       else
           m_lastLbid = INVALID_LBID;
       return rc;
    }
   /****************************************************************
    * DESCRIPTION:
    * Private Function for getting a segment for LIST
    * 
    ***************************************************************/         
    const int  IndexList::resetBlk(DataBlock* dataBlk)
    {
        memset( dataBlk->data, 0, sizeof(dataBlk->data));
        dataBlk->dirty = false;
        dataBlk->state = BLK_INIT;
        dataBlk->lbid  = INVALID_LBID;
        return NO_ERROR;
    }
   /****************************************************************
    * DESCRIPTION:
    * Private Function for getting block zero
    * either it is 0 or get it from BRM    
    * 
    ***************************************************************/         
    const int  IndexList::resetBlkZero(uint64_t& lbid0)
    {
        int rc = NO_ERROR;
        CommBlock cb;
        cb.file.oid   = m_oid;
        cb.file.pFile = m_pFile;
        
        memset( m_blockZero.data, 0, sizeof(m_blockZero.data));        
        m_blockZero.dirty = false;
        m_blockZero.state = BLK_INIT;
        
#ifdef BROKEN_BY_MULTIPLE_FILES_PER_OID
        rc = BRMWrapper::getInstance()->getBrmInfo( m_oid, 0, lbid0 );
#endif
        if (rc != NO_ERROR)
           return rc;
        rc = readDBFile( cb, m_blockZero.data, lbid0 );
        m_blockZero.lbid = lbid0; 
        m_blockZero.state = BLK_READ;
        
        return rc;
    } 
   /****************************************************************
    * DESCRIPTION:
    * Private Function for writing block zero
    * 
    ***************************************************************/         
    const int  IndexList::writeBlkZero(uint64_t& lbid0)
    { 
      int rc;
      CommBlock cb;
      
      cb.file.oid   = m_oid;
      cb.file.pFile = m_pFile;
      rc = writeDBFile( cb, m_blockZero.data, lbid0 ); 
      memset( m_blockZero.data, 0, sizeof(m_blockZero.data));
      m_blockZero.dirty = false;
      m_blockZero.state = BLK_INIT;
      return rc;
    } 
   /****************************************************************
    * DESCRIPTION:
    * Private Function for getting a segment for LIST
    * 
    ***************************************************************/         
    const int  IndexList::getSegment( FILE* pFile, 
                                      const IdxTreeGroupType segmentType, 
                                      IdxEmptyListEntry* assignPtr )
    {
      int rc = ERR_IDX_LIST_GET_SEGMT;
      CommBlock cb;
      uint64_t lbid0; 
      
      m_pFile = pFile;
      cb.file.oid   = m_oid;
      cb.file.pFile = m_pFile;
      /*
      DataBlock tmpBlock;
      IdxEmptyListEntry tmpEntry;
      memset(tmpBlock.data,0, 8192);
      rc = readSubBlockEntry( pFile, &tmpBlock, 1492798, 31, 31, 8, &tmpEntry );
      cout << "common273->tmpEntry.fbo=" << tmpEntry.fbo << " tmpEntry.sbid=" 
           << tmpEntry.sbid <<  " tmpEntry.entry=" << tmpEntry.entry << endl;
       */
      if (m_hdrLbid != INVALID_LBID)
      {
         setSubBlockEntry(m_hdrBlock.data, m_hdrSbid, 
                          m_hdrEntry, LIST_HDR_SIZE, 
                          &m_curIdxRidListHdr );
         m_hdrBlock.state = BLK_WRITE;
      }
      //write everything out before calling free space manager
      rc = updateIndexListWrite();     
      rc = resetBlkZero(lbid0);
      rc = m_freemgr.assignSegment( cb, &m_blockZero, LIST, segmentType,  
                                    assignPtr);
      if (rc!= NO_ERROR)
         return rc;
      rc = writeBlkZero(lbid0);
      if (segmentType==ENTRY_4)
      {
         m_hdrLbid = assignPtr->fbo;
         m_hdrSbid = assignPtr->sbid;
         m_hdrEntry = assignPtr->entry;
         return rc;
      }
      //get the header back for sure
      //recover other blocks as it goes
      rc = initBlksGetHdrBlk();
      return rc;   
    } 
   /****************************************************************
    * DESCRIPTION:
    * Private Function for releasing a segment for LIST
    * Only block can be released, not subblock or header    
    * 
    ***************************************************************/         
    const int  IndexList::releaseSegment()                                     
    {
       IdxEmptyListEntry      releasePtrEntry;
       int entryType = 0;
       int rc;
       uint64_t lbid0;
       CommBlock cb;
       cb.file.oid = m_oid;
       cb.file.pFile = m_pFile;
       
       memset(&releasePtrEntry,0, sizeof(IdxEmptyListEntry));
       releasePtrEntry.fbo   = m_lbid;
       releasePtrEntry.sbid  = 0;
       releasePtrEntry.entry = 0;
       releasePtrEntry.spare =0;       
       
       entryType =ENTRY_BLK;
       // The following is related to ptr
       releasePtrEntry.spare2=0 ;
       releasePtrEntry.type = EMPTY_PTR;
       releasePtrEntry.group = entryType;
       //release free manager
       rc = resetBlkZero(lbid0);
     
       rc = m_freemgr.releaseSegment( cb,&m_blockZero, LIST,
                                      (const IdxTreeGroupType)entryType, 
                                       &releasePtrEntry );
       rc = writeBlkZero(lbid0);
       return rc;
    }
    /****************************************************************
    * DESCRIPTION:
    * Private Function for setting the last Fbo on header
    * 
    ***************************************************************/    
    const int  IndexList::setLastLbid( uint64_t& lastLbid)
    {
       int rc = NO_ERROR;
       uint64_t lbid;
       int sbid,entry;
       IdxRidListPtr lastFboListPtr;
       
       if (m_curIdxRidListHdr.nextIdxRidListPtr.type != (int)LIST_SUBBLOCK_TYPE)
       {
            m_lastLbid = INVALID_LBID;
            return NO_ERROR;
       }
  
       if (m_curType == LIST_SUBBLOCK_TYPE)
       {
            m_lastLbid = INVALID_LBID;
            return NO_ERROR;
       }
       else
       {
              m_lastLbid = lastLbid;  
       }       
        
       rc = getSubBlk(lbid, sbid, entry);
       //First link 
       memset(&lastFboListPtr, 0, sizeof(lastFboListPtr));
       lastFboListPtr.type = LIST_BLOCK_TYPE ;
       ((IdxEmptyListEntry*)&lastFboListPtr)->fbo = m_lastLbid; 
       ((IdxEmptyListEntry*)&lastFboListPtr)->sbid = 0;
       ((IdxEmptyListEntry*)&lastFboListPtr)->entry = 0;  
       lastFboListPtr.spare = 0x0;     
       rc = setLastFboPtr(lbid, sbid, lastFboListPtr);
       return rc;
    }
   /****************************************************************
    * DESCRIPTION:
    * Private Function for setting the last Fbo on header
    * 
    ***************************************************************/    
    const int IndexList::setLastFboPtr(uint64_t& lbid, int& sbid, 
                                       IdxRidListPtr& lastFboListPtr)
    {
      int rc = NO_ERROR;
      CommBlock cb;
      DataBlock dataBlock;
      IdxRidListPtr oldFboListPtr;
      
      cb.file.oid = m_oid;
      cb.file.pFile = m_pFile;

      if ((m_hdrBlock.lbid == lbid) && (m_hdrBlock.state>= BLK_READ))
      {
          setSubBlockEntry(m_hdrBlock.data, sbid, LIST_LAST_LBID_POS, 
                           LIST_ENTRY_WIDTH, &lastFboListPtr);
          m_hdrBlock.state = BLK_WRITE;  
      }
      else  if ((m_curBlock.lbid == lbid) && (m_curBlock.state>= BLK_READ))
      {
          setSubBlockEntry(m_curBlock.data, sbid, LIST_LAST_LBID_POS, 
                           LIST_ENTRY_WIDTH, &lastFboListPtr);
          m_curBlock.state = BLK_WRITE;
      }
      else
      {
          memset( dataBlock.data, 0, sizeof(dataBlock.data));
          rc = readSubBlockEntry(cb, &dataBlock, lbid, sbid, LIST_LAST_LBID_POS,
                                 LIST_ENTRY_WIDTH, &oldFboListPtr);
          if (((IdxEmptyListEntry*)&oldFboListPtr)->fbo !=m_lastLbid)
          {
            setSubBlockEntry(dataBlock.data, sbid, LIST_LAST_LBID_POS,
                                 LIST_ENTRY_WIDTH, &lastFboListPtr);
            rc = writeDBFile(cb, dataBlock.data, lbid);
          }
          else
           return NO_ERROR;
      }
      return rc;
    }    
   /****************************************************************
    * DESCRIPTION:
    * Private Function for getting the last Fbo on header
    * 
    ***************************************************************/ 
    const int  IndexList::getLastLbid()
    {
        int rc = NO_ERROR;
        uint64_t lbid;
        int sbid,entry;
        IdxRidListPtr lastFboListPtr;
        if (m_curIdxRidListHdr.nextIdxRidListPtr.type !=(int)LIST_SUBBLOCK_TYPE)
        {
          m_lastLbid = INVALID_LBID;
          return NO_ERROR;
        }
        
        rc = getSubBlk(lbid, sbid, entry);
        
        //First link           
        rc = getLastFboPtr(lbid, sbid, lastFboListPtr);
        if (lastFboListPtr.type == (int)LIST_BLOCK_TYPE)
        {
            m_lastLbid = ((IdxEmptyListEntry*)&lastFboListPtr)->fbo;  
        }
        else
          m_lastLbid = INVALID_LBID;

        return rc;
    }
   /****************************************************************
    * DESCRIPTION:
    * Private Function for setting the last Fbo on header
    * 
    ***************************************************************/    
    const int  IndexList::getLastFboPtr(uint64_t& lbid, int& sbid, 
                                        IdxRidListPtr& lastFboListPtr)
    {
      int rc = NO_ERROR;
      CommBlock cb;
      DataBlock dataBlock;
  
      cb.file.oid = m_oid;
      cb.file.pFile = m_pFile;
    
      if ((m_hdrBlock.lbid == lbid) && (m_hdrBlock.state>= BLK_READ))
      {
          getSubBlockEntry(m_hdrBlock.data, sbid, LIST_LAST_LBID_POS, 
                           LIST_ENTRY_WIDTH, &lastFboListPtr); 
      } 
      else if ((m_curBlock.lbid == lbid) && (m_curBlock.state>= BLK_READ))
      {
          getSubBlockEntry(m_curBlock.data, sbid, LIST_LAST_LBID_POS, 
                           LIST_ENTRY_WIDTH, &lastFboListPtr); 
      } 
      else
      {
          memset( dataBlock.data, 0, sizeof(dataBlock.data));
          dataBlock.dirty = false;
          dataBlock.state = BLK_INIT;
          rc = readSubBlockEntry(cb, &dataBlock, lbid, sbid, LIST_LAST_LBID_POS,
                           LIST_ENTRY_WIDTH, &lastFboListPtr);
      }
      return rc;
    } 

   /****************************************************************
    * DESCRIPTION:
    * Private Function for getting the sub block
    * 
    ***************************************************************/ 
    const int  IndexList::getSubBlk()
    {
        int rc = NO_ERROR;
        uint64_t lbid;
        int sbid,entry;
        RID rowIdArray[ENTRY_PER_SUBBLOCK];
        DataBlock dataBlk;
        CommBlock cb; 

        cb.file.oid   = m_oid;
        cb.file.pFile = m_pFile;
        
        memset( dataBlk.data, 0, sizeof(dataBlk.data));
        rc = getSubBlk(lbid, sbid, entry);
        if (lbid != INVALID_LBID)
         rc = readSubBlockEntry(cb, &dataBlk, lbid, 
                               sbid, 0, BYTE_PER_SUBBLOCK, 
                               rowIdArray);
        return rc;
    }
       /****************************************************************
    * DESCRIPTION:
    * Private Function for getting the sub block
    * 
    ***************************************************************/ 
    const int  IndexList::getSubBlk(uint64_t& lbid, int& sbid, int& entry)
    {
        int rc = NO_ERROR;
       if (m_curIdxRidListHdr.nextIdxRidListPtr.type == (int)LIST_SUBBLOCK_TYPE)
       {
        
             lbid= ((IdxEmptyListEntry*)
                      &(m_curIdxRidListHdr.nextIdxRidListPtr))->fbo;
             sbid=((IdxEmptyListEntry*)
                      &(m_curIdxRidListHdr.nextIdxRidListPtr))->sbid;
             entry=((IdxEmptyListEntry*)
                      &(m_curIdxRidListHdr.nextIdxRidListPtr))->entry;
       }
       else
       {
            lbid = INVALID_LBID;
            sbid = INVALID_NUM;
            entry= INVALID_NUM;
            return ERR_IDX_LIST_GET_SUB_BLK;
       }     

        return rc;
    }
   /****************************************************************
    * DESCRIPTION:
    * Private Function for getting a particular block
    * 
    ***************************************************************/ 
    const int  IndexList::getBlk(uint64_t& lbid)
    {
        int rc = NO_ERROR;
        RID rowIdArray[MAX_BLOCK_ENTRY];
        DataBlock dataBlk;
        CommBlock cb; 
        memset( dataBlk.data, 0, sizeof(dataBlk.data));
        cb.file.oid   = m_oid;
        cb.file.pFile = m_pFile;
    
        rc = readSubBlockEntry(cb, &dataBlk, lbid, 
                               0, 0, BYTE_PER_BLOCK, 
                               rowIdArray);
        return rc;
    }

   /****************************************************************
    * DESCRIPTION:
    * Update Write in one call
    * No Change    
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/   
    const int IndexList::updateIndexListWrite()
    {
      int rc = NO_ERROR;  
      CommBlock cb;
      cb.file.oid   = m_oid;
      cb.file.pFile = m_pFile;
      //Now write in one place
      if (m_hdrBlock.state == BLK_WRITE)   
      {//A
                rc = writeDBFile( cb, m_hdrBlock.data,   m_hdrLbid );
                if (rc!= NO_ERROR)
                       return rc;
                m_hdrBlock.state = BLK_READ;        
      }
      if (m_curBlock.state == BLK_WRITE)   
      {//B
                rc = writeDBFile( cb, m_curBlock.data, m_lbid );
                if (rc!= NO_ERROR)
                    return rc;
                m_curBlock.state = BLK_READ;
      }
      if (m_parentBlock.state == BLK_WRITE)
      {
          rc = writeDBFile( cb, m_parentBlock.data,   m_parentLbid );
          if (rc!= NO_ERROR)
            return rc;
          m_parentBlock.state = BLK_READ;
      }
      if (m_nextBlock.state == BLK_WRITE) 
      {            
          rc = writeDBFile( cb, m_nextBlock.data, m_nextLbid );
          if (rc!= NO_ERROR)
             return rc;
          m_nextBlock.state =BLK_READ;
      }
      return rc;
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
    const int  IndexList::findFirstBlk(FILE* pFile, const uint64_t& key,
                                 IdxEmptyListEntry* curIdxRidListHdrPtr,
                                 uint64_t& lbid)
    {
      int rc; 
      CommBlock cb;
      int count;
         
      cb.file.oid = m_oid;     
      cb.file.pFile = pFile;
      
      m_pFile = pFile;          
      rc = getHdrInfo(curIdxRidListHdrPtr);
      if (key!= m_curIdxRidListHdr.key) 
      {         
         return ERR_IDX_LIST_INVALID_KEY;;
      }
      int type = m_curIdxRidListHdr.nextIdxRidListPtr.type; 
      switch (type) 
      {
         case LIST_NOT_USED_TYPE://Header is not full, no sub-block linked
              lbid = INVALID_LBID;
              return NO_ERROR;  //not found 	         
         case LIST_RID_TYPE:// There is a row id here, Check!
              lbid = INVALID_LBID;
              return NO_ERROR;  //not found 	 
         case LIST_SUBBLOCK_TYPE://Not found in header
            //get the lbid sbid and entry out from the header last entry
              rc = getSubBlk(m_lbid, m_sbid, m_entry );
              m_curType = type; 
                      
              if (m_lbid != m_hdrLbid)
                      rc =readCurBlk();      
              rc = getNextInfoFromBlk(m_lastIdxRidListPtr);  
              count = m_lastIdxRidListPtr.count;  //current count  
              type  = m_lastIdxRidListPtr.type;    //block type

              if (type==LIST_BLOCK_TYPE)
                   lbid  = ((IdxEmptyListEntry*)&m_lastIdxRidListPtr)->fbo;
              else
                   lbid = INVALID_LBID;
              
              return NO_ERROR;
              break;
        default:
              //printf ("FIND FIRST BLOCK got no where, error out !! \n"); 
              break;  
      }; //end switch
      lbid =INVALID_LBID;
      return NO_ERROR;
    } //end function

   /****************************************************************
    * DESCRIPTION:
    * Private Function for setting the last Fbo on header
    * 
    ***************************************************************/    
    const int  IndexList::readCurBlk()
    {
         int rc = NO_ERROR;
         CommBlock cb;
         
         if (m_lbid == m_hdrLbid)  
           return NO_ERROR;
                   
         cb.file.oid = m_oid;     
         cb.file.pFile = m_pFile;
         
         if ((m_curBlock.lbid == m_lbid) && (m_curBlock.state != BLK_INIT))
           return NO_ERROR;
           
         if (m_curBlock.state == BLK_WRITE)
           rc = writeCurBlk();

         if ((m_curBlock.state == BLK_INIT) || (m_curBlock.lbid != m_lbid))
         {
            memset(m_curBlock.data, 0, sizeof(m_curBlock.data ) );
            rc = readDBFile(cb, m_curBlock.data, m_lbid);
            if (rc != NO_ERROR)
              return rc; 
            m_curBlock.dirty = true;
            m_curBlock.lbid = m_lbid;
            m_curBlock.state = BLK_READ; 
         }
         return rc;     
    }
   /****************************************************************
    * DESCRIPTION:
    * Private Function for setting the last Fbo on header
    * 
    ***************************************************************/    
    const int  IndexList::writeCurBlk()
    {
         int rc;
         CommBlock cb;
         if (m_curBlock.state == BLK_WRITE)
         {
           cb.file.oid = m_oid;     
           cb.file.pFile = m_pFile; 
           rc = writeDBFile(cb, m_curBlock.data, m_curBlock.lbid ); 
           m_curBlock.state =BLK_READ;
           return rc;
         }
         else
           return NO_ERROR;   
    }
   /************************************************
    * No Change    
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/  
    const int  IndexList::updateLastPtrAndParent(const int lastCount)  
    { 
       int rc = NO_ERROR;
       rc = updateLastPtr(lastCount);
       rc = updateParent();
       return rc;
    }
   /************************************************
    * No Change    
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/  
    const int  IndexList::updateParent()  
    {
      int rc;
      if (m_useNarray)    
      { 
         IdxRidParentListPtr parentIdxListPtr;
         memset(&parentIdxListPtr, 0, sizeof(parentIdxListPtr));
         parentIdxListPtr.parentLbid = (uint64_t)INVALID_LBID;
      
         rc = getParentInfoFromArray(parentIdxListPtr);
         m_parentLbid = (uint64_t)parentIdxListPtr.parentLbid;
         //If this block has no parent, then it is the first block
         if ((m_parentLbid<= (uint64_t)0)|| (m_parentLbid == (uint64_t)INVALID_LBID))
            m_parentLbid = m_lbid; //It is truly itself
         //The previous lbid m_lbid is full, so we have to go to nextLbid 
         // and register it to the parent block
         rc= updateParentStatus(m_nextLbid); 
         return rc; 
      } 
      else
        return NO_ERROR;  
    }
   /************************************************
    * No Change    
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/  
    const int  IndexList::updateLastPtr(const int lastCount)  
    { 
         int rc;
         if (m_curBlock.state == BLK_INIT)
              readCurBlk();    
         if (m_useNarray)
         {
            rc = setCurBlkNextPtr(m_nextLbid, lastCount);
            rc = writeCurBlk();            
         }
         else
         {
            ((IdxEmptyListEntry*)&m_lastIdxRidListPtr)->fbo  = m_nextLbid;
            ((IdxEmptyListEntry*)&m_lastIdxRidListPtr)->sbid = 0;
            ((IdxEmptyListEntry*)&m_lastIdxRidListPtr)->entry= 0;
            m_lastIdxRidListPtr.type= LIST_BLOCK_TYPE;
            m_lastIdxRidListPtr.spare=0x0;
            m_lastIdxRidListPtr.count = lastCount; 
                         
            setSubBlockEntry( m_curBlock.data, m_sbid, 
                              LIST_BLOCK_LLP_POS, LIST_ENTRY_WIDTH , 
                              &m_lastIdxRidListPtr );
            m_curBlock.state = BLK_WRITE;
            rc = writeCurBlk();
         } 
         return rc;  
    }            
         
   /************************************************
    * No Change    
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/  
    const int IndexList::addRidInBlk( const RID& newRid)
    {
      int maxCount, count=0;
      int rc = NO_ERROR;
      CommBlock cb;

      cb.file.oid = m_oid;
      cb.file.pFile = m_pFile;
      if (m_useNarray)
            maxCount = MAX_BLK_NARRAY_RID_CNT;
      else
            maxCount = MAX_BLK_RID_CNT;
      //Find the last block that has a space or 
      //No next block linked
      rc = findLastBlk(count);   
      if ((count ==maxCount) && (m_nextType==LIST_SIZE_TYPE))
      {//Full, also need a new segment
         IdxEmptyListEntry newIdxListEntryPtr;
         m_segType = LIST_BLOCK_TYPE;
         rc = getSegment( m_pFile,  ENTRY_BLK, &newIdxListEntryPtr); 
         m_nextLbid = ((IdxEmptyListEntry*)&newIdxListEntryPtr)->fbo;
         m_nextType = m_segType;
         m_lastIdxRidListPtr.llp=((IdxRidListPtr*)&newIdxListEntryPtr)->llp;
         rc = updateLastPtrAndParent(count);
         //the new block for insertion and count record
         m_lbid  = m_nextLbid;  
         m_sbid  = 0;
         m_entry = 0;
         m_curType = m_nextType;

         rc = readCurBlk();
         //free manager puts bad entry type at the last entry for new block
         //clean it up!
         IdxRidListPtr idxRidListPtr;
         memset(&idxRidListPtr, 0, sizeof(idxRidListPtr));         
         rc = setNextInfoFromBlk( idxRidListPtr);
         
         if (m_useNarray)
         {      
           rc = initCurBlock();
         }
         //Set the count to the beginning
         count = 0;
      }//end if FULL get new segment
      // insert in the current block at the location
      if (m_lastLbid != m_lbid)
      {
              rc = setLastLbid(m_lbid);  
      } 
      rc = insertRid(newRid, count);
      rc = updateCurCount();           
      rc = updateHdrCount();
      return rc;
   }
      /************************************************
    * No Change    
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/     
    const int IndexList::findLastBlk(int& count)
    {
       int maxCount;
       int rc ;
       
       if (m_useNarray)
         maxCount = MAX_BLK_NARRAY_RID_CNT;
       else
         maxCount = MAX_BLK_RID_CNT;
        
       rc = readCurBlk();
       rc = getNextInfo(count);
       while ((count == maxCount) &&(m_nextType==LIST_BLOCK_TYPE))
       { //current is a Full link, No space to insert, go to the next one             
                m_lbid     = m_nextLbid;
                m_sbid     = 0;
                m_entry    =0;
                rc = readCurBlk();
                rc = getNextInfo(count);
       }//end of while
       return rc;
     }
   /************************************************
    * No Change    
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/ 
    const int IndexList::insertRid(const RID& newRid, int& pos)
    {
       int rc = NO_ERROR;     
       IdxRidListEntry idxRidListEntry;  
       
       if (m_curType == LIST_BLOCK_TYPE)
       {
             m_sbid = 0;
             m_entry = 0;
       }
       memset(&idxRidListEntry, 0, LIST_ENTRY_WIDTH)  ;
       
       idxRidListEntry.type  =LIST_RID_TYPE;
       idxRidListEntry.spare =0;
       //cout << "line 910:newRid->" << newRid << endl;
       
       idxRidListEntry.rid   =newRid;  
       if (m_lbid != m_hdrLbid)  
       {
          rc = readCurBlk();     
          setSubBlockEntry( m_curBlock.data, m_sbid, pos, LIST_ENTRY_WIDTH, 
                            &idxRidListEntry );
          m_curBlock.state = BLK_WRITE;
          
       }
       else
       {
          setSubBlockEntry( m_hdrBlock.data, m_sbid, pos, LIST_ENTRY_WIDTH, 
                            &idxRidListEntry ); 
          m_hdrBlock.state = BLK_WRITE;
       }
       return rc;
    } 
   /************************************************
    * No Change    
    * RETURN:
    *    success    - successfully created the index list header
    *    failure    - it did not create the index list header    
    ***********************************************************/  
    const int IndexList::updateCurCount(int frequency)
    {
         int rc = NO_ERROR;
         int pos = 0;
         
         if (m_curType == LIST_SUBBLOCK_TYPE)
             pos = LIST_SUB_LLP_POS;
         else if (m_curType == LIST_BLOCK_TYPE)
         {
             pos = LIST_BLOCK_LLP_POS;
             m_sbid = 0;
             m_entry = 0;
         }
         if ((m_useNarray) && (m_curType == LIST_BLOCK_TYPE))
             rc = updateCurCountInArray();
         else
         {
             rc = getNextInfoFromBlk(m_lastIdxRidListPtr);
             for (int i=0; i< frequency; i++)
               m_lastIdxRidListPtr.count++;
             
             if  (m_lbid==m_hdrLbid)
             {   
                setSubBlockEntry( m_hdrBlock.data, m_sbid, pos, LIST_ENTRY_WIDTH,
                                  &m_lastIdxRidListPtr );
                m_hdrBlock.state  = BLK_WRITE;
             }
             else
             {
                setSubBlockEntry(m_curBlock.data, m_sbid, pos, LIST_ENTRY_WIDTH,
                                 &m_lastIdxRidListPtr );
                m_curBlock.state =BLK_WRITE;                          
             };
         }
         return rc;
    }  
   /****************************************************************
    * DESCRIPTION:
    * Private Function for setting the last Fbo on header
    * 
    ***************************************************************/    
    const int  IndexList::getNextInfo(int& count)
    {
         int rc;
         if ((!m_useNarray) ||(m_curType == LIST_SUBBLOCK_TYPE ))
         {
                   rc = getNextInfoFromBlk(m_lastIdxRidListPtr);                         
                   count      = m_lastIdxRidListPtr.count;
                   m_nextType = m_lastIdxRidListPtr.type;
                   m_nextLbid = ((IdxEmptyListEntry*)&m_lastIdxRidListPtr)->fbo;
         }
         else
         {
                   rc = getNextInfoFromArray(m_nextIdxListPtr );
                   count = m_nextIdxListPtr.count; 
                   m_nextType = m_nextIdxListPtr.type;     
                   m_nextLbid = m_nextIdxListPtr.nextLbid;                   
         }
         return rc;
    }
   /****************************************************************
    * DESCRIPTION:
    * Private Function non-array
    * 
    ***************************************************************/    
    const int  IndexList::getNextInfoFromBlk( IdxRidListPtr& idxRidListPtr)
    {
      int rc = NO_ERROR;
      int pos = 0;
      memset(&idxRidListPtr, 0, sizeof(idxRidListPtr));
      if (m_curType == LIST_SUBBLOCK_TYPE)
          pos = LIST_SUB_LLP_POS;
      else if (m_curType == LIST_BLOCK_TYPE)
          pos = LIST_BLOCK_LLP_POS;
      
      if ((m_hdrBlock.lbid == m_lbid) && (m_hdrBlock.state>= BLK_READ))
          getSubBlockEntry(m_hdrBlock.data, m_sbid, pos, 
                           LIST_ENTRY_WIDTH, &idxRidListPtr);  
      else if ((m_curBlock.lbid == m_lbid) && (m_curBlock.state>= BLK_READ))
          getSubBlockEntry(m_curBlock.data, m_sbid, pos, 
                           LIST_ENTRY_WIDTH, &idxRidListPtr);
      else
           return ERR_IDX_LIST_WRONG_BLK;
      if ((idxRidListPtr.type!= 0)&&(idxRidListPtr.type!= 4)&&(idxRidListPtr.type!= 5))
      {
         //cout << "line 1028->m_lbid=" << m_lbid << " m_sbid =" << m_sbid <<  " m_entry" <<m_entry<< endl;
         //cout <<"line 1029->idxRidListPtr.type = " << idxRidListPtr.type << endl;
         memset(&idxRidListPtr, 0, sizeof(idxRidListPtr));
      }
           
      return rc;
    }       
   /****************************************************************
    * DESCRIPTION:
    * Private Function for setting the last Fbo on header
    * 
    ***************************************************************/    
    const int  IndexList::setNextInfoFromBlk( IdxRidListPtr& idxRidListPtr)
    {
      int rc = NO_ERROR;
      CommBlock cb;

      cb.file.oid = m_oid;
      cb.file.pFile = m_pFile;
      int pos = 0;
     
      if (m_curType == LIST_SUBBLOCK_TYPE)
          pos = LIST_SUB_LLP_POS;
      else if (m_curType == LIST_BLOCK_TYPE)
          pos = LIST_BLOCK_LLP_POS;
          
      if (m_lbid==m_hdrLbid)
      {//when sub == hdr
          setSubBlockEntry( m_hdrBlock.data, m_sbid, 
                            pos, LIST_ENTRY_WIDTH, &idxRidListPtr ); 
          m_hdrBlock.state =BLK_WRITE;                                                   
      }
      else
      {
          readCurBlk();
          if (m_lbid == m_curBlock.lbid)
          {
               setSubBlockEntry( m_curBlock.data, m_sbid, 
                                 pos, LIST_ENTRY_WIDTH, 
                                 &idxRidListPtr );
               rc = writeDBFile(cb, m_curBlock.data, m_lbid ); 
               m_curBlock.state =BLK_READ;
          }
          else
               return ERR_IDX_LIST_WRONG_LBID_WRITE;
       }                
       return rc;
    }  
   
}//end namespace  
