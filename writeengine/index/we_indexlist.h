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
* $Id: we_indexlist.h 33 2006-07-19 09:18:27Z jhuang $
*
******************************************************************************************/
/** @file */

#ifndef _WE_IndexList_H_
#define _WE_IndexList_H_

#include <stdlib.h>
#include <sys/types.h>
#include <we_dbfileop.h>
#include <we_index.h>
#include <iostream>
#include <sstream>
#include <cstddef>
#include "we_freemgr.h"

/** Namespace WriteEngine */
namespace WriteEngine
{

class IndexList : public DbFileOp
{
public:
   /**
    * @brief Constructor
    */
   IndexList();
   //IndexList(FILE* pFile, IdxEmptyListEntry* newHeaderListPtr);
   /**
    * @brief Default Destructor
    */
   ~IndexList(){};

   /**
    * @brief Public index List related functions
    */
    
   /**
    * @brief Add a 4 bytes header for a rowid, key in the index list
    */
   const int  addIndexListHdr( FILE* pFile, const RID& listRid, const uint64_t& key, 
                                   IdxEmptyListEntry* newHeaderListPtr  );
                                                  
   const int  addIndexListHdr( CommBlock& cbList, const RID& listRid, const uint64_t& key, 
                                   IdxEmptyListEntry* newHeaderListPtr  )
      {  
         m_oid = cbList.file.oid;   m_pFile = cbList.file.pFile;       
         int rc = addIndexListHdr( m_pFile, listRid, key, newHeaderListPtr);
         return rc;
      }
    const int  addIndexListHdr( FILE* pFile, const RID* ridList, const int size, const uint64_t& key, 
                                   IdxEmptyListEntry* newHeaderListPtr  );
                                   
    const int  addIndexListHdr( CommBlock& cbList, RID* ridList, int& size, const uint64_t& key, 
                                   IdxEmptyListEntry* newHeaderListPtr  )
    {  
         m_oid = cbList.file.oid;   m_pFile = cbList.file.pFile;       
         int rc = addIndexListHdr( m_pFile, ridList, size, key, newHeaderListPtr);
         return rc;
    }
    const int  addIndexListHdr( CommBlock& cbList, const IdxMultiRid& multiRids,
                                const uint64_t& key, IdxEmptyListEntry* newHeaderListPtr)
    {
                m_oid = cbList.file.oid;   m_pFile = cbList.file.pFile;
                int size =  multiRids.totalRid;
                return addIndexListHdr( m_pFile, multiRids.ridArray, size,
                                        key, newHeaderListPtr  );

    }

   /**
    * @brief Update the indexlist when a new rowid an key inserted
    */
   const int  updateIndexList( FILE* pFile, const RID& listRid, const uint64_t& key, 
                                   IdxEmptyListEntry* oldHeaderListPtr  );
               
   const int  updateIndexList( CommBlock& cbList, const RID& listRid, const uint64_t& key, 
                                   IdxEmptyListEntry* oldHeaderListPtr  )
              {       
                m_oid = cbList.file.oid; m_pFile = cbList.file.pFile;                          
                
                int rc = updateIndexList( m_pFile, listRid, key, oldHeaderListPtr); 
                return rc;
              };
                
   const int  updateIndexList( FILE* pFile, const RID* ridList, const int size,const uint64_t& key, 
                                   IdxEmptyListEntry* oldHeaderListPtr  );
   const int  updateIndexList( CommBlock& cbList, const RID* ridList, const int size,const uint64_t& key, 
                                   IdxEmptyListEntry* oldHeaderListPtr  )
              {
                 m_oid = cbList.file.oid; m_pFile = cbList.file.pFile;                          
                
                 int rc = updateIndexList( m_pFile, ridList, size,key, oldHeaderListPtr); 
                 return rc;
              }
   const int  updateIndexList( CommBlock& cbList, const IdxMultiRid& multiRids,
                               const uint64_t& key, IdxEmptyListEntry* oldHeaderListPtr)
              {
                return updateIndexList( cbList, multiRids.ridArray, multiRids.totalRid,
                                        key, oldHeaderListPtr  );
              }

   /**
    * @brief Delete the rowid in the key indexlist
    */
   const int  deleteIndexList( FILE* pFile, const RID& listRid, const uint64_t& key, 
                               IdxEmptyListEntry* oldHeaderListPtr  );
               
   const int  deleteIndexList( CommBlock& cbList, const RID& listRid, const uint64_t& key, 
                               IdxEmptyListEntry* oldHeaderListPtr  )
              {
                m_oid = cbList.file.oid ; m_pFile = cbList.file.pFile;
                
                int rc = deleteIndexList( m_pFile, listRid, key, oldHeaderListPtr  );
                return rc;
              };  
                                                                    
   /**
    * @brief delete a row id from the key index list and return the location
    */  
    const int  deleteIndexList( FILE* pFile, const RID& listRid, const uint64_t& key, 
                               IdxEmptyListEntry* oldHeaderListPtr,
                               uint64_t& lbid, int& sbid, int& entry  );                              
                                
    const int  deleteIndexList( CommBlock& cbList, const RID& listRid, 
                                const uint64_t& key, 
                                IdxEmptyListEntry* oldHeaderListPtr,
                                uint64_t& lbid, int& sbid, int& entry  )
               {
                m_oid = cbList.file.oid ; m_pFile = cbList.file.pFile;                 
                int rc = deleteIndexList( m_pFile, listRid, key, 
                                          oldHeaderListPtr,
                                          lbid, sbid, entry);
                return rc;
               };
                                
   /**
    * @brief find a row id from the key index list and return the location
    */     
    bool       findRowId(FILE* pFile, const RID& rid, const uint64_t& key,
                         IdxEmptyListEntry* oldIdxRidListHdrPtr,
                         uint64_t& lbid, int& sbid, int& entry);
                          
    bool       findRowId(CommBlock& cbList, const RID& rid, const uint64_t& key,
                        IdxEmptyListEntry* oldIdxRidListHdrPtr,
                        uint64_t& lbid, int& sbid, int& entry)
               {      m_oid = cbList.file.oid ; m_pFile = cbList.file.pFile;
                                    
                      bool found =findRowId(m_pFile, rid, key, 
                                            oldIdxRidListHdrPtr,lbid, 
                                            sbid, entry);
                      return found;
               };
   /**
    * @brief get the total row ids or tokens from the index list header
    */                          
   const int       getRIDArrayFromListHdr(FILE* pFile, uint64_t& key,
                                     IdxEmptyListEntry* oldHeaderListPtr,
                                     RID* ridArrary, int& size); 
                                     
   const int       getRIDArrayFromListHdr(CommBlock& cbList, uint64_t& key,
                                     IdxEmptyListEntry* oldHeaderListPtr,
                                     RID* ridArrary, int& size)
                   { m_oid = cbList.file.oid ; m_pFile = cbList.file.pFile;
                     int rc = getRIDArrayFromListHdr(m_pFile, key,
                                     oldHeaderListPtr,
                                     ridArrary, size);
                     return rc;
                   };
  const int        getRIDArrayFromListHdrNarray(FILE* pFile, uint64_t& key,
                                    IdxEmptyListEntry* curIdxRidListHdrPtr,
                                    RID* ridArray, int& size, bool flag);
  const int        getRIDArrayFromListHdrNarray(CommBlock& cbList, uint64_t& key,
                                     IdxEmptyListEntry* oldHeaderListPtr,
                                     RID* ridArrary, int& size, bool flag)
                   { m_oid = cbList.file.oid ; m_pFile = cbList.file.pFile;
                     int rc = getRIDArrayFromListHdrNarray(m_pFile, key,
                                     oldHeaderListPtr,
                                     ridArrary, size, flag);
                     return rc;
                   };
                                    
              
   const int      init (CommBlock& cbList, const int& freemgr_type)
                  {  
                    m_oid = cbList.file.oid ; m_pFile = cbList.file.pFile;
                    int rc = m_freemgr.init(cbList,freemgr_type);  
                    return rc;
                  };
   const int      closeList(){int rc = updateIndexListWrite(); return rc;}
   void           setTransId( const TxnID txnId ) {FileOp::setTransId( txnId );m_freemgr.setTransId(txnId);}
   IdxRidListHdr  m_curIdxRidListHdr; /**@brief current list header */
   const int getHdrInfo(IdxEmptyListEntry* curIdxRidListHdrPtr);
   const int getHdrInfo(IdxEmptyListEntry* curIdxRidListHdrPtr, IdxRidListHdr* idxRidListHdr)
   { int rc= 0; rc =getHdrInfo(curIdxRidListHdrPtr); 
     memcpy(idxRidListHdr, &m_curIdxRidListHdr, LIST_HDR_SIZE); 
     return rc;
   }
   /**
    * @brief Timer functions
    */
   void           startTimer() { time( &m_startTime ); }
   void           stopTimer()  { time( &m_endTime );  m_totalTime = difftime( m_endTime, m_startTime); }
   double         getTotalRunTime() const { return m_totalTime; }
   void           setDebugLevel(const DebugLevel level){m_freemgr.setDebugLevel(level);}
   void           setUseSortFlag(const bool flag){m_useSortFlag = flag;}
   
   void           startfTimer(){ftime(&t_start);};
   void           stopfTimer(){ftime(&t_current);};
   int            getTotalfRunTime()
                  {int t_diff; 
                      t_diff= (int) (1000.0 * (t_current.time - t_start.time)+ (t_current.millitm - t_start.millitm));
                      return t_diff;}
   /**
    * @brief Private index List member functions
    */  
   const int  updateIndexListWrite();
   const int  init();
   const int  initBlksGetHdrBlk();
   const int  initGetHdr(const uint64_t &key, IdxEmptyListEntry* curIdxRidListHdrPtr);
   const int  resetBlk(DataBlock* dataBlk) ;
   const int  resetBlkZero(uint64_t& lbid0);
   const int  writeBlkZero(uint64_t& lbid0);

   const int  setLastLbid( uint64_t& lastLbid);
   const int  getLastLbid();
   
   const int  findLastBlk(int& count);
   const int  readCurBlk();
   const int  writeCurBlk();
   const int  addRidInBlk( const RID& newRid);
   const int  insertRid(const RID& newRid, int& pos);
   const int  updateCurCount(int frequency =1);
   const int  getNextInfo(int& count);
   const int  getNextInfoFromBlk( IdxRidListPtr& idxRidListPtr);
   const int  getNextInfoFromBlk(){return getNextInfoFromBlk(m_lastIdxRidListPtr); };
   const int  getLastFboPtr(uint64_t& lbid, int& sbid, IdxRidListPtr& lastFboListPtr);
   const int  setLastFboPtr(uint64_t& lbid, int& sbid, IdxRidListPtr& lastFboListPtr);
   const int  setNextInfoFromBlk( IdxRidListPtr& idxRidListPtr);
   const int  setNarray(bool flag){m_useNarray= flag; return NO_ERROR;};
   const int  findFirstBlk(FILE* pFile, const uint64_t& key,
                              IdxEmptyListEntry* curIdxRidListHdrPtr,
                              uint64_t& lbid);
   bool       getUseNarray(){return m_useNarray;};
   const int  printBlocks(uint64_t& lbid);
   const int  getBlk(uint64_t& lbid);
   const int  getSubBlk();
   const int  getSubBlk(uint64_t& lbid, int& sbid, int& entry);

   
private:
    struct timeb   t_start, t_current;   
	   const int getLastFbo();
    const int setLastFbo(int* fbo);
    //const int addRidList(RID* ridList, int & size);
   /**
    * @brief get a segment from freemanager
    */  
    const int  getSegment( FILE* pfile,  const IdxTreeGroupType segmentType, 
                           IdxEmptyListEntry* assignPtr );  	
   /**
    * @brief insert a rowid into the key index list
    */ 
    const int  updateIndexList( const RID& listRid, const uint64_t& key, 
                                uint64_t& startLbid, int& startSbid, int& startEntry,
                                uint64_t& endLbid, int& endSbid, int& endEntry
                                ); 
   
   /**
    * @brief insert a rowid into the key index list
    */  
    const int  updateIndexList( const RID& listRid, const uint64_t& key); 
    const int  updateIndexList( const RID* ridList, const int size, int& startPos);
    const int  addRid(const RID& newRid, IdxRidListPtr& lastIdxRidListPtr);
    const int  addRidList(const RID* ridList, const int size, int& startPos);
   /**
    * @brief write all of the blocks in the same time at the end
    */  
    
   /**
    * @brief add a new subblock segment to the linked list
    */
    const int  addNextIdxRidList(FILE* pFile, const RID& rowId, 
                                IdxEmptyListEntry* newIdxListEntryPtr); 
   /**
    * @brief find a row id from the key index list and return the location
    */
    bool findRowId( const RID& rowId,const uint64_t& key, uint64_t& lbid, int& sbid, int& entry);
   /**
    * @brief delete a row id from the key index list 
    */
    const int  deleteIndexList( const RID& rowId, const uint64_t& key);
   /**
    * @brief delete a row id from the key index list and return a location
    */
    
    const int  deleteIndexList(const RID& rowId, 
                              const uint64_t& key, 
                              uint64_t& lbid, int&sbid, int& entry);
   /**
    * @brief insert a new row id to the key index list 
    */
   const int  updateIdxRidList(const RID& rid, DataBlock* dataBlock, 
                               const int& sbid, const int& entry,
                               IdxRidListPtr* idxRidListPtr);
   
   const int  deleteInSub( const RID& rowId) ;
   const int  deleteInBlock(const RID& newRid);
   const int  releaseSegment() ;
         void setListOid(const OID& listOid){m_oid = listOid;}
   const OID  getListOid(){return m_oid;}
   const int  insertRowId( uint64_t& curLbid, int& pos, uint64_t& parentLbid);
   const int  setupBlock ( uint64_t& curLbid, int& pos, uint64_t& parentLbid);

   const int  updateHdrSub(const RID& newRid, const uint64_t &key);
   const int  addRidInSub(const RID& newRid, 
                                    IdxRidListPtr& lastIdxRidListPtr);
   const int  moveRidsToNewSub(FILE* pFile, const RID& oldRid, 
                                         const RID& newRid,
                                         IdxEmptyListEntry* newIdxListEntryPtr);
   const int  updateHdrCount();

   const int  setParentStatus(uint64_t& pLbid);
   const int  setCurBlkNextPtr(uint64_t& nextLbid, int count);
   const int  initNewBlock(int64_t& lbid, DataBlock* dataBlock, uint64_t& pLbid);
   const int  initNewBlock(DataBlock* dataBlock, uint64_t& pLbid);
   const int  initCurBlock();
   const int  getNextInfoFromArray(IdxRidNextListPtr& nextIdxListPtr);
   const int  getParentInfoFromArray(IdxRidParentListPtr& parentIdxListPtr);
   const int  updateCurCountInArray(int insCnt=1);
   const int  updateParentStatus(uint64_t& childLbid);

   const int  updateLastPtrAndParent(const int lastCount);
   const int  updateLastPtr(const int lastCount);
   const int  updateParent();
   
   /**
    * @brief private member data
    */  
    DataBlock m_curBlock;              /**@brief the current block buffer */
    DataBlock m_hdrBlock;              /**@brief the header block buffer*/
    DataBlock m_blockZero;             /**@brief 0,0,0 for free manager*/
    DataBlock m_parentBlock;
    DataBlock m_nextBlock;

    uint64_t m_hdrLbid;                      /**@brief the header block number */
    int m_hdrSbid;                     /**@brief the header subblock number */
    int m_hdrEntry;                    /**@brief the header entry number */
    
    uint64_t m_lbid;                         /**@brief the current block number */
    int m_sbid;                        /**@brief the current subblock number */
    int m_entry;                       /**@brief the current entry number */
    
    uint64_t m_newLbid;
    int m_newSbid;
    int m_newEntry;
    
    uint64_t m_parentLbid;
    int m_parentSbid;
    int m_parentEntry;
    
    uint64_t m_nextLbid;                     /**@brief the next block number */
    int m_nextSbid;                    /**@brief the next subblock number */
    int m_nextEntry;                   /**@brief the next entry number */
    
    uint64_t m_dLbid;                     /**@brief the next block number */
    int m_dSbid;                    /**@brief the next subblock number */
    int m_dEntry;                   /**@brief the next entry number */   
     
    FILE* m_pFile;                  /**@brief file handle for index list */
    OID     m_oid;   
    int     m_llpPos;
    bool    m_useBlock;
    int     m_entryGroup;
    int     m_type; 
    int     m_segType;
    int     m_curType;
    int     m_nextType;
    int     m_totalbytes;
	int     m_lastFbo;
	uint64_t m_lastLbid;
    FreeMgr m_freemgr;
    time_t         m_startTime;               // start time
    time_t         m_endTime;                 // end time
    double         m_totalTime;
    DebugLevel     m_debugLevel;              // internal use debug level
    bool           m_useSortFlag;
    bool           m_useNarray;
    IdxRidChildListPtr   m_childIdxRidListPtr[4];
    IdxRidNextListPtr    m_nextIdxListPtr;
    IdxRidParentListPtr  m_parentIdxListPtr;
    IdxRidListArrayPtr   m_idxRidListArrayPtr;
    IdxRidLastListPtr    m_lastBLKIdxListPtr;
    IdxRidListPtr        m_lastIdxRidListPtr;
    
    int                  m_curLevel;
    int                  m_curBlkPos;
    int                  m_curLevelPos;
    int                  m_curCount;
    bool                 m_flag;
};
//std::ostream& operator<<(std::ostream& os, const IndexList& rhs);
} //end of namespace
#endif // _WE_IndexList_H_
