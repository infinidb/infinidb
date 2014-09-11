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

/*******************************************************************************
* $Id: we_brm.h 3716 2012-04-03 18:34:00Z dcathey $
*
*******************************************************************************/
/** @file */

#ifndef _WE_BRM_H_
#define _WE_BRM_H_

#include <iostream>
#include <vector>
#include <boost/thread/tss.hpp>

#include "brm.h"
#include "we_obj.h"
#include<sys/time.h>
#include "brmtypes.h"

#if defined(_MSC_VER) && defined(WRITEENGINEBRM_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{

// forward reference
class DbFileOp;

/** Class BRMWrapper */
class BRMWrapper : public WEObj
{
public:
    /**
     * @brief Constructor
     */
    BRMWrapper()   { blockRsltnMgrPtr = new BRM::DBRM(); }

    /**
     * @brief Inform BRM to add an extent to the specified OID
     */
    EXPORT int allocateColExtent( const OID oid,
                     const u_int32_t colWidth,
                     u_int16_t&  dbRoot,
                     u_int32_t&  partition,
                     u_int16_t&  segment,
                     BRM::LBID_t& startLbid,
                     int&        allocSize,
                     u_int32_t&  startBlock);
    EXPORT int allocateDictStoreExtent( const OID oid,
                     u_int16_t   dbRoot,
                     u_int32_t   partition,
                     u_int16_t   segment,
                     BRM::LBID_t& startLbid,
                     int&        allocSize );

    /**
     * @brief Commit the transaction
     */
    EXPORT int commit( const BRM::VER_t transID );

    /**
     * @brief Copy blocks between write engine and version buffer
     */
    EXPORT int copyVBBlock( FILE* pSourceFile,
                               FILE* pTargetFile,
                               const i64 sourceFbo,
                               const i64 targetFbo,
                               DbFileOp* fileOp,
                               const Column& column );
// Not Current Used
#if 0
    EXPORT int copyVBBlock( FILE* pSourceFile,
                               File& targetFileInfo,
                               const i64 sourceFbo,
                               const i64 targetFbo );
    EXPORT int copyVBBlock( File&  sourceFileInfo,
                               File& targetFileInfo,
                               const i64 sourceFbo,
                               const i64 targetFbo );
#endif
    EXPORT int copyVBBlock( FILE* pSourceFile,
                               const OID sourceOid,
                               FILE* pTargetFile,
                               const OID targetOid,
                               const std::vector<i32>& fboList,
                               const BRM::VBRange& freeList,
                               size_t& nBlocksProcessed,
                               DbFileOp* pFileOp,
                               const size_t fboCurrentOffset = 0 );

    /**
     * @brief Inform BRM to delete certain oid
     */
    EXPORT int deleteOid( const OID oid );
   
    EXPORT int deleteOIDsFromExtentMap (const std::vector<int32_t>& oids);

    /**
     * @brief Flush inode cache
     */
    int        flushInodeCaches()
    { int rc = blockRsltnMgrPtr->flushInodeCaches();
      return getRC( rc, ERR_BRM_FLUSH_INODE_CACHE); }

    /**
     * @brief Get BRM information based on a specfic OID, DBRoot, partition,
     * and segment
     */
    EXPORT int getBrmInfo( const OID oid,
                      const u_int32_t partition,
                      const u_int16_t segment,
                      const int       fbo,
                      i64&            lbid );

    /**
     * @brief Get starting LBID from BRM for a specfic OID, DBRoot, partition,
     * segment, and block offset.
     */
    EXPORT int getStartLbid( const OID oid,
                      const u_int32_t partition,
                      const u_int16_t segment,
                      const int       fbo,
                      BRM::LBID_t&    startLbid );

    /**
     * @brief Get the real physical offset based on the LBID
     */
    EXPORT int getFboOffset( const i64 lbid, 
                      u_int16_t& dbRoot,
                      u_int32_t& partition,
                      u_int16_t& segment,
                      int&       fbo );
    EXPORT int getFboOffset( const i64 lbid, int& oid,
                      u_int16_t& dbRoot,
                      u_int32_t& partition,
                      u_int16_t& segment,
                      int&       fbo );

    /**
     * @brief Get last "local" HWM, DBRoot, partition, and segment for an OID
     */
    int getLastLocalHWM_int( const OID oid,
                      u_int16_t&  dbRoot,
                      u_int32_t&  partition,
                      u_int16_t&  segment,
                      int& hwm)
    { int rc = blockRsltnMgrPtr->getLastLocalHWM(
        (BRM::OID_t)oid, dbRoot, partition, segment, (BRM::HWM_t&)hwm);
      return getRC( rc, ERR_BRM_GET_HWM ); }
    // Eventual replacement for getLastLocalHWM_int(), at which time
    // this function should be renamed to getLastLocalHWM().
    int getLastLocalHWM_HWMt( const OID oid,
                      u_int16_t&  dbRoot,
                      u_int32_t&  partition,
                      u_int16_t&  segment,
                      HWM& hwm)
    { int rc = blockRsltnMgrPtr->getLastLocalHWM(
        (BRM::OID_t)oid, dbRoot, partition, segment, hwm);
      return getRC( rc, ERR_BRM_GET_HWM ); }

    /**
     * @brief Get HWM for a specific OID, partition, and segment
     */
    int getLocalHWM_int( const OID oid ,
                     u_int32_t   partition,
                     u_int16_t   segment,
                     int&        hwm)
    { int rc = blockRsltnMgrPtr->getLocalHWM(
        (BRM::OID_t)oid, partition, segment, (BRM::HWM_t&)hwm);
      return getRC( rc, ERR_BRM_GET_HWM ); }
    // Eventual replacement for getLocalHWM_int(), at which time
    // this function should be renamed to getLocalHWM().
    int getLocalHWM_HWMt( const OID oid ,
                     u_int32_t   partition,
                     u_int16_t   segment,
                     HWM&        hwm)
    { int rc = blockRsltnMgrPtr->getLocalHWM(
        (BRM::OID_t)oid, partition, segment, hwm);
      return getRC( rc, ERR_BRM_GET_HWM ); }

    /**
     * @brief Get extentRows
     */
    unsigned getExtentRows()
    { return  blockRsltnMgrPtr->getExtentRows( ); }

    /**
     * @brief Return the starting DBRoot and Partition # for the specified OID
     */
    int getStartExtent( const OID oid,
                                  uint16_t& dbRoot,
                                  uint32_t& partition, bool incOutOfService=false )
    { int rc = blockRsltnMgrPtr->getStartExtent( oid, dbRoot, partition, incOutOfService);
      return getRC( rc, ERR_BRM_GET_START_EXTENT ); }

	/**
     * @brief Return the extents info for specified OID
     */
    int getExtents( int oid,
            std::vector<struct BRM::EMEntry>& entries,
			bool sorted, bool notFoundErr,
			bool incOutOfService )
    { int rc = blockRsltnMgrPtr->getExtents( oid, entries, sorted, notFoundErr, incOutOfService);
      return rc; }
	  
    /**
     * @brief Return the read/write status of DBRM (helps detect if DBRM is up)
     */
    EXPORT int isReadWrite();

    /**
     * @brief Is InfiniDB system ready (completed startup)
     */
    bool isSystemReady()
    { return  blockRsltnMgrPtr->isSystemReady( ); }

    /**
     * @brief Lookup LBID ranges for column specified OID
     */
    int lookupLbidRanges( OID oid, BRM::LBIDRange_v& lbidRanges)
    { int rc = blockRsltnMgrPtr->lookup( oid, lbidRanges );
      return getRC( rc, ERR_BRM_LOOKUP_LBID_RANGES ); }

    /**
     * @brief Mark extent invalid for causal partioning
     */
    int markExtentInvalid( const i64 lbid )
    { int rc = blockRsltnMgrPtr->markExtentInvalid( lbid );
      return getRC( rc, ERR_BRM_MARK_INVALID ); }

    /**
     * @brief Mark multiple extents invalid for causal partioning
     */
    int markExtentsInvalid(const std::vector<BRM::LBID_t>& lbids)
    { int rc = blockRsltnMgrPtr->markExtentsInvalid( lbids ); 
      return getRC( rc, ERR_BRM_MARK_INVALID ); }

    /**
     * @brief Perform bulk rollback of any column extents that logically follow
     * the specified HWM for the given column OID.  The HWM for the last local
     * extent is reset to the specified hwm as well.  Any extents in subsequent
     * partitions are deleted.
     */
    int rollbackColumnExtents( const OID oid,
                     u_int32_t   partition,
                     u_int16_t   segment,
                     BRM::HWM_t  hwm )
    { int rc = blockRsltnMgrPtr->rollbackColumnExtents (
        oid, partition, segment, hwm );
      return getRC( rc, ERR_BRM_BULK_RB_COLUMN ); }

    /**
     * @brief Perform bulk rollback of the extents that follow the specified
     * dictionary extents for the given column OID.  The HWM for the last
     * retained extents, are reset as well.  Any trailing segment files for
     * the same parition, that are not specified in the hwm list, are deleted.
     * Any extents in subsequent partitions are deleted.
     */
    int rollbackDictStoreExtents( OID oid,
                     u_int32_t   partition,
                     const std::vector<BRM::HWM_t>& hwms )
    { int rc = blockRsltnMgrPtr->rollbackDictStoreExtents (
        oid, partition, hwms );
      return getRC( rc, ERR_BRM_BULK_RB_DCTNRY ); }

    /**
     * @brief Perform delete column extents 
     */
    int deleteEmptyColExtents( const std::vector<BRM::ExtentInfo>& extentsInfo )
    { int rc = blockRsltnMgrPtr->deleteEmptyColExtents ( extentsInfo );
      return getRC( rc, ERR_BRM_DELETE_EXTENT_COLUMN ); }

    /**
     * @brief Perform delete dictionary extents 
     */
    int deleteEmptyDictStoreExtents( const std::vector<BRM::ExtentInfo>& extentsInfo )
    { int rc = blockRsltnMgrPtr->deleteEmptyDictStoreExtents ( extentsInfo );
      return getRC( rc, ERR_BRM_DELETE_EXTENT_DCTNRY ); }

    /**
     * @brief Set HWM for a specific OID, partition, and segment
     */
    int setLocalHWM_int( const OID oid,
                     u_int32_t   partition,
                     u_int16_t   segment,
                     const int   hwm )
    { int rc = blockRsltnMgrPtr->setLocalHWM(
        (int)oid, partition, segment, (BRM::HWM_t)hwm);
      return getRC( rc, ERR_BRM_SET_HWM ); }

    // Eventual replacement for setLocalHWM_int(), at which time
    // this function should be renamed to setLocalHWM().
    int setLocalHWM_HWMt( const OID oid,
                     u_int32_t   partition,
                     u_int16_t   segment,
                     const HWM   hwm )
    { int rc = blockRsltnMgrPtr->setLocalHWM(
        (int)oid, partition, segment, hwm);
      return getRC( rc, ERR_BRM_SET_HWM ); }

    /**
     * @brief Atomically apply a batch of HWM and CP updates within the scope
     * of a single BRM lock.  CP info is merged with current min/max range.
     * @param hwmArgs         Vector of HWM updates
     * @param mergeCPDataArgs Vector of Casual Partition updates
     */
    int bulkSetHWMAndCP( const std::vector<BRM::BulkSetHWMArg>& hwmArgs,
                     const std::vector<BRM::CPInfoMerge>& mergeCPDataArgs)
    { std::vector<BRM::CPInfo> setCPDataArgs; // not used
      BRM::VER_t transID = 0;                 // n/a
      int rc = blockRsltnMgrPtr->bulkSetHWMAndCP(
        hwmArgs, setCPDataArgs, mergeCPDataArgs, transID );
      return getRC( rc, ERR_BRM_BULK_UPDATE );
    }

    int getTableLockInfo ( const OID tableOid,
                                     u_int32_t & processID,
                                     std::string & processName,
                                     bool & lockStatus,
                                     u_int32_t & sid)
    { int rc = blockRsltnMgrPtr->getTableLockInfo(
        (int)tableOid, processID, processName, lockStatus, sid );
      return getRC( rc, ERR_BRM_GET_TABLE_LOCK ); }
   
    int setTableLock ( const OID tableOid,
                                 const u_int32_t sessionID,
                                 u_int32_t  processID,
                                 std::string & processName,
                                 bool lock)
    { int rc = blockRsltnMgrPtr->setTableLock(
        (int)tableOid, sessionID, processID, processName, lock );
      return getRC( rc, ERR_BRM_SET_TABLE_LOCK ); }

    /**
     * @brief Rollback the specified transaction
     */
    EXPORT int rollBack( const BRM::VER_t transID, int sessionId );

    /**
     * @brief Tell BRM to make a snapshot of it's current state to disk.
     */
    int takeSnapshot()
    { int rc = blockRsltnMgrPtr->takeSnapshot();
      return getRC( rc, ERR_BRM_TAKE_SNAPSHOT ); }

    /**
     * @brief Set HWM for an OID
     */
    EXPORT int writeVB( FILE* pFile,
                              const BRM::VER_t transID,
                              const OID oid,
                              const i64 lbid,
                              DbFileOp* pFileOp );
    int        writeVB( FILE* pFile,
                              const BRM::VER_t transID,
                              const OID weOid,
                              std::vector<i32>& fboList,
                              std::vector<BRM::LBIDRange>& rangeList,
                              DbFileOp* pFileOp );
	void        writeVBEnd(const BRM::VER_t transID, std::vector<BRM::LBIDRange>& rangeList);					  

    /**
     * @brief Save brm structures to file
     */
    EXPORT int saveState();

    /**
     * @brief static functions
     */
    EXPORT static BRMWrapper* getInstance();
    EXPORT static void        deleteInstance();
    EXPORT static int         getBrmRc(bool reset=true);
    static bool   getUseVb()                 { return m_useVb; }
    static void   setUseVb( const bool val ) { m_useVb = val;  }

private:
    //--------------------------------------------------------------------------
    // Private methods
    //--------------------------------------------------------------------------

    // disable copy constructor and assignment operator
    BRMWrapper(const BRMWrapper&);
    BRMWrapper& operator= ( const BRMWrapper& wrapper );

    ~BRMWrapper()
    { if (blockRsltnMgrPtr)
        delete blockRsltnMgrPtr;
      blockRsltnMgrPtr = 0; }

    // Convert BRM return code to WE return code
    int getRC( int brmRc, int errRc )
    {
      if (brmRc == BRM::ERR_OK)
        return NO_ERROR;
      saveBrmRc( brmRc );
      return errRc;
    }

    EXPORT void saveBrmRc( int brmRc );

    FILE* openFile( File fileInfo,
                     const char* mode,
                     const bool bCache = false );

    void pruneLBIDList(BRM::VER_t transID, std::vector<BRM::LBIDRange> *rangeList,
    		std::vector<uint32_t> *fboList) const;



    //--------------------------------------------------------------------------
    // Private data members
    //--------------------------------------------------------------------------

    static BRMWrapper*  m_instance;
    static boost::thread_specific_ptr<int> m_ThreadDataPtr;

#if defined(_MSC_VER) && !defined(WRITEENGINEBRM_DLLEXPORT)
    __declspec(dllimport)
#endif
    EXPORT static bool  m_useVb;

    static OID          m_curVBOid;
    static FILE*        m_curVBFile;

    BRM::DBRM*          blockRsltnMgrPtr;
};

} //end of namespace

#undef EXPORT

#endif // _WE_BRM_H_
