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
* $Id: we_brm.h 4726 2013-08-07 03:38:36Z bwilkinson $
*
*******************************************************************************/
/** @file */

#ifndef _WE_BRM_H_
#define _WE_BRM_H_

#include <iostream>
#include <vector>
#include <boost/thread.hpp>
#include <boost/thread/tss.hpp>

#include "brm.h"
#include "we_obj.h"
#include<sys/time.h>
#include "brmtypes.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
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
     * @brief Initialize an Auto Increment sequence for the specified OID
     * @param colOID   Column OID of interest
     * @param startNextValue Starting next value for the AI sequence
     * @param colWidth Width of the relevant column (in bytes)
     * @param errMsg   Applicable error message.
     */
    EXPORT int startAutoIncrementSequence( OID colOID,
                     uint64_t    startNextValue, 
                     uint32_t    colWidth,
                     execplan::CalpontSystemCatalog::ColDataType colDataType,
                     std::string& errMsg);

    /**
     * @brief Reserve a range of Auto Increment numbers for the specified OID
     * @param colOID   Column OID of interest
     * @param count    Requested range of auto increment numbers
     * @param firstNum (out) First number of range that is reserved
     * @param errMsg   Applicable error message.
     */
    EXPORT int getAutoIncrementRange( OID colOID,
                     uint64_t    count,
                     uint64_t&   firstNum,
                     std::string& errMsg);

    /**
     * @brief Inform BRM to add an extent to each of the requested OIDs at
     * the specified DBRoot (and partition number if the DBRoot is empty).
     * @param cols (in) List of column OIDs and column widths
     * @param dbRoot (in) DBRoot for requested extents
     * @param partition (in/out) Physical partition number in file path.
     *        If allocating OID's first extent for this DBRoot, then
     *        partition is input, else it is only for output.
     * @param segmentNum (out) Segment number for new extents
     * @param extents (out) List of lbids, numBlks, and fbo for new extents
     */
    EXPORT int allocateStripeColExtents(
                 const std::vector<BRM::CreateStripeColumnExtentsArgIn>& cols,
                     uint16_t    dbRoot,
                     uint32_t&   partition,
                     uint16_t&   segmentNum,
                 std::vector<BRM::CreateStripeColumnExtentsArgOut>& extents);

    /**
     * @brief Inform BRM to add extent to the exact segment file specified by
     * OID, DBRoot, partition, and segment.
     */
    EXPORT int allocateColExtentExactFile( const OID oid,
                     const uint32_t colWidth,
                     uint16_t   dbRoot,
                     uint32_t   partition,
                     uint16_t   segment,
                     execplan::CalpontSystemCatalog::ColDataType colDataType,
                     BRM::LBID_t& startLbid,
                     int&        allocSize,
                     uint32_t&  startBlock);

    /**
     * @brief Inform BRM to add a dictionary store extent to the specified OID
     */
    EXPORT int allocateDictStoreExtent( const OID oid,
                     uint16_t   dbRoot,
                     uint32_t   partition,
                     uint16_t   segment,
                     BRM::LBID_t& startLbid,
                     int&        allocSize );

    /**
     * @brief Inform BRM to delete certain oid
     */
    EXPORT int deleteOid( const OID oid );
   
    /**
     * @brief Inform BRM to delete list of oids
     */
    EXPORT int deleteOIDsFromExtentMap (const std::vector<int32_t>& oids);

    /**
     * @brief Get BRM information based on a specfic OID, DBRoot, partition,
     * and segment
     */
    EXPORT int getBrmInfo( const OID oid,
                      const uint32_t partition,
                      const uint16_t segment,
                      const int       fbo,
                      BRM::LBID_t&    lbid );

    /**
     * @brief Get starting LBID from BRM for a specfic OID, DBRoot, partition,
     * segment, and block offset.
     */
    EXPORT int getStartLbid( const OID oid,
                      const uint32_t partition,
                      const uint16_t segment,
                      const int       fbo,
                      BRM::LBID_t&    startLbid );

    /**
     * @brief Get the real physical offset based on the LBID
     */
    EXPORT int getFboOffset( const uint64_t lbid, 
                      uint16_t& dbRoot,
                      uint32_t& partition,
                      uint16_t& segment,
                      int&       fbo );
    EXPORT int getFboOffset( const uint64_t lbid, int& oid,
                      uint16_t& dbRoot,
                      uint32_t& partition,
                      uint16_t& segment,
                      int&       fbo );

    /**
     * @brief Get last "local" HWM, partition, and segment for an OID and DBRoot
     */
    EXPORT int getLastHWM_DBroot( OID oid,
                      uint16_t   dbRoot,
                      uint32_t&  partition,
                      uint16_t&  segment,
                      HWM&        hwm,
                      int&        status,
                      bool&       bFound);

    /**
     * @brief Get HWM for a specific OID, partition, and segment
     */
    int getLocalHWM( OID       oid ,
                     uint32_t partition,
                     uint16_t segment,
                     HWM&      hwm,
                     int&      status);

    /**
     * @brief Get HWM info for a specific OID and PM
     */
    EXPORT int getDbRootHWMInfo( const OID oid ,
                     BRM::EmDbRootHWMInfo_v& emDbRootHwmInfos);

    /**
     * @brief Get status or state of the extents in the specified segment file.
     * bFound flag indicates whether an extent was found or not.
     */
    int getExtentState( OID       oid,
                        uint32_t partition,
                        uint16_t segment,
                        bool&     bFound,
                        int&      status);

    /**
     * @brief Get extentRows
     */
    unsigned getExtentRows();

   /**
     * @brief Return the extents info for specified OID
     */
    int getExtents( int oid,
            std::vector<struct BRM::EMEntry>& entries,
            bool sorted, bool notFoundErr,
            bool incOutOfService );

    /**
     * @brief Return the extents info for specified OID and dbroot
     */
    int getExtents_dbroot( int oid,
            std::vector<struct BRM::EMEntry>& entries,
            const uint16_t dbroot);

    /**
     * @brief Return the read/write status of DBRM (helps detect if DBRM is up)
     */
    EXPORT int isReadWrite();

    /**
     * @brief Return the state of the system state shutdown pending 
     *        flags
     */
    EXPORT int isShutdownPending(bool& bRollback, bool& bForce);

    /**
     * @brief Return the state of the system state suspend pending 
     *        flags
     */
    EXPORT int isSuspendPending();

    /**
     * @brief Is InfiniDB system ready (completed startup)
     */
    bool isSystemReady();

    /**
     * @brief Lookup LBID ranges for column specified OID
     */
    int lookupLbidRanges(OID oid, BRM::LBIDRange_v& lbidRanges);

    /**
     * @brief Mark extent invalid for causal partioning
     */
    int markExtentInvalid(const uint64_t lbid,
        const execplan::CalpontSystemCatalog::ColDataType colDataType);

    /**
     * @brief Mark multiple extents invalid for causal partioning
     */
    int markExtentsInvalid(std::vector<BRM::LBID_t>& lbids,
        const std::vector<execplan::CalpontSystemCatalog::ColDataType>&
            colDataTypes);

    /**
     * @brief set extents CP min/max info into extent map
     */
    int setExtentsMaxMin(const BRM::CPInfoList_t& cpinfoList);

    /**
     * @brief Perform bulk rollback of any column extents that logically follow
     * the specified HWM for the given column OID and DBRoot.  The HWM for the
     * last local extent is reset to the specified hwm as well.  Any extents in
     * subsequent partitions are deleted.  If bDeleteAll is true, then all
     * extents for the specified oid and dbroot are deleted.
     */
    int rollbackColumnExtents_DBroot( const OID oid,
                     bool        bDeleteAll,
                     uint16_t   dbRoot,
                     uint32_t   partition,
                     uint16_t   segment,
                     BRM::HWM_t  hwm );

    /**
     * @brief Perform bulk rollback of the extents that follow the specified
     * dictionary extents for the given column OID and DBRoot.  The HWM for
     * the last retained extents, are reset as well.  Any trailing segment
     * files for the same parition, that are not specified in the hwm list,
     * are deleted.  Any extents in subsequent partitions are deleted.  If
     * segNums and hwms vector are empty, then all extents for the specified
     * oid and dbroot are deleted.
     */
    int rollbackDictStoreExtents_DBroot( OID oid,
                     uint16_t   dbRoot,
                     uint32_t   partition,
                     const std::vector<uint16_t>&  segNums,
                     const std::vector<BRM::HWM_t>& hwms );

    /**
     * @brief Perform delete column extents 
     */
    int deleteEmptyColExtents(const std::vector<BRM::ExtentInfo>& extentsInfo);

    /**
     * @brief Perform delete dictionary extents 
     */
    int deleteEmptyDictStoreExtents(
        const std::vector<BRM::ExtentInfo>& extentsInfo );

    /**
     * @brief Set HWM for a specific OID, partition, and segment
     */
    int setLocalHWM( OID       oid,
                     uint32_t partition,
                     uint16_t segment,
                     const HWM hwm );

    //Set hwm for all columns in a table
    int bulkSetHWM( const std::vector<BRM::BulkSetHWMArg> & vec,
                     BRM::VER_t transID);

    /**
     * @brief Atomically apply a batch of HWM and CP updates within the scope
     * of a single BRM lock.  CP info is merged with current min/max range.
     * @param hwmArgs         Vector of HWM updates
     * @param mergeCPDataArgs Vector of Casual Partition updates
     */
    int bulkSetHWMAndCP( const std::vector<BRM::BulkSetHWMArg>& hwmArgs,
                     const std::vector<BRM::CPInfoMerge>& mergeCPDataArgs);

    /**
     * @brief Acquire a table lock for the specified table OID.
     * If nonzero lockID is returned, then the table is already locked.
     * @param tableOID  Table to be locked.
     * @param ownerName Requested (in) and current (out) owner for the lock.
     * @param processID Requested (in) and current (out) pid for the lock.
     * @param sessionID Requested (in) and current (out) session ID for the lock
     * @param transID   Requested (in) and current (out) transacton of the lock
     * @param lockID    Assigned or current lock for the specified table.
     * @param errMsg    Applicable error message.
     */
    EXPORT int getTableLock (    OID tableOid,
                                 std::string& ownerName,
                                 uint32_t&   processID,
                                 int32_t&     sessionID,
                                 int32_t&     transID,
                                 uint64_t&   lockID,
                                 std::string& errMsg);

    /**
     * @brief Change the state of the specified table lock ID.
     * @param lockID    Lock for which the status is to be changed.
     * @param lockState New state to be assigned to the specified lock.
     * @param bChanged  Indicates whether lock state was changed.
     * @param errMsg    Applicable error message.
     */
    EXPORT int changeTableLockState ( uint64_t lockID,
                                 BRM::LockState lockState,
                                 bool&        bChanged,
                                 std::string& errMsg);

    /**
     * @brief Release the specified table lock ID.
     * @param lockID    Lock to be released.
     * @param bReleased Indicates whether lock was released.
     * @param errMsg    Applicable error message.
     */
    EXPORT int releaseTableLock( uint64_t    lockID,
                                 bool&        bReleased,
                                 std::string& errMsg);

    /**
     * @brief Get current table lock information for the specified lock ID.
     * @param lockID      Lock to be retrieved.
     * @param lockInfo    Current lock information for the specified lock.
     * @param blockExists Indicates whether lock was found.
     * @param errMsg      Applicable error message.
     */
    EXPORT int getTableLockInfo( uint64_t    lockID,
                                 BRM::TableLockInfo* lockInfo,
                                 bool&        bLockExists,
                                 std::string& errMsg);

    /**
     * @brief Tell BRM to make a snapshot of it's current state to disk.
     */
    int takeSnapshot();

    /**
     * @brief Save brm structures to file
     */
    EXPORT int saveState();

    //--------------------------------------------------------------------------
    // Non-inline Versioning Functions Start Here
    //--------------------------------------------------------------------------
    /**
     * @brief Commit the transaction
     */
    EXPORT int commit( const BRM::VER_t transID );

    /**
     * @brief Copy blocks between write engine and version buffer
     */
    EXPORT int copyVBBlock( IDBDataFile* pSourceFile,
                               IDBDataFile* pTargetFile,
                               const uint64_t sourceFbo,
                               const uint64_t targetFbo,
                               DbFileOp* fileOp,
                               const Column& column );
    EXPORT int copyVBBlock( IDBDataFile* pSourceFile,
                               const OID sourceOid,
                               IDBDataFile* pTargetFile,
                               const OID targetOid,
                               const std::vector<uint32_t>& fboList,
                               const BRM::VBRange& freeList,
                               size_t& nBlocksProcessed,
                               DbFileOp* pFileOp,
                               const size_t fboCurrentOffset = 0 );

    /**
     * @brief Rollback the specified transaction
     */
    EXPORT int rollBack( const BRM::VER_t transID, int sessionId );

   /**
     * @brief Rollback the specified transaction
     */
    EXPORT int rollBackVersion( const BRM::VER_t transID, int sessionId );

   /**
     * @brief Rollback the specified transaction
     */
    EXPORT int rollBackBlocks( const BRM::VER_t transID, int sessionId );

    /**
     * @brief Write specified LBID to version buffer
     */
    EXPORT int writeVB( IDBDataFile* pFile,
                              const BRM::VER_t transID,
                              const OID oid,
                              const uint64_t lbid,
                              DbFileOp* pFileOp );
    int        writeVB( IDBDataFile* pFile,
                              const BRM::VER_t transID,
                              const OID weOid,
                              std::vector<uint32_t>& fboList,
                              std::vector<BRM::LBIDRange>& rangeList,
                              DbFileOp* pFileOp,
							  std::vector<BRM::VBRange>& freeList,
							  uint16_t dbRoot,
							  bool skipBeginVBCopy = false);
    void       writeVBEnd(const BRM::VER_t transID,
                              std::vector<BRM::LBIDRange>& rangeList);
							  
	BRM::DBRM*   getDbrmObject();
	void pruneLBIDList(BRM::VER_t transID,
            std::vector<BRM::LBIDRange> *rangeList,
            std::vector<uint32_t> *fboList) const;

    //--------------------------------------------------------------------------
    // Non-inline Versioning Functions End Here
    //--------------------------------------------------------------------------

    /**
     * @brief static functions
     */
    EXPORT static BRMWrapper* getInstance();
    EXPORT static int         getBrmRc(bool reset=true);
    static bool   getUseVb()                 { return m_useVb; }
    static void   setUseVb( const bool val ) { m_useVb = val;  }

private:
    //--------------------------------------------------------------------------
    // Private methods
    //--------------------------------------------------------------------------
    BRMWrapper();
    ~BRMWrapper();

    // disable copy constructor and assignment operator
    BRMWrapper(const BRMWrapper&);
    BRMWrapper& operator= ( const BRMWrapper& wrapper );

    // Convert BRM return code to WE return code
    int getRC( int brmRc, int errRc );

    EXPORT void saveBrmRc( int brmRc );

    IDBDataFile* openFile( const File& fileInfo,
                     const char* mode,
                     const bool bCache = false );




    //--------------------------------------------------------------------------
    // Private data members
    //--------------------------------------------------------------------------

    static BRMWrapper* volatile m_instance;
    static boost::thread_specific_ptr<int> m_ThreadDataPtr;
    static boost::mutex m_instanceCreateMutex;

#if defined(_MSC_VER) && !defined(WRITEENGINE_DLLEXPORT)
    __declspec(dllimport)
#endif
    EXPORT static bool  m_useVb;

    static OID          m_curVBOid;
    static IDBDataFile* m_curVBFile;

    BRM::DBRM*          blockRsltnMgrPtr;
};

//------------------------------------------------------------------------------
// Inline functions
//------------------------------------------------------------------------------
inline BRMWrapper::BRMWrapper()
{
    blockRsltnMgrPtr = new BRM::DBRM();
}

inline BRMWrapper::~BRMWrapper()
{
    if (blockRsltnMgrPtr)
        delete blockRsltnMgrPtr;
    blockRsltnMgrPtr = 0;
}

inline BRM::DBRM*   BRMWrapper::getDbrmObject()
{
	return blockRsltnMgrPtr;
}
inline int BRMWrapper::getRC( int brmRc, int errRc )
{
    if (brmRc == BRM::ERR_OK)
        return NO_ERROR;
    saveBrmRc( brmRc );
    return errRc;
}

inline int BRMWrapper::getLastHWM_DBroot( OID oid,
    uint16_t   dbRoot,
    uint32_t&  partition,
    uint16_t&  segment,
    HWM&        hwm,
    int&        status,
    bool&       bFound)
{
    int rc = blockRsltnMgrPtr->getLastHWM_DBroot(
        (BRM::OID_t)oid, dbRoot, partition, segment, hwm,
        status, bFound);
    return getRC( rc, ERR_BRM_GET_HWM );
}

inline int BRMWrapper::getLocalHWM( OID oid ,
    uint32_t   partition,
    uint16_t   segment,
    HWM&        hwm,
    int&        status)
{
    int rc = blockRsltnMgrPtr->getLocalHWM(
        (BRM::OID_t)oid, partition, segment, hwm, status);
    return getRC( rc, ERR_BRM_GET_HWM );
}

inline int BRMWrapper::getExtentState( OID oid,
    uint32_t partition,
    uint16_t segment,
    bool&     bFound,
    int&      status)
{
    int rc = blockRsltnMgrPtr->getExtentState(
        (BRM::OID_t)oid, partition, segment, bFound, status);
    return getRC( rc, ERR_BRM_GET_EXT_STATE );
}

inline unsigned BRMWrapper::getExtentRows()
{
    return  blockRsltnMgrPtr->getExtentRows( );
}

inline int BRMWrapper::getExtents( int oid,
    std::vector<struct BRM::EMEntry>& entries,
    bool sorted, bool notFoundErr,
    bool incOutOfService )
{
    int rc = blockRsltnMgrPtr->getExtents(
        oid, entries, sorted, notFoundErr, incOutOfService);
    return rc;
}

inline int BRMWrapper::getExtents_dbroot( int oid,
    std::vector<struct BRM::EMEntry>& entries,
    const uint16_t dbroot )
{
    int rc = blockRsltnMgrPtr->getExtents_dbroot(
        oid, entries, dbroot);
    return rc;
}

inline bool BRMWrapper::isSystemReady()
{
    return  blockRsltnMgrPtr->getSystemReady() > 0 ? true : false;
}

inline int BRMWrapper::lookupLbidRanges( OID oid, BRM::LBIDRange_v& lbidRanges)
{
    int rc = blockRsltnMgrPtr->lookup( oid, lbidRanges );
    return getRC( rc, ERR_BRM_LOOKUP_LBID_RANGES );
}

inline int BRMWrapper::markExtentInvalid( const uint64_t lbid,
    const execplan::CalpontSystemCatalog::ColDataType colDataType )
{
    int rc = blockRsltnMgrPtr->markExtentInvalid( lbid, colDataType );
    return getRC( rc, ERR_BRM_MARK_INVALID );
}

inline int BRMWrapper::markExtentsInvalid(std::vector<BRM::LBID_t>& lbids,
    const std::vector<execplan::CalpontSystemCatalog::ColDataType>&
        colDataTypes)
{
	int rc = 0;
	if (idbdatafile::IDBPolicy::useHdfs())
		return rc;
	rc = blockRsltnMgrPtr->markExtentsInvalid(lbids, colDataTypes); 
    return getRC( rc, ERR_BRM_MARK_INVALID );
}

inline int BRMWrapper::bulkSetHWMAndCP(
    const std::vector<BRM::BulkSetHWMArg>& hwmArgs,
    const std::vector<BRM::CPInfoMerge>& mergeCPDataArgs)
{
    std::vector<BRM::CPInfo> setCPDataArgs; // not used
    BRM::VER_t transID = 0;                 // n/a
    int rc = blockRsltnMgrPtr->bulkSetHWMAndCP(
        hwmArgs, setCPDataArgs, mergeCPDataArgs, transID );

    return getRC( rc, ERR_BRM_BULK_UPDATE );
}

inline int BRMWrapper::setExtentsMaxMin(const BRM::CPInfoList_t& cpinfoList)
{
    int rc = blockRsltnMgrPtr->setExtentsMaxMin( cpinfoList );
    return getRC( rc, ERR_BRM_SET_EXTENTS_CP );
}

inline int BRMWrapper::rollbackColumnExtents_DBroot( const OID oid,
    bool        bDeleteAll,
    uint16_t   dbRoot,
    uint32_t   partition,
    uint16_t   segment,
    BRM::HWM_t  hwm )
{
    int rc = blockRsltnMgrPtr->rollbackColumnExtents_DBroot (
        oid, bDeleteAll, dbRoot, partition, segment, hwm );
    return getRC( rc, ERR_BRM_BULK_RB_COLUMN );
}

inline int BRMWrapper::rollbackDictStoreExtents_DBroot( OID oid,
    uint16_t   dbRoot,
    uint32_t   partition,
    const std::vector<uint16_t>&  segNums,
    const std::vector<BRM::HWM_t>& hwms )
{
    int rc = blockRsltnMgrPtr->rollbackDictStoreExtents_DBroot (
        oid, dbRoot, partition, segNums, hwms );
    return getRC( rc, ERR_BRM_BULK_RB_DCTNRY );
}

inline int BRMWrapper::deleteEmptyColExtents(
    const std::vector<BRM::ExtentInfo>& extentsInfo )
{
    int rc = blockRsltnMgrPtr->deleteEmptyColExtents ( extentsInfo );
    return getRC( rc, ERR_BRM_DELETE_EXTENT_COLUMN );
}

inline int BRMWrapper::deleteEmptyDictStoreExtents(
    const std::vector<BRM::ExtentInfo>& extentsInfo )
{
    int rc = blockRsltnMgrPtr->deleteEmptyDictStoreExtents ( extentsInfo );
    return getRC( rc, ERR_BRM_DELETE_EXTENT_DCTNRY );
}

inline int BRMWrapper::setLocalHWM( OID oid,
    uint32_t   partition,
    uint16_t   segment,
    const HWM   hwm )
{
    int rc = blockRsltnMgrPtr->setLocalHWM(
    (int)oid, partition, segment, hwm);
    return getRC( rc, ERR_BRM_SET_HWM );
}

inline int BRMWrapper::bulkSetHWM( const std::vector<BRM::BulkSetHWMArg> & vec,
    BRM::VER_t transID = 0)
{
    int rc = blockRsltnMgrPtr->bulkSetHWM( vec, transID);
    return getRC( rc, ERR_BRM_SET_HWM );
}

inline int BRMWrapper::takeSnapshot()
{
    int rc = blockRsltnMgrPtr->takeSnapshot();
    return getRC( rc, ERR_BRM_TAKE_SNAPSHOT );
}

} //end of namespace

#undef EXPORT

#endif // _WE_BRM_H_
