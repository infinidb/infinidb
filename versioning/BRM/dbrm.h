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

/******************************************************************************
 * $Id: dbrm.h 1878 2013-05-02 15:17:12Z dcathey $
 *
 *****************************************************************************/

/** @file 
 * class DBRM
 */

#ifndef DBRM_H_
#define DBRM_H_

#include <unistd.h>
#include <sys/types.h>
#include <vector>
#include <set>
#include <string>
#include <boost/thread.hpp>
#include <boost/shared_array.hpp>

#include "brmtypes.h"
#include "messagequeue.h"

#include "extentmap.h"
#include "vss.h"
#include "vbbm.h"
#include "copylocks.h"
#include "calpontsystemcatalog.h"
#include "sessionmanagerserver.h"
#include "configcpp.h"
#include "mastersegmenttable.h"

#if defined(_MSC_VER) && defined(xxxDBRM_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

#ifdef BRM_DEBUG
#define DBRM_THROW
#else
#define DBRM_THROW throw()
#endif

namespace BRM {

/** @brief The interface to the Distributed BRM system.
 *
 * There are 3 components of the Distributed BRM (DBRM).
 * \li The interface
 * \li The Master node
 * \li Slave nodes
 *
 * The DBRM class is the interface, which is identical to the single-node
 * version, the BlockResolutionManager class.
 *
 * The DBRM components effectively implement a networking & synchronization
 * layer to the BlockResolutionManager class so that every node that needs
 * BRM data always has an up-to-date copy of it locally.  An operation that changes
 * BRM data is duplicated on all hosts that run a Slave node so that every
 * node has identical copies.  All "read" operations are satisfied locally.
 *
 * Calpont configuration file entries are necessary for the interface, Master,
 * and Slave to find each other.  Config file entries look like
 * \code
 * <DBRM_Controller>
 *	<IPAddr>
 * 	<Port>
 *	<NumWorkers>N</NumWorkers>
 * </DBRM_Controller>
 * <DBRM_Worker1>
 *	<IPAddr>
 *	<Port>
 * </DBRM_Worker1>
 *	...
 * <DBRM_WorkerN>
 *	<IPAddr>
 *	<Port>
 * </DBRM_WorkerN>
 * \endcode
 */

class DBRM {
public:
	// The param noBRMFcns suppresses init of the ExtentMap, VSS, VBBM, and CopyLocks.
	// It can speed up init if the caller only needs the other structures.
	EXPORT DBRM(bool noBRMFcns = false) throw();
	EXPORT ~DBRM() throw();
	
	// @bug 1055+ - Added functions below for multiple files per OID enhancement.

	/** @brief Get the OID, offset, db root, partition, and segment of a logical block ID.
	 *
	 * Get the OID, offset, db root, and partition of a logical block ID.
	 * @param lbid (in) The LBID to look up
	 * @param verid (in) The version of the LBID to look up
	 * @param vbFlags (in) If true, look for the block in the version buffer
	 * if false, the extent map
	 * @param oid (out) The OID of the file the LBID is allocated to
	 * @param fileBlockOffset (out) The file block offset of the LBID
	 * @param dbRoot (out) The DBRoot number that contains the file.
	 * @param partitionNum (out) The partition number for the file.
	 * @param segmentNum (out) The segment number for the file.
	 * 
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int lookupLocal(LBID_t lbid, VER_t verid, bool vbFlag, OID_t& oid,
		uint16_t& dbRoot, uint32_t& partitionNum, uint16_t& segmentNum, uint32_t& fileBlockOffset) throw();

	/** @brief Get the LBID assigned to the given OID, block offset, partion, and segment.
	 *
	 * Get the LBID assigned to the given OID, block offset, partition, and segment.
	 * @param oid (in) The OID
	 * @param partitionNum (in) The partition number
	 * @parm segmentNum (in) The segment number
	 * @param fileBlockOffset (in) The offset in the OID to return the LBID of
	 * @param lbid (out) The LBID of the offset of the OID.
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int lookupLocal(OID_t oid, uint32_t partitionNum, uint16_t segmentNum, uint32_t fileBlockOffset, LBID_t& lbid) throw();

	/** @brief A dbroot-specific version of lookupLocal() */
	EXPORT int lookupLocal_DBroot(OID_t oid, uint32_t dbroot,
			uint32_t partitionNum, uint16_t segmentNum,
			uint32_t fileBlockOffset, LBID_t& lbid) throw();
	// @bug 1055-	

	/** @brief Get the starting LBID assigned to the extent containing
	 *  the specified OID, block offset, partition, and segment.
	 *
	 * Get the LBID assigned to the given OID, partition, segment,
	 * and block offset.
	 * @param oid (in) The OID
	 * @param partitionNum (in) The partition number
	 * @parm segmentNum (in) The segment number
	 * @param fileBlockOffset (in) The requested offset in the specified OID
	 * @param lbid (out) The starting LBID of extent with specified offset
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int lookupLocalStartLbid(OID_t oid,
			 uint32_t partitionNum,
			 uint16_t segmentNum,
			 uint32_t fileBlockOffset,
			 LBID_t& lbid) throw();

	/**@brief Do a VSS lookup.
	 *
	 * Do a VSS lookup.  Gets the version ID of the block the caller should use
	 * and determines whether it is in the version buffer or the main database.
	 * @param lbid (in) The block number
	 * @param qc (in) The SCN & txn list provided by SessionManager::verID().
	 * @param txnID (in) If the caller has a transaction ID, put it here.  
	 * Otherwise use 0.
	 * @param outVer (out) The version the caller should use.
	 * @param vbFlag (out) If true, the block is in the version buffer; 
	 * false, the block is in the main database.
	 * @param vbOnly (in) If true, it will only consider entries in the Version Buffer
	 * (vbFlag will be true on return).  If false, it will also consider the main DB
	 * entries.  This defaults to false.
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int vssLookup(LBID_t lbid, const QueryContext &qc, VER_t txnID, VER_t *outVer,
		bool *vbFlag, bool vbOnly = false) throw();

	/** @brief Do many VSS lookups under one lock
	 *
	 * Do many VSS lookups under one lock.
	 * @param lbids (in) The LBIDs to look up
	 * @param qc (in) The input version info, equivalent to the verID param in vssLookup()
	 * @param txnID (in) The input transaction number, equivalent to the txnID param in vssLookup()
	 * @param out (out) The values equivalent to the out parameters in vssLookup() including the individual return codes, ordered as the lbid list.
	 * @return 0 on success, -1 on a fatal error.
	 */
	EXPORT int bulkVSSLookup(const std::vector<LBID_t> &lbids, const QueryContext_vss &qc, VER_t txnID,
		std::vector<VSSData> *out);
	
	/// returns the version in the main DB files or 0 if none exist
	EXPORT VER_t getCurrentVersion(LBID_t lbid, bool *isLocked = NULL) const;

	/// returns the highest version # in the version buffer, or -1 if none exist
	EXPORT VER_t getHighestVerInVB(LBID_t lbid, VER_t max=std::numeric_limits<VER_t>::max()) const;  // returns

	/// returns true if that block is in the version buffer, false otherwise including on error
	EXPORT bool isVersioned(LBID_t lbid, VER_t version) const;

	/// Do many getCurrentVersion lookups under one lock grab
	EXPORT int bulkGetCurrentVersion(const std::vector<LBID_t> &lbids, std::vector<VER_t> *versions,
			std::vector<bool> *isLocked = NULL) const;

	/** @brief Get a complete list of LBIDs assigned to an OID
	 *
	 * Get a complete list of LBIDs assigned to an OID.
	 */
	EXPORT int lookup(OID_t oid, LBIDRange_v& lbidList) throw();

	/** @brief Allocate a "stripe" of extents for columns in a table.
	 *
	 * Allocate a "stripe" of extents for the specified columns and DBRoot
	 * @param cols (in) List of column OIDs and column widths
	 * @param dbRoot (in) DBRoot for requested extents.
	 * @param partitionNum (in/out) Partition number in file path.
	 *        If allocating OID's first extent for this DBRoot, then
	 *        partitionNum is input, else it is an output arg.
	 * @param segmentNum (out) Segment number selected for new extents.
	 * @param extents (out) list of lbids, numBlks, and fbo for new extents
	 * @return 0 on success, -1 on error
	 */
	 EXPORT int createStripeColumnExtents(
					const std::vector<CreateStripeColumnExtentsArgIn>& cols,
					u_int16_t  dbRoot,
					u_int32_t& partitionNum,
					u_int16_t& segmentNum,
			std::vector<CreateStripeColumnExtentsArgOut>& extents) DBRM_THROW;

	/** @brief Allocate an extent for a column file
	 *
	 * Allocate a column extent for the specified OID and DBRoot.
	 * @param OID (in) The OID requesting the extent.
	 * @param colWidth (in) Column width of the OID.
	 * @param dbRoot (in) DBRoot where extent is to be added.
	 * @param partitionNum (in/out) Partition number in file path.
	 *        If allocating OID's first extent for this DBRoot, then
	 *        partitionNum is input, else it is an output arg.
	 * @param segmentNum (out) Segment number in file path.
     * @param colDataType (in) the column type
	 * @param lbid (out) The first LBID of the extent created.
	 * @param allocdSize (out) The total number of LBIDs allocated.
	 * @param startBlockOffset (out) The first block of the extent created.
	 * @return 0 on success, -1 on error
	 */
	 // @bug 4091: To be deprecated.  Replaced by createStripeColumnExtents().
	 EXPORT int createColumnExtent_DBroot(OID_t oid,
					u_int32_t  colWidth,
					u_int16_t  dbRoot,
					u_int32_t& partitionNum,
					u_int16_t& segmentNum,
					execplan::CalpontSystemCatalog::ColDataType colDataType,
					LBID_t&    lbid,
					int&       allocdSize,
					u_int32_t& startBlockOffset) DBRM_THROW;

	/** @brief Allocate extent for specified segment file
	 *
	 * Allocate column extent for the exact segment file
	 * specified by OID, DBRoot, partition, and segment.
	 * @param OID (in) The OID requesting the extent.
	 * @param colWidth (in) Column width of the OID.
	 * @param dbRoot (in) DBRoot where extent is to be added.
	 * @param partitionNum (in) Partition number in file path.
	 * @param segmentNum (in) Segment number in file path.
	 * @param lbid (out) The first LBID of the extent created.
	 * @param allocdSize (out) The total number of LBIDs allocated.
	 * @param startBlockOffset (out) The first block of the extent created.
	 * @return 0 on success, -1 on error
	 */
	 EXPORT int createColumnExtentExactFile(OID_t oid,
					u_int32_t  colWidth,
					u_int16_t  dbRoot,
					u_int32_t  partitionNum,
					u_int16_t  segmentNum,
					execplan::CalpontSystemCatalog::ColDataType colDataType,
					LBID_t&    lbid,
					int&       allocdSize,
					u_int32_t& startBlockOffset) DBRM_THROW;
					
	/** @brief Allocate an extent for a dictionary store file
	 *
	 * Allocate a dictionary store extent for the specified OID, dbRoot,
	 * partition number, and segment number.
	 * @param OID (in) The OID requesting the extent.
	 * @param dbRoot (in) DBRoot to assign to the extent.
	 * @param partitionNum (in) Partition number to assign to the extent.
	 * @param segmentNum (in) Segment number to assign to the extent.
     * @param colDataType (in) the column type
	 * @param lbid (out) The first LBID of the extent created.
	 * @param allocdSize (out) The total number of LBIDs allocated.
	 * @return 0 on success, -1 on error
	 */
	 EXPORT int createDictStoreExtent(OID_t oid,
					 u_int16_t  dbRoot,
					 u_int32_t  partitionNum,
					 u_int16_t  segmentNum,
					 LBID_t&    lbid,
					 int&       allocdSize) DBRM_THROW;

	/** @brief Rollback (delete) set of column extents for specific OID & DBRoot
	 *
	 * Deletes all the extents that logically follow the specified
	 * column extent; and sets the HWM for the specified extent.
	 * @param oid OID of the extents to be deleted.
	 * @param bDeleteAll Indicates if all extents in the oid and dbroot are to
	 *        be deleted; else part#, seg#, and hwm are used.
	 * @param dbRoot DBRoot of the extents to be deleted.
	 * @param partitionNum Last partition to be kept.
	 * @param segmentNum Last segment in partitionNum to be kept.
	 * @param hwm HWM to be assigned to the last extent that is kept.
	 * @return 0 on success
	 */
	EXPORT int rollbackColumnExtents_DBroot(OID_t oid,
					bool      bDeleteAll,
					u_int16_t dbRoot,
					u_int32_t partitionNum,
					u_int16_t segmentNum,
					HWM_t     hwm) DBRM_THROW;

	/** @brief Rollback (delete) a set of dict store extents for an OID & DBRoot
	 *
	 * Arguments specify the last stripe.  Any extents after this are
	 * deleted.  The hwm's of the extents in the last stripe are updated
	 * based on the contents of the hwm vector.  If hwms is a partial list,
	 * (as in the first stripe of a partition), then any extents in sub-
	 * sequent segment files for that partition are deleted.  if hwms is empty
	 * then all the extents in dbRoot are deleted.
	 * @param oid OID of the extents to be deleted or updated.
	 * @param dbRoot DBRoot of the extents to be deleted.
	 * @param partitionNum Last partition to be kept.
	 * @param segNums Vector of segment numbers for last partition to be kept.
	 * @param hwms Vector of hwms for the last partition to be kept.
	 * @return 0 on success
	 */
	EXPORT int rollbackDictStoreExtents_DBroot(OID_t oid,
					 u_int16_t dbRoot,
					 u_int32_t partitionNum,
					 const std::vector<u_int16_t>& segNums,
					 const std::vector<HWM_t>& hwms) DBRM_THROW;
					 
	/** @brief delete of column extents for the specified extents.
	 *
	 * Deletes the extents from extent map 
	 * @param extentInfo the information for extents
	 */
	EXPORT int deleteEmptyColExtents(const std::vector<ExtentInfo>& extentsInfo) DBRM_THROW;
	
	/** @brief delete of dictionary extents for the specified extents.
	 *
	 * Deletes the extents from extent map 
	 * @param extentInfo the information for extents
	 */
	EXPORT int deleteEmptyDictStoreExtents(const std::vector<ExtentInfo>& extentsInfo) DBRM_THROW;
	
	/** @brief Delete the extents of an OID and invalidate VSS references to them
	 *
	 * Delete the extents assigned to an OID and deletes entries in the VSS
	 * that refer to the LBIDs used by it.
	 * @note The old version of this function deliberately did not delete the entries
	 * in the version buffer.
	 * @note This function is ridiculously slow right now.
	 * @param OID The OID of the object being deleted
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int deleteOID(OID_t oid) DBRM_THROW;


	/** @brief Delete the extents of an OID and invalidate VSS references to them
	 *
	 * Delete the extents assigned to an OID and deletes entries in the VSS
	 * that refer to the LBIDs used by it.
	 * @note The old version of this function deliberately did not delete the entries
	 * in the version buffer.
	 * @note This function is ridiculously slow right now.
	 * @param OID The OID of the object being deleted
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int deleteOIDs( const std::vector<OID_t>& oids) DBRM_THROW;
	
	/** @brief Gets the last local high water mark for an OID and dbRoot
	 *
	 * Get last local high water mark of an OID for a given dbRoot, relative to 
	 * a segment file. The partition and segment numbers for the pertinent
	 * segment are also returned.  
	 * @param OID (in) The OID
	 * @param dbRoot (in) The relevant DBRoot
	 * @param partitionNum (out) The relevant partition number
	 * @param segmentNum (out) The relevant segment number
	 * @param hwm (out) Last local high water mark for specified input
	 * @param status (out) State of the extent (Available, OutOfService, etc)
	 * @param bFound (out) Indicates whether an extent was found on the dbRoot
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int getLastHWM_DBroot(int OID, uint16_t dbRoot,
			uint32_t& partitionNum, uint16_t& segmentNum, HWM_t& hwm,
			int& status, bool& bFound) throw();

	/** @brief Get the "high water mark" of an OID, partition, and segment
	 * 
	 * Get local high water mark (aka, the highest numbered written block
	 * offset) for an OID,partition,segment relative to the segment file.
	 * This applies to either column or dictionary store OIDs.
	 * @param oid (in) The OID
	 * @param partitionNum (in) Relevant partition number.
	 * @param segmentNum (in) Relevant segment number.
	 * @param hwm (out) The high water mark of oid
	 * @param status (out) State of the extent (Available, OutOfService, etc)
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int getLocalHWM(OID_t oid, uint32_t partitionNum,
	       uint16_t segmentNum, HWM_t& hwm, int& status) throw();
	
	/** @brief Set the "high water mark" of an OID, partition, and segment
	 * 
	 * Set the local high water mark (aka, the highest numbered written
	 * block offset) for the segment file referenced by the specified OID,
	 * partition, and segment number.
	 * This applies to either column or dictionary store OIDs.
	 * @param oid (in) The OID
	 * @param partitionNum (in) Relevant partition number.
	 * @param segmentNum (in) Relevant segment number.
	 * @param hwm (in) The high water mark of oid
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int setLocalHWM(OID_t oid, uint32_t partitionNum,
	       uint16_t segmentNum, HWM_t hwm) DBRM_THROW;

	EXPORT int bulkSetHWM(const std::vector<BulkSetHWMArg> &, VER_t transID = 0) DBRM_THROW;

	/** @brief Does setLocalHWM, casual partitioning changes, & commit.  The HWM &
	 * CP changes are atomic with this fcn.  All functionality is optional.  Passing
	 * in an empty vector for any of these parms makes it do nothing for the
	 * corresponding operation.
	 *
	 * It returns 0 if all operations succeed, -1 if any operation fails.
	 * The controllernode will undo all changes made on error.
	 */
	EXPORT int bulkSetHWMAndCP(const std::vector<BulkSetHWMArg> & hwmArgs,
			const std::vector<CPInfo> & setCPDataArgs,
			const std::vector<CPInfoMerge> & mergeCPDataArgs,
			VER_t transID) DBRM_THROW;

	EXPORT int bulkUpdateDBRoot(const std::vector<BulkUpdateDBRootArg> &);

	/** @brief Get HWM information about last segment file for each DBRoot
	 *  assigned to a specific PM.
	 *
	 * Vector will contain an entry for each DBRoot.  If no "available" extents
	 * are found for a DBRoot, then totalBlocks will be 0 (and hwmExtentIndex
	 * will be -1) for that DBRoot.
	 * @param oid The oid of interest.
	 * @param pmNumber The PM number of interest.
	 * @param emDbRootHwmInfos The vector of DbRoot/HWM related objects.
	 */
	EXPORT int getDbRootHWMInfo(OID_t oid, uint16_t pmNumber,
	       EmDbRootHWMInfo_v& emDbRootHwmInfos) throw();

	/** @brief Get the status (AVAILABLE, OUTOFSERVICE, etc) for the
	 * segment file represented by the specified OID, part# and seg#.
	 *
	 * Unlike many of the other DBRM functions, this function does
	 * not return a bad return code if no extent is found; the "found"
	 * flag indicates whether an extent was found or not.
	 *
	 * @param oid (in) The OID of interest
	 * @param partitionNum (in) The partition number of interest
	 * @param segmentNum (in) The segment number of interest
	 * @param bFound (out) Indicates if extent was found or not
	 * @param status (out) The state of the extents in the specified
	 *        segment file.
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int getExtentState(OID_t oid, uint32_t partitionNum,
			uint16_t segmentNum, bool& bFound, int& status) throw();

	/** @brief Gets the extents of a given OID
	 *
	 * Gets the extents of a given OID.
	 * @param OID (in) The OID to get the extents for.
	 * @param entries (out) A snapshot of the OID's Extent Map entries
	 * sorted by starting LBID; note that The Real Entries can change at 
	 * any time.  Also, internally the size of an extent is measured in
	 * multiples of 1024 LBIDs.  So, if the size field is 10, it represents
	 * 10240 LBIDs.
	 * @param sorted (in) indicates if output is to be sorted
	 * @param notFoundErr (in) indicates if no extents is considered an err
	 * @param incOutOfService (in) include/exclude out of service extents
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int getExtents(int OID, std::vector<struct EMEntry>& entries,
		bool sorted=true, bool notFoundErr=true,
		bool incOutOfService=false) throw();

	/** @brief Gets the extents of a given OID under specified dbroot
	 *
	 * Gets the extents of a given OID under specified dbroot.
	 * @param OID (in) The OID to get the extents for.
	 * @param entries (out) A snapshot of the OID's Extent Map entries
	 * @param dbroot (in) dbroot
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int getExtents_dbroot(int OID, std::vector<struct EMEntry>& entries,
		const uint16_t dbroot) throw();

	/** @brief Gets the number of extents for the specified OID and DBRoot
	 *
	 * @param OID (in) The OID of interest
	 * @param dbroot (in) The DBRoot of interest
	 * @param incOutOfService (in) include/exclude out of service extents
	 * @param numExtents (out) number of extents found for OID and dbroot
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int getExtentCount_dbroot(int OID, uint16_t dbroot,
		bool incOutOfService, uint64_t& numExtents) throw();

	/** @brief Gets the number of rows in an extent
	 * 
	 * Gets the number of rows in an extent.
	 * @return The extent size
	 */
// dmc-getExtentSize() to be deprecated, replaced by getExtentRows()
	EXPORT int getExtentSize() throw();
	EXPORT unsigned getExtentRows() throw();

	/** @brief Gets the DBRoot for the specified system catalog OID
	 *
	 * Function should only be called for System Catalog OIDs, as it assumes
	 * the OID is fully contained on a single DBRoot, returning the first
	 * DBRoot found.  This only makes since for a System Catalog OID, because
	 * all other column OIDs can span multiple DBRoots.
	 *
	 * @param oid The system catalog OID
	 * @param dbRoot (out) the DBRoot holding the system catalog OID
	 */
	EXPORT int getSysCatDBRoot(OID_t oid, uint16_t& dbRoot) throw();
	
	/** @brief Delete a Partition for the specified OID(s).
	 *
	 * @param OID (in) the OID of interest.
	 * @param partitionNums (in) the set of partitions to be deleted.
	 */
	EXPORT int deletePartition(const std::vector<OID_t>& oids,
						const std::set<LogicalPartition>& partitionNums, std::string& emsg) DBRM_THROW;

	/** @brief Mark a Partition for the specified OID(s) as out of service.
	 *
	 * @param OID (in) the OID of interest.
	 * @param partitionNums (in) the set of partitions to be marked out of service.
	 */
	EXPORT int markAllPartitionForDeletion(const std::vector<OID_t>& oids) DBRM_THROW;
					
	/** @brief Mark a Partition for the specified OID(s) as out of service.
	 *
	 * @param OID (in) the OID of interest.
	 * @param partitionNums (in) the set of partitions to be marked out of service.
	 */
	EXPORT int markPartitionForDeletion(const std::vector<OID_t>& oids,
						const std::set<LogicalPartition>& partitionNums, std::string& emsg) DBRM_THROW;
						
	/** @brief Restore a Partition for the specified OID(s).
	 *
	 * @param OID (in) the OID of interest.
	 * @param partitionNums (in) the set of partitions to be restored.
	 */
	EXPORT int restorePartition(const std::vector<OID_t>& oids,
						const std::set<LogicalPartition>& partitionNums, std::string& emsg) DBRM_THROW;

	/** @brief Get the list of out-of-service partitions for a given OID
	 *
	 * @param OID (in) the OID of interest.
	 * @param partitionNums (out) the out-of-service partitions for the oid.
	 * partitionNums will be in sorted order.
	 */
	EXPORT int getOutOfServicePartitions(OID_t oid,
						std::set<LogicalPartition>& partitionNums) throw();

	/** @brief Delete all rows in the extent map that reside on the given DBRoot
	 *
	 * @param dbroot (in) the dbroot to be deleted
	 */
	EXPORT int deleteDBRoot(uint16_t dbroot) DBRM_THROW;

	/** @brief Is the specified DBRoot empty with no extents.
	 * Returns error if extentmap shared memory is not loaded.
	 *
	 * @param dbroot DBRoot of interest
	 * @param isEmpty (output) Flag indiicating whether DBRoot is empty
	 * @param errMsg  (output) Error message corresponding to bad return code
	 * @return ERR_OK on success
	 */
	EXPORT int isDBRootEmpty(uint16_t dbroot,
		bool& isEmpty, std::string& errMsg) throw();

	/** @brief Registers a version buffer entry.
	 *
	 * Registers a version buffer entry at <vbOID, vbFBO> with
	 * values of <transID, lbid>.
	 * @note The version buffer locations must hold the 'copy' lock
	 * first.
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int writeVBEntry(VER_t transID, LBID_t lbid, OID_t vbOID, 
					 u_int32_t vbFBO) DBRM_THROW;
	
	/** @brief Retrieves a list of uncommitted LBIDs.
	 * 
	 * Retrieves a list of uncommitted LBIDs for the given transaction ID.
	 * @param lbidList (out) On success this contains the ranges of LBIDs
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int getUncommittedLBIDs(VER_t transID, 
							std::vector<LBID_t>& lbidList)
			throw();

	/** @brief Does what you think it does.
	 *
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int getDBRootsForRollback(VER_t transID, std::vector<uint16_t> *dbRootList)
		throw();

	// @bug 1509.  Added getUncommittedExtentLBIDs function. 
	/** @brief Retrieves a list of uncommitted extent LBIDs.
	 *
	 * Retrieves a list of uncommitted LBIDs for the given transaction ID.
	 * This function differs from getUncommittedLBIDs in that only one LBID per
	 * extent is returned.  It is used to return a list that can be used to update
	 * casual partitioning information which is tracked at the extent level rather
	 * than the block level.
	 * @param lbidList (out) On success this contains the ranges of LBIDs
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int getUncommittedExtentLBIDs(VER_t transID, std::vector<LBID_t>& lbidList) throw();
	
	/** @brief Atomically prepare to copy data to the version buffer
	 * 
	 * Atomically sets the copy flag on the specified LBID ranges
	 * and allocate blocks in the version buffer to copy them to.
	 * If any LBID in the range cannot be locked, none will be
	 * and this will return -1.
	 * @param transID The transaction ID doing the operation
	 * @param ranges (in) A list of LBID ranges that will be copied
	 * @param freeList (out) On success, a list of ranges of the version
	 * buffer blocks to copy the LBID range to.
	 * @note The caller must do a rollback or commit immediately after getting ERR_DEADLOCK (6).
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int beginVBCopy(VER_t transID, uint16_t dbRoot, const LBIDRange_v& ranges,
				  VBRange_v& freeList) DBRM_THROW;
	
	/** @brief Atomically unset the copy lock & update the VSS.  Beware!  Read the warning!
	 * 
	 * Atomically unset the copy lock for the specified LBID ranges
	 * and add a new locked VSS entry for each LBID in the range.
	 * @note The elements of the ranges parameter <b>MUST</b> be the
	 * same elements passed to beginVBCopy().  The number and order of the
	 * elements can be different, but every element in ranges must also
	 * have been an element in beginVBCopy's ranges.
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int endVBCopy(VER_t transID, const LBIDRange_v& ranges) 
			DBRM_THROW;
	
	/** @brief Commit the changes made for the given transaction.
	 *
	 * This unlocks the VSS entries with VerID = transID.
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int vbCommit(VER_t transID) DBRM_THROW;
	
	/** @brief Reverse the changes made during the given transaction.
	 *
	 * Record that the given LBID was reverted to version verID.
	 * @warning This removes the copy locks held on all ranges by transID.
	 * @param transID The transaction ID
	 * @param lbidList The list of ranges to rollback.
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int vbRollback(VER_t transID, const LBIDRange_v& lbidList) 
			DBRM_THROW;

	/** @brief Reverse the changes made during the given transaction.
	 *
	 * Record that the given LBID was reverted to version verID.
	 * @warning This removes the copy locks held on all ranges by transID.
	 * @param transID The transaction ID
	 * @param lbidList The list of singular LBIDs to rollback.
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int vbRollback(VER_t transID, const std::vector<LBID_t>& lbidList) 
			DBRM_THROW;


	EXPORT int getUnlockedLBIDs(BlockList_t *list) DBRM_THROW;

	/** @brief Reinitialize the versioning data structures.
	 * 
	 * This entry point empties the VSS and VBBM structures on the
	 * slave nodes.  At system startup and after recovery, the data left in
	 * the VSS and VBBM are unnecessary.  The primary purpose of this function
	 * is to free up memory.
	 * @return 0 on success, non-0 on error (see brmtypes.h).
	 */
	EXPORT int clear() DBRM_THROW;

	/** @brief Check the consistency of each data structure
	 *
	 * Check the consistency of each data structure
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int checkConsistency() throw();

	/** @brief Get a list of the transactions currently in progress.
	 * 
	 * Get a list of the transactions currently in progress.  This scans
	 * the copy locks & VSS for LBIDs that are locked by some transaction
	 * and stores the transaction ID.
	 * 
	 * @param txnList Caller-supplied set to store the results in.
	 * @return 0 on success, non-0 on error (see brmtypes.h)
	 */
	EXPORT int getCurrentTxnIDs(std::set<VER_t> &txnList) throw();

	/** @brief Persistence API.  Saves the local Extent Map to a file.
	 * 
	 * Persistence API.  Saves the <b>local</b> Extent Map to a file.
	 *
	 * @param filename Relative or absolute path to save to.
	 * @return 0 on success, -1 on error
	 */
	EXPORT int saveExtentMap(const std::string& filename) throw();

	/** @brief Persistence API.  Saves all BRM structures.
	 *
	 * Saves all <b>local</b> BRM structures to files.
	 *
	 * @param filename The filename prefix to use.  Saves 4 files with that prefix.
	 * @return 0 on success, -1 on error
	 */
	EXPORT int saveState(std::string filename) throw();

	/** @brief Persistence API.  Saves all BRM structures using the filenames from Calpont.xml.
	 *
	 * Saves all <b>local</b> BRM structures to files.
	 *
	 * @param filename The filename prefix to use.  Saves 4 files with that prefix.
	 * @return 0 on success, -1 on error
	 */
	EXPORT int saveState() throw();
	
	/** @brief A function that forces the BRM to write a snapshot of its data structures to disk
	 * 
	 * A function that forces the BRM to write a snapshot of its data structures to disk.
	 * This happens automatically after commits and rollbacks.  Bulk imports should call
	 * this fcn after an import.
	 * 
	 * @return 0 on success, non-0 on error (see brmtypes.h).
	 */
	EXPORT int takeSnapshot() throw();

	/* SessionManager interface */
	EXPORT const QueryContext verID();
	EXPORT const QueryContext sysCatVerID();
	EXPORT const TxnID newTxnID(const SessionManagerServer::SID session, bool block,
			bool isDDL = false);
	EXPORT void committed(BRM::TxnID& txnid);
	EXPORT void rolledback(BRM::TxnID& txnid);
	EXPORT const BRM::TxnID getTxnID(const SessionManagerServer::SID session);
	EXPORT boost::shared_array<SIDTIDEntry> SIDTIDMap(int& len);
	EXPORT void sessionmanager_reset();

	/* Note, these pull #s from two separate sequences.  That is, they both
	return 0, then 1, 2, 3, etc.  */
	EXPORT const uint32_t getUnique32();
	EXPORT const uint64_t getUnique64();

	/* New table lock interface */
	/* returns a unique ID (> 0) for the lock on success, 0 on failure.
	 * Also, on failure, the ownerName, pid, and session ID parameters will be set
	 * to the owner of one of the overlapping locks. */
	EXPORT uint64_t getTableLock(const std::vector<uint> &pmList, uint32_t tableOID,
			std::string *ownerName, uint32_t *ownerPID, int32_t *ownerSessionID,
			int32_t *ownerTxnID, LockState state);
	EXPORT bool releaseTableLock(uint64_t id);
	EXPORT bool changeState(uint64_t id, LockState state);
	EXPORT bool changeOwner(uint64_t id, const std::string &ownerName, uint ownerPID,
			int32_t ownerSessionID, int32_t ownerTxnID);
	EXPORT bool checkOwner(uint64_t id);
	EXPORT std::vector<TableLockInfo> getAllTableLocks();
	EXPORT void releaseAllTableLocks();
	EXPORT bool getTableLockInfo(uint64_t id, TableLockInfo *out);

	/** Casual partitioning support **/
	EXPORT int markExtentInvalid(const LBID_t lbid,
								 execplan::CalpontSystemCatalog::ColDataType colDataType) DBRM_THROW;
	EXPORT int markExtentsInvalid(const std::vector<LBID_t> &lbids,
								  const std::vector<execplan::CalpontSystemCatalog::ColDataType>& colDataTypes) DBRM_THROW;
	EXPORT int getExtentMaxMin(const LBID_t lbid, int64_t& max, int64_t& min, int32_t& seqNum) throw();

	EXPORT int setExtentMaxMin(const LBID_t lbid, const int64_t max, const int64_t min, const int32_t seqNum) DBRM_THROW;

	/** @brief Updates the max and min casual partitioning info for the passed extents.
	 *
	 * @bug 1970.
	 *
	 * @param cpInfos vector of CPInfo objects.  The firstLbid must be the first LBID in the extent.
	 * @return 0 on success, -1 on error
	 */
	EXPORT int setExtentsMaxMin(const CPInfoList_t &cpInfos) DBRM_THROW;

	/** @brief Merges max/min casual partitioning info for the specified
	 *  extents, with the current CP info.
	 *
	 * @param cpInfos vector of CPInfo objects.  The Lbids must be the
	 * starting LBID in the relevant extent.
	 * @return 0 on success, -1 on error
	 */
	EXPORT int mergeExtentsMaxMin(const CPInfoMergeList_t &cpInfos) DBRM_THROW;

	/* read-side interface for locking LBID ranges (used by PrimProc) */
	EXPORT void lockLBIDRange(LBID_t start, uint count);
	EXPORT void releaseLBIDRange(LBID_t start, uint count);

	/* write-side interface for locking LBID ranges (used by DML) */
	EXPORT int dmlLockLBIDRanges(const std::vector<LBIDRange> &ranges, int txnID);
	EXPORT int dmlReleaseLBIDRanges(const std::vector<LBIDRange> &ranges);

	/* OAM Interface */
	EXPORT int halt() DBRM_THROW;
	EXPORT int resume() DBRM_THROW;
	EXPORT int forceReload() DBRM_THROW;
	EXPORT int setReadOnly(bool b) DBRM_THROW;
	EXPORT int isReadWrite() throw();

	EXPORT bool isEMEmpty() throw();

	EXPORT std::vector<InlineLBIDRange> getEMFreeListEntries() throw();

	/** @brief Check if the system is ready for updates
	 *
	 * Check is the system has completed rollback processing and is
	 * ready for updates 
	 */
	EXPORT int getSystemReady() throw();

	/** @brief Check if the system is suspended
	 * 
	 * Suspended is caused by user at calpont-console
	 * @return 0 - system is not suspended, > 0 - system is 
	 *  	   suspended, < 0 on error
	 */
	EXPORT int getSystemSuspended() throw();

	/** @brief Check if system suspension is pending
	 * 
	 * If the user at calpont-console asks for suspension, but ddl, 
	 * dml or cpimport are running, then we can't suspend. If user 
	 * says suspend when all work completed, then this flag is set 
	 * to prevent new work from starting. 
	 * @param bRollback (out) if true, system is in a rollback mode 
	 *  		before suspension.
	 * @return 0 - system is not in a suspend pending state, > 0 - 
	 *  	   system is in a suspend pending state, < 0 on error
	 */
	EXPORT int getSystemSuspendPending(bool& bRollback) throw();

	/** @brief Check if system shutdown is pending
	 * 
	 * If the user at calpont-console asks for shutdown, but ddl, 
	 * dml or cpimport are running, then we can't shutdown. If user
	 * says shutdown when all work completed, then this flag is set
	 * to prevent new work from starting. 
	 * @param bRollback (out) if true, system is in rollback mode 
	 *  		before shutdown.
	 * @param bForce (out) if true, system is in force shutdown 
	 *  		mode. No work of any kind should be done.
	 * @return 0 - system is not pending a shutdown, > 0 - system is 
	 *  	   pending a shutdown, < 0 on error
	 */
	EXPORT int getSystemShutdownPending(bool& bRollback, bool& bForce) throw();

	/** @brief Mark the system as ready for updates
	 *
	 * The system has completed rollback processing and is therefore ready for updates
	 * @param bReady the state the ready flag should be set to.
	 * @return < 0 on error
	 */
	EXPORT int setSystemReady(bool bReady) throw();

	/** @brief Mark the system as suspended (or not)
	 *
	 * @param bSuspended the suspend state the system should be set 
	 *   				to.
	 * @return < 0 on error
	 */
	EXPORT int setSystemSuspended(bool bSuspended) throw();

	/** @brief Mark the system as suspension pending
	 *
	 * @param bPending the suspend pending state the system should 
	 *  				be set to.
	 * @param bRollback if true, rollback all active transactions 
	 *  				before full suspension of writes (only used
	 *  				if bPending is true).
	 * @return < 0 on error
	 */
	EXPORT int setSystemSuspendPending(bool bPending, bool bRollback = false) throw();

	/** @brief Mark the system as shutdown pending
	 *
	 * @param bPending the suspend pending state the system should 
	 *  				be set to.
	 * @param bRollback if true, rollback all active transactions 
	 *  				before shutting down. (only used if bPending
	 *  				is true).
	 * @param bForce if true, we're shutting down now. No further 
	 *  				work, including rollback, should be done.
	 *  				(only used if bPending is true).
	 * @return < 0 on error
	 */
	EXPORT int setSystemShutdownPending(bool bPending, bool bRollback = false, bool bForce = false) throw();

	/** @brief get the flags in the system state bit map.
	 * @param stateFlags (out)
	 * @return < 0 on error
	*/ 
	int getSystemState(uint32_t& stateFlags) throw(); 

	/** @brief set the flags in the system state bit map.
	 *  
	 * sets the flags that are set in stateFlags leaving other flags undisturbed
	 * @param stateFlags (out)
	 * @return < 0 on error 
	*/ 
	int setSystemState(uint32_t stateFlags) throw(); 

	/** @brief set the flags in the system state bit map.
	 *  
	 * Clears the flags that are set in stateFlags leaving other flags undisturbed 
	 * @param stateFlags (out)
	 * @return < 0 on error 
	*/ 
	int clearSystemState(uint32_t stateFlags) throw(); 

	/** @brief Check to see if Controller Node is functioning
	*/ 
	bool isDBRMReady() throw(); 

	/* OID Manager interface.  See oidserver.h for the API documentation. */
	EXPORT int allocOIDs(int num);
	EXPORT void returnOIDs(int start, int end);
	EXPORT int oidm_size();
	EXPORT int allocVBOID(uint dbroot);
	EXPORT int getDBRootOfVBOID(uint vbOID);
	EXPORT std::vector<uint16_t> getVBOIDToDBRootMap();

	/* Autoincrement interface */
	EXPORT void startAISequence(uint32_t OID, uint64_t firstNum, uint colWidth,
								execplan::CalpontSystemCatalog::ColDataType colDataType);
	EXPORT bool getAIRange(uint32_t OID, uint32_t count, uint64_t *firstNum);
	EXPORT bool getAIValue(uint32_t OID, uint64_t *value);
	EXPORT void resetAISequence(uint32_t OID, uint64_t value);
	EXPORT void getAILock(uint32_t OID);
	EXPORT void releaseAILock(uint32_t OID);

    /* Added to support unsigned */
    /** @brief Invalidate the casual partitioning for all uncommited
     *         extents.
     *  
     * Either txnid or plbidList can be passed . Only one will be 
     * used.
     * @param txnid (in) - The transaction for which to get the lbid 
     *              list.
     * @param plbidList (in) - a list of lbids whose extents are to 
     *               be invalidated. Only one lbid per extent should
     *               be in this list, such as returned in
     *               getUncommittedExtentLBIDs().
     * @return nothing.
    */ 
    EXPORT void invalidateUncommittedExtentLBIDs(execplan::CalpontSystemCatalog::SCN txnid,
                                                 std::vector<LBID_t>* plbidList = NULL);
private:
	DBRM(const DBRM& brm);
	DBRM& operator=(const DBRM& brm);
	int8_t send_recv(const messageqcpp::ByteStream& in, 
		messageqcpp::ByteStream& out) throw();

	void deleteAISequence(uint32_t OID);   // called as part of deleteOID & deleteOIDs

	boost::scoped_ptr<MasterSegmentTable> mst;
	boost::scoped_ptr<ExtentMap> em;
	boost::scoped_ptr<VBBM> vbbm;
	boost::scoped_ptr<VSS> vss;
	boost::scoped_ptr<CopyLocks> copylocks;
	messageqcpp::MessageQueueClient *msgClient;
	std::string masterName;
	boost::mutex mutex;
	config::Config *config;
	bool fDebug;
};

}

#undef EXPORT

#endif 
