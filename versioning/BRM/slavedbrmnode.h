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

/******************************************************************************
 * $Id: slavedbrmnode.h 1828 2013-01-30 16:13:05Z pleblanc $
 *
 *****************************************************************************/

/** @file 
 * class SlaveDBRMNode
 */

#ifndef SLAVEDBRMNODE_H_
#define SLAVEDBRMNODE_H_

#include <sys/types.h>
#include <vector>
#include <set>

#include "brmtypes.h"
#include "rwlock.h"
#include "mastersegmenttable.h"
#include "extentmap.h"

#include "vss.h"
#include "vbbm.h"
#include "copylocks.h"

#if defined(_MSC_VER) && defined(xxxSLAVEDBRMNODE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

/** @brief The Slave node of the DBRM system
 *
 * There are 3 components of the Distributed BRM (DBRM).
 * \li The interface
 * \li The Master node
 * \li Slave nodes
 *
 * The DBRM components effectively implement a networking & synchronization
 * layer to the BlockResolutionManager class so that every node that needs
 * BRM data always has an up-to-date copy of it locally.  An operation that changes
 * BRM data is duplicated on all hosts that run a Slave node so that every
 * node has identical copies.  All "read" operations are satisfied locally.
 *
 * The SlaveDBRMNode class does the work of modifying the BRM data structures
 * on the node it's running on.  Only nodes that use the BRM need to run a slave
 * and only one instance should run on any given node.
 *
 * The Calpont configuration file should contain entries for the Master
 * and every Slave node on the system.
 *
 * Config file entries look like
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

class SlaveDBRMNode {
	public:
		EXPORT SlaveDBRMNode() throw();
		EXPORT ~SlaveDBRMNode() throw();

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
						 uint16_t  dbRoot,
						 uint32_t& partitionNum,
						 uint16_t& segmentNum,
                         std::vector<CreateStripeColumnExtentsArgOut>& extents) throw();
		
		/** @brief Allocate extent in the specified segment file
		 *
		 * Allocate column extent for the exact segment file specified by the
		 * requested OID,DBRoot, partition, and segment.
		 * @param OID (in) The OID requesting the extent.
		 * @param colWidth (in) Column width of the OID.
		 * @param dbRoot (in) DBRoot where extent is to be added.
		 * @param partitionNum (in) Partition number in file path.
		 * @param segmentNum (in) Segment number in file path.
         * @param colDataType (in) the column type
		 * @param lbid (out) The first LBID of the extent created.
		 * @param allocdSize (out) The total number of LBIDs allocated.
         * @param startBlockOffset (out) The first block of the extent created. 
		 * @return 0 on success, -1 on error
		 */
		 EXPORT int createColumnExtentExactFile(OID_t oid,
						 uint32_t  colWidth,
						 uint16_t  dbRoot,
						 uint32_t  partitionNum,
						 uint16_t  segmentNum,
                         execplan::CalpontSystemCatalog::ColDataType colDataType,
						 LBID_t&    lbid,
						 int&       allocdSize,
						 uint32_t& startBlockOffset) throw();

		/** @brief Allocate an extent for a column file
		 *
		 * Allocate a column extent for the specified OID and DBRoot.
		 * @param OID (in) The OID requesting the extent.
		 * @param colWidth (in) Column width of the OID.
		 * @param dbRoot (in) DBRoot where extent is to be added.
         * @param colDataType (in) the column type
		 * @param partitionNum (in/out) Partition number in file path.
		 *        If allocating OID's first extent for this DBRoot, then
		 *        partitionNum is input, else it is an output arg.
		 * @param segmentNum (in/out) Segment number in file path.
		 *        If allocating OID's first extent for this DBRoot, then
		 *        segmentNum is input, else it is an output arg.
		 * @param lbid (out) The first LBID of the extent created.
		 * @param allocdSize (out) The total number of LBIDs allocated.
		 * @param startBlockOffset (out) The first block of the extent created.
		 * @return 0 on success, -1 on error
		 */
		 EXPORT int createColumnExtent_DBroot(OID_t oid,
						 uint32_t  colWidth,
						 uint16_t  dbRoot,
                         execplan::CalpontSystemCatalog::ColDataType colDataType,
                         uint32_t& partitionNum,
						 uint16_t& segmentNum,
						 LBID_t&    lbid,
						 int&       allocdSize,
						 uint32_t& startBlockOffset) throw();

		/** @brief Allocate an extent for a dictionary store file
		 *
		 * Allocate a dictionary store extent for the specified OID, dbRoot,
		 * partition number, and segment number.
		 * @param OID (in) The OID requesting the extent.
		 * @param dbRoot (in) DBRoot to assign to the extent.
		 * @param partitionNum (in) Partition number to assign to the extent.
		 * @param segmentNum (in) Segment number to assign to the extent.
		 * @param lbid (out) The first LBID of the extent created.
		 * @param allocdSize (out) The total number of LBIDs allocated.
		 * @return 0 on success, -1 on error
		 */
		 EXPORT int createDictStoreExtent(OID_t oid,
						 uint16_t  dbRoot,
						 uint32_t  partitionNum,
						 uint16_t  segmentNum,
						 LBID_t&    lbid,
						 int&       allocdSize) throw();

		/** @brief Rollback (delete) a set of extents for the specified OID.
		 *
		 * Deletes all the extents that logically follow the specified
		 * column extent; and sets the HWM for the specified extent.
		 * @param oid OID of the extents to be deleted.
		 * @param partitionNum Last partition to be kept.
		 * @param segmentNum Last segment in partitionNum to be kept.
		 * @param hwm HWM to be assigned to the last extent that is kept.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int rollbackColumnExtents(OID_t oid,
						 uint32_t partitionNum,	
						 uint16_t segmentNum,
						 HWM_t    hwm) throw();

		/** @brief Rollback (delete) set of extents for specified OID & DBRoot.
		 *
		 * Deletes all the extents that logically follow the specified
		 * column extent; and sets the HWM for the specified extent.
		 * @param oid OID of the extents to be deleted.
		 * @param bDeleteAll Indicates if all extents in oid and dbroot are to
		 *        be deleted; else part#, seg#, and hwm are used.
		 * @param dbRoot DBRoot of the extents to be deleted.
		 * @param partitionNum Last partition to be kept.
		 * @param segmentNum Last segment in partitionNum to be kept.
		 * @param hwm HWM to be assigned to the last extent that is kept.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int rollbackColumnExtents_DBroot(OID_t oid,
						 bool     bDeleteAll,
						 uint16_t dbRoot,
						 uint32_t partitionNum,	
						 uint16_t segmentNum,
						 HWM_t    hwm) throw();

		/** @brief Rollback (delete) a set of dict store extents for an OID.
		 *
		 * Arguments specify the last stripe.  Any extents after this are
		 * deleted.  The hwm's of the extents in the last stripe are updated
		 * based on the contents of the hwm vector.  If hwms is a partial list,
		 * (as in the first stripe of a partition), then any extents in sub-
		 *  sequent segment files for that partition are deleted.
		 * @param oid OID of the extents to be deleted or updated.
		 * @param partitionNum Last partition to be kept.
		 * @param hwms Vector of hwms for the last partition to be kept.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int rollbackDictStoreExtents(OID_t oid,
						 uint32_t         partitionNum,
						 const std::vector<HWM_t>& hwms) throw ();

		/** @brief Rollback (delete) a set of dict store extents for an OID &
		 *  DBRoot.
		 *
		 * Arguments specify the last stripe.  Any extents after this are
		 * deleted.  The hwm's of the extents in the last stripe are updated
		 * based on the contents of the hwm vector.  If hwms is a partial list,
		 * (as in the first stripe of a partition), then any extents in sub-
		 *  sequent segment files for that partition are deleted.  If hwms is
		 * empty then all the extents in dbRoot are deleted.
		 * @param oid OID of the extents to be deleted or updated.
		 * @param dbRoot DBRoot of the extents to be deleted.
		 * @param partitionNum Last partition to be kept.
		 * @param hwms Vector of hwms for the last partition to be kept.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int rollbackDictStoreExtents_DBroot(OID_t oid,
						 uint16_t          dbRoot,
						 uint32_t         partitionNum,
						 const std::vector<uint16_t>& segNums,
						 const std::vector<HWM_t>& hwms) throw ();
						 
		/** @brief delete of column extents for the specified extents.
		 *
		 * Deletes the extents from extent map 
		 * @param extentInfo the information for extents
		 */
		EXPORT int deleteEmptyColExtents(const ExtentsInfoMap_t& extentsInfo)  throw();
		
		/** @brief delete of dictionary extents for the specified extents.
		 *
		 * Deletes the extents from extent map 
		 * @param extentInfo the information for extents
		 */
		EXPORT int deleteEmptyDictStoreExtents(const ExtentsInfoMap_t& extentsInfo)  throw();
		
		/** @brief Delete the extents of an OID and invalidate VSS references to them
		 *
		 * Delete the extents assigned to an OID and deletes entries in the VSS
		 * that refer to the LBIDs used by it.
		 * @note The old version of this function deliberately did not delete the entries
		 * in the version buffer.
		 * @note This function is ridiculously slow right now.
		 * @param OID The OID of the object being deleted
		 * @return 0 on success, -1 on error
		 */
		EXPORT int deleteOID(OID_t oid) throw();

		/** @brief Delete the extents of OIDs and invalidate VSS references to them
		 *
		 * Delete the extents assigned to OIDs and deletes entries in the VSS
		 * that refer to the LBIDs used by it.
		 * @note The old version of this function deliberately did not delete the entries
		 * in the version buffer.
		 * @param OIDs The OIDs of the object being deleted
		 * @return 0 on success, -1 on error
		 */
		EXPORT int deleteOIDs(const OidsMap_t&  oids) throw();
		
		/** @brief Set the "high water mark" of an OID, partition, segment
		 * 
		 * Set the high water mark (aka, the highest numbered written
		 * block offset) for a specific OID, partition, segment file.
		 * @param oid (in) The OID
		 * @param partitionNum (in) The relevant partition number
		 * @param segmentNum (in) The relevant segment number
		 * @param hwm (in) The high water mark of oid
		 * @return 0 on success, -1 on error
		 */
		EXPORT int setLocalHWM(OID_t, uint32_t partitionNum, uint16_t segmentNum,
	                   HWM_t hwm, bool firstNode) throw();

		EXPORT int bulkSetHWM(const std::vector<BulkSetHWMArg> &, VER_t transID,
				bool firstNode) throw();

		EXPORT int bulkSetHWMAndCP(const std::vector<BulkSetHWMArg> &hwmArgs,
				const std::vector<CPInfo> & setCPDataArgs,
				const std::vector<CPInfoMerge> & mergeCPDataArgs,
				VER_t transID, bool firstNode) throw();

		EXPORT int bulkUpdateDBRoot(const std::vector<BulkUpdateDBRootArg> &) throw();

		/** @brief Delete a Partition for the specified OID(s).
		 *
		 * @param OID (in) the OID of interest.
		 * @param partitionNums (in) the set of partitions to be deleted.
		 */
		EXPORT int deletePartition(const std::set<OID_t>& oids,
						std::set<LogicalPartition>& partitionNums, std::string& emsg) throw();

		/** @brief Mark a Partition for the specified OID(s) as out of service.
		 *
		 * @param OID (in) the OID of interest.
		 * @param partitionNums (in) the set of partitions to be marked out of service.
		 */
		EXPORT int markPartitionForDeletion(const std::set<OID_t>& oids,
						std::set<LogicalPartition>& partitionNums, std::string& emsg) throw();
						
		/** @brief Mark all Partitions for the specified OID(s) as out of service.
		 *
		 * @param OID (in) the OID of interest.
		 */
		EXPORT int markAllPartitionForDeletion(const std::set<OID_t>& oids) throw();

		/** @brief Restore a Partition for the specified OID(s).
		 *
		 * @param OID (in) the OID of interest.
		 * @param partitionNums (in) the set of partitions to be restored.
		 */
		EXPORT int restorePartition(const std::set<OID_t>& oids,
						std::set<LogicalPartition>& partitionNum, std::string& emsg) throw();

		/** @brief Delete all extent map rows for the specified dbroot
		 *
		 * @param dbroot (in) the dbroot
		 */
		EXPORT int deleteDBRoot(uint16_t dbroot) throw();

		/** @brief Registers a version buffer entry.
		 *
		 * Registers a version buffer entry at <vbOID, vbFBO> with
		 * values of <transID, lbid>.
		 * @note The version buffer locations must hold the 'copy' lock
		 * first.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int writeVBEntry(VER_t transID, LBID_t lbid, OID_t vbOID, 
						 uint32_t vbFBO) throw();
		
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
		 * @return 0 on success, -1 on error.
		 */

		/* Note, the params to the analogous DBRM class fcn are slightly different.
		 * It takes a DBRoot param instead of a VB OID.  The conversion is
		 * done in the controllernode b/c the OID server is housed there.
		 */
		EXPORT int beginVBCopy(VER_t transID, uint16_t vbOID,
				const LBIDRange_v& ranges, VBRange_v& freeList, bool flushPMCache) throw();
		
		/** @brief Atomically unset the copy lock & update the VSS.  Beware!  Read the warning!
		 * 
		 * Atomically unset the copy lock for the specified LBID ranges
		 * and add a new locked VSS entry for each LBID in the range.
		 * @note The elements of the ranges parameter <b>MUST</b> be the
		 * same elements passed to beginVBCopy().  The number and order of the
		 * elements can be different, but every element in ranges must also
		 * have been an element in beginVBCopy's ranges.
		 * @return 0 on success, -1 on error.
		 */
		EXPORT int endVBCopy(VER_t transID, const LBIDRange_v& ranges) 
				throw();
		
		/** @brief Commit the changes made for the given transaction.
		 *
		 * This unlocks the VSS entries with VerID = transID.
		 * @return 0 on success, -1 on error.
		 */
		EXPORT int vbCommit(VER_t transID) throw();
		
		/** @brief Reverse the changes made during the given transaction.
		 *
		 * Record that the given LBID was reverted to version verID.
		 * @warning This removes the copy locks held on all ranges by transID.
		 * @param transID The transaction ID
		 * @param lbidList The list of ranges to rollback.
		 * @param verID The version of the block now in the database.
		 * @return 0 on success, -1 on error.
		 */
		EXPORT int vbRollback(VER_t transID, const LBIDRange_v& lbidList,
				bool flushPMCache) throw();

		/** @brief Reverse the changes made during the given transaction.
		 *
		 * Record that the given LBID was reverted to version verID.
		 * @warning This removes the copy locks held on all ranges by transID.
		 * @param transID The transaction ID
		 * @param lbidList The list of singular LBIDs to rollback.
		 * @param verID The version of the block now in the database.
		 * @return 0 on success, -1 on error.
		 */
		EXPORT int vbRollback(VER_t transID, const std::vector<LBID_t>& lbidList,
				bool flushPMCache) throw();

		EXPORT int clear() throw();

		/** @brief Check the consistency of each data structure
		 *
		 * Check the consistency of each data structure
		 * @return 0 on success, -1 on error.
		 */
		EXPORT int checkConsistency() throw();

		EXPORT void confirmChanges() throw();
		EXPORT void undoChanges() throw();

		EXPORT int loadExtentMap(const std::string &filename);
 		EXPORT int saveExtentMap(const std::string &filename);

		// Casual partitioning support
		//
		/** @brief mark the extent containing the lbid as not having valid max and min values
		*
		**/
		EXPORT int markExtentInvalid(const LBID_t lbid,
                execplan::CalpontSystemCatalog::ColDataType colDataType);
		EXPORT int markExtentsInvalid(const std::vector<LBID_t> &lbids,
                const std::vector<execplan::CalpontSystemCatalog::ColDataType>& colDataTypes);

		/** @brief update the extent with the lbidRange with max, min, & seqNum values
		*
		**/
		EXPORT int setExtentMaxMin(const LBID_t lbid, const int64_t max, const int64_t min,
				const int32_t seqNum, bool firstNode);

		// Casual partitioning support
		//
		/** @brief Sets min and max values from the information in the passed map.  
		 *   
		 *   @param cpMaxMinMap - Map with cp info.  The key must be the starting LBID.  
		 *
		 **/
		// @bug 1970.  Added setExtentsMaxMin. 
		EXPORT int setExtentsMaxMin(const CPMaxMinMap_t &cpMaxMinMap, bool firstNode);

		/** @brief Merges list of min/max values with current CP min/max info
		 *
		 *  @param cpMaxMinMap - Map with CP info.  The key is the starting LBID
		 */
		EXPORT int mergeExtentsMaxMin(CPMaxMinMergeMap_t &cpMaxMinMap);

		/* Write-side copylocks interface */
		EXPORT int dmlLockLBIDRanges(const std::vector<LBIDRange> &ranges, int txnID);
		EXPORT int dmlReleaseLBIDRanges(const std::vector<LBIDRange> &ranges);

		EXPORT int loadState(std::string filename) throw();
		EXPORT int saveState(std::string filename) throw();

		EXPORT const bool *getEMFLLockStatus();
		EXPORT const bool *getEMLockStatus();
		EXPORT const bool *getVBBMLockStatus();
		EXPORT const bool *getVSSLockStatus();

	private:
		explicit SlaveDBRMNode(const SlaveDBRMNode& brm);
		SlaveDBRMNode& operator=(const SlaveDBRMNode& brm);
		int lookup(OID_t oid, LBIDRange_v& lbidList) throw();

		MasterSegmentTable mst;
		ExtentMap em;
		VBBM vbbm;
		VSS vss;
		CopyLocks copylocks;
		bool locked[3];  // 0 = VBBM, 1 = VSS, 2 = CopyLocks
		
};

}

#undef EXPORT

#endif 
