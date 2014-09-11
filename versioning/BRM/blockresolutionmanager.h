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
 * $Id: blockresolutionmanager.h 1266 2011-02-08 14:36:09Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class BlockResolutionManager
 */

#ifndef BLOCKRESOLUTIONMANAGER_H_
#define BLOCKRESOLUTIONMANAGER_H_

#include <sys/types.h>
#include <vector>
#include <set>

#include "brmtypes.h"
#include "mastersegmenttable.h"
#include "extentmap.h"
#include "vbbm.h"
#include "vss.h"
#include "copylocks.h"

#if defined(_MSC_VER) && defined(BLOCKRESOLUTIONMANAGER_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

/** @brief The BlockResolutionManager manages the Logical Block ID space.
 *
 * The BlockResolutionManager manages the Logical Block ID space.  Its
 * primary use is to translate <LBID, VerID, VBFlag> triples
 * to <OID, FBO> pairs and vice-versa.
 *
 * @note This class will be used by C code, so it should not throw exceptions.
 */
class BlockResolutionManager {
	public:
		EXPORT explicit BlockResolutionManager(bool ronly = false) throw();
		EXPORT ~BlockResolutionManager() throw();
		
		/**@brief Do a VSS lookup.
		 *
		 * Do a VSS lookup.  Gets the version ID of the block the caller should use
		 * and determines whether it is in the version buffer or the main database.
		 * @param lbid (in) The block number
		 * @param verID (in/out) The input value is the version requested, 
		 * the output value is the value the caller should use.
		 * @param txnID (in) If the caller has a transaction ID, put it here.  
		 * Otherwise use 0.
		 * @param vbFlag (out) If true, the block is in the version buffer; 
		 * false, the block is in the main database.
		 * @param vbOnly (in) If true, it will only consider entries in the Version Buffer
		 * (vbFlag will be true on return).  If false, it will also consider the main DB
		 * entries.  This defaults to false.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int vssLookup(LBID_t lbid, VER_t& verID, VER_t txnID, bool& vbFlag, 
					  bool vbOnly = false) throw();
		
		/** @brief Get a complete list of LBIDs assigned to an OID
		 *
		 * Get a complete list of LBIDs assigned to an OID.
		 */
		EXPORT int lookup(OID_t oid, LBIDRange_v& lbidList) throw();

		// Casual partitioning support
		//
		/** @brief mark the extent containing the lbid as not having valid max and min values
		*
		**/
		EXPORT int markExtentInvalid(const LBID_t lbid);

		/** @brief set the extent with the lbidRange to max, min, & seqNum values
		*
		**/
		EXPORT int setExtentMaxMin(const LBID_t lbid, const int64_t max, const int64_t min, const int32_t seqNum);

		/** @brief retrieve the max, min, seqnum values for the extent containing the lbidRange
		*
		**/
		EXPORT int getExtentMaxMin(const LBID_t lbid, int64_t& max, int64_t& min, int32_t& seqNum);

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
		
		/** @brief Gets the number of LBIDs in an extent
		 * 
		 * Gets the number of rows in an extent.
		 * @return The extent size in rows
		 */
		EXPORT int getExtentRows() throw();
		
		/** @brief Registers a version buffer entry.
		 *
		 * Registers a version buffer entry at <vbOID, vbFBO> with
		 * values of <transID, lbid>.
		 * @note The version buffer locations must hold the 'copy' lock
		 * first.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int writeVBEntry(VER_t transID, LBID_t lbid, OID_t vbOID, 
						 u_int32_t vbFBO) throw();
		
		/** @brief Retrieves a list of uncommitted LBIDs.
		 * 
		 * Retrieves a list of uncommitted LBIDs for the given transaction ID.
		 * @param lbidList (out) On success this contains the ranges of LBIDs
		 * @return 0 on success, -1 on error.
		 */
		EXPORT int getUncommittedLBIDs(VER_t transID, 
								std::vector<LBID_t>& lbidList)
				throw();
		
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
		EXPORT int beginVBCopy(VER_t transID, const LBIDRange_v& ranges, 
					  VBRange_v& freeList) throw();
		
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
		EXPORT int vbRollback(VER_t transID, const LBIDRange_v& lbidList) 
				throw();

		/** @brief Reverse the changes made during the given transaction.
		 *
		 * Record that the given LBID was reverted to version verID.
		 * @warning This removes the copy locks held on all ranges by transID.
		 * @param transID The transaction ID
		 * @param lbidList The lsit of singular LBIDs to rollback.
		 * @param verID The version of the block now in the database.
		 * @return 0 on success, -1 on error.
		 */
		EXPORT int vbRollback(VER_t transID, const std::vector<LBID_t>& lbidList) 
				throw();

		/** @brief Persistence API.  Loads the local Extent Map from a file.
		 *
		 * Persistence API.  Loads the <b>local</b> Extent Map from a file.
		 * 
		 * @warning The load must be done on each slave node atomically wrt
		 * writing operations, otherwise nodes may be out of synch.
		 * @param filename Relative or absolute path to a file saved with saveExtentMap.
		 * @return 0, throws if EM throws
		 */
		EXPORT int loadExtentMap(const std::string& filename, bool fixFL);

		/** @brief Persistence API.  Saves the local Extent Map to a file.
		 * 
		 * Persistence API.  Saves the <b>local</b> Extent Map to a file.
		 *
		 * @param filename Relative or absolute path to save to.
		 * @return 0 on success, throws if EM throws
		 */
		EXPORT int saveExtentMap(const std::string& filename);

		/** @brief Check the consistency of each data structure
		 *
		 * Check the consistency of each data structure
		 * @return 0 on success, -1 on error.
		 */
		EXPORT int checkConsistency() throw();

		/** @brief Get a list of the transactions currently in progress.
		 * 
		 * Get a list of the transactions currently in progress.  This scans
		 * the copy locks & VSS for LBIDs that are locked by some transaction
		 * and stores the transaction ID.
		 * 
		 * @param txnList Caller-supplied set to store the results in.
		 * @return 0 on success, -1 on error.
		 */
		EXPORT int getCurrentTxnIDs(std::set<VER_t> &txnList) throw();

		/** @brief Persistence API.  Loads all BRM snapshots.
		 *
		 * Loads all <b>local</b> BRM structures from files saved with saveState().
		 *
		 * @warning The load must be done on each slave node atomically wrt
		 * writing operations, otherwise nodes may be out of synch.
		 * @param filename The filename prefix to use.  Loads 4 files with that prefix.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int loadState(std::string filename, bool fixFL=false) throw();

		/** @brief Persistence API.  Loads the BRM deltas since the last snapshot.
		 *
		 * Loads all <b>local</b> BRM structures from files saved with saveState().
		 *
		 * @warning The load must be done on each slave node atomically wrt
		 * writing operations, otherwise nodes may be out of synch.
		 * @param filename The filename prefix to use.  Loads 4 files with that prefix.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int replayJournal(std::string filename) throw();

		/** @brief Persistence API.  Saves all BRM structures.
		 *
		 * Saves all <b>local</b> BRM structures to files.
		 *
		 * @param filename The filename prefix to use.  Saves 4 files with that prefix.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int saveState(std::string filename) throw();
		
	private:
		explicit BlockResolutionManager(const BlockResolutionManager& brm);
		BlockResolutionManager& operator=(const BlockResolutionManager& brm);
		MasterSegmentTable mst;
		ExtentMap em;
		VBBM vbbm;
		VSS vss;
		CopyLocks copylocks;
		
};

}

#undef EXPORT

#endif 
