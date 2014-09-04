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
 * $Id: extentmap.h 1938 2013-07-11 17:06:49Z dhall $
 *
 *****************************************************************************/

/** @file 
 * class ExtentMap
 */

#ifndef _EXTENTMAP_H_
#define _EXTENTMAP_H_

#include <sys/types.h>
#include <vector>
#include <set>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
//#define NDEBUG
#include <cassert>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "shmkeys.h"
#include "brmtypes.h"
#include "mastersegmenttable.h"
#include "undoable.h"

#include "brmshmimpl.h"
#include "exceptclasses.h"

#ifdef NONE
#undef NONE
#endif
#ifdef READ
#undef READ
#endif
#ifdef WRITE
#undef WRITE
#endif

#if defined(_MSC_VER) && defined(xxxEXTENTMAP_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace oam {
	typedef std::vector<uint16_t> DBRootConfigList;
}

namespace BRM {

// assumed column width when calculating dictionary store extent size
#define DICT_COL_WIDTH 8

// valid values for EMEntry.status
const int16_t EXTENTSTATUSMIN(0); // equal to minimum valid status value
const int16_t EXTENTAVAILABLE(0);
const int16_t EXTENTUNAVAILABLE(1);
const int16_t EXTENTOUTOFSERVICE(2);
const int16_t EXTENTSTATUSMAX(2); // equal to maximum valid status value

enum partition_type_enum {
	PART_DEFAULT=0,
	PART_CASUAL,
	PART_RANGE
};
typedef partition_type_enum EMPartitionType_t;
typedef int64_t RangePartitionData_t;

const char CP_INVALID=0;
const char CP_UPDATING=1;
const char CP_VALID=2;

struct EMCasualPartition_struct {
	RangePartitionData_t hi_val;
	RangePartitionData_t lo_val;
	int32_t sequenceNum;
	char isValid; //CP_INVALID - No min/max and no DML in progress. CP_UPDATING - Update in progress. CP_VALID- min/max is valid
	EXPORT EMCasualPartition_struct();
	EXPORT EMCasualPartition_struct(const int64_t lo, const int64_t hi, const int32_t seqNum);
	EXPORT EMCasualPartition_struct(const EMCasualPartition_struct& em);
	EXPORT EMCasualPartition_struct& operator= (const EMCasualPartition_struct& em);
};
typedef EMCasualPartition_struct EMCasualPartition_t;

struct EMPartition_struct {
	EMCasualPartition_t		cprange;
};
typedef EMPartition_struct EMPartition_t;

struct EMPartition_struct_V3 {
	EMPartitionType_t 		type;
#ifndef __LP64__
	int32_t pad3;
#endif
	EMCasualPartition_t		cprange;
};
typedef EMPartition_struct_V3 EMPartition_V3_t;

struct EMEntry {
	InlineLBIDRange range;
	int fileID;
	u_int32_t blockOffset;
	HWM_t HWM;
	uint32_t	partitionNum; // starts at 0
	uint16_t	segmentNum;   // starts at 0
	uint16_t	dbRoot;       // starts at 1 to match Calpont.xml
	uint16_t	colWid;
	int16_t 	status;       //extent avail for query or not, or out of service
	EMPartition_t partition;
	EXPORT EMEntry();
	EXPORT EMEntry(const EMEntry&);
	EXPORT EMEntry& operator= (const EMEntry&);
	EXPORT bool operator< (const EMEntry&) const;
};

struct EMEntry_V3 {
	InlineLBIDRange range;
	int fileID;
	u_int32_t blockOffset;
	HWM_t HWM;
	u_int32_t txnID;
	HWM_t secondHWM;
#ifndef __LP64__
	int32_t pad2;
#endif
	uint64_t nextHeader;	// a var like HWM for use by the write engine
	EMPartition_V3_t partition;
	EXPORT EMEntry_V3();
	EXPORT EMEntry_V3(const EMEntry_V3&);
	EXPORT EMEntry_V3& operator= (const EMEntry_V3&);
	EXPORT bool operator< (const EMEntry_V3&) const;
};

// Bug 2989, moved from joblist
struct ExtentSorter
{
	bool operator()(const EMEntry &e1, const EMEntry &e2)
	{
		if (e1.dbRoot < e2.dbRoot)
			return true;
		if (e1.dbRoot == e2.dbRoot && e1.partitionNum < e2.partitionNum)
			return true;
		if (e1.dbRoot == e2.dbRoot && e1.partitionNum == e2.partitionNum && e1.blockOffset < e2.blockOffset)
			return true;
		if (e1.dbRoot == e2.dbRoot && e1.partitionNum == e2.partitionNum && e1.blockOffset == e2.blockOffset && e1.segmentNum < e2.segmentNum)
			return true;

		return false;
	}
};

class ExtentMapImpl
{
public:
	static ExtentMapImpl* makeExtentMapImpl(unsigned key, off_t size, bool readOnly=false);

	inline void grow(unsigned key, off_t size)
#ifdef NDEBUG
		{ fExtMap.grow(key, size); }
#else
		{ int rc=fExtMap.grow(key, size); idbassert(rc==0); }
#endif
	inline void makeReadOnly() { fExtMap.setReadOnly(); }
	inline void clear(unsigned key, off_t size) { fExtMap.clear(key, size); }
	inline void swapout(BRMShmImpl& rhs) { fExtMap.swap(rhs); rhs.destroy(); }
	inline unsigned key() const { return fExtMap.key(); }

	inline EMEntry* get() const { return reinterpret_cast<EMEntry*>(fExtMap.fMapreg.get_address()); }

private:
	ExtentMapImpl(unsigned key, off_t size, bool readOnly=false);
	~ExtentMapImpl();
	ExtentMapImpl(const ExtentMapImpl& rhs);
	ExtentMapImpl& operator=(const ExtentMapImpl& rhs);

	BRMShmImpl fExtMap;

	static boost::mutex fInstanceMutex;
	static ExtentMapImpl* fInstance;
};

class FreeListImpl
{
public:
	static FreeListImpl* makeFreeListImpl(unsigned key, off_t size, bool readOnly=false);

	inline void grow(unsigned key, off_t size)
#ifdef NDEBUG
		{ fFreeList.grow(key, size); }
#else
		{ int rc=fFreeList.grow(key, size); idbassert(rc==0); }
#endif
	inline void makeReadOnly() { fFreeList.setReadOnly(); }
	inline void clear(unsigned key, off_t size) { fFreeList.clear(key, size); }
	inline void swapout(BRMShmImpl& rhs) { fFreeList.swap(rhs); rhs.destroy(); }
	inline unsigned key() const { return fFreeList.key(); }

	inline InlineLBIDRange* get() const { return reinterpret_cast<InlineLBIDRange*>(fFreeList.fMapreg.get_address()); }

private:
	FreeListImpl(unsigned key, off_t size, bool readOnly=false);
	~FreeListImpl();
	FreeListImpl(const FreeListImpl& rhs);
	FreeListImpl& operator=(const FreeListImpl& rhs);

	BRMShmImpl fFreeList;

	static boost::mutex fInstanceMutex;
	static FreeListImpl* fInstance;
};

class ExtentMapConverter;

/** @brief This class encapsulates the extent map functionality of the system
 *
 * This class encapsulates the extent map functionality of the system.  It
 * is currently implemented in the quickest-to-write (aka dumb) way to
 * get something working into the hands of the other developers ASAP.  
 * The Extent Map shared data should be implemented in a more scalable
 * structure such as a tree or hash table.
 */
class ExtentMap : public Undoable {
public:
	EXPORT ExtentMap();
	EXPORT ~ExtentMap();
	
	/** @brief Loads the ExtentMap entries from a file
	 * 
	 * Loads the ExtentMap entries from a file.  This will
	 * clear out any existing entries.  The intention is that before
	 * the system starts, an external tool instantiates a single Extent
	 * Map and loads the stored entries.
	 * @param filename The file to load from.
	 * @note Throws an ios_base::failure exception on an IO error, runtime_error
	 * if the file "looks" bad.
	 */
	EXPORT void load(const std::string& filename, bool fixFL=false);

	/** @brief Saves the ExtentMap entries to a file
	 * 
	 * Saves the ExtentMap entries to a file.
	 * @param filename The file to save to.
	 */
	EXPORT void save(const std::string& filename);
	
	// @bug 1509.  Added new version of lookup below.
	/** @brief Returns the first and last LBID in the range for a given LBID
	 *
	 * Get the first and last LBID for the extent that contains the given LBID.
	 * @param LBID       (in) The lbid to search for
	 * @param firstLBID (out) The first lbid for the extent
	 * @param lastLBID  (out) the last lbid for the extent
	 * @return 0 on success, -1 on error
	 */
	EXPORT int lookup(LBID_t LBID, LBID_t& firstLBID, LBID_t& lastLBID);

	// @bug 1055+.  New functions added for multiple files per OID enhancement.

	/** @brief Look up the OID and file block offset assiciated with an LBID
	 *
	 * Look up the OID and file block offset assiciated with an LBID
	 * @param LBID (in) The lbid to search for
	 * @param OID (out) The OID associated with lbid
	 * @param dbRoot (out) The db root containing the LBID
	 * @param partitionNum (out) The partition containing the LBID
	 * @param segmentNum (out) The segment containing the LBID
	 * @param fileBlockOffset (out) The file block offset associated
	 * with LBID
	 * @return 0 on success, -1 on error
	 */
	EXPORT int lookupLocal(LBID_t LBID, int& OID, uint16_t& dbRoot, uint32_t& partitionNum, uint16_t& segmentNum, u_int32_t& fileBlockOffset);

	/** @brief Look up the LBID associated with a given OID, offset, partition, and segment.
	 *
	 * Look up the LBID associated with a given OID, offset, partition, and segment.
	 * @param OID (in) The OID to look up
	 * @param fileBlockOffset (in) The file block offset
	 * @param partitionNum (in) The partition containing the lbid
	 * @param segmentNum (in) The segement containing the lbid
	 * @param LBID (out) The LBID associated with the given offset of the OID.
	 * @return 0 on success, -1 on error
	 */
	EXPORT int lookupLocal(int OID, uint32_t partitionNum, uint16_t segmentNum, uint32_t fileBlockOffset, LBID_t& LBID);

	/** @brief Look up the LBID associated with a given dbroot, OID, offset,
	 * partition, and segment.
	 *
	 * Look up LBID associated with a given OID, offset, partition, and segment.
	 * @param OID (in) The OID to look up
	 * @param fileBlockOffset (in) The file block offset
	 * @param partitionNum (in) The partition containing the lbid
	 * @param segmentNum (in) The segement containing the lbid
	 * @param LBID (out) The LBID associated with the given offset of the OID.
	 * @return 0 on success, -1 on error
	 */
	EXPORT int lookupLocal_DBroot(int OID, uint16_t dbroot,
		uint32_t partitionNum, uint16_t segmentNum, uint32_t fileBlockOffset,
		LBID_t& LBID);

	// @bug 1055-.		

	/** @brief Look up the starting LBID associated with a given OID,
	 *  partition, segment, and offset.
	 *
	 * @param OID (in) The OID to look up
	 * @param partitionNum (in) The partition containing the lbid
	 * @param segmentNum (in) The segement containing the lbid
	 * @param fileBlockOffset (in) The file block offset
	 * @param LBID (out) The starting LBID associated with the extent
	 *        containing the given offset
	 * @return 0 on success, -1 on error
	 */
	int lookupLocalStartLbid(int OID,
							 uint32_t partitionNum,
							 uint16_t segmentNum,
							 uint32_t fileBlockOffset,
							 LBID_t& LBID);

	/** @brief Get a complete list of LBID ranges assigned to an OID
	 *
	 * Get a complete list of LBID ranges assigned to an OID.
	 */
	EXPORT void lookup(OID_t oid, LBIDRange_v& ranges);

	/** @brief Allocate a "stripe" of extents for columns in a table (in DBRoot)
	 *
	 * If this is the first extent for the OID/DBRoot, it will start at
	 * file offset 0.  If space for the OID already exists, the new
	 * extent will "logically" be appended to the end of the already-
	 * allocated space, although the extent may reside in a different
	 * physical file as indicated by dbRoot, partition, and segment.
	 * Partition and segment numbers are 0 based, dbRoot is 1 based.
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
	EXPORT void createStripeColumnExtents(
					const std::vector<CreateStripeColumnExtentsArgIn>& cols,
					u_int16_t  dbRoot,
					u_int32_t& partitionNum,
					u_int16_t& segmentNum,
					std::vector<CreateStripeColumnExtentsArgOut>& extents);
	
	/** @brief Allocates an extent for a column file
	 * 
	 * Allocates an extent for the specified OID and DBroot.
	 * If this is the first extent for the OID/DBRoot, it will start at
	 * file offset 0.  If space for the OID already exists, the new
	 * extent will "logically" be appended to the end of the already-
	 * allocated space, although the extent may reside in a different
	 * physical file as indicated by dbRoot, partition, and segment.
	 * Partition and segment numbers are 0 based, dbRoot is 1 based.
	 *
	 * @param OID (in) The OID requesting the extent.
	 * @param colWidth (in) Column width of the OID.
	 * @param dbRoot (in) DBRoot where extent is to be added.
	 * @param partitionNum (in/out) Partition number in file path.
	 *        If allocating OID's first extent for this DBRoot, then
	 *        partitionNum is input, else it is an output arg.
	 * @param segmentNum (out) Segment number assigned to the extent.
	 * @param lbid (out) The first LBID of the extent created.
	 * @param allocdsize (out) The total number of LBIDs allocated.
	 * @param startBlockOffset (out) The first block of the extent created.
	 * @param useLock Grab ExtentMap and FreeList WRITE lock to perform work
	 */
	// @bug 4091: To be deprecated as public function.  Should just be a
	// private function used by createStripeColumnExtents().
	EXPORT void createColumnExtent_DBroot(int OID,
					u_int32_t  colWidth,
					u_int16_t  dbRoot,
					u_int32_t& partitionNum,
					u_int16_t& segmentNum,
					LBID_t&    lbid,
					int&       allocdsize,
					u_int32_t& startBlockOffset,
					bool       useLock = true);

	/** @brief Allocates extent for exact file that is specified
	 * 
	 * Allocates an extent for the exact file specified by OID, DBRoot,
	 * partition, and segment.
	 * If this is the first extent for the OID/DBRoot, it will start at
	 * file offset 0.  If space for the OID already exists, the new
	 * extent will "logically" be appended to the end of the already-
	 * allocated space.
	 * Partition and segment numbers are 0 based, dbRoot is 1 based.
	 *
	 * @param OID (in) The OID requesting the extent.
	 * @param colWidth (in) Column width of the OID.
	 * @param dbRoot (in) DBRoot where extent is to be added.
	 * @param partitionNum (in) Partition number in file path.
	 *        If allocating OID's first extent for this DBRoot, then
	 *        partitionNum is input, else it is an output arg.
	 * @param segmentNum (in) Segment number in file path.
	 *        If allocating OID's first extent for this DBRoot, then
	 *        segmentNum is input, else it is an output arg.
	 * @param lbid (out) The first LBID of the extent created. 
	 * @param allocdSize (out) The total number of LBIDs allocated.
	 * @param startBlockOffset (out) The first block of the extent created.
	 */
	EXPORT void createColumnExtentExactFile(int OID,
					u_int32_t  colWidth,
					u_int16_t  dbRoot,
					u_int32_t  partitionNum,
					u_int16_t  segmentNum,
					LBID_t&    lbid,
					int&       allocdsize,
					u_int32_t& startBlockOffset);

	/** @brief Allocates an extent for a dictionary store file
	 * 
	 * Allocates an extent for the specified dictionary store OID,
	 * dbRoot, partition number, and segment number.   These should
	 * correlate with those belonging to the corresponding token file.
	 * The first extent for each store file will start at file offset 0.
	 * Other extents will be appended to the end of the already-
	 * allocated space for the same store file.
	 * Partition and segment numbers are 0 based, dbRoot is 1 based.
	 *
	 * @param OID (in) The OID requesting the extent.
	 * @param dbRoot (in) DBRoot to assign to the extent.
	 * @param partitionNum (in) Partition number to assign to the extent.
	 * @param segmentNum (in) Segment number to assign to the extent.
	 * @param lbid (out) The first LBID of the extent created.
	 * @param allocdsize (out) The total number of LBIDs allocated.
	 */
	EXPORT void createDictStoreExtent(int OID,
					u_int16_t  dbRoot,
					u_int32_t  partitionNum,
					u_int16_t  segmentNum,
					LBID_t&    lbid,
					int&       allocdsize);

	/** @brief Rollback (delete) a set of extents for the specified OID.
	 *
	 * Deletes all the extents that logically follow the specified
	 * column extent; and sets the HWM for the specified extent.
	 * @param oid OID of the extents to be deleted.
	 * @param partitionNum Last partition to be kept.
	 * @param segmentNum Last segment in partitionNum to be kept.
	 * @param hwm HWM to be assigned to the last extent that is kept.
	 */
	EXPORT void rollbackColumnExtents(int oid,
					u_int32_t partitionNum,
					u_int16_t segmentNum,
					HWM_t     hwm);

	/** @brief Rollback (delete) set of extents for specified OID & DBRoot.
	 *
	 * Deletes all the extents that logically follow the specified
	 * column extent; and sets the HWM for the specified extent.
	 * @param oid OID of the extents to be deleted.
	 * @param bDeleteAll Flag indicates if all extents for oid and dbroot are
	 *        to be deleted, else part#, seg#, and HWM are used.
	 * @param dbRoot DBRoot of the extents to be deleted.
	 * @param partitionNum Last partition to be kept.
	 * @param segmentNum Last segment in partitionNum to be kept.
	 * @param hwm HWM to be assigned to the last extent that is kept.
	 */
	EXPORT void rollbackColumnExtents_DBroot(int oid,
					bool      bDeleteAll,
					u_int16_t dbRoot,
					u_int32_t partitionNum,
					u_int16_t segmentNum,
					HWM_t     hwm);
					
	/** @brief delete of column extents for the specified extents.
	 *
	 * Deletes the extents that logically follow the specified
	 * column extent in  extentsInfo. It use the same algorithm as in 
	 * rollbackColumnExtents.
	 * @param extentInfo the information for extents
	 */
	EXPORT void deleteEmptyColExtents(const ExtentsInfoMap_t& extentsInfo);
	
	/** @brief delete of dictionary extents for the specified extents.
	 *
	 * Arguments specify the last stripe for all the oids.  Any extents after this are
	 * deleted.  The hwm's of the extents in the last stripe are updated
	 * based on the hwm in extentsInfo.  It use the same algorithm as in 
	 * rollbackDictStoreExtents.
	 * @param extentInfo the information for extents to be resetted
	 */
	EXPORT void deleteEmptyDictStoreExtents(const ExtentsInfoMap_t& extentsInfo);

	/** @brief Rollback (delete) a set of dict store extents for an OID.
	 *
	 * Arguments specify the last stripe.  Any extents after this are
	 * deleted.  The hwm's of the extents in the last stripe are updated
	 * based on the contents of the hwm vector.  If hwms is a partial list,
	 * (as in the first stripe of a partition), then any extents in sub-
	 * sequent segment files for that partition are deleted.
	 * @param oid OID of the extents to be deleted or updated.
	 * @param partitionNum Last partition to be kept.
	 * @param hwms Vector of hwms for the last partition to be kept.
	 */
	EXPORT void rollbackDictStoreExtents(int oid,
					 u_int32_t        partitionNum,
					 const std::vector<HWM_t>& hwms);

	/** @brief Rollback (delete) a set of dict store extents for an OID & DBRoot
	 *
	 * Arguments specify the last stripe.  Any extents after this are
	 * deleted.  The hwm's of the extents in the last stripe are updated
	 * based on the contents of the hwm vector.  If hwms is a partial list,
	 * (as in the first stripe of a partition), then any extents in sub-
	 * sequent segment files for that partition are deleted.  If hwms is empty
	 * then all the extents in dbRoot are deleted.
	 * @param oid OID of the extents to be deleted or updated.
	 * @param dbRoot DBRoot of the extents to be deleted.
	 * @param partitionNum Last partition to be kept.
	 * @param segNums Vector of segment files in last partition to be kept.
	 * @param hwms Vector of hwms for the last partition to be kept.
	 */
	EXPORT void rollbackDictStoreExtents_DBroot(int oid,
					 u_int16_t  dbRoot,
					 u_int32_t  partitionNum,
					 const std::vector<u_int16_t>& segNums,
					 const std::vector<HWM_t>& hwms);

	/** @brief Deallocates all extents associated with OID
	 *
	 * Deallocates all extents associated with OID
	 * @param OID The OID to delete
	 */
	EXPORT void deleteOID(int OID);

	/** @brief Deallocates all extents associated with each OID
	 *
	 * Deallocates all extents associated with each OID
	 * @param OIDs The OIDs to delete
	 */
	EXPORT void deleteOIDs(const OidsMap_t& OIDs);
	
	/** @brief Gets the current high water mark of an OID
	 *
	 * Gets the current absolute high water mark of an OID.  This only
	 * applies to column OIDs.
	 * @param OID The OID
	 * @return The last file block number written to.
	 */
	EXPORT HWM_t getHWM(int OID);

	/** @brief Gets the last local high water mark of an OID
	 *
	 * Get last local high water mark of an OID, relative to a segment file.
	 * The DBRoot, partition and segment numbers for the pertinent segment
	 * are also returned.  
	 * @param OID (in) The OID
	 * @param dbRoot (out) The relevant DBRoot
	 * @param partitionNum (out) The relevant partition number
	 * @param segmentNum (out) The relevant segment number
	 * @return The last file block number written to in the last
	 * partition/segment file for the given OID.
	 */
	EXPORT HWM_t getLastLocalHWM(int OID, uint16_t& dbRoot, uint32_t& partitionNum,
				 uint16_t& segmentNum);
	
	/** @brief Check if any of the given partitions is the last one of a DBroot
	 *
	 * This is for partitioning operations to use. The last partition of a DBroot
	 * can not be dropped or disabled.
	 *
	 * @param OID (in) The OID
	 * @param partitionNums (in) The logical partition numbers to check.
	 * @return true if any of the partitions in the set is the last partition of
	 * a DBroot.
	 */

	/** @brief Gets the last local high water mark of an OID for a given dbRoot
	 *
	 * Get last local high water mark of an OID for a given dbRoot, relative to
	 * a segment file. The partition and segment numbers for the pertinent
	 * segment are also returned.  
	 
	 * @param OID (in) The OID
	 * @param dbRoot (in) The relevant DBRoot
	 * @param partitionNum (out) The relevant partition number
	 * @param segmentNum (out) The relevant segment number
	 * @partitionNotExists (out) Is any partition in the set not exists
	 * @return The last file block number written to in the last
	 * partition/segment file for the given OID.
	 */
	EXPORT HWM_t getLastHWM_DBroot(int OID, uint16_t dbRoot,
				 uint32_t& partitionNum,
				 uint16_t& segmentNum);
				 
	/** @brief Gets the current high water mark of an OID,partition,segment
	 *
	 * Get current local high water mark of an OID, partition, segment;
	 * where HWM is relative to the specific segment file.
	 * @param OID (in) The OID
	 * @param partitionNum (in) The relevant partition number
	 * @param segmentNum (in) The relevant segment number
	 * @return The last file block number written to in the specified
	 * partition/segment file for the given OID.
	 */
	EXPORT HWM_t getLocalHWM(int OID, uint32_t partitionNum,
				 uint16_t segmentNum);
	
	/** @brief Sets the current high water mark of an OID,partition,segment
	 *
	 * Sets the current local high water mark of an OID, partition, segment;
	 * where HWM is relative to the specific segment file.
	 * @param OID The OID
	 * @param partitionNum (in) The relevant partition number
	 * @param segmentNum (in) The relevant segment number
	 * @param HWM The high water mark to record
	 */ 
	EXPORT void setLocalHWM(int OID, uint32_t partitionNum,
				uint16_t segmentNum, HWM_t HWM, bool firstNode,
				bool uselock = true);

	EXPORT void bulkSetHWM(const std::vector<BulkSetHWMArg> &, bool firstNode);

	EXPORT void bulkUpdateDBRoot(const std::vector<BulkUpdateDBRootArg> &);

	/** @brief Get HWM information about last segment file for each DBRoot
	 *  assigned to a specific PM.
	 *
	 * Vector will contain an entry for each DBRoot.  If no "available" extents
	 * are found for a DBRoot, then totalBlocks will be 0 (and hwmExtentIndex
	 * will be -1) for that DBRoot.
	 * @param OID The oid of interest.
	 * @param pmNumber The PM number of interest.
	 * @param emDbRootHwmInfos The vector of DbRoot/HWM related objects.
	 */
	EXPORT void getDbRootHWMInfo(int OID, uint16_t pmNumber,
				EmDbRootHWMInfo_v& emDbRootHwmInfos);
	
	/** @brief Gets the extents of a given OID
	 *
	 * Gets the extents of a given OID.  The returned entries will
	 * be NULL-terminated and will have to be destroyed individually
	 * using delete.
	 * @note Untested
	 * @param OID (in) The OID to get the extents for.
	 * @param entries (out) A snapshot of the OID's Extent Map entries
	 * sorted by starting LBID; note that The Real Entries can change at 
	 * any time.
	 * @param sorted (in) indicates if output is to be sorted
	 * @param notFoundErr (in) indicates if no extents is considered an err
	 * @param incOutOfService (in) include/exclude out of service extents
	 */
	EXPORT void getExtents(int OID, std::vector<struct EMEntry>& entries,
				bool sorted=true, bool notFoundErr=true,
				bool incOutOfService=false);
	
	/** @brief Gets the extents of a given OID under specified dbroot
	 *
	 * Gets the extents of a given OID under specified dbroot.  The returned entries will
	 * be NULL-terminated and will have to be destroyed individually
	 * using delete.
	 * @param OID (in) The OID to get the extents for.
	 * @param entries (out) A snapshot of the OID's Extent Map entries for the dbroot
	 * @param dbroot (in) the specified dbroot
	 */	
	EXPORT void getExtents_dbroot(int OID, std::vector<struct EMEntry>& entries, const uint16_t dbroot);
	/** @brief Gets the size of an extent in rows
	 * 
	 * Gets the size of an extent in rows.
	 * @return The number of rows in an extent.
	 */
	EXPORT unsigned getExtentSize(); //dmc-consider deprecating
	EXPORT unsigned getExtentRows();

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
	EXPORT void getSysCatDBRoot(OID_t oid, uint16_t& dbRoot);

	/** @brief Delete a Partition for the specified OID(s).
	 *
	 * @param oids (in) the OIDs of interest.
	 * @param partitionNums (in) the set of partitions to be deleted.
	 */
	EXPORT void deletePartition(const std::set<OID_t>& oids,
						const std::set<LogicalPartition>& partitionNums, std::string& emsg);

	/** @brief Mark a Partition for the specified OID(s) as out of service.
	 *
	 * @param oids (in) the OIDs of interest.
	 * @param partitionNums (in) the set of partitions to be marked out of service.
	 */
	EXPORT void markPartitionForDeletion(const std::set<OID_t>& oids,
						const std::set<LogicalPartition>& partitionNums, std::string& emsg);
	
	/** @brief Mark all Partition for the specified OID(s) as out of service.
	 *
	 * @param oids (in) the OIDs of interest.
	 */
	EXPORT void markAllPartitionForDeletion(const std::set<OID_t>& oids);
	
	/** @brief Restore a Partition for the specified OID(s).
	 *
	 * @param oids (in) the OIDs of interest.
	 * @param partitionNums (in) the set of partitions to be restored.
	 */
	EXPORT void restorePartition(const std::set<OID_t>& oids,
						const std::set<LogicalPartition>& partitionNums, std::string& emsg);

	/** @brief Get the list of out-of-service partitions for a given OID
	 *
	 * @param OID (in) the OID of interest.
	 * @param partitionNums (out) the out-of-service partitions for the oid.
	 * partitionNums will be in sorted order.
	 */
	EXPORT void getOutOfServicePartitions(OID_t oid,
						std::set<LogicalPartition>& partitionNums);

	/** @brief Delete all extent map rows for the specified dbroot
	 *
	 * @param dbroot (in) the dbroot
	 */
	EXPORT void deleteDBRoot(uint16_t dbroot);

	/** @brief Is the specified DBRoot empty with no extents.
	 *  Throws exception if extentmap shared memory is not loaded.
	 *
	 * @param dbroot DBRoot of interest
	 */
	EXPORT bool isDBRootEmpty(uint16_t dbroot);

	/** @brief Performs internal consistency checks (for testing only)
	 *
	 * Performs internal consistency checks (for testing only).
	 * @note It's incomplete
	 * @return 0 if all tests pass, -1 (or throws logic_error) if not.
	 */
	EXPORT int checkConsistency();

	EXPORT void setReadOnly();
	
	EXPORT virtual void undoChanges();

	EXPORT virtual void confirmChanges();

	EXPORT int markInvalid(const LBID_t lbid);
	EXPORT int markInvalid(const std::vector<LBID_t> &lbids);

	EXPORT int setMaxMin(const LBID_t lbidRange, const int64_t max, const int64_t min, const int32_t seqNum,
			bool firstNode);

	// @bug 1970.  Added setExtentsMaxMin function below.

	/** @brief Updates the extents in the passed map of CPMaxMin objects.
	 * @param cpMap - The key must be the first LBID in the range.
	 *                The values are a CPMaxMin struct with the
	 *                min, max, and sequence.
	 * @param firstNode - if true, logs a debugging msg when CP data is updated
	 * @return 0 if all tests pass, -1 (or throws logic_error) if not.
	*/
	EXPORT void setExtentsMaxMin(const CPMaxMinMap_t &cpMap, bool firstNode, bool useLock = true);

	/** @brief Merges the CP info for the extents contained in cpMap.
	 * @param cpMap - The key must be the starting LBID in the range.
	 * @return 0 if all tests pass, -1 (or throws logic_error) if not.
	*/
	void mergeExtentsMaxMin(CPMaxMinMergeMap_t &cpMap, bool useLock = true);

	EXPORT int getMaxMin(const LBID_t lbidRange, int64_t& max, int64_t& min, int32_t& seqNum);

	inline bool empty() {
		if (fEMShminfo == 0)
		{
			grabEMEntryTable(BRM::ExtentMap::READ);
			releaseEMEntryTable(BRM::ExtentMap::READ);
		}
		return (fEMShminfo->currentSize == 0);
	}

	EXPORT std::vector<InlineLBIDRange> getFreeListEntries();

	EXPORT void dumpTo(std::ostream& os);
	EXPORT const bool *getEMLockStatus();
	EXPORT const bool *getEMFLLockStatus();

#ifdef BRM_DEBUG		
	EXPORT void printEM() const;
	EXPORT void printEM(const OID_t& oid) const;
	EXPORT void printEM(const EMEntry& em) const;
	EXPORT void printFL() const;
#endif

	
	/** @brief Change segment number associated with an extent
	 *
	 * @param oid - OID of extent to be changed
	 * @param partNum - partition number of extent to be changed
	 * @param oldSegNum - old segment number of extent to be changed
	 * @param newSegNum - new segment number to assign to the extent
	 * @return Indicates if any matching extents were found or not
	 */
	EXPORT bool updateSegNum( OID_t oid,
		uint32_t partNum, uint16_t oldSegNum, uint16_t newSegNum );

private:
	static const size_t EM_INCREMENT_ROWS = 100;
	static const size_t EM_INITIAL_SIZE = EM_INCREMENT_ROWS * 10 * sizeof(EMEntry);
	static const size_t EM_INCREMENT = EM_INCREMENT_ROWS * sizeof(EMEntry);
	static const size_t EM_FREELIST_INITIAL_SIZE = 50 * sizeof(InlineLBIDRange);
	static const size_t EM_FREELIST_INCREMENT = 50 * sizeof(InlineLBIDRange);

	ExtentMap(const ExtentMap& em);
	ExtentMap& operator=(const ExtentMap& em);
	
	EMEntry* fExtentMap;
	InlineLBIDRange* fFreeList;
	key_t fCurrentEMShmkey;
	key_t fCurrentFLShmkey;
	MSTEntry* fEMShminfo;
	MSTEntry* fFLShminfo;
	const MasterSegmentTable fMST;
	bool r_only;
	typedef std::tr1::unordered_map<int,oam::DBRootConfigList*> PmDbRootMap_t;
	PmDbRootMap_t fPmDbRootMap;
	time_t fCacheTime; // timestamp associated with config cache
	
	int numUndoRecords;
	bool flLocked, emLocked;
	static boost::mutex mutex; // @bug5355 - made mutex static
	boost::mutex fConfigCacheMutex; // protect access to Config Cache
	
	enum OPS {
		NONE,
		READ,
		WRITE
	};
	
	OPS EMLock, FLLock;
	
	LBID_t _createColumnExtent_DBroot(u_int32_t size, int OID,
					u_int32_t colWidth,
					u_int16_t  dbRoot,
					u_int32_t& partitionNum,
					u_int16_t& segmentNum,
					u_int32_t& startBlockOffset);
	LBID_t _createColumnExtentExactFile(u_int32_t size, int OID,
					u_int32_t  colWidth,
					u_int16_t  dbRoot,
					u_int32_t  partitionNum,
					u_int16_t  segmentNum,
					u_int32_t& startBlockOffset);
	LBID_t _createDictStoreExtent(u_int32_t size, int OID,
					u_int16_t  dbRoot,
					u_int32_t  partitionNum,
					u_int16_t  segmentNum);
	bool isValidCPRange(int64_t max, int64_t min) const;
	void deleteExtent(int emIndex);
	LBID_t getLBIDsFromFreeList(u_int32_t size);
	void v3Tov4(uint8_t* v3, uint8_t* v4, const uint32_t emNum);

	key_t chooseEMShmkey();  //see the code for how keys are segmented
	key_t chooseFLShmkey();  //see the code for how keys are segmented
	void grabEMEntryTable(OPS op);
	void grabFreeList(OPS op);
	void releaseEMEntryTable(OPS op);
	void releaseFreeList(OPS op);
	void growEMShmseg(size_t nrows=0);
	void growFLShmseg();
	void readData(int fd, u_int8_t* data, off_t offset, int size);
	void writeData(int fd, u_int8_t* data, off_t offset, int size) const;
	void finishChanges();
	EXPORT unsigned getFilesPerColumnPartition();
	unsigned getExtentsPerSegmentFile();
	unsigned getDbRootCount();
	void getPmDbRoots(int pm, std::vector<int>& dbRootList);
	void checkReloadConfig();
	ShmKeys fShmKeys;

	bool fDebug;

	int _markInvalid(LBID_t lbid);

	ExtentMapImpl* fPExtMapImpl;
	FreeListImpl* fPFreeListImpl;

	friend class ExtentMapConverter;
};

inline std::ostream& operator<<(std::ostream& os, ExtentMap& rhs)
{
	rhs.dumpTo(os);
	return os;
}

} //namespace

#undef EXPORT

#endif
