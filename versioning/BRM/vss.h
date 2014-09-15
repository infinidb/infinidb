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
 * $Id: vss.h 1926 2013-06-30 21:18:14Z wweeks $
 *
 *****************************************************************************/

/** @file
 * class XXX interface
 */

#ifndef _VSS_H_
#define _VSS_H_

#include <set>
//#define NDEBUG
#include <cassert>
#include <boost/thread.hpp>

#include "brmshmimpl.h"

#include "brmtypes.h"
#include "undoable.h"
#include "mastersegmenttable.h"
#include "shmkeys.h"
#include "hasher.h"

#ifdef NONE
#undef NONE
#endif
#ifdef READ
#undef READ
#endif
#ifdef WRITE
#undef WRITE
#endif

// These config parameters need to be loaded

//will get a small hash function performance boost by using powers of 2
#define VSSSTORAGE_INITIAL_COUNT 200000
#define VSSSTORAGE_INITIAL_SIZE (VSSSTORAGE_INITIAL_COUNT*sizeof(VSSEntry))
#define VSSSTORAGE_INCREMENT_COUNT 20000
#define VSSSTORAGE_INCREMENT (VSSSTORAGE_INCREMENT_COUNT*sizeof(VSSEntry))

// (average list length = 4)
#define VSSTABLE_INITIAL_SIZE (50000*sizeof(int))
#define VSSTABLE_INCREMENT (5000*sizeof(int))

#define VSS_INITIAL_SIZE (sizeof(VSSShmsegHeader) + \
	VSSSTORAGE_INITIAL_SIZE + VSSTABLE_INITIAL_SIZE)

#define VSS_INCREMENT (VSSTABLE_INCREMENT + VSSSTORAGE_INCREMENT)

#define VSS_SIZE(entries) \
	((entries*sizeof(VSSEntry)) + (entries/4 * sizeof(int)) + sizeof(VSSShmsegHeader))

#if defined(_MSC_VER) && defined(xxxVSS_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

struct VSSEntry {
	LBID_t lbid;
	VER_t verID;
	bool vbFlag;
	bool locked;
	int next;
#ifndef __LP64__
	uint32_t pad1;
#endif
	EXPORT VSSEntry();
};

struct VSSShmsegHeader {
	int capacity;
	int currentSize;
	int LWM;
	int numHashBuckets;
	int lockedEntryCount;

//  the rest of the overlay looks like this
// 	int hashBuckets[numHashBuckets];
// 	VSSEntry storage[capacity];
};

class QueryContext_vss {
public:
	QueryContext_vss() : currentScn(0)
	{
		txns.reset(new std::set<VER_t>());
	}
	QueryContext_vss(const QueryContext &qc);
	VER_t currentScn;
	boost::shared_ptr<std::set<VER_t> > txns;
};

class VSSImpl
{
public:
	static VSSImpl* makeVSSImpl(unsigned key, off_t size, bool readOnly=false);

	inline void grow(unsigned key, off_t size)
#ifndef NDBUG
		{ fVSS.grow(key, size); }
#else
		{ int rc=fVSS.grow(key, size); idbassert(rc==0); }
#endif
	inline void makeReadOnly() { fVSS.setReadOnly(); }
	inline void clear(unsigned key, off_t size) { fVSS.clear(key, size); }
	inline void swapout(BRMShmImpl& rhs) { fVSS.swap(rhs); rhs.destroy(); }
	inline unsigned key() const { return fVSS.key(); }

	inline VSSShmsegHeader* get() const { return reinterpret_cast<VSSShmsegHeader*>(fVSS.fMapreg.get_address()); }

private:
	VSSImpl(unsigned key, off_t size, bool readOnly=false);
	~VSSImpl();
	VSSImpl(const VSSImpl& rhs);
	VSSImpl& operator=(const VSSImpl& rhs);

	BRMShmImpl fVSS;

	static boost::mutex fInstanceMutex;
	static VSSImpl* fInstance;
};

class VBBM;
class ExtentMap;

/** @brief The Version Substitution Structure (VSS)
 *
 * At a high level, the VSS maintains a table that associates an LBID with
 * a version number and 2 flags that indicate whether or not the block
 * identified by the <LBID, VerID> pair exists in the main database files
 * or the Version Buffer.  The VSS's main purpose is to resolve the version of
 * a specified block the caller can safely use given that there may be concurrent
 * writes to that block.
 *
 * As implemented, it is a hash table and a set of lists that exist in
 * shared memory.  The hash table is keyed by LBID, and
 * each valid entry points to the head of a unique list.  Each list element
 * contains the LBID, VerID, & the two flags that encapsulate "an entry in the
 * VSS table".  Every list contains all elements that collide on that hash table
 * entry that points to it, "load factor" has no bearing on performance,
 * and lists can grow arbitrarily large.
 * Technically lookups are O(n), but in normal circumstances it'll
 * be constant time.  As things are right now, we expect there to be about
 * 200k VSS entries.  The hash table is sized such that on average there will be 4
 * entries per list when it's at capacity.
 *
 * The memory management & structure manipulation code is nearly identical
 * to that in the VBBM, so any bugs found here are likely there as well.
 *
 * Shared memory is managed using code similar to the ExtentMap & VBBM.  When
 * the shared memory segment needs to grow, it is write-locked, a new one
 * is created, the contents are reinserted to the new one, the key is
 * registered, and the old segment is destroyed when the last reference to it
 * is detached.
 */

class VSS : public Undoable {
	public:

		enum OPS {
			NONE,
			READ,
			WRITE
		};

		EXPORT VSS();
		EXPORT ~VSS();

		EXPORT bool isLocked(const LBIDRange& l, VER_t txnID = -1) const;
		EXPORT void removeEntry(LBID_t lbid, VER_t verID, std::vector<LBID_t> *flushList);

		// Note, the use_vbbm switch should be used for unit testing the VSS only
		EXPORT void removeEntriesFromDB(const LBIDRange& range, VBBM& vbbm, bool use_vbbm = true);
		EXPORT int lookup(LBID_t lbid, const QueryContext_vss &, VER_t txnID, VER_t *outVer,
						  bool *vbFlag, bool vbOnly = false) const;

		/// Returns the version in the main DB files
		EXPORT VER_t getCurrentVersion(LBID_t lbid, bool *isLocked) const;  // returns the ver in the main DB files

		/// Returns the highest version in the version buffer, less than max
		EXPORT VER_t getHighestVerInVB(LBID_t lbid, VER_t max) const;

		/// returns true if that block is in the version buffer, false otherwise
		EXPORT bool isVersioned(LBID_t lbid, VER_t version) const;

		EXPORT void setVBFlag(LBID_t lbid, VER_t verID, bool vbFlag);
		EXPORT void insert(LBID_t, VER_t, bool vbFlag, bool locked, bool loading=false);
		EXPORT void commit(VER_t txnID);
		EXPORT void getUncommittedLBIDs(VER_t txnID, std::vector<LBID_t>& lbids);
		EXPORT void getUnlockedLBIDs(BlockList_t& lbids);
		EXPORT void getLockedLBIDs(BlockList_t& lbids);
		EXPORT void lock(OPS op);
		EXPORT void release(OPS op);
		EXPORT void setReadOnly();

		EXPORT int checkConsistency(const VBBM &vbbm, ExtentMap &em) const;
		EXPORT int size() const;
		EXPORT bool hashEmpty() const;
		EXPORT void getCurrentTxnIDs(std::set<VER_t> &txnList) const;

		EXPORT void clear();
		EXPORT void load(std::string filename);
#ifndef __LP64__
		//This method is OBE now that the structs are padded correctly
		EXPORT void load64(std::string filename);
#endif
		EXPORT void save(std::string filename);

#ifdef BRM_DEBUG
		EXPORT int getShmid() const;
#endif

		EXPORT bool isEmpty(bool doLock = true);

		/* Bug 2293.  VBBM will use this fcn to determine whether a block is
		 * currently in use. */
		EXPORT bool isEntryLocked(LBID_t lbid, VER_t verID) const;
		EXPORT bool isTooOld(LBID_t lbid, VER_t verID) const;

	private:
		VSS(const VSS &);
		VSS& operator=(const VSS &);

		struct VSSShmsegHeader *vss;
		int *hashBuckets;
		VSSEntry *storage;
		bool r_only;
		static boost::mutex mutex; // @bug5355 - made mutex static

		key_t currentVSSShmkey;
		int vssShmid;
		MSTEntry *vssShminfo;
		MasterSegmentTable mst;
		static const int MAX_IO_RETRIES=10;

		key_t chooseShmkey() const;
		void growVSS();
		void growForLoad(int count);
		void initShmseg();
		void copyVSS(VSSShmsegHeader *dest);

		int getIndex(LBID_t lbid, VER_t verID, int& prev, int& bucket) const;
		void _insert(VSSEntry& e, VSSShmsegHeader* dest, int* destTable, VSSEntry*
				destStorage, bool loading=false);
		ShmKeys fShmKeys;

		VSSImpl* fPVSSImpl;
		utils::Hasher hasher;
};

}

#undef EXPORT

#endif // _VSS_H_

