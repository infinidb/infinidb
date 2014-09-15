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
 * $Id: vbbm.h 1926 2013-06-30 21:18:14Z wweeks $
 *
 *****************************************************************************/

/** @file
 * class XXX interface
 */

#ifndef _VBBM_H_
#define _VBBM_H_

#include <vector>
//#define NDEBUG
#include <cassert>
#include <boost/thread.hpp>

#include "brmshmimpl.h"

#include "shmkeys.h"
#include "brmtypes.h"
#include "undoable.h"
#include "mastersegmenttable.h"

// These config parameters need to be loaded

//will get a small hash function performance boost by using powers of 2
#define VBSTORAGE_INITIAL_COUNT 100000
#define VBSTORAGE_INITIAL_SIZE (VBSTORAGE_INITIAL_COUNT*sizeof(VBBMEntry))
#define VBSTORAGE_INCREMENT_COUNT 10000
#define VBSTORAGE_INCREMENT (VBSTORAGE_INCREMENT_COUNT*sizeof(VBBMEntry))

// (average list length = 4)
#define VBTABLE_INITIAL_SIZE (25000*sizeof(int))
#define VBTABLE_INCREMENT (2500*sizeof(int))

#define VBBM_INCREMENT (VBTABLE_INCREMENT + VBSTORAGE_INCREMENT)

#define VBBM_SIZE(files, entries) \
		((entries * sizeof(VBBMEntry)) + (entries/4 * sizeof(int)) \
			+ (files * sizeof(VBFileMetadata)) + sizeof(VBShmsegHeader))


#if defined(_MSC_VER) && defined(xxxVBBM_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif


namespace idbdatafile {
class IDBDataFile;
}

namespace BRM {

class VSS;

struct VBFileMetadata {
	OID_t OID;
	uint64_t fileSize;
	uint64_t nextOffset;
};

struct VBBMEntry {
	LBID_t lbid;
	VER_t verID;
	OID_t vbOID;
	uint32_t vbFBO;
	int next;
	EXPORT VBBMEntry();
};

struct VBShmsegHeader {
	int nFiles;
	int vbCapacity;
	int vbCurrentSize;
	int vbLWM;
	int numHashBuckets;

	// the rest of the overlay looks like this
// 	VBFileMetadata files[nFiles];
// 	int hashBuckets[numHashBuckets];
// 	VBBMEntry storage[vbCapacity];
};

class VBBMImpl
{
public:
	static VBBMImpl* makeVBBMImpl(unsigned key, off_t size, bool readOnly=false);

	inline void grow(unsigned key, off_t size)
#ifdef NDEBUG
		{ fVBBM.grow(key, size); }
#else
		{ int rc=fVBBM.grow(key, size); idbassert(rc==0); }
#endif
	inline void makeReadOnly() { fVBBM.setReadOnly(); }
	inline void clear(unsigned key, off_t size) { fVBBM.clear(key, size); }
	inline void swapout(BRMShmImpl& rhs) { fVBBM.swap(rhs); rhs.destroy(); }
	inline unsigned key() const { return fVBBM.key(); }

	inline VBShmsegHeader* get() const { return reinterpret_cast<VBShmsegHeader*>(fVBBM.fMapreg.get_address()); }

private:
	VBBMImpl(unsigned key, off_t size, bool readOnly=false);
	~VBBMImpl();
	VBBMImpl(const VBBMImpl& rhs);
	VBBMImpl& operator=(const VBBMImpl& rhs);

	BRMShmImpl fVBBM;

	static boost::mutex fInstanceMutex;
	static VBBMImpl* fInstance;
};

/** @brief The Version Buffer Block Map (VBBM)
 *
 * At a high level, the VBBM maintains a table describing the contents of
 * the Version Buffer.  For every entry in the Version Buffer, it associates its
 * <LBID, VerID> identifier with the OID and offset it is stored at.
 *
 * As implemented, it is a hash table and a set of lists that exist in
 * shared memory.  The hash table is keyed by <LBID, VerID>, and
 * each valid entry points to the head of a unique list.  Each list element
 * contains the LBID, VerID, VB OID, and VB offset that encapsulate "an entry in the
 * VBBM table".  Every list contains all elements that collide on that hash table
 * entry that points to it, "load factor" has no bearing on performance,
 * and lists can grow arbitrarily large.
 * Technically lookups are O(n), but in normal circumstances it'll
 * be constant time.  As things are right now, we expect there to be about
 * 100k VBBM entries.  The hash table is sized such that on average there will be 4
 * entries per list when it's at capacity.
 *
 * The memory management & structure manipulation code is nearly identical
 * to that in the VSS, so any bugs found here are likely there as well.
 *
 * Shared memory is managed using code similar to the ExtentMap & VSS.  When
 * the shared memory segment needs to grow, it is write-locked, a new one
 * is created, the contents are reinserted to the new one, the key is
 * registered, and the old segment is destroyed when the last reference to it
 * is detached.
 */

class VBBM : public Undoable {
	public:
		enum OPS {
			NONE,
			READ,
			WRITE
		};


		EXPORT VBBM();
		EXPORT ~VBBM();

		EXPORT void lock(OPS op);
		EXPORT void release(OPS op);
		EXPORT int lookup(LBID_t lbid, VER_t ver, OID_t &oid, uint32_t &fbo) const;
		EXPORT void insert(LBID_t lbid, VER_t ver, OID_t oid, uint32_t fbo, bool loading = false);
		EXPORT void getBlocks(int num, OID_t vbOID, std::vector<VBRange> &vbRanges, VSS& vss,
				bool flushPMCache);
		EXPORT void removeEntry(LBID_t, VER_t ver);

		EXPORT int size() const;
		EXPORT bool hashEmpty() const;
		EXPORT int checkConsistency() const;
		EXPORT void setReadOnly();

		EXPORT void clear();
		EXPORT void load(std::string filename);
		EXPORT void loadVersion1(idbdatafile::IDBDataFile* in);
		EXPORT void loadVersion2(idbdatafile::IDBDataFile* in);
		EXPORT void save(std::string filename);

#ifdef BRM_DEBUG
		EXPORT int getShmid() const;
#endif

	private:
		VBBM(const VBBM &);
		VBBM& operator=(const VBBM &);

		VBShmsegHeader *vbbm;
		VBFileMetadata *files;
		int *hashBuckets;
		VBBMEntry *storage;

		key_t currentVBBMShmkey;
		int vbbmShmid;
		bool r_only;
		MSTEntry *vbbmShminfo;
		MasterSegmentTable mst;
		static boost::mutex mutex; // @bug5355 - made mutex static
		static const int MAX_IO_RETRIES=10;

		key_t chooseShmkey() const;
		void growVBBM(bool addAFile = false);
		void growForLoad(int count);
		void copyVBBM(VBShmsegHeader *dest);
		void initShmseg(int nFiles);

		void _insert(VBBMEntry& e, VBShmsegHeader* dest, int* destTable, VBBMEntry*
				destStorage, bool loading = false);
		int getIndex(LBID_t lbid, VER_t verID, int& prev, int& bucket) const;
		ShmKeys fShmKeys;
		VBBMImpl* fPVBBMImpl;

		/* Shared nothing mods */
		uint64_t currentFileSize;
		void setCurrentFileSize();
		uint32_t addVBFileIfNotExists(OID_t vbOID);
};

}

#undef EXPORT

#endif // _VBBM_H_

