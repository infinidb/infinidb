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
 * $Id: copylocks.h 1936 2013-07-09 22:10:29Z dhall $
 *
 *****************************************************************************/

/** @file 
 * class XXX interface
 */
 
#ifndef COPYLOCKS_H_
#define COPYLOCKS_H_

#include <set>
#include <sys/types.h>
//#define NDEBUG
#include <cassert>

#include <boost/thread.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "brmtypes.h"
#include "mastersegmenttable.h"

#include "shmkeys.h"
#include "undoable.h"

#include "brmshmimpl.h"

/* Should load these from a config file */
#define CL_INITIAL_SIZE (50*sizeof(CopyLockEntry))
#define CL_INCREMENT (50*sizeof(CopyLockEntry))

#if defined(_MSC_VER) && defined(xxxCOPYLOCKS_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif


namespace idbdatafile {
class IDBDataFile;
}

namespace BRM {

struct CopyLockEntry {
	LBID_t start;
	int size;
	VER_t txnID;
	EXPORT CopyLockEntry();
};

class CopyLocksImpl
{
public:
	static CopyLocksImpl* makeCopyLocksImpl(unsigned key, off_t size, bool readOnly=false);

	inline void grow(unsigned key, off_t size)
#ifdef NDEBUG
		{ fCopyLocks.grow(key, size); }
#else
		{ int rc=fCopyLocks.grow(key, size); idbassert(rc==0); }
#endif
	inline void makeReadOnly() { fCopyLocks.setReadOnly(); }
	inline void clear(unsigned key, off_t size) { fCopyLocks.clear(key, size); }
	inline void swapout(BRMShmImpl& rhs) { fCopyLocks.swap(rhs); rhs.destroy(); }
	inline unsigned key() const { return fCopyLocks.key(); }

	inline CopyLockEntry* get() const { return reinterpret_cast<CopyLockEntry*>(fCopyLocks.fMapreg.get_address()); }

private:
	CopyLocksImpl(unsigned key, off_t size, bool readOnly=false);
	~CopyLocksImpl();
	CopyLocksImpl(const CopyLocksImpl& rhs);
	CopyLocksImpl& operator=(const CopyLocksImpl& rhs);

	BRMShmImpl fCopyLocks;

	static boost::mutex fInstanceMutex;
	static CopyLocksImpl* fInstance;
};

class CopyLocks : public Undoable {
	public:
		
		enum OPS {
			NONE,
			READ,
			WRITE
		};
		
		EXPORT CopyLocks();
		EXPORT ~CopyLocks();
		
		EXPORT void lockRange(const LBIDRange& range, VER_t txnID);
		EXPORT void releaseRange(const LBIDRange& range);
		EXPORT bool isLocked(const LBIDRange& range) const;
		EXPORT void rollback(VER_t txnID);
		
		EXPORT void lock(OPS op);
		EXPORT void release(OPS op);
		EXPORT void setReadOnly();
		EXPORT void getCurrentTxnIDs(std::set<VER_t> &txnList) const;

		EXPORT void forceRelease(const LBIDRange &range);

	private:
		CopyLocks(const CopyLocks &);
		CopyLocks& operator=(const CopyLocks &);
		
		key_t chooseShmkey();
		void growCL();
		
		CopyLockEntry* entries;
		key_t currentShmkey;
		int shmid;    //shmid's necessary?
		MSTEntry* shminfo;
		MasterSegmentTable mst;
		bool r_only;
		static boost::mutex mutex;
		static const int MAX_IO_RETRIES=10;
		ShmKeys fShmKeys;
		CopyLocksImpl* fCopyLocksImpl;
};

} 

#undef EXPORT

#endif
