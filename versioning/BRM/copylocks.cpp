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

/*****************************************************************************
 * $Id: copylocks.cpp 1936 2013-07-09 22:10:29Z dhall $
 *
 ****************************************************************************/

#include <sys/types.h>
#include <iostream>
#include <stdexcept>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <string>
#ifdef _MSC_VER
#include <io.h>
#endif

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
namespace bi=boost::interprocess;

#include "shmkeys.h"
#include "brmtypes.h"
#include "rwlock.h"
#include "mastersegmenttable.h"
#define COPYLOCKS_DLLEXPORT
#include "copylocks.h"
#undef COPYLOCKS_DLLEXPORT

#define CL_MAGIC_V1 0x789ba6c1

#ifndef O_BINARY
#  define O_BINARY 0
#endif
#ifndef O_DIRECT
#  define O_DIRECT 0
#endif
#ifndef O_LARGEFILE
#  define O_LARGEFILE 0
#endif
#ifndef O_NOATIME
#  define O_NOATIME 0
#endif

using namespace std;
using namespace boost;
using namespace logging;

#include "IDBDataFile.h"
#include "IDBPolicy.h"
using namespace idbdatafile;

namespace BRM {

CopyLockEntry::CopyLockEntry() 
{
	start = 0;
	size = 0;
	txnID = 0;
}

/*static*/
boost::mutex CopyLocksImpl::fInstanceMutex;
boost::mutex CopyLocks::mutex;

/*static*/
CopyLocksImpl* CopyLocksImpl::fInstance=0;

/*static*/
CopyLocksImpl* CopyLocksImpl::makeCopyLocksImpl(unsigned key, off_t size, bool readOnly)
{
	boost::mutex::scoped_lock lk(fInstanceMutex);

	if (fInstance)
	{
		if (key != fInstance->fCopyLocks.key())
		{
			BRMShmImpl newShm(key, size, readOnly);
			fInstance->swapout(newShm);
		}
		idbassert(key == fInstance->fCopyLocks.key());
		return fInstance;
	}

	fInstance = new CopyLocksImpl(key, size, readOnly);

	return fInstance;
}

CopyLocksImpl::CopyLocksImpl(unsigned key, off_t size, bool readOnly) :
	fCopyLocks(key, size, readOnly)
{
}

CopyLocks::CopyLocks() 
{
	entries = NULL;
	currentShmkey = shmid = 0;
	shminfo = NULL;
	r_only = false;
	fCopyLocksImpl = 0;
}

CopyLocks::~CopyLocks()
{
}

void CopyLocks::setReadOnly()
{
	r_only = true;
}

/* always returns holding the specified lock type, and with the EM seg mapped */
void CopyLocks::lock(OPS op)
{	
	boost::mutex::scoped_lock lk(mutex);
	if (op == READ)
		shminfo = mst.getTable_read(MasterSegmentTable::CLSegment);
	else 
		shminfo = mst.getTable_write(MasterSegmentTable::CLSegment);

	if (currentShmkey != shminfo->tableShmkey) {
		if (entries != NULL)
			entries = NULL;
		if (shminfo->allocdSize == 0)
			if (op == READ) {
				mst.getTable_upgrade(MasterSegmentTable::CLSegment);
				if (shminfo->allocdSize == 0)
					growCL();
				mst.getTable_downgrade(MasterSegmentTable::CLSegment);
			}
			else
				growCL();
		else {
			currentShmkey = shminfo->tableShmkey;
			fCopyLocksImpl = CopyLocksImpl::makeCopyLocksImpl(currentShmkey, 0, r_only);
			entries = fCopyLocksImpl->get();
			if (entries == NULL) {
				log_errno(string("CopyLocks::lock(): shmat failed"));
				throw std::runtime_error("CopyLocks::lock(): shmat failed.  Check the error log.");
			}
		}
	}
}

void CopyLocks::release(OPS op)
{
	if (op == READ)
		mst.releaseTable_read(MasterSegmentTable::CLSegment);
	else
		mst.releaseTable_write(MasterSegmentTable::CLSegment);
}

key_t CopyLocks::chooseShmkey()
{
	int fixedKeys = 1;
	key_t ret;
	
	if (shminfo->tableShmkey + 1 == (key_t) (fShmKeys.KEYRANGE_CL_BASE + 
		   fShmKeys.KEYRANGE_SIZE - 1) || (unsigned)shminfo->tableShmkey < fShmKeys.KEYRANGE_CL_BASE)
		ret = fShmKeys.KEYRANGE_CL_BASE + fixedKeys;
	else
		ret = shminfo->tableShmkey + 1;

	return ret;
}

void CopyLocks::growCL()
{
	int allocSize;
	key_t newshmkey;

	if (shminfo->allocdSize == 0)
		allocSize = CL_INITIAL_SIZE;
	else
		allocSize = shminfo->allocdSize + CL_INCREMENT;

	newshmkey = chooseShmkey();
	idbassert((allocSize == CL_INITIAL_SIZE && !fCopyLocksImpl) || fCopyLocksImpl);

	if (!fCopyLocksImpl)
		fCopyLocksImpl = CopyLocksImpl::makeCopyLocksImpl(newshmkey, allocSize, r_only);
	else
		fCopyLocksImpl->grow(newshmkey, allocSize);
	shminfo->tableShmkey = currentShmkey = newshmkey;
	shminfo->allocdSize = allocSize;
	if (r_only)
		fCopyLocksImpl->makeReadOnly();

	entries = fCopyLocksImpl->get();
	// Temporary fix.  Get rid of the old undo records that now point to nothing.
	// Would be nice to be able to carry them forward.
	confirmChanges();
}

// this fcn is dumb; relies on external check on whether it's safe or not
// also relies on external write lock grab
void CopyLocks::lockRange(const LBIDRange& l, VER_t txnID)
{
	int i, numEntries;
	
	// grow if necessary
	if (shminfo->currentSize == shminfo->allocdSize)
		growCL();
	
	/* debugging code, check for an existing lock */
	//assert(!isLocked(l));

	//ostringstream os;
	//os << "Copylocks locking <" << l.start << ", " << l.size << "> txnID = " << txnID;
	//log(os.str());

	// scan for an empty entry
	numEntries = shminfo->allocdSize/sizeof(CopyLockEntry);
	for (i = 0; i < numEntries; i++) {
		if (entries[i].size == 0) {
			makeUndoRecord(&entries[i], sizeof(CopyLockEntry));
			entries[i].start = l.start;
			entries[i].size = l.size;
			entries[i].txnID = txnID;
			makeUndoRecord(shminfo, sizeof(MSTEntry));
			shminfo->currentSize += sizeof(CopyLockEntry);

			// make sure isLocked() now sees the lock
			//assert(isLocked(l));
			return;
		}
	}

	log(string("CopyLocks::lockRange(): shm metadata problem: could not find an empty copylock entry"));
	throw std::logic_error("CopyLocks::lockRange(): shm metadata problem: could not find an empty copylock entry");
}

// this fcn is dumb; relies on external check on whether it's safe or not
// also relies on external write lock grab
void CopyLocks::releaseRange(const LBIDRange& l)
{
	int i, numEntries;
	LBID_t lastBlock = l.start + l.size - 1;
	LBID_t eLastBlock;

#ifdef BRM_DEBUG
	// debatable whether this should be included or not given the timers
	// that automatically release locks
	idbassert(isLocked(l));
#endif

	numEntries = shminfo->allocdSize/sizeof(CopyLockEntry);
	for (i = 0; i < numEntries; i++) {
		CopyLockEntry &e = entries[i];
		if (e.size != 0) {
			eLastBlock = e.start + e.size - 1;
			if (l.start <= eLastBlock && lastBlock >= e.start) {
				makeUndoRecord(&entries[i], sizeof(CopyLockEntry));
				e.size = 0;
				makeUndoRecord(shminfo, sizeof(MSTEntry));
				shminfo->currentSize -= sizeof(CopyLockEntry);
			}
		}
	}

#ifdef BRM_DEBUG
	idbassert(!isLocked(l));
	//log(string("CopyLocks::releaseRange(): that range isn't locked", LOG_TYPE_WARNING));
	//throw std::invalid_argument("CopyLocks::releaseRange(): that range isn't locked");
#endif
}

/* This doesn't come from the controllernode right now,
 * shouldn't use makeUndoRecord() */
void CopyLocks::forceRelease(const LBIDRange &l)
{
	int i, numEntries;
	LBID_t lastBlock = l.start + l.size - 1;
	LBID_t eLastBlock;

	numEntries = shminfo->allocdSize/sizeof(CopyLockEntry);

	//ostringstream os;
	//os << "Copylocks force-releasing <" << l.start << ", " << l.size << ">";
	//log(os.str());


	/* If a range intersects l, get rid of it. */
	for (i = 0; i < numEntries; i++) {
		CopyLockEntry &e = entries[i];
		if (e.size != 0) {
			eLastBlock = e.start + e.size - 1;
			if (l.start <= eLastBlock && lastBlock >= e.start) {
				makeUndoRecord(&entries[i], sizeof(CopyLockEntry));
				e.size = 0;
				makeUndoRecord(shminfo, sizeof(MSTEntry));
				shminfo->currentSize -= sizeof(CopyLockEntry);
			}
		}
	}
	//assert(!isLocked(l));
}

//assumes read lock
bool CopyLocks::isLocked(const LBIDRange &l) const
{
	int i, numEntries;
	LBID_t lLastBlock, lastBlock;
	
	numEntries = shminfo->allocdSize/sizeof(CopyLockEntry);
	lLastBlock = l.start + l.size - 1;
	for (i = 0; i < numEntries; i++) {
		if (entries[i].size != 0) {
			lastBlock = entries[i].start + entries[i].size - 1;
			if (lLastBlock >= entries[i].start && l.start <= lastBlock)
				return true;
		}
	}
	return false;
}

void CopyLocks::rollback(VER_t txnID)
{
	int i, numEntries;
	
	numEntries = shminfo->allocdSize/sizeof(CopyLockEntry);
	for (i = 0; i < numEntries; i++) 
		if (entries[i].size != 0 && entries[i].txnID == txnID) {
			makeUndoRecord(&entries[i], sizeof(CopyLockEntry));
			entries[i].size = 0;
			makeUndoRecord(shminfo, sizeof(MSTEntry));
			shminfo->currentSize -= sizeof(CopyLockEntry);
		}
}	

void CopyLocks::getCurrentTxnIDs(std::set<VER_t> &list) const
{
	int i, numEntries;
	
	numEntries = shminfo->allocdSize/sizeof(CopyLockEntry);
	for (i = 0; i < numEntries; i++) 
		if (entries[i].size != 0) 
			list.insert(entries[i].txnID);
}


}  // namespace

