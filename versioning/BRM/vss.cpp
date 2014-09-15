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
 * $Id: vss.cpp 1926 2013-06-30 21:18:14Z wweeks $
 *
 ****************************************************************************/

#include <iostream>
#include <fstream>
#include <sstream>
//#define NDEBUG
#include <cassert>

#include <sys/types.h>
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>

#ifdef _MSC_VER
#include <io.h>
#endif

#include "rwlock.h"
#include "brmtypes.h"
#include "mastersegmenttable.h"
#include "vbbm.h"
#include "extentmap.h"
#include "cacheutils.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"


#define VSS_DLLEXPORT
#include "vss.h"
#undef VSS_DLLEXPORT

#define VSS_MAGIC_V1	0x7218db12

using namespace std;
using namespace boost;
using namespace idbdatafile;

namespace BRM {

VSSEntry::VSSEntry()
{
	lbid = -1;
	verID = 0;
	vbFlag = false;
	locked = false;
	next = -1;
}

/*static*/
boost::mutex VSSImpl::fInstanceMutex;
boost::mutex VSS::mutex;

/*static*/
VSSImpl* VSSImpl::fInstance=0;

/*static*/
VSSImpl* VSSImpl::makeVSSImpl(unsigned key, off_t size, bool readOnly)
{
	boost::mutex::scoped_lock lk(fInstanceMutex);

	if (fInstance)
	{
		if (key != fInstance->fVSS.key())
		{
			BRMShmImpl newShm(key, size);
			fInstance->swapout(newShm);
		}
		idbassert(key == fInstance->fVSS.key());
		return fInstance;
	}

	fInstance = new VSSImpl(key, size, readOnly);

	return fInstance;
}

VSSImpl::VSSImpl(unsigned key, off_t size, bool readOnly) :
	fVSS(key, size, readOnly)
{
}

VSS::VSS()
{
	vss = 0;
	hashBuckets = 0;
	storage = 0;
	currentVSSShmkey = -1;
	vssShmid = 0;
	vssShminfo = NULL;
	r_only = false;
	fPVSSImpl = 0;
}

VSS::~VSS()
{
}

// ported from ExtentMap
void VSS::lock(OPS op)
{
	char *shmseg;

	if (op == READ) {
		vssShminfo = mst.getTable_read(MasterSegmentTable::VSSSegment);
		mutex.lock();
	}
	else
		vssShminfo = mst.getTable_write(MasterSegmentTable::VSSSegment);

	// this means that either the VSS isn't attached or that it was resized
	if (!fPVSSImpl || fPVSSImpl->key() != (unsigned)vssShminfo->tableShmkey) {
		if (vssShminfo->allocdSize == 0)
		{
			if (op == READ) {
				mutex.unlock();
				mst.getTable_upgrade(MasterSegmentTable::VSSSegment);
				try {
					growVSS();
				}
				catch(...) {
					release(WRITE);
					throw;
				}
				mst.getTable_downgrade(MasterSegmentTable::VSSSegment);
			}
			else {
				try {
					growVSS();
				}
				catch(...) {
					release(WRITE);
					throw;
				}
			}
		}
		else {
			fPVSSImpl = VSSImpl::makeVSSImpl(vssShminfo->tableShmkey, 0);
			idbassert(fPVSSImpl);
			if (r_only)
				fPVSSImpl->makeReadOnly();
			vss = fPVSSImpl->get();
			shmseg = reinterpret_cast<char *>(vss);
			hashBuckets = reinterpret_cast<int*>
					(&shmseg[sizeof(VSSShmsegHeader)]);
			storage = reinterpret_cast<VSSEntry*>(&shmseg[sizeof(VSSShmsegHeader) +
					vss->numHashBuckets*sizeof(int)]);

			if (op == READ)
				mutex.unlock();
		}
	}
	else
	{
		vss = fPVSSImpl->get();
		shmseg = reinterpret_cast<char *>(vss);
		hashBuckets = reinterpret_cast<int*>
				(&shmseg[sizeof(VSSShmsegHeader)]);
		storage = reinterpret_cast<VSSEntry*>(&shmseg[sizeof(VSSShmsegHeader) +
				vss->numHashBuckets*sizeof(int)]);
		if (op == READ)
			mutex.unlock();
	}
}

// ported from ExtentMap
void VSS::release(OPS op)
{
	if (op == READ)
		mst.releaseTable_read(MasterSegmentTable::VSSSegment);
	else
		mst.releaseTable_write(MasterSegmentTable::VSSSegment);
}

void VSS::initShmseg()
{
	int i;
	char *newshmseg;
	int *buckets;
	VSSEntry *stor;

	vss->capacity = VSSSTORAGE_INITIAL_SIZE/sizeof(VSSEntry);
	vss->currentSize = 0;
	vss->lockedEntryCount = 0;
	vss->LWM = 0;
	vss->numHashBuckets = VSSTABLE_INITIAL_SIZE/sizeof(int);
	newshmseg = reinterpret_cast<char*>(vss);

	buckets = reinterpret_cast<int*>
			(&newshmseg[sizeof(VSSShmsegHeader)]);
	stor = reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) +
			vss->numHashBuckets*sizeof(int)]);

	for (i = 0; i < vss->numHashBuckets; i++)
		buckets[i] = -1;
	for (i = 0; i < vss->capacity; i++)
		stor[i].lbid = -1;
}

//assumes write lock is held
void VSS::growVSS()
{
	int allocSize;
	key_t newshmkey;
	char *newshmseg;

	if (vssShminfo->allocdSize == 0)
		allocSize = VSS_INITIAL_SIZE;
	else
		allocSize = vssShminfo->allocdSize + VSS_INCREMENT;

	newshmkey = chooseShmkey();
	idbassert((allocSize == VSS_INITIAL_SIZE && !fPVSSImpl) || fPVSSImpl);
	if (fPVSSImpl)
	{
		BRMShmImpl newShm(newshmkey, allocSize);
		newshmseg = static_cast<char*>(newShm.fMapreg.get_address());
		memset(newshmseg, 0, allocSize);
		idbassert(vss);
		VSSShmsegHeader *tmp = reinterpret_cast<VSSShmsegHeader*>(newshmseg);
		tmp->capacity = vss->capacity + VSSSTORAGE_INCREMENT/sizeof(VSSEntry);
		tmp->numHashBuckets = vss->numHashBuckets + VSSTABLE_INCREMENT/sizeof(int);
		tmp->LWM = 0;
		copyVSS(tmp);
		fPVSSImpl->swapout(newShm);
	}
	else
	{
		fPVSSImpl = VSSImpl::makeVSSImpl(newshmkey, allocSize);
		newshmseg = reinterpret_cast<char*>(fPVSSImpl->get());
		memset(newshmseg, 0, allocSize);
	}

	vss = fPVSSImpl->get();
	if (allocSize == VSS_INITIAL_SIZE)
		initShmseg();
	vssShminfo->tableShmkey = newshmkey;
	vssShminfo->allocdSize = allocSize;
	if (r_only)
	{
		fPVSSImpl->makeReadOnly();
		vss = fPVSSImpl->get();
	}
	newshmseg = reinterpret_cast<char *>(vss);
	hashBuckets = reinterpret_cast<int*>
			(&newshmseg[sizeof(VSSShmsegHeader)]);
	storage = reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) +
			vss->numHashBuckets*sizeof(int)]);
}

//assumes write lock is held
void VSS::growForLoad(int elementCount)
{
	int allocSize;
	key_t newshmkey;
	char *newshmseg;
	int i;

	if (elementCount < VSSSTORAGE_INITIAL_COUNT)
		elementCount = VSSSTORAGE_INITIAL_COUNT;
	/* round up to the next normal increment out of paranoia */
	if (elementCount % VSSSTORAGE_INCREMENT_COUNT)
		elementCount = ((elementCount/VSSSTORAGE_INCREMENT_COUNT) + 1) * VSSSTORAGE_INCREMENT_COUNT;
	allocSize = VSS_SIZE(elementCount);

	newshmkey = chooseShmkey();
	if (fPVSSImpl)
	{
		// isn't this the same as makeVSSImpl()?
		BRMShmImpl newShm(newshmkey, allocSize);
		fPVSSImpl->swapout(newShm);
	}
	else
	{
		fPVSSImpl = VSSImpl::makeVSSImpl(newshmkey, allocSize);
	}

	vss = fPVSSImpl->get();
	vss->capacity = elementCount;
	vss->currentSize = 0;
	vss->LWM = 0;
	vss->numHashBuckets = elementCount/4;
	vss->lockedEntryCount = 0;
	undoRecords.clear();
	newshmseg = reinterpret_cast<char *>(vss);
	hashBuckets = reinterpret_cast<int*>
		(&newshmseg[sizeof(VSSShmsegHeader)]);
	storage = reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) +
		vss->numHashBuckets*sizeof(int)]);
	for (i = 0; i < vss->capacity; i++)
		storage[i].lbid = -1;
	for (i = 0; i < vss->numHashBuckets; i++)
		hashBuckets[i] = -1;

	vssShminfo->tableShmkey = newshmkey;
	vssShminfo->allocdSize = allocSize;
}

//assumes write lock is held and the src is vbbm
//and that dest->{numHashBuckets, capacity, LWM} have been set.
void VSS::copyVSS(VSSShmsegHeader *dest)
{
	int i;
	int *newHashtable;
	VSSEntry *newStorage;
	char *cDest = reinterpret_cast<char *>(dest);

	// copy metadata
	dest->currentSize = vss->currentSize;
	dest->lockedEntryCount = vss->lockedEntryCount;

	newHashtable = reinterpret_cast<int*>(&cDest[sizeof(VSSShmsegHeader)]);
	newStorage = reinterpret_cast<VSSEntry*>(&cDest[sizeof(VSSShmsegHeader) +
			dest->numHashBuckets*sizeof(int)]);

	//initialize new storage & hash
	for (i = 0; i < dest->numHashBuckets; i++)
		newHashtable[i] = -1;

	for (i = 0; i < dest->capacity; i++)
		newStorage[i].lbid = -1;

	//walk the storage & re-hash all entries;
	for (i = 0; i < vss->currentSize; i++)
		if (storage[i].lbid != -1) {
			_insert(storage[i], dest, newHashtable, newStorage, true);
			//confirmChanges();
		}
}

key_t VSS::chooseShmkey() const
{
	int fixedKeys = 1;
	key_t ret;

	if (vssShminfo->tableShmkey + 1 == (key_t) (fShmKeys.KEYRANGE_VSS_BASE +
		   fShmKeys.KEYRANGE_SIZE - 1) || (unsigned)vssShminfo->tableShmkey < fShmKeys.KEYRANGE_VSS_BASE)
		ret = fShmKeys.KEYRANGE_VSS_BASE + fixedKeys;
	else
		ret = vssShminfo->tableShmkey + 1;

	return ret;
}

void VSS::insert(LBID_t lbid, VER_t verID, bool vbFlag, bool locked, bool loading)
{
	VSSEntry entry;

#ifdef BRM_DEBUG
	if (lbid < 0) {
		log("VSS::insert(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::insert(): lbid must be >= 0");
	}
	if (verID < 0) {
		log("VSS::insert(): verID must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::insert(): verID must be >= 0");
	}
#endif

	entry.lbid = lbid;
	entry.verID = verID;
	entry.vbFlag = vbFlag;
	entry.locked = locked;
	//cerr << "Insert to vss lbid:verID:locked:vbFlag = " << entry.lbid <<":" <<entry.verID<<":"<< entry.locked <<":"<<entry.vbFlag<<endl;
	// check for resize
	if (vss->currentSize == vss->capacity)
		growVSS();

	_insert(entry, vss, hashBuckets, storage, loading);
	if (!loading)
		makeUndoRecord(&vss->currentSize, sizeof(vss->currentSize));
	vss->currentSize++;
	if (locked)
		vss->lockedEntryCount++;
}



//assumes write lock is held and that it is properly sized already
//metadata is modified by the caller
void VSS::_insert(VSSEntry& e, VSSShmsegHeader *dest, int *destHash,
				  VSSEntry *destStorage, bool loading)
{
	int hashIndex, insertIndex;

	hashIndex = hasher((char*) &e.lbid, sizeof(e.lbid)) % dest->numHashBuckets;

	insertIndex = dest->LWM;
	while (destStorage[insertIndex].lbid != -1) {
		insertIndex++;
#ifdef BRM_DEBUG
		if (insertIndex == dest->capacity) {
			log("VSS:_insert(): There are no empty entries. Check resize condition.",
				logging::LOG_TYPE_DEBUG);
			throw logic_error("VSS:_insert(): There are no empty entries. Check resize condition.");
		}
#endif
	}
	if (!loading)
		makeUndoRecord(dest, sizeof(VSSShmsegHeader));
	dest->LWM = insertIndex + 1;

	if (!loading) {
		makeUndoRecord(&destStorage[insertIndex], sizeof(VSSEntry));
		makeUndoRecord(&destHash[hashIndex], sizeof(int));
	}
	e.next = destHash[hashIndex];
	destStorage[insertIndex] = e;
	destHash[hashIndex] = insertIndex;
}

//assumes read lock is held
int VSS::lookup(LBID_t lbid, const QueryContext_vss &verInfo, VER_t txnID, VER_t *outVer,
				bool *vbFlag, bool vbOnly) const
{
	int hashIndex, maxVersion = -1, minVersion = -1, currentIndex;
	VSSEntry *listEntry, *maxEntry = NULL;

#ifdef BRM_DEBUG
	if (lbid < 0) {
		log("VSS::lookup(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::lookup(): lbid must be >= 0");
	}
	if (verInfo.currentScn < 0) {
		log("VSS::lookup(): verID must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::lookup(): verID must be >= 0");
	}
	if (txnID < 0) {
		log("VSS::lookup(): txnID must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::lookup(): txnID must be >= 0");
	}
#endif

	hashIndex = hasher((char *) &lbid, sizeof(lbid)) % vss->numHashBuckets;

	currentIndex = hashBuckets[hashIndex];
	while (currentIndex != -1) {
		listEntry = &storage[currentIndex];
		if (listEntry->lbid == lbid) {

			if (vbOnly && !listEntry->vbFlag)
				goto next;

			/* "if it is/was part of a different transaction, ignore it"
			 */
			if (txnID != listEntry->verID &&
					(verInfo.txns->find(listEntry->verID) != verInfo.txns->end()))
				goto next;

			/* fast exit if the exact version is found */
			if (verInfo.currentScn == listEntry->verID) {
				*outVer = listEntry->verID;
				*vbFlag = listEntry->vbFlag;
				return 0;
			}

			/* Track the min version of this LBID */
			if (minVersion > listEntry->verID || minVersion == -1)
				minVersion = listEntry->verID;

			/* Pick the highest version <= the SCN of the query */
			if (verInfo.currentScn > listEntry->verID && maxVersion < listEntry->verID) {
				maxVersion = listEntry->verID;
				maxEntry = listEntry;
			}
		}
next:
		currentIndex = listEntry->next;
	}
	if (maxEntry != NULL) {
		*outVer = maxVersion;
		*vbFlag = maxEntry->vbFlag;
		return 0;
	}
	else if (minVersion > verInfo.currentScn) {
		*outVer = 0;
		*vbFlag = false;
		return ERR_SNAPSHOT_TOO_OLD;
	}

	*outVer = 0;
	*vbFlag = false;
	return -1;
}

VER_t VSS::getCurrentVersion(LBID_t lbid, bool *isLocked) const
{
	int hashIndex, currentIndex;
	VSSEntry *listEntry;

	hashIndex = hasher((char *) &lbid, sizeof(lbid)) % vss->numHashBuckets;
	currentIndex = hashBuckets[hashIndex];
	while (currentIndex != -1) {
		listEntry = &storage[currentIndex];
		if (listEntry->lbid == lbid && !listEntry->vbFlag) {
			if (isLocked != NULL)
				*isLocked = listEntry->locked;
			return listEntry->verID;
		}
		currentIndex = listEntry->next;
	}
	if (isLocked != NULL)
		*isLocked = false;
	return 0;
}

VER_t VSS::getHighestVerInVB(LBID_t lbid, VER_t max) const
{
	int hashIndex, currentIndex;
	VER_t ret = -1;
	VSSEntry *listEntry;

	hashIndex = hasher((char *) &lbid, sizeof(lbid)) % vss->numHashBuckets;
	currentIndex = hashBuckets[hashIndex];
	while (currentIndex != -1) {
		listEntry = &storage[currentIndex];
		if ((listEntry->lbid == lbid && listEntry->vbFlag) &&
				(listEntry->verID <= max && listEntry->verID > ret))
			ret = listEntry->verID;
		currentIndex = listEntry->next;
	}
	return ret;
}

bool VSS::isVersioned(LBID_t lbid, VER_t version) const
{
	int hashIndex, currentIndex;
	VSSEntry *listEntry;

	hashIndex = hasher((char *) &lbid, sizeof(lbid)) % vss->numHashBuckets;
	currentIndex = hashBuckets[hashIndex];
	while (currentIndex != -1) {
		listEntry = &storage[currentIndex];
		if (listEntry->lbid == lbid && listEntry->verID == version)
			return listEntry->vbFlag;
		currentIndex = listEntry->next;
	}
	return false;
}

bool VSS::isLocked(const LBIDRange& range, VER_t transID) const
{
	int hashIndex, currentIndex;
	LBID_t currentBlock;
	VSSEntry *listEntry;

	for (currentBlock = range.start;
			currentBlock < range.start + range.size;
			currentBlock++) {

		hashIndex = hasher((char *) &currentBlock, sizeof(currentBlock)) % vss->numHashBuckets;

		currentIndex = hashBuckets[hashIndex];
		while (currentIndex != -1) {
			listEntry = &storage[currentIndex];
			if (listEntry->lbid == currentBlock && listEntry->locked)
			{
				if (listEntry->verID == transID)
					return false;
				else
					return true;
			}
			currentIndex = listEntry->next;
		}
	}
	return false;
}

//requires write lock
void VSS::removeEntry(LBID_t lbid, VER_t verID, vector<LBID_t> *flushList)
{
	int index, prev, bucket;

#ifdef BRM_DEBUG
	if (lbid < 0) {
		log("VSS::removeEntry(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::removeEntry(): lbid must be >= 0");
	}
	if (verID < 0) {
		log("VSS::removeEntry(): verID must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::removeEntry(): verID must be >= 0");
	}
#endif

	index = getIndex(lbid, verID, prev, bucket);
	if (index == -1) {
#ifdef BRM_DEBUG
		ostringstream ostr;

		ostr << "VSS::removeEntry(): that entry doesn't exist lbid = " <<
			lbid << " verID = " << verID;
		log(ostr.str(), logging::LOG_TYPE_DEBUG);
		throw logic_error(ostr.str());
#else
		return;
#endif
	}

	makeUndoRecord(&storage[index], sizeof(VSSEntry));
	storage[index].lbid = -1;
	if (prev != -1) {
		makeUndoRecord(&storage[prev], sizeof(VSSEntry));
		storage[prev].next = storage[index].next;
	}
	else {
		makeUndoRecord(&hashBuckets[bucket], sizeof(int));
		hashBuckets[bucket] = storage[index].next;
	}
	makeUndoRecord(vss, sizeof(VSSShmsegHeader));
	vss->currentSize--;
	if (storage[index].locked && (vss->lockedEntryCount > 0))
		vss->lockedEntryCount--;
	if (index < vss->LWM)
		vss->LWM = index;

	// scan the list of entries with that lbid to see if there are others
	// to remove.
	for (index = hashBuckets[bucket]; index != -1; index = storage[index].next)
		if (storage[index].lbid == lbid && (storage[index].vbFlag || storage[index].locked))
			return;

	// if execution gets here, we should be able to remove all entries
	// with the given lbid because none point to the VB.
	for (prev = -1, index = hashBuckets[bucket];
		index != -1; index = storage[index].next) {
		if (storage[index].lbid == lbid) {
			makeUndoRecord(&storage[index], sizeof(VSSEntry));
			storage[index].lbid = -1;
			if (prev == -1) {
				makeUndoRecord(&hashBuckets[bucket], sizeof(int));
				hashBuckets[bucket] = storage[index].next;
			}
			else {
				makeUndoRecord(&storage[prev], sizeof(VSSEntry));
				storage[prev].next = storage[index].next;
			}
			vss->currentSize--;
			if (storage[index].locked && (vss->lockedEntryCount > 0))
				vss->lockedEntryCount--;
			if (index < vss->LWM)
				vss->LWM = index;
		}
		else
			prev = index;
	}
	flushList->push_back(lbid);
}

bool VSS::isTooOld(LBID_t lbid, VER_t verID) const
{
	int index, bucket;
	VER_t minVer = 0;
	VSSEntry *listEntry;

	bucket = hasher((char *) &lbid, sizeof(lbid)) % vss->numHashBuckets;

	index = hashBuckets[bucket];
	while (index != -1) {
		listEntry = &storage[index];
		if (listEntry->lbid == lbid && minVer > listEntry->verID)
			minVer = listEntry->verID;
		index = listEntry->next;
	}
	return (minVer > verID);
}

bool VSS::isEntryLocked(LBID_t lbid, VER_t verID) const
{
	int index;
	int bucket;

	if (lbid == -1)
		return false;
/* See bug 3287 for info on these code blocks */

/*  This version checks for the specific lbid,ver pair
	index = getIndex(lbid, verID, prev, bucket);
	if (index < 0)
		return false;
	return storage[index].locked;
*/

/*  This version considers any locked entry for an LBID to mean the block is locked.
 	VSSEntry *listEntry;
	bucket = hasher((char *) &lbid, sizeof(lbid)) % vss->numHashBuckets;

	index = hashBuckets[bucket];
	while (index != -1) {
		listEntry = &storage[index];
		if (listEntry->lbid == lbid && listEntry->locked)
		{
			ostringstream msg;
			msg << " Locked entry information lbid:verID = " << listEntry->lbid << ":" << listEntry->verID << endl;
			log(msg.str(), logging::LOG_TYPE_CRITICAL);
			return true;
		}
		index = listEntry->next;
	}
	return false;
*/

// This version considers the blocks needed for rollback to be locked,
// otherwise they're unlocked.  Note, the version with the 'locked' flag set
// will never be a candidate for aging out b/c it's not in the version buffer.
// TODO:  Update this when we support multiple transactions at once.  Need to
// identify ALL versions needed for rollback.

	VSSEntry *listEntry;
	bool hasALockedEntry = false;
	VER_t rollbackVersion = 0;

	bucket = hasher((char *) &lbid, sizeof(lbid)) % vss->numHashBuckets;

	index = hashBuckets[bucket];
	while (index != -1) {
		listEntry = &storage[index];
		if (listEntry->lbid == lbid) {
			if (listEntry->locked)
				hasALockedEntry = true;
			else if (rollbackVersion < listEntry->verID)
				rollbackVersion = listEntry->verID;
		}
		index = listEntry->next;
	}
	return (hasALockedEntry && verID == rollbackVersion);
}

//read lock
int VSS::getIndex(LBID_t lbid, VER_t verID, int &prev, int &bucket) const
{
	int currentIndex;
	VSSEntry *listEntry;

	prev = -1;
	bucket = hasher((char *) &lbid, sizeof(lbid)) % vss->numHashBuckets;

	currentIndex = hashBuckets[bucket];
	while (currentIndex != -1) {
		listEntry = &storage[currentIndex];
		if (listEntry->lbid == lbid && listEntry->verID == verID)
			return currentIndex;
		prev = currentIndex;
		currentIndex = listEntry->next;
	}
	return -1;
}

//write lock
void VSS::setVBFlag(LBID_t lbid, VER_t verID, bool vbFlag)
{
	int index, prev, bucket;

#ifdef BRM_DEBUG
	if (lbid < 0) {
		log("VSS::setVBFlag(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::setVBFlag(): lbid must be >= 0");
	}
	if (verID < 0) {
		log("VSS::setVBFlag(): verID must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::setVBFlag(): verID must be >= 0");
	}
#endif

 	index = getIndex(lbid, verID, prev, bucket);
	if (index == -1) {
		ostringstream ostr;

		ostr << "VSS::setVBFlag(): that entry doesn't exist lbid=" << lbid <<
			" ver=" << verID;
		log(ostr.str(), logging::LOG_TYPE_DEBUG);
		throw logic_error(ostr.str());
	}
	makeUndoRecord(&storage[index], sizeof(VSSEntry));
	storage[index].vbFlag = vbFlag;
}

//write lock
void VSS::commit(VER_t txnID)
{
	int i;

#ifdef BRM_DEBUG
	if (txnID < 1) {
		log("VSS::commit(): txnID must be > 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::commit(): txnID must be > 0");
	}
#endif

	for (i = 0; i < vss->capacity; i++)
		if (storage[i].lbid != -1 && storage[i].verID == txnID) {
#ifdef BRM_DEBUG
			if (storage[i].locked != true) {
				ostringstream ostr;
				ostr << "VSS::commit(): An entry has already been unlocked..? txnId = " << txnID ;
				log(ostr.str(), logging::LOG_TYPE_DEBUG);
				throw logic_error(ostr.str());
			}
#endif
			makeUndoRecord(&storage[i], sizeof(VSSEntry));
			storage[i].locked = false;

			// @ bug 1426 fix. Decrease the counter when an entry releases its lock.
			if (vss->lockedEntryCount > 0)
				vss->lockedEntryCount--;
		}
}

//read lock
void VSS::getUncommittedLBIDs(VER_t txnID, vector<LBID_t>& lbids)
{
	int i;

	lbids.clear();

#ifdef BRM_DEBUG
	if (txnID < 1) {
		log("VSS::getUncommittedLBIDs(): txnID must be > 0",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::getUncommittedLBIDs(): txnID must be > 0");
	}
#endif

	for (i = 0; i < vss->capacity; i++)
		if (storage[i].lbid != -1 && storage[i].verID == txnID) {
#ifdef BRM_DEBUG
			if (storage[i].locked == false) {
				log("VSS::getUncommittedLBIDs(): found an unlocked block with that TxnID",
					logging::LOG_TYPE_DEBUG);
				throw logic_error("VSS::getUncommittedLBIDs(): found an unlocked block with that TxnID");
			}
			if (storage[i].vbFlag == true) {
				log("VSS::getUncommittedLBIDs(): found a block with that TxnID in the VB",
					logging::LOG_TYPE_DEBUG);
				throw logic_error("VSS::getUncommittedLBIDs(): found a block with that TxnID in the VB");
			}
#endif
			lbids.push_back(storage[i].lbid);
		}

}

void VSS::getUnlockedLBIDs(BlockList_t& lbids)
{
	lbids.clear();

	for (int i = 0; i < vss->capacity; i++)
		if (storage[i].lbid != -1 && !storage[i].locked)
			lbids.push_back(LVP_t(storage[i].lbid, storage[i].verID));
}

void VSS::getLockedLBIDs(BlockList_t& lbids)
{
	lbids.clear();

	for (int i = 0; i < vss->capacity; i++)
		if (storage[i].lbid != -1 && storage[i].locked)
			lbids.push_back(LVP_t(storage[i].lbid, storage[i].verID));
}
//write lock
/* Rewritten on 6/2/10 to be O(n) with the size of range rather than
 * O(nlogn) with VSS capacity. */
void VSS::removeEntriesFromDB(const LBIDRange& range, VBBM& vbbm, bool use_vbbm)
{
	int bucket, index, prev;
	LBID_t lastLBID, lbid;

#ifdef BRM_DEBUG
	if (range.start < 0) {
		log("VSS::removeEntriesFromDB(): lbids must be positive.",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::removeEntriesFromDB(): lbids must be positive.");
	}
	if (range.size < 1) {
		log("VSS::removeEntriesFromDB(): size must be > 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VSS::removeEntriesFromDB(): size must be > 0");
	}
#endif

	makeUndoRecord(vss, sizeof(VSSShmsegHeader));

	lastLBID = range.start + range.size - 1;
	for (lbid = range.start; lbid <= lastLBID; lbid++) {
		bucket = hasher((char *) &lbid, sizeof(lbid)) % vss->numHashBuckets;

		for (prev = -1, index = hashBuckets[bucket]; index != -1;
		  index = storage[index].next) {
			if (storage[index].lbid == lbid) {
				if (storage[index].vbFlag && use_vbbm)
					vbbm.removeEntry(storage[index].lbid, storage[index].verID);
				makeUndoRecord(&storage[index], sizeof(VSSEntry));
				storage[index].lbid = -1;
				if (prev == -1) {
					makeUndoRecord(&hashBuckets[bucket], sizeof(int));
					hashBuckets[bucket] = storage[index].next;
				}
				else {
					makeUndoRecord(&storage[prev], sizeof(VSSEntry));
					storage[prev].next = storage[index].next;
				}
				vss->currentSize--;
				if (storage[index].locked && (vss->lockedEntryCount > 0))
					vss->lockedEntryCount--;
				if (index < vss->LWM)
					vss->LWM = index;
			}
			else
				prev = index;
		}
	}
}

int VSS::size() const
{
	int i, ret = 0;

	for (i = 0; i < vss->capacity; i++)
		if (storage[i].lbid != -1)
			ret++;

	if (ret != vss->currentSize) {
		ostringstream ostr;

		ostr << "VSS: actual size & recorded size disagree.  actual size = "
			<< ret << " recorded size = " << vss->currentSize;
		log(ostr.str(), logging::LOG_TYPE_DEBUG);
		throw logic_error(ostr.str());
	}

	return ret;
}

bool VSS::hashEmpty() const
{
	int i;

	for (i = 0; i < vss->numHashBuckets; i++)
		if (hashBuckets[i] != -1)
			return false;
	return true;
}

void VSS::clear()
{
	int allocSize;
	key_t newshmkey;
	char *newshmseg;

	allocSize = VSS_INITIAL_SIZE;

	newshmkey = chooseShmkey();

	idbassert(fPVSSImpl);
	idbassert(fPVSSImpl->key() != (unsigned)newshmkey);
	fPVSSImpl->clear(newshmkey, allocSize);
	vssShminfo->tableShmkey = newshmkey;
	vssShminfo->allocdSize = allocSize;
	vss = fPVSSImpl->get();
	initShmseg();
	if (r_only)
	{
		fPVSSImpl->makeReadOnly();
		vss = fPVSSImpl->get();
	}
	newshmseg = reinterpret_cast<char*>(vss);
	hashBuckets = reinterpret_cast<int*>
			(&newshmseg[sizeof(VSSShmsegHeader)]);
	storage = reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) +
			vss->numHashBuckets*sizeof(int)]);
}

// read lock
int VSS::checkConsistency(const VBBM &vbbm, ExtentMap &em) const
{
	/*
	1. Every valid entry in the VSS has an entry either in the VBBM or in the
	EM.  Verify that.
	2. Struct consistency checks
		a. current size agrees with actual # of used entries
		b. there are no empty elements in the hashed lists
		c. each hash table entry points to a non-empty element or -1
		d. verify that there are no empty entries below the LWM
		e. verify uniqueness of the entries

	*/

	int i, j, k, err;

	/* Test 1 */

	OID_t oid;
	uint32_t fbo;

	for (i = 0; i < vss->capacity; i++) {
		if (storage[i].lbid != -1) {
			if (storage[i].vbFlag) {
				err = vbbm.lookup(storage[i].lbid, storage[i].verID, oid,
								   fbo);
				if (err != 0) {
					cerr << "VSS: lbid=" << storage[i].lbid << " verID=" <<
							storage[i].verID << " vbFlag=true isn't in the VBBM" <<
							endl;
					throw logic_error("VSS::checkConsistency(): a VSS entry with vbflag set is not in the VBBM");
				}
			}
			else {
// This version of em.lookup was made obsolete with multiple files per OID.
// If ever want to really use this checkConsistency() function, this section
// of code needs to be updated to use the new lookup() API.
#if 0
				err = em.lookup(storage[i].lbid, (int&)oid, fbo);
				if (err != 0) {
					cerr << "VSS: lbid=" << storage[i].lbid << " verID=" <<
							storage[i].verID << " vbFlag=false has no extent" <<
							endl;
					throw logic_error("VSS::checkConsistency(): a VSS entry with vbflag unset is not in the EM");
				}
#endif
			}
		}
	}

	/* Test 2a is already implemented */
	size();

	/* Tests 2b & 2c - no empty elements reachable from the hash table */

	int nextElement;

	for (i = 0; i < vss->numHashBuckets; i++) {
		if (hashBuckets[i] != -1)
			for (nextElement = hashBuckets[i]; nextElement != -1;
				nextElement = storage[nextElement].next)
				if (storage[nextElement].lbid == -1)
					throw logic_error("VSS::checkConsistency(): an empty storage entry is reachable from the hash table");
	}

	/* Test 2d - verify that there are no empty entries below the LWM */

	for (i = 0; i < vss->LWM; i++)
		if (storage[i].lbid == -1) {
			cerr << "VSS: LWM=" << vss->LWM << " first empty entry=" << i << endl;
			throw logic_error("VSS::checkConsistency(): LWM accounting error");
		}

	/* Test 2e - verify uniqueness of each entry */

	for (i = 0;	i < vss->numHashBuckets; i++)
		if (hashBuckets[i] != -1)
			for (j = hashBuckets[i]; j != -1; j = storage[j].next)
				for (k = storage[j].next; k != -1; k = storage[k].next)
					if (storage[j].lbid == storage[k].lbid &&
						storage[j].verID == storage[k].verID) {
						cerr << "VSS: lbid=" << storage[j].lbid << " verID=" <<
							storage[j].verID << endl;
						throw logic_error("VSS::checkConsistency(): Duplicate entry found");
					}
	return 0;
}

void VSS::setReadOnly()
{
	r_only = true;
}

void VSS::getCurrentTxnIDs(set<VER_t> &list) const
{
	int i;

	for (i = 0; i < vss->capacity; i++)
		if (storage[i].lbid != -1 && storage[i].locked)
			list.insert(storage[i].verID);
}

/* File Format:

		VSS V1 magic (32-bits)
		# of VSS entries in capacity (32-bits)
		struct VSSEntry * #
*/

struct Header {
	int magic;
	int entries;
};

// read lock
void VSS::save(string filename)
{
	int i;
	struct Header header;
	mode_t utmp = ::umask(0);

	if (IDBPolicy::useHdfs()) {
		const char* filename_p = filename.c_str();
		scoped_ptr<IDBDataFile> out(IDBDataFile::open(
									IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
									filename_p, "wb", IDBDataFile::USE_VBUF));
		::umask(utmp);
		if (!out) {
			log_errno("VSS::save()");
			throw runtime_error("VSS::save(): Failed to open the file");
		}

		header.magic = VSS_MAGIC_V1;
		header.entries = vss->currentSize;

		if (out->write((char *)&header, sizeof(header)) != sizeof(header)) {
			log_errno("VSS::save()");
			throw runtime_error("VSS::save(): Failed to write header to the file");
		}

		for (i = 0; i < vss->capacity; i++) {
			if (storage[i].lbid != -1) {
				if (out->write((char *)&storage[i], sizeof(VSSEntry)) != sizeof(VSSEntry)) {
					log_errno("VSS::save()");
					throw runtime_error("VSS::save(): Failed to write vss entry to the file");
				}
			}
		}
	}
	else {
		ofstream out;
		out.open(filename.c_str(), ios_base::trunc|ios_base::out|ios_base::binary);
		::umask(utmp);
		if (!out) {
			log_errno("VSS::save()");
			throw runtime_error("VSS::save(): Failed to open the file");
		}
		out.exceptions(ios_base::badbit);

		header.magic = VSS_MAGIC_V1;
		header.entries = vss->currentSize;

		try {
			out.write((char *)&header, sizeof(header));

			for (i = 0; i < vss->capacity; i++)
				if (storage[i].lbid != -1)
					out.write((char *)&storage[i], sizeof(VSSEntry));
		}
		catch (std::exception &e) {
			out.close();
			throw;
		}
		out.close();
	}

}

// Ideally, we;d like to get in and out of this fcn as quickly as possible.
bool VSS::isEmpty(bool useLock)
{
#if 0
	if (fPVSSImpl == 0 || fPVSSImpl->key() != (unsigned)mst.getVSSShmkey())
	{
		lock(READ);
		release(READ);
	}

	//this really should be done under a read lock. There's a small chance that between the release()
	// above and now that the underlying SHM could be changed. This would be exacerbated during
	// high DML/query activity.
	return (fPVSSImpl->get()->currentSize == 0);
#endif
	//Should be race-free, but takes along time...
	bool rc;
	if (useLock)
		lock(READ);
	rc = (fPVSSImpl->get()->currentSize == 0);
	if (useLock)
		release(READ);
	return rc;
}


//#include "boost/date_time/posix_time/posix_time.hpp"
//using namespace boost::posix_time;

void VSS::load(string filename)
{
	int i;
	struct Header header;
	struct VSSEntry entry;
	//ptime time1, time2;

	//time1 = microsec_clock::local_time();
	//cout << "loading the VSS " << time1 << endl;
	const char* filename_p = filename.c_str();
	scoped_ptr<IDBDataFile>  in(IDBDataFile::open(
								IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
								filename_p, "rb", 0));
	if (!in) {
		log_errno("VSS::load()");
		throw runtime_error("VSS::load(): Failed to open the file");
	}

	if (in->read((char *)&header, sizeof(header)) != sizeof(header)) {
		log_errno("VSS::load()");
		throw runtime_error("VSS::load(): Failed to read header");
	}

	if (header.magic != VSS_MAGIC_V1) {
		log("VSS::load(): Bad magic.  Not a VSS file?");
		throw runtime_error("VSS::load(): Bad magic.  Not a VSS file?");
	}
	if (header.entries < 0) {
		log("VSS::load(): Bad size.  Not a VSS file?");
		throw runtime_error("VSS::load(): Bad size.  Not a VSS file?");
	}

	growForLoad(header.entries);

/*
	for (i = 0; i < vss->capacity; i++)
		storage[i].lbid = -1;
	for (i = 0; i < vss->numHashBuckets; i++)
		hashBuckets[i] = -1;
	vss->currentSize = 0;
	vss->lockedEntryCount = 0;
	vss->LWM = 0;
*/

	for (i = 0; i < header.entries; i++) {
		if (in->read((char *)&entry, sizeof(entry)) != sizeof(entry)) {
			log_errno("VSS::load()");
			throw runtime_error("VSS::load(): Failed to read entry");
		}
		insert(entry.lbid, entry.verID, entry.vbFlag, entry.locked, true);
	}

	//time2 = microsec_clock::local_time();
	//cout << "done loading " << time2 << " duration: " << time2-time1 << endl;
}

#ifndef __LP64__
//This code is OBE now that the structs are padded correctly
struct VSSEntry_ {
	LBID_t lbid;
	VER_t verID;
	bool vbFlag;
	bool locked;
	int next;
	uint32_t pad1;
};

void VSS::load64(string filename)
{
	int i;
	struct Header header;
	struct VSSEntry_ entry;

	const char* filename_p = filename.c_str();
	scoped_ptr<IDBDataFile>  in(IDBDataFile::open(
								IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
								filename_p, "rb", 0));
	if (!in) {
		log_errno("VSS::load()");
		throw runtime_error("VSS::load(): Failed to open the file");
	}

	if (in->read((char *)&header, sizeof(header)) != sizeof(header)) {
		log_errno("VSS::load()");
		throw runtime_error("VSS::load(): Failed to read header");
	}

	if (header.magic != VSS_MAGIC_V1) {
		log("VSS::load(): Bad magic.  Not a VSS file?");
		throw runtime_error("VSS::load(): Bad magic.  Not a VSS file?");
	}
	if (header.entries < 0) {
		log("VSS::load(): Bad size.  Not a VSS file?");
		throw runtime_error("VSS::load(): Bad size.  Not a VSS file?");
	}

	for (i = 0; i < vss->capacity; i++)
		storage[i].lbid = -1;
	for (i = 0; i < vss->numHashBuckets; i++)
		hashBuckets[i] = -1;
	vss->currentSize = 0;
	vss->lockedEntryCount = 0;
	vss->LWM = 0;

	for (i = 0; i < header.entries; i++) {
		if (in->read((char *)&entry, sizeof(entry)) != sizeof(entry)) {
			log_errno("VSS::load()");
			throw runtime_error("VSS::load(): Failed to read entry");
		}
		insert(entry.lbid, entry.verID, entry.vbFlag, entry.locked, true);
	}
}
#endif

#ifdef BRM_DEBUG
// read lock
int VSS::getShmid() const
{
	return vssShmid;
}
#endif

QueryContext_vss::QueryContext_vss(const QueryContext &qc) :
	currentScn(qc.currentScn)
{
	txns.reset(new set<VER_t>());
	for (uint32_t i = 0; i < qc.currentTxns->size(); i++)
		txns->insert((*qc.currentTxns)[i]);
}

}   //namespace
