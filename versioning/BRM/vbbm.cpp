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
 * $Id: vbbm.cpp 1926 2013-06-30 21:18:14Z wweeks $
 *
 ****************************************************************************/

#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>

#if __linux__
#include <values.h>
#endif
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifndef _MSC_VER
#include <ext/stdio_filebuf.h>
#else
#include <io.h>
#endif

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
namespace bi=boost::interprocess;

#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>

#include "blocksize.h"
#include "rwlock.h"
#include "brmtypes.h"
#include "mastersegmenttable.h"
#include "vss.h"
#include "configcpp.h"
#include "exceptclasses.h"
#include "hasher.h"
#include "cacheutils.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"

#define VBBM_DLLEXPORT
#include "vbbm.h"
#undef VBBM_DLLEXPORT

#define VBBM_MAGIC_V1 0x7b27ec13
#define VBBM_MAGIC_V2 0x1fb58c7a
#define VBBM_CHUNK_SIZE 100

using namespace std;
using namespace boost;
using namespace idbdatafile;

namespace BRM {

VBBMEntry::VBBMEntry()
{
	lbid = -1;
	verID = 0;
	vbOID = 0;
	vbFBO = 0;
	next = -1;
}

/*static*/
boost::mutex VBBMImpl::fInstanceMutex;
boost::mutex VBBM::mutex;

/*static*/
VBBMImpl* VBBMImpl::fInstance=0;

/*static*/
VBBMImpl* VBBMImpl::makeVBBMImpl(unsigned key, off_t size, bool readOnly)
{
	boost::mutex::scoped_lock lk(fInstanceMutex);

	if (fInstance)
	{
		if (key != fInstance->fVBBM.key())
		{
			BRMShmImpl newShm(key, size);
			fInstance->swapout(newShm);
		}
		idbassert(key == fInstance->fVBBM.key());
		return fInstance;
	}

	fInstance = new VBBMImpl(key, size, readOnly);

	return fInstance;
}

VBBMImpl::VBBMImpl(unsigned key, off_t size, bool readOnly) :
	fVBBM(key, size, readOnly)
{
}

VBBM::VBBM()
{
	vbbm = NULL;
	currentVBBMShmkey = -1;
	vbbmShmid = 0;
	vbbmShminfo = NULL;
	r_only = false;
	fPVBBMImpl = 0;
	currentFileSize = 0;
}

VBBM::~VBBM()
{
}

void VBBM::initShmseg(int nFiles)
{
//	VBFileMetadata *newfiles;
	int *newBuckets;
	VBBMEntry *newStorage;
	int i;
	char *shmseg;

	vbbm->vbCapacity = VBSTORAGE_INITIAL_SIZE/sizeof(VBBMEntry);
	vbbm->vbCurrentSize = 0;
	vbbm->vbLWM = 0;
	vbbm->numHashBuckets = VBTABLE_INITIAL_SIZE/sizeof(int);
	shmseg = reinterpret_cast<char*>(vbbm);
//	newfiles = reinterpret_cast<VBFileMetadata*>
//			(&shmseg[sizeof(VBShmsegHeader)]);
	newBuckets = reinterpret_cast<int*>
			(&shmseg[sizeof(VBShmsegHeader) +
			nFiles*sizeof(VBFileMetadata)]);
	newStorage = reinterpret_cast<VBBMEntry*>(&shmseg[sizeof(VBShmsegHeader) +
			nFiles*sizeof(VBFileMetadata) +
			vbbm->numHashBuckets*sizeof(int)]);
	setCurrentFileSize();
	vbbm->nFiles = nFiles;
	for (i = 0; i < vbbm->numHashBuckets; i++)
		newBuckets[i] = -1;
	for (i = 0; i < vbbm->vbCapacity; i++)
		newStorage[i].lbid = -1;
}

// ported from ExtentMap
void VBBM::lock(OPS op)
{
	char *shmseg;

	if (op == READ) {
		vbbmShminfo = mst.getTable_read(MasterSegmentTable::VBBMSegment);
		mutex.lock();
	}
	else
		vbbmShminfo = mst.getTable_write(MasterSegmentTable::VBBMSegment);

	// this means that either the VBBM isn't attached or that it was resized
	if (currentVBBMShmkey != vbbmShminfo->tableShmkey) {
		if (vbbm != NULL) {
			vbbm = NULL;
		}
		if (vbbmShminfo->allocdSize == 0) {
			if (op == READ) {
				mutex.unlock();
				mst.getTable_upgrade(MasterSegmentTable::VBBMSegment);
				if (vbbmShminfo->allocdSize == 0) {
					try {
						growVBBM();
					}
					catch(...) {
						release(WRITE);
						throw;
					}
				}
				mst.getTable_downgrade(MasterSegmentTable::VBBMSegment);
			}
			else {
				try {
					growVBBM();
				}
				catch(...) {
					release(WRITE);
					throw;
				}
			}
		}
		else {
			currentVBBMShmkey = vbbmShminfo->tableShmkey;
			fPVBBMImpl = VBBMImpl::makeVBBMImpl(currentVBBMShmkey, 0);
			idbassert(fPVBBMImpl);
			if (r_only)
				fPVBBMImpl->makeReadOnly();
			vbbm = fPVBBMImpl->get();
			shmseg = reinterpret_cast<char *>(vbbm);
			files = reinterpret_cast<VBFileMetadata*>
					(&shmseg[sizeof(VBShmsegHeader)]);
			hashBuckets = reinterpret_cast<int*>
					(&shmseg[sizeof(VBShmsegHeader) +
					vbbm->nFiles*sizeof(VBFileMetadata)]);
			storage = reinterpret_cast<VBBMEntry*>(&shmseg[sizeof(VBShmsegHeader) +
					vbbm->nFiles*sizeof(VBFileMetadata) +
					vbbm->numHashBuckets*sizeof(int)]);
			if (op == READ)
				mutex.unlock();
		}
	}
	else
		if (op == READ)
			mutex.unlock();
}

// ported from ExtentMap
void VBBM::release(OPS op)
{
	if (op == READ)
		mst.releaseTable_read(MasterSegmentTable::VBBMSegment);
	else
		mst.releaseTable_write(MasterSegmentTable::VBBMSegment);
}

//assumes write lock is held
// Right now, adding a file and growing are mutually exclusive ops.
void VBBM::growVBBM(bool addAFile)
{
	int allocSize;
	int nFiles = -1;
	key_t newshmkey;
	char *newshmseg;

	if (vbbmShminfo->allocdSize == 0) {
		if (addAFile)
			nFiles = 1;
		else
			nFiles = 0;
		allocSize = (sizeof(VBShmsegHeader) +
			(nFiles*sizeof(VBFileMetadata)) +
			VBSTORAGE_INITIAL_SIZE + VBTABLE_INITIAL_SIZE);
	}
	else {
		if (!addAFile)
			allocSize = vbbmShminfo->allocdSize + VBBM_INCREMENT;
		else {
			vbbm->nFiles++;
			allocSize = vbbmShminfo->allocdSize + sizeof(VBFileMetadata);
		}
	}

	newshmkey = chooseShmkey();
	if (fPVBBMImpl) {
		BRMShmImpl newShm(newshmkey, allocSize);
		newshmseg = static_cast<char*>(newShm.fMapreg.get_address());
		memset(newshmseg, 0, allocSize);
		if (vbbm != NULL) {
			VBShmsegHeader *tmp = reinterpret_cast<VBShmsegHeader*>(newshmseg);
			tmp->vbCapacity = vbbm->vbCapacity;
			tmp->numHashBuckets = vbbm->numHashBuckets;
			if (!addAFile) {
				tmp->vbCapacity += VBSTORAGE_INCREMENT/sizeof(VBBMEntry);
				tmp->numHashBuckets += VBTABLE_INCREMENT/sizeof(int);
			}
			tmp->vbLWM = 0;
			copyVBBM(tmp);
		}

		undoRecords.clear();
		fPVBBMImpl->swapout(newShm);
	}
	else {
		fPVBBMImpl = VBBMImpl::makeVBBMImpl(newshmkey, allocSize);
		newshmseg = reinterpret_cast<char*>(fPVBBMImpl->get());
		memset(newshmseg, 0, allocSize);
	}

	vbbm = fPVBBMImpl->get();
	if (vbbmShminfo->allocdSize == 0)   // this means the shmseg was created by this call
		initShmseg(nFiles);
	vbbmShminfo->tableShmkey = currentVBBMShmkey = newshmkey;
	vbbmShminfo->allocdSize = allocSize;
	if (r_only)
		fPVBBMImpl->makeReadOnly();

	files = reinterpret_cast<VBFileMetadata*>
			(&newshmseg[sizeof(VBShmsegHeader)]);
	hashBuckets = reinterpret_cast<int*>
			(&newshmseg[sizeof(VBShmsegHeader) +
			vbbm->nFiles*sizeof(VBFileMetadata)]);
	storage = reinterpret_cast<VBBMEntry*>(&newshmseg[sizeof(VBShmsegHeader) +
			vbbm->nFiles*sizeof(VBFileMetadata) +
			vbbm->numHashBuckets*sizeof(int)]);
}

void VBBM::growForLoad(int count)
{
	int allocSize;
	int nFiles;
	key_t newshmkey;
	char *newshmseg;
	int i;

	if (vbbm)
		nFiles = vbbm->nFiles;
	else
		nFiles = 0;

	if (count < VBSTORAGE_INITIAL_COUNT)
		count = VBSTORAGE_INITIAL_COUNT;
	// round up to next normal increment point out of paranoia.
	if (count % VBSTORAGE_INCREMENT_COUNT)
		count = ((count/VBSTORAGE_INCREMENT_COUNT) + 1) * VBSTORAGE_INCREMENT_COUNT;
	allocSize = VBBM_SIZE(nFiles, count);

	newshmkey = chooseShmkey();
	if (fPVBBMImpl) {
		BRMShmImpl newShm(newshmkey, allocSize);
		newshmseg = static_cast<char*>(newShm.fMapreg.get_address());
		// copy the file meta to the new segment
		memcpy((char *) &newshmseg[sizeof(VBShmsegHeader)], files, sizeof(VBFileMetadata)*nFiles);
		fPVBBMImpl->swapout(newShm);
	}
	else {
		fPVBBMImpl = VBBMImpl::makeVBBMImpl(newshmkey, allocSize);
	}

	vbbm = fPVBBMImpl->get();
	vbbm->nFiles = nFiles;
	vbbm->vbCapacity = count;
	vbbm->vbLWM = 0;
	vbbm->numHashBuckets = count/4;

	vbbmShminfo->tableShmkey = currentVBBMShmkey = newshmkey;
	vbbmShminfo->allocdSize = allocSize;
	newshmseg = (char *) vbbm;
	files = reinterpret_cast<VBFileMetadata*>
			(&newshmseg[sizeof(VBShmsegHeader)]);
	hashBuckets = reinterpret_cast<int*>
			(&newshmseg[sizeof(VBShmsegHeader) +
			vbbm->nFiles*sizeof(VBFileMetadata)]);
	storage = reinterpret_cast<VBBMEntry*>(&newshmseg[sizeof(VBShmsegHeader) +
			vbbm->nFiles*sizeof(VBFileMetadata) +
			vbbm->numHashBuckets*sizeof(int)]);

	for (i = 0; i < vbbm->numHashBuckets; i++)
		hashBuckets[i] = -1;
	for (i = 0; i < vbbm->vbCapacity; i++)
		storage[i].lbid = -1;
	undoRecords.clear();
}

//assumes write lock is held and the src is vbbm
//and that dest->{numHashBuckets, vbCapacity, vbLWM} have been set.
void VBBM::copyVBBM(VBShmsegHeader *dest)
{
	int i;
	int *newHashtable;
	VBBMEntry *newStorage;
	VBFileMetadata *newFiles;
	char *cDest = reinterpret_cast<char *>(dest);

	// copy metadata
	dest->nFiles = vbbm->nFiles;
	dest->vbCurrentSize = vbbm->vbCurrentSize;

	newFiles = reinterpret_cast<VBFileMetadata*>(&cDest[sizeof(VBShmsegHeader)]);
	newHashtable = reinterpret_cast<int*>(&cDest[sizeof(VBShmsegHeader) +
			dest->nFiles*sizeof(VBFileMetadata)]);
	newStorage = reinterpret_cast<VBBMEntry*>(&cDest[sizeof(VBShmsegHeader) +
			dest->nFiles*sizeof(VBFileMetadata) +
			dest->numHashBuckets*sizeof(int)]);

	memcpy(newFiles, files, sizeof(VBFileMetadata)*vbbm->nFiles);

	//initialize new storage & hash
	for (i = 0; i < dest->numHashBuckets; i++)
		newHashtable[i] = -1;

	for (i = 0; i < dest->vbCapacity; i++)
		newStorage[i].lbid = -1;

	//walk the storage & re-hash all entries;
	for (i = 0; i < vbbm->vbCurrentSize; i++)
		if (storage[i].lbid != -1) {
			_insert(storage[i], dest, newHashtable, newStorage, true);
			//confirmChanges();
		}
}

key_t VBBM::chooseShmkey() const
{
	int fixedKeys = 1;
	key_t ret;

	if (vbbmShminfo->tableShmkey + 1 == (key_t) (fShmKeys.KEYRANGE_VBBM_BASE +
	   fShmKeys.KEYRANGE_SIZE - 1) || (unsigned)vbbmShminfo->tableShmkey <
	   fShmKeys.KEYRANGE_VBBM_BASE)
		ret = fShmKeys.KEYRANGE_VBBM_BASE + fixedKeys;
	else
		ret = vbbmShminfo->tableShmkey + 1;

	return ret;
}

//write lock
void VBBM::insert(LBID_t lbid, VER_t verID, OID_t vbOID, uint32_t vbFBO, bool loading)
{
	VBBMEntry entry;

#ifdef BRM_DEBUG
	int i;

	if (lbid < 0) {
		log("VBBM::insert(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VBBM::insert(): lbid must be >= 0");
	}
	if (verID < 0) {
		log("VBBM::insert(): verID must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VBBM::insert(): verID must be >= 0");
	}
	for (i = 0; i < vbbm->nFiles; i++)
		if (vbOID == files[i].OID)
			break;
	if (i == vbbm->nFiles) {
		log("VBBM::insert(): vbOID must be the OID of a Version Buffer file",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VBBM::insert(): vbOID must be the OID of a Version Buffer file");
	}
	if (vbFBO > (files[i].fileSize/BLOCK_SIZE) || vbFBO < 0) {
		log("VBBM::insert(): vbFBO is out of bounds for that vbOID",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VBBM::insert(): vbFBO is out of bounds for that vbOID");
	}
#endif

	entry.lbid = lbid;
	entry.verID = verID;
	entry.vbOID = vbOID;
	entry.vbFBO = vbFBO;

	// check for resize
	if (vbbm->vbCurrentSize == vbbm->vbCapacity)
		growVBBM();

	_insert(entry, vbbm, hashBuckets, storage, loading);
	if (!loading)
		makeUndoRecord(&vbbm->vbCurrentSize, sizeof(vbbm->vbCurrentSize));
	vbbm->vbCurrentSize++;
}

//assumes write lock is held and that it is properly sized already
void VBBM::_insert(VBBMEntry& e, VBShmsegHeader *dest, int *destHash,
				  VBBMEntry *destStorage, bool loading)
{
	int hashIndex, cHashlen = sizeof(LBID_t) + sizeof(VER_t), insertIndex;
	char* cHash = (char*)alloca(cHashlen);
	utils::Hasher hasher;

	memcpy(cHash, &e.lbid, sizeof(LBID_t));
	memcpy(&cHash[sizeof(LBID_t)], &e.verID, sizeof(VER_t));
	hashIndex = hasher(cHash, cHashlen) % dest->numHashBuckets;

	insertIndex = dest->vbLWM;
	while (destStorage[insertIndex].lbid != -1) {
		insertIndex++;
#ifdef BRM_DEBUG
		if (insertIndex == dest->vbCapacity) {
			log("VBBM:_insert(): There are no empty entries. Possibly bad resize condition.",
				logging::LOG_TYPE_DEBUG);
			throw logic_error("VBBM:_insert(): There are no empty entries. Possibly bad resize condition.");
		}
#endif
	}

	if (!loading) {
		makeUndoRecord(dest, sizeof(VBShmsegHeader));
		makeUndoRecord(&destStorage[insertIndex], sizeof(VBBMEntry));
		makeUndoRecord(&destHash[hashIndex], sizeof(int));
	}

	dest->vbLWM = insertIndex;

	e.next = destHash[hashIndex];
	destStorage[insertIndex] = e;
	destHash[hashIndex] = insertIndex;
}

//assumes read lock is held
int VBBM::lookup(LBID_t lbid, VER_t verID, OID_t &oid, uint32_t &fbo) const
{
	int index, prev, bucket;

//#ifdef BRM_DEBUG
	if (lbid < 0) {
		log("VBBM::lookup(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VBBM::lookup(): lbid must be >= 0");
	}
	if (verID < 0) {
		log("VBBM::lookup(): verID must be > 1)", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VBBM::lookup(): verID must be > 1)");
	}
//#endif

	index = getIndex(lbid, verID, prev, bucket);
	if (index == -1)
		return -1;

	oid = storage[index].vbOID;
	fbo = storage[index].vbFBO;
	return 0;
}

//assumes write lock
void VBBM::getBlocks(int num, OID_t vbOID, vector<VBRange>& freeRanges, VSS& vss, bool flushPMCache)
{
	int blocksLeftInFile, blocksGathered = 0, i;
	uint32_t fileIndex;
	uint32_t firstFBO, lastFBO;
	VBRange range;
	vector<VBRange>::iterator it;
	vector<LBID_t> flushList;

	freeRanges.clear();

	fileIndex = addVBFileIfNotExists(vbOID);

	/*
	for (i = 0; i < vbbm->nFiles; i++) {
		cout << "file " << i << " vbOID=" << files[i].OID << " size=" << files[i].fileSize
				<< endl;
	}
	*/

	if ((uint32_t) num > files[fileIndex].fileSize/BLOCK_SIZE) {
		cout << "num = " << num << " filesize = " << files[fileIndex].fileSize << endl;
		log("VBBM::getBlocks(): num is larger than the size of the version buffer",
				logging::LOG_TYPE_DEBUG);
		throw logging::VBBMBufferOverFlowExcept
			("VBBM::getBlocks(): num is larger than the size of the version buffer");
	}

	while ((vbbm->vbCurrentSize + num) > vbbm->vbCapacity) {
		growVBBM();
		//cout << " requested num = " << num << " and Growing vbbm ... " << endl;
	}

	while (blocksGathered < num) {
		blocksLeftInFile = (files[fileIndex].fileSize - files[fileIndex].nextOffset)/BLOCK_SIZE;
		int blocksLeft = num - blocksGathered;

		range.vbOID = files[fileIndex].OID;
		range.vbFBO = files[fileIndex].nextOffset/BLOCK_SIZE;
		range.size = (blocksLeftInFile >= blocksLeft ? blocksLeft : blocksLeftInFile);
		makeUndoRecord(&files[fileIndex], sizeof(VBFileMetadata));
		if (range.size == (uint32_t) blocksLeftInFile)
			files[fileIndex].nextOffset = 0;
		else
			files[fileIndex].nextOffset += range.size * BLOCK_SIZE;
		blocksGathered += range.size;
		freeRanges.push_back(range);
	}

	//age the returned blocks out of the VB
	for (it = freeRanges.begin(); it != freeRanges.end(); it++) {
		uint32_t firstChunk, lastChunk;

		vbOID = it->vbOID;
		firstFBO = it->vbFBO;
		lastFBO = it->vbFBO + it->size - 1;

		/* Age out at least 100 blocks at a time to reduce the # of times we have to do it.
		 * How to detect when it needs to be done and when it doesn't?
		 *
		 * Split VB space into 100-block chunks.  When a chunk boundary is crossed,
		 * clear the whole chunk.
		 */

		firstChunk = firstFBO/VBBM_CHUNK_SIZE;
		lastChunk = lastFBO/VBBM_CHUNK_SIZE;

		// if the current range falls in the middle of a chunk and doesn't span chunks,
		// there's nothing to do b/c the chunk is assumed to have been cleared already
		if (((firstFBO % VBBM_CHUNK_SIZE) != 0) && (firstChunk == lastChunk))
			continue;

		// round up to the next chunk boundaries
		if ((firstFBO % VBBM_CHUNK_SIZE) != 0)   // this implies the range spans chunks
			firstFBO = (firstChunk + 1) * VBBM_CHUNK_SIZE;  // the first FBO of the next chunk
		lastFBO = ((lastChunk + 1) * VBBM_CHUNK_SIZE - 1);  // the last FBO of the last chunk

		// don't go past the end of the file
		if (lastFBO > files[fileIndex].fileSize/BLOCK_SIZE)
			lastFBO = files[fileIndex].fileSize/BLOCK_SIZE;

		// at this point [firstFBO, lastFBO] is the range to age out.

		// ugh, walk the whole vbbm looking for matches.
		for (i = 0; i < vbbm->vbCapacity; i++)
			if (storage[i].lbid != -1 && storage[i].vbOID == vbOID &&
			  storage[i].vbFBO >= firstFBO && storage[i].vbFBO <= lastFBO) {
				if (vss.isEntryLocked(storage[i].lbid, storage[i].verID)) {
					ostringstream msg;
					msg << "VBBM::getBlocks(): version buffer overflow. Increase VersionBufferFileSize. Overflow occured in aged blocks. Requested NumBlocks:VbOid:vbFBO:lastFBO = "
					<< num << ":" << vbOID <<":" << firstFBO << ":" << lastFBO <<" lbid locked is " << storage[i].lbid << endl;
					log(msg.str(), logging::LOG_TYPE_CRITICAL);
					freeRanges.clear();
					throw logging::VBBMBufferOverFlowExcept(msg.str());
				}
				vss.removeEntry(storage[i].lbid, storage[i].verID, &flushList);
				removeEntry(storage[i].lbid, storage[i].verID);
			}
	}
	if (flushPMCache && !flushList.empty())
		cacheutils::flushPrimProcAllverBlocks(flushList);
}

//read lock
int VBBM::getIndex(LBID_t lbid, VER_t verID, int &prev, int &bucket) const
{
	int cHashlen = sizeof(LBID_t) + sizeof(VER_t), currentIndex;
	char* cHash = (char*)alloca(cHashlen);
	VBBMEntry *listEntry;
	utils::Hasher hasher;

	memcpy(cHash, &lbid, sizeof(LBID_t));
	memcpy(&cHash[sizeof(LBID_t)], &verID, sizeof(VER_t));
	bucket = hasher(cHash, cHashlen) % vbbm->numHashBuckets;
	prev = -1;

	if (hashBuckets[bucket] == -1)
		return -1;

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

void VBBM::removeEntry(LBID_t lbid, VER_t verID)
{
	int index, prev, bucket;

#ifdef BRM_DEBUG
	if (lbid < 0) {
		log("VBBM::removeEntry(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VBBM::removeEntry(): lbid must be >= 0");
	}
	if (verID < 0) {
		log("VBBM::removeEntry(): verID must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VBBM::removeEntry(): verID must be >= 0");
	}
#endif

	index = getIndex(lbid, verID, prev, bucket);
	if (index == -1) {
#ifdef BRM_DEBUG
		ostringstream ostr;

		ostr << "VBBM::removeEntry(): that entry doesn't exist lbid = " << lbid <<
			" verID = " << verID << endl;
		log(ostr.str(), logging::LOG_TYPE_DEBUG);
		throw logic_error(ostr.str());
#else
		return;
#endif
	}
	makeUndoRecord(&storage[index], sizeof(VBBMEntry));
	storage[index].lbid = -1;
	if (prev != -1) {
		makeUndoRecord(&storage[prev], sizeof(VBBMEntry));
		storage[prev].next = storage[index].next;
	}
	else {
		makeUndoRecord(&hashBuckets[bucket], sizeof(int));
		hashBuckets[bucket] = storage[index].next;
	}

	makeUndoRecord(vbbm, sizeof(VBShmsegHeader));
	vbbm->vbCurrentSize--;
	if (vbbm->vbLWM > index)
		vbbm->vbLWM = index;
}

//read lock
int VBBM::size() const
{
#ifdef BRM_DEBUG
	int i, ret = 0;

	for (i = 0; i < vbbm->vbCapacity; i++)
		if (storage[i].lbid != -1)
			ret++;

	if (ret != vbbm->vbCurrentSize) {
		ostringstream ostr;

		ostr << "VBBM::checkConsistency(): actual size is " << ret <<
			", recorded size is " << vbbm->vbCurrentSize;
		log(ostr.str(), logging::LOG_TYPE_DEBUG);
		throw logic_error(ostr.str());
	}

	return ret;
#else
	return vbbm->vbCurrentSize;
#endif
}

//read lock
bool VBBM::hashEmpty() const
{
	int i;

	for (i = 0; i < vbbm->numHashBuckets; i++)
		if (hashBuckets[i] != -1)
			return false;
	return true;
}

//write lock
void VBBM::clear()
{
	int allocSize;
	int newshmkey;
	int nFiles = -1;
	char *newshmseg;

	// save vars we need after the clear()
	boost::scoped_array<VBFileMetadata> newFiles(new VBFileMetadata[vbbm->nFiles]);
	memcpy(&newFiles[0], files, vbbm->nFiles * sizeof(VBFileMetadata));

	setCurrentFileSize();
	for (int i = 0; i < vbbm->nFiles; i++) {
		newFiles[i].fileSize = currentFileSize;
		newFiles[i].nextOffset = 0;
	}
	nFiles = vbbm->nFiles;

	allocSize = (sizeof(VBShmsegHeader) +
		(nFiles*sizeof(VBFileMetadata)) +
		VBSTORAGE_INITIAL_SIZE + VBTABLE_INITIAL_SIZE);
	//cout << "clear:: allocSize = " << allocSize << endl;
	newshmkey = chooseShmkey();
	fPVBBMImpl->clear(newshmkey, allocSize);
	vbbm = fPVBBMImpl->get();
	newshmseg = reinterpret_cast<char*>(vbbm);
	initShmseg(nFiles);
	vbbmShminfo->tableShmkey = currentVBBMShmkey = newshmkey;
	vbbmShminfo->allocdSize = allocSize;

	files = reinterpret_cast<VBFileMetadata*>
			(&newshmseg[sizeof(VBShmsegHeader)]);
	hashBuckets = reinterpret_cast<int*>
			(&newshmseg[sizeof(VBShmsegHeader) +
			vbbm->nFiles*sizeof(VBFileMetadata)]);
	storage = reinterpret_cast<VBBMEntry*>(&newshmseg[sizeof(VBShmsegHeader) +
			vbbm->nFiles*sizeof(VBFileMetadata) +
			vbbm->numHashBuckets*sizeof(int)]);
	memcpy(files, &newFiles[0], vbbm->nFiles * sizeof(VBFileMetadata));
}

//read lock
int VBBM::checkConsistency() const {

	/*

	Struct integrity tests
	1: Verify that the recorded size matches the actual size
	2: Verify there are no empty entries reachable from the hash table
	3: Verify there are no empty entries below the LWM

	4a: Make sure every VBBM entry points to a unique position in the VB
	4b: Make sure every VBBM entry has unique LBID & VERID.
	*/

	int i, j, k;

	/* Test 1 is already implemented */
	size();

	/* Test 2 - no empty elements reachable from the hash table */

	int nextElement;

	for (i = 0; i < vbbm->numHashBuckets; i++) {
		if (hashBuckets[i] != -1)
			for (nextElement = hashBuckets[i]; nextElement != -1;
						  nextElement = storage[nextElement].next)
				if (storage[nextElement].lbid == -1)
					throw logic_error("VBBM::checkConsistency(): an empty storage entry is reachable from the hash table");
	}

	/* Test 3 - verify that there are no empty entries below the LWM */

	for (i = 0; i < vbbm->vbLWM; i++) {
		if (storage[i].lbid == -1) {
			cerr << "VBBM: LWM=" << vbbm->vbLWM << " first empty entry=" << i << endl;
			throw logic_error("VBBM::checkConsistency(): LWM accounting error");
		}
	}

	/* Test 4b - verify the uniqueness of the entries */

	for (i = 0;	i < vbbm->numHashBuckets; i++)
		if (hashBuckets[i] != -1)
			for (j = hashBuckets[i]; j != -1; j = storage[j].next)
				for (k = storage[j].next; k != -1; k = storage[k].next)
					if (storage[j].lbid == storage[k].lbid &&
						storage[j].verID == storage[k].verID) {
						cerr << "VBBM: lbid=" << storage[j].lbid << " verID=" <<
						storage[j].verID << endl;
						throw logic_error("VBBM::checkConsistency(): Duplicate entry found");
					}

	/* Test 4a - verify the uniqueness of vbOID, vbFBO fields */
	for (i = 0; i < vbbm->vbCapacity; i++)
		if (storage[i].lbid != -1)
			for (j = i+1; j < vbbm->vbCapacity; j++)
				if (storage[j].lbid != -1)
					if (storage[j].vbOID == storage[i].vbOID &&
						storage[j].vbFBO == storage[i].vbFBO) {
						cerr << "VBBM: lbid1=" << storage[i].lbid << " lbid2=" <<
							storage[j].lbid << " verID1=" << storage[i].verID <<
							" verID2=" << storage[j].verID << " share vbOID=" <<
							storage[j].vbOID << " vbFBO=" << storage[j].vbFBO <<
							endl;
						throw logic_error("VBBM::checkConsistency(): 2 VBBM entries share space in the VB");
					}
	return 0;
}

void VBBM::setReadOnly()
{
	r_only = true;
}

/* File Format (V1)

		VBBM V1 magic (32-bits)
		# of VBBM entries in capacity (32-bits)
		struct VBBMEntry * #

		These entries are considered optional to support going backward
		and forward between versions.
		nFiles (32-bits)
		currentFileIndex (32-bits)
		VBFileMetadata * nFiles
*/

/* File Format (V2):
 *
 * 		Version 2 magic (int)
 * 		number of used VBBM entries (numEntries) (int)
 * 		current number of VB files (nFiles) (int)
 * 		VBFileMetadata * nFiles
 * 		struct VBBMEntry * numEntries
 */

void VBBM::loadVersion1(IDBDataFile* in)
{
	int vbbmEntries, i;
	VBBMEntry entry;

	clear();
	if (in->read((char *) &vbbmEntries, 4) != 4) {
		log_errno("VBBM::load()");
		throw runtime_error("VBBM::load(): Failed to read entry number");
	}

	for (i = 0; i < vbbmEntries; i++) {
		if (in->read((char *)&entry, sizeof(entry)) != sizeof(entry)) {
			log_errno("VBBM::load()");
			throw runtime_error("VBBM::load(): Failed to load entry");
		}
		insert(entry.lbid, entry.verID, entry.vbOID, entry.vbFBO, true);
		//confirmChanges();
		addVBFileIfNotExists(entry.vbOID);
	}

	/* This will load the saved file data from 2.2, but it is not compatible with
	 * 3.0+.  If enabled, take out the addVBFile..() call above
	 */
#if 0
	int dummy, nFiles;

	in.read((char *) &nFiles, 4);
	cout << "got nfiles = " << nFiles << endl;
	in.read((char *) &dummy, 4);   // an unused var in 3.0+
	while (vbbm->nFiles < nFiles)
		growVBBM(true);  // this allocates one file, doesn't grow the main storage
	in.read((char *) files, sizeof(VBFileMetadata) * nFiles);
	for (i = 0; i < nFiles; i++)
		cout << "file " << i << ": oid=" << files[i].OID << " size=" << files[i].fileSize
				<< " offset=" << files[i].nextOffset << endl;
#endif

}

void VBBM::loadVersion2(IDBDataFile* in)
{
	int vbbmEntries;
	int nFiles;
	int i;
	VBBMEntry entry;

	if (in->read((char *) &vbbmEntries, 4) != 4) {
		log_errno("VBBM::load()");
		throw runtime_error("VBBM::load(): Failed to read entry number");
	}
	if (in->read((char *) &nFiles, 4) != 4) {
		log_errno("VBBM::load()");
		throw runtime_error("VBBM::load(): Failed to read file number");
	}

	// Need to make clear() truncate the files section
	if (vbbm->nFiles > nFiles)
		vbbm->nFiles = nFiles;

	clear();

	while (vbbm->nFiles < nFiles)
		growVBBM(true);  // this allocates one file, doesn't grow the main storage

	growForLoad(vbbmEntries);

	const int nfileSize = sizeof(VBFileMetadata) * nFiles;
	if (in->read((char *)files, nfileSize) != nfileSize) {
		log_errno("VBBM::load()");
		throw runtime_error("VBBM::load(): Failed to load vb file meta data");
	}

	for (i = 0; i < vbbmEntries; i++) {
		if (in->read((char *)&entry, sizeof(entry)) != sizeof(entry)) {
			log_errno("VBBM::load()");
			throw runtime_error("VBBM::load(): Failed to load entry");
		}
		insert(entry.lbid, entry.verID, entry.vbOID, entry.vbFBO, true);
	}

}

//#include "boost/date_time/posix_time/posix_time.hpp"
//using namespace boost::posix_time;

void VBBM::load(string filename)
{
	int magic;
	const char* filename_p = filename.c_str();
	scoped_ptr<IDBDataFile>  in(IDBDataFile::open(
								IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
								filename_p, "rb", 0));
	//ptime time1, time2;

	//time1 = microsec_clock::local_time();
	//cout << "loading the VBBM " << time1 << endl;

	if (!in) {
		log_errno("VBBM::load()");
		throw runtime_error("VBBM::load(): Failed to open the file");
	}

	int bytes = in->read((char *) &magic, 4);
	if (bytes != 4) {
		log("VBBM::load(): failed to read magic.");
		throw runtime_error("VBBM::load(): failed to read magic.");
	}

	switch (magic) {
		case VBBM_MAGIC_V1:	loadVersion1(in.get()); break;
		case VBBM_MAGIC_V2: loadVersion2(in.get()); break;
		default:
			log("VBBM::load(): Bad magic.  Not a VBBM file?");
			throw runtime_error("VBBM::load(): Bad magic.  Not a VBBM file?");
	}

	//time2 = microsec_clock::local_time();
	//cout << "done loading " << time2 << " duration: " << time2-time1 << endl;
}

// read lock
void VBBM::save(string filename)
{
	int i;
	mode_t utmp = ::umask(0);
	int var;

	if (IDBPolicy::useHdfs()) {
		const char* filename_p = filename.c_str();
		scoped_ptr<IDBDataFile> out(IDBDataFile::open(
									IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
									filename_p, "wb", IDBDataFile::USE_VBUF));
		::umask(utmp);
		if (!out) {
			log_errno("VBBM::save()");
			throw runtime_error("VBBM::save(): Failed to open the file");
		}

		var = VBBM_MAGIC_V2;
		int bytesWritten = 0;
		int bytesToWrite = 12;
		bytesWritten += out->write((char *) &var, 4);
		bytesWritten += out->write((char *) &vbbm->vbCurrentSize, 4);
		bytesWritten += out->write((char *) &vbbm->nFiles, 4);

		bytesWritten += out->write((char *) files, sizeof(VBFileMetadata) * vbbm->nFiles);
		bytesToWrite += sizeof(VBFileMetadata) * vbbm->nFiles;

		for (i = 0; i < vbbm->vbCapacity; i++) {
			if (storage[i].lbid != -1) {
				bytesToWrite += sizeof(VBBMEntry);
				bytesWritten += out->write((char *)&storage[i], sizeof(VBBMEntry));
			}
		}

		if (bytesWritten != bytesToWrite) {
			log_errno("VBBM::save()");
			throw runtime_error("VBBM::save(): Failed to write the file");
		}
	}
	else {
		ofstream out;
		out.open(filename.c_str(), ios_base::trunc|ios_base::out|ios_base::binary);
		::umask(utmp);
		if (!out) {
			log_errno("VBBM::save()");
			throw runtime_error("VBBM::save(): Failed to open the file");
		}
		out.exceptions(ios_base::badbit);

		var = VBBM_MAGIC_V2;
		out.write((char *) &var, 4);
		out.write((char *) &vbbm->vbCurrentSize, 4);
		out.write((char *) &vbbm->nFiles, 4);

		out.write((char *) files, sizeof(VBFileMetadata) * vbbm->nFiles);
		for (i = 0; i < vbbm->vbCapacity; i++)
			if (storage[i].lbid != -1)
				out.write((char *)&storage[i], sizeof(VBBMEntry));
	}

#if 0
	cout << "saving... nfiles=" << vbbm->nFiles << "\n";
	for (i = 0; i < vbbm->nFiles; i++) {
		cout << "file " << i << " vboid=" << files[i].OID << " size=" << files[i].fileSize << endl;
	}
#endif
}

uint32_t VBBM::addVBFileIfNotExists(OID_t vbOID)
{
	int i;

	/* Check if vbOID exists,
	 * 	add it if not, init to pos=0
	 */

	for (i = 0; i < vbbm->nFiles; i++)
		if (files[i].OID == vbOID)
			break;

	if (i == vbbm->nFiles) {
		setCurrentFileSize();
		growVBBM(true);
		files[i].OID = vbOID;
		files[i].fileSize = currentFileSize;
		files[i].nextOffset = 0;
	}
	return i;
}

void VBBM::setCurrentFileSize()
{
	config::Config *conf = config::Config::makeConfig();
	string stmp;
	int64_t ltmp;

	currentFileSize = 2147483648ULL;   // 2 GB default

	try {
		stmp = conf->getConfig("VersionBuffer", "VersionBufferFileSize");
	}
	catch(std::exception& e) {
		log("VBBM: Missing a VersionBuffer/VersionBufferFileSize key in the config file");
		throw invalid_argument("VBBM: Missing a VersionBuffer/VersionBufferFileSize key in the config file");
	}

	ltmp = conf->fromText(stmp.c_str());

	if (ltmp < 1) {
		log("VBBM: Config error: VersionBuffer/VersionBufferFileSize must be positive");
		throw invalid_argument("VBBM: Config error: VersionBuffer/VersionBufferFileSize must be positive");
	}
	else {
		currentFileSize = ltmp;
	}
}

#ifdef BRM_DEBUG
// read lock
int VBBM::getShmid() const
{
	return vbbmShmid;
}
#endif

}   //namespace
