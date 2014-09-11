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
 * $Id: vbbm.cpp 1748 2012-11-07 21:43:54Z pleblanc $
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

#include "blocksize.h"
#include "rwlock.h"
#include "brmtypes.h"
#include "mastersegmenttable.h"
#include "vss.h"
#include "configcpp.h"
#include "exceptclasses.h"
#include "hasher.h"

#define VBBM_DLLEXPORT
#include "vbbm.h"
#undef VBBM_DLLEXPORT

#define VBBM_MAGIC_V1 0x7b27ec13

using namespace std;

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
		assert(key == fInstance->fVBBM.key());
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
}

VBBM::~VBBM() 
{
}

void VBBM::initShmseg(int nFiles) 
{
	VBFileMetadata *newfiles;
	int *newBuckets;
	VBBMEntry *newStorage;
	int i; 
	int64_t vbFilesize;
	char *shmseg;
	config::Config *conf;
	string stmp, stmp2;
	const char *ctmp;
	int64_t ltmp;
	OID_t *vbOIDs;
	
	conf = config::Config::makeConfig();
	vbbm->nFiles = nFiles;
	
	try {
		stmp = conf->getConfig("VersionBuffer", "VersionBufferFileSize");
	}
	catch(exception& e) {
		log("VBBM: Missing a VersionBuffer/VersionBufferFileSize key in the config file");
		throw invalid_argument("VBBM: Missing a VersionBuffer/VersionBufferFileSize key in the config file");
	}
	
	ctmp = stmp.c_str();
	ltmp = config::Config::fromText(ctmp);

	// 32TB is the max file size ATM (2^32 blocks * 2^13 bytes/block)
	if (ltmp > 35184372088832LL || ltmp < 1)
	{
		log("VBBM: Config error: VersionBuffer/VersionBufferFileSize must be between 0 and 32TB");
		throw invalid_argument("VBBM: Config error: VersionBuffer/VersionBufferFileSize must be between 0 and 32TB");
	}
	else
		vbFilesize = ltmp;
	
	// @bug 372
	// Make sure this is done in 64 bits.
	vbbm->vbTotalSize = (uint64_t)vbFilesize * (uint64_t)nFiles;
	if (vbbm->vbTotalSize/BLOCK_SIZE < VBSTORAGE_INITIAL_SIZE/sizeof(VBBMEntry) ||
	  (vbFilesize % BLOCK_SIZE) != 0) {
		ostringstream ostr;
		ostr << "VBBM: Config file error.  Total version buffer size must be at least "
		  << VBSTORAGE_INITIAL_SIZE/sizeof(VBBMEntry) * BLOCK_SIZE << 
		  " bytes, and " << "VersionBuffer/VersionBufferFileSize must be a multiple of "
		  << BLOCK_SIZE;
		log(ostr.str());
		throw invalid_argument(ostr.str());
	}

	vbOIDs = new OID_t[vbbm->nFiles];
	for (i = 0; i < vbbm->nFiles; i++)
		vbOIDs[i] = i;
	
	vbbm->currentVBFileIndex = 0;
	vbbm->vbCapacity = VBSTORAGE_INITIAL_SIZE/sizeof(VBBMEntry);
	vbbm->vbCurrentSize = 0;
	vbbm->vbLWM = 0;
	vbbm->numHashBuckets = VBTABLE_INITIAL_SIZE/sizeof(int);
	shmseg = reinterpret_cast<char*>(vbbm);
	newfiles = reinterpret_cast<VBFileMetadata*>
			(&shmseg[sizeof(VBShmsegHeader)]);
	newBuckets = reinterpret_cast<int*>
			(&shmseg[sizeof(VBShmsegHeader) + 
			vbbm->nFiles*sizeof(VBFileMetadata)]);
	newStorage = reinterpret_cast<VBBMEntry*>(&shmseg[sizeof(VBShmsegHeader) + 
			vbbm->nFiles*sizeof(VBFileMetadata) +
			vbbm->numHashBuckets*sizeof(int)]);	
	for (i = 0; i < vbbm->nFiles; i++) {
		newfiles[i].OID = vbOIDs[i];
		newfiles[i].filesize = vbFilesize;
		newfiles[i].nextOffset = 0;
	}
	for (i = 0; i < vbbm->numHashBuckets; i++)
		newBuckets[i] = -1;
	for (i = 0; i < vbbm->vbCapacity; i++)
		newStorage[i].lbid = -1;
	delete [] vbOIDs;
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
			assert(fPVBBMImpl);
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
void VBBM::growVBBM()
{
	int allocSize;
	int nFiles = -1;
	key_t newshmkey;
	char *newshmseg;
	config::Config *conf;
	const char *ctmp;
	int64_t ltmp;

	if (vbbmShminfo->allocdSize == 0) {
		string stmp;

		conf = config::Config::makeConfig();
		try {
			stmp = conf->getConfig("VersionBuffer", "NumVersionBufferFiles");
		}
		catch(exception& e) {
			cout << e.what() << endl;
			throw;
		}
		ctmp = stmp.c_str();
		ltmp = config::Config::fromText(ctmp);
		if (ltmp > 1000 || ltmp < 1) {
			log("VBBM::growVBBM(): Config file error: VersionBuffer/NumVersionBufferFiles must exist and be <= 1000.");
			throw invalid_argument("VBBM::growVBBM(): Config file error: VersionBuffer/NumVersionBufferFiles must exist and be <= 1000.");
		}
		else
			nFiles = static_cast<int>(ltmp);
			allocSize = (sizeof(VBShmsegHeader) + 
				(nFiles*sizeof(VBFileMetadata)) + 
				VBSTORAGE_INITIAL_SIZE + VBTABLE_INITIAL_SIZE);
	}
	else
		allocSize = vbbmShminfo->allocdSize + VBBM_INCREMENT;
	
	newshmkey = chooseShmkey();
	if (fPVBBMImpl)
	{
		BRMShmImpl newShm(newshmkey, allocSize);
		newshmseg = static_cast<char*>(newShm.fMapreg.get_address());
		memset(newshmseg, 0, allocSize);
		if (vbbm != NULL)
		{
			VBShmsegHeader *tmp = reinterpret_cast<VBShmsegHeader*>(newshmseg);
			tmp->vbCapacity = vbbm->vbCapacity + VBSTORAGE_INCREMENT/sizeof(VBBMEntry);
			tmp->numHashBuckets = vbbm->numHashBuckets + VBTABLE_INCREMENT/sizeof(int);
			tmp->vbLWM = 0;
			copyVBBM(tmp);
		}
		fPVBBMImpl->swapout(newShm);
	}
	else
	{
		fPVBBMImpl = VBBMImpl::makeVBBMImpl(newshmkey, allocSize);
		newshmseg = reinterpret_cast<char*>(fPVBBMImpl->get());
		memset(newshmseg, 0, allocSize);
	}

	vbbm = fPVBBMImpl->get();
	if (nFiles != -1)
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
	dest->currentVBFileIndex = vbbm->currentVBFileIndex;
	dest->vbCurrentSize = vbbm->vbCurrentSize;
	dest->vbTotalSize = vbbm->vbTotalSize;
	
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
			_insert(storage[i], dest, newHashtable, newStorage);
			confirmChanges();
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
void VBBM::insert(LBID_t lbid, VER_t verID, OID_t vbOID, u_int32_t vbFBO)
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
	if (vbFBO > (files[i].filesize/BLOCK_SIZE) || vbFBO < 0) {
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
	
	_insert(entry, vbbm, hashBuckets, storage);
	makeUndoRecord(&vbbm->vbCurrentSize, sizeof(vbbm->vbCurrentSize));
	vbbm->vbCurrentSize++;
}

//assumes write lock is held and that it is properly sized already
void VBBM::_insert(VBBMEntry& e, VBShmsegHeader *dest, int *destHash, 
				  VBBMEntry *destStorage)
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
	
	makeUndoRecord(dest, sizeof(VBShmsegHeader));
	makeUndoRecord(&destStorage[insertIndex], sizeof(VBBMEntry));
	makeUndoRecord(&destHash[hashIndex], sizeof(int));
	
	dest->vbLWM = insertIndex;

	e.next = destHash[hashIndex];
	destStorage[insertIndex] = e;
	destHash[hashIndex] = insertIndex;
}
	
//assumes read lock is held
int VBBM::lookup(LBID_t lbid, VER_t verID, OID_t &oid, u_int32_t &fbo) const
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
void VBBM::getBlocks(int num, vector<VBRange>& freeRanges, VSS& vss)
{
	int blocksLeftInFile, blocksGathered = 0, i;
	OID_t vbOID;
	u_int32_t vbFBO, lastFBO;
	VBRange range;
	vector<VBRange>::iterator it;
	
	freeRanges.clear();
	//assert( num < 500 );
//#ifdef BRM_DEBUG
	if (num < 1) {
		log("VBBM::getBlocks(): num must be > 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VBBM::getBlocks(): num must be > 0");
	}
	if ((uint) num > vbbm->vbTotalSize) {
		log("VBBM::getBlocks(): num is larger than the size of the version buffer",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument("VBBM::getBlocks(): num is larger than the size of the version buffer");
	}
//#endif

	while ((vbbm->vbCurrentSize + num) > vbbm->vbCapacity &&
			((VBSTORAGE_INCREMENT/sizeof(VBBMEntry) + vbbm->vbCapacity) <=
			vbbm->vbTotalSize/BLOCK_SIZE)) {
		growVBBM();
		//cout << " requested num = " << num << " and Growing vbbm ... " << endl;
	}
	
	makeUndoRecord(vbbm, sizeof(VBShmsegHeader));
	while (blocksGathered < num) {	
		blocksLeftInFile = (files[vbbm->currentVBFileIndex].filesize - 
				files[vbbm->currentVBFileIndex].nextOffset) / BLOCK_SIZE;
		range.vbOID = files[vbbm->currentVBFileIndex].OID;
		range.vbFBO = files[vbbm->currentVBFileIndex].nextOffset/BLOCK_SIZE;
		//cout << "range.vbOID:range.vbFBO = " << range.vbOID << ":" << range.vbFBO << endl;
		if (blocksLeftInFile >= num - blocksGathered) {
			range.size = num - blocksGathered;
			makeUndoRecord(&files[vbbm->currentVBFileIndex], sizeof(VBFileMetadata));
			files[vbbm->currentVBFileIndex].nextOffset += 
					(num - blocksGathered) * BLOCK_SIZE;	
			blocksGathered = num;
		}
		else {
			range.size = blocksLeftInFile;
			vbbm->currentVBFileIndex = (vbbm->currentVBFileIndex + 1) %
				vbbm->nFiles;
			makeUndoRecord(&files[vbbm->currentVBFileIndex], sizeof(VBFileMetadata));
			files[vbbm->currentVBFileIndex].nextOffset = 0;
			blocksGathered += blocksLeftInFile;
		}
		freeRanges.push_back(range);
	}

	//age the returned blocks out of the VB
	for (it = freeRanges.begin(); it != freeRanges.end(); it++) {
		vbOID = (*it).vbOID;
		vbFBO = (*it).vbFBO;
		lastFBO = vbFBO + (*it).size - 1;
		
		// ugh, walk the whole vbbm looking for matches.
		for (i = 0; i < vbbm->vbCapacity; i++)
			if (storage[i].lbid != -1 && storage[i].vbOID == vbOID &&
			  storage[i].vbFBO >= vbFBO && storage[i].vbFBO <= lastFBO) {
				if (vss.isEntryLocked(storage[i].lbid, storage[i].verID)) {
					ostringstream msg;
					msg << "VBBM::getBlocks(): version buffer overflow. Increase VersionBufferFileSize. Overflow occured in aged blocks. Requested NumBlocks:VbOid:vbFBO:lastFBO = "
					<< num << ":" << vbOID <<":" << vbFBO << ":" << lastFBO <<" lbid locked is " << storage[i].lbid << endl;
					log(msg.str(), logging::LOG_TYPE_CRITICAL);
					freeRanges.clear();
					throw logging::VBBMBufferOverFlowExcept(msg.str());
				}
				vss.removeEntry(storage[i].lbid, storage[i].verID);
				removeEntry(storage[i].lbid, storage[i].verID);
			}
	}
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
	int nFiles = -1;
	int newshmkey;
	char *newshmseg;
	config::Config *conf;
	const char *ctmp;
	int64_t ltmp;
	string stmp;

	conf = config::Config::makeConfig();
	try {
		stmp = conf->getConfig("VersionBuffer", "NumVersionBufferFiles");
	}
	catch(exception& e) {
		cout << e.what() << endl;
		throw;
	}
	ctmp = stmp.c_str();
	ltmp = config::Config::fromText(ctmp);
	if (ltmp > 1000 || ltmp < 1)
		throw invalid_argument("VBBM::growVBBM(): Config file error: VersionBuffer/NumVersionBufferFiles must exist and be <= 1000.");
	else
		nFiles = static_cast<int>(ltmp);

	allocSize = (sizeof(VBShmsegHeader) + 
		(nFiles*sizeof(VBFileMetadata)) + 
		VBSTORAGE_INITIAL_SIZE + VBTABLE_INITIAL_SIZE);
	
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

void VBBM::writeData(int fd, u_int8_t *buf, off_t offset, int size) const
{
	int errCount, err, progress;
	off_t seekerr = -1;
	
	for (errCount = 0; errCount < MAX_IO_RETRIES && seekerr != offset; errCount++) {
		seekerr = lseek(fd, offset, SEEK_SET);
		if (seekerr < 0)
			log_errno("VBBM::writeData(): lseek");
	}
	if (errCount == MAX_IO_RETRIES) {
		log("VBBM::writeData(): lseek failed too many times");
		throw std::ios_base::failure("VBBM::writeData(): lseek failed "
				"too many times");
	}
	
	for (progress = 0, errCount = 0; progress < size && errCount < MAX_IO_RETRIES;) {
		err = write(fd, &buf[progress], size - progress);
		if (err < 0) {
			if (errno != EINTR) {  // EINTR isn't really an error
				errCount++;
				log_errno("VBBM::writeData(): write (retrying)");
			}
		}
		else 
			progress += err;		
	}
	if (errCount == MAX_IO_RETRIES) {
		log("VBBM::writeData(): write error");
		throw std::ios_base::failure("VBBM::writeData(): write error");	
	}
}

void VBBM::readData(int fd, u_int8_t *buf, off_t offset, int size)
{
	int errCount, err, progress;
	off_t seekerr = -1;
	
	for (errCount = 0; errCount < MAX_IO_RETRIES && seekerr != offset; errCount++) {
		seekerr = lseek(fd, offset, SEEK_SET);
		if (seekerr < 0)
			log_errno("VBBM::readData(): lseek");
	}
	if (errCount == MAX_IO_RETRIES) {
		log("VBBM::readData(): lseek failed too many times");
		throw std::ios_base::failure("VBBM::readData(): lseek failed "
				"too many times");
	}
	
	for (progress = 0, errCount = 0; progress < size && errCount < MAX_IO_RETRIES;) {
		err = read(fd, &buf[progress], size - progress);
		if (err < 0) {
			if (errno != EINTR) {  // EINTR isn't really an error
				errCount++;
				log_errno("VBBM::readData(): read (retrying)");
			}
		}
		else 
			progress += err;		
	}
	if (errCount == MAX_IO_RETRIES) {
		log("VBBM::readData(): read error");
		throw std::ios_base::failure("VBBM::readData(): read error");
	}
}	


/* File Format:

		VBBM V1 magic (32-bits)
		# of VBBM entries in capacity (32-bits)
		struct VBBMEntry * #

		These entries are considered optional to support going backward
		and forward between versions.
		nFiles (32-bits)
		currentFileIndex (32-bits)
		VBFileMetadata * nFiles
*/

struct Header {
	int magic;
	int entries;
};

void VBBM::load(string filename)
{
	int i;
	struct Header header;
	struct VBBMEntry entry;
	ifstream in;

 	in.open(filename.c_str(), ios_base::in | ios_base::binary);
	if (!in) {
		log_errno("VBBM::load()");
		throw runtime_error("VBBM::load(): Failed to open the file");
	}

 	in.exceptions(ios_base::badbit | ios_base::failbit);
	try {
		in.read((char *)&header, sizeof(header));
	}
	catch (ios_base::failure &e) {
		in.close();
		throw;
	}

	if (header.entries < 0) {
		log("VBBM::load(): Bad size.  Not a VBBM file?");
		throw runtime_error("VBBM::load(): Bad size.  Not a VBBM file?");
	}

	if (header.magic == VBBM_MAGIC_V1) {
		for (i = 0; i < vbbm->vbCapacity; i++)
			storage[i].lbid = -1;
		for (i = 0; i < vbbm->numHashBuckets; i++)
			hashBuckets[i] = -1;
		vbbm->vbCurrentSize = 0;
		vbbm->vbLWM = 0;

		try {
			for (i = 0; i < header.entries; i++) {
				in.read((char *)&entry, sizeof(entry));
				insert(entry.lbid, entry.verID, entry.vbOID, entry.vbFBO);
				confirmChanges();
			}
		}
		catch (exception &e) {
			in.close();
			throw;
		}
	}
	else  {
		log("VBBM::load(): Bad magic.  Not a VBBM file?");
		throw runtime_error("VBBM::load(): Bad magic.  Not a VBBM file?");
	}

	/* Try to load the data added for the v2.1 release.
	 * It's not an error if it's not there (backward compat issue). */
	try {
		int l_nFiles;
		int l_currentFile;
		boost::scoped_array<VBFileMetadata> l_meta;

		in.read((char *) &l_nFiles, sizeof(int));
		in.read((char *) &l_currentFile, sizeof(int));
		l_meta.reset(new VBFileMetadata[l_nFiles]);
		in.read((char *) l_meta.get(), sizeof(VBFileMetadata) * l_nFiles);

		// if no change was made to the VB file config, use the saved position data,
		// otherwise don't.  The system "should be" offline and shut down
		// cleanly if a change was made (safe to start at position 0,0).
		if (l_nFiles == vbbm->nFiles && l_meta[0].filesize == files[0].filesize) {
			vbbm->currentVBFileIndex = l_currentFile;
			memcpy(files, l_meta.get(), l_nFiles * sizeof(VBFileMetadata));
		}
				
	}
	catch (exception &e) { }
	in.close();
}

// read lock
void VBBM::save(string filename)
{
	int i;
	struct Header header;
	ofstream out;
	mode_t utmp;
	
	utmp = ::umask(0);
	out.open(filename.c_str(), ios_base::trunc|ios_base::out|ios_base::binary);
	::umask(utmp);
	if (!out) {
		log_errno("VBBM::save()");
		throw runtime_error("VBBM::save(): Failed to open the file");
	}
	out.exceptions(ios_base::badbit);
	
	header.magic = VBBM_MAGIC_V1;
	header.entries = vbbm->vbCurrentSize;

	try {
		out.write((char *)&header, sizeof(header));

		for (i = 0; i < vbbm->vbCapacity; i++)
			if (storage[i].lbid != -1)
				out.write((char *)&storage[i], sizeof(VBBMEntry));
		out.write((char *) &vbbm->nFiles, sizeof(int));
		out.write((char *) &vbbm->currentVBFileIndex, sizeof(int));
		out.write((char *) files, sizeof(VBFileMetadata) * vbbm->nFiles);
	}
	catch (exception &e) {
		out.close();
		throw;
	}
	out.close();
}

	

/*
void VBBM::load(string filename)
{
	int i, fd, offset;
	struct Header header;
	struct VBBMEntry entry;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0)
		throw runtime_error("VBBM::load(): Failed to open the file");

	try {
		readData(fd, (u_int8_t *) &header, 0, sizeof(header));
	}
	catch (exception &e) {
		close(fd);
		throw;
	}
	if (header.magic != VBBM_MAGIC_V1) {
		close(fd);
		throw runtime_error("VBBM::load(): Bad magic.  Not a VBBM file?");
	}
	if (header.entries < 0) {
		close(fd);
		throw runtime_error("VBBM::load(): Bad size.  Not a VBBM file?");
	}

	for (i = 0; i < vbbm->vbCapacity; i++)
		storage[i].lbid = -1;
	for (i = 0; i < vbbm->numHashBuckets; i++)
		hashBuckets[i] = -1;
	vbbm->vbCurrentSize = 0;
	vbbm->vbLWM = 0;

	try {
		for (i = 0, offset = sizeof(header); i < header.entries; 
			i++, offset += sizeof(VBBMEntry)) {
			readData(fd, (u_int8_t *) &entry, offset, sizeof(VBBMEntry));
			insert(entry.lbid, entry.verID, entry.vbOID, entry.vbFBO);
			confirmChanges();
		}
	}
	catch (exception &e) {
		close(fd);
		throw;
	}

	close(fd);
}
		

// read lock
void VBBM::save(string filename)
{
	int i, fd, offset;
	struct Header header;
	
	fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
	if (fd < 0) {
		cerr << "VBBM: filename is " << filename << endl;
		throw runtime_error("VBBM::save(): Failed to open the file");
	}

	header.magic = VBBM_MAGIC_V1;
	header.entries = vbbm->vbCurrentSize;

	try {
		writeData(fd, (u_int8_t *) &header, 0, sizeof(struct Header));
		offset = sizeof(struct Header);

		for (i = 0; i < vbbm->vbCapacity; i++)
			if (storage[i].lbid != -1) {
				writeData(fd, (u_int8_t *) &storage[i], offset, sizeof(VBBMEntry));
				offset += sizeof(VBBMEntry);
			}
	}
	catch (exception &e) {
		close(fd);
		throw;
	}
	close(fd);
}
*/

#ifdef BRM_DEBUG
// read lock
int VBBM::getShmid() const 
{
	return vbbmShmid;
}
#endif

}   //namespace
