/* Copyright (C) 2013 Calpont Corp.

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

/***************************************************************************
 *
 *   $Id: filebuffermgr.cpp 2045 2013-01-30 20:26:59Z pleblanc $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/
/**
* 	InitialDBBCSize - the starting number of elements the unordered set used to store disk blocks.
	This does not instantiate InitialDBBCSize disk blocks but only the initial size of the unordered_set

**/

//#define NDEBUG
#include <cassert>
#include <limits>
#include <boost/thread.hpp>

#ifndef _MSC_VER
#include <pthread.h>
#endif

#include "stats.h"
#include "configcpp.h"
#include "filebuffermgr.h"

using namespace config;
using namespace boost;
using namespace std;
using namespace BRM;

extern dbbc::Stats* gPMStatsPtr;
extern bool gPMProfOn;
extern uint32_t gSession;

namespace dbbc {
const uint32_t gReportingFrequencyMin(32768);

FileBufferMgr::FileBufferMgr(const uint32_t numBlcks, const uint32_t blkSz, const uint32_t deleteBlocks)
	:fMaxNumBlocks(numBlcks),
	fBlockSz(blkSz), 
	fWLock(),
	fbSet(), 
	fbList(), 
	fCacheSize(0), 
	fFBPool(), 
	fDeleteBlocks(deleteBlocks), 
	fEmptyPoolSlots(),
	fReportFrequency(0)
{
	fFBPool.reserve(numBlcks);
	fConfig = Config::makeConfig();
	setReportingFrequency(0);
#ifdef _MSC_VER
	fLog.open("C:/Calpont/log/trace/bc", ios_base::app | ios_base::ate);
#else
	fLog.open("/var/log/Calpont/trace/bc", ios_base::app | ios_base::ate);
#endif
}

FileBufferMgr::~FileBufferMgr()
{
	flushCache();
}

// param d is used as a togle only
void FileBufferMgr::setReportingFrequency(const uint32_t d)
{
    if (d==0) {
		fReportFrequency=0;
		return;
	}

    const string val = fConfig->getConfig("DBBC", "ReportFrequency");
	uint32_t temp=0;

    if (val.length()>0) temp=static_cast<int>(Config::fromText(val));
    if (temp>0 && temp<=gReportingFrequencyMin)
		fReportFrequency=gReportingFrequencyMin;
	else
		fReportFrequency=temp;

}

void FileBufferMgr::flushCache()
{
	mutex::scoped_lock lk(fWLock);
	{
		filebuffer_uset_t sEmpty;
		filebuffer_list_t lEmpty;
		emptylist_t vEmpty;

		fbList.swap(lEmpty);
		fbSet.swap(sEmpty);
		fEmptyPoolSlots.swap(vEmpty);
	}
	fCacheSize = 0;

	// the block pool should not be freed in the above block to allow us
	// to continue doing concurrent unprotected-but-"safe" memcpys
	// from that memory

	fFBPool.clear();
//	fFBPool.reserve(fMaxNumBlocks);
}

void FileBufferMgr::flushOne(const BRM::LBID_t lbid, const BRM::VER_t ver)
{
	//similar in function to depleteCache()
	mutex::scoped_lock lk(fWLock);

	filebuffer_uset_iter_t iter = fbSet.find(HashObject_t(lbid, ver, 0));
	if (iter != fbSet.end())
	{
		//remove it from fbList
		uint32_t idx = iter->poolIdx;
		fbList.erase(fFBPool[idx].listLoc());
		//add to fEmptyPoolSlots
		fEmptyPoolSlots.push_back(idx);
		//remove it from fbSet
		fbSet.erase(iter);
		//adjust fCacheSize
		fCacheSize--;
	}

}

void FileBufferMgr::flushMany(const LbidAtVer* laVptr, uint32_t cnt)
{
	mutex::scoped_lock lk(fWLock);

	BRM::LBID_t lbid;
	BRM::VER_t ver;
	filebuffer_uset_iter_t iter;
	for (uint32_t j = 0; j < cnt; j++)
	{
		lbid = static_cast<BRM::LBID_t>(laVptr->LBID);
		ver = static_cast<BRM::VER_t>(laVptr->Ver);
		iter = fbSet.find(HashObject_t(lbid, ver, 0));
		if (iter != fbSet.end())
		{
			//remove it from fbList
			uint32_t idx = iter->poolIdx;
			fbList.erase(fFBPool[idx].listLoc());
			//add to fEmptyPoolSlots
			fEmptyPoolSlots.push_back(idx);
			//remove it from fbSet
			fbSet.erase(iter);
			//adjust fCacheSize
			fCacheSize--;
		}
		++laVptr;
	}
}

void FileBufferMgr::flushManyAllversion(const LBID_t* laVptr, uint32_t cnt)
{
	filebuffer_uset_t::iterator it, tmpIt;
	tr1::unordered_set<LBID_t> uniquer;
	tr1::unordered_set<LBID_t>::iterator uit;

	mutex::scoped_lock lk(fWLock);

	if (fCacheSize == 0 || cnt == 0)
		return;

	for (uint i = 0; i < cnt; i++)
		uniquer.insert(laVptr[i]);

	for (it = fbSet.begin(); it != fbSet.end();) {
		if (uniquer.find(it->lbid) != uniquer.end()) {
			const uint32_t idx = it->poolIdx;
			fbList.erase(fFBPool[idx].listLoc());
			fEmptyPoolSlots.push_back(idx);
			tmpIt = it;
			++it;
			fbSet.erase(tmpIt);
			fCacheSize--;
		}
		else
			++it;
	}
}

void FileBufferMgr::flushOIDs(const uint32_t *oids, uint32_t count)
{
	DBRM dbrm;
	uint i;
	vector<EMEntry> extents;
	int err;
	uint currentExtent;
	LBID_t currentLBID;
	typedef tr1::unordered_multimap<LBID_t, filebuffer_uset_t::iterator> byLBID_t;
	byLBID_t byLBID;
	pair<byLBID_t::iterator, byLBID_t::iterator> itList;
	filebuffer_uset_t::iterator it;

	// If there are more than this # of extents to drop, the whole cache will be cleared
	const uint clearThreshold = 50000;

	mutex::scoped_lock lk(fWLock);
	
	if (fCacheSize == 0 || count == 0)
		return;

	/* Index the cache by LBID */
	for (it = fbSet.begin(); it != fbSet.end(); it++)
		byLBID.insert(pair<LBID_t, filebuffer_uset_t::iterator>(it->lbid, it));

	for (i = 0; i < count; i++) {
		extents.clear();
		err = dbrm.getExtents(oids[i], extents, true,true,true);  // @Bug 3838 Include outofservice extents
		if (err < 0 || (i == 0 && (extents.size() * count) > clearThreshold)) {
			// (The i == 0 should ensure it's not a dictionary column)
			lk.unlock();
			flushCache();
			return;
		}

		for (currentExtent = 0; currentExtent < extents.size(); currentExtent++) {
			EMEntry &range = extents[currentExtent];
			LBID_t lastLBID = range.range.start + (range.range.size * 1024);
			for (currentLBID = range.range.start; currentLBID < lastLBID;
			  currentLBID++) {
				itList = byLBID.equal_range(currentLBID);
				for (byLBID_t::iterator tmpIt = itList.first; tmpIt != itList.second;
						tmpIt++) {
					fbList.erase(fFBPool[tmpIt->second->poolIdx].listLoc());
					fEmptyPoolSlots.push_back(tmpIt->second->poolIdx);
					fbSet.erase(tmpIt->second);
					fCacheSize--;
				}
			}
		}
	}
}

void FileBufferMgr::flushPartition(const vector<OID_t> &oids, const set<BRM::LogicalPartition> &partitions)
{
	DBRM dbrm;
	uint i;
	vector<EMEntry> extents;
	int err;
	uint currentExtent;
	LBID_t currentLBID;
	typedef tr1::unordered_multimap<LBID_t, filebuffer_uset_t::iterator> byLBID_t;
	byLBID_t byLBID;
	pair<byLBID_t::iterator, byLBID_t::iterator> itList;
	filebuffer_uset_t::iterator it;
	uint32_t count = oids.size();

	mutex::scoped_lock lk(fWLock);

	if (fCacheSize == 0 || oids.size() == 0 || partitions.size() == 0)
		return;

	/* Index the cache by LBID */
	for (it = fbSet.begin(); it != fbSet.end(); it++)
		byLBID.insert(pair<LBID_t, filebuffer_uset_t::iterator>(it->lbid, it));

	for (i = 0; i < count; i++) {
		extents.clear();
		err = dbrm.getExtents(oids[i], extents, true, true,true); // @Bug 3838 Include outofservice extents
		if (err < 0) {
			lk.unlock();
			flushCache();   // better than returning an error code to the user
			return;
		}

		for (currentExtent = 0; currentExtent < extents.size(); currentExtent++) {
			EMEntry &range = extents[currentExtent];

			LogicalPartition logicalPartNum(range.dbRoot, range.partitionNum, range.segmentNum);
			if (partitions.find(logicalPartNum) == partitions.end())
				continue;

			LBID_t lastLBID = range.range.start + (range.range.size * 1024);
			for (currentLBID = range.range.start; currentLBID < lastLBID; currentLBID++) {
				itList = byLBID.equal_range(currentLBID);
				for (byLBID_t::iterator tmpIt = itList.first; tmpIt != itList.second;
						tmpIt++) {
					fbList.erase(fFBPool[tmpIt->second->poolIdx].listLoc());
					fEmptyPoolSlots.push_back(tmpIt->second->poolIdx);
					fbSet.erase(tmpIt->second);
					fCacheSize--;
				}
			}
		}
	}
}


bool FileBufferMgr::exists(const BRM::LBID_t& lbid, const BRM::VER_t& ver) const
{
	const HashObject_t fb(lbid, ver, 0);
	const bool b = exists(fb);
	return b;
}

FileBuffer* FileBufferMgr::findPtr(const HashObject_t& keyFb)
{
	mutex::scoped_lock lk(fWLock);

	filebuffer_uset_iter_t it = fbSet.find(keyFb);
	if (fbSet.end()!=it)
	{
		FileBuffer* fb=&(fFBPool[it->poolIdx]);
		fFBPool[it->poolIdx].listLoc()->hits++;
		fbList.splice( fbList.begin(), fbList, (fFBPool[it->poolIdx]).listLoc() );
		return fb;
	}	
	return NULL;
}


bool FileBufferMgr::find(const HashObject_t& keyFb, FileBuffer& fb)
{
	bool ret = false;

	mutex::scoped_lock lk(fWLock);

	filebuffer_uset_iter_t it = fbSet.find(keyFb);
	if (fbSet.end()!=it)
	{
		fFBPool[it->poolIdx].listLoc()->hits++;
		fbList.splice( fbList.begin(), fbList, (fFBPool[it->poolIdx]).listLoc() );
		fb = fFBPool[it->poolIdx];
		ret = true;
	}
	return ret;
}

bool FileBufferMgr::find(const HashObject_t& keyFb, void* bufferPtr)
{
	bool ret = false;

	if (gPMProfOn && gPMStatsPtr)
#ifdef _MSC_VER
		gPMStatsPtr->markEvent(keyFb.lbid, GetCurrentThreadId(), gSession, 'L');
#else
		gPMStatsPtr->markEvent(keyFb.lbid, pthread_self(), gSession, 'L');
#endif
	mutex::scoped_lock lk(fWLock);

	if (gPMProfOn && gPMStatsPtr)
#ifdef _MSC_VER
		gPMStatsPtr->markEvent(keyFb.lbid, GetCurrentThreadId(), gSession, 'M');
#else
		gPMStatsPtr->markEvent(keyFb.lbid, pthread_self(), gSession, 'M');
#endif
	filebuffer_uset_iter_t it = fbSet.find(keyFb);
	if (fbSet.end()!=it)
	{
		uint idx = it->poolIdx;

		//@bug 669 LRU cache, move block to front of list as last recently used.
		fFBPool[idx].listLoc()->hits++;
		fbList.splice(fbList.begin(), fbList, (fFBPool[idx]).listLoc());
		lk.unlock();
		memcpy(bufferPtr, (fFBPool[idx]).getData(), 8192);
		if (gPMProfOn && gPMStatsPtr)
#ifdef _MSC_VER
			gPMStatsPtr->markEvent(keyFb.lbid, GetCurrentThreadId(), gSession, 'U');
#else
			gPMStatsPtr->markEvent(keyFb.lbid, pthread_self(), gSession, 'U');
#endif
		ret = true;
	}

	return ret;
}

uint FileBufferMgr::bulkFind(const BRM::LBID_t *lbids, const BRM::VER_t *vers, uint8_t **buffers,
  bool *wasCached, uint count)
{
	uint i, ret = 0;
	filebuffer_uset_iter_t *it = (filebuffer_uset_iter_t *) alloca(count * sizeof(filebuffer_uset_iter_t));
	uint *indexes = (uint *) alloca(count * 4);
	
	if (gPMProfOn && gPMStatsPtr) {
		for (i = 0; i < count; i++) {
#ifdef _MSC_VER
			gPMStatsPtr->markEvent(lbids[i], GetCurrentThreadId(), gSession, 'L');
#else
			gPMStatsPtr->markEvent(lbids[i], pthread_self(), gSession, 'L');
#endif	
		}
	}
	
	mutex::scoped_lock lk(fWLock);

	if (gPMProfOn && gPMStatsPtr) {
		for (i = 0; i < count; i++) {
#ifdef _MSC_VER
			gPMStatsPtr->markEvent(lbids[i], GetCurrentThreadId(), gSession, 'M');
#else
			gPMStatsPtr->markEvent(lbids[i], pthread_self(), gSession, 'M');
#endif	
		}
	}

	for (i = 0; i < count; i++) {
		new ((void *) &it[i]) filebuffer_uset_iter_t();
		it[i] = fbSet.find(HashObject_t(lbids[i], vers[i], 0));
		if (it[i] != fbSet.end()) {
			indexes[i] = it[i]->poolIdx;
			wasCached[i] = true;
			fFBPool[it[i]->poolIdx].listLoc()->hits++;
			fbList.splice(fbList.begin(), fbList, (fFBPool[it[i]->poolIdx]).listLoc());
		}
		else {
			wasCached[i] = false;
			indexes[i] = 0;
		}
	}
	lk.unlock();   
	for (i = 0; i < count; i++) {
		if (wasCached[i]) {
			memcpy(buffers[i], fFBPool[indexes[i]].getData(), 8192);
			ret++;
			if (gPMProfOn && gPMStatsPtr) {
#ifdef _MSC_VER
				gPMStatsPtr->markEvent(lbids[i], GetCurrentThreadId(), gSession, 'U');
#else
				gPMStatsPtr->markEvent(lbids[i], pthread_self(), gSession, 'U');
#endif	
			}
		}
		it[i].filebuffer_uset_iter_t::~filebuffer_uset_iter_t();
	}
	return ret;
}

bool FileBufferMgr::exists(const HashObject_t& fb) const
{
	bool find_bool=false;
	mutex::scoped_lock lk(fWLock);

	filebuffer_uset_iter_t it = fbSet.find(fb);
	if (it != fbSet.end())
	{
		find_bool = true;
		fFBPool[it->poolIdx].listLoc()->hits++;
		fbList.splice(fbList.begin(), fbList, (fFBPool[it->poolIdx]).listLoc());
	}
	return find_bool;
}

// default insert operation.
// add a new fb into fbMgr and to fbList
// add to the front and age out from the back
// so add new fbs to the front of the list
//@bug 665: keep filebuffer in a vector. HashObject keeps the index of the filebuffer

int FileBufferMgr::insert(const BRM::LBID_t lbid, const BRM::VER_t ver, const uint8_t* data)
{
	int ret=0;

	if (gPMProfOn && gPMStatsPtr)
#ifdef _MSC_VER
		gPMStatsPtr->markEvent(lbid, GetCurrentThreadId(), gSession, 'I');
#else
		gPMStatsPtr->markEvent(lbid, pthread_self(), gSession, 'I');
#endif

	mutex::scoped_lock lk(fWLock);

	HashObject_t fbIndex(lbid, ver, 0);
	filebuffer_pair_t pr = fbSet.insert(fbIndex);
	if (pr.second) {
		// It was inserted (it wasn't there before)
		// Right now we have an invalid cache: we have inserted an entry with a -1 index.
		// We need to fix this quickly...
		fCacheSize++;
		FBData_t fbdata = {lbid, ver, 0};
		fbList.push_front(fbdata);
		fBlksLoaded++;
		if (fReportFrequency && (fBlksLoaded%fReportFrequency)==0) {
			struct timespec tm;
			clock_gettime(CLOCK_MONOTONIC, &tm);
			fLog 
				<< left << fixed << ((double)(tm.tv_sec+(1.e-9*tm.tv_nsec))) << " "
				<< right << setw(12) << fBlksLoaded << " "
				<< right << setw(12) << fBlksNotUsed << endl;
		}
	}
	else {  
		// if it's a duplicate there's nothing to do
		if (gPMProfOn && gPMStatsPtr)
#ifdef _MSC_VER
			gPMStatsPtr->markEvent(lbid, GetCurrentThreadId(), gSession, 'D');
#else
			gPMStatsPtr->markEvent(lbid, pthread_self(), gSession, 'D');
#endif
		return ret;
	}

	uint32_t pi = numeric_limits<int>::max();
	if (fCacheSize > maxCacheSize())
	{
		// If the insert above caused the cache to exceed its max size, find the lru block in
		// the cache and use its pool index to store the block data.
		FBData_t &fbdata = fbList.back();	//the lru block
		HashObject_t lastFB(fbdata.lbid, fbdata.ver, 0);
		filebuffer_uset_iter_t iter = fbSet.find( lastFB ); //should be there

		idbassert(iter != fbSet.end());
		pi = iter->poolIdx;
		idbassert(pi < maxCacheSize());
		idbassert(pi < fFBPool.size());

		// set iters are always const. We are not changing the hash here, and this gets us
		// the pointer we need cheaply...
		HashObject_t &ref = const_cast<HashObject_t &>(*pr.first);
		ref.poolIdx = pi;

		//replace the lru block with this block
		FileBuffer fb(lbid, ver, NULL, 0);
		fFBPool[pi] = fb;
		fFBPool[pi].setData(data, 8192);
		fbSet.erase(iter);
		if (fbList.back().hits==0)
			fBlksNotUsed++;
		fbList.pop_back();
		fCacheSize--;
		depleteCache();
		ret=1;
	}
	else
	{
		if ( ! fEmptyPoolSlots.empty() )
		{
			pi = fEmptyPoolSlots.front();
			fEmptyPoolSlots.pop_front();
			FileBuffer fb(lbid, ver, NULL, 0);
			fFBPool[pi] = fb;
			fFBPool[pi].setData(data, 8192);
		}
		else
		{
			pi = fFBPool.size();
			FileBuffer fb(lbid, ver, NULL, 0);
			fFBPool.push_back(fb);
			fFBPool[pi].setData(data, 8192);
		}

		// See comment above
		HashObject_t &ref = const_cast<HashObject_t &>(*pr.first);
		ref.poolIdx = pi;
		ret=1;
	}

	idbassert(pi < fFBPool.size());
	fFBPool[pi].listLoc(fbList.begin());

	if (gPMProfOn && gPMStatsPtr)
#ifdef _MSC_VER
		gPMStatsPtr->markEvent(lbid, GetCurrentThreadId(), gSession, 'J');
#else
		gPMStatsPtr->markEvent(lbid, pthread_self(), gSession, 'J');
#endif

	idbassert(fCacheSize <= maxCacheSize());
// 	idbassert(fCacheSize == fbSet.size());
// 	idbassert(fCacheSize == fbList.size());
	return ret;
}


void FileBufferMgr::depleteCache() 
{
	for (uint32_t i = 0; i < fDeleteBlocks && !fbList.empty(); ++i) 
	{
		FBData_t fbdata(fbList.back());	//the lru block
		HashObject_t lastFB(fbdata.lbid, fbdata.ver, 0);
		filebuffer_uset_iter_t iter = fbSet.find( lastFB ); 

		idbassert(iter != fbSet.end());
		uint32_t idx = iter->poolIdx;
		idbassert(idx < fFBPool.size());
		//Save position in FileBuffer pool for reuse.
		fEmptyPoolSlots.push_back(idx);
		fbSet.erase(iter);
		if (fbList.back().hits==0)
			fBlksNotUsed++;
		fbList.pop_back();
		fCacheSize--;
	}
}

ostream& FileBufferMgr::formatLRUList(ostream& os) const
{
	filebuffer_list_t::const_iterator iter=fbList.begin();
	filebuffer_list_t::const_iterator end=fbList.end();

	while (iter != end)
	{
		os << iter->lbid << '\t' << iter->ver << endl;
		++iter;
	}

	return os;
}

// puts the new entry at the front of the list
void FileBufferMgr::updateLRU(const FBData_t &f)
{
	if (fCacheSize > maxCacheSize()) {
		list<FBData_t>::iterator last = fbList.end();
		last--;
		FBData_t &fbdata = *last;
		HashObject_t lastFB(fbdata.lbid, fbdata.ver, 0);
		filebuffer_uset_iter_t iter = fbSet.find(lastFB);
		fEmptyPoolSlots.push_back(iter->poolIdx);
		if (fbdata.hits == 0)
			fBlksNotUsed++;
		fbSet.erase(iter);
		fbList.splice(fbList.begin(), fbList, last);
		fbdata = f;
		fCacheSize--;
		//cout << "booted an entry\n";
	}
	else {
		//cout << "new entry\n";
		fbList.push_front(f);
	}
}

uint32_t FileBufferMgr::doBlockCopy(const BRM::LBID_t &lbid, const BRM::VER_t &ver, const uint8_t *data)
{
	uint32_t poolIdx;
	
	if (!fEmptyPoolSlots.empty()) {
		poolIdx = fEmptyPoolSlots.front();
		fEmptyPoolSlots.pop_front();
	}
	else {
		poolIdx = fFBPool.size();
		fFBPool.resize(poolIdx + 1);   //shouldn't trigger a 'real' resize b/c of the reserve call
	}

	fFBPool[poolIdx].Lbid(lbid);
	fFBPool[poolIdx].Verid(ver);
	fFBPool[poolIdx].setData(data);
	return poolIdx;
}

int FileBufferMgr::bulkInsert(const vector<CacheInsert_t> &ops)
{
	uint i;
	int32_t pi;
	int ret = 0;

	mutex::scoped_lock lk(fWLock);

	for (i = 0; i < ops.size(); i++) {
		const CacheInsert_t &op = ops[i];

		if (gPMProfOn && gPMStatsPtr)
#ifdef _MSC_VER
			gPMStatsPtr->markEvent(op.lbid, GetCurrentThreadId(), gSession, 'I');
#else
			gPMStatsPtr->markEvent(op.lbid, pthread_self(), gSession, 'I');
#endif

		HashObject_t fbIndex(op.lbid, op.ver, 0);
		filebuffer_pair_t pr = fbSet.insert(fbIndex);
		
		if (!pr.second) {
			if (gPMProfOn && gPMStatsPtr)
#ifdef _MSC_VER
				gPMStatsPtr->markEvent(op.lbid, GetCurrentThreadId(), gSession, 'D');
#else
				gPMStatsPtr->markEvent(op.lbid, pthread_self(), gSession, 'D');
#endif
			continue;
		}
		
		//cout << "FBM: inserting <" << op.lbid << ", " << op.ver << endl;
		fCacheSize++;
		fBlksLoaded++;
		FBData_t fbdata = {op.lbid, op.ver, 0};
		updateLRU(fbdata);
		pi = doBlockCopy(op.lbid, op.ver, op.data);
		
		HashObject_t &ref = const_cast<HashObject_t &>(*pr.first);
		ref.poolIdx = pi;
		fFBPool[pi].listLoc(fbList.begin());
		if (gPMProfOn && gPMStatsPtr)
#ifdef _MSC_VER
			gPMStatsPtr->markEvent(op.lbid, GetCurrentThreadId(), gSession, 'J');
#else
			gPMStatsPtr->markEvent(op.lbid, pthread_self(), gSession, 'J');
#endif
		ret++;
	}
	idbassert(fCacheSize <= maxCacheSize());

	return ret;
}


}
