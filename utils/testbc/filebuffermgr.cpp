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

/***************************************************************************
 *
 *   $Id: filebuffermgr.cpp 699 2008-09-09 19:44:18Z rdempsey $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/
/**
* 	InitialDBBCSize - the starting number of elements the unordered set used to store disk blocks.
	This does not instantiate InitialDBBCSize disk blocks but only the initial size of the unordered_set

**/

#define NDEBUG //Turn off assert macro
#include <cassert>
#include <limits.h>
#include <boost/thread.hpp>
#include "filebuffermgr.h"
using namespace boost;
using namespace std;
#include "stats.h"

extern dbbc::Stats* gPMStatsPtr;
extern bool gPMProfOn;
extern uint32_t gSession;

namespace dbbc {

FileBufferMgr::FileBufferMgr(const uint32_t numBlcks, const uint32_t blkSz, const uint32_t deleteBlocks)
	:fMaxNumBlocks(numBlcks),
	fBlockSz(blkSz), 
	fWLock(), 
	fbSet(), 
	fbList(), 
	fCacheSize(0), 
	fFBPool(), 
	aging(false), 
	fDeleteBlocks(deleteBlocks), 
	fEmptyPoolSlots()
{
	fFBPool.reserve(numBlcks);
	fEmptyPoolSlots.reserve(deleteBlocks);
}

FileBufferMgr::~FileBufferMgr()
{
	flushCache();
}

void FileBufferMgr::flushCache()
{
	mutex::scoped_lock lk(fWLock);
	fbList.clear();
	fbSet.clear();
	fFBPool.clear();
	fEmptyPoolSlots.clear();
	//TODO:: re-init blocks in pool and HWM
}


bool FileBufferMgr::exists(const BRM::LBID_t& lbid, const BRM::VER_t& ver) const {
	const HashObject_t fb ={lbid, ver, 0};
	const bool b = exists(fb);
	return b;
} // bool FileBufferMgr::exists(const BRM::LBID_t& lbid, const BRM::VER_t& ver) const


FileBuffer* FileBufferMgr::findPtr(const HashObject_t& keyFb)  {
	mutex::scoped_lock lk(fWLock);
	filebuffer_uset_iter_t it = fbSet.find(keyFb);
	if (fbSet.end()!=it)
	{
		FileBuffer* fb=&(fFBPool[it->poolIdx]);
		fbList.splice( fbList.begin(), fbList, (fFBPool[it->poolIdx]).listLoc() );
		return fb;
	}	

	return NULL;

} // end findPtr(const HashObject_t& keyFB)


bool FileBufferMgr::find(const HashObject_t& keyFb, FileBuffer& fb)  {
	bool ret = false;

	mutex::scoped_lock lk(fWLock);
	filebuffer_uset_iter_t it = fbSet.find(keyFb);
	if (fbSet.end()!=it)
	{
		fbList.splice( fbList.begin(), fbList, (fFBPool[it->poolIdx]).listLoc() );
		lk.unlock();
		fb = fFBPool[it->poolIdx];
		ret = true;
	}	

	return ret;

} // end find(const HashObject_t& keyFB, HashObject_t& fb)


bool FileBufferMgr::find(const HashObject_t& keyFb, void* bufferPtr) {
	bool ret = false;

	if (gPMProfOn && gPMStatsPtr)
		gPMStatsPtr->markEvent(keyFb.lbid, pthread_self(), gSession, 'L');
	mutex::scoped_lock lk(fWLock);
	if (gPMProfOn && gPMStatsPtr)
		gPMStatsPtr->markEvent(keyFb.lbid, pthread_self(), gSession, 'M');
	filebuffer_uset_iter_t it = fbSet.find(keyFb);
	if (fbSet.end()!=it)
	{
		//@bug 669 LRU cache, move block to front of list as last recently used.
		fbList.splice(fbList.begin(), fbList, (fFBPool[it->poolIdx]).listLoc());
		lk.unlock();
		memcpy(bufferPtr, (fFBPool[it->poolIdx]).getData(), 8);
		if (gPMProfOn && gPMStatsPtr)
			gPMStatsPtr->markEvent(keyFb.lbid, pthread_self(), gSession, 'U');
		ret = true;
	}

	return ret;

} // end find(const FileBuffer& keyFB, void* bufferPtr)


bool FileBufferMgr::exists(const HashObject_t& fb) const {
	bool find_bool=false;
	mutex::scoped_lock lk(fWLock);
	filebuffer_uset_iter_t it = fbSet.find(fb);
	if (it != fbSet.end())
	{
		find_bool = true;
		fbList.splice(fbList.begin(), fbList, (fFBPool[it->poolIdx]).listLoc());
	}

	return find_bool;
}

// default insert operation.
// add a new fb into fbMgr and to fbList
// add to the front and age out from the back
// so add new fbs to the front of the list
//@bug 665: keep filebuffer in a vector. HashObject keeps the index of the filebuffer

const int FileBufferMgr::insert(const BRM::LBID_t lbid, const BRM::VER_t ver, const uint8_t* data)
{
	int ret=0;

	if (gPMProfOn && gPMStatsPtr)
		gPMStatsPtr->markEvent(lbid, pthread_self(), gSession, 'I');

	mutex::scoped_lock lk(fWLock);
	HashObject_t fbIndex = { lbid, ver, 0}; 
	filebuffer_pair_t pr = fbSet.insert(fbIndex);
	if (pr.second) {
		// It was inserted (it wasn't there before)
		// Right now we have an invalid cache: we have inserted an entry with a -1 index.
		// We need to fix this quickly...
		fCacheSize++;
		FBData_t fbdata = {lbid, ver};
		fbList.push_front(fbdata);
	}
	else {  
		// if it's a duplicate there's nothing to do
		if (gPMProfOn && gPMStatsPtr)
			gPMStatsPtr->markEvent(lbid, pthread_self(), gSession, 'D');
		return ret;
	}

	uint32_t pi = INT_MAX;
	if (fCacheSize > maxCacheSize())
	{
		// If the insert above caused the cache to exceed its max size, find the lru block in
		// the cache and use its pool index to store the block data.
		FBData_t &fbdata = fbList.back();	//the lru block
		HashObject_t lastFB = {fbdata.lbid, fbdata.ver, 0};
		filebuffer_uset_iter_t iter = fbSet.find( lastFB ); //should be there

		idbassert(iter != fbSet.end());
		pi = iter->poolIdx;
		idbassert(pi < maxCacheSize());
		idbassert(pi < fFBPool.size());

		/* Why does this iterator return a const HashObject_t? */
		HashObject_t &ref = const_cast<HashObject_t &>(*pr.first);
		ref.poolIdx = pi;

		//replace the lru block with this block
		FileBuffer fb(lbid, ver, NULL, 0);
		fFBPool[pi] = fb;
		fFBPool[pi].setData(data, BLOCK_SIZE);
		fbSet.erase(iter);
		fbList.pop_back();

		fCacheSize--;
		depleteCache();
		ret=1;
	}
	else
	{
		if ( ! fEmptyPoolSlots.empty() )
		{
			pi = fEmptyPoolSlots.back();
			fEmptyPoolSlots.pop_back();
			FileBuffer fb(lbid, ver, NULL, 0);
			fFBPool[pi] = fb;
			fFBPool[pi].setData(data, 8);
		}
		else
		{
			pi = fFBPool.size();
			FileBuffer fb(lbid, ver, NULL, 0);
			fFBPool.push_back(fb);
			fFBPool[pi].setData(data, 8);
		}

		/* Why does this iterator return a const? */
		HashObject_t &ref = const_cast<HashObject_t &>(*pr.first);
		ref.poolIdx = pi;
		ret=1;
	}

	idbassert(pi < fFBPool.size());
	fFBPool[pi].listLoc(fbList.begin());

	if (gPMProfOn && gPMStatsPtr)
		gPMStatsPtr->markEvent(lbid, pthread_self(), gSession, 'J');

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
		HashObject_t lastFB = {fbdata.lbid, fbdata.ver, 0};
		filebuffer_uset_iter_t iter = fbSet.find( lastFB ); 

		idbassert(iter != fbSet.end());
		uint32_t idx = iter->poolIdx;
		idbassert(idx < fFBPool.size());
		//Save position in FileBuffer pool for reuse.
		fEmptyPoolSlots.push_back(idx);
		fbSet.erase(iter);
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

}
