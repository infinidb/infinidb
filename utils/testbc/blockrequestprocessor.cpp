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
 *   $Id: blockrequestprocessor.cpp 725 2008-09-26 16:26:47Z jrodriguez $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/

#include "blockrequestprocessor.h"
#include "rwlock_local.h"
#include "dbrm.h"
#include <sys/time.h>
#include <pthread.h>
#include <sstream>
#include <string>
#include <list>
#include <boost/date_time/posix_time/posix_time.hpp>
using namespace std;

namespace dbbc {

BlockRequestProcessor::BlockRequestProcessor(uint32_t numBlcks,
										int thrCount, 
										int blocksPerRead,  
										uint32_t deleteBlocks, 
										uint32_t blckSz) :
									fbMgr(numBlcks, blckSz, deleteBlocks), 
									fIOMgr(fbMgr, fBRPRequestQueue, thrCount, blocksPerRead)
{
}


BlockRequestProcessor::~BlockRequestProcessor()
{
}

void BlockRequestProcessor::stop() {
	fBRPRequestQueue.stop();
	fIOMgr.stop();	
}


int BlockRequestProcessor::check(const BRM::InlineLBIDRange& range, const BRM::VER_t ver, uint32_t& lbidCount) {
	uint64_t maxLbid = range.start; // highest existent lbid
	uint64_t rangeLen = range.size;
	uint64_t idx;
	uint64_t adjSz;
	struct timespec start_tm;

	if (fTrace) clock_gettime(CLOCK_MONOTONIC, &start_tm);

	for (idx=0; fbMgr.exists(maxLbid, ver)==true&&idx<rangeLen; maxLbid++, idx++);

	if (idx==rangeLen) { // range is already loaded
		uint32_t fbo;
		BRM::OID_t oid;
		fdbrm.lookup(maxLbid, ver, false, oid, fbo);
			fLogFile
					<< oid << " " 
					<< maxLbid << " "
					<< fbo << " "
					<< rangeLen << " " 
					<< 0 << " " 
					<< 0 << " " 
					<< 0 << " " 
					<< right << fixed << ((double)(start_tm.tv_sec+(1.e-9*start_tm.tv_nsec)))
					<< endl;
		return 0;
	}

	adjSz=rangeLen-idx;
	BRM::InlineLBIDRange adjRange;
	adjRange.start=maxLbid;
	adjRange.size=adjSz;
	fileRequest rqstBlk(adjRange, ver);
	check(rqstBlk);
	lbidCount=rqstBlk.BlocksRead();

	if (fTrace) {
		uint32_t fbo;
		BRM::OID_t oid;
		fdbrm.lookup(maxLbid, ver, false, oid, fbo);
		fLogFile
				<< oid << " " 
				<< maxLbid << " "
				<< fbo << " "
				<< rangeLen << " " 
				<< adjSz << " " 
				<< rqstBlk.BlocksRead() << " " 
				<< rqstBlk.BlocksLoaded() << " " 
				<< right << fixed << ((double)(start_tm.tv_sec+(1.e-9*start_tm.tv_nsec)))
				<< endl;
	}

	return rqstBlk.BlocksLoaded();
} // check


int BlockRequestProcessor::check(fileRequest& rqstBlk) {
	pthread_mutex_lock(&rqstBlk.frMutex());
	rqstBlk.SetPredicate(fileRequest::SENDING);
	sendRequest(rqstBlk); 	// start file read request

	while(rqstBlk.frPredicate()<fileRequest::COMPLETE)
		pthread_cond_wait(&rqstBlk.frCond(), &rqstBlk.frMutex());
	pthread_mutex_unlock(&rqstBlk.frMutex());

	return 0;
}


int BlockRequestProcessor::check(BRM::LBID_t lbid, BRM::VER_t ver, bool flg, bool& wasBlockInCache) {
	if (fbMgr.exists(lbid, ver)==true) {
		wasBlockInCache = true;
		return 0;
	} else {
		wasBlockInCache = false;
		fileRequest rqstBlk(lbid, ver, flg);
		int ret=check(rqstBlk);
		return ret;
	}
}


int BlockRequestProcessor::sendRequest(fileRequest& blk)
{
	int ret = fBRPRequestQueue.push(blk);
	return ret;
}


const int BlockRequestProcessor::read(const BRM::InlineLBIDRange& range, FileBufferList_t& readList, const BRM::VER_t ver)
{
	int blksLoaded=0;
	HashObject_t fb	= {0, 0, 0};
	for(int idx=0; (uint64_t)idx<range.size; idx++) {
		fb.lbid=range.start+idx;
		fb.ver=ver;
		fb.poolIdx=0;
		FileBuffer fbRet(-1, -1);
		bool ret = false; //fbMgr.find(fb, fbRet);
		if (ret) {
			blksLoaded++;
			readList.push_back(fbRet);
		}
	}
	
	return blksLoaded;
}


FileBuffer* BlockRequestProcessor::getBlockPtr(const BRM::LBID_t lbid, const BRM::VER_t ver ) {

	HashObject_t hashObj = {lbid, ver, 0};
	FileBuffer* fb = fbMgr.findPtr(hashObj);
	return fb;
}


const int BlockRequestProcessor::read(const BRM::LBID_t& lbid, const BRM::VER_t& ver, FileBuffer& fb) {

	HashObject_t hashObj = {lbid, ver, 0};
	bool ret = fbMgr.find(hashObj, fb);
	if (ret==true)
		return 1;
	else
		return 0;
}

const int BlockRequestProcessor::read(const BRM::LBID_t& lbid, const BRM::VER_t& ver, void* bufferPtr) {
	HashObject_t hashObj = {lbid, ver, 0};
	bool ret = fbMgr.find(hashObj, bufferPtr);
	if (ret==true)
		return 1;
	else
		return 0;
}

const int BlockRequestProcessor::getBlock(const BRM::LBID_t& lbid, const BRM::VER_t& ver, void* bufferPtr, bool flg, bool &wasCached) 
{
	HashObject_t hashObj = {lbid, ver, 0};
	wasCached = fbMgr.find(hashObj, bufferPtr);
	if (wasCached)
		return 1;

	wasCached = false;
	fileRequest rqstBlk(lbid, ver, flg, (uint8_t *) bufferPtr);
	check(rqstBlk);
	return 1;
}

bool BlockRequestProcessor::exists(BRM::LBID_t lbid, BRM::VER_t ver)
{
	HashObject_t ho = {lbid, ver, 0};

	return fbMgr.exists(ho);
}

void BlockRequestProcessor::flushCache() {
	fbMgr.flushCache();
}
/**
const uint32_t BlockRequestProcessor::resize(const uint32_t s)
{
	int rc = fbMgr.resize(s);
	return rc;
}
**/
ostream& BlockRequestProcessor::formatLRUList(ostream& os) const
{
	return fbMgr.formatLRUList(os);
}

} // namespace dbbc
