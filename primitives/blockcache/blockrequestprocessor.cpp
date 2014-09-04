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
 *   $Id: blockrequestprocessor.cpp 1965 2012-10-11 19:58:47Z xlou $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/

#include <sys/time.h>
#include <sstream>
#include <string>
#include <list>
using namespace std;

#include "blockrequestprocessor.h"
#include "rwlock_local.h"
#include "dbrm.h"
#include "pp_logger.h"

namespace dbbc {

BlockRequestProcessor::BlockRequestProcessor(uint32_t numBlcks,
										int thrCount, 
										int blocksPerRead,  
										uint32_t deleteBlocks, 
										uint32_t blckSz) :
									fbMgr(numBlcks, blckSz, deleteBlocks), 
									fIOMgr(fbMgr, fBRPRequestQueue, thrCount, blocksPerRead)
{
	//pthread_mutex_init(&check_mutex, NULL);
	config::Config* fConfig=config::Config::makeConfig();
    string val = fConfig->getConfig("DBBC", "BRPTracing");
	int temp=0;
#ifdef _MSC_VER
	int tid = GetCurrentThreadId();
#else
	pthread_t tid = pthread_self();
#endif

    if (val.length()>0) temp=static_cast<int>(config::Config::fromText(val));
        
    if (temp > 0)
		fTrace=true;
	else
		fTrace=false;

	if (fTrace)
	{
		ostringstream brpLogFileName;
#ifdef _MSC_VER
		brpLogFileName << "C:/Calpont/log/trace/brp." << tid;
#else
		brpLogFileName << "/var/log/Calpont/trace/brp." << tid;
#endif
		fLogFile.open(brpLogFileName.str().c_str(), ios_base::app | ios_base::ate);
	}
}


BlockRequestProcessor::~BlockRequestProcessor()
{
	//pthread_mutex_destroy(&check_mutex);
	if (fTrace)
		fLogFile.close();
}

void BlockRequestProcessor::stop() {
	fBRPRequestQueue.stop();
	fIOMgr.stop();	
}

int BlockRequestProcessor::check(const BRM::InlineLBIDRange& range, const BRM::VER_t ver, const BRM::VER_t txn, const int compType, uint32_t& lbidCount) {
	uint64_t maxLbid = range.start; // highest existent lbid
	uint64_t rangeLen = range.size;
	uint64_t idx;
	uint64_t adjSz;
	struct timespec start_tm;
	lbidCount = 0;

	if (fTrace)
		clock_gettime(CLOCK_MONOTONIC, &start_tm);

	for (idx = 0; fbMgr.exists(maxLbid, ver) == true && idx<rangeLen; maxLbid++, idx++)
		(void)0;

	if (idx == rangeLen) { // range is already loaded
		if (fTrace)
		{
			uint16_t dbroot;
			uint32_t partNum;
			uint16_t segNum;
			uint32_t fbo;
			BRM::OID_t oid;
			fdbrm.lookupLocal(maxLbid, ver, false, oid, dbroot, partNum, segNum, fbo);
			fLogFile
					<< oid << " " 
					<< maxLbid << " "
					<< fbo << " "
					<< rangeLen << " " 
					<< 0 << " " 
					<< 0 << " " 
					<< 0 << " " 
					<< right << fixed << ((double)(start_tm.tv_sec + (1.e-9 * start_tm.tv_nsec)))
					<< endl;
		}
		return 0;
	}

	adjSz=rangeLen-idx;
	BRM::InlineLBIDRange adjRange;
	adjRange.start=maxLbid;
	adjRange.size=adjSz;
	fileRequest rqstBlk(adjRange, ver, txn, compType);
	check(rqstBlk);
	if (rqstBlk.RequestStatus() != fileRequest::SUCCESSFUL) {
		if (rqstBlk.RequestStatus() == fileRequest::FS_EINVAL)
			throw logging::IDBExcept(logging::IDBErrorInfo::instance()->
					errorMsg(logging::ERR_O_DIRECT), logging::ERR_O_DIRECT);
		else if (rqstBlk.RequestStatus() == fileRequest::FS_ENOENT)
			throw logging::IDBExcept(logging::IDBErrorInfo::instance()->
					errorMsg(logging::ERR_ENOENT), logging::ERR_ENOENT);

		throw runtime_error(rqstBlk.RequestStatusStr());
	}
	lbidCount=rqstBlk.BlocksRead();

	if (fTrace) {
		uint16_t dbroot;
		uint32_t partNum;
		uint16_t segNum;
		uint32_t fbo;
		BRM::OID_t oid;
		fdbrm.lookupLocal(maxLbid, ver, false, oid, dbroot, partNum, segNum, fbo);
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
	rqstBlk.frMutex().lock();
	rqstBlk.SetPredicate(fileRequest::SENDING);
	sendRequest(rqstBlk); 	// start file read request

	while(rqstBlk.frPredicate()<fileRequest::COMPLETE)
		rqstBlk.frCond().wait(rqstBlk.frMutex());
	rqstBlk.frMutex().unlock();

	return 0;
}

// For future use.  Not currently used.
int BlockRequestProcessor::check(BRM::LBID_t lbid, BRM::VER_t ver, BRM::VER_t txn, bool flg, int compType, bool& wasBlockInCache) {
	if (fbMgr.exists(lbid, ver)==true) {
		wasBlockInCache = true;
		return 0;
	} else {
		wasBlockInCache = false;
		fileRequest rqstBlk(lbid, ver, flg, txn, compType);
		int ret=check(rqstBlk);
		if (rqstBlk.RequestStatus() != fileRequest::SUCCESSFUL) {
			throw runtime_error(rqstBlk.RequestStatusStr());
		}
		return ret;
	}
}

const int BlockRequestProcessor::read(const BRM::InlineLBIDRange& range, FileBufferList_t& readList, const BRM::VER_t ver)
{
	int blksLoaded=0;
	HashObject_t fb(0, 0, 0);
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

const int BlockRequestProcessor::getBlock(const BRM::LBID_t& lbid, const BRM::VER_t& ver, BRM::VER_t txn,
	int compType, void* bufferPtr, bool vbFlg, bool &wasCached, bool *versioned, bool insertIntoCache,
	bool readFromCache)
{
	if (readFromCache) {
		HashObject_t hashObj(lbid, ver, 0);
		wasCached = fbMgr.find(hashObj, bufferPtr);
		if (wasCached)
			return 1;
	}

	wasCached = false;
	fileRequest rqstBlk(lbid, ver, vbFlg, txn, compType, (uint8_t *) bufferPtr, insertIntoCache);
	check(rqstBlk);
	if (rqstBlk.RequestStatus() == fileRequest::BRM_LOOKUP_ERROR)
	{
		ostringstream os;
		os << "BRP::getBlock(): got a BRM lookup error.  LBID=" << lbid << " ver=" << ver << " txn="
			<< txn << " vbFlg=" << (int) vbFlg;
		primitiveprocessor::Logger logger;
		logger.logMessage(os.str(), false);
		throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_BRM_LOOKUP), logging::ERR_BRM_LOOKUP);
	}
	else if (rqstBlk.RequestStatus() == fileRequest::FS_EINVAL)
	{
		throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_O_DIRECT),
									logging::ERR_O_DIRECT);
	}
	else if (rqstBlk.RequestStatus() == fileRequest::FS_ENOENT)
	{
		throw logging::IDBExcept(
						logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_ENOENT),
									logging::ERR_ENOENT);
	}
	else if (rqstBlk.RequestStatus() != fileRequest::SUCCESSFUL) {
		throw runtime_error(rqstBlk.RequestStatusStr());
	}
	if (versioned)
		*versioned = rqstBlk.versioned();
	return 1;
}

int BlockRequestProcessor::getCachedBlocks(const BRM::LBID_t *lbids, const BRM::VER_t *vers,
	uint8_t **ptrs, bool *wasCached, uint count)
{
	return fbMgr.bulkFind(lbids, vers, ptrs, wasCached, count);
}
	

} // namespace dbbc
