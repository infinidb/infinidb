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

/***********************************************************************
 *   $Id: primitiveserver.cpp 2027 2013-01-04 20:18:09Z pleblanc $
 *
 *
 ***********************************************************************/
#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdexcept>
//#define NDEBUG
#include <cassert>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#ifdef _MSC_VER
#include <intrin.h>
#include <unordered_map>
typedef int pthread_t;
#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#include <pthread.h>
#endif
#include <cerrno>

using namespace std;

#include <boost/timer.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "primproc.h"
#include "primitiveserver.h"
#include "primitivemsg.h"
#include "umsocketselector.h"
#include "brm.h"
using namespace BRM;

#include "writeengine.h"

#include "messagequeue.h"
using namespace messageqcpp;

#include "blockrequestprocessor.h"
#include "blockcacheclient.h"
#include "stats.h"
using namespace dbbc;

#include "liboamcpp.h"
using namespace oam;

#include "configcpp.h"
using namespace config;

#include "bppseeder.h"
#include "primitiveprocessor.h"
#include "pp_logger.h"
using namespace primitives;

#include "multicast.h"
using namespace multicast;
#include "errorcodes.h"
#include "exceptclasses.h"

#include "idbcompress.h"
using namespace compress;

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

typedef tr1::unordered_set<BRM::OID_t> USOID;

// make global for blockcache
//
static const char* statsName ={"pm"};
Stats* gPMStatsPtr=0;
bool gPMProfOn=false;
uint32_t gSession=0;
Stats pmstats(statsName);

//FIXME: there is an anon ns burried later in between 2 named namespaces...
namespace primitiveprocessor
{
#ifdef _MSC_VER
	extern CRITICAL_SECTION preadCSObject;
#else
//#define IDB_COMP_POC_DEBUG
#ifdef IDB_COMP_POC_DEBUG
extern boost::mutex compDebugMutex;
#endif
#endif

	BlockRequestProcessor **BRPp;
	Stats stats;
	extern DebugLevel gDebugLevel;
	BRM::DBRM* brm;
	int fCacheCount;
	bool fPMProfOn;
	bool fLBIDTraceOn;
	uint BPPCount;		// a config param
	uint blocksReadAhead;		// a config param
	uint defaultBufferSize;
	uint connectionsPerUM;
	const uint8_t fMaxColWidth(8);
	BPPMap bppMap;
	mutex bppLock;
	mutex djLock;  // djLock synchronizes destroy and joiner msgs, see bug 2619
#ifdef _MSC_VER
	volatile LONG asyncCounter;
#else
	int asyncCounter;
#endif
	const int asyncMax = 20;	// current number of asynchronous loads

	extern bool utf8;

	struct preFetchCond {
		//uint64_t lbid;
		boost::condition cond;
		unsigned waiters;

		preFetchCond(const uint64_t l) {
			//lbid=l;
			//pthread_cond_init(&cond, NULL);
			waiters = 0;
		}

		~preFetchCond() {/*pthread_cond_destroy(&cond);*/}
	};

	typedef preFetchCond preFetchBlock_t;
	typedef std::tr1::unordered_map<uint64_t, preFetchBlock_t*> pfBlockMap_t;
	typedef std::tr1::unordered_map<uint64_t, preFetchBlock_t*>::iterator pfBlockMapIter_t;

	pfBlockMap_t pfBlockMap;
	boost::mutex pfbMutex; // = PTHREAD_MUTEX_INITIALIZER;

	pfBlockMap_t pfExtentMap;
	boost::mutex pfMutex; // = PTHREAD_MUTEX_INITIALIZER;

	map<uint32_t, shared_ptr<DictEqualityFilter> > dictEqualityFilters;
	mutex eqFilterMutex;

	uint cacheNum(uint64_t lbid)
	{
		return ( lbid / brm->getExtentSize() ) % fCacheCount;
	}

	void buildOidFileName(const BRM::OID_t oid,
						  const uint16_t dbRoot,
						  const uint16_t partNum,
						  const uint32_t segNum,
						  char* file_name)
	{
		WriteEngine::FileOp fileOp(false);

		if (fileOp.getFileName(oid, file_name, dbRoot, partNum, segNum) != WriteEngine::NO_ERROR)
		{
			file_name[0]=0;
			throw std::runtime_error("fileOp.getFileName failed");
		}
		//cout << "Oid2Filename o: " << oid << " n: " << file_name << endl;
	}


	void waitForRetry(long count)
	{
		timespec ts;
		ts.tv_sec = 5L*count/10L;
		ts.tv_nsec = (5L*count%10L)*100000000L;
#ifdef _MSC_VER
		Sleep(ts.tv_sec * 1000 + ts.tv_nsec / 1000 / 1000);
#else
		nanosleep(&ts, 0);
#endif
	}


	void prefetchBlocks(const uint64_t lbid,
						const uint32_t ver,
						const uint32_t txn,
						const int compType,
						uint32_t* rCount)
	{
		uint16_t dbRoot;
		uint32_t partNum;
		uint16_t segNum;
		uint32_t hwm;
		uint32_t fbo;
		uint32_t lowfbo;
		uint32_t highfbo;
		BRM::OID_t oid;
		pfBlockMap_t::const_iterator iter;
		uint64_t lowlbid = (lbid/blocksReadAhead) * blocksReadAhead;
		blockCacheClient bc(*BRPp[cacheNum(lbid)]);
		BRM::InlineLBIDRange range;
		int err;

		pfbMutex.lock(); //pthread_mutex_lock(&pfbMutex);

		iter = pfBlockMap.find(lowlbid);
		if (iter!=pfBlockMap.end()) {
			iter->second->waiters++;
			iter->second->cond.wait(pfbMutex); //pthread_cond_wait(&(iter->second->cond), &pfbMutex);
			iter->second->waiters--;
			pfbMutex.unlock(); //pthread_mutex_unlock(&pfbMutex);
			return;
		}

		preFetchBlock_t* pfb = 0;
		pfb = new preFetchBlock_t(lowlbid);

		pfBlockMap[lowlbid]=pfb;
		pfbMutex.unlock(); //pthread_mutex_unlock(&pfbMutex);

		// loadBlock will catch a versioned block so vbflag can be set to false here
		err = brm->lookupLocal(lbid, ver, false, oid, dbRoot, partNum, segNum, fbo); // need the oid
		if (err < 0) {
			cerr << "prefetchBlocks(): BRM lookupLocal failed! Expect more errors.\n";
			goto cleanup;
		}
		err = brm->getLocalHWM(oid, partNum, segNum, hwm);
		if (err < 0) {
			cerr << "prefetchBlock(): BRM getLocalHWM failed! Expect more errors.\n";
			goto cleanup;
		}

		lowfbo = fbo - (lbid - lowlbid);
		highfbo = lowfbo + blocksReadAhead - 1;
		range.start = lowlbid;
		if (hwm < highfbo)
			range.size = hwm - lowfbo + 1;
		else
			range.size = blocksReadAhead;

		try {
			if (range.size>blocksReadAhead) {
				ostringstream os;
				os << "Invalid Range from HWM for lbid " << lbid
					<< ", range size should be <= blocksReadAhead: HWM " << hwm
					<< ", highfbo " << highfbo << ", lowfbo " << lowfbo
					<< ", blocksReadAhead " << blocksReadAhead
					<< ", range size " <<  range.size << endl;
				throw logging::InvalidRangeHWMExcept(os.str());
			}

			assert(range.size<=blocksReadAhead);

			bc.check(range, numeric_limits<VER_t>::max(), 0, compType, *rCount);
		}
		catch (...)
		{
			// Perform necessary cleanup before rethrowing the exception
			pfb->cond.notify_all(); //pthread_cond_broadcast(&(pfb->cond));

			pfbMutex.lock(); //pthread_mutex_lock(&(pfbMutex));
			while (pfb->waiters > 0)
			{
				pfbMutex.unlock();
				//handle race condition with other threads going into wait before the broadcast above
				pfb->cond.notify_one();
				usleep(1);
				pfbMutex.lock();
			}
			if (pfBlockMap.erase(lowlbid) > 0)
				delete pfb;
			pfb=0;
			pfbMutex.unlock(); //pthread_mutex_unlock(&pfbMutex);
			throw;
		}
	cleanup:
		pfb->cond.notify_all(); //pthread_cond_broadcast(&(pfb->cond));

		pfbMutex.lock(); //pthread_mutex_lock(&(pfbMutex));
		while (pfb->waiters > 0)
		{
			pfbMutex.unlock();
			//handle race condition with other threads going into wait before the broadcast above
			pfb->cond.notify_one();
			usleep(1);
			pfbMutex.lock();
		}
		if (pfBlockMap.erase(lowlbid) > 0)
			delete pfb;
		pfb=0;
		pfbMutex.unlock(); //pthread_mutex_unlock(&pfbMutex);

	} // prefetchBlocks()

	// returns the # that were cached.
	uint loadBlocks (
		LBID_t *lbids,
		VER_t *vers,
		VER_t txn,
		int compType,
		uint8_t **bufferPtrs,
		uint32_t *rCount,
		bool LBIDTrace,
		uint32_t sessionID,
		uint blockCount,
		bool *blocksWereVersioned,
		bool doPrefetch,
		VSSCache *vssCache)
	{
		blockCacheClient bc(*BRPp[cacheNum(lbids[0])]);
		uint32_t blksRead=0;
		VSSCache::iterator it;
		uint i, ret;
		bool *vbFlags;
		int *vssRCs;
		bool *cacheThisBlock;
		bool *wasCached;

		*blocksWereVersioned = false;

		if (LBIDTrace) {
			for (i = 0; i < blockCount; i++) {
		
#ifdef _MSC_VER
				stats.touchedLBID(lbids[i], GetCurrentThreadId(), sessionID);
#else
				stats.touchedLBID(lbids[i], pthread_self(), sessionID);
#endif
			}
		}

		vbFlags = (bool *) alloca(blockCount);
		vssRCs = (int *) alloca(blockCount * sizeof(int));
		cacheThisBlock = (bool *) alloca(blockCount);
		wasCached = (bool *) alloca(blockCount);
		for (i = 0; i < blockCount; i++) {
			if (vssCache) {
				it = vssCache->find(lbids[i]);
				if (it != vssCache->end()) {
					VSSData &vd = it->second;
					vers[i] = vd.verID;
					vbFlags[i] = vd.vbFlag;
					vssRCs[i] = vd.returnCode;
					if (vssRCs[i] == ERR_SNAPSHOT_TOO_OLD)
						throw runtime_error("Snapshot too old");
				}
			}
			if (!vssCache || it == vssCache->end())
				vssRCs[i] = brm->vssLookup(lbids[i], vers[i], txn, vbFlags[i]);
			*blocksWereVersioned |= vbFlags[i];

			// If the block is being modified by this txn, set the useCache flag to false
			if (txn > 0 && vers[i] == txn && !vbFlags[i])
				cacheThisBlock[i] = false;
			else
				cacheThisBlock[i] = true;
		}

		ret = bc.getCachedBlocks(lbids, vers, bufferPtrs, wasCached, blockCount);
		
		// Do we want to check any VB flags here?  Initial thought: no, because we have
		// no idea whether any other blocks in the prefetch range are versioned,
		// what's the difference if one in the visible range is?
		if (ret != blockCount && doPrefetch) {
			prefetchBlocks(lbids[0], vers[0], 0, compType, &blksRead);
			if (fPMProfOn)
#ifdef _MSC_VER
				pmstats.markEvent(lbids[0], -1, sessionID, 'M');
#else
				pmstats.markEvent(lbids[0], (pthread_t)-1, sessionID, 'M');
#endif

			/* After the prefetch they're all cached if they are in the same range, so
			 * prune the block list and try getCachedBlocks again first, then fall back 
			 * to single-block IO requests if for some reason they aren't. */
			uint l_blockCount = 0;
			for (i = 0; i < blockCount; i++) {
				if (!wasCached[i]) {
					lbids[l_blockCount] = lbids[i];
					vers[l_blockCount] = vers[i];
					bufferPtrs[l_blockCount] = bufferPtrs[i];
					vbFlags[l_blockCount] = vbFlags[i];
					cacheThisBlock[l_blockCount] = cacheThisBlock[i];
					++l_blockCount;
				}
			}
			ret += bc.getCachedBlocks(lbids, vers, bufferPtrs, wasCached, l_blockCount);
			
			if (ret != blockCount)
				for (i = 0; i < l_blockCount; i++)
					if (!wasCached[i]) {
						bool ver;
						bc.getBlock(lbids[i], vers[i], txn, compType, (void *) bufferPtrs[i],
							vbFlags[i], wasCached[i], &ver, cacheThisBlock[i], false);
						*blocksWereVersioned |= ver;
						blksRead++;
					}
		}
		/* Some blocks weren't cached, prefetch is disabled -> issue single-block IO requests,
		 * skip checking the cache again. */
		else if (ret != blockCount) {
			for (i = 0; i < blockCount; i++) {
				if (!wasCached[i]) {
					bool ver;

					bc.getBlock(lbids[i], vers[i], txn, compType, (void *) bufferPtrs[i], vbFlags[i],
						wasCached[i], &ver, cacheThisBlock[i], false);
					*blocksWereVersioned |= ver;
					blksRead++;
				}
			}
		}

		if (rCount)
			*rCount = blksRead;
		//if (*verBlocks)
		//	cout << "loadBlock says there were versioned blocks\n";
		return ret;
	}

	void loadBlock (
		u_int64_t lbid,
		u_int32_t v,
		u_int32_t t,
		int compType,
		void* bufferPtr,
		bool* pWasBlockInCache,
		uint32_t* rCount,
		bool LBIDTrace,
		uint32_t sessionID,
		bool doPrefetch,
		VSSCache *vssCache)
	{
		bool flg = false;
		BRM::OID_t oid;
		BRM::VER_t txn = (BRM::VER_t)t;
		uint16_t dbRoot=0;
		uint32_t partitionNum=0;
		uint16_t segmentNum=0;
		int rc;
		BRM::VER_t ver = (BRM::VER_t)v;
		blockCacheClient bc(*BRPp[cacheNum(lbid)]);
		char file_name[WriteEngine::FILE_NAME_SIZE]={0};
		char* fileNamePtr = file_name;
		uint32_t blksRead=0;
		VSSCache::iterator it;

		if (LBIDTrace)
#ifdef _MSC_VER
			stats.touchedLBID(lbid, GetCurrentThreadId(), sessionID);
#else
			stats.touchedLBID(lbid, pthread_self(), sessionID);
#endif

		if (vssCache) {
			it = vssCache->find(lbid);
			if (it != vssCache->end()) {
				VSSData &vd = it->second;
				ver = vd.verID;
				flg	= vd.vbFlag;
				rc = vd.returnCode;
			}
		}
		if (!vssCache || it == vssCache->end())
			rc = brm->vssLookup((BRM::LBID_t)lbid, ver, txn, flg);
			//cout << "VSS l/u: l=" << lbid << " v=" << ver << " t=" << txn << " flg=" << flg << " rc: " << rc << endl;

		// if this block is locked by this session, don't cache it, just read it directly from disk
		if (txn > 0 && ver == txn && !flg) {
			uint64_t offset;
			uint32_t fbo;
			boost::scoped_array<uint8_t> newBufferSa;
			boost::scoped_array<char> cmpHdrBufSa;
			boost::scoped_array<char> cmpBufSa;
			boost::scoped_array<unsigned char> uCmpBufSa;
			ptrdiff_t alignedBuffer = 0;
			void* readBufferPtr = NULL;
			char* cmpHdrBuf = NULL;
			char* cmpBuf = NULL;
			unsigned char* uCmpBuf = NULL;
			uint64_t cmpBufLen = 0;
			int blockReadRetryCount = 0;
			unsigned idx = 0;
			int pageSize = getpagesize();
#ifdef _MSC_VER
			HANDLE fdh;
#else
			int fd = -1;
#endif

			// need to catch any exceptions, make sure the readLock is released.
			setReadLock();

			try {

				rc = brm->lookupLocal((BRM::LBID_t)lbid,
									ver,
									flg,
									oid,
									dbRoot,
									partitionNum,
									segmentNum,
									fbo);


				// load the block
				buildOidFileName(oid, dbRoot, partitionNum, segmentNum, fileNamePtr);
#ifdef _MSC_VER
				fdh = CreateFile(fileNamePtr, GENERIC_READ, FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE, 0,
					OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL|FILE_FLAG_NO_BUFFERING, 0);
#else
				fd = open(fileNamePtr, O_RDONLY|O_DIRECT|O_LARGEFILE|O_NOATIME);
				if (fd < 0) {
					// try to remount filesystem
					string writePM, localModuleName;
					try_remount(fileNamePtr,
								oid,
								dbRoot,
								partitionNum,
								segmentNum,
								compType,
								fd,
								writePM,
								localModuleName,
								false);
				}
#endif

				//cout << "load VSS block o=" << oid << " l/u: l=" << lbid << " v=" << ver << " t=" << txn << " f=" << flg <<endl;

				// remount didn't solve the problem
#ifdef _MSC_VER
				if (fdh == INVALID_HANDLE_VALUE) {
#else
				if ( fd < 0 ) {
#endif
					SUMMARY_INFO2("open failed: ", fileNamePtr);
					throw std::runtime_error("file open failed");
				}

				//  fd >= 0 must be true, otherwise above exception thrown.
				offset = (uint64_t)fbo * (uint64_t)DATA_BLOCK_SIZE;
				idx = offset / (4 * 1024 * 1024);

				errno = 0;
				rc = 0;
				int i = -1;

#ifdef _MSC_VER
				//needs to be atomic
				//__int64 mrc;
				DWORD bytesRead;
				OVERLAPPED ovl;
#endif

				if (compType == 0) {
#ifdef _MSC_VER
#if 0
					//FIXME: why does ReadFile here always return EINVAL?
					ZeroMemory(&ovl, sizeof(ovl));
					ovl.Offset = static_cast<DWORD>(offset);
					ovl.OffsetHigh = static_cast<DWORD>(offset>>32);
					if (ReadFile(fdh, bufferPtr, 8192, &bytesRead, &ovl))
						i = bytesRead;
					else
						i = -1;
#else
					int xfd=open(fileNamePtr,O_RDONLY|O_BINARY);
					assert(xfd>=0);
					_lseeki64(xfd,offset,SEEK_SET);
					i = _read(xfd,bufferPtr,8192);
					close(xfd);
#endif  // if 0
#else // _MSC_VER
					// linux only for O_DIRECT, using O_DIRECT to be consistent with iomanager.
					newBufferSa.reset(new uint8_t[DATA_BLOCK_SIZE + pageSize]);
					alignedBuffer = (ptrdiff_t) newBufferSa.get();
					if ((alignedBuffer % pageSize) != 0) {
						alignedBuffer &= ~((ptrdiff_t)pageSize - 1);
						alignedBuffer += pageSize;
					}
					readBufferPtr = (void*) alignedBuffer;
					i = pread(fd, readBufferPtr, DATA_BLOCK_SIZE, offset);
					memcpy(bufferPtr, readBufferPtr, i);
#ifdef IDB_COMP_POC_DEBUG
{
boost::mutex::scoped_lock lk(primitiveprocessor::compDebugMutex);
cout << "pread2(" << fd << ", 0x" << hex << (ptrdiff_t)readBufferPtr << dec << ", " << DATA_BLOCK_SIZE << ", " << offset << ") = " << i << endl;
}
#endif // IDB_COMP_POC_DEBUG
#endif // _MSC_VER
				}  // if (compType == 0)

				else {  // if (compType != 0)

#ifdef _MSC_VER
// retry if file is out of sync -- compressed column file only.
blockReadRetry:
					uCmpBufSa.reset(new unsigned char[4 * 1024 * 1024 + 4]);
					uCmpBuf = uCmpBufSa.get();
					cmpHdrBufSa.reset(new char[4096 * 3]);
					cmpHdrBuf = cmpHdrBufSa.get();
					CompChunkPtrList ptrList;
					ZeroMemory(&ovl, sizeof(ovl));
					if (ReadFile(fdh, &cmpHdrBuf[0], 4096*3, &bytesRead, &ovl))
						i = bytesRead;
					else
						i = -1;
					IDBCompressInterface decompressor;

					uint64_t numHdrs = 0;
					int dcrc = decompressor.getPtrList(&cmpHdrBuf[4096], 4096, ptrList);
					if (dcrc == 0 && ptrList.size() > 0)
						numHdrs = ptrList[0].first / 4096ULL - 2ULL;

					if (numHdrs > 0) {
						boost::scoped_array<char> nextHdrBufsa(new char[numHdrs * 4096]);
						char* nextHdrBufPtr = nextHdrBufsa.get();
						ZeroMemory(&ovl, sizeof(ovl));
						ovl.Offset = static_cast<DWORD>(4096*2);
						if (ReadFile(fdh, &nextHdrBufPtr[0], numHdrs*4096, &bytesRead, &ovl))
							i = bytesRead;
						else
							i = -1;
						CompChunkPtrList nextPtrList;
						dcrc = decompressor.getPtrList(&nextHdrBufPtr[0], numHdrs * 4096, nextPtrList);
						if (dcrc == 0)
							ptrList.insert(ptrList.end(), nextPtrList.begin(), nextPtrList.end());
					}

					if (i < 0 || dcrc != 0 || idx >= (signed)ptrList.size()) {
						// Due to race condition, the header on disk may not upated yet.
						// Log an info message and retry.
						if (blockReadRetryCount == 0) {
							logging::Message::Args args;
							args.add(oid);
							ostringstream infoMsg;
							infoMsg << "retry read from " << fileNamePtr << ". dcrc=" << dcrc
									<< ", idx=" << idx << ", ptr.size=" << ptrList.size();
							args.add(infoMsg.str());
							mlp->logInfoMessage(logging::M0061, args);
						}

						if (++blockReadRetryCount < 30) {
							waitForRetry(blockReadRetryCount);
							goto blockReadRetry;
						}
						else {
							rc = -1001;
						}
					}

					if (rc == 0) {
						unsigned cmpBlkOff = offset % (4 * 1024 * 1024);
						uint64_t cmpBufOff = ptrList[idx].first;
						uint64_t cmpBufSz = ptrList[idx].second;
						if (cmpBufSa.get() == NULL || cmpBufLen < cmpBufSz) {
							cmpBufSa.reset(new char[cmpBufSz]);
							cmpBufLen = cmpBufSz;
							cmpBuf = cmpBufSa.get();
						}
						unsigned blen = 4 * 1024 * 1024;
						ZeroMemory(&ovl, sizeof(ovl));
						ovl.Offset = static_cast<DWORD>(cmpBufOff);
						ovl.OffsetHigh = static_cast<DWORD>(cmpBufOff>>32);
						if (ReadFile(fdh, cmpBuf, cmpBufSz, &bytesRead, &ovl))
							i = bytesRead;
						else
							i = -1;

						dcrc = decompressor.uncompressBlock(cmpBuf, cmpBufSz, uCmpBuf, blen);
						if (dcrc == 0) {
							memcpy(bufferPtr, &uCmpBuf[cmpBlkOff], DATA_BLOCK_SIZE);
						}
						else {
							// Due to race condition, the header on disk may not upated yet.
							// Log an info message and retry.
							if (blockReadRetryCount == 0) {
								logging::Message::Args args;
								args.add(oid);
								ostringstream infoMsg;
								infoMsg << "retry read from " << fileNamePtr << ". dcrc=" << dcrc
										<< ", idx=" << idx << ", ptr.size=" << ptrList.size();
								args.add(infoMsg.str());
								mlp->logInfoMessage(logging::M0061, args);
							}

							if (++blockReadRetryCount < 30) {
								waitForRetry(blockReadRetryCount);
								goto blockReadRetry;
							}
							else {
								rc = -1002;
							}
						}
					}
				}
#else
// retry if file is out of sync -- compressed column file only.
blockReadRetry:

					uCmpBufSa.reset(new unsigned char[4 * 1024 * 1024 + 4]);
					uCmpBuf = uCmpBufSa.get();
					cmpHdrBufSa.reset(new char[4096 * 3 + pageSize]);
					alignedBuffer = (ptrdiff_t) cmpHdrBufSa.get();
					if ((alignedBuffer % pageSize) != 0) {
						alignedBuffer &= ~((ptrdiff_t)pageSize - 1);
						alignedBuffer += pageSize;
					}
					cmpHdrBuf = (char*) alignedBuffer;

					i = pread(fd, &cmpHdrBuf[0], 4096 * 3, 0);

					CompChunkPtrList ptrList;
					IDBCompressInterface decompressor;
					int dcrc = 0;
					if (i == 4096 * 3) {
						uint64_t numHdrs = 0; // extra headers
						dcrc = decompressor.getPtrList(&cmpHdrBuf[4096], 4096, ptrList);
						if (dcrc == 0 && ptrList.size() > 0)
							numHdrs = ptrList[0].first / 4096ULL - 2ULL;

						if (numHdrs > 0) {
							boost::scoped_array<char> nextHdrBufsa(new char[numHdrs * 4096 + pageSize]);
							alignedBuffer = (ptrdiff_t) nextHdrBufsa.get();
							if ((alignedBuffer % pageSize) != 0) {
								alignedBuffer &= ~((ptrdiff_t)pageSize - 1);
								alignedBuffer += pageSize;
							}
							char* nextHdrBufPtr = (char*) alignedBuffer;

							i = pread(fd, &nextHdrBufPtr[0], numHdrs * 4096, 4096 * 2);

							CompChunkPtrList nextPtrList;
							dcrc = decompressor.getPtrList(&nextHdrBufPtr[0], numHdrs * 4096, nextPtrList);
							if (dcrc == 0)
								ptrList.insert(ptrList.end(), nextPtrList.begin(), nextPtrList.end());
						}
					}

					if (dcrc != 0 || idx >= ptrList.size()) {
						// Due to race condition, the header on disk may not upated yet.
						// Log an info message and retry.
						if (blockReadRetryCount == 0) {
							logging::Message::Args args;
							args.add(oid);
							ostringstream infoMsg;
							infoMsg << "retry read from " << fileNamePtr << ". dcrc=" << dcrc
									<< ", idx=" << idx << ", ptr.size=" << ptrList.size();
							args.add(infoMsg.str());
							mlp->logInfoMessage(logging::M0061, args);
						}

						if (++blockReadRetryCount < 30) {
							if (blockReadRetryCount == 5) {
								close(fd);
								fd = -1;

								string writePM, localModuleName;
								try_remount(fileNamePtr, oid, dbRoot, partitionNum, segmentNum,
									compType, fd, writePM, localModuleName, false);

								if (fd < 0)
									rc = -1003; // not valid fd
								else
									goto blockReadRetry;
							}
							else {
								waitForRetry(blockReadRetryCount);
								goto blockReadRetry;
							}
						}
						else {
							rc = -1004;
						}
					}

					if (rc == 0) {
						unsigned cmpBlkOff = offset % (4 * 1024 * 1024);
						uint64_t cmpBufOff = ptrList[idx].first;
						uint64_t cmpBufSz = ptrList[idx].second;
						if (cmpBufSa.get() == NULL || cmpBufLen < cmpBufSz) {
							cmpBufSa.reset(new char[cmpBufSz + pageSize]);
							cmpBufLen = cmpBufSz;
							alignedBuffer = (ptrdiff_t) cmpBufSa.get();
							if ((alignedBuffer % pageSize) != 0) {
								alignedBuffer &= ~((ptrdiff_t)pageSize - 1);
								alignedBuffer += pageSize;
							}
							cmpBuf = (char*) alignedBuffer;
						}
						unsigned blen = 4 * 1024 * 1024;

						i = pread(fd, cmpBuf, cmpBufSz, cmpBufOff);

						dcrc = decompressor.uncompressBlock(cmpBuf, cmpBufSz, uCmpBuf, blen);
						if (dcrc == 0) {
							memcpy(bufferPtr, &uCmpBuf[cmpBlkOff], DATA_BLOCK_SIZE);
						}
						else {
							// Due to race condition, the header on disk may not upated yet.
							// Log an info message and retry.
							if (blockReadRetryCount == 0) {
								logging::Message::Args args;
								args.add(oid);
								ostringstream infoMsg;
								infoMsg << "retry read from " << fileNamePtr << ". dcrc=" << dcrc
										<< ", idx=" << idx << ", ptr.size=" << ptrList.size();
								args.add(infoMsg.str());
								mlp->logInfoMessage(logging::M0061, args);
							}

							if (++blockReadRetryCount < 30) {
								if (blockReadRetryCount == 5) {
									close(fd);
									fd = -1;

									string writePM, localModuleName;
									try_remount(fileNamePtr, oid, dbRoot, partitionNum, segmentNum,
										compType, fd, writePM, localModuleName, false);

									if (fd < 0)
										rc = -1005; // not valid fd
									else
										goto blockReadRetry;
								}
								else {
									waitForRetry(blockReadRetryCount);
									goto blockReadRetry;
								}
							}
							else {
								rc = -1006;
							}
						}
					}
				}
#endif

				if ( rc < 0 ) {
					string msg("pread failed");
					ostringstream infoMsg;
					infoMsg <<
#ifndef _MSC_VER
						" (fd:" << fd <<
#endif
						" rc:" << rc << ")"; 
					msg = msg + ", error:" + strerror(errno) + infoMsg.str();
					SUMMARY_INFO(msg);
					//FIXME: free-up allocated memory!
					throw std::runtime_error(msg);
				}
			}
			catch (...) {
#ifdef _MSC_VER
				CloseHandle(fdh);
#else
				close(fd);
#endif

				releaseReadLock();

				throw;
			}

#ifdef _MSC_VER
			CloseHandle(fdh);
#else
			close(fd);
#endif

			releaseReadLock();

			// log the retries
			if (blockReadRetryCount > 0) {
				logging::Message::Args args;
				args.add(oid);
				ostringstream infoMsg;
				infoMsg << "Successfully uncompress " << fileNamePtr << " chunk "
						<< idx << " @" << " blockReadRetry:" << blockReadRetryCount;
				args.add(infoMsg.str());
				mlp->logInfoMessage(logging::M0006, args);
			}

			return;
		}

		FileBuffer* fbPtr=0;
		bool wasBlockInCache=false;
		
		fbPtr = bc.getBlockPtr(lbid, ver, flg);
		if (fbPtr) {
			memcpy(bufferPtr, fbPtr->getData(), BLOCK_SIZE);
			wasBlockInCache = true;
		}

		if (doPrefetch && !wasBlockInCache && !flg) {
			prefetchBlocks(lbid, ver, txn, compType, &blksRead);

			if (fPMProfOn)
#ifdef _MSC_VER
				pmstats.markEvent(lbid, -1, sessionID, 'M');
#else
				pmstats.markEvent(lbid, (pthread_t)-1, sessionID, 'M');
#endif
				bc.getBlock(lbid, ver, txn, compType, (uint8_t *) bufferPtr, flg, wasBlockInCache);
				if (!wasBlockInCache)
					blksRead++;
		}
		else if (!wasBlockInCache) {
			bc.getBlock(lbid, ver, txn, compType, (uint8_t *) bufferPtr, flg, wasBlockInCache);
			if (!wasBlockInCache)
				blksRead++;
		}

		if (pWasBlockInCache)
			*pWasBlockInCache = wasBlockInCache;
		if (rCount)
			*rCount = blksRead;

	}

	struct AsynchLoader {
		AsynchLoader(uint64_t l, 
					uint32_t v, 
					uint32_t t, 
					int ct, 
					uint32_t *cCount,
					uint32_t *rCount, 
					bool trace, 
					uint32_t sesID, 
					boost::mutex *m,
					uint *loaderCount,
					VSSCache *vCache) :
								lbid(l), 
								ver(v), 
								txn(t), 
								compType(ct), 
								LBIDTrace(trace), 
								sessionID(sesID),
								cacheCount(cCount), 
								readCount(rCount), 
								busyLoaders(loaderCount), 
								mutex(m),
								vssCache(vCache)
		{ }

		void operator()()
		{
			bool cached=false;
			uint32_t rCount=0;
			char buf[BLOCK_SIZE];

 			//cout << "asynch started " << pthread_self() << " l: " << lbid << endl;
			try {
				loadBlock(lbid, ver, txn, compType, buf, &cached, &rCount, LBIDTrace, sessionID, true, vssCache);
			}
			catch (std::exception& ex) {
				cerr << "AsynchLoader caught loadBlock exception: " << ex.what() << endl;
				assert(asyncCounter > 0);
#ifdef _MSC_VER
				InterlockedDecrement(&asyncCounter);
#else
				__sync_add_and_fetch(&asyncCounter, -1);
#endif
				mutex->lock(); //pthread_mutex_lock(mutex);
				--(*busyLoaders);
				mutex->unlock(); //pthread_mutex_unlock(mutex);
				logging::Message::Args args;
				args.add(string("PrimProc AsyncLoader caught error: "));
				args.add(ex.what());
				primitiveprocessor::mlp->logMessage(logging::M0000, args, false);
				return;
			}
			catch (...) {
				cerr << "AsynchLoader caught unknown exception: " << endl;
				//FIXME Use a locked processor primitive?
				assert(asyncCounter > 0);
#ifdef _MSC_VER
				InterlockedDecrement(&asyncCounter);
#else
				__sync_add_and_fetch(&asyncCounter, -1);
#endif
				mutex->lock(); //pthread_mutex_lock(mutex);
				--(*busyLoaders);
				mutex->unlock(); //pthread_mutex_unlock(mutex);
				logging::Message::Args args;
				args.add(string("PrimProc AsyncLoader caught unknown error"));
				primitiveprocessor::mlp->logMessage(logging::M0000, args, false);
				return;
			}
			assert(asyncCounter > 0);
#ifdef _MSC_VER
			InterlockedDecrement(&asyncCounter);
#else
			__sync_add_and_fetch(&asyncCounter, -1);
#endif
			mutex->lock(); //pthread_mutex_lock(mutex);
			if (cached)
				(*cacheCount)++;
			*readCount += rCount;
			--(*busyLoaders);
			mutex->unlock(); //pthread_mutex_unlock(mutex);
// 			cerr << "done\n";
		}

		private:
			uint64_t lbid;
			uint32_t ver;
			uint32_t txn;
			int compType;
			uint8_t dataWidth;
			bool LBIDTrace;
			uint32_t sessionID;
			uint32_t *cacheCount;
			uint32_t *readCount;
			uint *busyLoaders;
			boost::mutex *mutex;
			VSSCache *vssCache;
	};

	void loadBlockAsync(uint64_t lbid, 
						uint32_t v, 
						uint32_t txn,
						int compType,
						uint32_t *cCount, 
						uint32_t *rCount, 
						bool LBIDTrace, 
						uint32_t sessionID,
						boost::mutex *m,
						uint *busyLoaders,
						VSSCache *vssCache)
	{
		blockCacheClient bc(*BRPp[cacheNum(lbid)]);
		bool vbFlag;
		int vssret;
		BRM::VER_t ver = (BRM::VER_t) v;
		VSSCache::iterator it;
	
		if (vssCache) {
			it = vssCache->find(lbid);
			if (it != vssCache->end()) {
				//cout << "async: vss cache hit on " << lbid << endl;
				VSSData &vd = it->second;
				vssret = vd.returnCode;
				ver = vd.verID;
				vbFlag = vd.vbFlag;
			}
		}
		if (!vssCache || it == vssCache->end())
			vssret = brm->vssLookup((BRM::LBID_t) lbid, ver, txn, vbFlag);
		
		if (bc.exists(lbid, ver))
			return;

		/* a quick and easy stand-in for a threadpool for loaders */
#ifdef _MSC_VER
		if (_InterlockedOr(&asyncCounter, 0) >= asyncMax)
			return;

		InterlockedIncrement(&asyncCounter);
#else
		if (__sync_or_and_fetch(&asyncCounter, 0) >= asyncMax)
			return;

		__sync_add_and_fetch(&asyncCounter, 1);
#endif
		m->lock(); //pthread_mutex_lock(m);
		try {
			boost::thread thd(AsynchLoader(lbid, ver, txn, compType, cCount, rCount,
				LBIDTrace, sessionID, m, busyLoaders, vssCache));
			(*busyLoaders)++;
		}
		catch (boost::thread_resource_error &e) {
			cerr << "AsynchLoader: caught a thread resource error, need to lower asyncMax\n";
			assert(asyncCounter > 0);
#ifdef _MSC_VER
			InterlockedDecrement(&asyncCounter);
#else
			__sync_add_and_fetch(&asyncCounter, -1);
#endif
		}
		m->unlock(); //pthread_mutex_unlock(m);
	}

} //namespace primitiveprocessor

//#define DCT_DEBUG 1
#define SETUP_GUARD \
{ \
	unsigned char* o = outputp.get(); \
	memset(o, 0xa5, ouput_buf_size*3); \
}
#undef SETUP_GUARD
#define SETUP_GUARD
#define CHECK_GUARD(lbid) \
{ \
	unsigned char* o = outputp.get(); \
	for (int i = 0; i < ouput_buf_size; i++) \
	{ \
		if (*o++ != 0xa5) \
		{ \
			cerr << "Buffer underrun on LBID " << lbid << endl; \
			assert(0); \
		} \
	} \
	o += ouput_buf_size; \
	for (int i = 0; i < ouput_buf_size; i++) \
	{ \
		if (*o++ != 0xa5) \
		{ \
			cerr << "Buffer overrun on LBID " << lbid << endl; \
			assert(0); \
		} \
	} \
}
#undef CHECK_GUARD
#define CHECK_GUARD(x)

namespace
{
using namespace primitiveprocessor;

void pause_(unsigned delay)
{
	struct timespec req;
	struct timespec rem;

	req.tv_sec = delay;
	req.tv_nsec = 0;

	rem.tv_sec = 0;
	rem.tv_nsec = 0;
#ifdef _MSC_VER
	Sleep(req.tv_sec * 1000);
#else
again:
	if (nanosleep(&req, &rem) != 0) {
		if (rem.tv_sec > 0 || rem.tv_nsec > 0) {
			req = rem;
			goto again;
		}
	}
#endif
}

/** @brief The job type to process a dictionary scan (pDictionaryScan class on the UM)
 * TODO: Move this & the impl into different files
 */
class DictScanJob
{
public:
    DictScanJob(SP_UM_IOSOCK ios, SBS bs, SP_UM_MUTEX writeLock);
    virtual ~DictScanJob();

    void write(const ByteStream &);
    int operator()();
    void catchHandler(const std::string& ex, uint32_t id, uint16_t code = logging::primitiveServerErr);
    void sendErrorMsg(uint32_t id, uint16_t code);

private:
    SP_UM_IOSOCK fIos;
    SBS fByteStream;
    SP_UM_MUTEX fWriteLock;
};

DictScanJob::DictScanJob(SP_UM_IOSOCK ios, SBS bs, SP_UM_MUTEX writeLock) :
        fIos(ios), fByteStream(bs), fWriteLock(writeLock)
{
}

DictScanJob::~DictScanJob()
{
}

void DictScanJob::write(const ByteStream& bs)
{
    mutex::scoped_lock lk(*fWriteLock);
    fIos->write(bs);
}

int DictScanJob::operator()()
{
    uint8_t data[DATA_BLOCK_SIZE];
    uint output_buf_size = MAX_BUFFER_SIZE;
    uint32_t session;
    uint32_t uniqueId;
    bool wasBlockInCache;
    uint32_t blocksRead = 0;
    uint16_t runCount;

    map<uint32_t, shared_ptr<DictEqualityFilter> >::iterator eqFilter;
    ByteStream results(output_buf_size);
    TokenByScanRequestHeader *cmd = (TokenByScanRequestHeader *) fByteStream->buf();
    PrimitiveProcessor pproc(gDebugLevel);
    TokenByScanResultHeader *output;

    try {
#ifdef DCT_DEBUG
        DebugLevel oldDebugLevel = gDebugLevel;
        gDebugLevel = VERBOSE;
#endif
        session = cmd->Hdr.SessionID;
        uniqueId = cmd->Hdr.UniqueID;
        runCount = cmd->Count;
        output = (TokenByScanResultHeader *) results.getInputPtr();
        eqFilter = dictEqualityFilters.end();

        /* Grab the equality filter if one is specified */
        if (cmd->flags & HAS_EQ_FILTER) {
            while (eqFilter == dictEqualityFilters.end()) {
                mutex::scoped_lock sl(eqFilterMutex);
                eqFilter = dictEqualityFilters.find(uniqueId);
                sl.unlock();
                if (eqFilter == dictEqualityFilters.end())
                    usleep(10000);    // it's still being built, wait for it
            }
        }

        for (uint16_t i = 0; i < runCount; ++i) {
            loadBlock(cmd->LBID,
                cmd->Hdr.VerID,
                cmd->Hdr.TransactionID,
                cmd->CompType,
                data,
                &wasBlockInCache,
                &blocksRead,
                fLBIDTraceOn,
                session);
           pproc.setBlockPtr((int*) data);

            boost::shared_ptr<DictEqualityFilter> defp;
            if (eqFilter != dictEqualityFilters.end())
                defp = eqFilter->second;  // doing this assignment is more portable
            pproc.p_TokenByScan(cmd, output, output_buf_size, utf8, defp);

            if (wasBlockInCache)
                output->CacheIO++;
            else
                output->PhysicalIO += blocksRead;
            results.advanceInputPtr(output->NBYTES);
            write(results);
            results.restart();
            cmd->LBID++;
        }
#ifdef DCT_DEBUG
        gDebugLevel = oldDebugLevel;
#endif
    } catch (logging::IDBExcept& iex) {
        cerr << "DictScanJob caught an IDBException: " << iex.what() << endl;
        catchHandler(iex.what(), uniqueId, iex.errorCode());
    } catch(std::exception& re) {
        cerr << "DictScanJob caught an exception: " << re.what() << endl;
        catchHandler(re.what(), uniqueId);
    } catch(...) {
        string msg("Unknown exception caught in DictScanJob.");
        cerr << msg << endl;
        catchHandler(msg, uniqueId);
    }
    return 0;
}

void DictScanJob::catchHandler(const string& ex, uint32_t id, uint16_t code)
{
    Logger log;
    log.logMessage(ex);
    sendErrorMsg(id, code);
}

void DictScanJob::sendErrorMsg(uint32_t id, uint16_t code)
{
    ISMPacketHeader ism;
    PrimitiveHeader ph;
    ism.Status =  code;
    ph.UniqueID = id;

    ByteStream msg(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
    msg.append((uint8_t *) &ism, sizeof(ism));
    msg.append((uint8_t *) &ph, sizeof(ph));

    write(msg);
}


struct BPPHandler
{
	BPPHandler(PrimitiveServer* ps) : fPrimitiveServerPtr(ps){ }

	struct LastJoinerRunner {
		BPPHandler *rt;
		SBS bs;
		LastJoinerRunner(BPPHandler *r, SBS b) : rt(r), bs(b) { };
		void operator()() { rt->lastJoinerMsg(*bs); };
	};

	void doAbort(ByteStream &bs)
	{
		uint32_t key;
		BPPMap::iterator it;

		bs.advance(sizeof(ISMPacketHeader));
		bs >> key;
		mutex::scoped_lock scoped(bppLock);
		it = bppMap.find(key);
		scoped.unlock();
		if (it != bppMap.end())
			it->second->abort();
		fPrimitiveServerPtr->getProcessorThreadPool()->removeJobs(key);
	}

	void doAck(ByteStream &bs)
	{
		 uint32_t key;
		 int16_t msgCount;
		 BPPMap::iterator it;
		 const ISMPacketHeader *ism = (const ISMPacketHeader *) bs.buf();
		 
		 key = *((uint32_t *) &ism->Reserve);
		 msgCount = (int16_t) ism->Size;
		 bs.advance(sizeof(ISMPacketHeader));
		 
		 mutex::scoped_lock scoped(bppLock);
		 it = bppMap.find(key);
		 scoped.unlock();
		 if (it != bppMap.end())
			it->second->getSendThread()->sendMore(msgCount);
	}
		
	void createBPP(ByteStream &bs)
	{
		uint i;
		uint32_t key, initMsgsLeft;
		SBPP bpp;
		SBPPV bppv;

		// make the new BPP object
		bppv.reset(new BPPV());
		bpp.reset(new BatchPrimitiveProcessor(bs, fPrimitiveServerPtr->prefetchThreshold(), 
			bppv->getSendThread()));
		bs >> initMsgsLeft;
		assert(bs.length() == 0);
		bppv->getSendThread()->sendMore(initMsgsLeft);
		bppv->add(bpp);
		for (i = 1; i < BPPCount; i++) {
			SBPP dup = bpp->duplicate();

			/* Uncomment these lines to verify duplicate().  == op might need updating */
//  				if (*bpp != *dup)
//  					cerr << "createBPP: duplicate mismatch at index " << i << endl;
//  				assert(*bpp == *dup);
			bppv->add(dup);
		}

		key = bpp->getUniqueID();
		//cout << "creating BPP key = " << key << endl;
		mutex::scoped_lock scoped(bppLock);
		bool newInsert;
		newInsert = bppMap.insert(pair<uint32_t, SBPPV>(key, bppv)).second;
		scoped.unlock();
		if (!newInsert) {
			if (bpp->getSessionID() & 0x80000000)
				cerr << "warning: createBPP() tried to clobber a BPP with duplicate sessionID & stepID. sessionID=" << 
					(int) (bpp->getSessionID() ^ 0x80000000) << " stepID=" <<
					bpp->getStepID()<< " (syscat)" << endl;
			else
				cerr << "warning: createBPP() tried to clobber a BPP with duplicate sessionID & stepID.  sessionID=" <<
					bpp->getSessionID() << " stepID=" << bpp->getStepID()<< endl;
		}
	}

	SBPPV grabBPPs(uint32_t uniqueID, bool fatal=true)
	{
		BPPMap::iterator it;
		uint failCount = 0;
		uint maxFailCount = (fatal ? 500 : 5000);
		SBPPV ret;

		mutex::scoped_lock scoped(bppLock);
		it = bppMap.find(uniqueID);
		if (it != bppMap.end())
		{
			ret = it->second;
			return ret;
		}

		do
		{
			if (++failCount == maxFailCount) {
				//cout << "grabBPPs couldn't find the BPPs for " << uniqueID << endl;
				return ret;
				//throw logic_error("grabBPPs couldn't find the unique ID");
			}
			scoped.unlock();
			usleep(5000);
			scoped.lock();
			it = bppMap.find(uniqueID);
		} while (it == bppMap.end());

		ret = it->second;
		return ret;
	}

	void addJoinerToBPP(ByteStream &bs)
	{
		SBPPV bppv;
		uint32_t uniqueID;
		const uint8_t *buf;

		/* call addToJoiner() on the first BPP */
		
		buf = bs.buf();
		/* the uniqueID is after the ISMPacketHeader, sessionID, and stepID */
		uniqueID = *((const uint32_t *) &buf[sizeof(ISMPacketHeader) + 2*sizeof(uint32_t)]);
		bppv = grabBPPs(uniqueID);
		if (bppv) {
			mutex::scoped_lock lk(djLock);
			bppv->get()[0]->addToJoiner(bs);
		}
	}

	void lastJoinerMsg(ByteStream &bs)
	{
		SBPPV bppv;
		uint32_t uniqueID, i;
		const uint8_t *buf;

		/* call endOfJoiner() on the every BPP */

		buf = bs.buf();
		/* the uniqueID is after the ISMPacketHeader, sessionID, and stepID */
		uniqueID = *((const uint32_t *) &buf[sizeof(ISMPacketHeader) + 2*sizeof(uint32_t)]);
		
		bppv = grabBPPs(uniqueID, false);
		if (!bppv)   // they've already been destroyed
			return;
		mutex::scoped_lock lk(djLock);
		for (i = 0; i < bppv->get().size(); i++)
			bppv->get()[i]->endOfJoiner();
	}

	void destroyBPP(ByteStream &bs)
	{
		uint32_t uniqueID, sessionID, stepID;

		bs.advance(sizeof(ISMPacketHeader));
		bs >> sessionID;
		bs >> stepID;
		bs >> uniqueID;
		//cerr << "destroyBPP: unique is " << uniqueID << endl;
		mutex::scoped_lock lk(djLock);
		mutex::scoped_lock scoped(bppLock);
//		cout << "destroying BPP # " << uniqueID << endl;
		bppMap.erase(uniqueID);
// 			cout << "  destroy: new size is " << bppMap.size() << endl;
/*
		if (sessionID & 0x80000000)
			cerr << "destroyed BPP instances for sessionID " << (int)
			(sessionID ^ 0x80000000) << " stepID "<< stepID << " (syscat)\n";
		else
			cerr << "destroyed BPP instances for sessionID " << sessionID << 
			" stepID "<< stepID << endl;
*/
		scoped.unlock();
		fPrimitiveServerPtr->getProcessorThreadPool()->removeJobs(uniqueID);
	}

	void setBPPToError(uint32_t uniqueID, const string& error, logging::ErrorCodeValues errorCode)
	{
		SBPPV bppv;

		bppv = grabBPPs(uniqueID);
		if (!bppv)
			return;
		for (size_t i = 0; i < bppv->get().size(); i++)
			bppv->get()[i]->setError(error, errorCode);
		if (bppv->get().empty() && !bppMap.empty() )
			bppMap.begin()->second.get()->get()[0]->setError(error, errorCode);
	}

	PrimitiveServer* fPrimitiveServerPtr;
};

struct ReadThread
{
	ReadThread(const string& serverName, IOSocket& ios, PrimitiveServer* ps) :
		fServerName(serverName), fIos(ios), fPrimitiveServerPtr(ps), fBPPHandler(ps)
	{
	}

	struct LastJoinerRunner {
		BPPHandler* rt;
		SBS bs;
		LastJoinerRunner(BPPHandler *r, SBS b) : rt(r), bs(b) { };
		void operator()() { rt->lastJoinerMsg(*bs); };
	};

	const ByteStream buildCacheOpResp(int32_t result)
	{
		const int msgsize = sizeof(ISMPacketHeader) + sizeof(int32_t);
		ByteStream::byte msgbuf[msgsize];
		memset(msgbuf, 0, sizeof(ISMPacketHeader));
		ISMPacketHeader* hdrp = reinterpret_cast<ISMPacketHeader*>(&msgbuf[0]);
		hdrp->Command = CACHE_OP_RESULTS;
		int32_t* resp = reinterpret_cast<int32_t*>(&msgbuf[sizeof(ISMPacketHeader)]);
		*resp = result;
		return ByteStream(msgbuf, msgsize);
	}

	/* Message format:
	 * 	ISMPacketHeader
	 * 	OID count - 32 bits
	 *  OID array - 32 bits * count
	 */
	void doCacheFlushByOID(SP_UM_IOSOCK ios, ByteStream &bs)
	{
		uint8_t *buf = bs.buf();
		buf += sizeof(ISMPacketHeader);
		uint32_t count = *((uint32_t *) buf); buf += 4;
		uint32_t *oids = (uint32_t *) buf;

		for (int i = 0; i < fCacheCount; i++) {
			blockCacheClient bc(*BRPp[i]);
			bc.flushOIDs(oids, count);
		}
		ios->write(buildCacheOpResp(0));
	}

	/* Message format:
		 * 	ISMPacketHeader
		 * 	Partition number - 32 bits
		 * 	OID count - 32 bits
		 *  OID array - 32 bits * count
	*/
	void doCacheFlushByPartition(SP_UM_IOSOCK ios, ByteStream &bs)
	{
		uint8_t *buf = bs.buf();
		buf += sizeof(ISMPacketHeader);
		uint32_t partitionNum = *((uint32_t *) buf); buf += 4;
		uint32_t count = *((uint32_t *) buf); buf += 4;
		uint32_t *oids = (uint32_t *) buf;

		for (int i = 0; i < fCacheCount; i++) {
			blockCacheClient bc(*BRPp[i]);
			bc.flushPartition(oids, count, partitionNum);
		}
		ios->write(buildCacheOpResp(0));
	}

	void doCacheFlushCmd(SP_UM_IOSOCK ios, const ByteStream& bs)
	{
		for (int i = 0; i < fCacheCount; i++)
		{
			blockCacheClient bc(*BRPp[i]);
			bc.flushCache();
		}

		ios->write(buildCacheOpResp(0));
	}

	void doCacheDropFDs(SP_UM_IOSOCK ios)
	{
		dropFDCache();
		ios->write(buildCacheOpResp(0));
	}

	//N.B. this fcn doesn't actually clean the VSS, but rather instructs PP to flush its
	//   cache of specific LBID's
	void doCacheCleanVSSCmd(SP_UM_IOSOCK ios, const ByteStream& bs)
	{
		const ByteStream::byte* bytePtr = bs.buf();
		const uint32_t* cntp = reinterpret_cast<const uint32_t*>(&bytePtr[sizeof(ISMPacketHeader)]);
		const LbidAtVer* itemp =
			reinterpret_cast<const LbidAtVer*>(&bytePtr[sizeof(ISMPacketHeader) + sizeof(uint32_t)]);
		for (int i = 0; i < fCacheCount; i++)
		{
			blockCacheClient bc(*BRPp[i]);
			bc.flushMany(itemp, *cntp);
		}
		ios->write(buildCacheOpResp(0));
	}

	void doCacheFlushAllversion(SP_UM_IOSOCK ios, const ByteStream& bs)
	{
		const ByteStream::byte* bytePtr = bs.buf();
		const uint32_t* cntp = reinterpret_cast<const uint32_t*>(&bytePtr[sizeof(ISMPacketHeader)]);
		const LbidAtVer* itemp =
			reinterpret_cast<const LbidAtVer*>(&bytePtr[sizeof(ISMPacketHeader) + sizeof(uint32_t)]);
		for (int i = 0; i < fCacheCount; i++)
		{
			blockCacheClient bc(*BRPp[i]);
			bc.flushManyAllversion(itemp, *cntp);
		}
		ios->write(buildCacheOpResp(0));
	}
	
	void createEqualityFilter(SBS bs)
	{
		uint32_t uniqueID, count, i;
		string str;
		boost::shared_ptr<DictEqualityFilter> filter(new DictEqualityFilter());

		bs->advance(sizeof(ISMPacketHeader));
		*bs >> uniqueID;
		*bs >> count;
		for (i = 0; i < count; i++) {
			*bs >> str;
			filter->insert(str);
		}

		mutex::scoped_lock sl(eqFilterMutex);
		dictEqualityFilters[uniqueID] = filter;
	}

	void destroyEqualityFilter(SBS bs)
	{
		mutex::scoped_lock sl(eqFilterMutex);
		uint32_t uniqueID;

		bs->advance(sizeof(ISMPacketHeader));
		*bs >> uniqueID;
		dictEqualityFilters.erase(uniqueID);
	}

	void operator()()
	{
		threadpool::WeightedThreadPool* procPoolPtr = fPrimitiveServerPtr->getProcessorThreadPool();
		SBS bs;
		UmSocketSelector* pUmSocketSelector = UmSocketSelector::instance();

		// Establish default output IOSocket (and mutex) based on the input
		// IOSocket. If we end up rotating through multiple output sockets
		// for the same UM, we will use UmSocketSelector to select output.
		SP_UM_IOSOCK outIosDefault(new IOSocket(fIos));
		SP_UM_MUTEX  writeLockDefault(new mutex());

		bool bRotateDest = fPrimitiveServerPtr->rotatingDestination();
		if (bRotateDest) {
			// If we tried adding an IP address not listed as UM in config
			// file; probably a DMLProc connection.  We allow the connection
			// but disable destination rotation since not in Calpont.xml.
			if (!pUmSocketSelector->addConnection(outIosDefault, writeLockDefault)) {
				bRotateDest = false;
			}
		}

		SP_UM_IOSOCK outIos(outIosDefault);
		SP_UM_MUTEX  writeLock(writeLockDefault);

		//..Loop to process incoming messages on IOSocket fIos
		for (;;) {
			try {
				bs = fIos.read();
			} catch (...) {
				//This connection is dead, nothing useful will come from it ever again
				//We can't rely on the state of bs at this point...
				if (bRotateDest && pUmSocketSelector)
					pUmSocketSelector->delConnection(fIos);
				fIos.close();
				break;
			}

			try {	
			if (bs->length() != 0) {
				assert(bs->length() >= sizeof(ISMPacketHeader));

				// get step type from bs and send appropriate weight for step type

				const ByteStream::byte* bytePtr = bs->buf();

				/* TODO: add bounds checking */

				const ISMPacketHeader* ismHdr = reinterpret_cast<const ISMPacketHeader*>(bytePtr);

				switch(ismHdr->Command)
				{
				case CACHE_FLUSH_PARTITION:
					doCacheFlushByPartition(outIos, *bs);
					fIos.close();
					return;
				case CACHE_FLUSH_BY_OID:
					doCacheFlushByOID(outIos, *bs);
					fIos.close();
					return;
				case CACHE_FLUSH:
					doCacheFlushCmd(outIos, *bs);
					fIos.close();
					return;
				case CACHE_CLEAN_VSS:
					doCacheCleanVSSCmd(outIos, *bs);
					fIos.close();
					return;
				case FLUSH_ALL_VERSION:
					doCacheFlushAllversion(outIos, *bs);
					fIos.close();
					return;
				case CACHE_DROP_FDS:
					doCacheDropFDs(outIos);
					fIos.close();
					return;
				default:
					break;
				}

				switch(ismHdr->Command) {
				case DICT_CREATE_EQUALITY_FILTER: {
					createEqualityFilter(bs);
					break;
				}
				case DICT_DESTROY_EQUALITY_FILTER: {
					destroyEqualityFilter(bs);
					break;
				}
				case DICT_TOKEN_BY_SCAN_COMPARE:
				{
                    assert(bs->length() >= sizeof(TokenByScanRequestHeader));
                    TokenByScanRequestHeader *hdr = (TokenByScanRequestHeader *) ismHdr;
                    if (bRotateDest) {
                        if (!pUmSocketSelector->nextIOSocket(
                            fIos, outIos, writeLock)) {
                            // If we ever fall into this part of the
                            // code we have a "bug" of some sort.
                            // See handleUmSockSelErr() for more info.
                            // We reset ios and mutex to defaults.
                            handleUmSockSelErr(string("default cmd"));
                            outIos      = outIosDefault;
                            writeLock   = writeLockDefault;
                            pUmSocketSelector->delConnection(fIos);
                            bRotateDest = false;
                        }
                    }
                    if (hdr->flags & IS_SYSCAT)
                        boost::thread t(DictScanJob(outIos, bs, writeLock));
                    else
                        procPoolPtr->invoke(DictScanJob(outIos, bs, writeLock),
                            LOGICAL_BLOCK_RIDS, hdr->Hdr.UniqueID);
                    break;
				}
                case BATCH_PRIMITIVE_RUN: {
                    if (bRotateDest) {
                        if (!pUmSocketSelector->nextIOSocket(
                            fIos, outIos, writeLock)) {

                            // If we ever fall into this part of the
                            // code we have a "bug" of some sort.
                            // See handleUmSockSelErr() for more info.
                            // We reset ios and mutex to defaults.
                            handleUmSockSelErr(string("BPR cmd"));
                            outIos      = outIosDefault;
                            writeLock   = writeLockDefault;
                            pUmSocketSelector->delConnection(fIos);
                            bRotateDest = false;
                        }
                    }
                    /* Decide whether this is a syscat call and run
                    right away instead of queueing */
                    BPPSeeder bpps(bs, writeLock, outIos, fPrimitiveServerPtr->ProcessorThreads(),
                        fPrimitiveServerPtr->PTTrace());
                    if (bpps.isSysCat())
                        boost::thread t(bpps);
                    else
                        procPoolPtr->invoke(bpps, ismHdr->Size, bpps.getID());
                    break;
                }
                case BATCH_PRIMITIVE_CREATE: {
                    fBPPHandler.createBPP(*bs);
                    break;
                }
                case BATCH_PRIMITIVE_ADD_JOINER: {
                    fBPPHandler.addJoinerToBPP(*bs);
                    break;
                }
                case BATCH_PRIMITIVE_END_JOINER: {
                    // lastJoinerMsg can block; must do this in a different thread
                    boost::thread t(BPPHandler::LastJoinerRunner(&fBPPHandler, bs));
                    break;
                }
                case BATCH_PRIMITIVE_DESTROY: {
                    fBPPHandler.destroyBPP(*bs);
                    break;
                }
                case BATCH_PRIMITIVE_ACK: {
                    fBPPHandler.doAck(*bs);
                    break;
                }
                case BATCH_PRIMITIVE_ABORT: {
                    fBPPHandler.doAbort(*bs);
                    break;
                }
                default: {
                    std::ostringstream os;
                    Logger log;
                    os << "unknown primitive cmd: " << ismHdr->Command;
                    log.logMessage(os.str());
                    break;
                }
                }  // the switch stmt
			}
			else // bs.length() == 0
			{
				if (bRotateDest)
					pUmSocketSelector->delConnection(fIos);
				fIos.close();
				break;
			}
			}   // the try- surrounding the if stmt
			catch (std::exception &e) {
				Logger logger;
				logger.logMessage(e.what());
			}
		}
	}

	// If this function is called, we have a "bug" of some sort.  We added
	// the "fIos" connection to UmSocketSelector earlier, so at the very
	// least, UmSocketSelector should have been able to return that con-
	// nection/port.  We will try to recover by using the original fIos to
	// send the response msg; but as stated, if this ever happens we have
	// a bug we need to resolve.
	void handleUmSockSelErr(const string& cmd)
	{
		ostringstream oss;
		oss << "Unable to rotate through socket destinations (" <<
			cmd << ") for connection: " << fIos.toString();
		cerr << oss.str() << endl;
		logging::Message::Args args;
		args.add(oss.str());
		mlp->logMessage(logging::M0058,args,false);
	}

	~ReadThread() {}
	string fServerName;
	IOSocket fIos;
	PrimitiveServer* fPrimitiveServerPtr;
	BPPHandler	fBPPHandler;
};

/** @brief accept a primitive command from the user module
 */
struct ServerThread
{
	ServerThread(string serverName, PrimitiveServer* ps) :
		fServerName(serverName), fPrimitiveServerPtr(ps)
	{
		SUMMARY_INFO2("starting server ", fServerName);

		bool tellUser = true;
		bool toldUser = false;

		for (;;) {
			try {
				mqServerPtr = new MessageQueueServer(fServerName);
				break;
			} catch (runtime_error& re) {
				string what = re.what();
				if (what.find("Address already in use") != string::npos) {
					if (tellUser) {
						cerr << "Address already in use, retrying..." << endl;
						tellUser = false;
						toldUser = true;
					}
#ifdef _MSC_VER
					Sleep(5 * 1000);
#else
					sleep(5);
#endif
				} else {
					throw;
				}
			}
		}

		if (toldUser)
			cerr << "Ready." << endl;
	}

	void operator()()
	{
		IOSocket ios;
		try {
			for(;;) {
				ios = mqServerPtr->accept();
				//startup a detached thread to handle this socket's I/O
				boost::thread rt(ReadThread(fServerName, ios, fPrimitiveServerPtr));
			}
		} catch(std::exception& ex) {
			SUMMARY_INFO2("exception caught in ServerThread: ", ex.what());
		} catch(...) {
			SUMMARY_INFO("exception caught in ServerThread.");
		}
	}

	string fServerName;
	PrimitiveServer* fPrimitiveServerPtr;
	MessageQueueServer* mqServerPtr;
};



// Receives messages from Multicast PGM protocol
struct TransportReceiverThread
{
	PrimitiveServer* 	fPrimServer;
// 	MulticastReceiver 	fmcReceiver;
	uint64_t			fMaxBytes;
	bool				fMultiloop;
	string				fServerName;
	BPPHandler			fBPPHandler;
	uint8_t				fLastCommand;
	uint32_t			fLastUniqueId;

	TransportReceiverThread(uint64_t maxBytes, bool loop, const string& name, PrimitiveServer* ps) :
		fPrimServer(ps), fMaxBytes(maxBytes), /*fmcReceiver*/fMultiloop(loop), fServerName(name),
		fBPPHandler(ps), fLastCommand(0), fLastUniqueId(0)
	{
		SUMMARY_INFO("Starting multicast receiver");
	}

	void operator()()
	{
		MulticastReceiver fmcReceiver;
		do 
		{
			try
			{
				SBS byteStream = fmcReceiver.receive();  //blocks
				processMessage(byteStream);	
			}
			catch(const logging::MulticastException& ex)
			{
				logAndSendErrorMessage(ex.what());
#ifdef ECONNRESET
				if (ECONNRESET != ex.errorCode())
#endif
					return;
			}
			catch(const std::exception& ex)
			{
				logAndSendErrorMessage(ex.what());
				return;
			}
			catch(...)
			{
				logAndSendErrorMessage("Multicast caught unknown exception");
				return;
			}

		} 
		while (true);
	}
	
	void processMessage(const SBS& bs)
	{
		assert(bs->length() >= sizeof(ISMPacketHeader));

		const ByteStream::byte* bytePtr = bs->buf();
		const ISMPacketHeader* ismHdr = reinterpret_cast<const ISMPacketHeader*>(bytePtr);
		fLastCommand = ismHdr->Command;
		switch (ismHdr->Command) 
		{
			case BATCH_PRIMITIVE_CREATE:
				fLastUniqueId = *((const uint32_t *) &bytePtr[sizeof(ISMPacketHeader) + sizeof(uint8_t) + 4*sizeof(uint32_t)]);

				fBPPHandler.createBPP(*bs);
				break;
			case BATCH_PRIMITIVE_DESTROY:
				fBPPHandler.destroyBPP(*bs);
				break;
			case BATCH_PRIMITIVE_ADD_JOINER:
				fLastUniqueId = *((const uint32_t *) &bytePtr[sizeof(ISMPacketHeader) + 2*sizeof(uint32_t)]);
				fBPPHandler.addJoinerToBPP(*bs);
				break;
			case BATCH_PRIMITIVE_END_JOINER: 
			{
				// lastJoinerMsg blocks, this thread won't wait.
				boost::thread t(BPPHandler::LastJoinerRunner(&fBPPHandler, bs));
				break;
			}
			default: 
			 	ostringstream oss;
				oss << "Incorrect message type for Multicast ";
				oss <<  ismHdr->Command;
				logging::Message::Args args;
				args.add(oss.str());
				mlp->logMessage(logging::M0070,args);
		}
	}

	void logAndSendErrorMessage(const string& msg)
	{
		logging::Message::Args args;
		args.add(msg);
		mlp->logMessage(logging::M0070,args, true);
		if (BATCH_PRIMITIVE_ADD_JOINER == fLastCommand || BATCH_PRIMITIVE_CREATE == fLastCommand)
		{
			fBPPHandler.setBPPToError(fLastUniqueId, msg, logging::multicastErr);
		}
	}
};

} // namespace anon

namespace primitiveprocessor
{
PrimitiveServer::PrimitiveServer(int serverThreads,
								int serverQueueSize,
								int processorThreads,
								int processorWeight,
								int processorQueueSize,
								bool rotatingDestination,
								uint32_t BRPBlocks, 
								int BRPThreads, 
								int cacheCount,
								int maxBlocksPerRead,
								int readAheadBlocks,
								uint32_t deleteBlocks,
								bool ptTrace,
								double prefetch,
								bool multicast,
								bool multicastloop,
								uint64_t smallSide
								):
				fServerThreads(serverThreads),
				fServerQueueSize(serverQueueSize),
				fProcessorThreads(processorThreads), 
				fProcessorWeight(processorWeight), 
				fProcessorQueueSize(processorQueueSize),
				fMaxBlocksPerRead(maxBlocksPerRead),
				fReadAheadBlocks(readAheadBlocks),
				fRotatingDestination(rotatingDestination),
				fPTTrace(ptTrace),
				fPrefetchThreshold(prefetch),
				fMulticast(multicast),
				fMulticastloop(multicastloop),
				fPMSmallSide(smallSide)
{
	fCacheCount=cacheCount;
	fServerpool.setMaxThreads(fServerThreads + multicast);
	fServerpool.setQueueSize(fServerQueueSize);
	fProcessorpool.setMaxThreads(fProcessorThreads);
	fProcessorpool.setQueueSize(fProcessorQueueSize);
	fProcessorpool.setMaxThreadWeight(fProcessorWeight);
	asyncCounter = 0;

	brm = new DBRM();

	BRPp = new BlockRequestProcessor*[fCacheCount];
	try
	{
		for (int i = 0; i < fCacheCount; i++)
			BRPp[i] = new BlockRequestProcessor(BRPBlocks/fCacheCount, BRPThreads/fCacheCount,
				fMaxBlocksPerRead, deleteBlocks/fCacheCount);
	}
	catch (...)
	{
		cerr << "Unable to allocate " << BRPBlocks << " cache blocks. Adjust the DBBC config parameter "
			"downward." << endl;
		mlp->logMessage(logging::M0045, logging::Message::Args(), true);
		exit(1);
	}
}

PrimitiveServer::~PrimitiveServer()
{
}

void PrimitiveServer::start()
{
	// start all the server threads
	for ( int i = 1; i <= fServerThreads; i++) {
		string s("PMS");
		stringstream oss;
		oss << s << i;

		fServerpool.invoke(ServerThread(oss.str(), this));
	}

	if (fMulticast)
	{
		pause_(5);
		try
		{
		  fServerpool.invoke(TransportReceiverThread(fPMSmallSide, fMulticastloop, "PMS1", this));
		}
		catch (const logging::MulticastException& mce)
		{
			string errormsg("  Error starting Multicast Receiver Thread. Turning Multicast off");
			cout << mce.what() << errormsg << endl;
			logging::Message::Args args;
			args.add(mce.what());
			args.add(errormsg);
			mlp->logMessage(logging::M0070,args);
			fMulticast = false;
		}
	}
	// wait for things to settle down...
//	pause_(5);

	{
		Oam oam;
		try {
			oam.processInitComplete("PrimProc");
		} catch (...) {}
	}

	fServerpool.wait();

	cerr << "PrimitiveServer::start() exiting!" << endl;
}

BPPV::BPPV()
{
	sendThread.reset(new BPPSendThread());
}

BPPV::~BPPV()
{
}

void BPPV::add(boost::shared_ptr<BatchPrimitiveProcessor> a)
{
	v.push_back(a);
}
const vector<boost::shared_ptr<BatchPrimitiveProcessor> > & BPPV::get()
{
	return v;
}


boost::shared_ptr<BatchPrimitiveProcessor> BPPV::next()
{
	std::vector<boost::shared_ptr<BatchPrimitiveProcessor> >::iterator bpp = v.begin();

	for (bpp = v.begin(); bpp != v.end(); bpp++)
		if (!(*bpp)->busy()) {
			(*bpp)->busy(true);
			return *bpp;
		}
		
	// They're all busy, make threadpool reschedule the job
	return boost::shared_ptr<BatchPrimitiveProcessor>();
}

void BPPV::abort()
{
	sendThread->abort();
}

#ifdef PRIMPROC_STOPWATCH
map<pthread_t, logging::StopWatch*> stopwatchMap;
pthread_mutex_t stopwatchMapMutex;
bool stopwatchThreadCreated = false;

//------------------------------------------------------------------------------
// Stays in "sleep" state for specified until passed seconds have elapsed.
//------------------------------------------------------------------------------
void pause_(int seconds )
{
	struct timespec req;
	struct timespec rem;
	req.tv_sec  = seconds;
	req.tv_nsec = 0;
	rem.tv_sec  = 0;
	rem.tv_nsec = 0;

	while (1)
	{
		if (nanosleep(&req, &rem) != 0)
		{
			if (rem.tv_sec > 0 || rem.tv_nsec > 0)
			{
				req = rem;
				continue;
			}
		}
		break;
	}
}

void *autoFinishStopwatchThread(void *arg)
{
	struct timeval tvCurrent;
	int count = 0;
	for(;;)
	{
		// Pause two seconds.
		pause_(2);
		count++;
		// Iterate through the stopwatch map and see how long it's been since last activity.
		map<pthread_t, logging::StopWatch*>::iterator stopwatchMapIter = stopwatchMap.begin();
		logging::StopWatch *stopwatch;
		gettimeofday(&tvCurrent, 0);
		bool primProcIdle = true;
		while(stopwatchMapIter != stopwatchMap.end())
		{
			stopwatch = stopwatchMapIter->second;

			// If any threads have been active in the last 5 seconds, kick out.
			if(((tvCurrent.tv_sec - stopwatch->fTvLast.tv_sec) < 5) || stopwatch->isActive())
			{
				primProcIdle = false;
				break;
			}
			stopwatchMapIter++;
		}

		// No activity in last five seconds, display timing results.
		// if(primProcIdle || (count%15 == 0))
		if (primProcIdle)
		{
			pthread_mutex_lock(&stopwatchMapMutex);
			stopwatchMapIter = stopwatchMap.begin();
			while(stopwatchMapIter != stopwatchMap.end())
			{
				stopwatch = stopwatchMapIter->second;
				stopwatch->finish();
				stopwatchMapIter++;
				delete stopwatch;
			}
			stopwatchMap.clear();
			pthread_mutex_unlock(&stopwatchMapMutex);
		}
	}
	return 0;
};
#endif  // PRIMPROC_STOPWATCH

} // namespace primitiveprocessor
// vim:ts=4 sw=4:

