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

/***********************************************************************
 *   $Id: primitiveserver.h 2055 2013-02-08 19:09:09Z pleblanc $
 *
 *
 ***********************************************************************/
/** @file */
#ifndef PRIMITIVESERVER_H
#define PRIMITIVESERVER_H

#include <map>
#ifdef _MSC_VER
#include <unordered_map>
#include <unordered_set>
#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#endif
#include <boost/thread.hpp>

#include "threadpool.h"
#include "prioritythreadpool.h"
#include "messagequeue.h"
#include "blockrequestprocessor.h"
#include "batchprimitiveprocessor.h"

//#define PRIMPROC_STOPWATCH
#ifdef PRIMPROC_STOPWATCH
#include "stopwatch.h"
#endif

namespace primitiveprocessor
{
	extern dbbc::BlockRequestProcessor **BRPp;
	extern BRM::DBRM *brm;
	extern boost::mutex bppLock;
	extern uint highPriorityThreads, medPriorityThreads, lowPriorityThreads;

#ifdef PRIMPROC_STOPWATCH
	extern map<pthread_t, logging::StopWatch*> stopwatchMap;
	extern pthread_mutex_t stopwatchMapMutex;
	extern bool stopwatchThreadCreated;
	
	extern void pause_(int seconds);
	extern void *autoFinishStopwatchThread(void *arg);
#endif

	class BPPV {
		public:
			BPPV();
			~BPPV();
			boost::shared_ptr<BatchPrimitiveProcessor> next();
			void add(boost::shared_ptr<BatchPrimitiveProcessor> a);
			const std::vector<boost::shared_ptr<BatchPrimitiveProcessor> > & get();
			inline boost::shared_ptr<BPPSendThread> getSendThread() { return sendThread; }
			void abort();
		private:
			std::vector<boost::shared_ptr<BatchPrimitiveProcessor> > v;
			boost::shared_ptr<BPPSendThread> sendThread;
	};

	typedef boost::shared_ptr<BPPV> SBPPV;
	typedef std::map<uint32_t, SBPPV> BPPMap;
	extern BPPMap bppMap;

	void prefetchBlocks(uint64_t lbid, uint32_t* rCount);
	void prefetchExtent(uint64_t lbid, uint32_t ver, uint32_t txn, uint32_t* rCount);
	void loadBlock(u_int64_t lbid, BRM::QueryContext q, u_int32_t txn, int compType, void* bufferPtr,
		bool* pWasBlockInCache, uint32_t* rCount=NULL, bool LBIDTrace = false,
		uint32_t sessionID = 0, bool doPrefetch=true, VSSCache *vssCache = NULL);
	void loadBlockAsync(uint64_t lbid, const BRM::QueryContext &q, uint32_t txn, int CompType,
		uint32_t *cCount, uint32_t *rCount, bool LBIDTrace, uint32_t sessionID,
		boost::mutex *m, uint *busyLoaders, VSSCache* vssCache=0);
	uint loadBlocks(BRM::LBID_t *lbids, BRM::QueryContext q, BRM::VER_t txn, int compType,
		uint8_t **bufferPtrs, uint32_t *rCount, bool LBIDTrace, uint32_t sessionID,
		uint blockCount, bool *wasVersioned, bool doPrefetch = true, VSSCache *vssCache = NULL);
	uint cacheNum(uint64_t lbid);
	void buildFileName(BRM::OID_t oid, char* fileName);

    /** @brief process primitives as they arrive
     */
    class PrimitiveServer
    {
        public:
            /** @brief ctor
             */
            PrimitiveServer(int serverThreads,
						int serverQueueSize,
						int processorWeight,
						int processorQueueSize,
						bool rotatingDestination,
                		uint32_t BRPBlocks=(1024 * 1024 * 2), 
						int BRPThreads=64, 
						int cacheCount = 8,
						int maxBlocksPerRead = 128,
						int readAheadBlocks=256,
						uint32_t deleteBlocks = 0,
						bool ptTrace=false,
						double prefetchThreshold = 0,
						bool multicast = false,
						bool multicastloop = false,
						uint64_t pmSmallSide = 0);

            /** @brief dtor
             */
            ~PrimitiveServer();

            /** @brief start the primitive server
             *
             */
            void start();

            /** @brief get a pointer the shared processor thread pool
             */
            inline boost::shared_ptr<threadpool::PriorityThreadPool> getProcessorThreadPool() const { return fProcessorPool; }

// 			int fCacheCount;
			const int ReadAheadBlocks() const {return fReadAheadBlocks;}
			bool  rotatingDestination() const {return fRotatingDestination;}
			bool PTTrace() const {return fPTTrace;}
			double prefetchThreshold() const { return fPrefetchThreshold; }
			uint ProcessorThreads() const { return highPriorityThreads + medPriorityThreads + lowPriorityThreads; }
        protected:

        private:
            /** @brief the thread pool used to listen for
             * incoming primitive commands
             */
            threadpool::ThreadPool fServerpool;

            /** @brief the thread pool used to process
             * primitive commands
             */
            boost::shared_ptr<threadpool::PriorityThreadPool> fProcessorPool;

            int fServerThreads;
            int fServerQueueSize;
            int fProcessorWeight;
            int fProcessorQueueSize;
			int fMaxBlocksPerRead;
			int fReadAheadBlocks;
			bool fRotatingDestination;
			bool fPTTrace;
			double fPrefetchThreshold;
			bool fMulticast;
			bool fMulticastloop;
			uint64_t fPMSmallSide;
    };


} // namespace primitiveprocessor
#endif //PRIMITIVESERVER_H
