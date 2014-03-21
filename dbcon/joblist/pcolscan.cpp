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
*   $Id: pcolscan.cpp 9655 2013-06-25 23:08:13Z xlou $
*
*
***********************************************************************/

#include <unistd.h>
#include <stdexcept>
#include <cstring>
#include <utility>
#include <sstream>
#include <cassert>
using namespace std;

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
using namespace boost;

#include "messagequeue.h"
using namespace messageqcpp;
#include "configcpp.h"
using namespace config;
#include "messageobj.h"
using namespace logging;

#include "logicoperator.h"
using namespace execplan;


#include "distributedenginecomm.h"
#include "primitivemsg.h"
#include "timestamp.h"
#include "unique32generator.h"
#include "jlf_common.h"
#include "primitivestep.h"

//#define DEBUG 1
//#define DEBUG2 1


namespace
{
//// const uint32_t defaultScanLbidReqLimit     = 10000;
//// const uint32_t defaultScanLbidReqThreshold =  5000;
//
//struct pColScanStepPrimitive
//{
//    pColScanStepPrimitive(pColScanStep* pColScanStep) : fPColScanStep(pColScanStep)
//    {}
//    pColScanStep *fPColScanStep;
//    void operator()()
//    {
//        try
//        {
//            fPColScanStep->sendPrimitiveMessages();
//        }
//        catch(std::exception& re)
//        {
//			string msg = re.what();
//			cerr << "pColScanStep: send thread threw an exception: " << msg << endl;
//
//			//Whoa! is this really what we want to do? It's not clear that any good can be had by
//			//sticking around, but this seems drastic...
//			if (msg.find("there are no primitive processors") != string::npos)
//			{
//				SPJL logger = fPColScanStep->logger();
//				logger->logMessage(LOG_TYPE_CRITICAL, LogNoPrimProcs, Message::Args(), LoggingID(5));
//				exit(1);
//			}
//		}
//    }
//};
//
//struct pColScanStepAggregater
//{
//    pColScanStepAggregater(pColScanStep* pColScanStep, uint64_t index) :
//							fPColScanStepCol(pColScanStep), fThreadId(index)
//    {}
//    pColScanStep *fPColScanStepCol;
//    uint64_t fThreadId;
//
//    void operator()()
//    {
//        try
//        {
//            fPColScanStepCol->receivePrimitiveMessages(fThreadId);
//        }
//        catch(std::exception& re)
//        {
//			cerr << fPColScanStepCol->toString() << ": receive thread threw an exception: " << re.what() << endl;
//		}
//    }
//};
//

} // end of local unnamed namespace


namespace joblist
{

pColScanStep::pColScanStep(
	CalpontSystemCatalog::OID o,
	CalpontSystemCatalog::OID t,
	const CalpontSystemCatalog::ColType& ct,
	const JobInfo& jobInfo) :
		JobStep(jobInfo),
		fRm(jobInfo.rm),
		fNumThreads(fRm.getJlNumScanReceiveThreads()),
		fFilterCount(0),
		fOid(o),
		fTableOid(t),
		fColType(ct),
		fBOP(BOP_OR),
		sentCount(0),
		recvCount(0),
		fScanLbidReqLimit(fRm.getJlScanLbidReqLimit()),
		fScanLbidReqThreshold(fRm.getJlScanLbidReqThreshold()),
		fStopSending(false),
		fSingleThread(false),
		fPhysicalIO(0),
		fCacheIO(0),
		fNumBlksSkipped(0),
		fMsgBytesIn(0),
		fMsgBytesOut(0),
		fMsgsToPm(0)
{
	if (fTableOid == 0)  // cross engine support
		return;

	int err, i, mask;
	BRM::LBIDRange_v::iterator it;

	//pthread_mutex_init(&mutex, NULL);
	//pthread_mutex_init(&dlMutex, NULL);
	//pthread_mutex_init(&cpMutex, NULL);
	//pthread_cond_init(&condvar, NULL);
	//pthread_cond_init(&condvarWakeupProducer, NULL);
	finishedSending=false;
	recvWaiting=0;
	recvExited = 0;
	ridsReturned = 0;
	sendWaiting=false;
	rDoNothing = false;
	fIsDict = false;

	memset(&fMsgHeader, 0, sizeof(fMsgHeader));

	//If this is a dictionary column, fudge the numbers...
	if ( fColType.colDataType == CalpontSystemCatalog::VARCHAR )
	{
		if (8 > fColType.colWidth && 4 <= fColType.colWidth )
			fColType.colDataType = CalpontSystemCatalog::CHAR;

		fColType.colWidth++;
	}

	//If this is a dictionary column, fudge the numbers...
	if (fColType.colDataType == CalpontSystemCatalog::VARBINARY)
	{
		fColType.colWidth = 8;
		fIsDict = true;
	}
	else if (fColType.colWidth > 8 )
	{
		fColType.colWidth = 8;
		fIsDict = true;
		//TODO: is this right?
		fColType.colDataType = CalpontSystemCatalog::VARCHAR;
	}

	//Round colWidth up
	if (fColType.colWidth == 3)
		fColType.colWidth = 4;
	else if (fColType.colWidth == 5 || fColType.colWidth == 6 || fColType.colWidth == 7)
		fColType.colWidth = 8;

	err = dbrm.lookup(fOid, lbidRanges);
	if (err)
		throw runtime_error("pColScan: BRM LBID range lookup failure (1)");
	err = dbrm.getExtents(fOid, extents);
	if (err)
		throw runtime_error("pColScan: BRM HWM lookup failure (4)");
	sort(extents.begin(), extents.end(), BRM::ExtentSorter());
	numExtents = extents.size();
	extentSize = (fRm.getExtentRows()*fColType.colWidth)/BLOCK_SIZE;

	if (fOid>3000) {
		lbidList.reset(new LBIDList(fOid, 0));
	}

	/* calculate shortcuts for rid-based arithmetic */
	for (i = 1, mask = 1; i <= 32; i++) {
		mask <<= 1;
		if (extentSize & mask) {
			divShift = i;
			break;
		}
	}
	for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
		if (extentSize & mask)
			throw runtime_error("pColScan: Extent size must be a power of 2 in blocks");

	ridsPerBlock = BLOCK_SIZE/fColType.colWidth;
	for (i = 1, mask = 1; i <= 32; i++) {
		mask <<= 1;
		if (ridsPerBlock & mask) {
			rpbShift = i;
			break;
		}
	}
	for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
		if (ridsPerBlock & mask)
			throw runtime_error("pColScan: Block size and column width must be a power of 2");
}

pColScanStep::~pColScanStep()
{
	//pthread_mutex_destroy(&mutex);
	//pthread_mutex_destroy(&dlMutex);
	//pthread_mutex_destroy(&cpMutex);
	//pthread_cond_destroy(&condvar);
	//pthread_cond_destroy(&condvarWakeupProducer);
	//delete lbidList;
//	delete [] fProducerThread;
//	if (fDec)
//		fDec->removeQueue(uniqueID);   // in case it gets aborted
}

//------------------------------------------------------------------------------
// Initialize configurable parameters
//------------------------------------------------------------------------------
void pColScanStep::initializeConfigParms()
{
// 	const string section           ( "JobList" );
// 	const string sendLimitName     ( "ScanLbidReqLimit" );
// 	const string sendThresholdName ( "ScanLbidReqThreshold" );
// 	const string numReadThreadsName  ( "NumScanReceiveThreads" );
// 	Config* cf = Config::makeConfig();

// 	string        strVal;

	//...Get the tuning parameters that throttle msgs sent to primproc
	//...fScanLbidReqLimit puts a cap on how many LBID's we will request from
	//...    primproc, before pausing to let the consumer thread catch up.
	//...    Without this limit, there is a chance that PrimProc could flood
	//...    ExeMgr with thousands of messages that will consume massive
	//...    amounts of memory for a 100 gigabyte database.
	//...fScanLbidReqThreshold is the level at which the number of outstanding
	//...    LBID reqs must fall below, before the producer can send more LBIDs.
// 	strVal = cf->getConfig(section, sendLimitName);
// 	if (strVal.size() > 0)
// 		fScanLbidReqLimit = static_cast<uint32_t>(Config::uFromText(strVal));
//
// 	strVal = cf->getConfig(section, sendThresholdName);
// 	if (strVal.size() > 0)
// 		fScanLbidReqThreshold = static_cast<uint32_t>(Config::uFromText(strVal));
//
// 	fNumThreads = 8;
// 	strVal = cf->getConfig(section, numReadThreadsName);
// 	if (strVal.size() > 0)
// 		fNumThreads = static_cast<uint32_t>(Config::uFromText(strVal));

// 	fProducerThread = new SPTHD[fNumThreads];
}

void pColScanStep::startPrimitiveThread()
{
//    fConsumerThread.reset(new boost::thread(pColScanStepPrimitive(this)));
}

void pColScanStep::startAggregationThread()
{
//	for (uint32_t i = 0; i < fNumThreads; i++)
//    	fProducerThread[i].reset(new boost::thread(pColScanStepAggregater(this, i)));
}

void pColScanStep::run()
{
//	if (traceOn())
//	{
//		syslogStartStep(16,               // exemgr subsystem
//			std::string("pColScanStep")); // step name
//	}
//
//	//"consume" input datalist. In this case, there is no IDL, we just send one primitive
//	startPrimitiveThread();
//	//produce output datalist
//	//Don't start this yet...see below
//	//startAggregationThread();
}

void pColScanStep::join()
{
//	fConsumerThread->join();
//	if ( !fSingleThread ) {
//	for (uint32_t i = 0; i < fNumThreads; i++)
//		fProducerThread[i]->join();
//	}
}

void pColScanStep::sendPrimitiveMessages()
{
//  //The presence of an input DL means we (probably) have a pDictionaryScan step feeding this scan step
//  // a list of tokens to get the rids for. Convert the input tokens to a filter string.
//  if (fInputJobStepAssociation.outSize() > 0)
//  {
//    addFilters();
//    if (fTableOid >= 3000)
//      cout << toString() << endl;
//    //If we got no input rids (as opposed to no input DL at all) then there were no matching rows from
//    //  the previous step, so this step should not return any rows either. This would be the case, for
//    //  instance, if P_NAME LIKE '%xxxx%' produced no signature matches.
//    if (fFilterCount == 0) {
//		rDoNothing=true;
//		startAggregationThread();
//		return;
//	}
//  }
//
//  startAggregationThread();
//
//  /* for all the blocks that need to be sent
//  *  build out a message for each block with the primitive message header filled
//  *  out and all the NOPS and BOP structures as well.
//  *  Then serialize it to a BytStream and then send it on its way
//  */
//
//  LBIDRange_v::iterator it;
//  uint64_t fbo;
//
//  ISMPacketHeader ism;
//  ism.Flags = planFlagsToPrimFlags(fTraceFlags);
//  ism.Command=COL_BY_SCAN_RANGE;
//  ism.Size = sizeof(ISMPacketHeader) + sizeof(ColByScanRangeRequestHeader) + fFilterString.length();
//  ism.Type=2;
//  //bool firstWrite = true;
//
//  //...Counter used to track the number of LBIDs we are requesting from
//  //...primproc in the current set of msgs, till we reach fScanLbidReqLimit
//  uint32_t runningLbidCount = 0;
//  bool exitLoop = false;
//  const bool ignoreCP = ((fTraceFlags & CalpontSelectExecutionPlan::IGNORE_CP) != 0);
//
//  for (it = lbidRanges.begin(); it != lbidRanges.end(); it++)
//  {
// 	BRM::LBID_t lbid = (*it).start;
//
//	fbo = getFBO(lbid);
//	if (hwm < fbo)
//		continue;
//
//    if (fOid >= 3000 && lbidList->CasualPartitionDataType(fColType.colDataType, fColType.colWidth) )
//    {
//  		int64_t Min=0;
//  		int64_t Max=0;
//  		int64_t SeqNum=0;
//  		bool MinMaxValid=true;
//  		bool cpPredicate=true;
//
//		// can we consolidate these crit sections?
//		cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
//        MinMaxValid = lbidList->GetMinMax(Min, Max, SeqNum, lbid, 0);
//
//        if (MinMaxValid)
//        {
//			cpPredicate=lbidList->CasualPartitionPredicate(Min,
//															Max,
//															&fFilterString,
//															fFilterCount,
//															fColType,
//															fBOP) || ignoreCP;
//        }
//		cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
//
//		if (cpPredicate==false){ //don't scan this extent
//#ifdef DEBUG
//			cout << "Scan Skip " << lbid << endl;
//#endif
//			//...Track the number of LBIDs we skip due to Casual Partioning.
//			//...We use the same equation we use to initialize remainingLbids
//			//...in the code that follows down below this.
//			fNumBlksSkipped += ( (hwm > (fbo + it->size - 1)) ?
//							(it->size) : (hwm - fbo + 1) );
//			continue;
//		}
//#ifdef DEBUG
//		else
//			cout << "Scan " << lbid << endl;
//#endif
//
//    }
//
//	LBID_t    msgLbidStart   = it->start;
//	uint32_t remainingLbids =
//		( (hwm > (fbo + it->size - 1)) ? (it->size) : (hwm - fbo + 1) );
//	uint32_t msgLbidCount   = 0;
//
//	while ( remainingLbids > 0 )
//	{
//		//...Break up this range of LBIDs when we reach the msg size
//		//...limit for one request (fScanLbidReqLimit)
//		if ( (runningLbidCount + remainingLbids) >= fScanLbidReqLimit )
//		{
//			msgLbidCount = fScanLbidReqLimit - runningLbidCount;
//			sendAPrimitiveMessage(ism, msgLbidStart, msgLbidCount );
//			//...Wait for the consuming thread to catch up if our
//			//...backlog of work is >= the allowable threshold limit
//
//		 	mutex.lock(); //pthread_mutex_lock(&mutex);
//			sentCount += msgLbidCount;
//			if (recvWaiting)
//				condvar.notify_all(); //pthread_cond_broadcast(&condvar); //signal consumer to resume
//
//			while ( ((sentCount - recvCount) >= fScanLbidReqThreshold)
//					&& !fStopSending )
//			{
//				sendWaiting = true;
//#ifdef DEBUG2
//				if (fOid >= 3000)
//					cout << "pColScanStep producer WAITING: " <<
//						"st:"          << fStepId   <<
//						"; sentCount-" << sentCount <<
//						"; recvCount-" << recvCount <<
//						"; threshold-" << fScanLbidReqThreshold << endl;
//#endif
//				condvarWakeupProducer.wait(mutex); //pthread_cond_wait ( &condvarWakeupProducer, &mutex );
//#ifdef DEBUG2
//				if (fOid >= 3000)
//					cout << "pColScanStep producer RESUMING: " <<
//						"st:" << fStepId << endl;
//#endif
//				sendWaiting = false;
//			}
//
//			//...Set flag to quit if consumer thread tells us to
//			if (fStopSending)
//				exitLoop = true;
//
// 			mutex.unlock(); //pthread_mutex_unlock(&mutex);
//
//			runningLbidCount = 0;
//		}
//		else
//		{
//			msgLbidCount = remainingLbids;
//
//			sendAPrimitiveMessage(ism, msgLbidStart, msgLbidCount );
//			mutex.lock(); //pthread_mutex_lock(&mutex);
//			sentCount += msgLbidCount;
//			if (recvWaiting)
//				condvar.notify_all(); //pthread_cond_broadcast(&condvar); //signal consumer to resume
//
//			//...Set flag to quit if consumer thread tells us to
//			if (fStopSending)
//				exitLoop = true;
//
// 			mutex.unlock(); //pthread_mutex_unlock(&mutex);
//
//			runningLbidCount += msgLbidCount;
//		}
//
//		//...If consuming thread has quit, then we should do the same.
//		//...This can happen if consuming thread receives empty ByteStream
//		if (exitLoop)
//			break;
//
//		remainingLbids -= msgLbidCount;
//		msgLbidStart   += msgLbidCount;
//	}
//
//	if (exitLoop)
//		break;
//
//  } // end of loop through LBID ranges to be requested from primproc
//
// 	mutex.lock(); //pthread_mutex_lock(&mutex);
//	finishedSending = true;
//	if (recvWaiting)
//		condvar.notify_all(); //pthread_cond_broadcast(&condvar);
//	mutex.unlock(); //pthread_mutex_unlock(&mutex);
//// 	cerr << "send side exiting" << endl;
//
//#ifdef DEBUG2
//	if (fOid >= 3000)
//	{
//		time_t t = time(0);
//		char timeString[50];
//		ctime_r(&t, timeString);
//		timeString[strlen(timeString)-1 ] = '\0';
//		cout << "pColScanStep Finished sending primitives for: " <<
//			fOid << " at " << timeString << endl;
//	}
//#endif
//
}

//------------------------------------------------------------------------------
// Construct and send a single primitive message to primproc
//------------------------------------------------------------------------------
void pColScanStep::sendAPrimitiveMessage(
	ISMPacketHeader& ism,
	BRM::LBID_t      msgLbidStart,
	uint32_t        msgLbidCount
	)
{
//	ByteStream bs;
//
//	bs.load(reinterpret_cast<const ByteStream::byte*>(&ism), sizeof(ism));
//
//   	fMsgHeader.LBID              = msgLbidStart;
//  	fMsgHeader.DataSize          = fColType.colWidth;
//  	fMsgHeader.DataType          = fColType.colDataType;
//  	fMsgHeader.CompType          = fColType.compressionType;
// 	if (fFilterCount > 0)
//		fMsgHeader.OutputType        = 3;   // <rid, value> pairs
// 	else
// 		fMsgHeader.OutputType = OT_DATAVALUE;
//  	fMsgHeader.BOP               = fBOP;
//  	fMsgHeader.NOPS              = fFilterCount;
//  	fMsgHeader.NVALS             = 0;
//  	fMsgHeader.Count             = msgLbidCount;
//  	fMsgHeader.Hdr.SessionID     = fSessionId;
//  	//fMsgHeader.Hdr.StatementID   = 0;
//  	fMsgHeader.Hdr.TransactionID = fTxnId;
//  	fMsgHeader.Hdr.VerID         = fVerId;
//  	fMsgHeader.Hdr.StepID        = fStepId;
//	fMsgHeader.Hdr.UniqueID		 = uniqueID;
//
//  	bs.append(reinterpret_cast<const ByteStream::byte*>(&fMsgHeader),
//              sizeof(fMsgHeader));
//  	bs += fFilterString;
//
//#ifdef DEBUG2
//	if (fOid >= 3000)
//		cout << "pColScanStep producer st: " << fStepId <<
//			": sending req for lbid start "  << msgLbidStart <<
//			"; lbid count " << msgLbidCount  << endl;
//#endif
//
//	fMsgBytesOut += bs.lengthWithHdrOverhead();
//	fDec->write(bs);
//	fMsgsToPm++;
}

struct CPInfo {
	CPInfo(int64_t MIN, int64_t MAX, uint64_t l) : min(MIN), max(MAX), LBID(l) { };
	int64_t min;
	int64_t max;
	uint64_t LBID;
};

void pColScanStep::receivePrimitiveMessages(uint64_t tid)
{
//	AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
//	DataList_t* dlp = dl->dataList();
//	FifoDataList *fifo = dl->fifoDL();
//	BucketDL<ElementType> *bucket = dynamic_cast<BucketDL<ElementType> *>(dlp);
//	ZDL<ElementType> *zdl = dynamic_cast<ZDL<ElementType> *>(dlp);
//	int64_t l_ridsReturned = 0;
//	uint64_t l_physicalIO = 0, l_cachedIO = 0;
//	uint64_t fbo;
//	uint64_t ridBase;
//	vector<ElementType> v;
//	UintRowGroup rw;
//	vector<boost::shared_ptr<ByteStream> > bsv;
//	uint32_t i, k, size, bsLength;
//	bool lastThread = false;
//	vector<CPInfo> cpv;
//
//	if (bucket || zdl)
//	  	dlp->setMultipleProducers(true);
//
//	mutex.lock(); //pthread_mutex_lock(&mutex);
//
//	// count the LBIDs
//	for (; !rDoNothing; ) {
//
//		// sync with the send side
//		while (!finishedSending && sentCount == recvCount) {
//			recvWaiting++;
//			condvar.wait(mutex); //pthread_cond_wait(&condvar, &mutex);
//			recvWaiting--;
//		}
//		if (sentCount == recvCount && finishedSending) {
//// 			cout << "done recving" << endl;
//			break;
//		}
//
//		fDec->read_some(uniqueID, fNumThreads, bsv);
//		for (unsigned int jj=0; jj<bsv.size(); jj++) {
//			fMsgBytesIn += bsv[jj]->lengthWithHdrOverhead();
//		}
//
//
//		size = bsv.size();
//
//		if (size == 0) {
//			/* XXXPAT: Need to give other threads a chance to update recvCount.
//			As of 2/25/08, each BS contains multiple responses.  With the current
//			protocol, the exact # isn't known until they're processed.  Threads
//			waiting for more input will have to busy wait on either recvCount
//			being updated or input.  It's only an issue at the tail end of the
//			responses, and probably doesn't matter then either. */
//
//			mutex.unlock(); //pthread_mutex_unlock(&mutex);
//			usleep(1000);  // 1ms  Good?
//			mutex.lock(); //pthread_mutex_lock(&mutex);
//			continue;
//		}
//
//		if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0)
//			dlTimes.setFirstReadTime();
//		if (fOid>=3000) dlTimes.setLastReadTime();
//
//// 		cerr << "got a response of " << size << " msgs\n";
//
//		mutex.unlock(); //pthread_mutex_unlock(&mutex);
//
//		uint32_t msgCount = 0;
//		for (i = 0; i < size; i++) {
//			const ByteStream::byte* bsp = bsv[i]->buf();
//
//			bsLength = bsv[i]->length();
//			k = 0;
//			while (k < bsLength) {
//				++msgCount;
////  			cout << "got msg " << msgCount << " k = " << k << endl;
//				k += sizeof(ISMPacketHeader);
//				const ColResultHeader* crh = reinterpret_cast<const ColResultHeader*>(&bsp[k]);
//			 	// get the ColumnResultHeader out of the bytestream
//				k += sizeof(ColResultHeader);
//
//				l_cachedIO    += crh->CacheIO;
//				l_physicalIO += crh->PhysicalIO;
//				fbo = getFBO(crh->LBID);
//				ridBase = fbo << rpbShift;
//
// 	 			for(int j = 0; j < crh->NVALS; j++)
//  				{
//					uint64_t dv;
//					uint64_t rid;
//
//					if (crh->OutputType == OT_DATAVALUE) {
//						if (isEmptyVal(&bsp[k])) {
//							k += fColType.colWidth;
//							continue;
//						}
//						rid = j + ridBase;
//					}
//					else {
//				  		rid = *((const uint16_t *) &bsp[k]) + ridBase;
//						k += sizeof(uint16_t);
//					}
//
//					switch (fColType.colWidth) {
//						case 8: dv = *((const uint64_t *) &bsp[k]); k += 8; break;
//						case 4: dv = *((const uint32_t *) &bsp[k]); k += 4; break;
//						case 2: dv = *((const uint16_t *) &bsp[k]); k += 2; break;
//						case 1: dv = *((const uint8_t *) &bsp[k]); ++k; break;
//						default:
//							throw runtime_error("pColStep: invalid column width!");
//					}
//
//					v.push_back(ElementType(rid, dv));
//					++l_ridsReturned;
//#ifdef DEBUG
////					if (fOid >=3000)
////						cout << "  -- inserting <" << rid << ", " << dv << ">" << endl;
//#endif
//					// per row operations...
//  				} // for
//
//				// per block operations...
//
//#ifdef DEBUG
//					cout << "recvPrimMsgs Oid " << fOid
//						<< " valid " << crh->ValidMinMax
//						<< " LBID " << crh->LBID
//						<< " Mn/Mx " << crh->Min << "/" << crh->Max
//						<< " nvals " << crh->NVALS
//						<< "/" << l_ridsReturned << endl;
//#endif
//				if (fOid >= 3000 && crh->ValidMinMax)
//					cpv.push_back(CPInfo(crh->Min, crh->Max, crh->LBID));
//			}
//				// per ByteStream operations...
//		}
//			// per read operations....
//
//		if (bucket) {
//			if (fOid>=3000 && dlTimes.FirstInsertTime().tv_sec==0)
//			    dlTimes.setFirstInsertTime();
//
//			bucket->insert(v);
//		}
//		else if (zdl) {
//			zdl->insert(v);
//		}
//		else {
//			size = v.size();
//			if (size>0)
//				if (fOid>=3000 && dlTimes.FirstInsertTime().tv_sec==0)
//					dlTimes.setFirstInsertTime();
//
//			dlMutex.lock(); //pthread_mutex_lock(&dlMutex);
//			for (i = 0; i < size; ++i) {
//				rw.et[rw.count++] = v[i];
//				if (rw.count == rw.ElementsPerGroup)
//				{
//					fifo->insert(rw);
//					rw.count = 0;
//				}
//			}
//
//			if (rw.count > 0)
//			{
//				fifo->insert(rw);
//				rw.count = 0;
//			}
//
//			dlMutex.unlock(); //pthread_mutex_unlock(&dlMutex);
//		}
//		v.clear();
//
//		size = cpv.size();
//		if (size > 0) {
//			cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
//			for (i = 0; i < size; i++) {
//				CPInfo *cpi = &(cpv[i]);
//				lbidList->UpdateMinMax(cpi->min, cpi->max, cpi->LBID, fColType.colDataType);
//			}
//			cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
//			cpv.clear();
//		}
//
//		mutex.lock(); //pthread_mutex_lock(&mutex);
//		recvCount += msgCount;
//		//...If producer is waiting, and we have gone below our threshold value,
//		//...then we signal the producer to request more data from primproc
//		if ( (sendWaiting) && ( (sentCount - recvCount) < fScanLbidReqThreshold ) )
//		{
//#ifdef DEBUG2
//			if (fOid >= 3000)
//			cout << "pColScanStep consumer signaling producer for more data: "<<
//				"st:"          << fStepId   <<
//				"; sentCount-" << sentCount <<
//				"; recvCount-" << recvCount <<
//				"; threshold-" << fScanLbidReqThreshold << endl;
//#endif
//			condvarWakeupProducer.notify_one(); //pthread_cond_signal(&condvarWakeupProducer);
//		}
//	} // end of loop to read LBID responses from primproc
//
//	fPhysicalIO += l_physicalIO;
//	fCacheIO    += l_cachedIO;
//	ridsReturned += l_ridsReturned;
////  	cerr << "out of the main loop " << recvExited << endl;
//	if (++recvExited == fNumThreads) {
//		//...Casual partitioning could cause us to do no processing.  In that
//		//...case these time stamps did not get set.  So we set them here.
//		if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0) {
//			dlTimes.setFirstReadTime();
//			dlTimes.setLastReadTime();
//			dlTimes.setFirstInsertTime();
//		}
//		if (fOid>=3000) dlTimes.setEndOfInputTime();
//
//		//@bug 699: Reset StepMsgQueue
//		fDec->removeQueue(uniqueID);
//
//		if (fifo)
//			fifo->endOfInput();
//		else
//			dlp->endOfInput();
//		lastThread = true;
//	}
//
//	mutex.unlock(); //pthread_mutex_unlock(&mutex);
//
//	if (fTableOid >= 3000 && lastThread)
//	{
//		//...Construct timestamp using ctime_r() instead of ctime() not
//		//...necessarily due to re-entrancy, but because we want to strip
//		//...the newline ('\n') off the end of the formatted string.
//		time_t t = time(0);
//		char timeString[50];
//		ctime_r(&t, timeString);
//		timeString[strlen(timeString)-1 ] = '\0';
//
//		FifoDataList* pFifo    = 0;
//		uint64_t totalBlockedReadCount  = 0;
//		uint64_t totalBlockedWriteCount = 0;
//
//		//...Sum up the blocked FIFO reads for all input associations
//		size_t inDlCnt  = fInputJobStepAssociation.outSize();
//		for (size_t iDataList=0; iDataList<inDlCnt; iDataList++)
//		{
//			pFifo = fInputJobStepAssociation.outAt(iDataList)->fifoDL();
//			if (pFifo)
//			{
//				totalBlockedReadCount += pFifo->blockedReadCount();
//			}
//		}
//
//		//...Sum up the blocked FIFO writes for all output associations
//		size_t outDlCnt = fOutputJobStepAssociation.outSize();
//		for (size_t iDataList=0; iDataList<outDlCnt; iDataList++)
//		{
//			pFifo = fOutputJobStepAssociation.outAt(iDataList)->fifoDL();
//			if (pFifo)
//			{
//				totalBlockedWriteCount += pFifo->blockedWriteCount();
//			}
//		}
//
//		//...Roundoff inbound msg byte count to nearest KB for display;
//		//...no need to do so for outbound, because it should be small.
//		uint64_t msgBytesInKB = fMsgBytesIn >> 10;
//		if (fMsgBytesIn & 512)
//			msgBytesInKB++;
//        // @bug 807
//        if (fifo)
//            fifo->totalSize(ridsReturned);
//
//		if (traceOn())
//		{
//			//...Print job step completion information
//			ostringstream logStr;
//			logStr << "ses:" << fSessionId <<
//				" st: " << fStepId << " finished at " <<
//				timeString << "; PhyI/O-" << fPhysicalIO << "; CacheI/O-"  <<
//				fCacheIO << "; MsgsSent-" << fMsgsToPm << "; MsgsRcvd-" << recvCount <<
//				"; BlockedFifoIn/Out-" << totalBlockedReadCount <<
//				"/" << totalBlockedWriteCount <<
//				"; output size-" << ridsReturned << endl <<
//				"\tPartitionBlocksEliminated-" << fNumBlksSkipped <<
//				"; MsgBytesIn-"  << msgBytesInKB  << "KB" <<
//				"; MsgBytesOut-" << fMsgBytesOut  << "B"  << endl <<
//				"\t1st read " << dlTimes.FirstReadTimeString() <<
//				"; EOI " << dlTimes.EndOfInputTimeString() << "; runtime-" <<
//				JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(),dlTimes.FirstReadTime()) <<
//				"s" << endl;
//
//			logEnd(logStr.str().c_str());
//
//			syslogReadBlockCounts(16,    // exemgr subsystem
//				fPhysicalIO,             // # blocks read from disk
//				fCacheIO,                // # blocks read from cache
//				fNumBlksSkipped);        // # casual partition block hits
//			syslogProcessingTimes(16,    // exemgr subsystem
//				dlTimes.FirstReadTime(),   // first datalist read
//				dlTimes.LastReadTime(),    // last  datalist read
//				dlTimes.FirstInsertTime(), // first datalist write
//				dlTimes.EndOfInputTime()); // last (endOfInput) datalist write
//			syslogEndStep(16,            // exemgr subsystem
//				totalBlockedReadCount,   // blocked datalist input
//				totalBlockedWriteCount,  // blocked datalist output
//				fMsgBytesIn,             // incoming msg byte count
//				fMsgBytesOut);           // outgoing msg byte count
//		}
//
//	}
//
// 	if (fOid >=3000 && lastThread)
// 		lbidList->UpdateAllPartitionInfo();
//
//// 	cerr << "recv thread exiting" << endl;
}

void pColScanStep::addFilter(int8_t COP, float value)
{
	fFilterString << (uint8_t) COP;
	fFilterString << (uint8_t) 0;
	fFilterString << *((uint32_t *) &value);
	fFilterCount++;
}

void pColScanStep::addFilter(int8_t COP, int64_t value, uint8_t roundFlag)
{
	int8_t tmp8;
	int16_t tmp16;
	int32_t tmp32;

	fFilterString << (uint8_t) COP;
	fFilterString << roundFlag;

	// converts to a type of the appropriate width, then bitwise
	// copies into the filter ByteStream
	switch(fColType.colWidth) {
		case 1:
			tmp8 = value;
			fFilterString << *((uint8_t *) &tmp8);
			break;
		case 2:
			tmp16 = value;
			fFilterString << *((uint16_t *) &tmp16);
			break;
		case 4:
			tmp32 = value;
			fFilterString << *((uint32_t *) &tmp32);
			break;
		case 8:
			fFilterString << *((uint64_t *) &value);
			break;
		default:
			ostringstream o;

			o << "pColScanStep: CalpontSystemCatalog says OID " << fOid <<
				" has a width of " << fColType.colWidth;
			throw runtime_error(o.str());
	}
	fFilterCount++;
}

void pColScanStep::setBOP(int8_t B)
{
	fBOP = B;
}

void pColScanStep::setSingleThread( bool b)
{
	fSingleThread = b;
	fNumThreads = 1;
}

void pColScanStep::setOutputType(int8_t OutputType)
{
	fOutputType = OutputType;
}

const string pColScanStep::toString() const
{
	ostringstream oss;
	oss << "pColScanStep    ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId << " st:" << fStepId <<
		" tb/col:" << fTableOid << "/" << fOid;
	if (alias().length()) oss << " alias:" << alias();
	oss << " " << omitOidInDL
		<< fOutputJobStepAssociation.outAt(0) << showOidInDL;
	oss << " nf:" << fFilterCount;
	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
	{
		oss << fInputJobStepAssociation.outAt(i) << ", ";
	}
	return oss.str();
}

uint64_t pColScanStep::getFBO(uint64_t lbid)
{
	uint32_t i;
	uint64_t lastLBID;

	for (i = 0; i < numExtents; i++) {
 		lastLBID = extents[i].range.start + (extents[i].range.size << 10) - 1;
//  		lastLBID = extents[i].range.start + (extents[i].range.size * 1024) - 1;
//  		cerr << "start: " << extents[i].range.start << " end:" << lastLBID <<endl;
		if (lbid >= (uint64_t) extents[i].range.start && lbid <= lastLBID)
// 			return (lbid - extents[i].range.start) + (extentSize * i);
 			return (lbid - extents[i].range.start) + (i << divShift);
	}
	cerr << "pColScan: didn't find the FBO?\n";
	throw logic_error("pColScan: didn't find the FBO?");
}

pColScanStep::pColScanStep(const pColStep& rhs) :
	JobStep(rhs),
	fRm(rhs.resourceManager())
{
	fNumThreads = fRm.getJlNumScanReceiveThreads();
	fFilterCount = rhs.filterCount();
	fFilterString = rhs.filterString();
	isFilterFeeder = rhs.getFeederFlag();
	fOid = rhs.oid();
	fTableOid = rhs.tableOid();
	fColType = rhs.colType();
	fBOP = rhs.BOP();
	fIsDict = rhs.isDictCol();
	sentCount = 0;
	recvCount = 0;
	ridsReturned = 0;
	fScanLbidReqLimit = fRm.getJlScanLbidReqLimit();
	fScanLbidReqThreshold = fRm.getJlScanLbidReqThreshold();
	fStopSending = false;
	fSingleThread = false;
	fPhysicalIO = 0;
	fCacheIO = 0;
	fNumBlksSkipped = 0;
	fMsgBytesIn = 0;
	fMsgBytesOut = 0;
	fMsgsToPm = 0;
	fCardinality = rhs.cardinality();
	fFilters = rhs.fFilters;
	fOnClauseFilter = rhs.onClauseFilter();

	if (fTableOid == 0)  // cross engine support
		return;

	int err;

	memset(&fMsgHeader, 0, sizeof(fMsgHeader));

	err = dbrm.lookup(fOid, lbidRanges);
	if (err)
		throw runtime_error("pColScan: BRM LBID range lookup failure (1)");
	err = dbrm.getExtents(fOid, extents);
	if (err)
		throw runtime_error("pColScan: BRM HWM lookup failure (4)");
	sort(extents.begin(), extents.end(), BRM::ExtentSorter());
	numExtents = extents.size();
	extentSize = (fRm.getExtentRows()*fColType.colWidth)/BLOCK_SIZE;
	lbidList=rhs.lbidList;
	//pthread_mutex_init(&mutex, NULL);
	//pthread_mutex_init(&dlMutex, NULL);
	//pthread_mutex_init(&cpMutex, NULL);
	//pthread_cond_init(&condvar, NULL);
	//pthread_cond_init(&condvarWakeupProducer, NULL);
	finishedSending = sendWaiting = rDoNothing = false;
	recvWaiting = 0;
	recvExited = 0;

	/* calculate some shortcuts for extent-based arithmetic */
	ridsPerBlock = rhs.ridsPerBlock;
	rpbShift = rhs.rpbShift;
	divShift = rhs.divShift;

// 	initializeConfigParms ( );
	fTraceFlags = rhs.fTraceFlags;
//	uniqueID = UniqueNumberGenerator::instance()->getUnique32();
//	if (fDec)
//		fDec->addQueue(uniqueID);
//	fProducerThread = new SPTHD[fNumThreads];
}

void pColScanStep::addFilters()
{
	AnyDataListSPtr dl = fInputJobStepAssociation.outAt(0);
	DataList_t* bdl = dl->dataList();
	idbassert(bdl);
	int it = -1;
	bool more;
	ElementType e;
	int64_t token;

	try{
		it = bdl->getIterator();
	}catch(std::exception& ex) {
		cerr << "pColScanStep::addFilters: caught exception: "
			<< ex.what() << " stepno: " << fStepId << endl;
		throw;
	}catch(...) {
		cerr << "pColScanStep::addFilters: caught exception" << endl;
		throw;
	}

	fBOP = BOP_OR;
	more = bdl->next(it, &e);
	while (more)
	{
		token = e.second;
		addFilter(COMPARE_EQ, token);
		more = bdl->next(it, &e);
	}
	return;
}

bool pColScanStep::isEmptyVal(const uint8_t *val8) const
{
    const int width = fColType.colWidth;

    switch (fColType.colDataType)
    {
		case CalpontSystemCatalog::UTINYINT:
		{
			return(*val8 == joblist::UTINYINTEMPTYROW);
		}
		case CalpontSystemCatalog::USMALLINT:
		{
			const uint16_t *val16 = reinterpret_cast<const uint16_t *>(val8);
			return(*val16 == joblist::USMALLINTEMPTYROW);
		}
		case CalpontSystemCatalog::UMEDINT:
		case CalpontSystemCatalog::UINT:
		{
			const uint32_t *val32 = reinterpret_cast<const uint32_t *>(val8);
			return(*val32 == joblist::UINTEMPTYROW);
		}
		case CalpontSystemCatalog::UBIGINT:
		{
			const uint64_t *val64 = reinterpret_cast<const uint64_t *>(val8);
			return(*val64 == joblist::BIGINTEMPTYROW);
		}
        case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
        {
            const uint32_t *val32 = reinterpret_cast<const uint32_t *>(val8);
            return(*val32 == joblist::FLOATEMPTYROW);
        }
        case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
        {
            const uint64_t *val64 = reinterpret_cast<const uint64_t *>(val8);
            return(*val64 == joblist::DOUBLEEMPTYROW);
        }
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
            if (width == 1)
            {
                return(*val8 == joblist::CHAR1EMPTYROW);
            }
            else if (width == 2)
            {
                const uint16_t *val16 = reinterpret_cast<const uint16_t *>(val8);
                return(*val16 == joblist::CHAR2EMPTYROW);
            }
            else if (width <= 4)
            {
                const uint32_t *val32 = reinterpret_cast<const uint32_t *>(val8);
                return(*val32 == joblist::CHAR4EMPTYROW);
            }
            else if (width <= 8)
            {
                const uint64_t *val64 = reinterpret_cast<const uint64_t *>(val8);
                return(*val64 == joblist::CHAR8EMPTYROW);
            }
        default:
            break;
    }

    switch (width)
    {
        case 1:
        {
            return(*val8 == joblist::TINYINTEMPTYROW);
        }
        case 2:
        {
            const uint16_t *val16 = reinterpret_cast<const uint16_t *>(val8);
            return(*val16 == joblist::SMALLINTEMPTYROW);
        }
        case 4:
        {
            const uint32_t *val32 = reinterpret_cast<const uint32_t *>(val8);
            return(*val32 == joblist::INTEMPTYROW);
        }
        case 8:
        {
                const uint64_t *val64 = reinterpret_cast<const uint64_t *>(val8);
                return(*val64 == joblist::BIGINTEMPTYROW);
        }
        default:
            MessageLog logger(LoggingID(28));
            logging::Message::Args colWidth;
            Message msg(33);

            colWidth.add(width);
            msg.format(colWidth);
            logger.logErrorMessage(msg);
            return false;
    }

    return false;
}


void pColScanStep::addFilter(const Filter* f)
{
    if (NULL != f)
    	fFilters.push_back(f);
}


void pColScanStep::appendFilter(const std::vector<const execplan::Filter*>& fs)
{
    fFilters.insert(fFilters.end(), fs.begin(), fs.end());
}

}
// vim:ts=4 sw=4:

