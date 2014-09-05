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
*   $Id: pdictionary.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/

#include <iostream>
#include <stdexcept>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
using namespace std;

#include "messagequeue.h"
using namespace messageqcpp;
#include "configcpp.h"
using namespace config;

#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
using namespace logging;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "brm.h"
using namespace BRM;

#include "distributedenginecomm.h"
#include "elementtype.h"
#include "unique32generator.h"
#include "jlf_common.h"
#include "primitivestep.h"

namespace joblist
{

//struct pDictionaryStepPrimitive
//{
//    pDictionaryStepPrimitive(pDictionaryStep* pDictStep) : fPDictionaryStep(pDictStep)
//    {}
//
//	pDictionaryStep *fPDictionaryStep;
//
//    void operator()()
//    {
//        try
//        {
//            fPDictionaryStep->sendPrimitiveMessages();
//        } catch(runtime_error&)
//        {
//		}
//    }
//
//};
//
//struct pDictStepAggregator
//{
//    pDictStepAggregator(pDictionaryStep* pDictStep) : fPDictStep(pDictStep)
//    {}
//    pDictionaryStep *fPDictStep;
//    void operator()()
//    {
//        try
//        {
//            fPDictStep->receivePrimitiveMessages();
//        }
//        catch(runtime_error&)
//        {
//	}
//    }
//};



pDictionaryStep::pDictionaryStep(
	CalpontSystemCatalog::OID o,
	CalpontSystemCatalog::OID t,
	const CalpontSystemCatalog::ColType& ct,
	const JobInfo& jobInfo) :
	JobStep(jobInfo),
	fOid(o),
	fTableOid(t),
	fBOP(BOP_NONE),
	msgsSent(0),
	msgsRecvd(0),
	finishedSending(false),
	recvWaiting(false),
	ridCount(0),
	fColType(ct),
	fFilterCount(0),
	requestList(0),
	fInterval(jobInfo.flushInterval),
	fPhysicalIO(0),
	fCacheIO(0),
	fMsgBytesIn(0),
	fMsgBytesOut(0),
	fRm(jobInfo.rm),
	hasEqualityFilter(false)
{
//	uniqueID = UniqueNumberGenerator::instance()->getUnique32();

//	fColType.compressionType = fColType.ddn.compressionType = ct;
}

pDictionaryStep::~pDictionaryStep()
{
//	if (fDec)
//		fDec->removeQueue(uniqueID);
}

void pDictionaryStep::startPrimitiveThread()
{
//	pThread.reset(new boost::thread(pDictionaryStepPrimitive(this)));
}

void pDictionaryStep::startAggregationThread()
{
//	cThread.reset(new boost::thread(pDictStepAggregator(this)));
}

void pDictionaryStep::run()
{
//	if (traceOn())
//	{
//		syslogStartStep(16,                  // exemgr subsystem
//			std::string("pDictionaryStep")); // step name
//	}
//
//	const AnyDataListSPtr& dl = fInputJobStepAssociation.outAt(0);
//	DataList_t* dlp = dl->dataList();
//	setInputList(dlp);
//
//	startPrimitiveThread();
//	startAggregationThread();
}

void pDictionaryStep::join()
{
//	pThread->join();
//	cThread->join();
}

void pDictionaryStep::setInputList(DataList_t* dl)
{
	requestList = dl;
}

void pDictionaryStep::setBOP(int8_t b)
{
	fBOP = b;
}

void pDictionaryStep::addFilter(int8_t COP, const string& value)
{
	fFilterString << (uint8_t) COP;
	fFilterString << (uint16_t) value.size();
	fFilterString.append((const uint8_t *) value.c_str(), value.size());
	fFilterCount++;
	if (fFilterCount == 1 && (COP == COMPARE_EQ || COP == COMPARE_NE)) {
		hasEqualityFilter = true;
		tmpCOP = COP;
	}

	if (hasEqualityFilter) {
		if (COP != tmpCOP) {
			hasEqualityFilter = false;
			eqFilter.clear();
		}
		else
			eqFilter.push_back(value);
	}
}

void pDictionaryStep::sendPrimitiveMessages()
{
//	int it = -1;
//	int msgRidCount = 0;
//	bool more;
//	int64_t sigToken, msgLBID, nextLBID = -1;
//	uint16_t sigOrd;
//	ByteStream msgRidList, primMsg(65536);   //the MAX_BUFFER_SIZE as of 8/20
//	DictSignatureRequestHeader hdr;
//	ISMPacketHeader ism;
//	OldGetSigParams pt;
//	FifoDataList* fifo = fInputJobStepAssociation.outAt(0)->fifoDL();
//	UintRowGroup rw;
//
///* XXXPAT: Does this primitive need to care about the HWM as a sanity check, given
//that a ridlist is supplied? */
//
//	if (fifo == 0)
//		throw logic_error("Use p_colscanrange instead here");
//	
//	try{
//		it = fifo->getIterator();
//	}catch(exception& ex) {
//		cerr << "pDictionaryStep::sendPrimitiveMessages: caught exception: " << ex.what() << endl;
//	}catch(...) {
//		cerr << "pDictionaryStep::sendPrimitiveMessages: caught exception" << endl;
//	}
//
//	more = fifo->next(it, &rw);
//		
//	sigToken = rw.et[0].second;
//	msgLBID = sigToken >> 10;
//	while (more || msgRidCount > 0) {
//		for (uint64_t i = 0; ((i < rw.count) || (!more && msgRidCount > 0)); )
//		{
//			if (more)
//			{
//				ridCount++;
//				sigToken = rw.et[i].second;
//				nextLBID = sigToken >> 10;
//#ifdef DEBUG
// 			cout << "sigToken = " << sigToken << " lbid = " << nextLBID << endl;
//#endif
//			}
//
//			// @bug 472
//			if (nextLBID == msgLBID && more && msgRidCount < 8000) { //XXXPAT: need to prove N & S here
//				sigOrd = sigToken & 0x3ff;
//				pt.rid = (nextLBID >= 0 ? rw.et[i].first : 0x8000000000000000LL | rw.et[i].first);
//				pt.offsetIndex = sigOrd;
//				msgRidList.append(reinterpret_cast<ByteStream::byte*>(&pt), sizeof(pt));
//				msgRidCount++;
//				++i;
//#ifdef DEBUG
// 			cout << "added signature ordinal " << sigOrd << endl;
//#endif
//			}
//			else {
//#ifdef DEBUG
//				cout << "sending a prim msg" << endl;
//#endif
//
//				// send the primitive, start constructing the next msg
//				ism.Interleave=0;
//				ism.Flags=planFlagsToPrimFlags(fTraceFlags);
//				ism.Command=DICT_SIGNATURE;
//				ism.Size=sizeof(DictSignatureRequestHeader) + msgRidList.length();
//				ism.Type=2;
//
//				hdr.Hdr.SessionID = fSessionId;
//				//hdr.Hdr.StatementID = 0;
//				hdr.Hdr.TransactionID = fTxnId;
//				hdr.Hdr.VerID = fVerId;
//				hdr.Hdr.StepID = fStepId;
//				hdr.Hdr.UniqueID = uniqueID;
//
//				hdr.LBID = msgLBID; 
//				idbassert(msgRidCount <= 8000);
//				hdr.NVALS = msgRidCount;
//				hdr.CompType = fColType.ddn.compressionType;
//				
//				primMsg.load((const uint8_t *) &ism, sizeof(ism));
//				primMsg.append((const uint8_t *) &hdr, sizeof(DictSignatureRequestHeader));
//				primMsg += msgRidList;
//				fMsgBytesOut += primMsg.lengthWithHdrOverhead();
//				fDec->write(primMsg);
//
//				msgLBID = nextLBID;
//				primMsg.restart();
//				msgRidList.restart();
//				msgRidCount = 0;
//
//				mutex.lock();
//				msgsSent++;
//				if (recvWaiting)
//					condvar.notify_one();
//#ifdef DEBUG
//				cout << "msgsSent++ = " << msgsSent << endl;
//#endif
//				mutex.unlock();
//
//				if (!more)
//					break;
//			}
//		} // rw.count
//
//		if (more)
//		{
//			rw.count = 0;
//			more = fifo->next(it, &rw);
//		}
//	}
//
//	mutex.lock();
//	finishedSending = true;
//	if (recvWaiting)
//		condvar.notify_one();
//	mutex.unlock();
}

void pDictionaryStep::receivePrimitiveMessages()
{
//	int64_t ridResults = 0;
//	AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
//	StrDataList* dlp = dl->stringDataList();
//	StringFifoDataList *fifo = fOutputJobStepAssociation.outAt(0)->stringDL();
//	StringRowGroup rw;
//	
//	while (1) {
//
//		// sync with the send side
//		mutex.lock();
//
//		while (!finishedSending && msgsSent==msgsRecvd) {
//			recvWaiting = true;
//			condvar.wait(mutex);
//			if (msgsSent == msgsRecvd) {
//				mutex.unlock();
//				break;
//			}
//			recvWaiting = false;
//		}
//
//		if (finishedSending != 0 && msgsRecvd >= msgsSent) {
//			goto junk;
//		}
//		mutex.unlock();
//	
//		// do the recv
//
//		ByteStream bs = fDec->read(uniqueID);
//		fMsgBytesIn += bs.lengthWithHdrOverhead();
//		if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0)
//			dlTimes.setFirstReadTime();
//		if (fOid>=3000) dlTimes.setLastReadTime();
//	
//		msgsRecvd++;
//		if (bs.length() == 0) 
//			 break;
//
//		const ByteStream::byte* bsp = bs.buf();
//
//		// get the ResultHeader out of the bytestream
//		const DictOutput* drh = reinterpret_cast<const DictOutput*>(bsp);
//		
//		bsp += sizeof(DictOutput);
//
//		fCacheIO    += drh->CacheIO;
//		fPhysicalIO += drh->PhysicalIO;
//
//		// From this point on the rest of the bytestream is the data that comes back from the primitive server
//		// This needs to be fed to a datalist that is retrieved from the outputassociation object.
//
//		char d[8192];
//// 		memset(d, 0, 8192);
//		if (fOid>=3000 && dlTimes.FirstInsertTime().tv_sec==0)
//			dlTimes.setFirstInsertTime();
//		for(int j = 0; j < drh->NVALS; j++)
//		{
//			const uint64_t* ridp = (const uint64_t*)bsp;
//			bsp += sizeof(*ridp);
//			uint64_t rid = *ridp;
//			const uint16_t* lenp = (const uint16_t*)bsp;
//			bsp += sizeof(*lenp);
//			uint16_t len = *lenp;
//			memcpy(d, bsp, len);
//			bsp += len;
//			d[len] = 0;
//			if (rid == 0xFFFFFFFFFFFFFFFFULL)
//			{
//				strcpy(d, CPNULLSTRMARK.c_str());
//			}
//#ifdef FIFO_SINK
//			if (fOid < 3000)
//#endif
//				if (fifo)
//				{
//					rw.et[rw.count++] = StringElementType(rid, d);
//					if (rw.count == rw.ElementsPerGroup)
//					{
//						fifo->insert(rw);
//						rw.count = 0;
//					}
//				}
//				else
//				{
//					dlp->insert(StringElementType(rid, d));
//				}
//
//#ifdef DEBUG
//				cout << "  -- inserting <" << rid << ", " << d << ">" << endl;
//#endif
//				ridResults++;
//
//		}
// 	}
//
//junk:
//
//	if (fifo && rw.count > 0)
//		fifo->insert(rw);
//
//	//@bug 699: Reset StepMsgQueue
//	fDec->removeQueue(uniqueID);
//
//	if (fOid>=3000) dlTimes.setEndOfInputTime();
//	dlp->endOfInput();
//
//	if (fTableOid >= 3000)
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
//
//
//		//...Roundoff msg byte counts to nearest KB for display
//		uint64_t msgBytesInKB  = fMsgBytesIn  >> 10;
//		uint64_t msgBytesOutKB = fMsgBytesOut >> 10;
//		if (fMsgBytesIn & 512)
//			msgBytesInKB++;
//		if (fMsgBytesOut & 512)
//			msgBytesOutKB++;
//        
//        // @bug 807
//        if (fifo)
//            fifo->totalSize(ridResults);  
//
//		if (traceOn())
//		{
//			//...Print job step completion information
//			ostringstream logStr;
//			logStr << "ses:" << fSessionId << " st: " << fStepId <<
//				" finished at " <<
//				timeString << "; PhyI/O-" << fPhysicalIO << "; CacheI/O-" <<
//				fCacheIO << "; MsgsRcvd-" << msgsRecvd <<
//				"; BlockedFifoIn/Out-" << totalBlockedReadCount <<
//				"/" << totalBlockedWriteCount <<
//				"; output size-" << ridResults << endl << 
//				"\tMsgBytesIn-"  << msgBytesInKB  << "KB" <<
//				"; MsgBytesOut-" << msgBytesOutKB << "KB" << endl <<
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
//                0);                      // # casual partition block hits
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
//	}
//
}

const string pDictionaryStep::toString() const
{
	ostringstream oss;

	oss << "pDictionaryStep ses:"
		<< fSessionId << " txn:"
		<< fTxnId 	<< " ver:"
		<< fVerId 	<< " st:"
		<< fStepId 	<< " tb/col:"
		<< fTableOid << "/" << fOid;
	oss << " " << omitOidInDL
		<< fOutputJobStepAssociation.outAt(0) << showOidInDL;
	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
	{
		oss << fInputJobStepAssociation.outAt(i) << ", ";
	}
#ifdef FIFO_SINK
	if (fOid < 3000))
		oss << " (sink)";
#endif
	return oss.str();
}

void pDictionaryStep::appendFilter(const messageqcpp::ByteStream& filter, unsigned count)
{
	ByteStream bs(filter);  // need to preserve the input BS
	uint8_t *buf;
	uint8_t COP;
	uint16_t size;
	string value;

	while (bs.length() > 0) {
		bs >> COP;
		bs >> size;
		buf = bs.buf();
		value = string((char *) buf, size);
		addFilter(COP, value);
		bs.advance(size);
	}




    //fFilterString += filter;
    //fFilterCount += count;
}


void pDictionaryStep::addFilter(const Filter* f)
{
    if (NULL != f)
    	fFilters.push_back(f);
}


void pDictionaryStep::appendFilter(const std::vector<const execplan::Filter*>& fs)
{
    fFilters.insert(fFilters.end(), fs.begin(), fs.end());
}


}   //namespace
