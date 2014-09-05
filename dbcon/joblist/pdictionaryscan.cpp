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
*   $Id: pdictionaryscan.cpp 9308 2013-03-13 18:08:40Z pleblanc $
*
*
***********************************************************************/
#include <stdexcept>
#include <cstring>
#include <utility>
#include <sstream>
//#define NDEBUG
#include <cassert>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
using namespace std;

#include "distributedenginecomm.h"
#include "elementtype.h"
#include "unique32generator.h"
#include "oamcache.h"
#include "jlf_common.h"
#include "primitivestep.h"

#include "messagequeue.h"
using namespace messageqcpp;
#include "configcpp.h"
using namespace config;

#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
#include "liboamcpp.h"
using namespace logging;

#include "calpontsystemcatalog.h"
#include "logicoperator.h"
using namespace execplan;

#include "brm.h"
using namespace BRM;

#include "rowgroup.h"
using namespace rowgroup;

//#define DEBUG 1
//#define DEBUG2 1


namespace joblist
{

struct pDictionaryScanPrimitive
{
    pDictionaryScanPrimitive(pDictionaryScan* pDictScan) : fPDictScan(pDictScan)
    {}
	pDictionaryScan *fPDictScan;
    void operator()()
    {
        try
        {
            fPDictScan->sendPrimitiveMessages();
        }
        catch(runtime_error& re)
        {
			catchHandler(re.what(), ERR_DICTIONARY_SCAN, fPDictScan->errorInfo(), fPDictScan->sessionId());
		}
        catch(...)
        {
			catchHandler("pDictionaryScan send caught an unknown exception",
							ERR_DICTIONARY_SCAN, fPDictScan->errorInfo(), fPDictScan->sessionId());
		}

    }
};

struct pDictionaryScanAggregator
{
    pDictionaryScanAggregator(pDictionaryScan* pDictScan) : fPDictScan(pDictScan)
    {}
    pDictionaryScan *fPDictScan;
    void operator()()
    {
        try
        {
            fPDictScan->receivePrimitiveMessages();
        }
        catch(runtime_error& re)
        {
			catchHandler(re.what(), ERR_DICTIONARY_SCAN, fPDictScan->errorInfo(), fPDictScan->sessionId());
		}
        catch(...)
        {
			catchHandler("pDictionaryScan receive caught an unknown exception",
							ERR_DICTIONARY_SCAN, fPDictScan->errorInfo(), fPDictScan->sessionId());
		}

    }
};


pDictionaryScan::pDictionaryScan(
	CalpontSystemCatalog::OID o,
	CalpontSystemCatalog::OID t,
	const CalpontSystemCatalog::ColType& ct,
	const JobInfo& jobInfo) :
	JobStep(jobInfo),
	fDec(NULL),
	sysCat(jobInfo.csc),
	fOid(o),
	fTableOid(t),
	fFilterCount(0),
	fBOP(BOP_NONE),
	msgsSent(0),
	msgsRecvd(0),
	finishedSending(false),
	recvWaiting(false),
	sendWaiting(false),
	ridCount(0),
	ridList(0),
	colType(ct),
	fScanLbidReqLimit(jobInfo.rm.getJlScanLbidReqLimit()),
	fScanLbidReqThreshold(jobInfo.rm.getJlScanLbidReqThreshold()),
	fStopSending(false),
	fSingleThread(false),
	fPhysicalIO(0),
	fCacheIO(0),
	fMsgBytesIn(0),
	fMsgBytesOut(0),
	fMsgsToPm(0),
	fRm(jobInfo.rm),
	isEquality(false)
{
	int err;
	DBRM dbrm;

	fDictBlkCount=0;
	err = dbrm.getHWM(fOid, fDictBlkCount);
	if (err)
	{
		ostringstream oss;
		oss << "pDictionaryScan: getHWM error! For OID-" << fOid;
		throw runtime_error(oss.str());
	}

	if (fDictBlkCount <=0)
		fDictBlkCount=1;

	err = dbrm.lookup(fOid, fDictlbids);
	if (err)
	{
		ostringstream oss;
		oss << "pDictionaryScan: lookup error (2)! For OID-" << fOid;
		throw runtime_error(oss.str());
	}

	err = dbrm.getExtents(fOid, extents);
	if (err)
	{
		ostringstream oss;
		oss << "pDictionaryScan: dbrm.getExtents error! For OID-" << fOid;
		throw runtime_error(oss.str());
	}
	sort(extents.begin(), extents.end(), ExtentSorter());
	numExtents = extents.size();
	extentSize = (fRm.getExtentRows()*8)/BLOCK_SIZE;

	uint64_t i = 1, mask = 1;
	for (; i <= 32; i++)
	{
		mask <<= 1;
		if (extentSize & mask)
		{
			divShift = i;
			break;
		}
	}

	for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
		if (extentSize & mask)
			throw runtime_error("pDictionaryScan: Extent size must be a power of 2 in blocks");

	fCOP1 = COMPARE_NIL;
	fCOP2 = COMPARE_NIL;

	uniqueID = UniqueNumberGenerator::instance()->getUnique32();
 	initializeConfigParms();
	fExtendedInfo = "DSS: ";
}

pDictionaryScan::~pDictionaryScan()
{
	if (fDec) {
		if (isEquality)
			destroyEqualityFilter();
		fDec->removeQueue(uniqueID);
	}
}

//------------------------------------------------------------------------------
// Initialize configurable parameters
//------------------------------------------------------------------------------
void pDictionaryScan::initializeConfigParms()
{
	fLogicalBlocksPerScan = fRm.getJlLogicalBlocksPerScan();
}

void pDictionaryScan::startPrimitiveThread()
{
	pThread.reset(new boost::thread(pDictionaryScanPrimitive(this)));
}

void pDictionaryScan::startAggregationThread()
{
	cThread.reset(new boost::thread(pDictionaryScanAggregator(this)));
}

void pDictionaryScan::run()
{
	if (traceOn())
	{
		syslogStartStep(16,                  // exemgr subsystem
			std::string("pDictionaryScan")); // step name
	}

	//For now, we cannot handle an input DL to this step
	if (fInputJobStepAssociation.outSize() > 0)
		throw logic_error("pDictionaryScan::run: don't know what to do with an input DL!");

	if (isEquality)
		serializeEqualityFilter();

	startPrimitiveThread();
	startAggregationThread();
}

void pDictionaryScan::join()
{
	pThread->join();
	cThread->join();
	if (isEquality && fDec) {
		destroyEqualityFilter();
		isEquality = false;
	}
}

void pDictionaryScan::addFilter(int8_t COP, const string& value)
{
//	uint8_t* s = (uint8_t*)alloca(value.size() * sizeof(uint8_t));

//	memcpy(s, value.data(), value.size());
//	fFilterString << (uint16_t) value.size();
//	fFilterString.append(s, value.size());
	fFilterCount++;

	if (fFilterCount==1) {
		fCOP1 = COP;
		if (COP == COMPARE_EQ || COP == COMPARE_NE) {
			isEquality = true;
			equalityFilter.push_back(value);
		}
	}

	if (fFilterCount==2) {
		fCOP2 = COP;
		// This static_cast should be safe since COP's are small, non-negative numbers
		if ((COP == COMPARE_EQ || COP == COMPARE_NE) && COP == static_cast<int8_t>(fCOP1)) {
			isEquality = true;
			equalityFilter.push_back(value);
		}
		else {
			isEquality = false;
			equalityFilter.clear();
		}
	}
	if (fFilterCount > 2 && isEquality) {
		fFilterString.reset();
		equalityFilter.push_back(value);
	}
	else {
		uint8_t* s = (uint8_t*)alloca(value.size() * sizeof(uint8_t));

		memcpy(s, value.data(), value.size());
		fFilterString << (uint16_t) value.size();
		fFilterString.append(s, value.size());
	}
}

void pDictionaryScan::setRidList(DataList<ElementType> *dl)
{
	ridList = dl;
}

void pDictionaryScan::setBOP(int8_t b)
{
	fBOP = b;
}

void pDictionaryScan::sendPrimitiveMessages()
{
  	LBIDRange_v::iterator it;
	HWM_t hwm;
  	uint32_t fbo;
	ByteStream primMsg(65536);
	DBRM dbrm;
	uint16_t dbroot;
	uint32_t partNum;
	uint16_t segNum;
	BRM::OID_t oid;
	boost::shared_ptr<map<int, int> > dbRootPMMap;
	oam::OamCache *oamCache = oam::OamCache::makeOamCache();

	try
	{
		dbRootPMMap = oamCache->getDBRootToConnectionMap();

		it = fDictlbids.begin();
		for (; it != fDictlbids.end() && !cancelled(); it++)
		{
			LBID_t	msgLbidStart = it->start;
        	dbrm.lookupLocal(msgLbidStart,
						(BRM::VER_t)fVerId.currentScn, 
						false, 
						oid,
                        dbroot, 
						partNum, 
						segNum,
						fbo);

			dbrm.getLastHWM_DBroot(oid, dbroot, partNum, segNum, hwm);

			u_int32_t remainingLbids =
				( (hwm > (fbo + it->size - 1)) ? (it->size) : (hwm - fbo + 1) ); 

			u_int32_t msgLbidCount   =  fLogicalBlocksPerScan;
			
			while ( remainingLbids && !cancelled())
			{		
				if ( remainingLbids < msgLbidCount)					
					msgLbidCount = remainingLbids;

				if (dbRootPMMap->find(dbroot) == dbRootPMMap->end())
					throw IDBExcept(ERR_DATA_OFFLINE);
				sendAPrimitiveMessage(primMsg, msgLbidStart, msgLbidCount, (*dbRootPMMap)[dbroot]);
				primMsg.restart();

				mutex.lock();
				msgsSent += msgLbidCount;
				if (recvWaiting)
					condvar.notify_all();
		
				while (( msgsSent - msgsRecvd) >  fScanLbidReqThreshold) 
				{
					sendWaiting = true;
					condvarWakeupProducer.wait(mutex);
					sendWaiting = false;
				}
				mutex.unlock();

				remainingLbids -= msgLbidCount;
				msgLbidStart   += msgLbidCount;
			}
		} // end of loop through LBID ranges to be requested from primproc
	}//try
	catch(const exception& e)
    {
      catchHandler(e.what(), ERR_DICTIONARY_SCAN, fErrorInfo, fSessionId);
      sendError(ERR_DICTIONARY_SCAN);
    }
	catch(...)
    {
      catchHandler("pDictionaryScan caught an unknown exception.",
					ERR_DICTIONARY_SCAN, fErrorInfo, fSessionId);
      sendError(ERR_DICTIONARY_SCAN);
    }

	mutex.lock();
	finishedSending = true;

	if (recvWaiting) {
		condvar.notify_one();
	}

	mutex.unlock();

#ifdef DEBUG2
	if (fOid >= 3000)
	{
		time_t t = time(0);
		char timeString[50];
		ctime_r(&t, timeString);
		timeString[strlen(timeString)-1 ] = '\0';
		cout << "pDictionaryScan Finished sending primitives for: " <<
			fOid << " at " << timeString << endl;
    }
#endif

}

void pDictionaryScan::sendError(uint16_t s)
{
	status(s);
}

//------------------------------------------------------------------------------
// Construct and send a single primitive message to primproc
//------------------------------------------------------------------------------
void pDictionaryScan::sendAPrimitiveMessage(
	ByteStream& primMsg,
	BRM::LBID_t	msgLbidStart,
	uint32_t	msgLbidCount,
	uint16_t	pm
	)
{
	DictTokenByScanRequestHeader hdr;
	memset(&hdr, 0, sizeof(hdr));

	hdr.ism.Interleave    = pm;
	hdr.ism.Flags         = planFlagsToPrimFlags(fTraceFlags);
	hdr.ism.Command       = DICT_TOKEN_BY_SCAN_COMPARE;
	hdr.ism.Size          = sizeof(DictTokenByScanRequestHeader) +
                            fFilterString.length();
	hdr.ism.Type          = 2;

	hdr.Hdr.SessionID     = fSessionId;
	hdr.Hdr.TransactionID = fTxnId;
	hdr.Hdr.VerID         = fVerId.currentScn;
	hdr.Hdr.StepID        = fStepId;
	hdr.Hdr.UniqueID	  = uniqueID;
	hdr.Hdr.Priority	  = priority();

	hdr.LBID              = msgLbidStart;
	idbassert(hdr.LBID >= 0);
	hdr.OutputType        = OT_TOKEN;
	hdr.BOP               = fBOP;
	hdr.COP1              = fCOP1;
	hdr.COP2              = fCOP2;
 	hdr.NVALS             = fFilterCount;
	hdr.Count             = msgLbidCount;
	hdr.CompType          = colType.ddn.compressionType;
	idbassert(hdr.Count > 0);

	if (isEquality)
		hdr.flags |= HAS_EQ_FILTER;
	if (fSessionId & 0x80000000)
		hdr.flags |= IS_SYSCAT;


	/* TODO: Need to figure out how to get the full fVerID into this msg.
	 * XXXPAT: The way I did it is IMO the least kludgy, while requiring only a couple
	 * changes.
	 * The old msg was: TokenByScanRequestHeader + fFilterString
	 * The new msg is: TokenByScanRequestHeader + fVerId + old message
	 * Prepending verid wastes a few bytes that go over the network, but that is better
	 * than putting it in the middle or at the end in terms of simplicity & memory usage,
	 * given the current code.
	 */
	
	primMsg.load((const uint8_t *) &hdr, sizeof(DictTokenByScanRequestHeader));
	primMsg << fVerId;
	primMsg.append((const uint8_t*) &hdr, sizeof(DictTokenByScanRequestHeader));
	primMsg += fFilterString;

	//cout << "Sending rqst LBIDS " << msgLbidStart
	//	<< " blkCount " << fDictBlkCount << " hdr.Count " << hdr.Count
	//	<< " filterCount " << fFilterCount << endl;
#ifdef DEBUG2
    if (fOid >= 3000)
        cout << "pDictionaryScan producer st: " << fStepId <<
            ": sending req for lbid start "     << msgLbidStart <<
            "; lbid count " << msgLbidCount     << endl;
#endif
	
	try {
		fDec->write(uniqueID, primMsg);
	}
	catch (const IDBExcept& e) {
		abort();
		cerr << "pDictionaryScan::send() caught: " << e.what() << endl;
		catchHandler(e.what(), e.errorCode(), fErrorInfo, fSessionId);
	}
	catch (const std::exception& e)
	{
		abort();
		cerr << "pDictionaryScan::send() caught: " << e.what() << endl;
		catchHandler(e.what(), ERR_DICTIONARY_SCAN, fErrorInfo, fSessionId);
	}
	catch (...)
	{
		abort();
		cerr << "pDictionaryScan::send() caught unknown exception" << endl;
		catchHandler("pDictionaryScan::send() caught unknown exception",
					ERR_DICTIONARY_SCAN, fErrorInfo, fSessionId);
	}
	fMsgsToPm++;
}

void pDictionaryScan::receivePrimitiveMessages()
{
	AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
	DataList_t* dlp = dl->dataList();
	FifoDataList *fifo = fOutputJobStepAssociation.outAt(0)->fifoDL();
	RowGroupDL* rgFifo = fOutputJobStepAssociation.outAt(0)->rowGroupDL();
	UintRowGroup rw;
	boost::shared_ptr<ByteStream> bs;
	boost::shared_array<uint8_t> rgData;
	Row r;

	fRidResults = 0;

	if (rgFifo)
	{
		idbassert(fOutputRowGroup.getColumnCount() > 0);
		fOutputRowGroup.initRow(&r);
		rgData.reset(new uint8_t[fOutputRowGroup.getMaxDataSize()]);
		fOutputRowGroup.setData(rgData.get());
		fOutputRowGroup.resetRowGroup(0);
	}

	uint16_t error = status(); 
	//...Be careful here.  Mutex is locked prior to entering the loop, so
	//...any continue statement in the loop must be sure the mutex is locked.
	//error condition will not go through loop
	if (!error) mutex.lock();

	try
	{	
		while (!error) {

			// sync with the send side
			while (!finishedSending && msgsSent == msgsRecvd) {
				recvWaiting = true;
				condvar.wait(mutex);
				recvWaiting = false;
			}

			if (finishedSending && (msgsSent == msgsRecvd)) {
				mutex.unlock();
				break;
			}

			mutex.unlock();

			fDec->read(uniqueID, bs);
			if (fOid>=3000 && traceOn() && dlTimes.FirstReadTime().tv_sec==0)
				dlTimes.setFirstReadTime();
			if (fOid>=3000 && traceOn()) dlTimes.setLastReadTime();	

			if (bs->length() == 0)
			{
				mutex.lock();
				fStopSending = true;
				condvarWakeupProducer.notify_one();
				mutex.unlock();
				break;
			}
			ISMPacketHeader *hdr = (ISMPacketHeader*)(bs->buf());
			error = hdr->Status;
			if (! error)
			{	
				const ByteStream::byte* bsp = bs->buf();
					
				// get the ResultHeader out of the bytestream
				const TokenByScanResultHeader* crh = reinterpret_cast<const TokenByScanResultHeader*>(bsp);
				bsp += sizeof(TokenByScanResultHeader);

				fCacheIO    += crh->CacheIO;
				fPhysicalIO += crh->PhysicalIO;

				// From this point on the rest of the bytestream is the data that comes back from the primitive server
				// This needs to be fed to a datalist that is retrieved from the outputassociation object.
		 
				PrimToken pt;
				uint64_t token;
#ifdef DEBUG
				cout << "dict step " << fStepId << "  NVALS = " << crh->NVALS << endl;
#endif
				if (fOid>=3000 && traceOn() && dlTimes.FirstInsertTime().tv_sec==0)
					dlTimes.setFirstInsertTime();

				for(int j = 0; j < crh->NVALS && !cancelled(); j++)
				{
					memcpy(&pt, bsp, sizeof(pt));
					bsp += sizeof(pt);
					uint64_t rid = fRidResults++;
					token = (pt.LBID << 10) | pt.offset;

					if (fifo)
					{
						rw.et[rw.count++] = ElementType(rid, token);
						if (rw.count == rw.ElementsPerGroup)
						{
							fifo->insert(rw);
							rw.count = 0;
						}
					}
					else if (rgFifo)
					{
						fOutputRowGroup.getRow(fOutputRowGroup.getRowCount(), &r);
						// load r up w/ values
						r.setRid(rid);
// 						r.setUintField<8>(token, 1);
						r.setUintField<8>(token, 0);
						fOutputRowGroup.incRowCount();
						if (fOutputRowGroup.getRowCount() == 8192) {
							rgFifo->insert(rgData);
							rgData.reset(new uint8_t[fOutputRowGroup.getMaxDataSize()]);
							fOutputRowGroup.setData(rgData.get());
							fOutputRowGroup.resetRowGroup(0);
						}
					}
					else
					{
						dlp->insert(ElementType(rid, token));
					}
#ifdef DEBUG2
					cout << "  -- inserting <" << rid << ", " << token << ">" << endl;
#endif
				}

				mutex.lock();
				msgsRecvd++;

				//...If producer is waiting, and we have gone below our threshold value,
				//...then we signal the producer to request more data from primproc
				if ( (sendWaiting) &&
					 ( (msgsSent - msgsRecvd) < fScanLbidReqThreshold ) )
				{
#ifdef DEBUG2
					if (fOid >= 3000)
						cout << "pDictionaryScan consumer signaling producer for "
							"more data: "  <<
							"st:"          << fStepId   <<
							"; sentCount-" << msgsSent  <<
							"; recvCount-" << msgsRecvd <<
							"; threshold-" << fScanLbidReqThreshold << endl;
#endif
					condvarWakeupProducer.notify_one();
				}
			}   //if !error
			else
			{
				mutex.lock();
				fStopSending = true;
				condvarWakeupProducer.notify_one();
				mutex.unlock();
				string errMsg;
				//bs->advance(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
				//*bs >> errMsg;
				if (error < 1000)
				{
					logging::ErrorCodes errorcodes;
					errMsg = errorcodes.errorString(error);
				}
				else
				{
					errMsg = IDBErrorInfo::instance()->errorMsg(error);
				}
				errorMessage(errMsg);
				status(error);
			}
		} // end of loop to read LBID responses from primproc
	}
	catch(const LargeDataListExcept& ex)
    {
		catchHandler(ex.what(), ERR_DICTIONARY_SCAN, fErrorInfo, fSessionId);
		mutex.unlock();
    }
	catch(const exception& e)
    {
		catchHandler(e.what(), ERR_DICTIONARY_SCAN, fErrorInfo, fSessionId);
		mutex.unlock();
    }
	catch(...)
    {
		catchHandler("pDictionaryScan caught an unknown exception.",
						ERR_DICTIONARY_SCAN, fErrorInfo, fSessionId);
		mutex.unlock();
    }

	if (rgFifo && fOutputRowGroup.getRowCount() > 0) {
		rgFifo->insert(rgData);
		rgData.reset(new uint8_t[fOutputRowGroup.getMaxDataSize()]);
		fOutputRowGroup.setData(rgData.get());
		fOutputRowGroup.resetRowGroup(0);
	}

	// send the last remaining tokens
	if (fifo && rw.count > 0)
		fifo->insert(rw);

	Stats stats = fDec->getNetworkStats(uniqueID);
	fMsgBytesIn = stats.dataRecvd();
	fMsgBytesOut = stats.dataSent();
	
	//@bug 699: Reset StepMsgQueue
	fDec->removeQueue(uniqueID);

	if (fTableOid >= 3000)
	{
		//...Construct timestamp using ctime_r() instead of ctime() not
		//...necessarily due to re-entrancy, but because we want to strip
		//...the newline ('\n') off the end of the formatted string.
		time_t t = time(0);
		char timeString[50]; 
		ctime_r(&t, timeString);
		timeString[strlen(timeString)-1 ] = '\0';

		FifoDataList* pFifo    = 0;
		uint64_t totalBlockedReadCount  = 0;
		uint64_t totalBlockedWriteCount = 0;

		//...Sum up the blocked FIFO reads for all input associations
		size_t inDlCnt  = fInputJobStepAssociation.outSize();
		for (size_t iDataList=0; iDataList<inDlCnt; iDataList++)
		{
			pFifo = fInputJobStepAssociation.outAt(iDataList)->fifoDL();
			if (pFifo)
			{
				totalBlockedReadCount += pFifo->blockedReadCount();
			}
		}

		//...Sum up the blocked FIFO writes for all output associations
		size_t outDlCnt = fOutputJobStepAssociation.outSize();
		for (size_t iDataList=0; iDataList<outDlCnt; iDataList++)
		{
			pFifo = fOutputJobStepAssociation.outAt(iDataList)->fifoDL();
			if (pFifo)
			{
				totalBlockedWriteCount += pFifo->blockedWriteCount();
			}
		}

		//...Roundoff inbound msg byte count to nearest KB for display;
		//...no need to do so for outbound, because it should be small.
		uint64_t msgBytesInKB = fMsgBytesIn >> 10;
		if (fMsgBytesIn & 512)
			msgBytesInKB++;
        
        // @bug 828
        if (fifo)
            fifo->totalSize(fRidResults);  
        
		if (traceOn())
		{
			dlTimes.setEndOfInputTime();

			//...Print job step completion information
			ostringstream logStr;
			logStr << "ses:" << fSessionId << " st: " << fStepId <<" finished at "<<
				timeString << "; PhyI/O-" << fPhysicalIO << "; CacheI/O-" <<
				fCacheIO << "; MsgsSent-" << fMsgsToPm << "; MsgsRcvd-" << msgsRecvd <<
				"; BlockedFifoIn/Out-" << totalBlockedReadCount << 
				"/" << totalBlockedWriteCount <<
				"; output size-" << fRidResults << endl <<
				"\tMsgBytesIn-"  << msgBytesInKB  << "KB"
				"; MsgBytesOut-" << fMsgBytesOut  << "B" << endl    <<
				"\t1st read " << dlTimes.FirstReadTimeString() <<
				"; EOI " << dlTimes.EndOfInputTimeString() << "; runtime-" <<
				JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(),dlTimes.FirstReadTime())<< "s" << endl
				<< "\tJob completion status " << status() << endl;
				
			logEnd(logStr.str().c_str());

			syslogReadBlockCounts(16,    // exemgr subsystem
				fPhysicalIO,             // # blocks read from disk
				fCacheIO,                // # blocks read from cache
				0);                      // # casual partition block hits
			syslogProcessingTimes(16,    // exemgr subsystem
				dlTimes.FirstReadTime(),   // first datalist read
				dlTimes.LastReadTime(),    // last  datalist read
				dlTimes.FirstInsertTime(), // first datlist write
				dlTimes.EndOfInputTime()); // last (endOfInput) datalist write
			syslogEndStep(16,            // exemgr subsystem
				totalBlockedReadCount,   // blocked datalist input
				totalBlockedWriteCount,  // blocked datalist output
				fMsgBytesIn,             // incoming msg byte count
				fMsgBytesOut);           // outgoing msg byte count
			fExtendedInfo += toString() + logStr.str();
			formatMiniStats();
		}
	}

	// Bug 3136, let mini stats to be formatted if traceOn.
	dlp->endOfInput();
}

const string pDictionaryScan::toString() const
{
	ostringstream oss;
	oss << "pDictionaryScan ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId
		<< " st:" << fStepId << " alias: " << (fAlias.length() ? fAlias : "none")
		<< " tb/col:" << fTableOid << "/" << fOid;
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

void pDictionaryScan::formatMiniStats()
{
	ostringstream oss;
	oss << "DSS "
		<< "PM "
		<< fAlias << " "
		<< fTableOid << " (" << fName << ") "
		<< fPhysicalIO << " "
		<< fCacheIO << " "
	    << "- "
	    << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << " "
		<< fRidResults << " ";
	fMiniInfo += oss.str();
}


void pDictionaryScan::addFilter(const Filter* f)
{
    if (NULL != f)
    	fFilters.push_back(f);
}


void pDictionaryScan::appendFilter(const std::vector<const execplan::Filter*>& fs)
{
    fFilters.insert(fFilters.end(), fs.begin(), fs.end());
}


void pDictionaryScan::appendFilter(const messageqcpp::ByteStream& filter, unsigned count)
{
    fFilterString += filter;
    fFilterCount += count;
}


void pDictionaryScan::serializeEqualityFilter()
{
	ByteStream msg;
	ISMPacketHeader ism;
	uint i;
	vector<string> empty;

	memset(&ism, 0, sizeof(ISMPacketHeader));
	ism.Command  = DICT_CREATE_EQUALITY_FILTER;
	msg.load((uint8_t *) &ism, sizeof(ISMPacketHeader));
	msg << uniqueID;
	msg << (uint32_t) equalityFilter.size();
	for (i = 0; i < equalityFilter.size(); i++)
		msg << equalityFilter[i];
	try {
		fDec->write(uniqueID, msg);
	}
	catch (const IDBExcept &e) {
		abort();
		cerr << "pDictionaryScan::serializeEqualityFilter() caught: " << e.what() << endl;
		catchHandler(e.what(), e.errorCode(), fErrorInfo, fSessionId);
	}
	catch (const std::exception& e)
	{
		abort();
		cerr << "pDictionaryScan::serializeEqualityFilter() caught: " << e.what() << endl;
		catchHandler(e.what(), ERR_DICTIONARY_SCAN, fErrorInfo, fSessionId);
	}
	catch (...)
	{
		abort();
		cerr << "pDictionaryScan::serializeEqualityFilter() caught unknown exception" << endl;
		catchHandler("pDictionaryScan::serializeEqualityFilter() caught unknown exception",
						ERR_DICTIONARY_SCAN, fErrorInfo, fSessionId);
	}
	empty.swap(equalityFilter);
}

void pDictionaryScan::destroyEqualityFilter()
{
	ByteStream msg;
	ISMPacketHeader ism;

	memset(&ism, 0, sizeof(ISMPacketHeader));
	ism.Command  = DICT_DESTROY_EQUALITY_FILTER;
	msg.load((uint8_t *) &ism, sizeof(ISMPacketHeader));
	msg << uniqueID;
	try {
		fDec->write(uniqueID, msg);
	}
	catch (const IDBExcept &e) {
		abort();
		catchHandler(e.what(), e.errorCode(), fErrorInfo, fSessionId);
	}
	catch (const std::exception& e)
	{
		abort();
		cerr << "pDictionaryScan::destroyEqualityFilter() caught: " << e.what() << endl;
		catchHandler(e.what(), ERR_DICTIONARY_SCAN, fErrorInfo, fSessionId);
	}
	catch (...)
	{
		abort();
		cerr << "pDictionaryScan::destroyEqualityFilter() caught unknown exception" << endl;
		catchHandler("pDictionaryScan::destroyEqualityFilter() caught unknown exception",
						ERR_DICTIONARY_SCAN, fErrorInfo, fSessionId);
	}
}

void pDictionaryScan::abort()
{
	JobStep::abort();
	if (fDec)
		fDec->shutdownQueue(uniqueID);
}




}   //namespace
// vim:ts=4 sw=4:

