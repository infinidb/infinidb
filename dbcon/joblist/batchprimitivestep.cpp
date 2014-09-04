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
*   $Id: batchprimitivestep.cpp 8476 2012-04-25 22:28:15Z xlou $
*
*
***********************************************************************/

#include <unistd.h>
#include <cassert>
#include <sstream>
#include <iomanip>
#include <algorithm>
using namespace std;

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
using namespace boost;

#include "bpp-jl.h"
#include "distributedenginecomm.h"
#include "elementtype.h"
#include "unique32generator.h"
using namespace joblist;

#include "messagequeue.h"
using namespace messageqcpp;
#include "configcpp.h"
using namespace config;

#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
#include "errorcodes.h"
#include "liboamcpp.h"
using namespace logging;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "brm.h"
using namespace BRM;

#include "oamcache.h"
using namespace utils;

// #define DEBUG 1


// unname namespace for BatchPrimitiveStep profiling
namespace
{
enum
{
	sendPrimMsg_c = 0,
	sendWork_Scan,
	tableBand_c,
	getLBID_c,
	write_c,
	sendLock1_c,
	sendLock2_c,
	nextAddFilters1_c,
	nextAddFilters2_c,
	numOfSendCheckPoints,

	recvCheckpointOffset = numOfSendCheckPoints,
	recvPrimMsg_c = 0,
	recvWork_c,
	recvLock_c,
	recvWait_c,
	read_c,
	insert1_c,
	insert2_c,
	numOfRecvCheckPoints,

	TotalCheckPoints = numOfSendCheckPoints + numOfRecvCheckPoints
};

// !!these strings are for stats output. should be in the same order as the enum above. !!
static string CheckPoints[TotalCheckPoints] = {
	"betweenFirstSendAndFirstRead",
	"totalScanSend",
	"getTableBandUsed",
	"getLBID in send",
	"fDec->write in send",
	"mutex_lock_1 in send",
	"mutex_lock_2 in send",
	"1st ridList->next in addFilters",
	"ridList->next in addFilters",

	"receivePrimitiveMessages",
	"receive work",
	"mutex_lock in receive",
	"cond_wait in receive",
	"fDec->read in receive",
	"dlp->insert_1 in receive",
	"dlp->insert_2 in receive"
	};
}
#endif

extern boost::mutex fileLock_g;

namespace
{
const uint LOGICAL_EXTENT_CONVERTER = 10;  		// 10 + 13.  13 to convert to logical blocks,
												// 10 to convert to groups of 1024 logical blocks
void logDiskIoInfo(uint64_t stepId, const AnyDataListSPtr& spdl)
{
	boost::mutex::scoped_lock lk(fileLock_g);
	ofstream umDiskIoFile("/var/log/Calpont/trace/umdiskio.log", ios_base::app);

	CalpontSystemCatalog::OID oid;
	uint64_t maxBuckets = 0;
	list<DiskIoInfo>* infoList = NULL;
	string bkt("tbd");
	//BucketDL<ElementType>*       bdl  = spdl->bucketDL();
	//BucketDL<StringElementType>* sbdl = spdl->stringBucketDL();
	//ZDL<ElementType>*            zdl  = spdl->zonedDL();
	//ZDL<StringElementType>*      szdl = spdl->stringZonedDL();
	TupleBucketDataList* tbdl = spdl->tupleBucketDL();

	if (tbdl)
	{
		maxBuckets = tbdl->bucketCount();
		oid = tbdl->OID();
	}
	else
	{
		// not logged for now.
		return;
	}

	for (uint64_t i = 0; i < maxBuckets; i++)
	{
		infoList = &(tbdl->diskIoInfoList(i));

		for (list<DiskIoInfo>::iterator j = infoList->begin(); j != infoList->end(); j++)
		{
			boost::posix_time::time_duration td = j->fEnd - j->fStart;
			umDiskIoFile << setfill('0')
				<< "st" << setw(2) << stepId << "oid" << oid << bkt << setw(3) << i 
				<< (j->fWrite ? " writes " : " reads  ") << setw(7) << setfill(' ')
				<< j->fBytes << " bytes, at " << j->fStart << " duration "
				<< td.total_microseconds() << " mcs @ "
				<< (j->fBytes/td.total_microseconds()) << "MB/s" << endl;
		}
	}

	streampos curPos = umDiskIoFile.tellp( );
	umDiskIoFile.close();

	// move the current file to bak when size above .5 G, so total log is 1 G
	if (curPos > 0x20000000)
	{
		(void)system("/bin/mv /var/log/Calpont/trace/umdiskio.log /var/log/Calpont/trace/umdiskio.bak");
	}
}
}

/** Debug macro */
#define THROTTLE_DEBUG 0
#if THROTTLE_DEBUG
#define THROTTLEDEBUG std::cout
#else
#define THROTTLEDEBUG if (false) std::cout
#endif
namespace joblist
{
  //const uint32_t defaultProjectBlockReqThreshold =  256;
  //const uint32_t defaultScanBlockThreshold = 10000;
struct BatchPrimitiveStepPrimitive
{
	BatchPrimitiveStepPrimitive(BatchPrimitiveStep* batchPrimitiveStep) : fBatchPrimitiveStep(batchPrimitiveStep)
	{}
	BatchPrimitiveStep *fBatchPrimitiveStep;
	void operator()()
	{
		try
		{
			fBatchPrimitiveStep->sendPrimitiveMessages();
		}
		catch(std::exception& re)
		{
			cerr << "BatchPrimitiveStep: send thread threw an exception: " << re.what() << 
					"\t" << this << endl;
			catchHandler(re.what());
		}
		catch(...)
		{
			string msg("BatchPrimitiveStep: send thread threw an unknown exception ");
			catchHandler(msg);
			cerr << msg << this << endl;
		}
	}
};

struct BatchPrimitiveStepAggregator
{
	BatchPrimitiveStepAggregator(BatchPrimitiveStep* batchPrimitiveStep) : fBatchPrimitiveStepCol(batchPrimitiveStep)
	{}
	BatchPrimitiveStep *fBatchPrimitiveStepCol;
	void operator()()
	{
		try
		{
		   fBatchPrimitiveStepCol->receivePrimitiveMessages();
		}
		catch(std::exception& re)
		{
			cerr << fBatchPrimitiveStepCol->toString() << ": recv thread threw an exception: "
				 << re.what() << endl;
			catchHandler(re.what());
		}
		catch(...)
		{
			string msg("BatchPrimitiveStep: recv thread threw an unknown exception ");
			cerr << fBatchPrimitiveStepCol->toString() << msg << endl;
			catchHandler(msg);
		}
	}
};

struct BatchPrimitiveStepAggregatores
{
	BatchPrimitiveStepAggregatores( BatchPrimitiveStep* batchPrimitiveStep, uint64_t index) :
							fBatchPrimitiveStepCols(batchPrimitiveStep), fThreadId(index)
	{}
	BatchPrimitiveStep *fBatchPrimitiveStepCols;
	uint64_t fThreadId;

	void operator()()
	{
		try
		{
			fBatchPrimitiveStepCols->receiveMultiPrimitiveMessages(fThreadId);
		}
		catch(std::exception& re)
		{
			cerr << fBatchPrimitiveStepCols->toString() << ": receive thread threw an exception: "
				 << re.what() << endl;
			catchHandler(re.what());
		}
		catch(...)
		{
			string msg("BatchPrimitiveStep: recv thread threw an unknown exception ");
			cerr << fBatchPrimitiveStepCols->toString() << msg << endl;
			catchHandler(msg);
		}
	}
};


//------------------------------------------------------------------------------
// Initialize configurable parameters
//------------------------------------------------------------------------------
void BatchPrimitiveStep::initializeConfigParms()
{
	string        strVal;

	//...Get the tuning parameters that throttle msgs sent to primproc
	//...fFilterRowReqLimit puts a cap on how many rids we will request from
	//...    primproc, before pausing to let the consumer thread catch up.
	//...    Without this limit, there is a chance that PrimProc could flood
	//...    ExeMgr with thousands of messages that will consume massive
	//...    amounts of memory for a 100 gigabyte database.
	//...fFilterRowReqThreshold is the level at which the number of outstanding
	//...    rids must fall below, before the producer can send more rids.

	//These could go in constructor
	numDBRoots = fRm.getDBRootCount();
	fRequestSize = fRm.getJlRequestSize();
	fMaxOutstandingRequests = fRm.getJlMaxOutstandingRequests();
	fProcessorThreadsPerScan = fRm.getJlProcessorThreadsPerScan();
	if ( fRequestSize >= fMaxOutstandingRequests )
		fRequestSize = 1;
	fNumThreads = fRm.getJlNumScanReceiveThreads();	

	fProducerThread.reset(new SPTHD[fNumThreads]);
	//pthread_mutex_init(&mutex, NULL);
	//pthread_mutex_init(&dlMutex, NULL);
	//pthread_mutex_init(&cpMutex, NULL);
	//pthread_cond_init(&condvar, NULL);
	//pthread_cond_init(&condvarWakeupProducer, NULL);
}

BatchPrimitiveStep::BatchPrimitiveStep(const JobStepAssociation& inputJobStepAssociation,
	const JobStepAssociation& outputJobStepAssociation,
	DistributedEngineComm* dec, ResourceManager& rm):
	fInputJobStepAssociation(inputJobStepAssociation),
	fOutputJobStepAssociation(outputJobStepAssociation),
	fDec(dec),
	msgsSent(0),
	msgsRecvd(0),
	finishedSending(false),
	sendWaiting(false),
	fRm(rm), fUpdatedEnd(false)
{
	throw logic_error("BPS's are initialized from other JobSteps");

	fBPP.reset(new BatchPrimitiveProcessorJL(fRm));
	initializeConfigParms ( );
	rowsReturned = 0;
	fNumBlksSkipped = 0;
	fMsgBytesIn = 0;
	fMsgBytesOut = 0;
	fBlockTouched = 0;
	fMsgBppsToPm = 0;
	fIsProjectionOnly = false;
	fPhysicalIO = 0;
	fCacheIO = 0;
	recvWaiting = 0;
	isFilterFeeder = false;
	fSwallowRows = false;
	die = false;
}

BatchPrimitiveStep::BatchPrimitiveStep(const pColStep& rhs) : fRm (rhs.fRm), fUpdatedEnd(false)
{
	fInputJobStepAssociation = rhs.inputAssociation();
	fOutputJobStepAssociation = rhs.outputAssociation();
	fDec = rhs.dec();
	fSessionId = rhs.sessionId();
	fTxnId = rhs.txnId();
	fVerId = rhs.verId();
	fStepId = rhs.stepId();
	fStatementId = rhs.statementId();
	fFilterCount = rhs.filterCount();
	fFilterString = rhs.filterString();
	isFilterFeeder = rhs.getFeederFlag();
	fOid = rhs.oid();
	fTableOid = rhs.tableOid();
	fBOP = rhs.BOP();
	extents = rhs.extents;
	if (fOid>3000)
	lbidList = rhs.lbidList;
	rpbShift = rhs.rpbShift;
	divShift = rhs.divShift;
	modMask = rhs.modMask;
	numExtents = rhs.numExtents;
	ridsRequested = 0;
	ridsReturned = 0;
	rowsReturned = 0;
	recvExited = 0;
	msgsSent = 0;
	msgsRecvd = 0;
	fMsgBytesIn = 0;
	fMsgBytesOut = 0;
	fBlockTouched = 0;
	fMsgBppsToPm = 0;
	recvWaiting = 0;
	fStepCount = 1;
	fColType = rhs.colType();
	extentSize = (fRm.getExtentRows()*fColType.colWidth)/BLOCK_SIZE;
	alias(rhs.alias());
	view(rhs.view());
	name(rhs.name());
	fColWidth = fColType.colWidth;
	fBPP.reset(new BatchPrimitiveProcessorJL(fRm));
	initializeConfigParms ( );
	fBPP->setSessionID( fSessionId );
	fBPP->setStepID(fStepId);
	fBPP->setVersionNum( fVerId );
	fBPP->setTxnID( fTxnId );
	fTraceFlags = rhs.fTraceFlags;
	fBPP->setTraceFlags( fTraceFlags );
//	if (fOid>=3000)
//		cout << "BPS:initalized from pColStep. fSessionId=" << fSessionId << endl;
	finishedSending = fIsProjectionOnly = sendWaiting = false;
	fNumBlksSkipped = 0;
	fPhysicalIO = 0;
	fCacheIO = 0;
	BPPIsAllocated = false;
	uniqueID = UniqueNumberGenerator::instance()->getUnique32();
	fBPP->setUniqueID(uniqueID);
	fCardinality = rhs.cardinality();
	doJoin = false;
	fRunExecuted = false;
	fSwallowRows = false;
	
	// @1098 initialize scanFlags to be true
	scanFlags.assign(numExtents, true);
	die = false;
}

BatchPrimitiveStep::BatchPrimitiveStep(const pColScanStep& rhs) : fRm (rhs.fRm), fUpdatedEnd(false)
{
	fInputJobStepAssociation = rhs.inputAssociation();
	fOutputJobStepAssociation = rhs.outputAssociation();
	fDec = rhs.dec();
	fSessionId = rhs.sessionId();
	fTxnId = rhs.txnId();
	fVerId = rhs.verId();
	fStepId = rhs.stepId();
	fStatementId = rhs.statementId();
	fFilterCount = rhs.filterCount();
	fFilterString = rhs.filterString();
	isFilterFeeder = rhs.getFeederFlag();
	fOid = rhs.oid();
	fTableOid = rhs.tableOid();
	fBOP = rhs.BOP();
	lbidRanges = rhs.lbidRanges;
	hwm = rhs.hwm;
	extents = rhs.extents;
	divShift = rhs.divShift;
	numExtents = rhs.numExtents;
	msgsSent = 0;
	msgsRecvd = 0;
	ridsReturned = 0;
	ridsRequested = 0;
	rowsReturned = 0;
	fNumBlksSkipped = 0;
	fMsgBytesIn = 0;
	fMsgBytesOut = 0;
	fBlockTouched = 0;
	fMsgBppsToPm = 0;
	recvWaiting = 0;
	fSwallowRows = false;
	fStepCount = 1;
	fColType = rhs.colType();
	alias(rhs.alias());
	view(rhs.view());
	name(rhs.name());
	
	extentSize = (fRm.getExtentRows()*fColType.colWidth)/BLOCK_SIZE;
	fColWidth = fColType.colWidth;
	lbidList=rhs.lbidList;
			
	fLogger = rhs.logger();
	finishedSending = fIsProjectionOnly = sendWaiting = false;
	firstRead = true;
	fSwallowRows = false;
	recvExited = 0;
	fBPP.reset(new BatchPrimitiveProcessorJL(fRm));
	initializeConfigParms ( );
	fBPP->setSessionID( fSessionId );
	fBPP->setVersionNum( fVerId );
	fBPP->setTxnID( fTxnId );
	fTraceFlags = rhs.fTraceFlags;
	fBPP->setTraceFlags( fTraceFlags );
//	if (fOid>=3000)
//		cout << "BPS:initalized from pColScanStep. fSessionId=" << fSessionId << endl;
	fBPP->setStepID( fStepId );
	fPhysicalIO = 0;
	fCacheIO = 0;
	BPPIsAllocated = false;
	uniqueID = UniqueNumberGenerator::instance()->getUnique32();
	fBPP->setUniqueID(uniqueID);
	fCardinality = rhs.cardinality();
	doJoin = false;
	fRunExecuted = false;
	// @1098 initialize scanFlags to be true
	scanFlags.assign(numExtents, true);
	die = false;
}

BatchPrimitiveStep::BatchPrimitiveStep(const PassThruStep& rhs) : fRm (rhs.fRm), fUpdatedEnd(false)
{
	fInputJobStepAssociation = rhs.inputAssociation();
	fOutputJobStepAssociation = rhs.outputAssociation();
	fDec = rhs.dec();
	fSessionId = rhs.sessionId();
	fTxnId = rhs.txnId();
	fVerId = rhs.verId();
	fStepId = rhs.stepId();
	fStatementId = rhs.statementId();
	fFilterCount = 0;
	fBOP = 0;
	fOid = rhs.oid();
	fTableOid = rhs.tableOid();
	ridsReturned = 0;
	rowsReturned = 0;
	ridsRequested = 0;
	fMsgBytesIn = 0;
	fMsgBytesOut = 0;
	fBlockTouched = 0;
	fMsgBppsToPm = 0;
	recvExited = 0;
	msgsSent = 0;
	msgsRecvd = 0;
	recvWaiting = 0;
	fStepCount = 1;
	fColType = rhs.colType();
	alias(rhs.alias());
	view(rhs.view());
	name(rhs.name());
	fColWidth = fColType.colWidth;
	fBPP.reset(new BatchPrimitiveProcessorJL(fRm));
	initializeConfigParms ( );
	fBPP->setSessionID( fSessionId );
	fBPP->setStepID(fStepId);
	fBPP->setVersionNum( fVerId );
	fBPP->setTxnID( fTxnId );
	fTraceFlags = rhs.fTraceFlags;
	fBPP->setTraceFlags( fTraceFlags );
//	if (fOid>=3000)
//		cout << "BPS:initalized from PassThruStep. fSessionId=" << fSessionId << endl;
	
	finishedSending = fIsProjectionOnly = sendWaiting = false;
	fSwallowRows = false;
	fNumBlksSkipped = 0;
	fPhysicalIO = 0;
	fCacheIO = 0;
	BPPIsAllocated = false;
	uniqueID = UniqueNumberGenerator::instance()->getUnique32();
	fBPP->setUniqueID(uniqueID);
	doJoin = false;
	fRunExecuted = false;
	isFilterFeeder = false;
	die = false;
}

BatchPrimitiveStep::BatchPrimitiveStep(const pDictionaryStep& rhs) : fRm (rhs.fRm), fUpdatedEnd(false)
{
	fInputJobStepAssociation = rhs.inputAssociation();
	fOutputJobStepAssociation = rhs.outputAssociation();
	fDec = rhs.dec();
	fSessionId = rhs.sessionId();
	fTxnId = rhs.txnId();
	fVerId = rhs.verId();
	fStepId = rhs.stepId();
	fStatementId = rhs.statementId();
	fOid = rhs.oid();
	fTableOid = rhs.tableOid();
	msgsSent = 0;
	msgsRecvd = 0;
	ridsReturned = 0;
	rowsReturned = 0;
	ridsRequested = 0;
	fNumBlksSkipped = 0;
	fBlockTouched = 0;
	fMsgBytesIn = 0;
	fMsgBytesOut = 0;
	fMsgBppsToPm = 0;
	recvWaiting = 0;
	fSwallowRows = false;
	fStepCount = 1;
	//fColType = rhs.colType();
	alias( rhs.alias());
	view(rhs.view());
	name(rhs.name());

	fLogger = rhs.logger();
	finishedSending = fIsProjectionOnly = sendWaiting = false;
	recvExited = 0;
	fBPP.reset(new BatchPrimitiveProcessorJL(fRm));
	initializeConfigParms ( );
	fBPP->setSessionID( fSessionId );
//	if (fOid>=3000)
//		cout << "BPS:initalized from DictionaryStep. fSessionId=" << fSessionId << endl;
	fBPP->setStepID( fStepId );
	fBPP->setVersionNum( fVerId );
	fBPP->setTxnID( fTxnId );
	fTraceFlags = rhs.fTraceFlags;
	fBPP->setTraceFlags( fTraceFlags );
	fPhysicalIO = 0;
	fCacheIO = 0;
	BPPIsAllocated = false;
	uniqueID = UniqueNumberGenerator::instance()->getUnique32();
	fBPP->setUniqueID(uniqueID);
	fCardinality = rhs.cardinality();
	doJoin = false;
	fRunExecuted = false;
	isFilterFeeder = false;
	die = false;
}

BatchPrimitiveStep::~BatchPrimitiveStep()
{
	//pthread_mutex_destroy(&mutex);
	//pthread_mutex_destroy(&dlMutex);
	//pthread_mutex_destroy(&cpMutex);
	//pthread_cond_destroy(&condvar);
	//pthread_cond_destroy(&condvarWakeupProducer);
	if (fDec) {
		fDec->removeDECEventListener(this);
		if (BPPIsAllocated) {
			ByteStream bs;
			fBPP->destroyBPP(bs);
			try {
				fDec->write(bs);
			}
			catch(std::exception& re)
			{
				cerr << "~BatchPrimitiveStep: write(bs) exception: " << re.what() << endl;
				catchHandler(re.what());
			}
			catch(...)
			{
				string msg("~BatchPrimitiveStep: write(bs) threw an unknown exception ");
				catchHandler(msg);
			}
		}
		fDec->removeQueue(uniqueID);
	}
	
	if (traceOn())
	{
		for (uint64_t i = 0; i < fInputJobStepAssociation.outSize(); i++)
			logDiskIoInfo(fStepId, fInputJobStepAssociation.outAt(i));
		for (uint64_t i = 0; i < fOutputJobStepAssociation.outSize(); i++)
			logDiskIoInfo(fStepId, fOutputJobStepAssociation.outAt(i));
	}
}

void BatchPrimitiveStep::setBPP( JobStep* jobStep )
{ 
   	fCardinality = jobStep->cardinality();

	pColStep* pcsp = dynamic_cast<pColStep*>(jobStep);

	int colWidth = 0;
	if (pcsp != 0)
	{
		fBPP->addFilterStep( *pcsp ); 
		colWidth = (pcsp->colType()).colWidth;
		isFilterFeeder = pcsp->getFeederFlag();
//		if (fOid>=3000)
//			cout << "Adding pColStep BPS" << endl;
	}
	else 
	{
		pColScanStep* pcss = dynamic_cast<pColScanStep*>(jobStep);
		if (pcss != 0)	
		{	
			fBPP->addFilterStep( *pcss ); 
			colWidth = (pcss->colType()).colWidth;
			isFilterFeeder = pcss->getFeederFlag();
		}
		else 
		{
			pDictionaryStep* pdsp = dynamic_cast<pDictionaryStep*>(jobStep);
			if (pdsp != 0)	
			{	
				fBPP->addFilterStep( *pdsp );
				colWidth = (pdsp->colType()).colWidth;
			}
			else
			{
				FilterStep* pfsp = dynamic_cast<FilterStep*>(jobStep);
				if ( pfsp )
				{
					fBPP->addFilterStep( *pfsp );					
				}
			}

		}
	}
	
	if ( colWidth > fColWidth )
	{
		fColWidth = colWidth;
	}
}

void BatchPrimitiveStep::setProjectBPP( JobStep* jobStep1, JobStep* jobStep2 )
{ 
	int colWidth = 0;
	
	if ( jobStep2 != NULL )
	{
		pDictionaryStep* pdsp = 0;
		pColStep* pcsp = dynamic_cast<pColStep*>(jobStep1);	
		if (pcsp != 0)
		{	
			pdsp = dynamic_cast<pDictionaryStep*>(jobStep2);		
			fBPP->addProjectStep( *pcsp, *pdsp ); 
			//@Bug 961
			if ( !pcsp->isExeMgr())
				fBPP->setNeedRidsAtDelivery( true );
			colWidth = (pcsp->colType()).colWidth;
			projectOids.push_back(jobStep1->oid());
//			if (fOid>=3000)
//				cout << "Adding project step pColStep and pDictionaryStep to BPS" << endl;
		}
		else
		{
			PassThruStep* psth = dynamic_cast<PassThruStep*>(jobStep1);
			if (psth != 0)
			{	
				pdsp = dynamic_cast<pDictionaryStep*>(jobStep2);		
				fBPP->addProjectStep( *psth, *pdsp ); 
				//@Bug 961
				if ( !psth->isExeMgr())
					fBPP->setNeedRidsAtDelivery( true );
				projectOids.push_back(jobStep1->oid());
				colWidth = (psth->colType()).colWidth;
//				if (fOid>=3000)
//					cout << "Adding project step PassThruStep and pDictionaryStep to BPS" << endl;
			}
		}
	}	
	else 
	{
		pColStep* pcsp = dynamic_cast<pColStep*>(jobStep1);
		if (pcsp != 0)
		{
			fBPP->addProjectStep( *pcsp );
			//@Bug 961
			if ( !pcsp->isExeMgr())
					fBPP->setNeedRidsAtDelivery( true );
			colWidth = (pcsp->colType()).colWidth;
			projectOids.push_back(jobStep1->oid());
//			if (fOid>=3000)
//				cout << "Adding project step pColStep to BPS" << endl;
		}
		else 
		{
			PassThruStep* passthru = dynamic_cast<PassThruStep*>(jobStep1);
			if (passthru!= 0)	
			{	
				fBPP->addProjectStep( *passthru );
				//@Bug 961
				if ( !passthru->isExeMgr())
					fBPP->setNeedRidsAtDelivery( true );
				colWidth = (passthru->colType()).colWidth;
				projectOids.push_back(jobStep1->oid());
//				if (fOid>=3000)
//					cout << "Adding project step PassThruStep to BPS" << endl;
			}
			else
			{
				DeliveryStep* ds = dynamic_cast<DeliveryStep*>(jobStep1);
				if ( ds != 0 ) {
//					if ( fOid>=3000)
//						cout << "Adding DeliveryStep to BPS" << endl;
					fBPP->addDeliveryStep( *ds );
					fTableName = ds->tableName();
				}
			}
		}
	}

	if ( colWidth > fColWidth )
	{
		//if ( fOid>=3000)
			//cout << "colWidth resetted from " << fColWidth << " to " << colWidth << endl;
		fColWidth = colWidth;
	}
}

void BatchPrimitiveStep::addFilter(int8_t COP, int64_t value, uint8_t roundFlag)
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

			o << "BatchPrimitiveStep: CalpontSystemCatalog says OID " << fOid << 
				" has a width of " << fColType.colWidth;
			throw runtime_error(o.str());
	}

	fFilterCount++;
}

void BatchPrimitiveStep::storeCasualPartitionInfo()
{
	const vector<SCommand>& colCmdVec = fBPP->getFilterSteps();
	vector<ColumnCommandJL*> cpColVec;
	vector<int8_t> numExtentsToCheck;
	vector<SP_LBIDList> lbidListVec;
	ColumnCommandJL* colCmd = 0;
	uint firstColWidth = colCmdVec[0]->getWidth();
	
	// ZZ debug -- please keep it for a couple of releases
	// vector < vector<bool> > scanFs;
	// cout << "storeCasualPartitionInfo" << endl;
	
	for (uint i = 0; i < colCmdVec.size(); i++)
	{
		colCmd = dynamic_cast<ColumnCommandJL*>(colCmdVec[i].get());
		if (!colCmd)
			continue;
		SP_LBIDList tmplbidList(new LBIDList(0));
		if (tmplbidList->CasualPartitionDataType(colCmd->getColType().colDataType, colCmd->getWidth()))
		{
			lbidListVec.push_back(tmplbidList);
			cpColVec.push_back(colCmd);
			int8_t num = 1;
			if (firstColWidth > colCmd->getWidth())
				num = firstColWidth / colCmd->getWidth();
			else if (firstColWidth < colCmd->getWidth())
			{
				num = (colCmd->getWidth() / firstColWidth); //both ars are unsigned, result is unsigned
				num = -num; //make sure we get a signed result
			}
			numExtentsToCheck.push_back ( num );	 
	 		// debug
			// cout << "col " << i << " num=" << (int)num << endl;
			// vector<bool> scan;
			// scan.assign(numExtents, true);
			// scanFs.push_back(scan);
		}
	}
	
	//cout << "cp column number=" << cpColVec.size() << " 1st col extents size= " << scanFlags.size() 
	//	 << " fColWidth=" << firstColWidth << endl;
	
	if (cpColVec.size() == 0)
		return;

	const bool ignoreCP = ((fTraceFlags & CalpontSelectExecutionPlan::IGNORE_CP) != 0);
	
	for (uint idx=0; idx <numExtents; idx++)
	{
		for (uint i = 0; i < cpColVec.size(); i++)
		{
			scanFlags[idx] = false;
			colCmd = cpColVec[i];

			// if any extents that logically lined up with the first evaluate 
			// true then this colCmd gives a true.
			// If tracing says to ignore cp data, we still want to do all the work, then skip
			//@bug 1956. synchronize with tuple bps.
			if (colCmd->getExtents()[idx].partition.cprange.isValid != BRM::CP_VALID ||
					lbidListVec[i]->CasualPartitionPredicate(
										colCmd->getExtents()[idx].partition.cprange.lo_val,
										colCmd->getExtents()[idx].partition.cprange.hi_val,
										&(colCmd->getFilterString()),
										colCmd->getFilterCount(),
										colCmd->getColType(),
										colCmd->getBOP()) || ignoreCP )
			{
				scanFlags[idx] = true;
			}
			// if any colCmd gives a false, then this extent scanflag is false
			if (!scanFlags[idx])
			{
				//scanFs[i][idx] = false; // debug
				break;
			}			
		}
	}
	
	// debug
	/*for (uint i = 0; i < scanFs.size(); i++)
	{
		for (uint k = 0; k < scanFs[i].size(); k++)
			cout << scanFs[i][k] << " ";
		cout << endl;
	}
	for (uint i = 0; i < scanFlags.size(); i++)
		cout << scanFlags[i] << " ";
	cout << endl;*/
}

void BatchPrimitiveStep::startPrimitiveThread()
{
	pThread.reset(new boost::thread(BatchPrimitiveStepPrimitive(this)));
}

void BatchPrimitiveStep::startAggregationThreads()
{
	for (uint i = 0; i < fNumThreads; i++)
		fProducerThread[i].reset(new boost::thread(BatchPrimitiveStepAggregatores(this, i)));
}

void BatchPrimitiveStep::startAggregationThread()
{
	cThread.reset(new boost::thread(BatchPrimitiveStepAggregator(this)));
}

void BatchPrimitiveStep::serializeJoiner()
{
	ByteStream bs;
	bool more = true;

	/* false from nextJoinerMsg means it's the last msg,
		it's not exactly the exit condition*/
	while (more) {
		more = fBPP->nextJoinerMsg(bs);
		fDec->write(bs);
		bs.restart();
	}
}

void BatchPrimitiveStep::serializeJoiner(uint conn)
{
	ByteStream bs;
	bool more = true;

	/* false from nextJoinerMsg means it's the last msg,
		it's not exactly the exit condition*/
	while (more) {
		more = fBPP->nextJoinerMsg(bs);
		fDec->write(bs, conn);
		bs.restart();
	}
}

void BatchPrimitiveStep::run()
{
	fRunExecuted = true;
	
	if (traceOn())
	{
		syslogStartStep(16,                     // exemgr subsystem
			std::string("BatchPrimitiveStep")); // step name
	}
	
	//Set the input type
	size_t sz = fInputJobStepAssociation.outSize();
	if ( sz > 0 )
	{
		int ridListIdx = 0;
		if (sz > 1)
			ridListIdx = 1;

		const AnyDataListSPtr& dl = fInputJobStepAssociation.outAt(ridListIdx);
		DataList_t* dlp = dl->dataList();
		DataList<StringElementType>* strDlp = dl->stringDataList();
		if ( dlp )
		{
			setRidList(dlp);
			fifo = dl->fifoDL();
			if ( fifo )
				fInputType=FIFODATALIST;
			else
				fInputType=DATALIST;
		}
		else
		{
			setStrRidList( strDlp );
			strFifo = dl->stringDL();
			if ( strFifo )
				fInputType=STRINGFIFODATALIST;
			else
				fInputType=STRINGDATALIST;
		}
	}

	ByteStream bs;

	/* joiner needs to be set before the create msg is sent */
	if (doJoin && joiner->inPM())
		fBPP->useJoiner(joiner);
	fDec->addDECEventListener(this);
	fBPP->createBPP(bs);
	try {
		fDec->write(bs);
	}
	catch(std::exception& re)
	{
		cerr << "BatchPrimitiveStep::run() write(bs) in run: " << re.what() << endl;
		catchHandler(re.what());
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(batchPrimitiveStepErr);

		// bug 3141
		AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
		DataList_t* dlp = dl->dataList();
		StrDataList* strDlp = dl->stringDataList();
		FifoDataList *fifo = dl->fifoDL();
		StringFifoDataList *strFifo = dl->stringDL();
		TupleBucketDataList *tbdl = dl->tupleBucketDL();
		ZDL<ElementType> *zdl = dynamic_cast<ZDL<ElementType> *>(dlp);
		setEndOfInput(fifo, strFifo, strDlp, tbdl, zdl, dlp);		

		return;
	}
	catch(...)
	{
		string msg("BatchPrimitiveStep::run() write(bs) in run threw an unknown exception ");
		catchHandler(msg);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(batchPrimitiveStepErr);

		// bug 3141
		AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
		DataList_t* dlp = dl->dataList();
		StrDataList* strDlp = dl->stringDataList();
		FifoDataList *fifo = dl->fifoDL();
		StringFifoDataList *strFifo = dl->stringDL();
		TupleBucketDataList *tbdl = dl->tupleBucketDL();
		ZDL<ElementType> *zdl = dynamic_cast<ZDL<ElementType> *>(dlp);
		setEndOfInput(fifo, strFifo, strDlp, tbdl, zdl, dlp);		

		return;
	}

	BPPIsAllocated = true;

	try {
		startPrimitiveThread();
	
/*
		if (doJoin && joiner->inPM())
			cout << "BPS: inPM\n";
		else if (doJoin)
			cout << "BPS: inUM\n";
*/
		/* Do this in the send method maybe? */
		if (doJoin && joiner->inPM())
			serializeJoiner();
	}
	catch(std::exception& re)
	{
		cerr << "BatchPrimitiveStep: startPrimitive threw an exception: " << re.what() << endl;
		catchHandler(re.what());
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(batchPrimitiveStepErr);
		return;
	}
	catch(...)
	{
		string msg("BatchPrimitiveStep: startPrimitive threw an unknown exception ");
		catchHandler(msg);
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(batchPrimitiveStepErr);
		return;
	}

	// @bug 1293. setMultipleProducers() should be called only once.
	AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
	DataList_t* dlp = dl->dataList();
	TupleBucketDataList *tbdl = dl->tupleBucketDL();
	BucketDL<ElementType> *bucket = dynamic_cast<BucketDL<ElementType> *>(dlp);
	ZDL<ElementType> *zdl = dynamic_cast<ZDL<ElementType> *>(dlp);
	if (fNumThreads > 1 ) {
		if ( bucket || zdl )
		{
			dlp->setMultipleProducers(true);	  		
	  	}
	  	if ( tbdl )
	  	{
			tbdl->setMultipleProducers(true);
	  	}
	 }
				
	if ( fOutputType != TABLE_BAND )
	{
		try {
			startAggregationThreads();
		}
		catch(std::exception& re)
		{
			cerr << "BatchPrimitiveStep: startAggregation exception: " << re.what() << endl;
			catchHandler(re.what());
			if (fOutputJobStepAssociation.status() == 0)
				fOutputJobStepAssociation.status(batchPrimitiveStepErr);
		}
		catch(...)
		{
			string msg("BatchPrimitiveStep: startAggregation threw an unknown exception ");
			catchHandler(msg);
			if (fOutputJobStepAssociation.status() == 0)
				fOutputJobStepAssociation.status(batchPrimitiveStepErr);
		}
	}
}

void BatchPrimitiveStep::join()
{
	if (fRunExecuted)
	{
		if (fOutputType == TABLE_BAND && msgsRecvd < msgsSent) {
			// wake up the sending thread, it should drain the input dl and exit
			mutex.lock(); //pthread_mutex_lock(&mutex);
			condvarWakeupProducer.notify_all(); //pthread_cond_broadcast(&condvarWakeupProducer);
			mutex.unlock(); //pthread_mutex_unlock(&mutex);
			// wait for the PM to return all results before destroying the BPPs
			while (msgsRecvd + fDec->size(uniqueID) < msgsSent)
				sleep(1);
		}

		if (pThread.get())
			pThread->join();
		if ( fOutputType != TABLE_BAND && !fOutputJobStepAssociation.status()  )
		{
			for (uint i = 0; i < fNumThreads; i++)
				fProducerThread[i]->join();
		}

		if (BPPIsAllocated) {
			ByteStream bs;
			fDec->removeDECEventListener(this);
			fBPP->destroyBPP(bs);
			try {
				fDec->write(bs);
			}
			catch(std::exception& re)
			{
				cerr << "BatchPrimitiveStep: write(bs) in join: " << re.what() << endl;
				catchHandler(re.what());
			}
			catch(...)
			{
				string msg("BatchPrimitiveStep: write(bs) in join threw an unknown exception ");
				catchHandler(msg);
			}
			BPPIsAllocated = false;
			fDec->removeQueue(uniqueID);
			joiner.reset();
		}
	}
}

void BatchPrimitiveStep::sendError(uint16_t status)
{
	ByteStream msgBpp;
	fBPP->setCount ( 1 );
	fBPP->setStatus( status );
	fBPP->runErrorBPP(msgBpp);
	fMsgBytesOut += msgBpp.lengthWithHdrOverhead();
	fDec->write(msgBpp);
	fMsgBppsToPm++;
	fBPP->reset();
	msgsSent++;
	finishedSending = true;
	condvar.notify_all(); //pthread_cond_broadcast(&condvar);
	condvarWakeupProducer.notify_all(); //pthread_cond_broadcast(&condvarWakeupProducer);
}


void BatchPrimitiveStep::setRidList(DataList<ElementType> *dl)
{
	ridList = dl;
}

void BatchPrimitiveStep::setStrRidList(DataList<StringElementType> *strDl)
{
	strRidList = strDl;
}

void BatchPrimitiveStep::setBOP(int8_t b)
{
	fBOP = b;
}

void BatchPrimitiveStep::setOutputType(BPSOutputType outputType)
{
	fOutputType = outputType;
	fBPP->setOutputType(outputType );
}

void BatchPrimitiveStep::setBppStep()
{
	fBPP->setStepID( fStepId );
//	if ( fOid >= 3000)
//		cout << "fBPP: set step to " << fStepId << endl;
}

int BatchPrimitiveStep::getIterator()
{
	int it=-1;
	switch ( fInputType ){
		case DATALIST: it = ridList->getIterator(); break;
		case FIFODATALIST: it = fifo->getIterator(); break;
		case STRINGDATALIST: it = strRidList->getIterator(); break;
		case STRINGFIFODATALIST: it = strFifo->getIterator(); break;
		default:
			throw runtime_error("BatchPrimitiveStep: invalid input data type!");
	}
	return it;
}

inline bool BatchPrimitiveStep::getNextElement(const int it)
{
	bool more = false;
	switch ( fInputType ){
		case DATALIST: 
			more = ridList->next(it, &e);					
			rw.count = 1; 
			break;
		case FIFODATALIST: 
			more = fifo->next(it, &rw); 
			break;
		case STRINGDATALIST: 
			more = strRidList->next(it, &strE);
			strRw.count = 1; 
			break;
		case STRINGFIFODATALIST:
			more = strFifo->next(it, &strRw);
			break;
		default:
			throw runtime_error("BatchPrimitiveStep: invalid input data type!");
	}
	return more;
}

inline uint64_t BatchPrimitiveStep::getAbsRid( uint64_t i )
{
	uint64_t absRid;
	switch ( fInputType ){
		case DATALIST: 
			absRid = e.first;					 
			break;
		case FIFODATALIST: 
			absRid = rw.et[i].first; 
			break;
		case STRINGDATALIST: 
			absRid	= strE.first;				
			break;
		case STRINGFIFODATALIST:
			absRid	= strRw.et[i].first;
			break;
		default:
			throw runtime_error("BatchPrimitiveStep: invalid input data type!");
	}
	return absRid;
}

inline uint64_t BatchPrimitiveStep::getRowCount()
{
	uint64_t rwCount;
	if ( fInputType == DATALIST || fInputType == FIFODATALIST )
		rwCount = rw.count;
	else
		rwCount = strRw.count;
	
	return rwCount;
}

inline void BatchPrimitiveStep::addElementToBPP(uint64_t i, uint dbroot)
{
	switch ( fInputType ){
		case DATALIST: 	
			fBPP->addElementType(e, dbroot);
			break;
		case FIFODATALIST: 
			fBPP->addElementType(rw.et[i], dbroot);
			break;
		case STRINGDATALIST: 
			fBPP->addElementType(strE, dbroot);
			break;
		case STRINGFIFODATALIST:
			fBPP->addElementType(strRw.et[i], dbroot);
			break;
			default:
				throw runtime_error("BatchPrimitiveStep: invalid input data type!");
	}
}

const TableBand BatchPrimitiveStep::nextBand()
{
	ByteStream bs;
	TableBand tb;

	nextBand(bs);
	tb.unserialize(bs);
	return tb;
}

 uint BatchPrimitiveStep::nextBand ( messageqcpp::ByteStream &bs )
{
	int64_t min;
	int64_t max;
	uint64_t lbid;
	uint32_t cachedIO;
	uint32_t physIO;
	uint32_t touchedBlocks;
	bool validCPData;
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	uint rows = 0;

again:
try 
{
	while ( msgsRecvd == msgsSent && !finishedSending 
	  && !fInputJobStepAssociation.status())
		usleep ( 1000 );

	if ((msgsRecvd == msgsSent && finishedSending) || fInputJobStepAssociation.status())
	{
		bsIn.reset(new ByteStream());
//		if ( fOid>=3000)
//			cout << "BPS: sending last band for Table OID " << fTableOid << endl;
		if ( fOid>=3000 ) dlTimes.setLastReadTime();
		if ( fOid>=3000 ) dlTimes.setEndOfInputTime();
		if ( fBPP->getStatus() || 0 < fOutputJobStepAssociation.status() )
		{
			rows = fBPP->getErrorTableBand(fOutputJobStepAssociation.status(), &bs );
			if (!fOutputJobStepAssociation.status())
				fOutputJobStepAssociation.status(fBPP->getStatus());
		}
		else
		{
			rows = fBPP->getTableBand ( *bsIn, &bs, &validCPData, &lbid, &min, &max, &cachedIO, &physIO, &touchedBlocks );
		}
		ByteStream dbs;
		fDec->removeDECEventListener(this);
		fBPP->destroyBPP(dbs);
		fDec->write(dbs);
		BPPIsAllocated = false;
		fDec->removeQueue(uniqueID);
		joiner.reset();

		if ( fOid>=3000 )
		{
			time_t t = time ( 0 );
			char timeString[50];
			ctime_r ( &t, timeString );
			timeString[strlen ( timeString )-1 ] = '\0';
			FifoDataList* pFifo = 0;
			uint64_t totalBlockedReadCount = 0;
			uint64_t totalBlockedWriteCount = 0;

			//...Sum up the blocked FIFO reads for all input associations
			size_t inDlCnt  = fInputJobStepAssociation.outSize();
			for ( size_t iDataList=0; iDataList<inDlCnt; iDataList++ )
			{
				pFifo = fInputJobStepAssociation.outAt ( iDataList )->fifoDL();
				if ( pFifo )
				{
					totalBlockedReadCount += pFifo->blockedReadCount();
				}
			}

			//...Sum up the blocked FIFO writes for all output associations
			size_t outDlCnt = fOutputJobStepAssociation.outSize();
			for ( size_t iDataList=0; iDataList<outDlCnt; iDataList++ )
			{
				pFifo = fOutputJobStepAssociation.outAt ( iDataList )->fifoDL();
				if ( pFifo )
				{
					totalBlockedWriteCount += pFifo->blockedWriteCount();
				}
			}

			//...Roundoff msg byte counts to nearest KB for display
			uint64_t msgBytesInKB  = fMsgBytesIn >> 10;
			uint64_t msgBytesOutKB = fMsgBytesOut >> 10;
			if (fMsgBytesIn & 512)
				msgBytesInKB++;
			if (fMsgBytesOut & 512)
				msgBytesOutKB++;

			if ( traceOn() )
			{
				ostringstream logStr;
				logStr << "ses:" << fSessionId << " st: " << fStepId <<
				" finished at "<< timeString << "; total rows returned-" << rowsReturned <<
				" physicalIO-" << fPhysicalIO << " cachedIO-" << fCacheIO <<
				"; MsgsSent-" << fMsgBppsToPm << "; MsgsRvcd-" << msgsRecvd <<
				"; BlocksTouched-"<< fBlockTouched <<
				"; BlockedFifoIn/Out-" << totalBlockedReadCount <<
				"/" << totalBlockedWriteCount <<
				"; output size-" << rowsReturned << endl <<
				"\tPartitionBlocksEliminated-" << fNumBlksSkipped <<
				"; MsgBytesIn-"  << msgBytesInKB  << "KB" <<
				"; MsgBytesOut-" << msgBytesOutKB << "KB" << endl  <<
				"\t1st read " << dlTimes.FirstReadTimeString() <<
				";\n\tJob completion status " << fOutputJobStepAssociation.status() << endl;
				logEnd ( logStr.str().c_str() );
				syslogReadBlockCounts ( 16,  // exemgr sybsystem
										fPhysicalIO,             // # blocks read from disk
										fCacheIO,                // # blocks read from cache
										fNumBlksSkipped );       // # casual partition block hits
				syslogProcessingTimes ( 16,  // exemgr subsystem
										dlTimes.FirstReadTime(),   // first datalist read
										dlTimes.LastReadTime(),    // last  datalist read
										dlTimes.FirstInsertTime(), // first datalist write
										dlTimes.EndOfInputTime() ); // last (endOfInput) datalist write
				syslogEndStep ( 16,                      // exemgr subsystem
								totalBlockedReadCount,   // blocked datalist input
								totalBlockedWriteCount,  // blocked datalist output
								fMsgBytesIn,             // incoming msg byte count
								fMsgBytesOut );          // outgoing msg byte count
			}
		}

		if ( fOid>=3000 && ( ffirstStepType == SCAN ) ) {
			cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
			lbidList->UpdateAllPartitionInfo();
			cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
		}
	}
	else
	{
		if ( fOid>=3000 && dlTimes.FirstReadTime().tv_sec ==0 )
			dlTimes.setFirstReadTime();
		fDec->read(uniqueID, bsIn );
		/* XXXPAT: Need to check for 0-length BS here */
		firstRead = false;
		fMsgBytesIn += bsIn->lengthWithHdrOverhead();
		mutex.lock(); //pthread_mutex_lock(&mutex);
		msgsRecvd++;
		//@Bug 1424, 1298
		if ( (sendWaiting) && ( (msgsSent - msgsRecvd) <= (fMaxOutstandingRequests * (extentSize / fColWidth)) ) )
		{
			condvarWakeupProducer.notify_one(); //pthread_cond_signal(&condvarWakeupProducer);
			THROTTLEDEBUG << "nextBand wakes up sending side .. " << "  msgsSent: " << msgsSent << "  msgsRecvd = " << msgsRecvd << endl;
		}
		mutex.unlock(); //pthread_mutex_unlock(&mutex);

		ISMPacketHeader *hdr = (ISMPacketHeader*)bsIn->buf();
		
		// @bug 488. 0 length bs indicated PM connection lost
		if (bsIn->length() == 0 || 0 < hdr->Status || 0 < fBPP->getStatus() || 0 < fOutputJobStepAssociation.status() )
		{
			if (bsIn->length() == 0)
				fOutputJobStepAssociation.status(primitiveServerErr);
			if (!fOutputJobStepAssociation.status())
				fOutputJobStepAssociation.status(0 < hdr->Status ? hdr->Status : fBPP->getStatus());
			mutex.lock(); //pthread_mutex_lock(&mutex);
			if (!fSwallowRows) 
			{
				msgsRecvd = msgsSent;
				finishedSending = true;
				rows = 0;  //send empty message to indicate finished.
			}
			die = true;
			condvar.notify_all(); //pthread_cond_broadcast(&condvar);
			condvarWakeupProducer.notify_all(); //pthread_cond_broadcast(&condvarWakeupProducer);
			//cout << "BPS receive signal " << fStepId << endl;
			mutex.unlock(); //pthread_mutex_unlock(&mutex);
		}
		else 
		{
			rows = fBPP->getTableBand ( *bsIn, &bs, &validCPData, &lbid, &min, &max, &cachedIO, &physIO, &touchedBlocks );
		}
		fPhysicalIO += physIO;
		fCacheIO += cachedIO;
		fBlockTouched += touchedBlocks;

		if ( fOid>=3000 && ( ffirstStepType == SCAN ) && validCPData )
		{
			cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
			lbidList->UpdateMinMax ( min, max, lbid, fColType.colDataType );
			cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
		}
		rowsReturned += rows;
		if ( rows == 0 )
		{
			bs.restart();
			goto again;
		}
	}
// 	return rows;
}//try
catch(const LargeDataListExcept& ex)
{
	catchHandler(ex.what(), fSessionId);
	fOutputJobStepAssociation.status(batchPrimitiveStepLargeDataListFileErr);
 	return fBPP->getErrorTableBand(batchPrimitiveStepErr, &bs );
}
catch(const std::exception& ex)
{
	catchHandler(ex.what(), fSessionId);
	fOutputJobStepAssociation.status(batchPrimitiveStepErr);
	return fBPP->getErrorTableBand(batchPrimitiveStepErr, &bs );
}
catch(...)
{
	catchHandler("BatchPrimitiveStep next band caught an unknown exception", fSessionId);
	fBPP->getErrorTableBand(batchPrimitiveStepErr, &bs );
	fOutputJobStepAssociation.status(batchPrimitiveStepErr);
}
	return rows;
}

void BatchPrimitiveStep::sendPrimitiveMessages()
{
	int it = -1;
	bool more = false;
	uint64_t absoluteRID = 0;
	int64_t msgLargeBlock = -1;
	int64_t nextLargeBlock = -1;
	int64_t msgLogicalExtent = -1;
	int64_t nextLogicalExtent = -1;
	const uint logicBlockShift = 13;
	ByteStream msgBpp;
	int msgsSkip=0;
	bool scan=true;;
	bool scanThisBlock=true;
	
	int64_t Min=0;
	int64_t Max=0;
	int64_t SeqNum=0;
	bool MinMaxValid=true;

	if (0 < fInputJobStepAssociation.status())
	{
		finishedSending = true;
		return;
	}

try
{
	// @bug 1098
	if (fOid >= 3000 && !fIsProjectionOnly )
		storeCasualPartitionInfo();

	// Get the starting DBRoot for the column.  We want to send use the starting DBRoot to send the right messages
	// to the right PMs.  If the first extent, is on DBRoot1, the message will go to the first connection.  If it's
	// DBRoot2, the first extent will get sent to the second connection, and so on.  This is a change for the
	// multiple files per OID enhancement as a precurser to shared nothing.
	uint16_t startingDBRoot = 1;
	uint32_t startingPartitionNumber = 0;
	oam::OamCache *oamCache = oam::OamCache::makeOamCache();
	boost::shared_ptr<map<int, int> > dbRootPMMap = oamCache->getDBRootToPMMap();

	dbrm.getStartExtent(fOid, startingDBRoot, startingPartitionNumber);

	if ( ffirstStepType == SCAN )
	{
		LBIDRange_v::iterator it;
		uint32_t  extentIndex;
		uint64_t  fbo;
		bool cpPredicate = true;
  		bool firstSend = true;
  		//@Bug 913 
  		if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0) {
			dlTimes.setFirstReadTime();
		}
		for (it = lbidRanges.begin(); it != lbidRanges.end() && !die; it++) 
  		{
 			BRM::LBID_t lbid = (*it).start;

			fbo = getFBO(lbid);
			if (hwm < fbo)
				continue;				
					
			extentIndex = fbo >> divShift;
			if (fOid >= 3000)
			{
				// @bug 1297. getMinMax to prepare partition vector should consider
				// cpPredicate. only scanned lbidrange will be pushed into the vector.

				cpPredicate = (scanFlags[extentIndex] != 0);
				// @bug 1090. cal getMinMax to prepare the CP vector and update later
				if (lbidList->CasualPartitionDataType(fColType.colDataType, fColType.colWidth) && cpPredicate) {
					cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
					MinMaxValid = lbidList->GetMinMax(Min, Max, SeqNum,
						lbid, &extents);
					cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
				}
			}


			if (cpPredicate==false){ //don't scan this extent
				//...Track the number of LBIDs we skip due to Casual Partioning.
				//...We use the same equation we use to initialize remainingLbids
				//...in the code that follows down below this.
				fNumBlksSkipped += ( (hwm > (fbo + it->size - 1)) ?
							(it->size) : (hwm - fbo + 1) );
				continue;
			}								
			
			if (dbRootPMMap->find(extents[extentIndex].dbRoot) == dbRootPMMap->end())
				throw IDBExcept(ERR_DATA_OFFLINE);

			LBID_t    msgLbidStart   = it->start;
			u_int32_t remainingLbids = 0;
			u_int32_t lbidToBeSent =
				( (hwm > (fbo + it->size - 1)) ? (it->size) : (hwm - fbo + 1) ); 
			u_int32_t msgLbidCount   = 0;
			u_int32_t msgLbidCountBatch   = 0;	
			u_int32_t totalLbid = 0;
			u_int32_t requestLbids = 0;
			u_int32_t sentLbids = 0;
			u_int32_t maxOutstandingLbids = 0;
			if ( lbidToBeSent % fColType.colWidth == 0 )
			 	totalLbid= lbidToBeSent/fColType.colWidth;
			else
				totalLbid= lbidToBeSent/fColType.colWidth + 1;
			
			remainingLbids =  totalLbid;
//			cout << "fColWidth=" << fColWidth<< endl;
//			msgLbidCount = extentSize/fColWidth;

			// @bug 977
			// @bug 1264.  Changed the number of logical blocks per BPS scan to be configurable based on the LogicalBlocksPerScan Calpont.xml
			//             entry.
			// msgLbidCount = extentSize/8;
			if ( fColWidth == 0 )
				fColWidth = fColType.colWidth;
			requestLbids = fRequestSize * (extentSize / fColWidth);
			msgLbidCount =(requestLbids /fProcessorThreadsPerScan)/ fRequestSize;
			maxOutstandingLbids = fMaxOutstandingRequests * (extentSize / fColWidth);
			while ( remainingLbids > 0 && 0 == fInputJobStepAssociation.status() && !die)
			{
				msgLbidCountBatch = msgLbidCount;
				if ( remainingLbids < requestLbids)										
					requestLbids = remainingLbids;
								
				//@Bug 1424,1298Send one requestSize a batch
				while ( sentLbids < requestLbids )
				{
					if ( (requestLbids - sentLbids) < msgLbidCountBatch )
						msgLbidCountBatch = requestLbids - sentLbids;

				if ( fOid>=3000) {
					THROTTLEDEBUG << "st:" << fStepId << " sending msgLbidStart=" << msgLbidStart << " msgLbidCount="<< msgLbidCountBatch<< 
					" colWidth=" << fColWidth <<endl;
					THROTTLEDEBUG << "oid = " << fOid << "  numExtents:" << numExtents << "   extentSize:  " << extentSize << endl;
				}

					fBPP->setLBID( msgLbidStart, extents[extentIndex].dbRoot);
					idbassert(msgLbidCountBatch>0);
					fBPP->setCount ( msgLbidCountBatch );
					fBPP->runBPP(msgBpp, (*dbRootPMMap)[extents[extentIndex].dbRoot]);
					fMsgBytesOut += msgBpp.lengthWithHdrOverhead();
					//cout << "Requesting " << msgLbidCount << " logical blocks at LBID " << msgLbidStart; 
					//cout << "  sending fOid " << fOid << endl;
#ifdef SHARED_NOTHING_DEMO
					fDec->write(msgBpp, fOid);
#else
					fDec->write(msgBpp);
#endif
					fMsgBppsToPm++;
				
					msgBpp.restart();
					fBPP->reset();
					firstSend = false;
					msgLbidStart += msgLbidCountBatch * fColType.colWidth; 
					sentLbids += msgLbidCountBatch;
				}
				mutex.lock(); //pthread_mutex_lock(&mutex);
				msgsSent += sentLbids;
					
				if (recvWaiting)
					condvar.notify_all(); //pthread_cond_broadcast(&condvar); //signal consumer to resume
	
				//Bug 939
				while (!fSwallowRows && 
					(( msgsSent - msgsRecvd) >  maxOutstandingLbids) && !die)
				{
					sendWaiting = true;
					THROTTLEDEBUG << "BPS send thread wait on step " << fStepId << " oid = " << fOid 
					<< "  msgsSent: " << msgsSent << "  msgsRecvd = " << msgsRecvd << endl;
					condvarWakeupProducer.wait(mutex); //pthread_cond_wait ( &condvarWakeupProducer, &mutex );
					sendWaiting = false;
				}
				mutex.unlock(); //pthread_mutex_unlock(&mutex);	
				sentLbids = 0;
				remainingLbids -= requestLbids;
			}			
		}					
	}
	else if ( ffirstStepType == COLSTEP )
	{
		//The presence of more than 1 input DL means we (probably) have a pDictionaryScan step feeding this step
		// a list of tokens to get the rids for. Convert the input tokens to a filter string. We also have a ridwhile (more) more = getNextElement(it);
		// list as the second input dl
		//cout << toString() << endl;
		//if ( fOid >=3000 )
		//	cout << "SES:" << fSessionId << " ST:" << fStepId <<
		//		" total size = " << ridList->totalSize() << endl;
				
		if (fInputJobStepAssociation.outSize() > 1)
		{
			addFilters();
			if (fTableOid >= 3000)
				cout << toString() << endl;
			//If we got no input rids (as opposed to no input DL at all) then there were no matching rows from
			//  the previous step, so this step should not return any rows either. This would be the case, for
			//  instance, if P_NAME LIKE '%xxxx%' produced no signature matches.
			if (fFilterCount == 0)
			{
				goto done;
			}
		}
		if ( fColWidth == 0 )
				fColWidth = fColType.colWidth;
		it = getIterator( );
		more = getNextElement( it );
		if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0)
			dlTimes.setFirstReadTime();
		absoluteRID = getAbsRid ( 0 );

		if (more && !fIsProjectionOnly)
			scan = scanit(absoluteRID);

		scanThisBlock = scan;
		msgLargeBlock = absoluteRID >> logicBlockShift;
		nextLargeBlock = msgLargeBlock;
		msgLogicalExtent = msgLargeBlock >> LOGICAL_EXTENT_CONVERTER;
		nextLogicalExtent = msgLogicalExtent;
		uint numSentExtent = 0;
		while (more && !die) {
			if (0 < fInputJobStepAssociation.status())
			{
				sendError(fInputJobStepAssociation.status());
				while (more) more = getNextElement(it );
					break;
			}
			uint64_t rwCount;
			rwCount = getRowCount();
			
			for (uint64_t i = 0; i < rwCount && !die; ++i) {
				absoluteRID = getAbsRid(i);
				nextLargeBlock = absoluteRID >> logicBlockShift;
				if (!fIsProjectionOnly)
					scan = scanit(absoluteRID);
				if (msgLargeBlock == nextLargeBlock) {
					if (scanThisBlock) {
						addElementToBPP(i, startingDBRoot);
						ridsRequested++;
					}
					// @bug 1301. msgsSkip should not increment for every skipped row
//					else
//					msgsSkip++;
				}
				else {
					if (fBPP->getRidCount()) {						
						fBPP->runBPP(msgBpp, startingDBRoot);
						fMsgBytesOut += msgBpp.lengthWithHdrOverhead();

#ifdef SHARED_NOTHING_DEMO
						fDec->write(msgBpp, fOid);
#else
						fDec->write(msgBpp);
#endif
						mutex.lock(); //pthread_mutex_lock(&mutex);
						fMsgBppsToPm = ++msgsSent;
								
						if (recvWaiting)
							condvar.notify_all(); //pthread_cond_broadcast(&condvar); //signal consumer to resume
						mutex.unlock(); //pthread_mutex_unlock(&mutex);
						msgBpp.restart();
						fBPP->reset();
					}
					scanThisBlock = scan;
					msgLargeBlock = nextLargeBlock;
					nextLogicalExtent = msgLargeBlock >> LOGICAL_EXTENT_CONVERTER;
					if ( fOid >= 3000 )
						THROTTLEDEBUG << "msgLogicalExtent = " << msgLogicalExtent << " nextLogicalExtent = " << nextLogicalExtent << endl; 
					if (( nextLogicalExtent != msgLogicalExtent ) && ( ++numSentExtent == fRequestSize ) )
					{
						mutex.lock(); //pthread_mutex_lock(&mutex);
						//Bug 939,1424,1298
						while (!fSwallowRows && 
							(( msgsSent - msgsRecvd) >  (fMaxOutstandingRequests * (extentSize / fColWidth ))) && !die)
						{
							sendWaiting = true;
							THROTTLEDEBUG << "BPS send thread wait st:" << fStepId 
							<< "  msgsSent: " << msgsSent << "  msgsRecvd = " << msgsRecvd << endl;
							condvarWakeupProducer.wait(mutex); //pthread_cond_wait ( &condvarWakeupProducer, &mutex );
							sendWaiting = false;
						}
						mutex.unlock(); //pthread_mutex_unlock(&mutex);
						numSentExtent = 0;
						msgLogicalExtent = nextLogicalExtent;
					}
																									
					if (scanThisBlock) {
						addElementToBPP(i, startingDBRoot);
						ridsRequested++;
					}
					else
						msgsSkip++;
				}
			}
																																					
			more = getNextElement(it);
			if (!more && !die && fBPP->getRidCount() > 0) {
				//send last message
				fBPP->runBPP(msgBpp, startingDBRoot);
				fMsgBytesOut += msgBpp.lengthWithHdrOverhead();
#ifdef SHARED_NOTHING_DEMO
				fDec->write(msgBpp, fOid);
#else
				fDec->write(msgBpp);
#endif
				mutex.lock(); //pthread_mutex_lock(&mutex);
				fMsgBppsToPm = ++msgsSent;
				if (recvWaiting)
					condvar.notify_all(); //pthread_cond_broadcast(&condvar); //signal consumer to resume
				mutex.unlock(); //pthread_mutex_unlock(&mutex);	
				fBPP->reset();
			}
		}
		//cout << "st: "<< fStepId << " requested " << ridsRequested << endl;		
	}
	
	if (fOid>=3000) dlTimes.setLastReadTime();
done:
	/* If the query was aborted and there's an input datalist, drain it to make
	sure the producer finishes too. */
	if (more && die && ffirstStepType != SCAN) {
		while (more)
			more = getNextElement(it);
	}

	mutex.lock(); //pthread_mutex_lock(&mutex);
	finishedSending = true;
	if (recvWaiting)
		condvar.notify_all(); //pthread_cond_broadcast(&condvar); //signal consumer to resume
	mutex.unlock(); //pthread_mutex_unlock(&mutex);
	msgBpp.reset();
	//if ( fOid >=3000 )
	//	cout << "st: "<< fStepId << " requested " << ridsRequested << endl;		
		
	//...Track the number of LBIDs we skip due to Casual Partioning.
	fNumBlksSkipped += msgsSkip;
}  // try
catch(const std::exception& ex)
{
	finishedSending = true;
	msgBpp.reset();
	sendError(batchPrimitiveStepErr);
	processError(ex.what(), batchPrimitiveStepErr, "BatchPrimitiveStep::SendPrimitiveMessages()");
}
catch(...)
{
	finishedSending = true;
	msgBpp.reset();
	sendError(batchPrimitiveStepErr);
	processError("unknown", batchPrimitiveStepErr, "BatchPrimitiveStep::receivePrimitiveMessages()");
}
}

struct CPInfo {
	CPInfo(int64_t MIN, 
		int64_t MAX, 
		uint64_t l) : 
			min(MIN), 
			max(MAX), 
			LBID(l) { };
	int64_t min;
	int64_t max;
	uint64_t LBID;
};

void BatchPrimitiveStep::receivePrimitiveMessages()
{
	int64_t ridResults = 0;
	AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
	DataList_t* dlp = dl->dataList();
	StrDataList* strDlp = dl->stringDataList();
	FifoDataList *fifo = dl->fifoDL();
	StringFifoDataList *strFifo = dl->stringDL();
	TupleBucketDataList *tbdl = dl->tupleBucketDL();
	UintRowGroup rw;
	uint i = 0, size, vecSize;
	vector<boost::shared_ptr<ByteStream> > bsv;
	std::vector<ElementType> outBpp;
	std::vector<StringElementType> strOutBpp;
	std::vector<TupleType> outTp;
	int64_t min;
	int64_t max;
	uint64_t lbid;
	uint32_t cachedIO;
	uint32_t physIO;
	uint32_t touchedBlocks;
	bool validCPData;

	uint16_t error = fInputJobStepAssociation.status();
try 
{
	while (! error) {
		// sync with the send side 	
		while (!finishedSending && msgsSent == msgsRecvd)
			usleep(2000);

		if (msgsSent == msgsRecvd && finishedSending) {
			break;
		}
		// do the recv
		fDec->read_all(uniqueID, bsv);
		size = bsv.size();
		mutex.lock(); //pthread_mutex_lock(&mutex);
		msgsRecvd+= size;
		if ( (sendWaiting) && ( (msgsSent - msgsRecvd) <= ( fMaxOutstandingRequests * (extentSize / fColWidth) ) ) )
		{
			condvarWakeupProducer.notify_one(); //pthread_cond_signal(&condvarWakeupProducer);
			THROTTLEDEBUG << "receivePrimitiveMessages wakes up sending side .. " << "  msgsSent: " << msgsSent << "  msgsRecvd = " << msgsRecvd << endl;
		}
		mutex.unlock(); //pthread_mutex_unlock(&mutex);
		for (i = 0; i < size && !die; i++) {	
			//Order the message if needed
			//const ByteStream::byte* bsp = bsv[i].buf();
			ByteStream* bs = bsv[i].get();
			ISMPacketHeader *ism = (ISMPacketHeader*)(bs->buf());
			if (0 < ism->Status)
			{
				fOutputJobStepAssociation.status(ism->Status);
				error = ism->Status;
				break;
			}
			fMsgBytesIn += bs->lengthWithHdrOverhead();
			
			switch (fOutputType) {
				case BPS_ELEMENT_TYPE:
					uint16_t preJoinRidCount;

					outBpp.clear();
					fBPP->getElementTypes ( *bs, &outBpp, &validCPData, &lbid, &min, &max, &cachedIO, &physIO, &touchedBlocks, &preJoinRidCount);
					vecSize = outBpp.size();
					fPhysicalIO += physIO;
					fCacheIO += cachedIO;
					fBlockTouched += touchedBlocks;
					ridResults += preJoinRidCount;
					for ( uint j=0; j<vecSize; ++j)
					{
							if (doJoin) {
								if (!joiner->inPM()) {
									if (!joiner->match(outBpp[j]))
										continue;
								}
								else
									if (outBpp[j].first & joiner::Joiner::MSB) {
										// use joiner to mark the small side matches
										joiner->mark(outBpp[j]);
										continue;
									}
							}

						if (fifo)
						{
							rw.et[rw.count++] = outBpp[j];							
							if (rw.count == rw.ElementsPerGroup)
							{
								fifo->insert(rw);
								rw.count = 0;
							}
						}
						else
						{
							dlp->insert(outBpp[j]);
						}
					}
					break;
				case STRING_ELEMENT_TYPE:
					strOutBpp.clear();
					fBPP->getStringElementTypes( *bs, &strOutBpp, &validCPData, &lbid, &min, &max, &cachedIO, &physIO, &touchedBlocks);
					vecSize = strOutBpp.size();
					ridResults += vecSize;
					fPhysicalIO += physIO;
					fCacheIO += cachedIO;
					fBlockTouched += touchedBlocks;
					for ( uint j=0; j<vecSize; ++j)
					{
						if (strFifo)
						{
							strRw.et[strRw.count++] = strOutBpp[j];							
							if (strRw.count == strRw.ElementsPerGroup)
							{
								strFifo->insert(strRw);
								strRw.count = 0;
							}
						}
						else
						{
							strDlp->insert(strOutBpp[j]);
						}
					}
					break;

				default: throw logic_error("BPS::receivePrimitiveMessages(): Unknown output type");				
			}	

			if (fOid >= 3000 && ( ffirstStepType == SCAN ) && validCPData ) {
				cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
				lbidList->UpdateMinMax(min, max, lbid, fColType.colDataType);							
				cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
			}
		}
	}
	// done reading
}  // try
catch(const LargeDataListExcept& ex)
{
	processError(ex.what(), batchPrimitiveStepLargeDataListFileErr, "BatchPrimitiveStep::receivePrimitiveMessages()");
}
catch(const std::exception& ex)
{
	processError(ex.what(), batchPrimitiveStepErr, "BatchPrimitiveStep::receivePrimitiveMessages()");
}
catch(...)
{
	processError("unknown", batchPrimitiveStepErr, "BatchPrimitiveStep::receivePrimitiveMessages()");
}

	if (fifo && rw.count > 0)
		fifo->insert(rw);
	
	if (strFifo && strRw.count > 0)
		strFifo->insert(strRw);
	
	if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0) {
			dlTimes.setFirstReadTime();
			dlTimes.setLastReadTime();
			dlTimes.setFirstInsertTime();
		}
	if (fOid>=3000) dlTimes.setEndOfInputTime();

	ByteStream bs;
	fDec->removeDECEventListener(this);
	fBPP->destroyBPP(bs);
	fDec->write(bs);
	BPPIsAllocated = false;
	fDec->removeQueue(uniqueID);
	joiner.reset();

	if (fifo)
		fifo->endOfInput();
	else if ( strFifo )
		strFifo->endOfInput();
	else if ( strDlp )
		strDlp->endOfInput();
	else if ( tbdl )
		tbdl->endOfInput();
	else
		dlp->endOfInput();

	//...Print job step completion information
	if (fTableOid >= 3000) {		
		time_t t = time(0);
		char timeString[50];
		ctime_r(&t, timeString);
		timeString[strlen(timeString)-1 ] = '\0';
		
		FifoDataList* pFifo = 0;
		uint64_t totalBlockedReadCount = 0;
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
		
		//...Roundoff msg byte counts to nearest KB for display
		uint64_t msgBytesInKB  = fMsgBytesIn >> 10;
		uint64_t msgBytesOutKB = fMsgBytesOut >> 10;
		if (fMsgBytesIn & 512)
			msgBytesInKB++;
		if (fMsgBytesOut & 512)
			msgBytesOutKB++;

		// @bug 828
		if (fifo)
			fifo->totalSize(ridResults);
		
		if (traceOn())
		{
			ostringstream logStr;
			logStr << "ses:" << fSessionId << " st: " << fStepId<<" finished at "<<
				timeString << "; PhyI/O-" << fPhysicalIO << "; CacheI/O-"  <<
				fCacheIO << "; MsgsSent-" << fMsgBppsToPm << "; MsgsRvcd-" << msgsRecvd <<
				"; BlocksTouched-"<< fBlockTouched <<
				"; BlockedFifoIn/Out-" << totalBlockedReadCount <<
				"/" << totalBlockedWriteCount <<
				"; output size-" << ridResults << endl <<
				"\tPartitionBlocksEliminated-" << fNumBlksSkipped <<
				"; MsgBytesIn-"  << msgBytesInKB  << "KB" <<
				"; MsgBytesOut-" << msgBytesOutKB << "KB" << endl  <<
				"\t1st read " << dlTimes.FirstReadTimeString() << 
				"; EOI " << dlTimes.EndOfInputTimeString() << "; runtime-" <<
				JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(),dlTimes.FirstReadTime())<< "s"
				<< endl << "\tJob completion status " << fInputJobStepAssociation.status() << endl;
					 	
			logEnd(logStr.str().c_str());

			syslogReadBlockCounts(16,    // exemgr sybsystem
				fPhysicalIO,             // # blocks read from disk
				fCacheIO,                // # blocks read from cache
				fNumBlksSkipped);        // # casual partition block hits
			syslogProcessingTimes(16,    // exemgr subsystem
				dlTimes.FirstReadTime(),   // first datalist read
				dlTimes.LastReadTime(),    // last  datalist read
				dlTimes.FirstInsertTime(), // first datalist write
				dlTimes.EndOfInputTime()); // last (endOfInput) datalist write
			syslogEndStep(16,            // exemgr subsystem
				totalBlockedReadCount,   // blocked datalist input
				totalBlockedWriteCount,  // blocked datalist output
				fMsgBytesIn,             // incoming msg byte count
				fMsgBytesOut);           // outgoing msg byte count
		}
	}
	
	if (fOid >=3000 && ( ffirstStepType == SCAN )) {
		cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
 		lbidList->UpdateAllPartitionInfo();
		cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
	}
}

void BatchPrimitiveStep::receiveMultiPrimitiveMessages(uint threadID)
{
	AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
	DataList_t* dlp = dl->dataList();
	StrDataList* strDlp = dl->stringDataList();
	FifoDataList *fifo = dl->fifoDL();
	StringFifoDataList *strFifo = dl->stringDL();
	TupleBucketDataList *tbdl = dl->tupleBucketDL();
	BucketDL<ElementType> *bucket = dynamic_cast<BucketDL<ElementType> *>(dlp);
	ZDL<ElementType> *zdl = dynamic_cast<ZDL<ElementType> *>(dlp);
	UintRowGroup rw;
	uint i = 0, size = 0, vecSize = 0;
	vector<boost::shared_ptr<ByteStream> > bsv;
	vector<ElementType> elementVec;
	vector<StringElementType> strElementVec;
	std::vector<TupleType> outTp;
	std::vector<ElementType> outBpp;
	std::vector<StringElementType> strOutBpp;
	bool validCPData;
	int64_t min;
	int64_t max;
	uint64_t lbid;
	vector<CPInfo> cpv;
	uint32_t cachedIO;
	uint32_t physIO;
	uint32_t touchedBlocks;
	uint64_t msgBytesIn_Thread = 0;
	uint32_t cachedIO_Thread = 0;
	uint32_t physIO_Thread = 0;
	uint32_t touchedBlocks_Thread = 0;
	int64_t ridsReturned_Thread = 0;
	bool lastThread = false;
	bool caughtError = false;
	uint errorTimeout = 0;
	bool didEOF = false;

try
{
// 	if (! fInputJobStepAssociation.status() ) 
		mutex.lock(); //pthread_mutex_lock(&mutex);
// 	while (! fInputJobStepAssociation.status() ) {
	while (1) {
		// sync with the send side 	
		while (!finishedSending && msgsSent == msgsRecvd) {
			recvWaiting++;
			condvar.wait(mutex); //pthread_cond_wait(&condvar, &mutex);
			recvWaiting--;
		}

		if (msgsSent == msgsRecvd && finishedSending) {
			break;
		}
			
		// do the recv
		
		fDec->read_some(uniqueID, fNumThreads, bsv);
		size = bsv.size();
		msgsRecvd+= size;
		//@Bug 1424,1298
		if ( (sendWaiting) && ( (msgsSent - msgsRecvd) <= ( fMaxOutstandingRequests * (extentSize / fColWidth) ) ) )
		{
			condvarWakeupProducer.notify_one(); //pthread_cond_signal(&condvarWakeupProducer);
			THROTTLEDEBUG << "receiveMultiPrimitiveMessages wakes up sending side .. " << "  msgsSent: " << msgsSent << "  msgsRecvd = " << msgsRecvd << endl;
		}

		/* If there's an error and the joblist is being aborted, don't
			sit around forever waiting for responses.  Exit 30 secs after
			the last msg from the PMs. */
		if (fInputJobStepAssociation.status() != 0 || die) {
			mutex.unlock(); //pthread_mutex_unlock(&mutex);
			// thread 0 stays, the rest finish & exit to allow a quicker EOF
			if (threadID != 0)
				break;
			else if (!didEOF) {
				didEOF = true;
				while (recvExited < fNumThreads - 1)
					sleep(1);
				if (fifo)
					fifo->endOfInput();
				else if ( strFifo )
					strFifo->endOfInput();
				else if ( strDlp )
					strDlp->endOfInput();
				else if ( tbdl )
					tbdl->endOfInput();
				else
					dlp->endOfInput();
			}
			if (size != 0)  // got something, reset the timer
				errorTimeout = 0;
			sleep(1);
			mutex.lock(); //pthread_mutex_lock(&mutex);
			if (++errorTimeout == 30)
				break;
			continue;
		}

		if (size == 0) {
			mutex.unlock(); //pthread_mutex_unlock(&mutex);
			usleep(1000);
			mutex.lock(); //pthread_mutex_lock(&mutex);
			continue;
		}
		mutex.unlock(); //pthread_mutex_unlock(&mutex);
		for (i = 0; i < size; i++) {	
			ByteStream* bs = bsv[i].get();
			
			// @bug 488. one PrimProc node is down. error out
			//An error condition.  We are not going to do anymore.
			ISMPacketHeader *hdr = (ISMPacketHeader*)(bs->buf());
			if (bs->length() == 0)
			{
				mutex.lock(); //pthread_mutex_lock(&mutex);
				fOutputJobStepAssociation.status(primitiveServerErr);
				die = true;
				condvar.notify_all(); //pthread_cond_broadcast(&condvar);
				condvarWakeupProducer.notify_all(); //pthread_cond_broadcast(&condvarWakeupProducer);
				//cout << "BPS receive signal " << fStepId << endl;				
				mutex.unlock(); //pthread_mutex_unlock(&mutex);
				break;
			}
			if (0 < hdr->Status)
			{
				mutex.lock(); //pthread_mutex_lock(&mutex);
				fOutputJobStepAssociation.status(hdr->Status);
				die = true;
				condvar.notify_all(); //pthread_cond_broadcast(&condvar);
				condvarWakeupProducer.notify_all(); //pthread_cond_broadcast(&condvarWakeupProducer);
				//cout << "DEC receive signal" << endl;
				mutex.unlock(); //pthread_mutex_unlock(&mutex);
				break;
			}

			msgBytesIn_Thread += bs->lengthWithHdrOverhead();
			vecSize = 0;
			switch (fOutputType) {
				case BPS_ELEMENT_TYPE:
						uint16_t preJoinRidCount;

						outBpp.clear();
						fBPP->getElementTypes ( *bs, &outBpp, &validCPData, &lbid, &min, &max, &cachedIO, &physIO, &touchedBlocks, &preJoinRidCount);
						ridsReturned_Thread += preJoinRidCount;
						if (doJoin)
						{
							for (uint j=0, ssize = outBpp.size(); 
							  j<ssize; ++j)
							{
								if (!joiner->inPM())
								{
									if (!joiner->match(outBpp[j]))
										continue;
								}
								else
								if (outBpp[j].first & joiner::Joiner::MSB)
								{
									// use joiner to mark the small side matches
									joiner->mark(outBpp[j]);
									continue;
								}

								elementVec.push_back(outBpp[j]);
								vecSize++;
							}
						}
						else
						{
							vecSize = outBpp.size();
							//Push the elements into a tmp vector
							elementVec.insert(elementVec.end(), outBpp.begin(), outBpp.end());
						}
						break;
				case STRING_ELEMENT_TYPE:
						strOutBpp.clear();
						fBPP->getStringElementTypes( *bs, &strOutBpp, &validCPData, &lbid, &min, &max, &cachedIO, &physIO, &touchedBlocks);
						vecSize = strOutBpp.size();
						//Push the elements into a tmp vector
						strElementVec.insert(strElementVec.end(), strOutBpp.begin(), strOutBpp.end());
						break;
				case TUPLE:
						fBPP->getTuples( *bs, &outTp, &validCPData, &lbid, &min, &max, &cachedIO, &physIO, &touchedBlocks);
						vecSize = outTp.size();
						tbdl->insert(outTp);
						outTp.clear();
						break;
				case TABLE_BAND:				
						break;
						
				default: throw logic_error("BPS::receivePrimitiveMessages(): Unknown output type");
			}
			//@Bug 1099
			/* Hack to get different ridcount reporting when there's a join involved */
			if (fOutputType != BPS_ELEMENT_TYPE)
				ridsReturned_Thread += vecSize;
			cachedIO_Thread += cachedIO;
			physIO_Thread += physIO;
			touchedBlocks_Thread += touchedBlocks;
			if ( ( fOid >= 3000 ) && ( ffirstStepType == SCAN ) &&  validCPData  )
				cpv.push_back(CPInfo(min, max, lbid));					
		}	
		
		if (bucket) {
			if (fOid>=3000 && dlTimes.FirstInsertTime().tv_sec==0)
				dlTimes.setFirstInsertTime();

			bucket->insert(elementVec);
		}
		else if (fifo)
		{
			size = elementVec.size();
			if ( size > 0 )
				if (fOid>=3000 && dlTimes.FirstInsertTime().tv_sec==0)
					dlTimes.setFirstInsertTime();

				dlMutex.lock(); //pthread_mutex_lock(&dlMutex);

				for (i = 0; i < size; ++i) {
					rw.et[rw.count++] = elementVec[i];
					if (rw.count == rw.ElementsPerGroup)
					{
						fifo->insert(rw);
						rw.count = 0;
					}
				}

				if (rw.count > 0)
				{
					fifo->insert(rw);
					rw.count = 0;
				}
				dlMutex.unlock(); //pthread_mutex_unlock(&dlMutex);
		}
		else if (zdl)
		{
			if (fOid>=3000 && dlTimes.FirstInsertTime().tv_sec==0)
				dlTimes.setFirstInsertTime();

			dlp->insert(elementVec);
		}
		else if ( strFifo ) //@Bug 1308
		{
			size = strElementVec.size();
			if ( size > 0 )
				if (fOid>=3000 && dlTimes.FirstInsertTime().tv_sec==0)
					dlTimes.setFirstInsertTime();

				dlMutex.lock(); //pthread_mutex_lock(&dlMutex);

				for (i = 0; i < size; ++i) {
					strRw.et[strRw.count++] = strElementVec[i];
					if (strRw.count == strRw.ElementsPerGroup)
					{
						strFifo->insert(strRw);
						strRw.count = 0;
					}
				}

				if (strRw.count > 0)
				{
					strFifo->insert(strRw);
					strRw.count = 0;
				}
				dlMutex.unlock(); //pthread_mutex_unlock(&dlMutex);
		}
		else if ( strDlp )
		{
			strDlp->insert(strElementVec);
			
		}
		
		elementVec.clear();
		strElementVec.clear();
		//update casual partition
		size = cpv.size();
		if (size > 0) {
			cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
			for (i = 0; i < size; i++) {
				CPInfo *cpi = &(cpv[i]);
				lbidList->UpdateMinMax(cpi->min, cpi->max, cpi->LBID, fColType.colDataType);
			}
			cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
			cpv.clear();
		}
		mutex.lock(); //pthread_mutex_lock(&mutex);
	}// done reading	

}//try
catch(const LargeDataListExcept& ex)
{
	processError(ex.what(), batchPrimitiveStepLargeDataListFileErr, "BatchPrimitiveStep::receiveMultiPrimitiveMessages()");
	caughtError = true;
}
catch(const std::exception& ex)
{
	processError(ex.what(), batchPrimitiveStepErr, "BatchPrimitiveStep::receiveMultiPrimitiveMessages()");
	caughtError = true;
}
catch(...)
{
	processError("unknown", batchPrimitiveStepErr, "BatchPrimitiveStep::receiveMultiPrimitiveMessages()");
	caughtError = true;
}
	if (caughtError && !fUpdatedEnd)
		{
		setEndOfInput(fifo, strFifo, strDlp, tbdl, zdl, dlp);		
	}
	if (++recvExited == fNumThreads) {
		//...Casual partitioning could cause us to do no processing.  In that
		//...case these time stamps did not get set.  So we set them here.

		if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0) {
			dlTimes.setFirstReadTime();
			dlTimes.setLastReadTime();
			dlTimes.setFirstInsertTime();
		}
		if (fOid>=3000) dlTimes.setEndOfInputTime();

		ByteStream bs;
		fDec->removeDECEventListener(this);
		fBPP->destroyBPP(bs);
		fDec->write(bs);
		BPPIsAllocated = false;
		fDec->removeQueue(uniqueID);
		joiner.reset();

		lastThread = true;

		if (! fUpdatedEnd && !didEOF)
		{
			setEndOfInput(fifo, strFifo, strDlp, tbdl, zdl, dlp);
		}
	}
	//@Bug 1099
	ridsReturned += ridsReturned_Thread;
	fPhysicalIO += physIO_Thread;
	fCacheIO += cachedIO_Thread;
	fBlockTouched += touchedBlocks_Thread;
	fMsgBytesIn += msgBytesIn_Thread;
	mutex.unlock(); //pthread_mutex_unlock(&mutex);
	
	if (fTableOid >= 3000 && lastThread)
	{
		time_t t = time(0);
		char timeString[50];
		ctime_r(&t, timeString);
		timeString[strlen(timeString)-1 ] = '\0';
		
		FifoDataList* pFifo = 0;
		uint64_t totalBlockedReadCount = 0;
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
		
		//...Roundoff msg byte counts to nearest KB for display
		uint64_t msgBytesInKB  = fMsgBytesIn >> 10;
		uint64_t msgBytesOutKB = fMsgBytesOut >> 10;
		if (fMsgBytesIn & 512)
			msgBytesInKB++;
		if (fMsgBytesOut & 512)
			msgBytesOutKB++;
		// @bug 828
		if (fifo)
			fifo->totalSize(ridsReturned);
		ostringstream logStr;
		logStr << "ses:" << fSessionId << " st: " << fStepId<<" finished at "<<
			timeString << "; PhyI/O-" << fPhysicalIO << "; CacheI/O-"  <<
			fCacheIO << "; MsgsSent-" << fMsgBppsToPm << "; MsgsRvcd-" << msgsRecvd <<
			"; BlocksTouched-"<< fBlockTouched <<
			"; BlockedFifoIn/Out-" << totalBlockedReadCount <<
			"/" << totalBlockedWriteCount <<
			"; output size-" << ridsReturned<< endl <<
			"\tPartitionBlocksEliminated-" << fNumBlksSkipped <<
			"; MsgBytesIn-"  << msgBytesInKB  << "KB" <<
			"; MsgBytesOut-" << msgBytesOutKB << "KB" << endl  <<
			"\t1st read " << dlTimes.FirstReadTimeString() << 
			"; EOI " << dlTimes.EndOfInputTimeString() << "; runtime-" <<
			JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(),dlTimes.FirstReadTime())<< "s" << endl
			<< "\tJob completion status " << fOutputJobStepAssociation.status() << endl;

		logEnd(logStr.str().c_str());
		
		if (traceOn())
		{
			syslogReadBlockCounts(16,    // exemgr sybsystem
				fPhysicalIO,             // # blocks read from disk
				fCacheIO,                // # blocks read from cache
				fNumBlksSkipped);        // # casual partition block hits
			syslogProcessingTimes(16,    // exemgr subsystem
				dlTimes.FirstReadTime(),   // first datalist read
				dlTimes.LastReadTime(),    // last  datalist read
				dlTimes.FirstInsertTime(), // first datalist write
				dlTimes.EndOfInputTime()); // last (endOfInput) datalist write
			syslogEndStep(16,            // exemgr subsystem
				totalBlockedReadCount,   // blocked datalist input
				totalBlockedWriteCount,  // blocked datalist output
				fMsgBytesIn,             // incoming msg byte count
				fMsgBytesOut);           // outgoing msg byte count
		}
		
		if ( ffirstStepType == SCAN )
 			lbidList->UpdateAllPartitionInfo();
	}
}


void BatchPrimitiveStep::processError(const string& ex, uint16_t err, const string& src)
{
	ostringstream oss;
	oss << "st: " << fStepId << " " << src << " caught an exception: " << ex << endl;
	catchHandler(oss.str(), fSessionId);
	fOutputJobStepAssociation.status(err);
	cerr << oss.str();
}

void BatchPrimitiveStep::setEndOfInput(FifoDataList *fifo, StringFifoDataList *strFifo, StrDataList* strDlp, TupleBucketDataList *tbdl, ZDL<ElementType> *zdl, DataList_t* dlp)
{
	if (fifo)
		fifo->endOfInput();
	else if ( strFifo )
		strFifo->endOfInput();
	else if ( strDlp )
		strDlp->endOfInput();
	else if ( tbdl )
		tbdl->endOfInput();
	else if (zdl)
		zdl->endOfInput();
	else
		dlp->endOfInput();
	fUpdatedEnd = true;
}

const string BatchPrimitiveStep::toString() const
{
	ostringstream oss;
	oss << "BatchPrimitiveStep        ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId << " st:" << fStepId <<
		" tb/col:" << fTableOid << "/" << fOid;
	if (alias().length()) oss << " alias:" << alias();
	if (view().length()) oss << " view:" << view();

	// @bug 1282, don't have output datalist for delivery
	if (fOutputType != TABLE_BAND)
		oss << " " << omitOidInDL << fOutputJobStepAssociation.outAt(0) << showOidInDL;
	oss << " nf:" << fFilterCount;
	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
	{
		oss << fInputJobStepAssociation.outAt(i);
	}
	oss << " outputType=" << fOutputType;	
	oss << endl << "  " << fBPP->toString() << endl;
	return oss.str();
}

void BatchPrimitiveStep::addFilters()
{
	AnyDataListSPtr dl = fInputJobStepAssociation.outAt(0);
	DataList_t* bdl = dl->dataList();
	FifoDataList* fifo = fInputJobStepAssociation.outAt(0)->fifoDL();

	idbassert(bdl);
	bool more;
	ElementType e;
	int64_t token;
	int it = -1;

	if (fifo != NULL)
	{
		it = fifo->getIterator();

		fBOP = BOP_OR;
		UintRowGroup rw;

		more = fifo->next(it, &rw);

		while (more)
		{
			for (uint64_t i = 0; i < rw.count; ++i)
				addFilter(COMPARE_EQ, (int64_t) rw.et[i].second, 0);

			more = fifo->next(it, &rw);
		}
	}
	else
	{
		it = bdl->getIterator();

		fBOP = BOP_OR;

		more = bdl->next(it, &e);

		while (more)
		{
			token = e.second;
			addFilter(COMPARE_EQ, token, 0);
			more = bdl->next(it, &e);
		}
	}

	return;
}

/* This exists to avoid a DBRM lookup for every rid. */
inline bool BatchPrimitiveStep::scanit(uint64_t rid)
{
	uint64_t fbo;
	uint extentIndex;

	//if (fOid < 3000 || !lbidList->CasualPartitionDataType(fColType.colDataType))
	//	return true;
	if (fOid < 3000)
		return true;
	fbo = rid >> rpbShift;
	extentIndex = fbo >> divShift;
	return (scanFlags[extentIndex] != 0);
}

uint64_t BatchPrimitiveStep::getFBO(uint64_t lbid)
{
	uint i;
	uint64_t lastLBID;

	for (i = 0; i < numExtents; i++) {
 		lastLBID = extents[i].range.start + (extents[i].range.size << 10) - 1;

		if (lbid >= (uint64_t) extents[i].range.start && lbid <= lastLBID)
 			return (lbid - extents[i].range.start) + (i << divShift);
	}
	cerr << "BatchPrimitiveStep: didn't find the FBO?\n";
	throw logic_error("BatchPrimitiveStep: didn't find the FBO?");
}

void BatchPrimitiveStep::useJoiner(boost::shared_ptr<joiner::Joiner> j)
{
	joiner = j;
	doJoin = (j.get() != NULL);
}

void BatchPrimitiveStep::newPMOnline(uint connectionNumber)
{
	ByteStream bs;

	fBPP->createBPP(bs);
	fDec->write(bs, connectionNumber);
	if (doJoin && joiner->inPM())
		serializeJoiner(connectionNumber);

}

void BatchPrimitiveStep::setJobInfo(const JobInfo* jobInfo)
{
	fBPP->jobInfo(jobInfo);
}

void BatchPrimitiveStep::stepId(uint16_t stepId)
{
	fStepId = stepId;
	fBPP->setStepID(stepId);
}

void BatchPrimitiveStep::dec(DistributedEngineComm* dec)
{
	if (fDec)
		fDec->removeQueue(uniqueID);
	fDec = dec;
	if (fDec)
		fDec->addQueue(uniqueID, true); 
}

}   //namespace
// vim:ts=4 sw=4:

