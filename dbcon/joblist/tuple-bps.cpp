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

//  $Id: tuple-bps.cpp 8840 2012-08-29 14:54:30Z pleblanc $


#include <unistd.h>
//#define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <ctime>
#include <sys/time.h>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
using namespace std;

#include <boost/thread.hpp>
using namespace boost;

#include "bpp-jl.h"
#include "distributedenginecomm.h"
#include "elementtype.h"
#include "unique32generator.h"
using namespace joblist;

#include "messagequeue.h"
using namespace messageqcpp;
using namespace config;

#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
#include "errorcodes.h"
#include "rowestimator.h"
#include "errorids.h"
using namespace logging;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "brm.h"
using namespace BRM;

using namespace rowgroup;

// #define DEBUG 1

//#define PROFILE

//uint TBPSCount = 0;

#ifdef PROFILE
#include<profiling.h>

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
}

void timespec_diff(const struct timespec &tv1,
				const struct timespec &tv2,
								double &tm)
{
		tm = (double)(tv2.tv_sec - tv1.tv_sec) + 1.e-9*(tv2.tv_nsec - tv1.tv_nsec);
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

struct TupleBPSPrimitive
{
	TupleBPSPrimitive(TupleBPS* batchPrimitiveStep) :
	  fBatchPrimitiveStep(batchPrimitiveStep)
	{}
	TupleBPS *fBatchPrimitiveStep;
	void operator()()
	{
		try {
			fBatchPrimitiveStep->sendPrimitiveMessages();
		}
		catch(std::exception& re) {
			cerr << "TupleBPS: send thread threw an exception: " << re.what() << 
			  "\t" << this << endl;
			catchHandler(re.what());
		}
		catch(...) {
			string msg("TupleBPS: send thread threw an unknown exception ");
			catchHandler(msg);
			cerr << msg << this << endl;
		}
	}
};

struct TupleBPSAggregators
{
	TupleBPSAggregators(TupleBPS* batchPrimitiveStep, uint64_t index) :
	  fBatchPrimitiveStepCols(batchPrimitiveStep), fThreadId(index)
	{}
	TupleBPS *fBatchPrimitiveStepCols;
	uint64_t fThreadId;

	void operator()()
	{
		try {
			fBatchPrimitiveStepCols->receiveMultiPrimitiveMessages(fThreadId);
		}
		catch(std::exception& re) {
			cerr << fBatchPrimitiveStepCols->toString() << ": receive thread threw an exception: " << re.what() << endl;
			catchHandler(re.what());
		}
		catch(...) {
			string msg("TupleBPS: recv thread threw an unknown exception ");
			cerr << fBatchPrimitiveStepCols->toString() << msg << endl;
			catchHandler(msg);
		}
	}
};

//------------------------------------------------------------------------------
// Initialize configurable parameters
//------------------------------------------------------------------------------
void TupleBPS::initializeConfigParms()
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
	fRequestSize = fRm.getJlRequestSize();
	fMaxOutstandingRequests = fRm.getJlMaxOutstandingRequests();
	fProcessorThreadsPerScan = fRm.getJlProcessorThreadsPerScan();
	fExtentRows = fRm.getExtentRows()/BLOCK_SIZE; // convert to Blocks;

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

TupleBPS::TupleBPS(const pColStep& rhs) : fRm (rhs.fRm)
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
	extentSize = rhs.extentSize;

	scannedExtents = rhs.extents;
	extentsMap[fOid] = tr1::unordered_map<int64_t, EMEntry>();
	tr1::unordered_map<int64_t, EMEntry> &ref = extentsMap[fOid];
	for (uint z = 0; z < rhs.extents.size(); z++)
		ref[rhs.extents[z].range.start] = rhs.extents[z];

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
	fCPEvaluated = false; 
	fEstimatedRows = 0; 
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
	fBPP->setOutputType(ROW_GROUP);
//	if (fOid>=3000)
//		cout << "BPS:initalized from pColStep. fSessionId=" << fSessionId << endl;
	finishedSending = sendWaiting = false;
	fNumBlksSkipped = 0;
	fPhysicalIO = 0;
	fCacheIO = 0;
	BPPIsAllocated = false;
	uniqueID = Unique32Generator::instance()->getUnique32();
	fBPP->setUniqueID(uniqueID);
	fCardinality = rhs.cardinality();
	doJoin = false;
	hasPMJoin = false;
	hasUMJoin = false;
	fRunExecuted = false;
	fSwallowRows = false;
	smallOuterJoiner = -1;
	
	// @1098 initialize scanFlags to be true
	scanFlags.assign(numExtents, true);
	runtimeCPFlags.assign(numExtents, true);
	die = false;
	bop = BOP_AND;

	runRan = joinRan = false;
	isDelivery = false;
	fExtendedInfo = "TBPS: ";
//	cout << "TBPSCount = " << ++TBPSCount << endl;
}

TupleBPS::TupleBPS(const pColScanStep& rhs) : fRm (rhs.fRm)
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
	extentSize = rhs.extentSize;
	lbidRanges = rhs.lbidRanges;

	scannedExtents = rhs.extents;
	extentsMap[fOid] = tr1::unordered_map<int64_t, EMEntry>();
	tr1::unordered_map<int64_t, EMEntry> &ref = extentsMap[fOid];
	for (uint z = 0; z < rhs.extents.size(); z++)
		ref[rhs.extents[z].range.start] = rhs.extents[z];

	divShift = rhs.divShift;
	//numExtents = rhs.numExtents;
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
	fCPEvaluated = false;
	fEstimatedRows = 0; 
	fColType = rhs.colType();
	alias(rhs.alias());
	view(rhs.view());
	name(rhs.name());
	
	fColWidth = fColType.colWidth;
	lbidList = rhs.lbidList;
			
	fLogger = rhs.logger();
	finishedSending = sendWaiting = false;
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
	fBPP->setOutputType(ROW_GROUP);
	fPhysicalIO = 0;
	fCacheIO = 0;
	BPPIsAllocated = false;
	uniqueID = Unique32Generator::instance()->getUnique32();
	fBPP->setUniqueID(uniqueID);
	fCardinality = rhs.cardinality();
	doJoin = false;
	hasPMJoin = false;
	hasUMJoin = false;
	fRunExecuted = false;
	smallOuterJoiner = -1;
	// @1098 initialize scanFlags to be true
	//scanFlags.assign(numExtents, true);
	//runtimeCPFlags.assign(numExtents, true);
	die = false;
	bop = BOP_AND;

	runRan = joinRan = false;
	isDelivery = false;
	fExtendedInfo = "TBPS: ";

//	const std::vector<struct BRM::EMEntry> &scannedExtents = rhs.extents;
#if 0
	uint lastExtent = 0;
	hwm = 0;
	if (scannedExtents.size()>0)
	{
		for (uint i = 0; i < scannedExtents.size(); i++) {
			if (scannedExtents[i].HWM != 0 && scannedExtents[i].status != EXTENTOUTOFSERVICE)
				hwm += (scannedExtents[i].HWM + 1);
			if ( (scannedExtents[i].status == EXTENTAVAILABLE) &&
				((scannedExtents[i].partitionNum >  scannedExtents[lastExtent].partitionNum) ||
				((scannedExtents[i].partitionNum == scannedExtents[lastExtent].partitionNum) &&
				 (scannedExtents[i].blockOffset  >  scannedExtents[lastExtent].blockOffset)) ||
				((scannedExtents[i].partitionNum == scannedExtents[lastExtent].partitionNum) &&
				 (scannedExtents[i].blockOffset  == scannedExtents[lastExtent].blockOffset) &&
				 (scannedExtents[i].segmentNum   >= scannedExtents[lastExtent].segmentNum))) )
			lastExtent = i;
		}
		// if only 1 block is written in the last extent, HWM is 0 and didn't get counted.
		if (scannedExtents[lastExtent].HWM == 0)
			hwm++;
		lastScannedLBID = scannedExtents[lastExtent].range.start + (scannedExtents[lastExtent].HWM - 
			scannedExtents[lastExtent].blockOffset);
		if (hwm > 0)
			hwm--;
	}
#endif

	initExtentMarkers();
//	cout << "TBPSCount = " << ++TBPSCount << endl;
}

TupleBPS::TupleBPS(const PassThruStep& rhs) : fRm (rhs.fRm)
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
	fCPEvaluated = false;
	fEstimatedRows = 0; 
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
	fBPP->setOutputType(ROW_GROUP);
//	if (fOid>=3000)
//		cout << "BPS:initalized from PassThruStep. fSessionId=" << fSessionId << endl;
	
	finishedSending = sendWaiting = false;
	fSwallowRows = false;
	fNumBlksSkipped = 0;
	fPhysicalIO = 0;
	fCacheIO = 0;
	BPPIsAllocated = false;
	uniqueID = Unique32Generator::instance()->getUnique32();
	fBPP->setUniqueID(uniqueID);
	doJoin = false;
	hasPMJoin = false;
	hasUMJoin = false;
	fRunExecuted = false;
	isFilterFeeder = false;
	smallOuterJoiner = -1;
	
	// @1098 initialize scanFlags to be true
	scanFlags.assign(numExtents, true);
	runtimeCPFlags.assign(numExtents, true);
	die = false;
	bop = BOP_AND;

	runRan = joinRan = false;
	isDelivery = false;
	fExtendedInfo = "TBPS: ";
//	cout << "TBPSCount = " << ++TBPSCount << endl;
}

TupleBPS::TupleBPS(const pDictionaryStep& rhs) : fRm (rhs.fRm)
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
	fCPEvaluated = false;
	fEstimatedRows = 0; 
	//fColType = rhs.colType();
	alias(rhs.alias());
	view(rhs.view());
	name(rhs.name());

	fLogger = rhs.logger();
	finishedSending = sendWaiting = false;
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
	fBPP->setOutputType(ROW_GROUP);
	fPhysicalIO = 0;
	fCacheIO = 0;
	BPPIsAllocated = false;
	uniqueID = Unique32Generator::instance()->getUnique32();
	fBPP->setUniqueID(uniqueID);
	fCardinality = rhs.cardinality();
	doJoin = false;
	hasPMJoin = false;
	hasUMJoin = false;
	fRunExecuted = false;
	isFilterFeeder = false;
	smallOuterJoiner = -1;
	// @1098 initialize scanFlags to be true
	scanFlags.assign(numExtents, true);
	runtimeCPFlags.assign(numExtents, true);
	die = false;
	bop = BOP_AND;

	runRan = joinRan = false;
	isDelivery = false;
	fExtendedInfo = "TBPS: ";
//	cout << "TBPSCount = " << ++TBPSCount << endl;
}

TupleBPS::~TupleBPS()
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
			catch (const std::exception& e)
			{
				// log the exception
				cerr << "~TupleBPS caught: " << e.what() << endl;
				catchHandler(e.what());
			}
			catch (...)
			{
				cerr << "~TupleBPS caught unknown exception" << endl;
				catchHandler("~TupleBPS caught unknown exception");
			}
		}
		fDec->removeQueue(uniqueID);
	}
//	cout << "~TBPSCount = " << --TBPSCount << endl;
}

void TupleBPS::setBPP( JobStep* jobStep )
{ 
	fCardinality = jobStep->cardinality();

	pColStep* pcsp = dynamic_cast<pColStep*>(jobStep);

	int colWidth = 0;
	if (pcsp != 0)
	{
		fBPP->addFilterStep( *pcsp ); 

		extentsMap[pcsp->fOid] = tr1::unordered_map<int64_t, EMEntry>();
		tr1::unordered_map<int64_t, EMEntry> &ref = extentsMap[pcsp->fOid];
		for (uint z = 0; z < pcsp->extents.size(); z++)
			ref[pcsp->extents[z].range.start] = pcsp->extents[z];

		colWidth = (pcsp->colType()).colWidth;
		isFilterFeeder = pcsp->getFeederFlag();

		// it17 does not allow combined AND/OR, this pcolstep is for hashjoin optimization.
		if (bop == BOP_OR && isFilterFeeder == false)
			fBPP->setForHJ(true);
	}
	else 
	{
		pColScanStep* pcss = dynamic_cast<pColScanStep*>(jobStep);
		if (pcss != 0)	
		{
			fBPP->addFilterStep( *pcss, lastScannedLBID );

			extentsMap[pcss->fOid] = tr1::unordered_map<int64_t, EMEntry>();
			tr1::unordered_map<int64_t, EMEntry> &ref = extentsMap[pcss->fOid];
			for (uint z = 0; z < pcss->extents.size(); z++)
				ref[pcss->extents[z].range.start] = pcss->extents[z];

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

void TupleBPS::setProjectBPP( JobStep* jobStep1, JobStep* jobStep2 )
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
			if (!pcsp->isExeMgr())
				fBPP->setNeedRidsAtDelivery(true);
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

			extentsMap[pcsp->fOid] = tr1::unordered_map<int64_t, EMEntry>();
			tr1::unordered_map<int64_t, EMEntry> &ref = extentsMap[pcsp->fOid];
			for (uint z = 0; z < pcsp->extents.size(); z++)
				ref[pcsp->extents[z].range.start] = pcsp->extents[z];

			//@Bug 961
			if (!pcsp->isExeMgr())
				fBPP->setNeedRidsAtDelivery( true );
			colWidth = (pcsp->colType()).colWidth;
			projectOids.push_back(jobStep1->oid());
		}
		else 
		{
			PassThruStep* passthru = dynamic_cast<PassThruStep*>(jobStep1);
			if (passthru!= 0)	
			{	
				assert(!fBPP->getFilterSteps().empty());
				if (static_cast<CalpontSystemCatalog::OID>(fBPP->getFilterSteps().back()->getOID()) != passthru->oid())
				{
					SJSTEP pts;
					pts.reset(new pColStep(*passthru));
					pcsp = dynamic_cast<pColStep*>(pts.get());
					fBPP->addProjectStep(*pcsp);
					if (!passthru->isExeMgr())
						fBPP->setNeedRidsAtDelivery(true);
					colWidth = passthru->colType().colWidth;
					projectOids.push_back(pts->oid());
				}
				else
				{
					fBPP->addProjectStep( *passthru );
					//@Bug 961
					if ( !passthru->isExeMgr())
						fBPP->setNeedRidsAtDelivery( true );
					colWidth = (passthru->colType()).colWidth;
					projectOids.push_back(jobStep1->oid());
				}
			}
			else
			{
				DeliveryStep* ds = dynamic_cast<DeliveryStep*>(jobStep1);
				if ( ds != 0 ) {
					fBPP->addDeliveryStep( *ds );
					fTableName = ds->tableName();
				}
			}
		}
	}

	if ( colWidth > fColWidth )
	{
		fColWidth = colWidth;
	}
}

void TupleBPS::storeCasualPartitionInfo(const bool estimateRowCounts)
{
	const vector<SCommand>& colCmdVec = fBPP->getFilterSteps();
	vector<ColumnCommandJL*> cpColVec;
	vector<SP_LBIDList> lbidListVec;
	ColumnCommandJL* colCmd = 0;

	// @bug 2123.  We call this earlier in the process for the hash join estimation process now.  Return if we've already done the work.
	if(fCPEvaluated)
	{
		return;
	}
	fCPEvaluated = true;

	if (colCmdVec.size() == 0)
		return;
	
	// ZZ debug -- please keep it for a couple of releases
	// vector < vector<bool> > scanFs;
	// cout << "storeCasualPartitionInfo" << endl;
	
	for (uint i = 0; i < colCmdVec.size(); i++)
	{
		colCmd = dynamic_cast<ColumnCommandJL*>(colCmdVec[i].get());
		// bug 2116. skip CP check for function column
		if (!colCmd || colCmd->fcnOrd() !=0 ) 
			continue;

		SP_LBIDList tmplbidList(new LBIDList(0) );
		if (tmplbidList->CasualPartitionDataType(colCmd->getColType().colDataType, colCmd->getColType().colWidth))
		{
			lbidListVec.push_back(tmplbidList);
			cpColVec.push_back(colCmd);
		}

        // @Bug 3503. Use the total table size as the estimate for non CP columns.
        else if (fEstimatedRows == 0 && estimateRowCounts)
        {
			RowEstimator rowEstimator;
			fEstimatedRows = rowEstimator.estimateRowsForNonCPColumn(*colCmd);
        }

		// debug
	// vector<bool> scan;
	// scan.assign(numExtents, true);
	// scanFs.push_back(scan);
	}

	//cout << "cp column number=" << cpColVec.size() << " 1st col extents size= " << scanFlags.size() << endl;

	if (cpColVec.size() == 0)
		return;

	const bool ignoreCP = ((fTraceFlags & CalpontSelectExecutionPlan::IGNORE_CP) != 0);
	
	for (uint idx=0; idx <numExtents; idx++)
	{
		scanFlags[idx] = true;
		for (uint i = 0; i < cpColVec.size(); i++)
		{
			colCmd = cpColVec[i];
			EMEntry &extent = colCmd->getExtents()[idx];

			/* If any column filter eliminates an extent, it doesn't get scanned */
			scanFlags[idx] = scanFlags[idx] &&
				(ignoreCP || extent.partition.cprange.isValid != BRM::CP_VALID ||
					lbidListVec[i]->CasualPartitionPredicate(
							extent.partition.cprange.lo_val,
							extent.partition.cprange.hi_val,
							&(colCmd->getFilterString()),
							colCmd->getFilterCount(),
							colCmd->getColType(),
							colCmd->getBOP())
				);
			if (!scanFlags[idx])
				break;
		}

/*  Original version.  The code above should be equivalent.
 			scanFlags[idx] = false;
			colCmd = cpColVec[i];

			// if any extents that logically lined up with the first evaluate 
			// true then this colCmd gives a true.
			// If tracing says to ignore cp data, we still want to do all the work, then skip
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
*/

	}

	// @bug 2123.  Use the casual partitioning information to estimate the number of rows that will be returned for use in estimating
	// the large side table for hashjoins.
	if(estimateRowCounts)
	{
		RowEstimator rowEstimator;
		fEstimatedRows = rowEstimator.estimateRows(cpColVec, scanFlags, dbrm, fOid);
	}
	
	// debug
	/*for (uint i = 0; i < scanFs.size(); i++)
	{
			cout << "column " << i << ": ";
		for (uint k = 0; k < scanFs[i].size(); k++)
			cout << scanFs[i][k] << " ";
		cout << endl;
	}
	cout << "final: "
	for (uint i = 0; i < scanFlags.size(); i++)
		cout << scanFlags[i] << " ";
	cout << endl;*/
}

void TupleBPS::startPrimitiveThread()
{
	pThread.reset(new boost::thread(TupleBPSPrimitive(this)));
}

void TupleBPS::startAggregationThreads()
{
 	//fNumThreads = 1;
	for (uint i = 0; i < fNumThreads; i++)
		fProducerThread[i].reset(new boost::thread(TupleBPSAggregators(this, i)));
}

void TupleBPS::serializeJoiner()
{
	ByteStream bs;
	bool more = true;

	/* false from nextJoinerMsg means it's the last msg,
		it's not exactly the exit condition*/
	while (more) {
		more = fBPP->nextTupleJoinerMsg(bs);
#ifdef JLF_DEBUG
		cout << "serializing joiner into " << bs.length() << " bytes" << endl;
#endif
		fDec->write(bs);
		bs.restart();
	}
}

void TupleBPS::serializeJoiner(uint conn)
{
	ByteStream bs;
	bool more = true;

	/* false from nextJoinerMsg means it's the last msg,
		it's not exactly the exit condition*/
	while (more) {
		more = fBPP->nextTupleJoinerMsg(bs);
		fDec->write(bs, conn);
		bs.restart();
	}
}

bool TupleBPS::goodExtentCount()
{
	uint eCount = extentsMap.begin()->second.size();
	map<CalpontSystemCatalog::OID, tr1::unordered_map<int64_t, EMEntry> >
	  ::iterator it;

	for (it = extentsMap.begin(); it != extentsMap.end(); ++it)
		if (it->second.size() != eCount)
			return false;
	return true;
}

void TupleBPS::initExtentMarkers()
{
	tr1::unordered_map<int64_t, struct BRM::EMEntry> &ref = extentsMap[fOid];
    tr1::unordered_map<int64_t, struct BRM::EMEntry>::iterator it;
	uint lastExtent = 0;

    scannedExtents.clear();
    for (it = ref.begin(); it != ref.end(); ++it)
    	scannedExtents.push_back(it->second);

    sort(scannedExtents.begin(), scannedExtents.end(), ExtentSorter());

    numExtents = scannedExtents.size();
    // @1098 initialize scanFlags to be true
    scanFlags.assign(numExtents, true);
    runtimeCPFlags.assign(numExtents, true);

	hwm = 0;
	if (scannedExtents.size()>0)
	{
		for (uint i = 0; i < scannedExtents.size(); i++) {
			if (scannedExtents[i].HWM != 0 && scannedExtents[i].status != EXTENTOUTOFSERVICE)
				hwm += (scannedExtents[i].HWM + 1);
			if ( (scannedExtents[i].status == EXTENTAVAILABLE) &&
				((scannedExtents[i].partitionNum >  scannedExtents[lastExtent].partitionNum) ||
				((scannedExtents[i].partitionNum == scannedExtents[lastExtent].partitionNum) &&
				 (scannedExtents[i].blockOffset  >  scannedExtents[lastExtent].blockOffset)) ||
				((scannedExtents[i].partitionNum == scannedExtents[lastExtent].partitionNum) &&
				 (scannedExtents[i].blockOffset  == scannedExtents[lastExtent].blockOffset) &&
				 (scannedExtents[i].segmentNum   >= scannedExtents[lastExtent].segmentNum))) )
			lastExtent = i;
		}
		// if only 1 block is written in the last extent, HWM is 0 and didn't get counted.
		if (scannedExtents[lastExtent].HWM == 0)
			hwm++;
		lastScannedLBID = scannedExtents[lastExtent].range.start + (scannedExtents[lastExtent].HWM -
			scannedExtents[lastExtent].blockOffset);
		if (hwm > 0)
			hwm--;
	}
}


void TupleBPS::reloadExtentLists()
{
	/*
	 * Iterate over each ColumnCommand instance
	 *
	 * 1) reload its extent array
	 * 2) update TupleBPS's extent array
	 * 3) update vars dependent on the extent layout (lastExtent, scanFlags, etc)
	 */

	uint i, j;
	ColumnCommandJL *cc;
	vector<SCommand> &filters = fBPP->getFilterSteps();
	vector<SCommand> &projections = fBPP->getProjectSteps();
	uint32_t oid;

	/* To reduce the race, make all CC's get new extents as close together
	 * as possible, then rebuild the local copies.
	 */

	for (i = 0; i < filters.size(); i++) {
		cc = dynamic_cast<ColumnCommandJL *>(filters[i].get());
		if (cc != NULL)
			cc->reloadExtents();
	}

	for (i = 0; i < projections.size(); i++) {
		cc = dynamic_cast<ColumnCommandJL *>(projections[i].get());
		if (cc != NULL)
			cc->reloadExtents();
	}

	extentsMap.clear();

	for (i = 0; i < filters.size(); i++) {
		cc = dynamic_cast<ColumnCommandJL *>(filters[i].get());
		if (cc == NULL)
			continue;

		vector<EMEntry> &extents = cc->getExtents();
		oid = cc->getOID();

		extentsMap[oid] = tr1::unordered_map<int64_t, struct BRM::EMEntry>();
		tr1::unordered_map<int64_t, struct BRM::EMEntry> &mref = extentsMap[oid];
		for (j = 0; j < extents.size(); j++)
			mref[extents[j].range.start] = extents[j];
	}

	for (i = 0; i < projections.size(); i++) {
		cc = dynamic_cast<ColumnCommandJL *>(projections[i].get());
		if (cc == NULL)
			continue;

		vector<EMEntry> &extents = cc->getExtents();
		oid = cc->getOID();

		extentsMap[oid] = tr1::unordered_map<int64_t, struct BRM::EMEntry>();
		tr1::unordered_map<int64_t, struct BRM::EMEntry> &mref = extentsMap[oid];
		for (j = 0; j < extents.size(); j++)
			mref[extents[j].range.start] = extents[j];
	}

	initExtentMarkers();
}

void TupleBPS::run()
{
	uint i;
	boost::mutex::scoped_lock lk(jlLock);
	uint retryCounter = 0;
	const uint retryMax = 1000;   // 50s max; we've seen a 15s window so 50s should be 'safe'
	const uint waitInterval = 50000;  // in us


	if (fRunExecuted)
		return;

// 	cout << "TupleBPS::run()\n";

	fRunExecuted = true;

#ifdef PROFILE
	if (fOid>=3000)
	{
		fProfileData.initialize("");

		ProfileGroup sendGroup, recvGroup;
		sendGroup.initialize(numOfSendCheckPoints, CheckPoints);
		recvGroup.initialize(numOfRecvCheckPoints, CheckPoints+recvCheckpointOffset);

		fProfileData.addGroup(sendGroup);
		fProfileData.addGroup(recvGroup);
	}
#endif
	if (traceOn())
	{
		syslogStartStep(16,           // exemgr subsystem
			std::string("TupleBPS")); // step name
	}

	// make sure each numeric column has the same # of extents! See bugs 4564 and 3607
	try {
		while (!goodExtentCount() && retryCounter++ < retryMax) {
			usleep(waitInterval);
			reloadExtentLists();
		}
	}
	catch (std::exception &e) {
		ostringstream os;
		os << "TupleBPS: Could not get a consistent extent count for each column.  Got '"
		  << e.what() << "'\n";
		catchHandler(os.str());
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(tupleBPSErr);
		fOutputJobStepAssociation.outAt(0)->rowGroupDL()->endOfInput();
		return;
	}

	if (retryCounter == retryMax) {
		catchHandler("TupleBPS: Could not get a consistent extent count for each column.");
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(tupleBPSErr);
		fOutputJobStepAssociation.outAt(0)->rowGroupDL()->endOfInput();
		return;
	}

	ByteStream bs;

	if (doJoin) {
		for (i = 0; i < smallSideCount; i++)
			if (isDelivery)
				tjoiners[i]->setThreadCount(1);
			else
				tjoiners[i]->setThreadCount(fNumThreads);
	}

	if (fe1)
		fBPP->setFEGroup1(fe1, fe1Input);
	if (fe2 && runFEonPM)
		fBPP->setFEGroup2(fe2, fe2Output);
	if (fe2) {
		if (isDelivery) {
			fe2Data.reset(new uint8_t[fe2Output.getMaxDataSize()]);
			fe2Output.setData(fe2Data.get());
		}
		primRowGroup.initRow(&fe2InRow);
		fe2Output.initRow(&fe2OutRow);
	}
/*
	if (doJoin) {
		for (uint z = 0; z < smallSideCount; z++)
			cout << "join #" << z << " " << "0x" << hex << tjoiners[z]->getJoinType()
			  << std::dec << " typeless: " << (uint) tjoiners[z]->isTypelessJoin() << endl;
	}
*/

	try {
		fDec->addDECEventListener(this);
		fBPP->createBPP(bs);
		fDec->write(bs);
		BPPIsAllocated = true;
		if (doJoin && tjoiners[0]->inPM())
			serializeJoiner();

		startPrimitiveThread();

		if (!isDelivery)
			startAggregationThreads();
	}
	catch (const IDBExcept &e) {
		catchHandler(e.what());
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(e.errorCode());
		fOutputJobStepAssociation.outAt(0)->rowGroupDL()->endOfInput();
	}
	catch (const std::exception& e)
	{
		// log the exception
		cerr << "tuple-bps::run() caught: " << e.what() << endl;
		catchHandler(e.what());
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(tupleBPSErr);
		fOutputJobStepAssociation.outAt(0)->rowGroupDL()->endOfInput();
	}
	catch (...)
	{
		cerr << "tuple-bps::run() caught unknown exception" << endl;
		catchHandler("tuple-bps::run() caught unknown exception");
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(tupleBPSErr);
		fOutputJobStepAssociation.outAt(0)->rowGroupDL()->endOfInput();
	}
}

void TupleBPS::join()
{
	boost::mutex::scoped_lock lk(jlLock);
	if (joinRan)
		return;

	joinRan = true;

	if (fRunExecuted)
	{
		if (isDelivery && msgsRecvd < msgsSent) {
			// wake up the sending thread, it should drain the input dl and exit
			mutex.lock(); //pthread_mutex_lock(&mutex);
			condvarWakeupProducer.notify_all(); //pthread_cond_broadcast(&condvarWakeupProducer);
			mutex.unlock(); //pthread_mutex_unlock(&mutex);
			// wait for the PM to return all results before destroying the BPPs
			while (msgsRecvd + fDec->size(uniqueID) < msgsSent)
#ifdef _MSC_VER
				Sleep(1 * 1000);
#else
				sleep(1);
#endif
		}

		if (pThread)
			pThread->join();

		if (!isDelivery)
			for (uint i = 0; i < fNumThreads; i++)
				fProducerThread[i]->join();
		if (BPPIsAllocated) {
			ByteStream bs;
			fDec->removeDECEventListener(this);
			fBPP->destroyBPP(bs);

			try {
				fDec->write(bs);
			}
			catch (const IDBExcept &e) {
				catchHandler(e.what());
				if (fOutputJobStepAssociation.status() == 0)
					fOutputJobStepAssociation.status(e.errorCode());
				fOutputJobStepAssociation.outAt(0)->rowGroupDL()->endOfInput();
			}
			catch (const std::exception& e)
			{
				// log the exception
				cerr << "tuple-bps::join() write(bs) caught: " << e.what() << endl;
				catchHandler(e.what());
			}
			catch (...)
			{
				cerr << "tuple-bps::join() write(bs) caught unknown exception" << endl;
				catchHandler("tuple-bps::join() write(bs) caught unknown exception");
			}

			BPPIsAllocated = false;
			fDec->removeQueue(uniqueID);
			tjoiners.clear();
		}
	}
}

void TupleBPS::sendError(uint16_t status)
{
	ByteStream msgBpp;
	fBPP->setCount ( 1 );
	fBPP->setStatus( status );
	fBPP->runErrorBPP(msgBpp);
	fMsgBytesOut += msgBpp.lengthWithHdrOverhead();
	try {
		fDec->write(msgBpp);
	}
	catch(...) {
		// this fcn is only called in exception handlers
		// let the first error take precidence
	}

	fMsgBppsToPm++;
	fBPP->reset();
//	msgsSent++;   // not expecting a response from this msg
	finishedSending = true;
	condvar.notify_all(); //pthread_cond_broadcast(&condvar);
	condvarWakeupProducer.notify_all(); //pthread_cond_broadcast(&condvarWakeupProducer);
}

/* XXXPAT: This function is ridiculous.  Need to replace it with code that uses
 * the multithreaded recv fcn & a FIFO once we get tighter control over memory usage.
 */
uint TupleBPS::nextBand ( messageqcpp::ByteStream &bs )
{
	int64_t min;
	int64_t max;
	uint64_t lbid;
	uint32_t cachedIO=0;
	uint32_t physIO=0;
	uint32_t touchedBlocks=0;
	bool validCPData = false;
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	boost::shared_array<uint8_t> rgData;
	uint rows = 0;
	RowGroup &realOutputRG = (fe2 ? fe2Output : primRowGroup);

	bs.restart();

again:
try
{
	while (msgsRecvd == msgsSent && !finishedSending &&
	  !fInputJobStepAssociation.status())
		usleep ( 1000 );

	/* "If there are no more messages expected, or if there was an error somewhere else in the joblist..." */
	if ((msgsRecvd == msgsSent && finishedSending) || fInputJobStepAssociation.status())
	{
		bsIn.reset(new ByteStream());
		if ( fOid>=3000 && traceOn() ) dlTimes.setLastReadTime();
		if ( fOid>=3000 && traceOn() ) dlTimes.setEndOfInputTime();
#ifdef PROFILE
		if ( fOid>=3000 ) fProfileData.start ( 0, tableBand_c );
#endif
		/* "If there was an error, make a response with the error code... */
		if ( die || fBPP->getStatus() || 0 < fInputJobStepAssociation.status() )
		{
			/* Note, doesn't matter which rowgroup is used here */
			rgData = fBPP->getErrorRowGroupData(fInputJobStepAssociation.status());
			cout << "TBPS: returning error status " << fInputJobStepAssociation.status() << endl;
			rows = 0;
			if (!fInputJobStepAssociation.status())
				fInputJobStepAssociation.status(fBPP->getStatus());
			bs.load(rgData.get(), primRowGroup.getEmptySize());
		}
		else
		{
			/* else, send the special, 0-row last message. */
			bool unused;
			rgData = fBPP->getRowGroupData( *bsIn, &validCPData, &lbid, &min, &max, &cachedIO, &physIO, &touchedBlocks, &unused, 0);

			primRowGroup.setData(rgData.get());
			if (fe2)
			{
				if (!runFEonPM)
					processFE2_oneRG(primRowGroup, fe2Output, fe2InRow, fe2OutRow, fe2.get());
				else
					fe2Output.setData(rgData.get());
			}

			rows = realOutputRG.getRowCount();
			bs.load(realOutputRG.getData(), realOutputRG.getDataSize());

#ifdef PROFILE
			if ( fOid>=3000 ) fProfileData.stop ( 0, tableBand_c );
#endif
			/* send the cleanup commands to the PM & DEC */
			ByteStream dbs;
			fDec->removeDECEventListener(this);
			fBPP->destroyBPP(dbs);
			try {
				fDec->write(dbs);
			}
			catch (...) {
				// return the result since it's
				// completed instead of returning an error.
			}

			BPPIsAllocated = false;
			fDec->removeQueue(uniqueID);
			tjoiners.clear();

			/* A pile of stats gathering */
			if ( fOid>=3000 )
			{
				struct timeval tvbuf;
				gettimeofday(&tvbuf, 0);
				FIFO<boost::shared_array<uint8_t> > *pFifo = 0;
				uint64_t totalBlockedReadCount  = 0;
				uint64_t totalBlockedWriteCount = 0;

				//...Sum up the blocked FIFO reads for all input associations
				size_t inDlCnt  = fInputJobStepAssociation.outSize();
				for ( size_t iDataList=0; iDataList<inDlCnt; iDataList++ )
				{
					pFifo = dynamic_cast<FIFO<boost::shared_array<uint8_t> > *>(
						fInputJobStepAssociation.outAt(iDataList)->rowGroupDL());
					if ( pFifo )
					{
						totalBlockedReadCount += pFifo->blockedReadCount();
					}
				}

				//...Sum up the blocked FIFO writes for all output associations
				size_t outDlCnt = fOutputJobStepAssociation.outSize();
				for ( size_t iDataList=0; iDataList<outDlCnt; iDataList++ )
				{
					pFifo = dynamic_cast<FIFO<boost::shared_array<uint8_t> > *>(
						fOutputJobStepAssociation.outAt(iDataList)->rowGroupDL());
					if ( pFifo )
					{
						totalBlockedWriteCount += pFifo->blockedWriteCount();
					}
				}

				if ( traceOn() )
				{
					//...Roundoff msg byte counts to nearest KB for display
					uint64_t msgBytesInKB  = fMsgBytesIn >> 10;
					uint64_t msgBytesOutKB = fMsgBytesOut >> 10;
					if (fMsgBytesIn & 512)
						msgBytesInKB++;
					if (fMsgBytesOut & 512)
						msgBytesOutKB++;

					ostringstream logStr;
					logStr << "ses:" << fSessionId << " st: " << fStepId<<" finished at "<<
					JSTimeStamp::format(tvbuf) << "; PhyI/O-" << fPhysicalIO << "; CacheI/O-"  <<
					fCacheIO << "; MsgsSent-" << fMsgBppsToPm << "; MsgsRvcd-" << msgsRecvd <<
					"; BlocksTouched-"<< fBlockTouched <<
					"; BlockedFifoIn/Out-" << totalBlockedReadCount <<
					"/" << totalBlockedWriteCount <<
					"; output size-" << rowsReturned << endl <<
					"\tPartitionBlocksEliminated-" << fNumBlksSkipped <<
					"; MsgBytesIn-"  << msgBytesInKB  << "KB" <<
					"; MsgBytesOut-" << msgBytesOutKB << "KB" << endl  <<
					"\t1st read " << dlTimes.FirstReadTimeString() << 
					"; EOI " << dlTimes.EndOfInputTimeString() << "; runtime-" <<
					JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(),dlTimes.FirstReadTime())
					<< "s\n\tJob completion status " << fOutputJobStepAssociation.status() << endl;
					logEnd ( logStr.str().c_str() );
					syslogReadBlockCounts ( 16,                 // exemgr sybsystem
									fPhysicalIO,                // # blocks read from disk
									fCacheIO,                   // # blocks read from cache
									fNumBlksSkipped );          // # casual partition block hits
					syslogProcessingTimes ( 16,                 // exemgr subsystem
									dlTimes.FirstReadTime(),    // first datalist read
									dlTimes.LastReadTime(),     // last  datalist read
									dlTimes.FirstInsertTime(),  // first datalist write
									dlTimes.EndOfInputTime() ); // last (endOfInput) datalist write
					syslogEndStep ( 16,                         // exemgr subsystem
									totalBlockedReadCount,      // blocked datalist input
									totalBlockedWriteCount,     // blocked datalist output
									fMsgBytesIn,                // incoming msg byte count
									fMsgBytesOut );             // outgoing msg byte count
					fExtendedInfo += toString() + logStr.str();
					formatMiniStats();
				}

#ifdef PROFILE
				if ( fOid>=3000 )
				{
					cout << "BPS (st: " << fStepId << ") execution stats:" << endl;
					cout << "  total runtime: " << fProfileData << endl;
				}
#endif
			}

			if ( fOid>=3000 && ( ffirstStepType == SCAN ) && bop == BOP_AND) {
				cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
				lbidList->UpdateAllPartitionInfo();
				cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
			}
		}
	}

	/* .. else, this is the next msg to process from the PM... */
	else
	{
		if ( fOid>=3000 && traceOn() && dlTimes.FirstReadTime().tv_sec ==0 )
			dlTimes.setFirstReadTime();
		fDec->read(uniqueID, bsIn );
#ifdef PROFILE
		if ( fOid>=3000 && firstRead ) fProfileData.stop ( 0, sendPrimMsg_c );
#endif
		firstRead = false;
		fMsgBytesIn += bsIn->lengthWithHdrOverhead();
		if (bsIn->length() != 0 && fBPP->countThisMsg(*bsIn)) {
			mutex.lock(); //pthread_mutex_lock(&mutex);
			msgsRecvd++;
			//@Bug 1424, 1298
			if ((sendWaiting) && ((msgsSent - msgsRecvd) <=
			  (fMaxOutstandingRequests << LOGICAL_EXTENT_CONVERTER)))
			{
				condvarWakeupProducer.notify_one(); //pthread_cond_signal(&condvarWakeupProducer);
				THROTTLEDEBUG << "nextBand wakes up sending side .. " << "  msgsSent: " << msgsSent << "  msgsRecvd = " << msgsRecvd << endl;
			}
			mutex.unlock(); //pthread_mutex_unlock(&mutex);
		}

#ifdef PROFILE
		if ( fOid>=3000 ) fProfileData.start ( 0, tableBand_c );
#endif

		ISMPacketHeader *hdr = (ISMPacketHeader*)bsIn->buf();

		// Check for errors and abort
		if (bsIn->length() == 0 || 0 < hdr->Status || 0 < fBPP->getStatus() || 0 < fOutputJobStepAssociation.status() )
		{
			if (bsIn->length() == 0)
				fOutputJobStepAssociation.status(primitiveServerErr);
			else if (hdr->Status) {
				string errmsg;
				bsIn->advance(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
				*bsIn >> errmsg;
				fOutputJobStepAssociation.status(hdr->Status);
				fOutputJobStepAssociation.errorMessage(errmsg);
			}
			mutex.lock(); //pthread_mutex_lock(&mutex);

			/* What is this supposed to do? */
			if (!fSwallowRows)
			{
				msgsRecvd = msgsSent;
				finishedSending = true;
				rows = 0;  //send empty message to indicate finished.
			}
			abort_nolock();
			//cout << "BPS receive signal " << fStepId << endl;
			mutex.unlock(); //pthread_mutex_unlock(&mutex);
		}
		/* " ... no error, process the message */
		else
		{
			bool unused;
			rgData = fBPP->getRowGroupData( *bsIn, &validCPData, &lbid, &min, &max, &cachedIO, &physIO, &touchedBlocks, &unused, 0);

			primRowGroup.setData(rgData.get());
			if (fe2) {
				if (!runFEonPM)
					processFE2_oneRG(primRowGroup, fe2Output, fe2InRow, fe2OutRow, fe2.get());
				else
					fe2Output.setData(rgData.get());
			}

			if (dupColumns.size() > 0)
				dupOutputColumns(realOutputRG);

			if ( fOid>=3000 && ( ffirstStepType == SCAN ) && bop == BOP_AND)
			{
				cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
				lbidList->UpdateMinMax(min, max, lbid, fColType.colDataType, validCPData);
				cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
			}

			rows = realOutputRG.getRowCount();
// 			cout << "realOutputRG toString()\n";
// 			cout << realOutputRG.toString() << endl;
// 			cout << "  -- realout.. rowcount=" << realOutputRG.getRowCount() << "\n";
			bs.load(realOutputRG.getData(), realOutputRG.getDataSize());
// 			cout << "loaded\n";

		}
#ifdef PROFILE
		if ( fOid>=3000 ) fProfileData.stop ( 0, tableBand_c );
#endif

		fPhysicalIO += physIO;
		fCacheIO += cachedIO;
		fBlockTouched += touchedBlocks;

#if 0
		if ( fOid>=3000 && ( ffirstStepType == SCAN ) && bop == BOP_AND)
		{
			cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
			lbidList->UpdateMinMax(min, max, lbid, fColType.colDataType, validCPData);
			cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
		}
#endif

		rowsReturned += rows;
		if (rows == 0)
		{
			bs.restart();
			goto again;
		}
	}
}//try
catch(const std::exception& ex)
{
	std::string errstr("TupleBPS next band caught exception: ");
	errstr.append(ex.what());
	catchHandler(errstr, fSessionId);
	fOutputJobStepAssociation.status(tupleBPSErr);
	bs.restart();
	rgData = fBPP->getErrorRowGroupData(tupleBPSErr);
	primRowGroup.setData(rgData.get());
	bs.load(rgData.get(), primRowGroup.getDataSize());
	return 0;
}
catch(...)
{
	catchHandler("TupleBPS next band caught an unknown exception", fSessionId);
	fOutputJobStepAssociation.status(tupleBPSErr);
	bs.restart();
	rgData = fBPP->getErrorRowGroupData(tupleBPSErr);
	primRowGroup.setData(rgData.get());
	bs.load(rgData.get(), primRowGroup.getDataSize());
	return 0;
}
	return rows;
}

void TupleBPS::sendPrimitiveMessages()
{
	int it = -1;
	bool more = false;
	ByteStream msgBpp;
	int msgsSkip=0;
	
	int64_t Min=0;
	int64_t Max=0;
	int64_t SeqNum=0;
	bool MinMaxValid=true;
	RowGroupDL *in=0;
	boost::shared_array<uint8_t> rgData;
	const std::tr1::unordered_map<int64_t, BRM::EMEntry> &extents = extentsMap[fOid];

	if (fInputJobStepAssociation.status() != 0)
		goto abort;

try
{
	if (fOid >= 3000 && bop == BOP_AND)
		storeCasualPartitionInfo(false);

	// Get the starting DBRoot for the column.  We want to send use the starting DBRoot to send the right messages
	// to the right PMs.  If the first extent, is on DBRoot1, the message will go to the first connection.  If it's
	// DBRoot2, the first extent will get sent to the second connection, and so on.  This is a change for the 
	// multiple files per OID enhancement as a precurser to shared nothing.
	uint16_t startingDBRoot = 1;
	uint32_t startingPartitionNumber = 0;
	dbrm.getStartExtent(fOid, startingDBRoot, startingPartitionNumber);	

	if ( ffirstStepType == SCAN )
	{
#ifdef PROFILE
	if (fOid>=3000) fProfileData.start(0, sendWork_Scan);
#endif
		uint32_t  extentIndex;
		uint64_t  fbo;
		bool cpPredicate = true;
		bool firstSend = true;
		//@Bug 913 
		if (fOid>=3000 && traceOn() && dlTimes.FirstReadTime().tv_sec==0) {
			dlTimes.setFirstReadTime();
		}

		LBIDRange *it;
		LBIDRange tmpLBIDRange;
		it = &tmpLBIDRange;
		for (extentIndex = 0, fbo = 0; extentIndex < scannedExtents.size();
				fbo += it->size, extentIndex++)   {

			tmpLBIDRange.start = scannedExtents[extentIndex].range.start;
			tmpLBIDRange.size = scannedExtents[extentIndex].range.size * 1024;
			if (hwm < fbo)
				continue;

			if (fOid >= 3000 && bop == BOP_AND)
			{
				// @bug 1297. getMinMax to prepare partition vector should consider
				// cpPredicate. only scanned lbidrange will be pushed into the vector.

				cpPredicate = scanFlags[extentIndex] && runtimeCPFlags[extentIndex];
				//if (scanFlags[extentIndex] && !runtimeCPFlags[extentIndex])
				//	cout << "HJ feedback eliminated an extent!\n";

				// @bug 1090. cal getMinMax to prepare the CP vector and update later
				if (cpPredicate && lbidList->CasualPartitionDataType(fColType.colDataType, fColType.colWidth)) {
					cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
					MinMaxValid = lbidList->GetMinMax(&Min, &Max, &SeqNum,
									it->start, extents);
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
			
			remainingLbids =  totalLbid; // logical blocks
//			cout << "fColWidth=" << fColWidth<< endl;
//			msgLbidCount = extentSize/fColWidth;

			// @bug 977
			// @bug 1264.  Changed the number of logical blocks per BPS scan to be configurable based on the LogicalBlocksPerScan Calpont.xml
			//             entry.
			// msgLbidCount = extentSize/8;
			if ( fColWidth == 0 )
				fColWidth = fColType.colWidth;

			//We've lost the original CW depending on the other cols in the query, recalc it here
			const int realColWidth = extentSize / fExtentRows;
			requestLbids = fRequestSize * (extentSize / realColWidth);
			msgLbidCount =(requestLbids/fProcessorThreadsPerScan)/fRequestSize;
			maxOutstandingLbids = fMaxOutstandingRequests * (extentSize / realColWidth);
#if 0
cout << " rs = " << fRequestSize;
cout << " es = " << extentSize;
cout << " cw = " << fColWidth;
cout << " rcw = " << realColWidth;
cout << " rl (rs*(es/rcw) = " << requestLbids;
cout << " tps = " << fProcessorThreadsPerScan;
cout << " mlc ((rl/tps)/rs) = " << msgLbidCount;
cout << " mor = " << fMaxOutstandingRequests;
cout << " mol (mor*(es/rcw)) = " << maxOutstandingLbids;
cout << endl;
#endif
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
					/**
					if ( fOid>=3000) {
						cout << "st:" << fStepId
							<< " LbidStart=" << msgLbidStart 
							<< " LbidCount="<< msgLbidCountBatch
							<< " colW=" << fColWidth
							<< " oid = " << fOid
							<< " numExts:" << numExtents 
							<< " extSize:  " << extentSize
							<< " hwm: " << hwm
							<< " sentLbids: " << sentLbids
							<< " reqLbids: " << requestLbids
							<< " remLbids: " << remainingLbids
							<< " PTR: " << fProcessorThreadsPerScan
							<< endl;
					}
					**/

					fBPP->setLBID( msgLbidStart );
					assert(msgLbidCountBatch>0);
					fBPP->setCount ( msgLbidCountBatch );
					fBPP->runBPP(msgBpp, startingDBRoot);
					fMsgBytesOut += msgBpp.lengthWithHdrOverhead();
					//cout << "Requesting " << msgLbidCount << " logical blocks at " << msgLbidStart;
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
					msgLbidStart += msgLbidCountBatch * fColType.colWidth; //OK
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
		/*
			read a RowGroup from input
			check casual partitioning
			build the msg
			wait for throttling
			send it
			update all the counters
		*/

		uint64_t baseRid;
		const uint throttleCheckInterval = fRequestSize << LOGICAL_EXTENT_CONVERTER;
		uint lastCheck = 0;

		assert(fInputJobStepAssociation.outSize() >= 1);
		in = fInputJobStepAssociation.outAt(0)->rowGroupDL();
		assert(in);
		it = in->getIterator();

		more = in->next(it, &rgData);
		if (fOid >= 3000 && traceOn() && dlTimes.FirstReadTime().tv_sec == 0)
			dlTimes.setFirstReadTime();

		for (; more && !die; more = in->next(it, &rgData)) {

			/* error checking */
			if (fInputJobStepAssociation.status() > 0) {
				sendError(fInputJobStepAssociation.status());
				break;
			}

			/* grab the data */
			inputRowGroup.setData(rgData.get());

			/* Check casual partitioning */
			baseRid = inputRowGroup.getBaseRid();
			if (!scanit(baseRid) && bop == BOP_AND) {
				msgsSkip++;
				continue;
			}

			/* build the msg & do accounting */
			ridsRequested += inputRowGroup.getRowCount();
			if (inputRowGroup.getRowCount() == 0)
				continue;

			fBPP->setRowGroupData(inputRowGroup);
			fBPP->runBPP(msgBpp, startingDBRoot);

			/* Do some elaborate throttling */
			if (lastCheck >= throttleCheckInterval) {
				lastCheck = 0;
				mutex.lock(); //pthread_mutex_lock(&mutex);

				while (msgsSent - msgsRecvd >
				  (fMaxOutstandingRequests << LOGICAL_EXTENT_CONVERTER) && !die) {
					sendWaiting = true;
					condvarWakeupProducer.wait(mutex); //pthread_cond_wait(&condvarWakeupProducer, &mutex);
					sendWaiting = false;
				}
				mutex.unlock(); //pthread_mutex_unlock(&mutex);
			}
			else
				lastCheck++;

			/* Send the msg */
			#ifdef SHARED_NOTHING_DEMO
			fDec->write(msgBpp, fOid);
			#else
			fDec->write(msgBpp);
			#endif
			fMsgBytesOut += msgBpp.lengthWithHdrOverhead();

			fBPP->reset();
			msgBpp.restart();
			msgsSent++;
		}

	/* last bunch of accounting */
	fMsgBppsToPm = msgsSent;
	}
	
	if (fOid>=3000 && traceOn()) dlTimes.setLastReadTime();

}  // try
catch(IDBExcept &e)
{
	finishedSending = true;
	msgBpp.reset();
	sendError(e.errorCode());
	processError(e.what(), e.errorCode(), "TupleBPS::SendPrimitiveMessages()");
}
catch(const std::exception& ex)
{
	finishedSending = true;
	msgBpp.reset();
	sendError(tupleBPSErr);
	processError(ex.what(), tupleBPSErr, "TupleBPS::SendPrimitiveMessages()");
}
catch(...)
{
	finishedSending = true;
	msgBpp.reset();
	sendError(tupleBPSErr);
	processError("unknown", tupleBPSErr, "TupleBPS::SendPrimitiveMessages()");
}

abort:
	if (more && (die || fInputJobStepAssociation.status() > 0) && 
	  ffirstStepType != SCAN)
		while (more)
			more = in->next(it, &rgData);

	mutex.lock(); //pthread_mutex_lock(&mutex);
	finishedSending = true;
//	cout << "send finished, requested " << msgsSent << " logical blocks\n";
	if (recvWaiting)
		condvar.notify_all(); //pthread_cond_broadcast(&condvar); //signal consumer to resume
	mutex.unlock(); //pthread_mutex_unlock(&mutex);
	msgBpp.reset();
	//if ( fOid >=3000 )
	//	cout << "st: "<< fStepId << " requested " << ridsRequested << endl;
		
	//...Track the number of LBIDs we skip due to Casual Partioning.
	fNumBlksSkipped += msgsSkip;
#ifdef PROFILE
	if (fOid>=3000) fProfileData.stop(0, sendWork_Scan);
#endif

}

struct _CPInfo {
	_CPInfo(int64_t MIN, int64_t MAX, uint64_t l, bool val) : min(MIN), max(MAX), LBID(l), valid(val) { };
	int64_t min;
	int64_t max;
	uint64_t LBID;
	bool valid;
};

void TupleBPS::receiveMultiPrimitiveMessages(uint threadID)
{
	AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
	RowGroupDL* dlp = dl->rowGroupDL();

	uint size = 0, vecSize = 0;
	vector<boost::shared_ptr<ByteStream> > bsv;

	boost::shared_array<uint8_t> rgData;
	vector<boost::shared_array<uint8_t> > rgDatav;

	bool validCPData;
	int64_t min;
	int64_t max;
	uint64_t lbid;
	vector<_CPInfo> cpv;
	uint32_t cachedIO;
	uint32_t physIO;
	uint32_t touchedBlocks;
	uint64_t msgBytesIn_Thread = 0;
	uint32_t cachedIO_Thread = 0;
	uint32_t physIO_Thread = 0;
	uint32_t touchedBlocks_Thread = 0;
	int64_t ridsReturned_Thread = 0;
	bool lastThread = false;
	uint i, j, k;
	RowGroup local_primRG = primRowGroup;
	RowGroup local_outputRG = outputRowGroup;
	bool didEOF = false;
	bool unused;

	/* Join vars */
	vector<vector<uint8_t *> > joinerOutput;   // clean usage
	Row largeSideRow, joinedBaseRow, largeNull, joinFERow;  // LSR clean
	scoped_array<Row> smallSideRows, smallNulls;
	scoped_array<uint8_t> joinedBaseRowData;
	scoped_array<uint8_t> joinFERowData;
	shared_array<int> largeMapping;
	vector<shared_array<int> > smallMappings;
	vector<shared_array<int> > fergMappings;
	shared_array<uint8_t> joinedData;
	scoped_array<uint8_t> largeNullMemory;
	scoped_array<shared_array<uint8_t> > smallNullMemory;
	uint matchCount;

	/* Thread-scoped F&E 2 var */
	Row postJoinRow;    // postJoinRow is also used for joins
	RowGroup local_fe2Output;
	scoped_array<uint8_t> local_fe2Data;
	Row local_fe2OutRow;
	funcexp::FuncExpWrapper local_fe2;

try
{
	if (doJoin || fe2) {
		local_outputRG.initRow(&postJoinRow);
	}
	if (fe2) {
		local_fe2Output = fe2Output;
		local_fe2Output.initRow(&local_fe2OutRow);
		local_fe2Data.reset(new uint8_t[fe2Output.getMaxDataSize()]);
		local_fe2Output.setData(local_fe2Data.get());
		// local_fe2OutRow = fe2OutRow;
		local_fe2 = *fe2;
	}
	if (doJoin) {
		joinerOutput.resize(smallSideCount);
		smallSideRows.reset(new Row[smallSideCount]);
		smallNulls.reset(new Row[smallSideCount]);
		smallMappings.resize(smallSideCount);
		fergMappings.resize(smallSideCount + 1);
		smallNullMemory.reset(new shared_array<uint8_t>[smallSideCount]);
		local_primRG.initRow(&largeSideRow);
		local_outputRG.initRow(&joinedBaseRow);
		joinedBaseRowData.reset(new uint8_t[joinedBaseRow.getSize()]);
		joinedBaseRow.setData(joinedBaseRowData.get());
		largeMapping = makeMapping(local_primRG, local_outputRG);
		
		bool hasJoinFE = false;
		for (i = 0; i < smallSideCount; i++) {
			joinerMatchesRGs[i].initRow(&smallSideRows[i]);
			smallMappings[i] = makeMapping(joinerMatchesRGs[i], local_outputRG);
//			if (tjoiners[i]->semiJoin() || tjoiners[i]->antiJoin()) {
				if (tjoiners[i]->hasFEFilter()) {
					fergMappings[i] = makeMapping(joinerMatchesRGs[i], joinFERG);
					hasJoinFE = true;
				}
//			}
		}
		if (hasJoinFE) {
			joinFERG.initRow(&joinFERow);
			joinFERowData.reset(new uint8_t[joinFERow.getSize()]);
			memset(joinFERowData.get(), 0, joinFERow.getSize());
			joinFERow.setData(joinFERowData.get());
			fergMappings[smallSideCount] = makeMapping(local_primRG, joinFERG);
		}

		for (i = 0; i < smallSideCount; i++) {
			joinerMatchesRGs[i].initRow(&smallNulls[i]);
			smallNullMemory[i].reset(new uint8_t[smallNulls[i].getSize()]);
			smallNulls[i].setData(smallNullMemory[i].get());
			smallNulls[i].initToNull();
		}
		local_primRG.initRow(&largeNull);
		largeNullMemory.reset(new uint8_t[largeNull.getSize()]);
		largeNull.setData(largeNullMemory.get());
		largeNull.initToNull();

#if 0
		if (threadID == 0) {
			/* Some rowgroup debugging stuff. */
			uint8_t *tmp8;
			tmp8 = local_primRG.getData();
			local_primRG.setData(NULL);
			cout << "large-side RG: " << local_primRG.toString() << endl;
			local_primRG.setData(tmp8);
			for (i = 0; i < smallSideCount; i++) {
				tmp8 = joinerMatchesRGs[i].getData();
				joinerMatchesRGs[i].setData(NULL);
				cout << "small-side[" << i << "] RG: " << joinerMatchesRGs[i].toString() << endl;
			}	
			tmp8 = local_outputRG.getData();
			local_outputRG.setData(NULL);
			cout << "output RG: " << local_outputRG.toString() << endl;
			local_outputRG.setData(tmp8);

			cout << "large mapping:\n";
			for (i = 0; i < local_primRG.getColumnCount(); i++)
				cout << largeMapping[i] << " ";
			cout << endl;
			for (uint z = 0; z < smallSideCount; z++) {
				cout << "small mapping[" << z << "] :\n";
				for (i = 0; i < joinerMatchesRGs[z].getColumnCount(); i++)
					cout << smallMappings[z][i] << " ";
				cout << endl;
			}
		}
#endif
	}

	mutex.lock();

	while (1) {
		// sync with the send side 	
		while (!finishedSending && msgsSent == msgsRecvd) {
			recvWaiting++;
			condvar.wait(mutex);
			recvWaiting--;
		}

		if (msgsSent == msgsRecvd && finishedSending)
			break;

		fDec->read_some(uniqueID, fNumThreads, bsv);
		size = bsv.size();
		for (uint z = 0; z < size; z++) {
			if (bsv[z]->length() > 0 && fBPP->countThisMsg(*(bsv[z])))
				++msgsRecvd;
		}
		//@Bug 1424,1298

		if (sendWaiting && ((msgsSent - msgsRecvd) <=
		  (fMaxOutstandingRequests << LOGICAL_EXTENT_CONVERTER))) {
			condvarWakeupProducer.notify_one(); //pthread_cond_signal(&condvarWakeupProducer);
			THROTTLEDEBUG << "receiveMultiPrimitiveMessages wakes up sending side .. " << "  msgsSent: " << msgsSent << "  msgsRecvd = " << msgsRecvd << endl;
		}


		/* If there's an error and the joblist is being aborted, don't
			sit around forever waiting for responses.  */
		if (fInputJobStepAssociation.status() != 0 || die) {
			if (sendWaiting)
				condvarWakeupProducer.notify_one();
			break;

#if 0
			// thread 0 stays, the rest finish & exit to allow a quicker EOF
			if (threadID != 0 || fNumThreads == 1)
				break;
			else if (!didEOF) {
				didEOF = true;
				// need a time limit here too?  Better to leak something than to hang?
				while (recvExited < fNumThreads - 1)
					usleep(100000);
				dlp->endOfInput();
			}
			if (size != 0) // got something, reset the timer and go again
				errorTimeout = 0;

			usleep(5000);
			mutex.lock(); //pthread_mutex_lock(&mutex);
			if (++errorTimeout == 1000)     // 5 sec delay
				break;
			continue;
#endif
		}
		if (size == 0) {
			mutex.unlock();
			usleep(1000);
			mutex.lock();
			continue;
		}

		mutex.unlock();

// 		cout << "thread " << threadID << " has " << size << " Bytestreams\n";
		for (i = 0; i < size && !die; i++) {
			ByteStream* bs = bsv[i].get();

			// @bug 488. when PrimProc node is down. error out
			//An error condition.  We are not going to do anymore.
			ISMPacketHeader *hdr = (ISMPacketHeader*)(bs->buf());
			if (bs->length() == 0 || hdr->Status > 0)
			{
				/* PM errors mean this should abort right away instead of draining the PM backlog */
				mutex.lock();
				if (bs->length() == 0)
				{
					fOutputJobStepAssociation.status(primitiveServerErr);
				}
				else
				{
					string errMsg;
					
					bs->advance(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
					*bs >> errMsg;
					fOutputJobStepAssociation.status(hdr->Status);
					fOutputJobStepAssociation.errorMessage(errMsg);
				}

				abort_nolock();
				if (hdr && multicastErr == hdr->Status)
				{
					fDec->setMulticast(false);
				}
				goto out;
			}

			msgBytesIn_Thread += bs->lengthWithHdrOverhead();
			vecSize = 0;

			rgData = fBPP->getRowGroupData(*bs, &validCPData, &lbid, &min, &max,
				&cachedIO, &physIO, &touchedBlocks, &unused, threadID);

			local_primRG.setData(rgData.get());
// 			cout << "rowcount is " << local_primRG.getRowCount() << endl;
			ridsReturned_Thread += local_primRG.getRowCount();   // TODO need the pre-join count even on PM joins... later

			/* TupleHashJoinStep::joinOneRG() is a port of the main join loop here.  Any
			* changes made here should also be made there and vice versa. */
			if (hasUMJoin || !fBPP->pmSendsFinalResult()) {
				joinedData.reset(new uint8_t[local_outputRG.getMaxDataSize()]);
				local_outputRG.setData(joinedData.get());
				local_outputRG.resetRowGroup(local_primRG.getBaseRid());
// 				local_outputRG.getRow(0, &joinedRow);
				local_primRG.getRow(0, &largeSideRow);
				for (k = 0; k < local_primRG.getRowCount() && !die; k++, largeSideRow.nextRow()) {
					//cout << "TBPS: Large side row: " << largeSideRow.toString() << endl;
					matchCount = 0;
					for (j = 0; j < smallSideCount; j++) {
						tjoiners[j]->match(largeSideRow, k, threadID, &joinerOutput[j]);

						/* Debugging code to print the matches
							Row r;
							joinerMatchesRGs[j].initRow(&r);
							cout << "matches: \n";
							for (uint z = 0; z < joinerOutput[j].size(); z++) {
								r.setData(joinerOutput[j][z]);
								cout << "  " << r.toString() << endl;
							}
						*/
						matchCount = joinerOutput[j].size();
						if (tjoiners[j]->inUM()) {
							/* Count the # of rows that pass the join filter */
							if (tjoiners[j]->hasFEFilter() && matchCount > 0) {
								vector<uint8_t *> newJoinerOutput;
								applyMapping(fergMappings[smallSideCount], largeSideRow, &joinFERow);
								for (uint z = 0; z < joinerOutput[j].size(); z++) {
									smallSideRows[j].setData(joinerOutput[j][z]);
									applyMapping(fergMappings[j], smallSideRows[j], &joinFERow);
									if (!tjoiners[j]->evaluateFilter(joinFERow, threadID))
										matchCount--;
									else {
										/* The first match includes it in a SEMI join result and excludes it from an ANTI join
										 * result.  If it's SEMI & SCALAR however, it needs to continue.
										 */
										newJoinerOutput.push_back(joinerOutput[j][z]);
										if (tjoiners[j]->antiJoin() || (tjoiners[j]->semiJoin() && !tjoiners[j]->scalar()))
											break;
									}
								}
								// the filter eliminated all matches, need to join with the NULL row
								if (matchCount == 0 && tjoiners[j]->largeOuterJoin()) {
									newJoinerOutput.push_back(smallNullMemory[j].get());
									matchCount = 1;
								}

								joinerOutput[j].swap(newJoinerOutput);
							}
							// XXXPAT: This has gone through enough revisions it would benefit
							// from refactoring

							/* If anti-join, reverse the result */
							if (tjoiners[j]->antiJoin()) {
								matchCount = (matchCount ? 0 : 1);
						//		if (matchCount)
						//			cout << "in the result\n";
						//		else
						//			cout << "not in the result\n";
							}

							if (matchCount == 0) {
								joinerOutput[j].clear();
								break;
							}
							else if (!tjoiners[j]->scalar() &&
								  (tjoiners[j]->antiJoin() || tjoiners[j]->semiJoin())) {
									joinerOutput[j].clear();
									joinerOutput[j].push_back(smallNullMemory[j].get());
									matchCount = 1;
							}
						}

						if (matchCount == 0 && tjoiners[j]->innerJoin())
							break;
							
						/* Scalar check */
						if (tjoiners[j]->scalar() && matchCount > 1) {
							fOutputJobStepAssociation.status(ERR_MORE_THAN_1_ROW);
							abort();
						}
						if (tjoiners[j]->smallOuterJoin())
							tjoiners[j]->markMatches(threadID, joinerOutput[j]);

					}
					if (matchCount > 0) {
						applyMapping(largeMapping, largeSideRow, &joinedBaseRow);
						joinedBaseRow.setRid(largeSideRow.getRid());
						generateJoinResultSet(joinerOutput, joinedBaseRow, smallMappings,
						  0, local_outputRG, joinedData, &rgDatav, smallSideRows, postJoinRow);
						/* Bug 3510: Don't let the join results buffer get out of control.  Need
						   to refactor this.  All post-join processing needs to go here AND below 
						   for now. */
						if (rgDatav.size() * local_outputRG.getMaxDataSize() > 50000000) {
							RowGroup out(local_outputRG);
				            if (fe2 && !runFEonPM) {
				               	processFE2(out, local_fe2Output, postJoinRow, 
								  local_fe2OutRow, &rgDatav, &local_fe2);
                				rgDataVecToDl(rgDatav, local_fe2Output, dlp);
            				}
							else
								rgDataVecToDl(rgDatav, out, dlp);
						}
					}
				}  // end of the for-loop

				if (local_outputRG.getRowCount() > 0) {
					rgDatav.push_back(joinedData);
				}
			}
			else {
// 				cout << "TBPS: sending unjoined data\n";
				rgDatav.push_back(rgData);
			}

			/* Execute UM F & E group 2 on rgDatav */
			if (fe2 && !runFEonPM && rgDatav.size() > 0 && !die) {
				processFE2(local_outputRG, local_fe2Output, postJoinRow, local_fe2OutRow, &rgDatav, &local_fe2);
				rgDataVecToDl(rgDatav, local_fe2Output, dlp);
			}

			cachedIO_Thread += cachedIO;
			physIO_Thread += physIO;
			touchedBlocks_Thread += touchedBlocks;
			if (fOid >= 3000 && ffirstStepType == SCAN && bop == BOP_AND)
				cpv.push_back(_CPInfo(min, max, lbid, validCPData));
		}

		// insert the rowgroup data into dlp
		if (rgDatav.size() > 0) {
			if (fe2 && runFEonPM)
				rgDataVecToDl(rgDatav, local_fe2Output, dlp);
			else
				rgDataVecToDl(rgDatav, local_outputRG, dlp);
		}

		//update casual partition
		size = cpv.size();
		if (size > 0 && fInputJobStepAssociation.status() == 0) {
			cpMutex.lock();
			for (i = 0; i < size; i++) {
				lbidList->UpdateMinMax(cpv[i].min, cpv[i].max, cpv[i].LBID, fColType.colDataType,
						cpv[i].valid);
			}
			cpMutex.unlock();
		}
		cpv.clear();
		mutex.lock();
	} // done reading	

}//try
catch(const std::exception& ex)
{
	processError(ex.what(), tupleBPSErr, "TupleBPS::receiveMultiPrimitiveMessages()");
}
catch(...)
{
	processError("unknown", tupleBPSErr, "TupleBPS::receiveMultiPrimitiveMessages()");
}

out:
	if (++recvExited == fNumThreads) {
		
		if (doJoin && smallOuterJoiner != -1 &&
		  fInputJobStepAssociation.status() == 0 && !die) {
			/* If this was a left outer join, this needs to put the unmatched
			   rows from the joiner into the output 
			   XXXPAT: This might be a problem if later steps depend
			   on sensible rids and/or sensible ordering */
			vector<uint8_t *> unmatched;
#ifdef JLF_DEBUG
			cout << "finishing small-outer join output\n";
#endif
			i = smallOuterJoiner;
			tjoiners[i]->getUnmarkedRows(&unmatched);
			joinedData.reset(new uint8_t[local_outputRG.getMaxDataSize()]);
			local_outputRG.setData(joinedData.get());
			local_outputRG.resetRowGroup(-1);
			local_outputRG.getRow(0, &joinedBaseRow);
			for (j = 0; j < unmatched.size(); j++) {
				smallSideRows[i].setData(unmatched[j]);
				for (k = 0; k < smallSideCount; k++) {
					if (i == k)
						applyMapping(smallMappings[i], smallSideRows[i], &joinedBaseRow);
					else
						applyMapping(smallMappings[k], smallNulls[k], &joinedBaseRow);
				}
				applyMapping(largeMapping, largeNull, &joinedBaseRow);
				joinedBaseRow.setRid(0);
//				cout << "outer row is " << joinedBaseRow.toString() << endl;
//				joinedBaseRow.setRid(largeSideRow.getRid());
				joinedBaseRow.nextRow();
				local_outputRG.incRowCount();
				if (local_outputRG.getRowCount() == 8192) {
					if (fe2) {
						rgDatav.push_back(joinedData);
						processFE2(local_outputRG, local_fe2Output, postJoinRow, local_fe2OutRow, &rgDatav, &local_fe2);
						if (rgDatav.size() > 0)
							rgDataToDl(rgDatav[0], local_fe2Output, dlp);
						rgDatav.clear();
					}
					else
						rgDataToDl(joinedData, local_outputRG, dlp);

					joinedData.reset(new uint8_t[local_outputRG.getMaxDataSize()]);
					local_outputRG.setData(joinedData.get());
					local_outputRG.resetRowGroup(-1);
					local_outputRG.getRow(0, &joinedBaseRow);
				}
			}
			if (local_outputRG.getRowCount() > 0) {
				if (fe2) {
					rgDatav.push_back(joinedData);
					assert(joinedData);
					processFE2(local_outputRG, local_fe2Output, postJoinRow, local_fe2OutRow, &rgDatav, &local_fe2);
					if (rgDatav.size() > 0)
						rgDataToDl(rgDatav[0], local_fe2Output, dlp);
					rgDatav.clear();
				}
				else
					rgDataToDl(joinedData, local_outputRG, dlp);
			}
		}

		//...Casual partitioning could cause us to do no processing.  In that
		//...case these time stamps did not get set.  So we set them here.
		if (fOid>=3000 && traceOn() && dlTimes.FirstReadTime().tv_sec==0) {
			dlTimes.setFirstReadTime();
			dlTimes.setLastReadTime();
			dlTimes.setFirstInsertTime();
		}
		if (fOid>=3000 && traceOn()) dlTimes.setEndOfInputTime();
			
		ByteStream bs;
		try {
			fDec->removeDECEventListener(this);
			fBPP->destroyBPP(bs);
			fDec->write(bs);
			BPPIsAllocated = false;
			fDec->removeQueue(uniqueID);
			tjoiners.clear();
		}
		// catch and do nothing. Let it continues with the clean up and profiling
		catch (const std::exception& e)
		{
			cerr << "tuple-bps caught: " << e.what() << endl;
		}
		catch (...)
		{
			cerr << "tuple-bps caught unknown exception" << endl;
		}

		lastThread = true;
	}
	//@Bug 1099
	ridsReturned += ridsReturned_Thread;
	fPhysicalIO += physIO_Thread;
	fCacheIO += cachedIO_Thread;
	fBlockTouched += touchedBlocks_Thread;
	fMsgBytesIn += msgBytesIn_Thread;
	mutex.unlock();
	
	if (fTableOid >= 3000 && lastThread)
	{
		struct timeval tvbuf;
		gettimeofday(&tvbuf, 0);
		FIFO<boost::shared_array<uint8_t> > *pFifo = 0;
		uint64_t totalBlockedReadCount  = 0;
		uint64_t totalBlockedWriteCount = 0;
		
		//...Sum up the blocked FIFO reads for all input associations
		size_t inDlCnt  = fInputJobStepAssociation.outSize();
		for (size_t iDataList=0; iDataList<inDlCnt; iDataList++)
		{
			pFifo = dynamic_cast<FIFO<boost::shared_array<uint8_t> > *>(
				fInputJobStepAssociation.outAt(iDataList)->rowGroupDL());
			if (pFifo)
			{
				totalBlockedReadCount += pFifo->blockedReadCount();
			}
		}

		//...Sum up the blocked FIFO writes for all output associations
		size_t outDlCnt = fOutputJobStepAssociation.outSize();
		for (size_t iDataList=0; iDataList<outDlCnt; iDataList++)
		{
			pFifo = dynamic_cast<FIFO<boost::shared_array<uint8_t> > *>(dlp);

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

		if (traceOn())
		{
			// @bug 828
			ostringstream logStr;
			logStr << "ses:" << fSessionId << " st: " << fStepId<<" finished at "<<
			JSTimeStamp::format(tvbuf) << "; PhyI/O-" << fPhysicalIO << "; CacheI/O-"  <<
			fCacheIO << "; MsgsSent-" << fMsgBppsToPm << "; MsgsRvcd-" << msgsRecvd <<
			"; BlocksTouched-"<< fBlockTouched <<
			"; BlockedFifoIn/Out-" << totalBlockedReadCount <<
			"/" << totalBlockedWriteCount <<
			"; output size-" << ridsReturned << endl <<
			"\tPartitionBlocksEliminated-" << fNumBlksSkipped <<
			"; MsgBytesIn-"  << msgBytesInKB  << "KB" <<
			"; MsgBytesOut-" << msgBytesOutKB << "KB" << endl  <<
			"\t1st read " << dlTimes.FirstReadTimeString() << 
			"; EOI " << dlTimes.EndOfInputTimeString() << "; runtime-" <<
			JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(),dlTimes.FirstReadTime())
			<< "s\n\tJob completion status " << fOutputJobStepAssociation.status() << endl;
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
			fExtendedInfo += toString() + logStr.str();
			formatMiniStats();
		}
		
		if ( ffirstStepType == SCAN && bop == BOP_AND)
		{
			cpMutex.lock(); //pthread_mutex_lock(&cpMutex);
 			lbidList->UpdateAllPartitionInfo();
			cpMutex.unlock(); //pthread_mutex_unlock(&cpMutex);
		}
	}

	// Bug 3136, let mini stats to be formatted if traceOn.
	if (lastThread && !didEOF)
		dlp->endOfInput();
}

void TupleBPS::processError(const string& ex, uint16_t err, const string& src)
{
	ostringstream oss;
	oss << "st: " << fStepId << " " << src << " caught an exception: " << ex << endl;
	catchHandler(oss.str(), fSessionId);
	fOutputJobStepAssociation.status(err);
	abort_nolock();
	cerr << oss.str();
}

const string TupleBPS::toString() const
{
	ostringstream oss;
	oss << "TupleBPS        ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId << " st:" << fStepId <<
		" tb/col:" << fTableOid << "/" << fOid;
	if (alias().length()) oss << " alias:" << alias();
	if (view().length()) oss << " view:" << view();

#if 0
	// @bug 1282, don't have output datalist for delivery
	if (!isDelivery)
		oss << " " << omitOidInDL << fOutputJobStepAssociation.outAt(0) << showOidInDL;
#else
if (isDelivery)
	oss << " is del ";
else
	oss << " not del ";
if (bop == BOP_OR)
	oss << " BOP_OR ";
if (die)
	oss << " aborting " << msgsSent << "/" << msgsRecvd << " " << uniqueID << " ";
if (fOutputJobStepAssociation.outSize() > 0)
{
	oss << fOutputJobStepAssociation.outAt(0);
	if (fOutputJobStepAssociation.outSize() > 1)
		oss << " (too many outputs?)";
}
else
{
		oss << " (no outputs?)";
}
#endif
	oss << " nf:" << fFilterCount;
	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
	{
		oss << fInputJobStepAssociation.outAt(i);
	}
	oss << endl << "  " << fBPP->toString() << endl;
	return oss.str();
}

/* This exists to avoid a DBRM lookup for every rid. */
inline bool TupleBPS::scanit(uint64_t rid)
{
	uint64_t fbo;
	uint extentIndex;

	if (fOid < 3000)
		return true;
	fbo = rid >> rpbShift;
	extentIndex = fbo >> divShift;
	//if (scanFlags[extentIndex] && !runtimeCPFlags[extentIndex])
	//	cout << "HJ feedback eliminated an extent!\n";
	return scanFlags[extentIndex] && runtimeCPFlags[extentIndex];
}

uint64_t TupleBPS::getFBO(uint64_t lbid)
{
	uint i;
	uint64_t lastLBID;

	for (i = 0; i < numExtents; i++) {
		lastLBID = scannedExtents[i].range.start + (scannedExtents[i].range.size << 10) - 1;

		if (lbid >= (uint64_t) scannedExtents[i].range.start && lbid <= lastLBID)
			return (lbid - scannedExtents[i].range.start) + (i << divShift);
	}
	throw logic_error("TupleBPS: didn't find the FBO?");
}

void TupleBPS::useJoiner(shared_ptr<joiner::TupleJoiner> tj)
{
	vector<shared_ptr<joiner::TupleJoiner> > v;
	v.push_back(tj);
	useJoiners(v);
}

void TupleBPS::useJoiners(const vector<boost::shared_ptr<joiner::TupleJoiner> > &joiners)
{
	uint i;

	tjoiners = joiners;
	doJoin = (joiners.size() != 0);

	joinerMatchesRGs.clear();
	smallSideCount = tjoiners.size();
	hasPMJoin = false;
	hasUMJoin = false;
	for (i = 0; i < smallSideCount; i++) {
		joinerMatchesRGs.push_back(tjoiners[i]->getSmallRG());
		if (tjoiners[i]->inPM())
			hasPMJoin = true;
		else
			hasUMJoin = true;

		if (tjoiners[i]->getJoinType() & SMALLOUTER)
			smallOuterJoiner = i;
	}
	if (hasPMJoin)
		fBPP->useJoiners(tjoiners);
}

void TupleBPS::useJoiner(boost::shared_ptr<joiner::Joiner> j)
{
}

void TupleBPS::newPMOnline(uint connectionNumber)
{
	ByteStream bs;

	fBPP->createBPP(bs);
	try {
		fDec->write(bs, connectionNumber);
		if (hasPMJoin)
			serializeJoiner(connectionNumber);
	}
	catch (IDBExcept &e) {
		abort();
		catchHandler(e.what());
		if (fOutputJobStepAssociation.status() == 0)
			fOutputJobStepAssociation.status(e.errorCode());
	}
}

void TupleBPS::setInputRowGroup(const rowgroup::RowGroup &rg)
{
	inputRowGroup = rg;
	fBPP->setInputRowGroup(rg);
}

void TupleBPS::setOutputRowGroup(const rowgroup::RowGroup &rg)
{
	outputRowGroup = rg;
	primRowGroup = rg;
	fBPP->setProjectionRowGroup(rg);
	checkDupOutputColumns(rg);
	if (fe2)
		fe2Mapping = makeMapping(outputRowGroup, fe2Output);
}

void TupleBPS::setJoinedResultRG(const rowgroup::RowGroup &rg)
{
	outputRowGroup = rg;
	checkDupOutputColumns(rg);
	fBPP->setJoinedRowGroup(rg);
	if (fe2)
		fe2Mapping = makeMapping(outputRowGroup, fe2Output);
}

/* probably worthwhile to make some of these class vars */
void TupleBPS::generateJoinResultSet(const vector<vector<uint8_t *> > &joinerOutput,
  Row &baseRow, const vector<shared_array<int> > &mappings, const uint depth,
  RowGroup &outputRG, shared_array<uint8_t> &rgData,
  vector<shared_array<uint8_t> > *outputData, const scoped_array<Row> &smallRows,
  Row &joinedRow)
{
	uint i;
	Row &smallRow = smallRows[depth];

	if (depth < smallSideCount - 1) {
		for (i = 0; i < joinerOutput[depth].size(); i++) {
			smallRow.setData(joinerOutput[depth][i]);
			applyMapping(mappings[depth], smallRow, &baseRow);
// 			cout << "depth " << depth << ", size " << joinerOutput[depth].size() << ", row " << i << ": " << smallRow.toString() << endl;
			generateJoinResultSet(joinerOutput, baseRow, mappings, depth + 1,
			  outputRG, rgData, outputData, smallRows, joinedRow);
		}
	}
	else {
		outputRG.getRow(outputRG.getRowCount(), &joinedRow);
		for (i = 0; i < joinerOutput[depth].size(); i++, joinedRow.nextRow(),
		  outputRG.incRowCount()) {
			smallRow.setData(joinerOutput[depth][i]);
			if (UNLIKELY(outputRG.getRowCount() == 8192)) {
// 				cout << "GJRS adding data\n";
				outputData->push_back(rgData);
				rgData.reset(new uint8_t[outputRG.getMaxDataSize()]);
				outputRG.setData(rgData.get());
				outputRG.resetRowGroup(baseRow.getRid());
				outputRG.getRow(0, &joinedRow);
			}
// 			cout << "depth " << depth << ", size " << joinerOutput[depth].size() << ", row " << i << ": " << smallRow.toString() << endl;
			applyMapping(mappings[depth], smallRow, &baseRow);
			memcpy(joinedRow.getData(), baseRow.getData(), joinedRow.getSize());
//			cout << "(step " << fStepId << ") fully joined row is: " << joinedRow.toString() << endl;
		}
	}
}

const rowgroup::RowGroup & TupleBPS::getOutputRowGroup() const
{
	return outputRowGroup;
}

void TupleBPS::setAggregateStep(const rowgroup::SP_ROWAGG_PM_t& agg, const rowgroup::RowGroup &rg)
{
	if (rg.getColumnCount() > 0)
	{
		fAggRowGroupPm = rg;
		fAggregatorPm = agg;

		fBPP->addAggregateStep(agg, rg);
		fBPP->setNeedRidsAtDelivery(false);
	}
}

void TupleBPS::setBOP(uint8_t op)
{
	bop = op;
	fBPP->setBOP(bop);
}

void TupleBPS::setJobInfo(const JobInfo* jobInfo)
{
	fBPP->jobInfo(jobInfo);
}

uint64_t TupleBPS::getEstimatedRowCount()
{
	// Call function that populates the scanFlags array based on the extents that qualify based on casual partitioning.	
	storeCasualPartitionInfo(true);
	// TODO:  Strip out the cout below after a few days of testing.
#ifdef JLF_DEBUG
	cout << "OID-" << fOid << " EstimatedRowCount-" << fEstimatedRows << endl;
#endif
	return fEstimatedRows;
}

void TupleBPS::checkDupOutputColumns(const rowgroup::RowGroup &rg)
{
	// bug 1965, find if any duplicate columns selected
	map<uint, uint> keymap; // map<unique col key, col index in the row group>
	dupColumns.clear();
	const vector<uint>& keys = rg.getKeys();
	for (uint i = 0; i < keys.size(); i++)
	{
		map<uint, uint>::iterator j = keymap.find(keys[i]);
		if (j == keymap.end())
			keymap.insert(make_pair(keys[i], i));          // map key to col index
		else
			dupColumns.push_back(make_pair(i, j->second)); // dest/src index pair
	}
}

void TupleBPS::dupOutputColumns(boost::shared_array<uint8_t>& data, RowGroup& rg)
{
	rg.setData(data.get());
	dupOutputColumns(rg);
}

void TupleBPS::dupOutputColumns(RowGroup& rg)
{
	Row workingRow;
	rg.initRow(&workingRow);
	rg.getRow(0, &workingRow);

	for (uint64_t i = 0; i < rg.getRowCount(); i++)
	{
		for (uint64_t j = 0; j < dupColumns.size(); j++)
			workingRow.copyField(dupColumns[j].first, dupColumns[j].second);
		workingRow.nextRow();
	}
}

void TupleBPS::stepId(uint16_t stepId)
{
	fStepId = stepId;
	fBPP->setStepID(stepId);
}

void TupleBPS::addFcnExpGroup1(const boost::shared_ptr<execplan::ParseTree>& fe)
{
	if (!fe1)
		fe1.reset(new funcexp::FuncExpWrapper());
	fe1->addFilter(fe);
}

void TupleBPS::setFE1Input(const RowGroup &feInput)
{
	fe1Input = feInput;
}

void TupleBPS::setFcnExpGroup2(const boost::shared_ptr<funcexp::FuncExpWrapper>& fe,
	const rowgroup::RowGroup &rg, bool runFE2onPM)
{
	fe2 = fe;
	fe2Output = rg;
	checkDupOutputColumns(rg);
	fe2Mapping = makeMapping(outputRowGroup, fe2Output);
	runFEonPM = runFE2onPM;
	if (runFEonPM)
		fBPP->setFEGroup2(fe2, fe2Output);
}

void TupleBPS::setFcnExpGroup3(const vector<shared_ptr<execplan::ReturnedColumn> >& fe)
{
	if (!fe2)
		fe2.reset(new funcexp::FuncExpWrapper());

	for (uint i = 0; i < fe.size(); i++)
		fe2->addReturnedColumn(fe[i]);

	// if this is called, there's no join, so it can always run on the PM
	runFEonPM = true;
	fBPP->setFEGroup2(fe2, fe2Output);
}

void TupleBPS::setFE23Output(const rowgroup::RowGroup &feOutput)
{
	fe2Output = feOutput;
	checkDupOutputColumns(feOutput);
	fe2Mapping = makeMapping(outputRowGroup, fe2Output);
	if (fe2 && runFEonPM)
		fBPP->setFEGroup2(fe2, fe2Output);
}

void TupleBPS::processFE2_oneRG(RowGroup &input, RowGroup &output, Row &inRow,
  Row &outRow, funcexp::FuncExpWrapper* local_fe)
{
	bool ret;
	uint i;

	output.resetRowGroup(input.getBaseRid());
	output.getRow(0, &outRow);
	input.getRow(0, &inRow);
	for (i = 0; i < input.getRowCount(); i++, inRow.nextRow()) {
		ret = local_fe->evaluate(&inRow);
		if (ret) {
			applyMapping(fe2Mapping, inRow, &outRow);
			//cout << "fe2 passed row: " << outRow.toString() << endl;
			outRow.setRid(inRow.getRid());
			output.incRowCount();
			outRow.nextRow();
		}
	}
}

void TupleBPS::processFE2(RowGroup &input, RowGroup &output, Row &inRow, Row &outRow,
  vector<shared_array<uint8_t> > *rgData, funcexp::FuncExpWrapper* local_fe)
{
	vector<shared_array<uint8_t> > results;
	shared_array<uint8_t> result;
	uint i, j;
	bool ret;

	result.reset(new uint8_t[output.getMaxDataSize()]);
	output.setData(result.get());
	output.resetRowGroup(-1);
	output.getRow(0, &outRow);

	for (i = 0; i < rgData->size(); i++) {
		input.setData((*rgData)[i].get());
		if (output.getRowCount() == 0)
			output.resetRowGroup(input.getBaseRid());
		input.getRow(0, &inRow);
		for (j = 0; j < input.getRowCount(); j++, inRow.nextRow()) {
			ret = local_fe->evaluate(&inRow);
			if (ret) {
				applyMapping(fe2Mapping, inRow, &outRow);
				outRow.setRid(inRow.getRid());
				output.incRowCount();
				outRow.nextRow();
				if (output.getRowCount() == 8192 ||
				  output.getBaseRid() != input.getBaseRid()
				) {
//					cout << "FE2 produced a full RG\n";
					results.push_back(result);
					result.reset(new uint8_t[output.getMaxDataSize()]);
					output.setData(result.get());
					output.resetRowGroup(input.getBaseRid());
					output.getRow(0, &outRow);
				}
			}
		}
	}
	if (output.getRowCount() > 0) {
//		cout << "FE2 produced " << output.getRowCount() << " rows\n";
		results.push_back(result);
	}
//	else
//		cout << "no rows from FE2\n";
	rgData->swap(results);
}

const rowgroup::RowGroup& TupleBPS::getDeliveredRowGroup() const
{
	if (fe2)
		return fe2Output;

	return outputRowGroup;
}

void TupleBPS::formatMiniStats()
{

	ostringstream oss;
	oss << "BPS "
		<< "PM "
		<< alias() << " "
		<< fTableOid << " "
		<< fBPP->toMiniString() << " "
		<< fPhysicalIO << " "
		<< fCacheIO << " "
		<< fNumBlksSkipped << " "
		<< JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << " ";

	if (ridsReturned == 0)
		oss << rowsReturned << " ";
	else
		oss << ridsReturned << " ";

	fMiniInfo += oss.str();
}

void TupleBPS::rgDataToDl(shared_array<uint8_t>& rgData, RowGroup& rg, RowGroupDL* dlp)
{
	// bug 1965, populate duplicate columns if any.
	if (dupColumns.size() > 0)
		dupOutputColumns(rgData, rg);

	dlp->insert(rgData);
}


void TupleBPS::rgDataVecToDl(vector<shared_array<uint8_t> >& rgDatav, RowGroup& rg, RowGroupDL* dlp)
{
	uint64_t size = rgDatav.size();
	if (size > 0 && fInputJobStepAssociation.status() == 0) {
		dlMutex.lock(); //pthread_mutex_lock(&dlMutex);
		for (uint64_t i = 0; i < size; i++) {
			rgDataToDl(rgDatav[i], rg, dlp);
		}
		dlMutex.unlock(); //pthread_mutex_unlock(&dlMutex);
	}
	rgDatav.clear();
}

void TupleBPS::setJoinFERG(const RowGroup &rg)
{
	joinFERG = rg;
	fBPP->setJoinFERG(rg);
}

void TupleBPS::addCPPredicates(uint32_t OID, const vector<int64_t> &vals, bool isRange)
{
	
	if (fTraceFlags & CalpontSelectExecutionPlan::IGNORE_CP || fOid < 3000)
		return;
	
	uint i, j, k;
	int64_t min, max, seq;
	bool isValid, intersection;
	vector<SCommand> colCmdVec = fBPP->getFilterSteps();
	ColumnCommandJL *cmd;
	
	for (i = 0; i < fBPP->getProjectSteps().size(); i++)
		colCmdVec.push_back(fBPP->getProjectSteps()[i]);
	
	LBIDList ll(OID, 0);
		
	/* Find the columncommand with that OID.
	 * Check that the column type is one handled by CP.
	 * For each extent in that OID,
	 *    grab the min & max,
	 *    OR together all of the intersection tests,
	 *    AND it with the current CP flag.
	 */

	for (i = 0; i < colCmdVec.size(); i++) {
		cmd = dynamic_cast<ColumnCommandJL *>(colCmdVec[i].get());
		if (cmd != NULL && cmd->getOID() == OID) {
			if (!ll.CasualPartitionDataType(cmd->getColType().colDataType, cmd->getColType().colWidth)
			  || cmd->fcnOrd() || cmd->isDict())
				return;

			// @bug 2989, use correct extents
			tr1::unordered_map<int64_t, struct BRM::EMEntry> *extentsPtr = NULL;
			vector<struct BRM::EMEntry> extents;  // in case the extents of OID is not in Map

            // TODO: store the sorted vectors from the pcolscans/steps as a minor optimization
            dbrm.getExtents(OID, extents);
            if (extents.empty()) {
                ostringstream os;
                os << "TupleBPS::addCPPredicates(): OID " << OID << " is empty.";
                throw runtime_error(os.str());
            }
			sort(extents.begin(), extents.end(), ExtentSorter());

			if (extentsMap.find(OID) != extentsMap.end()) {
				extentsPtr = &extentsMap[OID];
			}
			else if (dbrm.getExtents(OID, extents) == 0) {
				extentsMap[OID] = tr1::unordered_map<int64_t, struct BRM::EMEntry>();
				tr1::unordered_map<int64_t, struct BRM::EMEntry> &mref = extentsMap[OID];
				for (uint z = 0; z < extents.size(); z++)
					mref[extents[z].range.start] = extents[z];
				extentsPtr = &mref;
			}

			for (j = 0; j < extents.size(); j++) {
				isValid = ll.GetMinMax(&min, &max, &seq, extents[j].range.start, *extentsPtr);
				if (isValid) {
					if (isRange)
						runtimeCPFlags[j] = ll.checkRangeOverlap(min, max, vals[0], vals[1],
						  cmd->isCharType()) && runtimeCPFlags[j];
					else {
						intersection = false;
						for (k = 0; k < vals.size(); k++)
							intersection = intersection ||
							  ll.checkSingleValue(min, max, vals[k], cmd->isCharType());
						runtimeCPFlags[j] = intersection && runtimeCPFlags[j];
					}
				}
			}
			break;
		}
	}
}

void TupleBPS::dec(DistributedEngineComm* dec)
{
	if (fDec)
		fDec->removeQueue(uniqueID);
	fDec = dec;
	if (fDec)
		fDec->addQueue(uniqueID, true); 
}

void TupleBPS::abort_nolock()
{
	if (die)
		return;
	JobStep::abort();
	if (fDec) {
		ByteStream bs;
		fBPP->abortProcessing(&bs);
		try {
			fDec->write(bs);
		}
		catch(...) {
			// this throws only if there are no PMs left.  If there are none,
			// that is the cause of the abort and that will be reported to the
			// front-end already.  Nothing to do here.
		}
		fDec->shutdownQueue(uniqueID);
	}
	condvarWakeupProducer.notify_all();
	condvar.notify_all();
}

void TupleBPS::abort()
{
	boost::mutex::scoped_lock scoped(mutex, boost::defer_lock);
	if (die)
		return;
	JobStep::abort();
	if (fDec) {
		ByteStream bs;
		fBPP->abortProcessing(&bs);
		try {
			fDec->write(bs);
		}
		catch(...) {
			// this throws only if there are no PMs left.  If there are none,
			// that is the cause of the abort and that will be reported to the
			// front-end already.  Nothing to do here.
		}
		fDec->shutdownQueue(uniqueID);
	}
	scoped.lock();
	condvarWakeupProducer.notify_all();
	condvar.notify_all();
	scoped.unlock();
}

}   //namespace
// vim:ts=4 sw=4:
