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
*   $Id: pcolstep.cpp 8272 2012-01-19 16:28:34Z xlou $
*
*
***********************************************************************/
#include <sstream>
#include <iomanip>
#include <algorithm>
//#define NDEBUG
#include <cassert>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
using namespace std;

#include "jobstep.h"
#include "distributedenginecomm.h"
#include "elementtype.h"
#include "unique32generator.h"

#include "messagequeue.h"
using namespace messageqcpp;
#include "configcpp.h"
using namespace config;

#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
using namespace logging;

#include "calpontsystemcatalog.h"
#include "logicoperator.h"
using namespace execplan;

#include "brm.h"
using namespace BRM;

#include "idbcompress.h"

// #define DEBUG 1

//#define PROFILE

#ifdef PROFILE
#include<profiling.h>

// unname namespace for pcolstep profiling
namespace
{
enum
{
	sendPrimMsg_c = 0,
	sendWork_c,
	next1_c,
	next2_c,
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
	"sendPrimitiveMessages",
	"send work",
	"1st ridList->next in send",
	"ridList->next in send",
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

namespace joblist
{
  //const uint32_t defaultProjectBlockReqLimit     = 32768;
  //const uint32_t defaultProjectBlockReqThreshold =  16384;
struct pColStepPrimitive
{
    pColStepPrimitive(pColStep* pColStep) : fPColStep(pColStep)
    {}
	pColStep *fPColStep;
    void operator()()
    {
        try
        {
            fPColStep->sendPrimitiveMessages();
        }
        catch(exception& re)
        {
			cerr << "pColStep: send thread threw an exception: " << re.what() << 
				"\t" << this << endl;
		}
    }
};

struct pColStepAggregator
{
    pColStepAggregator(pColStep* pColStep) : fPColStepCol(pColStep)
    {}
    pColStep *fPColStepCol;
    void operator()()
    {
        try
        {
           fPColStepCol->receivePrimitiveMessages();
        }
        catch(exception& re)
        {
			cerr << fPColStepCol->toString() << ": recv thread threw an exception: " << re.what() << endl;
		}
    }
};


pColStep::pColStep(const JobStepAssociation& inputJobStepAssociation,
	const JobStepAssociation& outputJobStepAssociation,
	DistributedEngineComm* dec,
	CalpontSystemCatalog* cat,
	CalpontSystemCatalog::OID o,
	CalpontSystemCatalog::OID t,
	uint32_t session,
	uint32_t txn,
	uint32_t verID,
	uint16_t step,
	uint32_t statementId,
	ResourceManager& rm,
	uint32_t fInterval,
	bool isEMgr) :
	fRm(rm),
	fInputJobStepAssociation(inputJobStepAssociation),
	fOutputJobStepAssociation(outputJobStepAssociation),
	fDec(dec),
	sysCat(cat),
	fOid(o),
	fTableOid(t),
	fSessionId(session),
	fTxnId(txn),
	fVerId(verID),
	fStepId(step),
	fStatementId(statementId),
	fFilterCount(0),
	fBOP(BOP_NONE),
	ridList(0),
	msgsSent(0),
	msgsRecvd(0),
	finishedSending(false),
	recvWaiting(false),
	fIsDict(false),
	isEM(isEMgr),
	ridCount(0),
	fFlushInterval(fInterval),
	fSwallowRows(false),
	fProjectBlockReqLimit(rm.getJlProjectBlockReqLimit()),
	fProjectBlockReqThreshold(rm.getJlProjectBlockReqThreshold()),
	fStopSending(false),
	isFilterFeeder(false),
	fPhysicalIO(0),
	fCacheIO(0),
	fNumBlksSkipped(0),
	fMsgBytesIn(0),
	fMsgBytesOut(0)
{
	int err, i;
	uint mask;
	
	if (fInterval == 0 || !isEM)
		fOutputType = OT_BOTH;
	else
		fOutputType = OT_TOKEN;
	if ( fOid < 1000 )
		throw runtime_error("pColStep: invalid column");
	fColType = sysCat->colType(fOid);

	compress::IDBCompressInterface cmpif;
	if (!cmpif.isCompressionAvail(fColType.compressionType))
	{
		ostringstream oss;
		oss << "Unsupported compression type " << fColType.compressionType;
		oss << " for " << sysCat->colName(fOid);
#ifdef SKIP_IDB_COMPRESSION
		oss << ". It looks you're running Community binaries on an Enterprise database.";
#endif
		throw runtime_error(oss.str());
	}

	realWidth = fColType.colWidth;

	if ( fColType.colDataType == CalpontSystemCatalog::VARCHAR )
	{
		if (8 > fColType.colWidth && 4 <= fColType.colWidth ) fColType.colDataType = CalpontSystemCatalog::CHAR;
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
	assert(fColType.colWidth > 0);
	ridsPerBlock = BLOCK_SIZE/fColType.colWidth;

	/* calculate some shortcuts for extent and block based arithmetic */
	extentSize = (fRm.getExtentRows()*fColType.colWidth)/BLOCK_SIZE;
	for (i = 1, mask = 1, modMask = 0; i <= 32; i++) {
		mask <<= 1;
		modMask = (modMask << 1) | 1;
		if (extentSize & mask) {
			divShift = i;
			break;
		}
	}
	for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
		if (extentSize & mask)
			throw runtime_error("pColStep: Extent size must be a power of 2 in blocks");

	/* calculate shortcuts for rid-based arithmetic */
	for (i = 1, mask = 1, rpbMask = 0; i <= 32; i++) {
		mask <<= 1;
		rpbMask = (rpbMask << 1) | 1;
		if (ridsPerBlock & mask) {
			rpbShift = i;
			break;
		}
	}
	for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
		if (ridsPerBlock & mask)
			throw runtime_error("pColStep: Block size and column width must be a power of 2");

	for (i = 0, mask = 1, blockSizeShift = 0; i < 32; i++) {
		if (mask == BLOCK_SIZE) {
			blockSizeShift = i;
			break;
		}
		mask <<= 1;
	}

	if (i == 32)
		throw runtime_error("pColStep: Block size must be a power of 2");

 	err = dbrm.getExtents(o, extents);
	if (err)
		throw runtime_error("pColStep: BRM error!");
	//TODO: what is magic number 5 for?

	if (fOid>3000) {
		lbidList.reset(new LBIDList(fOid, 0));
	}
	sort(extents.begin(), extents.end(), ExtentSorter());
	numExtents = extents.size();
	uniqueID = Unique32Generator::instance()->getUnique32();
	if (fDec)
		fDec->addQueue(uniqueID);
// 	initializeConfigParms ( );
}

pColStep::pColStep(const pColScanStep& rhs) :
	fRm(rhs.resourceManager()),
	fInputJobStepAssociation(rhs.inputAssociation()),
	fOutputJobStepAssociation(rhs.outputAssociation()),
	fDec(rhs.dec()),
	fOid(rhs.oid()),
	fTableOid(rhs.tableOid()),
	fSessionId(rhs.sessionId()),
	fTxnId(rhs.txnId()),
	fVerId(rhs.verId()),
	fStepId(rhs.stepId()),
	fStatementId(rhs.statementId()),
	fFilterCount(rhs.filterCount()),
	fBOP(rhs.BOP()),
	ridList(0),
	fFilterString(rhs.filterString()),
	fColType(rhs.colType()),
	msgsSent(0),
	msgsRecvd(0),
	finishedSending(false),
	recvWaiting(false),
	fIsDict(rhs.isDictCol()),
	ridCount(0),
	// Per Cindy, it's save to put fFlushInterval to be 0
	fFlushInterval(0),
	fSwallowRows(false),
	fProjectBlockReqLimit(fRm.getJlProjectBlockReqLimit()),
	fProjectBlockReqThreshold(fRm.getJlProjectBlockReqThreshold()),
	fStopSending(false),
	fPhysicalIO(0),
	fCacheIO(0),
	fNumBlksSkipped(0),
	fMsgBytesIn(0),
	fMsgBytesOut(0),
	fLogger(rhs.logger()),
	fUdfName(rhs.udfName())
{
    fCardinality = rhs.cardinality();
    fAlias = rhs.alias();
    fView = rhs.view();
    fName = rhs.name();
	fTupleId = rhs.tupleId();
	int err, i;
	uint mask;
	if ( fOid < 1000 )
		throw runtime_error("pColStep: invalid column");
	ridsPerBlock = rhs.getRidsPerBlock();
	/* calculate some shortcuts for extent and block based arithmetic */
	extentSize = (fRm.getExtentRows()*fColType.colWidth)/BLOCK_SIZE;
	for (i = 1, mask = 1, modMask = 0; i <= 32; i++) {
		mask <<= 1;
		modMask = (modMask << 1) | 1;
		if (extentSize & mask) {
			divShift = i;
			break;
		}
	}
	for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
		if (extentSize & mask)
			throw runtime_error("pColStep: Extent size must be a power of 2 in blocks");

	/* calculate shortcuts for rid-based arithmetic */
	for (i = 1, mask = 1, rpbMask = 0; i <= 32; i++) {
		mask <<= 1;
		rpbMask = (rpbMask << 1) | 1;
		if (ridsPerBlock & mask) {
			rpbShift = i;
			break;
		}
	}
	for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
		if (ridsPerBlock & mask)
			throw runtime_error("pColStep: Block size and column width must be a power of 2");

	for (i = 0, mask = 1, blockSizeShift = 0; i < 32; i++) {
		if (mask == BLOCK_SIZE) {
			blockSizeShift = i;
			break;
		}
		mask <<= 1;
	}

	if (i == 32)
		throw runtime_error("pColStep: Block size must be a power of 2");

 	err = dbrm.getExtents(fOid, extents);
	if (err)
		throw runtime_error("pColStep: BRM error!");
	
	lbidList=rhs.getlbidList();

	sort(extents.begin(), extents.end(), ExtentSorter());
	numExtents = extents.size();
	uniqueID = Unique32Generator::instance()->getUnique32();
	if (fDec)
		fDec->addQueue(uniqueID);
// 	initializeConfigParms ( );
}

pColStep::pColStep(const PassThruStep& rhs) :
	fRm(rhs.resourceManager()),
	fInputJobStepAssociation(rhs.inputAssociation()),
	fOutputJobStepAssociation(rhs.outputAssociation()),
	fDec(rhs.dec()),
	fOid(rhs.oid()),
	fTableOid(rhs.tableOid()),
	fSessionId(rhs.sessionId()),
	fTxnId(rhs.txnId()),
	fVerId(rhs.verId()),
	fStepId(rhs.stepId()),
	fStatementId(rhs.statementId()),
	fFilterCount(0),
	fBOP(BOP_NONE),
	ridList(0),
	fColType(rhs.colType()),
	msgsSent(0),
	msgsRecvd(0),
	finishedSending(false),
	recvWaiting(false),
	fIsDict(rhs.isDictCol()),
	ridCount(0),
	// Per Cindy, it's save to put fFlushInterval to be 0
	fFlushInterval(0),
	fSwallowRows(false),
	fProjectBlockReqLimit(fRm.getJlProjectBlockReqLimit()),
	fProjectBlockReqThreshold(fRm.getJlProjectBlockReqThreshold()),
	fStopSending(false),
	fPhysicalIO(0),
	fCacheIO(0),
	fNumBlksSkipped(0),
	fMsgBytesIn(0),
	fMsgBytesOut(0),
	fLogger(rhs.logger())
{
    fCardinality = rhs.cardinality();
    fAlias = rhs.alias();
    fView = rhs.view();
    fName = rhs.name();
	fTupleId = rhs.tupleId();
	int err, i;
	uint mask;
	if ( fOid < 1000 )
		throw runtime_error("pColStep: invalid column");
	ridsPerBlock = BLOCK_SIZE/fColType.colWidth;
	/* calculate some shortcuts for extent and block based arithmetic */
	extentSize = (fRm.getExtentRows()*fColType.colWidth)/BLOCK_SIZE;
	for (i = 1, mask = 1, modMask = 0; i <= 32; i++) {
		mask <<= 1;
		modMask = (modMask << 1) | 1;
		if (extentSize & mask) {
			divShift = i;
			break;
		}
	}
	for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
		if (extentSize & mask)
			throw runtime_error("pColStep: Extent size must be a power of 2 in blocks");

	/* calculate shortcuts for rid-based arithmetic */
	for (i = 1, mask = 1, rpbMask = 0; i <= 32; i++) {
		mask <<= 1;
		rpbMask = (rpbMask << 1) | 1;
		if (ridsPerBlock & mask) {
			rpbShift = i;
			break;
		}
	}
	for (i++, mask <<= 1; i <= 32; i++, mask <<= 1)
		if (ridsPerBlock & mask)
			throw runtime_error("pColStep: Block size and column width must be a power of 2");

	for (i = 0, mask = 1, blockSizeShift = 0; i < 32; i++) {
		if (mask == BLOCK_SIZE) {
			blockSizeShift = i;
			break;
		}
		mask <<= 1;
	}

	if (i == 32)
		throw runtime_error("pColStep: Block size must be a power of 2");

 	err = dbrm.getExtents(fOid, extents);
	if (err)
		throw runtime_error("pColStep: BRM error!");

	sort(extents.begin(), extents.end(), ExtentSorter());
	numExtents = extents.size();
	uniqueID = Unique32Generator::instance()->getUnique32();
	if (fDec)
		fDec->addQueue(uniqueID);
// 	initializeConfigParms ( );
}

pColStep::~pColStep()
{
	// join?
	//delete lbidList;
	if (fDec)
		fDec->removeQueue(uniqueID);
}

//------------------------------------------------------------------------------
// Initialize configurable parameters
//------------------------------------------------------------------------------
void pColStep::initializeConfigParms()
{

// 	const string section           ( "JobList" );
// 	const string sendLimitName     ( "ProjectBlockReqLimit" );
// 	const string sendThresholdName ( "ProjectBlockReqThreshold" );
// 	Config* cf = Config::makeConfig();
// 
// 	string        strVal;
// 	uint64_t numVal;

	//...Get the tuning parameters that throttle msgs sent to primproc
	//...fFilterRowReqLimit puts a cap on how many rids we will request from
	//...    primproc, before pausing to let the consumer thread catch up.
	//...    Without this limit, there is a chance that PrimProc could flood
	//...    ExeMgr with thousands of messages that will consume massive
	//...    amounts of memory for a 100 gigabyte database.
	//...fFilterRowReqThreshhold is the level at which the number of outstanding
	//...    rids must fall below, before the producer can send more rids.

// 	strVal = cf->getConfig(section, sendLimitName);
// 	if (strVal.size() > 0)
// 	{
// 		errno  = 0;
// 		numVal = Config::uFromText(strVal);
// 		if ( errno == 0 )
// 			fProjectBlockReqLimit     = (u_int32_t)numVal;
// 	}
// 
// 	strVal = cf->getConfig(section, sendThresholdName);
// 	if (strVal.size() > 0)
// 	{
// 		errno  = 0;
// 		numVal = Config::uFromText(strVal);
// 		if ( errno == 0 )
// 			fProjectBlockReqThreshold = (u_int32_t)numVal; 
// 	}
}

void pColStep::startPrimitiveThread()
{
	pThread.reset(new boost::thread(pColStepPrimitive(this)));
}

void pColStep::startAggregationThread()
{
	cThread.reset(new boost::thread(pColStepAggregator(this)));
}

void pColStep::run()
{
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
			std::string("pColStep")); // step name
	}

	size_t sz = fInputJobStepAssociation.outSize();
	assert(sz > 0);
	const AnyDataListSPtr& dl = fInputJobStepAssociation.outAt(0);
	DataList_t* dlp = dl->dataList();
	DataList<StringElementType>* strDlp = dl->stringDataList();
	if ( dlp )
		setRidList(dlp);
	else
	{
		setStrRidList( strDlp );
	}
	//Sort can be set through the jobstep or the input JSA if fFlushinterval is 0
	fToSort = (fFlushInterval) ? 0 : (!fToSort) ? fInputJobStepAssociation.toSort() : fToSort;
	fToSort = 0;
	//pthread_mutex_init(&mutex, NULL);
	//pthread_cond_init(&condvar, NULL);
	//pthread_cond_init(&flushed, NULL);
	startPrimitiveThread();
	startAggregationThread();
}

void pColStep::join()
{
	pThread->join();
	cThread->join();
	//pthread_mutex_destroy(&mutex);
	//pthread_cond_destroy(&condvar);
	//pthread_cond_destroy(&flushed);
}

void pColStep::addFilter(int8_t COP, float value)
{
	fFilterString << (uint8_t) COP;
	fFilterString << (uint8_t) 0;
	fFilterString << *((uint32_t *) &value);
	fFilterCount++;
} 

void pColStep::addFilter(int8_t COP, int64_t value, uint8_t roundFlag)
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

void pColStep::setRidList(DataList<ElementType> *dl)
{
	ridList = dl;
}

void pColStep::setStrRidList(DataList<StringElementType> *strDl)
{
	strRidList = strDl;
}

void pColStep::setBOP(int8_t b)
{
	fBOP = b;
}

void pColStep::setOutputType(int8_t OutputType)
{
	fOutputType = OutputType;
}

void pColStep::setSwallowRows(const bool swallowRows)
{
	fSwallowRows = swallowRows;
}

void pColStep::sendPrimitiveMessages()
{
#ifdef PROFILE
	if (fOid>=3000) fProfileData.start(0, sendPrimMsg_c);
#endif

	int it = -1;
	int msgRidCount = 0;
	int ridListIdx = 0;
	bool more = false;
	uint64_t absoluteRID = 0;
	int64_t msgLBID = -1;
	int64_t nextLBID = -1;
	int64_t msgLargeBlock = -1;
	int64_t nextLargeBlock = -1;
	uint16_t blockRelativeRID;
	uint msgCount = 0;
	uint sentBlockCount = 0;
	int msgsSkip=0;
	bool scan=false;
	bool scanThisBlock=false;
	ElementType e;
	UintRowGroup rw;
	StringElementType strE;
	StringRowGroup strRw;
	
	ByteStream msgRidList;
	ByteStream primMsg(MAX_BUFFER_SIZE);   //the MAX_BUFFER_SIZE as of 8/20

	NewColRequestHeader hdr;

	AnyDataListSPtr dl;
	FifoDataList *fifo = NULL;
	StringFifoDataList* strFifo = NULL;

    const bool ignoreCP = ((fTraceFlags & CalpontSelectExecutionPlan::IGNORE_CP) != 0);

	//The presence of more than 1 input DL means we (probably) have a pDictionaryScan step feeding this step
	// a list of tokens to get the rids for. Convert the input tokens to a filter string. We also have a rid
	// list as the second input dl
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

	// determine which ranges/extents to eliminate from this step

#ifdef DEBUG
	if (fOid>=3000)
		cout << "oid " << fOid << endl;
#endif

	scanFlags.resize(numExtents);

	for (uint idx=0; idx <numExtents; idx++)
	{
		if (extents[idx].partition.cprange.isValid != BRM::CP_VALID) {
			scanFlags[idx]=1;
		}
		else
		{

		bool flag = lbidList->CasualPartitionPredicate(
											extents[idx].partition.cprange.lo_val,
											extents[idx].partition.cprange.hi_val,
											&fFilterString,
                                            fFilterCount,
                                            fColType,
                                            fBOP) || ignoreCP;
		scanFlags[idx]=flag;
#ifdef DEBUG
		if (fOid >= 3000 && flushInterval == 0)
			cout << (flag ? "  will scan " : "  will not scan ")
				<< "extent with range " << extents[idx].partition.cprange.lo_val
				<< "-" << extents[idx].partition.cprange.hi_val << endl;
#endif

		}

//		if (fOid>=3000)
//		cout << " " << scanFlags[idx];
	}
//	if (scanFlags.size()>0)	
//		cout << endl;

	// If there was more than 1 input DL, the first is a list of filters and the second is a list of rids,
	// otherwise the first is the list of rids.
	if (fInputJobStepAssociation.outSize() > 1)
		ridListIdx = 1;
	else
		ridListIdx = 0;

	dl = fInputJobStepAssociation.outAt(ridListIdx);
	ridList = dl->dataList();
	if ( ridList ) 
	{
		fifo = dl->fifoDL();

		if (fifo)
			it = fifo->getIterator();
		else
			it = ridList->getIterator();
	}
	else
	{
		strRidList = dl->stringDataList();
		strFifo = dl->stringDL();

		if (strFifo)
			it = strFifo->getIterator();
		else
			it = strRidList->getIterator();		
	}

#ifdef PROFILE
	if (fOid>=3000) fProfileData.start(0, next1_c);
#endif

	if (ridList)
	{
		if (fifo)
		{
			more = fifo->next(it, &rw);
			if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0) {
		    	dlTimes.setFirstReadTime();
	   	 	}
			absoluteRID = rw.et[0].first;
		}
		else
		{
			more = ridList->next(it, &e);
			if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0) {
		    	dlTimes.setFirstReadTime();
	   		}
			absoluteRID = e.first;
			rw.count = 1;
		}
	}
	else
	{
		if (strFifo)
		{
			more = strFifo->next(it, &strRw);
			if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0) {
		    	dlTimes.setFirstReadTime();
	   	 	}
			absoluteRID = strRw.et[0].first;
		}
		else
		{
			more = strRidList->next(it, &strE);
			if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0) {
		    	dlTimes.setFirstReadTime();
	   		}
			absoluteRID = strE.first;
			strRw.count = 1;
		}
	}

#ifdef PROFILE
	if (fOid>=3000) fProfileData.stop(0, next1_c);
#endif
#ifdef PROFILE
	if (fOid>=3000) fProfileData.start(0,getLBID_c);
#endif
	if (more)
		msgLBID = getLBID(absoluteRID, scan);
#ifdef PROFILE
	if (fOid>=3000) fProfileData.stop(0,getLBID_c);
#endif
	scanThisBlock = scan;
	msgLargeBlock = absoluteRID >> blockSizeShift;
			
	while (more || msgRidCount > 0) {
#ifdef PROFILE
		if (fOid>=3000) fProfileData.start(0, sendWork_c);
#endif

		uint64_t rwCount;
		if ( ridList)
			rwCount = rw.count;
		else
			rwCount = strRw.count;
			
		for (uint64_t i = 0; ((i < rwCount) || (!more && msgRidCount > 0)); )
		{
			if ( ridList)
			{
				if (fifo)
					absoluteRID = rw.et[i].first;
				else
					absoluteRID = e.first;
			}
			else
			{
				if (strFifo)
					absoluteRID = strRw.et[i].first;
				else
					absoluteRID = strE.first;
			}
			
			if (more) {
			    nextLBID = getLBID(absoluteRID, scan);
			    nextLargeBlock = absoluteRID >> blockSizeShift;
			}

			//XXXPAT: need to prove N & S here
			if (nextLBID == msgLBID && more) {
// 				blockRelativeRID = absoluteRID % ridsPerBlock;
				blockRelativeRID = absoluteRID & rpbMask;
				msgRidList << blockRelativeRID;
				msgRidCount++;
				++i;
			}
			else {
				//Bug 831: move building msg after the check of scanThisBlock
				if (scanThisBlock==true)
				{
					hdr.ism.Reserve=0;
					hdr.ism.Flags=planFlagsToPrimFlags(fTraceFlags);
					hdr.ism.Command=COL_BY_SCAN;
					hdr.ism.Size=sizeof(NewColRequestHeader) + fFilterString.length() +
					msgRidList.length();
					hdr.ism.Type=2;

					hdr.hdr.SessionID = fSessionId;
					//hdr.hdr.StatementID = 0;
					hdr.hdr.TransactionID = fTxnId;
					hdr.hdr.VerID = fVerId;
					hdr.hdr.StepID = fStepId;
					hdr.hdr.UniqueID = uniqueID;

					hdr.LBID = msgLBID;
// 					assert(hdr.LBID >= 0);
					hdr.DataSize = fColType.colWidth;
					hdr.DataType = fColType.colDataType;
					hdr.CompType = fColType.compressionType;
					hdr.OutputType = fOutputType;
					hdr.BOP = fBOP;
					hdr.NOPS = fFilterCount;
					hdr.NVALS = msgRidCount;
					hdr.sort = fToSort;
					
					primMsg.append((const uint8_t *) &hdr, sizeof(NewColRequestHeader));
					primMsg += fFilterString;
					primMsg += msgRidList;
					ridCount += msgRidCount;
					++sentBlockCount;
					
#ifdef DEBUG
					if (flushInterval == 0 && fOid >= 3000)
						cout << "sending a prim msg for LBID " << msgLBID << endl;
#endif
					++msgCount;
//  				cout << "made a primitive\n";
					if (msgLargeBlock != nextLargeBlock || !more) {
//  					cout << "writing " << msgCount << " primitives\n";
#ifdef PROFILE
						if (fOid>=3000) fProfileData.stop(0, sendWork_c);
						if (fOid>=3000) fProfileData.start(0, write_c);
#endif
						fMsgBytesOut += primMsg.lengthWithHdrOverhead();
						fDec->write(primMsg);
						msgsSent += msgCount;
						msgCount = 0;
#ifdef PROFILE
						if (fOid>=3000) fProfileData.stop(0, write_c);
						if (fOid>=3000) fProfileData.start(0, sendWork_c);
#endif
						primMsg.restart();
						msgLargeBlock = nextLargeBlock;
						
						// @bug 769 - Added "&& !fSwallowRows" condition below to fix problem with 
						// caltraceon(16) not working for tpch01 and some other queries.  If a query ever held 
						// off requesting more blocks, it would lock and never finish.  
						//Bug 815
						if (( sentBlockCount >= fProjectBlockReqLimit) && !fSwallowRows && 
						   (( msgsSent - msgsRecvd) >  fProjectBlockReqThreshold))
						{
							mutex.lock(); //pthread_mutex_lock(&mutex);
							fStopSending = true;						
			
							// @bug 836.  Wake up the receiver if he's sleeping.
							if (recvWaiting)
								condvar.notify_one(); //pthread_cond_signal(&condvar);
							flushed.wait(mutex); //pthread_cond_wait(&flushed, &mutex);
							fStopSending = false;
							mutex.unlock(); //pthread_mutex_unlock(&mutex);
							sentBlockCount = 0;	
						}
					}
				}
				else
				{
					msgsSkip++;
				}
				msgLBID = nextLBID;
				msgRidList.restart();
				msgRidCount = 0;

#ifdef PROFILE
				if (fOid>=3000) fProfileData.start(0, sendLock1_c);
#endif
				mutex.lock(); //pthread_mutex_lock(&mutex);
#ifdef PROFILE
				if (fOid>=3000) fProfileData.stop(0, sendLock1_c);
#endif
				if (scanThisBlock) {
					if (recvWaiting)
						condvar.notify_one(); //pthread_cond_signal(&condvar);
					#ifdef DEBUG
// 					cout << "msgsSent++ = " << msgsSent << endl;
					#endif
				}
				scanThisBlock = scan;
				mutex.unlock(); //pthread_mutex_unlock(&mutex);	

				// break the for loop
				if (!more)
				break;
			}
		} // for rw.count
#ifdef PROFILE
		if (fOid>=3000) fProfileData.stop(0, sendWork_c);
#endif

		if (more)
		{
			if ( ridList )
			{
				if (fifo)
				{
					rw.count = 0;
#ifdef PROFILE
					if (fOid>=3000) fProfileData.start(0, next2_c);
#endif
					more = fifo->next(it, &rw);
#ifdef PROFILE
					if (fOid>=3000) fProfileData.stop(0, next2_c);
#endif
				}
				else
				{
					rw.count = 1;
#ifdef PROFILE
					if (fOid>=3000) fProfileData.start(0, next2_c);
#endif
					more = ridList->next(it, &e);
#ifdef PROFILE
					if (fOid>=3000) fProfileData.stop(0, next2_c);
#endif
				}
			}
			else
			{
				if (strFifo)
				{
					strRw.count = 0;
#ifdef PROFILE
					if (fOid>=3000) fProfileData.start(0, next2_c);
#endif
					more = strFifo->next(it, &strRw);
#ifdef PROFILE
					if (fOid>=3000) fProfileData.stop(0, next2_c);
#endif
				}
				else
				{
					strRw.count = 1;
#ifdef PROFILE
					if (fOid>=3000) fProfileData.start(0, next2_c);
#endif
					more = strRidList->next(it, &strE);
#ifdef PROFILE
					if (fOid>=3000) fProfileData.stop(0, next2_c);
#endif
				}
			}
		}
	}

	if (fOid>=3000) dlTimes.setLastReadTime();

done:
#ifdef PROFILE
	if (fOid>=3000) fProfileData.start(0, sendLock2_c);
#endif
	mutex.lock(); //pthread_mutex_lock(&mutex);
#ifdef PROFILE
	if (fOid>=3000) fProfileData.stop(0, sendLock2_c);
#endif
	finishedSending = true;
	if (recvWaiting)
		condvar.notify_one(); //pthread_cond_signal(&condvar);
	mutex.unlock(); //pthread_mutex_unlock(&mutex);

#ifdef DEBUG
	if (fOid >=3000)
		cout << "pColStep msgSent "
			<< msgsSent << "/" << msgsSkip
			<< " rids " << ridCount
			<< " oid " << fOid << " " << msgLBID << endl;
#endif
	//...Track the number of LBIDs we skip due to Casual Partioning.
	fNumBlksSkipped += msgsSkip;

#ifdef PROFILE
	if (fOid>=3000) fProfileData.stop(0, sendPrimMsg_c);
#endif
}

void pColStep::receivePrimitiveMessages()
{
#ifdef PROFILE
	if (fOid>=3000) fProfileData.start(1, recvPrimMsg_c);
#endif
	int64_t ridResults = 0;
	AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
	DataList_t* dlp = dl->dataList();
	uint64_t fbo;
	FifoDataList *fifo = dl->fifoDL();
	UintRowGroup rw;
	uint64_t ridBase;
	boost::shared_ptr<ByteStream> bs;
	uint i = 0, length;
	
	while (1) {
		// sync with the send side
#ifdef PROFILE
		if (fOid>=3000) fProfileData.start(1, recvLock_c);
#endif
		mutex.lock(); //pthread_mutex_lock(&mutex);
#ifdef PROFILE
		if (fOid>=3000) fProfileData.stop(1, recvLock_c);
#endif
		while (!finishedSending && msgsSent == msgsRecvd) {
			recvWaiting = true;
 			#ifdef DEBUG
 			cout << "c sleeping" << endl;
 			#endif
#ifdef PROFILE
			if (fOid>=3000) fProfileData.start(1, recvWait_c);
#endif
			// @bug 836.  Wake up the sender if he's sleeping.
			if (fStopSending)
				flushed.notify_one(); //pthread_cond_signal(&flushed);
			condvar.wait(mutex); //pthread_cond_wait(&condvar, &mutex);
#ifdef PROFILE
			if (fOid>=3000) fProfileData.stop(1, recvWait_c);
#endif
 			#ifdef DEBUG
			cout << "c waking" << endl;
 			#endif
			recvWaiting = false;
		}
		if (msgsSent == msgsRecvd) {
			mutex.unlock(); //pthread_mutex_unlock(&mutex);
			break;
		}
		mutex.unlock(); //pthread_mutex_unlock(&mutex);
			
		// do the recv
#ifdef PROFILE
		if (fOid>=3000) fProfileData.start(1, read_c);
#endif

		fDec->read(uniqueID, bs);
		fMsgBytesIn += bs->lengthWithHdrOverhead();
#ifdef PROFILE
		if (fOid>=3000) fProfileData.stop(1, read_c);
#endif

		// no more messages, and buffered messages should be already processed by now.
		if (bs->length() == 0) break;

		#ifdef DEBUG
		cout << "msgsRecvd++ = " << msgsRecvd << ".  RidResults = " << ridResults << endl;
		cout << "Got a ColResultHeader!: " << bs.length() << " bytes" << endl;
		#endif

#ifdef PROFILE
		if (fOid>=3000) fProfileData.start(1, recvWork_c);
#endif

		const ByteStream::byte* bsp = bs->buf();
		
		// get the ISMPacketHeader out of the bytestream
 		//const ISMPacketHeader* ism = reinterpret_cast<const ISMPacketHeader*>(bsp);

		// get the ColumnResultHeader out of the bytestream
		const ColResultHeader* crh = reinterpret_cast<const ColResultHeader*>
			(&bsp[sizeof(ISMPacketHeader)]);

		bool firstRead = true;
		length = bs->length();
	
		i = 0;
		uint msgCount = 0;
		while (i < length) {
			++msgCount;

			i += sizeof(ISMPacketHeader);
			crh = reinterpret_cast<const ColResultHeader*>(&bsp[i]);
			// double check the sequence number is increased by one each time
			i += sizeof(ColResultHeader);

			fCacheIO    += crh->CacheIO;
			fPhysicalIO += crh->PhysicalIO;

			// From this point on the rest of the bytestream is the data that comes back from the primitive server
			// This needs to be fed to a datalist that is retrieved from the outputassociation object.
 
			fbo = getFBO(crh->LBID);
			ridBase = fbo << rpbShift;

			#ifdef DEBUG
//	 		cout << "  NVALS = " << crh->NVALS << "  fbo = " << fbo << "  lbid = " << crh->LBID << endl;
			#endif

			//Check output type
			if ( fOutputType == OT_RID )
			{
				ridResults += crh->NVALS;
			}

			/* XXXPAT: This clause is executed when ExeMgr calls the
			   new nextBand(BS) fcn.  

			   TODO: both classes have to agree 
			   on which nextBand() variant will be called.  pColStep
			   currently has to infer that from flushInterval and the
			   Table OID.  It would be better to have a more explicit form
			   of agreement.

			   The goal of the nextBand(BS) fcn is to avoid iterating over
			   every row except at unserialization.  This clause copies
			   the raw results from the PrimProc response directly into
			   the memory used for the ElementType array.  DeliveryStep
			   will also treat the ElementType array as raw memory and
			   serialize that.  TableColumn now parses the packed data
			   instead of whole ElementTypes. 
			*/
			else if (fOutputType == OT_TOKEN && fFlushInterval > 0 && !fIsDict) {

				if (fOid>=3000 && dlTimes.FirstInsertTime().tv_sec==0)
					dlTimes.setFirstInsertTime();
				ridResults += crh->NVALS;

				/* memcpy the bytestream into the output set */
				uint toCopy, bsPos = 0;
				uint8_t *pos;
				while (bsPos < crh->NVALS) {
					toCopy = (crh->NVALS - bsPos > rw.ElementsPerGroup - rw.count ?
						rw.ElementsPerGroup - rw.count : crh->NVALS - bsPos);
					pos = ((uint8_t *) &rw.et[0]) + (rw.count * fColType.colWidth);
					memcpy(pos, &bsp[i], toCopy * fColType.colWidth);
					bsPos += toCopy;
					i += toCopy * fColType.colWidth;
					rw.count += toCopy;
					if (rw.count == rw.ElementsPerGroup) {
						if (!fSwallowRows)
							fifo->insert(rw);
						rw.count = 0;
					}
				}
			}
			else if ( fOutputType == OT_TOKEN)
			{
				uint64_t dv;
				uint64_t rid;

				if (fOid>=3000 && dlTimes.FirstInsertTime().tv_sec==0)
					dlTimes.setFirstInsertTime();
				ridResults += crh->NVALS;
				for(int j = 0; j < crh->NVALS; ++j)
  				{
					// XXXPAT: Only use this when the RID doesn't matter or when
					// the response contains every row.

					rid = j + ridBase;
					switch (fColType.colWidth) {
						case 8: dv = *((const uint64_t *) &bsp[i]); i += 8; break;
						case 4: dv = *((const uint32_t *) &bsp[i]); i += 4; break;
						case 2: dv = *((const uint16_t *) &bsp[i]); i += 2; break;
						case 1: dv = *((const uint8_t *) &bsp[i]); ++i; break;
						default:
							throw runtime_error("pColStep: invalid column width!");
					}	

						// @bug 663 - Don't output any rows if fSwallowRows (caltraceon(16)) is on.
						// 	      This options swallows rows in the project steps.
	   				if (!fSwallowRows)
						{
							if (fifo)
							{
								rw.et[rw.count].first = rid;
								rw.et[rw.count++].second = dv;
								if (rw.count == rw.ElementsPerGroup)
								{
#ifdef PROFILE
									if (fOid>=3000) fProfileData.stop(1, recvWork_c);
									if (fOid>=3000) fProfileData.start(1, insert1_c);
#endif
									fifo->insert(rw);
#ifdef PROFILE
									if (fOid>=3000) fProfileData.stop(1, insert1_c);
									if (fOid>=3000) fProfileData.start(1, recvWork_c);
#endif
									rw.count = 0;
								}
							}
							else
							{
#ifdef PROFILE
								if (fOid>=3000) fProfileData.stop(1, recvWork_c);
								if (fOid>=3000) fProfileData.start(1, insert1_c);
#endif
								dlp->insert(ElementType(rid, dv));
#ifdef PROFILE
								if (fOid>=3000) fProfileData.stop(1, insert1_c);
								if (fOid>=3000) fProfileData.start(1, recvWork_c);
#endif
							}
				#ifdef DEBUG	
					//cout << "  -- inserting <" << rid << ", " << dv << "> " << *prid << endl;
				#endif
						}
					}
				}
				else if ( fOutputType == OT_BOTH )
				{
					ridResults += crh->NVALS;
	  				for(int j = 0; j < crh->NVALS; ++j)
  					{
						uint64_t dv;
						uint64_t rid;

						rid = *((const uint16_t *) &bsp[i]) + ridBase;
						i += sizeof(uint16_t);
						switch (fColType.colWidth) {
							case 8: dv = *((const uint64_t *) &bsp[i]); i += 8; break;
							case 4: dv = *((const uint32_t *) &bsp[i]); i += 4; break;
							case 2: dv = *((const uint16_t *) &bsp[i]); i += 2; break;
							case 1: dv = *((const uint8_t *) &bsp[i]); ++i; break;
							default:
								throw runtime_error("pColStep: invalid column width!");
						}

						// @bug 663 - Don't output any rows if fSwallowRows (caltraceon(16)) is on.
						// 	      This options swallows rows in the project steps.
   					if (!fSwallowRows) {
						if (fOid>=3000 && dlTimes.FirstInsertTime().tv_sec==0)
							dlTimes.setFirstInsertTime();
						if(fifo)
						{
// 							rw.et[rw.count++] = ElementType(rid, dv);
							rw.et[rw.count].first = rid;
							rw.et[rw.count++].second = dv;
							if (rw.count == rw.ElementsPerGroup)
							{
#ifdef PROFILE
									if (fOid>=3000) fProfileData.stop(1, recvWork_c);
									if (fOid>=3000) fProfileData.start(1, insert2_c);
#endif
								fifo->insert(rw);
#ifdef PROFILE
									if (fOid>=3000) fProfileData.stop(1, insert2_c);
									if (fOid>=3000) fProfileData.start(1, recvWork_c);
#endif
								rw.count = 0;
							}
						}
						else
						{
#ifdef PROFILE
							if (fOid>=3000) fProfileData.stop(1, recvWork_c);
							if (fOid>=3000) fProfileData.start(1, insert2_c);
#endif
							dlp->insert(ElementType(rid, dv));
#ifdef PROFILE
							if (fOid>=3000) fProfileData.stop(1, insert2_c);
							if (fOid>=3000) fProfileData.start(1, recvWork_c);
#endif
							
						}
				#ifdef DEBUG
					//cout << "  -- inserting <" << rid << ", " << dv << "> " << *prid << endl;
				#endif
						}
					}
				}
			}	// unpacking the BS
			
			//Bug 815: Check whether we have enough to process
			//++lockCount;
			mutex.lock(); //pthread_mutex_lock(&mutex);
			if  ( fStopSending && ((msgsSent - msgsRecvd ) <=  fProjectBlockReqThreshold) )
			{					
					flushed.notify_one(); //pthread_cond_signal(&flushed);
			}		
			mutex.unlock(); //pthread_mutex_unlock(&mutex);
			
			firstRead = false;
			msgsRecvd += msgCount;

#ifdef PROFILE
		if (fOid>=3000) fProfileData.stop(1, recvWork_c);
#endif
	}	// read loop
	// done reading

	if (fifo && rw.count > 0)
		fifo->insert(rw);

	//...Casual partitioning could cause us to do no processing.  In that
	//...case these time stamps did not get set.  So we set them here.
	if (fOid>=3000 && dlTimes.FirstReadTime().tv_sec==0) {
		dlTimes.setFirstReadTime();
		dlTimes.setLastReadTime();
		dlTimes.setFirstInsertTime();
	}
	if (fOid>=3000) dlTimes.setEndOfInputTime();

	//@bug 699: Reset StepMsgQueue
	fDec->removeQueue(uniqueID);

	if (fifo)
		fifo->endOfInput();
	else
		dlp->endOfInput();

	if (fTableOid >= 3000)
	{
		//...Construct timestamp using ctime_r() instead of ctime() not
		//...necessarily due to re-entrancy, but because we want to strip
		//...the newline ('\n') off the end of the formatted string.
		time_t t = time(0);
		char timeString[50];
		ctime_r(&t, timeString);
		timeString[ strlen(timeString)-1 ] = '\0';

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

		//...Roundoff msg byte counts to nearest KB for display
		uint64_t msgBytesInKB  = fMsgBytesIn  >> 10;
		uint64_t msgBytesOutKB = fMsgBytesOut >> 10;
		if (fMsgBytesIn  & 512)
			msgBytesInKB++;
		if (fMsgBytesOut & 512)
			msgBytesOutKB++;
             
        // @bug 828
        if (fifo)
            fifo->totalSize(ridResults);     

		if (traceOn())
		{
			//...Print job step completion information
			ostringstream logStr;
			logStr << "ses:" << fSessionId <<
				" st: " << fStepId << " finished at " <<
				timeString << "; PhyI/O-" << fPhysicalIO << "; CacheI/O-"  <<
				fCacheIO << "; MsgsRvcd-" << msgsRecvd <<
				"; BlockedFifoIn/Out-" << totalBlockedReadCount <<
				"/" << totalBlockedWriteCount <<
				"; output size-" << ridResults << endl <<
				"\tPartitionBlocksEliminated-" << fNumBlksSkipped <<
				"; MsgBytesIn-"  << msgBytesInKB  << "KB" <<
				"; MsgBytesOut-" << msgBytesOutKB << "KB" << endl  <<
				"\t1st read " << dlTimes.FirstReadTimeString() << 
				"; EOI " << dlTimes.EndOfInputTimeString() << "; runtime-" <<
				JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(),dlTimes.FirstReadTime()) <<
				"s" << endl;
					 	
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

#ifdef PROFILE
	if (fOid>=3000)
	{
		fProfileData.stop(1, recvPrimMsg_c);
		cout << "pColStep (st: " << fStepId << ") execution stats:" << endl;
		cout << "  Primitive msgs sent & recvd: " << msgsSent << endl;
		cout << "  # of rids requested: " << ridCount << endl;
		cout << "  # of rids returned: " << ridResults << endl;
		cout << "  filter elements: " << (int) fFilterCount << endl;
		cout << "  total runtime: " << fProfileData << endl;
	}
#endif
}

const string pColStep::toString() const
{
	ostringstream oss;
	oss << "pColStep        ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId << " st:" << fStepId <<
		" tb/col:" << fTableOid << "/" << fOid;
	if (alias().length()) oss << " alias:" << alias();
	if (view().length()) oss << " view:" << view();
	if (fOutputJobStepAssociation.outSize() > 0)
		oss << " " << omitOidInDL
			<< fOutputJobStepAssociation.outAt(0) << showOidInDL;
	else
		oss << " (no output yet)";
	oss << " nf:" << fFilterCount;
	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
	{
		oss << fInputJobStepAssociation.outAt(i) << ", ";
	}
	if (fSwallowRows)
		oss << " (sink)";
	return oss.str();
}

void pColStep::addFilters()
{
	AnyDataListSPtr dl = fInputJobStepAssociation.outAt(0);
	DataList_t* bdl = dl->dataList();
    FifoDataList* fifo = fInputJobStepAssociation.outAt(0)->fifoDL();

	assert(bdl);
	int it = -1;
	bool more;
	ElementType e;
	int64_t token;

	if (fifo != NULL)
	{
		try{
			it = fifo->getIterator();
		}catch(exception& ex) {
			cerr << "pColStep::addFilters: caught exception: " << ex.what() << " stepno: " <<
				fStepId << endl;
		}catch(...) {
			cerr << "pColStep::addFilters: caught exception" << endl;
		}

		fBOP = BOP_OR;
		UintRowGroup rw;

#ifdef PROFILE
	if (fOid>=3000) fProfileData.start(0, nextAddFilters1_c);
#endif
		more = fifo->next(it, &rw);

#ifdef PROFILE
	if (fOid>=3000) fProfileData.stop(0, nextAddFilters1_c);
#endif
		while (more)
		{
			for (uint64_t i = 0; i < rw.count; ++i)
				addFilter(COMPARE_EQ, (int64_t) rw.et[i].second);
#ifdef PROFILE
			if (fOid>=3000) fProfileData.start(0, nextAddFilters2_c);
#endif
			more = fifo->next(it, &rw);

#ifdef PROFILE
			if (fOid>=3000) fProfileData.stop(0, nextAddFilters2_c);
#endif
		}
	}
	else
	{
		try{
			it = bdl->getIterator();
		}catch(exception& ex) {
			cerr << "pColStep::addFilters: caught exception: " << ex.what() << " stepno: " <<
				fStepId << endl;
		}catch(...) {
			cerr << "pColStep::addFilters: caught exception" << endl;
		}

		fBOP = BOP_OR;

#ifdef PROFILE
		if (fOid>=3000) fProfileData.start(0, nextAddFilters1_c);
#endif
		more = bdl->next(it, &e);

#ifdef PROFILE
		if (fOid>=3000) fProfileData.stop(0, nextAddFilters1_c);
#endif
		while (more)
		{
			token = e.second;
			addFilter(COMPARE_EQ, token);
#ifdef PROFILE
			if (fOid>=3000) fProfileData.start(0, nextAddFilters2_c);
#endif
			more = bdl->next(it, &e);

#ifdef PROFILE
			if (fOid>=3000) fProfileData.stop(0, nextAddFilters2_c);
#endif
		}
	}

	return;
}

/* This exists to avoid a DBRM lookup for every rid. */
inline uint64_t pColStep::getLBID(uint64_t rid, bool& scan)
{
	uint extentIndex, extentOffset;
	uint64_t fbo;
	fbo = rid >> rpbShift;
	extentIndex = fbo >> divShift;
	extentOffset = fbo & modMask;
	scan = (scanFlags[extentIndex] != 0);
	return extents[extentIndex].range.start + extentOffset;
}

inline uint64_t pColStep::getFBO(uint64_t lbid)
{
	uint i;
	uint64_t lastLBID;

	for (i = 0; i < numExtents; i++) {
 		lastLBID = extents[i].range.start + (extents[i].range.size << 10) - 1;
		if (lbid >= (uint64_t) extents[i].range.start && lbid <= lastLBID)
			return (lbid - extents[i].range.start) + (i << divShift);
	}
	cerr << "pColStep: didn't find the FBO?\n";
	throw logic_error("pColStep: didn't find the FBO?");
}
		

void pColStep::appendFilter(const messageqcpp::ByteStream& filter, unsigned count)
{
	fFilterString += filter;
	fFilterCount += count;
}


void pColStep::addFilter(const Filter* f)
{
	if (NULL == f)
		return;

	fFilters.push_back(f);
}


void pColStep::appendFilter(const std::vector<const execplan::Filter*>& fs)
{
	fFilters.insert(fFilters.end(), fs.begin(), fs.end());
}

}   //namespace
// vim:ts=4 sw=4:

