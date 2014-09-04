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

//
// $Id: hashjoin.cpp 8436 2012-04-04 18:18:21Z rdempsey $
// C++ Implementation: hashjoin
//
// Description: 
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//
#include "hashjoin.h"
#include "calpontsystemcatalog.h"
#include "elementcompression.h"
#include <climits>
#include <cstdio>
#include <ctime>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <unistd.h>
 
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

using namespace execplan;
using namespace std;
using namespace joiner;
using namespace joblist;

namespace
{
	const uint64_t LHJ_STEP_OFFSET = 1000;//added to HJ stepId to get LHJ stepId
	const uint64_t BR_STEP_OFFSET  = 2000;//added to BPS to get BucketReuse step
	const uint64_t BDL_VEC_MAXSIZE = 4096;
}

namespace joblist
{

HashJoinStep::HashJoinStep(JoinType jt, uint32_t ses, uint32_t stmt, uint32_t txn,
	ResourceManager *rm) :
	joinType(jt), sessionID(ses), statementID(stmt), txnID(txn), resourceManager(rm),
	sizeFlag(true), fAllowLargeSideFifoToDisk(true),
	om(BOTH), leftCount(0), rightCount(0), bps(0), fSmallCardinalityEstimate(0),
	fSmallSideStepsOut(0), fLargeSideStepsOut(0), fLhjStep(0),
	fHashJoinMode(UM_MEMORY), fTempFilePath(rm->getScTempDiskPath()),
	fFilenameCounter(1), fCompMode(COMPRESS_NONE), fumMemory( rm->getHjUmMaxMemorySmallSide(sessionID)),fpmMemory( rm->getHjPmMaxMemorySmallSide(sessionID)),fhjMemory(0)
{ 
// 	cout << "HashJoinStep(): sessionid" << ses << endl;

	/* The node based STL structures seem to use ~3x the space as an array
		when the elements are only 16 bytes */
	fUMThreshold = fumMemory/(sizeof(ElementType) * 3);
	fPMThreshold = fpmMemory/(sizeof(ElementType) * 3);
	for (unsigned int i=0; i<NUM_ELAPSED_TIMES; i++) {
		fElapsedTimes[i].first  = 0.0; // initialize current start of timer
		fElapsedTimes[i].second = 0.0; // initialize elapsed time
	}
}

HashJoinStep::~HashJoinStep()
{
	if (fhjMemory)
		resourceManager->returnHJUmMaxMemorySmallSide(fhjMemory);
	if (fLhjStep)
		delete fLhjStep;
	if (fBucketReuseParms[0].fStep)
		delete fBucketReuseParms[0].fStep;
	if (fBucketReuseParms[1].fStep)
		delete fBucketReuseParms[1].fStep;

	// Any temp file we created should have been deleted as soon as we were
	// through with it, but we will put a check here just in case some error
	// condition caused us to bypass the normal file cleanup.
	if (fFile.is_open())
		fFile.close();
	if (!fFilename.empty())
		unlink(fFilename.c_str());
}

void HashJoinStep::setOutputMode(OutputMode o)
{
	om = o;
}

const JobStepAssociation & HashJoinStep::inputAssociation() const
{
	return inJSA;
}

void HashJoinStep::inputAssociation(const JobStepAssociation &in)
{
	inJSA = in;
}

const JobStepAssociation & HashJoinStep::outputAssociation() const
{
	return outJSA;
}

void HashJoinStep::outputAssociation(const JobStepAssociation &out)
{
	outJSA = out;
}

void HashJoinStep::logger(const SPJL &logger)
{
	fLogger = logger;
}

void HashJoinStep::stepId(uint16_t id)
{
	stepID = id;
}

uint16_t HashJoinStep::stepId() const
{
	return stepID;
}

uint32_t HashJoinStep::sessionId() const
{
	return sessionID;
}

uint32_t HashJoinStep::statementId() const
{
	return statementID;
}

uint32_t HashJoinStep::txnId() const
{
	return txnID;
}

void HashJoinStep::run()
{
	// any init to do?
	// sanity checks?

	idbassert(inJSA.outSize() == 2);
	idbassert(outJSA.outSize() == 2);
 
	mainRunner.reset(new boost::thread(HJRunner(this)));
}

void HashJoinStep::abort()
{
	die = true;
	if (fBucketReuseParms[0].fStepUsing)
		fBucketReuseParms[0].fStep->abort();
	if (fBucketReuseParms[1].fStepUsing)
		fBucketReuseParms[1].fStep->abort();
	if (fLhjStep != NULL)
		fLhjStep->abort();
}

void HashJoinStep::join()
{
	// Note that we already joined the threads that perform LHJ and
	// small-side writer, so those threads are not "joined" here.
	if (fBucketReuseParms[0].fStepUsing)
		fBucketReuseParms[0].fStep->join();
	if (fBucketReuseParms[1].fStepUsing)
		fBucketReuseParms[1].fStep->join();

	mainRunner->join();
}

void HashJoinStep::setSizeFlag(bool b)
{
	sizeFlag = b;
}

//------------------------------------------------------------------------------
// Set flag controlling how HJ should handle the case where the large-side
// output Fifo "appears" to have become blocked.
// if true  - the HJ should start dumping the elements to disk
// if false - the HJ will wait indefinitely for the fifo to become unblocked
//------------------------------------------------------------------------------
void HashJoinStep::setAllowLargeSideFifoToDisk(bool b)
{
	fAllowLargeSideFifoToDisk = b;
}

//------------------------------------------------------------------------------
// Returns true/false indicating whether temp disk was used to cache the output
// large-side (Fifo or DeliveryWSDL) for the specified output association index.
//------------------------------------------------------------------------------
bool HashJoinStep::didOutputAssocUseDisk( unsigned int index ) const
{
	// Eliminate check for small-side, since we never cache small-side to disk
	if (sizeFlag) {
		if (index == 0)
			return false;
	}
	else {
		if (index == 1)
			return false;
	}

	// Get disk usage total from applicable large-side datalist
	uint64_t            nFiles = 0;
	uint64_t            nBytes = 0;
	FifoDataList*       pFdsl  = 0;
	DeliveryWSDL*       pDWSDL = 0;
	const AnyDataListSPtr& pDl = outJSA.outAt(index);

	if      ((pFdsl   = pDl->fifoDL()) != 0) {
		pFdsl->totalFileCounts (nFiles, nBytes);
	}
	else if ((pDWSDL  = pDl->deliveryWSDL()) != 0) {
		pDWSDL->totalFileCounts(nFiles, nBytes);
	}

	if (nBytes > 0)
		return true;

	return false;
}

//------------------------------------------------------------------------------
// This method is called by JLF prior to knowing anything about small-side vs
// large-side, and whether we will employ LHJ or BucketReuse, etc.  JLF is
// providing us with the information we will need to use BucketReuse, should
// we decide to use it at query execution time.
// pFifoDL           - The input JSA fifo that this BucketReuse info applies to
// filterString      - filter string used in column scan
// tblOid            - OID of table containing colOid
// colOid            - column OID to be scanned from database
// verId             - database version
//------------------------------------------------------------------------------
void
HashJoinStep::addBucketReuse(FifoDataList*        pFifoDL,
	const std::string&                            filterString,
	execplan::CalpontSystemCatalog::OID           tblOid,
	execplan::CalpontSystemCatalog::OID           colOid,
	uint32_t                                      verId,
	execplan::CalpontSystemCatalog::TableColName& colName)
{
	// Find the input JSA that matches the specified FIFO
	for (unsigned int i=0; i<inJSA.outSize(); ++i) {
		if ((ptrdiff_t)pFifoDL == (ptrdiff_t)inJSA.outAt(i)->fifoDL()) {
			fBucketReuseParms[i].fTblOid       = tblOid;
			fBucketReuseParms[i].fColOid       = colOid;
			fBucketReuseParms[i].fVerId        = verId;
			fBucketReuseParms[i].fColName      = colName;
			fBucketReuseParms[i].fFilterString = filterString;
			break;
		}
	}
}

//------------------------------------------------------------------------------
// Initiate creation or reuse of Buckets (BDL) in BucketReuseStep for the
// specified input association.
// idx - jobstep datalist for which bucket reuse is to be initialized
//------------------------------------------------------------------------------
void
HashJoinStep::initiateBucketReuse(unsigned int idx)
{
	// prevent syscat calls get into the reuse manager due to bad xml config
	// which may invalid the db version being reused
	if (fBucketReuseParms[idx].fTblOid < 3000)
		return;

	bool scan = true;

	// lock this check, so the version in reuse manager will not be changed
	// in a race condition
	boost::mutex::scoped_lock lock(BucketReuseManager::instance()->getMutex());

	uint64_t buckets = resourceManager->getHjMaxBuckets();
	BucketReuseControlEntry* entry =
		BucketReuseManager::instance()->userRegister(
			fBucketReuseParms[idx].fColName,
			fBucketReuseParms[idx].fFilterString,
			fBucketReuseParms[idx].fVerId,
			buckets,
			scan);

	if (entry)
	{
		fBucketReuseParms[idx].fBucketReuseEntry = entry;
		fBucketReuseParms[idx].fReadOnly         = !scan;
		if (scan == false)
		{
			fBucketReuseParms[idx].fStep = new BucketReuseStep(
				fBucketReuseParms[idx].fColOid,
				fBucketReuseParms[idx].fTblOid,
				sessionID,
				txnID,
				fBucketReuseParms[idx].fVerId,
				statementID);
		}
	}
}

/* Is it possible to get rid of the DataList switching? */
void HashJoinStep::hjRunner()
{
// 	cout << "hjrunner()\n";
	uint64_t      nFiles   = 0;
	uint64_t      nBytes   = 0;
	bool          bFifoDiskUsed = false;
	FifoDataList* largeOut = 0;

	if (0 < inJSA.status())
	{
		outJSA.status(inJSA.status());
		outJSA.outAt(0)->dataList()->endOfInput();
		outJSA.outAt(1)->dataList()->endOfInput();
		startAdjoiningSteps();
	}
	else
	{

	FifoDataList *leftDL, *rightDL, *smallDL, *largeDL, *leftOut, *rightOut,
		*smallOut;
	DataList_t *smallOutDL = NULL, *largeOutDL = NULL, *leftOutDL = NULL, 
		*rightOutDL = NULL;
	uint leftIt, rightIt, smallIt, largeIt, i, smallSize;
	UintRowGroup inputRG;
	bool more, sendSmall, sendLarge, sendAllSmallSide;
	uint64_t *smallCount, *largeCount;

	leftDL = inJSA.outAt(0)->fifoDL();
	rightDL = inJSA.outAt(1)->fifoDL();
	leftOut = outJSA.outAt(0)->fifoDL();
	if (leftOut == NULL)
		leftOutDL = outJSA.outAt(0)->dataList();
	rightOut = outJSA.outAt(1)->fifoDL();
	if (rightOut == NULL)
		rightOutDL = outJSA.outAt(1)->dataList();
	idbassert(leftDL);
	idbassert(rightDL);
	idbassert(leftOut || leftOutDL);
	idbassert(rightOut || rightOutDL);

	leftIt = leftDL->getIterator();
	rightIt = rightDL->getIterator();

	if (sizeFlag) {
		smallDL = leftDL;
		largeDL = rightDL;
		smallOut = leftOut;
		largeOut = rightOut;
		smallOutDL = leftOutDL;
		largeOutDL = rightOutDL;
		smallIt = leftIt;
		largeIt = rightIt;
		sendSmall = (om == BOTH || om == LEFTONLY);
		sendLarge = (om == BOTH || om == RIGHTONLY);
		smallCount = &leftCount;
		largeCount = &rightCount;
	}
	else {
		smallDL = rightDL;
		largeDL = leftDL;
		smallOut = rightOut;
		largeOut = leftOut;
		smallOutDL = rightOutDL;
		largeOutDL = leftOutDL;
		smallIt = rightIt;
		largeIt = leftIt;
		sendSmall = (om == BOTH || om == RIGHTONLY);
		sendLarge = (om == BOTH || om == LEFTONLY);
		smallCount = &rightCount;
		largeCount = &leftCount;
	}

	/* include all of the large side when:
		- jointype is left outer and left is large
		- jointype is right outer and right is large
	   include all of the small side when:
		- jointype is left outer and left is small
		- jointype is right outer and right is small
	*/

	joiner.reset(new Joiner
		((joinType == LEFTOUTER && !sizeFlag) ||
		 (joinType == RIGHTOUTER && sizeFlag)));
	sendAllSmallSide = ((joinType == LEFTOUTER && sizeFlag) ||
		(joinType == RIGHTOUTER && !sizeFlag));

	/*
	read the small side and instantiate a Joiner
	send the Joiner to BPS
	start the BPS
	
	read from the large side.
		For the In-UM alg, an optimization would be to connect the BPS
		directly to the large-side output.  No real need for a passthrough,
		except to be notified that it's done.
	*/
	smallSize = 0;

	/* Automatically perform LHJ if estimated cardinality exceeds UM limit. */
	/* After LHJ completes, update dlTimes for result graphs.               */
	if (fSmallCardinalityEstimate > fUMThreshold)
	{
		fHashJoinMode = LARGE_HASHJOIN_CARD;
		performLargeHashJoin( smallDL, smallIt );
		fLhjStep->join();
		dlTimes.setFirstReadTime ( fLhjStep->dlTimes.FirstReadTime()  );
		dlTimes.setEndOfInputTime( fLhjStep->dlTimes.EndOfInputTime() );
		return;
	}

	string currentAction("reading small-side");
	bool startedAdjSteps = false;

	try {
	more = smallDL->next(smallIt, &inputRG);

	startElapsedTime(WAIT_FOR_UMMEMORY);
	//This can block.
	if (fUMThreshold > inputRG.count)
		fhjMemory = resourceManager->requestHJMaxMemorySmallSide(sessionID, fpmMemory);  
	else 
		fhjMemory = resourceManager->requestHJUmMaxMemorySmallSide(sessionID); 
	stopElapsedTime(WAIT_FOR_UMMEMORY);

	startElapsedTime(READ_SMALL_SIDE);
	if (fTableOID2 >= 3000) dlTimes.setFirstReadTime();
	while (more && !die) {
		smallSize += inputRG.count;
		if (fhjMemory < fumMemory && smallSize > fPMThreshold)
		{
			startElapsedTime(WAIT_FOR_UMMEMORY);
			fhjMemory += resourceManager->requestHJMaxMemorySmallSide(sessionID, fumMemory - fpmMemory);
			stopElapsedTime(WAIT_FOR_UMMEMORY);
		}
		for (i = 0; i < inputRG.count; ++i) {
			joiner->insert(inputRG.et[i]);
		}
		/* largehashjoin */
		/* After LHJ completes, update dlTimes for result graphs. */
		if (smallSize > fUMThreshold)
		{
			fHashJoinMode = LARGE_HASHJOIN_RUNTIME;
			stopElapsedTime(READ_SMALL_SIDE);
			performLargeHashJoin( smallDL, smallIt );

 			resourceManager->returnHJUmMaxMemorySmallSide(fhjMemory);
		
			fLhjStep->join();
			dlTimes.setFirstReadTime ( fLhjStep->dlTimes.FirstReadTime()  );
			dlTimes.setEndOfInputTime( fLhjStep->dlTimes.EndOfInputTime() );
			return;
		}
		more = smallDL->next(smallIt, &inputRG);
	}
	stopElapsedTime(READ_SMALL_SIDE);

	startElapsedTime(RETURN_UNUSED_MEMORY);
	uint64_t usedMemory =  joiner->getMemUsage();
	//Joiner returns a minimum of chunksize, which may be less than fpmMemory
	if (usedMemory < fhjMemory)
	{
		resourceManager->returnHJUmMaxMemorySmallSide(fhjMemory - usedMemory);
 		fhjMemory = usedMemory;
	}
	stopElapsedTime(RETURN_UNUSED_MEMORY);


	currentAction = "setting up joiner";

	/* If vec is small enough, do In-PM or In-UM else use largehashjoin */
	/* We should also restrict based on the expected size of the large side.
		We'd lose time on all these extra msgs if there are < 1M elements to check
		for example.  Might also want to do it only if there are multiple PMs */

	if (smallSize < fPMThreshold && fTableOID2 >= 3000) {
		fHashJoinMode = PM_MEMORY;
		joiner->inPM(true);
	}

	/* In-UM algorithm */
	bps->useJoiner(joiner);
	startElapsedTime(START_OTHER_STEPS);
	startAdjoiningSteps();
	stopElapsedTime(START_OTHER_STEPS);

	startedAdjSteps = true;
	currentAction   = "writing large-side";

	bFifoDiskUsed = outputLargeSide(
		largeDL,largeIt,sendLarge,sendSmall,largeOut,largeOutDL,largeCount);

	smallSideWriter.reset(new boost::thread(HJSmallSideWriter(this,
		sendSmall, sendAllSmallSide, smallOut, smallOutDL, smallCount)));

	/* If large-side (FIFO) was cached to temp disk, then go back and  */
	/* copy the contents of the file to the large-side output datalist */
	if (bFifoDiskUsed)
		outputLargeSideFifoFromDisk(largeOut);
	if (largeOut)
		largeOut->totalSize(*largeCount);

	} // try

	//...If enter a "catch" block, we keep going and try starting adjoining
	//...steps else ExeMgr could block waiting for a jobstep to be "joined".
	//...Similarly, we need to call endOfInput() on any output datalists.
	catch (const logging::LargeDataListExcept& ex)
	{
		ostringstream errMsg;
		errMsg << "HashJoinStep::hjRunner LDL error " << currentAction << "; "<<
			ex.what();
		errorLogging(errMsg.str());
		outJSA.status(logging::hashJoinStepLargeDataListFileErr);
		if (!startedAdjSteps)
			startAdjoiningSteps();
	}
	catch (const exception& ex)
	{
		stringstream errMsg;
		errMsg << "HashJoinStep::hjRunner exception " << currentAction << "; "<<
			ex.what();
		errorLogging(errMsg.str());
		outJSA.status(logging::hashJoinStepErr);
		if (!startedAdjSteps)
			startAdjoiningSteps();
	}
	catch (...)
	{
		stringstream errMsg;
		errMsg << "HashJoinStep::hjRunner unknown error " << currentAction;
		errorLogging(errMsg.str());
		outJSA.status(logging::hashJoinStepErr);
		if (!startedAdjSteps)
			startAdjoiningSteps();
	}

	if (largeOut)
		largeOut->endOfInput();
	else
		largeOutDL->endOfInput();

	if (bFifoDiskUsed)
		stopElapsedTime(LARGE_SIDE_FIFO_FROM_DISK);
	else
		stopElapsedTime(RW_LARGE_SIDE_DL);

	if (smallSideWriter.get()) {
		smallSideWriter->join();
	}
	else { // send endOfInput if exception kept us from starting writer thread
		if (smallOut)
			smallOut->endOfInput();
		else
			smallOutDL->endOfInput();
	}

	startElapsedTime(RETURN_USED_MEMORY);
	resourceManager->returnHJUmMaxMemorySmallSide(fhjMemory);
	stopElapsedTime(RETURN_USED_MEMORY);
	fhjMemory = 0;

	} // else (inJSA.status() == 0)

	if (fTableOID2 >= 3000)
	{
		dlTimes.setEndOfInputTime();
		char leftSideLabel  = 'S';
		char rightSideLabel = 'L';
		if (!sizeFlag) {
			leftSideLabel  = 'L';
			rightSideLabel = 'S';
		}

		if (largeOut) {
			largeOut->totalFileCounts(nFiles, nBytes);
		}
		else {
			DeliveryWSDL* pDWSDL = 0;
			if (sizeFlag)
				pDWSDL = outJSA.outAt(1)->deliveryWSDL();
			else
				pDWSDL = outJSA.outAt(0)->deliveryWSDL();
			if (pDWSDL)
				pDWSDL->totalFileCounts(nFiles, nBytes);
		}

    	ostringstream oss;
		oss << "ses:" << sessionID << " st: " << stepID <<
			" Hashjoin finished:"
			" output sizes left-side(" << leftSideLabel << ") " << leftCount <<
			", right-side(" << rightSideLabel << ") " << rightCount << "; " <<
			((fHashJoinMode==PM_MEMORY) ? "PM-MemoryJoin" : "UM-MemoryJoin");
		if (nBytes > 0) {
			uint64_t kBytes = nBytes >> 10;
			if (nBytes & 512)
				kBytes++;
			oss << " (tempDisk-" << kBytes << "KB)";
		}
		oss << endl <<
			"\t1st read " << dlTimes.FirstReadTimeString() <<
			"; EOI " << dlTimes.EndOfInputTimeString() << " run time: " <<
				JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << "s" <<
			endl <<
			"\tElapsed times: " << fixed << setprecision(2) <<
			"startSteps-"    << fElapsedTimes[START_OTHER_STEPS].second   <<
			"s; rdSmall-"    << fElapsedTimes[READ_SMALL_SIDE].second     <<
			"s; wrSmall-"    << fElapsedTimes[WRITE_SMALL_SIDE].second    <<
			"s; waitLarge-"  << fElapsedTimes[WAIT_FOR_LARGE_SIDE].second <<
			"s; r/wLargeDL-" << fElapsedTimes[RW_LARGE_SIDE_DL].second    <<"s";
			if (bFifoDiskUsed) {
				oss <<
				"; LFifoToDisk-" << 
				fElapsedTimes[LARGE_SIDE_FIFO_TO_DISK].second <<
				"s; LFifoFromDisk-" <<
				fElapsedTimes[LARGE_SIDE_FIFO_FROM_DISK].second << "s";
			}

			oss << "\n\tResourceManager: wait for UmMemory-"  <<  setprecision(6) << fElapsedTimes[WAIT_FOR_UMMEMORY].second  
			<< " return unused-" << fElapsedTimes[RETURN_UNUSED_MEMORY].second
			<< " return used-" << fElapsedTimes[RETURN_USED_MEMORY].second;

			oss << endl <<
			"\tJob completion status " << inJSA.status() << endl << endl;
		cout << oss.str();
	}
}

void HashJoinStep::setLargeSideBPS(BatchPrimitive* b)
{
	bps = b;
}

// Set estimated small side cardinality used to determine if LHJ is to be used
void HashJoinStep::setSmallSideCardEst( uint64_t card )
{
	fSmallCardinalityEstimate = card;
}

//------------------------------------------------------------------------------
// Outputs elements to large-side output datalist
// largeDL    - input large-side datalist elements are to be read from
// largeIt    - iterator used to read from largeDL
// sendLarge  - boolean indicating whether jointype requires largeside output
// sendSmall  - boolean indicating whether smallside is to be output
// largeOut   - fifo output large-side datalist
// largeOutDL - alternate (zdl) large-side output datalist
// largeCount - number of elements read from largeDL
// return value of true indicates elements had to be cached to temp disk.
//------------------------------------------------------------------------------
bool HashJoinStep::outputLargeSide(
	FifoDataList *largeDL,
	uint          largeIt,
	bool          sendLarge,
	bool          sendSmall,
	FifoDataList *largeOut,
	DataList_t   *largeOutDL,
	uint64_t     *largeCount)
{
	UintRowGroup inputRG;

	startElapsedTime(WAIT_FOR_LARGE_SIDE);
	bool more = largeDL->next(largeIt, &inputRG);
	stopElapsedTime(WAIT_FOR_LARGE_SIDE);
	startElapsedTime(RW_LARGE_SIDE_DL);
	bool consumeStarted = false;
	while (more && !die) {
		if (sendLarge) {
			bool bufferFull     = false;

			/* if large output is a FIFO, need to pack row groups... */
			*largeCount += inputRG.count;
			if (largeOut) {
// 				cout << "large side:\n";
// 				for (i = 0; i < rg.count; ++i)
// 					cout << "  <" << rg.et[i].first << ", " << rg.et[i].second << ">" << endl;
// 				cout << endl;

				// If we are sending both small and large-sides then we have to
				// handle case where large-side becomes blocked, by sending
				// large-side FIFO to disk.  If not sending small-side we can
				// always output large-side to FIFO w/o using disk.
				// If fAllowLargeSideFifoToDisk is false, then JLF has de-
				// tected a pattern whereby permanent blocking will not occur,
				// so we do not need to worry about caching the fifo to disk.
				if (sendSmall && fAllowLargeSideFifoToDisk) {
					largeOut->insert(inputRG, bufferFull, consumeStarted);

					// if large-side FIFO is blocked awaiting consumer,
					// first retry, and if necessary, cache to temp disk
					if(bufferFull) {
						if (checkIfFifoBlocked(largeOut)) {
							stopElapsedTime(RW_LARGE_SIDE_DL);
							outputLargeSideFifoToDisk(largeDL,largeIt,
								largeCount,largeOut);
							return true;
						}
					}
				}
				else {
					largeOut->insert(inputRG);
				}
			}
			else {
				for (uint i = 0; i < inputRG.count; ++i)
					largeOutDL->insert(inputRG.et[i]);
			}
		}
		more = largeDL->next(largeIt, &inputRG);
	}
	return false;
}

//------------------------------------------------------------------------------
// Go into loop MAX_SLEEP_RETRIES times, waiting to see if a blocked FIFO
// output datalist will eventually accept insertions.
// largeOut - fifo output large-side datalist
// return value of true means largeOut is still blocked.
//------------------------------------------------------------------------------
bool HashJoinStep::checkIfFifoBlocked(FifoDataList *largeOut)
{
	const uint MAX_SLEEP_RETRIES = 100;
	const uint USLEEP_INTERVAL   = 5000; // half a second

	bool blocked = true;
	for (uint k=0; k<MAX_SLEEP_RETRIES; ++k) {
		usleep ( USLEEP_INTERVAL );
		blocked = largeOut->isOutputBlocked();
		if (!blocked)
			break;
	}

	return blocked;
}

//------------------------------------------------------------------------------
// When large-side output FIFO datalist is blocked, this method is called to
// save the elements out to disk, so that we can go ahead and complete the join,
// and send the small-side result set to the subsequent small-side output
// jobstep.  The saved FIFO datalist, must then be read back in and routed to
// the large-side output jobstep by outputLargeSideFifoFromDisk().
// largeDL    - input large-side datalist elements are to be read from
// largeIt    - iterator used to read from largeDL
// largeCount - number of elements read from largeDL
// largeOut   - fifo output large-side datalist
//------------------------------------------------------------------------------
void HashJoinStep::outputLargeSideFifoToDisk(
	FifoDataList *largeDL,
	uint          largeIt,
	uint64_t     *largeCount,
	FifoDataList *largeOut)
{
	startElapsedTime(LARGE_SIDE_FIFO_TO_DISK);
	uint32_t ridSize = largeOut->getDiskElemSize1st();
	uint32_t valSize = largeOut->getDiskElemSize2nd();
	UintRowGroup inputRG;

	// Set enum and generic buffer ptr relative to how we intend to compress
	// the data saved to disk;  We use a char* buffer so that we can share a
	// common buffer across all compression modes.
	uint32_t elemCnt = inputRG.ElementsPerGroup;
	if (largeOut->getElementMode() == FifoDataList::RID_ONLY) {
		if (ridSize == 4) {
			fCompMode = COMPRESS_32;
			fCompBuffer.reset(new char[sizeof(uint32_t)*elemCnt]);
		}
		else {
			fCompMode = COMPRESS_64;
			fCompBuffer.reset(new char[sizeof(uint64_t)*elemCnt]);
		}
	}
	else {
		if      ((ridSize == 8) && (valSize == 4)) {
			fCompMode = COMPRESS_64_32;
			fCompBuffer.reset(new char[sizeof(CompElement64Rid32Val)*elemCnt]);
		}
		else if ((ridSize == 4) && (valSize == 8)) {
			fCompMode = COMPRESS_32_64;
			fCompBuffer.reset(new char[sizeof(CompElement32Rid64Val)*elemCnt]);
		}
		else if ((ridSize == 4) && (valSize == 4)) {
			fCompMode = COMPRESS_32_32;
			fCompBuffer.reset(new char[sizeof(CompElement32Rid32Val)*elemCnt]);
		}
		else {
			fCompMode = COMPRESS_NONE;
		}
	}

	try {
		// open temporary file for our output fifo datalist
		createTempFile();
    	//ostringstream oss;
		//oss << "ses:" << sessionID << " HashJoinStep " << stepID <<
		//	" caching large-side FIFO to file " << fFilename << endl;
		//cout << oss.str();

		// write elements coming from largeDL (input datalist) to disk


		bool more = largeDL->next(largeIt, &inputRG);
		while (more && !die) {
			/* if large output is a FIFO, need to pack row groups... */
			*largeCount += inputRG.count;
			writeRowGroupToDisk(inputRG);
			more = largeDL->next(largeIt, &inputRG);
		}

		// save the file usage stats in the FIFO datalist
		largeOut->setTotalFileCounts(1, fFile.tellg());
	}
	catch (...) {
		if (fFile.is_open())
			fFile.close();
		if (!fFilename.empty()) {
			unlink(fFilename.c_str());
			fFilename.clear();
		}

        ostringstream errMsg;
		errMsg << "Error occurred saving HashJoin FIFO into file " << fFilename;
		errorLogging(errMsg.str());
		throw;		// rethrow to main try/catch block in hjRunner()
	}
	stopElapsedTime(LARGE_SIDE_FIFO_TO_DISK);
}

//------------------------------------------------------------------------------
// Writes the current contents of the "rg" RowGroup to a temp disk file.  We
// compress the elements using a common char* buffer if compression is needed.
//------------------------------------------------------------------------------
void HashJoinStep::writeRowGroupToDisk(const UintRowGroup &rg)
{
	fFile.write((char*)&(rg.count), sizeof(rg.count));

	switch (fCompMode)
	{
		case COMPRESS_64:
		{
			uint64_t* ridArray = reinterpret_cast<uint64_t*>(fCompBuffer.get());
			for (unsigned int i=0; i<rg.count; ++i) {
				ridArray[i] = rg.et[i].first;
			}
			fFile.write(fCompBuffer.get(), (sizeof(uint64_t)*rg.count));
			break;
		}
		case COMPRESS_32:
		{
			uint32_t* ridArray = reinterpret_cast<uint32_t*>(fCompBuffer.get());
			for (unsigned int i=0; i<rg.count; ++i) {
				ridArray[i] = rg.et[i].first;
			}
			fFile.write(fCompBuffer.get(), (sizeof(uint32_t)*rg.count));
			break;
		}
		case COMPRESS_64_32:
		{
			CompElement64Rid32Val* elemArray =
				reinterpret_cast<CompElement64Rid32Val*>(fCompBuffer.get());
			for (unsigned int i=0; i<rg.count; ++i) {
				elemArray[i].first  = rg.et[i].first;
				elemArray[i].second = rg.et[i].second;
			}
			fFile.write(fCompBuffer.get(),
				(sizeof(CompElement64Rid32Val)*rg.count));
			break;
		}
		case COMPRESS_32_64:
		{
			CompElement32Rid64Val* elemArray =
				reinterpret_cast<CompElement32Rid64Val*>(fCompBuffer.get());
			for (unsigned int i=0; i<rg.count; ++i) {
				elemArray[i].first  = rg.et[i].first;
				elemArray[i].second = rg.et[i].second;
			}
			fFile.write(fCompBuffer.get(),
				(sizeof(CompElement32Rid64Val)*rg.count));
			break;
		}
		case COMPRESS_32_32:
		{
			CompElement32Rid32Val* elemArray =
				reinterpret_cast<CompElement32Rid32Val*>(fCompBuffer.get());
			for (unsigned int i=0; i<rg.count; ++i) {
				elemArray[i].first  = rg.et[i].first;
				elemArray[i].second = rg.et[i].second;
			}
			fFile.write(fCompBuffer.get(),
				(sizeof(CompElement32Rid32Val)*rg.count));
			break;
		}
		case COMPRESS_NONE:
		default:
		{
			fFile.write((char*)rg.et, (sizeof(ElementType)*rg.count));
			break;
		}
	}
}

//------------------------------------------------------------------------------
// Create FIFO cache disk file in Calpont temp disk directory.
// (Basically stole this code from LargeDataList::createTempFile())
//------------------------------------------------------------------------------
void HashJoinStep::createTempFile()
{
    int64_t fd;
    /* Is there a good way to do this through an fstream? */
    do {
        fFilename = getFilename();
        fd = open(fFilename.c_str(), O_CREAT | O_RDWR | O_EXCL | O_BINARY, 0666);
    } while (fd < 0 && errno == EEXIST);

    if (fd < 0)
    {
        ostringstream errMsg;
        errMsg << "Error occurred creating HashJoin FIFO file " <<
			fFilename << "; errno-" << errno << "; errmsg-" <<
			strerror(errno);
        throw runtime_error(errMsg.str());
    }

    close(fd);

    //cout << "Creating/opening file: " << fFilename << endl;
    fFile.open(fFilename.c_str(),
        ios_base::in | ios_base::out | ios_base::binary );
    if (!(fFile.is_open())) {
        ostringstream errMsg;
        errMsg << "Error occurred opening HashJoin FIFO file " <<
			fFilename << "; errno-" << errno << "; errmsg-" <<
			strerror(errno);
        throw runtime_error(errMsg.str());
    }
}

//------------------------------------------------------------------------------
// Build/return unique filename in Calpont temp disk directory.
// File name is built, using a counter in addition to "this" pointer address.
// If file is found to exist, then this function can be called again, and a
// new counter value is used.
// (Basically stole this code from LargeDataList::getFilename())
//------------------------------------------------------------------------------
string HashJoinStep::getFilename()
{
    stringstream o;

    o << fTempFilePath << "/FIFO-0x" << hex << (ptrdiff_t)this <<
		dec << "-" << fFilenameCounter++;
    return o.str();
}

//------------------------------------------------------------------------------
// Copy elements from temporary FIFO temp disk file, and insert into the
// specified largeOut datalist.
// largeOut - fifo output large-side datalist
//------------------------------------------------------------------------------
void HashJoinStep::outputLargeSideFifoFromDisk(FifoDataList *largeOut)
{
	startElapsedTime(LARGE_SIDE_FIFO_FROM_DISK);
	try {
		UintRowGroup rg;

		// Reposition the temp file back to the start of the file
		fFile.seekg ( 0 );

		largeOut->waitTillReadyForInserts();

		// Loop through reading row groups from the temp file and inserting
		// them into the large-side output datalist.
		while (!die) {
			if (readRowGroupFromDisk(&rg))
				break;

			largeOut->insert(rg);
		}
	} // try
	catch (...) {
		ostringstream errMsg;
		errMsg << "Error occurred copying HashJoin FIFO from file " <<fFilename;
		errorLogging(errMsg.str());
		fFile.close();
		unlink(fFilename.c_str());
		fFilename.clear();
		throw;		// rethrow to main try/catch block in hjRunner()
	}

	fFile.close();
	unlink(fFilename.c_str());
	fFilename.clear();
}

//------------------------------------------------------------------------------
// Reads next RowGroup from temp disk into "rg", expanding the data if needed.
// Returns "true" to indicate EOF.
//------------------------------------------------------------------------------
bool HashJoinStep::readRowGroupFromDisk(UintRowGroup *rg)
{
	fFile.read((char*)&(rg->count), sizeof(rg->count));
	if (fFile.eof())
		return true;

	switch (fCompMode)
	{
		case COMPRESS_64:
		{
			fFile.read(fCompBuffer.get(), (sizeof(uint64_t)*rg->count));
			if (!fFile.good())
				break;

			uint64_t* ridArray = reinterpret_cast<uint64_t*>(fCompBuffer.get());
			for (unsigned int i=0; i<rg->count; ++i) {
				rg->et[i].first  = ridArray[i];
				rg->et[i].second = 0;
			}
			break;
		}
		case COMPRESS_32:
		{
			fFile.read(fCompBuffer.get(), (sizeof(uint32_t)*rg->count));
			if (!fFile.good())
				break;

			uint32_t* ridArray = reinterpret_cast<uint32_t*>(fCompBuffer.get());
			for (unsigned int i=0; i<rg->count; ++i) {
				rg->et[i].first  = ridArray[i];
				rg->et[i].second = 0;
			}
			break;
		}
		case COMPRESS_64_32:
		{
			fFile.read(fCompBuffer.get(),
				(sizeof(CompElement64Rid32Val)*rg->count));
			if (!fFile.good())
				break;

			CompElement64Rid32Val* elemArray =
				reinterpret_cast<CompElement64Rid32Val*>(fCompBuffer.get());
			for (unsigned int i=0; i<rg->count; ++i) {
				rg->et[i].first  = elemArray[i].first;
				rg->et[i].second = elemArray[i].second;
			}
			break;
		}
		case COMPRESS_32_64:
		{
			fFile.read(fCompBuffer.get(),
				(sizeof(CompElement32Rid64Val)*rg->count));
			if (!fFile.good())
				break;

			CompElement32Rid64Val* elemArray =
				reinterpret_cast<CompElement32Rid64Val*>(fCompBuffer.get());
			for (unsigned int i=0; i<rg->count; ++i) {
				rg->et[i].first  = elemArray[i].first;
				rg->et[i].second = elemArray[i].second;
			}
			break;
		}
		case COMPRESS_32_32:
		{
			fFile.read(fCompBuffer.get(),
				(sizeof(CompElement32Rid32Val)*rg->count));
			if (!fFile.good())
				break;

			CompElement32Rid32Val* elemArray =
				reinterpret_cast<CompElement32Rid32Val*>(fCompBuffer.get());
			for (unsigned int i=0; i<rg->count; ++i) {
				rg->et[i].first  = elemArray[i].first;
				rg->et[i].second = elemArray[i].second;
			}
			break;
		}
		case COMPRESS_NONE:
		default:
		{
			fFile.read((char*)rg->et, (sizeof(ElementType)*rg->count));
			if (!fFile.good())
				break;

			break;
		}
	}

	if (!fFile.good()) {
       	ostringstream errMsg;
		errMsg << "Error occurred reading HashJoin FIFO file " <<
			fFilename << "; errno-" << errno << "; errmsg-" <<
			strerror(errno) << endl;
       	throw runtime_error(errMsg.str());
	}

	return false;
}

//------------------------------------------------------------------------------
// Outputs elements to small-side output datalist.  Input comes from joiner.
// sendSmall  - boolean indicating whether jointype requires smallside output
// sendAllSmallSide - boolean requesting all of small-side (due to outerjoin)
// smallOut   - fifo output small-side datalist
// smallOutDL - alternate (zdl) small-side output datalist
// smallCount - number of elements copied to output datalist
//------------------------------------------------------------------------------
void HashJoinStep::outputSmallSide(
	bool          sendSmall,
	bool          sendAllSmallSide,
	FifoDataList *smallOut,
	DataList_t   *smallOutDL,
	uint64_t     *smallCount)
{
	startElapsedTime(WRITE_SMALL_SIDE);
	try {
	UintRowGroup smallRg;

	smallRg.count = 0;
	if (sendSmall) {
		boost::shared_ptr<vector<ElementType> > sortedSmallSide;

		if (sendAllSmallSide) {
			sortedSmallSide = joiner->getSmallSide();
			sort<vector<ElementType>::iterator>(sortedSmallSide->begin(),
				sortedSmallSide->end());
		}
		else
			sortedSmallSide = joiner->getSortedMatches();

		joiner.reset();

		uint vSize = sortedSmallSide->size();
		*smallCount = vSize;
// 		cout << "small side:\n";
		for (uint i = 0; i < vSize && !die; ++i) {
			/* if small output is a FIFO, need to pack row groups... */
			if (smallOut) {
				smallRg.et[smallRg.count++] = (*sortedSmallSide)[i];
				if (smallRg.count == smallRg.ElementsPerGroup) {
					smallOut->insert(smallRg);
					smallRg.count = 0;
				}
			}
			else
				smallOutDL->insert((*sortedSmallSide)[i]);
		}
	}
	else {
		joiner.reset();
	}

	if (smallOut) {
		if (smallRg.count > 0)
			smallOut->insert(smallRg);
		smallOut->totalSize(*smallCount);
	}
	} // try
	catch (const logging::LargeDataListExcept& ex)
	{
		ostringstream errMsg;
		errMsg<<"HashJoinStep::outputSmallSide LDL error writing small-side; "<<
			ex.what();
		errorLogging(errMsg.str());
		outJSA.status( logging::hashJoinStepLargeDataListFileErr );
	}
	catch (const exception& ex)
	{
		stringstream errMsg;
		errMsg<<"HashJoinStep::outputSmallSide exception writing small-side; "<<
			ex.what();
		errorLogging(errMsg.str());
		outJSA.status( logging::hashJoinStepErr );
	}
	catch (...)
	{
		string errMsg (
			"HashJoinStep::outputSmallSide unknown error writing small-side");
		errorLogging(errMsg);
		outJSA.status( logging::hashJoinStepErr );
	}

	if (smallOut)
		smallOut->endOfInput();
	else
		smallOutDL->endOfInput();
	stopElapsedTime(WRITE_SMALL_SIDE);
}

//------------------------------------------------------------------------------
// Public method called by JLF to provide list of jobsteps connected to this
// hashjoin's large-side output.
//------------------------------------------------------------------------------
void HashJoinStep::setLargeSideStepsOut(const vector<SJSTEP>& largeSideSteps)
{
	if (largeSideSteps.size() > 0)
	{
		ostringstream oss;
		for (unsigned i=0; i<largeSideSteps.size(); ++i)
		{
			fLargeSideStepsOut.push_back( largeSideSteps[i] );
			//if (traceOn())
			//{
			//	if (i == 0)
			//		oss << "  HJ step " << stepId() <<
			//		" adding steps to LargeSideOut: " <<
			//		largeSideSteps[i]->stepId();
			//	else
			//		oss << ", " << largeSideSteps[i]->stepId();
			//}
		}
		//if (traceOn())
		//{
		//	oss  << endl;
		//	cout << oss.str();
		//}
	}
}

//------------------------------------------------------------------------------
// Public method called by JLF to provide list of jobsteps connected to this
// hashjoin's small-side output.
//------------------------------------------------------------------------------
void HashJoinStep::setSmallSideStepsOut(const vector<SJSTEP>& smallSideSteps)
{
	if (smallSideSteps.size() > 0)
	{
   		ostringstream oss;
		for (unsigned i=0; i<smallSideSteps.size(); ++i)
		{
			fSmallSideStepsOut.push_back( smallSideSteps[i] );
			//if (traceOn())
			//{
			//	if (i == 0)
			//		oss << "  HJ step " << stepId() <<
			//		" adding steps to SmallSideOut: " <<
			//		smallSideSteps[i]->stepId();
			//	else
			//		oss << ", " << smallSideSteps[i]->stepId();
			//}
		}
		//if (traceOn())
		//{
		//	oss  << endl;
		//	cout << oss.str();
		//}
	}
}

//------------------------------------------------------------------------------
// Start jobsteps that have been delayed, if they can be started now, else
// we just decrement the count, and another hashjoin will start them later,
// when the wait step count is decremented to 1.
// Also note the extra BucketReuse logic involving the start of the large-
// side input step.  For a LHJ, if we can replace the input step with a
// BucketReuseStep, then we do so.
//------------------------------------------------------------------------------
void HashJoinStep::startAdjoiningSteps()
{
	if (bps->decWaitToRunStepCnt() == 1)
	{
		bool startedBucketReuseStep = false;

		if ((fHashJoinMode == LARGE_HASHJOIN_CARD) ||
			(fHashJoinMode == LARGE_HASHJOIN_RUNTIME))
		{
			unsigned int largeSide = 0;
			if (sizeFlag)
				largeSide = 1;

			// If possible, substitute a BucketReuseStep for large-side in BPS
			if (fBucketReuseParms[largeSide].fBucketReuseEntry &&
				fBucketReuseParms[largeSide].fReadOnly)
			{
				JobStepAssociation bucketReuseJSA(inJSA.statusPtr());
				bucketReuseJSA.outAdd( inJSA.outAt(largeSide) );
				fBucketReuseParms[largeSide].fStep->outputAssociation(
					bucketReuseJSA );
				fBucketReuseParms[largeSide].fStep->stepId(bps->stepId() +
					BR_STEP_OFFSET);
				fBucketReuseParms[largeSide].fStepUsing = true;

				if (traceOn())
				{
					time_t t = time(0);
					char timeString[50];
					ctime_r(&t, timeString);

					ostringstream oss;
					oss << "ses:" << sessionID << " HashJoinStep " << stepID <<
						" starting large-side BucketReuse InputStep " <<
						fBucketReuseParms[largeSide].fStep->stepId() << 
						" at: " << timeString;
					//oss<<fBucketReuseParms[largeSide].fStep->toString()<<endl;
					cout << oss.str();
				}
				fBucketReuseParms[largeSide].fStep->run();
				startedBucketReuseStep = true;

				// We won't be running the BPS, but we still update the output
				// JSA so that the result graph can connect the steps correctly.
				bps->outputAssociation( bucketReuseJSA );
			}
		}

		if (!startedBucketReuseStep)
		{
			if (traceOn())
			{
				time_t t = time(0);
				char timeString[50];
				ctime_r(&t, timeString);

				ostringstream oss;
				oss << "ses:" << sessionID << " HashJoinStep " << stepID <<
					" starting large-side InputStep " << bps->stepId() <<
					" at: " << timeString;
				//oss << bps->toString() << endl;
				cout << oss.str();
			}
			bps->run();
		}
	}

	for (unsigned int i=0; i<fSmallSideStepsOut.size(); ++i)
	{
		if (fSmallSideStepsOut[i]->decWaitToRunStepCnt() == 1)
		{
			if (traceOn())
			{
				time_t t = time(0);
				char timeString[50];
				ctime_r(&t, timeString);

	    		ostringstream oss;
				oss << "ses:" << sessionID << " HashJoinStep " << stepID <<
					" starting small-side OutputStep " << 
					fSmallSideStepsOut[i]->stepId() << " at: " << timeString;
				//oss << fSmallSideStepsOut[i]->toString() << endl;
				cout << oss.str();
			}
			fSmallSideStepsOut[i]->run();
		}
	}

	for (unsigned int i=0; i<fLargeSideStepsOut.size(); ++i)
	{
		if (fLargeSideStepsOut[i]->decWaitToRunStepCnt() == 1)
		{
			if (traceOn())
			{
				time_t t = time(0);
				char timeString[50];
				ctime_r(&t, timeString);

	    		ostringstream oss;
				oss << "ses:" << sessionID << " HashJoinStep " << stepID <<
					" starting large-side OutputStep " << 
					fLargeSideStepsOut[i]->stepId() << " at: " << timeString;
				//oss << fLargeSideStepsOut[i]->toString() << endl;
				cout << oss.str();
			}
			fLargeSideStepsOut[i]->run();
		}
	}
}

//------------------------------------------------------------------------------
// Perform LargeHashJoin employing disk cache with BDL input and ZDL output
// smallDL - fifo datalist we are using to read in the small-side input
// smallIt - iterator we are using to read elements in from smallDL
//------------------------------------------------------------------------------
void
HashJoinStep::performLargeHashJoin(
	FifoDataList* smallDL,
	uint smallIt )
{
	startElapsedTime(START_OTHER_STEPS);
	//...Create jobstep to perform "large" hashjoin
	fLhjStep = new LargeHashJoin( joinType,
		sessionID,
		txnID,
		statementID,
		*resourceManager);

	//..."Might" consider adding a LargeHashJoin constructor that takes a
	//...HashJoinStep arg, so we would not have to call all these setters.
	fLhjStep->logger     ( fLogger      );
	fLhjStep->stepId     ( stepId() + LHJ_STEP_OFFSET );
	fLhjStep->setTraceFlags(fTraceFlags );
	fLhjStep->alias1     ( fAlias1      );
	fLhjStep->alias2     ( fAlias2      );
	fLhjStep->view1      ( fView1       );
	fLhjStep->view2      ( fView2       );
	fLhjStep->tableOid1  ( fTableOID1   );
	fLhjStep->tableOid2  ( fTableOID2   );
	fLhjStep->cardinality( fCardinality );

	JobStepAssociation lhjJsaIn (inJSA.statusPtr());   // JSA Input to LHJ
	JobStepAssociation lhjJsaOut(inJSA.statusPtr());   // JSA Output from LHJ
	BucketDataList*    lhjSmallBdlIn = 0; // BDL small-side input to LHJ

	//...Construct BDL jobstep association going into LargeHashJoin.
	//...(For the sake of the jobstep graph, we also modify the jobstep assoc
	//... for the original hj step, so that they reflect the new associations
	//... used by the LargeHashJoin step.)
	JobStepAssociation myNewInputJSA(inJSA.statusPtr());

	bool bBucketReuseError = false;
	try {
	if (sizeFlag) // input 1 is large-side
	{
		buildLhjInJsa( inJSA.outAt(0), 0  , lhjJsaIn );
		buildLhjInJsa( inJSA.outAt(1), bps, lhjJsaIn );
		lhjSmallBdlIn = lhjJsaIn.outAt(0)->bucketDL();
		myNewInputJSA.outAdd( inJSA.outAt(0) );   // original small-side input
		myNewInputJSA.outAdd( lhjJsaIn.outAt(1) );// new large-side input

		//...Provide bucket-reuse info to large-side BDL
		initiateBucketReuse(1);
		if (fBucketReuseParms[1].fBucketReuseEntry)
			lhjJsaIn.outAt(1)->bucketDL()->reuseControl(
				fBucketReuseParms[1].fBucketReuseEntry,
				fBucketReuseParms[1].fReadOnly );
	}
	else          // input 0 is large-side
	{
		buildLhjInJsa( inJSA.outAt(0), bps, lhjJsaIn );
		buildLhjInJsa( inJSA.outAt(1), 0  , lhjJsaIn );
		lhjSmallBdlIn = lhjJsaIn.outAt(1)->bucketDL();
		myNewInputJSA.outAdd( lhjJsaIn.outAt(0) );// new large-side input
		myNewInputJSA.outAdd( inJSA.outAt(1) );   // original small-side input

		//...Provide bucket-reuse info to large-side BDL
		initiateBucketReuse(0);
		if (fBucketReuseParms[0].fBucketReuseEntry)
			lhjJsaIn.outAt(0)->bucketDL()->reuseControl(
				fBucketReuseParms[0].fBucketReuseEntry,
				fBucketReuseParms[0].fReadOnly );
	}
	} // try

	//...If enter a "catch" block, we keep going and try starting adjoining
	//...steps else ExeMgr could block waiting for a jobstep to be "joined".
	//...Similarly, we need to call endOfInput() on any output datalists.
	catch (const logging::LargeDataListExcept& ex)
	{
		ostringstream errMsg;
		errMsg << "HashJoinStep::performLHJ LDL error with BucketReuse; " <<
			ex.what();
		errorLogging(errMsg.str());
		outJSA.status( logging::hashJoinStepLargeDataListFileErr );
		bBucketReuseError = true;
	}
	catch (const exception& ex)
	{
		stringstream errMsg;
		errMsg << "HashJoinStep::performLHJ exception with BucketReuse; " <<
			ex.what();
		errorLogging(errMsg.str());
		outJSA.status( logging::hashJoinStepErr );
		bBucketReuseError = true;
	}
	catch (...)
	{
		string errMsg (
			"HashJoinStep::performLHJ unknown error with BucketReuse");
		errorLogging(errMsg);
		outJSA.status( logging::hashJoinStepErr );
		bBucketReuseError = true;
	}

	fLhjStep->inputAssociation( lhjJsaIn );
	inputAssociation( myNewInputJSA );// update my input assoc for jobstep graph

	//...Construct ZDL jobstep asssociation coming out of LargeHashJoin
	if (sizeFlag)
	{
		buildLhjOutJsa( outJSA.outAt(0), fSmallSideStepsOut, lhjJsaOut,
			(om == RIGHTONLY) );
		buildLhjOutJsa( outJSA.outAt(1), fLargeSideStepsOut, lhjJsaOut,
			(om == LEFTONLY)  );
	}
	else
	{
		buildLhjOutJsa( outJSA.outAt(0), fLargeSideStepsOut, lhjJsaOut,
			(om == RIGHTONLY) );
		buildLhjOutJsa( outJSA.outAt(1), fSmallSideStepsOut, lhjJsaOut,
			(om == LEFTONLY)  );
	}
	fLhjStep->outputAssociation( lhjJsaOut );
	outputAssociation( lhjJsaOut );  // update my output assoc for jobstep graph

	startAdjoiningSteps();

	//...Start the LargeHashJoin step itself
	if (traceOn())
	{
		ostringstream oss;
		oss << "ses:" << sessionID << " st: " << stepID <<
			" creating the following LargeHashJoin step: " << endl <<
			fLhjStep->toString() << endl;
		cout << oss.str();
	}
	fLhjStep->run();
	stopElapsedTime(START_OTHER_STEPS);

	//...If we have problems with BucketReuse, then we can skip sending
	//...any elements to our small-side output datalist, else we start
	//...sending the small-side elements to LHJ.
	uint64_t smallSideCount = joiner->size();
	if (!bBucketReuseError)
	{
		//...Forward small-side data read in so far, to LargeHashJoin.
		//...If we have already started reading from small side, then resume
		//...the READ_SMALL_SIDE timer, else we wait till after our first read.
		if (smallSideCount > 0)
			startElapsedTime(READ_SMALL_SIDE);

		try {
		vector<ElementType> bdlVec;
		for (Joiner::iterator it=joiner->begin(); it!=joiner->end(); ++it)
		{
			bdlVec.push_back(ElementType(it->second & ~Joiner::MSB, it->first));
			if (bdlVec.size() > BDL_VEC_MAXSIZE)
			{
				lhjSmallBdlIn->insert(bdlVec);
				bdlVec.clear();
			}
		}
		joiner.reset();
		if (bdlVec.size() > 0)
		{
			lhjSmallBdlIn->insert(bdlVec);
			bdlVec.clear();
		}
		vector<ElementType>().swap(bdlVec); // immediately release memory
	
		//...Enter loop to forward remaining small-side elements to LHJ
		bool         more;
		UintRowGroup rg;

		more = smallDL->next(smallIt, &rg);
		if (smallSideCount == 0)
		{
			startElapsedTime(READ_SMALL_SIDE); // start timer if first read
			if (fTableOID2 >= 3000) dlTimes.setFirstReadTime();
		}
		while (more)
		{
			smallSideCount += rg.count;
			lhjSmallBdlIn->insert(rg.et, rg.count);
			more = smallDL->next(smallIt, &rg);
		}
		} // try
		catch (const logging::LargeDataListExcept& ex)
		{
			ostringstream errMsg;
			errMsg << "HashJoinStep::performLHJ LDL error writing BDL; " <<
				ex.what();
			errorLogging(errMsg.str());
			outJSA.status( logging::hashJoinStepLargeDataListFileErr );
		}
		catch (const exception& ex)
		{
			stringstream errMsg;
			errMsg << "HashJoinStep::performLHJ exception writing BDL; " <<
				ex.what();
			errorLogging(errMsg.str());
			outJSA.status( logging::hashJoinStepErr );
		}
		catch (...)
		{
			string errMsg (
				"HashJoinStep::performLHJ unknown error writing BDL");
			errorLogging(errMsg);
			outJSA.status( logging::hashJoinStepErr );
		}
	}
	else
	{
		startElapsedTime(READ_SMALL_SIDE);
		joiner.reset();
	}

	lhjSmallBdlIn->endOfInput();
	stopElapsedTime(READ_SMALL_SIDE);

	if (fTableOID2 >= 3000)
	{
		dlTimes.setEndOfInputTime();

    	ostringstream oss;
		oss << "ses:" << sessionID << " st: " << stepID <<
			"; Initial HashJoin step finished: Using LHJ step: " <<
			fLhjStep->stepId() << " (" <<
			((fHashJoinMode==LARGE_HASHJOIN_CARD) ?
				"card-select" : "run-select") << "); input " <<
			((sizeFlag) ? " left" : " right") << "-side (small-side) count: " <<
			smallSideCount << endl <<
			"\t1st read " << dlTimes.FirstReadTimeString() <<
			"; EOI-to-LHJ " << dlTimes.EndOfInputTimeString() << " run time: "<<
				JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << "s" <<
			endl <<
			"\tJob completion status " << inJSA.status() << endl << endl;
		cout << oss.str();
	}
}

//------------------------------------------------------------------------------
// Build BDL jobstep associations connecting jobStep2 output to LHJ input
// anyDL1    - if jobStep2 is nonzero, then anyDL1 is the datalist we are
//             replacing in jobStep2's output association.
// jobStep2  - (optional) jobstep that is connected to LHJ input
// newJsa1   - jobstep input association to be stored in LHJ jobstep
//------------------------------------------------------------------------------
void
HashJoinStep::buildLhjInJsa(
	const  AnyDataListSPtr& anyDl1,
	JobStep*                jobStep2,
	JobStepAssociation&     newJsa1)
{
	FifoDataList* fdl = anyDl1->fifoDL();
	if (fdl)
	{
		BucketDataList* pBdl = new BucketDataList(
			resourceManager->getHjMaxBuckets(), fdl->getNumConsumers(),
			resourceManager->getHjMaxElems(), *resourceManager);
		pBdl->setHashMode(1);
		pBdl->setDiskElemSize( fdl->getDiskElemSize1st(),
			fdl->getDiskElemSize2nd());
		pBdl->OID( fdl->OID() );
		if (fTraceFlags & CalpontSelectExecutionPlan::TRACE_DISKIO_UM)
			pBdl->enableDiskIoTrace();
		AnyDataListSPtr spdlBdl(new AnyDataList());
		spdlBdl->bucketDL(pBdl);

		// Place new BDL into output JSA of jobStep2
		if (jobStep2)
		{
			DataList_t* dl1in = anyDl1->dataList();
			JobStepAssociation newJsa2(inJSA.statusPtr());

			for (unsigned int i = 0;
				i < jobStep2->outputAssociation().outSize();
				++i)
			{
				AnyDataListSPtr dl2In=jobStep2->outputAssociation().outAt(i);
				if ((ptrdiff_t)(dl2In->dataList()) == (ptrdiff_t)dl1in)
					newJsa2.outAdd( spdlBdl ); // found match; replace with BDL
				else
					newJsa2.outAdd( dl2In );   // no match; reuse old datalist
			}

			if (traceOn())
			{
				ostringstream oss;
				oss << " updated output JSA in step " <<
					jobStep2->stepId() << " for LHJ: ";
				printJSA( oss.str().c_str(), newJsa2 );
			}
			jobStep2->outputAssociation( newJsa2 );
		}

		newJsa1.outAdd( spdlBdl );
	}
	else
	{
		newJsa1.outAdd( anyDl1 ); // assume already a BDL, so just copy as-is
	}
}

//------------------------------------------------------------------------------
// Build ZDL jobstep associations connecting LHJ output to jobSteps2 input
// anyDL1    - output datalist from LHJ that we are searching for in adjoining
//             jobSteps2 vector
// jobSteps2 - jobstep vector of jobsteps that are connected to LHJ output
// newJsa1   - jobstep output association to be stored in LHJ jobstep
// dropInserts-flag indicating if we should drop inserts to this output ZDL
//------------------------------------------------------------------------------
void
HashJoinStep::buildLhjOutJsa(
	const  AnyDataListSPtr& anyDl1,
	vector<SJSTEP>&         jobSteps2,
	JobStepAssociation&     newJsa1,
	bool                    dropInserts )
{
	// If hashjoin output is not ZDL, we switch to ZDL
	if (!anyDl1->zonedDL())
	{
		// Create new ZDL to connect hashjoin to subsequent jobstep.
		uint     numConsumers;
		uint32_t diskElemSize1st, diskElemSize2nd;
		execplan::CalpontSystemCatalog::OID dlOID;
		uint     elementMode;

		FifoDataList* fdl = anyDl1->fifoDL();
		if (fdl) {
			numConsumers    = fdl->getNumConsumers();
			diskElemSize1st = fdl->getDiskElemSize1st();
			diskElemSize2nd = fdl->getDiskElemSize2nd();
			dlOID           = fdl->OID();
			if (fdl->getElementMode() == FifoDataList::RID_ONLY)
				elementMode = ZonedDL::RID_ONLY;
			else
				elementMode = ZonedDL::RID_VALUE;
		}
		else {
			DeliveryWSDL* dwdl = anyDl1->deliveryWSDL();
			numConsumers    = dwdl->getNumConsumers();
			diskElemSize1st = dwdl->getDiskElemSize1st();
			diskElemSize2nd = dwdl->getDiskElemSize2nd();
			dlOID           = dwdl->OID();
			if (dwdl->getElementMode() == DeliveryWSDL::RID_ONLY)
				elementMode = ZonedDL::RID_ONLY;
			else
				elementMode = ZonedDL::RID_VALUE;
		}
		
		ZonedDL* pZdl = new ZonedDL( numConsumers, *resourceManager);
		pZdl->setMultipleProducers(true);
		pZdl->setDiskElemSize( diskElemSize1st, diskElemSize2nd );
		pZdl->OID( dlOID );
		pZdl->setElementMode( elementMode );
		if (dropInserts)
			pZdl->dropAllInserts();
		if (fTraceFlags & CalpontSelectExecutionPlan::TRACE_DISKIO_UM)
			pZdl->enableDiskIoTrace();
		AnyDataListSPtr spdlZdl(new AnyDataList());
		spdlZdl->zonedDL(pZdl);

		DataList_t* dl1in = anyDl1->dataList();

		// Place new ZDL into "input" JSA of steps in jobStep2 vector
		for (unsigned int m=0; m<jobSteps2.size(); m++)
		{
			JobStepAssociation newJsa2(inJSA.statusPtr());
			for (unsigned int i = 0;
				i < jobSteps2[m]->inputAssociation().outSize();
				++i)
			{
				AnyDataListSPtr dl2In=jobSteps2[m]->inputAssociation().outAt(i);
				if ((ptrdiff_t)(dl2In->dataList()) == (ptrdiff_t)dl1in)
					newJsa2.outAdd( spdlZdl ); // found match; replace with ZDL
				else
					newJsa2.outAdd( dl2In );   // no match; reuse old datalist
			}

			if (traceOn())
			{
    			ostringstream oss;
				oss << " updated input JSA in step " <<
					jobSteps2[m]->stepId() << " for LHJ: ";
				printJSA( oss.str().c_str(), newJsa2 );
			}
			jobSteps2[m]->inputAssociation( newJsa2 );
		}

		// Place new ZDL into "output" JSA of hashjoin step.
		newJsa1.outAdd( spdlZdl );
	}
	else
	{
		newJsa1.outAdd( anyDl1 ); // assume already a ZDL, so just copy as-is
	}
}

//------------------------------------------------------------------------------
// Debug function used to dump a JobStepAssociation
//------------------------------------------------------------------------------
void
HashJoinStep::printJSA(const char* label, const JobStepAssociation& jsa)
{
    ostringstream oss;
	oss << "ses:" << sessionID << " st:" << stepID << "; " << label;
	for (unsigned int i=0; i<jsa.outSize(); ++i)
	{
		if (i > 0)
			oss << ", ";
		oss << jsa.outAt(i);
	}
	oss << endl;
	cout << oss.str();
}

const string HashJoinStep::toString() const
{
    ostringstream oss;
    CalpontSystemCatalog::OID oid1 = 0;
    CalpontSystemCatalog::OID oid2 = 0;
    DataList_t* dl1;
    DataList_t* dl2;
    size_t idlsz;

    idlsz = inJSA.outSize();
    idbassert(idlsz == 2);
    dl1 = inJSA.outAt(0)->dataList();
    if (dl1) oid1 = dl1->OID();
    dl2 = inJSA.outAt(1)->dataList();
    if (dl2) oid2 = dl2->OID();
    oss << "HashJoinStep    ses:" << sessionID << " st:" << stepID;
    oss << omitOidInDL;
    oss << " in  tb/col1:" << fTableOID1 << "/" << oid1;
    oss << " " << inJSA.outAt(0);
    oss << " in  tb/col2:" << fTableOID2 << "/" << oid2;
    oss << " " << inJSA.outAt(1);

    idlsz = outJSA.outSize();
	if (idlsz > 0)
	{
    	oss << endl << "                    ";
    	dl1 = outJSA.outAt(0)->dataList();
    	if (dl1) oid1 = dl1->OID();
    	oss << " out tb/col1:" << fTableOID1 << "/" << oid1;
    	oss << " " << outJSA.outAt(0);
	}
	if (idlsz > 1)
	{
    	dl2 = outJSA.outAt(1)->dataList();
    	if (dl2) oid2 = dl2->OID();
    	oss << " out tb/col2:" << fTableOID2 << "/" << oid2;
    	oss << " " << outJSA.outAt(1);
	}

    return oss.str();
}

//------------------------------------------------------------------------------
// Log specified error to stderr and the critical log
//------------------------------------------------------------------------------
void HashJoinStep::errorLogging(const string& msg) const
{
	ostringstream errMsg;
	errMsg << "Step " << stepId() << "; " << msg;
	cerr   << errMsg.str() << endl;
	catchHandler( errMsg.str(), sessionId() );
}

//------------------------------------------------------------------------------
// Start a timer used to track elapsed time for the specified HjElapsedTime enum
// fElapsedTimes[i].first is the start of current elapsed time we are tracking.
// fElapsedTimers[i].second is the sum of elapsed times.
//------------------------------------------------------------------------------
void HashJoinStep::startElapsedTime(HjElapsedTime whichTime)
{
	struct timeval currentTime;
	gettimeofday(&currentTime, 0);

	fElapsedTimes[whichTime].first =
		(double)currentTime.tv_sec+(double)currentTime.tv_usec/1000000.0;
}

//------------------------------------------------------------------------------
// Stop a timer for the specified HjElapsedTime enum, and update elapsed time
//------------------------------------------------------------------------------
void HashJoinStep::stopElapsedTime(HjElapsedTime whichTime)
{
	struct timeval currentTime;
	gettimeofday(&currentTime, 0);

	double endTime = 
		(double)currentTime.tv_sec+(double)currentTime.tv_usec/1000000.0;
	fElapsedTimes[whichTime].second += (endTime-fElapsedTimes[whichTime].first);
}

}
