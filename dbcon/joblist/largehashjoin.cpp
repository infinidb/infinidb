/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

// $Id: largehashjoin.cpp 9655 2013-06-25 23:08:13Z xlou $
#include <string>
#include <sstream>
#include <cassert>
#include <stdexcept>
#include <ctime>
#include <sys/time.h>
#include <iomanip>
using namespace std;

#include <boost/thread/mutex.hpp>

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "jobstep.h"
#include "largehashjoin.h"
#include "elementtype.h"
using namespace joblist;

boost::mutex fileLock_g;

namespace
{
void logDiskIoInfo(uint64_t stepId, const AnyDataListSPtr& spdl)
{
	boost::mutex::scoped_lock lk(fileLock_g);
	ofstream umDiskIoFile("/var/log/Calpont/trace/umdiskio.log", ios_base::app);

	CalpontSystemCatalog::OID oid;
	uint64_t maxBuckets = 0;
	list<DiskIoInfo>* infoList = NULL;
	string bkt("bkt");
	BucketDL<ElementType>*       bdl  = spdl->bucketDL();
	BucketDL<StringElementType>* sbdl = spdl->stringBucketDL();
	ZDL<ElementType>*            zdl  = spdl->zonedDL();
	ZDL<StringElementType>*      szdl = spdl->stringZonedDL();

	if (bdl != NULL)
	{
		maxBuckets = bdl->bucketCount();
		oid = bdl->OID();
	}
	else if (zdl != NULL)
	{
		maxBuckets = zdl->bucketCount();
		oid = zdl->OID();
		bkt = "zdl";
	}
	else if (sbdl != NULL)
	{
		maxBuckets = sbdl->bucketCount();
		oid = sbdl->OID();
	}
	else if (szdl != NULL)
	{
		maxBuckets = szdl->bucketCount();
		oid = szdl->OID();
		bkt = "zdl";
	}
	else
	{
		// not logged for now.
		return;
	}

	for (uint64_t i = 0; i < maxBuckets; i++)
	{
		if (bdl) infoList = &(bdl->diskIoInfoList(i));
		else if (zdl) infoList = &(zdl->diskIoInfoList(i));
		else if (sbdl) infoList = &(sbdl->diskIoInfoList(i));
		else if (szdl) infoList = &(szdl->diskIoInfoList(i));

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


namespace joblist
{
const uint64_t ZDL_VEC_SIZE = 4096;
//@bug 686. Make hashjoin doing jobs in seperate thread and return to main immediately.
// So the other job steps can start.
struct HJRunner
{
	HJRunner(LargeHashJoin* p) : joiner(p)
    {}
	LargeHashJoin *joiner;
    void operator()()
    {
        try {
			joiner->doHashJoin();
		}
		catch (std::exception &e) {
			ostringstream errMsg;
			errMsg << "HJRunner caught: " << e.what();
			joiner->errorLogging(errMsg.str());
			joiner->unblockDatalists(logging::largeHashJoinErr);
		}
		catch (...) {
			string msg("HJRunner caught something not an exception!");
			joiner->errorLogging(msg);
			joiner->unblockDatalists(logging::largeHashJoinErr);
		}
    }
};

struct StringHJRunner
{
	StringHJRunner(StringHashJoinStep* p) : joiner(p)
    {}
	StringHashJoinStep *joiner;
    void operator()()
    {
        try {
			joiner->doStringHashJoin();
		}
		catch (std::exception &e) {
			ostringstream errMsg;
			errMsg << "StringHJRunner caught: " << e.what();
			joiner->errorLogging(errMsg.str());
			joiner->unblockDatalists(logging::stringHashJoinStepErr);
		}
		catch (...) {
			string msg("StringHJRunner caught something not an exception!");
			joiner->errorLogging(msg);
			joiner->unblockDatalists(logging::stringHashJoinStepErr);
		}
    }
};

// Thread function used by HashJoin
//
template<typename e_t>
void* HashJoinByBucket_thr(void* arg)
{
	typename HashJoin<e_t>::thrParams_t* params = (typename HashJoin<e_t>::thrParams_t*)arg;
	HashJoin<e_t>* hjPtr = params->hjptr;
	const uint32_t thrIdx = params->thrIdx;
	long set1Size = 0;
	long set2Size = 0;
	bool sendAllHashSet = false;
	bool sendAllSearchSet = false;
try
{
	for (uint idx=0, bucketIdx=params->startBucket;
		idx<params->numBuckets;
		idx++, bucketIdx++) {

#ifdef DEBUG
	cout << "\tJoinByBucket() thr " << dec << thrIdx
		<< " bkt " << bucketIdx
		<<  "/" << hjPtr->Set1()->bucketCount()
		<< "/" << params->numBuckets << endl;
#endif

	JoinType joinType = hjPtr->getJoinType();

	set1Size = hjPtr->Set1()->size(bucketIdx);
	set2Size = hjPtr->Set2()->size(bucketIdx);

	if ( set1Size <= 0 && set2Size <= 0 ) {
		continue;
	}
	else
	{
		if (set1Size > set2Size)
		{
			hjPtr->setSearchSet(hjPtr->Set1()->getBDL(), thrIdx);
			hjPtr->setHashSet(hjPtr->Set2()->getBDL(), thrIdx);
			hjPtr->setSearchResult(hjPtr->Result1(), thrIdx);
			hjPtr->setHashResult(hjPtr->Result2(), thrIdx);
			sendAllHashSet = (joinType == RIGHTOUTER);
			sendAllSearchSet = (joinType == LEFTOUTER);
		}
		else
		{
			hjPtr->setHashSet(hjPtr->Set1()->getBDL(), thrIdx);
			hjPtr->setSearchSet(hjPtr->Set2()->getBDL(), thrIdx);
			hjPtr->setHashResult(hjPtr->Result1(), thrIdx);
			hjPtr->setSearchResult(hjPtr->Result2(), thrIdx);
			sendAllHashSet = (joinType == LEFTOUTER);
			sendAllSearchSet = (joinType == RIGHTOUTER);
		} //if set1Size > set2Size ...

	} // if set1Size <=0 . . .
	params->timeset.setTimer(createHashStr);
	hjPtr->createHash(hjPtr->HashSet(thrIdx),
			  hjPtr->HashTable(thrIdx),
			  bucketIdx,
			  sendAllHashSet,
			  hjPtr->HashResult(thrIdx),
			  params->dlTimes, params->die);
	params->timeset.holdTimer(createHashStr);

#ifdef DEBUG
	long hashSetTotal = 0;
	long searchSetTotal = 0;
	for (uint j=0;j<hjPtr->HashSet(thrIdx)->bucketCount();j++)
		hashSetTotal+=hjPtr->HashSet(thrIdx)->bucketCount(); // are bucketDL

	for (uint j=0;j<hjPtr->HashSet(thrIdx)->bucketCount();j++)
		searchSetTotal+=hjPtr->SearchSet(thrIdx)->size(j); // can be any datalist

	cout << "\t\tJoinByBucket() thr " << dec << thrIdx
		<< " bkt " << bucketIdx
		<< " hashSize " << hashSetTotal
		<< " searchSize " << searchSetTotal << endl;
#endif

	bool more;
	e_t e;
	e_t e2;
	const uint64_t InvalidRID = static_cast<uint64_t>(-1);
	int iter = hjPtr->SearchSet(thrIdx)->getIterator(bucketIdx);

	ZonedDL* zdl1 = dynamic_cast<ZonedDL*>(hjPtr->SearchResult(thrIdx));
	ZonedDL* zdl2 = dynamic_cast<ZonedDL*>(hjPtr->HashResult(thrIdx));
	vector <e_t> vec1;
	vector <e_t> vec2;

	std::pair<typename HashJoin<e_t>::hashIter_t, typename HashJoin<e_t>::hashIter_t> hashItPair;
	typename HashJoin<e_t>::hashIter_t hashIt;
	typename HashJoin<e_t>::hash_t* ht = hjPtr->HashTable(thrIdx);
	params->timeset.setTimer(hashJoinStr);
	for (more = hjPtr->SearchSet(thrIdx)->next(bucketIdx, iter, &e);
		more && !(*params->die);
		more = hjPtr->SearchSet(thrIdx)->next(bucketIdx, iter, &e) )
	{

		// If sendAllSearchSet=true, keep all of the search set.  If this is
		// a right outer, we are dealing with a join such as
		// col1 = col2 (+)
		// where col1 is the SearchSet and col2 is the HashSet.  We want to include
		// all of col1 in this case regardless of whether there is a matching col2.
		if (sendAllSearchSet)
		{
		    if (zdl1)
		    {
		        vec1.push_back(e);
		        if (vec1.size() >= ZDL_VEC_SIZE){
			    params->timeset.setTimer(insertResultsStr);
		            hjPtr->SearchResult(thrIdx)->insert(vec1);
		            vec1.clear();
			    params->timeset.holdTimer(insertResultsStr);
		        }
		    }
		    else
			    hjPtr->SearchResult(thrIdx)->insert(e);
		}

		hashIt = ht->find(e.second);

		if(hashIt != ht->end())
		{
#ifdef DEBUG
			if(hjPtr->SearchResult(thrIdx)->OID() >= 3000)
			cout << "JoinByBucket() SearchResult add " << bucketIdx
				<< " [" << e.first << "][" << e.second << "]" << endl;
			uint a=0;
			e_t b=e_t();
#endif

			// If sendAllSearchSet=false, we already added the search result
			// before the if condition above.
			if (!sendAllSearchSet)
			{
				if (zdl1)
				{
					vec1.push_back(e);
					if (vec1.size() >= ZDL_VEC_SIZE)
					{
						params->timeset.setTimer(insertResultsStr);
						hjPtr->SearchResult(thrIdx)->insert(vec1);
						vec1.clear();
						params->timeset.holdTimer(insertResultsStr);
					}
				}
				else
					hjPtr->SearchResult(thrIdx)->insert(e);
			}

			// If sendAllHashSet=false, add the hash results to the output datalist.
			// If it is a left outer join then we already added all of the right side rows
			// in the bucket in the createHash call earlier in this function.
			if (!sendAllHashSet)
			{

				// If the matching pair has it's RID set to invalid, it's already been encountered,
				// so no reason to add it to the output datalist or keep searching for more matching values.
				if(hashIt->second != InvalidRID)
				{

					// If the matching pair has it's RID set to invalid, it's already been encountered,
					hashItPair = ht->equal_range(e.second);
					for(hashIt = hashItPair.first; hashIt != hashItPair.second; hashIt++)
					{
						e2.first = hashIt->second;
						e2.second = e.second;

						if (zdl2)
						{
							vec2.push_back(e2);
							if (vec2.size() >= ZDL_VEC_SIZE)
							{
								params->timeset.setTimer(insertResultsStr);
								hjPtr->HashResult(thrIdx)->insert(vec2);
								vec2.clear();
								params->timeset.holdTimer(insertResultsStr);
							}
						}
						else
							hjPtr->HashResult(thrIdx)->insert(e2);

#ifdef DEBUG
						a++;
						b=v.second;
#endif

						// Set the RID to invalid rid now that it's been matched and added to the output datalist.
						// This will keep us from duplicating it if it is matched again.
						hashIt->second = InvalidRID;

					}

#ifdef DEBUG
					cout << "\t\tadded " << b << " " << a << " times" << endl << endl;
#endif
				}

			}

		} //  if hashIt != hashIt->end()

	} // for ( hjPtr...
	params->timeset.holdTimer(hashJoinStr);

	params->timeset.setTimer(insertLastResultsStr);
	if (vec1.size() != 0) {
	    hjPtr->SearchResult(thrIdx)->insert(vec1);
	    vec1.clear();
	}
	if (vec2.size() != 0) {
	    hjPtr->HashResult(thrIdx)->insert(vec2);
	    vec2.clear();
	}
	params->timeset.holdTimer(insertLastResultsStr);

	// typename HashJoin<e_t>::hash_t* ht = hjPtr->HashTable(thrIdx);
	ht->clear();

	} // for (bucketIdx...
}  // try
// We don't have to call JSA.endOfInput() for this exception, because
// the parent thread takes care of that in performThreadedJoin().
catch (const logging::LargeDataListExcept& ex)
{
	ostringstream errMsg;
	if (typeid(e_t) == typeid(StringElementType)) {
		errMsg << "HashJoinByBucket_thr<String>: caught LDL error: " <<
			ex.what();
		hjPtr->status(logging::stringHashJoinStepLargeDataListFileErr);
	}
	else {
		errMsg << "HashJoinByBucket_thr: caught LDL error: " << ex.what();
		hjPtr->status(logging::largeHashJoinLargeDataListFileErr);
	}
	cerr << errMsg.str() << endl;
	catchHandler(errMsg.str(),hjPtr->sessionId());
}
catch (const exception& ex)
{
	ostringstream errMsg;
	if (typeid(e_t) == typeid(StringElementType)) {
		errMsg << "HashJoinByBucket_thr<String>: caught: " << ex.what();
		hjPtr->status(logging::stringHashJoinStepErr);
	}
	else {
		errMsg << "HashJoinByBucket_thr: caught: " << ex.what();
		hjPtr->status(logging::largeHashJoinErr);
	}
	cerr << errMsg.str() << endl;
	catchHandler(errMsg.str(),hjPtr->sessionId());
}
catch (...)
{
	ostringstream errMsg;
	if (typeid(e_t) == typeid(StringElementType)) {
		errMsg << "HashJoinByBucket_thr<String>: caught unknown exception: ";
		hjPtr->status(logging::stringHashJoinStepErr);
	}
	else {
		errMsg << "HashJoinByBucket_thr: caught unknown exception";
		hjPtr->status(logging::largeHashJoinErr);
	}
	cerr << errMsg.str() << endl;
	catchHandler(errMsg.str(),hjPtr->sessionId());
}

	return NULL;
} // HashJoinByBucket_thr

LargeHashJoin::LargeHashJoin(JoinType joinType,
			uint32_t sessionId,
			uint32_t txnId,
			uint32_t statementId,
			   ResourceManager& rm ):
	fSessionId(sessionId), fTxnId(txnId),
	fStepId(0), fStatementId(statementId), fTableOID1(0), fTableOID2(0),
	fJoinType(joinType), fRm(rm), fAlias1(), fAlias2()
{
// 	fConfig = config::Config::makeConfig();
// 	fJoinType = joinType;
}

LargeHashJoin::~LargeHashJoin()
{
	if (traceOn())
	{
		for (uint64_t i = 0; i < fInputJobStepAssociation.outSize(); i++)
			logDiskIoInfo(fStepId, fInputJobStepAssociation.outAt(i));
		for (uint64_t i = 0; i < fOutputJobStepAssociation.outSize(); i++)
			logDiskIoInfo(fStepId, fOutputJobStepAssociation.outAt(i));
	}
}

void LargeHashJoin::join()
{
    runner->join();
}

void LargeHashJoin::run()
{
	if (traceOn())
	{
		syslogStartStep(16,               // exemgr subsystem
			std::string("LargeHashJoin")); // step name
	}

    runner.reset(new boost::thread(HJRunner(this)));
}

void LargeHashJoin::unblockDatalists(uint16_t status)
{
	fOutputJobStepAssociation.status(status);
	fOutputJobStepAssociation.outAt(0)->dataList()->endOfInput();
	fOutputJobStepAssociation.outAt(1)->dataList()->endOfInput();
}

void LargeHashJoin::errorLogging(const string& msg) const
{
	ostringstream errMsg;
	errMsg << "Step " << stepId() << "; " << msg;
	cerr   << errMsg.str() << endl;
	catchHandler( errMsg.str(), sessionId() );
}

void LargeHashJoin::doHashJoin()
{
	string val;

	idbassert(fInputJobStepAssociation.outSize() >= 2);
	idbassert(fOutputJobStepAssociation.outSize() >= 2);
	BucketDataList* Ap = 0;
	BucketDataList* Bp = 0;
	BucketDataList* tAp = 0;
	BucketDataList* tBp = 0;
	DataList_t* inDL1 = 0;
	DataList_t* inDL2 = 0;
	inDL1 = fInputJobStepAssociation.outAt(0)->dataList();
	inDL2 = fInputJobStepAssociation.outAt(1)->dataList();
	idbassert(inDL1);
	idbassert(inDL2);

	HashJoin<ElementType>* hj = 0;
	double createWorkTime     = 0;
	double hashWorkTime       = 0;
	double insertWorkTime     = 0;
	DataList_t* resultA       = fOutputJobStepAssociation.outAt(0)->dataList();
	DataList_t* resultB       = fOutputJobStepAssociation.outAt(1)->dataList();

	if (0 < fInputJobStepAssociation.status())
	{
		unblockDatalists(fInputJobStepAssociation.status());
	}
	else
	{

	string currentAction("preparing join");
	try
	{
	//If we're given BucketDL's, use them
	if (typeid(*inDL1) == typeid(BucketDataList))
	{

		if (typeid(*inDL2) != typeid(BucketDataList))
		{
			throw logic_error("LargeHashJoin::run: expected either 0 or 2 BucketDL's!");
		}
		Ap = dynamic_cast<BucketDataList*>(inDL1);
		Bp = dynamic_cast<BucketDataList*>(inDL2);
	}
	else
	{
		throw logic_error("HashJoin will take only BucketDLs as inputs");
		int maxBuckets = fRm.getHjMaxBuckets();
		joblist::ridtype_t maxElems = fRm.getHjMaxElems();

		BucketDataList* tAp = new BucketDataList(maxBuckets, 1, maxElems, fRm);
		BucketDataList* tBp = new BucketDataList(maxBuckets, 1, maxElems, fRm);
		tAp->setHashMode(1);
		tBp->setHashMode(1);

		ElementType element;
		int id;

		id = inDL1->getIterator();

		while (inDL1->next(id, &element))
		{
			tAp->insert(element);
		}
		tAp->endOfInput();

		id = inDL2->getIterator();

		while (inDL2->next(id, &element))
		{
			tBp->insert(element);
		}
		tBp->endOfInput();

		Ap = tAp;
		Bp = tBp;
	}

	unsigned numThreads = fRm.getHjNumThreads();

	BDLWrapper< ElementType > setA(Ap);
	BDLWrapper< ElementType > setB(Bp);

	hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, fJoinType, &dlTimes, fOutputJobStepAssociation.statusPtr(), sessionId(), &die);
	if (fTableOID2 >= 3000)
	{
		ostringstream logStr2;
   		logStr2 << "LargeHashJoin::run: ses:" << fSessionId <<
			" st:" << fStepId <<
			" input sizes: " << setA.size() << "/" << setB.size() << endl;
		cout << logStr2.str();
	}

	currentAction = "performing join";

	if (fTableOID2 >= 3000)
	{
		dlTimes.setFirstReadTime();
		dlTimes.setEndOfInputTime( dlTimes.FirstReadTime() );
	}
	hj->performJoin(numThreads);

	} // try
	catch (const logging::LargeDataListExcept& ex)
	{
		ostringstream errMsg;
		errMsg << __FILE__ << " doHashJoin: " <<
			currentAction << ", caught LDL error: " << ex.what();
		errorLogging(errMsg.str());
		unblockDatalists(logging::largeHashJoinLargeDataListFileErr);
	}
	catch (const exception& ex)
	{
		ostringstream errMsg;
		errMsg << __FILE__ << " doHashJoin: " <<
			currentAction << ", caught: " << ex.what();
		errorLogging(errMsg.str());
		unblockDatalists(logging::largeHashJoinErr);
	}
	catch (...)
	{
		ostringstream errMsg;
		errMsg << __FILE__ << " doHashJoin: " <<
			currentAction << ", caught unknown exception";
		errorLogging(errMsg.str());
		unblockDatalists(logging::largeHashJoinErr);
	}

	if (hj)
	{
		//..hashWorkTime is the time to perform the hashjoin excluding the
		//  the output insertion time.  insertWorkTime is the sum or total
		//  of both insert times.  The end result is that createWorkTime +
		//  hashWorkTime + insertWorkTime roughly equates to the total work
		//  time.
		createWorkTime  = hj->getTimeSet()->totalTime(createHashStr);
		hashWorkTime    = hj->getTimeSet()->totalTime(hashJoinStr) -
		                  hj->getTimeSet()->totalTime(insertResultsStr);
		insertWorkTime  = hj->getTimeSet()->totalTime(insertResultsStr) +
		                  hj->getTimeSet()->totalTime(insertLastResultsStr);
	}

	} // (fInputJobStepAssociation.status() == 0)

	if (fTableOID2 >= 3000 && traceOn())
	{
		time_t finTime = time(0);
		char finTimeString[50];
		ctime_r(&finTime, finTimeString);
		finTimeString[strlen(finTimeString)-1 ] = '\0';

		ostringstream logStr;
		logStr << "ses:" << fSessionId << " st: " << fStepId <<
			" finished at " << finTimeString <<
			"; 1st read " << dlTimes.FirstReadTimeString() <<
			"; EOI " << dlTimes.EndOfInputTimeString() << endl
		    << "\tLargeHashJoin::run: output sizes: "
			<< resultA->totalSize() << "/" << resultB->totalSize()
			<< " run time: " <<
			   JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime())
			<< fixed << setprecision(2)
			<< "s\n\tTotal work times: create hash: " << createWorkTime
			<< "s, hash join: " << hashWorkTime
			<< "s, insert results: " << insertWorkTime << "s\n"
			<< "\tJob completion status " << fInputJobStepAssociation.status() << endl;
		logEnd(logStr.str().c_str());

		syslogProcessingTimes(16, // exemgr subsystem
			dlTimes.FirstReadTime(),      // use join start time for first read time
			dlTimes.EndOfInputTime(),        // use join end   time for last  read time
			dlTimes.FirstReadTime(),      // use join start time for first write time
			dlTimes.EndOfInputTime());       // use join end   time for last  write time
		syslogEndStep(16, // exemgr subsystem
			0,            // no blocked datalist input  to report
			0);           // no blocked datalist output to report
	}

	delete hj;
	delete tAp;
	delete tBp;
}

const string LargeHashJoin::toString() const
{
	ostringstream oss;
	CalpontSystemCatalog::OID oid1 = 0;
	CalpontSystemCatalog::OID oid2 = 0;
	DataList_t* dl1;
	DataList_t* dl2;
	size_t idlsz;

	idlsz = fInputJobStepAssociation.outSize();
	idbassert(idlsz == 2);
	dl1 = fInputJobStepAssociation.outAt(0)->dataList();
	if (dl1) oid1 = dl1->OID();
	dl2 = fInputJobStepAssociation.outAt(1)->dataList();
	if (dl2) oid2 = dl2->OID();
	oss << "LargeHashJoin    ses:" << fSessionId << " st:" << fStepId;
	oss << omitOidInDL;
	oss << " in  tb/col1:" << fTableOID1 << "/" << oid1;
	oss << " " << fInputJobStepAssociation.outAt(0);
	oss << " in  tb/col2:" << fTableOID2 << "/" << oid2;
	oss << " " << fInputJobStepAssociation.outAt(1);

	idlsz = fOutputJobStepAssociation.outSize();
	idbassert(idlsz == 2);
	dl1 = fOutputJobStepAssociation.outAt(0)->dataList();
	if (dl1) oid1 = dl1->OID();
	dl2 = fOutputJobStepAssociation.outAt(1)->dataList();
	if (dl2) oid2 = dl2->OID();
	oss << endl << "                    ";
	oss << " out tb/col1:" << fTableOID1 << "/" << oid1;
	oss << " " << fOutputJobStepAssociation.outAt(0);
	oss << " out tb/col2:" << fTableOID2 << "/" << oid2;
	oss << " " << fOutputJobStepAssociation.outAt(1) << endl;

	return oss.str();
}

StringHashJoinStep::StringHashJoinStep(JoinType joinType,
		uint32_t sessionId,
		uint32_t txnId,
		uint32_t statementId,
		ResourceManager& rm):
	LargeHashJoin(joinType, sessionId, txnId, statementId, rm)
{
}

StringHashJoinStep::~StringHashJoinStep()
{
}

void StringHashJoinStep::run()
{
	if (traceOn())
	{
		syslogStartStep(16,                     // exemgr subsystem
			std::string("StringHashJoinStep")); // step name
	}

    runner.reset(new boost::thread(StringHJRunner(this)));
}

void StringHashJoinStep::doStringHashJoin()
{
	string val;

	idbassert(fInputJobStepAssociation.outSize() >= 2);
	idbassert(fOutputJobStepAssociation.outSize() >= 2);
	DataList<StringElementType>* inDL1 = fInputJobStepAssociation.outAt(0)->stringDataList();
	DataList<StringElementType>* inDL2 = fInputJobStepAssociation.outAt(1)->stringDataList();
	idbassert(inDL1);
	idbassert(inDL2);

	BucketDL<StringElementType>* Ap = 0;
	BucketDL<StringElementType>* Bp = 0;
	BucketDL<StringElementType>* tAp = 0;
	BucketDL<StringElementType>* tBp = 0;

	HashJoin<StringElementType>* hj = 0;
	double createWorkTime     = 0;
	double hashWorkTime       = 0;
	double insertWorkTime     = 0;
	DataList_t* resultA       = fOutputJobStepAssociation.outAt(0)->dataList();
	DataList_t* resultB       = fOutputJobStepAssociation.outAt(1)->dataList();
	struct timeval start_time; gettimeofday(&start_time, 0);
	struct timeval end_time   = start_time;
	ZonedDL* bdl1             = 0;
	ZonedDL* bdl2             = 0;

	// result from hashjoinstep is expected to be BandedDataList
	// but the HashJoin<StringElementType> returns StringDataList
	// also, the null is reported as "_CpNuLl_" by pDictionStep
	// create two StringDataList for the intermediate result BDL
    // @bug 721. use zdl.
	StringZonedDL* dlA = new StringZonedDL(1, fRm);
	dlA->setMultipleProducers(true);
	StringZonedDL* dlB = new StringZonedDL(1, fRm);
	dlB->setMultipleProducers(true);

	if (0 < fInputJobStepAssociation.status() )
	{
		unblockDatalists(fInputJobStepAssociation.status());
	}
	else
	{

	string currentAction("preparing join");
	try
	{
	//If we're given BucketDL's, use them
	if (typeid(*inDL1) == typeid(BucketDL<StringElementType>))
	{
		if (typeid(*inDL2) != typeid(BucketDL<StringElementType>))
		{
			throw logic_error("StringHashJoinStep::run: expected either 0 or 2 BucketDL's!");
		}
		Ap = dynamic_cast<BucketDL<StringElementType>*>(inDL1);
		Bp = dynamic_cast<BucketDL<StringElementType>*>(inDL2);
	}
	else
	{
		int maxBuckets = fRm.getHjMaxBuckets();
		joblist::ridtype_t maxElems = fRm.getHjMaxElems();

// 		int maxBuckets=4;
// 		joblist::ridtype_t maxElems=1024*8;
// 		val = fConfig->getConfig("HashJoin", "MaxBuckets");  // same as HashJoin
//    		if (val.size() > 0)
// 			maxBuckets = static_cast<int>(config::Config::fromText(val));
// 		if (maxBuckets <=0)
// 			maxBuckets=4;
// 		val = fConfig->getConfig("HashJoin", "MaxElems");    // same as HashJoin
// 		if (val.size() >0)
// 			maxElems = config::Config::uFromText(val);
// 		if (maxElems<=0)
// 			maxElems=1024*8;

		tAp = new BucketDL<StringElementType>(maxBuckets, 1, maxElems, fRm);
		tBp = new BucketDL<StringElementType>(maxBuckets, 1, maxElems, fRm);
		tAp->setHashMode(1);
		tBp->setHashMode(1);

		StringElementType element;
		int id = inDL1->getIterator();

		while (inDL1->next(id, &element))
		{
			tAp->insert(element);
		}
		tAp->endOfInput();

		id = inDL2->getIterator();

		while (inDL2->next(id, &element))
		{
			tBp->insert(element);
		}
		tBp->endOfInput();

		Ap = tAp;
		Bp = tBp;
	}

	unsigned numThreads = fRm.getHjNumThreads();
// 	unsigned numThreads = 0;
// 	val = fConfig->getConfig("HashJoin", "NumThreads");
// 	if (val.size() > 0)
// 		numThreads = static_cast<unsigned>(config::Config::uFromText(val));
// 	if (numThreads <= 0)
// 		numThreads = 4;

	BDLWrapper< StringElementType > setA(Ap);
	BDLWrapper< StringElementType > setB(Bp);

	HashJoin<StringElementType>* hj =
	  new HashJoin<StringElementType>(setA, setB, dlA, dlB, fJoinType, &dlTimes, fOutputJobStepAssociation.statusPtr(), sessionId(), &die);

	if ((dlA == NULL) || (dlB == NULL) || (hj == NULL))
	{
		ostringstream oss;
		oss << "StringHashJoinStep::run() null pointer from new -- ";
		oss << "StringDataList A(0x" << hex << (ptrdiff_t)dlA << "), B(0x"
			<< (ptrdiff_t)dlB << "), HashJoin hj(0x" << (ptrdiff_t)hj << ")";
		throw(runtime_error(oss.str().c_str()));
	}

	// leave this in
	if (fTableOID2 >= 3000)
	{
		ostringstream logStr2;
		logStr2 << "StringHashJoinStep::run: ses:" << fSessionId <<
			" st:" << fStepId <<
			" input sizes: " << setA.size() << "/" << setB.size() << endl;
		cout << logStr2.str();
	}

	currentAction = "performing join";

	if (fTableOID2 >= 3000)
	{
		dlTimes.setFirstReadTime();
		dlTimes.setEndOfInputTime( dlTimes.FirstReadTime() );
	}
	hj->performJoin(numThreads);

	currentAction = "after join";

	// convert from StringElementType to ElementType by grabbing the rid
	// take _CpNuLl_ out of the result
	StringElementType se;
	ElementType       e;
	int id = dlA->getIterator();

	bdl1 = dynamic_cast<ZonedDL*>(resultA);
	bdl2 = dynamic_cast<ZonedDL*>(resultB);
	vector <ElementType> v;
	v.reserve(ZDL_VEC_SIZE);
	if (bdl1)
	{
	    while (dlA->next(id, &se))
    	{
    		if (se.second != CPNULLSTRMARK)
    		{
    			e.first = se.first;
    			v.push_back(e);
    			if (v.size() >= ZDL_VEC_SIZE)
    			{
    			    resultA->insert(v);
    			    v.clear();
    			}
    		}
    	}
    	if (v.size() > 0)
    	    resultA->insert(v);

    	resultA->endOfInput();
	}

	else
	{
    	while (dlA->next(id, &se))
    	{
    		if (se.second != CPNULLSTRMARK)
    		{
    			e.first = se.first;
    			resultA->insert(e);
    		}
    	}
    	resultA->endOfInput();
    }

	id = dlB->getIterator();

	if (bdl2)
	{
	    v.clear();
	    while (dlB->next(id, &se))
    	{
    		if (se.second != CPNULLSTRMARK)
    		{
    			e.first = se.first;
    			v.push_back(e);
    			if (v.size() >= ZDL_VEC_SIZE)
    			{
    			    resultB->insert(v);
    			    v.clear();
    			}
    		}
    	}
    	if (v.size() > 0)
    	    resultB->insert(v);

    	resultB->endOfInput();
	}
	else
	{
    	while (dlB->next(id, &se))
    	{
    		if (se.second != CPNULLSTRMARK)
    		{
    			e.first = se.first;
    			resultB->insert(e);
    		}
    	}
    	resultB->endOfInput();
    }
	} // try
	catch (const logging::LargeDataListExcept& ex)
	{
		ostringstream errMsg;
		errMsg << __FILE__ << " doStringHashJoin: " <<
			currentAction << ", caught LDL error: " << ex.what();
		errorLogging(errMsg.str());
		unblockDatalists(logging::stringHashJoinStepLargeDataListFileErr);
		dlA->endOfInput();
		dlB->endOfInput();
	}
	catch (const exception& ex)
	{
		ostringstream errMsg;
		errMsg << __FILE__ << " doStringHashJoin: " <<
			currentAction << ", caught: " << ex.what();
		errorLogging(errMsg.str());
		unblockDatalists(logging::stringHashJoinStepErr);
		dlA->endOfInput();
		dlB->endOfInput();
	}
	catch (...)
	{
		ostringstream errMsg;
		errMsg << __FILE__ << " doStringHashJoin: " <<
			currentAction << ", caught unknown exception";
		errorLogging(errMsg.str());
		unblockDatalists(logging::stringHashJoinStepErr);
		dlA->endOfInput();
		dlB->endOfInput();
	}
	gettimeofday(&end_time, 0);
	if (fTableOID2 >= 3000) dlTimes.setEndOfInputTime();

	if (hj)
	{
		//..hashWorkTime is the time to perform the hashjoin excluding the
		//  the output insertion time.  insertWorkTime is the sum or total
		//  of both insert times.  The end result is that createWorkTime +
		//  hashWorkTime + insertWorkTime roughly equates to the total work
		//  time.
		createWorkTime  = hj->getTimeSet()->totalTime(createHashStr);
		hashWorkTime    = hj->getTimeSet()->totalTime(hashJoinStr) -
		                  hj->getTimeSet()->totalTime(insertResultsStr);
		insertWorkTime  = hj->getTimeSet()->totalTime(insertResultsStr) +
		                  hj->getTimeSet()->totalTime(insertLastResultsStr);
	}

	} // (fInputJobStepAssociation.status() == 0)

	if (fTableOID2 >= 3000 && traceOn())
	{
		time_t finTime = time(0);
		char finTimeString[50];
		ctime_r(&finTime, finTimeString);
		finTimeString[strlen(finTimeString)-1 ] = '\0';

		ostringstream logStr;
		logStr << "ses:" << fSessionId << " st: " << fStepId <<
			" finished at " << finTimeString <<
			"; 1st read " << dlTimes.FirstReadTimeString() <<
			"; EOI " << dlTimes.EndOfInputTimeString() << endl
			<< "\tStringHashJoinStep::run: output sizes: "
			<< dlA->totalSize() << "/" << dlB->totalSize() << " [";
		if (bdl1 && bdl2)
			logStr << bdl1->totalSize() << "/" << bdl2->totalSize();
		logStr << "] run time: " <<
			   JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime())
			<< fixed << setprecision(2)
			<< "s\n\tTotal work times: create hash: " << createWorkTime
			<< "s, hash join: " << hashWorkTime
			<< "s, insert results: " << insertWorkTime << "s\n"
			<< "\tJob completion status " << fInputJobStepAssociation.status() << endl;
		logEnd(logStr.str().c_str());

		syslogProcessingTimes(16, // exemgr subsystem
			start_time,   // use join start time for first read time
			end_time,     // use join end   time for last  read time
			start_time,   // use join start time for first write time
			end_time);    // use join end   time for last  write time
		syslogEndStep(16, // exemgr subsystem
			0,            // no blocked datalist input  to report
			0);           // no blocked datalist output to report
	}

	delete hj;
	delete tAp;
	delete tBp;
	delete dlA;
	delete dlB;
}

const string StringHashJoinStep::toString() const
{
	ostringstream oss;
	CalpontSystemCatalog::OID oid1 = 0;
	CalpontSystemCatalog::OID oid2 = 0;

	size_t idlsz = fInputJobStepAssociation.outSize();
	idbassert(idlsz == 2);
	DataList<StringElementType>* dl1 = fInputJobStepAssociation.outAt(0)->stringDataList();
	if (dl1) oid1 = dl1->OID();
	DataList<StringElementType>* dl2 = fInputJobStepAssociation.outAt(1)->stringDataList();
	if (dl2) oid2 = dl2->OID();

	oss << "StringHashJoinStep    ses:" << fSessionId << " st:" << fStepId;
	oss << omitOidInDL;
	oss << " in  tb/col1:" << fTableOID1 << "/" << oid1;
	oss << " " << fInputJobStepAssociation.outAt(0);
	oss << " in  tb/col2:" << fTableOID2 << "/" << oid2;
	oss << " " << fInputJobStepAssociation.outAt(1);

	idlsz = fOutputJobStepAssociation.outSize();
	idbassert(idlsz == 2);
	DataList_t *dl3 = fOutputJobStepAssociation.outAt(0)->dataList();
	if (dl3) oid1 = dl3->OID();
	DataList_t *dl4 = fOutputJobStepAssociation.outAt(1)->dataList();
	if (dl4) oid2 = dl4->OID();

	oss << endl << "                          ";
	oss << " out tb/col1:" << fTableOID1 << "/" << oid1;
	oss << " " << fOutputJobStepAssociation.outAt(0);
	oss << " out tb/col2:" << fTableOID2 << "/" << oid2;
	oss << " " << fOutputJobStepAssociation.outAt(1);

	return oss.str();
}

}

