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

#ifndef LARGEHASHJOIN_H
#define LARGEHASHJOIN_H
// $Id: largehashjoin.h 9210 2013-01-21 14:10:42Z rdempsey $
//
// C++ Implementation: hashjoin
//
// Author: Jason Rodriguez <jrodriguez@calpont.com>
//
// Description: 
//
//
//

/** @file */

#include <sstream>
#include <vector>
#include <list>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif

#include <boost/thread.hpp>
#include <boost/scoped_array.hpp>

#ifndef _HASHFIX_
#define _HASHFIX_
#ifndef __LP64__
// This is needed for /usr/include/c++/4.1.1/tr1/functional on 32-bit compiles
// tr1_hashtable_define_trivial_hash(long long unsigned int);
#include "jl_logger.h"
namespace std
{
namespace tr1
{
  template<>
    struct hash<long long unsigned int>
    : public std::unary_function<long long unsigned int, std::size_t>
    {
      std::size_t
      operator()(long long unsigned int val) const
      { return static_cast<std::size_t>(val); }
    };
}
}
#endif
#endif

#include <sys/time.h>
#include <cassert>
#include <boost/scoped_array.hpp>

#include "elementtype.h"
#include "bdlwrapper.h"
#include "joblisttypes.h"
#include "hasher.h"
#include "timestamp.h"
#include "timeset.h"
#include "jl_logger.h"

#ifdef PROFILE
extern void timespec_sub(const struct timespec &tv1,
						const struct timespec &tv2,
						struct timespec &diff);
#endif

namespace joblist {

const string createHashStr("create hash");
const string hashJoinStr("hash join");
const string insertResultsStr("insert results");
const string insertLastResultsStr("insert last results");

template <typename element_t>
void* HashJoinByBucket_thr(void* arg);

/** @brief class HjHasher
 *
 */
template<typename element_t>
class HjHasher
{
private:
	utils::Hasher hasher;

public:
	uint32_t operator()(const typename element_t::second_type& v) const
	{
		return hasher((const char *) &v, sizeof(typename element_t::second_type));
	}
};

// template specialization for string
template<>
class HjHasher<StringElementType>
{
private:
	utils::Hasher hasher;
public:
	uint32_t operator()(const std::string& v) const
	{
		return hasher(v.c_str(), (uint) v.length());
	}
};

/** @brief class HashJoin
 *
 */
template <typename element_t>
class HashJoin
{

public:
	typedef std::list<element_t> hashList_t;
	typedef typename std::list<element_t>::iterator hashListIter_t;

	// @Bug 867 - Changed the unordered_map to an unordered_multimap and changed the value to be RIDs rather than a list of ElementType to reduce
	// memory utilization and to increase the performance of loading the map.
	// typedef std::tr1::unordered_map<typename element_t::second_type, std::list<element_t>, HjHasher<element_t> > hash_t;
	typedef typename std::tr1::unordered_multimap<typename element_t::second_type, typename element_t::first_type> hash_t;
	typedef typename std::tr1::unordered_multimap<typename element_t::second_type, typename element_t::first_type>::iterator hashIter_t;
	typedef typename std::tr1::unordered_multimap<typename element_t::second_type, typename element_t::first_type>::value_type hashPair_t;

	// allow each thread to have its own pointers
	struct control_struct {
		hash_t hashTbl;
		BucketDL<element_t>* searchSet;
		BucketDL<element_t>* hashSet;
		DataList<element_t>* searchResult;
		DataList<element_t>* hashResult;
	};

	boost::scoped_array<struct control_struct> controls; //TODO: needs to be >= HJ threads from Calpont.xml
	typedef struct thrParams_struct {
		HashJoin<element_t>* hjptr;
		uint32_t startBucket;
		uint32_t numBuckets;
		uint32_t thrIdx;
		TimeSet timeset;		
		JSTimeStamp dlTimes;
		volatile bool *die;
	} thrParams_t;

	HashJoin(joblist::BDLWrapper<element_t>& set1,
			joblist::BDLWrapper<element_t>& set2,
			joblist::DataList<element_t>* result1,
			joblist::DataList<element_t>* result2,
			JoinType joinType,
			JSTimeStamp *dlTimes,
			const SErrorInfo& status,
			uint32_t sessionId,
			volatile bool *die);
	
	HashJoin();
	HashJoin(const HashJoin& hj);
	JoinType getJoinType() { return fJoinType; }
    virtual ~HashJoin();
	virtual int performJoin(const uint thrCount=1);
	virtual int performThreadedJoin(const uint32_t numThreads);
	
	joblist::BDLWrapper<element_t>* Set1() { return & fSet1;}
	joblist::BDLWrapper<element_t>* Set2() { return & fSet2;}
	joblist::DataList<element_t>* Result1() { return fResult1;}
	joblist::DataList<element_t>* Result2() { return fResult2;}

	void setSearchSet(BucketDL<element_t> *bdl, const uint32_t idx) {controls[idx].searchSet=bdl;}
	void setHashSet(BucketDL<element_t> *bdl, const uint32_t idx) {controls[idx].hashSet=bdl;}

	BucketDL<element_t>* SearchSet(const uint32_t idx) {return controls[idx].searchSet;}
	BucketDL<element_t>* HashSet(const uint32_t idx) {return controls[idx].hashSet;}

	void setSearchResult(DataList<element_t> *bdl, const uint32_t idx) {controls[idx].searchResult=bdl;}
	void setHashResult(DataList<element_t> *bdl, const uint32_t idx) {controls[idx].hashResult=bdl;}

	joblist::DataList<element_t>* SearchResult(const uint32_t idx) { return controls[idx].searchResult; }
	joblist::DataList<element_t>* HashResult(const uint32_t idx) { return controls[idx].hashResult; }

	hash_t* HashTable(const uint64_t i ) { return &(controls[i].hashTbl); }

	void createHash(BucketDL<element_t>* bdlptr,
			hash_t* destHashTbl,
 			const uint32_t idx,
			bool populateResult,  // true if bdlptr is opposite an outer join
			joblist::DataList<element_t>* result,  // populated if populateResult true
			JSTimeStamp& thrDlTimes, volatile bool *die);
	void init();
	TimeSet* getTimeSet() { return &fTimeSet; }
	uint16_t status() const { return fStatus->errCode; }
	void  status(uint16_t s)  { fStatus->errCode; }
	uint32_t sessionId()    { return fSessionId; }

private:
	// input sets
	joblist::BDLWrapper<element_t> fSet1;
	joblist::BDLWrapper<element_t> fSet2;

	// result sets
	joblist::DataList<element_t>* fResult1;
	joblist::DataList<element_t>* fResult2;

	// convenience pointers 
	BucketDL<element_t> *fSearchSet;
	BucketDL<element_t> *fHashSet;
	joblist::DataList<element_t>* fSearchResult;
	joblist::DataList<element_t>* fHashResult;

	JoinType fJoinType;
	JSTimeStamp *dlTimes;
	TimeSet  fTimeSet;
	SErrorInfo fStatus;
	uint32_t fSessionId;
	volatile bool *die;
};

template <typename element_t>
HashJoin<element_t>::HashJoin()
{
}

template <typename element_t>
HashJoin<element_t>::HashJoin(joblist::BDLWrapper<element_t>& set1,
				joblist::BDLWrapper<element_t>& set2,
				joblist::DataList<element_t>* result1,
				joblist::DataList<element_t>* result2,
				JoinType joinType,
				JSTimeStamp *dlt,	
				const SErrorInfo& status,
				uint32_t sessionId, volatile bool *d) :
					fTimeSet(),fStatus(status), fSessionId(sessionId)
{
	fSet1=set1;
	fSet2=set2;
	fResult1=result1;
	fResult2=result2;
	fSearchResult=NULL;
	fHashResult=NULL;
	fJoinType = joinType;
	die = d;
	init();
	dlTimes = dlt;
}

template <typename element_t>
void HashJoin<element_t>::init()
{
	controls.reset(new control_struct[32]);
	for(int idx=0; idx<32; idx++) {
		HashTable(idx)->clear();
		setSearchSet(NULL, idx);
		setHashSet(NULL, idx);
		setSearchResult(NULL, idx);
		setHashResult(NULL, idx);
	}
}

template <typename element_t>
HashJoin<element_t>::~HashJoin()
{
	controls.reset();
}

template <typename element_t>
int HashJoin<element_t>::performThreadedJoin(const uint32_t numThreads)
{
	//boost::thread thrArr[numThreads];
	boost::scoped_array<boost::thread> thrArr(new boost::thread[numThreads]);
	//typename HashJoin<element_t>::thrParams_t params[numThreads];
	boost::scoped_array<typename HashJoin<element_t>::thrParams_t> params(new typename HashJoin<element_t>::thrParams_t[numThreads]);
	uint32_t maxThreads=numThreads;
	int realCnt=0;
	uint bucketsPerThr=0;
	uint totalBuckets=0;

	// TODO: maybe this should throw an exception
	if (maxThreads<=0 || maxThreads >32) {
		maxThreads=1;
#ifdef DEBUG
		cerr << "HashJoin: invalid requested thread value n=" << numThreads << endl;
#endif
	}
	// s/b equal buckets so check Set1() buckets
	if (Set1()->bucketCount()<maxThreads) {
		maxThreads=Set1()->bucketCount();
	}

	bucketsPerThr=(uint)(Set1()->bucketCount()/maxThreads);
	uint idx=0;
	for(idx=0; idx<maxThreads && totalBuckets<Set1()->bucketCount(); idx++)
	{		
		params[idx].hjptr = this;
		params[idx].startBucket=totalBuckets;
		params[idx].numBuckets=bucketsPerThr;
		params[idx].thrIdx=idx;
		params[idx].die = die;
		totalBuckets+=bucketsPerThr;

		if ( (totalBuckets+bucketsPerThr) > Set1()->bucketCount() )
			bucketsPerThr=Set1()->bucketCount()-totalBuckets;

#ifdef DEBUG
	cout << "thr i " << idx
		<< " [" << params[idx].startBucket << ", " << params[idx].numBuckets << "] "
		<< totalBuckets << " " << (int)bucketsPerThr << endl;
#endif

	} // for(

	// add the remaining buckets to the last thread
	// instead of adding a thread
	if (totalBuckets<Set1()->bucketCount()) {
		idx--;
		params[idx].numBuckets+=bucketsPerThr;

	}

#ifdef DEBUG
	cout << "thr i " << idx << " [" << params[idx].startBucket << ", "
		<< params[idx].numBuckets << "] "
		<< totalBuckets << " " << (int)bucketsPerThr << "-" << endl;
#endif

	try {
    for (idx=0; idx<maxThreads; idx++) {

		int ret = 0;
        //ret = pthread_create(&thrArr[idx], NULL, HashJoinByBucket_thr<element_t>, &params[idx]);
		boost::thread t(HashJoinByBucket_thr<element_t>, &params[idx]);
		thrArr[idx].swap(t);
        if (ret!=0)
            throw logic_error("HashJoin: pthread_create failure");
        else {
            realCnt++;
		}

#ifdef DEBUG
		cout << "Started thr " << idx << endl;
#endif
    }

    idbassert((unsigned)realCnt == maxThreads);
	} // try
	catch (std::exception& e) {
		std::ostringstream errMsg;
		if (typeid(element_t) == typeid(StringElementType)) {
			errMsg << "performThreadedJoin<String>: caught: " << e.what();
			*fStatus = logging::stringHashJoinStepErr;
		}
		else {
			errMsg << "performThreadedJoin: caught: " << e.what();
			*fStatus = logging::largeHashJoinErr;
		}
		std::cerr << errMsg.str() << std::endl;
		catchHandler(errMsg.str(),sessionId());
	}
	catch (...) {
		std::ostringstream errMsg;
		if (typeid(element_t) == typeid(StringElementType)) {
			errMsg << "performThreadedJoin<String>: unknown exception";
			*fStatus = logging::stringHashJoinStepErr;
		}
		else {
			errMsg << "performThreadedJoin: caught: unknown exception";
			*fStatus = logging::largeHashJoinErr;
		}
		std::cerr << errMsg.str() << std::endl;
		catchHandler(errMsg.str(),sessionId());
	}

    for (int idx=0; idx<realCnt; idx++) {
		thrArr[idx].join(); //pthread_join(thrArr[idx], NULL);
#ifdef DEBUG
		cout << "HJ " << hex << this << dec << ": Joining thr " << idx << endl;
#endif
	}

	Result1()->endOfInput();
	Result2()->endOfInput();

	if (realCnt > 0)
		dlTimes->setFirstReadTime( params[0].dlTimes.FirstReadTime() );
	dlTimes->setEndOfInputTime();
	for(int i=0; i< realCnt; i++)
	{
		fTimeSet += params[i].timeset;

		// Select earliest read time as overall firstReadTime
		if ( params[i].dlTimes.FirstReadTime().tv_sec < dlTimes->FirstReadTime().tv_sec )
			dlTimes->setFirstReadTime( params[i].dlTimes.FirstReadTime() );
	} 
	controls.reset();

    return realCnt;
}

// defaults to 1 thread
template <typename element_t>
int HashJoin<element_t>::performJoin(const uint thrCount)
{
	return performThreadedJoin(thrCount);
}

// create a hash table from the elements in the bucketDL at bdlptr[bucketNum]
//
template <typename element_t>
void HashJoin<element_t>::createHash(BucketDL<element_t> *srcBucketDL,
					hash_t* destHashTbl,
					const uint32_t bucketNum,
					bool populateResult,
					joblist::DataList<element_t>* result,
					JSTimeStamp& thrDlTimes, volatile bool *die)
{
	bool more;
	element_t e;
	element_t temp;
#ifdef DEBUG
	int idx=0;
#endif
#ifdef PROFILE
	timespec ts1, ts2, diff;
	clock_gettime(CLOCK_REALTIME, &ts1);
#endif
	uint bucketIter = srcBucketDL->getIterator((int)bucketNum);
    // @bug 828. catch hashjoin starting time
	more = srcBucketDL->next(bucketNum, bucketIter, &e);
	if (thrDlTimes.FirstReadTime().tv_sec == 0) {
		thrDlTimes.setFirstReadTime();
	}
	for (; more & !(*die); more = srcBucketDL->next(bucketNum, bucketIter, &e)) {

#ifdef DEBUG
		cout << "createHash() bkt " << bucketNum
				<< " idx " << idx << " find(" << e.second << ")" << endl;
#endif
		// If the bucket dl is the other side of an outer join, we want to go ahead and populate the output
		// data list with every row.  For example, if the where clause is:
		// where colA (+) = colB
		// and the passed bucket datalist contains colb, we will go ahead and populate the output datalist here
		// because all of colB should be returned regardless of whether there is a matching colA.
		if(populateResult)
		{		    
			result->insert(e);
		}

		try {
			// std::list<element_t> tmp(1,e);
			destHashTbl->insert(std::pair<typename element_t::second_type, typename element_t::first_type >
				(e.second, e.first));
		} catch (exception& exc) {
			std::ostringstream errMsg;
			errMsg << "Exception in createHash() " << exc.what();
			std::cerr << errMsg.str() << endl;
			catchHandler(errMsg.str(),sessionId());
			throw;		// rethrow
		} catch (...) {
			std::string errMsg ("Unknown exception in createHash()");
			std::cerr << errMsg << endl;
			catchHandler(errMsg,sessionId());
			throw;		// rethrow
		}

	} // for (more...

#ifdef DEBUG
	cout << "createHash() bkt " << bucketNum << " complete" << endl;
#endif

#ifdef PROFILE
	clock_gettime(CLOCK_REALTIME, &ts2);
	timespec_sub(ts1, ts2, diff);
	std::cout << "Time to create hash for bucket pair "
		<< bucketNum << ": "
		<< diff.tv_sec << "s "
		<< diff.tv_nsec << "ns"
		<< std::endl; 
#endif
} // createHash


} //namespace

#endif
