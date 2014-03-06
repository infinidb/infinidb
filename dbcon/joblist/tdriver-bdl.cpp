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
*   $Id: tdriver-bdl.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <iostream>
#include <vector>

#include <pthread.h>
#include <time.h>
#include <sys/time.h>

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "configcpp.h"
#include "bucketdl.h"
#include "elementtype.h"
#include "stopwatch.cpp"

using namespace std;
using namespace joblist;

Stopwatch timer;
//dmc-uint64_t count = 20000000/*1000000*/;
uint64_t count = 8000000;

const int NUM_PRODUCERS = 8;
const int NUM_CONSUMERS = 4;
const int NUM_BUCKETS   = 128;
const int MAX_ELEMENTS  = 8192;
uint64_t  readCounts[NUM_CONSUMERS];

struct ThreadParms
{
	void*        bdl;
	unsigned int threadNumber;
	uint64_t     count; // for producer this is the number of elements
                        //   each producer is to write
                        // for consumer this is the number of buckets
                        //   each consumer is to read
};

//------------------------------------------------------------------------------

void timespec_sub(const struct timespec &tv1, const struct timespec &tv2,
	struct timespec &diff) 
{
	if (tv2.tv_nsec < tv1.tv_nsec)
	{
		diff.tv_sec = tv2.tv_sec - tv1.tv_sec - 1;
		diff.tv_nsec = tv1.tv_nsec - tv2.tv_nsec;
	}
	else
	{
		diff.tv_sec = tv2.tv_sec - tv1.tv_sec;
		diff.tv_nsec = tv2.tv_nsec - tv1.tv_nsec;
	}
}

//------------------------------------------------------------------------------
// thread callbacks and utilities for BDL testing
//------------------------------------------------------------------------------
template<typename ElemT>
void *BDL_producer(void *arg)
{
	ThreadParms* pThreadParms = reinterpret_cast<ThreadParms*>(arg);
	uint64_t tNum             = pThreadParms->threadNumber;
	BucketDL<ElemT> *bdl      = reinterpret_cast<BucketDL<ElemT> *>
								(pThreadParms->bdl);
	uint64_t elementCount     = pThreadParms->count;

	for (uint64_t i = tNum;
		i < (elementCount*NUM_PRODUCERS)-((NUM_PRODUCERS-1)-tNum);
		i = i+NUM_PRODUCERS)
	{
		bdl->insert(ElemT(i,i));
    }

	return NULL;
}

//
// Can't use BDL_producer() to test RID only element type(s)
// because we need an ElemT constructor that takes a single argument.
//
template<typename ElemT>
void *BDL_producer_ridonly(void *arg)
{
	ThreadParms* pThreadParms = reinterpret_cast<ThreadParms*>(arg);
	uint64_t tNum             = pThreadParms->threadNumber;
	BucketDL<ElemT> *bdl      = reinterpret_cast<BucketDL<ElemT> *>
								(pThreadParms->bdl);
	uint64_t elementCount     = pThreadParms->count;

	for (uint64_t i = tNum;
		i < (elementCount*NUM_PRODUCERS)-((NUM_PRODUCERS-1)-tNum);
		i = i+NUM_PRODUCERS)
	{
		bdl->insert(ElemT(i));
    }

	return NULL;
}

template<typename ElemT>
void *BDL_consumer(void *arg)
{
	ThreadParms* pThreadParms = reinterpret_cast<ThreadParms*>(arg);
	uint64_t tNum             = pThreadParms->threadNumber;
	BucketDL<ElemT> *bdl      = reinterpret_cast<BucketDL<ElemT> *>
								(pThreadParms->bdl);
	uint64_t numBucketsToConsume = pThreadParms->count;

	bool nextRet;
	ElemT e;

	for (uint64_t i=0; i<numBucketsToConsume; i++)
	{
		uint64_t bucketIndex = (tNum * numBucketsToConsume) + i;
		uint64_t id          = bdl->getIterator( bucketIndex );

		nextRet = bdl->next(bucketIndex, id, &e);
		while (nextRet) 
		{
			::readCounts[tNum]++;
			nextRet = bdl->next(bucketIndex, id, &e);
		}
	}

	return NULL;
}

template<typename ElemT>
void BDL_printFileStats(void *arg)
{
	BucketDL<ElemT> *bdl = reinterpret_cast<BucketDL<ElemT> *>(arg);
	uint64_t nFiles;
	uint64_t nBytes;
	bdl->totalFileCounts(nFiles, nBytes);
	uint32_t size1st = bdl->getDiskElemSize1st();
	uint32_t size2nd = bdl->getDiskElemSize2nd();
	cout << "NumberOfFiles: " << nFiles << endl;
	cout << "NumberOfBytes: " << nBytes << endl;
	cout << "ElementSize:   " << size1st << "/" << size2nd << endl;
}

//------------------------------------------------------------------------------
// TestDriver class derived from CppUnit
//------------------------------------------------------------------------------
			
class DataListDriver : public CppUnit::TestFixture
{

CPPUNIT_TEST_SUITE(DataListDriver);
//CPPUNIT_TEST(configure);
//CPPUNIT_TEST(load_save);
CPPUNIT_TEST(BDL_bench_uncompressed);
CPPUNIT_TEST(BDL_bench_compressed_32_32);
CPPUNIT_TEST(BDL_bench_compressed_64_32);
CPPUNIT_TEST(BDL_bench_compressed_32_64);
CPPUNIT_TEST(BDL_bench_ridonly_uncompressed);
CPPUNIT_TEST(BDL_bench_ridonly_compressed_32);
CPPUNIT_TEST_SUITE_END();

private:
public:
	//--------------------------------------------------------------------------
	// setup method run prior to each unit test
	//--------------------------------------------------------------------------
	void setUp()
	{
		for (int i=0; i<NUM_CONSUMERS; i++)
		{
			::readCounts[i] = 0;
		}
	}

	//--------------------------------------------------------------------------
	// validate results from a unit test
	//--------------------------------------------------------------------------
	void validateResults()
	{
		uint64_t totalElementsRead = 0;
		uint64_t totalElementsExpected = ::count*NUM_PRODUCERS;
		for (int i=0; i<NUM_CONSUMERS; i++)
		{
			totalElementsRead += ::readCounts[i];
			cout << "consumer " << i << " read " << ::readCounts[i] <<
				" elements " << endl;
		}
		CPPUNIT_ASSERT(totalElementsRead == totalElementsExpected);
	}

	//--------------------------------------------------------------------------
	// BDL benchmark functions
	//--------------------------------------------------------------------------
	void BDL_bench(char* testDesc, bool compress,
		uint32_t size1st, uint32_t size2nd)
	{
	    typedef ElementType Element;

		string produceTag ("bdl-produce_");
		string eofInputTag("bdl-endofinput_");
		string consumeTag ("bdl-consume_");
		produceTag  += testDesc;
		eofInputTag += testDesc;
		consumeTag  += testDesc;

		uint32_t i;
		uint32_t numOfProducers = NUM_PRODUCERS;
		uint32_t numOfConsumers = NUM_CONSUMERS;
		uint32_t numBuckets     = NUM_BUCKETS;
		uint32_t maxElements    = MAX_ELEMENTS;
		BucketDL<Element> bdl(numBuckets, 1, maxElements); 
	    bdl.setMultipleProducers(true);		
		bdl.setElementMode(1); // RID_VALUE
		if ( compress )
			bdl.setDiskElemSize ( size1st, size2nd );
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		ThreadParms producerThreadParms[NUM_PRODUCERS];
		ThreadParms consumerThreadParms[NUM_CONSUMERS];
		struct timespec ts1, ts2, diff;
        
        timer.start(produceTag);
		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		{
			producerThreadParms[i].bdl          = &bdl;
			producerThreadParms[i].threadNumber = i;
			producerThreadParms[i].count        = ::count;
		    pthread_create(&producer[i], NULL,
				BDL_producer<Element>, &producerThreadParms[i]);
		}
		for (i = 0; i < numOfConsumers; i++)
		{
			consumerThreadParms[i].bdl          = &bdl;
			consumerThreadParms[i].threadNumber = i;
			consumerThreadParms[i].count        = NUM_BUCKETS / NUM_CONSUMERS;
		    pthread_create(&consumer[i], NULL,
				BDL_consumer<Element>, &consumerThreadParms[i]);	    		
		}
		for (i = 0; i < numOfProducers; i++)
		    pthread_join(producer[i], NULL);
        timer.stop(produceTag);

        timer.start(eofInputTag);
		bdl.endOfInput();
		timer.stop(eofInputTag);

		timer.start(consumeTag);
		for (i = 0; i < numOfConsumers; i++)
		    pthread_join(consumer[i], NULL);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timer.stop(consumeTag);

		timer.finish();
		timespec_sub(ts1, ts2, diff);
		cout << "compress state: " << (compress?"on":"off") << endl;
		if (compress)
			cout << "size 1st/2nd:   " << size1st << "/" << size2nd << endl;
        cout << "# of Producers: " << numOfProducers << endl;
        cout << "# of Consumers: " << numOfConsumers << endl;
		cout << "BDL_bench_" << testDesc <<
			": producer & consumer passed " << 
			bdl.totalSize() << " elements in " << diff.tv_sec << "s " <<
			diff.tv_nsec << "ns" << endl;

		BDL_printFileStats<Element>(&bdl);
		validateResults();
		cout << endl;
	}

	void BDL_bench_uncompressed()
	{
		BDL_bench( "uncompressed", false, 0, 0 );
	}

	void BDL_bench_compressed_32_32()
	{
		BDL_bench( "compressed_32_32", true, 4, 4 );
	}

	void BDL_bench_compressed_64_32()
	{
		BDL_bench( "compressed_64_32", true, 8, 4 );
	}

	void BDL_bench_compressed_32_64()
	{
		BDL_bench( "compressed_32_64", true, 4, 8 );
	}
	
	void BDL_bench_ridonly(char* testDesc, bool compress,
		uint32_t size1st)
	{
	    typedef RIDElementType Element;
		uint32_t size2nd = 0;

		string produceTag ("bdl-produce_ridonly");
		string eofInputTag("bdl-endofinput_ridonly");
		string consumeTag ("bdl-consume_ridonly");
		produceTag  += testDesc;
		eofInputTag += testDesc;
		consumeTag  += testDesc;

		uint32_t i;
		uint32_t numOfProducers = NUM_PRODUCERS;
		uint32_t numOfConsumers = NUM_CONSUMERS;
		uint32_t numBuckets     = NUM_BUCKETS;
		uint32_t maxElements    = MAX_ELEMENTS;
		BucketDL<Element> bdl(numBuckets, 1, maxElements); 
	    bdl.setMultipleProducers(true);		
		if ( compress )
			bdl.setDiskElemSize ( size1st, size2nd );
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		ThreadParms producerThreadParms[NUM_PRODUCERS];
		ThreadParms consumerThreadParms[NUM_CONSUMERS];
		struct timespec ts1, ts2, diff;
        
        timer.start(produceTag);
		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		{
			producerThreadParms[i].bdl          = &bdl;
			producerThreadParms[i].threadNumber = i;
			producerThreadParms[i].count        = ::count;
		    pthread_create(&producer[i], NULL,
				BDL_producer_ridonly<Element>, &producerThreadParms[i]);
		}
		for (i = 0; i < numOfConsumers; i++)
		{
			consumerThreadParms[i].bdl          = &bdl;
			consumerThreadParms[i].threadNumber = i;
			consumerThreadParms[i].count        = NUM_BUCKETS / NUM_CONSUMERS;
		    pthread_create(&consumer[i], NULL,
				BDL_consumer<Element>, &consumerThreadParms[i]);	    		
		}
		for (i = 0; i < numOfProducers; i++)
		    pthread_join(producer[i], NULL);
        timer.stop(produceTag);

        timer.start(eofInputTag);
		bdl.endOfInput();
		timer.stop(eofInputTag);

		timer.start(consumeTag);
		for (i = 0; i < numOfConsumers; i++)
		    pthread_join(consumer[i], NULL);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timer.stop(consumeTag);

		timer.finish();
		timespec_sub(ts1, ts2, diff);
		cout << "compress state: " << (compress?"on":"off") << endl;
		if (compress)
			cout << "size 1st/2nd:   " << size1st << "/" << size2nd << endl;
        cout << "# of Producers: " << numOfProducers << endl;
        cout << "# of Consumers: " << numOfConsumers << endl;
		cout << "BDL_bench_ridonly_" << testDesc <<
			": producer & consumer passed " << 
			bdl.totalSize() << " elements in " << diff.tv_sec << "s " <<
			diff.tv_nsec << "ns" << endl;

		BDL_printFileStats<Element>(&bdl);
		validateResults();
		cout << endl;
	}

	void BDL_bench_ridonly_uncompressed()
	{
		BDL_bench_ridonly( "uncompressed", false, 0 );
	}

	void BDL_bench_ridonly_compressed_32()
	{
		BDL_bench_ridonly( "compressed_32", true, 4 );
	}
	
	//--------------------------------------------------------------------------
	// test the saving and loading of a bdl file
	//--------------------------------------------------------------------------
	void load_save()
	{
	    typedef ElementType Element;

    	vector <Element> v;
    	for (uint32_t i = 0; i < ::count; i++)
    	    v.push_back(Element(i,i));
    	
    	vector<Element> v1;
    	vector<Element> v2;
    	vector<Element> v3;
    	
    	// save
    	ofstream f1;
    	ifstream f;
    	string filename = "bdl.txt";
    	uint64_t cnt = v.size();
    	f1.open(filename.c_str(), std::ios::binary);
    	f1.write((char *) &cnt, sizeof(cnt));
    	f1.write((char *) (v.begin().operator->()), sizeof(Element) * cnt);
    	f.close();
    	
    	// load
    	v1.push_back(Element(3,4));
    	f.open(filename.c_str(), std::ios::binary);
    	timer.start("read");
    	v1.resize(v1.size()+::count);
    	f.read((char *) ((v1.begin()+1).operator->()), cnt * sizeof(Element));
    	cout << v1.size() << endl;
        timer.stop("read");
        cout << "E1: " << v1[0].first << endl;
        f.close();
        
        f.open(filename.c_str(), std::ios::binary);
        timer.start("assign");
        v2.assign(std::istream_iterator<Element>(f), 
        				std::istream_iterator<Element>());
        cout << v2.size() << endl;    				    
        timer.stop("assign");
        f.close();
        
        f.open(filename.c_str(), std::ios::binary);
        timer.start("insert");
        v3.insert(v3.end(), std::istream_iterator<Element>(f), 
        				std::istream_iterator<Element>());
        cout << v3.size() << endl;    				    
        timer.stop("insert");
        f.close();
        timer.finish();
    }

	//--------------------------------------------------------------------------
	// test the reading of bdl configuration parameters
	//--------------------------------------------------------------------------
    void configure()
    {
	    timer.start("configure");
        config::Config *config = config::Config::makeConfig();
        std::string strBuckets;
        std::string strElems;
        std::string strThreads;
	    uint64_t maxBuckets;
	    uint64_t maxElems;
	    uint64_t numThreads;

	    strBuckets = config->getConfig("HashJoin", "MaxBuckets");
		CPPUNIT_ASSERT(strBuckets.size() > 0);
	    maxBuckets = config::Config::uFromText(strBuckets);

	    strElems = config->getConfig("HashJoin", "MaxElems");
		CPPUNIT_ASSERT(strElems.size() > 0);
	   	maxElems = config::Config::uFromText(strElems);

	    strThreads = config->getConfig("HashJoin", "NumThreads");
		CPPUNIT_ASSERT(strThreads.size() > 0);
	    numThreads = config::Config::uFromText(strThreads);

		cout << "config: hashjoin:maxbuckets - " << maxBuckets << endl;
		cout << "config: hashjoin:maxelems   - " << maxElems   << endl;
		cout << "config: hashjoin:numThreads - " << numThreads << endl;

		timer.stop("configure");
		timer.finish();
    }
}; 

CPPUNIT_TEST_SUITE_REGISTRATION(DataListDriver);

//------------------------------------------------------------------------------
// main entry point
//------------------------------------------------------------------------------
int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


