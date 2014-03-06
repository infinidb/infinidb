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
*   $Id: tdriver-zdl.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <iostream>
#include <sstream>
#include <fstream>
#include <list>
#include <vector>

#include <pthread.h>
#include <time.h>
#include <sys/time.h>

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "fifo.h"
#include "wsdl.h"
#include "constantdatalist.h"
#include "bucketdl.h"
#include "bandeddl.h"
#include "elementtype.h"
#include "zdl.h"
#include "stopwatch.cpp"

// #undef CPPUNIT_ASSERT
// #define CPPUNIT_ASSERT(x)

using namespace std;
using namespace joblist;

Stopwatch timer;
//dmc-uint64_t count = 20000000/*1000000*/;
uint64_t count1Set   = 2000000;
uint64_t countMulSet = 8000000;
int maxElements = 16000000/*50000*/;   //max elements in memory at once for the benchmarks
int id_sw = -1;
pthread_mutex_t writeLock;

const int NUM_PRODUCERS = 8;
const int NUM_CONSUMERS = 4;
uint64_t  readCounts[NUM_CONSUMERS];

struct ThreadParms
{
	void*        zdl;
	unsigned int threadNumber;
	uint64_t     count; // for producer this is the number of elements
                        //   each producer is to write
                        // for consumer this is not currently used
};

//------------------------------------------------------------------------------

void timespec_sub(const struct timespec &tv1, const struct timespec &tv2,
	struct timespec &diff) 
{
		if (tv2.tv_nsec < tv1.tv_nsec) {
			diff.tv_sec = tv2.tv_sec - tv1.tv_sec - 1;
			diff.tv_nsec = tv1.tv_nsec - tv2.tv_nsec;
		}
		else {
			diff.tv_sec = tv2.tv_sec - tv1.tv_sec;
			diff.tv_nsec = tv2.tv_nsec - tv1.tv_nsec;
		}
}

//------------------------------------------------------------------------------
// thread callbacks for SWSDL testing
//------------------------------------------------------------------------------
template<typename ElemT>
void *SWSDL_producer_1set_seq(void *arg)
{
    pthread_mutex_lock(&writeLock);
    id_sw++;
    pthread_mutex_unlock(&writeLock);
	SWSDL<ElemT> *sw = reinterpret_cast<SWSDL<ElemT> *>(arg);
	for (uint64_t i = id_sw;
		i < (::count1Set*NUM_PRODUCERS)-((NUM_PRODUCERS-1)-id_sw);
		i = i+NUM_PRODUCERS)
	{
		sw->insert(ElemT(i,i));
	}
	return NULL;
}

template<typename ElemT>
void *SWSDL_producer_mulSet_seq(void *arg)
{
    pthread_mutex_lock(&writeLock);
    id_sw++;
    pthread_mutex_unlock(&writeLock);
	SWSDL<ElemT> *sw = reinterpret_cast<SWSDL<ElemT> *>(arg);
	for (uint64_t i = id_sw;
		i < (::countMulSet*NUM_PRODUCERS)-((NUM_PRODUCERS-1)-id_sw);
		i = i+NUM_PRODUCERS)
	{
		sw->insert(ElemT(i,i));
	}

	return NULL;
}

template<typename ElemT>
void *SWSDL_producer_1set_rand(void *arg)
{
	SWSDL<ElemT> *sw = reinterpret_cast<SWSDL<ElemT> *>(arg);
	for (uint64_t i = 0; i < ::count1Set; i++)
	{
		sw->insert(ElemT((uint64_t)(::count1Set * rand()/(RAND_MAX + 1.0)), i));
	}

	return NULL;
}

template<typename ElemT>
void *SWSDL_producer_mulSet_rand(void *arg)
{
	SWSDL<ElemT> *sw = reinterpret_cast<SWSDL<ElemT> *>(arg);
	for (uint64_t i = 0; i < ::countMulSet; i++)
	{
		sw->insert(ElemT((uint64_t)(countMulSet * rand()/(RAND_MAX + 1.0)), i));
	}

	return NULL;
}

template<typename ElemT>
void *SWSDL_consumer(void *arg)
{
	SWSDL<ElemT> *sw = reinterpret_cast<SWSDL<ElemT> *>(arg);
	uint64_t id;
	bool nextRet;
	ElemT e;

	id = sw->getIterator();
	nextRet = sw->next(id, &e);
	while (nextRet) 
		nextRet = sw->next(id, &e);
	return NULL;
}

//------------------------------------------------------------------------------
// thread callbacks and utilities for ZDL testing
//------------------------------------------------------------------------------
template<typename ElemT>
void *ZDL_producer_1set_seq(void *arg)
{
	ThreadParms* pThreadParms = reinterpret_cast<ThreadParms*>(arg);
	uint64_t tNum             = pThreadParms->threadNumber;
	ZDL<ElemT> *zdl           = reinterpret_cast<ZDL<ElemT> *>
                                (pThreadParms->zdl);
	uint64_t elementCount     = pThreadParms->count;

	for (uint64_t i = tNum;
		i < (elementCount*NUM_PRODUCERS)-((NUM_PRODUCERS-1)-tNum);
		i = i+NUM_PRODUCERS)
	{
		zdl->insert(ElemT(i,i));
    }

	return NULL;
}

template<typename ElemT>
void *ZDL_producer_mulSet_seq(void *arg)
{
	ThreadParms* pThreadParms = reinterpret_cast<ThreadParms*>(arg);
	uint64_t tNum             = pThreadParms->threadNumber;
	ZDL<ElemT> *zdl           = reinterpret_cast<ZDL<ElemT> *>
                                (pThreadParms->zdl);
	uint64_t elementCount     = pThreadParms->count;

	for (uint64_t i = tNum;
		i < (elementCount*NUM_PRODUCERS)-((NUM_PRODUCERS-1)-tNum);
		i = i+NUM_PRODUCERS)
	{
		zdl->insert(ElemT(i,i));
    }

	return NULL;
}

//
// Can't use ZDL_producer_mulSet_seq() to test RID only element type(s)
// because we need an ElemT constructor that takes a single argument.
//
template<typename ElemT>
void *ZDL_producer_mulSet_seq_ridonly(void *arg)
{
	ThreadParms* pThreadParms = reinterpret_cast<ThreadParms*>(arg);
	uint64_t tNum             = pThreadParms->threadNumber;
	ZDL<ElemT> *zdl           = reinterpret_cast<ZDL<ElemT> *>
                                (pThreadParms->zdl);
	uint64_t elementCount     = pThreadParms->count;

	for (uint64_t i = tNum;
		i < (elementCount*NUM_PRODUCERS)-((NUM_PRODUCERS-1)-tNum);
		i = i+NUM_PRODUCERS)
	{
		zdl->insert(ElemT(i));
    }

	return NULL;
}

template<typename ElemT>
void *ZDL_producer_1set_rand(void *arg)
{
	ThreadParms* pThreadParms = reinterpret_cast<ThreadParms*>(arg);
	//uint64_t tNum           = pThreadParms->threadNumber;
	ZDL<ElemT> *zdl           = reinterpret_cast<ZDL<ElemT> *>
                                (pThreadParms->zdl);
	uint64_t elementCount     = pThreadParms->count;

	for (uint64_t i = 0; i < elementCount; i++)
	{
		zdl->insert(ElemT((uint64_t)(elementCount*rand()/(RAND_MAX + 1.0)),i));
    }

	return NULL;
}

template<typename ElemT>
void *ZDL_producer_mulSet_rand(void *arg)
{
	ThreadParms* pThreadParms = reinterpret_cast<ThreadParms*>(arg);
	//uint64_t tNum           = pThreadParms->threadNumber;
	ZDL<ElemT> *zdl           = reinterpret_cast<ZDL<ElemT> *>
                                (pThreadParms->zdl);
	uint64_t elementCount     = pThreadParms->count;

	for (uint64_t i = 0; i < elementCount; i++)
	{
		zdl->insert(ElemT((uint64_t)(elementCount * rand()/(RAND_MAX+1.0)),i));
    }

	return NULL;
}

template<typename ElemT>
void *ZDL_consumer(void *arg)
{
	ThreadParms* pThreadParms = reinterpret_cast<ThreadParms*>(arg);
	uint64_t tNum             = pThreadParms->threadNumber;
	ZDL<ElemT> *zdl           = reinterpret_cast<ZDL<ElemT> *>
                                (pThreadParms->zdl);

	uint64_t id;
	bool nextRet;
	ElemT e;

	id = zdl->getIterator();
	nextRet = zdl->next(id, &e);
	while (nextRet) 
	{
		::readCounts[tNum]++;
		nextRet = zdl->next(id, &e);
	}

	return NULL;
}

template<typename ElemT>
void ZDL_printFileStats(void *arg)
{
	ZDL<ElemT> *zdl = reinterpret_cast<ZDL<ElemT> *>(arg);
	uint64_t nFiles;
	uint64_t nBytes;
	zdl->totalFileCounts(nFiles, nBytes);
	uint32_t size1st = zdl->getDiskElemSize1st();
	uint32_t size2nd = zdl->getDiskElemSize2nd();
	cout << "NumberOfFiles: " << nFiles << endl;
	cout << "NumberOfBytes: " << nBytes << endl;
	cout << "ElementSize:   " << size1st << "/" << size2nd << endl;
}

//------------------------------------------------------------------------------
// TestDriver class derived from CppUnit
//------------------------------------------------------------------------------
			
class DataListDriver : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(DataListDriver);
//CPPUNIT_TEST(configure);
//CPPUNIT_TEST(load_save);
//CPPUNIT_TEST(SWSDL_bench_1set_seq);
//CPPUNIT_TEST(SWSDL_bench_1set_rand);
//CPPUNIT_TEST(SWSDL_bench_mulSet_seq);
//CPPUNIT_TEST(SWSDL_bench_mulSet_rand);
//CPPUNIT_TEST(ZDL_bench_1set_seq);
//CPPUNIT_TEST(ZDL_bench_1set_rand);
CPPUNIT_TEST(ZDL_bench_mulSet_seq_uncompressed);
CPPUNIT_TEST(ZDL_bench_mulSet_seq_compressed_32_32);
CPPUNIT_TEST(ZDL_bench_mulSet_seq_compressed_64_32);
CPPUNIT_TEST(ZDL_bench_mulSet_seq_compressed_32_64);
//CPPUNIT_TEST(ZDL_bench_mulSet_rand);
CPPUNIT_TEST(ZDL_bench_mulSet_seq_ridonly_uncompressed);
CPPUNIT_TEST(ZDL_bench_mulSet_seq_ridonly_compressed_32);
CPPUNIT_TEST_SUITE_END();

ResourceManager fRm;
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
	void validateResults(uint64_t totalElementsExpected)
	{
		for (int i=0; i<NUM_CONSUMERS; i++)
		{
			cout << "consumer " << i << " read " << ::readCounts[i] <<
				" elements" << endl;
		}
		cout << endl;
		for (int i=0; i<NUM_CONSUMERS; i++)
		{
			CPPUNIT_ASSERT(readCounts[i] == totalElementsExpected);
		}
	}

	//--------------------------------------------------------------------------
	// SWSDL benchmark functions
	//--------------------------------------------------------------------------
	void SWSDL_bench_1set_seq()
	{	   
		typedef ElementType Element;

	    id_sw = 0;
		uint32_t i;
		uint32_t numOfProducers = NUM_PRODUCERS;
		uint32_t numOfConsumers = NUM_CONSUMERS;
		SWSDL<Element> sw(numOfConsumers, fRm);
        sw.setMultipleProducers(true);
		
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		    pthread_create(&producer[i], NULL,
				SWSDL_producer_1set_seq<Element>, &sw);
		    
		for (i = 0; i < numOfConsumers; i++)
		    pthread_create(&consumer[i], NULL,
				SWSDL_consumer<Element>, &sw);

		for (i = 0; i < numOfProducers; i++)
		    pthread_join(producer[i], NULL);

		sw.endOfInput();
		for (i = 0; i < numOfConsumers; i++)
		    pthread_join(consumer[i], NULL);

		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);

        cout << "# of Producers: " << numOfProducers << endl;
        cout << "# of Consumers: " << numOfConsumers << endl;
		cout << "SWSDL_bench_1set_seq: producer & consumer passed " << 
			sw.totalSize() << " elements in " << diff.tv_sec << "s " <<
			diff.tv_nsec << "ns" << endl;
	}
	
	void SWSDL_bench_mulSet_seq()
	{	   
		typedef ElementType Element;

	    id_sw = 0;
		uint32_t i;
		uint32_t numOfProducers = NUM_PRODUCERS;
		uint32_t numOfConsumers = NUM_CONSUMERS;
		SWSDL<Element> sw(numOfConsumers, fRm);
        sw.setMultipleProducers(true);
		
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		struct timespec ts1, ts2, diff;
        
        timer.start("swsdl-produce");
		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		    pthread_create(&producer[i], NULL,
				SWSDL_producer_mulSet_seq<Element>, &sw);
		    
		for (i = 0; i < numOfProducers; i++)
		    pthread_join(producer[i], NULL);
        timer.stop("swsdl-produce");
        timer.start("swsdl-endofinput");
		sw.endOfInput();
		timer.stop("swsdl-endofinput");
		
		//timer.stop("swsdl-produce");
		timer.start("swsdl-consume");
		for (i = 0; i < numOfConsumers; i++)
		    pthread_create(&consumer[i], NULL,
				SWSDL_consumer<Element>, &sw);
		for (i = 0; i < numOfConsumers; i++)
		    pthread_join(consumer[i], NULL);

		clock_gettime(CLOCK_REALTIME, &ts2);
		timer.stop("swsdl-consume");
		timer.finish();
		timespec_sub(ts1, ts2, diff);

        cout << "# of Producers: " << numOfProducers << endl;
        cout << "# of Consumers: " << numOfConsumers << endl;
		cout << "SWSDL_bench_mulSet_seq: producer & consumer passed " << 
			sw.totalSize() << " elements in " << diff.tv_sec << "s " <<
			diff.tv_nsec << "ns" << endl;
	}

    void SWSDL_bench_1set_rand()
	{	   
		typedef ElementType Element;

	    id_sw = 0;
		uint32_t i;
		uint32_t numOfProducers = NUM_PRODUCERS;
		uint32_t numOfConsumers = NUM_CONSUMERS;
		SWSDL<Element> sw(numOfConsumers, ::maxElements, fRm);
        sw.setMultipleProducers(true);
		
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		    pthread_create(&producer[i], NULL,
				SWSDL_producer_1set_rand<Element>, &sw);
		    
		for (i = 0; i < numOfConsumers; i++)
		    pthread_create(&consumer[i], NULL,
				SWSDL_consumer<Element>, &sw);

		for (i = 0; i < numOfProducers; i++)
		    pthread_join(producer[i], NULL);

		sw.endOfInput();
		for (i = 0; i < numOfConsumers; i++)
		    pthread_join(consumer[i], NULL);

		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);

        cout << "# of Producers: " << numOfProducers << endl;
        cout << "# of Consumers: " << numOfConsumers << endl;
		cout << "SWSDL_bench_1set_rand: producer & consumer passed " << 
			sw.totalSize() << " elements in " << diff.tv_sec << "s " <<
			diff.tv_nsec << "ns" << endl;
	}
	
	void SWSDL_bench_mulSet_rand()
	{	   
		typedef ElementType Element;

	    id_sw = 0;
		uint32_t i;
		uint32_t numOfProducers = NUM_PRODUCERS;
		uint32_t numOfConsumers = NUM_CONSUMERS;
		SWSDL<Element> sw(numOfConsumers, ::maxElements, fRm);
        sw.setMultipleProducers(true);
		
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		    pthread_create(&producer[i], NULL,
				SWSDL_producer_mulSet_rand<Element>, &sw);
		    
		for (i = 0; i < numOfConsumers; i++)
		    pthread_create(&consumer[i], NULL,
				SWSDL_consumer<Element>, &sw);

		for (i = 0; i < numOfProducers; i++)
		    pthread_join(producer[i], NULL);

		sw.endOfInput();
		for (i = 0; i < numOfConsumers; i++)
		    pthread_join(consumer[i], NULL);

		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);

        cout << "# of Producers: " << numOfProducers << endl;
        cout << "# of Consumers: " << numOfConsumers << endl;
		cout << "SWSDL_bench_mulSet_rand: producer & consumer passed " << 
			sw.totalSize() << " elements in " << diff.tv_sec << "s " <<
			diff.tv_nsec << "ns" << endl;
	}
 	
	//--------------------------------------------------------------------------
	// ZDL benchmark functions
	//--------------------------------------------------------------------------
 	void ZDL_bench_1set_seq()
	{
		typedef ElementType Element;

		uint32_t i;
		uint32_t numOfProducers = NUM_PRODUCERS;
		uint32_t numOfConsumers = NUM_CONSUMERS;
		ZDL<Element> zdl(numOfConsumers, fRm);
	    zdl.setMultipleProducers(true);		
		zdl.setElementMode(1); // RID_VALUE
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		ThreadParms producerThreadParms[NUM_PRODUCERS];
		ThreadParms consumerThreadParms[NUM_CONSUMERS];
		struct timespec ts1, ts2, diff;
        
        timer.start("zdl-produce_1set_seq");
		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		{
			producerThreadParms[i].zdl          = &zdl;
			producerThreadParms[i].threadNumber = i;
            producerThreadParms[i].count        = ::count1Set;
		    pthread_create(&producer[i], NULL,
				ZDL_producer_1set_seq<Element>, &producerThreadParms[i]);
		}
		for (i = 0; i < numOfProducers; i++)
		    pthread_join(producer[i], NULL);
		zdl.endOfInput();
		timer.stop("zdl-produce_1set_seq");
		
		timer.start("zdl-consume_1set_seq");
		for (i = 0; i < numOfConsumers; i++)
		{
			consumerThreadParms[i].zdl          = &zdl;
			consumerThreadParms[i].threadNumber = i;
            consumerThreadParms[i].count        = 0;
		    pthread_create(&consumer[i], NULL,
				ZDL_consumer<Element>, &consumerThreadParms[i]);
		}
		for (i = 0; i < numOfConsumers; i++)
		    pthread_join(consumer[i], NULL);   
		clock_gettime(CLOCK_REALTIME, &ts2);
        timer.stop("zdl-consume_1set_seq");	

        timer.finish();	
		timespec_sub(ts1, ts2, diff);
        cout << "# of Producers: " << numOfProducers << endl;
        cout << "# of Consumers: " << numOfConsumers << endl;
		cout << "ZDL_bench_1set_seq: producer & consumer passed " << 
			zdl.totalSize() << " elements in " << diff.tv_sec << "s " <<
			diff.tv_nsec << "ns" << endl;

		ZDL_printFileStats<Element>(&zdl);
		validateResults(::count1Set*NUM_PRODUCERS);
	}
	
	void ZDL_bench_mulSet_seq(char* testDesc, bool compress,
		uint32_t size1st, uint32_t size2nd)
	{
	    typedef ElementType Element;

		string produceTag ("zdl-produce_");
		string eofInputTag("zdl-endofinput_");
		string consumeTag ("zdl-consume_");
		produceTag  += testDesc;
		eofInputTag += testDesc;
		consumeTag  += testDesc;

		uint32_t i;
		uint32_t numOfProducers = NUM_PRODUCERS;
		uint32_t numOfConsumers = NUM_CONSUMERS;
		ZDL<Element> zdl(numOfConsumers, fRm); 
	    zdl.setMultipleProducers(true);		
		zdl.setElementMode(1); // RID_VALUE
		if ( compress )
			zdl.setDiskElemSize ( size1st, size2nd );
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		ThreadParms producerThreadParms[NUM_PRODUCERS];
		ThreadParms consumerThreadParms[NUM_CONSUMERS];
		struct timespec ts1, ts2, diff;
        
        timer.start(produceTag);
		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		{
			producerThreadParms[i].zdl          = &zdl;
			producerThreadParms[i].threadNumber = i;
            producerThreadParms[i].count        = ::countMulSet;
		    pthread_create(&producer[i], NULL,
				ZDL_producer_mulSet_seq<Element>, &producerThreadParms[i]);
		}
		for (i = 0; i < numOfConsumers; i++)
		{
			consumerThreadParms[i].zdl          = &zdl;
			consumerThreadParms[i].threadNumber = i;
            consumerThreadParms[i].count        = 0;
		    pthread_create(&consumer[i], NULL,
				ZDL_consumer<Element>, &consumerThreadParms[i]);
		}
		for (i = 0; i < numOfProducers; i++)
		    pthread_join(producer[i], NULL);
        timer.stop(produceTag);

        timer.start(eofInputTag);
		zdl.endOfInput();
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
		cout << "ZDL_bench_mulSet_seq_" << testDesc <<
			": producer & consumer passed " << 
			zdl.totalSize() << " elements in " << diff.tv_sec << "s " <<
			diff.tv_nsec << "ns" << endl;

		ZDL_printFileStats<Element>(&zdl);
		validateResults(::countMulSet*NUM_PRODUCERS);
	}

	void ZDL_bench_mulSet_seq_uncompressed()
	{
		ZDL_bench_mulSet_seq( "uncompressed", false, 0, 0 );
	}

	void ZDL_bench_mulSet_seq_compressed_32_32()
	{
		ZDL_bench_mulSet_seq( "compressed_32_32", true, 4, 4 );
	}

	void ZDL_bench_mulSet_seq_compressed_64_32()
	{
		ZDL_bench_mulSet_seq( "compressed_64_32", true, 8, 4 );
	}

	void ZDL_bench_mulSet_seq_compressed_32_64()
	{
		ZDL_bench_mulSet_seq( "compressed_32_64", true, 4, 8 );
	}
	
	void ZDL_bench_1set_rand()
	{
	    typedef ElementType Element;

		uint32_t i;
		uint32_t numOfProducers = NUM_PRODUCERS;
		uint32_t numOfConsumers = NUM_CONSUMERS;
		ZDL<Element> zdl(numOfConsumers, fRm);
	    zdl.setMultipleProducers(true);		
		zdl.setElementMode(1); // RID_VALUE
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		ThreadParms producerThreadParms[NUM_PRODUCERS];
		ThreadParms consumerThreadParms[NUM_CONSUMERS];
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		{
			producerThreadParms[i].zdl          = &zdl;
			producerThreadParms[i].threadNumber = i;
            producerThreadParms[i].count        = ::count1Set;
		    pthread_create(&producer[i], NULL,
				ZDL_producer_1set_rand<Element>, &producerThreadParms[i]);
		}
		    
		for (i = 0; i < numOfConsumers; i++)
		{
			consumerThreadParms[i].zdl          = &zdl;
			consumerThreadParms[i].threadNumber = i;
            consumerThreadParms[i].count        = 0;
		    pthread_create(&consumer[i], NULL,
				ZDL_consumer<Element>, &consumerThreadParms[i]);
		}

		for (i = 0; i < numOfProducers; i++)
		    pthread_join(producer[i], NULL);

		zdl.endOfInput();
		for (i = 0; i < numOfConsumers; i++)
		    pthread_join(consumer[i], NULL);
			
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
        cout << "# of Producers: " << numOfProducers << endl;
        cout << "# of Consumers: " << numOfConsumers << endl;
		cout << "ZDL_bench_1set_rand: producer & consumer passed " << 
			zdl.totalSize() << " elements in " << diff.tv_sec << "s " <<
			diff.tv_nsec << "ns" << endl;

		ZDL_printFileStats<Element>(&zdl);
		validateResults(::count1Set);
	}
	
	void ZDL_bench_mulSet_rand()
	{
	    typedef ElementType Element;

		uint32_t i;
		uint32_t numOfProducers = NUM_PRODUCERS;
		uint32_t numOfConsumers = NUM_CONSUMERS;
		ZDL<Element> zdl(numOfConsumers, fRm);
	    zdl.setMultipleProducers(true);		
		zdl.setElementMode(1); // RID_VALUE
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		ThreadParms producerThreadParms[NUM_PRODUCERS];
		ThreadParms consumerThreadParms[NUM_CONSUMERS];
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		{
			producerThreadParms[i].zdl          = &zdl;
			producerThreadParms[i].threadNumber = i;
            producerThreadParms[i].count        = ::countMulSet;
		    pthread_create(&producer[i], NULL,
				ZDL_producer_mulSet_rand<Element>, &producerThreadParms[i]);
		}
		    
		for (i = 0; i < numOfConsumers; i++)
		{
			consumerThreadParms[i].zdl          = &zdl;
			consumerThreadParms[i].threadNumber = i;
            consumerThreadParms[i].count        = 0;
		    pthread_create(&consumer[i], NULL,
				ZDL_consumer<Element>, &consumerThreadParms[i]);
		}

		for (i = 0; i < numOfProducers; i++)
		    pthread_join(producer[i], NULL);

		zdl.endOfInput();
		for (i = 0; i < numOfConsumers; i++)
		    pthread_join(consumer[i], NULL);
			
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
        cout << "# of Producers: " << numOfProducers << endl;
        cout << "# of Consumers: " << numOfConsumers << endl;
		cout << "ZDL_bench_mulSet_rand: producer & consumer passed " << 
			zdl.totalSize() << " elements in " << diff.tv_sec << "s " <<
			diff.tv_nsec << "ns" << endl;

		ZDL_printFileStats<Element>(&zdl);
		validateResults(::countMulSet);
	}

	void ZDL_bench_mulSet_seq_ridonly(char* testDesc, bool compress,
		uint32_t size1st)
	{
	    typedef RIDElementType Element;
		uint32_t size2nd = 0;

		string produceTag ("zdl-produce_ridonly");
		string eofInputTag("zdl-endofinput_ridonly");
		string consumeTag ("zdl-consume_ridonly");
		produceTag  += testDesc;
		eofInputTag += testDesc;
		consumeTag  += testDesc;

		uint32_t i;
		uint32_t numOfProducers = NUM_PRODUCERS;
		uint32_t numOfConsumers = NUM_CONSUMERS;
		ZDL<Element> zdl(numOfConsumers, fRm); 
	    zdl.setMultipleProducers(true);		
		if ( compress )
			zdl.setDiskElemSize ( size1st, size2nd );
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		ThreadParms producerThreadParms[NUM_PRODUCERS];
		ThreadParms consumerThreadParms[NUM_CONSUMERS];
		struct timespec ts1, ts2, diff;
        
        timer.start(produceTag);
		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		{
			producerThreadParms[i].zdl          = &zdl;
			producerThreadParms[i].threadNumber = i;
            producerThreadParms[i].count        = ::countMulSet;
		    pthread_create(&producer[i], NULL,
				ZDL_producer_mulSet_seq_ridonly<Element>,
					&producerThreadParms[i]);
		}
		for (i = 0; i < numOfConsumers; i++)
		{
			consumerThreadParms[i].zdl          = &zdl;
			consumerThreadParms[i].threadNumber = i;
            consumerThreadParms[i].count        = 0;
		    pthread_create(&consumer[i], NULL,
				ZDL_consumer<Element>, &consumerThreadParms[i]);
		}
		for (i = 0; i < numOfProducers; i++)
		    pthread_join(producer[i], NULL);
        timer.stop(produceTag);

        timer.start(eofInputTag);
		zdl.endOfInput();
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
		cout << "ZDL_bench_mulSet_seq_ridonly_" << testDesc <<
			": producer & consumer passed " << 
			zdl.totalSize() << " elements in " << diff.tv_sec << "s " <<
			diff.tv_nsec << "ns" << endl;

		ZDL_printFileStats<Element>(&zdl);
		validateResults(::countMulSet*NUM_PRODUCERS);
	}

	void ZDL_bench_mulSet_seq_ridonly_uncompressed()
	{
		ZDL_bench_mulSet_seq_ridonly( "uncompressed", false, 0 );
	}

	void ZDL_bench_mulSet_seq_ridonly_compressed_32()
	{
		ZDL_bench_mulSet_seq_ridonly( "compressed_32", true, 4 );
	}
	
	//--------------------------------------------------------------------------
	// test the saving and loading of a zdl file
	//--------------------------------------------------------------------------
	void load_save(){
	    typedef ElementType Element;

    	vector <Element> v;
    	for (uint32_t i = 0; i < ::count1Set*8; i++)
    	    v.push_back(Element(i,i));
    	
    	vector<Element> v1;
    	vector<Element> v2;
    	vector<Element> v3;
    	
    	// save
    	ofstream f1;
    	ifstream f;
    	string filename = "zdl.txt";
    	uint64_t ctn = v.size();
    	f1.open(filename.c_str(), std::ios::binary);
    	f1.write((char *) &ctn, sizeof(ctn));
    	f1.write((char *) (v.begin().operator->()), sizeof(Element) * ctn);
    	f.close();
    	
    	// load
    	v1.push_back(Element(3,4));
    	f.open(filename.c_str(), std::ios::binary);
    	timer.start("read");
    	v1.resize(v1.size()+::count1Set*8);
    	f.read((char *) ((v1.begin()+1).operator->()), ctn * sizeof(Element));
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
	// test the reading of zdl configuration parameters
	//--------------------------------------------------------------------------
    void configure()
    {
        config::Config *config = config::Config::makeConfig();
        std::string strVal;
	    strVal = config->getConfig("ZDL", "MaxMemConsumption");
	    uint64_t maxMemConsumption;
	    timer.start("configure");
	    for (int i = 0; i < 20; i++){	    
    	    if (strVal.size() > 0){
    		    maxMemConsumption = config::Config::uFromText(strVal);
    		    if ((maxMemConsumption - 1) & maxMemConsumption)
       			    throw std::runtime_error("ZDL: maxMemConsumption "
						"should be a power of 2.");
    	    }
    	    else
    		    maxMemConsumption = 1000000;
		}
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
  pthread_mutex_init(&writeLock, NULL);
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


