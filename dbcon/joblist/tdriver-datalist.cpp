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

// $Id: tdriver-datalist.cpp 9210 2013-01-21 14:10:42Z rdempsey $
#include <iostream>
#include <sstream>
#include <list>

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
#include "stopwatch.cpp"

// #undef CPPUNIT_ASSERT
// #define CPPUNIT_ASSERT(x)

using namespace std;
using namespace joblist;
Stopwatch timer;

struct DLElement { 
	public: 
		int i; 
		bool operator<(const DLElement &c) const {
			return (i < c.i);
		}
		const char * getHashString(int mode, int *len) const {
			*len = 4;
			return (char *) &i;
		}
};

istream& operator>>(istream &is, DLElement &dl) { is.read((char *)&dl.i, sizeof(int)); return is; }
ostream& operator<<(ostream &os, const DLElement &dl) { os.write((char *)&dl.i, sizeof(int)); return os; }

int64_t count = 150000000/*1000000*/;
int maxElements = 32000000/*50000*/;   //max elements in memory at once for the benchmarks
int buckets = 16;  // # of buckets to use in bucketDL tests.
#define MAXINT64 (uint64_t) 0xffffffffffffffffLL

/* Note, this is like what we'll use for banding operations */
class BandGrouper
{
	public:
		uint32_t operator()(const char *data, uint32_t len) const
		{
			stringstream ss(string(data, len));
			ElementType dle;

 			ss >> dle;
//  			cout << "bandGrouper sees " << dle.i << " returning " << dle.i/(::count/buckets) << endl;
			return dle.first / (::count/buckets);
		}
};

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

int cCount = 0, pCount = 0;

void *MP_FIFO_stress_producer(void *arg)
{
	FIFO<ElementType> *f = reinterpret_cast<FIFO<ElementType> *>(arg);
	uint64_t i;
	ElementType val;
	uint64_t id = ::pCount++;

// 	cout << "producer " << id << " started" << endl;
	for (i = 0; i < (uint32_t) ::count/10; i++) {
		val.first = (id << 60) | i;
		val.second = val.first;
		f->insert(val);
	}

	return NULL;
}

void *MP_FIFO_stress_consumer(void *arg)
{
	FIFO<ElementType> *f = reinterpret_cast<FIFO<ElementType> *>(arg);
	int it;
	ElementType val;
	bool ret;
// 	int id = ::cCount++;
	uint64_t pCounter[10];			// 10 producers right now; make this a global var...
	uint64_t pnum, pnext;

	memset(pCounter, 0, 8*10);

//  	cout << "consumer " << id << " started" << endl;
	it = f->getIterator();
	ret = f->next(it, &val);
	while (ret) {
		pnum = val.first >> 60;
		pnext = pCounter[pnum]++;
//  		cerr << id << ":  .first=0x" << hex << val.first << dec << " pnum:" << pnum << " val:" << pnext << endl;
		CPPUNIT_ASSERT((val.first & 0xfffffffffffffffLL) == pnext);
		CPPUNIT_ASSERT(val.second == val.first);
		ret = f->next(it, &val);
	}

	return NULL;
}

void *FIFO_stress_consumer(void *arg)
{
	FIFO<ElementType> *f = reinterpret_cast<FIFO<ElementType> *>(arg);
	uint64_t i;
	int it;
	ElementType val;
	bool ret;
	int id = ++::cCount;

	cout << "consumer " << id << " started" << endl;
	it = f->getIterator();
	for (i = 0; i < MAXINT64; i++) {
//  		cout << id << ": " << i << " ";
		ret = f->next(it, &val);
		if (!ret) {
			cout << "consumer " << id << " exiting" << endl;
			return NULL;
		}

 		else {
//  			cout << "first: " << val.first << " second: " << val.second << endl;
           	CPPUNIT_ASSERT(ret == true);
       		CPPUNIT_ASSERT(val.first == i && val.second == i);
 		}

	}
	return NULL;
}

void *FIFO_bench_consumer(void *arg)
{
	FIFO<int> *f = reinterpret_cast<FIFO<int> *>(arg);
	int i, it, val;
	bool ret;

	it = f->getIterator();
	for (i = 0; i < ::count; i++) {
// 		cout << "t " << i << endl;
		ret = f->next(it, &val);
//    		CPPUNIT_ASSERT(ret == true);
	}
	return NULL;
}

void *FIFO_2_helper(void *arg) 
{
	FIFO<DLElement> *f = reinterpret_cast<FIFO<DLElement> *>(arg);
	int i;
	DLElement fe;

	for (i = 0; i < ::count; i++) {
		fe.i = i;
#ifdef DEBUG
  		cout << "inserting " << i << endl;
#endif
		f->insert(fe);
	}
	sleep(5);		// causes the test to block on moreData until endOfInput is called
	f->endOfInput();
	return NULL;
}

void *WSDL_2_helper(void *arg)
{
	WSDL<ElementType> *w = reinterpret_cast<WSDL<ElementType> *>(arg);
	uint32_t i, id;
	bool nextRet;
	ElementType dle;

	id = w->getIterator();
//  	cout << "id = " << id << endl;
	for (i = 0; i < ::count; i++) {
		nextRet = w->next(id, &dle);
//   		cout << "tgot " << dle.i << endl;
		CPPUNIT_ASSERT(nextRet == true);
  		CPPUNIT_ASSERT(dle.first == i && dle.second == i);
	}
	return NULL;
}

void *BandedDL_1_helper(void *arg)
{
	BandedDL<ElementType> *w = reinterpret_cast<BandedDL<ElementType> *>(arg);
	uint32_t i, id;
	bool nextRet;
	ElementType dle;

	id = w->getIterator();
//   	cout << "id = " << id << endl;
	for (i = 0; i < ::count; i++) {
		nextRet = w->next(id, &dle);
//    		cout << "tgot " << dle.i << endl;
//		CPPUNIT_ASSERT(nextRet == true);
//  		CPPUNIT_ASSERT(dle.first == i && dle.second == i);
	}
	return NULL;
}

void *BandedDL_2_helper(void *arg)
{
	BandedDL<StringElementType> *w = reinterpret_cast<BandedDL<StringElementType> *>(arg);
	uint32_t i, id;
	bool nextRet;
	StringElementType dle;

	id = w->getIterator();
//   	cout << "id = " << id << endl;
	for (i = 0; i < ::count; i++) {
		ostringstream os;

		os << "blah blah" << i;
		nextRet = w->next(id, &dle);
//    		cout << "tgot " << dle.i << endl;
		CPPUNIT_ASSERT(nextRet == true);
  		CPPUNIT_ASSERT(dle.first == i && dle.second == os.str());
	}
	return NULL;
}

void *WSDL_bench_helper(void *arg)
{
	WSDL<ElementType> *w = reinterpret_cast<WSDL<ElementType> *>(arg);
	int i, id;
	bool nextRet;
	ElementType e;

	id = w->getIterator();
	for (i = 0; i < ::count; i++)
		nextRet = w->next(id, &e);
	return NULL;
}

void *SWSDL_bench_helper(void *arg)
{
	SWSDL<ElementType> *sw = reinterpret_cast<SWSDL<ElementType> *>(arg);
	int i, id;
	bool nextRet;
	ElementType e;

	id = sw->getIterator();
	for (i = 0; i < ::count * 8; i++)
		nextRet = sw->next(id, &e);
	return NULL;
}

void *BDL_bench_helper(void *arg)
{
	BandedDL<ElementType> *sw = reinterpret_cast<BandedDL<ElementType> *>(arg);
	int i, id;
	bool nextRet;
	ElementType e;

	id = sw->getIterator();
	for (i = 0; i < ::count * 8; i++)
		nextRet = sw->next(id, &e);
	return NULL;
}

void *SWSDL_producer(void *arg)
{
	SWSDL<ElementType> *sw = reinterpret_cast<SWSDL<ElementType> *>(arg);
	for (int i = 0; i < ::count; i++)
		sw->insert(ElementType(i, i));

	return NULL;
}

void *BDL_producer(void *arg)
{
	BandedDL<ElementType> *sw = reinterpret_cast<BandedDL<ElementType> *>(arg);
	ElementType dle;
	for (int i = 0; i < ::count; i++)
	{
	    //dle.first = dle.second = i;
		sw->insert(ElementType(i, i));		
    }

	//sw->endOfInput();
	//cout << "end of input size is " << sw->totalSize() << endl;
	return NULL;
}

void *ZDL_producer(void *arg)
{
	ZDL<ElementType> *zdl = reinterpret_cast<ZDL<ElementType> *>(arg);
	for (int i = 0; i < ::count; i++)
	{
		zdl.insert(ElementType((int) N * rand() / (RAND_MAX + 1.0), i));
    }

	return NULL;
}

void *ZDL_consumer(void *arg)
{
	ZDL<ElementType> *zdl = reinterpret_cast<ZDL<ElementType> *>(arg);
	int i, id;
	bool nextRet;
	ElementType e;

	id = zdl->getIterator();
	nextRet = zdl->next(id, &e);
	while (nextRet) 
		nextRet = zdl->next(id, &e);
	return NULL;
}

void *SWSDL_consumer(void *arg)
{
	SWSDL<ElementType> *sw = reinterpret_cast<SWSDL<ElementType> *>(arg);
	int i, id;
	bool nextRet;
	ElementType e;

	id = sw->getIterator();
	nextRet = sw->next(id, &e);
	while (nextRet) 
		nextRet = zdl->next(id, &e);
	return NULL;
}
			
class DataListDriver : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(DataListDriver);

//CPPUNIT_TEST(FIFO_1);
//CPPUNIT_TEST(FIFO_2);
//CPPUNIT_TEST(MP_FIFO_stress);		//10 consumers 10 producers
// CPPUNIT_TEST(FIFO_stress);  // 10 consumers 1 producer; doesn't stop
// CPPUNIT_TEST(FIFO_bench);
// CPPUNIT_TEST(FIFO_singlethread_bench);
//CPPUNIT_TEST(WSDL_1);
//CPPUNIT_TEST(WSDL_2);
//CPPUNIT_TEST(WSDL_3);		// WSDL_1 with StringElementTypes
// CPPUNIT_TEST(WSDL_bench);
// CPPUNIT_TEST(WSDL_singlethread_bench);
//CPPUNIT_TEST(CON_DL_1);   // ConstantDataList
//CPPUNIT_TEST(BucketDL_1);

//CPPUNIT_TEST(BandedDL_1);		// WSDL_2 ported to BandedDLs
//CPPUNIT_TEST(BandedDL_2);		// BandedDL_1 that uses StringElementTypes

// make sure the DataList consumer-side interface works.
//CPPUNIT_TEST(BandedDL_as_WSDL_1);

// random inserts to bucketDL, create bandedDL, make sure it's in order
//CPPUNIT_TEST(BandedDL_as_WSDL_2);

/* tests prompted by feedback */
//CPPUNIT_TEST(polymorphism_1);

// make sure we can store basic types (int in this case)
//CPPUNIT_TEST(polymorphism_2);

// make sure we can create a FIFO, fill it, cast it to a DataList, and read the elements.
//CPPUNIT_TEST(polymorphism_3);  
//CPPUNIT_TEST(SWSDL_bench);
//CPPUNIT_TEST(BDL_multiproducer_bench);
//CPPUNIT_TEST(BDL_consumer_bench);
//CPPUNIT_TEST(SWSDL_multiproducer_bench);
CPPUNIT_TEST(SWSDL_consumer_bench);
//CPPUNIT_TEST(BDL_singlethread_bench);
CPPUNIT_TEST(ZDL_bench);
CPPUNIT_TEST_SUITE_END();

private:
public:

    void FIFO_1() 
	{
		int i, it;
		DLElement dummy;
		FIFO<DLElement> f(1, 20);
		bool nextRet;

		for (i = 0; i < 10; i++) {
			dummy.i = i;
			f.insert(dummy);
		}
		f.endOfInput();
		it = f.getIterator();
		CPPUNIT_ASSERT(it == 0);
		for (i = 0; i < 10; i++) {
			nextRet = f.next(it, &dummy);
			CPPUNIT_ASSERT(nextRet == true);
			CPPUNIT_ASSERT(dummy.i == i);
		}
    }

	void FIFO_2()
	{
		int i, it;
		DLElement fe;
		FIFO<DLElement> fifo(1, 2000);
		pthread_t thread;
		bool ret;

		fe.i = 0;
		pthread_create(&thread, NULL, FIFO_2_helper, &fifo);
		sleep(1);   // make sure the thread sleeps after 2000 inserts
		it = fifo.getIterator();
    	for (i = 0; i < ::count; i++) {
 			if (i % 100000 == 0) 
  				cout << i << "/" << ::count << endl;
			ret = fifo.next(it, &fe);
// 			cout << "fe = " << fe.i << endl;
			CPPUNIT_ASSERT(ret == true);
			CPPUNIT_ASSERT(fe.i == i);
		}
		ret = fifo.next(it, &fe);		// should sleep until the thread calls endOfInput()
		CPPUNIT_ASSERT(ret == false);
		pthread_join(thread, NULL);
	}

    void MP_FIFO_stress() 
	{
		int64_t i;
  		FIFO<ElementType> f(10, 25000);
		pthread_t consumer[10];
		pthread_t producer[10];
		
 		f.setMultipleProducers(true);
		
		for (i = 0; i < 10; i++) 
	  		pthread_create(&consumer[i], NULL, MP_FIFO_stress_consumer, &f);
		
		for (i = 0; i < 10; i++)
			pthread_create(&producer[i], NULL, MP_FIFO_stress_producer, &f);

		for (i = 0; i < 10; i++) 
			pthread_join(producer[i], NULL);
	
		f.endOfInput();

/*
//  		for (i = 0; i < MAXINT64; i++) {
 		for (i = 0; i < ::count; i++) {
 			if ((i % 10000000) == 0)
				cout << i << endl;
			f.insert(ElementType(i, i));
		}
		f.endOfInput();
*/
		for (i = 0; i < 10; i++)
	  		pthread_join(consumer[i], NULL);
	}

    void FIFO_stress() 
	{
		int64_t i;
  		FIFO<ElementType> f(10, 25000);
		pthread_t consumer[10];
		
		for (i = 0; i < 10; i++)
	  		pthread_create(&consumer[i], NULL, FIFO_stress_consumer, &f);
//  		for (i = 0; i < MAXINT64; i++) {
 		for (i = 0; i < ::count; i++) {
 			if ((i % 10000000) == 0)
				cout << i << endl;
			f.insert(ElementType(i, i));
		}
		f.endOfInput();
		for (i = 0; i < 10; i++) 
	  		pthread_join(consumer[i], NULL);
	}

    void FIFO_bench() 
	{
		int i;
  		FIFO<int> f(1, ::maxElements);
		pthread_t consumer;
		struct timespec tv1, tv2, diff;
		

  		pthread_create(&consumer, NULL, FIFO_bench_consumer, &f);
		clock_gettime(CLOCK_REALTIME, &tv1);
		for (i = 0; i < ::count; i++) {
			f.insert(i);
		}
		f.endOfInput();
  		pthread_join(consumer, NULL);
		clock_gettime(CLOCK_REALTIME, &tv2);
		timespec_sub(tv1, tv2, diff);
		cout << "producer & consumer passed " << ::count << " elements in " << 
			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;
			
    }

    void FIFO_singlethread_bench() 
	{
		int i, it, e;
  		FIFO<int> f(1, ::count);
		bool more;
		struct timespec tv1, tv2, diff;

		clock_gettime(CLOCK_REALTIME, &tv1);
		for (i = 0; i < ::count; i++) {
			f.insert(i);
		}
		f.endOfInput();
		clock_gettime(CLOCK_REALTIME, &tv2);
		timespec_sub(tv1, tv2, diff);

		cout << "FIFO_singlethreaded_bench: inserted " << ::count << 
			" ints in "<< diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;

		it = f.getIterator();
		for (i = 0; i < ::count; i++) {
  			more = f.next(it, &e);
		}
		clock_gettime(CLOCK_REALTIME, &tv1);
		timespec_sub(tv2, tv1, diff);
		cout << "FIFO_singlethread_bench: consumed, took " << diff.tv_sec << "s " << 
			diff.tv_nsec << "ns" << endl;
    }


	void WSDL_1()
	{
		WSDL<ElementType> w(1, ::count/100);
		ElementType dle;
		uint32_t i, id;
		bool nextRet;

 		for (i = 0; i < ::count; i++) {
// 			cout << "inserting " << i << endl;
			dle.first = dle.second = i;
			w.insert(dle);
		}

		w.endOfInput();
		id = w.getIterator();
		for (i = 0; i < ::count; i++) {
			nextRet = w.next(id, &dle);
//  			cout << i << ":got " << dle.first << " " << dle.second << endl;
			CPPUNIT_ASSERT(nextRet == true);
 			CPPUNIT_ASSERT(dle.first == i && dle.second == i);
		}
	}

	void WSDL_2()
	{
		WSDL<ElementType> w(2, ::count/10);
		ElementType dle;
		uint32_t i, id, size;
		bool nextRet;
		pthread_t consumer1;
		
		pthread_create(&consumer1, NULL, WSDL_2_helper, &w);

		// thread should wait here waiting on endOfInput()

 		for (i = 0; i < ::count; i++) {
			dle.first = dle.second = i;
			w.insert(dle);
		}

		sleep(1);
		w.endOfInput();

		/* let the thread consume the loaded set; make sure that the load 
		doesn't happen until the main thread finishes with the loaded set */

		sleep(1);
		size = w.totalSize();
		id = w.getIterator();
		for (i = 0; i < ::count; i++) {
			nextRet = w.next(id, &dle);
//   			cout << "got " << dle.i << endl;
			CPPUNIT_ASSERT(nextRet == true);
 			CPPUNIT_ASSERT(dle.first == i && dle.second == i);
		}
		pthread_join(consumer1, NULL);
	}

	void WSDL_3()
	{
		WSDL<StringElementType> w(1, ::count/10);
		StringElementType dle;
		uint32_t i, id;
		bool nextRet;

 		for (i = 0; i < ::count; i++) {
			ostringstream os;
		
			os << "blah blah blah" << i;
// 			cout << "inserting " << i << endl;
			dle.first = i;
			dle.second = os.str();
			w.insert(dle);
		}

		w.endOfInput();
		id = w.getIterator();
		for (i = 0; i < ::count; i++) {
			ostringstream os;		

			os << "blah blah blah" << i;
			nextRet = w.next(id, &dle);
//   			cout << i << ":got " << dle.first << " " << dle.second << endl;
			CPPUNIT_ASSERT(nextRet == true);
 			CPPUNIT_ASSERT(dle.first == i && dle.second == os.str());
		}
	}

	void WSDL_bench()
	{
		WSDL<ElementType> w(1, ::maxElements);
		int i;
		pthread_t consumer;
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		pthread_create(&consumer, NULL, WSDL_bench_helper, &w);
 		for (i = 0; i < ::count; i++)
			w.insert(ElementType(i, i));

		w.endOfInput();
		cout << "end of input size is " << w.totalSize() << endl;
		pthread_join(consumer, NULL);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "WSDL_bench: producer & consumer passed " << ::count << " elements in " << 
			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;
	}

	void WSDL_singlethread_bench()
	{
		WSDL<int> w(1, ::maxElements);
		int i, id, e;
		bool nextRet;
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
 		for (i = 0; i < ::count; i++) 
			w.insert(i);

		w.endOfInput();
		id = w.getIterator();
		for (i = 0; i < ::count; i++)
			nextRet = w.next(id, &e);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "WSDL_singlethread_bench: producer & consumer passed " << 
			::count << " elements in " << diff.tv_sec << "s " << diff.tv_nsec << 
			"ns" << endl;
	}
	
	void SWSDL_bench()
	{
        /*	    
		SWSDL<ElementType> sw(1, ::maxElements);
		int i;
		pthread_t consumer;
		pthread_t producer;
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		pthread_create(&producer, NULL, SWSDL_producer, &sw);
		pthread_create(&consumer, NULL, SWSDL_bench_helper, &sw);

		pthread_join(producer, NULL);
		pthread_join(consumer, NULL);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "SWSDL_bench: producer & consumer passed " << ::count << " elements in " << 
			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;
		*/
		SWSDL<ElementType> sw(1, ::maxElements);
		int i;
		pthread_t consumer;
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		pthread_create(&consumer, NULL, SWSDL_bench_helper, &sw);
 		for (i = 0; i < ::count; i++)
			sw.insert(ElementType(i, i));

		sw.endOfInput();
		cout << "end of input size is " << sw.totalSize() << endl;
		pthread_join(consumer, NULL);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "SWSDL_bench: producer & consumer passed " << ::count << " elements in " << 
			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;
	}

	void SWSDL_multiproducer_bench()
	{
		SWSDL<ElementType> sw(1, ::maxElements);
		sw.setMultipleProducers(true);
		int i, id;
		ElementType e;
		bool nextRet;
		uint32_t numOfThreads = 8;
		pthread_t producer[numOfThreads];
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfThreads; i++)
		    pthread_create(&producer[i], NULL, SWSDL_producer, &sw);

		for (i = 0; i < numOfThreads; i++)
		    pthread_join(producer[i], NULL);

        sw.endOfInput();
        cout << "end of input size is " << sw.totalSize() << endl;
        clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
        cout << "# of Producer: " << numOfThreads << endl;	
        cout << "SWSDL_producer_phase_bench: producer & consumer passed " << 
			::count * numOfThreads << " elements in " << diff.tv_sec << "s " << diff.tv_nsec << 
			"ns" << endl;
		clock_gettime(CLOCK_REALTIME, &ts1);		
		id = sw.getIterator();
		for (i = 0; i < ::count; i++)
			nextRet = sw.next(id, &e);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);

		cout << "SWSDL_consumer_phase_bench: producer & consumer passed " << 
			::count * numOfThreads << " elements in " << diff.tv_sec << "s " << diff.tv_nsec << 
			"ns" << endl;
	}
	
	void SWSDL_consumer_bench()
	{
	    SWSDL<ElementType> sw(4);
        sw.setMultipleProducers(true);
		ElementType dle;
		uint32_t i, id;
		bool nextRet;
		uint32_t numOfProducers = 8;
		uint32_t numOfConsumers = 4;
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		    pthread_create(&producer[i], NULL, SWSDL_producer, &sw);
		    
		for (i = 0; i < numOfConsumers; i++)
		    pthread_create(&consumer[i], NULL, SWSDL_consumer, &sw);

		for (i = 0; i < numOfThreads; i++)
		    pthread_join(producer[i], NULL);

		sw.endOfInput();

		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);

        cout << "# of Producer: " << numOfThreads << endl;
		cout << "SWSDL_consumer_bench: producer & consumer passed " << 
			::count * numOfThreads << " elements in " << diff.tv_sec << "s " << diff.tv_nsec << 
			"ns" << endl;
	}


    void BDL_multiproducer_bench()
	{
        BandedDL<ElementType> w(2);
        w.setMultipleProducers(true);
		ElementType dle;
		uint32_t i, id;
		bool nextRet;
		uint32_t numOfThreads = 8;
		pthread_t producer[numOfThreads];
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfThreads; i++)
		    pthread_create(&producer[i], NULL, BDL_producer, &w);

		for (i = 0; i < numOfThreads; i++)
		    pthread_join(producer[i], NULL);

		w.endOfInput();
        cout << "end of input size is " << w.totalSize() << endl;
        clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "# of Producer: " << numOfThreads << endl;	
        cout << "BDL_producer_phase_bench: producer & consumer passed " << 
			::count * numOfThreads << " elements in " << diff.tv_sec << "s " << diff.tv_nsec << 
			"ns" << endl;
			
		clock_gettime(CLOCK_REALTIME, &ts1);
		id = w.getIterator();
		for (i = 0; i < ::count; i++) {
			nextRet = w.next(id, &dle);
		}
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);

		cout << "BDL_consumer_phase_bench: producer & consumer passed " << 
			::count * numOfThreads << " elements in " << diff.tv_sec << "s " << diff.tv_nsec << 
			"ns" << endl;
/*		
		SWSDL<ElementType> sw(1, ::maxElements);
		int i;
		pthread_t consumer;
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		pthread_create(&consumer, NULL, SWSDL_bench_helper, &sw);
 		for (i = 0; i < ::count; i++)
			sw.insert(ElementType(i, i));

		sw.endOfInput();
		cout << "end of input size is " << sw.totalSize() << endl;
		pthread_join(consumer, NULL);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "SWSDL_bench: producer & consumer passed " << ::count << " elements in " << 
			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;
*/			
	}
	
	void BDL_consumer_bench()
	{
        BandedDL<ElementType> w(2);
        w.setMultipleProducers(true);
		ElementType dle;
		uint32_t i, id;
		bool nextRet;
		uint32_t numOfThreads = 8;
		pthread_t producer[numOfThreads];
		pthread_t consumer;
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfThreads; i++)
		    pthread_create(&producer[i], NULL, BDL_producer, &w);

		for (i = 0; i < numOfThreads; i++)
		    pthread_join(producer[i], NULL);

		w.endOfInput();
		//pthread_create(&consumer, NULL, BDL_bench_helper, &w);
        //cout << "end of input size is " << w.totalSize() << endl;
        //clock_gettime(CLOCK_REALTIME, &ts2);
		//timespec_sub(ts1, ts2, diff);
		//cout << "# of Producer: " << numOfThreads << endl;	
        //cout << "BDL_producer_phase_bench: producer & consumer passed " << 
		//	::count * numOfThreads << " elements in " << diff.tv_sec << "s " << diff.tv_nsec << 
		//	"ns" << endl;
			
		//clock_gettime(CLOCK_REALTIME, &ts1);
		id = w.getIterator();
		for (i = 0; i < ::count * 8; i++) {
			nextRet = w.next(id, &dle);
		}
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
        cout << "end of input size is " << w.totalSize() << endl;
		cout << "BDL_consumer_phase_bench: producer & consumer passed " << 
			::count * numOfThreads << " elements in " << diff.tv_sec << "s " << diff.tv_nsec << 
			"ns" << endl;
/*		
		SWSDL<ElementType> sw(1, ::maxElements);
		int i;
		pthread_t consumer;
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		pthread_create(&consumer, NULL, SWSDL_bench_helper, &sw);
 		for (i = 0; i < ::count; i++)
			sw.insert(ElementType(i, i));

		sw.endOfInput();
		cout << "end of input size is " << sw.totalSize() << endl;
		pthread_join(consumer, NULL);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "SWSDL_bench: producer & consumer passed " << ::count << " elements in " << 
			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;
*/			
	}
	
	void BDL_singlethread_bench()
	{
		/*SWSDL<ElementType> sw(1, ::maxElements);
		int i;
		pthread_t consumer;
		pthread_t producer;
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		pthread_create(&producer, NULL, SWSDL_producer, &sw);
		pthread_join(producer, NULL);
		pthread_create(&consumer, NULL, SWSDL_bench_helper, &sw);

		pthread_join(consumer, NULL);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);*/
		SWSDL<ElementType> sw(1, ::maxElements);
		int i, id;
		ElementType e;
		bool nextRet;
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		cout << "start of input size is " << ::count << endl;
 		for (i = 0; i < ::count; i++) 
			sw.insert(ElementType(i, i));
        //cout << "end of input size is " << sw.totalSize() << endl;
		//sw.endOfInput();
		//id = sw.getIterator();
		for (i = 0; i < ::count; i++)
			nextRet = sw.next(id, &e);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);

		//cout << "SWSDL_consumer_phase_bench: producer & consumer passed " << 
		//	::count << " elements in " << diff.tv_sec << "s " << diff.tv_nsec << 
		//	"ns" << endl;
	}
		
	void CON_DL_1()
	{
		DLElement d, d2;
		int i = 0;
		bool nextRet;

		d.i = 1;

		ConstantDataList<DLElement> c(d);
	
		nextRet = c.next(i, &d2);
		CPPUNIT_ASSERT(nextRet == true);
		CPPUNIT_ASSERT(d.i == 1 && d2.i == 1);
		
		d.i = 2;
		c.insert(d);
		nextRet = c.next(i, &d2);
		CPPUNIT_ASSERT(nextRet == true);
		CPPUNIT_ASSERT(d.i == 2 && d2.i == 2);
	}

	void BucketDL_1()
	{
		BucketDL<ElementType> bdl(16, 1, ::count/100);
		ElementType dle;
		uint32_t i, it, eCount = 0, buckets;
		bool nextRet;

		for (i = 0; i < ::count; i++) {
//  			if (i % 1000000 == 0) cout << i << endl;
 			dle.first = dle.second = i;
			bdl.insert(dle);
		}
 		bdl.endOfInput();
//  		cout << "inserted " << ::count << " elements" << endl;

		buckets = bdl.bucketCount();
		for (i = 0; i < buckets; i++) {
			it = bdl.getIterator(i);
			do {
				nextRet = bdl.next(i, it, &dle);
				if (nextRet) {
					CPPUNIT_ASSERT(dle.first >= 0 && dle.first <= (uint64_t) ::count);
					CPPUNIT_ASSERT(++eCount <= ::count);
				}
			} while (nextRet == true);
		}
		CPPUNIT_ASSERT(eCount == ::count);
	}

	void BandedDL_1()
	{
		BandedDL<ElementType> w(2);
		ElementType dle;
		uint32_t i, id, size;
		bool nextRet;
		pthread_t consumer1;
		
		pthread_create(&consumer1, NULL, BandedDL_1_helper, &w);

		// thread should wait here waiting on endOfInput()

 		for (i = 0; i < ::count; i++) {
			dle.first = dle.second = i;
			w.insert(dle);
			if (((i+1) % (::count/10)) == 0) {
				cout << "inserted " << i+1 << "/" << ::count << endl;
				w.saveBand();
			}
		}

		sleep(1);
		w.endOfInput();

		/* let the thread consume the loaded set; make sure that the load 
		doesn't happen until the main thread finishes with the loaded set */

		sleep(5);
		size = w.totalSize();
		id = w.getIterator();
		for (i = 0; i < ::count; i++) {
			nextRet = w.next(id, &dle);
//    			cout << "got " << dle.i << endl;
			CPPUNIT_ASSERT(nextRet == true);
 			CPPUNIT_ASSERT(dle.first == i);
		}
		pthread_join(consumer1, NULL);
	}

	void BandedDL_2()
	{
		BandedDL<StringElementType> w(2);
		StringElementType dle;
		uint32_t i, id, size;
		bool nextRet;
		pthread_t consumer1;
		
		pthread_create(&consumer1, NULL, BandedDL_2_helper, &w);

		// thread should wait here waiting on endOfInput()

 		for (i = 0; i < ::count; i++) {
			ostringstream os;

			os << "blah blah" << i;

			dle.first = i;
			dle.second = os.str();
			w.insert(dle);
			if (((i+1) % (::count/10)) == 0) {
				cout << "inserted " << i+1 << "/" << ::count << endl;
				w.saveBand();
			}
		}

		sleep(1);
		w.endOfInput();

		/* let the thread consume the loaded set; make sure that the load 
		doesn't happen until the main thread finishes with the loaded set */

		cout << "endofInput finished\n";

		sleep(5);
		size = w.totalSize();
		id = w.getIterator();
		for (i = 0; i < ::count; i++) {
			ostringstream os;

			os << "blah blah" << i;
			nextRet = w.next(id, &dle);
//    			cout << "got " << dle.i << endl;
			CPPUNIT_ASSERT(nextRet == true);
 			CPPUNIT_ASSERT(dle.first == i && dle.second == os.str());
		}
		pthread_join(consumer1, NULL);
	}

	void BandedDL_as_WSDL_1()
	{
		BandedDL<ElementType> bdl(1);
		ElementType e;
		uint32_t i, it;
		bool more;

		for (i = 1; i <= ::count; i++) {
			e.first = e.second = i;
			bdl.insert(e);
			if (i % (::count/10) == 0)
				bdl.saveBand();
		}
		bdl.endOfInput();
		
		it = bdl.getIterator();
		for (i = 1; i <= ::count; i++) {
			more = bdl.next(it, &e);
			CPPUNIT_ASSERT(more == true);
			CPPUNIT_ASSERT(e.first == i && e.second == i);
		}
		more = bdl.next(it, &e);
		CPPUNIT_ASSERT(more == false);

		bdl.restart();

		/* make sure it can be read again */
		for (i = 1; i <= ::count; i++) {
			more = bdl.next(it, &e);
			if (!more)
				cerr << i << endl;
			CPPUNIT_ASSERT(more == true);
			CPPUNIT_ASSERT(e.first == i && e.second == i);
		}
		more = bdl.next(it, &e);
		CPPUNIT_ASSERT(more == false);
	}
	
	void BandedDL_as_WSDL_2()
	{
		BucketDL<ElementType> bucketDL(::buckets, 1, 1000, BandGrouper());
		uint32_t last, i, it;
		ElementType dle;	
		bool more;

		srand(time(NULL));
		for (i = 0; i < ::count; i++) {
			dle.first = dle.second = rand() % ::count;   // artificial max
			bucketDL.insert(dle);
		}
		bucketDL.endOfInput();
		
// 		cout << "made bucketdl" << endl;
		
		BandedDL<ElementType> bdl(bucketDL, 1);
		
//  		cout << "made bandeddl" << endl;

		it = bdl.getIterator();
		last = 0;
		// duplicates were removed, there won't be ::count elements in bdl
		more = bdl.next(it, &dle);
		while (more) {		
			CPPUNIT_ASSERT(dle.first < (uint64_t) ::count);
//  			cout << dle.first << " >= " << last << endl;
			CPPUNIT_ASSERT(dle.first >= last);
			last = dle.first;
			more = bdl.next(it, &dle);
		}
		
// 		cerr << "read through bandeddl" << endl;

		more = bdl.next(it, &dle);
		CPPUNIT_ASSERT(more == false);

	}
	
	void ZDL_bench()
	{
	    ZDL<ElementType> zdl(4);
        zdl.setMultipleProducers(true);
		ElementType dle;
		uint32_t i, id;
		bool nextRet;
		uint32_t numOfProducers = 8;
		uint32_t numOfConsumers = 4;
		pthread_t producer[numOfProducers];
		pthread_t consumer[numOfConsumers];
		struct timespec ts1, ts2, diff;

		clock_gettime(CLOCK_REALTIME, &ts1);
		for (i = 0; i < numOfProducers; i++)
		    pthread_create(&producer[i], NULL, ZDL_producer, &zdl);
		    
		for (i = 0; i < numOfConsumers; i++)
		    pthread_create(&consumer[i], NULL, ZDL_consumer, &zdl);

		for (i = 0; i < numOfThreads; i++)
		    pthread_join(producer[i], NULL);

		zdl.endOfInput();
			
		//clock_gettime(CLOCK_REALTIME, &ts1);
		id = w.getIterator();
		for (i = 0; i < ::count * 8; i++) {
			nextRet = w.next(id, &dle);
		}
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
        cout << "end of input size is " << w.totalSize() << endl;
		cout << "ZDL_consumer_producer_bench: producer & consumer passed " << 
			::count * numOfProducers << " elements in " << diff.tv_sec << "s " << diff.tv_nsec << 
			"ns" << endl;
	    
	}

	/* Sanity test */
	void polymorphism_1()
	{
		DataList<DLElement> *dl;
		FIFO<DLElement> *f;
		DLElement dle;
		int it, i;
		bool more;

		f = new FIFO<DLElement>(1, 20);
 		dl = (DataList<DLElement> *) f;

		it = dl->getIterator();
		for (i = 0; i < 10; i++) {
			dle.i = i;
			dl->insert(dle);
		}
 		dl->endOfInput();

		for (i = 0; i < 10; i++) {
			more = dl->next(it, &dle);
// 			cout << dle.i << endl;
			CPPUNIT_ASSERT(more == true);
			CPPUNIT_ASSERT(dle.i == i);
		}

		delete f;
	}

	/* Sanity test 2. Can we stuff basic datatypes into DataLists?  Apparently not.  WHY?*/
	void polymorphism_2()
	{
		DataList<int> *dl;
		FIFO<int> *f = new FIFO<int>(1, 20);
		int it, i, dle;
		bool more;

 		dl = (DataList<int> *) f;

		it = dl->getIterator();
		for (i = 0; i < 10; i++) 
			dl->insert(i);
	
		dl->endOfInput();

		for (i = 0; i < 10; i++) {
			more = dl->next(it, &dle);
			CPPUNIT_ASSERT(more == true);
			CPPUNIT_ASSERT(dle == i);
// 			cout << "dle[" << i << "]: " << dle << endl;
		}

		delete f;
	}

	void polymorphism_3()
	{
		DataList<DLElement> *dl;
		FIFO<DLElement> *f;
		int it, i;
		DLElement dle;
		bool more;

		f = new FIFO<DLElement>(1, 20);
 		dl = (DataList<DLElement> *) f;

		it = dl->getIterator();
		for (i = 0; i < 10; i++) {
			dle.i = i;
			f->insert(dle);
		}
	
		f->endOfInput();

		for (i = 0; i < 10; i++) {
// 			cout << "next" << endl;
			more = dl->next(it, &dle);
			CPPUNIT_ASSERT(more == true);
			CPPUNIT_ASSERT(dle.i == i);
//  			cout << "dle[" << i << "]: " << dle.i << endl;
		}

		delete f;
	}
		


				


}; 

CPPUNIT_TEST_SUITE_REGISTRATION(DataListDriver);


int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


