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

// $Id: fifo-bench.cpp 7396 2011-02-03 17:54:36Z rdempsey $
#include <iostream>
#include <sstream>
#include <list>

#include <pthread.h>
// #include <sched.h>
#include <time.h>
#include <sys/time.h>
#include "bytestream.h"

#include "fifo.h"
#include "elementtype.h"

using namespace std;
using namespace joblist;
using namespace messageqcpp;

int64_t count = 100 * 1000 * 1000;
int maxElements = 50 * 1000;   //max elements in memory at once for the benchmarks

int fifoSize = 128;
int rowContainerSize = 8 * 1024;

int cCount = 0;

struct RowContainer
{
        int count;
        ElementType *et;

        RowContainer(uint64_t rowsPerRowContainer):count(rowsPerRowContainer)
        {
                et = new ElementType[rowsPerRowContainer];
        }

	RowContainer(const RowContainer &rhs)
	{
		et = new ElementType[rhs.count];
		count = rhs.count;
		memcpy(et, rhs.et, rhs.count * sizeof(ElementType));
	}

	RowContainer():count(::rowContainerSize)
	{
		et = new ElementType[::rowContainerSize];
	}

	RowContainer& operator = (const RowContainer &rhs)
	{
		if(this == &rhs)
			return *this;
		if(et != NULL)
		{
			delete [] et;
		}
		et = new ElementType[rhs.count];
		count = rhs.count;
		memcpy(et, rhs.et, rhs.count * sizeof(ElementType));
		return *this;
	}

        ~RowContainer()
        {
              delete [] et;
        }
};

struct RowContainer2
{
        int count;
	ByteStream bs;

        RowContainer2(uint64_t rowsPerRowContainer):count(0)
        {
        }

        RowContainer2():count(0)
        {
        }

        ~RowContainer2()
        {
        }
};

class NearFIFO {
	private:
		int size;
		ElementType *et;
		int pindex, cindex;

	public:
		NearFIFO() : size(::maxElements), pindex(0), cindex(0)
		{ 
			et = new ElementType[size];
		};

		inline bool next(uint64_t it, ElementType *e) 
		{ 
			// uint64_t it2 = it++;

			*e = et[cindex];

 			if ((++cindex) << 1 >= size) {
 				cindex = 0;
 				return false;
 			}
 			else
				return true;
		}

		inline void insert(const ElementType &e) 
		{ 
			et[pindex] = e;
 			if (++pindex == size)
 				pindex = 0;
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

void *FIFO_stress_consumer(void *arg)
{
	FIFO<ElementType> *f = reinterpret_cast<FIFO<ElementType> *>(arg);
	uint64_t it;
	ElementType val;
	bool ret;
	int id = ++::cCount;

	cout << "consumer " << id << " started" << endl;
	it = f->getIterator();
	// WWW+
	int blockCount = 0;
	ByteStream bs;
	// WWW-
	while (1) {
		ret = f->next(it, &val);
		// WWW+
		bs << val.first;
		blockCount++;
		if(blockCount%1000 == 0)
		{
			bs.restart();
		}
		// WWW-

		if (!ret) {
			cout << "consumer " << id << " exiting" << endl;
			return NULL;
		}
	}
	return NULL;
}


    void FIFO_stress(int threads) 
	{
		int64_t i;
  		FIFO<ElementType> f(threads, ::maxElements);
		pthread_t consumer[threads];
		ElementType val;
		int err = 0;

		::cCount = 0;
		for (i = 0; i < threads; i++)
	  		err = pthread_create(&consumer[i], NULL, FIFO_stress_consumer, &f);
			if (err)
				cout << "ERROR starting threads" << endl;
//  		for (i = 0; i < MAXINT64; i++) {
 		for (i = 0; i < ::count; i++) {
//  			if ((i % 1000000) == 0)
// 				cout << i << endl;
			val.first = i;
			val.second = i;
			f.insert(val);
		}
		f.endOfInput();
		for (i = 0; i < threads; i++) 
	  		pthread_join(consumer[i], NULL);
	}

void *FIFO_stress_consumer_with_rowcontainers(void *arg)
{
        FIFO<RowContainer> *f = reinterpret_cast<FIFO<RowContainer> *>(arg);
        uint64_t it;
        ElementType val;
        RowContainer rc(::rowContainerSize);
        bool ret;
        int id = ++::cCount;

        it = f->getIterator();
	uint64_t rid = 0;
        while (1) {
                ret = f->next(it, &rc);
                if (!ret) {
                        return NULL;
                }
                else
                {
			ByteStream bs;
			
                        for(int i = 0; i < rc.count; i++)
                        {
				if(i%1000 == 0)
					bs.restart();
				bs << rc.et[i].first;
				if(id == 1 && rc.et[i].first != rid) {
				}
				rid++;
                        }
                }
        }
        return NULL;
}


    void FIFO_stress_with_rowcontainers(int threads, int64_t fifoSize, int64_t rowsPerRowContainer) 
        {
                int64_t i;
                  FIFO<RowContainer> f(threads, fifoSize);
                pthread_t consumer[threads];
                ElementType val;
                int err = 0;

                ::cCount = 0;
                for (i = 0; i < threads; i++)
                        err = pthread_create(&consumer[i], NULL, FIFO_stress_consumer_with_rowcontainers, &f);
                        if (err)
                                cout << "ERROR starting threads" << endl;

                RowContainer rw(rowsPerRowContainer);
                int wrapperCount = 0;
                int rwCount = 0;
                for (i = 0; i < ::count; i++) {
                        val.first = i;
                        val.second = i;
                        rw.et[rwCount] = val;
                        rwCount++;
                        if(rwCount == rowsPerRowContainer)
                        {
                                rw.count = rwCount;
                                wrapperCount++;
                                rwCount = 0;
                                f.insert(rw);
                        }
                }
                f.endOfInput();
                for (i = 0; i < threads; i++) 
                        pthread_join(consumer[i], NULL);
        }


void *FIFO_stress_consumer_with_rowcontainers2(void *arg)
{
        FIFO<RowContainer2> *f = reinterpret_cast<FIFO<RowContainer2> *>(arg);
        uint64_t it;
        ElementType val;
        RowContainer2 rw;
        bool ret;
	ByteStream bs;

        it = f->getIterator();
        while (1) {
                ret = f->next(it, &rw);
                if (!ret) {
                        // cout << "consumer " << id << " exiting" << endl;
                        return NULL;
                }
                else
                {
			uint64_t i = 1;
			bs << i;
			bs += rw.bs;
			bs.restart();	
                }
        }
        return NULL;
}


    void FIFO_stress_with_rowcontainers2(int threads, int64_t fifoSize, int64_t rowsPerRowContainer) 
        {
                int64_t i;
  //            FIFO<ElementType> f(threads, ::maxElements);
                FIFO<RowContainer2> f(threads, fifoSize);
                pthread_t consumer[threads];
                ElementType val;
                int err = 0;

                ::cCount = 0;
                for (i = 0; i < threads; i++)
                        err = pthread_create(&consumer[i], NULL, FIFO_stress_consumer_with_rowcontainers2, &f);
                        if (err)
                                cout << "ERROR starting threads" << endl;
//              for (i = 0; i < MAXINT64; i++) {

                RowContainer2 rw(rowsPerRowContainer);
                int wrapperCount = 0;
                int rwCount = 0;
		ByteStream bs;
		uint64_t j = 0;
                for (i = 0; i < ::count; i++) {
//                      if ((i % 1000000) == 0)
//                              cout << i << endl;
                        val.first = i;
                        val.second = i;
			rw.bs << j;
			rwCount++;
                        if(rwCount == rowsPerRowContainer)
                        {
                                rw.count = wrapperCount;
                                wrapperCount++;
                                rwCount = 0;
                                f.insert(rw);
				rw.bs.restart();
                        }
                }
                f.endOfInput();
                for (i = 0; i < threads; i++) 
                        pthread_join(consumer[i], NULL);
        }

void *bandwidth_consumer(void *arg)
{
 	ElementType *f = reinterpret_cast<ElementType *>(arg);
	int64_t i;
	int j;
	ElementType val;
	int id = ++::cCount;

	cout << "consumer " << id << " started" << endl;
	for (i = 0, j = 0; i < ::count; i++) {
 		val = f[j++];
 		if (j == ::maxElements)
 			j = 0;
	}
	cout << "consumer " << id << " exiting" << endl;
	return NULL;

}

void bandwidth(int threads) 
{
	int64_t i;
	int j;
  	ElementType *f = new ElementType[::maxElements];
	pthread_t consumer[threads];
	ElementType val;

	::cCount = 0;

	for (i = 0; i < threads; i++)
  		pthread_create(&consumer[i], NULL, bandwidth_consumer, f);
	for (j = 0, i = 0; i < ::count; i++) {
// 		if ((i % 1000000) == 0)
// 			cout << i << endl;
		val.first = i;
		val.second = i;
 		f[j++] = val;
 		if (j == ::maxElements)
 			j = 0;
	}
	for (i = 0; i < threads; i++) 
  		pthread_join(consumer[i], NULL);
}


void *bandwidth_nearfifo_consumer(void *arg)
{
 	NearFIFO *nf = reinterpret_cast<NearFIFO *>(arg);
	int64_t i;
	int j;
	ElementType val;
	int id = ++::cCount;
	bool ret;

	cout << "consumer " << id << " started" << endl;
	for (i = 0, j = 0; i < ::count; i++) {
 		ret = nf->next(i, &val);
 		if (ret)
 			continue;
	}
	cout << "consumer " << id << " exiting" << endl;
	return NULL;

}

void bandwidth_nearfifo(int threads) 
{
	int64_t i;
	int j;
 	NearFIFO nf;
	pthread_t consumer[threads];
	ElementType val;

	::cCount = 0;

	for (i = 0; i < threads; i++)
  		pthread_create(&consumer[i], NULL, bandwidth_nearfifo_consumer, &nf);
	for (j = 0, i = 0; i < ::count; i++) {
// 		if ((i % 1000000) == 0)
// 			cout << i << endl;
		val.first = i;
		val.second = i;
 		nf.insert(val);
	}
	for (i = 0; i < threads; i++) 
  		pthread_join(consumer[i], NULL);
}

ElementType *et;
int cindex = 0, pindex = 0, size = 50000;

inline bool next(uint64_t it, ElementType *e) 
{
	// uint64_t it2 = it++;

	*e = et[cindex];

	if ((++cindex) << 1 >= size) {
		cindex = 0;
		return false;
 	}
 	else
		return true;
}

inline void insert(const ElementType &e) 
{ 
	et[pindex] = e;
	if (++pindex == size)
		pindex = 0;
}


void *bandwidth_fcn_consumer(void *arg)
{
	int64_t i;
	int j;
	ElementType val;
	int id = ++::cCount;
	bool ret;

	cout << "consumer " << id << " started" << endl;
	for (i = 0, j = 0; i < ::count; i++) {
 		ret = next(i, &val);
 		if (ret)
 			continue;
	}
	cout << "consumer " << id << " exiting" << endl;
	return NULL;

}

void bandwidth_fcn(int threads) 
{
	int64_t i;
	int j;
 	NearFIFO nf;
	pthread_t consumer[threads];
	ElementType val;

	::cCount = 0;

	et = new ElementType[size];

	for (i = 0; i < threads; i++)
  		pthread_create(&consumer[i], NULL, bandwidth_fcn_consumer, NULL);
	for (j = 0, i = 0; i < ::count; i++) {
// 		if ((i % 1000000) == 0)
// 			cout << i << endl;
		val.first = i;
		val.second = i;
 		insert(val);
	}
	for (i = 0; i < threads; i++) 
  		pthread_join(consumer[i], NULL);
}

int main( int argc, char **argv)
{
	struct timespec ts1, ts2, diff;

	if (argc == 1) {
		cout << "running with 1 consumer" << endl;
		clock_gettime(CLOCK_REALTIME, &ts1);
 		FIFO_stress(1);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "passed " << ::count << " elements in " << 
			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;
		cout << "running bandwidth check" << endl;

		clock_gettime(CLOCK_REALTIME, &ts1);
		bandwidth(1);
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "passed " << ::count << " elements in " << 
			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;
	}
	else {
 		cout << "running with " << argv[1] << " consumers" << endl;
                cout << ::count/(1000*1000) << " million rows." << endl << endl;
		cout << "running FIFO benchmark" << endl;
 		clock_gettime(CLOCK_REALTIME, &ts1);
  		FIFO_stress(atoi(argv[1]));
 		clock_gettime(CLOCK_REALTIME, &ts2);
 		timespec_sub(ts1, ts2, diff);
 		cout << "passed " << ::count/1000/1000 << " million elements in " << 
 			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;

		cout << "running FIFO benchmark with RowContainers" << endl;
 		clock_gettime(CLOCK_REALTIME, &ts1);
  		FIFO_stress_with_rowcontainers(atoi(argv[1]), ::fifoSize, ::rowContainerSize); // Consume up to 128 MB
 		clock_gettime(CLOCK_REALTIME, &ts2);
 		timespec_sub(ts1, ts2, diff);
 		cout << "passed " << ::count/1000/1000 << " million elements in " << 
 			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;

		cout << "running FIFO benchmark with RowContainers and bytestream" << endl;
 		clock_gettime(CLOCK_REALTIME, &ts1);
  		FIFO_stress_with_rowcontainers2(atoi(argv[1]), ::fifoSize, ::rowContainerSize); // Consume up to 128 MB
 		clock_gettime(CLOCK_REALTIME, &ts2);
 		timespec_sub(ts1, ts2, diff);
 		cout << "passed " << ::count/1000/1000 << " million elements in " << 
 			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;

/*
		cout << "running NearFIFO benchmark" << endl;
		clock_gettime(CLOCK_REALTIME, &ts1);
		bandwidth_nearfifo(atoi(argv[1]));
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "passed " << ::count << " elements in " << 
			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;

 		cout << "running shared array via fcns benchmark" << endl;
		clock_gettime(CLOCK_REALTIME, &ts1);
		bandwidth_fcn(atoi(argv[1]));
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "passed " << ::count << " elements in " << 
			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;

*/

 		cout << "running shared array benchmark" << endl;
		clock_gettime(CLOCK_REALTIME, &ts1);
		bandwidth(atoi(argv[1]));
		clock_gettime(CLOCK_REALTIME, &ts2);
		timespec_sub(ts1, ts2, diff);
		cout << "passed " << ::count << " elements in " << 
			diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;

	}

	
	return 0;
}


