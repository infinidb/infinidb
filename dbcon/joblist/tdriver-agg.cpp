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
*   $Id: tdriver-agg.cpp 9210 2013-01-21 14:10:42Z rdempsey $
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
#include "elementtype.h"
#include "zdl.h"
#include "stopwatch.cpp"
#include "jobstep.h"
//#include "aggregator.h"
#include "constantcolumn.h"
#include "simplefilter.h"
#include "aggregatecolumn.h"
#include "simplecolumn.h"
#include "dataconvert.h"
#include "largehashjoin.h"

using namespace dataconvert;
 
// #undef CPPUNIT_ASSERT
// #define CPPUNIT_ASSERT(x)

using namespace std;
using namespace joblist;
using namespace execplan;

Stopwatch timer1; 
const uint32_t NUM_BUCKETS = 256; 
const uint32_t MAX_SIZE = 0x100000; 
const uint32_t MAX_ELEMENTS = 0x20000;
const uint32_t NUM_THREADS = 4;
const string datapath="/home/zzhu/genii/tools/dbbuilder/lineitem.tbl";
//const string datapath="/usr/local/Calpont/bin/lineitem.tbl";
int numConsumers = 1;
int numRuns = 1;
int printInterval = numRuns * 100000;
JSTimeStamp dlTimes; 

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

    struct ThreadParms
    {
    	BucketDL<ElementType> *bdl;
    	TupleBucketDataList *tbdl;
    	int count;
    	int threadNumber;
    };
    
   void *bucketConsumer(void *arg)
    {
    	ThreadParms *parms = reinterpret_cast<ThreadParms*>(arg);
    	// BucketDL<ElementType> *bdl = reinterpret_cast<BucketDL<ElementType> *>(arg);
    	BucketDL<ElementType> *bdl = parms->bdl;
    	int numBucketsToConsume = parms->count;
    	int threadNumber = parms->threadNumber;
    	int64_t i;
    	ElementType val;
    	uint32_t it;
    	int bucketIndex;
    	
        int cnt = 0;
        for (i = 0; i < numBucketsToConsume; i++) 
    	{
    		bucketIndex = (threadNumber * numBucketsToConsume) + i;
    		it = bdl->getIterator(bucketIndex);
    		while(bdl->next(bucketIndex, it, &val))
    			cnt++;
         }
        cout << "consumer " << threadNumber << "consumed " << cnt << endl;      
    	return NULL;
    }	
    
    void *tupleConsumer(void *arg)
    {
    	ThreadParms *parms = reinterpret_cast<ThreadParms*>(arg);
    	// BucketDL<ElementType> *bdl = reinterpret_cast<BucketDL<ElementType> *>(arg);
    	TupleBucketDataList *tbdl = parms->tbdl;
    	int numBucketsToConsume = parms->count;
    	int threadNumber = parms->threadNumber;
    	int i;
    	//ElementType val;
    	TupleType val;
    	uint32_t it;
    	int bucketIndex;
    	
        int cnt = 0;
        for (i = 0; i < numBucketsToConsume; i++) 
    	{
    		bucketIndex = (threadNumber * numBucketsToConsume) + i;
    		it = tbdl->getIterator(bucketIndex);
    		while(tbdl->next(bucketIndex, it, &val))
    			cnt++;
         }
        cout << "consumer " << threadNumber << "consumed " << cnt << endl;      
    	return NULL;
    }
   
   /** @brief class TupleHasher
 *
 */
class TupleHasher1
{
private:
    Hasher fHasher;
    uint32_t fHashLen;

public:
    TupleHasher1(uint32_t len) : fHashLen(len) {}
    uint32_t operator()(const char* v) const
    {
        return fHasher(v, fHashLen);
    }
    /*
    uint32_t operator()(const uint64_t v) const
    {
        return v;
    }*/
};

/** @brief class TupleComparator
 *
 */
class TupleComparator1
{
private:
    uint32_t fCmpLen;

public:
    TupleComparator1(uint32_t len) : fCmpLen(len) {}
    bool operator()(const char* v1, const char *v2) const
    {
        return (memcmp(v1, v2, fCmpLen) == 0);
    }
    /*
    bool operator()(const uint64_t v1, const uint64_t v2) const
    {
        return (v1 == v2);
    }*/
};

   
    void *aggregator(void *arg)
    {
        uint32_t fHashLen = 4;
        TupleType tt;
        tt.second = new char[fHashLen];
        ThreadParms *parms = reinterpret_cast<ThreadParms*>(arg);
        int threadNumber = parms->threadNumber;
        struct timespec ts1, ts2, diff;
        uint32_t size = 585938;
#if 0        
        //typedef std::vector<uint64_t> RIDVec;
        typedef std::pair<int64_t, Elem> Results;
        //typedef std::tr1::unordered_map<uint64_t, Results, TupleHasher, TupleComparator> shmp;
        //typedef std::tr1::unordered_map<uint64_t, Results, TupleHasher, TupleComparator>::iterator TupleHMIter;
        //typedef boost::shared_ptr<TupleHashMap> SHMP;
        typedef std::tr1::unordered_multimap<uint64_t, Results> TupleHashMap;
        typedef boost::shared_ptr<TupleHashMap> SHMP;
        typedef std::tr1::unordered_multimap<uint64_t, Results>::iterator TupleHMIter;
        typedef std::pair<TupleHMIter, TupleHMIter> HashItPair;
        HashItPair hashItPair;
        
        SHMP shmp(new TupleHashMap());
//        TupleHashMap *shmp = new TupleHashMap();
        TupleHMIter iter;           
        uint32_t val = 0;
        uint64_t hv;       
        Hasher hasher;
        bool flag = true;

        for (uint32_t k = 0; k < NUM_BUCKETS/4; k++)
        {
          clock_gettime(CLOCK_REALTIME, &ts1);

          for (uint32_t j = 0; j < 4; j++) 
	  {
            for (uint32_t i = 0; i < size/10; i++)
            {                                         	
                flag = true;
            	memcpy(tt.second, &i, 4);
            	hv = hasher(tt.second, fHashLen);
                iter = shmp->find(hv);
                if (iter != shmp->end())
                {
                  hashItPair = shmp->equal_range(hv);
    	          for(iter = hashItPair.first; iter != hashItPair.second; iter++)
    		  {
                    if (memcmp(iter->second.second.hashStr, tt.second, fHashLen) == 0)
                    {
                      //cout << "real hit" << endl;
                      //updateAggResult<result_t>(tt, hashIt->second.first);
                      iter->second.second.rids.push_back(tt.first);  
                      flag = false; 
                      break;
                    }
                  }
                }
                  
            	if (flag)
            	{		
            	    Results rr;
        	    rr.second.hashStr = new char[fHashLen];
        	    //getAggResult<result_t>(tt, rr.first);
                    memcpy(rr.second.hashStr, tt.second, fHashLen);
                    rr.second.rids.push_back(tt.first);
        	    shmp->insert(std::pair<uint64_t, Results> (hv, rr));			    
                }
            }
             
        }
        cout << "thread " << threadNumber << " vector " << k << " size=" << shmp->size() << endl;
        // clean up hashmap memory        
        for (iter = shmp->begin(); iter != shmp->end(); iter++)
       	    delete [] iter->second.second.hashStr;
       	shmp->clear();
//	shmp.reset(new TupleHashMap());
	clock_gettime(CLOCK_REALTIME, &ts2);
    	timespec_sub(ts1, ts2, diff);
    	cout << "thread " << threadNumber << "do aggregation took " << diff.tv_sec << "s " << diff.tv_nsec << " ns" << endl;
      } 
#endif

#if 0       
        typedef std::vector<uint64_t> RIDVec;
        typedef std::pair<uint64_t, RIDVec> Results;
        typedef std::tr1::unordered_map<char*, Results, TupleHasher1, TupleComparator1> TupleHashMap1;
        typedef std::tr1::unordered_map<char*, Results, TupleHasher1, TupleComparator1>::iterator TupleHMIter1;
        typedef boost::shared_ptr<TupleHashMap1> SHMP1;

        SHMP1 shmp;
        TupleHasher1 tupleHash(fHashLen);
        TupleComparator1 tupleComp(fHashLen);
        
        shmp.reset(new TupleHashMap1(1, tupleHash, tupleComp));
		TupleHMIter1 iter;
		Results rr;
    
        uint32_t val = 0;
        for (uint32_t k = 0; k < NUM_BUCKETS/4; k++)
        {
          clock_gettime(CLOCK_REALTIME, &ts1);
          for (uint32_t j = 0; j < 4; j++)
          {
          for (uint32_t i = 0; i < size/10; i++)
          {          	
        	memcpy(tt.second, &i, 4);
            iter = shmp->find(tt.second);
                
        	if (iter == shmp->end()) 
        	{		
    		    rr.second.clear();
    		    //getAggResult<result_t>(tt, rr.first);   
    		    rr.second.push_back(tt.first);
    		    char*  hashStr = new char[fHashLen];
                memcpy(hashStr, tt.second, fHashLen);
    		    shmp->insert(std::pair<char*, Results> (hashStr, rr));			    
            }
            else
            {
                //updateAggResult<result_t>(tt, hashIt->second.first);
                iter->second.second.push_back(tt.first);   
            }  
          }
        } 
    
        cout << "thread " << threadNumber << " hashmap " << k << " size=" << shmp->size() << endl;

        // clean up hashmap memory
        for (iter = shmp->begin(); iter != shmp->end(); iter++)
        	delete [] iter->first;
        shmp->clear();
        	clock_gettime(CLOCK_REALTIME, &ts2);
    	timespec_sub(ts1, ts2, diff);
    	cout << "thread " << threadNumber << "do aggregation took " << diff.tv_sec << "s " << diff.tv_nsec << " ns" << endl;
    }
#endif

#if 1       
        //typedef std::vector<uint64_t> RIDVec;
        //typedef std::pair<uint64_t, RIDVec> Results;
        typedef std::tr1::unordered_map<char*, int64_t, TupleHasher1, TupleComparator1> TupleHashMap1;
        typedef std::tr1::unordered_map<char*, int64_t, TupleHasher1, TupleComparator1>::iterator TupleHMIter1;
        typedef boost::shared_ptr<TupleHashMap1> SHMP1;
        vector<TupleType> vt;
        vt.reserve(size/10*4);

        SHMP1 shmp;
        TupleHasher1 tupleHash(fHashLen);
        TupleComparator1 tupleComp(fHashLen);
        
        shmp.reset(new TupleHashMap1(1, tupleHash, tupleComp));
		TupleHMIter1 iter;
		int64_t rr;
    
        uint32_t val = 0;
        for (uint32_t k = 0; k < NUM_BUCKETS/4; k++)
        {
          clock_gettime(CLOCK_REALTIME, &ts1);
          for (uint32_t j = 0; j < 4; j++)
          {
          for (uint32_t i = 0; i < size/10; i++)
          {          	
        	memcpy(tt.second, &i, 4);
        	vt.push_back(tt);
            
            iter = shmp->find(tt.second);
                            
        	if (iter == shmp->end()) 
        	{		
    		    //rr.second.clear();
    		    //getAggResult<result_t>(tt, rr.first);   
    		    //rr.second.push_back(tt.first);
    		    char*  hashStr = new char[fHashLen];
                memcpy(hashStr, tt.second, fHashLen);
    		    shmp->insert(std::pair<char*, int64_t> (hashStr, i));			    
            }
            else
            {
                //updateAggResult<result_t>(tt, hashIt->second.first);
                //iter->second.second.push_back(tt.first);   
                iter->second += i;
            }  
          }
        } 
    
        cout << "thread " << threadNumber << " hashmap " << k << " size=" << shmp->size() << endl;
        
        // loop through vector to find match in map for rid 
        vector<TupleType>::iterator it;
        int val = 0;
        for (it = vt.begin(); it != vt.end(); it++)
        {
            iter = shmp->find(it->second);
            if (iter != shmp->end())
                val++;
        }
        cout << "val = " << val << endl;
        // clean up hashmap memory
        for (iter = shmp->begin(); iter != shmp->end(); iter++)
        	delete [] iter->first;
        shmp->clear();
        vt.clear();
        clock_gettime(CLOCK_REALTIME, &ts2);
    	timespec_sub(ts1, ts2, diff);
    	cout << "thread " << threadNumber << "do aggregation took " << diff.tv_sec << "s " << diff.tv_nsec << " ns" << endl;
    }
#endif
      
    }
    
    void *TBDL_producer(void *arg)
    {
        ThreadParms *parms = reinterpret_cast<ThreadParms*>(arg);
        TupleBucketDataList *tbdl = parms->tbdl;
        TupleType t;
        vector<TupleType> vt;
        for (uint32_t i = 0; i < 150000000; i++)
        {            
            t.first = i;
            t.second = new char[4];
            memcpy(t.second, &i, 4);
            vt.push_back(t);
            if (vt.size() == 8192)
            {
                tbdl->insert(vt);
                vt.clear();
            }            
        }
        if (vt.size() > 0)
            tbdl->insert(vt);
    }
    
    void *BDL_producer(void *arg)
    {
        ThreadParms *parms = reinterpret_cast<ThreadParms*>(arg);
        BucketDL<ElementType> *bdl = parms->bdl;
        TupleType t;
        ElementType e;
        vector<ElementType> vt;
        for (uint32_t i = 0; i < 150000000; i++)
        {            
            e.first = i;
            e.second = i;
            //t.second = new char[4];
            //memcpy(t.second, &i, 4);
            vt.push_back(e);
            if (vt.size() == 8192)
            {
                bdl->insert(vt);
                vt.clear();
            }            
        }
        if (vt.size() > 0)
            bdl->insert(vt);
    }
    	
class AggDriver : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(AggDriver);
//CPPUNIT_TEST(aggFilter_group2);
//CPPUNIT_TEST(tuplewsdl);
//CPPUNIT_TEST(bucketdl);
//CPPUNIT_TEST(aggFilter_group1);
//CPPUNIT_TEST(hashjoin);
//CPPUNIT_TEST(hashmap_tuple);
//CPPUNIT_TEST(hashmap_tuple_multi);
//CPPUNIT_TEST(hashmap_ET);
//CPPUNIT_TEST(tuplewsdl_multi);
CPPUNIT_TEST(bdl_multi);
CPPUNIT_TEST_SUITE_END();
typedef boost::shared_ptr<SimpleFilter> SSFP;
       
private:
	ResourceManager fRm;
public:
    void setUp1(JobStepAssociation& in, JobStepAssociation& out) 
    {
        //sleep(20);
        // input - TupleBucket
        AnyDataListSPtr adl1(new AnyDataList());
        TupleBucketDataList *tbdl = new TupleBucketDataList(NUM_BUCKETS, numConsumers, MAX_SIZE, fRm);
        tbdl->setMultipleProducers(0);
        //tbdl->setElementMode(1);

        adl1->tupleBucketDL(tbdl);
        in.outAdd(adl1);
        
        // output - ElementType Bucket
        AnyDataListSPtr adl2(new AnyDataList());
        BucketDataList *bdl = new BucketDataList(NUM_BUCKETS, numConsumers, MAX_ELEMENTS, fRm);
        bdl->setElementMode(1);
        adl2->bucketDL(bdl);
        out.outAdd(adl2); 
         
        // prepare the input datalist
        //ifstream ifs(datapath.c_str(), ios::in);
        uint64_t l_orderkey;
	    uint64_t l_quantity;
        char data[1000];
        //char row[50];  
        char* tok; 
        uint64_t count = 0;
	    uint32_t  rid = 0;
        TupleType *tt = new TupleType(); 
        tbdl->hashLen(sizeof(l_orderkey));
        uint32_t size = sizeof(l_orderkey)+sizeof(l_quantity);
        //tbdl->elementLen(size + sizeof(rid));
        tbdl->elementLen(sizeof(rid), size);
        //char* second = new char[size]; 
        vector<TupleType> v;
         
        timer1.start("input");
        ifstream ifs(datapath.c_str(), ios::in);
        while (!ifs.eof())
        {
            ifs.getline(data, 1000);
            tok = strtok(data, "|");
            count = 0;
            while (tok != NULL)
            {
                if (count == 0) 
                    l_orderkey = atol(tok);
                if (count == 4)
                    l_quantity = atof(tok) * 100;
                count++;
                tok = strtok(NULL, "|");
            }
            for (int i = 0; i < numRuns; i++)
            {
                l_orderkey += 6000000*i;
                tt->first = rid;
                tt->second = new char[size];
                memcpy(tt->second, &l_orderkey, sizeof(l_orderkey));
                memcpy(tt->second+sizeof(l_orderkey), &l_quantity, sizeof(l_quantity)); 
                v.push_back(*tt);
                if (v.size() == 2048)
                {
                    tbdl->insert(v); 
                    v.clear();
                }
                rid++;
            }
            if (rid %printInterval == 0)
                cout << rid << " " << l_orderkey << " " << l_quantity << endl;
        }
        ifs.close();

        if (v.size() > 0)
        {
            tbdl->insert(v); 
            //for (uint32_t i = 0; i < v.size(); i++)
            //    delete [] (v[i].second);
            v.clear();
        }
        tbdl->endOfInput();
        timer1.stop("input");
        cout << "input size=" << tbdl->totalSize() << endl;
        //ifs.close();
        delete tt;
    }
       
    int64_t convertValueNum(const string& str, const CalpontSystemCatalog::ColType& ct, bool isNull )
    {
    	//if (str.size() == 0 || isNull ) return valueNullNum(ct);
    
    	int64_t v = 0;
    
    	boost::any anyVal = DataConvert::convertColumnData(ct, str, false);
    
    	switch (ct.colDataType)
    	{
    	case CalpontSystemCatalog::BIT:
    		v = boost::any_cast<bool>(anyVal);
    		break;
    	case CalpontSystemCatalog::TINYINT:
    		v = boost::any_cast<char>(anyVal);
    		break;
    	case CalpontSystemCatalog::SMALLINT:
    		v = boost::any_cast<int16_t>(anyVal);
    		break;
    	case CalpontSystemCatalog::MEDINT:
    	case CalpontSystemCatalog::INT:
    		v = boost::any_cast<int32_t>(anyVal);
    		break;
    	case CalpontSystemCatalog::BIGINT:
    		v = boost::any_cast<long long>(anyVal);
    		break;
    	case CalpontSystemCatalog::FLOAT:
    		{
            		float i = boost::any_cast<float>(anyVal);
              		v = (Int64)i;
    		}
    		break;
    	case CalpontSystemCatalog::DOUBLE:
    		{
            		double i = boost::any_cast<double>(anyVal);
              		v = (Int64)i;
    		}
    		break;
    	case CalpontSystemCatalog::CHAR:
    	case CalpontSystemCatalog::VARCHAR:
    	case CalpontSystemCatalog::BLOB:
    	case CalpontSystemCatalog::CLOB:
    	  {
    		//v = boost::any_cast<string>(anyVal);
    		        string i = boost::any_cast<string>(anyVal);
              		v = *((Int64 *) i.c_str());
    	  }
    
    		break;
    	case CalpontSystemCatalog::DATE:
    		v = boost::any_cast<uint32_t>(anyVal);
    		break;
    	case CalpontSystemCatalog::DATETIME:
    		v = boost::any_cast<uint64_t>(anyVal);
    		break;
    	case CalpontSystemCatalog::DECIMAL:
    		v = boost::any_cast<long long>(anyVal);
    		break;
    	default:
    		break;
    	}
    
    	return v;
    }
        
    void tuplewsdl_multi()
    {
      TupleBucketDataList *tbdl = new TupleBucketDataList(NUM_BUCKETS, 1, MAX_SIZE, fRm);
        tbdl->setMultipleProducers(true);		
		tbdl->setElementMode(1); // RID_VALUE
		tbdl->hashLen(4);
        tbdl->elementLen(4, 12);
		pthread_t producer[4];
		ThreadParms producerThreadParms[4];
        
        timer1.start("insert-tbdl");
		for (uint32_t i = 0; i < 4; i++)
		{
			producerThreadParms[i].tbdl          = tbdl;
			producerThreadParms[i].threadNumber = i;
			//producerThreadParms[i].count        = ::count;
		    pthread_create(&producer[i], NULL,
			TBDL_producer, &producerThreadParms[i]);
		}
		for (uint32_t i = 0; i < 4; i++)
		    pthread_join(producer[i], NULL);
		tbdl->endOfInput();
		timer1.stop("insert-tbdl");
		cout << "tbdl finish insert " << tbdl->totalSize() << endl;
		//timer1.finish();
    }
    
    void bdl_multi()
    {
      BucketDL<ElementType> *bdl = new BucketDL<ElementType>(128, 1, MAX_ELEMENTS, fRm);
        bdl->setMultipleProducers(true);		
		bdl->setElementMode(1); // RID_VALUE
		bdl->setDiskElemSize ( 4, 4 );
		pthread_t producer[4];
		ThreadParms producerThreadParms[4];
        
        timer1.start("insert-bdl");
		for (uint32_t i = 0; i < 4; i++)
		{
			producerThreadParms[i].bdl          = bdl;
			producerThreadParms[i].threadNumber = i;
			//producerThreadParms[i].count        = ::count;
		    pthread_create(&producer[i], NULL,
			BDL_producer, &producerThreadParms[i]);
		}
		for (uint32_t i = 0; i < 4; i++)
		    pthread_join(producer[i], NULL);
		bdl->endOfInput();
		timer1.stop("insert-bdl");
		cout << "bdl finish insert " << bdl->totalSize() << endl;
		timer1.finish();
    }
    
    void tuplewsdl()
    {
      TupleBucketDataList *tbdl = new TupleBucketDataList(NUM_BUCKETS, numConsumers, MAX_SIZE, fRm);
        // prepare the input datalist
        //ifstream ifs(datapath.c_str(), ios::in);
        uint64_t l_orderkey, l_quantity;
        char data[1000];
        //char row[50];  
        char* tok; 
        uint64_t count = 0, rid = 0;
        TupleType *tt = new TupleType(); 
        tbdl->hashLen(sizeof(l_orderkey));
        uint32_t size = sizeof(l_orderkey)+sizeof(l_quantity);
        //tbdl->elementLen(size + sizeof(rid));
        tbdl->elementLen(sizeof(rid), size);
        //char* second = new char[size]; 
        vector<TupleType> v;
         
        timer1.start("tuple-input");
        int i;
        for (i = 0; i < numRuns; i++)
        {
            ifstream ifs(datapath.c_str(), ios::in);
            while (!ifs.eof())
            {
                ifs.getline(data, 1000);
                tok = strtok(data, "|");
                count = 0;
                while (tok != NULL)
                {
                    if (count == 0) 
                        l_orderkey = 6000000*i + atol(tok);
                    if (count == 4)
                        l_quantity = atof(tok) * 100;
                    count++;
                    tok = strtok(NULL, "|");
                }
                tt->first = rid;
                tt->second = new char[size];
                memcpy(tt->second, &l_orderkey, sizeof(l_orderkey));
                memcpy(tt->second+sizeof(l_orderkey), &l_quantity, sizeof(l_quantity)); 
                v.push_back(*tt);
                if (v.size() == 2048)
                {
                    tbdl->insert(v); 
                    for (uint32_t i = 0; i < v.size(); i++)
                        v[i].deleter();
                    v.clear();
                }
                rid++;
                if (rid % printInterval == 0)
                    cout << rid << " " << l_orderkey << " " << l_quantity << endl;
            }
            ifs.close();
        }
        if (v.size() > 0)
        {
            tbdl->insert(v); 
            for (uint32_t i = 0; i < v.size(); i++)
                delete [] (v[i].second);
            v.clear();
        }
        tbdl->endOfInput();
        timer1.stop("tuple-input");
        cout << "input size=" << tbdl->totalSize() << endl;
        //ifs.close();
        delete tt;
        
        ThreadParms thparms;
		thparms.count = NUM_BUCKETS / NUM_THREADS;
		thparms.tbdl = tbdl;
		pthread_t consumer[NUM_THREADS];	
		
		ThreadParms thParms[NUM_THREADS];	
		timer1.start("tuple-consumer");	
        for (i = 0; i < NUM_THREADS; i++)
		{
			thparms.threadNumber = i;
			thParms[i] = thparms;
            pthread_create(&consumer[i], NULL, tupleConsumer, &thParms[i]);
		}

        for (i = 0; i < NUM_THREADS; i++)
            pthread_join(consumer[i], NULL);	
        timer1.stop("tuple-consumer");
                        
        timer1.finish();
        delete tbdl;	    
    }
        
	void aggFilter_group1()
	{	     
/*
	    int tm;
          for (int m = 0; m < 1024; m++)
          {
            char *a = new char[5*1024*1024];
            memset(a,0,5*1024*1024);
            memcpy(&tm, a, 4);
          }
*/
	    //for (int i = 0; i < 10; i++) {
	    JobStepAssociation in, out;
	    setUp1(in, out);
	    CalpontSystemCatalog::TableName tn("test", "lineitem");	    
	    string s_l_orderkey = "test.lineitem.l_orderkey";
	    string s_l_quantity = "test.lineitem.l_quantity";
	    uint32_t sessionid = 1;
	    uint32_t txnId = 1;
	    uint32_t verId = 1;
	    uint16_t stepID = 0;
	    uint32_t statementID = 1;
	    boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionid);
	        
	    SRCP srcp;
	    SimpleColumn *l_orderkey = new SimpleColumn(s_l_orderkey, sessionid);
	    SimpleColumn *l_quantity = new SimpleColumn(s_l_quantity, sessionid);
	    CalpontSystemCatalog::ROPair ropair = csc->tableRID(tn);
	    CalpontSystemCatalog::OID tableoid = ropair.objnum;
	    
	    // sum(l_quantity) group by l_ordereky
	    srcp.reset(l_quantity);
	    AggregateColumn *ac = new AggregateColumn("sum", srcp.get()->clone(), sessionid);
  
	    ac->addProjectCol(srcp);
	    srcp.reset(l_orderkey);
	    ac->addGroupByCol(srcp); 
	     
	    // constant column 318 
	    ConstantColumn *cc = new ConstantColumn(318, ConstantColumn::NUM);
	    // >
	    Operator *op = new Operator(">");
	    SOP sop(op);
	    // sum(l_quantity) > 318
	    SSFP ssfp;
	    SimpleFilter *sf = new SimpleFilter(sop, ac, cc);
	    ssfp.reset(sf);
	    SJSTEP step;
 	     
	    AggregateFilterStep *afs = 
	        new AggregateFilterStep(in, 
        		                    out,
        		                    ac->functionName(),
        		                    ac->groupByColList(),
        	                        ac->projectColList(),
        	                        ac->functionParms(),	    
        	                        tableoid,
        		                    sessionid,
        		                    txnId,
        		                    verId,
        		                    stepID,
					statementID, fRm);
                       
        // output column l_orderkey;
        afs->outputCol(dynamic_cast<SimpleColumn*>(srcp.get())->oid());
        step.reset(afs);
        // one column case, nomalize filter value
        CalpontSystemCatalog::ColType ct;
        int64_t intVal;
        string strVal;
        //int8_t cop = op2num(sop);
        if (typeid((*ac->functionParms().get())) == typeid(SimpleColumn))
        {
            SimpleColumn* sc = reinterpret_cast<SimpleColumn*>(ac->functionParms().get());
            ct = csc->colType(sc->oid());
            intVal = convertValueNum(cc->constval(), ct, false); 
            afs->addFilter(COMPARE_GT, intVal);
        } 
        else
        {
            if (cc->type() == ConstantColumn::NUM)
            {
                intVal = atol(cc->constval().c_str());
                afs->addFilter(COMPARE_GT, intVal, false);
            }
            else if (cc->type() == ConstantColumn::LITERAL)
                afs->addFilter(COMPARE_GT, cc->constval(), false);
        }
        
        timer1.start("agg");
        afs->run(); 
        afs->join();
        timer1.stop("agg");
        timer1.finish(); 
        //}
	}	
    
    void bucketdl()
    {
      BucketDL<ElementType> *bdl = new BucketDL<ElementType>(NUM_BUCKETS, 1, MAX_ELEMENTS, fRm);
        bdl->setElementMode(1);
        ElementType *et = new ElementType();

        uint64_t l_orderkey, l_quantity;
        char data[1000];
        char* tok; 
        uint64_t count = 0, rid = 0;
        TupleType *tt = new TupleType(); 

        vector<ElementType> v;
        timer1.start("bucket-input");
        int i;
        for (i = 0; i < numRuns; i++)
        {
            ifstream ifs(datapath.c_str(), ios::in);
            while (!ifs.eof())
            {
                ifs.getline(data, 1000);
                tok = strtok(data, "|");
                count = 0;
                while (tok != NULL)
                {
                    if (count == 0) 
                        l_orderkey = 6000000*i + atol(tok);
                    if (count == 4)
                        l_quantity = atof(tok) * 100;
                    count++;
                    tok = strtok(NULL, "|");
                }
                et->first = rid;
                et->second = rid;
                //tt->second = new char[16];
                //memcpy(tt->second, &l_orderkey, sizeof(l_orderkey));
                //memcpy(tt->second+sizeof(l_orderkey), &l_quantity, sizeof(l_quantity)); 
                v.push_back(*et);
                if (v.size() == 2048)
                {
                    bdl->insert(v); 
                    v.clear();
                }
                rid++;
                //delete [] tt->second;
                if (rid %printInterval == 0)
                    cout << rid << " " << l_orderkey << " " << l_quantity << endl;
            }
            ifs.close();
        }
        if (v.size() > 0)
        {
            bdl->insert(v); 
            v.clear();
        }
        bdl->endOfInput();
        timer1.stop("bucket-input");
        cout << "input size=" << bdl->totalSize() << endl;
        delete tt;
        delete et;
        
        ThreadParms thparms;
		thparms.count = NUM_BUCKETS / NUM_THREADS;
		thparms.bdl = bdl;
		pthread_t consumer[NUM_THREADS];	
		
		ThreadParms thParms[NUM_THREADS];	
		timer1.start("bucket-consumer");	
        for (i = 0; i < NUM_THREADS; i++)
		{
			thparms.threadNumber = i;
			thParms[i] = thparms;
            pthread_create(&consumer[i], NULL, bucketConsumer, &thParms[i]);
		}

        for (i = 0; i < NUM_THREADS; i++)
            pthread_join(consumer[i], NULL);	
        timer1.stop("bucket-consumer");
                        
        timer1.finish();
        delete bdl;
    }
    
    void aggFilter_group2()
	{	   
	    CalpontSystemCatalog::TableName tn("test", "lineitem");	    
	    string s_l_orderkey = "test.lineitem.l_orderkey";
	    string s_l_quantity = "test.lineitem.l_quantity";
	    string s_l_partkey = "test.lineitem.l_partkey";
	    uint32_t sessionid = 1;
	    uint32_t txnId = 1;
	    uint32_t verId = 1;
	    uint16_t stepID = 0;
	    uint32_t statementID = 1;
	    boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionid);
	    
	    SRCP srcp;
	    SimpleColumn *l_orderkey = new SimpleColumn(s_l_orderkey, sessionid);
	    SimpleColumn *l_quantity = new SimpleColumn(s_l_quantity, sessionid);
	    SimpleColumn *l_partkey = new SimpleColumn(s_l_partkey, sessionid);
	    CalpontSystemCatalog::ROPair ropair = csc->tableRID(tn);
	    CalpontSystemCatalog::OID tableoid = ropair.objnum;
	    
	    // sum(l_quantity) group by l_ordereky
	    AggregateColumn *ac = new AggregateColumn("sum", l_quantity, sessionid);
	    srcp.reset(l_quantity);
	    ac->addProjectCol(srcp);
	    srcp.reset(l_orderkey);
	    ac->addGroupByCol(srcp);
	    srcp.reset(l_partkey);
	    ac->addGroupByCol(srcp);
	    
	    // constant column 318
	    ConstantColumn *cc = new ConstantColumn(318, ConstantColumn::NUM);
	    // >
	    Operator *op = new Operator(">");
	    SOP sop(op);
	    // sum(l_quantity) > 318
	    SimpleFilter *sf = new SimpleFilter(sop, ac, cc);
	    
	    // aggregate filter step
	    /** @brief Constructor */
	    JobStepAssociation in;
	    JobStepAssociation out;
	    AggregateFilterStep *afs = 
	        new AggregateFilterStep(in, 
        		                    out,
        		                    ac->functionName(),
        		                    ac->groupByColList(),
        	                        ac->projectColList(),
        	                        ac->functionParms(),	    
        	                        tableoid,
        		                    sessionid,
        		                    txnId,
        		                    verId,
        		                    stepID,
					statementID, fRm);
    afs->run();
    afs->join();        		                   
	}	
	
	void hashjoin()
	{
	  BucketDL< ElementType > A(NUM_BUCKETS, 1, MAX_ELEMENTS, fRm);
		BucketDL< ElementType > B(NUM_BUCKETS, 1, MAX_ELEMENTS, fRm);
		BucketDL< ElementType > C(NUM_BUCKETS, 1, MAX_ELEMENTS, fRm);
		BucketDL< ElementType > D(NUM_BUCKETS, 1, MAX_ELEMENTS, fRm);
		A.setDiskElemSize( 4, 4 );
		B.setDiskElemSize( 4, 4 );
		A.setElementMode(1);
		A.setHashMode(1);
		B.setElementMode(1);
		B.setHashMode(1);
		int i;
		timer1.start("insert");
		for (i = 0; i < 6000000*numRuns; i++)
		    A.insert(ElementType(i, i));
		A.endOfInput();
		for (i = 0; i < 1500000*numRuns; i++)		    
		    B.insert(ElementType(i, i+1000000));
		B.endOfInput();
		timer1.stop("insert");
		C.setElementMode(1);
		D.setElementMode(1);
	    BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	    timer1.start("hashjoin");
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, INNER, &dlTimes);
		hj->performJoin();
	    timer1.stop("hashjoin");
        timer1.finish();		
	    cout << "hash join output size: " << resultA->totalSize() << "/" << resultB->totalSize() << endl;
	}
	
	typedef struct Elem
	{
	    char* hashStr;
	    //int64_t result;
	    vector<uint64_t> rids;
	} Elem;
	

	void hashmap_tuple()
	{
	  int tm;
	  for (int m = 0; m < 1024; m++)
	  {
	    char *a = new char[5*1024*1024];
	    memset(a,0,5*1024*1024);
	    memcpy(&tm, a, 4);
	  }
	  uint32_t fHashLen = 4;
          TupleType tt;
          tt.second = new char[fHashLen]; 
          uint64_t size = 585937;     
          uint64_t i = 0, j = 0;
#if 1        
          pthread_t agg[NUM_THREADS];	
          ThreadParms thParms[NUM_THREADS];	
          ThreadParms thParm;
          struct timespec ts1, ts2, diff;

          clock_gettime(CLOCK_REALTIME, &ts1);
		
          for (i = 0; i < NUM_THREADS; i++)
          {
	    thParm.threadNumber = i;
	    thParms[i] = thParm;
            pthread_create(&agg[i], NULL, aggregator, &thParms[i]);
	  }

          for (i = 0; i < NUM_THREADS; i++)
            pthread_join(agg[i], NULL);
        
        clock_gettime(CLOCK_REALTIME, &ts2);
        timespec_sub(ts1, ts2, diff);
        cout << "aggregation took " << diff.tv_sec << "s " << diff.tv_nsec << "ns" << endl;
#endif
#if 0
        //typedef std::vector<uint64_t> RIDVec;
        typedef std::pair<int64_t, Elem> Results;
        //typedef std::tr1::unordered_map<uint64_t, Results, TupleHasher, TupleComparator> shmp;
        //typedef std::tr1::unordered_map<uint64_t, Results, TupleHasher, TupleComparator>::iterator TupleHMIter;
        //typedef boost::shared_ptr<TupleHashMap> SHMP;
        typedef std::tr1::unordered_multimap<uint64_t, Results> TupleHashMap;
        typedef boost::shared_ptr<TupleHashMap> SHMP;
        typedef std::tr1::unordered_multimap<uint64_t, Results>::iterator TupleHMIter;
        typedef std::pair<TupleHMIter, TupleHMIter> HashItPair;
        HashItPair hashItPair;
        
        SHMP shmp(new TupleHashMap());
        //TupleHashMap *shmp = new TupleHashMap();
        TupleHMIter iter;           
        uint32_t val = 0;
        uint64_t hv;
        Elem elem;
        Results rr;
        Hasher hasher;
        bool flag = true;
        timer1.start("vectormap");
	for (int k = 0; k < 1; k++)
        {
	  for (j = 0; j < 4; j++) 
	  {
            for (i = 0; i < size/10; i++)
            {                                         	
                flag = true;
            	memcpy(tt.second, &i, 4);
            	hv = hasher(tt.second, fHashLen);
                iter = shmp->find(hv);
                if (iter != shmp->end())
                {
                    hashItPair = shmp->equal_range(hv);
    		    for(iter = hashItPair.first; iter != hashItPair.second; iter++)
    		    {
                        if (memcmp(iter->second.second.hashStr, tt.second, fHashLen) == 0)
                        {
                            //cout << "real hit" << endl;
                            //updateAggResult<result_t>(tt, hashIt->second.first);
                            iter->second.second.rids.push_back(tt.first);  
                            flag = false; 
                            break;
                        }
                    }
                }
                  
            	if (flag)
            	{		
        	    rr.second.hashStr = new char[fHashLen];
                    memcpy(rr.second.hashStr, tt.second, fHashLen);
                    rr.second.rids.clear();
                    rr.second.rids.push_back(tt.first);
        	    shmp->insert(std::pair<uint64_t, Results> (hv, rr));			    
                }
            }             
        }

        cout << "vector size=" << shmp->size() << endl;

        // clean up hashmap memory        
        for (iter = shmp->begin(); iter != shmp->end(); iter++)
       	    delete [] iter->second.second.hashStr;
	shmp->clear();
    }
    timer1.stop("vectormap");
    timer1.finish();
        
#endif
#if 0    
        typedef std::vector<uint64_t> RIDVec;
        typedef std::pair<uint64_t, RIDVec> Results;
        typedef std::tr1::unordered_map<char*, Results, TupleHasher, TupleComparator> TupleHashMap1;
        typedef std::tr1::unordered_map<char*, Results, TupleHasher, TupleComparator>::iterator TupleHMIter1;
        typedef boost::shared_ptr<TupleHashMap1> SHMP1;

        SHMP1 shmp;
        TupleHasher tupleHash(fHashLen);
        TupleComparator tupleComp(fHashLen);
        
        shmp.reset(new TupleHashMap1(1, tupleHash, tupleComp));
		TupleHMIter1 iter;
		Results rr;
    
        uint32_t val = 0;
        timer1.start("uniquemap");
        for (j = 0; j < 4; j++)
        for (i = 0; i < size/10; i++)
        {          	
        	memcpy(tt.second, &i, 4);
            iter = shmp->find(tt.second);
                
        	if (iter == shmp->end()) 
        	{		
    		    rr.second.clear();
    		    //getAggResult<result_t>(tt, rr.first);   
    		    rr.second.push_back(tt.first);
    		    char*  hashStr = new char[fHashLen];
                memcpy(hashStr, tt.second, fHashLen);
    		    shmp->insert(std::pair<char*, Results> (hashStr, rr));			    
            }
            else
            {
                //updateAggResult<result_t>(tt, hashIt->second.first);
                iter->second.second.push_back(tt.first);   
            }  
        } 
    
		timer1.stop("uniquemap");
        timer1.finish();
        cout << "hashmap size=" << shmp->size() << endl;
    
        // clean up hashmap memory
        for (iter = shmp->begin(); iter != shmp->end(); iter++)
        	delete [] iter->first;
        shmp->clear();
#endif        	
	}

#if 0	
	void hashmap_tuple_multi()
	{
		uint32_t fHashLen = 4;
		TupleHasher tupleHash(fHashLen);
        TupleComparator tupleComp(fHashLen);  
        TupleType tt;     
        
        //typedef std::vector<uint64_t> RIDVec;
        typedef std::pair<uint64_t, uint64_t> Results;
        typedef std::tr1::unordered_multimap<char*, Results, TupleHasher, TupleComparator> TupleHashMap;
        typedef std::tr1::unordered_multimap<char*, Results, TupleHasher, TupleComparator>::iterator TupleHMIter;
        typedef boost::shared_ptr<TupleHashMap> SHMP;
        std::pair<TupleHMIter, TupleHMIter> hashItPair;
                
        SHMP shmp;
        shmp.reset(new TupleHashMap(1, tupleHash, tupleComp));
		TupleHMIter iter;
		Results rr;
		Hasher fHasher;
    
        timer1.start("multi_insert");
        uint32_t i = 0, j = 0;
        uint32_t val = 0;

        for ( j = 0; j < 4; j++)
        for ( i = 0; i < 1171875; i++)
        {    
        	tt.second = new char[fHashLen]; 
        	memcpy(tt.second, &i, fHashLen);
        	rr.first = j; 
        	rr.second = i;
        	shmp->insert(std::pair<char*, Results> (tt.second, rr));   
        }
        timer1.stop("multi_insert");
        cout << "hashmap size=" << shmp->size() << endl;
    
        tt.second = new char[fHashLen]; 
        timer1.start("multi_result");
    
        iter = shmp->begin();
        char* key;
        if (iter != shmp->end())
            key = iter->first;
        uint32_t ct = 0;
        for (iter = ++iter; iter != shmp->end(); iter++)
        {
            if (memcmp(iter->first, key, 4) == 0)
                val += iter->second.first;
            else
            {
                key = iter->first;
                val = iter->second.first;
                ct++;
            }
        }
		timer1.stop("multi_result");
		
		timer1.start("base");
		for ( j = 0; j < 4; j++)
        for ( i = 0; i < 1171875; i++)
        {    
        	tt.second = new char[fHashLen]; 
        	memcpy(tt.second, &i, fHashLen);
        }
		timer1.stop("base");
        timer1.finish();
        cout << "group val=" << val << endl;
    
    // clean up hashmap memory
    for (iter = shmp->begin(); iter != shmp->end(); iter++)
    	delete [] iter->first;
	}
#endif	
	void hashmap_ET()
	{
		typedef std::list<ElementType> hashList_t;
		typedef std::list<ElementType>::iterator hashListIter_t;

		//typedef std::tr1::unordered_multimap<ElementType::second_type, ElementType::first_type> hash_t;
		//typedef std::tr1::unordered_multimap<ElementType::second_type, ElementType::first_type>::iterator hashIter_t;
		//typedef std::tr1::unordered_multimap<ElementType::second_type, ElementType::first_type>::value_type hashPair_t;
			
		typedef std::tr1::unordered_map<ElementType::second_type, hashList_t> hash_t;
		typedef std::tr1::unordered_map<ElementType::second_type, hashList_t>::iterator hashIter_t;
		typedef std::tr1::unordered_map<ElementType::second_type, hashList_t>::value_type hashPair_t;
		
		hash_t hashmap;
		hashIter_t it;
		hashList_t list;
    
    timer1.start("et_insert");
    ElementType et;
    //while (true)
    for (int i = 0; i < 1200000; i++)
    {    	
    	list.push_back(et);
    	et.second = i;
    	hashmap.insert(hashPair_t(et.second, list));
    	list.clear();
    }
    timer1.stop("et_insert");
    cout << "hashmap size=" << hashmap.size() << endl;
   
   	int val = 0, i, j;
    timer1.start("et_find");
    for (j = 0; j < 4; j++)
    for (i = 0; i < 1200000; i++)
    {      
    	et.second = i;
      it = hashmap.find(i);           
      if (it == hashmap.end()) 
    	{		
		    list.clear();
		    //getAggResult<result_t>(tt, rr.first);   
		    list.push_back(et);
		    hashmap.insert(hashPair_t (et.second, list));			    
        }
        else
        {
            it->second.push_back(et);   
        }  
    }
		timer1.stop("et_find");
    timer1.finish();
    cout << "hashmap size=" << hashmap.size() << endl;
    }
}; 

CPPUNIT_TEST_SUITE_REGISTRATION(AggDriver);
 

int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}
