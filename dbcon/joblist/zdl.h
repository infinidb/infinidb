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
*   $Id: zdl.h 8436 2012-04-04 18:18:21Z rdempsey $
*
*
***********************************************************************/

/** @file 
 * class ZDL interface
 */

#include <boost/function.hpp>
#include <boost/function_equal.hpp>
#include <boost/shared_ptr.hpp>
#include <sstream>
#include <cmath>
#include <cassert>
#include <vector>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#include "wsdl.h"
#include "calpontsystemcatalog.h"
#include "configcpp.h"
#include "blocksize.h"

#ifndef _ZDL_HPP_
#define _ZDL_HPP_

/** Debug macro */
#define ZDL_DEBUG 0
#if ZDL_DEBUG
#define ZDEBUG std::cout
#else
#define ZDEBUG if (false) std::cout
#endif

namespace joblist {
//const uint64_t defaultMaxElementsInMem = 32 * 1024 * 1024;     // 32M
//const uint64_t defaultNumBuckets = 128;
//const uint64_t defaultMaxElementsPerBuckert = 16 * 1024 * 1024; // 16M

/** @brief class BucketDL
 *
 */
template<typename element_t>
class ZDL : public DataList<element_t>
{
	typedef DataList<element_t> base;
    typedef boost::shared_ptr< WSDL<element_t> > SWSP;
    typedef boost::shared_ptr< WSDL<RIDElementType> > RIDSWSP;
    typedef std::vector<SWSP> WSVec;
    typedef std::vector<RIDSWSP> RIDWSVec;
    
	public:
    	enum ElementMode {
        	RID_ONLY,
        	RID_VALUE
    	};

		/** Main constuctor.
		 * @param numConsumers The number of consumers that will eventually read this DL.
		 */
        ZDL(uint64_t numConsumers, ResourceManager& rm);
		virtual ~ZDL();

		// datalist interface.  
		virtual void insert(const element_t &e);
		virtual void insert(const std::vector<element_t> &e);
		virtual void endOfInput();
		virtual uint64_t getIterator();
		virtual bool next(uint64_t it, element_t *e);
		// multithreaded read interface
		//bool next (uint64_t bucketID, uint64_t it, element_t *e);
		virtual void setMultipleProducers(bool);

		/** Returns the size of the specified bucket */
		uint64_t size(uint64_t bucket);

		/** Returns the total number of elements stored */
		uint64_t totalSize();

		/** Returns the number of buckets */
		uint64_t bucketCount();

		/** Get the number of consumers */
		virtual uint getNumConsumers() const {return numConsumers; }
		
		/** Reset the number of consumers for the datalist */
		void resetNumConsumers(uint numConsumers);
		
		/** consumers block here until endOfInput */
		void waitForConsumePhase();
		
		/** set element type for buckets */
		void setElementMode (const uint mode);

		/** Total number of files and filespace used for temp files */
		void totalFileCounts(uint64_t& numFiles, uint64_t& numBytes) const;

		/** This class does employ temp disk */
		virtual bool useDisk() const { return true; }

		/** Sets the size of the element components that are saved to disk */
		virtual void setDiskElemSize(uint32_t size1st,uint32_t size2nd);

		/** Configure this ZDL to behave as a stub; all inserts are dropped */
		void dropAllInserts() { fDropInserts = true; }

		/** Enables disk I/O time logging */
		void enableDiskIoTrace();

		/** Returns the disk I/O time in seconds */
		bool totalDiskIoTime(uint64_t& w, uint64_t& r);

		/** Returns the reference of the disk I/O info list */
		std::list<DiskIoInfo>& diskIoInfoList(uint64_t bucket);

	protected:


	private:
		// Declare default constructors but don't define to disable their use
		explicit ZDL();
		explicit ZDL(const ZDL<element_t> &);
		ZDL<element_t>& operator=(const ZDL<element_t> &);

		uint64_t nextBucket(uint64_t bucketID);
		
		
		void insert_r(const RIDElementType &e);
		void insert_rv(const element_t &e);
		void endOfInput_r();
		void endOfInput_rv();
		uint64_t getIterator_r();
		uint64_t getIterator_rv();
		bool next_r(uint64_t it, RIDElementType *e);
		bool next_rv(uint64_t it, element_t *e);
		void setMultipleProducers_r(bool);
		void setMultipleProducers_rv(bool);
		uint64_t nextBucket_r(uint64_t bucketID);
		uint64_t nextBucket_rv(uint64_t bucketID);
		void checkBucket_r(uint64_t bucketID, bool lockFlag=true);
		void checkBucket_rv(uint64_t bucketID, bool lockFlag=true);
		void totalFileCounts_r(uint64_t& numFiles, uint64_t& numBytes) const;
		void totalFileCounts_rv(uint64_t& numFiles, uint64_t& numBytes)const;

		ResourceManager& fRm;
		uint64_t numBuckets;
		uint64_t numConsumers;
// 		uint64_t maxElements;
		uint64_t bitShifter;
		int64_t phase;  // 0 = produce phase, 1 = consume phase
		WSVec rvbuckets; 
		RIDWSVec rbuckets;
		bool multiProducer;
		uint64_t lowestBucketID;
		uint64_t largestBucketID;
		boost::condition nextBucketReady;
		boost::condition consumePhase;	
		uint64_t maxElementsInMem;
		uint64_t insertCheckingThresh;
		uint64_t maxElemPerBucket;
		uint64_t nextBucketID;
		uint elementMode;   // 0 = RIDElementType, 1 = element_t (rid/value)
		bool fDropInserts;
		bool fTraceOn;
};

template<typename element_t>
ZDL<element_t>::ZDL(uint64_t nc, ResourceManager& rm) :
		base(), fRm(rm), numBuckets(defaultNumBuckets), numConsumers(nc), phase(0), 
		multiProducer(false), lowestBucketID(defaultNumBuckets), largestBucketID(0), 
		maxElementsInMem(rm.getZdl_MaxElementsInMem()),
		maxElemPerBucket(rm.getZdl_MaxElementsPerBucket()), 
		nextBucketID(0), elementMode(RID_ONLY), fDropInserts(false), fTraceOn(false)
{
    /* 1. start with 1 bucket
       2. calculate maxElements, bitShifter
       3. initialize condition and buckets 
    */
    uint64_t i;
//     numConsumers = nc;  
//     nextBucketID = 0;
//     multiProducer = false;
   

	if ((maxElemPerBucket - 1) & maxElemPerBucket)
		throw std::runtime_error("ZDL: ZDL_MaxElementsPerBucket should be a power of 2.");

//     config::Config *config = config::Config::makeConfig();
//     std::string strVal;
// 	strVal = config->getConfig("ZDL", "ZDL_MaxElementsPerBucket");
// 	if (strVal.size() > 0){
// 		maxElemPerBucket = config::Config::uFromText(strVal);
// 		if ((maxElemPerBucket - 1) & maxElemPerBucket)
//    			throw std::runtime_error("ZDL: ZDL_MaxElementsPerBucket should be a power of 2.");
// 	}
// 	else
// 		maxElemPerBucket = defaultMaxElementsPerBuckert;
// 		
//     strVal = config->getConfig("ZDL", "ZDL_MaxElementsInMem");
// 	if (strVal.size() > 0){
// 		maxElementsInMem = config::Config::uFromText(strVal);
// 	}
// 	else
// 		maxElementsInMem = defaultMaxElementsInMem;
    
    // calculate shifter		   
    for (i = 1; i <= 32; i++) {
		if ((maxElemPerBucket >> i) & 1 ){
		    bitShifter = i;
		    break;
		}
	}
	
    // initialize buckets
    for (i = 0; i < defaultNumBuckets; i++) {
        rbuckets.push_back(RIDSWSP(new WSDL<RIDElementType>
				   (numConsumers, maxElemPerBucket, fRm) ));
    }
        
	insertCheckingThresh = maxElementsInMem / defaultNumBuckets;
	//pthread_cond_init(&nextBucketReady, NULL);
    //pthread_cond_init(&consumePhase, NULL);
    ZDEBUG << "ZDL-" << this << " maxMem=" << maxElemPerBucket
           << " shifter=" << bitShifter 
           << " #consumer= " << numConsumers << std::endl;
}	   

template<typename element_t>
ZDL<element_t>::~ZDL()
{
    //pthread_cond_destroy(&nextBucketReady);
    //pthread_cond_destroy(&consumePhase);    
}

template<typename element_t>
void ZDL<element_t>::setMultipleProducers(bool b) 
{
    uint64_t i;
    multiProducer = b;
    if (elementMode == RID_ONLY)
	    for (i = 0; i < rbuckets.size(); i++)
            rbuckets[i].get()->setMultipleProducers(b);
    else
        for (i = 0; i < rvbuckets.size(); i++)
		    rvbuckets[i].get()->setMultipleProducers(b);
}

template<typename element_t>
void ZDL<element_t>::setMultipleProducers_r(bool b) 
{
	uint64_t i;
	multiProducer = b;
    
    for (i = 0; i < rbuckets.size(); i++)
		    rbuckets[i].get()->setMultipleProducers(b);
}

template<typename element_t>
void ZDL<element_t>::setMultipleProducers_rv(bool b) 
{
	uint64_t i;
	multiProducer = b;
    
    for (i = 0; i < rvbuckets.size(); i++)
		    rvbuckets[i].get()->setMultipleProducers(b);
}

template<typename element_t>
void ZDL<element_t>::insert(const element_t &e)
{
	if (fDropInserts)
		return;
    if (elementMode == RID_ONLY){
        RIDElementType rid(e.first);
	    insert_r(rid);
	}
	else
	    insert_rv(e);
}

template<typename element_t>
void ZDL<element_t>::insert_r(const RIDElementType &e)
{   
    /* 1. calculate bucketID
       2. if new bucketID, create new bucket, insert to vector at right pos
       3. update lowestBucketID and largestBucketID
       4. insert to bucket[bucketID]
    */
    uint64_t bucketID = e.first >> bitShifter;    
    
    checkBucket_r(bucketID);
      
    rbuckets[bucketID]->insert(e);  
    if (rbuckets[bucketID]->curSize % insertCheckingThresh == 0){
        uint64_t totalSize = 0;
        uint64_t maxSize = 0;
        uint64_t bigBucket = defaultNumBuckets;
        for (uint64_t i = 0; i < rbuckets.size(); i++){
            uint64_t size = rbuckets[i]->curSize;
            totalSize += size;
            if (size > maxSize){
                bigBucket = i;
                maxSize = size;
            }
        }
        if (totalSize > maxElementsInMem) {
            rbuckets[bigBucket]->flush(insertCheckingThresh);
        }
    }              
}

template<typename element_t>
void ZDL<element_t>::insert_rv(const element_t &e)
{   
    /* 1. calculate bucketID
       2. if new bucketID, create new bucket, insert to vector at right pos
       3. update lowestBucketID and largestBucketID
       4. insert to bucket[bucketID]
    */
    uint64_t bucketID = e.first >> bitShifter;    
    
    checkBucket_rv(bucketID);
    
    rvbuckets[bucketID]->insert(e);  
    if (rvbuckets[bucketID]->curSize % insertCheckingThresh == 0){
        uint64_t totalSize = 0;
        uint64_t maxSize = 0;
        uint64_t bigBucket = defaultNumBuckets;
        for (uint64_t i = 0; i < rvbuckets.size(); i++){
            uint64_t size = rvbuckets[i]->curSize;
            totalSize += size;
            if (size > maxSize){
                bigBucket = i;
                maxSize = size;
            }
        }
        if (totalSize > maxElementsInMem) {
            rvbuckets[bigBucket]->flush(insertCheckingThresh);
        }
    }              
}

template<typename element_t>
void ZDL<element_t>::insert(const std::vector<element_t> &v)
{    
	if (fDropInserts)
		return;

    typename std::vector<element_t>::const_iterator it, end;
    
    end = v.end();
    uint64_t bucketID = 0;
    bool checkFlush = false;
    
    if (multiProducer) base::lock();

try
{
    if (elementMode == RID_ONLY) 
    {
        RIDElementType rid;
        for (it = v.begin(); it != end; ++it) 
        {
            bucketID = (*it).first >> bitShifter;  
            checkBucket_r(bucketID, false);
            rid.first = (*it).first;
            rbuckets[bucketID]->insert_nolock(rid);
            // check threshold
            if (!checkFlush && rbuckets[bucketID]->curSize % insertCheckingThresh == 0)
                checkFlush = true;                        
        }
                  
        if (checkFlush)
        {
            uint64_t totalSize = 0;
            uint64_t maxSize = 0;
            uint64_t bigBucket = defaultNumBuckets;
            for (uint64_t i = 0; i < rbuckets.size(); i++)
            {
                uint64_t size = rbuckets[i]->curSize;
                totalSize += size;
                if (size > maxSize){
                    bigBucket = i;
                    maxSize = size;
                }
            }
            if (totalSize > maxElementsInMem) 
            {
                rbuckets[bigBucket]->flush(insertCheckingThresh, false);
            }      
        }
    }
    else 
    {
        for (it = v.begin(); it != end; ++it) 
        {
            bucketID = (*it).first >> bitShifter;  
            checkBucket_rv(bucketID, false);
            rvbuckets[bucketID]->insert_nolock(*it);              
            if (!checkFlush && rvbuckets[bucketID]->curSize % insertCheckingThresh == 0)
                checkFlush = true;            
        }                
        
        if (checkFlush)
        {
            uint64_t totalSize = 0;
            uint64_t maxSize = 0;
            uint64_t bigBucket = defaultNumBuckets;
            for (uint64_t i = 0; i < rvbuckets.size(); i++)
            {
                uint64_t size = rvbuckets[i]->curSize;
                totalSize += size;
                if (size > maxSize)
                {
                    bigBucket = i;
                    maxSize = size;
                }
            }
            if (totalSize > maxElementsInMem) 
            {
                rvbuckets[bigBucket]->flush(insertCheckingThresh, false);
            }
        }        
    }
}
catch(...)
{
	if (multiProducer) base::unlock();
	std::cerr << __FILE__ << "@" << __LINE__ << " ZDL insert vector caught an exception\n";
	throw;
}
    if (multiProducer) base::unlock();
}

template<typename element_t>
uint64_t ZDL<element_t>::getIterator()
{
    if (elementMode == RID_ONLY)
        return getIterator_r();
    else
        return getIterator_rv();
}

template<typename element_t>
uint64_t ZDL<element_t>::getIterator_r()
{   
    uint64_t id; 
        
    base::lock();    
    waitForConsumePhase();
    base::unlock(); 
    
    if (lowestBucketID >= rbuckets.size()) {
        return 0;
    }
    else
        id = rbuckets[lowestBucketID]->getIterator();
    ZDEBUG << "ZDL-" << this << " consumer get iterator " << id << std::endl;
    
    return id;
}

template<typename element_t>
uint64_t ZDL<element_t>::getIterator_rv()
{   
    uint64_t id; 
        
    base::lock();    
    waitForConsumePhase();
    base::unlock(); 
    
    if (lowestBucketID >= rvbuckets.size()) {
        return 0;
    }
    else
        id = rvbuckets[lowestBucketID]->getIterator();
    ZDEBUG << "ZDL-" << this << " consumer get iterator " << id << std::endl;
    
    return id;
}

template<typename element_t>
void ZDL<element_t>::endOfInput()
{
    if (elementMode == RID_ONLY)
        endOfInput_r();
    else
        endOfInput_rv();
}

template<typename element_t>
void ZDL<element_t>::endOfInput_r()
{
    /* 1. call endOfInput on all buckets
       2. merge lowestBucketID bucket and sort
    */
	uint64_t i;
    if (multiProducer)
        base::lock();	
    
	for (i = 0; i < rbuckets.size(); i++) {
	    rbuckets[i]->endOfInput_nosave();
	    if ( rbuckets[i]->totalSize() != 0 ){
	        largestBucketID = i;
		    if (i < lowestBucketID) lowestBucketID = i;
		}
	}
	nextBucketID = lowestBucketID;
	
	// debug / profile
	if (ZDL_DEBUG) {
    	uint64_t saveSize = 0;
    	for (i = 0; i < rbuckets.size(); i++) 
    	    saveSize += rbuckets[i]->saveSize;
    	    
    	ZDEBUG << "ZDL-" << this << " end of input" 
           << " lowestID=" << lowestBucketID 
           << " largestID=" << largestBucketID << " saveSize=" << saveSize << std::endl;
    }
	
	if (lowestBucketID < rbuckets.size()){
	    ZDEBUG << "ZDL-" << this << " load bucket " << lowestBucketID << std::endl;
        rbuckets[lowestBucketID]->mergeSets();
    }
    phase = 1;
    consumePhase.notify_all(); //pthread_cond_broadcast(&consumePhase);
    ZDEBUG << "ZDL-" << this << " signal consumer" << std::endl;
    if (multiProducer)
        base::unlock();	
}

template<typename element_t>
void ZDL<element_t>::endOfInput_rv()
{
    /* 1. call endOfInput on all buckets
       2. merge lowestBucketID bucket and sort
    */
	uint64_t i;
    if (multiProducer)
        base::lock();	
    
	for (i = 0; i < rvbuckets.size(); i++) {
	    rvbuckets[i]->endOfInput_nosave();
	    if ( rvbuckets[i]->totalSize() != 0 ){
	        largestBucketID = i;
		    if (i < lowestBucketID) lowestBucketID = i;
		}
	}
	nextBucketID = lowestBucketID;
	
	// debug / profile
	if (ZDL_DEBUG) {
    	uint64_t saveSize = 0;
    	for (i = 0; i < rvbuckets.size(); i++) 
    	    saveSize += rvbuckets[i]->saveSize;
    	    
    	ZDEBUG << "ZDL-" << this << " end of input" 
           << " lowestID=" << lowestBucketID 
           << " largestID=" << largestBucketID << " saveSize=" << saveSize << std::endl;
    }
	
	// merge the first bucket here
	if (lowestBucketID < rvbuckets.size()){
	    ZDEBUG << "zdl-" << this << " load bucket " << lowestBucketID << std::endl;
        rvbuckets[lowestBucketID]->mergeSets();
    }
    phase = 1;
    consumePhase.notify_all(); //pthread_cond_broadcast(&consumePhase);
    ZDEBUG << "ZDL-" << this << " signal consumer" << std::endl;
    if (multiProducer)
        base::unlock();	
}

template<typename element_t>
bool ZDL<element_t>::next(uint64_t it, element_t *e)
{
    if (elementMode == RID_ONLY)
    {
        RIDElementType rid;
        bool ret = next_r(it, &rid);
        e->first = rid.first;
        return ret;
    }
    else
        return next_rv(it, e);
}

template<typename element_t>
bool ZDL<element_t>::next_r(uint64_t it, RIDElementType *e)
{
    /* 1. read from lowestBucketID bucket
       2. if read false, check if more buckets are available and update lowestBucketID
       3. if yes and this is the last consumer, merge lowestBucketID bucket and signal
       4. if not the last consumer, wait
       5. read from new lowestBucketID bucket
       6. if no more, all done and return false
    */
    int ret;
    bool locked = false;
    if (phase == 0)
    {
        locked = true;
        base::lock();
        waitForConsumePhase();
     }
    
    if (lowestBucketID >= rbuckets.size()){
        ZDEBUG << "ZDL-" << this << " consumer " << it << " return false on empty ZDL" << std::endl;
        if (locked)
            base::unlock();
        return false;
    }    
    
    ret = rbuckets[lowestBucketID]->next(it, e);
    if (!ret) {  
         
        if (lowestBucketID == largestBucketID){
            if (locked)
                base::unlock();
            ZDEBUG << "ZDL-" << this << " Consumer " << it << " finished" << std::endl;
            return (ret != 0);
        }
               
        if (numConsumers > 1 || phase == 0){
            locked = true;
            base::lock();
        }
        
        // more buckets. last consumer load new bucket
        if (++base::consumersFinished == numConsumers) {       
            if (lowestBucketID != nextBucketID) {     
                lowestBucketID = nextBucketID;                                
                ZDEBUG << "ZDL-" << this << " Consumer " << it << " call next bucket " << lowestBucketID << std::endl;
                nextBucketReady.notify_all(); //pthread_cond_broadcast(&nextBucketReady);
            }
            else {
                // being the only consumer
                nextBucketID = nextBucket(lowestBucketID);
                lowestBucketID = nextBucketID;
                ZDEBUG << "ZDL-" << this << " consumer " << it << " merge next bucket " << nextBucketID << std::endl;            
                rbuckets[lowestBucketID]->mergeSets();
            }
            base::consumersFinished = 0;
        }
        else {                            
            // 1st finished consumer merge next set
            if (base::consumersFinished == 1) {
                nextBucketID = nextBucket(lowestBucketID);
                ZDEBUG << "ZDL-" << this << " consumer " << it << " merge next bucket " << nextBucketID << std::endl;            
                rbuckets[nextBucketID]->mergeSets();
            }        
            nextBucketReady.wait(this->mutex); //pthread_cond_wait(&nextBucketReady, &(this->mutex));
        }         
        if (locked)
            base::unlock();
                
        ret = rbuckets[lowestBucketID]->next(it, e);
    }
  
    return (ret != 0);
	   
}

template<typename element_t>
bool ZDL<element_t>::next_rv(uint64_t it, element_t *e)
{
    /* 1. read from lowestBucketID bucket
       2. if read false, check if more buckets are available and update lowestBucketID
       3. if yes and this is the last consumer, merge lowestBucketID bucket and signal
       4. if not the last consumer, wait
       5. read from new lowestBucketID bucket
       6. if no more, all done and return false
    */
    int ret;
    bool locked = false;
    if (phase == 0)
    {
        locked = true;
        base::lock();
        waitForConsumePhase();
     }
    
    if (lowestBucketID >= rvbuckets.size()){
       ZDEBUG << "ZDL-" << this << " consumer " << it << " return false on empty ZDL" << std::endl;
        if (locked)
            base::unlock();
        return false;
    }    
    
    ret = rvbuckets[lowestBucketID]->next(it, e);
    if (!ret) {  
         
        if (lowestBucketID == largestBucketID){
            if (locked)
                base::unlock();
            ZDEBUG << "ZDL-" << this << " Consumer " << it << " finished" << std::endl;
            return (ret != 0);
        }
               
        if (numConsumers > 1 || phase == 0){
            locked = true;
            base::lock();
        }
        
        // more buckets. last consumer load new bucket
        if (++base::consumersFinished == numConsumers) {       
            if (lowestBucketID != nextBucketID) {     
                lowestBucketID = nextBucketID;                                
                ZDEBUG << "ZDL-" << this << " Consumer " << it << " call next bucket " << lowestBucketID << std::endl;
                nextBucketReady.notify_all(); //pthread_cond_broadcast(&nextBucketReady);
            }
            else {
                // being the only consumer
                nextBucketID = nextBucket(lowestBucketID);
                lowestBucketID = nextBucketID;
                ZDEBUG << "ZDL-" << this << " consumer " << it << " merge next bucket " << nextBucketID << std::endl;            
                rvbuckets[lowestBucketID]->mergeSets();
            }
            base::consumersFinished = 0;
        }
        else {                            
            // 1st finished consumer merge next set
            if (base::consumersFinished == 1) {
                nextBucketID = nextBucket(lowestBucketID);
                ZDEBUG << "ZDL-" << this << " consumer " << it << " merge next bucket " << nextBucketID << std::endl;            
                rvbuckets[nextBucketID]->mergeSets();
            }        
            nextBucketReady.wait(this->mutex); //pthread_cond_wait(&nextBucketReady, &(this->mutex));
        }         
        if (locked)
            base::unlock();
                
        ret = rvbuckets[lowestBucketID]->next(it, e);
    }
  
    return (ret != 0);
	   
}

template<typename element_t>
uint64_t ZDL<element_t>::size(uint64_t bucket)
{
    if (elementMode == RID_ONLY)
	    return rbuckets[bucket].get()->totalSize();
	else
	    return rvbuckets[bucket].get()->totalSize();
}

template<typename element_t>
uint64_t ZDL<element_t>::bucketCount()
{
    waitForConsumePhase();
	return numBuckets;
}

template<typename element_t>
uint64_t ZDL<element_t>::totalSize()
{
	uint64_t ret = 0;
	uint i;
    
    if (elementMode == RID_ONLY)
	    for (i = 0; i < rbuckets.size(); i++)
		    ret += rbuckets[i].get()->totalSize();
    else
        for (i = 0; i < rvbuckets.size(); i++)
		    ret += rvbuckets[i].get()->totalSize();
	return ret;
}

template<typename element_t>
uint64_t ZDL<element_t>::nextBucket(uint64_t bucketID) 
{
    if (elementMode == RID_ONLY)
        return nextBucket_r (bucketID);
    else
        return nextBucket_rv (bucketID);
}

template<typename element_t>
uint64_t ZDL<element_t>::nextBucket_r(uint64_t bucketID) 
{
    // should already be consumption phase by now
    uint64_t i;
    if ( bucketID >= rbuckets.size())
        return bucketID;
    
    for (i = bucketID+1; i <= largestBucketID; i++) {
        if (rbuckets[i]->totalSize() != 0){
            return i;
        }
    }    
    return bucketID;    
}

template<typename element_t>
uint64_t ZDL<element_t>::nextBucket_rv(uint64_t bucketID) 
{
    // should already be consumption phase by now
    uint64_t i;
    if ( bucketID >= rvbuckets.size())
        return bucketID;
    
    for (i = bucketID+1; i <= largestBucketID; i++) {
        if (rvbuckets[i]->totalSize() != 0){
            return i;
        }
    }    
    return bucketID;    
}

template<typename element_t>
void ZDL<element_t>::resetNumConsumers(uint nc)
{
    uint64_t i;
    numConsumers = nc;
    if (elementMode == RID_ONLY)
        for (i = 0; i < rbuckets.size(); i++)
            rbuckets[i].get()->resetNumConsumers(numConsumers);
    else
        for (i = 0; i < rvbuckets.size(); i++)
            rvbuckets[i].get()->resetNumConsumers(numConsumers);
}

template<typename element_t>
inline void ZDL<element_t>::waitForConsumePhase()
{
	while (phase == 0)
	    consumePhase.wait(this->mutex); //pthread_cond_wait(&consumePhase, &(this->mutex));	
}

template<typename element_t>
void ZDL<element_t>::setElementMode(const uint mode)
{   
  ZDEBUG << "ZDL-" << this << " set elementmode " << mode << std::endl;
	if (elementMode != mode)
	{
	    if (elementMode == RID_ONLY)
	        rbuckets.clear();
	    else
	        rvbuckets.clear();
	    elementMode = mode;
	    if (elementMode == RID_ONLY){
	        RIDSWSP ridswsp;
            for (uint64_t i = 0; i < defaultNumBuckets; i++) {
                ridswsp.reset(new WSDL<RIDElementType>
					(numConsumers, maxElemPerBucket,
					base::getDiskElemSize1st(),
					 base::getDiskElemSize2nd(), fRm) );
                ridswsp->setMultipleProducers(multiProducer);
                ridswsp->traceOn(fTraceOn);
                rbuckets.push_back(ridswsp);
            }
        }
        else {
            SWSP swsp;
            for (uint64_t i = 0; i < defaultNumBuckets; i++) {
                swsp.reset(new WSDL<element_t>
					(numConsumers, maxElemPerBucket,
					base::getDiskElemSize1st(),
					 base::getDiskElemSize2nd(), fRm) );
                swsp->setMultipleProducers(multiProducer);
                swsp->traceOn(fTraceOn);
                rvbuckets.push_back(swsp);
            }
        }
	}	 
}

template<typename element_t>
inline void ZDL<element_t>::checkBucket_r(uint64_t bucketID, bool lockFlag)
{
	if (bucketID >= rbuckets.size())
	{
		if (lockFlag) base::lock();    
		RIDSWSP ridswsp;
        // increase buckets when necessary
        while (bucketID >= rbuckets.size()) {
            for (uint64_t i = 0; i < defaultNumBuckets; i++) {
                ridswsp.reset(new WSDL<RIDElementType>
					(numConsumers, maxElemPerBucket,
					base::getDiskElemSize1st(),
					 base::getDiskElemSize2nd(), fRm ) );
                ridswsp->setMultipleProducers(multiProducer);
                rbuckets.push_back(ridswsp);
            }
        }
        numBuckets = rbuckets.size();
        if (lockFlag) base::unlock();
    }
}

template<typename element_t>
// @bug 831. check lockFlag to lock or not
inline void ZDL<element_t>::checkBucket_rv(uint64_t bucketID, bool lockFlag)
{
	if (bucketID >= rvbuckets.size())
	{
		if (lockFlag) base::lock();    
		SWSP swsp;
        // increase buckets when necessary
        while (bucketID >= rvbuckets.size()) {
            for (uint64_t i = 0; i < defaultNumBuckets; i++) {
                swsp.reset(new WSDL<element_t>
					(numConsumers, maxElemPerBucket,
					base::getDiskElemSize1st(),
					 base::getDiskElemSize2nd(), fRm ));
                swsp->setMultipleProducers(multiProducer);
                rvbuckets.push_back(swsp);
            }
        }
        numBuckets = rvbuckets.size();       
	    if (lockFlag) base::unlock();
    }
}

//
// Returns the number of temp files and the space taken up by those files
// (in bytes) by this ZDL collection.
//
template<typename element_t>
void ZDL<element_t>::totalFileCounts(uint64_t& numFiles, uint64_t& numBytes)
	const
{
	numFiles = 0;
	numBytes = 0;

    if (elementMode == RID_ONLY)
		totalFileCounts_r (numFiles, numBytes);
	else
		totalFileCounts_rv(numFiles, numBytes);
}

template<typename element_t>
void ZDL<element_t>::totalFileCounts_r(uint64_t& numFiles, uint64_t& numBytes)
	const
{
	for (uint64_t i = 0; i < rbuckets.size(); i++)
	{
		uint64_t setCnt = rbuckets[i].get()->initialSetCount();

		if (setCnt > 1)
		{
			//std::cout << "ZDL: bucket " << i << " has " << setCnt <<
			//	" sets" << std::endl;

			numFiles += rbuckets[i].get()->numberOfTempFiles();
    	    numBytes += rbuckets[i].get()->saveSize;
		}
	}
}

template<typename element_t>
void ZDL<element_t>::totalFileCounts_rv(uint64_t& numFiles, uint64_t& numBytes)
	const
{
	for (uint64_t i = 0; i < rvbuckets.size(); i++)
	{
		uint64_t setCnt = rvbuckets[i].get()->initialSetCount();

		if (setCnt > 1)
		{
			//std::cout << "ZDL: bucket " << i << " has " << setCnt <<
			//	" sets" << std::endl;

			numFiles += rvbuckets[i].get()->numberOfTempFiles();
    	    numBytes += rvbuckets[i].get()->saveSize;
		}
	}
}

#if 0
template<typename element_t>
bool ZDL<element_t>::next(uint64_t bucket, uint64_t it, element_t *e)
{
    if (elementMode == RID_ONLY) 
    {
        RIDElementType rid;
        bool ret; 
        if (!rbuckets[bucket]->mergeFlag)
        {
            if (numConsumers >1)
                base::lock();
            rbuckets[bucket]->mergeSets();
            if (numConsumers >1)
                base::unlock();
        }
        ret = rbuckets[bucket]->next(it, &rid);
        e->first = rid.first;
        return ret;
    }
    else
    {
        if (!rvbuckets[bucket]->mergeFlag)
        {
            if (numConsumers >1)
                base::lock();
            rvbuckets[bucket]->mergeSets();
            if (numConsumers >1)
                base::unlock();
        }
        return rvbuckets[bucket]->next(it, e);
    }
}
#endif

//
// Sets the sizes to be employed in saving the elements to disk.
// size1st is the size in bytes of element_t.first.
// size2nd is the size in bytes of element_t.second.
//
template<typename element_t>
void ZDL<element_t>::setDiskElemSize(uint32_t size1st,uint32_t size2nd)
{
	base::fElemDiskFirstSize  = size1st;
	base::fElemDiskSecondSize = size2nd;

	//...Forward this size information to our internal WSDL containers.

    if (elementMode == RID_ONLY) 
	{
		for (uint64_t i = 0; i < rbuckets.size(); i++)
		{
			rbuckets[i]->setDiskElemSize ( size1st, size2nd );
		}
	}
	else
	{
		for (uint64_t i = 0; i < rvbuckets.size(); i++)
		{
			rvbuckets[i]->setDiskElemSize ( size1st, size2nd );
		}
	}
}

template<typename element_t>
void ZDL<element_t>::enableDiskIoTrace()
{
	fTraceOn = true;
	if (elementMode == RID_ONLY)
		for (uint64_t bucket = 0; bucket < numBuckets; bucket++)
			rbuckets[bucket]->traceOn(fTraceOn);
	else
		for (uint64_t bucket = 0; bucket < numBuckets; bucket++)
			rvbuckets[bucket]->traceOn(fTraceOn);
}

template<typename element_t>
bool ZDL<element_t>::totalDiskIoTime(uint64_t& w, uint64_t& r)
{
	boost::posix_time::time_duration wTime(0,0,0,0);
	boost::posix_time::time_duration rTime(0,0,0,0);
	bool diskIo = false;

	for (uint64_t bucket = 0; bucket < numBuckets; bucket++)
	{
		std::list<DiskIoInfo>& infoList = diskIoInfoList(bucket);
		std::list<DiskIoInfo>::iterator k = infoList.begin();
		while (k != infoList.end())
		{
			if (k->fWrite == true)
				wTime += k->fEnd - k->fStart;
			else
				rTime += k->fEnd - k->fStart;
			k++;
		}

		if (infoList.size() > 0)
			diskIo = true;
	}

	w = wTime.total_seconds();
	r = rTime.total_seconds();

	return diskIo;
}

template<typename element_t>
std::list<DiskIoInfo>& ZDL<element_t>::diskIoInfoList(uint64_t bucket)
{
	if (elementMode == RID_ONLY)
		return (rbuckets[bucket]->diskIoList());
	else
		return (rvbuckets[bucket]->diskIoList());
}


} //namespace

#endif
