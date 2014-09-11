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

/******************************************************************************
 * $Id: swsdl.h 7409 2011-02-08 14:38:50Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class XXX interface
 */

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#include "largedatalist.h"
#include "elementtype.h"
//#include "configcpp.h"
#include "resourcemanager.h"

#ifndef _SWSDL_HPP
#define _SWSDL_HPP

/** Debug macro */
#define SWSDL_DEBUG 1
#if SWSDL_DEBUG
#define SDEBUG std::cout
#else
#define SDEBUG if (false) std::cout
#endif

namespace joblist {
/** @brief class StagedDL
 *
 */
template<typename element_t>
class SWSDL : public LargeDataList<std::vector<element_t>, element_t>
{
	typedef LargeDataList<std::vector<element_t>, element_t> base;
    typedef std::vector<element_t> container_t;
    
	public:
	    SWSDL(uint numConsumers, ResourceManager& rm);
		SWSDL(uint numConsumers, uint maxElements, ResourceManager& rm);
		virtual ~SWSDL();

		void insert(const element_t &e);
		void insert(const container_t &e);
		void insert_nolock(const element_t &e);
		bool next(uint64_t it, element_t *e);
		void endOfInput();
		uint64_t getIterator();
		uint64_t totalSize();

	protected:

	private:
		// Default constructors and assignment operator are declared but
		// never defined to disable their use
		SWSDL();
		SWSDL(const SWSDL &);
		SWSDL & operator=(const SWSDL &);

		void load(uint64_t setNum);
		uint64_t save();
		uint64_t save(container_t *c);
        bool inputDone();

		ResourceManager& fRm;
		uint64_t maxElements;
		uint64_t waitingConsumers;
		boost::condition nextSetLoaded;
		boost::condition nextSetReady;
		bool endInput;
		container_t *tempC;     // temp buffer for the producer to write to
		boost::mutex writeLock;  // lock to protect the temp buffer
};

template<typename element_t>
SWSDL<element_t>::SWSDL(uint nc, ResourceManager& rm) :
		base(nc,sizeof(uint64_t),sizeof(uint64_t), rm), fRm(rm), maxElements(rm.getScTempSaveSize()),
		waitingConsumers(0), endInput(false), tempC(new container_t())
		
{
    
//     config::Config *config = config::Config::makeConfig();
//     std::string strVal;
// 	strVal = config->getConfig("SystemConfig", "TempSaveSize");
// 	if (strVal.size() > 0)
// 		maxElements = config::Config::uFromText(strVal);
// 	else
// 		maxElements = defaultMaxElements;
// 	waitingConsumers = 0;
// 	endInput = false;
// 	tempC = new container_t();
	// need to modify base assignment from 1 to 0 to enssure concurrency correctness
	base::setCount = 0;      
	//pthread_cond_init(&nextSetLoaded, NULL);
	//pthread_cond_init(&nextSetReady, NULL);
	//pthread_mutex_init(&writeLock, NULL);
	SDEBUG << "SWSDL-" << this << " maxElements=" << maxElements << std::endl;
}

template<typename element_t>
SWSDL<element_t>::SWSDL(uint nc, uint me, ResourceManager& rm) :
		base(nc,sizeof(uint64_t),sizeof(uint64_t), rm), fRm(rm), maxElements(me), 
		waitingConsumers(0), endInput(false), tempC(new container_t())
{
// 	maxElements = me;
// 	waitingConsumers = 0;
// 	endInput = false;
// 	tempC = new container_t();
	// need to modify base assignment from 1 to 0 to enssure concurrency correctness
	base::setCount = 0;
	//pthread_cond_init(&nextSetLoaded, NULL);
	//pthread_cond_init(&nextSetReady, NULL);
	//pthread_mutex_init(&writeLock, NULL);
	SDEBUG << "SWSDL-" << this << " maxElements=" << me << std::endl;
}

template<typename element_t>
SWSDL<element_t>::~SWSDL()
{
	//pthread_cond_destroy(&nextSetLoaded);
	//pthread_cond_destroy(&nextSetReady);
	//pthread_mutex_destroy(&writeLock);
	SDEBUG << "swsdl-" << this << " destructed" << std::endl;
}

template<typename element_t>
void SWSDL<element_t>::load(uint64_t setNum)
{
	if (typeid(element_t) == typeid(ElementType) ||
		typeid(element_t) == typeid(DoubleElementType))
		base::load_contiguous(setNum);
	else
		base::load(setNum);
}

template<typename element_t>
uint64_t SWSDL<element_t>::save()
{
    sort(base::c->begin(), base::c->end());
	if (typeid(element_t) == typeid(ElementType) ||
		typeid(element_t) == typeid(DoubleElementType))
		return base::save_contiguous();
	else
		return base::save();
}

template<typename element_t>
uint64_t SWSDL<element_t>::save(container_t *c)
{
    sort(c->begin(), c->end());
	if (typeid(element_t) == typeid(ElementType) ||
		typeid(element_t) == typeid(DoubleElementType))
		return base::save_contiguous(c);
	else
		return base::save(c);
}

template<typename element_t>
void SWSDL<element_t>::insert(const element_t &e)
{
 	if (base::multipleProducers)
     	writeLock.lock(); //pthread_mutex_lock(&writeLock);

 	insert_nolock(e);
 	if (base::multipleProducers)
 	   	writeLock.unlock(); //pthread_mutex_unlock(&writeLock);
}

template<typename element_t>
void SWSDL<element_t>::insert_nolock(const element_t &e)
{
    /*  1. insert into tempC
        2. if maxElement, save tempC to disk. increment setCount, and renew tempC
        3. if initial state (phase==0), load(0), flip phase and signal consumer load ready
        4. else, signal consumer next set ready
    */
 	if (tempC->size() == maxElements)
 	{
 	    save(tempC);
	    delete tempC;
	    tempC = new container_t();

        base::lock();
        base::setCount++;
        
        // when 1st set ready, save and load it, and flip phase. so phase=0 represents
        // the initial state; phase=1 represent in-process state (at least one set has
        // been loaded and consumers have stared consuming).
		if (base::phase == 0) 
		{    
		    load(0);
		    base::phase = 1; 
		    SDEBUG << this << " first set ready. " << std::endl;
		    nextSetLoaded.notify_all(); //pthread_cond_broadcast(&nextSetLoaded);
		}
		else
		{
		    SDEBUG << this << " next set ready" << std::endl;
		    nextSetReady.notify_all(); //pthread_cond_broadcast(&nextSetReady);
		}
		base::unlock();
		    
	}
    base::totSize++;
    tempC->insert(tempC->end(), e);
}

template<typename element_t>
void SWSDL<element_t>::insert(const container_t &e)
{
	throw std::logic_error("StagedDL::insert(vector) isn't implemented yet");
}

template<typename element_t>
void SWSDL<element_t>::endOfInput()
{
    /*
        1. if current is not the only set, save tempC to disk and increment setCount;
        2. else, sort tempC, assign to base::C and signal consumer load ready
        3. set endInput flag true
    */
	base::lock();
	SDEBUG << "end of input " << this << std::endl;
	base::endOfInput();
	if (base::setCount > 0) {
		save(tempC);
	    base::setCount++;
	    nextSetReady.notify_all(); //pthread_cond_broadcast(&nextSetReady);    
	}
	else
	{
	    sort(tempC->begin(), tempC->end());
	    base::c = tempC;
	    SDEBUG << this << " One only set ready" << std::endl;
	    base::setCount++;
		nextSetLoaded.notify_all(); //pthread_cond_broadcast(&nextSetLoaded);
		base::resetIterators();
    }
    endInput = true; // setCount is the final count now
	base::unlock();
}

template<typename element_t>
uint64_t SWSDL<element_t>::getIterator()
{
	uint64_t ret;

	base::lock();
	ret = base::getIterator();
	base::unlock();
	return ret;
}

template<typename element_t>
bool SWSDL<element_t>::inputDone()
{
	return ( base::loadedSet >= base::setCount-1 && endInput);
}

template<typename element_t>
bool SWSDL<element_t>::next(uint64_t id, element_t *e)
{
    /*
        1. if initial state (phase==0), wait for load ready.
        2. read without wait
        3. if end of current set, and more set is ready, and no more consumer is still reading this set, load new set.
        4. if end of current set, and more set is ready, and other consumers are still working, wait for next load signal.
        5. if end of current set, and end of set, shrink().
    */
	bool ret, locked = false;
	uint64_t nextSet;
/*    
    if (base::numConsumers > 1 || !inputDone())
    {
        locked = true;
        base::lock();
    }
*/
    if (base::phase == 0) {
        locked = true;
        base::lock();
    } 
        
    while (base::phase == 0)
    {
        SDEBUG << id << " waiting on nextSetLoaded: 215 for " << this << std::endl;
        nextSetLoaded.wait(this->mutex); //pthread_cond_wait(&nextSetLoaded, &(this->mutex));  
        SDEBUG << "consumer " << id << " wake up for " << this << std::endl;
    }

	ret = base::next_nowait(id, e);
	    
	if (ret == false){
	    SDEBUG << id << " One set read done for " << this << " loadedSet="<< base::loadedSet
	        << " setCount=" << base::setCount << std::endl;  
	    if (locked) base::unlock();
	 }
	
    // signal the caller is at the end of the loaded set, 
	// but there are more sets
	if (ret == false && !inputDone()) {
         if (!locked && (base::numConsumers > 1 || !inputDone())) {
 		    locked = true;
		    base::lock();
 	    }
        
		nextSet = base::loadedSet + 1;
		waitingConsumers++;
		// other consumers not done on this set yet
		if (waitingConsumers < base::numConsumers) 
			while (nextSet != base::loadedSet) {
 				SDEBUG << id << " waiting on nextSetLoaded: 217 for " << this 
 				    << " nextSet=" << nextSet << " loadedSet=" << base::loadedSet << std::endl;
				nextSetLoaded.wait(this->mutex); //pthread_cond_wait(&nextSetLoaded, &(this->mutex));
				SDEBUG << id << " wake up nextSetLoaded: 217 for " << this << std::endl;
			}
		// current consumer is the last one who done reading this set
		else {
		    // set ready, not loaded yet
		    if (base::loadedSet < base::setCount-1)
		    {
			    load(nextSet);
			    SDEBUG << id << " Loaded next set for " << this << std::endl;
			    nextSetLoaded.notify_all(); //pthread_cond_broadcast(&nextSetLoaded);
			    waitingConsumers = 0;
			}
			// next set not saved yet
			else if (!inputDone())
			{
			    SDEBUG << id << " wait on nextSetReady for " << this << std::endl;
			    nextSetReady.wait(this->mutex); //pthread_cond_wait(&nextSetReady, &(this->mutex));
			    SDEBUG << id << " wake up ready for " << this << std::endl;
			    load(nextSet);
			    nextSetLoaded.notify_all(); //pthread_cond_broadcast(&nextSetLoaded);
			    waitingConsumers = 0;
			}
		}
		ret = base::next(id, e);
	}
	
	// end of input
	if (ret == false && inputDone() && ++base::consumersFinished == base::numConsumers) 
	{
	    SDEBUG << "finished consumer # = " << base::consumersFinished << " # consumers = " << base::numConsumers << std::endl;
	    SDEBUG << "shrink " << this << std::endl;
	    base::shrink();
	}
    
    if (locked)
	    base::unlock();	
	return ret;	
}

template<typename element_t>
uint64_t SWSDL<element_t>::totalSize()
{
	uint64_t ret;

	base::lock();
	ret = base::totalSize();
	base::unlock();
	
	return ret;
}

}  //namespace

#endif
