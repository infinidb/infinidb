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
 * $Id: wsdl.h 8436 2012-04-04 18:18:21Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class XXX interface
 */

#include <vector>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include "elementtype.h"
#include "largedatalist.h"

#ifndef _WSDL_HPP
#define _WSDL_HPP

namespace joblist {

/** @brief class WSDL
 *
 */
template<typename element_t>
class WSDL : public LargeDataList<std::vector<element_t>, element_t>
{
	typedef LargeDataList<std::vector<element_t>, element_t> base;
	typedef std::vector<element_t> container_t;

	public:
		// elementeSaveSize1st - size (in bytes) to be used in saving
		//                       element_t.first to disk.
		// elementSaveSize2    - size (in bytes) to be used in saving
		//                       element_t.second to disk.
		WSDL(uint numConsumers, uint maxElements, ResourceManager& rm);
		WSDL(uint numConsumers, uint maxElements,
			uint32_t elementSaveSize1st,
			uint32_t elementSaveSize2nd,
			ResourceManager& rm);
		virtual ~WSDL();

		void insert(const element_t &e);
		void insert(const std::vector<element_t> &e);
		void insert_nolock(const element_t &e);
		bool next(uint64_t it, element_t *e);
		void endOfInput();
		void endOfInput_nosave();
		uint64_t getIterator();
		uint64_t totalSize();
        uint64_t maxElem() { return maxElements; }
		uint64_t initialSetCount() const { return initialSetCnt; }
        void mergeSets();
        void flush (const uint64_t &threshold, bool lockFlag = true);
        uint64_t curSize;
        uint64_t saveSize;  // total save to disk size. debug usage
        
	protected:
        
	private:
		// this doesn't make sense during the consume phase.
		// if necessary we have to put a little thought into what to do
		// with waiting threads.  Same with =.
		// Declare but don't define default and copy constuctor, and
		// assignment operator to disable their use.
		WSDL();
		WSDL(const WSDL &);
		WSDL & operator=(const WSDL &);
        
        // @bug 721. add append option
		void load(uint64_t setNum, bool append=false);
		uint64_t save();
        uint64_t save(container_t *c);
		uint64_t maxElements;
		uint64_t waitingConsumers;
		boost::condition nextSetLoaded;
		uint64_t initialSetCnt; // total number of sets prior to any merges
};

template<typename element_t>
WSDL<element_t>::WSDL(uint nc, uint me, ResourceManager& rm) :
		base(nc,sizeof(uint64_t),sizeof(uint64_t), rm), curSize(0),
	saveSize(0), maxElements(me), waitingConsumers(0), initialSetCnt(0)
{
	//pthread_cond_init(&nextSetLoaded, NULL);
}

template<typename element_t>
WSDL<element_t>::WSDL(uint nc, uint me,
	uint32_t elementSaveSize1st, uint32_t elementSaveSize2nd, ResourceManager& rm) :
		base(nc,elementSaveSize1st,elementSaveSize2nd, rm), curSize(0),
	saveSize(0), maxElements(me), waitingConsumers(0), initialSetCnt(0)
{
	//pthread_cond_init(&nextSetLoaded, NULL);
}

template<typename element_t>
WSDL<element_t>::~WSDL()
{
	//pthread_cond_destroy(&nextSetLoaded);
}

template<typename element_t>
void WSDL<element_t>::load(uint64_t setNum, bool append)
{
	if (typeid(element_t) == typeid(ElementType) ||
		typeid(element_t) == typeid(DoubleElementType) ||
		typeid(element_t) == typeid(RIDElementType))
		base::load_contiguous(setNum, append);
	else
		base::load(setNum, append);
}

// returns the number of bytes written by this save operation
template<typename element_t>
uint64_t WSDL<element_t>::save()
{
	if (typeid(element_t) == typeid(ElementType) ||
		typeid(element_t) == typeid(DoubleElementType) ||
		typeid(element_t) == typeid(RIDElementType))
		return base::save_contiguous();
	else
		return base::save();
}

// returns the number of bytes written by this save operation
template<typename element_t>
uint64_t WSDL<element_t>::save(container_t *c)
{
    sort(c->begin(), c->end());
	if (typeid(element_t) == typeid(ElementType) ||
		typeid(element_t) == typeid(DoubleElementType) ||
		typeid(element_t) == typeid(RIDElementType))
		return base::save_contiguous(c);
	else
		return base::save(c);
}

template<typename element_t>
void WSDL<element_t>::insert(const element_t &e)
{
 	if (base::multipleProducers) 
     	base::lock(); 
   try
   {    	
    insert_nolock(e);
   }
   catch ( ... )
   {
  	if (base::multipleProducers)
 	   	base::unlock();
	throw;
   }
	if (base::multipleProducers)
 	   	base::unlock();
}

template<typename element_t>
void WSDL<element_t>::insert_nolock(const element_t &e)
{
 	if (base::c->size() == maxElements) { 	    
// 	    std::cout << "WSDL-" << this << " full and save" << std::endl;
		curSize = 0;
		saveSize += save();
		base::registerNewSet();		
	}

	base::insert(e);	
	curSize++;
	
}

template<typename element_t>
void WSDL<element_t>::insert(const std::vector<element_t> &e)
{
	throw std::logic_error("WSDL::insert(vector) isn't implemented yet");
}

template<typename element_t>
void WSDL<element_t>::endOfInput()
{
	base::lock();
	initialSetCnt = base::setCount;
	if (base::loadedSet > 0)
	{
		saveSize += save();
		load(0);
	}
	else
	{
		// save the in memory set for reuse
		if (base::saveForReuse() == true)
			saveSize += save();

		base::resetIterators_nowait();
	}

	base::endOfInput();
	base::unlock();
}

template<typename element_t>
void WSDL<element_t>::endOfInput_nosave()
{
	base::lock();
	initialSetCnt = base::setCount;
	base::endOfInput();
	base::unlock();
	
}

template<typename element_t>
uint64_t WSDL<element_t>::getIterator()
{
	uint64_t ret;

	base::lock();
	ret = base::getIterator();
	base::unlock();
	return ret;
}

template<typename element_t>
bool WSDL<element_t>::next(uint64_t id, element_t *e)
{
	bool ret, locked = false;
	uint64_t nextSet;

    if (base::phase == 0) {
        locked = true;
        base::lock();
        base::waitForConsumePhase();
    } 	

	ret = base::next_nowait(id, e);

	/* XXXPAT: insignificant race condition here.  Technically, there's no
	guarantee the caller will be wakened when the next set is loaded.  It could
	get skipped.  It won't happen realistically, but it exists... */

	// signifies the caller is at the end of the loaded set, 
	// but there are more sets
	if (ret == false && (base::loadedSet < base::setCount - 1)) {

        if (!locked && (base::numConsumers > 1 || base::phase == 0)) {
 		    locked = true;
		    base::lock();
 	    }
 	    
		nextSet = base::loadedSet + 1;
		waitingConsumers++;
		if (waitingConsumers < base::numConsumers)
			while (nextSet != base::loadedSet) {
				nextSetLoaded.wait(this->mutex); //pthread_cond_wait(&nextSetLoaded, &(this->mutex));
			}
		else {
			load(nextSet);
			nextSetLoaded.notify_all(); //pthread_cond_broadcast(&nextSetLoaded);
		}
		waitingConsumers--;
		ret = base::next(id, e);		
	}

	if (ret == false && ++base::consumersFinished == base::numConsumers) {	
		base::shrink();
		base::removeFile();
	}
 	if (locked)
		base::unlock();
		
	return ret;	
}

template<typename element_t>
uint64_t WSDL<element_t>::totalSize()
{
	uint64_t ret;

	base::lock();
	ret = base::totalSize();
	base::unlock();
	
	return ret;
}

/** @bug 721. merge all the saved sets to one set and sort */
template<typename element_t>
void WSDL<element_t>::mergeSets()
{
    uint64_t i, size;
    container_t *temp = reinterpret_cast<std::vector<element_t> *>(base::c);
    size = totalSize();
    temp->reserve(size);
            
    for (i = 0; i < base::setCount-1; i++) {
        // append set to the end of current set
        load(i, true);   
    }

    sort(base::c->begin(), base::c->end());
    //std::cout << "WSDL-" << this << " mergeSets " << "setCount=" << base::setCount 
    //          << " totalSize=" << size << std::endl;
    base::setCount = 1;
    base::resetIterators_nowait();  
    
}

/** @bug 721. flush the largest bucket to disk when over threshold */
template<typename element_t>
void WSDL<element_t>::flush(const uint64_t &threshold, bool lockFlag)
{

    if (lockFlag) base::lock();
    try
    {
    if (curSize > threshold){   
        // force flush
//        std::cout << "WSDL-" << this << " flushed " << "curSize=" << curSize 
//		          << " max= " << maxElements << std::endl;	
		curSize = 0;
		saveSize += save();
		base::registerNewSet();		
    }
    }
    catch(...)
    {
	if (lockFlag) base::unlock();
	std::cerr << __FILE__ << "@" << __LINE__ << " ZDL insert vector caught an exception\n";
	throw;
    }
    if (lockFlag) base::unlock();
}


}  //namespace

#endif
