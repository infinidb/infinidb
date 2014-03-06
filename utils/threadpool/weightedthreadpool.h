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
*   $Id: $
*
*
***********************************************************************/
/** @file */

#ifndef WEIGHTEDTHREADPOOL_H
#define WEIGHTEDTHREADPOOL_H

#include <string>
#include <iostream>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>

namespace threadpool
{
/** @brief ThreadPool is a component for working with pools of threads and asynchronously
  * executing tasks. It is responsible for creating threads and tracking which threads are "busy"
  * and which are idle. Idle threads are utilized as "work" is added to the system.
  */

class WeightedThreadPool
{
public:
    typedef boost::function0<int> Functor_T;

    /*********************************************
     *  ctor/dtor
     * 
     *********************************************/

    /** @brief ctor
      */
    WeightedThreadPool();

    /** @brief ctor
      *
      * @param maxThreads the maximum number of threads in this pool. This is the maximum number
      *        of simultaneuous operations that can go on.
      * @param queueSize  the maximum number of work tasks in the queue. This is the maximum
      *        number of jobs that can queue up in the work list before invoke() blocks.
      */
    explicit WeightedThreadPool( size_t maxThreadWeight, size_t maxThreads, size_t queueSize );

    /** @brief dtor
      */
    ~WeightedThreadPool() throw();


    /*********************************************
     *  accessors/mutators
     * 
     *********************************************/
    /** @brief set the work queue size
      *
      * @param queueSize the size of the work queue
      */
    void setQueueSize( size_t queueSize );

    /** @brief fet the work queue size
      */
    inline size_t getQueueSize() const { return fQueueSize; }

    /** @brief set the maximum number of threads to be used to process
      * the work queue
      *
      * @param maxThreads the maximum number of threads
      */
    void  setMaxThreads( size_t maxThreads );

    /** @brief get the maximum number of threads
      */
    inline size_t getMaxThreads() const { return fMaxThreads; }

    /** @brief set the maximum processing weight of a thread to be
      * submitted for execution from the existing jobs
	  * scheduled in the work queue
      *
      * @param maxWeight for execution
      */
    void  setMaxThreadWeight( size_t maxWeight );

    /** @brief get the maximum number of threads
      */
    inline uint32_t getMaxThreadWeight() const { return fMaxThreadWeight; }

    /** @brief register a functor to be called when a new thread
      *  is created
      */
    void setThreadCreatedListener(const Functor_T &f) ;

    /** @brief queue size accessor
      *
      */
    inline uint32_t getWaiting() const { return fWaitingFunctorsSize; }

    inline uint32_t getWeight() const { return fWaitingFunctorsWeight; }

    void removeJobs(uint32_t id);

    /*********************************************
     *  operations
     * 
     *********************************************/

    /** @brief invoke a functor in a separate thread managed by the pool
      *
      * If all maxThreads are busy, threadfunc will be added to a work list and
      * will run when a thread comes free. If all threads are busy and there are
      * queueSize tasks already waiting, invoke() will block until a slot in the
      * queue comes free.
      */
    void invoke(const Functor_T &threadfunc, uint32_t functor_weight, uint32_t id);

    /** @brief stop the threads
      */
    void stop();

    /** @brief wait on all the threads to complete
      */
    void wait();

    /** @brief for use in debugging
      */
    void dump();

protected:

private:
    /** @brief initialize data memebers
      */
    void init();

    /** @brief add a functor to the list
      */
    void addFunctor(const Functor_T &func, uint32_t functor_weight, uint32_t id);

    /** @brief thread entry point
      */
    void beginThread() throw();


    WeightedThreadPool(const WeightedThreadPool&);
    WeightedThreadPool& operator = (const WeightedThreadPool&);

    friend struct beginThreadFunc;

    struct beginThreadFunc
    {
        beginThreadFunc(WeightedThreadPool& impl)
                : fImpl(impl)
        {}

        void operator() ()
        {
            fImpl.beginThread();
        }

        WeightedThreadPool &fImpl;
    };

    struct NoOp
    {
        void operator () () const
        {}
	};

    size_t fThreadCount;
    size_t fMaxThreadWeight;
    size_t fMaxThreads;
    size_t fQueueSize;

    //typedef std::list<Functor_T> Container_T;
	struct FunctorListItemStruct {
		Functor_T functor;
		uint32_t functorWeight;
		uint32_t id;
	};

	typedef FunctorListItemStruct FunctorListItem;
    typedef std::list<FunctorListItem> Container_T;
    Container_T fWaitingFunctors;
    Container_T::iterator fNextFunctor;

	uint32_t issued;
    boost::mutex fMutex;
    boost::condition fThreadAvailable; // triggered when a thread is available
    boost::condition fNeedThread;      // triggered when a thread is needed
    boost::thread_group fThreads;

    bool 	fStop;
    long 	fGeneralErrors;
    long	fFunctorErrors;
	uint16_t 	fWaitingFunctorsSize;
	uint16_t 	fWaitingFunctorsWeight;

};

} // namespace threadpool

#endif //WEIGHTEDTHREADPOOL_H
