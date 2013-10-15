/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/***********************************************************************
*   $Id: threadpool.h 3495 2013-01-21 14:09:51Z rdempsey $
*
*   Work dervied from Devguy.com's Open Source C++ thread pool implementation 
*	released under public domain.
*
*
***********************************************************************/
/** @file */

#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <string>
#include <iostream>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>

#if defined(_MSC_VER) && defined(xxxTHREADPOOL_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace threadpool
{
/** @brief ThreadPool is a component for working with pools of threads and asynchronously
  * executing tasks. It is responsible for creating threads and tracking which threads are "busy"
  * and which are idle. Idle threads are utilized as "work" is added to the system.
  */

class ThreadPool
{
public:
    typedef boost::function0<void> Functor_T;

    /*********************************************
     *  ctor/dtor
     * 
     *********************************************/

    /** @brief ctor
      */
    EXPORT ThreadPool();

    /** @brief ctor
      *
      * @param maxThreads the maximum number of threads in this pool. This is the maximum number
      *        of simultaneuous operations that can go on.
      * @param queueSize  the maximum number of work tasks in the queue. This is the maximum
      *        number of jobs that can queue up in the work list before invoke() blocks.
      */
    EXPORT explicit ThreadPool( size_t maxThreads, size_t queueSize );

    /** @brief dtor
      */
    EXPORT ~ThreadPool() throw();


    /*********************************************
     *  accessors/mutators
     * 
     *********************************************/
    /** @brief set the work queue size
      *
      * @param queueSize the size of the work queue
      */
    EXPORT void setQueueSize( size_t queueSize );

    /** @brief fet the work queue size
      */
    inline size_t getQueueSize() const { return fQueueSize; }

    /** @brief set the maximum number of threads to be used to process
      * the work queue
      *
      * @param maxThreads the maximum number of threads
      */
    EXPORT void  setMaxThreads( size_t maxThreads );

    /** @brief get the maximum number of threads
      */
    inline size_t getMaxThreads() const { return fMaxThreads; }

    /** @brief register a functor to be called when a new thread
      *  is created
      */
    EXPORT void setThreadCreatedListener(const Functor_T &f) ;

    /** @brief queue size accessor
      *
      */
    inline uint getWaiting() const { return waitingFunctorsSize; }


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
    EXPORT void invoke(const Functor_T &threadfunc);

    /** @brief stop the threads
      */
    EXPORT void stop();

    /** @brief wait on all the threads to complete
      */
    EXPORT void wait();

    /** @brief for use in debugging
      */
    EXPORT void dump();

protected:

private:
    /** @brief initialize data memebers
      */
    void init();

    /** @brief add a functor to the list
      */
    void addFunctor(const Functor_T &func);

    /** @brief thread entry point
      */
    void beginThread() throw();


    ThreadPool(const ThreadPool&);
    ThreadPool& operator = (const ThreadPool&);

    friend struct beginThreadFunc;

    struct beginThreadFunc
    {
        beginThreadFunc(ThreadPool& impl)
                : fImpl(impl)
        {}

        void operator() ()
        {
            fImpl.beginThread();
        }

        ThreadPool &fImpl;
    };

    struct NoOp
    {
        void operator () () const
        {}}
    ;

    size_t fThreadCount;
    size_t fMaxThreads;
    size_t fQueueSize;

    typedef std::list<Functor_T> Container_T;
    Container_T fWaitingFunctors;
    Container_T::iterator fNextFunctor;
//     Functor_T	* fThreadCreated;

	uint32_t issued;
    boost::mutex fMutex;
    boost::condition fThreadAvailable; // triggered when a thread is available
    boost::condition fNeedThread;      // triggered when a thread is needed
    boost::thread_group fThreads;

    bool 	fStop;
    long 	fGeneralErrors;
    long	fFunctorErrors;
	uint 	waitingFunctorsSize;

};

} // namespace threadpool

#undef EXPORT

#endif //THREADPOOL_H
