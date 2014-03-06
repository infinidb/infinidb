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

#ifndef PRIORITYTHREADPOOL_H
#define PRIORITYTHREADPOOL_H

#include <string>
#include <iostream>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include "winport.h"

namespace threadpool
{

class PriorityThreadPool
{
public:

	class Functor {
	public:
		virtual ~Functor() { };
		// as of 12/3/13, all implementors return 0 and -1.  -1 will cause
        // this thread pool to reschedule the job, 0 will throw it away on return.
		virtual int operator()() = 0;
	};

    //typedef boost::function0<int> Functor;

	struct Job {
		Job() : weight(1), priority(0), id(0) { }
		boost::shared_ptr<Functor> functor;
		uint32_t weight;
		uint32_t priority;
		uint32_t id;
	};

	enum Priority {
		LOW,
		MEDIUM,
		HIGH,
		_COUNT
	};

    /*********************************************
     *  ctor/dtor
     *
     *********************************************/

    /** @brief ctor
      */

    PriorityThreadPool(uint targetWeightPerRun, uint highThreads, uint midThreads,
    		uint lowThreads, uint id = 0);
    virtual ~PriorityThreadPool();

    void removeJobs(uint32_t id);
    void addJob(const Job &job, bool useLock = true);
    void stop();

    /** @brief for use in debugging
      */
    void dump();

protected:

private:
    struct ThreadHelper {
        ThreadHelper(PriorityThreadPool *impl, Priority queue) : ptp(impl), preferredQueue(queue) { }
        void operator()() { ptp->threadFcn(preferredQueue); }
        PriorityThreadPool *ptp;
        Priority preferredQueue;
    };

    explicit PriorityThreadPool();
    explicit PriorityThreadPool(const PriorityThreadPool &);
    PriorityThreadPool & operator=(const PriorityThreadPool &);

    Priority pickAQueue(Priority preference);
    void threadFcn(const Priority preferredQueue) throw();

    std::list<Job> jobQueues[3];  // higher indexes = higher priority
    uint32_t threadCounts[3];
    boost::mutex mutex;
    boost::condition newJob;
    boost::thread_group threads;
    bool _stop;
    uint32_t weightPerRun;
    volatile uint id;   // prevent it from being optimized out
};

} // namespace threadpool

#endif //PRIORITYTHREADPOOL_H
