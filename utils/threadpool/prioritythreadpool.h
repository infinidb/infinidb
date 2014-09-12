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

namespace threadpool
{

class PriorityThreadPool
{
public:

	class Functor {
	public:
		virtual ~Functor() { };
		virtual int operator()() = 0;
	};

    //typedef boost::function0<int> Functor;

	struct Job {
		boost::shared_ptr<Functor> functor;
		uint weight;
		uint priority;
		uint id;
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
    		uint lowThreads);
    virtual ~PriorityThreadPool();

    void removeJobs(uint id);
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
    uint threadCounts[3];
    boost::mutex mutex;
    boost::condition newJob;
    boost::thread_group threads;
    bool _stop;
    uint weightPerRun;
};

} // namespace threadpool

#endif //PRIORITYTHREADPOOL_H
