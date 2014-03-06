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
*   $Id: threadpool.cpp 553 2008-02-27 17:51:16Z rdempsey $
*
*
***********************************************************************/

#include <stdexcept>
#include <unistd.h>
using namespace std;

#include "messageobj.h"
#include "messagelog.h"
using namespace logging;

#include "weightedthreadpool.h"

namespace threadpool
{

WeightedThreadPool::WeightedThreadPool()
        :fMaxThreadWeight(0), fMaxThreads( 0 ), fQueueSize( 0 )
{
    init();
}

WeightedThreadPool::WeightedThreadPool( size_t maxThreadWeight, size_t maxThreads, size_t queueSize )
        :fMaxThreadWeight(maxThreadWeight), fMaxThreads( maxThreads ), fQueueSize( queueSize )
{
    init();

    if (fQueueSize == 0)
        fQueueSize = fMaxThreads*2;
}


WeightedThreadPool::~WeightedThreadPool() throw()
{
// 	delete fThreadCreated;
    try
    {
        stop();
    }
    catch(...)
    {}
}


void WeightedThreadPool::init()
{
    fThreadCount = 0;
    fGeneralErrors = 0;
    fFunctorErrors = 0;
	fWaitingFunctorsSize = 0;
	fWaitingFunctorsWeight=0;
	issued = 0;
    fStop = false;
//     fThreadCreated = new NoOp();
    fNextFunctor = fWaitingFunctors.end();
}


void WeightedThreadPool::setQueueSize(size_t queueSize)
{
    boost::mutex::scoped_lock lock1(fMutex);
    fQueueSize = queueSize;
}


void WeightedThreadPool::setMaxThreads(size_t maxThreads)
{
    boost::mutex::scoped_lock lock1(fMutex);
    fMaxThreads = maxThreads;
}


void WeightedThreadPool::setMaxThreadWeight(size_t maxWeight)
{
    boost::mutex::scoped_lock lock1(fMutex);
    fMaxThreadWeight = maxWeight;
}


void WeightedThreadPool::setThreadCreatedListener(const Functor_T &f)
{
//     fThreadCreated = f;
}


void WeightedThreadPool::stop()
{
    boost::mutex::scoped_lock lock1(fMutex);
    fStop = true;
    lock1.unlock();

    fNeedThread.notify_all();
    fThreads.join_all();
}


void WeightedThreadPool::wait()
{
    boost::mutex::scoped_lock lock1(fMutex);

    while (fWaitingFunctorsSize > 0)
    {
		//cout << "waiting ..." << endl;
        fThreadAvailable.wait(lock1);
		//cerr << "woke!" << endl;
    }
}

void WeightedThreadPool::removeJobs(uint32_t id)
{
	boost::mutex::scoped_lock lock1(fMutex);
	Container_T::iterator it;

	it = fNextFunctor;
	while (it != fWaitingFunctors.end()) {
		if (it->id == id) {
			fWaitingFunctorsWeight -= it->functorWeight;
			fWaitingFunctorsSize--;
			if (it == fNextFunctor) {
				fWaitingFunctors.erase(fNextFunctor++);
				it = fNextFunctor;
			}
			else
				fWaitingFunctors.erase(it++);
		}
		else
			++it;
	}
}

void WeightedThreadPool::invoke(const Functor_T &threadfunc, uint32_t functor_weight,
		uint32_t id)
{
    boost::mutex::scoped_lock lock1(fMutex);

    for(;;)
    {
        try
        {
            if ( fWaitingFunctorsSize < fThreadCount)
            {
                // Don't create a thread unless it's needed.  There
                // is a thread available to service this request.
                addFunctor(threadfunc, functor_weight, id);
                lock1.unlock();
                break;
            }

            bool bAdded = false;

            if ( fWaitingFunctorsSize < fQueueSize)
            {
                // Don't create a thread unless you have to
                addFunctor(threadfunc, functor_weight, id);
                bAdded = true;
            }

			// add a thread is necessary
            if ( fThreadCount < fMaxThreads)
            {
                ++fThreadCount;
				//cout << "\t++invoke() tcnt=" << fThreadCount << endl;
                lock1.unlock();
                fThreads.create_thread(beginThreadFunc(*this));

                if (bAdded)
                    break;

                // If the mutex is unlocked before creating the thread
                // this allows fThreadAvailable to be triggered
                // before the wait below runs.  So run the loop again.
                lock1.lock();
                continue;
            }
			//else
			//	cout << "invoke() no thread created c=" << fThreadCount << " m=" << fMaxThreads << endl;

            if (bAdded)
            {
                lock1.unlock();
                break;
            }

            fThreadAvailable.wait(lock1);
        }
        catch(...)
        {
            ++fGeneralErrors;
            throw;
        }
    }

    fNeedThread.notify_one();
}

void WeightedThreadPool::beginThread() throw()
{
	vector<bool> reschedule;
    try
    {
//         fThreadCreated();
        boost::mutex::scoped_lock lock1(fMutex);

        for(;;)
        {
            if (fStop)
                break;

            if (fNextFunctor == fWaitingFunctors.end())
            {
                // Wait until someone needs a thread
                fNeedThread.wait(lock1);
            }
            else
            {
				vector<Container_T::iterator> todoList;
				int i, num = (fWaitingFunctorsSize - issued);
				Container_T::const_iterator iter;
				uint32_t weight=0;

				for (i = 0; i < num && weight < fMaxThreadWeight; i++) {
					weight += (*fNextFunctor).functorWeight;
                	todoList.push_back(fNextFunctor++);
				}
				issued+=i;
				num=i;
                lock1.unlock();
				
   				//cerr << "beginThread() " << num
				//	<< " jobs - fWaitingFunctorsSize=" << fWaitingFunctorsSize
				//	<< " fWaitingFunctorsWeight=" << fWaitingFunctorsWeight
				//	<< " weight=" << weight
				//	<< " issued=" << issued << " todo=" << todoList.size()
				//	<< " fThreadCount=" << fThreadCount << endl;

				i = 0;
				reschedule.resize(num);
				bool allWereRescheduled = true, someWereRescheduled = false;
				while (i < num) {
					try {
						for (; i < num; i++) {
							reschedule[i] = false;  // in case of exception in the next line
		                    reschedule[i] = ((*todoList[i]).functor)();
							allWereRescheduled &= reschedule[i];
							someWereRescheduled |= reschedule[i];
						}
					}
					catch(exception &e) {
						i++;
						++fFunctorErrors;
						cerr << e.what() << endl;
					}
				}
				
				// no real work was done, prevent intensive busy waiting
				if (allWereRescheduled)
					usleep(1000);

				//cout << "running " << i << "/" << num << " functor" <<endl;
				lock1.lock();

				if (someWereRescheduled) {
					for (i = 0; i < num; i++)
						if (reschedule[i])
							addFunctor((*todoList[i]).functor, (*todoList[i]).functorWeight,
							  (*todoList[i]).id);
					if (num > 1)
						fNeedThread.notify_all();
					else
						fNeedThread.notify_one();
				}

				issued -= num;
				for (i = 0; i < num; i++) {
					fWaitingFunctorsWeight-=(*todoList[i]).functorWeight;
					fWaitingFunctors.erase(todoList[i]);
				}
				fWaitingFunctorsSize -= num;

				//if (fWaitingFunctorsSize != fWaitingFunctors.size()) ;
				//	cerr << "num=" << num << " cleaned=" << i << " size="
				//		<< fWaitingFunctorsSize << " list size="
				//		<< fWaitingFunctors.size()
				//		<< " w="<<fWaitingFunctorsWeight << endl;

                fThreadAvailable.notify_all();
				
            }
        }
    }
    catch (exception& ex)
    {

        ++fGeneralErrors;

        // Log the exception and exit this thread
        try
        {
            logging::Message::Args args;
            logging::Message message(5);
            args.add("beginThread: Caught exception: ");
            args.add(ex.what());

            message.format( args );

            logging::LoggingID lid(22);
            logging::MessageLog ml(lid);

            ml.logErrorMessage( message );

        }
        catch(...)
        {
        }

    }
    catch(...)
    {
        ++fGeneralErrors;

        // Log the exception and exit this thread
        try
        {
            logging::Message::Args args;
            logging::Message message(6);
            args.add("beginThread: Caught unknown exception!");

            message.format( args );

            logging::LoggingID lid(22);
            logging::MessageLog ml(lid);

            ml.logErrorMessage( message );
        }
        catch(...)
        {
        }
    }
}

void WeightedThreadPool::addFunctor(const Functor_T &func, uint32_t functor_weight,
		uint32_t id)
{
    bool bAtEnd=false;

    if (fNextFunctor == fWaitingFunctors.end())
        bAtEnd = true;

	//cout << "addFunctor() w=" << fWaitingFunctorsWeight
	//	<< " s=" << fWaitingFunctorsSize << " i=" << id << endl;

	FunctorListItem fl = {func, functor_weight, id};
    fWaitingFunctors.push_back(fl);
	fWaitingFunctorsSize++;
	fWaitingFunctorsWeight+=functor_weight;

    if (bAtEnd)
    {
        --fNextFunctor;
    }
}

void WeightedThreadPool::dump()
{
    std::cout << "General Errors: " << fGeneralErrors << std::endl;
    std::cout << "Functor Errors: " << fFunctorErrors << std::endl;
    std::cout << "Waiting functors: " << fWaitingFunctors.size() << std::endl;
    std::cout << "Waiting functors weight : " << fWaitingFunctorsWeight << std::endl;
}

} // namespace threadpool
