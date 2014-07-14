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

/*****************************************************************************
 * $Id$
 *
 ****************************************************************************/

#include <unistd.h>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sstream>
#include <iomanip>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif

#define NDEBUG
#include <cassert>
using namespace std;

#include <boost/thread/thread.hpp>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/sync/named_semaphore.hpp>
#include <boost/version.hpp>
namespace bi=boost::interprocess;

#define RWLOCK_DLLEXPORT
#include "rwlock.h"
#undef RWLOCK_DLLEXPORT

using namespace boost::posix_time;

#include "shmkeys.h"

namespace
{
using namespace rwlock;

//This mutex needs to be fully instantiated by the runtime static object
// init mechanism or the lock in makeRWLockShmImpl() will fail
boost::mutex instanceMapMutex;
typedef std::tr1::unordered_map<int, RWLockShmImpl*> LockMap_t;
//Windows doesn't init static objects the same as Linux, so make this a ptr
LockMap_t* lockMapPtr=0;

}

namespace rwlock
{

#if defined(DEBUG) && !defined(_MSC_VER)
#define RWLOCK_DEBUG
#endif


#ifdef RWLOCK_DEBUG
#define PRINTSTATE() \
	cerr << "  reading = " << fPImpl->fState->reading << endl \
		<< "  writing = " << fPImpl->fState->writing << endl \
		<< "  readerswaiting = " << fPImpl->fState->readerswaiting << endl \
		<< "  writerswaiting = " << fPImpl->fState->writerswaiting << endl

#define CHECKSAFETY() \
	do { if (!\
		(\
			(fPImpl->fState->reading == 0 && \
				(fPImpl->fState->writing == 0 || fPImpl->fState->writing == 1)) || \
			(fPImpl->fState->reading > 0 && fPImpl->fState->writing == 0) \
		)) { \
		cerr << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": safety invariant violation" << endl; \
		PRINTSTATE(); \
		throw std::logic_error("RWLock: safety invariant violation"); \
	} } while (0)

#define CHECKLIVENESS() \
	do { if (!( \
		(!(fPImpl->fState->readerswaiting > 0 || fPImpl->fState->writerswaiting > 0) || \
			(fPImpl->fState->reading > 0 || fPImpl->fState->writing > 0)) || \
		(!(fPImpl->fState->reading == 0 && fPImpl->fState->writing == 0) || \
			(fPImpl->fState->readerswaiting == 0 && fPImpl->fState->writerswaiting == 0)) \
		)) { \
		cerr << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": liveness invariant violation" << endl; \
		PRINTSTATE(); \
		throw std::logic_error("RWLock: liveness invariant violation"); \
	} } while (0)
#else
#define PRINTSTATE() (void)0
#define CHECKSAFETY() (void)0
#define CHECKLIVENESS() (void)0
#endif

/*static*/
RWLockShmImpl* RWLockShmImpl::makeRWLockShmImpl(int key, bool* excl)
{
	boost::mutex::scoped_lock lk(instanceMapMutex);
    LockMap_t::iterator iter;

	if (!lockMapPtr)
		lockMapPtr = new LockMap_t();

    iter = lockMapPtr->find(key);
    if (iter == lockMapPtr->end())
    {
		RWLockShmImpl* ptr=0;
        bool bExcl = excl ? *excl : false;
        ptr = new RWLockShmImpl(key, bExcl);
        lockMapPtr->insert(make_pair(key, ptr));
        return ptr;
    }
    else if (excl)
    {
        *excl = false;   // This isn't the first time for this lock.
    }

	return iter->second;
}

RWLockShmImpl::RWLockShmImpl(int key, bool excl)
{
	string keyName = BRM::ShmKeys::keyToName(key);
	fKeyString = keyName;
	try
	{
#if BOOST_VERSION < 104500
		bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write);
#ifdef __linux__
		{
			string pname = "/dev/shm/" + keyName;
			chmod(pname.c_str(), 0666);
		}
#endif
#else
		bi::permissions perms;
		perms.set_unrestricted();
		bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write, perms);
#endif
		shm.truncate(sizeof(struct State));
		fStateShm.swap(shm);
		bi::mapped_region region(fStateShm, bi::read_write);
		fRegion.swap(region);
		fState = static_cast<State*>(fRegion.get_address());
		fState->writerswaiting = 0;
		fState->readerswaiting = 0;
		fState->reading = 0;
		if (excl)
			fState->writing = 1;
		else
			fState->writing = 0;
		new (&fState->sems[RWLock::MUTEX]) bi::interprocess_semaphore(1);
		new (&fState->sems[RWLock::READERS]) bi::interprocess_semaphore(0);
		new (&fState->sems[RWLock::WRITERS]) bi::interprocess_semaphore(0);
	}
	catch (bi::interprocess_exception&)
	{
		if (excl) {
			//don't think we can get here anymore...
			throw not_excl();
		}
		bi::shared_memory_object shm(bi::open_only, keyName.c_str(), bi::read_write);
		fStateShm.swap(shm);
		bi::mapped_region region(fStateShm, bi::read_write);
		fRegion.swap(region);
		fState = static_cast<State*>(fRegion.get_address());
	}
	catch (...)
	{
		runtime_error rex("RWLockShmImpl::RWLockShmImpl(): caught unknown exception");
		cerr << rex.what() << endl;
		throw rex;
	}
}

RWLock::RWLock(int key, bool* excl)
{
	fPImpl = RWLockShmImpl::makeRWLockShmImpl(key, excl);
}

RWLock::~RWLock()
{
}

void RWLock::down(int num, bool block)
{

again:
	try {
		if (block)
			fPImpl->fState->sems[num].wait();
		else {
			bool gotIt = fPImpl->fState->sems[num].try_wait();
			if (!gotIt)
				throw wouldblock();
		}
	}
	catch (bi::interprocess_exception& bipe) {
		ostringstream os;
		os << "RWLock::down(): caught boost ipe: " << bipe.what() << " key = " << fPImpl->keyString() <<
			" error code = " << bipe.get_error_code();
		if (bipe.get_error_code() == 1)   // it passes through EINTR apparently
			goto again;
		runtime_error rex(os.str());
		cerr << bipe.what() << endl;
		throw rex;
	} catch (std::exception& ex) {
		cerr <<
#ifdef _MSC_VER
			__FUNCTION__ <<
#else
			__PRETTY_FUNCTION__ <<
#endif
			":" << __LINE__ << ": caught an exception: " << ex.what() << endl;
		throw;
	} catch (...) {
		cerr <<
#ifdef _MSC_VER
			__FUNCTION__ <<
#else
			__PRETTY_FUNCTION__ <<
#endif
			":" << __LINE__ << ": caught an exception" << endl;
		throw runtime_error("RWLock::down(): caught an exception");
	}
}

bool RWLock::timed_down(int num, const ptime &delay)
{
	bool gotTheLock = false;

//	cout << "timed_down: current time = " << to_simple_string(microsec_clock::local_time()) <<
//			"  wake time = " << to_simple_string(delay) << endl;

again:
	try {
		// I don't think I've seen timed_wait() ever wait, need to do this 'manually'
		//gotTheLock = fPImpl->fState->sems[num].timed_wait(delay);
		do {
			gotTheLock = fPImpl->fState->sems[num].try_wait();
			if (!gotTheLock)
				usleep(100000);
		} while (!gotTheLock && microsec_clock::local_time() < delay);
	}
	catch (boost::thread_interrupted&) {
		// no need to do anything here
	}
	catch (bi::interprocess_exception& bipe) {
		ostringstream os;
		os << "RWLock::timed_down(): caught boost ipe: " << bipe.what() << " key = " << fPImpl->keyString() <<
			" error code = " << bipe.get_error_code();
		if (bipe.get_error_code() == 1)   // it passes through EINTR apparently
			goto again;
		runtime_error rex(os.str());
		cerr << bipe.what() << endl;
		throw rex;
	}
	catch (std::exception& ex) {
		cerr <<
#ifdef _MSC_VER
			__FUNCTION__ <<
#else
			__PRETTY_FUNCTION__ <<
#endif
			":" << __LINE__ << ": caught an exception: " << ex.what() << endl;
		throw;
	}
	catch (...) {
		cerr <<
#ifdef _MSC_VER
			__FUNCTION__ <<
#else
			__PRETTY_FUNCTION__ <<
#endif
			":" << __LINE__ << ": caught an exception" << endl;
		throw runtime_error("RWLock::timed_down(): caught an exception");
	}
	return gotTheLock;
}

void RWLock::up(int num)
{
	try {
		fPImpl->fState->sems[num].post();
	} catch (bi::interprocess_exception& bipe) {
		ostringstream os;
		os << "RWLock::up(): caught boost ipe: " << bipe.what() << " key = " << fPImpl->keyString();
		runtime_error rex(os.str());
		cerr << bipe.what() << endl;
		throw rex;
	} catch (...) {
		throw runtime_error("RWLock::up(): caught an exception");
	}
}

void RWLock::read_lock(bool block)
{
	down(MUTEX, true);
	CHECKSAFETY();
	CHECKLIVENESS();
	
	if (fPImpl->fState->writerswaiting > 0 || fPImpl->fState->writing > 0) {
		if (!block) {
			up(MUTEX);
			throw wouldblock();
		}

		fPImpl->fState->readerswaiting++;
		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
		down(READERS);			//unblocked by write_unlock();
#ifdef RWLOCK_DEBUG
		down(MUTEX, true);
		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
#endif
	}
	else {
		fPImpl->fState->reading++;

		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
	}
}
	
void RWLock::read_lock_priority(bool block)
{
	down(MUTEX, true);
	CHECKSAFETY();
	CHECKLIVENESS();
	
	if (fPImpl->fState->writing > 0) {
		if (!block) {
			up(MUTEX);
			throw wouldblock();
		}

		fPImpl->fState->readerswaiting++;
		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
		down(READERS);			//unblocked by write_unlock();
#ifdef RWLOCK_DEBUG
		down(MUTEX, true);
		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
#endif
	}
	else {
		fPImpl->fState->reading++;

		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
	}
}

void RWLock::read_unlock()
{
	down(MUTEX, true);
	CHECKSAFETY();
	CHECKLIVENESS();

	/* Made this a bit more tolerant of errors b/c the lock recovery code can technically
	 * be wrong.  In the practically zero chance that happens, better to take the chance
	 * on a read race than a sure assertion failure
	 */
	if (fPImpl->fState->reading > 0) {
		--fPImpl->fState->reading;
		if (fPImpl->fState->writerswaiting > 0 && fPImpl->fState->reading == 0) {
			--fPImpl->fState->writerswaiting;
			fPImpl->fState->writing++;
			up(WRITERS);
		}
	}

	CHECKSAFETY();
	CHECKLIVENESS();
	up(MUTEX);
}

void RWLock::write_lock(bool block)
{
	down(MUTEX, true);
	CHECKSAFETY();
	CHECKLIVENESS();

	if (fPImpl->fState->writing > 0 || fPImpl->fState->reading > 0) {
		if (!block) {
			up(MUTEX);
			throw wouldblock();
		}

		fPImpl->fState->writerswaiting++;
		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
		down(WRITERS);	//unblocked by write_unlock() or read_unlock()
#ifdef RWLOCK_DEBUG
		down(MUTEX, true);
		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
#endif
	}
	else {
		fPImpl->fState->writing++;

		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
	}
}

// this exists only for the sake of code cleanup
#define RETURN_STATE(mutex_state, state) \
	if (state) { \
		state->mutexLocked = mutex_state; \
		state->readerswaiting = fPImpl->fState->readerswaiting; \
		state->reading = fPImpl->fState->reading; \
		state->writerswaiting = fPImpl->fState->writerswaiting; \
		state->writing = fPImpl->fState->writing; \
	}

bool RWLock::timed_write_lock(const struct timespec &ts, struct LockState *state)
{
	bool gotIt, gotIt2;
	ptime delay;

	delay = microsec_clock::local_time() + seconds(ts.tv_sec) + microsec(ts.tv_nsec/1000);

	gotIt = timed_down(MUTEX, delay);
	if (!gotIt) {
		RETURN_STATE(true, state)
		return false;
	}
	CHECKSAFETY();
	CHECKLIVENESS();
	if (fPImpl->fState->writing > 0 || fPImpl->fState->reading > 0) {
		fPImpl->fState->writerswaiting++;

		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
		gotIt = timed_down(WRITERS, delay);
		if (!gotIt) {
			// need to grab the mutex again to revoke the lock request
			// need to adjust the timeout value to make sure the attempt is made.
			delay = microsec_clock::local_time() + seconds(10);
			gotIt2 = timed_down(MUTEX, delay);
			if (!gotIt2) {
				RETURN_STATE(true, state);
				return false;
			}

			/* This thread could have been granted the write lock during that second
			   lock grab attempt.  Observation: the thread that granted it up'd the writers sem,
			   but didn't wake this thread.  If there were other writers waiting, one of those
			   woke.  At this point, if writerswaiting > 0, this thread should consider itself
			   one that's still waiting and should back out of the request.
			   If writerswaiting == 0, this thread was the one granted the write lock, and
			   needs to take possession.
			 */
			if (fPImpl->fState->writerswaiting == 0) {
				try {
					down(WRITERS, false);
				}
				catch(const wouldblock&) {
					// Somehow another writer was able to jump in front.  This is "impossible".
					RETURN_STATE(false, state);
					up(MUTEX);
					return false;
				}
				up(MUTEX);
				return true;
			}

			fPImpl->fState->writerswaiting--;

			// need to unblock whatever was blocked by this lock attempt
			if (fPImpl->fState->writing == 0 && fPImpl->fState->writerswaiting == 0) {
				fPImpl->fState->reading += fPImpl->fState->readerswaiting;
				while (fPImpl->fState->readerswaiting > 0) {
					--fPImpl->fState->readerswaiting;
					up(READERS);
				}
			}
			// else if there's an active writer, do nothing

			CHECKSAFETY();
			CHECKLIVENESS();
			RETURN_STATE(false, state);
			up(MUTEX);
			return false;
		}
		else
			return true;
	}
	else {
		fPImpl->fState->writing++;
		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
		return true;
	}
}

void RWLock::write_unlock()
{
	down(MUTEX, true);
	CHECKSAFETY();
	CHECKLIVENESS();

	assert(fPImpl->fState->writing > 0);
	--fPImpl->fState->writing;
	if (fPImpl->fState->writerswaiting > 0) {
		--fPImpl->fState->writerswaiting;
		fPImpl->fState->writing++;
		up(WRITERS);
	}
	else if (fPImpl->fState->readerswaiting > 0) {
		//let up to state->readerswaiting readers go

		fPImpl->fState->reading = fPImpl->fState->readerswaiting;
		while (fPImpl->fState->readerswaiting > 0)
		{
			--fPImpl->fState->readerswaiting;
			up(READERS);
		}
	}
	CHECKSAFETY();
	CHECKLIVENESS();
	up(MUTEX);
}

void RWLock::upgrade_to_write()
{
	
	down(MUTEX, true);
	CHECKSAFETY();
	CHECKLIVENESS();

	/* Made this a bit more tolerant of errors b/c the lock recovery code can technically
	 * be wrong.  In the practically zero chance that happens, better to take the chance
	 * on a read race than a sure assertion failure
	 */
	if (fPImpl->fState->reading > 0) {
		--fPImpl->fState->reading;
	
		// try to cut in line
		// On entry we hold a read lock, so reading > 0, and at this point, reading >= 0
		if (fPImpl->fState->reading == 0) {
			fPImpl->fState->writing++;
			CHECKSAFETY();
			CHECKLIVENESS();
			up(MUTEX);
			return;
		}
	}
		
	// cut & paste from write_lock()
	// The invariants hold that, on entry, writing == 0 and, by now, reading > 0, so this branch is always taken
	if (fPImpl->fState->writing > 0 || fPImpl->fState->reading > 0) {

		fPImpl->fState->writerswaiting++;

		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
		down(WRITERS, true);	//unblocked by write_unlock() or read_unlock()
#ifdef RWLOCK_DEBUG
		down(MUTEX, true);
		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
#endif
	}
	else { // don't think we can get here: reading > 0 && writing == 0 && reading == 0
		fPImpl->fState->writing++;

		CHECKSAFETY();
		CHECKLIVENESS();
		up(MUTEX);
	}
}

/* It's safe (and necessary) to simply convert this writer to a reader without
 blocking */
void RWLock::downgrade_to_read()
{
	down(MUTEX, true);
	CHECKSAFETY();
	CHECKLIVENESS();
	assert(fPImpl->fState->writing>0);
	--fPImpl->fState->writing;
	
	if (fPImpl->fState->readerswaiting > 0) {
		//let up to state->readerswaiting readers go
		fPImpl->fState->reading = fPImpl->fState->readerswaiting;
		while (fPImpl->fState->readerswaiting > 0)
		{
			--fPImpl->fState->readerswaiting;
			up(READERS);
		}
	}
	fPImpl->fState->reading++;
	CHECKSAFETY();
	CHECKLIVENESS();
	up(MUTEX);
}

void RWLock::reset()
{
	fPImpl->fState->writerswaiting = 0;
	fPImpl->fState->readerswaiting = 0;
	fPImpl->fState->reading = 0;
	fPImpl->fState->writing = 0;
	for (int i = 0; i < 3; i++)
	{
		while (fPImpl->fState->sems[i].try_wait())
		{
		}
	}
	fPImpl->fState->sems[MUTEX].post();
}

LockState RWLock::getLockState()
{
	bool gotIt;
	LockState ret;

	gotIt = fPImpl->fState->sems[MUTEX].try_wait();
	ret.reading = fPImpl->fState->reading;
	ret.writing = fPImpl->fState->writing;
	ret.readerswaiting = fPImpl->fState->readerswaiting;
	ret.writerswaiting = fPImpl->fState->writerswaiting;
	ret.mutexLocked = !gotIt;
	if (gotIt)
		fPImpl->fState->sems[MUTEX].post();

	return ret;
}

} //namespace rwlock
// vim:ts=4 sw=4:

