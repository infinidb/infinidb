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
 * $Id$
 *
 *****************************************************************************/

/** @file 
 * class RWLock interface
 */

#ifndef RWLOCK_H_
#define RWLOCK_H_

#include <unistd.h>
#include <stdexcept>

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#if defined(_MSC_VER) && defined(xxxRWLOCK_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace rwlock
{

/// the layout of the shmseg
struct State {
#ifdef _MSC_VER
	volatile LONG writerswaiting;
	volatile LONG writing;
	volatile LONG readerswaiting;
	volatile LONG reading;
#else
	volatile int writerswaiting;
	volatile int writing;
	volatile int readerswaiting;
	volatile int reading;
#endif
	boost::interprocess::interprocess_semaphore sems[3];
};

/* the lock state without the semaphores, passed out by timed_write_lock() for
class RWLockMonitor
*/
struct LockState {
#ifdef _MSC_VER
	LONG writerswaiting;
	LONG writing;
	LONG readerswaiting;
	LONG reading;
	bool mutexLocked;
#else
	int writerswaiting;
	int writing;
	int readerswaiting;
	int reading;
	bool mutexLocked;
#endif
};

class RWLockShmImpl
{
public:
	static RWLockShmImpl* makeRWLockShmImpl(int key, bool* excl=0);

	boost::interprocess::shared_memory_object fStateShm;
	boost::interprocess::mapped_region fRegion;
	State *fState;

	std::string keyString() { return fKeyString; }
private:
	explicit RWLockShmImpl(int key, bool excl=false);
	~RWLockShmImpl();
	RWLockShmImpl(const RWLockShmImpl& rhs);
	RWLockShmImpl& operator=(const RWLockShmImpl& rhs);
	std::string fKeyString;
};

class not_excl : public std::exception
{
	public:
	virtual const char* what() const throw() {
		return "not_excl";
	}
};

class wouldblock : public std::exception
{
	public:
	virtual const char* what() const throw() {
		return "wouldblock";
	}
};

/** @brief Implements RW locks for use across threads & processes
 * 
 * Implements RW locks for use across threads & processes.  Every 
 * instance that shares a lock must be instantiated using the same
 * key.  There is 'no limit' on the number of RW locks that can 
 * exist on the system at any one time. 
 *
 * Summary of operation:
 * 		- readers can work concurrently
 *		- writers get exclusive access
 * 		- writers have priority
 *		- all state persists across all invocations sharing a given key
 *
 * Note: because state has to persist, it will have to be cleaned
 * up somewhere else.  Crashes while holding a read or write lock will
 * eventually deadlock the set of processes that share the same key obviously.
 */
class RWLock {
	public:

		// semaphore numbers
		static const int MUTEX=0;
		static const int READERS=1;
		static const int WRITERS=2;

		/** @brief Keyed constructor.
		 *
		 * Instantiate an RWLock with the given key.  All instances that
		 * share a key share the same lock.
		 * 
		 * @param key The key
		 * @param excl If true and this is the first instance with the 
		 * supplied key, it will return holding the write lock.  If true and
		 * this is not the first instance, it will throw not_excl.  The intent
		 * is similar to the IPC_EXCL flag in the sem/shm implementations.
		 */
		EXPORT explicit RWLock(int key, bool* excl = 0);

		EXPORT ~RWLock();
		
		/** @brief Grab a read lock
		 *
		 * Grab a read lock.  This will block iff writers are waiting or
		 * a writer is active. The version with priority ignores any 
		 * waiting threads and grabs the lock. 
		 * 
		 * @param block (For testing only) If false, will throw 
		 * wouldblock instead of blocking
		 */
		EXPORT void read_lock(bool block = true);

		EXPORT void read_lock_priority(bool block = true);

		/** @brief Release a read lock.
		 *
		 * Release a read lock.
		 */
		EXPORT void read_unlock();

		/** @brief Grab a write lock
		 *
		 * Grab a write lock.  This will block while another writer or reader is 
		 * active and will have exclusive access on waking.
		 *
		 * @param block (For testing only) If false, will throw 
		 * wouldblock instead of blocking
		 */
		EXPORT void write_lock(bool block = true);

		/** @brief A timed write lock.
		 *
		 * Queues up for the write lock for a specified amount of time.  Returns
		 * true if it got the lock, return false if it timed out first.
		 * If the timeout happens, it will also return the lock state if passed
		 * a non-NULL LockState struct.  This is a specialization for supporting
		 * the RWLockMonitor class.
		 */
		EXPORT bool timed_write_lock(const struct timespec &ts,
				struct LockState *state = 0);

		/** @brief Release a write lock.
		 *
		 * Release a write lock.
		 */
		EXPORT void write_unlock();

		/* note: these haven't been proven yet */
		
		/** @brief Upgrade a read lock to a write lock
		 *
		 * Upgrade a read lock to a write lock.  It may have to block
		 * if there are other readers currently reading.  No guarantees of atomicity.
		 */
		EXPORT void upgrade_to_write();
		
		/** @brief Downgrade a write lock to a read lock
		 *
		 * Downgrade a write lock to a read lock.  The conversion happens 
		 * atomically.
		 */
		EXPORT void downgrade_to_read();
		
		/** @brief Reset the lock's state (Use with caution!)
		 * 
		 * If the lock gets into a bad state in testing or something,
		 * this will reset the state.
		 * @warning This is safe only if there are no other threads using this 
		 * lock.
	 	 */
		EXPORT void reset();

		/* These are for white box testing only */
		inline void lock() { down(MUTEX, true); }
		inline void unlock() { up(MUTEX); }
		inline int getWriting() const { return fPImpl->fState->writing; }
		inline int getReading() const { return fPImpl->fState->reading; }
		inline int getWritersWaiting() const { return fPImpl->fState->writerswaiting; }
		inline int getReadersWaiting() const { return fPImpl->fState->readerswaiting; }
		LockState getLockState();
		
	private:
		RWLock(const RWLock& rwl);
		RWLock& operator=(const RWLock& rwl);

		inline int getSemval(int) const { return 0; }
		void down(int num, bool block = true);
		bool timed_down(int num, const boost::posix_time::ptime &ts);  // to support timed_write_lock()
		void up(int num);

		RWLockShmImpl* fPImpl;
};

} //namespace rwlock

#undef EXPORT

#endif
// vim:ts=4 sw=4:
