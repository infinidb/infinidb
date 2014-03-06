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
 * class RWLock_local interface
 */

#ifndef RWLock_local_LOCAL_H_
#define RWLock_local_LOCAL_H_

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#if defined(_MSC_VER) && defined(xxxRWLOCK_LOCAL_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace rwlock {

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


class RWLock_local {
	public:
		
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
		
		/** @brief Keyed constructor.
		 *
		 * Instantiate an RWLock_local with the given key.  All instances that
		 * share a key share the same lock.
		 * 
		 * @param key The key
		 * @param excl If true and this is the first instance with the 
		 * supplied key, it will return holding the write lock.  If true and
		 * this is not the first instance, it will throw not_excl.  The intent
		 * is similar to the IPC_EXCL flag in the sem/shm implementations.
		 */
		EXPORT RWLock_local();

		EXPORT ~RWLock_local();
		
		/** @brief Grab a read lock
		 *
		 * Grab a read lock.  This will block iff writers are waiting or
		 * a writer is active.
		 * 
		 * @param block (For testing only) If false, will throw 
		 * wouldblock instead of blocking
		 */
		EXPORT void read_lock();
		
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
		EXPORT void write_lock();
		
		/** @brief Release a write lock.
		 *
		 * Release a write lock.
		 */
		EXPORT void write_unlock();
		
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

		/* These are for white box testing only */
		EXPORT void lock();
		EXPORT void unlock();
		EXPORT int getWriting();
		EXPORT int getReading();
		EXPORT int getWritersWaiting();
		EXPORT int getReadersWaiting();
		
	private:
		// Not copyable
		RWLock_local(const RWLock_local& rwl);
		RWLock_local& operator=(const RWLock_local& rwl);

		/// the layout of the shmseg
		struct State {
			int writerswaiting, writing, readerswaiting, reading;
		} state;
		boost::mutex mutex;
		boost::condition okToRead;
		boost::condition okToWrite;		

		
};

enum rwlock_mode {
	R,
	W
};

class ScopedRWLock_local {
	public:
		ScopedRWLock_local(RWLock_local *, rwlock_mode);
		~ScopedRWLock_local();

		void lock();
		void unlock();

	private:
		explicit ScopedRWLock_local() {}
		explicit ScopedRWLock_local(const ScopedRWLock_local &) {}
		ScopedRWLock_local& operator=(const ScopedRWLock_local &) { return *this; }
		RWLock_local *thelock;
		rwlock_mode mode;
		bool locked;
};


#undef EXPORT

}

#endif
