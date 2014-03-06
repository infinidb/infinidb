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

/*
 * Brief description of the file contents
 *
 * More detailed description
 */

#include <iostream>
using namespace std;
 
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
using namespace boost;

#define RWLOCK_LOCAL_DLLEXPORT
#include "rwlock_local.h"
#undef RWLOCK_LOCAL_DLLEXPORT

// semaphore numbers
#define MUTEX 0
#define READERS 1
#define WRITERS 2

#ifdef DEBUG
using namespace std;
#define PRINTSTATE() \
	cerr << "  reading = " << state.reading << endl \
		<< "  writing = " << state.writing << endl \
		<< "  readerswaiting = " << state.readerswaiting << endl \
		<< "  writerswaiting = " << state.writerswaiting << endl;

#define CHECKSAFETY() \
	if (!((state.reading == 0 && (state.writing == 0 || state.writing == 1)) || \
		(state.reading > 0 && state.writing == 0))) { \
		cerr << "RWLock_local::" << __func__ << ": safety invariant violation" << endl; \
		PRINTSTATE(); \
		throw std::logic_error("RWLock_local: safety invariant violation"); \
	}
	
#define CHECKLIVENESS() \
	if (!( \
		(!(state.readerswaiting > 0 || state.writerswaiting > 0) || \
			(state.reading > 0 || state.writing > 0)) || \
		(!(state.reading == 0 && state.writing == 0) || \
			(state.readerswaiting == 0 && state.writerswaiting == 0)) \
		)) { \
		cerr << "RWLock_local::" << __func__ << ": liveness invariant violation" << endl; \
		PRINTSTATE(); \
		throw std::logic_error("RWLock_local: liveness invariant violation"); \
	}

#undef CHECKLIVENESS
#define CHECKLIVENESS()

#endif

namespace rwlock {

RWLock_local::RWLock_local()
{
	state.reading = 0;
	state.readerswaiting = 0;
	state.writing = 0;
	state.writerswaiting = 0;
}

RWLock_local::~RWLock_local()
{
}

void RWLock_local::read_lock()
{
	mutex.lock();
#ifdef DEBUG
	CHECKSAFETY();
	CHECKLIVENESS();
#endif

	if (state.writerswaiting > 0 || state.writing > 0) {
		state.readerswaiting++;
#ifdef DEBUG
		CHECKSAFETY();
		CHECKLIVENESS();
#endif
		while (state.writerswaiting > 0 || state.writing > 0)
			okToRead.wait(mutex);
		state.readerswaiting--;
	}
	state.reading++;	

#ifdef DEBUG
	CHECKSAFETY();
	CHECKLIVENESS();
#endif

	mutex.unlock();
}
	
void RWLock_local::read_unlock()
{
	mutex.lock();
#ifdef DEBUG
	CHECKSAFETY();
	CHECKLIVENESS();
#endif

	state.reading--;
	if (state.writerswaiting > 0 && state.reading == 0) 
		okToWrite.notify_one();
	
#ifdef DEBUG
	CHECKSAFETY();
	CHECKLIVENESS();
#endif	
	mutex.unlock();
}

void RWLock_local::write_lock()
{
	mutex.lock();
#ifdef DEBUG
	CHECKSAFETY();
	CHECKLIVENESS();
#endif

	if (state.writing > 0 || state.reading > 0) {
		state.writerswaiting++;
		
#ifdef DEBUG
		CHECKSAFETY();
		CHECKLIVENESS();
#endif
		while (state.writing > 0 || state.reading > 0)
			okToWrite.wait(mutex);
		state.writerswaiting--;
	}
	state.writing++;

#ifdef DEBUG
	CHECKSAFETY();
	CHECKLIVENESS();
#endif

}

void RWLock_local::write_unlock()
{

#ifdef DEBUG
	CHECKSAFETY();
	CHECKLIVENESS();
#endif
	
	state.writing--;
	if (state.writerswaiting > 0)
		okToWrite.notify_one();
	else if (state.readerswaiting > 0)
		okToRead.notify_all();
	
#ifdef DEBUG
	CHECKSAFETY();
	CHECKLIVENESS();
#endif	
	mutex.unlock();
}

void RWLock_local::upgrade_to_write()
{
	mutex.lock();
#ifdef DEBUG
	CHECKSAFETY();
	CHECKLIVENESS();
#endif
	state.reading--;
	
	// try to cut in line
	if (state.reading == 0) {
		state.writing++;
#ifdef DEBUG
		CHECKSAFETY();
		CHECKLIVENESS();
#endif
		return;
	}
		
	// cut & paste from write_lock()
	if (state.writing > 0 || state.reading > 0) {
		state.writerswaiting++;
		
#ifdef DEBUG
		CHECKSAFETY();
		CHECKLIVENESS();
#endif
		while (state.writing > 0 || state.reading > 0)
			okToWrite.wait(mutex);
		state.writerswaiting--;
	}
	state.writing++;
}

/* It's safe (and necessary) to simply convert this writer to a reader without
 blocking */
void RWLock_local::downgrade_to_read()
{
#ifdef DEBUG
	CHECKSAFETY();
	CHECKLIVENESS();
#endif	

	state.writing--;
	
	if (state.readerswaiting > 0) 
		okToRead.notify_all();
	state.reading++;
	
#ifdef DEBUG
	CHECKSAFETY();
	CHECKLIVENESS();
#endif	
	mutex.unlock();
}

void RWLock_local::lock() 
{
	mutex.lock();
}

void RWLock_local::unlock()
{
	mutex.unlock();
}

int RWLock_local::getWriting()
{
	return state.writing;
}
		
int RWLock_local::getReading() 
{
	return state.reading;
}

int RWLock_local::getWritersWaiting() 
{
	return state.writerswaiting;
}

int RWLock_local::getReadersWaiting()
{
	return state.readerswaiting;
}

ScopedRWLock_local::ScopedRWLock_local(RWLock_local *l, rwlock_mode m)
{
	thelock = l;
	mode = m;
	assert(m == R || m == W);
	locked = false;
	lock();
}

ScopedRWLock_local::~ScopedRWLock_local()
{
	if (locked)
		unlock();
}

void ScopedRWLock_local::lock()
{
	if (mode == R)
		thelock->read_lock();
	else
		thelock->write_lock();
	locked = true;
}

void ScopedRWLock_local::unlock()
{
	if (mode == R)
		thelock->read_unlock();
	else
		thelock->write_unlock();
	locked = false;
}

}

