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

/** @file
 * Brief description of the file contents
 *
 * More detailed description
 */

#include <iostream>
#include <sys/types.h>
#include <stdexcept>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <pthread.h>

#include <cppunit/extensions/HelperMacros.h>

#include "rwlock.h"
#include "rwlock_local.h"

using namespace std;

int threadStop;

static void *RWRunner(void *arg)
{
	struct timeval tv;
	int op, op2, interval;
	RWLock* rwlock;
	
	gettimeofday(&tv, NULL);
	rwlock = new RWLock(reinterpret_cast<int>(arg));
	
	while (!threadStop) {
		op = rand_r(reinterpret_cast<uint *>(&tv.tv_usec)) % 5;
		switch(op) {
			case 0:    //read
			case 1:
			case 2:
			{
				interval = rand_r(reinterpret_cast<uint *>(&tv.tv_usec)) % 100000;
				rwlock->read_lock();
				rwlock->lock();
				CPPUNIT_ASSERT(rwlock->getReading() > 0);
				CPPUNIT_ASSERT(rwlock->getWriting() == 0);
				rwlock->unlock();
				usleep(interval);

				op2 = rand_r(reinterpret_cast<uint *>(&tv.tv_usec)) % 2;
				if (op2) {
					rwlock->upgrade_to_write();
					rwlock->lock();
					CPPUNIT_ASSERT(rwlock->getReading() == 0);
					CPPUNIT_ASSERT(rwlock->getWriting() == 1);
					rwlock->unlock();
					usleep(interval);
					rwlock->write_unlock();
				}
				else

					rwlock->read_unlock();
				break;
			}
			case 3:		//write
			{
				interval = rand_r(reinterpret_cast<uint *>(&tv.tv_usec)) % 100000;
				rwlock->write_lock();
				rwlock->lock();
				CPPUNIT_ASSERT(rwlock->getReading() == 0);
				CPPUNIT_ASSERT(rwlock->getWriting() == 1);
				rwlock->unlock();
				usleep(interval);

				op2 = rand_r(reinterpret_cast<uint *>(&tv.tv_usec)) % 2;
				if (op2) {
					rwlock->downgrade_to_read();
					rwlock->lock();
					CPPUNIT_ASSERT(rwlock->getReading() > 0);
					CPPUNIT_ASSERT(rwlock->getWriting() == 0);
					rwlock->unlock();
					usleep(interval);
					rwlock->read_unlock();
				}
				else		

					rwlock->write_unlock();
				break;
			}
			case 4:		//delete
			{
				delete rwlock;
				rwlock = new RWLock(reinterpret_cast<int>(arg));
				break;
			}
			default:
				break;
		}
	}
	delete rwlock;
	pthread_exit(0);
}
				
static void *RWRunner_local(void *arg)
{
	struct timeval tv;
	int op, op2, interval;
	RWLock_local* rwlock = reinterpret_cast<RWLock_local *>(arg);
	
	gettimeofday(&tv, NULL);
	
	while (!threadStop) {
		op = rand_r(reinterpret_cast<uint *>(&tv.tv_usec)) % 4;
//    		cout << "doing op " << op << endl;
		switch(op) {
			case 0:    //read
			case 1:
			case 2:
			{
				interval = rand_r(reinterpret_cast<uint *>(&tv.tv_usec)) % 100000;
				rwlock->read_lock();
				rwlock->lock();
				CPPUNIT_ASSERT(rwlock->getReading() > 0);
				CPPUNIT_ASSERT(rwlock->getWriting() == 0);
				rwlock->unlock();
 				usleep(interval);
				op2 = rand_r(reinterpret_cast<uint *>(&tv.tv_usec)) % 2;
				if (op2) {
					rwlock->upgrade_to_write();
// 					rwlock->lock();
					CPPUNIT_ASSERT(rwlock->getReading() == 0);
					CPPUNIT_ASSERT(rwlock->getWriting() == 1);
// 					rwlock->unlock();
 					usleep(interval);
					rwlock->write_unlock();
				}
				else
					rwlock->read_unlock();
				break;
			}
			case 3:		//write
			{
				interval = rand_r(reinterpret_cast<uint *>(&tv.tv_usec)) % 100000;
				rwlock->write_lock();
// 				rwlock->lock();
				CPPUNIT_ASSERT(rwlock->getReading() == 0);
				CPPUNIT_ASSERT(rwlock->getWriting() == 1);
// 				rwlock->unlock();
 				usleep(interval);
				op2 = rand_r(reinterpret_cast<uint *>(&tv.tv_usec)) % 2;
				if (op2) {
					rwlock->downgrade_to_read();
					rwlock->lock();
					CPPUNIT_ASSERT(rwlock->getReading() > 0);
					CPPUNIT_ASSERT(rwlock->getWriting() == 0);
					rwlock->unlock();
 					usleep(interval);
					rwlock->read_unlock();
				}
				else		
					rwlock->write_unlock();
				break;
			}
			default:
				break;
		}
	}
	pthread_exit(0);
}


class RWLockTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(RWLockTest);

// CPPUNIT_TEST(LongRWTest_1);
CPPUNIT_TEST(LongRWLocalTest_1);

CPPUNIT_TEST_SUITE_END();

private:
public:
	void LongRWTest_1() {
		int key = 0x8215125f;
		
		const int threadCount = 40;
		int i;
		pthread_t threads[threadCount];
	
		cerr << endl << "Multithreaded RWLock test.  "
				"This runs for 5 minutes." << endl;
	
		threadStop = 0;
		
		for (i = 0; i < threadCount; i++) {
			if (pthread_create(&threads[i], NULL, RWRunner, 
				reinterpret_cast<void *>(key)) < 0)
				throw logic_error("Error creating threads for the ipc test");
		}
		
 		sleep(300);
		threadStop = 1;
		for (i = 0; i < threadCount; i++) {
			cerr << "Waiting for thread #" << i << endl;
			pthread_join(threads[i], NULL);	
		}
	}

	void LongRWLocalTest_1() {
		const int threadCount = 40;
		int i;
		pthread_t threads[threadCount];
		RWLock_local rwlock;
	
		cerr << endl << "Multithreaded RWLock_local test.  "
				"This runs for 30-60 seconds." << endl;
	
		threadStop = 0;
		
		for (i = 0; i < threadCount; i++) {
			if (pthread_create(&threads[i], NULL, RWRunner_local, 
				reinterpret_cast<void *>(&rwlock)) < 0)
				throw logic_error("Error creating threads for the local test");
		}
		
		sleep(30);
		threadStop = 1;
		for (i = 0; i < threadCount; i++) {
			cerr << "Waiting for thread #" << i << endl;
			pthread_join(threads[i], NULL);	
		}
	}

};

CPPUNIT_TEST_SUITE_REGISTRATION( RWLockTest );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


