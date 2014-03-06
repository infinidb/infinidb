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

#include <cppunit/extensions/HelperMacros.h>

#include "rwlock.h"

using namespace std;

class RWLockTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(RWLockTest);

CPPUNIT_TEST(RWTest_1);

CPPUNIT_TEST_SUITE_END();

private:
public:
	void RWTest_1() {
		RWLock *rwlock;
		int caughtException = 0;
		int key;
		
		srand(time(NULL));
		key = rand();
		
		rwlock = new RWLock(key);
		rwlock->read_lock(false);
		rwlock->lock();
		CPPUNIT_ASSERT(rwlock->getReading() == 1);
		CPPUNIT_ASSERT(rwlock->getWriting() == 0);
		CPPUNIT_ASSERT(rwlock->getReadersWaiting() == 0);
		CPPUNIT_ASSERT(rwlock->getWritersWaiting() == 0);
		rwlock->unlock();
		rwlock->read_lock(false);
		rwlock->read_lock(false);
		rwlock->lock();
		CPPUNIT_ASSERT(rwlock->getReading() == 3);
		CPPUNIT_ASSERT(rwlock->getWriting() == 0);
		CPPUNIT_ASSERT(rwlock->getReadersWaiting() == 0);
		CPPUNIT_ASSERT(rwlock->getWritersWaiting() == 0);
		rwlock->unlock();
		try {
			rwlock->write_lock(false);
		}
		catch(RWLock::wouldblock& e) {
			caughtException++;
// 			cerr << endl << "Caught expected exception: " << e.what() << endl;
		}
		CPPUNIT_ASSERT(caughtException == 1);
		rwlock->lock();
		CPPUNIT_ASSERT(rwlock->getReading() == 3);
		CPPUNIT_ASSERT(rwlock->getWriting() == 0);
		CPPUNIT_ASSERT(rwlock->getReadersWaiting() == 0);
		CPPUNIT_ASSERT(rwlock->getWritersWaiting() == 0);
		rwlock->unlock();
		rwlock->read_unlock();
		rwlock->lock();
		CPPUNIT_ASSERT(rwlock->getReading() == 2);
		CPPUNIT_ASSERT(rwlock->getWriting() == 0);
		CPPUNIT_ASSERT(rwlock->getReadersWaiting() == 0);
		CPPUNIT_ASSERT(rwlock->getWritersWaiting() == 0);
		rwlock->unlock();
		rwlock->read_unlock();
		rwlock->read_unlock();
		rwlock->lock();
		CPPUNIT_ASSERT(rwlock->getReading() == 0);
		CPPUNIT_ASSERT(rwlock->getWriting() == 0);
		CPPUNIT_ASSERT(rwlock->getReadersWaiting() == 0);
		CPPUNIT_ASSERT(rwlock->getWritersWaiting() == 0);
		rwlock->unlock();
		rwlock->write_lock(false);
		rwlock->lock();
		CPPUNIT_ASSERT(rwlock->getReading() == 0);
		CPPUNIT_ASSERT(rwlock->getWriting() == 1);
		CPPUNIT_ASSERT(rwlock->getReadersWaiting() == 0);
		CPPUNIT_ASSERT(rwlock->getWritersWaiting() == 0);
		rwlock->unlock();
		try {
			rwlock->write_lock(false);
		}
		catch(RWLock::wouldblock& e) {
			caughtException++;
// 			cerr << endl << "Caught expected exception: " << e.what() << endl;
		}
		CPPUNIT_ASSERT(caughtException == 2);
		rwlock->lock();
		CPPUNIT_ASSERT(rwlock->getReading() == 0);
		CPPUNIT_ASSERT(rwlock->getWriting() == 1);
		CPPUNIT_ASSERT(rwlock->getReadersWaiting() == 0);
		CPPUNIT_ASSERT(rwlock->getWritersWaiting() == 0);
		rwlock->unlock();
		try {
			rwlock->read_lock(false);
		}
		catch(RWLock::wouldblock& e) {
			caughtException++;
// 			cerr << endl << "Caught expected exception: " << e.what() << endl;
		}
		CPPUNIT_ASSERT(caughtException == 3);
		rwlock->lock();
		CPPUNIT_ASSERT(rwlock->getReading() == 0);
		CPPUNIT_ASSERT(rwlock->getWriting() == 1);
		CPPUNIT_ASSERT(rwlock->getReadersWaiting() == 0);
		CPPUNIT_ASSERT(rwlock->getWritersWaiting() == 0);
		rwlock->unlock();
		delete rwlock;
		rwlock = new RWLock(key);
		rwlock->lock();
		CPPUNIT_ASSERT(rwlock->getReading() == 0);
		CPPUNIT_ASSERT(rwlock->getWriting() == 1);
		CPPUNIT_ASSERT(rwlock->getReadersWaiting() == 0);
		CPPUNIT_ASSERT(rwlock->getWritersWaiting() == 0);
		rwlock->unlock();
		rwlock->write_unlock();
		delete rwlock;
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


