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
 * This file contains the longer OIDManager stress tests.
 */
 
#include <string>
#include <typeinfo>
#include <sstream>
#include <stdexcept>
#include <iostream>
#include <unistd.h>
#include <cstdlib>
#include <pthread.h>
#include <values.h>
#include <signal.h>
#include "objectidmanager.h"
#include <cppunit/extensions/HelperMacros.h>

using namespace std;


void* OIDManClient(void *arg);
int timer;
pthread_mutex_t lock;

using namespace execplan;

class ExecPlanTest : public CppUnit::TestFixture {

	CPPUNIT_TEST_SUITE( ExecPlanTest );
	unlink("/tmp/oidbitmap");
	CPPUNIT_TEST( objectIDManager_2 );
	CPPUNIT_TEST( objectIDManager_3 );
	unlink("/tmp/oidbitmap");
	CPPUNIT_TEST_SUITE_END();
	
	public:
		
	void objectIDManager_2()
	{
		ObjectIDManager o;
		int i, j, allocsize, loopCount, firstOID;
		const int chunkcap = 10000;
		const int OIDSpaceSize = 16777216;
		
		cout << endl << "Long OID Manager test" << endl;
		cout << "using bitmap file " << o.getFilename() << endl;
		
		try {
			// make sure we can fill the entire OID space and clear it out a
			// few times.  This should stress test the implementation pretty
			// well.
			
			firstOID = o.allocOID();
			cerr << "first OID is " << firstOID << endl;
			o.returnOIDs(0, firstOID);
			
			srand(time(NULL));
			
			// fill the entire space then empty it out a few times.
			for (i = 0; i < 10; i++) {
				cout << "fill & empty test " << i+1 << "/10" << endl;
				for (j = 0, loopCount = 1; j < OIDSpaceSize ; j+=allocsize, loopCount++) {
					allocsize = rand() % chunkcap;
					allocsize = (allocsize > OIDSpaceSize - j ? 
							OIDSpaceSize - j : allocsize);
					CPPUNIT_ASSERT(o.allocOIDs(allocsize) > -1);
				}
				CPPUNIT_ASSERT(o.allocOID() == -1);
				CPPUNIT_ASSERT(o.size() == OIDSpaceSize);
				o.returnOIDs(0, OIDSpaceSize-1);  // (gets rid of fragmentation in the freelist)
			}
			
			// fill the space again, then deallocate randomly to fragment
			// the freelist
			for (j = 0, loopCount = 1; j < OIDSpaceSize ; j+=allocsize, loopCount++) {
				allocsize = rand() % chunkcap;
				allocsize = (allocsize > OIDSpaceSize - j ? 
						OIDSpaceSize - j : allocsize);
				CPPUNIT_ASSERT(o.allocOIDs(allocsize) > -1);
			}
			CPPUNIT_ASSERT(o.allocOID() == -1);
			CPPUNIT_ASSERT(o.size() == OIDSpaceSize);
			for (j = 0, loopCount = 1; j < OIDSpaceSize; j+=allocsize, loopCount++) {
				allocsize = rand() % chunkcap;
				allocsize = (allocsize > OIDSpaceSize - j ? 
						OIDSpaceSize - j : allocsize);
				o.returnOIDs(j, j+allocsize-1); 
			}
			CPPUNIT_ASSERT(o.size() == 0);
			CPPUNIT_ASSERT(o.allocOIDs(OIDSpaceSize) == 0);

			
			
			// create small random holes & fill them.  This test takes > 15 
			// minutes, so it should be commented out unless there's a
			// specific need for it
#if 0		
			const int smallcap = 20;
			
			cout << "small random hole test step 1/8 (this takes a while)" << endl;
			for (j = 0, loopCount = 1; j < OIDSpaceSize; j+=allocsize, loopCount++) {
				allocsize = rand() % smallcap;
				allocsize = (allocsize > OIDSpaceSize - j ? 
						OIDSpaceSize - j : allocsize);
				if ((rand() % 2) == 0) // empty about 50% of the OID space
					o.returnOIDs(j, j+allocsize-1);
		}
				
			cout << "small random hole test step 2/8" << endl;
			while (o.allocOIDs(1000) != -1) ;
			cout << "small random hole test step 3/8" << endl; 
			while (o.allocOIDs(500) != -1) ;
			cout << "small random hole test step 4/8" << endl;  
			while (o.allocOIDs(100) != -1) ;
			cout << "small random hole test step 5/8" << endl;   
			while (o.allocOIDs(50) != -1) ;
			cout << "small random hole test step 6/8" << endl;  
			while (o.allocOIDs(10) != -1) ;
			cout << "small random hole test step 7/8" << endl;   
			while (o.allocOIDs(3) != -1) ;
			cout << "small random hole test step 8/8" << endl; 
			while (o.allocOID() != -1) ;
			CPPUNIT_ASSERT(o.size() == OIDSpaceSize);
			CPPUNIT_ASSERT(o.allocOID() == -1);
			cout << "done." << endl;
#endif		
		}
		catch(...) {
			unlink(o.getFilename().c_str());
			throw;
		}
		unlink(o.getFilename().c_str());
	}

	/* This test attempts to simulate real transactions: many small 
	* blocks of allocs and returns in a multithreaded/multiprocess
	* way.  This test only uses threads, but that should be good enough.
	*/
	void objectIDManager_3()
	{
		const int threadCount = 4;
		int i;
		pthread_t threads[threadCount];
		
		cout << endl << "Multithreaded OIDManager test.  "
				"This runs for 2 minutes." << endl;
		
		timer = 0;
		
		for (i = 0; i < threadCount; i++)
			if (pthread_create(&threads[i], NULL, OIDManClient, 
				reinterpret_cast<void *>(i+1)) < 0)
				throw logic_error("Error creating threads for the OID Manager test");
		
		sleep(120);
		timer = 1;
		for (i = 0; i < threadCount; i++)
			pthread_join(threads[i], NULL);	
	}
	
};

CPPUNIT_TEST_SUITE_REGISTRATION( ExecPlanTest );

void* OIDManClient(void *arg)
{
	ObjectIDManager o;
	const int sizecap = 5;
	int op, size, tmp;
	uint32_t seed;
	int threadnum = reinterpret_cast<int>(arg);	
	
	struct entry {
		int begin;
		int end;
		struct entry *next;
	};
		
	struct entry *e;
	struct entry *listHead = NULL;
	struct entry *listTail = NULL;
	
	cout << "  Thread " << threadnum << " started." << endl;
	
	seed = time(NULL) % MAXINT;	
	
	while (timer == 0) {
		op = rand_r(&seed) % 2;
		size = (rand_r(&seed) % sizecap) + 1;
		if (op == 1) {   			// allocate an OID block
			e = new struct entry;
			tmp = o.allocOIDs(size);
			CPPUNIT_ASSERT(tmp != -1);
			e->begin = tmp;
			e->end = tmp + size - 1;
			e->next = NULL;
			if (listTail != NULL) 
				listTail->next = e;
			else 
				listHead = e;				
			listTail = e;
		}
		else {					//deallocate an OID block
			if (listHead == NULL)
				continue;
				
			o.returnOIDs(listHead->begin, listHead->end);
			e = listHead;
			listHead = listHead->next;
			if (listHead == NULL)
				listTail = NULL;
			delete e;
		}
	}
	while (listHead != NULL) {
		e = listHead;
		listHead = listHead->next;
		delete e;
	}
	
	cout << "  Thread " << threadnum << " exiting." << endl;
	pthread_exit(0);
}


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
