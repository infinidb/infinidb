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
 * This file contains the longer SessionManager stress tests.
 */
 
#include <string>
#include <typeinfo>
#include <sstream>
#include <stdexcept>
#include <iostream>
#include <unistd.h>
#include <cstdlib>
#include <values.h>
#include <errno.h>
#include <signal.h>
#include "calpontsystemcatalog.h"
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include "sessionmanager.h"
#include <cppunit/extensions/HelperMacros.h>
#include <boost/thread.hpp>

using namespace std;
using namespace execplan;

int threadStop, threadNum;

static void* SMRunner(void* arg)
{
	SessionManager *sm;
	int op;
	uint32_t seed, sessionnum;
	SessionManager::TxnID tmp;
	int tNum = reinterpret_cast<int>(arg);
			
	struct entry {
		SessionManager::TxnID tid;
		uint32_t sessionnum;
		struct entry *next;
	};
		
	struct entry *e;
	struct entry *listHead = NULL;
	struct entry *listTail = NULL;
	
	cerr << "Thread " << tNum << " started." << endl;
	
	seed = time(NULL) % MAXINT;
	
	sm = new SessionManager();
	
	while (!threadStop) {
		sm->verifySize();
		op = rand_r(&seed) % 4;  // 0 = newTxnID, 1 = committed, 2 = getTxnID, 3 = delete sm; new sm
		sessionnum = rand_r(&seed);
		switch (op) {
			case 0:
				e = new struct entry;
				e->tid = sm->newTxnID(sessionnum, false);
				if (e->tid.valid == false) {   // SM is full
					delete e;
					break;
				}
				e->sessionnum = sessionnum;
				e->next = NULL;
				if (listTail != NULL) 
					listTail->next = e;
				else 
					listHead = e;				
				listTail = e;
				break;
			case 1:
				if (listHead == NULL)
					continue;
				sm->committed(listHead->tid);
				e = listHead;
				listHead = listHead->next;
				if (listHead == NULL)
					listTail = NULL;
				delete e;
				break;
			case 2:
				if (listHead == NULL)
					continue;
				tmp = sm->getTxnID(listHead->sessionnum);
				CPPUNIT_ASSERT(tmp.valid == listHead->tid.valid == true);
				// there's some risk of collision here if 2 threads happen to choose the 
				// same session number
				//CPPUNIT_ASSERT(tmp.id == listHead->tid.id);
				break;
			case 3:
				delete sm;
				sm = new SessionManager();
				break;
			default:
				cerr << "SMRunner: ??" << endl;
		};
	}
	
	while (listHead != NULL) {
		e = listHead;
		listHead = listHead->next;
		delete e;
	}
	
	delete sm;
	cerr << "Thread " << tNum << " exiting." << endl;
	pthread_exit(0);
}	

class ExecPlanTest : public CppUnit::TestFixture {

	CPPUNIT_TEST_SUITE( ExecPlanTest );
	CPPUNIT_TEST(sessionManager_3);
	unlink("/tmp/CalpontShm");
	CPPUNIT_TEST_SUITE_END();
	

public:
		
	/*
	* destroySemaphores() and destroyShmseg() will print error messages
	* if there are no objects to destroy.  That's OK.
	*/
	void destroySemaphores()
	{
		key_t semkey;
		char* semseed = "/usr/local/Calpont/etc/Calpont.xml";
		int sems, err;
		
// 		semkey = ftok(semseed, 0x2149bdd2);   // these things must match in the SM constructor
		semkey = 0x2149bdd2;
		if (semkey == -1)
			perror("tdriver: ftok");
		sems = semget(semkey, 2, 0666);
		if (sems != -1) {
			err = semctl(sems, 0, IPC_RMID);
			if (err == -1)
				perror("tdriver: semctl");
		}
	}
	
	void destroyShmseg()
	{
		key_t shmkey;
		char* shmseed = "/usr/local/Calpont/etc/Calpont.xml";
		int shms, err;
		
// 		shmkey = ftok(shmseed, 0x2149bdd2);   // these things much match in the SM constructor
		shmkey = 0x2149bdd2;
		if (shmkey == -1)
			perror("tdriver: ftok");
		shms = shmget(shmkey, 0, 0666);
		if (shms != -1) {
			err = shmctl(shms, IPC_RMID, NULL);
			if (err == -1 && errno != EINVAL) {
				perror("tdriver: shmctl");
				return;
			}
		}
	}
			
	/** This launches several threads to stress test the Session Manager
	*/
	void sessionManager_3() {
		const int threadCount = 4;
		int i;
		pthread_t threads[threadCount];
		
		cerr << endl << "Multithreaded SessionManager test.  "
				"This runs for 2 minutes." << endl;
		
		destroySemaphores();
		destroyShmseg();
		unlink("/tmp/CalpontShm");
		
		threadStop = 0;
		
		for (i = 0; i < threadCount; i++) {
			if (pthread_create(&threads[i], NULL, SMRunner, 
				reinterpret_cast<void *>(i+1)) < 0)
				throw logic_error("Error creating threads for the Session Manager test");
			usleep(1000);
		}
		
		sleep(120);
		threadStop = 1;
		for (i = 0; i < threadCount; i++)
			pthread_join(threads[i], NULL);	

		destroySemaphores();
		destroyShmseg();
	}

	
};

CPPUNIT_TEST_SUITE_REGISTRATION( ExecPlanTest );

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
