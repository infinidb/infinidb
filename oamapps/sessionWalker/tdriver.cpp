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

/***************************************************************************
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/

using namespace std;
#include <iostream>
#include <cstdlib>
#include <sys/types.h>
#include <unistd.h>
#include "sessionmonitor.h"
#include "sessionmanager.h"

#include <cppunit/extensions/HelperMacros.h>

using namespace execplan;

int maxNewTxns=1000;
int maxTxns = 1000;

class ExecPlanTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ExecPlanTest );

CPPUNIT_TEST( MonitorTestPlan_1 );
CPPUNIT_TEST_SUITE_END();

private:
public:

    void setUp() {
    }
    
    void tearDown() {
    }
    
	int verifyLen;
	SessionManager* manager;
	SessionManager::TxnID managerTxns[1000];

int createTxns(const int& start, const int& end) {

	int first=start;
	int last=end;
	int newTxns=0;

	verifyLen=manager->verifySize();
	for (int idx = first; idx<last && verifyLen<maxNewTxns ; idx++)
	{
		managerTxns[idx] = manager->newTxnID((uint32_t)idx+1000);
		CPPUNIT_ASSERT(managerTxns[idx].id>0);
		CPPUNIT_ASSERT(managerTxns[idx].valid==true);
		verifyLen=manager->verifySize();
		CPPUNIT_ASSERT(verifyLen>0);
		newTxns++;
	}
	CPPUNIT_ASSERT(newTxns==last-first);
	return newTxns;
}

int closeTxns(const int& start, const int& end) {

	int first=start;
	int last=end;
	int totalClosed=0;

	for (int idx=first; idx<last ; idx++)
	{
		try
		{
			SessionManager::TxnID tmp = manager->getTxnID(idx+1000);
			if (tmp.valid == true)
			{
				manager->committed(tmp);
				CPPUNIT_ASSERT(tmp.valid==false);
				totalClosed++;
			}
			
		}
		catch (exception& e)
		{
			cerr << e.what() << endl;
			continue;
		}
	}
	return totalClosed;

} //closeTxns

void MonitorTestPlan_1() {

	int currStartTxn=0;
	int currEndTxn=5;
	int txnCntIncr=5;
	const int sleepTime=1;
	const int iterMax=1;
	vector<SessionMonitor::MonSIDTIDEntry*> toTxns;

	manager = new SessionManager();
	//CPPUNIT_ASSERT(manager->verifySize()==0);

	SessionMonitor* monitor = NULL;
	for(int jdx=0; jdx<iterMax; jdx++) {

		// store the current state of the SessionManager
		monitor = new SessionMonitor();
		monitor->AgeLimit(sleepTime);
		delete monitor;
		int idx=0;
		int grpStart=currStartTxn;
		for (idx=0; idx < 3; idx++ ) {

			createTxns(currStartTxn, currEndTxn);
			//CPPUNIT_ASSERT(manager->verifySize()==(idx+1)*txnCntIncr);

			currStartTxn+=txnCntIncr;
			currEndTxn+=txnCntIncr;
			sleep(sleepTime+1); //make sessions time out

			monitor = new SessionMonitor(); // read Monitor data
			monitor->AgeLimit(sleepTime);
    		toTxns.clear();
			toTxns = monitor->timedOutTxns(); // get timed out txns
			CPPUNIT_ASSERT(toTxns.size()==(uint32_t)txnCntIncr*idx);

			delete monitor;
		}

		int grpEnd=currEndTxn;
		monitor = new SessionMonitor();
		monitor->AgeLimit(sleepTime);
		closeTxns(grpStart, grpEnd); // close this iteration of txns
		//CPPUNIT_ASSERT(manager->verifySize()==0);
		toTxns = monitor->timedOutTxns(); // get timed out txns
		CPPUNIT_ASSERT(toTxns.size()==0);

		delete monitor;

	}

	monitor = new SessionMonitor(); // readload Monitor data
	monitor->AgeLimit(sleepTime-1);

    toTxns.clear();
	toTxns = monitor->timedOutTxns(); // get timed out txns
	CPPUNIT_ASSERT(toTxns.size()==0);
	delete monitor;

	//CPPUNIT_ASSERT(manager->verifySize()==0);
	delete manager;
}

}; // test suite

CPPUNIT_TEST_SUITE_REGISTRATION( ExecPlanTest);

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main(int argc, char *argv[])
{

  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}
