/*
* $Id: locks.cpp 282 2007-10-28 02:18:55Z rdempsey $
*/

#include <iostream>
#include <stdexcept>
using namespace std;

#include "sessionmanager.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "shmkeys.h"
#include "rwlock.h"
#include "mastersegmenttable.h"
using namespace BRM;

int query_locks()
{
	ShmKeys keys;
	RWLock *rwlock[MasterSegmentTable::nTables];
	int RWLockKeys[MasterSegmentTable::nTables];
	int i;

	RWLockKeys[0] = keys.KEYRANGE_EXTENTMAP_BASE;
	RWLockKeys[1] = keys.KEYRANGE_EMFREELIST_BASE;
	RWLockKeys[2] = keys.KEYRANGE_VBBM_BASE;
	RWLockKeys[3] = keys.KEYRANGE_VSS_BASE;
	RWLockKeys[4] = keys.KEYRANGE_CL_BASE;

	for (i = 0; i < MasterSegmentTable::nTables; i++)
		rwlock[i] = new RWLock(RWLockKeys[i]);

	for (i = 0; i < MasterSegmentTable::nTables; i++) 
		if (rwlock[i]->getWriting() > 0 ||
		    //rwlock[i]->getReading() > 0 ||
		    rwlock[i]->getWritersWaiting() > 0 ||
		    rwlock[i]->getReadersWaiting() > 0)
			return 1;
	
	for (i = 0; i < MasterSegmentTable::nTables; i++)
		delete rwlock[i];
	
	return 0;
}

int reset_locks()
{
	ShmKeys keys;
	RWLock *rwlock[MasterSegmentTable::nTables];
	int RWLockKeys[MasterSegmentTable::nTables];
	int i;
	SessionManager sm(true);

	RWLockKeys[0] = keys.KEYRANGE_EXTENTMAP_BASE;
	RWLockKeys[1] = keys.KEYRANGE_EMFREELIST_BASE;
	RWLockKeys[2] = keys.KEYRANGE_VBBM_BASE;
	RWLockKeys[3] = keys.KEYRANGE_VSS_BASE;
	RWLockKeys[4] = keys.KEYRANGE_CL_BASE;

	for (i = 0; i < MasterSegmentTable::nTables; i++)
		rwlock[i] = new RWLock(RWLockKeys[i]);

	for (i = 0; i < MasterSegmentTable::nTables; i++) 
		rwlock[i]->reset();
	
	for (i = 0; i < MasterSegmentTable::nTables; i++)
		delete rwlock[i];
	
	try {
		sm.reset();
	}
	catch (exception &e) {
		cout << e.what() << endl;
		return -1;
	}

	return 0;
}

