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

/*********************************************************************
 *   $Id: tdriver-dbrm.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ********************************************************************/

#include <iostream>
#include <pthread.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <stdexcept>
#include <signal.h>
#include <values.h>
#include <cppunit/extensions/HelperMacros.h>

#include "brm.h"
#include "sessionmanager.h"
#include "IDBPolicy.h"

#ifdef NO_TESTS
#undef CPPUNIT_ASSERT
#define CPPUNIT_ASSERT(a)
#endif

using namespace BRM;
using namespace std;
using namespace execplan;

void keepalive(int signum) {
	cerr << "Yes, it's still going..." << endl;
	alarm(290);
}

void *DBRM_dummy_1(void *arg) 
{
	DBRM dbrm;
	int err;
	VER_t txn = 2;
	vector<LBIDRange> ranges;
	LBIDRange range;
	vector<VBRange> freeList;

	range.start = 500;
	range.size = 1000;
	ranges.push_back(range);
  	err = dbrm.beginVBCopy(txn, ranges, freeList);
  	CPPUNIT_ASSERT(err == 0);
  	err = dbrm.endVBCopy(txn, ranges);
 	CPPUNIT_ASSERT(err == 0);
	err = dbrm.vbCommit(txn);
 	CPPUNIT_ASSERT(err == 0);

	return NULL;
}

void *DBRM_deadlock(void *arg) 
{
	DBRM dbrm;
	int terr;
	VER_t txn = 2;
	vector<LBIDRange> ranges;
	LBIDRange range;
	vector<VBRange> freeList;

	range.start = 2000;
	range.size = 1000;
	ranges.push_back(range);
  	terr = dbrm.beginVBCopy(txn, ranges, freeList);

	range.start = 1000;
	range.size = 1000;
	ranges.clear();
	freeList.clear();
	ranges.push_back(range);
	terr = dbrm.beginVBCopy(txn, ranges, freeList);

	// block waiting on the main thread, main thread deadlocks, rolls back, 
	// thread wakes

	CPPUNIT_ASSERT(terr == 0);
	terr = dbrm.endVBCopy(txn, ranges);

 	CPPUNIT_ASSERT(terr == 0);
	terr = dbrm.vbCommit(txn);

  	CPPUNIT_ASSERT(terr == 0);

	return NULL;
}

class BRMTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(BRMTest);


CPPUNIT_TEST(sessionmanager_1);
CPPUNIT_TEST(sessionmanager_2);
CPPUNIT_TEST(sessionmanager_3);
CPPUNIT_TEST(sessionmanager_4);
CPPUNIT_TEST(sessionmanager_5);
CPPUNIT_TEST(sessionmanager_6);
CPPUNIT_TEST(sessionmanager_7);
CPPUNIT_TEST(sessionmanager_8);
CPPUNIT_TEST(brm_dumb_1);
CPPUNIT_TEST(brm_extentmap_good_1);
CPPUNIT_TEST(brm_good_2);
CPPUNIT_TEST(brm_good_3);
CPPUNIT_TEST(brm_deleteOID);
CPPUNIT_TEST(brm_HWM);
CPPUNIT_TEST(dbrm_clear);

CPPUNIT_TEST(DBRM_resource_graph_1);
CPPUNIT_TEST(DBRM_resource_graph_deadlock);
CPPUNIT_TEST(brm_markExtentsInvalid);

CPPUNIT_TEST_SUITE_END();

private:
public:
	
	void brm_dumb_1()
	{
		DBRM brm;
		vector<LBID_t> lbids;
		int allocdSize, err;
		const uint32_t extentSize = brm.getExtentSize();

 		err = brm.createExtent(extentSize, 1, lbids, allocdSize);
		CPPUNIT_ASSERT(err == 0);
 		err = brm.markExtentInvalid(lbids[0]);
		CPPUNIT_ASSERT(err == 0);
		int64_t min;	
		int64_t max;	
		int32_t seq;
 		err = brm.getExtentMaxMin(lbids[0], max, min, seq);
#ifdef SAFE_CP
		CPPUNIT_ASSERT(err == 0);
#else
		CPPUNIT_ASSERT(err == 1);
#endif

 		err = brm.setExtentMaxMin(lbids[0], max, min, seq);
		CPPUNIT_ASSERT(err == 0);

		err = brm.deleteOID(1);
		CPPUNIT_ASSERT(err == 0);
	}	

	void brm_extentmap_good_1()
	{
		
		DBRM brm;
		int i, err, oid, allocdSize, 
			iterations = 100;  // (EM_INITIAL_SIZE + 3*EM_INCREMENT)
		uint32_t fbo, hwm;
		vector<LBID_t> lbids;
		HWM_t hwm2;
		VER_t txnID;
		const uint32_t extentSize = brm.getExtentSize();
		
 		cerr << "brm_extentmap_good_1" << endl;

		for (i = 1; i < iterations; i++) {
			err = brm.createExtent(extentSize, i, lbids, allocdSize);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(lbids.back() == static_cast<LBID_t>((i-1)*extentSize));

			err=brm.markExtentInvalid(lbids[0]);
			CPPUNIT_ASSERT(err==0);

			int64_t min;	
			int64_t max;	
			int32_t seq;
 			err = brm.getExtentMaxMin(lbids[0], max, min, seq);
#ifdef SAFE_CP
			CPPUNIT_ASSERT(err == 0);
#else
			CPPUNIT_ASSERT(err == 1);
#endif

 			err = brm.setExtentMaxMin(lbids[0], max, min, seq);
			CPPUNIT_ASSERT(err == 0);

		}

		CPPUNIT_ASSERT(err==0);

		for (i = 1; i < iterations; i++) {
			err = brm.lookup(static_cast<LBID_t>((i-1)*extentSize), 0, false, oid, fbo);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(oid == i);
			CPPUNIT_ASSERT(fbo == 0);
			if (i != 1) {
				err = brm.lookup(static_cast<LBID_t>((i-1)*extentSize - 1), 0, false, oid, fbo);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(oid == i-1);
				CPPUNIT_ASSERT(fbo == extentSize - 1);
			}
			if (i != iterations) {
				err = brm.lookup(static_cast<LBID_t>((i-1)*extentSize + 1), 0, false, oid, fbo);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(oid == i);
				CPPUNIT_ASSERT(fbo == 1);
			}
			err = brm.markExtentInvalid(oid);
			CPPUNIT_ASSERT(err==0);
			err = brm.markExtentInvalid(lbids[0]);
			CPPUNIT_ASSERT(err==0);
			int64_t min;	
			int64_t max;	
			int32_t seq;
 			err = brm.getExtentMaxMin(lbids[0], max, min, seq);
#ifdef SAFE_CP
			CPPUNIT_ASSERT(err == 0);
#else
			CPPUNIT_ASSERT(err == 1);
#endif

 			err = brm.setExtentMaxMin(lbids[0], max, min, seq);
			CPPUNIT_ASSERT(err == 0);
		}
		
 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

		err = brm.lookup(static_cast<LBID_t>((i-1)*extentSize), 0, false, oid, fbo);
		CPPUNIT_ASSERT(err == -1);
		
		for (i = 1; i < iterations; i++) {
			err = brm.getBulkInsertVars(static_cast<LBID_t>((i-1)*extentSize),
									   hwm2, txnID);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm2 == 0);
			CPPUNIT_ASSERT(txnID == 0);
			err = brm.setBulkInsertVars(static_cast<LBID_t>((i-1)*extentSize),
									   i, i + 1);
			CPPUNIT_ASSERT(err == 0);
			err = brm.getBulkInsertVars(static_cast<LBID_t>((i-1)*extentSize),
									   hwm2, txnID);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm2 == static_cast<BRM::LBID_t>(i));
			CPPUNIT_ASSERT(txnID == static_cast<BRM::VER_t>(i+1));
			
			err = brm.getHWM(i, hwm);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm == 0);
			err = brm.setHWM(i, ((uint32_t)i > extentSize - 1 ? extentSize - 1 : i));
			CPPUNIT_ASSERT(err == 0);
			err = brm.getHWM(i, hwm);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm == static_cast<uint32_t>((uint32_t)i > extentSize - 1 ? extentSize - 1 : i));
		}
		
 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

#ifdef BRM_DEBUG
		err = brm.setHWM(i, hwm);
		CPPUNIT_ASSERT(err != 0);
#endif

		for (i = 1; i < iterations; i++) {
			err = brm.deleteOID(i);
			CPPUNIT_ASSERT(err == 0);
		}
 
		err = brm.deleteOID(i);
		CPPUNIT_ASSERT(err != 0);
		
 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

		err = brm.saveState();
		CPPUNIT_ASSERT(err==0);

	}
	
	void brm_good_2()
	{
		DBRM brm;
		VBBM vbbm;
		VSS vss;
		CopyLocks cl;
		int i, err, size;
		vector<LBID_t> lbids;
		vector<EMEntry> extents;
		LBIDRange_v ranges;
		LBIDRange_v::iterator lbidRangeIT;
		vector<VBRange> vbRanges, vbRanges2;
		vector<VBRange>::iterator vbRangesIT;
		EMEntry em;
		OID_t oid;
		uint32_t fbo;
		LBIDRange range;
		VBRange vbRange;
		VER_t verID;
		bool vbFlag;
		
		// Buildbot times out on the getBlocks() call during leakcheck b/c it takes
		// > 5 mins for some reason.  Have to ping it before 300 seconds go by.
		void (*oldsig)(int);

 		cerr << "brm_good_2" << endl;		

		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		oldsig = signal(SIGALRM, keepalive);
		alarm(290);
		
		err = brm.lookup(0, 0, false, oid, fbo);
		CPPUNIT_ASSERT(err != 0); 
		err = brm.lookup(0, 0, true, oid, fbo);
		CPPUNIT_ASSERT(err != 0);
		
		err = brm.createExtent(8000, 1, lbids, size);
		CPPUNIT_ASSERT(err == 0);
		CPPUNIT_ASSERT(size == brm.getExtentSize());
		CPPUNIT_ASSERT(lbids.size() == 1);
//  		CPPUNIT_ASSERT(*(lbids.begin()) == 0);
		
		err = brm.getExtents(1, extents);
		CPPUNIT_ASSERT(err == 0);
		CPPUNIT_ASSERT(extents.size() == 1);
		
		em = *(extents.begin());
// 		CPPUNIT_ASSERT(em.range.start == 0);
 		CPPUNIT_ASSERT(em.range.size*1024 == static_cast<uint32_t>(brm.getExtentSize()));
		CPPUNIT_ASSERT(em.HWM == 0);
		CPPUNIT_ASSERT(em.blockOffset == 0);
		
		for (i = 0; i < 5; i++) {
			range.start = i * 100;
			range.size = 100;
			ranges.push_back(range);
		}
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

		err = brm.beginVBCopy(1, ranges, vbRanges);
		CPPUNIT_ASSERT(err == 0);
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		cl.lock(CopyLocks::READ);
		for (lbidRangeIT = ranges.begin(); lbidRangeIT != ranges.end(); 
		    lbidRangeIT++)
				CPPUNIT_ASSERT(cl.isLocked(*lbidRangeIT));
		cl.release(CopyLocks::READ);
		
		err = brm.beginVBCopy(1, ranges, vbRanges2);
		CPPUNIT_ASSERT(err != 0);
		
		cl.lock(CopyLocks::READ);
		for (lbidRangeIT = ranges.begin(); lbidRangeIT != ranges.end(); 
			lbidRangeIT++)
			CPPUNIT_ASSERT(cl.isLocked(*lbidRangeIT));
		cl.release(CopyLocks::READ);
		
		for (i = 0; i < 500; i++) {
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 1, vbFlag);
			CPPUNIT_ASSERT(err != 0);
		}
		
		vbRange = *(vbRanges.begin());
// 		CPPUNIT_ASSERT(vbRange.vbFBO == 0);
		for (i = 0; i < (int)vbRange.size; i++) {
			err = brm.writeVBEntry(1, i, vbRange.vbOID, vbRange.vbFBO + i);
			CPPUNIT_ASSERT(err == 0);
		}
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		for (i = 0; i < (int)vbRange.size; i++) { 
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 1, vbFlag);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(verID == 1);
			CPPUNIT_ASSERT(vbFlag == false);
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 0, vbFlag);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(verID == 0);
			CPPUNIT_ASSERT(vbFlag == true);
		}
		
		for (; i < 500; i++) {
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 1, vbFlag);
			CPPUNIT_ASSERT(err != 0);
		}
					
		err = brm.endVBCopy(0, ranges);
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
			
		cl.lock(CopyLocks::READ);
		for (lbidRangeIT = ranges.begin(); lbidRangeIT != ranges.end(); 
				   lbidRangeIT++)
			CPPUNIT_ASSERT(!cl.isLocked(*lbidRangeIT));
		cl.release(CopyLocks::READ);

		brm.vbCommit(1);
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		for (i = 0; i < (int)vbRange.size; i++) { 
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 0, vbFlag);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(verID == 1);
			CPPUNIT_ASSERT(vbFlag == false);
			
			verID = 0;
			err = brm.vssLookup(i, verID, 0, vbFlag);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(verID == 0);
			CPPUNIT_ASSERT(vbFlag == true);
			
			err = brm.lookup(i, verID, vbFlag, oid, fbo);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(oid == vbRange.vbOID);
			CPPUNIT_ASSERT(fbo == static_cast<uint32_t>(i + vbRange.vbFBO));
			
			vbbm.lock(VBBM::WRITE);
			vss.lock(VSS::WRITE);
			vbbm.removeEntry(i, verID);
// 			vss.removeEntry(i, 1);
			vss.removeEntry(i, verID);
			vbbm.confirmChanges();
			vss.confirmChanges();
			vss.release(VSS::WRITE);
			vbbm.release(VBBM::WRITE);
		}
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		brm.deleteOID(1);
		
		vss.lock(VSS::READ);
		vbbm.lock(VBBM::READ);
		CPPUNIT_ASSERT(vbbm.size() == 0);
		CPPUNIT_ASSERT(vbbm.hashEmpty());
		CPPUNIT_ASSERT(vss.size() == 0);
		CPPUNIT_ASSERT(vss.hashEmpty());
		vss.release(VSS::READ);
		vbbm.release(VBBM::READ);
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		err = brm.saveState();
		CPPUNIT_ASSERT(err==0);
	}
	
	// cut & pasted from brm_good_2(), but with rollback instead of commit.
	void brm_good_3()
	{
		DBRM brm;
		VBBM vbbm;
		VSS vss;
		CopyLocks cl;
		int i, err, size;
		vector<LBID_t> lbids;
		vector<LBID_t>::iterator lbid;
		vector<EMEntry> extents;
		LBIDRange_v ranges;
		LBIDRange_v::iterator lbidRangeIT;
		VBRange_v vbRanges, vbRanges2;
		VBRange_v::iterator vbRangesIT;
		LBIDRange_v tmp;
		EMEntry em;
		OID_t oid;
		uint32_t fbo;
		LBIDRange range;
		VBRange vbRange;
		VER_t verID;
		bool vbFlag;
		bool caughtException;
		
		// Buildbot times out on the getBlocks() call during leakcheck b/c it takes
		// > 5 mins for some reason.  Have to ping it before 300 seconds go by.
		void (*oldsig)(int);
		
 		cerr << "brm_good_3" << endl;

		oldsig = signal(SIGALRM, keepalive);
		alarm(290);
		
 		err = brm.lookup(0, 0, false, oid, fbo);
 		CPPUNIT_ASSERT(err != 0); 
		err = brm.lookup(0, 0, true, oid, fbo);
		CPPUNIT_ASSERT(err != 0);
		
 		err = brm.createExtent(8000, 1, lbids, size);
 		CPPUNIT_ASSERT(err == 0);
 		CPPUNIT_ASSERT(size == brm.getExtentSize());
 		CPPUNIT_ASSERT(lbids.size() == 1);
  		CPPUNIT_ASSERT(*(lbids.begin()) == 0);
		
		err = brm.getExtents(1, extents);
		CPPUNIT_ASSERT(err == 0);
		CPPUNIT_ASSERT(extents.size() == 1);
		
		em = *(extents.begin());
 		CPPUNIT_ASSERT(em.range.start == 0);
		CPPUNIT_ASSERT(em.range.size*1024 == static_cast<uint32_t>(brm.getExtentSize()));
		CPPUNIT_ASSERT(em.HWM == 0);
		CPPUNIT_ASSERT(em.blockOffset == 0);
		
		for (i = 0; i < 5; i++) {
			range.start = i * 100;
			range.size = 100;
			ranges.push_back(range);
		}

		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		err = brm.beginVBCopy(1, ranges, vbRanges);
		CPPUNIT_ASSERT(err == 0);
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		cl.lock(CopyLocks::READ);
		for (lbidRangeIT = ranges.begin(); lbidRangeIT != ranges.end(); 
				   lbidRangeIT++)
			CPPUNIT_ASSERT(cl.isLocked(*lbidRangeIT));
		cl.release(CopyLocks::READ);
		
		err = brm.beginVBCopy(1, ranges, vbRanges2);
		CPPUNIT_ASSERT(err != 0);
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		cl.lock(CopyLocks::READ);
		for (lbidRangeIT = ranges.begin(); lbidRangeIT != ranges.end(); 
				   lbidRangeIT++)
			CPPUNIT_ASSERT(cl.isLocked(*lbidRangeIT));
		cl.release(CopyLocks::READ);
		
		for (i = 0; i < 500; i++) {
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 1, vbFlag);
			CPPUNIT_ASSERT(err != 0);
		}
		
		vbRange = *(vbRanges.begin());
// 		CPPUNIT_ASSERT(vbRange.vbFBO == 0);
		for (i = 0; i < (int)vbRange.size; i++) {
			err = brm.writeVBEntry(1, i, vbRange.vbOID, vbRange.vbFBO + i);
			CPPUNIT_ASSERT(err == 0);
		}
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		for (i = 0; i < (int)vbRange.size; i++) { 
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 1, vbFlag);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(verID == 1);
			CPPUNIT_ASSERT(vbFlag == false);
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 0, vbFlag);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(verID == 0);
			CPPUNIT_ASSERT(vbFlag == true);
		}
		
		for (; i < 500; i++) {
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 1, vbFlag);
			CPPUNIT_ASSERT(err != 0);
		}
					
		err = brm.endVBCopy(0, ranges);
			
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		cl.lock(CopyLocks::READ);
		for (lbidRangeIT = ranges.begin(); lbidRangeIT != ranges.end(); 
				   lbidRangeIT++)
			CPPUNIT_ASSERT(!cl.isLocked(*lbidRangeIT));
		cl.release(CopyLocks::READ);
		
		err = brm.getUncommittedLBIDs(1, lbids);
		CPPUNIT_ASSERT(err == 0);
		CPPUNIT_ASSERT(lbids.size() == vbRange.size);
		sort<vector<LBID_t>::iterator>(lbids.begin(), lbids.end());
		lbid = lbids.begin();
		for (i = 0; i < static_cast<int>(lbids.size()); i++, lbid++)
			CPPUNIT_ASSERT((*lbid) == static_cast<LBID_t>(i));
		
		range.start = 0;
		range.size = i;
		tmp.push_back(range);
		err = brm.vbRollback(1, tmp);
		CPPUNIT_ASSERT(err == 0);
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		for (i = 0; i < (int)vbRange.size; i++) { 
			verID = MAXINT;
			
			err = brm.vssLookup(i, verID, 0, vbFlag);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(verID == 0);
			CPPUNIT_ASSERT(vbFlag == false);
			
			err = brm.lookup(i, verID, vbFlag, oid, fbo);
			CPPUNIT_ASSERT(err == 0);
 			CPPUNIT_ASSERT(oid == 1);
			CPPUNIT_ASSERT(fbo == static_cast<uint32_t>(i));
			
			vbbm.lock(VBBM::WRITE);
			vss.lock(VSS::WRITE);

#ifdef BRM_DEBUG
 			caughtException = false;
			try {
				vbbm.removeEntry(i, verID);
				vbbm.confirmChanges();
			}
			catch (logic_error &e) {
				vbbm.undoChanges();
				caughtException = true;
			}
			CPPUNIT_ASSERT(caughtException);
			caughtException = false;
			try {
				vss.removeEntry(i, 1);
				vss.confirmChanges();
			}
			catch (logic_error &e) {
				vss.undoChanges();
				caughtException = true;
			}
			CPPUNIT_ASSERT(caughtException);
#endif

			vss.removeEntry(i, verID);
			vss.confirmChanges();
			vss.release(VSS::WRITE);
			vbbm.release(VBBM::WRITE);
		}
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		brm.deleteOID(1);
		
		vbbm.lock(VBBM::READ);
		vss.lock(VSS::READ);
		CPPUNIT_ASSERT(vbbm.size() == 0);
		CPPUNIT_ASSERT(vbbm.hashEmpty());
		CPPUNIT_ASSERT(vss.size() == 0);
		CPPUNIT_ASSERT(vss.hashEmpty());
		vss.release(VSS::READ);
		vbbm.release(VBBM::READ);
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

		err = brm.saveState();
		CPPUNIT_ASSERT(err==0);
	}

	/* This test verifies that deleteOID returns an error for
	non-existant OIDs (bug #105) */
	void brm_deleteOID()
	{
		DBRM brm;
		int err;
		vector<EMEntry> extents;
		
 		cerr << "brm_deleteOID" << endl;

		err = brm.getExtents(1, extents);
#ifdef BRM_DEBUG
		if (err == 0)
			cerr << "Make sure OID 1 isn't allocated in the extent map" << endl;
#endif
		CPPUNIT_ASSERT(err != 0);
		CPPUNIT_ASSERT(extents.empty());
		err = brm.deleteOID(1);
		CPPUNIT_ASSERT(err != 0);
	}
	
	/* This test verifies that setHWM and getHWM return an error for
	nonexistant OIDs (bugs #106, 107) */
	
	void brm_HWM()
	{
		DBRM brm;
		int err;
		HWM_t hwm;
		vector<EMEntry> extents;
		
 		cerr << "brm_HWM" << endl;

		err = brm.getExtents(1, extents);
#ifdef BRM_DEBUG
		if (err == 0)
			cerr << "Make sure OID 1 isn't allocated in the extent map" << endl;
#endif
		CPPUNIT_ASSERT(extents.size() == 0);
		err = brm.setHWM(1, 10);
		CPPUNIT_ASSERT(err != 0);
		err = brm.getHWM(1, hwm);
		CPPUNIT_ASSERT(err != 0);
	}
	
void DBRM_resource_graph_1()
{
	DBRM dbrm;
	pthread_t t;
	VER_t txn = 1;
	int err, allocSize;
	LBIDRange range;
	vector<LBID_t> lbids;
	vector<LBIDRange> ranges;
	vector<VBRange> freeList;

	err = dbrm.createExtent(8000, 1, lbids, allocSize);
	CPPUNIT_ASSERT(err == 0);

	range.start = 1000;
	range.size = 1000;
	ranges.push_back(range);
	err = dbrm.beginVBCopy(txn, ranges, freeList);
	CPPUNIT_ASSERT(err == 0);

	pthread_create(&t, NULL, DBRM_dummy_1, NULL);
	sleep(1);
	
	// thread tries to grab 500-1500, blocks
	err = dbrm.endVBCopy(txn, ranges);
	CPPUNIT_ASSERT(err == 0);
	err = dbrm.vbCommit(txn);
	CPPUNIT_ASSERT(err == 0);

	dbrm.deleteOID(1);
	
	//thread finishes
	pthread_join(t, NULL);
}

void DBRM_resource_graph_deadlock()
{
	DBRM dbrm;
	pthread_t t;
	VER_t txn = 1;
	int err, i;
	LBIDRange range;
	vector<LBID_t> lbids;
	vector<LBIDRange> ranges;
	vector<VBRange> freeList;

	range.start = 1000;
	range.size = 1000;
	ranges.push_back(range);
	err = dbrm.beginVBCopy(txn, ranges, freeList);
	CPPUNIT_ASSERT(err == 0);

	for (i = range.start; i < range.start + range.size; i++) {
		err = dbrm.writeVBEntry(txn, i, 1, i);
		CPPUNIT_ASSERT(err == 0);
	}
	err = dbrm.endVBCopy(txn, ranges);
	CPPUNIT_ASSERT(err == 0);

	pthread_create(&t, NULL, DBRM_deadlock, NULL);

	// thread grabs 2000-2999 and 1000-1999 as 2 seperate ranges
	sleep(1);
	
	range.start = 2000;
	range.size = 1000;
	ranges.clear();
	ranges.push_back(range);
	err = dbrm.beginVBCopy(txn, ranges, freeList);
	CPPUNIT_ASSERT(err == ERR_DEADLOCK);
	
	// roll back the blocks we "wrote"
	range.start = 1000;
	range.size = 1000;
	ranges.clear();
	ranges.push_back(range);
	err = dbrm.vbRollback(txn, ranges);
	CPPUNIT_ASSERT(err == ERR_OK);

	// thread finishes	

	txn = 3;
	err = dbrm.beginVBCopy(txn, ranges, freeList);
	CPPUNIT_ASSERT(err == 0);

	for (i = range.start; i < range.start + range.size; i++) {
		err = dbrm.writeVBEntry(txn, i, 1, i);
		CPPUNIT_ASSERT(err == 0);
	}
	err = dbrm.endVBCopy(txn, ranges);
	CPPUNIT_ASSERT(err == 0);

	err = dbrm.vbRollback(txn, ranges);
	CPPUNIT_ASSERT(err == 0);

	pthread_join(t, NULL);
}

void dbrm_clear()
{
	DBRM dbrm;
	VSS vss;
	VBBM vbbm;
	int err, vssShmid, vbbmShmid, txnID = 1, i;
	struct shmid_ds vssShminfo[3], vbbmShminfo[3];
	LBIDRange_v ranges;
	LBIDRange range;
	VBRange_v freelist;

 	err = dbrm.clear();
 	CPPUNIT_ASSERT(err == ERR_OK);
	
	// grab the size of vss and vbbm shmsegs somehow
	vss.lock(VSS::READ);
	vbbm.lock(VBBM::READ);
	vssShmid = vss.getShmid();
	vbbmShmid = vbbm.getShmid();
	err = shmctl(vssShmid, IPC_STAT, &vssShminfo[0]);
	CPPUNIT_ASSERT(err == 0);
	err = shmctl(vbbmShmid, IPC_STAT, &vbbmShminfo[0]);
	CPPUNIT_ASSERT(err == 0);
	vss.release(VSS::READ);
	vbbm.release(VBBM::READ);

	// do begin, write, end vbcopy for 150k blocks
	cerr << endl << "Adding 150k block entries. ";
	range.start = 1;
	range.size = 150000;
	ranges.push_back(range);
	err = dbrm.beginVBCopy(txnID, ranges, freelist);
	CPPUNIT_ASSERT(err == 0);

	for (i = range.start; (uint32_t) i < range.size; i++) {
		if (i % 50000 == 0)
			cerr << " ... " << i;
		err = dbrm.writeVBEntry(txnID, i, 1, i);
		CPPUNIT_ASSERT(err == 0);
	}
	err = dbrm.endVBCopy(txnID, ranges);
	CPPUNIT_ASSERT(err == 0);
	cerr << "  done." << endl;

	// grab the sizes again
	vss.lock(VSS::READ);
	vbbm.lock(VBBM::READ);
	vssShmid = vss.getShmid();
	vbbmShmid = vbbm.getShmid();
	err = shmctl(vssShmid, IPC_STAT, &vssShminfo[1]);
	CPPUNIT_ASSERT(err == 0);
	err = shmctl(vbbmShmid, IPC_STAT, &vbbmShminfo[1]);
	CPPUNIT_ASSERT(err == 0);
	vss.release(VSS::READ);
	vbbm.release(VBBM::READ);

	// make sure they grew
	CPPUNIT_ASSERT(vssShminfo[0].shm_segsz < vssShminfo[1].shm_segsz);
	CPPUNIT_ASSERT(vbbmShminfo[0].shm_segsz < vbbmShminfo[1].shm_segsz);

	dbrm.clear();

	// check that the new size is the same as the original
	vss.lock(VSS::READ);
	vbbm.lock(VBBM::READ);
	vssShmid = vss.getShmid();
	vbbmShmid = vbbm.getShmid();
	err = shmctl(vssShmid, IPC_STAT, &vssShminfo[2]);
	CPPUNIT_ASSERT(err == 0);
	err = shmctl(vbbmShmid, IPC_STAT, &vbbmShminfo[2]);
	CPPUNIT_ASSERT(err == 0);
	vss.release(VSS::READ);
	vbbm.release(VBBM::READ);

	CPPUNIT_ASSERT(vssShminfo[0].shm_segsz == vssShminfo[2].shm_segsz);
	CPPUNIT_ASSERT(vbbmShminfo[0].shm_segsz == vbbmShminfo[2].shm_segsz);

}

void sessionmanager_1()
{
	DBRM dbrm;
	int tmp = -1;

	tmp = dbrm.verID();
	// there's no "correct" value b/c it's monotonically increasing.
}

void sessionmanager_2()
{
	DBRM dbrm;
	SessionManagerServer::TxnID tmp;
	int ver1, ver2;

	ver1 = dbrm.verID();
	tmp = dbrm.newTxnID(1000, true);
	ver2 = dbrm.verID();
	
	CPPUNIT_ASSERT(ver2 == ver1 + 1);
	CPPUNIT_ASSERT(ver2 == tmp.id);
	CPPUNIT_ASSERT(tmp.valid == true);

	dbrm.committed(tmp);
	CPPUNIT_ASSERT(tmp.valid == false);
}

void sessionmanager_3()
{
	DBRM dbrm;
	SessionManagerServer::TxnID txn, txn2;
	
	txn = dbrm.newTxnID(1000, true);
	txn2 = dbrm.getTxnID(1000);

	CPPUNIT_ASSERT(txn.id == txn2.id);
	CPPUNIT_ASSERT(txn.valid == txn2.valid == true);

	dbrm.rolledback(txn);
	CPPUNIT_ASSERT(txn.valid == false);

	txn2 = dbrm.getTxnID(1000);
	CPPUNIT_ASSERT(txn2.valid == false);
}

void sessionmanager_4()
{
	DBRM dbrm;
	SessionManagerServer::TxnID txn, txn2;
	const SessionManagerServer::SIDTIDEntry *stmap;
	int len;

	txn = dbrm.newTxnID(1000, true);
	txn2 = dbrm.newTxnID(1001, true);
	stmap = dbrm.SIDTIDMap(len);

	CPPUNIT_ASSERT(len == 2);

#ifdef BRM_VERBOSE
	int i;
	cerr << "len = " << len << endl;
	for (i = 0; i < len; i++) {
		cerr << "  " << i << ": txnid=" << stmap[i].txnid.id << " valid=" << 
			stmap[i].txnid.valid << " sessionid=" << stmap[i].sessionid << endl;
	}
#endif

	dbrm.committed(txn);
	dbrm.committed(txn2);
	delete [] stmap;
	stmap = dbrm.SIDTIDMap(len);

	CPPUNIT_ASSERT(len == 0);
	delete [] stmap;
}

void sessionmanager_5()
{
	SessionManager sm;
	int tmp = -1;

	tmp = sm.verID();
	// there's no "correct" value b/c it's monotonically increasing.
}

void sessionmanager_6()
{
	SessionManager sm;
	SessionManager::TxnID tmp;
	int ver1, ver2;

	ver1 = sm.verID();
	tmp = sm.newTxnID(1000);
	ver2 = sm.verID();
	
	CPPUNIT_ASSERT(ver2 == ver1 + 1);
	CPPUNIT_ASSERT(ver2 == tmp.id);
	CPPUNIT_ASSERT(tmp.valid == true);

	sm.committed(tmp);
	CPPUNIT_ASSERT(tmp.valid == false);
}

void sessionmanager_7()
{
	SessionManager sm;
	SessionManager::TxnID txn, txn2;
	
	txn = sm.newTxnID(1000);
	txn2 = sm.getTxnID(1000);

	CPPUNIT_ASSERT(txn.id == txn2.id);
	CPPUNIT_ASSERT(txn.valid == txn2.valid == true);

	sm.rolledback(txn);
	CPPUNIT_ASSERT(txn.valid == false);

	txn2 = sm.getTxnID(1000);
	CPPUNIT_ASSERT(txn2.valid == false);
}

void sessionmanager_8()
{
	SessionManager sm;
	SessionManager::TxnID txn, txn2;
	const SessionManager::SIDTIDEntry *stmap;
	int len;

	txn = sm.newTxnID(1000, true);
	txn2 = sm.newTxnID(1001, true);
	stmap = sm.SIDTIDMap(len);

	CPPUNIT_ASSERT(len == 2);

#ifdef BRM_VERBOSE
	int i;
	cerr << "len = " << len << endl;
	for (i = 0; i < len; i++) {
		cerr << "  " << i << ": txnid=" << stmap[i].txnid.id << " valid=" << 
			stmap[i].txnid.valid << " sessionid=" << stmap[i].sessionid << endl;
	}
#endif

	sm.committed(txn);
	sm.committed(txn2);
	delete [] stmap;
	stmap = sm.SIDTIDMap(len);

	CPPUNIT_ASSERT(len == 0);
	delete [] stmap;
}

	void brm_markExtentsInvalid()
	{
		
		DBRM brm;
		int i, err, allocdSize, iterations = 100;  // (EM_INITIAL_SIZE + 3*EM_INCREMENT)
		vector<LBID_t> lbids;
		int64_t min, max;
		int32_t seqNum;
		const uint32_t extentSize = brm.getExtentSize();
		
		cerr << "brm_markExtentsInvalid" << endl;

		err = brm.createExtent(extentSize * iterations, 1, lbids, allocdSize);
		CPPUNIT_ASSERT(lbids.size() == iterations);
		CPPUNIT_ASSERT(err == 0);

		// mark all extents valid
		for (i = 0; i < iterations; i++) {
			err = brm.setExtentMaxMin(lbids[i], 1, 0, 0);
			CPPUNIT_ASSERT(err == 0);
		}

		err = brm.markExtentsInvalid(lbids);
		CPPUNIT_ASSERT(err == 0);

		// check that they are all invalid/updating/whatever.
		// != CP_VALID is what we're looking for
		for (i = 0; i < iterations; i++) {
			err = brm.getExtentMaxMin(lbids[i], max, min, seqNum);
			CPPUNIT_ASSERT(err == CP_UPDATING);
		}

		// cleanup
		err = brm.deleteOID(1);
	}
};


CPPUNIT_TEST_SUITE_REGISTRATION( BRMTest );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  idbdatafile::IDBPolicy::configIDBPolicy();
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


