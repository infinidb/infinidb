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
 *   $Id: tdriver-load.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ********************************************************************/

/* The tests in this file validate the load/save functionality
in the ExtentMap.  In tdriver.cpp, the test extentMap_good_1 saves a snapshot
of the extent map.  Here, we do the same tests starting from where the
snapshot was taken. */

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
#include "IDBPolicy.h"

#ifdef NO_TESTS
#undef CPPUNIT_ASSERT
#define CPPUNIT_ASSERT(a)
#endif

using namespace BRM;
using namespace std;

void keepalive(int signum) {
	cerr << "Yes, it's still going..." << endl;
	alarm(290);
}

class BRMTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(BRMTest);

CPPUNIT_TEST(extentMap_good_1);
CPPUNIT_TEST(extentMap_freelist);
//CPPUNIT_TEST(extentMap_overfill);
CPPUNIT_TEST(many_ExtentMap_instances);
CPPUNIT_TEST(brm_extentmap_good_1);
CPPUNIT_TEST(brm_good_2);
CPPUNIT_TEST(brm_good_3);
CPPUNIT_TEST(brm_deleteOID);
CPPUNIT_TEST(brm_HWM);

CPPUNIT_TEST_SUITE_END();

private:
public:
	
	void masterSegmentTest_goodbad()
	{
		MasterSegmentTable mst;
		struct MSTEntry *mt;
		
		mt = mst.getTable_write(MasterSegmentTable::EMTable, false);
		CPPUNIT_ASSERT(mt != NULL);
		mt = mst.getTable_write(MasterSegmentTable::EMTable, false);
		CPPUNIT_ASSERT(mt == NULL);
		mt = mst.getTable_write(MasterSegmentTable::EMFreeList, false);
		CPPUNIT_ASSERT(mt != NULL);
		mt = mst.getTable_write(MasterSegmentTable::EMFreeList, false);
		CPPUNIT_ASSERT(mt == NULL);
		mt = mst.getTable_write(MasterSegmentTable::VBBMSegment, false);
		CPPUNIT_ASSERT(mt != NULL);
		mt = mst.getTable_write(MasterSegmentTable::VBBMSegment, false);
		CPPUNIT_ASSERT(mt == NULL);
		
		mst.releaseTable_write(MasterSegmentTable::EMTable);
		mst.releaseTable_write(MasterSegmentTable::EMFreeList);
		mst.releaseTable_write(MasterSegmentTable::VBBMSegment);
		
		mt = mst.getTable_write(MasterSegmentTable::EMTable, false);
		CPPUNIT_ASSERT(mt != NULL);
		mt = mst.getTable_write(MasterSegmentTable::EMFreeList, false);
		CPPUNIT_ASSERT(mt != NULL);
		mt = mst.getTable_write(MasterSegmentTable::VBBMSegment, false);
		CPPUNIT_ASSERT(mt != NULL);
		mt = mst.getTable_write(MasterSegmentTable::EMTable, false);
		CPPUNIT_ASSERT(mt == NULL);
		mt = mst.getTable_write(MasterSegmentTable::EMFreeList, false);
		CPPUNIT_ASSERT(mt == NULL);
		mt = mst.getTable_write(MasterSegmentTable::VBBMSegment, false);
		CPPUNIT_ASSERT(mt == NULL);
		mst.releaseTable_write(MasterSegmentTable::EMTable);
		mst.releaseTable_write(MasterSegmentTable::EMFreeList);
		mst.releaseTable_write(MasterSegmentTable::VBBMSegment);
		
		mt = mst.getTable_read(MasterSegmentTable::EMTable, false);
		CPPUNIT_ASSERT(mt != NULL);
		mt = mst.getTable_read(MasterSegmentTable::EMTable, false);
		CPPUNIT_ASSERT(mt != NULL);
		mt = mst.getTable_read(MasterSegmentTable::EMTable, false);
		CPPUNIT_ASSERT(mt != NULL);
		mt = mst.getTable_write(MasterSegmentTable::EMTable, false);
		CPPUNIT_ASSERT(mt == NULL);
		mst.releaseTable_read(MasterSegmentTable::EMTable);
		mst.releaseTable_read(MasterSegmentTable::EMTable);
		mst.releaseTable_read(MasterSegmentTable::EMTable);
		
		mt = mst.getTable_write(MasterSegmentTable::EMTable, false);
		CPPUNIT_ASSERT(mt != NULL);
		mt = mst.getTable_read(MasterSegmentTable::EMTable, false);
		CPPUNIT_ASSERT(mt == NULL);
		mst.releaseTable_write(MasterSegmentTable::EMTable);
	}
	
	void extentMap_good_1()
	{
		ExtentMap em;
		int i, err, oid, iterations = 1300;  // (EM_INITIAL_SIZE + 3*EM_INCREMENT)
		int caughtException = 0, allocdSize;
		uint32_t fbo, hwm;
		BRM::HWM_t hwm2;
		BRM::VER_t txnID;
		vector<LBID_t> lbids;
		const uint32_t extentSize = em.getExtentSize();

		em.load(string("EMImage"));
		em.checkConsistency();
		
		for (i = 0; i < iterations; i++) {
			err = em.lookup(static_cast<LBID_t>(i*extentSize), oid, fbo);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(oid == i);
			CPPUNIT_ASSERT(fbo == 0);
			if (i != 0) {
				err = em.lookup(static_cast<LBID_t>(i*extentSize - 1), oid, fbo);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(oid == i-1);
				CPPUNIT_ASSERT(fbo == extentSize - 1);
			}
			if (i != iterations - 1) {
				err = em.lookup(static_cast<LBID_t>(i*extentSize + 1), oid, fbo);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(oid == i);
				CPPUNIT_ASSERT(fbo == 1);
			}
		}
		
		em.checkConsistency();

		err = em.lookup(static_cast<LBID_t>(i*extentSize), oid, fbo);
		CPPUNIT_ASSERT(err == -1);
		
		for (i = 0; i < iterations; i++) {
			err = em.getBulkInsertVars(static_cast<LBID_t>(i*extentSize),
									   hwm2, txnID);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm2 == 0);
			CPPUNIT_ASSERT(txnID == 0);
			err = em.setBulkInsertVars(static_cast<LBID_t>(i*extentSize),
									   i, i + 1);
			em.confirmChanges();
			CPPUNIT_ASSERT(err == 0);
			err = em.getBulkInsertVars(static_cast<LBID_t>(i*extentSize),
									   hwm2, txnID);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm2 == static_cast<LBID_t>(i));
			CPPUNIT_ASSERT(txnID == static_cast<VER_t>(i+1));
			
			hwm = em.getHWM(i);
			CPPUNIT_ASSERT(hwm == 0);
			em.setHWM(i, (i > (extentSize - 1) ? extentSize - 1 : i));
			em.confirmChanges();
			hwm = em.getHWM(i);
			CPPUNIT_ASSERT(hwm == static_cast<uint32_t>(i > extentSize-1 ? extentSize-1 : i));
		}

		em.checkConsistency();
		
#ifdef BRM_DEBUG
		caughtException = 0;
		try {
			em.setHWM(i, hwm);
		}
		catch(std::invalid_argument e) {
			caughtException = 1;
		}
		em.undoChanges();
		CPPUNIT_ASSERT(caughtException == 1);
#endif

		for (i = 0; i < iterations; i++) {
			em.deleteOID(i);
			em.confirmChanges();
		}

#ifdef BRM_DEBUG		
		caughtException = 0;
		try {
			em.deleteOID(i);
		}
		catch(std::invalid_argument &e) {
			caughtException = 1;
		}
		em.undoChanges();
		CPPUNIT_ASSERT(caughtException == 1);
#endif		

		em.checkConsistency();
	}
	
	void extentMap_freelist()
	{
		ExtentMap em;
		int i, allocdSize, iterations = 1400;  // (EM_INITIAL_SIZE + 4*EM_INCREMENT)
		vector<LBID_t> lbids;
		const int extentSize = em.getExtentSize();
		
		for (i = 0; i < iterations; i++) {
			em.createExtent(extentSize, i, lbids, allocdSize);
			em.confirmChanges();
			CPPUNIT_ASSERT(lbids.back() == static_cast<LBID_t>(i*extentSize));
		}
		
		em.checkConsistency();
		
		//frag the lbid space to blow up the free list
		for (i = 0; i < iterations; i += 2) {
			em.deleteOID(i);
			em.confirmChanges();
		}
		
		em.checkConsistency();
		
		//fill in the holes
		for (i = 0; i < iterations; i += 2) {
			em.createExtent(extentSize, i, lbids, allocdSize);
			em.confirmChanges();
		}
		
		for (i = 0; i < iterations; i += 2) {
			em.deleteOID(i);
			em.confirmChanges();
		}
		
		for (i = 1; i < iterations; i += 2) {
			em.deleteOID(i);
			em.confirmChanges();
		}
	
		em.checkConsistency();
	}
	
	/* This test verifies that the ExtentMap stops allocating space when it's full.
	(Bug #104)	It takes a long time to run with the default LBID 
	space characteristics so by default it's disabled.*/
	void extentMap_overfill()
	{
		int i, err, tmp;
		BlockResolutionManager brm;	
		vector<LBID_t> lbids;
		
		for (i = 0; i < 67108; i++) {
			err = brm.createExtent(1024000, i, lbids, tmp);
			CPPUNIT_ASSERT(err == 0);
		}
		err = brm.createExtent(1024000, i, lbids, tmp);
		CPPUNIT_ASSERT(err == -1);
		
		for (i = 0; i < 67108; i++) {
			err = brm.deleteOID(i);
			CPPUNIT_ASSERT(err == 0);
		}
		err = brm.deleteOID(i);
		CPPUNIT_ASSERT(err == -1);
	}			   
				   
	void copyLocks_good_1()
	{
		CopyLocks cl;
		LBIDRange range;
		int i, iterations = 1000;
		
		cl.lock(CopyLocks::WRITE);
		range.start = 5;
		range.size = 2;
		cl.lockRange(range, 0);
		CPPUNIT_ASSERT(cl.isLocked(range) == true);
		range.start++;
		CPPUNIT_ASSERT(cl.isLocked(range) == true);
		range.start++;
		CPPUNIT_ASSERT(cl.isLocked(range) == false);
		range.start = 0;
		CPPUNIT_ASSERT(cl.isLocked(range) == false);
		range.start = 3;
		CPPUNIT_ASSERT(cl.isLocked(range) == false);
		range.start++;
		CPPUNIT_ASSERT(cl.isLocked(range) == true);
		range.start = 5;
		range.size = 1;
		CPPUNIT_ASSERT(cl.isLocked(range) == true);
		range.size = 2;
		cl.releaseRange(range);
		
		//make sure it can grow
		for (i = 0; i < iterations*2; i+=2) {
			range.start = i;
			cl.lockRange(range, 0);
		}
		
		range.size = 1;
		
		for (i = 0; i < iterations*2; i++) {
			range.start = i;
			CPPUNIT_ASSERT(cl.isLocked(range) == true);
		}
		range.start = i;
		CPPUNIT_ASSERT(cl.isLocked(range) == false);
		
		for (i = 0; i < iterations*2; i+=2) {
			range.start = i;
			cl.releaseRange(range);
		}
		cl.release(CopyLocks::WRITE);
	}
	
	void brm_extentmap_good_1()
	{
		
		BlockResolutionManager brm;
		int i, err, oid, allocdSize, 
			iterations = 1300;  // (EM_INITIAL_SIZE + 3*EM_INCREMENT)
		uint32_t fbo, hwm;
		vector<LBID_t> lbids;
		HWM_t hwm2;
		VER_t txnID;
		const uint32_t extentSize = brm.getExtentSize();
		
		for (i = 0; i < iterations; i++) {
			brm.createExtent(extentSize, i, lbids, allocdSize);
			CPPUNIT_ASSERT(lbids.back() == static_cast<LBID_t>(i*extentSize));
		}

 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		brm.saveExtentMap(string("EMImage"));
		brm.loadExtentMap(string("EMImage"));
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

		for (i = 0; i < iterations; i++) {
			err = brm.lookup(static_cast<LBID_t>(i*extentSize), 0, false, oid, fbo);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(oid == i);
			CPPUNIT_ASSERT(fbo == 0);
			if (i != 0) {
				err = brm.lookup(static_cast<LBID_t>(i*extentSize - 1), 0, false, oid, fbo);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(oid == i-1);
				CPPUNIT_ASSERT(fbo == extentSize - 1);
			}
			if (i != iterations) {
				err = brm.lookup(static_cast<LBID_t>(i*extentSize + 1), 0, false, oid, fbo);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(oid == i);
				CPPUNIT_ASSERT(fbo == 1);
			}
		}
		
 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

		err = brm.lookup(static_cast<LBID_t>(i*extentSize), 0, false, oid, fbo);
		CPPUNIT_ASSERT(err == -1);
		
		for (i = 0; i < iterations; i++) {
			err = brm.getBulkInsertVars(static_cast<LBID_t>(i*extentSize),
									   hwm2, txnID);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm2 == 0);
			CPPUNIT_ASSERT(txnID == 0);
			err = brm.setBulkInsertVars(static_cast<LBID_t>(i*extentSize),
									   i, i + 1);
			CPPUNIT_ASSERT(err == 0);
			err = brm.getBulkInsertVars(static_cast<LBID_t>(i*extentSize),
									   hwm2, txnID);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm2 == static_cast<BRM::LBID_t>(i));
			CPPUNIT_ASSERT(txnID == static_cast<BRM::VER_t>(i+1));
			
			err = brm.getHWM(i, hwm);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm == 0);
			err = brm.setHWM(i, (i > extentSize - 1 ? extentSize - 1 : i));
			CPPUNIT_ASSERT(err == 0);
			err = brm.getHWM(i, hwm);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm == static_cast<uint32_t>(i > extentSize - 1 ? extentSize - 1 : i));
		}
		
 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

#ifdef BRM_DEBUG
		err = brm.setHWM(i, hwm);
		CPPUNIT_ASSERT(err == -1);
#endif

		for (i = 0; i < iterations; i++) {
			err = brm.deleteOID(i);
			CPPUNIT_ASSERT(err == 0);
		}

		err = brm.deleteOID(i);
		CPPUNIT_ASSERT(err == -1);
		
 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

	}
	
	void brm_good_2()
	{
		BlockResolutionManager brm;
		VBBM vbbm;
		VSS vss;
		CopyLocks cl;
		int i, err, size;
		vector<LBID_t> lbids;
		vector<EMEntry> extents;
		LBIDRange_v ranges;
		LBIDRange_v::iterator lbidRangeIT;
		VBRange_v vbRanges, vbRanges2;
		VBRange_v::iterator vbRangesIT;
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
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		oldsig = signal(SIGALRM, keepalive);
		alarm(290);
		
		err = brm.lookup(0, 0, false, oid, fbo);
		CPPUNIT_ASSERT(err == -1); 
		err = brm.lookup(0, 0, true, oid, fbo);
		CPPUNIT_ASSERT(err == -1);
		
		err = brm.createExtent(10000, 0, lbids, size);
		CPPUNIT_ASSERT(err == 0);
		CPPUNIT_ASSERT(size == brm.getExtentSize());
		CPPUNIT_ASSERT(lbids.size() == 1);
//  		CPPUNIT_ASSERT(*(lbids.begin()) == 0);
		
		err = brm.getExtents(0, extents);
		CPPUNIT_ASSERT(err == 0);
		CPPUNIT_ASSERT(extents.size() == 1);
		
		em = *(extents.begin());
// 		CPPUNIT_ASSERT(em.range.start == 0);
 		CPPUNIT_ASSERT(em.range.size*1024 == static_cast<uint32_t>(brm.getExtentSize()));
		CPPUNIT_ASSERT(em.HWM == 0);
		CPPUNIT_ASSERT(em.blockOffset == 0);
		
		for (i = 0; i < 50; i++) {
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
		CPPUNIT_ASSERT(err == -1);
		
		cl.lock(CopyLocks::READ);
		for (lbidRangeIT = ranges.begin(); lbidRangeIT != ranges.end(); 
			lbidRangeIT++)
			CPPUNIT_ASSERT(cl.isLocked(*lbidRangeIT));
		cl.release(CopyLocks::READ);
		
		for (i = 0; i < 5000; i++) {
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 1, vbFlag);
			CPPUNIT_ASSERT(err == -1);
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
		
		for (; i < 5000; i++) {
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 1, vbFlag);
			CPPUNIT_ASSERT(err == -1);
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
			vss.release(VSS::WRITE);
			vbbm.release(VBBM::WRITE);
		}
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		brm.deleteOID(0);
		
		vss.lock(VSS::READ);
		vbbm.lock(VBBM::READ);
		CPPUNIT_ASSERT(vbbm.size() == 0);
		CPPUNIT_ASSERT(vbbm.hashEmpty());
		CPPUNIT_ASSERT(vss.size() == 0);
		CPPUNIT_ASSERT(vss.hashEmpty());
		vss.release(VSS::READ);
		vbbm.release(VBBM::READ);
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
	}
	
	// cut & pasted from brm_good_2(), but with rollback instead of commit.
	void brm_good_3()
	{
		BlockResolutionManager brm;
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
		
		oldsig = signal(SIGALRM, keepalive);
		alarm(290);
		
 		err = brm.lookup(0, 0, false, oid, fbo);
 		CPPUNIT_ASSERT(err == -1); 
		err = brm.lookup(0, 0, true, oid, fbo);
		CPPUNIT_ASSERT(err == -1);
		
 		err = brm.createExtent(10000, 0, lbids, size);
 		CPPUNIT_ASSERT(err == 0);
 		CPPUNIT_ASSERT(size == brm.getExtentSize());
 		CPPUNIT_ASSERT(lbids.size() == 1);
  		CPPUNIT_ASSERT(*(lbids.begin()) == 0);
		
		err = brm.getExtents(0, extents);
		CPPUNIT_ASSERT(err == 0);
		CPPUNIT_ASSERT(extents.size() == 1);
		
		em = *(extents.begin());
 		CPPUNIT_ASSERT(em.range.start == 0);
		CPPUNIT_ASSERT(em.range.size*1024 == static_cast<uint32_t>(brm.getExtentSize()));
		CPPUNIT_ASSERT(em.HWM == 0);
		CPPUNIT_ASSERT(em.blockOffset == 0);
		
		for (i = 0; i < 50; i++) {
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
		CPPUNIT_ASSERT(err == -1);
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		cl.lock(CopyLocks::READ);
		for (lbidRangeIT = ranges.begin(); lbidRangeIT != ranges.end(); 
				   lbidRangeIT++)
			CPPUNIT_ASSERT(cl.isLocked(*lbidRangeIT));
		cl.release(CopyLocks::READ);
		
		for (i = 0; i < 5000; i++) {
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 1, vbFlag);
			CPPUNIT_ASSERT(err == -1);
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
		
		for (; i < 5000; i++) {
			verID = MAXINT;
			err = brm.vssLookup(i, verID, 1, vbFlag);
			CPPUNIT_ASSERT(err == -1);
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
			CPPUNIT_ASSERT(oid == 0);
			CPPUNIT_ASSERT(fbo == static_cast<uint32_t>(i));
			
			vbbm.lock(VBBM::WRITE);
			vss.lock(VSS::WRITE);

#ifdef BRM_DEBUG
			caughtException = false;
			try {
				vbbm.removeEntry(i, verID);
			}
			catch (logic_error &e) {
				caughtException = true;
			}
			CPPUNIT_ASSERT(caughtException);
			caughtException = false;
			try {
				vss.removeEntry(i, 1);
			}
			catch (logic_error &e) {
				caughtException = true;
			}
			CPPUNIT_ASSERT(caughtException);
#endif

			vss.removeEntry(i, verID);
			vss.release(VSS::WRITE);
			vbbm.release(VBBM::WRITE);
		}
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		brm.deleteOID(0);
		
		vbbm.lock(VBBM::READ);
		vss.lock(VSS::READ);
		CPPUNIT_ASSERT(vbbm.size() == 0);
		CPPUNIT_ASSERT(vbbm.hashEmpty());
		CPPUNIT_ASSERT(vss.size() == 0);
		CPPUNIT_ASSERT(vss.hashEmpty());
		vss.release(VSS::READ);
		vbbm.release(VBBM::READ);
		
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
	}

	/* This test verifies that deleteOID returns an error for
	non-existant OIDs (bug #105) */
	void brm_deleteOID()
	{
		BlockResolutionManager brm;
		int err;
		vector<EMEntry> extents;
		
		err = brm.getExtents(0, extents);
#ifdef BRM_DEBUG
		if (err != -1)
			cerr << "Make sure OID 0 isn't allocated in the extent map" << endl;
		CPPUNIT_ASSERT(err == -1);
#else
		CPPUNIT_ASSERT(err == 0);
		CPPUNIT_ASSERT(extents.empty());
#endif
		err = brm.deleteOID(0);
		CPPUNIT_ASSERT(err == -1);
	}
	
	/* This test verifies that setHWM and getHWM return an error for
	nonexistant OIDs (bugs #106, 107) */
	
	void brm_HWM()
	{
		BlockResolutionManager brm;
		int err;
		HWM_t hwm;
		vector<EMEntry> extents;
		
		err = brm.getExtents(0, extents);
#ifdef BRM_DEBUG
		if (err != -1)
			cerr << "Make sure OID 0 isn't allocated in the extent map" << endl;
		CPPUNIT_ASSERT(err == -1);
#else
		CPPUNIT_ASSERT(err == 0);
#endif
		CPPUNIT_ASSERT(extents.size() == 0);
		err = brm.setHWM(0, 10);
		CPPUNIT_ASSERT(err == -1);
		err = brm.getHWM(0, hwm);
		CPPUNIT_ASSERT(err == -1);
	}
	
	/* This test was suggested by Jean, who observed some odd behavior at around
	1032 EM instances in her code.  This test creates 2000 instances and uses 1400 of them
	to do the extentmap_freelist_1 test above. */
	void many_ExtentMap_instances()
	{
		ExtentMap **ems;
		int i, allocdSize, iterations = 1400;  // (EM_INITIAL_SIZE + 4*EM_INCREMENT)
		vector<LBID_t> lbids;
		uint32_t extentSize;
		
// 		cerr << endl << "Extent Map instance test" << endl;
		
		ems = new ExtentMap*[2000];
		for (i = 0; i < 2000; i++) 
			ems[i] = new ExtentMap();

		extentSize = ems[0]->getExtentSize();
// 		cerr << "  - Successfully created 2000 instances, starting the freelist test" 
// 				<< endl;
		for (i = 0; i < iterations; i++) {
			ems[i]->createExtent(extentSize, i, lbids, allocdSize);
			ems[i]->confirmChanges();
 			CPPUNIT_ASSERT(lbids.back() == static_cast<LBID_t>(i*extentSize));
		}
		
		//frag the lbid space to blow up the free list
		for (i = 0; i < iterations; i += 2) {
			ems[i]->deleteOID(i);
			ems[i]->confirmChanges();
		}
		
		//fill in the holes
		for (i = 0; i < iterations; i += 2) {
			ems[i]->createExtent(extentSize, i, lbids, allocdSize);
			ems[i]->confirmChanges();
		}
	
		for (i = 0; i < iterations; i += 2) {
			ems[i]->deleteOID(i);
			ems[i]->confirmChanges();
		}
		
		for (i = 1; i < iterations; i += 2) {
			ems[i]->deleteOID(i);
			ems[i]->confirmChanges();
		}
		
// 		cerr << "done.  sleeping for 30 seconds." << endl;
// 		sleep(30);
		
		for (i = 0; i < 2000; i++)
			delete ems[i];
		delete [] ems;
		
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


