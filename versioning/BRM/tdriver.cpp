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
 *   $Id: tdriver.cpp 1823 2013-01-21 14:13:09Z rdempsey $
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
#include "lbidresourcegraph.h"

#ifdef NO_TESTS
#undef CPPUNIT_ASSERT
#define CPPUNIT_ASSERT(a)
#endif

using namespace BRM;
using namespace std;
using namespace config;

pthread_mutex_t mutex;

void keepalive(int signum) {
	cerr << "Yes, it's still going..." << endl;
	alarm(290);
}

void *dummy_1(void *arg) 
{
	LBIDResourceGraph *rg = (LBIDResourceGraph *) arg;
	int err;
	VER_t txn = 2;

	pthread_mutex_lock(&mutex);
	err = rg->reserveRange(0, 2000, txn, mutex);
 	CPPUNIT_ASSERT(err == 0);
	rg->releaseResources(txn);
	pthread_mutex_unlock(&mutex);
	return NULL;
}

void *dummy_deadlock(void *arg) 
{
	LBIDResourceGraph *rg = (LBIDResourceGraph *) arg;
	int err;
	VER_t txn = 2;

	pthread_mutex_lock(&mutex);
	err = rg->reserveRange(1001, 2000, txn, mutex);
	CPPUNIT_ASSERT(err == 0);
	err = rg->reserveRange(0, 1000, txn, mutex);
	CPPUNIT_ASSERT(err == 0);
	rg->releaseResources(txn);
	CPPUNIT_ASSERT(err == 0);
	pthread_mutex_unlock(&mutex);
	return NULL;
}

void *fragd_range_helper(void *arg)
{
	LBIDResourceGraph *rg = (LBIDResourceGraph *) arg;
	set<ResourceNode *, RNLess<ResourceNode *> > *resources;
	map<VER_t, TransactionNode *> *txns;
	int err;
	VER_t txn = 2;

	pthread_mutex_lock(&mutex);
	resources = rg->getResources();
	txns = rg->getTxns();
	CPPUNIT_ASSERT(resources->size() == 2);
	CPPUNIT_ASSERT(txns->size() == 1);
	err = rg->reserveRange(0, 5000, txn, mutex);

	// blocks, main thread releases

	CPPUNIT_ASSERT(err == 0);
	CPPUNIT_ASSERT(resources->size() == 5);
	CPPUNIT_ASSERT(txns->size() == 1);
	rg->releaseResources(txn);
	pthread_mutex_unlock(&mutex);

	return NULL;
}

void *overlapping_range_helper(void *arg)
{
	LBIDResourceGraph *rg = (LBIDResourceGraph *) arg;
	set<ResourceNode *, RNLess<ResourceNode *> > *resources;
	set<ResourceNode *, RNLess<ResourceNode *> >::const_iterator it;
	map<VER_t, TransactionNode *> *txns;
	int err;
	VER_t txn = 2;

	pthread_mutex_lock(&mutex);
	resources = rg->getResources();
	txns = rg->getTxns();
	CPPUNIT_ASSERT(resources->size() == 2);
	CPPUNIT_ASSERT(txns->size() == 1);
	err = rg->reserveRange(1100, 3900, txn, mutex);

	// blocks, main thread releases

	CPPUNIT_ASSERT(err == 0);
	CPPUNIT_ASSERT(resources->size() == 3);
	it = resources->begin();
	CPPUNIT_ASSERT((*it)->start() == 1100);
	CPPUNIT_ASSERT((*it)->end() == 2000);
	it++;
	CPPUNIT_ASSERT((*it)->start() == 2001);
	CPPUNIT_ASSERT((*it)->end() == 3000);
	it++;
	CPPUNIT_ASSERT((*it)->start() == 3001);
	CPPUNIT_ASSERT((*it)->end() == 3900);
	CPPUNIT_ASSERT(txns->size() == 1);
	rg->releaseResources(txn);
	pthread_mutex_unlock(&mutex);

	return NULL;
}

class BRMTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(BRMTest);
CPPUNIT_TEST(masterSegmentTest_goodbad);
CPPUNIT_TEST(extentMap_good_1);
#ifdef PARTITIONING
CPPUNIT_TEST(extentMap_range_1);
#endif
CPPUNIT_TEST(extentMap_freelist);
// CPPUNIT_TEST(extentMap_overfill);
//CPPUNIT_TEST(many_ExtentMap_instances); //Jean unsuggested this case since writeengine use singleton
CPPUNIT_TEST(copyLocks_good_1);
CPPUNIT_TEST(vbbm_good_1);
CPPUNIT_TEST(vbbm_good_1);
CPPUNIT_TEST(vss_good_1);
CPPUNIT_TEST(vss_good_1);
//CPPUNIT_TEST(brm_extentmap_good_1);//fix for bug 640, this test case has a bug
#ifdef PARTITIONING
CPPUNIT_TEST(brm_extentmap_range_1);
#endif
CPPUNIT_TEST(brm_good_2);
CPPUNIT_TEST(brm_good_3);
CPPUNIT_TEST(brm_deleteOID);
CPPUNIT_TEST(brm_HWM);
CPPUNIT_TEST(resource_graph_1);
CPPUNIT_TEST(resource_graph_fragd_range);
CPPUNIT_TEST(resource_graph_deadlock);
CPPUNIT_TEST(resource_graph_overlapping_ranges);

CPPUNIT_TEST_SUITE_END();

private:
public:
	
	void masterSegmentTest_goodbad()
	{
		MasterSegmentTable mst;
		struct MSTEntry *mt;

// 		cerr << "MST goodbad" << endl;
		
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
                Config::deleteInstanceMap();
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
		
// 		cerr << "em_good_1" << endl;

		for (i = 1; i < iterations; i++) {
			em.createExtent(extentSize, i, lbids, allocdSize);
			em.confirmChanges();
			CPPUNIT_ASSERT(lbids.size() == 1);
			CPPUNIT_ASSERT((uint32_t)allocdSize == extentSize);
			CPPUNIT_ASSERT(lbids.back() == static_cast<LBID_t>((i-1)*extentSize));
		}
		
		em.checkConsistency();
		em.save(string("EMImage"));
		em.load(string("EMImage"));
		em.checkConsistency();
		
		for (i = 1; i < iterations; i++) {
			err = em.lookup(static_cast<LBID_t>((i-1)*extentSize), oid, fbo);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(oid == i);
			CPPUNIT_ASSERT(fbo == 0);
			if (i != 1) {
				err = em.lookup(static_cast<LBID_t>((i-1)*extentSize - 1), oid, fbo);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(oid == i-1);
				CPPUNIT_ASSERT(fbo == extentSize - 1);
			}
			if (i != iterations - 1) {
				err = em.lookup(static_cast<LBID_t>((i-1)*extentSize + 1), oid, fbo);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(oid == i);
				CPPUNIT_ASSERT(fbo == 1);
			}
		}
		
		em.checkConsistency();

		err = em.lookup(static_cast<LBID_t>(i*extentSize), oid, fbo);
		CPPUNIT_ASSERT(err == -1);
		
		for (i = 1; i < iterations; i++) {
			err = em.getBulkInsertVars(static_cast<LBID_t>((i-1)*extentSize),
									   hwm2, txnID);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm2 == 0);
			CPPUNIT_ASSERT(txnID == 0);
			err = em.setBulkInsertVars(static_cast<LBID_t>((i-1)*extentSize),
									   i, i + 1);
			em.confirmChanges();
			CPPUNIT_ASSERT(err == 0);
			err = em.getBulkInsertVars(static_cast<LBID_t>((i-1)*extentSize),
									   hwm2, txnID);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(hwm2 == static_cast<LBID_t>(i));
			CPPUNIT_ASSERT(txnID == static_cast<VER_t>(i+1));
			
			hwm = em.getHWM(i);
			CPPUNIT_ASSERT(hwm == 0);
			em.setHWM(i, ((uint32_t)i > (extentSize - 1) ? extentSize - 1 : i));
			em.confirmChanges();
			hwm = em.getHWM(i);
			CPPUNIT_ASSERT(hwm == static_cast<uint32_t>((uint32_t)i > extentSize-1 ? extentSize-1 : i));
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

		for (i = 1; i < iterations; i++) {
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
                Config::deleteInstanceMap();
	}

#ifdef PARTITIONING	
	void extentMap_range_1()
	{
		ExtentMap em;
		int i, err, iterations = 1300;  // (EM_INITIAL_SIZE + 3*EM_INCREMENT)
		int caughtException = 0, allocdSize;
		uint32_t hwm;
		BRM::HWM_t hwm2;
		BRM::VER_t txnID;
		vector<LBID_t> lbids;
		const uint32_t extentSize = em.getExtentSize();
		
		for (i = 1; i < iterations; i++) {
			em.createRangeExtent(extentSize, i, lbids, allocdSize, EMRangePartition_t(0, 10, true,true));
			em.confirmChanges();
			CPPUNIT_ASSERT(lbids.size() == 1);
			CPPUNIT_ASSERT((uint32_t)allocdSize == extentSize);
			CPPUNIT_ASSERT(lbids.back() == static_cast<LBID_t>((i-1)*extentSize));
		}
		
		em.checkConsistency();
		em.save(string("EMImage"));
		em.load(string("EMImage"));
		em.checkConsistency();
	
		LBIDRange_v lbidList;		
		for (i = 1; i < iterations; i++) {

			err = em.lookup(i, 1, lbidList);
			CPPUNIT_ASSERT(lbidList.size() == 1);
			CPPUNIT_ASSERT(err == lbidList.size());

			err = em.lookup(i, i+10, lbidList);
			CPPUNIT_ASSERT(lbidList.size() == 0);
			CPPUNIT_ASSERT(err == lbidList.size());

			err = em.lookup(i*10000, 1, lbidList);
			CPPUNIT_ASSERT(lbidList.size() == 0);
			CPPUNIT_ASSERT(err == lbidList.size());
		}
		
		em.checkConsistency();

		err = em.lookup(i, 99, lbidList);
		CPPUNIT_ASSERT(err == 0);
		CPPUNIT_ASSERT(lbidList.size() == 0);
		
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

		for (i = 1; i < iterations; i++) {
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
                Config::deleteInstanceMap();
	}
#endif

	void extentMap_freelist()
	{
		ExtentMap em;
		int i, allocdSize, iterations = 1400;  // (EM_INITIAL_SIZE + 4*EM_INCREMENT)
		vector<LBID_t> lbids;
		const int extentSize = em.getExtentSize();
		
// 		cerr << "em_freelist" << endl;

		for (i = 1; i < iterations; i++) {
			em.createExtent(extentSize, i, lbids, allocdSize);
			em.confirmChanges();
			CPPUNIT_ASSERT(lbids.back() == static_cast<LBID_t>((i-1)*extentSize));
		}		

		em.checkConsistency();
		
		//frag the lbid space to blow up the free list
		for (i = 1; i < iterations; i += 2) {
			em.deleteOID(i);
			em.confirmChanges();
		}
		
		em.checkConsistency();
		
		//fill in the holes
		for (i = 1; i < iterations; i += 2) {
			em.createExtent(extentSize, i, lbids, allocdSize);
			em.confirmChanges();
		}
		
		for (i = 1; i < iterations; i += 2) {
			em.deleteOID(i);
			em.confirmChanges();
		}
		
		for (i = 2; i < iterations; i += 2) {
			em.deleteOID(i);
			em.confirmChanges();
		}
	
		em.checkConsistency();
                Config::deleteInstanceMap();
	}
	
	/* This test verifies that the ExtentMap stops allocating space when it's full.
	(Bug #104)	It takes a long time to run with the default LBID 
	space characteristics so by default it's disabled.*/
	void extentMap_overfill()
	{
		int i, err, tmp;
		BlockResolutionManager brm;	
		vector<LBID_t> lbids;
		
		for (i = 1; i < 67109; i++) {
			err = brm.createExtent(1024000, i, lbids, tmp);
			CPPUNIT_ASSERT(err == 0);
		}
		err = brm.createExtent(1024000, i, lbids, tmp);
		CPPUNIT_ASSERT(err == -1);
		
		for (i = 1; i < 67109; i++) {
			err = brm.deleteOID(i);
			CPPUNIT_ASSERT(err == 0);
		}
		err = brm.deleteOID(i);
		CPPUNIT_ASSERT(err == -1);
                Config::deleteInstanceMap();
	}			   
				   
	void copyLocks_good_1()
	{
		CopyLocks cl;
		LBIDRange range;
		int i, iterations = 1000;
		
// 		cerr << "copylocks_good_1" << endl;

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
		
		cl.save(string("CLImage"));
		cl.load(string("CLImage"));

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
                Config::deleteInstanceMap();
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
		
// 		cerr << "brm_extentmap_good_1" << endl;

		for (i = 1; i < iterations; i++) {
			brm.createExtent(extentSize, i, lbids, allocdSize);
			CPPUNIT_ASSERT(lbids.back() == static_cast<LBID_t>((i-1)*extentSize));
		}

 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		brm.saveExtentMap(string("EMImage"));
		brm.loadExtentMap(string("EMImage"));
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		ExtentMap em;

		//int32_t seqNum=0;
		//int64_t max=0;
		//int64_t min=0;
		LBID_t lbid;
/*
		for (i = 1; i < iterations; i++) {
			lbid=(i-1)*extentSize;
			err=em.updateMaxMin(lbid, 1, 0, seqNum); // sequenceNum=0
			em.confirmChanges();
			CPPUNIT_ASSERT(err==0);

			max=-1;
			min=-1;
			err=em.getMaxMin(lbid, max, min, seqNum);
			CPPUNIT_ASSERT(max==1);
			CPPUNIT_ASSERT(min==0);
			CPPUNIT_ASSERT(err==2);			
			CPPUNIT_ASSERT(seqNum==0);

			err=em.updateMaxMin(lbid, 4, 2, seqNum); // sequenceNum=0
			em.confirmChanges();
			CPPUNIT_ASSERT(err==0);

			max=-1;
			min=-1;
			seqNum++;
			err=em.getMaxMin(lbid, max, min, seqNum);
			CPPUNIT_ASSERT(max==4);
			CPPUNIT_ASSERT(min==2);
			CPPUNIT_ASSERT(err==2);			
			CPPUNIT_ASSERT(seqNum==0);

			err=em.markInvalid(lbid); // sequenceNum=1, valid=1
			em.confirmChanges();
			CPPUNIT_ASSERT(err==0);

			seqNum=0; // re-init seqNum
			err=em.getMaxMin(lbid, max, min, seqNum);
			CPPUNIT_ASSERT(err==1);
			CPPUNIT_ASSERT(seqNum==1);

			seqNum=0;
		}
*/

 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

		vector<LBID_t> lbidVector;
		uint32_t offset;
		for(i=1; i < iterations; i++) {
			lbid = (i-1)*extentSize;
			for (int j=0; (uint32_t)j < extentSize;j++)
				lbidVector.push_back(lbid+j);
			err=em.markInvalid(lbidVector);
			em.confirmChanges();
			CPPUNIT_ASSERT(err==1);
			lbidVector.clear();
			em.lookup(lbid, oid, offset);
			err=em.markInvalid(oid);
			em.confirmChanges();
			CPPUNIT_ASSERT(err==0);
			//CPPUNIT_ASSERT(oid==i);
			oid=-1;
			offset=0;
		}

 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

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
		CPPUNIT_ASSERT(err == -1);
#endif

		for (i = 1; i < iterations; i++) {
			err = brm.deleteOID(i);
			CPPUNIT_ASSERT(err == 0);
		}

		err = brm.deleteOID(i);
		CPPUNIT_ASSERT(err == -1);
		
 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
                Config::deleteInstanceMap();
	}

#ifdef PARTITIONING	
	void brm_extentmap_range_1()
	{
		
		BlockResolutionManager brm;
		int i, 
			err, 
			oid, 
			allocdSize, 
			iterations = 1300;  // (EM_INITIAL_SIZE + 3*EM_INCREMENT)
		uint32_t fbo, hwm;
		vector<LBID_t> lbids;
		HWM_t hwm2;
		VER_t txnID;
		const uint32_t extentSize = brm.getExtentSize();
	    LBIDRange_v lbidList;
	
// 		cerr << "brm_extentmap_range_1" << endl;

		for (i = 1; i < iterations; i++) {
			brm.createRangeExtent(extentSize, i, lbids, allocdSize, 
				EMRangePartition_t(0, 9, true, true) );
			CPPUNIT_ASSERT(lbids.back() == static_cast<LBID_t>((i-1)*extentSize));
			CPPUNIT_ASSERT(lbids.size() == 1);
		}

 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		brm.saveExtentMap(string("EMImage"));
		brm.loadExtentMap(string("EMImage"));
		CPPUNIT_ASSERT(brm.checkConsistency() == 0);

		for (i = 1; i < iterations; i++) {

			err = brm.lookup(i, 1, lbidList);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(lbidList.size() == 1);
			
			err = brm.lookup(i, 10, lbidList);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(lbidList.size() == 0);
			
			err = brm.lookup(i*10000, 1, lbidList);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(lbidList.size() == 0);
			
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

		for (i = 1; i < iterations; i++) {
			err = brm.deleteOID(i);
			CPPUNIT_ASSERT(err == 0);
		}

		err = brm.deleteOID(i);
		CPPUNIT_ASSERT(err == -1);
 		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
                Config::deleteInstanceMap();
	}
#endif

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

// 		cerr << "brm_good_2" << endl;		

		CPPUNIT_ASSERT(brm.checkConsistency() == 0);
		
		oldsig = signal(SIGALRM, keepalive);
		alarm(290);
		
		err = brm.lookup(0, 0, false, oid, fbo);
		CPPUNIT_ASSERT(err == -1); 
		err = brm.lookup(0, 0, true, oid, fbo);
		CPPUNIT_ASSERT(err == -1);
		
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
		
		brm.saveState("TestImage");
		brm.loadState("TestImage");

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
		Config::deleteInstanceMap();
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
		
// 		cerr << "brm_good_3" << endl;

		oldsig = signal(SIGALRM, keepalive);
		alarm(290);
		
 		err = brm.lookup(0, 0, false, oid, fbo);
 		CPPUNIT_ASSERT(err == -1); 
		err = brm.lookup(0, 0, true, oid, fbo);
		CPPUNIT_ASSERT(err == -1);
		
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
                Config::deleteInstanceMap();
	}

	/* This test verifies that deleteOID returns an error for
	non-existant OIDs (bug #105) */
	void brm_deleteOID()
	{
		BlockResolutionManager brm;
		int err;
		vector<EMEntry> extents;
		
// 		cerr << "brm_deleteOID" << endl;

		err = brm.getExtents(1, extents);
#ifdef BRM_DEBUG
		if (err != -1)
			cerr << "Make sure OID 1 isn't allocated in the extent map" << endl;
		CPPUNIT_ASSERT(err == -1);
#else
		CPPUNIT_ASSERT(err == 0);
		CPPUNIT_ASSERT(extents.empty());
#endif
		err = brm.deleteOID(1);
		CPPUNIT_ASSERT(err == -1);
                Config::deleteInstanceMap();
	}
	
	/* This test verifies that setHWM and getHWM return an error for
	nonexistant OIDs (bugs #106, 107) */
	
	void brm_HWM()
	{
		BlockResolutionManager brm;
		int err;
		HWM_t hwm;
		vector<EMEntry> extents;
		
// 		cerr << "brm_HWM" << endl;

		err = brm.getExtents(1, extents);
#ifdef BRM_DEBUG
		if (err != -1)
			cerr << "Make sure OID 1 isn't allocated in the extent map" << endl;
		CPPUNIT_ASSERT(err == -1);
#else
		CPPUNIT_ASSERT(err == 0);
#endif
		CPPUNIT_ASSERT(extents.size() == 0);
		err = brm.setHWM(1, 10);
		CPPUNIT_ASSERT(err == -1);
		err = brm.getHWM(1, hwm);
		CPPUNIT_ASSERT(err == -1);
                Config::deleteInstanceMap();
	}

	void vbbm_good_1() 
	{
		
		// add some VBBM entries
		// do some lookups
		// manipulate them
		// do more lookups	
		
		VBBM vbbm;
		VSS vss;
		vector<VBRange> ranges;
		vector<VBRange>::iterator it;
		int i, iterations = 100010, err;
		OID_t oid;
		uint32_t fbo, fbo2;
		
		// Buildbot times out on the getBlocks() call during leakcheck b/c it takes
		// > 5 mins for some reason.  Have to ping it before 300 seconds go by.
		void (*oldsig)(int);
		
// 		cerr << "vbbm_good_1" << endl;

		oldsig = signal(SIGALRM, keepalive);
		alarm(290);
		
//  		cerr << "\nVBBM test (this can take awhile)\n";
		vbbm.lock(VBBM::READ);
		vbbm.release(VBBM::READ);
		vbbm.lock(VBBM::WRITE);
		vbbm.release(VBBM::WRITE);
		
		vbbm.lock(VBBM::WRITE);
		
		err = vbbm.lookup(0, 0, oid, fbo);
		CPPUNIT_ASSERT(err == -1); 

//  		cerr << "step 1/4: get some VB blocks" << endl;
		
		vss.lock(VSS::WRITE);
		vbbm.getBlocks(iterations, ranges, vss);
		vbbm.confirmChanges();
		vss.confirmChanges();
		vss.release(VSS::WRITE);
		CPPUNIT_ASSERT(!ranges.empty());	

//   		cerr << "step 2/4: inserts & lookups" << endl;
		for (it = ranges.begin(), i = 0; it != ranges.end(); it++) {
			for (fbo = (*it).vbFBO; fbo < (*it).vbFBO + (*it).size; fbo++, i++) {
				vbbm.insert(i, i+1, (*it).vbOID, fbo);
				vbbm.confirmChanges();
				err = vbbm.lookup(i, i+1, oid, fbo2);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(oid == (*it).vbOID);
				CPPUNIT_ASSERT(fbo2 == fbo);
			}
		}
				
		/*
			for (i = 0; i < iterations; i++) {
			
			vbbm.insert(i, i+1, i+2, i+3);
			err = vbbm.lookup(i, i+1, oid, fbo);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(oid == i + 2);
			CPPUNIT_ASSERT(fbo == static_cast<uint32_t>(i + 3));
		}
		*/

  		vbbm.save(string("VBBMImage"));
  		vbbm.load(string("VBBMImage"));	

//   		cerr << "step 3/4: lookups" << endl;
		for (it = ranges.begin(), i = 0; it != ranges.end(); it++) {
			for (fbo = (*it).vbFBO; fbo < (*it).vbFBO + (*it).size; fbo++, i++) {
				err = vbbm.lookup(i, i+1, oid, fbo2);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(oid == (*it).vbOID);
				CPPUNIT_ASSERT(fbo2 == fbo);
			}
		}
		
		/*
		for (i = 0; i < iterations; i++) {
			err = vbbm.lookup(i, i+1, oid, fbo);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(oid == i + 2);
			CPPUNIT_ASSERT(fbo == static_cast<uint32_t>(i + 3));
	} */
		
//   		cerr << "step 4/4: remove all entries" << endl;
		for (i = 0; i < iterations; i++) {
			vbbm.removeEntry(i, i+1);		
			vbbm.confirmChanges();
		}
		
		CPPUNIT_ASSERT(vbbm.size() == 0);
		CPPUNIT_ASSERT(vbbm.hashEmpty());
		
		vbbm.release(VBBM::WRITE);
                Config::deleteInstanceMap();
	}

	void vss_good_1() 
	{
		VSS vss;
		VBBM vbbm;
		ExtentMap em;
 		int i, err, iterations = 100010;
		VER_t verID;
		bool vbFlag;
		vector<LBID_t> lbids;
		LBIDRange range;
		
		// Buildbot times out on the getBlocks() call during leakcheck b/c it takes
		// > 5 mins for some reason.  Have to ping it before 300 seconds go by.
		void (*oldsig)(int);
		
// 		cerr << "vss_good_1" << endl;

 		oldsig = signal(SIGALRM, keepalive);
 		alarm(290);
		
		cerr << endl << "VSS test (this can take awhile)" << endl;
		
		vss.lock(VSS::READ);
		vss.release(VSS::READ);
		
		vss.lock(VSS::WRITE);
		
		cerr << "step 1/5: insert 2 entries for 100010 LBIDs & test lookup logic" << endl;
		
		range.start = 0;
		range.size = 200000;
		
		CPPUNIT_ASSERT(!vss.isLocked(range));
		
		for (i = 0; i < iterations; i++) {
			vss.insert(i, i+1, (i % 2 ? true : false), (i % 2 ? false : true));
			vss.insert(i, i, true, false);
			vss.confirmChanges();
			if (i == 0) {
				range.start = 0;
				range.size = 1;
 				CPPUNIT_ASSERT(vss.isLocked(range));
			}
			else if (i == 1) {
				range.start = 1;
				CPPUNIT_ASSERT(!vss.isLocked(range));
			}
			verID = i + 10;
			err = vss.lookup(i, verID, i + 1, vbFlag);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(verID == i + 1);
			CPPUNIT_ASSERT((vbFlag && (i % 2)) || (!vbFlag && !(i % 2)));
			verID = i + 10;
			err = vss.lookup(i, verID, 0, vbFlag);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(verID == (i % 2 ? i+1 : i));
			CPPUNIT_ASSERT(vbFlag == true);
			if (i > 0) {
				verID = i - 1;
				err = vss.lookup(i, verID, 0, vbFlag);
				CPPUNIT_ASSERT(err == -1);
			}
			
		}
		
		range.start = 0;
		range.size = 200000;
		CPPUNIT_ASSERT(vss.isLocked(range));
		vss.save(string("VSSImage"));
		vss.load(string("VSSImage"));		

		// this loop actually breaks the locked -> !vbFlag invariant
		// switch it back afterward
		cerr << "step 2/5: flip the vbFlag on half of the entries & test lookup logic" << endl;
		for (i = 0; i < iterations; i++) {
			vss.setVBFlag(i, i + 1, (i % 2 ? false : true));
			vss.confirmChanges();
		}
		
		for (i = 0; i < iterations; i++) {
			verID = i + 10;
			err = vss.lookup(i, verID, i + 1, vbFlag);
			CPPUNIT_ASSERT(err == 0);
			CPPUNIT_ASSERT(verID == i + 1);
			CPPUNIT_ASSERT((!vbFlag && (i % 2)) || (vbFlag && !(i % 2)));
		}
		
		cerr << "step 3/5: some clean up" << endl;
		for (i = 0; i < iterations; i++) {
			vss.setVBFlag(i, i + 1, (i % 2 ? true : false));
			vss.confirmChanges();
		}
		
		
		// there are 2 entries for every txnid at this point, one
		// which is locked, one which isn't.  That's technically a violation,
		// so we have to get rid of the unlocked ones for the next test.
		for (i = 0; i < iterations; i++) {
			vss.removeEntry(i, i);
			vss.confirmChanges();
		}
		
		//speed up the next step!
 	  	for (i = iterations/50; i < iterations; i++) {
  			vss.removeEntry(i, i+1);
			vss.confirmChanges();
		}
		
		cerr << "step 4/5: get \'uncommitted\' LBIDs, commit, and test lookup logic" << endl;
		for (i = 1; i < iterations/50; i++) {
			bool ex = false;

			if (i % 2 == 0) {
#ifdef BRM_DEBUG
				try {	
					vss.getUncommittedLBIDs(i, lbids);
				}
				catch (logic_error &e) { 
					ex = true;
				}
				CPPUNIT_ASSERT(ex == true);
#endif	
			}
			else {
				LBID_t lbid;

				vss.getUncommittedLBIDs(i, lbids);
  				CPPUNIT_ASSERT(lbids.size() == 1);
				lbid = *(lbids.begin());
				CPPUNIT_ASSERT(lbid == i - 1);
				verID = i + 10;
				err = vss.lookup(i-1, verID, i, vbFlag);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(verID == i);
				CPPUNIT_ASSERT(vbFlag == false);
				
				err = vss.lookup(i-1, verID, 0, vbFlag);
				CPPUNIT_ASSERT(err == -1);
				
				vss.commit(i);
				vss.confirmChanges();
				verID = i + 10;
				err = vss.lookup(i-1, verID, 0, vbFlag);
				CPPUNIT_ASSERT(err == 0);
				CPPUNIT_ASSERT(verID == i);
				CPPUNIT_ASSERT(vbFlag == false);
				
			}
		}
		
		cerr << "step 5/5: final clean up" << endl;
	
		CPPUNIT_ASSERT(vss.size() == iterations/50);	
		range.start = 0;
		range.size = iterations/50;
		vss.removeEntriesFromDB(range, vbbm, false);
		vbbm.confirmChanges();
		vss.confirmChanges();

		// the new logic for removeEntriesFromDB deletes all entries in the range, 
		// locked or not, in the VB or not

// 		CPPUNIT_ASSERT(vss.size() == iterations/100);
		
//  		for (i = 1; i < iterations/50; i+=2)
//  			vss.removeEntry(i, i+1);
		
		CPPUNIT_ASSERT(vss.size() == 0);
		CPPUNIT_ASSERT(vss.hashEmpty());
		vss.release(VSS::WRITE);
		
 		alarm(0);
 		signal(SIGALRM, oldsig);
                Config::deleteInstanceMap();
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
		
//  		cerr << endl << "Extent Map instance test" << endl;
		
		ems = new ExtentMap*[2000];
		for (i = 0; i < 2000; i++) 
			ems[i] = new ExtentMap();

		extentSize = ems[0]->getExtentSize();
// 		cerr << "  - Successfully created 2000 instances, starting the freelist test" 
// 				<< endl;
		for (i = 1; i < iterations; i++) {
			ems[i]->createExtent(extentSize, i, lbids, allocdSize);
			ems[i]->confirmChanges();
 			CPPUNIT_ASSERT(lbids.back() == static_cast<LBID_t>((i-1)*extentSize));
		}
		
		//frag the lbid space to blow up the free list
		for (i = 1; i < iterations; i += 2) {
			ems[i]->deleteOID(i);
			ems[i]->confirmChanges();
		}
		
		//fill in the holes
		for (i = 1; i < iterations; i += 2) {
			ems[i]->createExtent(extentSize, i, lbids, allocdSize);
			ems[i]->confirmChanges();
		}
	
		for (i = 1; i < iterations; i += 2) {
			ems[i]->deleteOID(i);
			ems[i]->confirmChanges();
		}
		
		for (i = 2; i < iterations; i += 2) {
			ems[i]->deleteOID(i);
			ems[i]->confirmChanges();
		}
		
// 		cerr << "done.  sleeping for 30 seconds." << endl;
// 		sleep(30);
		
		for (i = 0; i < 2000; i++)
			delete ems[i];
		delete [] ems;
                Config::deleteInstanceMap();
		
	}
	
void resource_graph_1()
{
	LBIDResourceGraph rg;
	pthread_t t;
	VER_t txn = 1;
	int err;

	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_lock(&mutex);

	err = rg.reserveRange(1001, 2000, txn, mutex);
	CPPUNIT_ASSERT(err == 0);
	err = rg.reserveRange(2001, 2000, txn, mutex);
	CPPUNIT_ASSERT(err == 0);
	err = rg.reserveRange(0, 1000, txn, mutex);
	CPPUNIT_ASSERT(err == 0);
	err = rg.reserveRange(3001, 2000, txn, mutex);
	CPPUNIT_ASSERT(err == 0);
	pthread_create(&t, NULL, dummy_1, &rg);
	pthread_mutex_unlock(&mutex);
	sleep(5);  
	
	// thread should sleep here
	
	pthread_mutex_lock(&mutex);
	rg.releaseResources(txn);
	pthread_mutex_unlock(&mutex);

	// thread should be unblocked and finish here

	pthread_join(t, NULL);
	pthread_mutex_lock(&mutex);
	err = rg.reserveRange(0, 1000, txn, mutex);
	CPPUNIT_ASSERT(err == 0);
	rg.releaseResources(txn);
	pthread_mutex_unlock(&mutex);
	pthread_mutex_destroy(&mutex);
        Config::deleteInstanceMap();
}

void resource_graph_deadlock()
{
	LBIDResourceGraph rg;
	pthread_t t;
	VER_t txn = 1;
	int err;

	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_lock(&mutex);
	err = rg.reserveRange(0, 1000, txn, mutex);
	CPPUNIT_ASSERT(err == 0);
	pthread_create(&t, NULL, dummy_deadlock, &rg);
	pthread_mutex_unlock(&mutex);
	sleep(1);

	// thread grabs 1001-2000, then 0-1000 and blocks

	pthread_mutex_lock(&mutex);
	err = rg.reserveRange(1001, 2000, txn, mutex);  // deadlock
	CPPUNIT_ASSERT(err == ERR_DEADLOCK);	
	rg.releaseResources(txn);

	pthread_mutex_unlock(&mutex);  // should wake the thread
	pthread_join(t, NULL);		// wait for it to finish

	pthread_mutex_lock(&mutex);
	err = rg.reserveRange(0, 2000, txn, mutex);  //verify we can now grab both ranges
	CPPUNIT_ASSERT(err == ERR_OK);
	pthread_mutex_unlock(&mutex);
	pthread_mutex_destroy(&mutex);
        Config::deleteInstanceMap();
}

void resource_graph_fragd_range()
{
	LBIDResourceGraph rg;
	set<ResourceNode *, RNLess<ResourceNode *> > *resources;
	map<VER_t, TransactionNode *> *txns;
	pthread_t t;
	VER_t txn = 1;
	int err;

	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_lock(&mutex);

	err = rg.reserveRange(1001, 2000, txn, mutex);
	CPPUNIT_ASSERT(err == ERR_OK);
	err = rg.reserveRange(3001, 4000, txn, mutex);
	CPPUNIT_ASSERT(err == ERR_OK);
	pthread_create(&t, NULL, fragd_range_helper, &rg);
	pthread_mutex_unlock(&mutex);
	sleep(1);
	
	// thread wakes, tries to grab 0-5000
	// check that there are 5 resource nodes

	pthread_mutex_lock(&mutex);
	resources = rg.getResources();
	txns = rg.getTxns();
	CPPUNIT_ASSERT(resources->size() == 5);
	CPPUNIT_ASSERT(txns->size() == 2);
	rg.releaseResources(txn);
	CPPUNIT_ASSERT(resources->size() == 3);
	CPPUNIT_ASSERT(txns->size() == 1);
	pthread_mutex_unlock(&mutex);

	// thread releases, exits
	
	pthread_join(t, NULL);
	CPPUNIT_ASSERT(resources->size() == 0);
	CPPUNIT_ASSERT(txns->size() == 0);

	pthread_mutex_destroy(&mutex);
        Config::deleteInstanceMap();
}

void resource_graph_overlapping_ranges()
{
	LBIDResourceGraph rg;
	set<ResourceNode *, RNLess<ResourceNode *> > *resources;
	set<ResourceNode *, RNLess<ResourceNode *> >::const_iterator it;
	map<VER_t, TransactionNode *> *txns;
	pthread_t t;
	VER_t txn = 1;
	int err;

	// make a fragmented range, then request a range that overlaps existing
	// ranges on both ends

	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_lock(&mutex);

	err = rg.reserveRange(1001, 2000, txn, mutex);
	CPPUNIT_ASSERT(err == ERR_OK);
	err = rg.reserveRange(3001, 4000, txn, mutex);
	CPPUNIT_ASSERT(err == ERR_OK);
	pthread_create(&t, NULL, overlapping_range_helper, &rg);
	pthread_mutex_unlock(&mutex);
	sleep(1);

	// thread executes, tries to grab 1100-3900

	pthread_mutex_lock(&mutex);
	resources = rg.getResources();
	txns = rg.getTxns();

#ifdef BRM_VERBOSE
	cerr << " resources->size = " << resources->size() << endl;
	for (it = resources->begin(); it != resources->end(); it++)
		cerr << "  " << (*it)->start() << "-" << (*it)->end() << endl;
#endif
	
	it = resources->begin();
 	CPPUNIT_ASSERT(resources->size() == 3);
	CPPUNIT_ASSERT((*it)->start() == 1001);
	CPPUNIT_ASSERT((*it)->end() == 2000);
	it++;
	CPPUNIT_ASSERT((*it)->start() == 2001);
	CPPUNIT_ASSERT((*it)->end() == 3000);
	it++;
	CPPUNIT_ASSERT((*it)->start() == 3001);
	CPPUNIT_ASSERT((*it)->end() == 4000);

	CPPUNIT_ASSERT(txns->size() == 2);
	rg.releaseResources(txn);
	CPPUNIT_ASSERT(txns->size() == 1);
	pthread_mutex_unlock(&mutex);

	pthread_join(t, NULL);
	CPPUNIT_ASSERT(resources->size() == 0);
	CPPUNIT_ASSERT(txns->size() == 0);

	pthread_mutex_destroy(&mutex);
        Config::deleteInstanceMap();
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
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


