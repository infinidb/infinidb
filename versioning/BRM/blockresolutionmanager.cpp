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
 * $Id: blockresolutionmanager.cpp 1478 2012-01-10 20:30:18Z pleblanc $
 *
 ****************************************************************************/

#include <iostream>
#include <sys/types.h>
#include <vector>
#ifdef __linux__
#include <values.h>
#endif
#include <limits>
#include <sys/stat.h>

#include "brmtypes.h"
#include "rwlock.h"
#include "mastersegmenttable.h"
#include "extentmap.h"
#include "copylocks.h"
#include "vss.h"
#include "vbbm.h"
#include "exceptclasses.h"
#include "slavecomm.h"
#define BLOCKRESOLUTIONMANAGER_DLLEXPORT
#include "blockresolutionmanager.h"
#undef BLOCKRESOLUTIONMANAGER_DLLEXPORT

#ifdef _MSC_VER
#define umask _umask
#endif

using namespace logging;

using namespace std;

namespace BRM {

BlockResolutionManager::BlockResolutionManager(bool ronly) throw()
{
 	if (ronly) {
		em.setReadOnly();
		vss.setReadOnly();
		vbbm.setReadOnly();
		copylocks.setReadOnly();
 	}
}

BlockResolutionManager::BlockResolutionManager(const BlockResolutionManager& brm)
{
	throw logic_error("BRM: Don't use the copy constructor.");
}

BlockResolutionManager::~BlockResolutionManager() throw()
{
}

BlockResolutionManager& BlockResolutionManager::operator=(const BlockResolutionManager& brm)
{
	throw logic_error("BRM: Don't use the = operator.");
}

int BlockResolutionManager::lookup(OID_t oid, LBIDRange_v& lbidList) throw()
{
	
	try {
		em.lookup(oid, lbidList);
		return 0;
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return -1;
	}
}

int BlockResolutionManager::vssLookup(LBID_t lbid, VER_t& verID, VER_t txnID, 
									  bool& vbFlag, bool vbOnly) throw()
{
	try {
		return vss.lookup(lbid, verID, txnID, vbFlag, vbOnly);
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return -1;
	}
}

// Casual partitioning support
//
int BlockResolutionManager::markExtentInvalid(const LBID_t lbid)
{
	int err=0;

	try {
		err=em.markInvalid(lbid);
		em.confirmChanges();
	} catch (exception& e) {
		cerr << e.what() << endl;
		em.undoChanges();
		return -1;
	}

	return err;
	
}

int BlockResolutionManager::setExtentMaxMin(const LBID_t lbid,
											const int64_t max,
											const int64_t min,
											const int32_t seqNum)
{
	int err=0;

	try {
		err=em.setMaxMin(lbid, max, min, seqNum, false);
		em.confirmChanges();
	} catch (exception& e) {
		cerr << e.what() << endl;
		em.undoChanges();
		return -1;
	}

	return err;
	
}

int BlockResolutionManager::getExtentMaxMin(const LBID_t lbid,
										int64_t& max,
										int64_t& min,
										int32_t& seqNum)
{
	int err=0;

	try {
		err=em.getMaxMin(lbid, max, min, seqNum);
		em.confirmChanges();
	} catch (exception& e) {
		cerr << e.what() << endl;
		em.undoChanges();
		return -1;
	}

	return err;

}

int BlockResolutionManager::deleteOID(OID_t oid) throw()
{
	bool locked[2] = {false, false};
	LBIDRange_v lbids;
	LBIDRange_v::iterator it;
	int err;
	
	try {
 		vbbm.lock(VBBM::WRITE);
 		locked[0] = true;
		vss.lock(VSS::WRITE);
		locked[1] = true;
		
		err = lookup(oid, lbids);
		if (err == -1 || lbids.empty()) {
			vss.release(VSS::WRITE);
			locked[1] = false;
			vbbm.release(VBBM::WRITE);
			locked[0] = false;
			return -1;
		}
		for (it = lbids.begin(); it != lbids.end(); it++)
			vss.removeEntriesFromDB(*it, vbbm);
		
		em.deleteOID(oid);
		vss.confirmChanges();
		vbbm.confirmChanges();
		em.confirmChanges();
		vss.release(VSS::WRITE);
		locked[1] = false;
 		vbbm.release(VBBM::WRITE);
 		locked[0] = false;
	}
	catch (exception& e) {
		vss.undoChanges();
		vbbm.undoChanges();
		em.undoChanges();
		if (locked[1])
			vss.release(VSS::WRITE);
 		if (locked[0])
 			vbbm.release(VBBM::WRITE);
		
		cerr << e.what() << endl;
		return -1;
	}
	
	return 0;
}
	
int BlockResolutionManager::getExtentRows() throw()
{
	return em.getExtentRows();
}

int BlockResolutionManager::writeVBEntry(VER_t transID, LBID_t lbid, OID_t vbOID,
										 u_int32_t vbFBO) throw()
{
	bool locked[2] = {false, false}, vbFlag;
	VER_t oldVerID;
	int err;
	
	try {
		vbbm.lock(VBBM::WRITE);
		locked[0] = true;
		vss.lock(VSS::WRITE);
		locked[1] = true;
	
		//figure out the current version of the block
		oldVerID = numeric_limits<VER_t>::max();
		err = vss.lookup(lbid, oldVerID, transID, vbFlag);

		if (err == -1)
			oldVerID = 0;
		else if (oldVerID == transID)
			goto out;
#ifdef BRM_DEBUG
		else if (vbFlag) {
//			oldVerID = transID - 1;
			cerr << "BRM::writeVBEntry(): found the most recent version of LBID " << 
				lbid << " in the version buffer not the main DB" << endl;
			throw logic_error("BRM::writeVBEntry(): VBBM contains the most recent version of an LBID");
		}
#endif
	
		vbbm.insert(lbid, oldVerID, vbOID, vbFBO);
		if (err != -1)
			vss.setVBFlag(lbid, oldVerID, true);
		else
			vss.insert(lbid, oldVerID, true, false);

		// XXXPAT:  There's a problem if we use transID as the new version here.  
		// Need to use at least oldVerID + 1.  OldverID can be > TransID
		vss.insert(lbid, transID, false, true);
		
		vss.confirmChanges();
		vbbm.confirmChanges();
out:
		vbbm.release(VBBM::WRITE);
		locked[1] = false;
		vss.release(VSS::WRITE);
		locked[0] = false;
	}
	catch (exception &e) {
		vss.undoChanges();
		vbbm.undoChanges();
		if (locked[1])
			vss.release(VSS::WRITE);
		if (locked[0])
			vbbm.release(VBBM::WRITE);
		
		cerr << e.what() << endl;
		return -1;
	}
	
	return 0;
}
		
int BlockResolutionManager::getUncommittedLBIDs(VER_t transID, 
		vector<LBID_t>& lbidList) throw()
{
	bool locked = false;
	
	try {
		vss.lock(VSS::READ);
		locked = true;
		
		vss.getUncommittedLBIDs(transID, lbidList);
		
		vss.confirmChanges();
		vss.release(VSS::READ);
		locked = false;
		return 0;
	}
	catch (exception &e) {
		vss.undoChanges();
		if (locked)
			vss.release(VSS::READ);
		cerr << e.what() << endl;
		return -1;
	}
}
		
int BlockResolutionManager::beginVBCopy(VER_t transID, const LBIDRange_v& ranges,
									  VBRange_v& freeList) throw()
{
	LBIDRange_v::const_iterator it, it2;
	int sum = 0, ret = 0;
	bool locked[3] = {false, false, false};

#ifdef BRM_DEBUG
	if (transID < 1) {
		cerr << "BlockResolutionManager::beginVBCopy(): transID must be > 0" << endl;
		return -1;
	}
#endif
	
	try {
		vbbm.lock(VBBM::WRITE);
		locked[0] = true;
		vss.lock(VSS::WRITE);
		locked[1] = true;
		copylocks.lock(CopyLocks::WRITE);
		locked[2] = true;
	
		for (it = ranges.begin(); it != ranges.end(); it++) {
			if (vss.isLocked(*it, transID) || copylocks.isLocked(*it)) {
				ret = -1;
				goto out;	
			}
			sum += (*it).size;
		}

		for (it = ranges.begin(); it != ranges.end(); it++)
			copylocks.lockRange(*it, transID);
		vbbm.getBlocks(sum, freeList, vss);

		copylocks.confirmChanges();
		vbbm.confirmChanges();
out:
		copylocks.release(CopyLocks::WRITE);
		locked[2] = false;
		vss.release(VSS::WRITE);
		locked[1] = false;
		vbbm.release(VBBM::WRITE);
		locked[0] = false;
		return ret;
	}
	catch(const VBBMBufferOverFlowExcept &e) {
		copylocks.undoChanges();
		vbbm.undoChanges();
		
		if (locked[2])
			copylocks.release(CopyLocks::WRITE);
		if (locked[1])
			vss.release(VSS::WRITE);
		if (locked[0])
			vbbm.release(VBBM::WRITE);
		
		cerr << e.what() << endl;
		return e.errorCode();
	}
	catch(exception &e) {
		copylocks.undoChanges();
		vbbm.undoChanges();
		
		if (locked[2])
			copylocks.release(CopyLocks::WRITE);
		if (locked[1])
			vss.release(VSS::WRITE);
		if (locked[0])
			vbbm.release(VBBM::WRITE);
		
		cerr << e.what() << endl;
		return -1;
	}
}

		
int BlockResolutionManager::endVBCopy(VER_t transID, const LBIDRange_v& ranges)
		throw()
{
	LBIDRange_v::const_iterator it;
	bool locked = false;
	
	try {
		copylocks.lock(CopyLocks::WRITE);
		locked = true;
		
		for (it = ranges.begin(); it != ranges.end(); it++)
			copylocks.releaseRange(*it);
		copylocks.confirmChanges();

		copylocks.release(CopyLocks::WRITE);
		locked = false;
		return 0;
	}
	catch (exception &e) {
		copylocks.undoChanges();
		if (locked)
			copylocks.release(CopyLocks::WRITE);
		cerr << e.what() << endl;
		return -1;
	}
}
		
int BlockResolutionManager::vbCommit(VER_t transID) throw()
{
	
	bool locked = false;
	
	try {
		vss.lock(VSS::WRITE);
		locked = true;
		
		vss.commit(transID);
		vss.confirmChanges();		

		vss.release(VSS::WRITE);
		locked = false;
		return 0;
	}
	catch (exception &e) {
		vss.undoChanges();
		if (locked)
			vss.release(VSS::WRITE);
		cerr << e.what() << endl;
		return -1;
	}
}

/* XXXPAT: This fcn needs to be changed s.t. if it returns -1, there is some concrete definition
   of the state of each shared structure. */
int BlockResolutionManager::vbRollback(VER_t transID, const LBIDRange_v& lbidList)
		throw()
{
	bool locked[3] = {false, false, false};
	LBIDRange_v::const_iterator it;
	LBID_t lbid;
	VER_t oldVerID;
	int err;
	bool vbFlag;

#ifdef BRM_DEBUG
	if (transID < 1) {
		cerr << "BlockResolutionManager::vbRollback(): transID must be > 0" << endl;
		return -1;
	}
#endif

	try {
		vbbm.lock(VBBM::WRITE);
		locked[0] = true;
		vss.lock(VSS::WRITE);
		locked[1] = true;
		copylocks.lock(CopyLocks::WRITE);
		locked[2] = true;
	
		copylocks.rollback(transID);
		for (it = lbidList.begin(); it != lbidList.end(); it++) {
			for (lbid = (*it).start; lbid < (*it).start + (*it).size; lbid++) {
// 				oldVerID = numeric_limits<VER_t>::max();
				oldVerID = transID;
				err = vss.lookup(lbid, oldVerID, 0, vbFlag, true);
				if (err == -1)
					oldVerID = 0;
				
				vss.removeEntry(lbid, transID);
				if (vbFlag)
					vbbm.removeEntry(lbid, oldVerID);
				vss.setVBFlag(lbid, oldVerID, false);
			}
		}
	
		copylocks.confirmChanges();
		vss.confirmChanges();
		vbbm.confirmChanges();
		copylocks.release(CopyLocks::WRITE);
		locked[2] = false;
		vss.release(VSS::WRITE);
		locked[1] = false;
		vbbm.release(VBBM::WRITE);
		locked[0] = false;
		return 0;
	}
	catch (exception &e) {
		copylocks.undoChanges();
		vss.undoChanges();
		vbbm.undoChanges();
		if (locked[2])
			copylocks.release(CopyLocks::WRITE);
		if (locked[1])
			vss.release(VSS::WRITE);
		if (locked[0])
			vbbm.release(VBBM::WRITE);
		
		cerr << e.what() << endl;
		return -1;
	}
}

int BlockResolutionManager::vbRollback(VER_t transID, const vector<LBID_t>& lbidList)
		throw()
{
	bool locked[3] = {false, false, false};
	vector<LBID_t>::const_iterator it;
	VER_t oldVerID;
	int err;
	bool vbFlag;

#ifdef BRM_DEBUG
	if (transID < 1) {
		cerr << "BlockResolutionManager::vbRollback(): transID must be > 0" << endl;
		return -1;
	}
#endif

	try {
		vbbm.lock(VBBM::WRITE);
		locked[0] = true;
		vss.lock(VSS::WRITE);
		locked[1] = true;
		copylocks.lock(CopyLocks::WRITE);
		locked[2] = true;
	
		copylocks.rollback(transID);
		for (it = lbidList.begin(); it != lbidList.end(); it++) {
// 			oldVerID = numeric_limits<VER_t>::max();
			oldVerID = transID;
			err = vss.lookup(*it, oldVerID, 0, vbFlag, true);
			if (err == -1)
				oldVerID = 0;
			
			vss.removeEntry(*it, transID);
			if (vbFlag)
				vbbm.removeEntry(*it, oldVerID);
			vss.setVBFlag(*it, oldVerID, false);
		}
	
		copylocks.confirmChanges();
		vss.confirmChanges();
		vbbm.confirmChanges();
		copylocks.release(CopyLocks::WRITE);
		locked[2] = false;
		vss.release(VSS::WRITE);
		locked[1] = false;
		vbbm.release(VBBM::WRITE);
		locked[0] = false;
		return 0;
	}
	catch (exception &e) {
		copylocks.undoChanges();
		vss.undoChanges();
		vbbm.undoChanges();
		if (locked[2])
			copylocks.release(CopyLocks::WRITE);
		if (locked[1])
			vss.release(VSS::WRITE);
		if (locked[0])
			vbbm.release(VBBM::WRITE);
		
		cerr << e.what() << endl;
		return -1;
	}
}


int BlockResolutionManager::checkConsistency() throw()
{
	bool locked[2] = {false, false};
	
	try {
		em.checkConsistency();
	}
	catch (exception &e) {
		cerr << e.what() << endl;
		return -1;
	}		
	
	try {
		vbbm.lock(VBBM::READ);
		locked[0] = true;
		vss.lock(VSS::READ);
		locked[1] = true;
		vss.checkConsistency(vbbm, em);
		vss.release(VSS::READ);
		locked[1] = false;
		vbbm.release(VBBM::READ);
		locked[0] = false;
	}
	catch (exception &e) {
		cerr << e.what() << endl;
		if (locked[1])
			vss.release(VSS::READ);
		if (locked[0])
			vbbm.release(VBBM::READ);
		return -1;
	}
		
	try {
		vbbm.lock(VBBM::READ);
		vbbm.checkConsistency();
		vbbm.release(VBBM::READ);
	}
	catch (exception &e) {
		cerr << e.what() << endl;
		vbbm.release(VBBM::READ);
		return -1;
	}
			
	return 0;
}

int BlockResolutionManager::loadExtentMap(const string &filename, bool fixFL)
{
	em.load(filename, fixFL);
	return 0;
}

int BlockResolutionManager::saveExtentMap(const string &filename)
{
	em.save(filename);
	return 0;
}

int BlockResolutionManager::getCurrentTxnIDs(set<VER_t> &txnList) throw()
{

	bool locked[2] = { false, false };

	try {
		txnList.clear();
		vss.lock(VSS::READ);
		locked[0] = true;
		copylocks.lock(CopyLocks::READ);
		locked[1] = true;
		copylocks.getCurrentTxnIDs(txnList);
		vss.getCurrentTxnIDs(txnList);
		copylocks.release(CopyLocks::READ);
		locked[1] = false;
		vss.release(VSS::READ);
		locked[0] = false;
	}
	catch (exception &e) {
		if (locked[1])
			copylocks.release(CopyLocks::READ);
		if (locked[0])
			vss.release(VSS::READ);
		cerr << e.what() << endl;
		return -1;
	}

	return 0;
}

int BlockResolutionManager::saveState(string filename) throw()
{
	string emFilename = filename + "_em";
	string vssFilename = filename + "_vss";
	string vbbmFilename = filename + "_vbbm";
	string journalFilename = filename + "_journal";

	bool locked[2] = { false, false };
	ofstream journal;	

	try {
		vbbm.lock(VBBM::READ);
		locked[0] = true;
		vss.lock(VSS::READ);
		locked[1] = true;

		saveExtentMap(emFilename);
		uint utmp = ::umask(0);
		journal.open(journalFilename.c_str(), ios_base::out | ios_base::trunc | ios_base::binary);
		journal.close();
		::umask(utmp);
		vbbm.save(vbbmFilename);
		vss.save(vssFilename);

		vss.release(VSS::READ);
		locked[1] = false;
		vbbm.release(VBBM::READ);
		locked[0] = false;
	}
	catch (exception &e) {
		if (locked[1])
			vss.release(VSS::READ);
		if (locked[0])
			vbbm.release(VBBM::READ);
		cout << e.what() << endl;
		return -1;
	}

	return 0;
}

int BlockResolutionManager::loadState(string filename, bool fixFL) throw()
{
	string emFilename = filename + "_em";
	string vssFilename = filename + "_vss";
	string vbbmFilename = filename + "_vbbm";
	bool locked[2] = { false, false};

	try {
		vbbm.lock(VBBM::WRITE);
		locked[0] = true;
		vss.lock(VSS::WRITE);
		locked[1] = true;

		loadExtentMap(emFilename, fixFL);
		vbbm.load(vbbmFilename);
		vss.load(vssFilename);

		vss.release(VSS::WRITE);
		locked[1] = false;
		vbbm.release(VBBM::WRITE);
		locked[0] = false;
	}
	catch (exception &e) {
		if (locked[1])
			vss.release(VSS::WRITE);
		if (locked[0])
			vbbm.release(VBBM::WRITE);
		cout << e.what() << endl;
		return -1;
	}
	return 0;
}

int BlockResolutionManager::replayJournal(string prefix) throw()
{
	SlaveComm sc;
	int err = -1;

	try {
		err = sc.replayJournal(prefix);
	}
	catch(exception &e) {
		cout << e.what();
	}
	return err;
}


}   //namespace

