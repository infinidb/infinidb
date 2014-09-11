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
 * $Id: sessionmanagerserver.cpp 1546 2012-04-03 18:32:59Z dcathey $
 *
 ****************************************************************************/

/*
 * This class issues Transaction ID and keeps track of the current version ID
 */
 
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cerrno>
#include <unistd.h>

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/sync/named_semaphore.hpp>
#include <boost/version.hpp>
namespace bi=boost::interprocess;

#include <iostream>
#include <fstream>
#include <string>
#include <stdexcept>
#include <ios>
#include <limits>
#ifdef _MSC_VER
#include <io.h>
#include <psapi.h>
#endif

#include "brmtypes.h"
#include "calpontsystemcatalog.h"
#include "configcpp.h"
#define SESSIONMANAGERSERVER_DLLEXPORT
#include "sessionmanagerserver.h"
#undef SESSIONMANAGERSERVER_DLLEXPORT

#ifndef O_BINARY
#  define O_BINARY 0
#endif
#ifndef O_DIRECT
#  define O_DIRECT 0
#endif
#ifndef O_LARGEFILE
#  define O_LARGEFILE 0
#endif
#ifndef O_NOATIME
#  define O_NOATIME 0
#endif

using namespace std;

namespace BRM {

SessionManagerServer::SessionManagerServer() : unique32(0)
#ifdef _MSC_VER
	, fPids(0), fMaxPids(64)
#endif
{	
	config::Config* conf;
	//int err;
	int madeSems;
	string stmp;
	const char *ctmp;
	
	conf = config::Config::makeConfig();
	try {
		stmp = conf->getConfig("SessionManager", "MaxConcurrentTransactions");
	}
	catch(exception& e) {
		cout << e.what() << endl;
		stmp.empty();
	}
	if (stmp != "") {
		int64_t tmp;
		ctmp = stmp.c_str();
		tmp = config::Config::fromText(ctmp);
		if (tmp == numeric_limits<int64_t>::min() || tmp == numeric_limits<int64_t>::max() || tmp < 1)
			MaxTxns = 1000;
		else
			MaxTxns = static_cast<int>(tmp);
	}
	else
		MaxTxns = 1000;
	

	stmp.clear();
	try {
		stmp = conf->getConfig("SessionManager", "SharedMemoryTmpFile");
	}
	catch(exception& e) {
		cout << e.what() << endl;
		stmp.empty();
	}
	if (stmp != "")
		segmentFilename = strdup(stmp.c_str());
	else
		segmentFilename = strdup("/tmp/CalpontShm");
	
	txnidFilename = conf->getConfig("SessionManager", "TxnIDFile");

	txnidfd = open(txnidFilename.c_str(), O_RDWR | O_CREAT | O_BINARY, 0666);
	if (txnidfd < 0) {
		perror("SessionManagerServer(): open");
		throw runtime_error("SessionManagerServer: Could not open the transaction ID file");
	}
	//FIXME: do we need this on Win?
#ifndef _MSC_VER
	else
		fchmod(txnidfd, 0666);
#endif

	madeSems = makeSems();
	if (madeSems) {
		for (int i = 0; i < MaxTxns; i++)
		{
			shared->sems[1].post();
		}
	}
	
	getSharedData();
	unlock();
}

SessionManagerServer::SessionManagerServer(bool nolock) : unique32(0)
#ifdef _MSC_VER
	, fPids(0), fMaxPids(64)
#endif
{	
	config::Config* conf;
	string stmp;
	const char *ctmp;
	
	conf = config::Config::makeConfig();
	try {
		stmp = conf->getConfig("SessionManager", "MaxConcurrentTransactions");
	}
	catch(exception& e) {
		cout << e.what() << endl;
		stmp.empty();
	}
	if (stmp != "") {
		int64_t tmp;
		ctmp = stmp.c_str();
		tmp = config::Config::fromText(ctmp);
		if (tmp == numeric_limits<int64_t>::min() || tmp == numeric_limits<int64_t>::max() || tmp < 1)
			MaxTxns = 1000;
		else
			MaxTxns = static_cast<int>(tmp);
	}
	else
		MaxTxns = 1000;

	stmp.clear();
	try {
		stmp = conf->getConfig("SessionManager", "SharedMemoryTmpFile");
	}
	catch(exception& e) {
		cout << e.what() << endl;
		stmp.empty();
	}
	if (stmp != "")
		segmentFilename = strdup(stmp.c_str());
	else
		segmentFilename = strdup("/tmp/CalpontShm");
		
	txnidFilename = conf->getConfig("SessionManager", "TxnIDFile");

	txnidfd = open(txnidFilename.c_str(), O_RDWR | O_CREAT | O_BINARY, 0666);
	if (txnidfd < 0) {
		perror("SessionManagerServer(): open");
		throw runtime_error("SessionManagerServer: Could not open the transaction ID file");
	}

	shared = NULL;
	string keyName = ShmKeys::keyToName(fShmKeys.SESSIONMANAGER_SYSVKEY);
	bi::shared_memory_object shm(bi::open_only, keyName.c_str(), bi::read_write);
	bi::mapped_region region(shm, bi::read_write);
	fOverlayShm.swap(shm);
	fRegion.swap(region);
	shared = static_cast<Overlay*>(fRegion.get_address());
}

SessionManagerServer::~SessionManagerServer()
{
	if (shared != NULL) {
		lock();
		detachSegment();
		unlock();
	}
	free(segmentFilename);
	close(txnidfd);
}

void SessionManagerServer::detachSegment()
{
	
#ifdef DESTROYSHMSEG
	struct shmid_ds seginfo;

	err = shmctl(shmid, IPC_STAT, &seginfo);
	if (err < 0) {
		perror("SessionManagerServer::detachSegment(): shmctl(IPC_STAT)");
		saveSegment();
	}
	else if (seginfo.shm_nattch == 1) 
		saveSegment();
#endif
}

//returns 1 if it created the semaphores, 0 if they already existed
int SessionManagerServer::makeSems()
{
	int rc = -1;
	string keyName = ShmKeys::keyToName(fShmKeys.SESSIONMANAGER_SYSVKEY);
	try
	{
#if BOOST_VERSION < 104500
		bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write);
#ifdef __linux__
		{
			string pname = "/dev/shm/" + keyName;
			chmod(pname.c_str(), 0666);
		}
#endif
#else
		bi::permissions perms;
		perms.set_unrestricted();
		bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write, perms);
#endif
		unsigned size = sizeof(Overlay) + MaxTxns * sizeof(SIDTIDEntry);
		shm.truncate(size);
		bi::mapped_region region(shm, bi::read_write);
		fOverlayShm.swap(shm);
		fRegion.swap(region);
		shared = static_cast<Overlay*>(fRegion.get_address());
		shared->systemState = SS_NOT_READY;
		shared->txnCount = 0;
		shared->verID = 0;
		shared->sysCatVerID = 0;
		new (&shared->sems[0]) bi::interprocess_semaphore(0);
		new (&shared->sems[1]) bi::interprocess_semaphore(0);
		initSegment();
		rc = 1;
	}
	catch (...)
	{
		bi::shared_memory_object shm(bi::open_only, keyName.c_str(), bi::read_write);
		bi::mapped_region region(shm, bi::read_write);
		fOverlayShm.swap(shm);
		fRegion.swap(region);
		shared = static_cast<Overlay*>(fRegion.get_address());
		rc = 0;
	}
	return rc;
}
	
void SessionManagerServer::lock() 
{
again:
	try {
		shared->sems[0].wait();
	}
	catch (boost::interprocess::interprocess_exception &b) {
		if (b.get_error_code() == 1)   // handle EINTR
			goto again;
		throw;
	}
}

void SessionManagerServer::unlock() 
{
	shared->sems[0].post();
}

void SessionManagerServer::reset()
{
	while (shared->sems[0].try_wait())
	{
	}
	shared->sems[0].post();
	while (shared->sems[1].try_wait())
	{
	}
	for (int i = 0; i < MaxTxns; i++)
	{
		shared->sems[1].post();
	}
}

void SessionManagerServer::getSharedData() 
{
	//segment will always be there by now...
	string keyName = ShmKeys::keyToName(fShmKeys.SESSIONMANAGER_SYSVKEY);
	bi::shared_memory_object shm(bi::open_only, keyName.c_str(), bi::read_write);
	bi::mapped_region region(shm, bi::read_write);
	fOverlayShm.swap(shm);
	fRegion.swap(region);
	shared = static_cast<Overlay*>(fRegion.get_address());
}

inline void SessionManagerServer::initSegment()
{
	int lastTxnID;
	int err;
	int lastSysCatVerId;
	
again:
	lseek(txnidfd, 0, SEEK_SET);
	err = read(txnidfd, &lastTxnID, 4);
	if (err < 0 && errno != EINTR) {
		perror("Sessionmanager::initSegment(): read");
		throw runtime_error("SessionManagerServer: read failed, aborting");
	}
	else if (err < 0)
		goto again;
	else if (err == sizeof(int))
		shared->verID = lastTxnID;
    
    //lseek(txnidfd, 4, SEEK_SET);
    err = read(txnidfd, &lastSysCatVerId, 4);
    if (err < 0 && errno != EINTR) {
		perror("Sessionmanager::initSegment(): read");
		throw runtime_error("SessionManagerServer: read failed, aborting");
	}
	else if (err < 0)
		goto again;
	else if (err == sizeof(int))
		shared->sysCatVerID = lastSysCatVerId;
	// if 0 <= err < sizeof(int), the file is empty and txn's start at 1 like normal
}

#ifdef DESTROYSHMSEG
void SessionManagerServer::loadSegment() 
{
	int fd, err = 0, errCount = 0;
	uint progress = 0, size;
	char *seg = reinterpret_cast<char *>(shared);
	
	//FIXME: this calc is wrong
	size = 2*sizeof(int) + MaxTxns*sizeof(SIDTIDEntry);
	
	fd = open(segmentFilename, O_RDONLY);
	if (fd < 0) {
		perror("SessionManagerServer::loadSegment(): open");
		if (errno == ENOENT) {
			cerr << "SessionManagerServer::loadSegment(): (assuming this is the first invocation)" << endl;
			initSegment();
			return;
		}
		else
			throw ios_base::failure("SessionManagerServer::loadSegment(): open failed.  Check the error log.");
	}
	while (progress < size && errCount < MaxRetries) {
		err = read(fd, &seg[progress], size - progress);
		if (err < 0) {
			if (errno != EINTR) {
				perror("SessionManagerServer::loadSegment(): read");
				errCount++;
			}
		}
		else if (err == 0) {
			close(fd);
			throw ios_base::failure("SessionManagerServer::loadSegment(): read reports EOF prematurely");
		}
		else
			progress += err;
	}
	if (errCount == MaxRetries) {
		close(fd);
		throw ios_base::failure("SessionManagerServer::loadSegment(): too many read errors");
	}
	close(fd);
	
	// a quick, loose consistency check
	if (semctl(sems, 1, GETVAL) != (MaxTxns - shared->txnCount))
		throw runtime_error("SessionManagerServer::loadSegment(): ERROR: the txn "
			"semaphore does not match the txn count");

}

void SessionManagerServer::saveSegment()
{
	int fd, err = 0, errCount = 0;
	uint progress = 0, size;
	char *seg = reinterpret_cast<char *>(shared);

	size = 2*(sizeof(int)) + MaxTxns*sizeof(SIDTIDEntry);
	
	fd = open(segmentFilename, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0666);
	if (fd < 0) {
		perror("SessionManagerServer::saveSegment(): open");
		throw ios_base::failure("SessionManagerServer::saveSegment(): open failed.  Check the error log.");
	}
	while (progress < size && errCount < MaxRetries) {
		err = write(fd, &seg[progress], size - progress);
		if (err < 0) {
			if (errno != EINTR) {
				perror("SessionManagerServer::saveSegment(): write");
				errCount++;
			}
		}
		else
			progress += err;
	}
	if (errCount == MaxRetries) {
		close(fd);
		throw ios_base::failure("SessionManagerServer::saveSegment(): too many write errors");
	}
	close(fd);
}
#endif  // DESTROYSHMSEG

/* See bug 3330.  The SCN returned to queries has to be < the transaction ID.
 * This will have to be revised when we eventually support multiple
 * active transactions.
 */
const execplan::CalpontSystemCatalog::SCN SessionManagerServer::verID()
{
	execplan::CalpontSystemCatalog::SCN ret;

	lock();
	ret = shared->verID - shared->txnCount;
	unlock();
	return ret;

//	return shared->verID - shared->txnCount;
}

const execplan::CalpontSystemCatalog::SCN SessionManagerServer::sysCatVerID()
{
    execplan::CalpontSystemCatalog::SCN ret;

    lock();
    ret = shared->sysCatVerID - shared->txnCount;
    unlock();
    return ret;

//	return shared->sysCatVerID - shared->txnCount;
}

const TxnID SessionManagerServer::newTxnID(const SID session, bool block, bool isDDL) 
{
	TxnID ret; //ctor must set valid = false
	int i;
	int err;
	
	//@Bug 2661 Check if any active transaction This is a just a quick hack to allow only one active transaction a time. 
	// When we need to support concurrent transaction, this need to change.
	lock();
	for (i = 0; i < MaxTxns; i++) {
		if (shared->activeTxns[i].txnid.valid && (shared->activeTxns[i].sessionid != session) )
		{
			unlock();
			return ret;
		}
	}

again:
	try {
		if (!shared->sems[1].try_wait())
		{
			if (block)
				shared->sems[1].wait();
			else
				return ret;
		}
	}
	catch(boost::interprocess::interprocess_exception &e) {
		if (e.get_error_code() == 1)  // handle EINTR
			goto again;
		throw;
	}

	for (i = 0; i < MaxTxns; i++) {
		if (!shared->activeTxns[i].txnid.valid &&
			(shared->activeTxns[i].tableOID == 0)) {
			shared->activeTxns[i].sessionid = session;
			shared->activeTxns[i].txnid.id = ++shared->verID;
			
			if ( isDDL )
				++shared->sysCatVerID;
			
			shared->activeTxns[i].txnid.valid = true;
			ret = shared->activeTxns[i].txnid;
			shared->txnCount++;
			//printSIDTIDEntry("DBG:newTXNID create  ", i);
			lseek(txnidfd, 0, SEEK_SET);
			err = write(txnidfd, &shared->verID, sizeof(int));
			if (err < 0) {
				perror("SessionManagerServer::newTxnID(): write(txnid)");
				throw runtime_error("SessionManagerServer::newTxnID(): write(txnid) failed");
			}
			//lseek(txnidfd, 4, SEEK_SET);
			err = write(txnidfd, &shared->sysCatVerID, sizeof(int));
			if (err < 0) {
				perror("SessionManagerServer::newTxnID(): write(txnid)");
				throw runtime_error("SessionManagerServer::newTxnID(): write(txnid) failed");
			}
			break;
		}
	}
	
	unlock();
	if (i == MaxTxns)
		throw runtime_error("SessionManagerServer: txn semaphore does not agree with txnCount");
	
	return ret;
}

void SessionManagerServer::finishTransaction(TxnID& txn, bool commit)
{
	int i;
	bool found = false;
	
	if (!txn.valid)
		throw invalid_argument("SessionManagerServer::finishTransaction(): transaction is invalid");

	SID sessionId = 0;
	
	lock();

	// Note that we do NOT break from the loop after we find a match, because
	// we could have multiple entries for the same transaction.  There will be
	// an entry having a nonzero tableOID for each table in the transaction.
	for (i = 0; i < MaxTxns; i++) {   //for a constant time op, store the index in the txnid
		if ((shared->activeTxns[i].txnid.valid) &&
			(shared->activeTxns[i].txnid.id == txn.id)) {
			//printSIDTIDEntry("DBG:finishTransaction", i);
			sessionId = shared->activeTxns[i].sessionid;
			txn.valid = false;
			shared->activeTxns[i].init();
			
			// @bug 2576.  Added use of found bool below.  There were bogus entries showing up in the warning.log file for successful
			// commits and rollbacks.  For example, the entry below was showing up for successful create table statements.
			// CAL0000: DBRM: warning: SessionManager::committed() failed (valid error code)
			found = true;
		}
	}

	if(found) {
		shared->txnCount--;
		shared->sems[1].post();
	}
	unlock();

	if (!found)
		throw invalid_argument("SessionManagerServer::finishTransaction(): transaction doesn't exist");
}

void SessionManagerServer::committed(TxnID& txn)
{
	finishTransaction(txn, true);
}

void SessionManagerServer::rolledback(TxnID& txn)
{
	finishTransaction(txn, false);
}

const TxnID SessionManagerServer::getTxnID(const SID session)
{
	int i;
	TxnID ret;
	
	lock();
	for (i = 0; i < MaxTxns; i++) {
		if (shared->activeTxns[i].sessionid == session &&
		   shared->activeTxns[i].txnid.valid) {
			ret = shared->activeTxns[i].txnid;
			break;
		}
	}
	unlock();
	
	return ret;
}

char * SessionManagerServer::getShmContents(int &len)
{
	char *ret;
	
	len = sizeof(Overlay) + MaxTxns * sizeof(SIDTIDEntry);
	ret = new char[len];
	lock();
	memcpy(ret, shared, len);
	unlock();
	return ret;	
}

const SIDTIDEntry* SessionManagerServer::SIDTIDMap(int& len)
{
	int i, j;
	SIDTIDEntry *ret;

	lock();
	len = shared->txnCount;
	ret = new SIDTIDEntry[len];
	for (i = 0, j = 0; i < MaxTxns && j < shared->txnCount; i++) {
		if (shared->activeTxns[i].txnid.valid)
			ret[j++] = shared->activeTxns[i];
	}
	try {
		unlock();
	}
	catch (...) {
		delete [] ret;
		throw;
	}
	
	if (j != len) {
		delete [] ret;
		throw runtime_error("Sessionmanager::SIDTIDMap(): txnCount is invalid");
	}
	
	return ret;
}

string SessionManagerServer::getTxnIDFilename() const 
{
	return txnidFilename;
}

int SessionManagerServer::verifySize()
{
	int i, ret, countsnapshot, semsnap;
	
	lock();
	for (i = 0, ret = 0; i < MaxTxns; i++)
		if (shared->activeTxns[i].txnid.valid == true)
			ret++;
					
	countsnapshot = shared->txnCount;
	//semsnap = semctl(sems, 1, GETVAL);
	//unfortuneately, boost::interprocess does not provide an API to access the sem count :-(
	semsnap = MaxTxns - ret;
	unlock();
	
	if (ret != countsnapshot) {
		cerr << "SessionManagerServer::verifySize(): actual count = " << ret 
				<< " txnCount = " << countsnapshot << endl;
		throw logic_error("SessionManagerServer::verifySize(): txnCount != actual count");
	}
			
	if (semsnap == -1) {
		perror("SessionManagerServer::verifySize(): semctl");
		throw runtime_error("SessionManagerServer::verifySize(): semctl gave an error. Check the log.");
	}
		
	if (ret != MaxTxns - semsnap) {
		cerr << "SessionManagerServer::verifySize(): actual count = " << ret 
				<< " semsnap = " << semsnap << endl;
		throw logic_error("SessionManagerServer::verifySize(): (MaxTxns - semsnap) != actual count");
	}
			
	return ret;
}

const uint32_t SessionManagerServer::getUnique32()
{
#ifdef _MSC_VER
	return InterlockedIncrement(&unique32);
#else
	return __sync_add_and_fetch(&unique32, 1);
#endif
}

int8_t SessionManagerServer::setTableLock (  const OID_t tableOID, const u_int32_t sessionID,  const u_int32_t processID, const string processName, bool tolock ) 
{
	int i;
	int8_t  err = 0;
	TxnID txnid  = getTxnID( sessionID );
	//DML will share table lock. Only cpimport needs exclusive lock
	lock();
	if ( tolock )
	{
		for (i = 0; i < MaxTxns; i++) {
			if ((shared->activeTxns[i].tableOID == tableOID) && ( shared->activeTxns[i].processID != processID ) )
			{
				err = ERR_TABLE_LOCKED_ALREADY;
				//cerr << "SessionManagerServer::setTableLock(lock): table is locked by pid " << shared->activeTxns[i].processID << endl;
				unlock();
				return err;
			}
			else if ((shared->activeTxns[i].tableOID == tableOID) && ( shared->activeTxns[i].processID == processID ) )
			{
				//The table is locked already by this session
				unlock();
				return err;
			}
		}
	}
	
	if ( tolock )
	{
		int write_rc = 0;
		for (i = 0; i < MaxTxns; i++) {
			if (!shared->activeTxns[i].txnid.valid &&
				(shared->activeTxns[i].tableOID == 0))
			{
				shared->activeTxns[i].sessionid = sessionID;
				shared->activeTxns[i].tableOID = tableOID;
				shared->activeTxns[i].processID = processID;
				shared->activeTxns[i].txnid = txnid;
			
				//@Bug 2569,2570
				memset( shared->activeTxns[i].processName, 0, MAX_PROCNAME );
				strncpy( shared->activeTxns[i].processName, processName.c_str(), MAX_PROCNAME-1 );
				//printSIDTIDEntry("DBG:setTableLock LOCK", i);
				lseek(txnidfd, 0, SEEK_SET);
				write_rc = write(txnidfd, &shared->verID, sizeof(int));
				if (write_rc < 0) {
					perror("SessionManagerServer::setTableLock(lock): write(verID)");
					throw runtime_error("SessionManagerServer::setTableLock(nonDML,lock): write(verID) failed");
				}
				//lseek(txnidfd, 4, SEEK_SET);
				write_rc = write(txnidfd, &shared->sysCatVerID, sizeof(int));
				if (write_rc < 0) {
					perror("SessionManagerServer::setTableLock(lock): write(sysCatVerID)");
					throw runtime_error("SessionManagerServer::setTableLock(nonDML,lock): write(sysCatVerID) failed");
				}
				break;
			}
		}
	}
	else
	{
		int write_rc = 0;
		for (i = 0; i < MaxTxns; i++) {
			if (shared->activeTxns[i].tableOID == tableOID)
			{
				//printSIDTIDEntry("DBG:setTableLock FREE", i);
				shared->activeTxns[i].init();
				lseek(txnidfd, 0, SEEK_SET);
				write_rc = write(txnidfd, &shared->verID, sizeof(int));
				if (write_rc < 0) {
					perror("SessionManagerServer::setTableLock(unlock): write(verID)");
					throw runtime_error("SessionManagerServer::setTableLock(unlock): write(verID) failed");
				}
				//lseek(txnidfd, 4, SEEK_SET);
				write_rc = write(txnidfd, &shared->sysCatVerID, sizeof(int));
				if (write_rc < 0) {
					perror("SessionManagerServer::setTableLock(unlock): write(sysCatVerID)");
					throw runtime_error("SessionManagerServer::setTableLock(unlock): write(sysCatVerID) failed");
				}
				break;
			}
		}
	}
	
	unlock();
	if (i == MaxTxns) {
		if ( tolock )
			throw runtime_error("SessionManagerServer::setTableLock(lock): cannot add table lock; "
				"maximum allowable transactions are currently active");
		else
			err = ERR_TABLE_NOT_LOCKED;
	}
	
	return err;

}

int8_t SessionManagerServer::updateTableLock (  const OID_t tableOID,  u_int32_t & processID, std::string & processName )
{
	int i;
	int8_t  err = 0;
	bool tableLocked = false;
	bool validLock = false;
	lock();
	
	for (i = 0; i < MaxTxns; i++) {
		if ((shared->activeTxns[i].tableOID == tableOID) )
		{
			//Check the lock info
			if (shared->activeTxns[i].processID != 0)
			{
				validLock = lookupProcessStatus (shared->activeTxns[i].processName, shared->activeTxns[i].processID);
				if (validLock)
				{
					processID = shared->activeTxns[i].processID;
					processName = shared->activeTxns[i].processName;
					unlock();
					err = ERR_TABLE_LOCKED_ALREADY;
					return err;
				}
				else //reset the lock
				{
					//save the previous lock info
					u_int32_t preProcessID;
					std::string  preProcessName;
					preProcessID = shared->activeTxns[i].processID;
					preProcessName = shared->activeTxns[i].processName;
					shared->activeTxns[i].tableOID = tableOID;
					shared->activeTxns[i].processID = processID;
					strncpy( shared->activeTxns[i].processName, processName.c_str(), MAX_PROCNAME-1 );
					processID = preProcessID;
					processName = preProcessName;
					tableLocked = true;
					break;
				}				
			}	
			else
			{
				processID = shared->activeTxns[i].processID;
				processName = shared->activeTxns[i].processName;
				unlock();
                                err = ERR_TABLE_LOCKED_ALREADY;
                                return err;

			}	
		}
	}
	
	if ( !tableLocked ) //The table is not locked by any process yet. Lock the table with passed in process
	{
		int write_rc = 0;
		for (i = 0; i < MaxTxns; i++) {
			if (!shared->activeTxns[i].txnid.valid &&
				(shared->activeTxns[i].tableOID == 0))
			{
				shared->activeTxns[i].tableOID = tableOID;
				shared->activeTxns[i].processID = processID;
			
				memset( shared->activeTxns[i].processName, 0, MAX_PROCNAME );
				strncpy( shared->activeTxns[i].processName, processName.c_str(), MAX_PROCNAME-1 );
				lseek(txnidfd, 0, SEEK_SET);
				write_rc = write(txnidfd, &shared->verID, sizeof(int));
				if (write_rc < 0) {
					perror("SessionManagerServer::updateTableLock: write(verID)");
					throw runtime_error("SessionManagerServer::updateTableLock: write(verID) failed");
				}
				
				write_rc = write(txnidfd, &shared->sysCatVerID, sizeof(int));
				if (write_rc < 0) {
					perror("SessionManagerServer::updateTableLock: write(sysCatVerID)");
					throw runtime_error("SessionManagerServer::updateTableLock: write(sysCatVerID) failed");
				}
				break;
			}
		}
	}
	unlock();
	return err;
}

bool SessionManagerServer::lookupProcessStatus(std::string   processName, u_int32_t     processId)
{
#ifdef _MSC_VER
	boost::mutex::scoped_lock lk(fPidMemLock);	
	if (!fPids)
		fPids = (DWORD*)malloc(fMaxPids * sizeof(DWORD));
	DWORD needed = 0;
	if (EnumProcesses(fPids, fMaxPids * sizeof(DWORD), &needed) == 0)
		return false;
	while (needed == fMaxPids * sizeof(DWORD))
	{
		fMaxPids *= 2;
		fPids = (DWORD*)realloc(fPids, fMaxPids * sizeof(DWORD));
		if (EnumProcesses(fPids, fMaxPids * sizeof(DWORD), &needed) == 0)
			return false;
	}
	DWORD numPids = needed / sizeof(DWORD);
	for (DWORD i = 0; i < numPids; i++)
	{
		if (fPids[i] == processId)
		{
			TCHAR szProcessName[MAX_PATH] = TEXT("<unknown>");

			// Get a handle to the process.
			HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION |
										   PROCESS_VM_READ,
										   FALSE, fPids[i]);
			// Get the process name.
			if (hProcess != NULL)
			{
				HMODULE hMod;
				DWORD cbNeeded;

				if (EnumProcessModules(hProcess, &hMod, sizeof(hMod), &cbNeeded))
					GetModuleBaseName(hProcess, hMod, szProcessName, 
									   sizeof(szProcessName)/sizeof(TCHAR));

				CloseHandle(hProcess);

				if (processName == szProcessName)
					return true;
			}
		}
	}
	return false;
#else
	bool bMatchFound = false;

	std::ostringstream fileName;
	fileName << "/proc/" << processId << "/stat";
	FILE* pFile = fopen( fileName.str().c_str(), "r" );
	if (pFile)
	{
		pid_t pid;
		char  pName[100];

		// Read in process name based on format of /proc/stat file described
		// in "proc" manpage.  Have to allow for pName being enclosed in ().
		if ( fscanf(pFile, "%d%s", &pid, pName) == 2 )
		{
			pName[strlen(pName)-1] = '\0'; // strip trailing ')'
			if (processName == &pName[1])  // skip leading '(' in comparison
			{
				bMatchFound = true;
			}
		}

		fclose( pFile );
	}
	
	return bMatchFound;
#endif
}
		
int8_t SessionManagerServer::getTableLockInfo ( const OID_t tableOID, u_int32_t & processID,
	string & processName, bool & lockStatus, SID & sid )
{
	int i;
	int8_t err = 0;
	lockStatus = false;
	lock();
	for (i = 0; i < MaxTxns; i++) {
		if ( shared->activeTxns[i].tableOID == tableOID ) {
			lockStatus = true;
			processID = shared->activeTxns[i].processID;
			processName = shared->activeTxns[i].processName;
			sid = shared->activeTxns[i].sessionid;
			break;
		}
	}
	unlock();
	
	return err;
}

std::vector<SIDTIDEntry> SessionManagerServer::getTableLocksInfo ( )
{
	int i;
	std::vector<SIDTIDEntry> sidTidEntries;
	lock();
	for (i = 0; i < MaxTxns; i++) {
		if ( shared->activeTxns[i].tableOID > 0 ) {
			sidTidEntries.push_back( shared->activeTxns[i] );
		}
	}
	unlock();

	return sidTidEntries;
}

void SessionManagerServer::printSIDTIDEntry ( const char* commentHdr, int idx ) const
{
	std::cout << commentHdr <<
		": txnid.id: "    << shared->activeTxns[idx].txnid.id    <<
		"; index: "       << idx                                 <<
		"; txnid.valid: " << shared->activeTxns[idx].txnid.valid <<
		"; sessionId:   " << shared->activeTxns[idx].sessionid   <<
		"; tableOID: "    << shared->activeTxns[idx].tableOID    <<
		"; processID: "   << shared->activeTxns[idx].processID   <<
		"; processName: " << ( (shared->activeTxns[idx].processName[0]) ? 
			(shared->activeTxns[idx].processName) : "(empty)" )  << std::endl;
}

int8_t SessionManagerServer::setSystemState(int state)
{
	int8_t err = -1;
	if (state == SS_READY || state == SS_NOT_READY)
	{
		lock();
		shared->systemState = state;
		unlock();
		err = 0;
	}
	return err;
}

int8_t SessionManagerServer::getSystemState(int& state)
{
	lock();
	state = shared->systemState;
	unlock();
	return 0;
}

}  //namespace
