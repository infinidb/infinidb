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
 * $Id: sessionmanagerserver.cpp 1706 2012-09-20 12:43:42Z rdempsey $
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

#include <iostream>
#include <string>
#include <stdexcept>
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
using namespace boost;
using namespace execplan;

namespace BRM {

const uint32_t SessionManagerServer::SS_READY				= 1 << 0;	// Set by dmlProc one time when dmlProc is ready
const uint32_t SessionManagerServer::SS_SUSPENDED			= 1 << 1;	// Set by console when the system has been suspended by user.
const uint32_t SessionManagerServer::SS_SUSPEND_PENDING		= 1 << 2;	// Set by console when user wants to suspend, but writing is occuring.
const uint32_t SessionManagerServer::SS_SHUTDOWN_PENDING	= 1 << 3;	// Set by console when user wants to shutdown, but writing is occuring.
const uint32_t SessionManagerServer::SS_ROLLBACK			= 1 << 4;   // In combination with a PENDING flag, force a rollback as soom as possible.
const uint32_t SessionManagerServer::SS_FORCE				= 1 << 5;   // In combination with a PENDING flag, force a shutdown without rollback.


SessionManagerServer::SessionManagerServer() : unique32(0), unique64(0)
{	
	config::Config* conf;
	string stmp;
	const char *ctmp;
	
	conf = config::Config::makeConfig();
	try {
		stmp = conf->getConfig("SessionManager", "MaxConcurrentTransactions");
	}
	catch(const std::exception &e) {
		cout << e.what() << endl;
		stmp.empty();
	}
	if (stmp != "") {
		int64_t tmp;
		ctmp = stmp.c_str();
		tmp = config::Config::fromText(ctmp);
		if (tmp < 1)
			maxTxns = 1;
		else
			maxTxns = static_cast<int>(tmp);
	}
	else
		maxTxns = 1;
	
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

	semValue = maxTxns;
	_verID = 0;
	_sysCatVerID = 0;
	systemState = 0;
	try {
		loadState();
	}
	catch (...) {
		// first-time run most likely, ignore the error
	}
}

SessionManagerServer::~SessionManagerServer()
{
	close(txnidfd);
}

void SessionManagerServer::reset()
{
	mutex.try_lock();
	semValue = maxTxns;
	condvar.notify_all();
	activeTxns.clear();
	mutex.unlock();
}

void SessionManagerServer::loadState()
{
	int lastTxnID;
	int err;
	int lastSysCatVerId;
	
again:

	// There are now 3 pieces of info stored in the txnidfd file: last
	// transaction id, last system catalog version id, and the
	// system state flags. All these values are stored in shared, an
	// instance of struct Overlay.
	// If we fail to read a full four bytes for any value, then the
	// value isn't in the file, and we start with the default.

	// Last transaction id
	lseek(txnidfd, 0, SEEK_SET);
	err = read(txnidfd, &lastTxnID, 4);
	if (err < 0 && errno != EINTR) {
		perror("Sessionmanager::initSegment(): read");
		throw runtime_error("SessionManagerServer: read failed, aborting");
	}
	else if (err < 0)
		goto again;
	else if (err == sizeof(int))
		_verID = lastTxnID;
    
    // last system catalog version id
    err = read(txnidfd, &lastSysCatVerId, 4);
    if (err < 0 && errno != EINTR) {
		perror("Sessionmanager::initSegment(): read");
		throw runtime_error("SessionManagerServer: read failed, aborting");
	}
	else if (err < 0)
		goto again;
	else if (err == sizeof(int))
		_sysCatVerID = lastSysCatVerId;

	// System state. Contains flags regarding the suspend state of the system.
	err = read(txnidfd, &systemState, 4);
	if (err < 0 && errno == EINTR) {
		goto again;
	}
	else if (err == sizeof(int))
	{
		// Turn off the pending and force flags. They make no sense for a clean start.
		// Turn off the ready flag. DMLProc will set it back on when
		// initialized.
		systemState &= ~(SS_READY | SS_SUSPEND_PENDING | SS_SHUTDOWN_PENDING | SS_ROLLBACK | SS_FORCE);
	}
	else
	{
		// else no problem. System state wasn't saved. Might be an upgraded system.
		systemState = 0;
	}
}

/* Save the systemState flags of the Overlay
 * segment. This is saved in the third
 * word of txnid File
*/
void SessionManagerServer::saveSystemState() 
{ 
	int err = 0;
	uint32_t lSystemState = systemState;

	// We don't save the pending flags, the force flag or the ready flag.
	lSystemState &= ~(SS_READY | SS_SUSPEND_PENDING | SS_SHUTDOWN_PENDING | SS_FORCE);
	lseek(txnidfd, 8, SEEK_SET);
	err = write(txnidfd, &lSystemState, sizeof(int));
	if (err < 0) {
		perror("SessionManagerServer::saveSystemState(): write(systemState)");
		throw runtime_error("SessionManagerServer::saveSystemState(): write(systemState) failed");
	}
} 

/* See bug 3330.  The SCN returned to queries has to be < the transaction ID.
 * This will have to be revised when we eventually support multiple
 * active transactions.
 */
const CalpontSystemCatalog::SCN SessionManagerServer::verID()
{
	execplan::CalpontSystemCatalog::SCN ret;

	mutex::scoped_lock lk(mutex);
	ret = _verID - activeTxns.size();
	return ret;
}

const CalpontSystemCatalog::SCN SessionManagerServer::sysCatVerID()
{
    execplan::CalpontSystemCatalog::SCN ret;

    mutex::scoped_lock lk(mutex);
    ret = _sysCatVerID - activeTxns.size();
    return ret;
}

const TxnID SessionManagerServer::newTxnID(const SID session, bool block, bool isDDL) 
{
	TxnID ret; //ctor must set valid = false
	int err;
	iterator it;
	
	mutex::scoped_lock lk(mutex);

	// if it already has a txn...
	it = activeTxns.find(session);
	if (it != activeTxns.end()) {
		ret.id = it->second;
		ret.valid = true;
		return ret;
	}

	if (!block && semValue == 0)
		return ret;
	else while (semValue == 0)
		condvar.wait(lk);

	semValue--;
	idbassert(semValue <= (uint)maxTxns);

	ret.id = ++_verID;
	ret.valid = true;
	activeTxns[session] = ret.id;
	if (isDDL)
		++_sysCatVerID;

	int filedata[2];
	filedata[0] = _verID;
	filedata[1] = _sysCatVerID;

	lseek(txnidfd, 0, SEEK_SET);
	err = write(txnidfd, filedata, 8);
	if (err < 0) {
		perror("SessionManagerServer::newTxnID(): write(verid)");
		throw runtime_error("SessionManagerServer::newTxnID(): write(verid) failed");
	}

	return ret;
}

void SessionManagerServer::finishTransaction(TxnID& txn)
{
	iterator it;
	mutex::scoped_lock lk(mutex);
	
	if (!txn.valid)
		throw invalid_argument("SessionManagerServer::finishTransaction(): transaction is invalid");

	for (it = activeTxns.begin(); it != activeTxns.end(); ++it) {
		if (it->second == txn.id) {
			activeTxns.erase(it);
			txn.valid = false;
			break;
		}
	}

	if (it != activeTxns.end()) {
		semValue++;
		idbassert(semValue <= (uint)maxTxns);
		condvar.notify_one();
	}
	else
		throw invalid_argument("SessionManagerServer::finishTransaction(): transaction doesn't exist");
}

void SessionManagerServer::committed(TxnID& txn)
{
	finishTransaction(txn);
}

void SessionManagerServer::rolledback(TxnID& txn)
{
	finishTransaction(txn);
}

const TxnID SessionManagerServer::getTxnID(const SID session)
{
	TxnID ret;
	iterator it;
	
	mutex::scoped_lock lk(mutex);

	it = activeTxns.find(session);
	if (it != activeTxns.end()) {
		ret.id = it->second;
		ret.valid = true;
	}
	
	return ret;
}

shared_array<SIDTIDEntry> SessionManagerServer::SIDTIDMap(int &len)
{
	int j;
	shared_array<SIDTIDEntry> ret;
	mutex::scoped_lock lk(mutex);
	iterator it;

	ret.reset(new SIDTIDEntry[activeTxns.size()]);

	len = activeTxns.size();
	for (it = activeTxns.begin(), j = 0; it != activeTxns.end(); ++it, ++j) {
		ret[j].sessionid = it->first;
		ret[j].txnid.id = it->second;
		ret[j].txnid.valid = true;
	}
	
	return ret;
}

string SessionManagerServer::getTxnIDFilename() const 
{
	return txnidFilename;
}

const uint32_t SessionManagerServer::getUnique32()
{
#ifdef _MSC_VER
	return InterlockedIncrement(&unique32);
#else
	return __sync_add_and_fetch(&unique32, 1);
#endif
}

const uint64_t SessionManagerServer::getUnique64()
{
#ifdef _MSC_VER
	return InterlockedIncrement64(&unique64);
#else
	return __sync_add_and_fetch(&unique64, 1);
#endif
}

void SessionManagerServer::setSystemState(uint32_t state)
{
	mutex::scoped_lock lk(mutex);

	systemState |= state;
	saveSystemState();
}

void SessionManagerServer::clearSystemState(uint32_t state)
{
	mutex::scoped_lock lk(mutex);

	systemState &= ~state;
	saveSystemState();
}

void SessionManagerServer::getSystemState(uint32_t& state)
{
	state = systemState;
}

uint32_t SessionManagerServer::getTxnCount()
{
	mutex::scoped_lock lk(mutex);
	return activeTxns.size();
}

}  //namespace
