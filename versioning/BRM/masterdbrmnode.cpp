/* Copyright (C) 2013 Calpont Corp.

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
 * $Id: masterdbrmnode.cpp 1899 2013-06-12 13:32:30Z dcathey $
 *
 ****************************************************************************/

#include <unistd.h>
#include <sys/types.h>
#include <sstream>

#include "sessionmanager.h"
#include "socketclosed.h"
#include "snmpglobal.h"
#include "snmpmanager.h"
#include "liboamcpp.h"
#include "stopwatch.h"
#include "masterdbrmnode.h"

// #define BRM_VERBOSE

// minor improvement to code clarity...
#define CHECK_ERROR1(x) \
	if (halting) { \
		for (it = responses.begin(); it != responses.end(); it++) \
			delete *it; \
		responses.clear(); \
		undo(); \
		slaveLock.unlock(); \
		usleep(50000); \
		goto retrycmd; \
	} \
	if (x) { \
		undo(); \
		slaveLock.unlock(); \
		sendError(p->sock, x); \
		goto out; \
	} else {}

#define THREAD_EXIT { \
	mutex.lock(); \
	for (vector<IOSocket *>::iterator _it = activeSessions.begin(); \
	  _it != activeSessions.end(); ++_it) \
		if (p->sock == *_it) { \
			activeSessions.erase(_it); \
			break; \
		} \
	mutex.unlock(); \
 	p->sock->close(); \
	delete p->sock; \
	delete p->t; \
	delete p; \
	for (it = responses.begin(); it != responses.end(); it++) \
		delete *it; }

#if 1
#define SEND_ALARM \
	try { \
		snmpmanager::SNMPManager alarmMgr; \
		alarmMgr.sendAlarmReport("System", oam::DBRM_READ_ONLY, snmpmanager::SET); \
	} \
	catch (...) { }
#define CLEAR_ALARM \
	try { \
		snmpmanager::SNMPManager alarmMgr; \
		alarmMgr.sendAlarmReport("System", oam::DBRM_READ_ONLY, snmpmanager::CLEAR); \
	} \
	catch (...) { }
#else
#define SEND_ALARM 
#define CLEAR_ALARM
#endif

using namespace std;
using namespace messageqcpp;
using namespace logging;
namespace BRM {

MasterDBRMNode::MasterDBRMNode()
{
	config::Config *config;

	config = config::Config::makeConfig();
	if (config == NULL) 
		throw invalid_argument("MasterDBRMNode: Configuration error.");
	
	runners = 0;
	die = false;
	reloadCmd = false;
	readOnly = false;
	halting = false;

	tableLockServer.reset(new TableLockServer(&sm));
	initMsgQueues(config);
	rg = new LBIDResourceGraph();
	//@Bug 2325 DBRMTimeOut is default to 60 seconds
	std::string retStr = config->getConfig("SystemConfig", "DBRMTimeOut");
	int secondsToWait = config->fromText(retStr);
	MSG_TIMEOUT.tv_nsec = 0;
	if ( secondsToWait > 0 )
		MSG_TIMEOUT.tv_sec = secondsToWait;
	else
		MSG_TIMEOUT.tv_sec = 20;
}

MasterDBRMNode::~MasterDBRMNode()
{
 	die = true;
 	finalCleanup();	
}

MasterDBRMNode::MasterDBRMNode(const MasterDBRMNode &m)
{
	throw logic_error("Don't use the MasterDBRMNode copy constructor");
}

MasterDBRMNode& MasterDBRMNode::operator=(const MasterDBRMNode &m)
{
	throw logic_error("Don't use the MasterDBRMNode = operator");
}

void MasterDBRMNode::initMsgQueues(config::Config *config)
{
	string stmp;
	int ltmp;
	char ctmp[50];
	int i;
	
	stmp = config->getConfig("DBRM_Controller", "NumWorkers");
	if (stmp.length() == 0) 
		throw runtime_error("MasterDBRMNode::initMsgQueues(): config file error looking for <DBRM_Controller><NumWorkers>");
	
	ltmp = static_cast<int>(config::Config::fromText(stmp));
	if (ltmp < 1) 
		throw runtime_error("MasterDBRMNode::initMsgQueues(): Bad NumWorkers value");

	NumWorkers = ltmp;

	serverLock.lock();
	try {
		dbrmServer = new MessageQueueServer("DBRM_Controller", config);
	}
	catch (...) {
		serverLock.unlock();
		throw;
	}
	serverLock.unlock();
	for (i = 1; i <= NumWorkers; i++) {
		snprintf(ctmp, 50, "DBRM_Worker%d", i);
		slaves.push_back(new MessageQueueClient(ctmp, config));
	}
}

void MasterDBRMNode::stop()
{
	die = true;
}

void MasterDBRMNode::lock()
{
	slaveLock.lock();
}

void MasterDBRMNode::unlock()
{
	slaveLock.unlock();
}

void MasterDBRMNode::reload()
{
	config::Config *config;

#ifdef BRM_VERBOSE
	cerr << "DBRM Controller: reloading the config file." << endl;
#endif

	reloadCmd = false;
	config = config::Config::makeConfig();
	if (config == NULL)
		throw runtime_error("DBRM Controller: Configuration error.  Reload aborted.");

	die = true;
	finalCleanup();
#ifdef BRM_VERBOSE
	cerr << "DBRM Controller: reinitializing..." << endl;
#endif

	try {
		initMsgQueues(config);
	}
	catch (exception&) {
		throw runtime_error("DBRM Controller: Configuration error.  Reload aborted.");
		// masterdbrm.run() will exit after this
	}
	die = false;

	rg = new LBIDResourceGraph();
#ifdef BRM_VERBOSE
 	cerr << "DBRM Controller: reload successful." << endl;
#endif
	readOnly = false;
}

void MasterDBRMNode::setReadOnly(bool ro)
{
	slaveLock.lock();
	readOnly = ro;
	slaveLock.unlock();
}

void MasterDBRMNode::run()
{
	ByteStream msg;
	IOSocket *s;
	boost::thread *reader;

	CLEAR_ALARM

	while (!die) {
		s = new IOSocket();
#ifdef BRM_VERBOSE
		cerr << "DBRM Controller waiting..." << endl;
#endif
		serverLock.lock();
		if (dbrmServer != NULL)
			try {
				*s = dbrmServer->accept(&MSG_TIMEOUT);
			}
			catch (runtime_error &e) {
				cerr << e.what() << " continuing...\n";
				serverLock.unlock();
				delete s;
				continue;
			}
		serverLock.unlock();

		if (reloadCmd) {
			if (s->isOpen())
				s->close();
			delete s;
			reload();
			continue;
		}

		if (die || !s->isOpen()) {
			if (s->isOpen())
				s->close();
			delete s;
			continue;
		}

#ifdef BRM_VERBOSE
		cerr << "DBRM Controller: got a connection..." << endl;
#endif
 		mutex.lock();
		activeSessions.push_back(s);
		params = new ThreadParams();
#ifdef BRM_VERBOSE
		cerr << "DBRM Controller: starting another thread" << endl;
#endif
		mutex2.lock();
		
		try {
			reader = new boost::thread(MsgProcessor(this));
		}
		catch (boost::thread_resource_error&) {
			log("DBRM Controller: WARNING!!  Got thread resource error!  Increase system stack size or decrease the # of active sessions.");
#ifdef BRM_VERBOSE
			cerr << "DBRM Controller: WARNING!!  Got thread resource error!  Increase system stack size or decrease the # of active sessions.\n";
#endif
			activeSessions.pop_back();
			sendError(s, ERR_NETWORK);
			sleep(1);  // don't close right away to avoid broken pipe on the client
			s->close();
			delete s;
			delete params;
			mutex2.unlock();
			mutex.unlock();
			continue;
		}
	
		params->t = reader;
		params->sock = s;
		mutex2.unlock();
#ifdef __FreeBSD__
		mutex.unlock();
#endif
	}
	serverLock.lock();
	delete dbrmServer;
	dbrmServer = NULL;
	serverLock.unlock();
}

void MasterDBRMNode::msgProcessor()
{
	struct ThreadParams *p;
	ByteStream msg;
	vector<ByteStream *> responses;
	vector<ByteStream *>::iterator it;
	int err;
	uint8_t cmd;
	StopWatch timer;
#ifdef BRM_VERBOSE
	cerr << "DBRM Controller: msgProcessor()" << endl;
#endif

	mutex2.lock();
	p = params;
	mutex2.unlock();
#ifndef __FreeBSD__
	mutex.unlock();
#endif
	while (!die) {
		try {
			msg = p->sock->read(&MSG_TIMEOUT);
		}
		catch (...) {
			THREAD_EXIT
			throw;
		}

		if (die) // || msg.length() == 0)
			break;
  		
 		else if (msg.length() == 0)
   			continue;

		/* Check for an command for the master */
		msg.peek(cmd);
#ifdef BRM_VERBOSE
		cerr << "DBRM Controller: recv'd message " << (int)cmd << " length " << msg.length() << endl;
#endif
		switch (cmd) {
			case HALT: doHalt(p->sock); continue;
			case RESUME: doResume(p->sock); continue;
			case RELOAD: 
				try { 
					doReload(p->sock);
				}
				catch (exception &e) {
					cerr << e.what() << endl;
				}
				continue;
			case SETREADONLY: doSetReadOnly(p->sock, true); continue;
			case SETREADWRITE: doSetReadOnly(p->sock, false); continue;
			case GETREADONLY: doGetReadOnly(p->sock); continue;
		}

		/* Process SessionManager calls */
		switch (cmd) {
			case VER_ID: doVerID(msg, p); continue;
			case SYSCAT_VER_ID: doSysCatVerID(msg, p); continue;
			case NEW_TXN_ID: doNewTxnID(msg, p); continue;
			case COMMITTED: doCommitted(msg, p); continue;
			case ROLLED_BACK: doRolledBack(msg, p); continue;
			case GET_TXN_ID: doGetTxnID(msg, p); continue;
			case SID_TID_MAP: doSIDTIDMap(msg, p); continue;
			case GET_UNIQUE_UINT32: doGetUniqueUint32(msg, p); continue;
			case GET_UNIQUE_UINT64: doGetUniqueUint64(msg, p); continue;
			case GET_SYSTEM_STATE: doGetSystemState(msg, p); continue;
			case SET_SYSTEM_STATE: doSetSystemState(msg, p); continue;
			case CLEAR_SYSTEM_STATE: doClearSystemState(msg, p); continue;
			case SM_RESET: doSessionManagerReset(msg, p); continue;
		}

		/* Process TableLock calls */
		switch (cmd) {
			case GET_TABLE_LOCK: doGetTableLock(msg, p); continue;
			case RELEASE_TABLE_LOCK: doReleaseTableLock(msg, p); continue;
			case CHANGE_TABLE_LOCK_STATE: doChangeTableLockState(msg, p); continue;
			case CHANGE_TABLE_LOCK_OWNER: doChangeTableLockOwner(msg, p); continue;
			case GET_ALL_TABLE_LOCKS: doGetAllTableLocks(msg, p); continue;
			case RELEASE_ALL_TABLE_LOCKS: doReleaseAllTableLocks(msg, p); continue;
			case GET_TABLE_LOCK_INFO: doGetTableLockInfo(msg, p); continue;
			case OWNER_CHECK: doOwnerCheck(msg, p); continue;
		}

		/* Process OIDManager calls */
		switch (cmd) {
			case ALLOC_OIDS: doAllocOIDs(msg, p); continue;
			case RETURN_OIDS: doReturnOIDs(msg, p); continue;
			case OIDM_SIZE: doOidmSize(msg, p); continue;

			case ALLOC_VBOID: doAllocVBOID(msg, p); continue;
			case GETDBROOTOFVBOID: doGetDBRootOfVBOID(msg, p); continue;
			case GETVBOIDTODBROOTMAP: doGetVBOIDToDBRootMap(msg, p); continue;
		}

		/* Process Autoincrement calls */
		switch (cmd) {
			case START_AI_SEQUENCE: doStartAISequence(msg, p); continue;
			case GET_AI_RANGE: doGetAIRange(msg, p); continue;
			case RESET_AI_SEQUENCE: doResetAISequence(msg, p); continue;
			case GET_AI_LOCK: doGetAILock(msg, p); continue;
			case RELEASE_AI_LOCK: doReleaseAILock(msg, p); continue;
			case DELETE_AI_SEQUENCE: doDeleteAISequence(msg, p); continue;
		}

retrycmd:
		uint haltloops = 0;

		while (halting && ++haltloops < static_cast<uint>(FIVE_MIN_TIMEOUT.tv_sec))
			sleep(1);

		slaveLock.lock();
		if (haltloops == FIVE_MIN_TIMEOUT.tv_sec) {
			ostringstream os;
			os << "A node is unresponsive for cmd = " << (uint32_t)cmd <<
				", no reconfigure in at least " << 
				FIVE_MIN_TIMEOUT.tv_sec << " seconds.  Setting read-only mode.";
			log(os.str());
			readOnly = true;
			halting = false;
		}	
	
		if (readOnly) {
			SEND_ALARM
			slaveLock.unlock();
			sendError(p->sock, ERR_READONLY);
			goto out;
		}

		/* TODO: Separate these out-of-band items into separate functions */

		/* Need to get the dbroot, convert to vbOID */
		if (cmd == BEGIN_VB_COPY) {
			try {
				boost::mutex::scoped_lock lk(oidsMutex);
				uint8_t *buf = msg.buf();

				// dbroot is currently after the cmd and transid
				uint16_t *dbRoot = (uint16_t *) &buf[1+4];

				// If that dbroot has no vboid, create one
				int16_t err;
				err = oids.getVBOIDOfDBRoot(*dbRoot);
				//cout << "dbRoot " << *dbRoot << " -> vbOID " << err << endl;
				if (err < 0) {
					err = oids.allocVBOID(*dbRoot);
				//	cout << "  - allocated oid " << err << endl;
				}
				*dbRoot = err;
			}
			catch (exception& ex) {
				ostringstream os;
				os << "DBRM Controller: Begin VBCopy failure. " << ex.what();
				log(os.str());
				sendError(p->sock, -1);
				goto out;
			}
		}

		/* Check for deadlock on beginVBCopy */
/*		if (cmd == BEGIN_VB_COPY) {
			ByteStream params(msg);
			VER_t txn;
			uint8_t tmp8;
			uint32_t tmp32;
			LBIDRange_v ranges;
			LBIDRange_v::iterator it;
			LBID_t end;

			params >> tmp8;   //throw away the cmd
			params >> tmp32;
			txn = tmp32;
			deserializeVector(params, ranges);
			for (it = ranges.begin(); it != ranges.end(); it++) {
				end = (*it).start + (*it).size - 1;
				err = rg->reserveRange((*it).start, end, txn, slaveLock);
				if (err != ERR_OK) {
					pthread_mutex_unlock(&slaveLock);
					sendError(p->sock, err);
					goto out;
				}
			}
		}
*/
		/* Release all blocks of lbids on vbRollback or vbCommit */
		if (cmd == VB_ROLLBACK1 || cmd == VB_ROLLBACK2 || cmd == VB_COMMIT) {
			ByteStream params(msg);
			VER_t txn;
			uint8_t tmp8;
			uint32_t tmp32;

			params >> tmp8;
			params >> tmp32;
			txn = tmp32;
			rg->releaseResources(txn);
		}

	/* XXXPAT: If beginVBCopy, vbRollback, or vbCommit fail for some reason,
	   the resource graph will be out of sync with the copylocks and/or vss.
	   There are 2 reasons they might fail:
			1) logical inconsistency between the resource graph and the 
			copylocks & vss
			2) "out of band" failures, like a network problem
	*/

		/* Need to atomically do the safety check and the clear. */
		if (cmd == BRM_CLEAR) {
			uint txnCount = sm.getTxnCount();
			// do nothing if there's an active transaction
            if (txnCount != 0) {
				ByteStream *reply = new ByteStream();
				*reply << (uint8_t) ERR_FAILURE;
				responses.push_back(reply);
				goto no_confirm;
			}
		}

		try {
			distribute(&msg);
		}
		catch (...) {
			if (!halting) {
				SEND_ALARM
				undo();
				readOnly = true;
				slaveLock.unlock();
				ostringstream ostr;
				ostr << "DBRM Controller: Caught network error.  "
					"Sending command " << (uint32_t)cmd <<
					", length " << msg.length() << ".  Setting read-only mode.";
				log(ostr.str());
				sendError(p->sock, ERR_NETWORK);
				goto out;
			}
		}

#ifdef BRM_VERBOSE
		cerr << "DBRM Controller: distributed msg" << endl;
#endif

		bool readErrFlag; // ignore this flag in this case
		err = gatherResponses(cmd, msg.length(), &responses, readErrFlag);
		
#ifdef BRM_VERBOSE
		cerr << "DBRM Controller: got responses" << endl;
#endif

		CHECK_ERROR1(err)
		err = compareResponses(cmd, msg.length(), responses);
#ifdef BRM_VERBOSE
		cerr << "DBRM Controller: compared responses" << endl;
#endif

#ifdef BRM_DEBUG
		if ((cmd == BEGIN_VB_COPY || cmd == VB_ROLLBACK1 || cmd == VB_ROLLBACK2 ||
			cmd == VB_COMMIT) && err == -1)
			cerr << "DBRM Controller: inconsistency detected between the resource graph and the VSS or CopyLocks logic." << endl;
#endif

		// these command will have error message carried in the response
		if (!responses.empty() && (cmd == DELETE_PARTITION || cmd == MARK_PARTITION_FOR_DELETION || cmd == RESTORE_PARTITION) 
			 && err)
		{
			if (err != ERR_PARTITION_DISABLED && err != ERR_PARTITION_ENABLED && 
				  err != ERR_INVALID_OP_LAST_PARTITION && err != ERR_NOT_EXIST_PARTITION &&
				  err != ERR_NO_PARTITION_PERFORMED)
				undo();
			//goto no_confirm;
		}
		else 
		{ 
  		CHECK_ERROR1(err)
  	}

		// these cmds don't need the 2-phase commit
		if (cmd == FLUSH_INODE_CACHES || cmd == BRM_CLEAR || cmd == TAKE_SNAPSHOT)
			goto no_confirm;

#ifdef BRM_VERBOSE
		cerr << "DBRM Controller: sending confirmation" << endl;
#endif
		try {
			confirm();
		}
		catch (...) { 
			if (!halting) {
				SEND_ALARM
				ostringstream ostr;
				ostr << "DBRM Controller: Caught network error.  "
					"Confirming command " << (uint32_t)cmd <<
					", length " << msg.length() << ".  Setting read-only mode.";
				log(ostr.str());
				readOnly = true;
			}
		}

no_confirm:
		slaveLock.unlock();

		try {
			p->sock->write(*(responses.front()));
		}
		catch (...) {
			p->sock->close();
			log("DBRM Controller: Warning: could not send the reply to a command", logging::LOG_TYPE_WARNING);
		}

out:
		for (it = responses.begin(); it != responses.end(); it++)
			delete *it;
		responses.clear();
	}

#ifdef BRM_VERBOSE
	cerr << "DBRM Controller: closing connection" << endl;
#endif
	THREAD_EXIT
	return;
}

void MasterDBRMNode::distribute(ByteStream *msg)
{
	uint i;

	for (i = 0, iSlave = slaves.begin(); iSlave != slaves.end() && !halting; iSlave++, i++)
		try {
			(*iSlave)->write(*msg);
		}
		catch (exception&) {
			if (!halting) {
				ostringstream os;
				os << "DBRM Controller: network error distributing command to worker " <<
					i + 1 << endl;
				log(os.str());
				throw;
			}
		}
}

// readErrFlag is a separate return flag used by doChangeTableLockOwner()
// (or any subsequent function) which calls gatherResponses() outside the
// scope of msgProcessor() which instead uses the halting flag for error
// handling.
int MasterDBRMNode::gatherResponses(uint8_t cmd,
	uint32_t cmdMsgLength,
	vector<ByteStream*>* responses,
	bool& readErrFlag) throw()
{
	int i;
	ByteStream *tmp=0;
	readErrFlag = false;

		//Bug 2258 gather all responses
	int error = 0;

	for (i = 0, iSlave = slaves.begin(); iSlave != slaves.end() && !halting; iSlave++, i++) {
		tmp = new ByteStream();
		try {
			// can't just block for 5 mins
			timespec newtimeout = {10, 0};
			uint ntRetries = FIVE_MIN_TIMEOUT.tv_sec/newtimeout.tv_sec;
			uint retries = 0;

			while (++retries < ntRetries && tmp->length() == 0 && !halting)
				*tmp = (*iSlave)->read(&newtimeout);
			//*tmp = (*iSlave)->read();
		}
		catch (...) {
			/* 2/21/12 - instead of setting readonly here, to support a peaceful failover
			we will wait for a configuration change to come, then report the error
			after a long timeout.
			*/

            ostringstream os;
            os << "DBRM Controller: Network error reading from node " << i + 1 <<
                ".  Reading response to command " << (uint32_t)cmd <<
                ", length " << cmdMsgLength << ".  Will see if retry is possible.";
            log(os.str());

            halting = true;
			readErrFlag = true;
			delete tmp;
			return ERR_OK;

			/*
			ostringstream os;
			if (!halting) {
				SEND_ALARM
				readOnly = true;
				os << "DBRM Controller: Network error reading from node " << i + 1 <<
					".  Reading response to command " << (uint32_t)cmd <<
					", length " << cmdMsgLength << ".  Setting read-only mode.";
				log(os.str());
				error++;
			}
			*/
		}

		if (tmp->length() == 0 && !halting) {
			/* See the comment above */
            ostringstream os;
            os << "DBRM Controller: Network error reading from node " << i + 1<<
                ".  Reading response to command " << (uint32_t)cmd <<
                ", length " << cmdMsgLength << 
                ".  0 length response, possible time-out"
                ".  Will see if retry is possible.";
            log(os.str());
			halting = true;
			readErrFlag = true;
			delete tmp;
			return ERR_OK;

			/*
			ostringstream os;

			SEND_ALARM;

			readOnly = true;
			os << "DBRM Controller: Network error reading from node " << i + 1<<
				".  Reading response to command " << (uint32_t)cmd <<
				", length " << cmdMsgLength << 
				".  0 length response, possible time-out"
				".  Setting read-only mode.";
			log(os.str());
			error++;
			*/
		}
		if ( error == 0 )
			responses->push_back(tmp);
		else
			delete tmp;
	}
	if ( error > 0 )
		return ERR_NETWORK;
	else
		return ERR_OK;
}
	
int MasterDBRMNode::compareResponses(uint8_t cmd,
	uint32_t cmdMsgLength,
	const vector<ByteStream *>& responses) const
{
	vector<ByteStream *>::const_iterator it, it2;
	uint8_t errCode;
	ByteStream *first;
	int i;
	
	first = responses.front();
	try {
		first->peek(errCode);
	}
	catch (exception&) {
		SEND_ALARM

		readOnly = true;
		ostringstream os;
		os << "DBRM Controller: Network error on node 1.  "
			"Verifying response to command " << (uint32_t)cmd <<
			", length " << cmdMsgLength << ".  Reverting to read-only mode.";
		log(os.str());
		return ERR_NETWORK;
	}

	/*if (errCode != ERR_OK) {
#ifdef BRM_VERBOSE
		cerr << "DBRM Controller: first response has error code " << errCode << endl;
#endif
		return errCode;
	}*/
	
	for (it = responses.begin(), it2 = it + 1, i = 2; it2 != responses.end(); it++, it2++, i++)
		if (**it != **it2 && !halting) {
			SEND_ALARM

			ostringstream ostr;
#ifdef BRM_VERBOSE
			cerr << "DBRM Controller: error: response from node " << i << " is different" << endl;
#endif
 			readOnly = true;
			ostr << "DBRM Controller: image inconsistency detected at node " << i << 
				".  Verifying response to command " << (uint32_t)cmd <<
				", length " << cmdMsgLength << ".  Setting read-only mode.";
			log(ostr.str());
			return ERR_SLAVE_INCONSISTENCY;
		}

	//return ERR_OK;
	return errCode;
}
		
void MasterDBRMNode::undo() throw()
{
	vector<MessageQueueClient *>::iterator it, lastSlave;
	ByteStream undoMsg;
	int i;

#ifdef BRM_VERBOSE
	cerr << "DBRM Controller: sending undo()" << endl;
#endif

	undoMsg << (uint8_t) BRM_UNDO;
	if (iSlave != slaves.end())
		lastSlave = iSlave + 1; //@Bug 2258 
	else
		lastSlave = iSlave;

 	for (it = slaves.begin(), i = 1; it != lastSlave; it++, i++) {
		try {
#ifdef BRM_VERBOSE
			cerr << "DBRM Controller: sending undo() to worker number " << i <<  endl;
#endif
			(*it)->write(undoMsg);
		}
		catch (...) {
			ostringstream ostr;

			ostr << "DBRM Controller: undo(): warning, could not contact worker number " 
				<< i << endl;
			log(ostr.str());
		}
	}
}

void MasterDBRMNode::confirm()
{
	ByteStream confirmMsg;

	confirmMsg << CONFIRM;
	distribute(&confirmMsg);
}

void MasterDBRMNode::finalCleanup()
{
	vector<MessageQueueClient *>::iterator sIt;
	int retry = 0;

 	cerr << "DBRM Controller: Waiting for threads to finish..." << endl;

	delete rg;  //unblocks any waiting transactions
	rg = NULL;

	// XXXPAT: assumption here: join_all() blocks until all threads are joined
	// which implies the case where all threads are removed from the group.  
	// We're relying on that second condition for synchronization here.
	// Problem: it looks as if join_all holds a mutex which prevents other calls
	// on dbrmSessions, so threads can't be removed from the group.
//  	dbrmSessions.join_all();  
	// blah: busy wait for now, max 15 seconds, then assume everything's dead.
	// @bug 1381: change retry from 15 to 5 more than the socket read() timeout.
	while (runners > 0 && retry++ < (MSG_TIMEOUT.tv_sec + 5))
		sleep(1);

#ifdef BRM_VERBOSE
	cerr << "Closing connections" << endl;
#endif
	for (sIt = slaves.begin(); sIt != slaves.end(); sIt++) {
		(*sIt)->shutdown();
		delete *sIt;
	}
 	slaves.clear();

	/* Close any connections that lost their thread somehow.  This should "never" happen.
	*/

#if 0    // if we see instances of blockage here flip this switch
	int tmp; 
	tmp = pthread_mutex_trylock(&mutex);
	if (tmp != 0) {   // try one more time then steal the lock TODO: why is this necessary?
		sleep(1);
		tmp = pthread_mutex_trylock(&mutex);
		cerr << "DBRM Controller: warning!  Trying to recover mutex!\n";
		if (tmp != 0) {
			pthread_mutex_unlock(&mutex);
			sleep(5);  // let the other threads finish
			tmp = pthread_mutex_trylock(&mutex);
			if (tmp != 0) {
				cerr << "DBRM Controller: error!  Bad mutex state, going down!\n";
				exit(1);
			}
		}
	}
#else
	mutex.lock();
#endif

#ifdef BRM_VERBOSE
	if (activeSessions.size() > 0)
		cerr << "There are still live sessions\n";
#endif
	for (uint i = 0; i < activeSessions.size(); ++i) {
		activeSessions[i]->close();
		delete activeSessions[i];
	}
	activeSessions.clear();
	mutex.unlock();

	serverLock.lock();
	delete dbrmServer;
	dbrmServer = NULL;
	serverLock.unlock();
}

void MasterDBRMNode::sendError(IOSocket *caller, uint8_t err) const throw()
{
	ByteStream msg;

#ifdef BRM_VERBOSE
	cerr << "DBRM Controller: returning " << (int) err << " to the caller" << endl;
#endif
	msg << err;
	try {
		caller->write(msg);
	} 
	catch (exception&) {
		log("DBRM Controller: failed to send return code", logging::LOG_TYPE_WARNING);
#ifdef BRM_VERBOSE
		cerr << "DBRM Controller: failed to send return code" << endl;
#endif
	}
}

void MasterDBRMNode::doHalt(messageqcpp::IOSocket *sock)
{
	ByteStream reply;

	log("Halting...", LOG_TYPE_INFO);
	halting = true;
	lock();
	log("Halted", LOG_TYPE_INFO);
	reply << (uint8_t) ERR_OK;
	try {
		sock->write(reply);
	}
	catch (exception&) { }
}	

void MasterDBRMNode::doResume(messageqcpp::IOSocket *sock)
{
	ByteStream reply;

	unlock();
	halting = false;
	log("Resuming", LOG_TYPE_INFO);
	reply << (uint8_t) ERR_OK;
	try {
		sock->write(reply);
	}
	catch (exception&) { }
}

void MasterDBRMNode::doSetReadOnly(messageqcpp::IOSocket *sock, bool b)
{
	ByteStream reply;

	setReadOnly(b);
	reply << (uint8_t) ERR_OK;
	try {
		sock->write(reply);
	}
	catch (exception&) { }
}

void MasterDBRMNode::doGetReadOnly(messageqcpp::IOSocket *sock)
{
	ByteStream reply;

	reply << (uint8_t)isReadOnly();
	try {
		sock->write(reply);
	}
	catch (exception&) { }
}

void MasterDBRMNode::doReload(messageqcpp::IOSocket *sock)
{
	/* This version relies on the caller to do a 'halt' and 'resume' around
	 * the 'reload' call, but it is synchronous.  When reload() returns
	 * the new workernode connections have been established.
	 */

	ByteStream reply;
	string stmp;
	int ltmp;
	char ctmp[50];
	int i;
	config::Config *config = config::Config::makeConfig();

	log("Reloading", LOG_TYPE_INFO);

	stmp = config->getConfig("DBRM_Controller", "NumWorkers");
	if (stmp.length() == 0) {
		reply << (uint8_t) ERR_FAILURE;
		try {
			sock->write(reply);
		}
		catch (exception&) { }
		throw runtime_error("MasterDBRMNode::doReload(): config file error looking for <DBRM_Controller><NumWorkers>");
	}

	ltmp = static_cast<int>(config::Config::fromText(stmp));
	if (ltmp < 1) {
		reply << (uint8_t) ERR_FAILURE;
		try {
			sock->write(reply);
		}
		catch (exception&) { }
		throw runtime_error("MasterDBRMNode::doReload(): Bad NumWorkers value");
	}

	for (i = 0; i < (int) slaves.size(); i++) {
		slaves[i]->shutdown();
		delete slaves[i];
	}
	slaves.clear();

	NumWorkers = ltmp;

	for (i = 1; i <= NumWorkers; i++) {
		snprintf(ctmp, 50, "DBRM_Worker%d", i);
		slaves.push_back(new MessageQueueClient(ctmp, config));
	}
	
	iSlave = slaves.end();
	undo();

	readOnly = false;

	reply << (uint8_t) ERR_OK;
	try {
		sock->write(reply);
	}
	catch (exception&) { }

/*  Asynchronous version
	ByteStream reply;

	reply << (uint8_t) ERR_OK;
	try {
		sock->write(reply);
	}
	catch (exception&) { }
// 	sock->close();

	reloadCmd = true;
*/
}	

void MasterDBRMNode::doVerID(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	QueryContext context;

	context = sm.verID();
#ifdef BRM_VERBOSE
	cerr << "doVerID returning " << ver << endl;
#endif

	reply << (uint8_t) ERR_OK;
	reply << context;
	try {
		p->sock->write(reply);
	}
	catch (exception&) { }
}

void MasterDBRMNode::doSysCatVerID(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	QueryContext context;

	context = sm.sysCatVerID();
#ifdef BRM_VERBOSE
	cerr << "doSysCatVerID returning " << ver << endl;
#endif

	reply << (uint8_t) ERR_OK;
	reply << context;
	try {
		p->sock->write(reply);
	}
	catch (exception&) { }
}

void MasterDBRMNode::doNewTxnID(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	TxnID txnid;
	uint32_t sessionID;
	uint8_t block, cmd, isDDL;
	
	try {
		msg >> cmd;
		msg >> sessionID;
		msg >> block;
		msg >> isDDL;
		txnid = sm.newTxnID(sessionID, (block!=0), (isDDL!=0));
		reply << (uint8_t) ERR_OK;
		reply << (uint32_t) txnid.id;
		reply << (uint8_t) txnid.valid;
#ifdef BRM_VERBOSE
		cerr << "newTxnID returning id=" << txnid.id << " valid=" << txnid.valid << endl;
#endif
	}
	catch (exception&) {
		reply.reset();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}

	try {
		p->sock->write(reply);
	}
	catch (exception&) { }
}

void MasterDBRMNode::doCommitted(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	TxnID txnid;
	uint8_t cmd, tmp;
	uint32_t tmp32;

	try {
		msg >> cmd;
		msg >> tmp32;
		txnid.id = tmp32;
		msg >> tmp;
		txnid.valid = (tmp != 0 ? true : false);	
#ifdef BRM_VERBOSE
		cerr << "doCommitted" << endl;
#endif
		sm.committed(txnid);
	}
	catch (exception&) {
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}
	
	reply << (uint8_t) ERR_OK;
	try {
		p->sock->write(reply);
	}
	catch (...) { }
}

void MasterDBRMNode::doRolledBack(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	TxnID txnid;
	uint8_t cmd, tmp;
	uint32_t tmp32;

	try {
		msg >> cmd;
		msg >> tmp32;
		msg >> tmp;

		txnid.id = tmp32;
		txnid.valid = (tmp != 0 ? true: false);
#ifdef BRM_VERBOSE
		cerr << "doRolledBack" << endl;
#endif
		sm.rolledback(txnid);
	}
	catch (exception&) {
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}

	reply << (uint8_t) ERR_OK;
	try {
		p->sock->write(reply);
	}
	catch (...) { }
}


void MasterDBRMNode::doGetTxnID(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	SessionManagerServer::SID sid;
	TxnID txnid;
	uint8_t cmd;

	try {
		msg >> cmd;
		msg >> sid;

		txnid = sm.getTxnID(sid);
#ifdef BRM_VERBOSE
		cerr << "doGetTxnID returning id=" << txnid.id << " valid=" << 
			txnid.valid << endl;
#endif
	}
	catch (exception&) {
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}

	reply << (uint8_t) ERR_OK;
	reply << (uint32_t) txnid.id;
	reply << (uint8_t) txnid.valid;
	try {
		p->sock->write(reply);
	}
	catch (...) { }
}

void MasterDBRMNode::doSIDTIDMap(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	int len, i;
	boost::shared_array<SIDTIDEntry> entries;

	try {
		entries = sm.SIDTIDMap(len);
	}
	catch (exception&) {
		reply << (uint8_t) ERR_FAILURE;
		try { 
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}

	reply << (uint8_t) ERR_OK;
	reply << (uint32_t) len;

#ifdef BRM_VERBOSE
	cerr << "doSIDTIDMap returning " << len << " elements... " << endl;
#endif
	for (i = 0; i < len; i++) {
#ifdef BRM_VERBOSE
		cerr << "   " << i << ": txnid=" << entries[i].txnid.id << " valid=" 
			<< entries[i].txnid.valid << " sessionid=" << entries[i].sessionid 
			<< endl;
#endif
		reply << (uint32_t) entries[i].txnid.id << (uint8_t) entries[i].txnid.valid << 
			(uint32_t) entries[i].sessionid;
	}
	
	try {
		p->sock->write(reply);
	}
	catch (...) { }
}

void MasterDBRMNode::doGetUniqueUint32(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	uint32_t ret;

	try {
		ret = sm.getUnique32();
		reply << (uint8_t) ERR_OK;
		reply << ret;
#ifdef BRM_VERBOSE
		cerr << "getUnique32() returning " << ret << endl;
#endif
	}
	catch (exception&) {
		reply.reset();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}

	try {
		p->sock->write(reply);
	}
	catch (exception&) { }
}

void MasterDBRMNode::doGetUniqueUint64(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	uint64_t ret;

	try {
		ret = sm.getUnique64();
		reply << (uint8_t) ERR_OK;
		reply << ret;
#ifdef BRM_VERBOSE
		cerr << "getUnique64() returning " << ret << endl;
#endif
	}
	catch (exception&) {
		reply.reset();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}

	try {
		p->sock->write(reply);
	}
	catch (exception&) { }
}

void MasterDBRMNode::doGetSystemState(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	uint32_t ss = 0;
	ByteStream::byte err = ERR_FAILURE;

	try {
		sm.getSystemState(ss);
		err = ERR_OK;
		reply << err;
		reply << static_cast<ByteStream::quadbyte>(ss);
#ifdef BRM_VERBOSE
		cerr << "getSystemState() returning " << static_cast<int>(err) << endl;
#endif
	}
	catch (exception&) {
		reply.reset();
		err = ERR_FAILURE;
		reply << err;
		try {
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}

	try {
		p->sock->write(reply);
	}
	catch (exception&) { }
}

void MasterDBRMNode::doSetSystemState(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	ByteStream::byte cmd;
	ByteStream::byte err = ERR_FAILURE;
	uint32_t ss;

	try {
		msg >> cmd;
		msg >> ss;

		sm.setSystemState(ss);
#ifdef BRM_VERBOSE
		cerr << "doSetSystemState setting " << hex << ss << dec << endl;
#endif
	}
	catch (exception&) {
		err = ERR_FAILURE;
		reply << err;
		try {
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}

	err = ERR_OK;
	reply << err;
	try {
		p->sock->write(reply);
	}
	catch (...) { }
}

void MasterDBRMNode::doClearSystemState(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	ByteStream::byte cmd;
	ByteStream::byte err = ERR_FAILURE;
	uint32_t ss;

	try {
		msg >> cmd;
		msg >> ss;

		sm.clearSystemState(ss);
	}
	catch (exception&) {
		err = ERR_FAILURE;
		reply << err;
		try {
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}

	err = ERR_OK;
	reply << err;
	try {
		p->sock->write(reply);
	}
	catch (...) { }
}

void MasterDBRMNode::doSessionManagerReset(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;

	try {
		sm.reset();
		reply << (uint8_t) ERR_OK;
	}
	catch (...) {
		reply << (uint8_t) ERR_FAILURE;
	}

	try {
		p->sock->write(reply);
	}
	catch (...) { }
}

void MasterDBRMNode::doAllocOIDs(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	int ret;
	uint32_t tmp32;
	int num;
	uint8_t cmd;

	try {
		boost::mutex::scoped_lock lk(oidsMutex);

		msg >> cmd;
		msg >> tmp32;
		num = (int) tmp32;
		ret = oids.allocOIDs(num);
		reply << (uint8_t) ERR_OK;
		reply << (uint32_t) ret;
		p->sock->write(reply);
	}
	catch (exception& ex) {
		ostringstream os;
		os << "DBRM Controller: OID allocation failure. " << ex.what();
		log(os.str());
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		} catch (...) { }
	}
}

void MasterDBRMNode::doReturnOIDs(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	uint8_t cmd;
	uint32_t tmp32;
	int start, end;

	try {
		boost::mutex::scoped_lock lk(oidsMutex);

		msg >> cmd;
		msg >> tmp32;
		start = (int) tmp32;
		msg >> tmp32;
		end = (int) tmp32;
		oids.returnOIDs(start, end);
		reply << (uint8_t) ERR_OK;
		p->sock->write(reply);
	}
	catch (exception& ex) {
		ostringstream os;
		os << "DBRM Controller: Return OIDs failure. " << ex.what();
		log(os.str());
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		} catch (...) { }
	}
}

void MasterDBRMNode::doOidmSize(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	int ret;

	try {
		boost::mutex::scoped_lock lk(oidsMutex);

		ret = oids.size();
		reply << (uint8_t) ERR_OK;
		reply << (uint32_t) ret;
		p->sock->write(reply);
	}
	catch (exception& ex) {
		ostringstream os;
		os << "DBRM Controller: Failure to get OID count. " << ex.what();
		log(os.str());
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		} catch (...) { }
	}
}

void MasterDBRMNode::doAllocVBOID(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	uint32_t dbroot;
	uint32_t ret;
	uint8_t cmd;

	msg >> cmd;
	try {
		boost::mutex::scoped_lock lk(oidsMutex);

		msg >> dbroot;
		ret = oids.allocVBOID(dbroot);
		reply << (uint8_t) ERR_OK;
		reply << ret;
		p->sock->write(reply);
	}
	catch (exception& ex) {
		ostringstream os;
		os << "DBRM Controller: VB OID allocation failure. " << ex.what();
		log(os.str());
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		} catch (...) { }
	}
}

void MasterDBRMNode::doGetDBRootOfVBOID(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	uint32_t vbOID;
	uint32_t ret;
	uint8_t cmd;

	msg >> cmd;
	try {
		boost::mutex::scoped_lock lk(oidsMutex);

		msg >> vbOID;
		ret = oids.getDBRootOfVBOID(vbOID);
		reply << (uint8_t) ERR_OK;
		reply << ret;
		p->sock->write(reply);
	}
	catch (exception& ex) {
		ostringstream os;
		os << "DBRM Controller: Get DBRoot of VB OID failure. " << ex.what();
		log(os.str());
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		} catch (...) { }
	}
}

void MasterDBRMNode::doGetVBOIDToDBRootMap(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	uint8_t cmd;

	msg >> cmd;
	try {
		boost::mutex::scoped_lock lk(oidsMutex);

		const vector<uint16_t> &ret = oids.getVBOIDToDBRootMap();
		reply << (uint8_t) ERR_OK;
		serializeInlineVector<uint16_t>(reply, ret);
		p->sock->write(reply);
	}
	catch (exception& ex) {
		ostringstream os;
		os << "DBRM Controller: Get VB OID DBRoot map failure. " << ex.what();
		log(os.str());
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		} catch (...) { }
	}
}


void MasterDBRMNode::doGetTableLock(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	TableLockInfo tli;
	uint64_t id;
	ByteStream reply;

	msg >> cmd;

	try {
		msg >> tli;
		idbassert(msg.length() == 0);
		id = tableLockServer->lock(&tli);
		reply << (uint8_t) ERR_OK;
		reply << id;
		if (id == 0) {
			reply << tli.ownerPID;
			reply << tli.ownerName;
			reply << (uint32_t) tli.ownerSessionID;
			reply << (uint32_t) tli.ownerTxnID;
		}
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doReleaseTableLock(ByteStream &msg, ThreadParams *p)
{
	uint64_t id;
	uint8_t cmd;
	bool ret;
	ByteStream reply;

	msg >> cmd;

	try {
		msg >> id;
		idbassert(msg.length() == 0);
		ret = tableLockServer->unlock(id);
		reply << (uint8_t) ERR_OK;
		reply << (uint8_t) ret;
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doChangeTableLockState(ByteStream &msg, ThreadParams *p)
{
	uint64_t id;
	uint32_t tmp32;
	uint8_t cmd;
	LockState state;
	bool ret;
	ByteStream reply;

	msg >> cmd;
	try {
		msg >> id;
		msg >> tmp32;
		idbassert(msg.length() == 0);
		state = (LockState) tmp32;
		ret = tableLockServer->changeState(id, state);
		reply << (uint8_t) ERR_OK;
		reply << (uint8_t) ret;
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doChangeTableLockOwner(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	uint64_t id;
	string name;
	uint pid;
	ByteStream reply;
	ByteStream workerNodeCmd;
	uint32_t tmp32;
	int32_t sessionID;
	int32_t txnID;
	bool ret;
	vector<ByteStream *> responses;
	int err;

	// current owner vars
	TableLockInfo tli;
	string processName;
	string::size_type namelen;
	bool exists;

	try {
		msg >> cmd >> id >> name >> pid >> tmp32;
		sessionID = tmp32;
		msg >> tmp32;
		txnID = tmp32;
		idbassert(msg.length() == 0);

		/* get the current owner info
		 * send cmd to look for the owner
		 * if there's an existing owner, reject the request
		 */

		ret = tableLockServer->getLockInfo(id, &tli);
		if (!ret) {
			reply << (uint8_t) ERR_OK << (uint8_t) ret;
			goto write;
		}

		namelen = tli.ownerName.find_first_of(" ");
		if (namelen == string::npos)
			processName = tli.ownerName;
		else
			processName = tli.ownerName.substr(0, namelen);
		workerNodeCmd << (uint8_t) OWNER_CHECK << processName << tli.ownerPID;
		bool readErrFlag;
		{
			boost::mutex::scoped_lock lk(slaveLock);
			distribute(&workerNodeCmd);
			err = gatherResponses(OWNER_CHECK, workerNodeCmd.length(), &responses,
				readErrFlag);
		}
		if ((err != ERR_OK) || (readErrFlag)) {
			reply << (uint8_t) ERR_FAILURE;
			goto write;
		}

		exists = false;
		for (uint i = 0; i < responses.size(); i++) {
			/* Parse msg from worker node */
			uint8_t ret;
			idbassert(responses[i]->length() == 1);
			*(responses[i]) >> ret;
			if (ret == 1)
				exists = true;
			delete responses[i];
		}
		if (exists) {
			reply << (uint8_t) ERR_OK << (uint8_t) false;
			goto write;
		}

		ret = tableLockServer->changeOwner(id, name, pid, sessionID, txnID);
		reply << (uint8_t) ERR_OK << (uint8_t) ret;

write:
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doGetAllTableLocks(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	vector<TableLockInfo> ret;
	ByteStream reply;

	try {
		msg >> cmd;
		idbassert(msg.length() == 0);
		ret = tableLockServer->getAllLocks();
		reply << (uint8_t) ERR_OK;
		serializeVector<TableLockInfo>(reply, ret);
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doReleaseAllTableLocks(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	ByteStream reply;

	try {
		msg >> cmd;
		idbassert(msg.length() == 0);
		tableLockServer->releaseAllLocks();
		reply << (uint8_t) ERR_OK;
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doGetTableLockInfo(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	ByteStream reply;
	uint64_t id;
	TableLockInfo tli;
	bool ret;

	try {
		msg >> cmd >> id;
		idbassert(msg.length() == 0);
		ret = tableLockServer->getLockInfo(id, &tli);
		reply << (uint8_t) ERR_OK << (uint8_t) ret;
		if (ret)
			reply << tli;
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doOwnerCheck(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	uint64_t id;
	ByteStream reply;
	ByteStream workerNodeCmd;
	bool ret;
	vector<ByteStream *> responses;
	int err;

	// current owner vars
	TableLockInfo tli;
	string processName;
	string::size_type namelen;
	bool exists;

	try {
		msg >> cmd >> id;
		idbassert(msg.length() == 0);

		/* get the current owner info
		 * send cmd to look for the owner
		 * if there's an existing owner, reject the request
		 */

		ret = tableLockServer->getLockInfo(id, &tli);
		if (!ret) {
			reply << (uint8_t) ERR_OK << (uint8_t) ret;
			goto write;
		}

		namelen = tli.ownerName.find_first_of(" ");
		if (namelen == string::npos)
			processName = tli.ownerName;
		else
			processName = tli.ownerName.substr(0, namelen);
		workerNodeCmd << (uint8_t) OWNER_CHECK << processName << tli.ownerPID;
		bool readErrFlag;
		{
			boost::mutex::scoped_lock lk(slaveLock);
			distribute(&workerNodeCmd);
			err = gatherResponses(OWNER_CHECK, workerNodeCmd.length(), &responses,
				readErrFlag);
		}
		if ((err != ERR_OK) || (readErrFlag)) {
			reply << (uint8_t) ERR_FAILURE;
			goto write;
		}

		exists = false;
		for (uint i = 0; i < responses.size(); i++) {
			/* Parse msg from worker node */
			uint8_t ret;
			idbassert(responses[i]->length() == 1);
			*(responses[i]) >> ret;
			if (ret == 1)
				exists = true;
			delete responses[i];
		}
		reply << (uint8_t) ERR_OK << (uint8_t) exists;

write:
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doStartAISequence(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	ByteStream reply;
    uint8_t tmp8;
	uint32_t oid, colWidth;
	uint64_t firstNum;
    execplan::CalpontSystemCatalog::ColDataType colDataType;
	try {
		msg >> cmd >> oid >> firstNum >> colWidth >> tmp8;
        colDataType = (execplan::CalpontSystemCatalog::ColDataType)tmp8;
		idbassert(msg.length() == 0);
		aiManager.startSequence(oid, firstNum, colWidth, colDataType);
		reply << (uint8_t) ERR_OK;
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doGetAIRange(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	ByteStream reply;
	uint32_t oid, count;
	uint64_t nextVal;
	bool ret;

	try {
		msg >> cmd >> oid >> count;
		idbassert(msg.length() == 0);
		ret = aiManager.getAIRange(oid, count, &nextVal);
		reply << (uint8_t) ERR_OK << (uint8_t) ret << nextVal;
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doResetAISequence(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	ByteStream reply;
	uint32_t oid;
	uint64_t val;

	try {
		msg >> cmd >> oid >> val;
		idbassert(msg.length() == 0);
		aiManager.resetSequence(oid, val);
		reply << (uint8_t) ERR_OK;
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doGetAILock(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	ByteStream reply;
	uint32_t oid;

	try {
		msg >> cmd >> oid;
		idbassert(msg.length() == 0);
		aiManager.getLock(oid);
		reply << (uint8_t) ERR_OK;
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doReleaseAILock(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	ByteStream reply;
	uint32_t oid;

	try {
		msg >> cmd >> oid;
		idbassert(msg.length() == 0);
		aiManager.releaseLock(oid);
		reply << (uint8_t) ERR_OK;
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

void MasterDBRMNode::doDeleteAISequence(ByteStream &msg, ThreadParams *p)
{
	uint8_t cmd;
	ByteStream reply;
	uint32_t oid;

	try {
		msg >> cmd >> oid;
		idbassert(msg.length() == 0);
		aiManager.deleteSequence(oid);
		reply << (uint8_t) ERR_OK;
		p->sock->write(reply);
	}
	catch (exception&) {
		reply.restart();
		reply << (uint8_t) ERR_FAILURE;
		try {
			p->sock->write(reply);
		}
		catch(...) { }
	}
}

MasterDBRMNode::MsgProcessor::MsgProcessor(MasterDBRMNode *master) : m(master)
{
}

void MasterDBRMNode::MsgProcessor::operator()()
{
	m->runners++;
	try {
		m->msgProcessor();
	}
#ifdef BRM_VERBOSE
	catch (SocketClosed& e) {
		cerr << e.what() << endl;
#else
	catch (SocketClosed& ) {
#endif
	}
	catch (exception& e) {
 		log(e.what());
#ifdef BRM_VERBOSE
		cerr << e.what() << endl;
#endif
	}
	catch (...) { 
		log("caught something that's not an exception", logging::LOG_TYPE_WARNING);
		cerr << "DBRM Controller: caught something that's not an exception" << endl;
	}
	m->runners--;
}

MasterDBRMNode::MsgProcessor::~MsgProcessor()
{
}

}  //namespace
