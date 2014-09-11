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
 * $Id: masterdbrmnode.cpp 1546 2012-04-03 18:32:59Z dcathey $
 *
 ****************************************************************************/

//Major hack to get oam and brm to play nice.
#define OAM_BRM_LEAN_AND_MEAN

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

#ifdef _MSC_VER
#define snprintf _snprintf
#endif

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
	mutex.unlock();
	
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

#ifdef BRM_VERBOSE
		cerr << "DBRM Controller: recv'd a message..." << endl;
#endif
		
		/* Check for an command for the master */
		msg.peek(cmd);
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

		/* Check if the command is for the SessionManager */
		switch (cmd) {
			case VER_ID: doVerID(msg, p); continue;
			case SYSCAT_VER_ID: doSysCatVerID(msg, p); continue;
			case NEW_TXN_ID: doNewTxnID(msg, p); continue;
			case COMMITTED: doCommitted(msg, p); continue;
			case ROLLED_BACK: doRolledBack(msg, p); continue;
			case GET_TXN_ID: doGetTxnID(msg, p); continue;
			case SID_TID_MAP: doSIDTIDMap(msg, p); continue;
			case GET_SHM_CONTENTS: doGetShmContents(msg, p); continue;
			case GET_UNIQUE_UINT32: doGetUniqueUint32(msg, p); continue;
			case SET_TABLE_LOCK: doSetTableLock(msg, p); continue;
			case UPDATE_TABLE_LOCK: doUpdateTableLock(msg, p); continue;
			case GET_TABLE_LOCK: doGetTableLock(msg, p); continue;
			case GET_TABLE_LOCKS: doGetTableLocks(msg, p); continue;
			case GET_SYSTEM_STATE: doGetSystemState(msg, p); continue;
			case SET_SYSTEM_STATE: doSetSystemState(msg, p); continue;
		}

retrycmd:
		uint haltloops = 0;

		while (halting && ++haltloops < FIVE_MIN_TIMEOUT.tv_sec)
			sleep(1);

		slaveLock.lock(); //pthread_mutex_lock(&slaveLock);
		if (haltloops == FIVE_MIN_TIMEOUT.tv_sec) {
			ostringstream os;
			os << "A node is unresponsive, no reconfigure in at least " << FIVE_MIN_TIMEOUT.tv_sec <<
				" seconds.  Setting read-only mode.";
			log(os.str());
			readOnly = true;
			halting = false;
		}

		if (readOnly) {
			SEND_ALARM
			slaveLock.unlock(); //pthread_mutex_unlock(&slaveLock);
			sendError(p->sock, ERR_READONLY);
			goto out;
		}

		/* TODO: Seperate these out-of-band items into seperate functions */

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

		/* Need to atomically do the safety check and the clear */
		if (cmd == CLEAR) {
			const SIDTIDEntry *entries = NULL;
			int len;

			try {
				entries = sm.SIDTIDMap(len);
			}
			catch (exception&) { }
			// do nothing if there's an active transaction
			if (entries == NULL || len != 0) {
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

		err = gatherResponses(cmd, msg.length(), &responses);
		
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

		CHECK_ERROR1(err)

		// these cmds don't need the 2-phase commit
		if (cmd == FLUSH_INODE_CACHES || cmd == CLEAR || cmd == TAKE_SNAPSHOT)
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

int MasterDBRMNode::gatherResponses(uint8_t cmd,
	uint32_t cmdMsgLength,
	vector<ByteStream*>* responses) throw()
{
	int i;
	ByteStream *tmp;
	vector<ByteStream *>::const_iterator it;

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

			halting = true;
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
			halting = true;
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

	if (errCode != ERR_OK) {
#ifdef BRM_VERBOSE
		cerr << "DBRM Controller: first response has error code " << errCode << endl;
#endif
		return errCode;
	}
	
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

	return ERR_OK;
}
		
void MasterDBRMNode::undo() throw()
{
	vector<MessageQueueClient *>::iterator it, lastSlave;
	ByteStream undoMsg;
	int i;

#ifdef BRM_VERBOSE
	cerr << "DBRM Controller: sending undo()" << endl;
#endif

	undoMsg << (uint8_t) UNDO;
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
	int ver;

	ver = sm.verID();
#ifdef BRM_VERBOSE
	cerr << "doVerID returning " << ver << endl;
#endif

	reply << (uint8_t) ERR_OK;
	reply << (uint32_t) ver;
	try {
		p->sock->write(reply);
	}
	catch (exception&) { }
}

void MasterDBRMNode::doSysCatVerID(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	int ver;

	ver = sm.sysCatVerID();
#ifdef BRM_VERBOSE
	cerr << "doSysCatVerID returning " << ver << endl;
#endif

	reply << (uint8_t) ERR_OK;
	reply << (uint32_t) ver;
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
		txnid = sm.newTxnID(sessionID, (block == 0 ? false : true), (isDDL == 0 ? false : true) );
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

void MasterDBRMNode::doSetTableLock(messageqcpp::ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	OID_t tableOID;
	u_int32_t sessionID;
	u_int32_t processID;
	std::string processName;
	bool lock;
	uint8_t cmd;
	int8_t rc = 0;
	uint32_t tmp32;
	uint8_t tmp8;
	try {
		msg >> cmd;
		msg >> tmp32;
		tableOID = tmp32;
		msg >> tmp32;
		sessionID = tmp32;
		msg >> tmp32;
		processID = tmp32;;
		msg >> processName;
		msg >> tmp8;
		lock = (tmp8 != 0);

		rc = sm.setTableLock ( tableOID, sessionID, processID, processName, lock );
#ifdef BRM_VERBOSE
		cerr << "doSetTableLock returning rc=" << rc << endl;
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
	reply << (uint8_t) rc;
	try {
		p->sock->write(reply);
	}
	catch (...) { }
}

void MasterDBRMNode::doUpdateTableLock(messageqcpp::ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	OID_t tableOID;
	u_int32_t processID;
	std::string processName;
	uint8_t cmd;
	int8_t rc = 0;
	uint32_t tmp32;
	try {
		msg >> cmd;
		msg >> tmp32;
		tableOID = tmp32;
		msg >> tmp32;
		processID = tmp32;;
		msg >> processName;

		rc = sm.updateTableLock ( tableOID, processID, processName );
#ifdef BRM_VERBOSE
		cerr << "doUpdateTableLock returning rc=" << rc << endl;
#endif
	}
	catch (exception&) {
		reply << (uint8_t) ERR_FAILURE;
		reply << (uint32_t) processID;
		reply << processName;
		try {
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}

	reply << (uint8_t) ERR_OK;
	reply << (uint8_t) rc;
	
	reply << (uint32_t) processID;
	reply << processName;
	
	try {
		p->sock->write(reply);
	}
	catch (...) { }
}

void MasterDBRMNode::doGetTableLock(messageqcpp::ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	OID_t tableOID;
	u_int32_t processID;
	std::string processName;
	bool lockStatus;
	SessionManagerServer::SID sid;
	uint8_t cmd;
	int8_t rc = 0;
	uint32_t tmp32;
	uint8_t tmp8;
	try {
		msg >> cmd;
		msg >> tmp32;
		tableOID = tmp32;
		msg >> tmp32;
		processID = tmp32;
		msg >> processName;
		msg >> tmp8;
		lockStatus = (tmp8 != 0);

		rc = sm.getTableLockInfo ( tableOID, processID, processName, lockStatus, sid );
#ifdef BRM_VERBOSE
		cerr << "doGetTableLock returning rc=" << rc << endl;
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
	reply <<  (uint8_t) rc;
	reply << (uint32_t) processID;
	reply << processName;
	reply << (uint8_t) lockStatus;
	reply << (uint32_t) sid;

	try {
		p->sock->write(reply);
	}
	catch (...) { }
	
}

void MasterDBRMNode::doGetTableLocks(messageqcpp::ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	uint8_t cmd;
	std::vector<SIDTIDEntry> sidTidEntries;
	try {
		msg >> cmd;
		sidTidEntries = sm.getTableLocksInfo ();
#ifdef BRM_VERBOSE
		cerr << "doGetTableLocks returning " <<  sidTidEntries.size() << " entries. " << endl;
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
	reply << (uint8_t) sidTidEntries.size();
	for ( uint8_t i = 0; i < sidTidEntries.size(); i++ )
	{
		reply << (uint32_t) sidTidEntries[i].txnid.id;
		reply << (uint8_t) sidTidEntries[i].txnid.valid;
		reply << (uint32_t) sidTidEntries[i].sessionid;
		reply << (uint32_t) sidTidEntries[i].tableOID;
		reply << (uint32_t) sidTidEntries[i].processID;
		reply << sidTidEntries[i].processName;
	}
	try {
		p->sock->write(reply);
	}
	catch (...) { }
	
}

void MasterDBRMNode::doSIDTIDMap(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	int len, i;
	const SIDTIDEntry *entries;

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
		reply << (uint32_t) entries[i].tableOID << (uint32_t) entries[i].processID <<
			string(entries[i].processName);
	}
	
	delete [] entries;
	try {
		p->sock->write(reply);
	}
	catch (...) { }
}



void MasterDBRMNode::doGetShmContents(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	int len;
	char *shm;

	try {
		shm = sm.getShmContents(len);
	}
	catch(exception&) {
		reply << (uint8_t) ERR_FAILURE;
		try { 
			p->sock->write(reply);
		}
		catch (...) { }
		return;
	}

	reply << (uint8_t) ERR_OK;
	reply.append((uint8_t *)shm, len);
	delete [] shm;
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

void MasterDBRMNode::doGetSystemState(ByteStream &msg, ThreadParams *p)
{
	ByteStream reply;
	int ret;
	int ss = SessionManagerServer::SS_NOT_READY;
	ByteStream::byte err = ERR_FAILURE;

	try {
		ret = sm.getSystemState(ss);
		if (ret == 0) err = ERR_OK;
		reply << err;
		reply << static_cast<ByteStream::byte>(ss);
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
	int rc = 0;
	ByteStream::byte tmp8;
	ByteStream::byte err = ERR_FAILURE;
	int ss;

	try {
		msg >> cmd;
		msg >> tmp8;
		ss = static_cast<int>(tmp8);

		rc = sm.setSystemState(ss);
#ifdef BRM_VERBOSE
		cerr << "doSetSystemState returning rc=" << rc << endl;
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

	if (rc == 0) err = ERR_OK;
	reply << err;
	try {
		p->sock->write(reply);
	}
	catch (...) { }
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
	catch (SocketClosed&) {
#ifdef BRM_VERBOSE
		cerr << e.what() << endl;
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
