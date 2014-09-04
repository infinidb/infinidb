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

/******************************************************************************
 * $Id: masterdbrmnode.h 1705 2012-09-19 18:48:45Z dhall $
 *
 *****************************************************************************/

/** @file 
 * class MasterDBRMNode interface
 */

#ifndef MASTERDBRMNODE_H_
#define MASTERDBRMNODE_H_

#include <boost/thread.hpp>

#include <stdint.h>
#include "brmtypes.h"
#include "lbidresourcegraph.h"
#include "messagequeue.h"
#include "bytestream.h"
#include "configcpp.h"
#include "sessionmanagerserver.h"
#include "oidserver.h"
#include "tablelockserver.h"
#include "autoincrementmanager.h"

namespace BRM {

/** @brief The Master node of the DBRM system.
 *
 * There are 3 components of the Distributed BRM (DBRM).
 * \li The interface
 * \li The Master node
 * \li Slave nodes
 *
 * The DBRM components effectively implement a networking & synchronization
 * layer to the BlockResolutionManager class so that every node that needs
 * BRM data always has an up-to-date copy of it locally.  An operation that changes
 * BRM data is duplicated on all hosts that run a Slave node so that every
 * node has identical copies.  All "read" operations are satisfied locally.
 *
 * The MasterDBRMNode class implements the Master node.  All changes to BRM
 * data are serialized and distributed through the Master node.
 *
 * The Master node requires configuration file entries for itself and
 * every slave it should connect to.
 *
 * \code
 * <DBRM_Controller>
 * 	<IPAddr>
 *	<Port>
 * 	<NumWorkers>N</NumWorkers>
 * </DBRM_Controller>
 * <DBRM_Worker1>
 *	<IPAddr>
 *	<Port>
 * </DBRM_Worker1>
 *	...
 * <DBRM_WorkerN>
 *	<IPAddr>
 *	<Port>
 * </DBRM_WorkerN>
 * \endcode
 */	

class MasterDBRMNode
{
public:
	MasterDBRMNode();
	~MasterDBRMNode();

	/** @brief The primary function of the class.
	 * 
	 * The main loop of the master node.  It accepts connections from the DBRM
	 * class, receives commands, and distributes them to each slave.  It returns
	 * only after stop() or the destructor is called by another thread.
	 */
	void run();

	/** @brief Tells the Master to shut down cleanly.
	 * 
	 * Tells the Master to shut down cleanly.
	 */
	void stop();

	/** @brief Effectively makes the whole DBRM system stop.
	 *
	 * Grabs a lock that effectively halts all further BRM data changes.
	 * @warning Use with care.  It's basically an accessor to a raw pthread_mutex.
	 */
	void lock();
	
	/** @brief Resumes DBRM functionality.
	 *
	 * Releases a lock that allows the DBRM to continue processing changes.
	 * @warning Use with care.  It's basically an accessor to a raw pthread_mutex.
	 */
	void unlock();

	/** @brief Reload the config file and reconnect to all slaves.
	 *
	 * Drops all existing connections, reloads the config file and
	 * reconnects with all slaves.
	 * @note Doesn't work yet.  Redundant anyway.
	 */
	void reload();

	/** @brief Sets either read/write or read-only mode
	 * 
	 * Sets either read/write or read-only mode.  When in read-only mode
	 * all BRM change requests will return ERR_READONLY immediately.
	 * @param ro true specifies read-only, false specifies read/write
	 */
	void setReadOnly(bool ro);

	/** @brief Returns true if the Master is in read-only mode, false if in RW mode.
	 * 
	 * @returns true if the Master is in read-only mode, false if in read-write 
	 * mode
	 */
	bool isReadOnly() const { return readOnly; }

private:

	class MsgProcessor {
		public:
			MsgProcessor(MasterDBRMNode *master);
			~MsgProcessor();
			void operator()();
		private:
			MasterDBRMNode *m;
	};

	struct ThreadParams {
		messageqcpp::IOSocket *sock;
		boost::thread *t;
	};


	MasterDBRMNode(const MasterDBRMNode &m);
	MasterDBRMNode& operator=(const MasterDBRMNode &m);

	void initMsgQueues(config::Config *config);
	void msgProcessor();
	void distribute(messageqcpp::ByteStream *msg);
	void undo() throw();
	void confirm();
	void sendError(messageqcpp::IOSocket *dest, uint8_t err) const throw();
	int gatherResponses(uint8_t cmd, uint32_t msgCmdLength,
		std::vector<messageqcpp::ByteStream *>* responses,
		bool& readErrFlag) throw();
	int compareResponses(uint8_t cmd, uint32_t msgCmdLength,
		const std::vector <messageqcpp::ByteStream *>& responses) const;
	void finalCleanup();

	/* Commands the master executes */
	void doHalt(messageqcpp::IOSocket *sock);
	void doResume(messageqcpp::IOSocket *sock);
	void doReload(messageqcpp::IOSocket *sock);
	void doSetReadOnly(messageqcpp::IOSocket *sock, bool b);
	void doGetReadOnly(messageqcpp::IOSocket *sock);
	
	/* SessionManager interface */
	SessionManagerServer sm;
	void doVerID(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doSysCatVerID(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doNewTxnID(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doCommitted(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doRolledBack(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doGetTxnID(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doSIDTIDMap(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doGetShmContents(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doGetUniqueUint32(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doGetUniqueUint64(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doGetSystemState(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doSetSystemState(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doClearSystemState(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doSessionManagerReset(messageqcpp::ByteStream &msg, ThreadParams *p);

	/* OID Manager interface */
	OIDServer oids;
	boost::mutex oidsMutex;
	void doAllocOIDs(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doReturnOIDs(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doOidmSize(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doAllocVBOID(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doGetDBRootOfVBOID(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doGetVBOIDToDBRootMap(messageqcpp::ByteStream &msg, ThreadParams *p);

	/* Table lock interface */
	boost::scoped_ptr<TableLockServer> tableLockServer;
	void doGetTableLock(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doReleaseTableLock(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doChangeTableLockState(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doChangeTableLockOwner(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doGetAllTableLocks(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doReleaseAllTableLocks(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doGetTableLockInfo(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doOwnerCheck(messageqcpp::ByteStream &msg, ThreadParams *p);

	/* Autoincrement interface */
	AutoincrementManager aiManager;
	void doStartAISequence(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doGetAIRange(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doResetAISequence(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doGetAILock(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doReleaseAILock(messageqcpp::ByteStream &msg, ThreadParams *p);
	void doDeleteAISequence(messageqcpp::ByteStream &msg, ThreadParams *p);

	messageqcpp::MessageQueueServer *dbrmServer;
	std::vector<messageqcpp::MessageQueueClient *> slaves;
	std::vector<messageqcpp::MessageQueueClient *>::iterator iSlave;
	std::vector<messageqcpp::IOSocket *> activeSessions;

	LBIDResourceGraph *rg;

	boost::mutex mutex;
	boost::mutex mutex2;		// protects params and the hand-off  TODO: simplify
	boost::mutex slaveLock;	// syncs communication with the slaves
	boost::mutex serverLock;	// kludge to synchronize reloading
	int runners, NumWorkers;
	ThreadParams *params;
	volatile bool die, halting;
	bool reloadCmd;
	mutable bool readOnly;
	struct timespec MSG_TIMEOUT;
};

}

#endif
