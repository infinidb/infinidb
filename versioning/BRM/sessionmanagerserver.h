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
 * $Id: sessionmanagerserver.h 1906 2013-06-14 19:15:32Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class SessionManagerServer interface
 */
 
#ifndef _SESSIONMANAGERSERVER_H
#define _SESSIONMANAGERSERVER_H

#include <map>

#include <boost/shared_array.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include "calpontsystemcatalog.h"
#include "brmtypes.h"

#include "atomicops.h"

#if defined(_MSC_VER) && defined(xxxSESSIONMANAGERSERVER_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning (disable : 4200)
#endif

namespace BRM {

/** @brief Issues transaction IDs and keeps track of the current system-wide version ID.
 * 
 * This class's primary purpose is to keep track of the current system-wide version ID and issue transaction IDs. 
 * It can be used simultaneously by multiple threads and processes.
 *
 * It uses system-wide semaphores and shared memory segments for IPC.
 * It allocates both if they don't already exist, but it only deallocates
 * the shmseg (saves it to a file so state isn't lost).  It may be a waste of
 * CPU time to deallocate either while the system is running.  It may be more
 * appropriate to do that in the DB shut down script.
 *
 * Note: Added a macro 'DESTROYSHMSEG' which enables/disables the code to load/save
 * and deallocate the shared memory segment.
 *
 * This class uses 3 parameters from the Calpont.xml file:
 * SessionManager/MaxConcurrentTransactions: defines how many active transactions the
 * 		system should support.  When a new request comes in and there are already
 * 		MaxConcurrentTransactions active, the new request blocks by default.  The
 * 		default value is 1000.
 * SessionManager/SharedMemoryTmpFile: the file to store the shared memory segment
 * 		data in between invocations if DESTROYSHMSEG is defined below.  The
 * 		default is /tmp/CalpontShm.
 * SessionManager/TxnIDFile: the file to store the last transaction ID issued
 */
 
/* 
 * Define DESTROYSHMSEG if the SM should deallocate the shared memory segment
 * after the last reference has died.  This also enables the load/save
 * operations.  If it is undefined, the segment is never deallocated
 * by SM, and is therefore more efficient between instances.
 * NOTE: It seems that valgrind doesn't work well with shared memory segments.
 * Specifically, when the code executes shmctl(IPC_RMID), which normally means
 * "destroy the segment when refcount == 0", the segment is destroyed
 * immediately, causing all subsequent references to fail.  This only affects
 * 'leakcheck'.
 */
//#define DESTROYSHMSEG

class SessionManagerServer {
public:
	/** @brief SID = Session ID */
	typedef uint32_t SID;

	// State flags. These bit values are stored in Overlay::state and reflect the current state of the system
	static const uint32_t SS_READY;				/// bit 0 => Set by dmlProc one time when dmlProc is ready
	static const uint32_t SS_SUSPENDED;			/// bit 1 => Set by console when the system has been suspended by user.
	static const uint32_t SS_SUSPEND_PENDING;	/// bit 2 => Set by console when user wants to suspend, but writing is occuring.
	static const uint32_t SS_SHUTDOWN_PENDING;	/// bit 3 => Set by console when user wants to shutdown, but writing is occuring.
	static const uint32_t SS_ROLLBACK;			/// bit 4 => In combination with a PENDING flag, force a rollback as soom as possible.
	static const uint32_t SS_FORCE;				/// bit 5 => In combination with a PENDING flag, force a shutdown without rollback.

	/** @brief Constructor
	 *
	 * This sets up the shared memory segment & semaphores used by this
	 * instance.  No additional set-up calls are necessary.
	 * @note throws ios_base::failure on file IO error, runtime_error for
	 * other types of errors.  It might be worthwhile to define additional
	 * exceptions for all the different types of failures that can happen
	 * here.
	 */
	EXPORT SessionManagerServer();

	/** @brief Destructor
	 *
	 * This detaches the shared memory segment.  If DESTROYSHMSEG is defined and this
	 * is the last reference to it, it will be saved to disk and destroyed.
	 * It does not destroy the semaphores.  Those persist until the system
	 * is shut down.
	 */
	virtual ~SessionManagerServer() { if (txnidfd >=0 ) close(txnidfd); }

	/** @brief Gets the current version ID
	 *
	 * Gets the current version ID.
	 */
	EXPORT const QueryContext verID();
	
	/** @brief Gets the current version ID
	 *
	 * Gets the current version ID.
	 */
	EXPORT const QueryContext sysCatVerID();
	
	/** @brief Gets a new Transaction ID
	 *
	 * Makes a new Transaction ID, associates it with the given session
	 * and increments the system-wide version ID
	 * @note This blocks until (# active transactions) \< MaxTxns unless block == false
	 * @note Throws runtime_error on semaphore-related error
	 * @note This will always return a valid TxnID unless block == false, in which case
	 * it will return a valid TxnID iff it doesn't have to block.
	 * @param session The session ID to associate with the transaction ID
	 * @param block If true, this will block until there is an available slot, otherwise
	 * it will return a TxnID marked invalid, signifying that it could not start a new transaction
	 * @return The new transaction ID.
	 */
	EXPORT const TxnID newTxnID(const SID session, bool block = true, bool isDDL = false);
	
	/** @brief Record that a transaction has been committed
	 *
	 * Record that a transaction has been committed.
	 * @note Throws runtime_error on a semaphore-related error, invalid_argument
	 * when txnid can't be found
	 * @param txnid The committed transaction ID.  This is marked invalid
	 * on return.
	 */
	void committed(TxnID& txn) { finishTransaction(txn); }
	
	/** @brief Record that a transaction has been rolled back
	 *
	 * Record that a transaction has been rolled back.
	 * @note Throws runtime_error on a semaphore-related error, invalid_argument
	 * when txnid can't be found
	 * @param txnid The rolled back transaction ID.  This is marked invalid
	 * on return.
	 */
	void rolledback(TxnID& txn) { finishTransaction(txn); }
	
	/** @brief Gets the transaction ID associated with a given session ID
	 * 
	 * Gets the transaction ID associated with a given session ID.
	 * @note Throws runtime_error on a semaphore-related error
	 * @param session The session ID
	 * @return A valid transaction ID if there's an association; an invalid
	 * one if there isn't.
	 */
	EXPORT const TxnID getTxnID(const SID session);
	
	/** @brief Gets an array containing all active SID-TID associations
	 *
	 * Gets an array containing the SID-TID associations of all active
	 * transactions.  This is intended to be used by another object
	 * that can determine which sessions are still live and which
	 * transactions need to go away.
	 * @note Throws runtime_error on a semaphore-related error
	 * @param len (out) the length of the array
	 * @return A pointer to the array.  Note: The caller is responsible for
	 * deallocating it.  Use delete[].
	 */
	EXPORT boost::shared_array<SIDTIDEntry> SIDTIDMap(int &len);

	/**
	 * get a unique 32-bit number
	 */
	uint32_t getUnique32() { return atomicops::atomicInc(&unique32); }

	/**
	 * get a unique 64-bit number
	 */
	uint64_t getUnique64() { return atomicops::atomicInc(&unique64); }

	/** @brief Resets the semaphores to their original state.  For testing only.
	 * 
	 * Resets the semaphores to their original state.  For testing only.
	 */
	EXPORT void reset();

	/**
	 * get the Txn ID filename
	 */
	std::string getTxnIDFilename() const { return txnidFilename; }

	/** @brief set system state info
	 * 
	 * Sets the bits on in Overlay::systemState that are found on in 
	 * state. That is Overlay::systemState | state.
	 */
	EXPORT void setSystemState(uint32_t state);

	/** @brief set system state info
	 *  
	 * Clears the bits on in Overlay::systemState that are found on 
	 * in state. That is Overlay::systemState & ~state. 
	 */
	EXPORT void clearSystemState(uint32_t state);

	/** @brief get system state info
	 * 
	 * Returns the Overlay::systemState flags
	 */
	void getSystemState(uint32_t &state) { state = systemState; }
	
	/**
	 * get the number of active txn's
	 */
	EXPORT uint32_t getTxnCount();

private:
	SessionManagerServer(const SessionManagerServer&);
	SessionManagerServer& operator=(const SessionManagerServer&);

	void loadState();
	void saveSystemState();
	void finishTransaction(TxnID& txn);
	void saveSMTxnIDAndState();

	volatile uint32_t unique32;
	volatile uint64_t unique64;

	int maxTxns;  // the maximum number of concurrent transactions
	std::string txnidFilename;
	int txnidfd;		// file descriptor for the "last txnid" file
	execplan::CalpontSystemCatalog::SCN _verID;
	execplan::CalpontSystemCatalog::SCN _sysCatVerID;
	uint32_t systemState;

	std::map<SID, execplan::CalpontSystemCatalog::SCN> activeTxns;
	typedef std::map<SID, execplan::CalpontSystemCatalog::SCN>::iterator iterator;

	boost::mutex mutex;
	boost::condition_variable condvar;		// used to synthesize a semaphore
	uint32_t semValue;
};

}   //namespace

#ifdef _MSC_VER
#pragma warning (pop)
#endif

#undef EXPORT

#endif
