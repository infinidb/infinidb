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

/******************************************************************************
 * $Id: sessionmanager.h 9215 2013-01-24 18:40:12Z pleblanc $
 *
 *****************************************************************************/

/** @file 
 * class SessionManager interface
 */
 
#ifndef _SESSIONMANAGER_H
#define _SESSIONMANAGER_H

#include "calpontsystemcatalog.h"
#include "brm.h"
#include "boost/shared_array.hpp"

namespace execplan {

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

class SessionManager {
public:
	
	/** @brief SID = Session ID */
	typedef uint32_t SID;
	
	/** @brief Constructor
	 *
	 * This sets up the shared memory segment & semaphores used by this
	 * instance.  No additional set-up calls are necessary.
	 * @note throws ios_base::failure on file IO error, runtime_error for
	 * other types of errors.  It might be worthwhile to define additional
	 * exceptions for all the different types of failures that can happen
	 * here.
	 */
	SessionManager();
	SessionManager(const SessionManager&);

	/** @brief Constructor for use during debugging only
	 * 
	 * This constructor is only used to grab the semaphores & reset them
	 * if they exist.  It does not attach the shared memory segment,
	 * and no operation other than reset() should be performed on a
	 * SessionManager instantiated with this.
	 */
	SessionManager(bool nolock);
	
	/** @brief Destructor
	 *
	 * This detaches the shared memory segment.  If DESTROYSHMSEG is defined and this
	 * is the last reference to it, it will be saved to disk and destroyed.
	 * It does not destroy the semaphores.  Those persist until the system
	 * is shut down.
	 */
	virtual ~SessionManager();
	
	/** @brief Gets the current version ID
	 *
	 * Gets the current version ID.
	 */
	const BRM::QueryContext verID();
	
	/** @brief Gets the current systemcatalog version ID
	 *
	 * Gets the current systemcatalog version ID.
	 */
	const BRM::QueryContext sysCatVerID();
	
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
	const BRM::TxnID newTxnID(const SID session, bool block = true, bool isDDL = false);
	
	/** @brief Record that a transaction has been committed
	 *
	 * Record that a transaction has been committed.
	 * @note Throws runtime_error on a semaphore-related error, invalid_argument
	 * when txnid can't be found
	 * @param txnid The committed transaction ID.  This is marked invalid
	 * on return.
	 */
	void committed(BRM::TxnID& txnid);
	
	/** @brief Record that a transaction has been rolled back
	 *
	 * Record that a transaction has been rolled back.
	 * @note Throws runtime_error on a semaphore-related error, invalid_argument
	 * when txnid can't be found
	 * @param txnid The rolled back transaction ID.  This is marked invalid
	 * on return.
	 */
	void rolledback(BRM::TxnID& txnid);
	
	/** @brief Gets the transaction ID associated with a given session ID
	 * 
	 * Gets the transaction ID associated with a given session ID.
	 * @note Throws runtime_error on a semaphore-related error
	 * @param session The session ID
	 * @return A valid transaction ID if there's an association; an invalid
	 * one if there isn't.
	 */
	const BRM::TxnID getTxnID(const SID session);
	
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
	boost::shared_array<BRM::SIDTIDEntry> SIDTIDMap(int& len);
	
	/** @brief Returns a unique uint32_t.  It eventually wraps around, but who cares.
	 *
	 * This returns a "unique" uint32_t for when we need such a thing.  The current
	 * usage is to get a unique identifier for each BPPJL and its associated BPP
	 * object on the PM.
	 * @return A "unique" uint32_t.
	 */
	const uint32_t getUnique32();

	/** @brief Returns the number of active transactions.  Only useful in testing.
	 * 
	 * This returns the number of active transactions and verifies it against
	 * the transaction semaphore's value and the value reported by shared->txnCount.
	 * @note Throws logic_error if there's a mismatch, runtime_error on a 
	 * semaphore operation error.
	 * @return The number of active transactions.
	 */
	int verifySize();

	/** @brief Resets the semaphores to their original state.  For testing only.
	 * 
	 * Resets the semaphores to their original state.  For testing only.
	 */
	void reset();

	std::string getTxnIDFilename() const;
	
	const bool checkActiveTransaction(const SID sessionId, bool& bIsDbrmUp, BRM::SIDTIDEntry& blocker);
	const bool isTransactionActive(const SID sessionId, bool& bIsDbrmUp);
	
private:
	BRM::DBRM dbrm;
	std::string txnidFilename;
};

}   //namespace

#endif
// vim:ts=4 sw=4:

