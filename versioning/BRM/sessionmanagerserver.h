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
 * $Id: sessionmanagerserver.h 1546 2012-04-03 18:32:59Z dcathey $
 *
 *****************************************************************************/

/** @file 
 * class SessionManagerServer interface
 */
 
#ifndef _SESSIONMANAGERSERVER_H
#define _SESSIONMANAGERSERVER_H

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>

#include "calpontsystemcatalog.h"
#include "shmkeys.h"
#include "brmtypes.h"

#if defined(_MSC_VER) && defined(SESSIONMANAGERSERVER_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning (disable : 4200)
#endif

#ifndef __GNUC__
#  ifndef __attribute__
#    define __attribute__(x)
#  endif
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
	typedef u_int32_t SID;

	enum SystemState {
		SS_NOT_READY,
		SS_READY,
	};

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
	//EXPORT SessionManagerServer(const SessionManagerServer&);

	/** @brief Constructor for use during debugging only
	 * 
	 * This constructor is only used to grab the semaphores & reset them
	 * if they exist.  It does not attach the shared memory segment,
	 * and no operation other than reset() should be performed on a
	 * SessionManagerServer instantiated with this.
	 */
	EXPORT SessionManagerServer(bool nolock);
	
	/** @brief Destructor
	 *
	 * This detaches the shared memory segment.  If DESTROYSHMSEG is defined and this
	 * is the last reference to it, it will be saved to disk and destroyed.
	 * It does not destroy the semaphores.  Those persist until the system
	 * is shut down.
	 */
	EXPORT virtual ~SessionManagerServer();
	
	/** @brief Gets the current version ID
	 *
	 * Gets the current version ID.
	 */
	EXPORT const execplan::CalpontSystemCatalog::SCN verID(void);
	
	/** @brief Gets the current version ID
	 *
	 * Gets the current version ID.
	 */
	EXPORT const execplan::CalpontSystemCatalog::SCN sysCatVerID(void);
	
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
	EXPORT void committed(TxnID& txnid);
	
	/** @brief Record that a transaction has been rolled back
	 *
	 * Record that a transaction has been rolled back.
	 * @note Throws runtime_error on a semaphore-related error, invalid_argument
	 * when txnid can't be found
	 * @param txnid The rolled back transaction ID.  This is marked invalid
	 * on return.
	 */
	EXPORT void rolledback(TxnID& txnid);
	
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
	EXPORT const BRM::SIDTIDEntry* SIDTIDMap(int& len);
	
	EXPORT char * getShmContents(int &len);

	EXPORT const uint32_t getUnique32();

	/** @brief Returns the number of active transactions.  Only useful in testing.
	 * 
	 * This returns the number of active transactions and verifies it against
	 * the transaction semaphore's value and the value reported by shared->txnCount.
	 * @note Throws logic_error if there's a mismatch, runtime_error on a 
	 * semaphore operation error.
	 * @return The number of active transactions.
	 */
	EXPORT int verifySize();

	/** @brief Resets the semaphores to their original state.  For testing only.
	 * 
	 * Resets the semaphores to their original state.  For testing only.
	 */
	EXPORT void reset();

	EXPORT std::string getTxnIDFilename() const;
	/** @brief set a table to lock/unlock state
	 * 
	 * set the table to lock/unlock state depending on lock request. error code will be returned.
	 */
	EXPORT int8_t  setTableLock (  const OID_t tableOID, const u_int32_t sessionID,  const u_int32_t processID, const std::string processName, bool tolock ) ;
	/** @brief update a table lock
	 * 
	 * validate and update the table lock if the original lock is not valid anymore. The lock is
	 * consided to be still valid if the process hold the tablelock is still active. error code will be returned.
	 */
	EXPORT int8_t  updateTableLock (  const OID_t tableOID,  u_int32_t& processID, std::string & processName ) ;
	
	
	/** @brief get table lock information
	 * 
	 * if the table is locked, the processID, processName will be valid. error code will be returned.
	 */
	EXPORT int8_t getTableLockInfo ( const OID_t tableOID, u_int32_t & processID,
		std::string & processName, bool & lockStatus, SID & sid );
	
	EXPORT std::vector<BRM::SIDTIDEntry>  getTableLocksInfo ();

	/** @brief set system state info
	 * 
	 * used to keep cpimport from starting until DMLProc has finished rollback
	 */
	EXPORT int8_t setSystemState(int state);

	/** @brief get system state info
	 * 
	 * used to keep cpimport from starting until DMLProc has finished rollback
	 */
	EXPORT int8_t getSystemState(int& state);
	
private:
	SessionManagerServer(const SessionManagerServer&);
	SessionManagerServer& operator=(const SessionManagerServer&);

	int MaxTxns;  // the maximum number of concurrent transactions
	static const int MaxRetries = 10; // the max number of retries on file IO
	char* segmentFilename;
	std::string txnidFilename;
	int txnidfd;		// file descriptor for the "last txnid" file
	
	/** @brief This struct describes the layout of the shared memory segment */
	struct Overlay {
		int txnCount;
		execplan::CalpontSystemCatalog::SCN verID;
		execplan::CalpontSystemCatalog::SCN sysCatVerID;
		int systemState;
		boost::interprocess::interprocess_semaphore sems[2];
		BRM::SIDTIDEntry activeTxns[];
	};
	
	Overlay* shared;
	//int sems;   // refers to 2 semaphores; the first is a mutex that guards the shmseg
				// the second is used to block processes when there are too many concurrent txns
	//int shmid;
	
	void getSharedData(void);
	void detachSegment(void);
	inline void initSegment(void);

#ifdef DESTROYSHMSEG
	void loadSegment(void);
	void saveSegment(void);
#endif
	int makeSems(void);
	void lock(void);
	void unlock(void);
	void finishTransaction(TxnID& txn, bool commit);
	void printSIDTIDEntry ( const char* commentHdr, int entryIndex ) const;
	bool lookupProcessStatus ( std::string   processName, u_int32_t     processId); // are fProcessId/fProcessName active
	ShmKeys fShmKeys;
#ifdef _MSC_VER
	volatile LONG unique32;
#else
	uint32_t unique32 __attribute__((aligned));
#endif
#ifdef _MSC_VER
	boost::mutex fPidMemLock;
	DWORD* fPids;
	DWORD fMaxPids;
#endif
	boost::interprocess::shared_memory_object fOverlayShm;
	boost::interprocess::mapped_region fRegion;
};

}   //namespace

#ifdef _MSC_VER
#pragma warning (pop)
#endif

#undef EXPORT

#endif
