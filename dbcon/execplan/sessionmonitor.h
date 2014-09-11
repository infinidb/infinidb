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
 * $Id: sessionmonitor.h 7409 2011-02-08 14:38:50Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class SessionMonitor interface
 */
 
#ifndef _SESSIONMONITOR_H
#define _SESSIONMONITOR_H

#include <vector>
#include <exception>
#include <sys/types.h>
#include <unistd.h>
#include "calpontsystemcatalog.h"
#include "sessionmanager.h"
#include "shmkeys.h"
#include "brmtypes.h"

//#define SM_DEBUG

namespace execplan {

/** @brief Identifies stale and orphaned transaction IDs.
 * 
 *
 * This class uses 2 parameters from the Calpont.xml file:
 * SessionManager/MaxConcurrentTransactions: defines how many active transactions the
 *		system should support.	When a new request comes in and there are already
 *		MaxConcurrentTransactions active, the new request blocks by default.  The
 *		default value is 1000.
 * 
 * SessionMonitor/SharedMemoryTmpFile: the file to store the shared memory segment
 *		data in.  This needs to be a different file than
 *		file used for the SessionManager/SharedMemoryTmpFile.
 *		The default is /tmp/CalpontMonShm.
 */
 
/* 
 * NOTE: It seems that valgrind doesn't work well with shared memory segments.
 * Specifically, when the code executes shmctl(IPC_RMID), which normally means
 * "destroy the segment when refcount == 0", the segment is destroyed
 * immediately, causing all subsequent references to fail.	This only affects
 * 'leakcheck'.
 * This comment is originally from SessionManager.h. I am assuming we will see
 * the same behavior here.
 */

class SessionMonitor {
	public:
		
		/** @brief A type describing a single transaction ID */
		struct MonTxnID {
			/// The TransactionID number
			CalpontSystemCatalog::SCN	id;
			/// True if the id is valid.
			bool						valid;
			/// timestamp of firstrecord of this TID
			time_t						firstrecord;
			MonTxnID(){id=0;valid=false;firstrecord=0;};
		};
		
		/** @brief A type describing a single transaction ID */
		typedef struct MonTxnID MonTxnID;
		
		/** @brief A type associating a session with a transaction */
		struct MonSIDTIDEntry {
			/// The Transaction ID.  txnid.valid determines whether or not this SIDTIDEntry is valid
			MonTxnID			txnid;
			/// The session doing the transaction
			SessionManager::SID		sessionid;
			MonSIDTIDEntry(){txnid=MonTxnID(); sessionid=0;};
		};
		
		/** @brief A type associating a session with a transaction */
		typedef struct MonSIDTIDEntry MonSIDTIDEntry_t;

		/** @brief Vector of MonSIDTIDEntry structures */
		typedef std::vector<MonSIDTIDEntry_t*> MonSIDTIDEntryVector_t;

		/** @brief needed to sort txns list in find timedOutTnxs()*/
		struct lessMonSIDTIDEntry {
			bool operator()(MonSIDTIDEntry const* p1, MonSIDTIDEntry const* p2)
			{
	  			if(!p1)
			    	return true;
	  			if(!p2)
					return false;

	  			return p1->txnid.firstrecord < p2->txnid.firstrecord;
			}
  		};

		/** @brief monitor version of the SessionManagerData */
		struct SessionMonitorData_struct {
			int txnCount;
			CalpontSystemCatalog::SCN verID;
			MonSIDTIDEntry_t* activeTxns;
			SessionMonitorData_struct(){txnCount=0;verID=0;activeTxns=NULL;};
		};
		typedef struct SessionMonitorData_struct	SessionMonitorData_t;

		/** @brief typedef if SessionManager struct SIDTIDEntry. For convenience.*/
		typedef BRM::SIDTIDEntry SIDTIDEntry_t;
		/** @brief This struct describes the layout of the shared memory segment copied from SessionManager.h */
		struct SessionManagerData_struct {
			int txnCount;
			CalpontSystemCatalog::SCN verID;
			SIDTIDEntry_t activeTxns[];
		};
		typedef SessionManagerData_struct SessionManagerData_t;

		/** @brief Constructor
		 *
		 * This attaches to existing shared memory segments.
		 * @note throws ios_base::failure on file IO error, runtime_error for
		 * other types of errors.  It might be worthwhile to define additional
		 * exceptions for all the different types of failures that can happen
		 * here.
		 */
		SessionMonitor();
		
		/** @brief Destructor
		 *
		 * This detaches the shared memory segment then saves it
		 * to disk and destroyed. It does not destroy the semaphores
		 * or the shared memory segment. Deletes the local copies
		 * of the SessionManager data and SessionMonitor data.
		 */
		virtual ~SessionMonitor();
					
		const int maxTxns() const { return fMaxTxns;}
		const int txnCount() const;

		/**
		 * @brief identifies timed out SessionManager structures
		 * @param
		 * @return
		*/
		MonSIDTIDEntryVector_t timedOutTxns();

		const SessionMonitorData_t* previousManagerData() { return &fPrevSegment;}
		SessionManagerData_t* currentManagerData() { return fCurrentSegment;}
		const SessionManagerData_t* sessionManagerData() { return fSessionManagerData;}

		void printTxns(const MonSIDTIDEntry_t& txn) const;
#ifdef SM_DEBUG
		void printSegment(const SessionManagerData_t* seg, const int l=1000) const;
		void printMonitorData(const int l=1000) const;
		void printTxns(const SIDTIDEntry_t& txn) const;
#endif
		const int AgeLimit() const {return fAgeLimit;} // to speed up testing
		void AgeLimit(const int& age) {fAgeLimit=age;}
		const char* segmentFilename() const { return fSegmentFilename.c_str();}
		bool haveSemaphores() const {return fHaveSemaphores;}
		bool haveSharedMemory() const {return fIsAttached;}

	private:

		int fMaxTxns;		// the maximum number of concurrent transactions
		static const int fMaxRetries = 10; // the max number of retries on file IO
		std::string fSegmentFilename; // filename used to store the image of the SessionManager data
		time_t fDefaultAgeLimit;
		time_t fAgeLimit; // age limit in seconds used to timeout/orpan a transaction
		
		SessionMonitorData_t fSessionMonitorData;	// pointer to in memory copy of the Monitor data
		SessionMonitorData_t fPrevSegment;			// in memory copy of SessionManager data from backup file
		SessionManagerData_t* fCurrentSegment;		// in memory of SessionManager data
		SessionManagerData_t* fSessionManagerData;	// current shared memory SessionManager data

		// refers to 2 semaphores; the first is a mutex that guards the shmseg
		// the second is used to block processes when there are too many concurrent txns
		int fsems;
		int fshmid; 	// shared memory segment id
		u_int32_t fuid; // user id used to create the shared memory key 

		SessionManager sm;

		bool isStaleSIDTID(const SIDTIDEntry_t& src, const MonSIDTIDEntry_t& dest) const;
		bool isEqualSIDTID(const SIDTIDEntry_t& src, const MonSIDTIDEntry_t& dest) const;
		bool isUsedSIDTID(const SIDTIDEntry_t& e) const;
		bool isUsed(const MonSIDTIDEntry_t& e) const;
	
		/**
		 * @brief attached to SessionManagerData shared memory
		 * @param  
		 * @return void
		 */
		void getSharedData(void); 

		bool fIsAttached;

		/**
		 * @brief unattached from SessionManagerData shared memory
		 * @param  
		 * @return void
		 */
		void detachSegment(void);
  
		/**
		 * @brief initialize SessionMonitorData
		 * @param  
		 * @return void
		 */
		void initSegment(SessionMonitorData_t* seg);
  
		/**
		 * @brief initialize SessionManagerData
		 * @param  
		 * @return void
		 */
		void initSegment(SessionManagerData_t* seg);
		
		const key_t IPCKey() const { return fShmKeys.SESSIONMANAGER_SYSVKEY;};

		/**
		 * @brief copies the SessionMonitor data from a file into memory
		 * @param  
		 * @return void
		 */
		void copyPreviousSegment();

		/**
		 * @brief copies the SessionManager data from shared memory into an SessionMonitor memory
		 * @param
		 * @return void
		 */
		void copyCurrentSegment();
		
		/**
		 * @brief Reads the SM data from file into memory. Used by copyPreviousSegment().
		 * @param
		 * @return bool
		*/
		bool readMonitorDataFromFile(const std::string);

		/**
		 * @brief write the current SessionManagerData as SessionMonitorData to a file.
		 * @param
		 * @return void
		 */
		void saveAsMonitorData(const std::string);

		bool fHaveSemaphores;

		/**
		 * @brief get the SessionManager semaphores
		 * @param
		 * @return void
		 */
		int getSems(void);
		
		void lock(void);
		
		void unlock(void);

		/**
		 * @brief do not use the copy constructor.
		 * @param
		 * @return
		 */
		SessionMonitor(const SessionMonitor&);

private:
	BRM::ShmKeys fShmKeys;
};

}	//namespace

#endif
