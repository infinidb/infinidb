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
 * $Id: mastersegmenttable.h 1635 2012-08-03 18:57:10Z rdempsey $
 *
 *****************************************************************************/

/** @file
 * 
 * class MasterSegmentTable
 *
 * The MasterSegmentTable regulates access to the shared memory segments 
 * used by the BRM classes and provides the means for detecting when to resize
 * a segment and when it has been relocated (due to resizing).
 * 
 * XXXPAT: We should make a cleanup class here also.
 */

#ifndef _MASTERSEGMENTTABLE_H_
#define _MASTERSEGMENTTABLE_H_

#include <stdexcept>
#include <sys/types.h>
#include <boost/thread.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "rwlock.h"
#include "shmkeys.h"

#if defined(_MSC_VER) && defined(xxxMASTERSEGMENTTABLE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

struct MSTEntry {
	key_t tableShmkey;
	int allocdSize;
	int currentSize;
	EXPORT MSTEntry();
};

class MasterSegmentTableImpl
{
public:
	static MasterSegmentTableImpl* makeMasterSegmentTableImpl(int key, int size);

	boost::interprocess::shared_memory_object fShmobj;
	boost::interprocess::mapped_region fMapreg;

private:
	MasterSegmentTableImpl(int key, int size);
	~MasterSegmentTableImpl();
	MasterSegmentTableImpl(const MasterSegmentTableImpl& rhs);
	MasterSegmentTableImpl& operator=(const MasterSegmentTableImpl& rhs);

	static boost::mutex fInstanceMutex;
	static MasterSegmentTableImpl* fInstance;
};

/** @brief This class regulates access to the BRM tables in shared memory
 * 
 * This class regulates access to the BRM tables in shared memory
 */
class MasterSegmentTable {
	public:
		/** @brief Constructor.
		 * @note Throws runtime_error on a semaphore-related error.
		 */
		EXPORT MasterSegmentTable();
		//MasterSegmentTable(const MasterSegmentTable& mst);
		EXPORT ~MasterSegmentTable();
		
		/// specifies the Extent Map table
		static const int EMTable = 0;
		/// specifies the Extent Map's Freelist table
		static const int EMFreeList = 1;
		/// specifies the Version Buffer Block Map segment
		static const int VBBMSegment = 2;
		/// specifies the Version Substitution Structure segment
		static const int VSSSegment = 3;
		/// specifies the copy lock segment
		static const int CLSegment = 4;
		/// the number of tables currently defined
		static const int nTables = 5;

		/** @brief This function gets the specified table.
		 *
		 * This function gets the specified table and grabs the 
		 * associated read lock.
		 * @param num EMTable, EMFreeList, or VBBMTable
		 * @param block If false, it won't block.
		 * @note throws invalid_argument if num is outside the valid range 
		 * and runtime_error on a semaphore-related error.
		 * @return If block == true, it always returns the specified MSTEntry;
		 * if block == false, it can also return NULL if it could not grab
		 * the table's lock.
		 */
		EXPORT MSTEntry* getTable_read(int num, bool block = true) const;
		
		/** @brief This function gets the specified table.
		 *
		 * This function gets the specified table and grabs the 
		 * associated write lock.
		 * @param num EMTable, EMFreeList, or VBBMTable
		 * @param block If false, it won't block.
		 * @note throws invalid_argument if num is outside the valid range 
		 * and runtime_error on a semaphore-related error.
		 * @return If block == true, it always returns the specified MSTEntry;
		 * if block == false, it can also return NULL if it could not grab
		 * the table's lock.
		 */
		EXPORT MSTEntry* getTable_write(int num, bool block = true) const;
		
		/** @brief Upgrade a read lock to a write lock.
		 *
		 * Upgrade a read lock to a write lock.  This is not an atomic
		 * operation.
		 * @param num The table the caller holds the read lock to.
		 */
		EXPORT void getTable_upgrade(int num) const;
		
		/** @brief Downgrade a write lock to a read lock.
		 * 
		 * Downgrade a write lock to a read lock.  This is an atomic
		 * operation.
		 * @param num The table the caller holds the write lock to.
		 */
		EXPORT void getTable_downgrade(int num) const;

		/** @brief This function unlocks the specified table.
		 *
		 * This function unlocks the specified table.
		 *
		 * @param num EMTable, EMFreeList, or VBBMTable
		 * @note throws invalid_argument if num is outside the valid range 
		 * and runtime_error on a semaphore-related error.
		 */
		EXPORT void releaseTable_read(int num) const;
		
		/** @brief This function unlocks the specified table.
		 *
		 * This function unlocks the specified table.
		 *
		 * @param num EMTable, EMFreeList, or VBBMTable
		 * @note throws invalid_argument if num is outside the valid range 
		 * and runtime_error on a semaphore-related error.
		 */
		EXPORT void releaseTable_write(int num) const;

		/** @brief This function gets the current VSS key out of shared memory without locking
		 *
		 * This function gets the current VSS key out of shared memory without any locking. It is used
		 * (eventually) by DBRM::vssLookup() to try and correctly & efficiently short-circuit a vss
		 * lookup when there are no locked blocks in the vss. This read should be atomic. Even if it's
		 * not, we should still be okay.
		 */
		inline key_t getVSSShmkey() const { return fShmDescriptors[VSSSegment].tableShmkey; }

	private:
		MasterSegmentTable(const MasterSegmentTable& mst);
		MasterSegmentTable& operator=(const MasterSegmentTable& mst);

		int shmid;
		mutable boost::scoped_ptr<rwlock::RWLock> rwlock[nTables];
		
		static const int MSTshmsize = nTables * sizeof(MSTEntry);
		int RWLockKeys[nTables];
		
		/// indexed by EMTable, EMFreeList, and VBBMTable
		MSTEntry* fShmDescriptors;
		
		void makeMSTSegment();
		void initMSTData();
		ShmKeys fShmKeys;
		MasterSegmentTableImpl* fPImpl;
};
		
}  //namespace

#undef EXPORT

#endif
