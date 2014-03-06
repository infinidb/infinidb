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
 * $Id: oidserver.h 7409 2011-02-08 14:38:50Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class OIDServer interface.
 */

#ifndef OBJECTIDMANAGER_H_
#define OBJECTIDMANAGER_H_

#include <string>
#include <stdint.h>
#include <vector>
#include <sys/types.h>

#include <boost/thread/mutex.hpp>

#if defined(_MSC_VER) && defined(xxxBRMOIDSVR_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace idbdatafile {
class IDBDataFile;
}


namespace BRM {

/** @brief The class that manages the Object ID space
 *
 * The class that manages the allocation and deallocation of IDs
 * for objects in the database.
 */
class OIDServer
{
private:
	struct FEntry {
		int begin, end;
	};

public:
	/** @brief Default constructor
	 *
	 * @note Throws ios::failure on a file IO error
	 */
	EXPORT OIDServer();
	EXPORT virtual ~OIDServer();

	/** @brief Allocate a contiguous range of Object IDs
	 *
	 * @param num The number of contiguous Object IDs to allocate
	 * @return The first OID allocated on success, -1 on failure
	 */
	EXPORT int allocOIDs(int num);


	/** @brief Allocates a single OID for a version buffer file, given its dbroot
	 *
	 * A new requirement in the shared nothing implementation is for version buffer
	 * files to be anchored to a specific dbroot.  This association is stored on disk as
	 * an array of uint16s.  Version buffer OIDs use 0-999, so the 0th offset in the
	 * array is the dbroot of the OID 0, and so on.
	 */
	EXPORT int allocVBOID(uint16_t dbroot);

	/** @brief Returns the DBRoot of the given version buffer file.
	 *
	 */
	EXPORT int getDBRootOfVBOID(uint32_t vbOID);

	/** @brief Returns the VB OID of the given DB root.
	 *
	 */
	EXPORT int getVBOIDOfDBRoot(uint32_t vbOID);

	/** @brief Returns the whole VB OID -> DBRoot mapping. The returned vector is indexed by VB OID.
	 *
	 */
	EXPORT const std::vector<uint16_t> & getVBOIDToDBRootMap();

	/** @brief Return a list of OIDs to the pool
	 *
	 * @param start The first OID to return
	 * @param end The last OID to return
	 */
	EXPORT void returnOIDs(int start, int end) const;

	/** @brief Counts the number of allocated OIDs
	 *
	 * @note This currently assumes the bitmap length is a multiple of 4096
	 * @return The number of allocated OIDs
	 */
	EXPORT int size() const;

	/** @brief Get the OID bitmap filename
	 */
	const std::string getFilename() const;

private:
	std::string fFilename;
	static const int FreeListEntries = 256;
	static const int HeaderSize = FreeListEntries * sizeof(FEntry);
	static const int StartOfVBOidSection = HeaderSize + 2097152;  // (2^24/8)
	static const int MaxRetries = 10;   /// max number of retries on file operations
	static boost::mutex fMutex;
	idbdatafile::IDBDataFile* fFp;
	int fFd;  /// file descriptor referencing the bitmap file
	std::vector<uint16_t> vbOidDBRootMap;

	/** @brief Grab the file lock
	 *
	 * Grab the file lock.
	 * @note Throws ios::failure after MaxRetries hard errors
	 */
	//void lockFile() const;

	/** @brief Reliably reads data from the bitmap file
	 *
	 * Reliably reads data from the bitmap file.
	 * @note Throws ios::failure after MaxRetries hard errors
	 * @note Caller is responsible for locking & unlocking
	 * @param buf the buffer to read into
	 * @param offset the offset to start reading at
	 * @param size the number of bytes to read into buf
	 */
	void readData(uint8_t* buf, off_t offset, int size) const;

	/** @brief Reliably writes data to the bitmap file
	 *
	 * Reliably write data to the bitmap file.
	 * @note Throws ios::failure after MaxRetries hard errors
	 * @note Caller is responsible for locking & unlocking
	 * @param buf the data to write
	 * @param offset the offset to start writing at
	 * @param size the number of bytes to write
	 */
	void writeData(uint8_t* buf, off_t offset, int size) const;

	/** @brief If there is no bitmap file yet, this is used to make one
	 * 
	 * If there is no bitmap file yet, this is used to make one.
	 * @note It assumes no OIDs are allocated, we may want
	 *       to fully rebuild it from actual DB contents instead.
	 */
	void initializeBitmap() const;

	/** @brief Allocates OIDs using a designated freelist entry
	 *
	 * Allocates OIDs using a designated freelist entry
	 * @param fe The freelist entry to use.
	 * @param num The number of OIDs to allocate from that block
	 */
	void useFreeListEntry(struct FEntry& fe, int num);

	/** @brief This allocates or deallocates a block of OIDs
	 *
	 * This allocates or deallocates a block of OIDs
	 * @param start The first OID to alloc/dealloc
	 * @param end The number of OIDs to flip
	 * @param mode mode = 0 means 'allocate', mode = 1 means 'deallocate'
	 */
	void flipOIDBlock(int start, int num, int mode) const;

	/** @brief This scans the whole bitmap for a block of free OIDs
	 *
	 * This scans the whole bitmap for a block of free OIDs
	 * @param num the size of the block to locate
	 * @param freelist the freelist
	 * @return The first OID of the block allocated, or -1 if there is no match
	 */
	int fullScan(int num, struct FEntry* freelist) const;

	/** @brief This is used by allocOIDs to fix the freelist after a full scan
	 *
	 * This is used by fullScan to fix the freelist before an allocation
	 * @param freelist The freelist
	 * @param start The first OID of the block allocated by fullScan
	 * @param num The number of OIDs allocated
	 * @note At the moment it throws logic_error if it detects a specific error in
	 * fullscan
	 */
	void patchFreelist(struct FEntry* freelist, int start, int num) const;

	void loadVBOIDs();
};

}	// namespace

#undef EXPORT

#endif
