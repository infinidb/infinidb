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

/***********************************************************************
*   $Id: bucketreuse.h 8436 2012-04-04 18:18:21Z rdempsey $
*
*
***********************************************************************/
/** @file
 * class BucketReuseManager interface
 */

#ifndef JOBLIST_BUCKETREUSE_H_
#define JOBLIST_BUCKETREUSE_H_

#include <map>
#include <list>
#include <string>
#include <ostream>
#include <fstream>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

#include "calpontsystemcatalog.h"

// forward reference
class BucketReUseDriver;

#if defined(_MSC_VER) && defined(BUCKETREUSEMANAGER_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace joblist
{

class ResourceManager;
/** @brief key to the bucket reuse map
 *
 * the bucket reuse map use the column oid and filter string as key,
 * mapped value is a pointer to a bucket reuse control entry
 */
typedef std::pair<execplan::CalpontSystemCatalog::TableColName, std::string> BucketFileKey;


/** @brief offset to the file on disk
 *
 * vector keeps the offset of each set
 */
typedef std::vector<std::fstream::pos_type> OffsetVec;


/** @brief information for buckets restore
 *
 * information necessary to restore the bucket files from disk
 * defined in largedatalist.h
 */
struct SetRestoreInfo
{
	uint64_t  fSetCount;
	uint64_t  fTotalSize;
	OffsetVec fSetStartPositions;

	SetRestoreInfo() : fSetCount(0), fTotalSize(0) {}
};


/** @brief bucket reuse control entry
 *
 * this entry contains the file basename, status, and other control data
 */
class BucketReuseControlEntry
{
public:

	enum BucketFileStatus
	{
		null_c,
		progress_c,
		using_c,
		ready_c,
		obsolete_c,
		finished_c
	};

	BucketReuseControlEntry() : fStatus(null_c), fBucketCount(0), fUserCount(0), fNotified(false) {}
	BucketReuseControlEntry(const std::string& name) :
		fFileBaseName(name), fStatus(null_c), fBucketCount(0), fUserCount(0), fNotified(false) {}

	~BucketReuseControlEntry() { }

	const std::string& baseName() { return fFileBaseName; }
	void baseName(const std::string& name) { fFileBaseName = name; }

	BucketFileStatus fileStatus()  { return fStatus; }
	void fileStatus(BucketFileStatus s)  { fStatus = s; }

	boost::condition& stateChange() { return fStateChange; }

	uint64_t bucketCount() { return fBucketCount; }
	void bucketCount(uint64_t buckets) { fBucketCount = buckets; }

	std::vector<SetRestoreInfo>& restoreInfoVec() { return fRestoreInfoVec; }
	std::pair<uint64_t, uint64_t>& dataSize() { return fDataSize; }

	void notifyUsers();
	bool userNotified() { return fNotified; }

	friend class BucketReuseManager;
	friend std::ostream& operator<<(std::ostream&, const BucketReuseControlEntry&);

private:
	std::string fFileBaseName;
	BucketFileStatus fStatus;
	boost::condition fStateChange;
	uint64_t  fBucketCount;
	uint64_t  fUserCount;
	bool      fNotified;

	std::pair<uint64_t, uint64_t> fDataSize;
	std::vector<SetRestoreInfo> fRestoreInfoVec;

	// disable copy constructor and assignment operator
	BucketReuseControlEntry(const BucketReuseControlEntry&);
	BucketReuseControlEntry& operator=(const BucketReuseControlEntry&);
};

typedef std::multimap<std::string, BucketFileKey>         ReuseConfigMap;
typedef std::map<BucketFileKey, BucketReuseControlEntry*> BucketReuseMap;

/** @brief bucket reuse manager
 *
 * this singleton class maintains the reuse information and co-ordinates producer and consumers
 */
class BucketReuseManager
{
public:
	// constructors are disabled for singleton, only destructor is public.
	EXPORT ~BucketReuseManager();

	// instance access method
	EXPORT static BucketReuseManager* instance();

	// parse the configure data
	EXPORT void startup(ResourceManager& rm);

	// take registration info from joblistfactory,
	// update and return the entry to joblistfactory
	// !! the caller shall have a lock on fMutex before call this method !!
	// -- boost::mutex::scoped_lock lock(BucketReuseManager::instance()->getMutex());
	EXPORT BucketReuseControlEntry* userRegister(const execplan::CalpontSystemCatalog::TableColName& tcn,
							  std::string filter, uint64_t version, uint64_t buckets, bool& scan);

	// notified when the user is complete
	EXPORT void userDeregister(BucketReuseControlEntry*);

	// get the reference to the mutex
	EXPORT boost::mutex& getMutex() { return fMutex; }

	friend class ::BucketReUseDriver;

private:
	class Eraser
	{
	public:
   		Eraser(BucketReuseManager* eraser) : fEraser(eraser) {}
		void operator()() { fEraser->cleanupOldVersions(); }

	private:
		//Eraser(const Eraser&);
		//Eraser& operator=(const Eraser&);
		BucketReuseManager* fEraser;
	};

	// initialize the map using the configure records in Calpont.xml BucketReuse section, 
	void initializeControlMap();

	// update the map version number, move currently re-using entries to remove list
	void handleNewDbVersion(uint64_t version);

	// send signal to the cleanup thread
	void signalToCleanup();

	// cleanup old version files, which are not in use, from filesystem
	void cleanupOldVersions();

	// data members
	uint64_t fVersion;
	std::string fReuseDir;
	boost::mutex fMutex;
	boost::thread *fEraser;
	boost::condition fNewVer;
	ReuseConfigMap fConfigMap;
	BucketReuseMap fControlMap;
	std::list<BucketReuseControlEntry*> fToRemoveEntries;
	bool fEraserStop;

	static BucketReuseManager* fInstance;

	// private constructor
	BucketReuseManager() : fVersion(0), fEraser(NULL), fEraserStop(false) {}

	// disable copy constructor and assignment operator
	// private without implementation
	BucketReuseManager(const BucketReuseManager&);
	BucketReuseManager& operator=(const BucketReuseManager&);
};

//extern const std::string defaultTempDiskPath;
extern std::ostream& operator<<(std::ostream&, const BucketReuseControlEntry&);

}

#undef EXPORT

#endif // JOBLIST_BUCKETREUSE_H_
