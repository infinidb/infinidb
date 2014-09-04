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
*   $Id: bucketreuse.cpp 8436 2012-04-04 18:18:21Z rdempsey $
*
*
***********************************************************************/

#include <iomanip>
#include <iostream>
#include <iostream>
#include <sstream>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/erase.hpp>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>

#define BUCKETREUSEMANAGER_DLLEXPORT
#include "bucketreuse.h"
#undef BUCKETREUSEMANAGER_DLLEXPORT
#include "configcpp.h"
#include "resourcemanager.h"


namespace joblist
{
using namespace std;
using namespace boost;

BucketReuseManager* BucketReuseManager::fInstance = NULL;

BucketReuseManager* BucketReuseManager::instance()
{
	if (fInstance == NULL) fInstance = new BucketReuseManager;
	return fInstance;
}


BucketReuseManager::~BucketReuseManager()
{
	// stop the Eraser
	fEraserStop = true;
	fNewVer.notify_one();
	fEraser->join();

	for (BucketReuseMap::iterator it = fControlMap.begin(); it != fControlMap.end(); it++)
		delete it->second;

	for (list<BucketReuseControlEntry*>::iterator it = fToRemoveEntries.begin();
			it != fToRemoveEntries.end(); it++)
		delete *it;

	delete fEraser;
	fEraser = NULL;

	// don't call delete, already in destructor
	fInstance = NULL;
}


void BucketReuseManager::startup(ResourceManager& rm)
{
	fReuseDir = rm.getScTempDiskPath();
	fReuseDir += "/bucketreuse";

	filesystem::path diskPath(fReuseDir);
	if (filesystem::exists(diskPath))
		filesystem::remove_all(diskPath);    // removes diskPath dir and all its contents

	filesystem::create_directory(diskPath);
	if (!filesystem::exists(diskPath))
		throw runtime_error("Failed to create bucket reuse directory");

	vector<string>	columns(rm.getHbrPredicate());
// 	vector<string> columns;
// 	cf->getConfig("HashBucketReuse", "Predicate", columns);

	// if no predicates defined, no need to make a calpont system catalog
	if (columns.size() == 0)
		return;

	// no need to check duplicate elements in columns vector,
	// because map will ensure the values inserted is unique
	for (vector<string>::iterator it = columns.begin(); it != columns.end(); ++it)
	{
		string value = algorithm::erase_all_copy(*it," ");
		algorithm::to_lower(value);

		// column namestring:filterstring, first search for the ":"
		size_t separate = value.find_first_of(":");

		// get the column name part
		string name = value.substr(0, separate);
		size_t schema = name.find_first_of(".");
		size_t column = name.find_last_of(".");

		if (schema == string::npos || column == string::npos || schema == column)
		{
			cerr << "incorrect column name in HashBucketReuse: " << name << endl;
			continue;
		}

		execplan::CalpontSystemCatalog::TableColName columnName;
        columnName.schema = value.substr(0, schema);
        columnName.table  = value.substr(schema+1, column-schema-1);
        columnName.column = value.substr(column+1);

		// the filter string is not parsed in iter15 because only "allrows" is supported
		// when arbitrary predicates are supported, may need to parse the filter part
		string filter;
		if (separate == string::npos || separate == value.length())
			filter = "allrows";
		else
			filter = value.substr(separate+1);

		fConfigMap.insert(pair<string, BucketFileKey>(name, BucketFileKey(columnName, filter)));
	}

	initializeControlMap();
}

// caller of this method should alread get a lock to avoid race condition.
// no need to lock again in this method.
BucketReuseControlEntry* BucketReuseManager::userRegister(
									const execplan::CalpontSystemCatalog::TableColName& tcn,
									string filter, uint64_t version, uint64_t buckets, bool& scan)
{
	BucketReuseControlEntry* entry = NULL;
	if (filter.length() == 0) filter = "allrows";

	// do NOT lock here, locked in joblistfactory::checkBucketReuse()
	// boost::mutex::scoped_lock lock(fMutex);
	if (version != fVersion)
	{
		handleNewDbVersion(version);
	}

	// default to true, unless entry found not in null state
	scan = true;

	BucketReuseMap::iterator it = fControlMap.find(BucketFileKey(tcn, filter));
	if (it != fControlMap.end())
	{
		entry = it->second;
		if (entry->fileStatus() == BucketReuseControlEntry::null_c)
		{
			entry->fBucketCount = buckets;
			entry->fUserCount = 1;
			entry->fileStatus(BucketReuseControlEntry::progress_c);
		}
		else
		{
			if (entry->fBucketCount == buckets)
			{
				scan = false;
				entry->fUserCount++;
				if (entry->fStatus == BucketReuseControlEntry::ready_c)
					entry->fStatus =  BucketReuseControlEntry::using_c;
			}
			else
			{
				// how to handle bucket change??
				entry = NULL;
			}
		}
	}

	return entry;
}

void BucketReuseManager::userDeregister(BucketReuseControlEntry* entry)
{
	boost::mutex::scoped_lock lock(BucketReuseManager::instance()->getMutex());
	entry->fUserCount--;
	if (entry->fUserCount == 0)
	{
		if (entry->fStatus == BucketReuseControlEntry::obsolete_c)
		{
			entry->fStatus = BucketReuseControlEntry::finished_c;
			signalToCleanup();
		}
		else
		{
			entry->fStatus = BucketReuseControlEntry::ready_c;
		}
	}
}

void BucketReuseManager::initializeControlMap()
{
	for (ReuseConfigMap::iterator it = fConfigMap.begin(); it != fConfigMap.end(); ++it)
	{
		stringstream ss;
		ss << fReuseDir << "/" << it->first << "." << it->second.second << ".ver" << fVersion;
		fControlMap[it->second] = new BucketReuseControlEntry(ss.str().c_str());
	}
}

void BucketReuseManager::handleNewDbVersion(uint64_t version)
{
	fVersion = version;

	// move all entries in the reuse map to remove list, let the cleanup thread to further process
	for (BucketReuseMap::iterator mapIt = fControlMap.begin(); mapIt != fControlMap.end(); ++mapIt)
		fToRemoveEntries.push_back(mapIt->second);

	if (fEraser != NULL)
		signalToCleanup();
	else
		fEraser = new boost::thread(Eraser(this));

	initializeControlMap();
}

void BucketReuseManager::cleanupOldVersions()
{
	// let the query thread continue to make job list
	boost::thread::yield();

	while (fEraserStop != true)
	{
		boost::mutex::scoped_lock lock(BucketReuseManager::instance()->getMutex());
		list<BucketReuseControlEntry*>::iterator it;
		for (it = fToRemoveEntries.begin(); it != fToRemoveEntries.end(); )
		{
			BucketReuseControlEntry* entry = *it;
			if (entry->fileStatus() == BucketReuseControlEntry::null_c)
			{
				it = fToRemoveEntries.erase(it);
				delete entry;
			}
			else if (entry->fileStatus() == BucketReuseControlEntry::ready_c ||
					entry->fileStatus() == BucketReuseControlEntry::finished_c)
			{
				for (uint64_t i = 0; i < entry->bucketCount(); ++i)
				{
					stringstream ss;
					ss << entry->baseName() << "." << i;

					boost::filesystem::remove(ss.str().c_str());
				}

				it = fToRemoveEntries.erase(it);
				delete entry;
			}
			else
			{
				entry->fileStatus(BucketReuseControlEntry::obsolete_c);
				++it;
			}
		}

		if (fToRemoveEntries.size() == 0)
		{
			delete fEraser;
			fEraser = NULL;
			break;
		}

		// give up the current time slice when waken up
		// this thread should have lower priority
		fNewVer.wait(lock);
		boost::thread::yield();
	}
}

void BucketReuseManager::signalToCleanup()
{
	fNewVer.notify_one();
}

ostream& operator<<(ostream& os, const BucketReuseControlEntry& entry)
{
	const char* statsName[] = {"null", "progress", "using", "ready", "obsolete", "finished"};
	os << "basename: " << entry.fFileBaseName << "buckets:" << entry.fBucketCount
	   << " status: " << statsName[entry.fStatus] << " users:" << entry.fUserCount << endl;

	return os;
}

void BucketReuseControlEntry::notifyUsers()
{
	boost::mutex::scoped_lock lock(BucketReuseManager::instance()->getMutex());
	if (fStatus == progress_c) fStatus = using_c;  // could be already obsolete
	fNotified = true;
	lock.unlock();
	fStateChange.notify_all();
}

}
