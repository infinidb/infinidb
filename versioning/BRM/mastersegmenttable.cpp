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
 * $Id: mastersegmenttable.cpp 1904 2013-06-14 18:30:46Z dhall $
 *
 ****************************************************************************/

#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <sys/types.h>
#include <cerrno>
using namespace std;

#include <boost/thread.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/version.hpp>
namespace bi=boost::interprocess;

#include "brmtypes.h"
#include "calpontsystemcatalog.h"
#include "rwlock.h"
using namespace rwlock;
#define MASTERSEGMENTTABLE_DLLEXPORT
#include "mastersegmenttable.h"
#undef MASTERSEGMENTTABLE_DLLEXPORT

namespace
{
using namespace BRM;

}

namespace BRM {
 
/*static*/
boost::mutex MasterSegmentTableImpl::fInstanceMutex;

/*static*/
MasterSegmentTableImpl* MasterSegmentTableImpl::fInstance=0;

/*static*/
MasterSegmentTableImpl* MasterSegmentTableImpl::makeMasterSegmentTableImpl(int key, int size)
{
	boost::mutex::scoped_lock lk(fInstanceMutex);

	if (fInstance)
		return fInstance;

	fInstance = new MasterSegmentTableImpl(key, size);

	return fInstance;
}

MasterSegmentTableImpl::MasterSegmentTableImpl(int key, int size)
{
	string keyName = ShmKeys::keyToName(key);
	try
	{
#if BOOST_VERSION < 104500
		bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write);
#ifdef __linux__
		{
			string pname = "/dev/shm/" + keyName;
			chmod(pname.c_str(), 0666);
		}
#endif
#else
		bi::permissions perms;
		perms.set_unrestricted();
		bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write, perms);
#endif
		shm.truncate(size);
		fShmobj.swap(shm);
	}
	catch (bi::interprocess_exception& biex)
	{
		bi::shared_memory_object shm(bi::open_only, keyName.c_str(), bi::read_write);
#ifdef __linux__
		{
			string pname = "/dev/shm/" + keyName;
			chmod(pname.c_str(), 0666);
		}
#endif
		fShmobj.swap(shm);
	}
	catch (...)
	{
		throw;
	}
	bi::mapped_region region(fShmobj, bi::read_write);
	fMapreg.swap(region);
}

MSTEntry::MSTEntry() :
		tableShmkey(-1),
 		allocdSize(0),
 		currentSize(0)
{	
}

MasterSegmentTable::MasterSegmentTable()
{
#ifdef _MSC_VER
	const char* envp = getenv("SystemRoot");
	string SystemRoot;
	if (envp && *envp)
		SystemRoot = envp;
	else
		SystemRoot = "C:\\WINDOWS";
	string tmpEnv = "TMP=" + SystemRoot + "\\Temp";
	_putenv(tmpEnv.c_str());
#endif

	int i;
	bool initializer = false;
	
	RWLockKeys[0] = fShmKeys.KEYRANGE_EXTENTMAP_BASE;
	RWLockKeys[1] = fShmKeys.KEYRANGE_EMFREELIST_BASE;
	RWLockKeys[2] = fShmKeys.KEYRANGE_VBBM_BASE;
	RWLockKeys[3] = fShmKeys.KEYRANGE_VSS_BASE;
	RWLockKeys[4] = fShmKeys.KEYRANGE_CL_BASE;
		
    try
    {
        // if initializer is returned false, then this is not the first time for this key.
        rwlock[0].reset(new RWLock(RWLockKeys[0], &initializer));
	}
	catch (exception &e) 
    {
		cerr << "ControllerSegmentTable: RWLock() threw: " << e.what() << endl;
		throw;
	}
	if (rwlock[0] == NULL) {
		cerr << "ControllerSegmentTable(): RWLock() failed..?" << endl;
		throw runtime_error("ControllerSegmentTable(): RWLock() failed..?");
	}
	
	for (i = 1; i < nTables; i++)
		rwlock[i].reset(new RWLock(RWLockKeys[i]));
	
	makeMSTSegment();
	if (initializer) {
		initMSTData();
		rwlock[0]->write_unlock();
	}
	else {
		rwlock[0]->read_lock_priority();     // this is to synch with the initializer
		rwlock[0]->read_unlock();
	}
}

MasterSegmentTable::~MasterSegmentTable()
{
//	int i;
	
//	for (i = 0; i < nTables; i++)
//		delete rwlock[i];
}
	
void MasterSegmentTable::makeMSTSegment()
{	
	fPImpl = MasterSegmentTableImpl::makeMasterSegmentTableImpl(fShmKeys.MST_SYSVKEY, MSTshmsize);
	fShmDescriptors = static_cast<MSTEntry*>(fPImpl->fMapreg.get_address());
}

void MasterSegmentTable::initMSTData()
{
	memset(fShmDescriptors, 0, MSTshmsize);
}

MSTEntry* MasterSegmentTable::getTable_read(int num, bool block) const
{
	if (num < 0 || num > nTables - 1)
		throw std::invalid_argument("ControllerSegmentTable::getTable_read()");
	if (!block)
		try {
			rwlock[num]->read_lock(false);
		}
		catch(rwlock::wouldblock& e) {
			return NULL;
		}
	else
		rwlock[num]->read_lock();

	return &fShmDescriptors[num];
}

MSTEntry* MasterSegmentTable::getTable_write(int num, bool block) const
{
	if (num < 0 || num > nTables - 1)
		throw std::invalid_argument("ControllerSegmentTable::getTable_write()");
	if (!block)
		try {
			rwlock[num]->write_lock(false);
		}
		catch(rwlock::wouldblock& e) {
			return NULL;
		}
	else
		rwlock[num]->write_lock();
	
	return &fShmDescriptors[num];
}

void MasterSegmentTable::getTable_upgrade(int num) const
{
	if (num < 0 || num > nTables - 1)
		throw std::invalid_argument("ControllerSegmentTable::getTable_upgrade()");
	
	rwlock[num]->upgrade_to_write();
}

void MasterSegmentTable::getTable_downgrade(int num) const
{
	if (num < 0 || num > nTables - 1)
		throw std::invalid_argument("ControllerSegmentTable::getTable_downgrade()");
	
	rwlock[num]->downgrade_to_read();
}

void MasterSegmentTable::releaseTable_read(int num) const
{
	if (num < 0 || num >= nTables)
		throw std::invalid_argument("ControllerSegmentTable::releaseTable()");
	
	rwlock[num]->read_unlock();
}

void MasterSegmentTable::releaseTable_write(int num) const
{
	if (num < 0 || num >= nTables)
		throw std::invalid_argument("ControllerSegmentTable::releaseTable()");
	
	rwlock[num]->write_unlock();
}

}		//namespace
