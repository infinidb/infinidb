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
 * $Id: objectidmanager.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 ****************************************************************************/

/** @file
 * This file implements ObjectIDManager
 *
 * The ObjectIDManager is responsible for allocating and deallocating
 * Object IDs from a 24-bit space consistently across all processes & threads.
 * The expected allocation size is 1 or 2 OIDs, and the usage pattern is
 * unknown.  The OID space is described by a 16Mbit bitmap file on disk
 * and a brief free list.  Accesses are synchronized by using flock().
 *
 * This class must implement a high degree of correctness.  However, it
 * also requires file IO.  Most functions throw an exception if a hard IO error
 * occurs more than MaxRetries times in a row.  Right now the code makes
 * no attempt to back out changes that occured before the error although it
 * may be possible to do so for certain errors.  Probably the best course of 
 * action would be to halt the system if an exception is thrown here
 * to prevent database corruption resulting allocation of OIDs from a 
 * possibly corrupt OID bitmap.  The OID bitmap can be rebuilt at system
 * startup if necessary (need to write a tool to do that still).
 *
 * There are a few checks to verify the safety of allocations and 
 * deallocations & the correctness of this implementation in general.  
 * Those errors will throw logic_error.  IO errors will throw 
 * ios_base::failure.
 *
 * There are probably oodles of optimizations possible given this implmementation.
 * For example:
 * 		- make fullScan() construct a freelist
 *		- sorting & coalescing free list entries will raise the hit rate 
 *		  (at what cost?)
 *		- implement a bias for high numbered OIDs in the free list to reduce
 *	 	  the number of fullScans searching far in the bitmap
 *		- avoid the extra read in flipOIDBlock when called by fullscan
 * 		- singleton object operating entirely in memory (no file IO)
 *			- singleton object using only a free list (no bitmap)
 *		- a different implementation altogether.
 *
 * Time permitting we might look into those things.
 */

#include "objectidmanager.h"
#include "configcpp.h"
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <cstring>
#include <stdexcept>
//#define NDEBUG
#include <boost/thread.hpp>

using namespace std;

#include "dbrm.h"

#ifndef O_BINARY
#  define O_BINARY 0
#endif
#ifndef O_DIRECT
#  define O_DIRECT 0
#endif
#ifndef O_LARGEFILE
#  define O_LARGEFILE 0
#endif
#ifndef O_NOATIME
#  define O_NOATIME 0
#endif

namespace
{
boost::mutex CtorMutex;
}

namespace execplan {

ObjectIDManager::ObjectIDManager()
{
	boost::mutex::scoped_lock lk(CtorMutex);

	config::Config* conf;
	string tmp;
	
	conf = config::Config::makeConfig();
	try {
		fFilename = conf->getConfig("OIDManager", "OIDBitmapFile");
	}
	catch(exception&) {
		fFilename = "/mnt/OAM/dbrm/oidbitmap";
	}

	if (fFilename.empty())
		fFilename = "/mnt/OAM/dbrm/oidbitmap";
}

ObjectIDManager::~ObjectIDManager()
{
}	
			
int ObjectIDManager::allocOID()
{
	return allocOIDs(1);
}

int ObjectIDManager::allocVBOID(uint32_t dbroot)
{
	return dbrm.allocVBOID(dbroot);
}

int ObjectIDManager::getDBRootOfVBOID(uint32_t vboid)
{
	return dbrm.getDBRootOfVBOID(vboid);
}

int ObjectIDManager::allocOIDs(int num)
{
	return dbrm.allocOIDs(num);
}

void ObjectIDManager::returnOID(int oid)
{
	returnOIDs(oid, oid);
}

void ObjectIDManager::returnOIDs(int start, int end)
{
	//@Bug 1412. Do not reuse oids for now.
	return;
}

const string ObjectIDManager::getFilename() const
{
	return fFilename;
}

int ObjectIDManager::size()
{
	return dbrm.oidm_size();
}

vector<uint16_t> ObjectIDManager::getVBOIDToDBRootMap()
{
	return dbrm.getVBOIDToDBRootMap();
}
	

}  // namespace
