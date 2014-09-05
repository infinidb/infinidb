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

// $Id: writeonce.cpp 3495 2013-01-21 14:09:51Z rdempsey $

#include "writeonce.h"

#include <string>
#include <stdexcept>
#include <sstream>
#include <fstream>
#include <errno.h>
#include <unistd.h>
//#define NDEBUG
#include <cassert>
#include <cstring>
using namespace std;

#include <boost/any.hpp>
using namespace boost;

#include "bytestream.h"
using namespace messageqcpp;

#include "installdir.h"

namespace
{
const string DefaultWriteOnceConfigFilename("woparms.dat");
}

namespace config
{

//If you add parm, you need to update all the methods below until the next comment

void WriteOnceConfig::initializeDefaults()
{
	fLBID_Shift =           make_pair("13", false);
	fDBRootCount =          make_pair("1", false);
	fDBRMRoot =             make_pair("/mnt/OAM/dbrm/BRM_saves", false);
	fSharedMemoryTmpFile1 = make_pair("/tmp/CalpontShm,", false);
	fTxnIDFile =            make_pair("/mnt/OAM/dbrm/SMTxnID", false);
	fSharedMemoryTmpFile2 = make_pair("/tmp/CalpontSessionMonitorShm", false);
}

void WriteOnceConfig::setup()
{
	typedef EntryMap_t::value_type VT;

	fEntryMap.insert(VT("PrimitiveServers.LBID_Shift",        &fLBID_Shift));
	fEntryMap.insert(VT("SystemConfig.DBRootCount",           &fDBRootCount));
	fEntryMap.insert(VT("SystemConfig.DBRMRoot",              &fDBRMRoot));
	fEntryMap.insert(VT("SessionManager.SharedMemoryTmpFile", &fSharedMemoryTmpFile1));
	fEntryMap.insert(VT("SessionManager.TxnIDFile",           &fTxnIDFile));
	fEntryMap.insert(VT("SessionMonitor.SharedMemoryTmpFile", &fSharedMemoryTmpFile2));

	ByteStream ibs = load();
	if (ibs.length() > 0)
		unserialize(ibs);
	else
		initializeDefaults();
}

void WriteOnceConfig::serialize(ByteStream& obs) const
{
	obs << WriteOnceConfigVersion;

	obs << fLBID_Shift.first;
	obs << fDBRootCount.first;
	obs << fDBRMRoot.first;
	obs << fSharedMemoryTmpFile1.first;
	obs << fTxnIDFile.first;
	obs << fSharedMemoryTmpFile2.first;
}

void WriteOnceConfig::unserialize(ByteStream& ibs)
{
	uint32_t version;
	ibs >> version;

	if (version < WriteOnceConfigVersion)
	{
		ostringstream oss;
		oss << "Invalid version found in WriteOnceConfig file: " << version;
		throw runtime_error(oss.str().c_str());
	}
	else if (version > WriteOnceConfigVersion)
	{
		ostringstream oss;
		oss << "Invalid version found in WriteOnceConfig file: " << version;
		throw runtime_error(oss.str().c_str());
	}

	ibs >> fLBID_Shift.first; fLBID_Shift.second = true;
	ibs >> fDBRootCount.first; fDBRootCount.second = true;
	ibs >> fDBRMRoot.first; fDBRMRoot.second = true;
	ibs >> fSharedMemoryTmpFile1.first; fSharedMemoryTmpFile1.second = true;
	ibs >> fTxnIDFile.first; fTxnIDFile.second = true;
	ibs >> fSharedMemoryTmpFile2.first; fSharedMemoryTmpFile2.second = true;
}


//End of methods that need to be changed when adding parms

ByteStream WriteOnceConfig::load()
{
	ByteStream bs;
	if (access(fConfigFileName.c_str(), F_OK) != 0)
	{
		initializeDefaults();
		return bs;
	}

	idbassert(access(fConfigFileName.c_str(), F_OK) == 0);

	ifstream ifs(fConfigFileName.c_str());
	int e = errno;
	if (!ifs.good())
	{
		ostringstream oss;
		oss << "Error opening WriteOnceConfig file " << fConfigFileName << ": " << strerror(e);
		throw runtime_error(oss.str().c_str());
	}
	ifs >> bs;
	return bs;
}

void WriteOnceConfig::save(ByteStream& ibs) const
{
	ofstream ofs(fConfigFileName.c_str());
	int e = errno;
	if (!ofs.good())
	{
		ostringstream oss;
		oss << "Error opening WriteOnceConfig file " << fConfigFileName << ": " << strerror(e);
		throw runtime_error(oss.str().c_str());
	}
	ofs << ibs;
}

WriteOnceConfig::WriteOnceConfig(const char* cf)
{
	string cfs;

	if (cf != 0)
		cfs = cf;
	else
		cfs = startup::StartUp::installDir() + "/etc/" + DefaultWriteOnceConfigFilename;

	fConfigFileName = cfs;

	setup();
}

void WriteOnceConfig::setConfig(const string& section, const string& name, const string& value, bool force)
{
	EntryMap_t::iterator iter;
	iter = fEntryMap.find(string(section + "." + name));
	if (iter == fEntryMap.end())
	{
		ostringstream oss;
		oss << "Invalid request for " << section << '.' << name;
		throw runtime_error(oss.str().c_str());
	}

	if ((*iter->second).second && !force)
	{
		ostringstream oss;
		oss << "Invalid attempt to write read-only " << section << '.' << name;
		throw runtime_error(oss.str().c_str());
	}

	(*iter->second).first = value;
	(*iter->second).second = true;

	ByteStream obs;
	serialize(obs);
	save(obs);
}

const string WriteOnceConfig::getConfig(const string& section, const string& name) const
{
	string val;
	EntryMap_t::const_iterator iter;
	iter = fEntryMap.find(string(section + "." + name));
	if (iter == fEntryMap.end())
	{
		ostringstream oss;
		oss << "Invalid request for " << section << '.' << name;
		throw runtime_error(oss.str().c_str());
	}

	val = (*iter->second).first;

	return val;
}

}

