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

// $Id: shmkeys.cpp 1823 2013-01-21 14:13:09Z rdempsey $

#include <cstdlib>
#include <string>
#include <sstream>
#include <iomanip>
using namespace std;

#ifdef WANT_EM_BRM_UID
#include "configcpp.h"
#endif
#define SHMKEYS_DLLEXPORT
#include "shmkeys.h"
#undef SHMKEYS_DLLEXPORT

namespace BRM
{

ShmKeys::ShmKeys()
{
	uint32_t BRM_UID = 0x0;

#ifdef WANT_EM_BRM_UID
	config::Config* cf = config::Config::makeConfig();
	string brm_str = cf->getConfig("ExtentMap", "BRM_UID");
	if (brm_str.length() > 0)
		BRM_UID = static_cast<uint32_t>(config::Config::uFromText(brm_str));
#endif

	KEYRANGE_VSS_BASE =          0x10000 | (BRM_UID << 20);
	KEYRANGE_EXTENTMAP_BASE =    0x20000 | (BRM_UID << 20);
	KEYRANGE_EMFREELIST_BASE =   0x30000 | (BRM_UID << 20);
	KEYRANGE_VBBM_BASE =         0x40000 | (BRM_UID << 20);
	KEYRANGE_CL_BASE =           0x50000 | (BRM_UID << 20);
	MST_SYSVKEY =             0xff000000 | BRM_UID;
	PROCESSSTATUS_SYSVKEY =   0xfd000000 | BRM_UID;
	SYSTEMSTATUS_SYSVKEY =    0xfc000000 | BRM_UID;
	SWITCHSTATUS_SYSVKEY =    0xfb000000 | BRM_UID;
	STORAGESTATUS_SYSVKEY =   0xfa000000 | BRM_UID;
	NICSTATUS_SYSVKEY =       0xf9000000 | BRM_UID;
	DBROOTSTATUS_SYSVKEY =    0xf8000000 | BRM_UID;
	DECOMSVRMUTEX_SYSVKEY =   0xf7000000 | BRM_UID;
}

string ShmKeys::keyToName(unsigned key)
{
	ostringstream oss;
	oss << "InfiniDB-shm-";
	oss << setw(8) << setfill('0') << hex << key;
	return oss.str();
}

}

