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

// $Id: shmkeys.h 1823 2013-01-21 14:13:09Z rdempsey $
/** @file */
#ifndef BRM_SHMEYS_H_
#define BRM_SHMEYS_H_

#include <stdint.h>
#include <string>

#if defined(_MSC_VER) && defined(xxxSHMKEYS_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM
{

/** A struct to hold shared memory keys
 *
 * A struct to hold shared memory keys that can be tuned via the config file
 */
struct ShmKeys
{
public:
	EXPORT ShmKeys();
	~ShmKeys() { }

	/******** The shmseg/sem key range assigments  *******/
#if defined(COMMUNITY_KEYRANGE)
	const static uint32_t KEYRANGE_SIZE = 0x1000;
#elif defined(_MSC_VER)
	const static uint32_t KEYRANGE_SIZE = 0x3800;
#else
	const static uint32_t KEYRANGE_SIZE = 0x10000;
#endif
	uint32_t KEYRANGE_CL_BASE;
	uint32_t KEYRANGE_EXTENTMAP_BASE;
	uint32_t KEYRANGE_EMFREELIST_BASE;
	uint32_t KEYRANGE_VBBM_BASE;
	uint32_t KEYRANGE_VSS_BASE;

	/****** Fixed location assignments *******/
	uint32_t MST_SYSVKEY;
	uint32_t PROCESSSTATUS_SYSVKEY;
	uint32_t SYSTEMSTATUS_SYSVKEY;
	uint32_t SWITCHSTATUS_SYSVKEY;
	uint32_t STORAGESTATUS_SYSVKEY;
	uint32_t NICSTATUS_SYSVKEY;
	uint32_t DBROOTSTATUS_SYSVKEY;
	uint32_t DECOMSVRMUTEX_SYSVKEY;

	EXPORT static std::string keyToName(unsigned key);

private:
	//defaults okay
	//ShmKeys(const ShmKeys& rhs);
	//ShmKeys operator=(const ShmKeys& rhs);
};

}

#undef EXPORT

#endif

