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

// $Id: cacheutils.h 3518 2013-01-31 19:13:17Z pleblanc $
//
/** @file */

#ifndef CACHEUTILS_H__
#define CACHEUTILS_H__

#include "brmtypes.h"

/**
 * block cache utilities
 */
namespace cacheutils
{
	/** @brief flush the block cache
	 *
	 * Use the config file and messaging to signal all PrimProc's to flush their caches.
	 */
	int flushPrimProcCache();

	/** @brief flush these blocks from cache
	 *
	 * Use the config file and messaging to signal all PrimProc's to flush their caches of any of
	 * the specified LBID@@Vers.
	 */
	int flushPrimProcBlocks(const BRM::BlockList_t& list);
	
	/** @brief flush all version of blocks from cache
	 *
	 * Use the config file and messaging to signal all PrimProc's to flush their caches of any of
	 * the specified LBIDs.
	 */
	int flushPrimProcAllverBlocks(const std::vector<BRM::LBID_t> &list);

	/** @brief flush all versions of all lbids belonging to the given oids.
	 *
	 * Flush all versions of all lbids belonging to the given oids.
	 */
	int flushOIDsFromCache(const std::vector<BRM::OID_t> &);

	/** @brief Flush all versions of all lbids for the given OIDs and partition number.
	 *
	 * Flush all versions of all lbids for the given OIDs and partition number.
	 */
	int flushPartition(const std::vector<BRM::OID_t> &, std::set<BRM::LogicalPartition>& partitionNum);

	/** @brief drop file descriptor cache
	 *
	 * Use the config file and messaging to signal all PrimProc's to drop the fd cache
	 */
	int dropPrimProcFdCache();

}
// vim:ts=4 sw=4:

#endif

