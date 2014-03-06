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

/***************************************************************************
 *
 *   $Id: blockcacheclient.h 2055 2013-02-08 19:09:09Z pleblanc $
 *
 *                                                                         *
 ***************************************************************************/

#ifndef BLOCKCACHEREQUESTCLIENT_H
#define BLOCKCACHEREQUESTCLIENT_H

#include "blockrequestprocessor.h"
#include "brmtypes.h"

/**
	@author Jason Rodriguez <jrodriguez@calpont.com>
*/

/**
 * @brief API for the Disk Block Buffer Cache
 *
 * 
 */

namespace dbbc
{

class blockCacheClient
{
public:

	/**
	 * @brief ctor requires reference to BlockRequestProcessor object
	 **/
	blockCacheClient(BlockRequestProcessor& brp) : fBCCBrp(&brp) {}

	/**
	 * @brief dtor
	 **/
	virtual ~blockCacheClient() {}

	/**
	 * @brief verify that the Disk Block for the LBID lbid, ver are loaded into the Cache.
	 **/
	inline void check(BRM::LBID_t lbid, const BRM::QueryContext &ver, BRM::VER_t txn, bool flg, int compType, bool& wasBlockInCache) {
		fBCCBrp->check(lbid, ver, txn, flg, compType, wasBlockInCache); }

	/**
	 * @brief verify all Disk Blocks for the LBID range are loaded into the Cache
	 **/
	inline void check(const BRM::InlineLBIDRange& range, const BRM::QueryContext &ver, const BRM::VER_t txn, const int compType,
		uint32_t& rCount) {
		fBCCBrp->check(range, ver, txn, compType, rCount); }

	inline FileBuffer* getBlockPtr(const BRM::LBID_t& lbid, const BRM::VER_t& ver, bool flg) {
		return fBCCBrp->getBlockPtr(lbid, ver, flg); }
	
	/**
	 * @brief retrieve the Disk Block at lbid, ver from the Disk Block Buffer Cache
	 **/
	inline const int read(const BRM::LBID_t& lbid, const BRM::VER_t& ver, FileBuffer& fb) {
		return fBCCBrp->read(lbid, ver, fb); }

	/**
	 * @brief retrieve the Disk Block at lbid, ver from the Disk Block Buffer Cache
	 **/
	inline const int read(const BRM::LBID_t& lbid, const BRM::VER_t& ver, void* bufferPtr) {
		return fBCCBrp->read(lbid, ver, bufferPtr); }
	
	inline const int getBlock(const BRM::LBID_t& lbid, const BRM::QueryContext &ver, const BRM::VER_t txn, const int compType,
		void* bufferPtr, bool flg, bool &wasCached, bool *wasVersioned = NULL, bool insertIntoCache = true,
		bool readFromCache = true) {
		return fBCCBrp->getBlock(lbid, ver, txn, compType, bufferPtr, flg, wasCached, wasVersioned, insertIntoCache,
			readFromCache); }
		
	inline int getCachedBlocks(const BRM::LBID_t *lbids, const BRM::VER_t *vers, uint8_t **bufferPtrs,
		bool *wasCached, uint32_t blockCount)
		{ return fBCCBrp->getCachedBlocks(lbids, vers, bufferPtrs, wasCached, blockCount); }

	inline bool exists(BRM::LBID_t lbid, BRM::VER_t ver) {
		return fBCCBrp->exists(lbid, ver); }

	/**
	 * @brief flush the cache
	 **/
	inline void flushCache() {
		fBCCBrp->flushCache(); }

	/**
	 * @brief flush one LBID@Ver from the cache
	 **/
	inline void flushOne(const BRM::LBID_t& lbid, const BRM::VER_t& ver) {
		fBCCBrp->flushOne(lbid, ver); }

	/**
	 * @brief flush specific LBID/version pairs from the cache
	 **/
	inline void flushMany(const LbidAtVer* laVptr, uint32_t cnt) {
		fBCCBrp->flushMany(laVptr, cnt); }
		
	/**
	 * @brief flush all versions of the given lbids from the cache.
	 **/
	inline void flushManyAllversion(const BRM::LBID_t* laVptr, uint32_t cnt) {
		fBCCBrp->flushManyAllversion(laVptr, cnt); }

	/**
	 * @brief Flush all versions of all LBIDs belonging to the given OIDs.
	 */
	inline void flushOIDs(const uint32_t *oids, uint32_t count) {
		fBCCBrp->flushOIDs(oids, count); }

	/**
	 * @brief Flush all versions of a partition from the given OIDs.
	 */
	inline void flushPartition(const std::vector<BRM::OID_t> &oids, const std::set<BRM::LogicalPartition> partitions) {
			fBCCBrp->flushPartition(oids, partitions); }

private:

	/**
	 * @brief pointer to the BlockRequestProcessor object on which the API will operate
	 **/
	BlockRequestProcessor* fBCCBrp;

	//do not implement
	blockCacheClient(const blockCacheClient& bc);
	blockCacheClient& operator=(const blockCacheClient& blk);

};

}

#endif
// vim:ts=4 sw=4:

