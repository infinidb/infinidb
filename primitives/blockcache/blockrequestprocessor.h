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

#ifndef BLOCKREQUESTPROCESSOR_H
#define BLOCKREQUESTPROCESSOR_H

/***************************************************************************
 *
 *   $Id: blockrequestprocessor.h 2055 2013-02-08 19:09:09Z pleblanc $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/

#include <boost/thread.hpp>

#include "blocksize.h"
#include "fileblockrequestqueue.h"
#include "filebuffermgr.h"
#include "iomanager.h"
#include <stdint.h>

/**
	@author Jason Rodriguez <jrodriguez@calpont.com>
*/

namespace dbbc {

typedef std::list<FileBuffer> FileBufferList_t;

/**
 * @brief class to control the populating of the Disk Block Buffer Cache and manage Block requests.
 **/
 
class BlockRequestProcessor {

public:
	
	/**
	 * @brief default ctor
	 **/
    BlockRequestProcessor(uint32_t numBlcks, int thrCount, int blocksPerRead, uint32_t deleteBlocks=0,
		uint32_t blckSz=BLOCK_SIZE);

	/**
	 * @brief default dtor
	 **/
	virtual ~BlockRequestProcessor();

	/**
	 * @brief send a request for disk blocks to the IO manager
	 **/
	int sendRequest(fileRequest& blk) {
		return fBRPRequestQueue.push(blk); }

	/**
	 * @brief verify that the lbid@ver disk block is in the block cache. Send request if it is not
	 **/
	int check(BRM::LBID_t lbid, const BRM::QueryContext &ver, BRM::VER_t txn, bool flg, int compType, bool& wasBlockInCache);

	/**
	 * @brief verify the LBIDRange of disk blocks is in the block cache. Send request if it is not
	 **/
	int check(const BRM::InlineLBIDRange& range, const BRM::QueryContext &ver, const BRM::VER_t txn, const int compType,
		uint32_t& lbidCount);

	/**
	 * @brief retrieve the lbid@ver disk block from the block cache
	 **/
	inline FileBuffer* getBlockPtr(const BRM::LBID_t lbid, const BRM::VER_t ver, bool flg) {
		return fbMgr.findPtr(HashObject_t(lbid, ver, flg)); }

	inline const int read(const BRM::LBID_t& lbid, const BRM::VER_t& ver, FileBuffer& fb) {
		return (fbMgr.find(HashObject_t(lbid, ver, 0), fb) ? 1 : 0); }

	/**
	 * @brief retrieve the lbid@ver disk block from the block cache
	 **/
	inline const int read(const BRM::LBID_t& lbid, const BRM::VER_t &ver, void* bufferPtr) {
		return (fbMgr.find(HashObject_t(lbid, ver, 0), bufferPtr) ? 1 : 0); }
	
	const int getBlock(const BRM::LBID_t& lbid, const BRM::QueryContext &ver, BRM::VER_t txn, int compType,
		void* bufferPtr, bool flg, bool &wasCached, bool *wasVersioned, bool insertIntoCache,
		bool readFromCache);

	int getCachedBlocks(const BRM::LBID_t *lbids, const BRM::VER_t *vers, uint8_t **ptrs,
		bool *wasCached, uint32_t count);

	inline bool exists(BRM::LBID_t lbid, BRM::VER_t ver) {
		return fbMgr.exists(HashObject_t(lbid, ver, 0)); }

	/**
	 * @brief 
	 **/
	void flushCache() {
		fbMgr.flushCache(); }

	/**
	 * @brief 
	 **/
	void flushOne(BRM::LBID_t lbid, BRM::VER_t ver) {
		fbMgr.flushOne(lbid, ver); }

	void flushMany(const LbidAtVer* laVptr, uint32_t cnt) {
		fbMgr.flushMany(laVptr, cnt); }
		
	void flushManyAllversion(const BRM::LBID_t* laVptr, uint32_t cnt) {
		fbMgr.flushManyAllversion(laVptr, cnt); }

	void flushOIDs(const uint32_t *oids, uint32_t count) {
		fbMgr.flushOIDs(oids, count); }

	void flushPartition(const std::vector<BRM::OID_t> &oids, const std::set<BRM::LogicalPartition> &partitions) {
		fbMgr.flushPartition(oids, partitions); }

	void setReportingFrequency(const uint32_t d) {
		fbMgr.setReportingFrequency(d); }

	uint32_t ReportingFrequency() const {return fbMgr.ReportingFrequency();}

	std::ostream& formatLRUList(std::ostream& os) const {
		return fbMgr.formatLRUList(os); }
	
private:
	
	FileBufferMgr fbMgr;
	fileBlockRequestQueue fBRPRequestQueue;
	ioManager fIOMgr;
	boost::mutex check_mutex;

	/**
	 * helper function for public check functions
	 **/
	int check(fileRequest& rqstBlk);

	/**
	 * send stop requests for IOmanager and request Q
	 **/
	void stop();

	std::ofstream fLogFile;
	bool fTrace;

	BRM::DBRM fdbrm;

	// do not implement
	BlockRequestProcessor(const BlockRequestProcessor& brp);
	BlockRequestProcessor& operator=(const BlockRequestProcessor& brp);
	
};

}
#endif
// vim:ts=4 sw=4:

