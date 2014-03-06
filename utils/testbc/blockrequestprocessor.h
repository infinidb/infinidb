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
 *   $Id: blockrequestprocessor.h 725 2008-09-26 16:26:47Z jrodriguez $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/


#include "blocksize.h"
#include "fileblockrequestqueue.h"
#include "filebuffermgr.h"
#include "iomanager.h"

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
	int sendRequest(fileRequest& blk);

	/**
	 * @brief verify that the lbid@ver disk block is in the block cache. Send request if it is not
	 **/
	int check(BRM::LBID_t lbid, BRM::VER_t ver, bool flg, bool& wasBlockInCache);
	
	/**
	 * @brief verify the LBIDRange of disk blocks is in the block cache. Send request if it is not
	 **/
	int check(const BRM::InlineLBIDRange& range, const BRM::VER_t ver, uint32_t& lbidCount);
	
	/**
	 * @brief retrieve the lbid@ver disk block from the block cache
	 **/
	FileBuffer* getBlockPtr(const BRM::LBID_t lbid, const BRM::VER_t ver);

	const int read(const BRM::LBID_t& lbid, const BRM::VER_t& ver, FileBuffer& fb);

	/**
	 * @brief retrieve the lbid@ver disk block from the block cache
	 **/
	const int read(const BRM::LBID_t& lbid, const BRM::VER_t& ver, void* bufferPtr);
	
	/**
	 * @brief retrieve the LBIDRange of disk blocks from the block cache
	 **/
	const int read(const BRM::InlineLBIDRange& range, FileBufferList_t& fbList, const BRM::VER_t ver);
	
	const int getBlock(const BRM::LBID_t& lbid, const BRM::VER_t& ver, void* bufferPtr, 
		bool flg, bool &wasCached);

	bool exists(BRM::LBID_t lbid, BRM::VER_t ver);

	/**
	 * @brief 
	 **/
	void flushCache();

	//const uint32_t resize(const uint32_t s);

	std::ostream& formatLRUList(std::ostream& os) const;
	
private:
	
	FileBufferMgr fbMgr;
	fileBlockRequestQueue fBRPRequestQueue;
	ioManager fIOMgr;
	pthread_mutex_t check_mutex;

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
