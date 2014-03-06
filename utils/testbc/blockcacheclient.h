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
 *   $Id: blockcacheclient.h 684 2008-08-19 22:22:59Z pleblanc $
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

namespace dbbc {
class blockCacheClient {
	
public:

	/**
	 * @brief ctor requires reference to BlockRequestProcessor object
	 **/
	blockCacheClient(BlockRequestProcessor& brp);

	/**
	 * @brief dtor
	 **/
    virtual ~blockCacheClient();

	/**
	 * @brief verify that the Disk Block for the LBID lbid, ver are loaded into the Cache.
	 **/
	void check(BRM::LBID_t lbid, BRM::VER_t ver, bool flg, bool& wasBlockInCache);
	
	/**
	 * @brief verify all Disk Blocks for the LBID range are loaded into the Cache
	 **/
	void check(const BRM::InlineLBIDRange& range, const BRM::VER_t ver, uint32_t& rCount);

	/**
	 * @brief retrieve the Disk Block at lbid, ver from the Disk Block Buffer Cache
	 **/
	const int read(const BRM::LBID_t& lbid, const BRM::VER_t& ver, FileBuffer& fb);

	FileBuffer* getBlockPtr(const BRM::LBID_t& lbid, const BRM::VER_t& ver);
	
	/**
	 * @brief retrieve the Disk Block at lbid, ver from the Disk Block Buffer Cache
	 **/
	const int read(const BRM::LBID_t& lbid, const BRM::VER_t& ver, void* bufferPtr);

	/**
	 * @brief retrieve all disk Blocks in the LBIDRange range and insert them into fbList
	 **/
	const int read(const BRM::InlineLBIDRange& range, FileBufferList_t& fbList, const BRM::VER_t ver);
	
	const int getBlock(const BRM::LBID_t& lbid, const BRM::VER_t& ver, void* bufferPtr, 
		bool flg, bool &wasCached);

	bool exists(BRM::LBID_t lbid, BRM::VER_t ver);

	/**
	 * @brief flush the cache
	 **/
	void flushCache();

private:

	/**
	 * @brief pointer to the BlockRequestProcessor object on which the API will operate
	 **/
	BlockRequestProcessor* fBCCBrp;
	
	//do not implement
	blockCacheClient();
	blockCacheClient(const blockCacheClient& bc);
	const blockCacheClient& operator=(const blockCacheClient& blk);
	
};

}

#endif
