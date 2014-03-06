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
 *   $Id: blockcacheclient.cpp 684 2008-08-19 22:22:59Z pleblanc $
 *
 *   jrodriguez@calpont.com   *
 ***************************************************************************/
 
#include <sstream>
#include <string>
#include "blockcacheclient.h"

namespace dbbc {

blockCacheClient::blockCacheClient() {
	fBCCBrp=NULL;
}

blockCacheClient::blockCacheClient(BlockRequestProcessor& brp) {
	fBCCBrp=&brp;
}

blockCacheClient::~blockCacheClient() {
}


void blockCacheClient::check(BRM::LBID_t lbid, BRM::VER_t ver, bool flg, bool& wasBlockInCache) {
	fBCCBrp->check(lbid, ver, flg, wasBlockInCache);
}


void blockCacheClient::check(const BRM::InlineLBIDRange& range, const BRM::VER_t ver, uint32_t& rCount) {
	fBCCBrp->check(range, ver, rCount);
}


FileBuffer* blockCacheClient::getBlockPtr(const BRM::LBID_t& lbid, const BRM::VER_t& ver) {
	FileBuffer* fb = fBCCBrp->getBlockPtr(lbid, ver);
	return fb;	
}

const int blockCacheClient::read(const BRM::LBID_t& lbid, const BRM::VER_t& ver, FileBuffer& fb) {
	int ret = fBCCBrp->read(lbid, ver, fb);
	return ret;	
}

const int blockCacheClient::read(const BRM::LBID_t& lbid, const BRM::VER_t& ver, void* bufferPtr) {
	int ret = fBCCBrp->read(lbid, ver, bufferPtr);
	return ret;	
}

const int blockCacheClient::read(const BRM::InlineLBIDRange& range, FileBufferList_t& fbList, const BRM::VER_t ver) {
	int ret = fBCCBrp->read(range, fbList, ver);
	return ret;
}

const int blockCacheClient::getBlock(const BRM::LBID_t& lbid, const BRM::VER_t& ver, void* bufferPtr, bool flg, bool &wasCached) {
	int ret = fBCCBrp->getBlock(lbid, ver, bufferPtr, flg, wasCached);
	return ret;	
}

bool blockCacheClient::exists(BRM::LBID_t lbid, BRM::VER_t ver)
{
	return fBCCBrp->exists(lbid, ver);
}

void blockCacheClient::flushCache() {
	fBCCBrp->flushCache();
}

}
