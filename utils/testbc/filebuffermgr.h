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
 *   $Id: filebuffermgr.h 699 2008-09-09 19:44:18Z rdempsey $
 *
 *                                                                         *
 ***************************************************************************/

#ifndef FILEBUFFERMGR_H
#define FILEBUFFERMGR_H
#include <pthread.h>
#include "blocksize.h"
#include "filebuffer.h"
#include "rwlock_local.h"
#include <tr1/unordered_set>
#include <boost/thread.hpp>

/**
	@author Jason Rodriguez <jrodriguez@calpont.com>
*/

/**
 * @brief manages storage of Disk Block Buffers via and LRU cache using the stl classes unordered_set and list.
 * 
 **/

namespace dbbc {

/**
 * @brief used as the hasher algorithm for the unordered_set used to store the disk blocks
 **/

typedef struct {
	BRM::LBID_t lbid;
	BRM::VER_t ver;
	uint32_t poolIdx;	
	} FileBufferIndex_t;

typedef FileBufferIndex_t HashObject_t;

class bcHasher
{
	public:
		size_t operator()(const HashObject_t& rhs) const
		{
			return (((rhs.ver & 0xffffULL) << 48) | (rhs.lbid & 0xffffffffffffULL));
		}
};

class bcEqual
{
	public:
		size_t operator()(const HashObject_t& f1, const HashObject_t& f2) const
		{
			return ((f1.lbid == f2.lbid) && (f1.ver == f2.ver));
		}
};

inline bool operator<(const HashObject_t& f1, const HashObject_t& f2)
{
	//return ((f1.lbid < f2.lbid) || (f1.ver < f2.ver));
#if 1
	if (f1.lbid < f2.lbid)
		return true;
	else if (f1.lbid == f2.lbid)
		return (f1.ver < f2.ver);

	return false;
#else
	bcHasher bh1, bh2;
	return (bh1(f1) < bh2(f2));
#endif
}


class FileBufferMgr {

public:

	typedef std::tr1::unordered_set<HashObject_t, bcHasher, bcEqual> filebuffer_uset_t;
	typedef std::tr1::unordered_set<HashObject_t, bcHasher, bcEqual>::const_iterator filebuffer_uset_iter_t; 
	typedef std::pair<filebuffer_uset_t::iterator, bool> filebuffer_pair_t; // return type for insert

	typedef std::vector<uint32_t> intvec_t; 

	/**
	 * @brief ctor. Set max buffer size to numBlcks and block buffer size to blckSz
	 **/

	FileBufferMgr(uint32_t numBlcks, uint32_t blckSz=BLOCK_SIZE, uint32_t deleteBlocks = 0);

	/**
	 * @brief default dtor
	 **/
    virtual ~FileBufferMgr();

	/**
	 * @brief return TRUE if the Disk block lbid@ver is loaded into the Disk Block Buffer cache otherwise return FALSE.
	 **/
	bool exists(const BRM::LBID_t& lbid, const BRM::VER_t& ver) const;
	
	/**
	 * @brief return TRUE if the Disk block referenced by fb is loaded into the Disk Block Buffer cache otherwise return FALSE.
	 **/
	bool exists(const HashObject_t& fb) const;

	/**
	 * @brief add the Disk Block reference by fb into the Disk Block Buffer Cache
	 **/
	const int insert(const BRM::LBID_t lbid, const BRM::VER_t ver, const uint8_t* data);

	/**
	 * @brief returns the total number of Disk Blocks in the Cache
	 **/
	uint32_t size() const {return fbSet.size();}

	/**
	 * @brief 
	 **/
	void flushCache();

	/**
	 * @brief return the disk Block referenced by fb
	 **/

	FileBuffer* findPtr(const HashObject_t& keyFb);

	bool find(const HashObject_t& keyFb, FileBuffer& fb);

	/**
	 * @brief return the disk Block referenced by bufferPtr
	 **/

	bool find(const HashObject_t& keyFb, void* bufferPtr);
	
	uint32_t maxCacheSize() const {return fMaxNumBlocks;}

	uint32_t listSize() const {return fbList.size();}

 	const filebuffer_uset_iter_t end() const {return fbSet.end();}

	void displayCounts() const;

	std::ostream& formatLRUList(std::ostream& os) const;

private:

	uint32_t fMaxNumBlocks; 	// the max number of blockSz blocks to keep in the Cache list
	uint32_t fBlockSz; 		// size in bytes size of a data block - probably 8

 	mutable boost::mutex fWLock;
	mutable filebuffer_uset_t fbSet;

	mutable filebuffer_list_t fbList; // rename this
	uint32_t fCacheSize;

	FileBufferPool_t fFBPool; // vector<FileBuffer>
	
	// do not implement
	FileBufferMgr(const FileBufferMgr& fbm);
	const FileBufferMgr& operator =(const FileBufferMgr& fbm);
	bool aging;

	uint32_t fDeleteBlocks;
	intvec_t fEmptyPoolSlots;	//keep track of FBPool slots that can be reused

	void depleteCache();
};
}
#endif
