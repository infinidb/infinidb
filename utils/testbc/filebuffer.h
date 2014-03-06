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
 *   $Id: filebuffer.h 643 2008-06-30 16:39:59Z jrodriguez $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
***************************************************************************/

#ifndef FILEBUFFER_H
#define FILEBUFFER_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>
#include <stdint.h>
#include <time.h>
#include <we_define.h>
#include "brmtypes.h"
#include <list>
#include <vector>

/**
	@author Jason Rodriguez <jrodriguez@calpont.com>
*/


/**
 * @brief represents a disk blockrequest
 **/
namespace dbbc {

//Set block cache alogorithm to last recently used by defining LRU.
//Otherwise it will be FIFO.
typedef struct FBData{
	BRM::LBID_t lbid;
	BRM::VER_t ver;
} FBData_t;

//@bug 669 Change to list for last recently used cache 
typedef std::list<FBData_t> filebuffer_list_t;
typedef std::list<FBData_t>::iterator filebuffer_list_iter_t;

class FileBuffer {

public:
	
	/**
	 * @brief copy ctor
	 **/
	FileBuffer(const FileBuffer& fb);

	/**
	 * @brief the disk block from lbid@ver, and a data block len bytes long
	 **/
	FileBuffer(const BRM::LBID_t lbid, const BRM::VER_t ver, const uint8_t* data, const uint32_t len);

	/**
	 * @brief disk block lbid@ver and empty data
	 **/
	FileBuffer(const BRM::LBID_t lbid, const BRM::VER_t ver);

	/**
	 * @brief class dtor
	 **/
    ~FileBuffer();

	/**
	 * @brief set the data value of this block to d have len bytestream
	 **/
	void setData(const uint8_t* d, const int len=8192);

	/**
	 * @brief retrieve the data in byte* format from this data block
	 **/
	const uint8_t* getData() const {return fByteData;}
	uint8_t* getData() {return fByteData;}

	const uint32_t datLen() const {return fDataLen;}

	/**
	 * @brief assignment operator
	 **/
	FileBuffer& operator= (const FileBuffer& rhs);

	/**
	 * @brief equality operator is based on lbid@ver
	 **/
	bool operator==(const FileBuffer& rhs) const {
		return (fLbid == rhs.fLbid && fVerid == rhs.fVerid);
	}

	/**
	 * @brief inequality operator
	 **/
	bool operator!=(const FileBuffer& rhs) const {
		return (!(fLbid == rhs.fLbid && fVerid == rhs.fVerid));
	}

	FileBuffer* thisPtr() {return this;}
	/**
	 * @brief return the lbid value of disk bloc
	 **/
	const BRM::LBID_t Lbid() const {return fLbid;}
	void Lbid(const BRM::LBID_t l) {fLbid=l;}

	/**
	 * @brief return the version of this disk block. ignored for range retrievals
	 **/
	const BRM::VER_t Verid() const {return fVerid;}
	void Verid(BRM::VER_t v) {fVerid=v;}

	/**
	 * @brief return the number of bytes in this disk blockrequest
	 **/

	void listLoc(const filebuffer_list_iter_t& loc) {fListLoc = loc;}

	const filebuffer_list_iter_t& listLoc() const {return fListLoc;}

private:

	uint8_t fByteData[WriteEngine::BYTE_PER_BLOCK];
	uint32_t fDataLen;

	BRM::LBID_t fLbid;
	BRM::VER_t fVerid;
	filebuffer_list_iter_t fListLoc;

	// do not implement
	FileBuffer() {};
};

typedef std::vector<FileBuffer> FileBufferPool_t;

}
#endif
