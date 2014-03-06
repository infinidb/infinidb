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

/*******************************************************************************
* $Id: we_getfilesizes.h 4450 2013-04-15 14:13:24Z chao $
*
*******************************************************************************/
#ifndef WE_GETFILESIZES_H__
#define WE_GETFILESIZES_H__

#include <unistd.h>
#include <string>

#include "bytestream.h"

#include "atomicops.h"

namespace WriteEngine {

/** @brief Get all file sizes for the given table
 */
class WE_GetFileSizes
{
public:
	
	static int processTable(messageqcpp::ByteStream& bs, std::string& errMsg, int key);

};

class ActiveThreadCounter
{
public:
	ActiveThreadCounter(int size) : factiveThreadCount(size){}
	virtual ~ActiveThreadCounter() {}

	void decr()
	{
		int atc;
		for (;;) {
			atomicops::atomicMb();
			atc = factiveThreadCount;
			if (atc <= 0)		//hopefully atc will never be < 0!
				return;
			if (atomicops::atomicCAS(&factiveThreadCount, atc, (atc - 1)))
				return;
			atomicops::atomicYield();
		}
	}

	uint32_t cur() 
	{ 
		return factiveThreadCount; 
	}

private:
	ActiveThreadCounter(const ActiveThreadCounter& rhs);
	ActiveThreadCounter& operator=(const ActiveThreadCounter& rhs);

	volatile int32_t factiveThreadCount;
};

}

#endif
