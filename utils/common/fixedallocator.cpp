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

/******************************************************************************************
* $Id$
*
******************************************************************************************/

//This is one of the first files we compile, check the compiler...
#if defined(__GNUC__)
#if __GNUC__ < 4 || (__GNUC__ == 4 && __GNUC_MINOR__ < 1)
#error "This is a very old GCC, and it's probably not going to work."
#endif
#elif defined(_MSC_VER)
#else
#error "This compiler is not known and it's probably not going to work."
#endif

#include <stdint.h>
#include <iostream>

#define FIXEDALLOCATOR_DLLEXPORT
#include "fixedallocator.h"
#undef FIXEDALLOCATOR_DLLEXPORT

using namespace std;
using namespace boost;

namespace utils {

FixedAllocator::FixedAllocator(const FixedAllocator &f)
{
	elementCount = f.elementCount;
	elementSize = f.elementSize;
	tmpSpace = f.tmpSpace;
	capacityRemaining = 0;
	currentlyStored = 0;
}

FixedAllocator & FixedAllocator::operator=(const FixedAllocator &f)
{
	elementCount = f.elementCount;
	elementSize = f.elementSize;
	tmpSpace = f.tmpSpace;
	deallocateAll();
	return *this;
}

void FixedAllocator::newBlock()
{
	shared_array<uint8_t> next;

	capacityRemaining = elementCount * elementSize;
	if (!tmpSpace || mem.size() == 0) {
		next.reset(new uint8_t[elementCount * elementSize]);
		mem.push_back(next);
		nextAlloc = next.get();
	}
	else {
		currentlyStored = 0;
		nextAlloc = mem.front().get();
	}
}

void * FixedAllocator::allocate()
{
	void *ret;
	if (capacityRemaining < elementSize)
		newBlock();
	ret = nextAlloc;
	nextAlloc += elementSize;
	capacityRemaining -= elementSize;
	currentlyStored += elementSize;
	return ret;
}

void * FixedAllocator::allocate(uint32_t len)
{
	void *ret;
	if (capacityRemaining < len)
		newBlock();
	ret = nextAlloc;
	nextAlloc += len;
	capacityRemaining -= len;
	currentlyStored += len;
	return ret;
}

void FixedAllocator::truncateBy(uint32_t amt)
{
	nextAlloc -= amt;
	capacityRemaining += amt;
	currentlyStored -= amt;
}

void FixedAllocator::deallocateAll()
{
	mem.clear();
	currentlyStored = 0;
	capacityRemaining = 0;
}

uint64_t FixedAllocator::getMemUsage() const 
{
	return (mem.size() * elementCount * elementSize);
}

}
