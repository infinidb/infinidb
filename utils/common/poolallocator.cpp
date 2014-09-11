//
// C++ Implementation: varlenallocator
//
// Description: 
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2009
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <iostream>
//#define NDEBUG
#include <cassert>

#include "poolallocator.h"

using namespace std;
using namespace boost;

namespace utils
{

PoolAllocator::PoolAllocator(uint windowSize, bool isTmpSpace) :
	allocSize(windowSize),
	tmpSpace(isTmpSpace),
	capacityRemaining(0),
	memUsage(0),
	nextAlloc(NULL)
{ }

PoolAllocator::PoolAllocator(const PoolAllocator &p) :
	allocSize(p.allocSize),
	tmpSpace(p.tmpSpace),
	capacityRemaining(0),
	memUsage(0),
	nextAlloc(NULL)
{ }

PoolAllocator::~PoolAllocator()
{
}

PoolAllocator & PoolAllocator::operator=(const PoolAllocator &v)
{
	allocSize = v.allocSize;
	tmpSpace = v.tmpSpace;
	deallocateAll();
	return *this;
}

void PoolAllocator::deallocateAll()
{
	capacityRemaining = 0;
	nextAlloc = NULL;
	memUsage = 0;
	mem.clear();
	oob.clear();
}

void PoolAllocator::newBlock()
{
	shared_array<uint8_t> next;

	capacityRemaining = allocSize;
	if (!tmpSpace || mem.size() == 0) {
		next.reset(new uint8_t[allocSize]);
		mem.push_back(next);
		nextAlloc = next.get();
	}
	else
		nextAlloc = mem.front().get();
}

void * PoolAllocator::allocate(uint64_t size)
{
	void *ret;

	if (size > allocSize) {
		OOBMemInfo memInfo;
		
		memUsage += size;
		memInfo.mem.reset(new uint8_t[size]);
		memInfo.size = size;
		ret = (void *) memInfo.mem.get();
		oob[ret] = memInfo;
		return ret;
	}

	if (size > capacityRemaining)
		newBlock();
	ret = (void *) nextAlloc;
	nextAlloc += size;
	capacityRemaining -= size;
	memUsage += size;
	return ret;
}

void PoolAllocator::deallocate(void *p)
{
	OutOfBandMap::iterator it = oob.find(p);
	
	if (it == oob.end())
		return;
	memUsage -= it->second.size;
	oob.erase(it);
}

uint PoolAllocator::getWindowSize() const
{
	return allocSize;
}

}
