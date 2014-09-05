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

/******************************************************************************
 * $Id: simpleallocator.h 3495 2013-01-21 14:09:51Z rdempsey $
 *
 ******************************************************************************/

/** @file
 * class SimpleAllocator interface
 */

#ifndef UTILS_SIMPLEALLOCATOR_H
#define UTILS_SIMPLEALLOCATOR_H

#include <unistd.h>
#include <list>
#include <stdint.h>
#include <limits>

#undef min
#undef max

namespace utils
{

// A specialized allocator for std::tr1::unordered_multimap<uint64_t, uint64_t> based joiner
// or std::tr1::unordered_map<uint8_t*, uint8_t*> based aggregation.
// User shall initialize a pool and pass it to allocator, release the pool when map is done.
template<typename T> class SimpleAllocator;

// this pool is best for node size of 3*sizeof(int64).
// map nodes are taken from fixed size blocks, and control hash tables are from ::new.
// assumption is the nodes are not reallocated, but the controls will reallocated when rehash.
// efficient only if the map does not remove nodes, otherwise will take more memory.

#define OPT_NODE_UNITS 10
class SimplePool
{
  public:
	SimplePool() : fNext(NULL), fEnd(NULL), fTableMemSize(0) {}
	~SimplePool() { reset(); }

	inline void* allocate(size_t n, const void* = 0);
	inline void deallocate(void* p, size_t n);
	inline size_t max_size() const throw();
	inline uint64_t getMemUsage() const;

private:
	static const size_t fUnitPerChunk = OPT_NODE_UNITS*10240;

	inline void reset();
	inline void allocateNewChunk();

	// MemUnit stores a pointer to next unit before allocated, and T after allocated.
	union MemUnit
	{
		MemUnit* fNext;
		uint64_t fData;
	} *fNext, *fEnd;    // fNext: next available unit, fEnd: one off the last unit

	std::list<MemUnit*> fBlockList;
	uint64_t fTableMemSize;

	static const size_t fUnitSize = sizeof(MemUnit);
	static const size_t fMaxNodeSize = fUnitSize * OPT_NODE_UNITS;
	static const size_t fChunkSize = fUnitSize * fUnitPerChunk;
};


template<typename T>
class SimpleAllocator
{
  public:
	typedef size_t size_type;
	typedef ptrdiff_t difference_type;
	typedef T *pointer;
	typedef const T *const_pointer;
	typedef T& reference;
	typedef const T& const_reference;
	typedef T value_type;
	template<typename U> struct rebind { typedef SimpleAllocator<U> other; };

	SimpleAllocator() throw() {}
	SimpleAllocator(boost::shared_ptr<SimplePool> pool) throw() { fPool = pool; }
	SimpleAllocator(const SimpleAllocator& alloc) { fPool = alloc.fPool; }
	template<class U> SimpleAllocator(const SimpleAllocator<U>& alloc) { fPool = alloc.fPool; }

	~SimpleAllocator() throw() { }

	pointer address(reference x) const { return &x; }
	const_pointer address(const_reference x) const { return &x; }

	pointer allocate(size_type n, const void* = 0)
	{ return static_cast<pointer>(fPool->allocate(n*sizeof(T))); }
	void deallocate(pointer p, size_type n)
	{ fPool->deallocate(p, n*sizeof(T)); }

#ifdef _MSC_VER
	//The MSVC STL library really needs this to return a big number...
	size_type max_size() const throw() { return std::numeric_limits<size_type>::max(); }
#else
	size_type max_size() const throw() { return fPool->max_size()/sizeof(T); }
#endif
	void construct(pointer ptr, const T& val) { new ((void *)ptr) T(val); }
	void destroy(pointer ptr) { ptr->T::~T(); }

	SimplePool* getPool() { return fPool; }
	void setPool(SimplePool* pool) { fPool = pool; }

	boost::shared_ptr<SimplePool> fPool;
};


// inlines
inline void * SimplePool::allocate(size_t n, const void *dur)
{
	// make sure the block allocated is on unit boundary
	size_t unitCount = n / fUnitSize;
	if ((n % fUnitSize) != 0)
		unitCount += 1;

	// if for control table, let new allocator handle it.
	if (unitCount > OPT_NODE_UNITS) {
		fTableMemSize += n;
		return new uint8_t[n];
	}
	
	// allocate node
	MemUnit *curr = fNext;
	do {
		if (curr == NULL) {
			allocateNewChunk();
			curr = fNext;
		}
		fNext = curr + unitCount;
		if (fNext > fEnd)
			curr = NULL;
	} while (!curr);

	return curr;
}

inline void SimplePool::deallocate(void* p, size_t n)
{
	// only delete the old control table, which is allocated by new allocator.
	if (n > fMaxNodeSize)
	{
		fTableMemSize -= n;
		delete [] (static_cast<uint8_t*>(p));
	}
}

inline size_t SimplePool::max_size() const throw() 
{
	return fUnitSize * fUnitPerChunk;
}

inline uint64_t SimplePool::getMemUsage() const
{
	return fTableMemSize + fBlockList.size() * fChunkSize +
		// add list overhead, element type is a pointer, and
		// lists store a next pointer.
		fBlockList.size() * 2 * sizeof(void *);
}

inline void SimplePool::reset()
{
	for (std::list<MemUnit*>::iterator i = fBlockList.begin(); i != fBlockList.end(); i++)
		delete [] (*i);
	fNext = NULL;
	fEnd = NULL;
}

inline void SimplePool::allocateNewChunk()
{
	MemUnit* chunk = new MemUnit[fUnitPerChunk];
	fBlockList.push_back(chunk);
	fNext = chunk;
	fEnd = chunk + fUnitPerChunk;
}

template <typename T1, typename T2>
inline bool operator==(const SimpleAllocator<T1>&, const SimpleAllocator<T2>&) {return true;}

template <typename T1, typename T2>
inline bool operator!=(const SimpleAllocator<T1>&, const SimpleAllocator<T2>&) {return false;}
}

#endif  // UTILS_SIMPLEALLOCATOR_H

