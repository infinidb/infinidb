//
// C++ Interface: poolallocator
//
// Description: 
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2009
//
// Copyright: See COPYING file that comes with this distribution
//
//


/* This allocator is an attempt to consolidate small allocations and
   deallocations to boost performance and reduce mem fragmentation. */

#ifndef POOLALLOCATOR_H_
#define POOLALLOCATOR_H_

#include <memory>
#include <vector>
#include <map>
#include <boost/shared_array.hpp>
#include <stdint.h>

namespace utils {

class PoolAllocator
{
	public:
		PoolAllocator(uint windowSize = DEFAULT_WINDOW_SIZE, bool isTmpSpace = false);
		PoolAllocator(const PoolAllocator &);
		virtual ~PoolAllocator();

		PoolAllocator & operator=(const PoolAllocator &);

		void *allocate(uint64_t size);
		void deallocate(void *p);
		void deallocateAll();

		inline uint64_t getMemUsage() const { return memUsage; }
		uint getWindowSize() const;
		static const uint DEFAULT_WINDOW_SIZE = 4096 * 40;  // should be an integral # of pages

	private:
		void newBlock();

		uint allocSize;
		std::vector<boost::shared_array<uint8_t> > mem;
		bool tmpSpace;
		uint capacityRemaining;
		uint64_t memUsage;
		uint8_t *nextAlloc;
		
		struct OOBMemInfo {
			boost::shared_array<uint8_t> mem;
			uint64_t size;
		};
		typedef std::map<void *, OOBMemInfo> OutOfBandMap;
		OutOfBandMap oob;  // for mem chunks bigger than the window size; these can be dealloc'd
};

}

#endif
