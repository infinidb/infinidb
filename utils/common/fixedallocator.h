//
// C++ Interface: fixedallocator
//
// Description: 
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2009
//
// Copyright: See COPYING file that comes with this distribution
//
//


/* This allocator is for frequent small allocations that all get deallocated at once.
   It allocates large blocks of memory from the system and distributes 'allocsize'
   units to the caller.  When the large allocation is used up, it will allocate 
   more unless the 'tmpspace' flag is set.  If it is, it will reuse the memory it
   already allocated.  This is useful for short-lived vars that are guaranteed to be
   out of scope by the time the allocator wraps around.

   TODO: make it STL and boost compliant...
*/

#ifndef FIXEDALLOCATOR_H_
#define FIXEDALLOCATOR_H_

#include <stdint.h>
#include <boost/shared_array.hpp>
#include <vector>

#if defined(_MSC_VER) && defined(xxxFIXEDALLOCATOR_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace utils {

class FixedAllocator
{
	public:
		EXPORT FixedAllocator();
		EXPORT FixedAllocator(unsigned long allocsize, bool isTmpSpace = false,
		  unsigned long numelements = DEFAULT_NUM_ELEMENTS);
		EXPORT FixedAllocator(const FixedAllocator &);
		EXPORT virtual ~FixedAllocator();
		EXPORT FixedAllocator & operator=(const FixedAllocator &);

		EXPORT void * allocate();
		void deallocate() { };   // does nothing
		EXPORT void deallocateAll();		// drops all memory in use
		EXPORT inline uint64_t getMemUsage() const {
			return (tmpSpace ? elementSize * elementCount : currentlyStored * elementSize);
		}

		EXPORT static const unsigned long DEFAULT_NUM_ELEMENTS=16384;  // should be a multiple of pagesize

	private:
		void newBlock();

		std::vector<boost::shared_array<uint8_t> > mem;
		unsigned long capacityRemaining;
		unsigned long elementCount;
		unsigned long elementSize;
		unsigned long currentlyStored;
		bool tmpSpace;
		uint8_t* nextAlloc;
};

}

#undef EXPORT

#endif
