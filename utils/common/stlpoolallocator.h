//
// C++ Interface: stlpoolallocator
//
// Description: 
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2009
//
// Copyright: See COPYING file that comes with this distribution
//
//

/* Makes PoolAllocator STL-compliant */

#include <memory>
#include <boost/shared_ptr.hpp>
#include "poolallocator.h"

#undef min
#undef max

#ifndef STLPOOLALLOCATOR_H_
#define STLPOOLALLOCATOR_H_

namespace utils {

/* If using the pool allocator with a boost smart ptr, use an instance of this
as the deleter. */
struct BoostPoolDeallocator
{
	inline void operator()(void *ptr) { };
};

/* This is an STL-compliant wrapper for PoolAllocator + an optimization for containers
 * that aren't entirely node based (ex: vectors and hash tables)
 */
template<class T>
class STLPoolAllocator
{
	public:
		typedef size_t size_type;
		typedef ptrdiff_t difference_type;
		typedef T *pointer;
		typedef const T *const_pointer;
		typedef T& reference;
		typedef const T& const_reference;
		typedef T value_type;
		template<class U> struct rebind { typedef STLPoolAllocator<U> other; };

		STLPoolAllocator() throw();
		STLPoolAllocator(const STLPoolAllocator &) throw();
		STLPoolAllocator(uint capacity) throw();
		template<class U> STLPoolAllocator(const STLPoolAllocator<U> &) throw();
		~STLPoolAllocator();

		STLPoolAllocator<T>& operator=(const STLPoolAllocator<T> &);

		void usePoolAllocator(boost::shared_ptr<PoolAllocator> b);
		boost::shared_ptr<utils::PoolAllocator> getPoolAllocator();

		pointer allocate(size_type, const void *hint = 0);
		void deallocate(pointer p, size_type n);
		size_type max_size() const throw();
		inline uint64_t getMemUsage() const { return pa->getMemUsage(); }

		void construct(pointer p, const T& val);
		void destroy(pointer p);

		static const uint DEFAULT_SIZE = 4096*sizeof(T);

		boost::shared_ptr<utils::PoolAllocator> pa;
};

template<class T>
STLPoolAllocator<T>::STLPoolAllocator() throw()
{
	pa.reset(new PoolAllocator(DEFAULT_SIZE));
}

template<class T>
STLPoolAllocator<T>::STLPoolAllocator(const STLPoolAllocator<T> &s) throw()
{
	pa = s.pa;
}

template<class T>
STLPoolAllocator<T>::STLPoolAllocator(uint capacity) throw()
{
	pa.reset(new PoolAllocator(capacity));
}

template<class T>
template<class U>
STLPoolAllocator<T>::STLPoolAllocator(const STLPoolAllocator<U> &s) throw()
{
	pa = s.pa;
}

template<class T>
STLPoolAllocator<T>::~STLPoolAllocator()
{
}

template<class T>
void STLPoolAllocator<T>::usePoolAllocator(boost::shared_ptr<PoolAllocator> p)
{
	pa = p;
}
template<class T>
boost::shared_ptr<utils::PoolAllocator> STLPoolAllocator<T>::getPoolAllocator()
{
	return pa;
}

template<class T>
typename STLPoolAllocator<T>::pointer
STLPoolAllocator<T>::allocate(typename STLPoolAllocator<T>::size_type s,
  typename std::allocator<void>::const_pointer hint)
{
	return (pointer) pa->allocate(s*sizeof(T));
}

template<class T>
void STLPoolAllocator<T>::deallocate(typename STLPoolAllocator<T>::pointer p,
  typename STLPoolAllocator<T>::size_type n)
{
	pa->deallocate((void *) p);
}

template<class T>
typename STLPoolAllocator<T>::size_type STLPoolAllocator<T>::max_size() const throw()
{
	return std::numeric_limits<uint>::max()/sizeof(T);
}

template<class T>
void STLPoolAllocator<T>::construct(typename STLPoolAllocator<T>::pointer p, const T& val)
{
	new((void *)p) T(val);
}

template<class T>
void STLPoolAllocator<T>::destroy(typename STLPoolAllocator<T>::pointer p)
{
	p->T::~T();
}

template<class T>
STLPoolAllocator<T>& STLPoolAllocator<T>::operator=(const STLPoolAllocator<T> &c)
{
	pa = c.pa;
	return *this;
}

template<typename T>
bool operator==(const STLPoolAllocator<T> &, const STLPoolAllocator<T> &)
{
	return true;
}

template<typename T>
bool operator!=(const STLPoolAllocator<T> &, const STLPoolAllocator<T> &)
{
	return false;
}

}

#endif
