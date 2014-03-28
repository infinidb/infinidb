/* Copyright (C) 2013 Calpont Corp.

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

#ifndef JOINER_H_
#define JOINER_H_

#include <iostream>
#include <vector>
#include <boost/shared_ptr.hpp>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif

#include "simpleallocator.h"

#ifndef _HASHFIX_
#define _HASHFIX_
#ifndef __LP64__
#if __GNUC__ == 4 && __GNUC_MINOR__ < 2
// This is needed for /usr/include/c++/4.1.1/tr1/functional on 32-bit compiles
// tr1_hashtable_define_trivial_hash(long long unsigned int);
namespace std
{
namespace tr1
{
  template<>
    struct hash<long long unsigned int>
    : public std::unary_function<long long unsigned int, std::size_t>
    {
      std::size_t
      operator()(long long unsigned int val) const
      { return static_cast<std::size_t>(val); }
    };
}
}
#endif
#endif
#endif

#define NO_DATALISTS
#include <elementtype.h>
#undef NO_DATALISTS


namespace joiner
{

/* There has to be a better name for this.  Not used ATM. */
struct MatchedET {
	MatchedET() { }
	MatchedET(const joblist::ElementType &et) : e(et) { }
	joblist::ElementType e;
//	bool matched;    // Might need this, might not

	inline bool operator<(const MatchedET &c) const { return e.second < c.e.second; }
};


class Joiner {
	public:
//		typedef std::tr1::unordered_multimap<uint64_t, uint64_t> hash_t;
		typedef std::tr1::unordered_multimap<uint64_t, uint64_t,
				std::tr1::hash<uint64_t>, std::equal_to<uint64_t>,
				utils::SimpleAllocator<std::pair<uint64_t const, uint64_t> > > hash_t;

		typedef hash_t::iterator iterator;

		Joiner(bool bIncludeAll);
		virtual ~Joiner();

		// elements are stored as <value, rid>
		inline iterator begin() { return h->begin(); }
		inline iterator end() { return h->end(); }
		inline size_t size() { return h->size(); }
		inline void insert(const joblist::ElementType &e)
		{
			h->insert(std::pair<uint64_t, uint64_t>(e.second, e.first));
		}
		void doneInserting();
		boost::shared_ptr<std::vector<joblist::ElementType> > getSmallSide();
		boost::shared_ptr<std::vector<joblist::ElementType> > getSortedMatches();

		/* Used by the UM */
		inline bool match(const joblist::ElementType &large)
		{
			std::pair<iterator, iterator> range;
			iterator it = h->find(large.second);

			if (it == h->end())
				return _includeAll;
			else
				if (it->second & MSB)
					return true;
				else {
					range = h->equal_range(large.second);
					for( ; range.first != range.second; ++range.first)
						range.first->second |= MSB;
					return true;
				}
		}

		inline void mark(const joblist::ElementType &large)
		{
			std::pair<iterator, iterator> range;
	
			range = h->equal_range(large.second);
			for( ; range.first != range.second; ++range.first)
				range.first->second |= MSB;
		}

		/* Used by the PM */
		inline bool getNewMatches(const uint64_t value,
			std::vector<joblist::ElementType> *newMatches)
		{
			std::pair<iterator, iterator> range;
			iterator it = h->find(value);

			if (it == h->end())
				return _includeAll;
			else
				if (it->second & MSB)
					return true;
				else {
					newMatches->push_back(
						joblist::ElementType(it->second | MSB, value));
					range = h->equal_range(value);
					for( ; range.first != range.second; ++range.first)
						range.first->second |= MSB;
					return true;
				}
		}

		inline bool inPM() { return _inPM; }
		void inPM(bool b) { _inPM = b; }
		inline bool inUM() { return !_inPM; }
		void inUM(bool b) { _inPM = !b; }
		bool includeAll() { return _includeAll; }

		uint64_t getMemUsage() { return (_pool ? _pool->getMemUsage() : 0); }

		static const uint64_t MSB = 0x8000000000000000ULL;
	protected:
		Joiner();
		Joiner(const Joiner &);
		Joiner & operator=(const Joiner &);
	private:
		boost::shared_ptr<hash_t> h;
		bool _includeAll;
		bool _inPM;			// true -> should execute on the PM, false -> UM
		boost::shared_ptr<utils::SimplePool> _pool;	// pool for the table and nodes
};

}

#endif
