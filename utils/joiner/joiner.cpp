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

#include "joiner.h"
#include <algorithm>

using namespace std;
using namespace joblist;
using namespace utils;

namespace joiner {

Joiner::Joiner(bool ia) : _includeAll(ia), _inPM(false), _pool(new SimplePool)
{
	SimpleAllocator<pair<uint64_t const, uint64_t> > alloc(_pool);
	h.reset(new hash_t(10, hash_t::hasher(), hash_t::key_equal(), alloc));
// 	cout << "Joiner()\n";
}

Joiner::Joiner()
{ }

Joiner::Joiner(const Joiner &j)
{ }

Joiner & Joiner::operator=(const Joiner &j) 
{
	return *this;
}

Joiner::~Joiner() 
{
// 	cout << "~Joiner()\n";
	// get rid of the hash table first
	h.reset();
// 	delete _pool;
// 	_pool = NULL;
}

boost::shared_ptr<vector<ElementType> > Joiner::getSortedMatches()
{
	boost::shared_ptr<vector<ElementType> > ret;
	iterator it;

	ret.reset(new vector<ElementType>());
	for (it = begin(); it != end(); ++it)
		if (it->second & MSB)
			ret->push_back(ElementType(it->second & ~MSB, it->first));
	sort<vector<ElementType>::iterator>(ret->begin(), ret->end());
	return ret;
}

boost::shared_ptr<std::vector<joblist::ElementType> > Joiner::getSmallSide()
{
	boost::shared_ptr<vector<ElementType> > ret;
	iterator it;

	ret.reset(new vector<ElementType>());
	for (it = begin(); it != end(); ++it)
		ret->push_back(ElementType(it->second & ~MSB, it->first));
	return ret;
}

void Joiner::doneInserting()
{
	//sort here if the data structure is a vector
}

}
