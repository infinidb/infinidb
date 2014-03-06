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
 * $Id: bandeddl.h 9655 2013-06-25 23:08:13Z xlou $
 *
 *****************************************************************************/

/** @file
 * class XXX interface
 */

#include <set>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#include "largedatalist.h"
//#include "bucketdl.h"

#include <time.h>

#ifndef _BANDEDDL_HPP_
#define _BANDEDDL_HPP_

namespace joblist {

/** @brief class BandedDL
 *
 */
template<typename element_t>
class BandedDL : public LargeDataList<std::vector<element_t>, element_t>
{
	typedef LargeDataList<std::vector<element_t>, element_t> base;

	public:
		BandedDL(uint32_t numConsumers, const ResourceManager& rm);
//		BandedDL(BucketDL<element_t> &, uint32_t numConsumers, const ResourceManager& rm);
		virtual ~BandedDL();

		int64_t saveBand();
		void loadBand(uint64_t);
		int64_t bandCount();

		/// loads the first band, next() will return the first element
		void restart();

		void insert(const element_t &);
		void insert(const std::vector<element_t> &);
		uint64_t getIterator();
		bool next(uint64_t it, element_t *e);
		void endOfInput();
		using DataListImpl<std::vector<element_t>, element_t>::shrink;
		uint64_t totalSize();
		bool next(uint64_t it, element_t *e, bool *endOfBand);

	protected:

	private:
		explicit BandedDL() { };
		explicit BandedDL(const BandedDL &) { };
		BandedDL & operator=(const BandedDL &) { };

		// vars to support the WSDL-like next() fcn
		boost::condition nextSetLoaded;
		uint64_t waitingConsumers;
};

template<typename element_t>
  BandedDL<element_t>::BandedDL(uint32_t nc, const ResourceManager& rm) : base(nc,  sizeof(uint64_t), sizeof(uint64_t), rm)
{
	//pthread_cond_init(&nextSetLoaded, NULL);
	waitingConsumers = 0;
}

#if 0
template<typename element_t>
  BandedDL<element_t>::BandedDL(BucketDL<element_t> &b, uint32_t nc, const ResourceManager& rm) : base(nc, sizeof(uint64_t), sizeof(uint64_t), rm)
{
	uint64_t i, it;
	element_t e;
	bool more;

	//pthread_cond_init(&nextSetLoaded, NULL);
	waitingConsumers = 0;

	for (i = 0; i < b.bucketCount(); i++) {
		it = b.getIterator(i);
		more = b.next(i, it, &e);
		while (more) {
 			insert(e);
			more = b.next(i, it, &e);
		}
		saveBand();
	}
	endOfInput();
}
#endif

template<typename element_t>
BandedDL<element_t>::~BandedDL()
{
	//pthread_cond_destroy(&nextSetLoaded);
}

template<typename element_t>
int64_t BandedDL<element_t>::saveBand()
{
	int64_t ret;

	if (base::multipleProducers)
	 	base::lock();
	sort(base::c->begin(), base::c->end());
	if (typeid(element_t) == typeid(ElementType) ||
		typeid(element_t) == typeid(DoubleElementType))
		ret = base::save_contiguous();
	else
		ret = base::save();
	base::registerNewSet();
	if (base::multipleProducers)
		base::unlock();

	return ret;
}

template<typename element_t>
void BandedDL<element_t>::loadBand(uint64_t band)
{

	base::lock();
	if (typeid(element_t) == typeid(ElementType) ||
		typeid(element_t) == typeid(DoubleElementType))
		base::load_contiguous(band);
	else
		base::load(band);
	if (waitingConsumers > 0)
		nextSetLoaded.notify_all(); //pthread_cond_broadcast(&nextSetLoaded);
	base::unlock();
}

template<typename element_t>
int64_t BandedDL<element_t>::bandCount()
{
	int64_t ret;

	base::lock();
	ret = base::setCount();
	base::unlock();
	return ret;
}

template<typename element_t>
uint64_t BandedDL<element_t>::getIterator()
{
	uint64_t ret;

	base::lock();
	ret = base::getIterator();
	base::unlock();
	return ret;
}

template<typename element_t>
void BandedDL<element_t>::endOfInput()
{
	base::lock();
	sort(base::c->begin(), base::c->end());
	base::endOfInput();
  	if (base::setCount > 1) {
		if (typeid(element_t) == typeid(ElementType) ||
			typeid(element_t) == typeid(DoubleElementType)) {
			base::save_contiguous();
			base::load_contiguous(0);
		}
		else {
			base::save();
			base::load(0);
		}
  	}
	else
		base::resetIterators();
	base::unlock();
}

template<typename element_t>
bool BandedDL<element_t>::next(uint64_t it, element_t *e)
{

/* Note: this is the code for WSDL::next().  The more I think about it,
the more I think they're the same thing.  Not entirely sure yet though. */

	bool ret, locked = false;
	uint64_t nextSet;

 	if (base::numConsumers > 1 || base::phase == 0) {
 		locked = true;
		base::lock();
 	}

	ret = base::next(it, e);

	/* XXXPAT: insignificant race condition here.  Technically, there's no
	guarantee the caller will be wakened when the next set is loaded.  It could
	get skipped.  It won't happen realistically, but it exists... */

	// signifies the caller is at the end of the loaded set,
	// but there are more sets
	if (ret == false && (base::loadedSet < base::setCount - 1)) {

 		nextSet = base::loadedSet + 1;
		waitingConsumers++;
		if (waitingConsumers < base::numConsumers)
			while (nextSet != base::loadedSet) {
// 				std::cout << "waiting on nextSetLoaded" << std::endl;
				nextSetLoaded.wait(this->mutex); //pthread_cond_wait(&nextSetLoaded, &(this->mutex));
			}
		else {
// 			std::cout << "loading set " << nextSet << std::endl;
			if (typeid(element_t) == typeid(ElementType) ||
				typeid(element_t) == typeid(DoubleElementType))
				base::load_contiguous(nextSet);
			else
				base::load(nextSet);
			nextSetLoaded.notify_all(); //pthread_cond_broadcast(&nextSetLoaded);
		}
		waitingConsumers--;
		ret = base::next(it, e);
	}

	if (ret == false && ++base::consumersFinished == base::numConsumers)
		base::shrink();
	if (locked)
		base::unlock();

	return ret;

}

template<typename element_t>
void BandedDL<element_t>::insert(const element_t &e)
{
	if (base::multipleProducers)
		base::lock();
	base::insert(e);
	if (base::multipleProducers)
		base::unlock();
}

template<typename element_t>
void BandedDL<element_t>::insert(const std::vector<element_t> &e)
{
	throw std::logic_error("BandedDL::insert(vector) isn't implemented yet");
}

/*
template<typename element_t>
bool BandedDL<element_t>::get(const element_t &key, element_t *out)
{
	typename std::set<element_t>::iterator it;
	bool ret, locked = false;

 	if (base::numConsumers > 1 || base::phase == 0) {
 		locked = true;
		base::lock();
 	}

	it = base::c->find(key);
	if (it != base::c->end()) {
		*out = *it;
		ret = true;
	}
	else
		ret = false;

	if (locked)
		base::unlock();

	return ret;
}
*/

template<typename element_t>
void BandedDL<element_t>::restart()
{
	base::lock();
// 	base::waitForConsumePhase();

	// hack!  has it been shrunk already?
	if (base::c == NULL)
		base::c = new std::vector<element_t>();

	if (base::setCount > 1) {
		if (typeid(element_t) == typeid(ElementType) ||
			typeid(element_t) == typeid(DoubleElementType))
			base::load_contiguous(0);
		else
			base::load(0);
	}
	else
		base::resetIterators();
	base::unlock();
}

template<typename element_t>
bool BandedDL<element_t>::next(uint64_t it, element_t *e, bool *endOfBand)
{
	bool ret, locked = false;

 	if (base::numConsumers > 1 || base::phase == 0) {
 		locked = true;
		base::lock();
 	}

	base::waitForConsumePhase();
	ret = base::next(it, e);
	if (ret) {
		if (locked)
			base::unlock();
		*endOfBand = false;
		return ret;
	}
	else {
		*endOfBand = true;
		ret = base::loadedSet < (base::setCount() - 1);
		if (locked)
			base::unlock();
		return ret;
	}
}

template<typename element_t>
uint64_t BandedDL<element_t>::totalSize()
{
//std::cout << "BandedDL: c.size() = " << base::c.size() << std::endl; return base::c.size();
	uint64_t ret;

	base::lock();
	ret = base::totalSize();
	base::unlock();

	return ret;
}

}  // namespace

#endif

