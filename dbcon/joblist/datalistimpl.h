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
 * $Id: datalistimpl.h 9655 2013-06-25 23:08:13Z xlou $
 *
 *****************************************************************************/

/** @file
 * class XXX interface
 */

#include <vector>
#include <set>
#include <typeinfo>
#include <stdexcept>
#include <cassert>
#include <sstream>

#include "datalist.h"

#ifndef _DATALISTIMPL_HPP_
#define _DATALISTIMPL_HPP_

namespace joblist {

/** @brief class DataListImpl
 *
 */
template<typename container_t, typename element_t>
class DataListImpl : public DataList<element_t>
{

	public:
		DataListImpl(uint32_t numConsumers);
		DataListImpl(const DataListImpl &dl);
		virtual ~DataListImpl();

		DataListImpl& operator=(const DataListImpl &dl);

		// derived classes need to lock around these fcns
		virtual void insert(const element_t &e);
		virtual void insert(const std::vector<element_t> &v);
		virtual uint64_t getIterator();
		virtual bool next(uint64_t it, element_t *e);

		virtual void setNumConsumers(uint32_t);
		virtual uint32_t getNumConsumers() const;
		void resetNumConsumers(uint32_t numConsumers);

	protected:
		bool endOfData(uint64_t id) const;
		void eraseUpTo(uint64_t id);
		void reset();
		virtual void shrink();
		typename container_t::iterator iInsert(const element_t &e);

		container_t *c;
		typename container_t::iterator *cIterators;
		uint64_t numConsumers;
		uint64_t itIndex;

	private:
		explicit DataListImpl();
};

template<typename container_t, typename element_t>
DataListImpl<container_t, element_t>::DataListImpl() : DataList<element_t>(), c(0), cIterators(0), numConsumers(0),
	itIndex(0)
{ }

template<typename container_t, typename element_t>
void DataListImpl<container_t, element_t>::setNumConsumers(uint32_t nc)
{
	resetNumConsumers(nc);
}

template<typename container_t, typename element_t>
uint32_t DataListImpl<container_t, element_t>::getNumConsumers() const
{
	return numConsumers;
}

template<typename container_t, typename element_t>
void DataListImpl<container_t, element_t>::resetNumConsumers(uint32_t nc)
{
	if (itIndex != 0)
		throw std::logic_error("DataListImpl::resetNumConsumers(): attempt to change numConsumers "
			"after iterators have been issued");

	uint32_t i;

	numConsumers = nc;
	delete [] cIterators;
	cIterators = new typename container_t::iterator[numConsumers];
	for (i = 0; i < numConsumers; i++)
		cIterators[i] = c->end();
}

template<typename container_t, typename element_t>
DataListImpl<container_t, element_t>::DataListImpl(uint32_t nc) : DataList<element_t>()
{
	uint32_t i;

	numConsumers = nc;
	itIndex = 0;
	c = new container_t();
	cIterators = new typename container_t::iterator[numConsumers];
	for (i = 0; i < numConsumers; i++)
		cIterators[i] = c->end();
}

template<typename container_t, typename element_t>
DataListImpl<container_t, element_t>::DataListImpl
	(const DataListImpl<container_t, element_t> &dl)
	: DataList<element_t>(dl)
{
	int i;

	c = dl.c;
	numConsumers = dl.numConsumers;
	itIndex = dl.itIndex;

	//delete [] cIterators;
	cIterators = new typename container_t::iterator[numConsumers];
	for (i = 0; i < numConsumers; i++)
		cIterators[i] = dl.cIterators[i];
};

template<typename container_t, typename element_t>
DataListImpl<container_t, element_t>::~DataListImpl()
{
	delete c;
	delete [] cIterators;
};

// lock at a higher level
template<typename container_t, typename element_t>
DataListImpl<container_t, element_t> & DataListImpl<container_t, element_t>::operator=
	(const DataListImpl<container_t, element_t> &dl)
{
	uint64_t i;

	static_cast<DataList<element_t> >(*this) =
		static_cast<DataList<element_t> >(dl);
	delete c;
	c = dl.c;
	numConsumers = dl.numConsumers;
	itIndex = dl.itIndex;

	delete [] cIterators;
	cIterators = new typename container_t::iterator[numConsumers];
	for (i = 0; i < numConsumers; i++)
		cIterators[i] = dl.cIterators[i];

	return *this;
};

template<typename container_t, typename element_t>
uint64_t DataListImpl<container_t, element_t>::getIterator()
{
	if (itIndex >= numConsumers)
	{
		std::ostringstream oss;
		oss << "DataListImpl::getIterator(): caller attempted to grab too many iterators: " <<
			"have " << numConsumers << " asked for " << (itIndex + 1);
		throw std::logic_error(oss.str().c_str());
	}
	cIterators[itIndex] = c->begin();
	return itIndex++;
}

template<typename container_t, typename element_t>
inline void DataListImpl<container_t, element_t>::insert(const element_t &e)
{
	c->insert(c->end(), e);
}

template<typename container_t, typename element_t>
inline void DataListImpl<container_t, element_t>::insert(const std::vector<element_t> &v)
{
	if (typeid(container_t) == typeid(std::vector<element_t>)) {
		std::vector<element_t> *vc = (std::vector<element_t> *) c;
		vc->insert(vc->end(), v.begin(), v.end());
	}
	else
		throw std::logic_error("insert(vector) isn't supported for non-vector-based DLs yet");
}

template<typename container_t, typename element_t>
inline typename container_t::iterator DataListImpl<container_t, element_t>::iInsert(const element_t &e)
{
	return c->insert(c->end(), e);
}

template<typename container_t, typename element_t>
inline bool DataListImpl<container_t, element_t>::next(uint64_t id, element_t *e)
{

// 	idbassert(id < numConsumers);
	if (c == 0 || cIterators[id] == c->end())
		return false;
	*e = *(cIterators[id]);
	cIterators[id]++;
	return true;
}

template<typename container_t, typename element_t>
bool DataListImpl<container_t, element_t>::endOfData(uint64_t id) const
{
	idbassert(id < numConsumers);
	return (cIterators[id] == c->end());
}

template<typename container_t, typename element_t>
void DataListImpl<container_t, element_t>::eraseUpTo(uint64_t id)
{
	idbassert(id < numConsumers);
#ifdef DEBUG
	uint64_t i;
	typename container_t::iterator it;

	for (it = c->begin(), i = 0; it != cIterators[id]; it++, i++) ;
	std::cout << "DLI: erasing " << i << " elements" << std::endl;
#endif

 	c->erase(c->begin(), cIterators[id]);
};

template<typename container_t, typename element_t>
void DataListImpl<container_t, element_t>::reset()
{
	uint64_t i;

	delete c;
	c = new container_t();
	for (i = 0; i < numConsumers; i++)
		cIterators[i] = c->begin();
}

template<typename container_t, typename element_t>
void DataListImpl<container_t, element_t>::shrink()
{

	delete c;
	c = 0;
}

}   //namespace

#endif
