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
 * $Id: constantdatalist.h 7409 2011-02-08 14:38:50Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class XXX interface
 */

#include "datalist.h"

#ifndef _CONSTANTDATALIST_H_
#define _CONSTANTDATALIST_H_

namespace joblist {

/** @brief class ConstantDataList
 *
 */
template<typename element_t>
class ConstantDataList : public DataList<element_t>
{
	typedef DataList<element_t> base;

	public:
		ConstantDataList(const element_t &);
		virtual ~ConstantDataList();

		ConstantDataList<element_t>& operator=
			(const ConstantDataList<element_t> &dl);

		void insert(const element_t &e);
		void insert(const std::vector<element_t> &e);
		uint64_t getIterator();
		bool next(uint64_t it, element_t *e);
		void setMultipleProducers(bool b);

	protected:

	private:
		explicit ConstantDataList();
		ConstantDataList(const ConstantDataList<element_t> &);

		element_t value;
};

template<typename element_t>
ConstantDataList<element_t>::ConstantDataList()
{ }

template<typename element_t>
ConstantDataList<element_t>::ConstantDataList(const element_t &e)
{
	value = e;
}

template<typename element_t>
ConstantDataList<element_t>::~ConstantDataList()
{ }

template<typename element_t>
ConstantDataList<element_t> & ConstantDataList<element_t>::operator=
	(const ConstantDataList<element_t> &dl)
{
	value = dl.value;
}

template<typename element_t>
void ConstantDataList<element_t>::insert(const element_t &e)
{
	value = e;
}

template<typename element_t>
void ConstantDataList<element_t>::insert(const std::vector<element_t> &e)
{
	throw std::logic_error("ConstantDataList::insert(vector) isn't implemented yet");
}

template<typename element_t>
uint64_t ConstantDataList<element_t>::getIterator()
{
	return 0;
}

template<typename element_t>
bool ConstantDataList<element_t>::next(uint64_t id, element_t *e)
{
	if (base::noMoreInput)
		return false;

	*e = value;
	return true;
}	

template<typename element_t>
void ConstantDataList<element_t>::setMultipleProducers(bool b)
{ }


}  // namespace

#endif
