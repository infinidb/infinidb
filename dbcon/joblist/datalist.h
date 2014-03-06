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
 * $Id: datalist.h 9655 2013-06-25 23:08:13Z xlou $
 *
 *****************************************************************************/

/** @file
 * DataList interface definition
 */

#include <boost/thread.hpp>

#include "calpontsystemcatalog.h"

#ifndef _DATALIST_HPP_
#define _DATALIST_HPP_

namespace joblist
{

/*
	Need to implement the following members in element_t:
	copy constructor
	= operator
	< operator if using BandedDL
*/

/** @brief class DataList<element_t>
 *
 *  DataList is an abstract interface that defines container-like methods.
 *  A producer adds elements using insert(e).  When it's done, it calls
 *  endOfInput().
 *
 *  To retrieve elements, a
 *  consumer calls getIterator() once, then next(iterator, &e) for every
 *  element in the DataList.  next returns true when there was data to
 *  consume, false when there was no data (e will not be changed in this case).
 *
 *  The exact semantics of these calls depends on the derived class's
 *  implementation.
 *
 *  - ConstantDataList is the simplest DataList.  All it does is return
 *  the same value over and over again.  It simplifies things upstream
 *  significantly for that behavior to be packaged as a DataList.
 *
 *  - FIFO is made for simultaneous access by readers
 *  and writers and have a maximum size, so it's basically an implementation
 *  of the producer-consumer problem right out of the algorithm textbooks,
 *  except that there can be multiple consumers and all will see the entire
 *  dataset.  One consumer doesn't change the data the others receive.
 *  insert() will block
 *  when the FIFO is full, and next() will block when it's empty.
 *
 *  - WSDL is meant for cases when the consuming step needs the entire
 *  dataset to be available before it starts, so they are designed to store
 *  very large amounts of data.  Simultaneous access doesn't make sense here,
 *  so next() blocks until the producer calls endOfInput().
 *
 *  - BucketDL is used to cluster data according to some function provided
 *  by the caller.  This DataList is used by the Hash join jobstep to reduce
 *  the too-large-for-memory hash problem into a set of subhashes.  It is
 *  also used as the intermediate step between a large random dataset
 *  and a fully sorted "banded" datalist.
 *
 *  - BandedDL is the "banded" datalists mentioned above.  The only real
 *  difference between this and the WSDL class is that the underlying container
 *  is an STL set, so it has some notion of order.  It also exposes an interface
 *  for loading and saving sets because band boundaries are only known at a higher
 *  level.  To use this class, be sure you have the \< operator defined for
 *  whatever element type you're storing.
 */
template<typename element_t>
class DataList
{
	public:
		typedef element_t value_type;

		DataList();
		DataList(const DataList<element_t> &dl);
		virtual ~DataList();

		DataList<element_t>& operator=(const DataList<element_t> &dl);

		virtual void insert(const element_t &e) = 0;
		virtual void insert(const std::vector<element_t> &v) = 0;
		virtual uint64_t getIterator() = 0;
		virtual bool next(uint64_t it, element_t *e) = 0;
		virtual void endOfInput();
		virtual void setMultipleProducers(bool b) = 0;
		virtual uint64_t totalSize() { return 0; }
		virtual bool totalDiskIoTime(uint64_t& w, uint64_t& r) { return false; }

		virtual void OID(execplan::CalpontSystemCatalog::OID OID) { fOID = OID; }
		virtual execplan::CalpontSystemCatalog::OID OID() const { return fOID; }

		//...Following methods indicate whether this datalist employs temp disk;
		//...and if so, the num bytes for element_t.first and element_t.second.
		//...Currently support sizes: (8,8), (8,4), (4,8), and (4,4).
		virtual  bool useDisk() const { return false; }
		virtual  void setDiskElemSize(uint32_t size1st,uint32_t size2nd);
		uint32_t getDiskElemSize1st() const { return fElemDiskFirstSize; }
		uint32_t getDiskElemSize2nd() const { return fElemDiskSecondSize;}

	protected:
		void lock();
		void unlock();

		boost::mutex& getMutex() { return mutex; }   // why in the world is this necessary in FIFO?

		boost::mutex mutex;
		bool noMoreInput;
		uint64_t consumersFinished;
		uint32_t fElemDiskFirstSize; //byte size of element.first saved to disk
		uint32_t fElemDiskSecondSize;//byte size of element.second saved to disk

	private:
		execplan::CalpontSystemCatalog::OID fOID;
};

template<typename element_t>
DataList<element_t>::DataList() :
	noMoreInput(false), consumersFinished(0),
	fElemDiskFirstSize(sizeof(uint64_t)), fElemDiskSecondSize(sizeof(uint64_t)),
	fOID(0)
{
	//pthread_mutex_init(&mutex, NULL);
};

template<typename element_t>
DataList<element_t>::DataList(const DataList<element_t> &dl)
{
	noMoreInput = dl.noMoreInput;
	//pthread_mutex_init(&mutex, NULL);
	fOID = dl.fOID;
	consumersFinished   = dl.consumersFinished;
	fElemDiskFirstSize  = dl.fElemDiskFirstSize;
	fElemDiskSecondSize = dl.fElemDiskSecondSize;
};

template<typename element_t>
DataList<element_t>::~DataList()
{
	//pthread_mutex_destroy(&mutex);
};

template<typename element_t>
DataList<element_t> & DataList<element_t>::operator=
	(const DataList<element_t> &dl)
{
	noMoreInput = dl.noMoreInput;
	fOID = dl.fOID;
	consumersFinished   = dl.consumersFinished;
	fElemDiskFirstSize  = dl.fElemDiskFirstSize;
	fElemDiskSecondSize = dl.fElemDiskSecondSize;
};

template<typename element_t>
void DataList<element_t>::endOfInput()
{
	noMoreInput = true;
};

template<typename element_t>
void DataList<element_t>::lock()
{
	mutex.lock(); //pthread_mutex_lock(&mutex);
};

template<typename element_t>
void DataList<element_t>::unlock()
{
	mutex.unlock(); //pthread_mutex_unlock(&mutex);
};

template<typename element_t>
void DataList<element_t>::setDiskElemSize(uint32_t size1st,uint32_t size2nd)
{
	fElemDiskFirstSize  = size1st;
	fElemDiskSecondSize = size2nd;
}

}  // namespace

#endif
