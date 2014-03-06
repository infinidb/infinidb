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
 * $Id: fifo.h 9655 2013-06-25 23:08:13Z xlou $
 *
 *****************************************************************************/


/** @file
 * class XXX interface
 */

#ifndef FIFO_HPP__
#define FIFO_HPP__

#include <vector>
#include <iostream>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include <stdexcept>
#include "elementtype.h"
#include "datalistimpl.h"

namespace joblist {

/** @brief class FIFO
 *
 */

/* This derives from DataListImpl<vector<element_t> > but it manages its own data */
template<typename element_t>
class FIFO : public DataListImpl<std::vector<element_t>, element_t>
{
private:
	typedef DataListImpl<std::vector<element_t>, element_t> base;

public:
	enum ElementMode {
		RID_ONLY,
		RID_VALUE
	};

	FIFO(uint32_t numConsumers, uint32_t maxElements);
	virtual ~FIFO();

	/* DataList<element_t> interface */
	inline void insert(const element_t &e);
	inline void insert(const std::vector<element_t> &v);
	inline bool next(uint64_t it, element_t *e);
	uint64_t getIterator();
	void endOfInput();
	void setMultipleProducers(bool b);

	/* Use this insert() to detect when insertion fills up buffer.      */
	/* When this happens, call waitTillReadyForInserts() before resuming*/
	/* with more inserts.                                               */
	inline void insert(const element_t &e,
		bool& bufferFullBlocked, bool& consumptionStarted);
	inline void waitTillReadyForInserts();
	inline bool isOutputBlocked() const;

	void OID(execplan::CalpontSystemCatalog::OID oid) { base::OID(oid); }
	execplan::CalpontSystemCatalog::OID OID() const { return base::OID(); }

	inline void dropToken() { };
	inline void dropToken(uint32_t) { };

	// Counters that reflect how many many times this FIFO blocked on reads/writes
	uint64_t blockedWriteCount() const;
	uint64_t blockedReadCount()  const;

	// @bug 653 set number of consumers when it is empty.
	void setNumConsumers( uint32_t nc );

	void inOrder(bool order) { fInOrder = order; }
	bool inOrder() const { return fInOrder; }

	// Default behavior tracks number of element inserts.  If application
	// is inserting complex elements (ex: RowWrappers), then application
	// should call totalSize() mutator to record accurate element count.
	// This is not a blocking call.  If totalSize() accessor is called
	// prior to completion, the "current" total size will be returned.
	void totalSize(const uint64_t totSize) {fTotSize = totSize;}
	uint64_t totalSize() { return fTotSize;}

	void maxElements(uint64_t max);
	uint64_t maxElements() { return fMaxElements;}

	// FIFO only uses elementmode to control how the elements are saved
	// to temp disk, should the application choose to page to disk.  If
	// a FIFO is converted to another datalist, this enum should be copied
	// over to the datalist, as a ZDL for example can use the element mode
	// for other reasons.
	void setElementMode(uint32_t mode)     { fElementMode = mode; }
	uint32_t getElementMode() const        { return fElementMode; }

	// Total number of files and filespace used for temp files
	void setTotalFileCounts(uint64_t numFiles, uint64_t numBytes);
	void totalFileCounts(uint64_t& numFiles, uint64_t& numBytes) const;

	// returns true if there might be more data to read,
	// false if there is no more data.  Similar to next(), but
	// does not return data.
	bool more(uint64_t id);
protected:

private:
	boost::condition finishedConsuming, moreData;

	element_t *pBuffer;
	element_t *cBuffer;
	uint64_t ppos;
	uint64_t *cpos;
	uint64_t cDone;
	uint64_t fMaxElements;
	uint64_t cWaiting;
	uint64_t fTotSize;
	bool     fInOrder;
	uint64_t fConsumerFinishedCount;
	volatile bool fConsumptionStarted;
	uint32_t     fElementMode;
	uint64_t fNumFiles;
	uint64_t fNumBytes;

	// Counters that reflect how many many times this FIFO blocked
	// on reads and writes due to the FIFO being empty or full.
	uint64_t blockedInsertWriteCount;
	uint64_t blockedNextReadCount;

	FIFO & operator=(const FIFO &);
	FIFO(const FIFO &);
	FIFO();

	void signalPs();
	bool swapBuffers(bool waitIfBlocked=true);
	bool waitForSwap(uint64_t id);

};

// #define FIFO_DEBUG

// toggles consumer behavior st it only has one critical section.
// Need to bench both ways before changing it.
// #define ONE_CS

template<typename element_t>
FIFO<element_t>::FIFO(uint32_t con, uint32_t max) : DataListImpl<std::vector<element_t>, element_t>(con)
{
	fMaxElements = max;
	pBuffer = 0;
	cBuffer = 0;
	cpos = new uint64_t[con];
	ppos = 0;
	cWaiting = 0;
	fTotSize = 0;
	fInOrder = false;
	fConsumerFinishedCount = 0;
	fConsumptionStarted = false;
	fElementMode        = RID_ONLY;
	fNumFiles = 0;
	fNumBytes = 0;

	for (uint64_t i = 0; i < con; ++i)
		cpos[i] = fMaxElements;

	cDone = con;

	blockedInsertWriteCount = blockedNextReadCount = 0;
}

template<typename element_t>
FIFO<element_t>::FIFO()
{
	throw std::logic_error("don't use FIFO()");
}

template<typename element_t>
FIFO<element_t>::FIFO(const FIFO<element_t> &f)
{
	throw std::logic_error("don't use FIFO(FIFO &)");
}

template<typename element_t>
FIFO<element_t> & FIFO<element_t>::operator=(const FIFO<element_t> &f)
{
	throw std::logic_error("don't use FIFO:: =");
}

template<typename element_t>
FIFO<element_t>::~FIFO()
{
	if (pBuffer)
		delete [] pBuffer;
	if (cBuffer)
		delete [] cBuffer;
	delete [] cpos;
}

// if waitIfBlocked is false, the return value indicates if the swap blocked
template<typename element_t>
bool FIFO<element_t>::swapBuffers(bool waitIfBlocked)
{
	element_t *tmp;

	boost::mutex::scoped_lock scoped(base::mutex);
	if (cDone < base::numConsumers) {
		blockedInsertWriteCount++;
		if (!waitIfBlocked)
			return true;
		while (cDone < base::numConsumers)
			finishedConsuming.wait(scoped);
	}
	tmp = pBuffer;
	pBuffer = cBuffer;
	cBuffer = tmp;
	cDone = 0;
	ppos = 0;
	memset(cpos, 0, sizeof(*cpos) * base::numConsumers);
	if (cWaiting) {
		moreData.notify_all();
		cWaiting = 0;
	}
	return false;
}

template<typename element_t>
inline void FIFO<element_t>::insert(const element_t &e)
{
	if (!pBuffer) {
		pBuffer = new element_t[fMaxElements];
		cBuffer = new element_t[fMaxElements];
	}
	pBuffer[ppos++] = e;
	fTotSize++;

	if (ppos == fMaxElements)
		swapBuffers();
}

// version of insert that will return rather than block if insert would block
template<typename element_t>
inline void FIFO<element_t>::insert(const element_t &e,
	bool& bufferFullBlocked, bool& consumptionStarted)
{
	if (!pBuffer) {
		pBuffer = new element_t[fMaxElements];
		cBuffer = new element_t[fMaxElements];
	}
	pBuffer[ppos++] = e;
	fTotSize++;

	bufferFullBlocked  = false;
	consumptionStarted = fConsumptionStarted;
	if (ppos == fMaxElements)
		bufferFullBlocked = swapBuffers(false);
}

template<typename element_t>
inline void FIFO<element_t>::waitTillReadyForInserts()
{
	if (ppos == fMaxElements)
		swapBuffers();
}

template<typename element_t>
inline bool FIFO<element_t>::isOutputBlocked() const
{
	if (ppos == fMaxElements)
		return true;
	else
		return false;
}

template<typename element_t>
inline void FIFO<element_t>::insert(const std::vector<element_t> &e)
{
	typename std::vector<element_t>::const_iterator it = e.begin();
	typename std::vector<element_t>::const_iterator end = e.end();

	while (it != end) {
		insert(*it);
		++it;
	}
}

template<typename element_t>
bool FIFO<element_t>::waitForSwap(uint64_t id)
{
	boost::mutex::scoped_lock scoped(base::mutex);

#ifdef ONE_CS
	if (cpos[id] == fMaxElements)
		if (++cDone == base::numConsumers)
			finishedConsuming.notify_all()
#endif
 	while (cpos[id] == fMaxElements && !base::noMoreInput) {
 		++cWaiting;
		blockedNextReadCount++;
		moreData.wait(scoped);
	}
	if (cpos[id] == fMaxElements) {
		// Before we free the lock, let's check to see if all our consumers
		// are finished, in which case we can delete our data buffers.
		if (++fConsumerFinishedCount == base::numConsumers) {
			delete [] pBuffer;
			delete [] cBuffer;
			pBuffer = 0;
			cBuffer = 0;
		}
		return false;
	}
	return true;
}

template<typename element_t>
bool FIFO<element_t>::more(uint64_t id)
{
	boost::mutex::scoped_lock scoped(base::mutex);
	return !(cpos[id] == fMaxElements && base::noMoreInput);
}

template<typename element_t>
void FIFO<element_t>::signalPs()
{
	boost::mutex::scoped_lock scoped(base::mutex);
	if (++cDone == base::numConsumers)
		finishedConsuming.notify_all();
}

template<typename element_t>
inline bool FIFO<element_t>::next(uint64_t id, element_t *out)
{
	fConsumptionStarted = true;
 	if (cpos[id] >= fMaxElements)
		if (!waitForSwap(id))
			return false;

	*out = cBuffer[cpos[id]++];

#ifndef ONE_CS
 	if (cpos[id] == fMaxElements)
		signalPs();
#endif
	return true;
}

template<typename element_t>
void FIFO<element_t>::endOfInput()
{
	element_t *tmp;

	boost::mutex::scoped_lock scoped(base::mutex);
	if (ppos != 0) {
		while (cDone < base::numConsumers)
			finishedConsuming.wait(scoped);
		fMaxElements = ppos;
		tmp = pBuffer;
		pBuffer = cBuffer;
		cBuffer = tmp;
		cDone = 0;
 		memset(cpos, 0, sizeof(*cpos) * base::numConsumers);
	}
	base::endOfInput();
 	if (cWaiting)
		moreData.notify_all();
}

template<typename element_t>
uint64_t FIFO<element_t>::getIterator()
{
	uint64_t ret;

	boost::mutex::scoped_lock scoped(base::mutex);
	ret = base::getIterator();
	return ret;
}

template<typename element_t>
uint64_t FIFO<element_t>::blockedWriteCount() const
{
	return blockedInsertWriteCount;
}

template<typename element_t>
uint64_t FIFO<element_t>::blockedReadCount() const
{
	return blockedNextReadCount;
}

template<typename element_t>
void FIFO<element_t>::setMultipleProducers(bool b)
{
	if (b)
		throw std::logic_error("FIFO: setMultipleProducers() doesn't work yet");
}

//@bug 653
template<typename element_t>
void FIFO<element_t>::setNumConsumers( uint32_t nc )
{
	delete [] cpos;
	base::setNumConsumers(nc);
	cpos = new uint64_t[nc];

	for (uint64_t i = 0; i < nc; ++i)
		cpos[i] = fMaxElements;
	cDone = nc;
}

//@bug 864
template<typename element_t>
void FIFO<element_t>::maxElements(uint64_t max)
{
	if (fMaxElements != max)
	{
		fMaxElements = max;

		if (pBuffer)
			delete [] pBuffer;
		if (cBuffer)
			delete [] cBuffer;

		pBuffer = 0;
		cBuffer = 0;

		for (uint64_t i = 0; i < base::numConsumers; ++i)
			cpos[i] = fMaxElements;
	}
}

//
// Sets/Returns the number of temp files and the space taken up by those files
// (in bytes) by this FIFO collection, if the application code chose to cache
// to disk.
//
template<typename element_t>
void FIFO<element_t>::setTotalFileCounts(uint64_t numFiles, uint64_t numBytes)
{
	fNumFiles = numFiles;
	fNumBytes = numBytes;
}
template<typename element_t>
void FIFO<element_t>::totalFileCounts(uint64_t& numFiles, uint64_t& numBytes)
    const
{
	numFiles = fNumFiles;
	numBytes = fNumBytes;
}

}   // namespace

#endif
// vim:ts=4 sw=4:

