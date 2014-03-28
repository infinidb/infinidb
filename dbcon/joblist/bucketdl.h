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

/******************************************************************************
 * $Id: bucketdl.h 9655 2013-06-25 23:08:13Z xlou $
 *
 *****************************************************************************/

/** @file
 * class XXX interface
 */


#include "wsdl.h"
#include "hasher.h"
#include "bucketreuse.h"
#include "tuplewsdl.h"

#include <boost/function.hpp>
#include <boost/function_equal.hpp>
#include <boost/shared_ptr.hpp>
#include <sstream>
#ifndef _BUCKETDL_HPP_
#define _BUCKETDL_HPP_
namespace joblist {

/** @brief class BucketDL
 *
 */
template<typename element_t>
class BucketDL : public DataList<element_t>
{
    typedef DataList<element_t> base;
    typedef boost::shared_ptr< TupleWSDL > TWSSP;
    typedef std::vector<TWSSP> TWSVec;

    enum ElementMode {
        RID_MODE,
        RID_VALUE
    };

    public:
        /** Main constuctor.
         * @param numBuckets The number of buckets to create
         * @param numConsumers The number of consumers that will eventually read this DL.
         * @param maxElementsPerBucket The maximum # of elements each bucket should keep in memory.
         * @param hash The function object that calculates which bucket an elements goes into on insertion.
         */

        BucketDL(uint numBuckets, uint numConsumers, uint maxElementsPerBucket, ResourceManager& rm,
            boost::function<uint32_t (const char *data, uint len)> hash = utils::Hasher());

        virtual ~BucketDL();

        // datalist interface.  insert() and endOfInput() are the only
        // datalist function that makes sense.  The consumer side functions
        // are stubs.  Consumers know they're consuming a BucketDL.
        void insert(const element_t &e);
        void insert(const std::vector<element_t> &e);
		void insert(const element_t* array, uint64_t arrayCount);

        void insert(TupleType &e);
        void insert(std::vector<TupleType> &e);

        void endOfInput();
        uint64_t getIterator();
        bool next(uint64_t it, element_t *e);
        void setMultipleProducers(bool);

        // BucketDL consumer fcns
        uint64_t getIterator(uint64_t bucket);
        bool next(uint64_t bucket, uint64_t it, element_t *e);

        /** Returns the size of the specified bucket */
        uint64_t size(uint64_t bucket);

        /** Returns the total number of elements stored */
        uint64_t totalSize();

        /** Returns the number of buckets */
        uint64_t bucketCount();

        /** Sets the value to pass down to element_t::getHashString() */
        void setHashMode(uint64_t mode);

        /** Sets the value to RID_only or rid_value mode */
        void setElementMode(uint64_t mode);

		/** Total number of files and filespace used for temp files */
		void totalFileCounts(uint64_t& numFiles, uint64_t& numBytes) const;

		const uint32_t hashLen() const {return fHashLen;}
		void hashLen (const uint32_t hashLen) {fHashLen = hashLen;}

    	const uint32_t elementLen() const {return fElementLen;}
		void elementLen (const uint64_t ridSize, const uint64_t dataSize);

		/** This class does employ temp disk */
		virtual bool useDisk() const { return true; }

		/** Sets the size of the element components that are saved to disk */
		virtual void setDiskElemSize(uint32_t size1st,uint32_t size2nd);

		/** Accessor and mutator to the BucketReuseControlEntry */
		void reuseControl(BucketReuseControlEntry* control, bool readonly);
		BucketReuseControlEntry* reuseControl() { return fReuseControl; }

		/** Restores the buckets' set number and start positions */
		void restoreBucketInformation();

		/** @brief return tuple rid size */
        const uint64_t ridSize() const {return fRidSize;}

        /** @brief return tuple data size */
        const uint64_t dataSize() const {return fDataSize;}

		/** Enables disk I/O time logging */
		void enableDiskIoTrace();

		/** Returns the disk I/O time in seconds */
		bool totalDiskIoTime(uint64_t& w, uint64_t& r);

		/** Returns the reference of the disk I/O info list */
		std::list<DiskIoInfo>& diskIoInfoList(uint64_t bucket);

    protected:


    private:
		// Declare default constructors but don't define to disable their use
        explicit BucketDL();
        explicit BucketDL(const BucketDL<element_t> &);
		BucketDL<element_t>& operator=(const BucketDL<element_t> &);

	ResourceManager& fRm;
        WSDL<element_t> **buckets;
        WSDL<RIDElementType> **rbuckets;
        TWSVec fTBuckets;
        uint64_t numBuckets;
        uint64_t numConsumers;
        uint64_t maxElements;
        boost::function<uint32_t (const char *data, uint len)> hashFcn;
        uint64_t hashMode;
        uint64_t bucketMask;
        bool multiProducer;
        bool fTraceOn;
        uint64_t elementMode;
        uint32_t fHashLen;  // @bug 844. hash length for tuple type
        uint64_t fRidSize; // @bug 844.
        uint64_t fDataSize;
        uint64_t fElementLen;
        uint64_t bucketDoneCount;
        BucketReuseControlEntry* fReuseControl;

};

template<typename element_t>
BucketDL<element_t>::BucketDL(uint nb, uint nc, uint me, ResourceManager& rm,
    boost::function<uint32_t (const char *data, uint len)> hash)
    : base(), fRm(rm), buckets(0), rbuckets(0), fTraceOn(false), fHashLen(0), fElementLen(0),
      bucketDoneCount(0), fReuseControl(NULL)
{
    uint i;
    uint64_t mask;

    numBuckets = nb;
    numConsumers = nc;
    maxElements = me;
    hashFcn = hash;
    hashMode = 0;
    elementMode = RID_MODE;
    multiProducer = false;

    // initialize buckets
    if (typeid (element_t) == typeid(TupleType))
    {
        for (i = 0; i < numBuckets; i++)
            fTBuckets.push_back(TWSSP(new TupleWSDL(numConsumers, maxElements, fRm)));
    }
    else
    {
        rbuckets = new WSDL<RIDElementType> *[numBuckets];
        for (i = 0; i < numBuckets; i++)
          rbuckets[i] = new WSDL<RIDElementType>
    		(numConsumers, maxElements, rm);
    }

    for (i = 1, mask = 1, bucketMask = 0; i <= 64; i++) {
        mask <<= 1;
        bucketMask = (bucketMask << 1) | 1;
        if (numBuckets & mask)
            break;
    }

    for (i++, mask <<= 1; i <= 64; i++, mask <<= 1)
        if (numBuckets & mask)
            throw std::runtime_error("BucketDL: The number of buckets should be a power of 2.");
}

template<typename element_t>
BucketDL<element_t>::~BucketDL()
{
    if (typeid(element_t) == typeid(TupleType))
        return;
    uint64_t i;
    if (elementMode == RID_MODE){
        for (i = 0; i < numBuckets; i++)
            delete rbuckets[i];
        delete [] rbuckets;
    }
    else {
        for (i = 0; i < numBuckets; i++)
            delete buckets[i];
        delete [] buckets;
    }
}

template<typename element_t>
void BucketDL<element_t>::setMultipleProducers(bool b)
{
    multiProducer = b;
    uint64_t i;
    if (typeid(element_t) == typeid(TupleType))
    {
        for (i = 0; i < numBuckets; i++)
            fTBuckets[i]->setMultipleProducers(b);
    }
    else
    {
        if (elementMode == RID_MODE){
            for (i = 0; i < numBuckets; i++)
                rbuckets[i]->setMultipleProducers(b);
        }
        else {
            for (i = 0; i < numBuckets; i++)
                buckets[i]->setMultipleProducers(b);
        }
    }
}

template<typename element_t>
void BucketDL<element_t>::insert(const element_t &e)
{
/*	Need the element type to provide what to hash, which conflicts
    with the standard meaning of "<<".  For our currently-defined element types,
    this would be the rid field only, not the entire contents of the structure */

    uint64_t bucket, len = fHashLen;
    const char *hashStr;

    hashStr = e.getHashString(hashMode, &len);
    bucket = hashFcn(hashStr, len) & bucketMask;

    if (elementMode == RID_MODE)
    {
        RIDElementType rid(e.first);
        rbuckets[bucket]->insert(rid);
    }
    else
        buckets[bucket]->insert(e);

}

template<typename element_t>
void BucketDL<element_t>::insert(TupleType &e)
{
    uint64_t bucket, len = fHashLen;
    const char *hashStr;

    hashStr = e.getHashString(hashMode, &len);

    bucket = hashFcn(hashStr, len) & bucketMask;
    fTBuckets[bucket]->insert(e);
}

template<typename element_t>
void BucketDL<element_t>::insert(std::vector<TupleType> &v)
{
    std::vector<TupleType>::iterator it, end;

    if (multiProducer)
        base::lock();
   try
   {
    end = v.end();
    for (it = v.begin(); it != end; ++it)
        fTBuckets[hashFcn(it->second, fHashLen) & bucketMask]->insert_nolock(*it);
   }
   catch( ... )
   {
    if (multiProducer)
        base::unlock();
    throw;
   }
    if (multiProducer)
        base::unlock();
}
template<typename element_t>
void BucketDL<element_t>::insert(const std::vector<element_t> &v)
{
    typename std::vector<element_t>::const_iterator it, end;
    const char *hashStr;
    uint64_t len = fHashLen;

    if (multiProducer)
    	base::lock();
    try
    {
    end = v.end();
    if (elementMode == RID_MODE) {
        RIDElementType rid;
        for (it = v.begin(); it != end; ++it) {
            hashStr = it->getHashString(hashMode, &len);
            rid.first = (*it).first;
            rbuckets[hashFcn(hashStr, len) & bucketMask]->insert_nolock(rid);
        }
    }
    else
        for (it = v.begin(); it != end; ++it) {
            hashStr = it->getHashString(hashMode, &len);
            buckets[hashFcn(hashStr, len) & bucketMask]->insert_nolock(*it);
        }
    }
   catch( ... )
   {
    if (multiProducer)
        base::unlock();
    throw;
   }
    if (multiProducer)
    	base::unlock();
}
template<typename element_t>
void BucketDL<element_t>::insert(const element_t* array, uint64_t arrayCount)
{
    const char *hashStr;
    uint64_t len = fHashLen;

    if (multiProducer)
    	base::lock();
    try
    {
    if (elementMode == RID_MODE) {
        RIDElementType rid;
        for (uint64_t i=0; i<arrayCount; ++i) {
            hashStr = array[i].getHashString(hashMode, &len);
            rid.first = array[i].first;
            rbuckets[hashFcn(hashStr, len) & bucketMask]->insert_nolock(rid);
        }
    }
    else
        for (uint64_t i=0; i<arrayCount; ++i) {
            hashStr = array[i].getHashString(hashMode, &len);
            buckets[hashFcn(hashStr, len) & bucketMask]->insert_nolock(
				array[i]);
        }
    }
   catch( ... )
   {
    if (multiProducer)
        base::unlock();
    throw;
   }
    if (multiProducer)
    	base::unlock();
}

template<typename element_t>
uint64_t BucketDL<element_t>::getIterator()
{
    throw std::logic_error("don't call BucketDL::getIterator(), call getIterator(uint)");
}

template<typename element_t>
void BucketDL<element_t>::endOfInput()
{
	uint64_t i;
	uint64_t saveSize = 0; //debug

    if (typeid(element_t) == typeid(TupleType))
    {
        for (i = 0; i < numBuckets; i++) {
			fTBuckets[i]->endOfInput();
			saveSize += fTBuckets[i]->saveSize();
		}
		//std::cout << "bucketdl-" << this << " saveSize=" << saveSize << std::endl;
	}
    else
    	if (elementMode == RID_MODE) {
    		for (i = 0; i < numBuckets; i++)
    			rbuckets[i]->endOfInput();
    	}
    	else {
    		for (i = 0; i < numBuckets; i++)
    			buckets[i]->endOfInput();
    	}

	if (fReuseControl != NULL && fReuseControl->userNotified() == false)
		fReuseControl->notifyUsers();

}

template<typename element_t>
bool BucketDL<element_t>::next(uint64_t it, element_t *e)
{
    throw std::logic_error("don't call BucketDL::next(uint, element_t), call next(uint, uint, element_t");
}

template<typename element_t>
uint64_t BucketDL<element_t>::getIterator(uint64_t bucket)
{
	if (typeid(element_t) == typeid(TupleType))
	    return fTBuckets[bucket]->getIterator();
	if (elementMode == RID_MODE)
		return rbuckets[bucket]->getIterator();
	else
		return buckets[bucket]->getIterator();
}

template<typename element_t>
bool BucketDL<element_t>::next(uint64_t bucket, uint64_t it, element_t *e)
{
    if (typeid(element_t) == typeid(TupleType))
        return fTBuckets[bucket]->next(it, reinterpret_cast<TupleType*>(e));

  	bool ret;
  	if (elementMode == RID_MODE)
   	{
   		RIDElementType rid;
   		ret = rbuckets[bucket]->next(it, &rid);
   		e->first = rid.first;
   	}
   	else
   	{
   		ret = buckets[bucket]->next(it, e);
   	}

	if (ret != true  && fReuseControl != NULL)
	{
		// because not all the buckets are consumed at the same time,
		// getIterator(i) maybe called sequentially by one or more threads,
		// need to make sure all consumers are done with all the buckets
		base::lock();
		if (++bucketDoneCount == numConsumers*numBuckets)
			BucketReuseManager::instance()->userDeregister(fReuseControl);
		base::unlock();
	}

	return ret;
}

template<typename element_t>
uint64_t BucketDL<element_t>::size(uint64_t bucket)
{
    if (typeid(element_t) == typeid(TupleType))
        return fTBuckets[bucket]->totalSize();
    if (elementMode == RID_MODE)
        return rbuckets[bucket]->totalSize();
    else
        return buckets[bucket]->totalSize();
}

template<typename element_t>
uint64_t BucketDL<element_t>::bucketCount()
{
    return numBuckets;
}

template<typename element_t>
uint64_t BucketDL<element_t>::totalSize()
{
    uint64_t ret = 0;
    uint64_t i;

    if (typeid(element_t) == typeid(TupleType))
        for (i = 0; i < numBuckets; i++)
            ret += fTBuckets[i]->totalSize();
    else
        if (elementMode == RID_MODE) {
            for (i = 0; i < numBuckets; i++)
                ret += rbuckets[i]->totalSize();
        }
        else {
            for (i = 0; i < numBuckets; i++)
                ret += buckets[i]->totalSize();
        }
    return ret;
}

template<typename element_t>
void BucketDL<element_t>::setHashMode(uint64_t mode)
{
    hashMode = mode;
    // Make elementMode the same as hashMode unless setElementMode
    // is explicitly called by the caller, like filterstep.
    setElementMode (mode);
}

template<typename element_t>
void BucketDL<element_t>::setElementMode(uint64_t mode)
{
    uint64_t i;

    if (typeid(element_t) == typeid(TupleType))
        return;
    if (elementMode != mode) {
        if (elementMode == RID_MODE){
            for (i = 0; i < numBuckets; i++)
                delete rbuckets[i];
            delete [] rbuckets;
        }
        else {
            for (i = 0; i < numBuckets; i++)
                delete buckets[i];
            delete [] buckets;
        }
        elementMode = mode;
        if (elementMode == RID_MODE) {
            rbuckets = new WSDL<RIDElementType> *[numBuckets];
            for (i = 0; i < numBuckets; i++) {
                rbuckets[i] = new WSDL<RIDElementType>
					(numConsumers, maxElements,
					base::getDiskElemSize1st(),
					 base::getDiskElemSize2nd(), fRm);
                rbuckets[i]->setMultipleProducers(multiProducer);
                rbuckets[i]->traceOn(fTraceOn);
            }
        }
        else {
            buckets = new WSDL<element_t>*[numBuckets];
            for (i = 0; i < numBuckets; i++) {
                buckets[i] = new WSDL<element_t>
					(numConsumers, maxElements,
					base::getDiskElemSize1st(),
					 base::getDiskElemSize2nd(), fRm);
                buckets[i]->setMultipleProducers(multiProducer);
                buckets[i]->traceOn(fTraceOn);
            }
        }
    }
//    std::cout << "bucketdl-" << this << " setElementMode " << hashMode << std::endl;

}

//
// Returns the number of temp files and the space taken up by those files
// (in bytes) by this Bucket collection.
//
template<typename element_t>
void BucketDL<element_t>::totalFileCounts(
	uint64_t& numFiles,
	uint64_t& numBytes) const
{
	numFiles = 0;
	numBytes = 0;

    if (typeid(element_t) == typeid(TupleType))
    {
		for (uint64_t i = 0; i < numBuckets; i++)
		{
			uint64_t setCnt = fTBuckets[i]->initialSetCount();

			if (setCnt > 1)
			{
				numFiles += fTBuckets[i]->numberOfTempFiles();
				numBytes += fTBuckets[i]->saveSize();
			}
		}
	}
	else
    	if (elementMode == RID_MODE)
    	{
    		for (uint64_t i = 0; i < numBuckets; i++)
    		{
    			uint64_t setCnt = rbuckets[i]->initialSetCount();

    			if (setCnt > 1)
    			{
    				//std::cout << "BDL: bucket " << i << " has " << setCnt <<
    				//	" sets" << std::endl;

    				numFiles += rbuckets[i]->numberOfTempFiles();
    				numBytes += rbuckets[i]->saveSize;
    			}
    		}
    	}
    	else
    	{
    		for (uint64_t i = 0; i < numBuckets; i++)
    		{
    			uint64_t setCnt = buckets[i]->initialSetCount();

    			if (setCnt > 1)
    			{
    				//std::cout << "BDL: bucket " << i << " has " << setCnt <<
    				//	" sets" << std::endl;

    				numFiles += buckets[i]->numberOfTempFiles();
    				numBytes += buckets[i]->saveSize;
    			}
    		}
    	}
}

template<typename element_t>
void BucketDL<element_t>::elementLen(const uint64_t ridSize, const uint64_t dataSize)
{
    fElementLen = ridSize + dataSize;
    uint64_t i;
    if (typeid(element_t) == typeid(TupleType))
    {
        for (i = 0; i < numBuckets; i++)
            fTBuckets[i]->tupleSize(ridSize, dataSize);
    }
}

//
// Sets the sizes to be employed in saving the elements to disk.
// size1st is the size in bytes of element_t.first.
// size2nd is the size in bytes of element_t.second.
//
template<typename element_t>
void BucketDL<element_t>::setDiskElemSize(uint32_t size1st,uint32_t size2nd)
{
	base::fElemDiskFirstSize  = size1st;
	base::fElemDiskSecondSize = size2nd;

	//...Forward this size information to our internal WSDL containers.
	// @todo compress for tuplewsdl
    if (typeid(element_t) == typeid(TupleType))
        return;
	if (elementMode == RID_MODE)
	{
		for (uint64_t i = 0; i < numBuckets; i++)
		{
			rbuckets[i]->setDiskElemSize ( size1st, size2nd );
		}
	}
	else
	{
		for (uint64_t i = 0; i < numBuckets; i++)
		{
			buckets[i]->setDiskElemSize ( size1st, size2nd );
		}
	}

	if (fReuseControl != NULL)
	{
		fReuseControl->dataSize().first = size1st;
		fReuseControl->dataSize().second = size2nd;
	}
}

template<typename element_t>
void BucketDL<element_t>::reuseControl(BucketReuseControlEntry* control, bool readonly)
{
    // @todo reuse for tuplewsdl
    if (typeid(element_t) == typeid(TupleType))
        return;

	if (control == NULL)
		return;

	fReuseControl = control;
	std::vector<SetRestoreInfo>& infoVec = fReuseControl->restoreInfoVec();
	infoVec.resize(numBuckets);

	for (uint64_t i = 0; i < numBuckets; i++)
	{
		std::stringstream ss;
		ss << control->baseName() << "." << i;
		if (elementMode == RID_MODE)
			rbuckets[i]->setReuseInfo(&(infoVec[i]), ss.str().c_str(), readonly);
		else
			buckets[i]->setReuseInfo(&(infoVec[i]), ss.str().c_str(), readonly);
	}
}

template<typename element_t>
void BucketDL<element_t>::restoreBucketInformation()
{
    if (typeid(element_t) == typeid(TupleType))
        return;

	std::vector<SetRestoreInfo>& infoVec = fReuseControl->restoreInfoVec();
	if (elementMode == RID_MODE)
		for (uint64_t i = 0; i < numBuckets; i++)
			rbuckets[i]->restoreSetForReuse(infoVec[i]);
	else
		for (uint64_t i = 0; i < numBuckets; i++)
			buckets[i]->restoreSetForReuse(infoVec[i]);
}

template<typename element_t>
void BucketDL<element_t>::enableDiskIoTrace()
{
	fTraceOn = true;
	if (typeid(element_t) == typeid(TupleType))
	{
	    for (uint64_t bucket = 0; bucket < numBuckets; bucket++)
	        fTBuckets[bucket]->traceOn(fTraceOn);
	    return;
	}
	if (elementMode == RID_MODE)
		for (uint64_t bucket = 0; bucket < numBuckets; bucket++)
			rbuckets[bucket]->traceOn(fTraceOn);
	else
		for (uint64_t bucket = 0; bucket < numBuckets; bucket++)
			buckets[bucket]->traceOn(fTraceOn);
}

template<typename element_t>
bool BucketDL<element_t>::totalDiskIoTime(uint64_t& w, uint64_t& r)
{
	boost::posix_time::time_duration wTime(0,0,0,0);
	boost::posix_time::time_duration rTime(0,0,0,0);
	bool diskIo = false;

	for (uint64_t bucket = 0; bucket < numBuckets; bucket++)
	{
		std::list<DiskIoInfo>& infoList = diskIoInfoList(bucket);
		std::list<DiskIoInfo>::iterator k = infoList.begin();
		while (k != infoList.end())
		{
			if (k->fWrite == true)
				wTime += k->fEnd - k->fStart;
			else
				rTime += k->fEnd - k->fStart;
			k++;
		}

		if (infoList.size() > 0)
			diskIo = true;
	}

	w = wTime.total_seconds();
	r = rTime.total_seconds();

	return diskIo;
}

template<typename element_t>
std::list<DiskIoInfo>& BucketDL<element_t>::diskIoInfoList(uint64_t bucket)
{
    if (typeid(element_t) == typeid(TupleType))
        return (fTBuckets[bucket]->diskIoList());
	if (elementMode == RID_MODE)
		return (rbuckets[bucket]->diskIoList());
	else
		return (buckets[bucket]->diskIoList());
}

} //namespace

#endif
