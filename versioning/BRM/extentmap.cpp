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

/*****************************************************************************
 * $Id: extentmap.cpp 1936 2013-07-09 22:10:29Z dhall $
 *
 ****************************************************************************/

#include <iostream>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <algorithm>
#include <ios>
#include <cerrno>
#include <sstream>
#include <vector>
#include <limits>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#ifndef _MSC_VER
#include <tr1/unordered_set>
#else
#include <unordered_set>
#endif

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
namespace bi=boost::interprocess;

#include "liboamcpp.h"
#include "brmtypes.h"
#include "configcpp.h"
#include "rwlock.h"
#include "calpontsystemcatalog.h"
#include "mastersegmenttable.h"
#include "blocksize.h"
#include "dataconvert.h"
#include "oamcache.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"
#ifdef BRM_INFO
 #error BRM_INFO is broken right now
 #include "tracer.h"
 #include "configcpp.h"
#endif

#define EXTENTMAP_DLLEXPORT
#include "extentmap.h"
#undef EXTENTMAP_DLLEXPORT

#define EM_MAX_SEQNUM               2000000000
#define MAX_IO_RETRIES 10
#define EM_MAGIC_V1 0x76f78b1c
#define EM_MAGIC_V2 0x76f78b1d
#define EM_MAGIC_V3 0x76f78b1e
#define EM_MAGIC_V4 0x76f78b1f

#ifndef NDEBUG
#define ASSERT(x) \
	if (!(x)) { \
		cerr << "assertion at file " << __FILE__ << " line " << __LINE__ << " failed" << endl; \
		throw logic_error("assertion failed"); \
	}
#else
#define ASSERT(x)
#endif

using namespace std;
using namespace boost;
using namespace logging;
using namespace idbdatafile;

namespace
{
unsigned ExtentSize = 0; // dmc-need to deprecate
unsigned ExtentRows              = 0;
unsigned filesPerColumnPartition = 0;
unsigned extentsPerSegmentFile   = 0;

// Increment CP sequence (version) number, and wrap-around when applicable
inline void incSeqNum(int32_t& seqNum)
{
	seqNum++;
	if (seqNum > EM_MAX_SEQNUM)
		seqNum = 0;
}

}

namespace BRM {

//------------------------------------------------------------------------------
// EMCasualPartition_struct methods
//------------------------------------------------------------------------------

EMCasualPartition_struct::EMCasualPartition_struct()
{
	lo_val=numeric_limits<int64_t>::min();
	hi_val=numeric_limits<int64_t>::max();
	sequenceNum=0;
	isValid = CP_INVALID;
}

EMCasualPartition_struct::EMCasualPartition_struct(const int64_t lo, const int64_t hi, const int32_t seqNum)
{
	lo_val=lo;
	hi_val=hi;
	sequenceNum=seqNum;
	isValid = CP_INVALID;
}

EMCasualPartition_struct::EMCasualPartition_struct(const EMCasualPartition_struct& em)
{
	lo_val=em.lo_val;
	hi_val=em.hi_val;
	sequenceNum=em.sequenceNum;
	isValid = em.isValid;
}

EMCasualPartition_struct& EMCasualPartition_struct::operator= (const EMCasualPartition_struct& em)
{
	lo_val=em.lo_val;
	hi_val=em.hi_val;
	sequenceNum=em.sequenceNum;
	isValid = em.isValid;
	return *this;
}

//------------------------------------------------------------------------------
// Version 4 EmEntry methods
//------------------------------------------------------------------------------

EMEntry::EMEntry()
{
	fileID = 0;
	blockOffset = 0;
	HWM = 0;
	partitionNum = 0;
	segmentNum   = 0;
	dbRoot       = 0;
	colWid       = 0;
	status		= 0;
}

EMEntry::EMEntry(const EMEntry& e) 
{
	range.start = e.range.start;
	range.size = e.range.size;
	fileID = e.fileID;
	blockOffset = e.blockOffset;
	HWM = e.HWM;
	partition = e.partition;
	partitionNum = e.partitionNum;
	segmentNum   = e.segmentNum;
	dbRoot       = e.dbRoot;
	colWid       = e.colWid;
	status		= e.status;
}

EMEntry& EMEntry::operator= (const EMEntry& e)
{
	range.start = e.range.start;
	range.size = e.range.size;
	fileID = e.fileID;
	blockOffset = e.blockOffset;
	HWM = e.HWM;
	partition = e.partition;
	partitionNum = e.partitionNum;
	segmentNum   = e.segmentNum;
	colWid       = e.colWid;
	dbRoot       = e.dbRoot;
	status		= e.status;
	return *this;
}

bool EMEntry::operator< (const EMEntry& e) const
{
	if (range.start < e.range.start)
		return true;
	return false;
}

/*static*/
boost::mutex ExtentMapImpl::fInstanceMutex;
boost::mutex ExtentMap::mutex;

/*static*/
ExtentMapImpl* ExtentMapImpl::fInstance=0;

/*static*/
ExtentMapImpl* ExtentMapImpl::makeExtentMapImpl(unsigned key, off_t size, bool readOnly)
{
	boost::mutex::scoped_lock lk(fInstanceMutex);

	if (fInstance)
	{
		if (key != fInstance->fExtMap.key())
		{
			BRMShmImpl newShm(key, 0);
			fInstance->swapout(newShm);
		}
		ASSERT(key == fInstance->fExtMap.key());
		return fInstance;
	}

	fInstance = new ExtentMapImpl(key, size, readOnly);

	return fInstance;
}

ExtentMapImpl::ExtentMapImpl(unsigned key, off_t size, bool readOnly) :
	fExtMap(key, size, readOnly)
{
}

/*static*/
boost::mutex FreeListImpl::fInstanceMutex;

/*static*/
FreeListImpl* FreeListImpl::fInstance=0;

/*static*/
FreeListImpl* FreeListImpl::makeFreeListImpl(unsigned key, off_t size, bool readOnly)
{
	boost::mutex::scoped_lock lk(fInstanceMutex);

	if (fInstance)
	{
		if (key != fInstance->fFreeList.key())
		{
			BRMShmImpl newShm(key, 0);
			fInstance->swapout(newShm);
		}
		ASSERT(key == fInstance->fFreeList.key());
		return fInstance;
	}

	fInstance = new FreeListImpl(key, size, readOnly);

	return fInstance;
}

FreeListImpl::FreeListImpl(unsigned key, off_t size, bool readOnly) :
	fFreeList(key, size, readOnly)
{
}

ExtentMap::ExtentMap()
{
	fExtentMap = NULL;
	fFreeList = NULL;
	fCurrentEMShmkey = -1;
	fCurrentFLShmkey = -1;
	fEMShminfo = NULL;
	fFLShminfo = NULL;
	r_only = false;
	flLocked = false;
	emLocked = false;
	fPExtMapImpl = 0;
	fPFreeListImpl = 0;

#ifdef BRM_INFO
	fDebug = ("Y" == config::Config::makeConfig()->getConfig("DBRM", "Debug"));
#endif
}

ExtentMap::~ExtentMap()
{
	PmDbRootMap_t::iterator iter = fPmDbRootMap.begin();
	PmDbRootMap_t::iterator end = fPmDbRootMap.end();
	while (iter != end)
	{
		delete iter->second;
		iter->second = 0;
		++iter;
	}
	fPmDbRootMap.clear();
}

// Casual Partioning support
//

/**
* @brief mark the max/min values of an extent as invalid
*
* mark the extent containing the lbid as invalid and
* increment the sequenceNum value. If the lbid is found
* in the extent map a 0 is returned otherwise a 1.
*
**/

int ExtentMap::_markInvalid(const LBID_t lbid, const execplan::CalpontSystemCatalog::ColDataType colDataType)
{
	int entries;
	int i;
	LBID_t lastBlock;

	entries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (i = 0; i < entries; i++) {
		lastBlock = fExtentMap[i].range.start +
			(static_cast<LBID_t>(fExtentMap[i].range.size) * 1024) - 1;
		if (fExtentMap[i].range.size != 0) {
			if (lbid >= fExtentMap[i].range.start && lbid <= lastBlock) {
				makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));
				fExtentMap[i].partition.cprange.isValid = CP_UPDATING;
                if (isUnsigned(colDataType))
                {
                    fExtentMap[i].partition.cprange.lo_val=numeric_limits<uint64_t>::max();
                    fExtentMap[i].partition.cprange.hi_val=0;
                }
                else
                {
                    fExtentMap[i].partition.cprange.lo_val=numeric_limits<int64_t>::max();
                    fExtentMap[i].partition.cprange.hi_val=numeric_limits<int64_t>::min();
                }
				incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);
#ifdef BRM_DEBUG 
                ostringstream os;
                os << "ExtentMap::_markInvalid(): casual partitioning update: firstLBID=" <<
                    fExtentMap[i].range.start << " lastLBID=" << fExtentMap[i].range.start +
                    fExtentMap[i].range.size*1024 - 1 << " OID=" << fExtentMap[i].fileID <<
                    " min=" << fExtentMap[i].partition.cprange.lo_val << 
                    " max=" << fExtentMap[i].partition.cprange.hi_val << 
                    "seq=" << fExtentMap[i].partition.cprange.sequenceNum;
                log(os.str(), logging::LOG_TYPE_DEBUG);
#endif
				return 0;
			}
		}
	}
	throw logic_error("ExtentMap::markInvalid(): lbid isn't allocated");
}

int ExtentMap::markInvalid(const LBID_t lbid, 
                           const execplan::CalpontSystemCatalog::ColDataType colDataType)
{
#ifdef BRM_DEBUG
	if (lbid < 0)
		throw invalid_argument("ExtentMap::markInvalid(): lbid must be >= 0");
#endif
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("_markInvalid");
		TRACER_ADDINPUT(lbid);
		TRACER_WRITE;
	}
#endif

#ifdef BRM_DEBUG 
    ostringstream os;
    os << "ExtentMap::markInvalid(" << lbid << "," << colDataType << ")";
    log(os.str(), logging::LOG_TYPE_DEBUG);
#endif

    grabEMEntryTable(WRITE);
	return _markInvalid(lbid, colDataType);
}

/**
* @brief calls markInvalid(LBID_t lbid) for each extent containing any lbid in vector<LBID_t>& lbids
*
**/

int ExtentMap::markInvalid(const vector<LBID_t>& lbids,
                           const vector<execplan::CalpontSystemCatalog::ColDataType>& colDataTypes)
{
	uint32_t i, size = lbids.size();

#ifdef BRM_DEBUG
	for (i = 0; i < size; ++i)
		if (lbids[i] < 0)
			throw invalid_argument("ExtentMap::markInvalid(vector): all lbids must be >= 0");
#endif
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("_markInvalid");
		TRACER_ADDINPUT(size);
		TRACER_WRITE;
	}
#endif

	grabEMEntryTable(WRITE);

	// XXXPAT: what's the proper return code when one and only one fails?
    for (i = 0; i < size; ++i) {
#ifdef BRM_DEBUG 
        ostringstream os;
        os << "ExtentMap::markInvalid() lbids[" << i << "]=" << lbids[i] <<
            " colDataTypes[" << i << "]=" << colDataTypes[i];
        log(os.str(), logging::LOG_TYPE_DEBUG);
#endif
		try {
			_markInvalid(lbids[i], colDataTypes[i]);
		}
		catch (std::exception &e) {
			cerr << "ExtentMap::markInvalid(vector): warning!  lbid " << lbids[i] <<
				" caused " << e.what() << endl;
		}
	}
	return 0;
}

/**
* @brief set the max/min values for the extent if the seqNum matches the extents sequenceNum
*
* reset the lbid's hi_val to max and lo_val to min 
* the seqNum matches the ExtentMap.sequenceNum. Then increments
* the current sequenceNum value by 1. If the sequenceNum does not
* match the seqNum value do not update the lbid's max/min values
* or increment the sequenceNum value and return a -1.

**/

int ExtentMap::setMaxMin(const LBID_t lbid,
							const int64_t max,
							const int64_t min, 
							const int32_t seqNum,
							bool firstNode)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("updateMaxMin");
		TRACER_ADDINPUT(lbid);
		TRACER_ADDINPUT(max);
		TRACER_ADDINPUT(min);
		TRACER_ADDINPUT(seqNum);
		TRACER_WRITE;
	}

#endif
	int entries;
	int i;
	LBID_t lastBlock;
	int32_t curSequence;

#ifdef BRM_DEBUG
	if (lbid< 0)
		throw invalid_argument("ExtentMap::setMaxMin(): lbid must be >= 0");
#endif

	grabEMEntryTable(WRITE);
	entries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (i = 0; i < entries; i++) {
		if (fExtentMap[i].range.size != 0) {
			lastBlock = fExtentMap[i].range.start + 
				(static_cast<LBID_t>(fExtentMap[i].range.size) * 1024) - 1;
			curSequence = fExtentMap[i].partition.cprange.sequenceNum;
			if (lbid >= fExtentMap[i].range.start && lbid <= lastBlock)
			{
#ifdef BRM_DEBUG
                if (firstNode) {
                    ostringstream os;
                    os << "ExtentMap::setMaxMin(): casual partitioning update: firstLBID=" <<
                        fExtentMap[i].range.start << " lastLBID=" << fExtentMap[i].range.start +
                        fExtentMap[i].range.size*1024 - 1 << " OID=" << fExtentMap[i].fileID <<
                        " min=" << min << " max=" << max << "seq=" << seqNum;
                    log(os.str(), logging::LOG_TYPE_DEBUG);
                }
#endif
				if (curSequence == seqNum)
				{
					makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));
					fExtentMap[i].partition.cprange.hi_val = max;
					fExtentMap[i].partition.cprange.lo_val = min;
					fExtentMap[i].partition.cprange.isValid = CP_VALID;
					incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);
					return 0;
				}
				//special val to indicate a reset--used by editem -c.
				//Also used by COMMIT and ROLLBACK to invalidate CP.
				else if (seqNum == -1)
				{
					makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));
                    // We set hi_val and lo_val to correct values for signed or unsigned
                    // during the markinvalid step, which sets the invalid variable to CP_UPDATING.
                    // During this step (seqNum == -1), the min and max passed in are not reliable
                    // and should not be used.
					fExtentMap[i].partition.cprange.isValid = CP_INVALID;
					incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);
					return 0;
				}
				else 
				{
					return 0;
				}
			}
		}
	}
	if (emLocked)
		releaseEMEntryTable(WRITE);
	throw logic_error("ExtentMap::setMaxMin(): lbid isn't allocated");
// 	return -1;
}

// @bug 1970.  Added updateExtentsMaxMin function.
// @note - The key passed in the map must the the first LBID in the extent.
void ExtentMap::setExtentsMaxMin(const CPMaxMinMap_t &cpMap, bool firstNode, bool useLock)
{
	CPMaxMinMap_t::const_iterator it;

#ifdef BRM_DEBUG
    log("ExtentMap::setExtentsMaxMin()", logging::LOG_TYPE_DEBUG);
    for(it = cpMap.begin(); it != cpMap.end(); ++it)
    {
        ostringstream os;
        os << "FirstLBID=" << it->first <<
            " min=" << it->second.min << 
            " max=" << it->second.max << 
            " seq=" << it->second.seqNum;
        log(os.str(), logging::LOG_TYPE_DEBUG);
    }
#endif


#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("setExtentsMaxMin");
		for(it = cpMap.begin(); it != cpMap.end(); ++it)
		{
			TRACER_ADDINPUT((*it).first);
			TRACER_ADDINPUT((*it).second.max);
			TRACER_ADDINPUT((*it).second.min);
			TRACER_ADDINPUT((*it).second.seqNum);
			TRACER_WRITE;
		}
	}

#endif
	int entries;
	int i;
	int32_t curSequence;
	const int32_t extentsToUpdate = cpMap.size();
	int32_t extentsUpdated = 0;

#ifdef BRM_DEBUG
	if (extentsToUpdate <= 0)
		throw invalid_argument("ExtentMap::setExtentsMaxMin(): cpMap must be populated");
#endif

	if (useLock)
		grabEMEntryTable(WRITE);
	entries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	for (i = 0; i < entries; i++) {
		if (fExtentMap[i].range.size != 0) {
			it = cpMap.find(fExtentMap[i].range.start);
			if(it != cpMap.end())
			{
				curSequence = fExtentMap[i].partition.cprange.sequenceNum;
				if (curSequence == it->second.seqNum &&
						fExtentMap[i].partition.cprange.isValid == CP_INVALID)
				{
					makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));
					fExtentMap[i].partition.cprange.hi_val = it->second.max;
					fExtentMap[i].partition.cprange.lo_val = it->second.min;
					fExtentMap[i].partition.cprange.isValid = CP_VALID;
					incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);
					extentsUpdated++;
#ifdef BRM_DEBUG
					if (firstNode) {
                        ostringstream os;
						os << "ExtentMap::setExtentsMaxMin(): casual partitioning update: firstLBID=" <<
							fExtentMap[i].range.start << " lastLBID=" << fExtentMap[i].range.start +
							fExtentMap[i].range.size*1024 - 1 << " OID=" << fExtentMap[i].fileID <<
                            " min=" << it->second.min << " max=" <<
                            it->second.max << " seq=" <<
                            it->second.seqNum;
						log(os.str(), logging::LOG_TYPE_DEBUG);
					}
#endif
				}
				//special val to indicate a reset -- ignore the min/max
				else if (it->second.seqNum == -1)
				{
                    makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));
                    // We set hi_val and lo_val to correct values for signed or unsigned
                    // during the markinvalid step, which sets the invalid variable to CP_UPDATING.
                    // During this step (seqNum == -1), the min and max passed in are not reliable
                    // and should not be used.
					fExtentMap[i].partition.cprange.isValid = CP_INVALID;
					incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);
					extentsUpdated++;
				}
                //special val to indicate a reset -- assign the min/max
                else if (it->second.seqNum == -2)
                {
                    makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));
                    fExtentMap[i].partition.cprange.hi_val = it->second.max;
                    fExtentMap[i].partition.cprange.lo_val = it->second.min;
                    fExtentMap[i].partition.cprange.isValid = CP_INVALID;
                    incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);
                    extentsUpdated++;
                }
				// else sequence has changed since start of the query.  Don't update the EM entry.
				else
                {
					extentsUpdated++;
                }

				if (extentsUpdated == extentsToUpdate)
				{
					return;
				}
			}
		}
	}
	throw logic_error("ExtentMap::setExtentsMaxMin(): lbid isn't allocated");
}

//------------------------------------------------------------------------------
// @bug 1970.  Added mergeExtentsMaxMin to merge CP info for list of extents.
// @note - The key passed in the map must the starting LBID in the extent.
// Used by cpimport to update extentmap casual partition min/max.
// NULL or empty values should not be passed in as min/max values.
// seqNum in the input struct is not currently used.
//
// Note that DML calls markInvalid() to flag an extent as CP_UPDATING and incre-
// ments the sequence number prior to any change, and then marks the extent as
// CP_INVALID at transaction's end.
// Since cpimport locks the entire table prior to making any changes, it is
// assumed that the state of an extent will not be changed (by anyone else)
// during an import; so cpimport does not employ the intermediate CP_UPDATING
// state that DML uses.  cpimport just waits till the end of the job and incre-
// ments the sequence number and changes the state to CP_INVALID at that time.
// We may want/need to reconsider this at some point.
//------------------------------------------------------------------------------
void ExtentMap::mergeExtentsMaxMin(CPMaxMinMergeMap_t &cpMap, bool useLock)
{
	CPMaxMinMergeMap_t::const_iterator it;

#ifdef BRM_DEBUG
    log("ExtentMap::mergeExtentsMaxMin()", logging::LOG_TYPE_DEBUG);
    for(it = cpMap.begin(); it != cpMap.end(); ++it)
    {
        ostringstream os;
        os << "FirstLBID=" << it->first <<
            " min=" << it->second.min << 
            " max=" << it->second.max << 
            " seq=" << it->second.seqNum <<
            " typ: " << (*it).second.type <<
            " new: " << (*it).second.newExtent;
        log(os.str(), logging::LOG_TYPE_DEBUG);
    }
#endif

#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("mergeExtentsMaxMin");
		unsigned int count = 1;
		for(it = cpMap.begin(); it != cpMap.end(); ++it)
		{
			ostringstream oss;
			oss << "  "   << count                <<
				". LBID: "<< (*it).first          <<
				"; max: " << (*it).second.max     <<
				"; min: " << (*it).second.min     <<
				"; seq: " << (*it).second.seqNum  <<
				"; typ: " << (*it).second.type    <<
				"; new: " << (*it).second.newExtent;
			TRACER_WRITEDIRECT(oss.str());
			count++;
		}
	}
#endif

	const int32_t extentsToMerge = cpMap.size();
	int32_t extentsMerged = 0;

#ifdef BRM_DEBUG
	if (extentsToMerge <= 0)
		throw invalid_argument("ExtentMap::mergeExtentsMaxMin(): "
                               "cpMap must be populated");
#endif

	if (useLock)
		grabEMEntryTable(WRITE);
	int entries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	for (int i = 0; i < entries; i++) {			// loop through all extents
		if (fExtentMap[i].range.size != 0) {	// find eligible extents
			it = cpMap.find(fExtentMap[i].range.start);
			if(it != cpMap.end())
			{
#ifdef BRM_DEBUG
                ostringstream os;
                os << "ExtentMap::mergeExtentsMaxMin(): casual partitioning update: firstLBID=" <<
                    fExtentMap[i].range.start << " lastLBID=" << fExtentMap[i].range.start +
                    fExtentMap[i].range.size*1024 - 1 << " OID=" << fExtentMap[i].fileID <<
                    " hi_val=" << fExtentMap[i].partition.cprange.hi_val <<
                    " lo_val=" << fExtentMap[i].partition.cprange.lo_val <<
                    " min=" << it->second.min << " max=" << it->second.max << 
                    " seq=" << it->second.seqNum;
                log(os.str(), logging::LOG_TYPE_DEBUG);
#endif
				switch (fExtentMap[i].partition.cprange.isValid)
				{
					// Merge input min/max with current min/max
					case CP_VALID:
					{
						if (!isValidCPRange( it->second.max,
											 it->second.min,
                                             it->second.type ))
						{
							break;
						}

						makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));

						// We check the validity of the current min/max,
						// because isValid could be CP_VALID for an extent
						// having all NULL values, in which case the current
						// min/max needs to be set instead of merged.

                        if (isValidCPRange(
							fExtentMap[i].partition.cprange.hi_val,
							fExtentMap[i].partition.cprange.lo_val,
                            it->second.type))
						{
							// Swap byte order to do binary string comparison
							if (isCharType(it->second.type))
							{
								int64_t newMinVal =
									static_cast<int64_t>( uint64ToStr(
									static_cast<uint64_t>(it->second.min)));
								int64_t newMaxVal =
									static_cast<int64_t>( uint64ToStr(
									static_cast<uint64_t>(it->second.max)));
								int64_t oldMinVal =
									static_cast<int64_t>( uint64ToStr(
									static_cast<uint64_t>(
									fExtentMap[i].partition.cprange.lo_val)) );
								int64_t oldMaxVal =
									static_cast<int64_t>( uint64ToStr(
									static_cast<uint64_t>(
									fExtentMap[i].partition.cprange.hi_val)) );

								if (newMinVal < oldMinVal)
									fExtentMap[i].partition.cprange.lo_val =
										it->second.min;
								if (newMaxVal > oldMaxVal)
									fExtentMap[i].partition.cprange.hi_val =
										it->second.max;
							}
							else
                            if (isUnsigned(it->second.type))
							{
                                if (static_cast<uint64_t>(it->second.min) <
                                    static_cast<uint64_t>(fExtentMap[i].partition.cprange.lo_val))
                                {
                                    fExtentMap[i].partition.cprange.lo_val =
                                        it->second.min;
                                }
                                if (static_cast<uint64_t>(it->second.max) >
                                    static_cast<uint64_t>(fExtentMap[i].partition.cprange.hi_val))
                                {
                                    fExtentMap[i].partition.cprange.hi_val =
                                        it->second.max;
                                }
                            }
                            else
                            {
								if (it->second.min <
                                    fExtentMap[i].partition.cprange.lo_val)
                                    fExtentMap[i].partition.cprange.lo_val =
                                        it->second.min;
								if (it->second.max >
									fExtentMap[i].partition.cprange.hi_val)
									fExtentMap[i].partition.cprange.hi_val =
										it->second.max;
							}
						}
						else
						{
							fExtentMap[i].partition.cprange.lo_val =
								it->second.min;
							fExtentMap[i].partition.cprange.hi_val =
								it->second.max;
						}
						incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);

						break;
					}

					// DML is updating; just increment seqnum.
					// This case is here for completeness.  Table lock should
					// prevent this state from occurring (see notes at top of
					// this function)
					case CP_UPDATING:
					{
						makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));
						incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);

						break;
					}

					// Reset min/max to new min/max only "if" we can treat this
					// as a new extent, else leave the extent marked as INVALID
					case CP_INVALID:
					default:
					{
						makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));

						if (it->second.newExtent)
						{
							if (isValidCPRange( it->second.max,
												it->second.min,
                                                it->second.type ))
							{
								fExtentMap[i].partition.cprange.lo_val =
									it->second.min;
								fExtentMap[i].partition.cprange.hi_val =
									it->second.max;
							}

							// Even if invalid range; we set state to CP_VALID,
							// because the extent is valid, it is just empty.
							fExtentMap[i].partition.cprange.isValid = CP_VALID;
						}
						incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);
						break;
					}
				}	// switch on isValid state

				extentsMerged++;
				if (extentsMerged == extentsToMerge)
				{
					return; // Leave when all extents in map are matched
				}

				// Deleting objects from map, may speed up successive searches
				cpMap.erase( it );

			}	// found a matching extent in the Map
		}	// extent map range size != 0
	}	// end of loop through extent map

	throw logic_error("ExtentMap::mergeExtentsMaxMin(): lbid not found");
}

//------------------------------------------------------------------------------
// Use this function to see if the range is a valid min/max range or not.
// Range is considered invalid if min or max, are NULL (min()), or EMPTY
// (min()+1). For unsigned types NULL is max() and EMPTY is max()-1.
//------------------------------------------------------------------------------
bool ExtentMap::isValidCPRange(int64_t max, int64_t min, execplan::CalpontSystemCatalog::ColDataType type) const
{
    if (isUnsigned(type))
    {
        if ( (static_cast<uint64_t>(min) >= (numeric_limits<uint64_t>::max()-1)) ||
             (static_cast<uint64_t>(max) >= (numeric_limits<uint64_t>::max()-1)) )
        {
            return false;
        }
    }
    else
    {
        if ( (min <= (numeric_limits<int64_t>::min()+1)) ||
             (max <= (numeric_limits<int64_t>::min()+1)) )
        {
            return false;
        }
    }

	return true;
}

/**
* @brief retrieve the hi_val and lo_val or sequenceNum of the extent containing the LBID lbid.
*
* For the extent containing the LBID lbid, return the max/min values if the extent range values
* are valid and a -1 in the seqNum parameter. If the range values are flaged as invalid
* return the sequenceNum of the extent and the max/min values as -1.
**/

int ExtentMap::getMaxMin(const LBID_t lbid,
						int64_t& max,
						int64_t& min, 
						int32_t& seqNum)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getMaxMin");
		TRACER_ADDINPUT(lbid);
		TRACER_ADDOUTPUT(max);
		TRACER_ADDOUTPUT(min);
		TRACER_ADDOUTPUT(seqNum);
		TRACER_WRITE;
	}

#endif
	max=numeric_limits<uint64_t>::max();
	min=0;
	seqNum*=(-1);
	int entries;
	int i;
	LBID_t lastBlock;
	int isValid = CP_INVALID;

#ifdef BRM_DEBUG
	if (lbid< 0)
		throw invalid_argument("ExtentMap::getMaxMin(): lbid must be >= 0");
#endif

	grabEMEntryTable(READ);
	entries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	for (i = 0; i < entries; i++) {
		if (fExtentMap[i].range.size != 0) {
			lastBlock = fExtentMap[i].range.start + 
				(static_cast<LBID_t>(fExtentMap[i].range.size) * 1024) - 1;
			if (lbid >= fExtentMap[i].range.start && lbid <= lastBlock) {
				max = fExtentMap[i].partition.cprange.hi_val;
				min = fExtentMap[i].partition.cprange.lo_val;
				seqNum = fExtentMap[i].partition.cprange.sequenceNum;
				isValid = fExtentMap[i].partition.cprange.isValid;
				releaseEMEntryTable(READ);
				return isValid;
			}
		}
	}
	releaseEMEntryTable(READ);
	throw logic_error("ExtentMap::getMaxMin(): that lbid isn't allocated");
//   	return -1;
}

/* Removes a range from the freelist.  Used by load() */
void ExtentMap::reserveLBIDRange(LBID_t start, uint8_t size)
{
	int i;
	int flEntries = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);
	LBID_t lastLBID = start + (size * 1024) - 1;
	int32_t freeIndex = -1;

	/* Find a range the request intersects.  There should be one and only one. */
	for (i = 0; i < flEntries; i++) {
		LBID_t eLastLBID;

		// while scanning, grab the first free slot
		if (fFreeList[i].size == 0) {
			if (freeIndex == -1)
				freeIndex = i;
			continue;
		}

		eLastLBID = fFreeList[i].start + (((int64_t) fFreeList[i].size) * 1024) - 1;
		/* if it's at the front... */
		if (start == fFreeList[i].start) {
			/* if the request is larger than the freelist entry -> implies an extent
			 * overlap.  This is debugging code. */
			//idbassert(size > fFreeList[i].size);
			makeUndoRecord(&fFreeList[i], sizeof(InlineLBIDRange));
			fFreeList[i].start += size * 1024;
			fFreeList[i].size -= size;
			if (fFreeList[i].size == 0) {
				makeUndoRecord(fFLShminfo, sizeof(MSTEntry));
				fFLShminfo->currentSize -= sizeof(InlineLBIDRange);
			}
			break;
		}
		/* if it's at the back... */
		else if (eLastLBID == lastLBID) {
			makeUndoRecord(&fFreeList[i], sizeof(InlineLBIDRange));
			fFreeList[i].size -= size;
			if (fFreeList[i].size == 0) {
				makeUndoRecord(fFLShminfo, sizeof(MSTEntry));
				fFLShminfo->currentSize -= sizeof(InlineLBIDRange);
			}
			break;
			/* This entry won't be the same size as the request or the first
			 * clause would have run instead.
			 */
		}
		/* if it's in the middle... */
		/* break it into two elements */
		else if (fFreeList[i].start < start && eLastLBID > lastLBID) {
			if (freeIndex == -1) {
				if (fFLShminfo->currentSize == fFLShminfo->allocdSize) {
					growFLShmseg();
					freeIndex = flEntries;
				}
				else
					for (freeIndex = i+1; freeIndex < flEntries; freeIndex++)
						if (fFreeList[freeIndex].size == 0)
							break;
#ifdef BRM_DEBUG
				idbassert(nextIndex < flEntries);
#endif
			}
			makeUndoRecord(&fFreeList[i], sizeof(InlineLBIDRange));
			makeUndoRecord(&fFreeList[freeIndex], sizeof(InlineLBIDRange));
			makeUndoRecord(fFLShminfo, sizeof(MSTEntry));
			fFreeList[i].size = (start - fFreeList[i].start)/1024;
			fFreeList[freeIndex].start = start + (size * 1024);
			fFreeList[freeIndex].size = (eLastLBID - lastLBID)/1024;
			fFLShminfo->currentSize += sizeof(InlineLBIDRange);
			break;
		}
	}
}

/* 
	The file layout looks like this:

	  EM Magic (32-bits)
	  number of EM entries  (32-bits)
	  number of FL entries  (32-bits)
	  EMEntry
	    ...   (* numEM)
	  struct InlineLBIDRange
	    ...   (* numFL)
*/

void ExtentMap::loadVersion4(ifstream &in)
{
	int emNumElements, flNumElements;

	in.read((char *) &emNumElements, sizeof(int));
	in.read((char *) &flNumElements, sizeof(int));
	idbassert(emNumElements > 0);

	memset(fExtentMap, 0, fEMShminfo->allocdSize);
	fEMShminfo->currentSize = 0;

	// init the free list
	memset(fFreeList, 0, fFLShminfo->allocdSize);
	fFreeList[0].size = (1 << 26);   // 2^36 LBIDs
	fFLShminfo->currentSize = sizeof(InlineLBIDRange);

	// @Bug 3498
	// Calculate how big an extent map we're going to need and allocate it in one call
	if ((fEMShminfo->allocdSize / sizeof(EMEntry)) < (unsigned)emNumElements) {
		size_t nrows = emNumElements;
		//Round up to the nearest EM_INCREMENT_ROWS
		if ((nrows % EM_INCREMENT_ROWS) != 0) {
			nrows /= EM_INCREMENT_ROWS;
			nrows++;
			nrows *= EM_INCREMENT_ROWS;
		}
		growEMShmseg(nrows);
	}

	for (int i = 0; i < emNumElements; i++) {
		in.read((char *) &fExtentMap[i], sizeof(EMEntry));
		reserveLBIDRange(fExtentMap[i].range.start, fExtentMap[i].range.size);

		//@bug 1911 - verify status value is valid
		if (fExtentMap[i].status<EXTENTSTATUSMIN ||
			fExtentMap[i].status>EXTENTSTATUSMAX)
		  fExtentMap[i].status=EXTENTAVAILABLE;
	}

	fEMShminfo->currentSize = emNumElements * sizeof(EMEntry);

#ifdef DUMP_EXTENT_MAP
	EMEntry* emSrc = fExtentMap;
	cout << "lbid\tsz\toid\tfbo\thwm\tpart#\tseg#\tDBRoot\twid\tst\thi\tlo\tsq\tv" << endl;
	for (int i = 0; i < emNumElements; i++) {
		cout << 
		emSrc[i].start
		<< '\t' << emSrc[i].size
		<< '\t' << emSrc[i].fileID
		<< '\t' << emSrc[i].blockOffset
		<< '\t' << emSrc[i].HWM
		<< '\t' << emSrc[i].partitionNum
		<< '\t' << emSrc[i].segmentNum
		<< '\t' << emSrc[i].dbRoot
		<< '\t' << emSrc[i].status
		<< '\t' << emSrc[i].partition.cprange.hi_val
		<< '\t' << emSrc[i].partition.cprange.lo_val
		<< '\t' << emSrc[i].partition.cprange.sequenceNum
		<< '\t' << (int)(emSrc[i].partition.cprange.isValid)
		<< endl;
	}
	cout << "Free list entries:" << endl;
	cout << "start\tsize" << endl;
	for (int i = 0; i < flNumElements; i++)
		cout << fFreeList[i].start << '\t' << fFreeList[i].size << endl;
#endif
}

void ExtentMap::loadVersion4(IDBDataFile* in)
{
	int emNumElements = 0, flNumElements = 0;

	int nbytes = 0;
	nbytes += in->read((char *) &emNumElements, sizeof(int));
	nbytes += in->read((char *) &flNumElements, sizeof(int));
	idbassert(emNumElements > 0);
	if ((size_t) nbytes != sizeof(int) + sizeof(int)) {
		log_errno("ExtentMap::loadVersion4(): read ");
		throw runtime_error("ExtentMap::loadVersion4(): read failed. Check the error log.");
	}

	memset(fExtentMap, 0, fEMShminfo->allocdSize);
	fEMShminfo->currentSize = 0;

	// init the free list
	memset(fFreeList, 0, fFLShminfo->allocdSize);
	fFreeList[0].size = (1 << 26);   // 2^36 LBIDs
	fFLShminfo->currentSize = sizeof(InlineLBIDRange);

	// @Bug 3498
	// Calculate how big an extent map we're going to need and allocate it in one call
	if ((fEMShminfo->allocdSize / sizeof(EMEntry)) < (unsigned)emNumElements) {
		size_t nrows = emNumElements;
		//Round up to the nearest EM_INCREMENT_ROWS
		if ((nrows % EM_INCREMENT_ROWS) != 0) {
			nrows /= EM_INCREMENT_ROWS;
			nrows++;
			nrows *= EM_INCREMENT_ROWS;
		}
		growEMShmseg(nrows);
	}

	for (int i = 0; i < emNumElements; i++) {
		if (in->read((char *) &fExtentMap[i], sizeof(EMEntry)) != sizeof(EMEntry)) {
			log_errno("ExtentMap::loadVersion4(): read ");
			throw runtime_error("ExtentMap::loadVersion4(): read failed. Check the error log.");
		}
		reserveLBIDRange(fExtentMap[i].range.start, fExtentMap[i].range.size);

		//@bug 1911 - verify status value is valid
		if (fExtentMap[i].status<EXTENTSTATUSMIN ||
			fExtentMap[i].status>EXTENTSTATUSMAX)
		  fExtentMap[i].status=EXTENTAVAILABLE;
	}

	fEMShminfo->currentSize = emNumElements * sizeof(EMEntry);

#ifdef DUMP_EXTENT_MAP
	EMEntry* emSrc = fExtentMap;
	cout << "lbid\tsz\toid\tfbo\thwm\tpart#\tseg#\tDBRoot\twid\tst\thi\tlo\tsq\tv" << endl;
	for (int i = 0; i < emNumElements; i++) {
		cout << 
		emSrc[i].start
		<< '\t' << emSrc[i].size
		<< '\t' << emSrc[i].fileID
		<< '\t' << emSrc[i].blockOffset
		<< '\t' << emSrc[i].HWM
		<< '\t' << emSrc[i].partitionNum
		<< '\t' << emSrc[i].segmentNum
		<< '\t' << emSrc[i].dbRoot
		<< '\t' << emSrc[i].status
		<< '\t' << emSrc[i].partition.cprange.hi_val
		<< '\t' << emSrc[i].partition.cprange.lo_val
		<< '\t' << emSrc[i].partition.cprange.sequenceNum
		<< '\t' << (int)(emSrc[i].partition.cprange.isValid)
		<< endl;
	}
	cout << "Free list entries:" << endl;
	cout << "start\tsize" << endl;
	for (int i = 0; i < flNumElements; i++)
		cout << fFreeList[i].start << '\t' << fFreeList[i].size << endl;
#endif
}

void ExtentMap::load(const string& filename, bool fixFL)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("load");
		TRACER_ADDSTRINPUT(filename);
		TRACER_WRITE;
	}
#endif

	grabEMEntryTable(WRITE);
	try {
		grabFreeList(WRITE);
	}
	catch(...) {
		releaseEMEntryTable(WRITE);
		throw;
	}

	if (IDBPolicy::useHdfs()) {
		const char* filename_p = filename.c_str();
		scoped_ptr<IDBDataFile>  in(IDBDataFile::open(
									IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
									filename_p, "r", 0));
		if (!in) {
			log_errno("ExtentMap::load(): open");
			releaseFreeList(WRITE);
			releaseEMEntryTable(WRITE);
			throw ios_base::failure("ExtentMap::load(): open failed. Check the error log.");
		}
		
		try {
			int emVersion = 0;
			int bytes = in->read((char *) &emVersion, sizeof(int));
			if (bytes == (int) sizeof(int) && emVersion == EM_MAGIC_V4)
				loadVersion4(in.get());
			else {
				log("ExtentMap::load(): That file is not a valid ExtentMap image");
				throw runtime_error("ExtentMap::load(): That file is not a valid ExtentMap image");
			}
		}
		catch(...) {
			releaseFreeList(WRITE);
			releaseEMEntryTable(WRITE);
			throw;
		}
	}
	else {
		ifstream in;
		in.open(filename.c_str(), ios_base::in|ios_base::binary);
		if (!in) {
			log_errno("ExtentMap::load(): open");
			releaseFreeList(WRITE);
			releaseEMEntryTable(WRITE);
			throw ios_base::failure("ExtentMap::load(): open failed. Check the error log.");
		}

		in.exceptions(ios_base::badbit | ios_base::failbit);

		try {
			int emVersion;
			in.read((char *) &emVersion, sizeof(int));

			if (emVersion == EM_MAGIC_V4)
				loadVersion4(in);
			else {
				log("ExtentMap::load(): That file is not a valid ExtentMap image");
				throw runtime_error("ExtentMap::load(): That file is not a valid ExtentMap image");
			}
		}
		catch(...) {
			in.close();
			releaseFreeList(WRITE);
			releaseEMEntryTable(WRITE);
			throw;
		}

		in.close();
	}

	releaseFreeList(WRITE);
	releaseEMEntryTable(WRITE);
//	checkConsistency();
}

void ExtentMap::save(const string& filename)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("save");
		TRACER_ADDSTRINPUT(filename);
		TRACER_WRITE;
	}
#endif

	int allocdSize, loadSize[3], i;
	mode_t utmp;

	grabEMEntryTable(READ);
	try {
		grabFreeList(READ);
	}
	catch(...) {
		releaseEMEntryTable(READ);
		throw;
	}

	if (fEMShminfo->currentSize == 0) {
		log("ExtentMap::save(): got request to save an empty BRM");
		releaseFreeList(READ);
		releaseEMEntryTable(READ);
		throw runtime_error("ExtentMap::save(): got request to save an empty BRM");
	}

	if (IDBPolicy::useHdfs()) {
		utmp = ::umask(0);
		const char* filename_p = filename.c_str();
		scoped_ptr<IDBDataFile> out(IDBDataFile::open(
									IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
									filename_p, "wb", IDBDataFile::USE_VBUF));
		::umask(utmp);
		if (!out) {
			log_errno("ExtentMap::save(): open");
			releaseFreeList(READ);
			releaseEMEntryTable(READ);
			throw ios_base::failure("ExtentMap::save(): open failed. Check the error log.");
		}

		loadSize[0] = EM_MAGIC_V4;
		loadSize[1] = fEMShminfo->currentSize/sizeof(EMEntry);
		loadSize[2] = fFLShminfo->allocdSize/sizeof(InlineLBIDRange); // needs to send all entries

		int bytes = 0;
		try {
			const int wsize = 3*sizeof(int);
			bytes = out->write((char *)loadSize, wsize);
			if (bytes != wsize)
				throw ios_base::failure("ExtentMap::save(): write failed. Check the error log.");
		}
		catch(...) {
			releaseFreeList(READ);
			releaseEMEntryTable(READ);
			throw;
		}

		allocdSize = fEMShminfo->allocdSize/sizeof(EMEntry);
		const int emEntrySize = sizeof(EMEntry);
		for (i = 0; i < allocdSize; i++) {
			if (fExtentMap[i].range.size > 0) {
				try {
					bytes = out->write((char *) &fExtentMap[i], emEntrySize);
					if (bytes != emEntrySize)
						throw ios_base::failure("ExtentMap::save(): write failed. Check the error log.");
				}
				catch(...) {
					releaseFreeList(READ);
					releaseEMEntryTable(READ);
					throw;
				}
			}
		}

		allocdSize = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);
		const int inlineLbidRangeSize = sizeof(InlineLBIDRange);
		for (i = 0; i < allocdSize; i++) {
//			if (fFreeList[i].size > 0) {
				try {
					int bytes = out->write((char *) &fFreeList[i], inlineLbidRangeSize);
					if (bytes != inlineLbidRangeSize)
						throw ios_base::failure("ExtentMap::save(): write failed. Check the error log.");
				}
				catch(...) {
					releaseFreeList(READ);
					releaseEMEntryTable(READ);
					throw;
				}
//			}
		}
	}
	else {
		ofstream out;

		// Make em writes to disk use a buffer size of StrmBufSize bytes (instead of the default 8K)
		const unsigned StrmBufSize = 1 * 1024 * 1024;
		scoped_array<char> buf(new char[StrmBufSize]);
		out.rdbuf()->pubsetbuf(buf.get(), StrmBufSize);

		utmp = ::umask(0);
		out.open(filename.c_str(), ios_base::out|ios_base::binary);
		::umask(utmp);
		if (!out) {
			log_errno("ExtentMap::save(): open");
			releaseFreeList(READ);
			releaseEMEntryTable(READ);
			throw ios_base::failure("ExtentMap::save(): open failed. Check the error log.");
		}

		out.exceptions(ios_base::badbit);

		loadSize[0] = EM_MAGIC_V4;
		loadSize[1] = fEMShminfo->currentSize/sizeof(EMEntry);
		loadSize[2] = fFLShminfo->allocdSize/sizeof(InlineLBIDRange); // needs to send all entries

		try {
			out.write((char *)loadSize, 3*sizeof(int));
		}
		catch(...) {
			out.close();
			releaseFreeList(READ);
			releaseEMEntryTable(READ);
			throw;
		}

		allocdSize = fEMShminfo->allocdSize/sizeof(EMEntry);
		for (i = 0; i < allocdSize; i++) {
			if (fExtentMap[i].range.size > 0) {
				try {
					out.write((char *) &fExtentMap[i], sizeof(EMEntry));
				}
				catch(...) {
					out.close();
					releaseFreeList(READ);
					releaseEMEntryTable(READ);
					throw;
				}
			}
		}

		allocdSize = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);
		for (i = 0; i < allocdSize; i++) {
//			if (fFreeList[i].size > 0) {
				try {
					out.write((char *) &fFreeList[i], sizeof(InlineLBIDRange));
				}
				catch(...) {
					out.close();
					releaseFreeList(READ);
					releaseEMEntryTable(READ);
					throw;
				}
//			}
		}
		out.close();
	}

	releaseFreeList(READ);
	releaseEMEntryTable(READ);
}

/* always returns holding the EM lock, and with the EM seg mapped */
void ExtentMap::grabEMEntryTable(OPS op)
{
	mutex::scoped_lock lk(mutex);

	if (op == READ)
		fEMShminfo = fMST.getTable_read(MasterSegmentTable::EMTable);
	else { 
		fEMShminfo = fMST.getTable_write(MasterSegmentTable::EMTable);
		emLocked = true;
	}

	if (!fPExtMapImpl || fPExtMapImpl->key() != (unsigned)fEMShminfo->tableShmkey) {
		if (fExtentMap != NULL) {
			fExtentMap = NULL;
		}
		if (fEMShminfo->allocdSize == 0)
		{
			if (op == READ) {
				fMST.getTable_upgrade(MasterSegmentTable::EMTable);
				emLocked = true;
				if (fEMShminfo->allocdSize == 0)
					growEMShmseg();
				emLocked = false;	// has to be done holding the write lock
				fMST.getTable_downgrade(MasterSegmentTable::EMTable);
			}
			else
				growEMShmseg();
		}
		else {
			fPExtMapImpl = ExtentMapImpl::makeExtentMapImpl(fEMShminfo->tableShmkey, 0);
			ASSERT(fPExtMapImpl);
			if (r_only)
				fPExtMapImpl->makeReadOnly();

			fExtentMap = fPExtMapImpl->get();
			if (fExtentMap == NULL) {
				log_errno("ExtentMap::grabEMEntryTable(): shmat");
				throw runtime_error("ExtentMap::grabEMEntryTable(): shmat failed.  Check the error log.");
			}
		}
	}
	else 
		fExtentMap = fPExtMapImpl->get();
}

/* always returns holding the FL lock */
void ExtentMap::grabFreeList(OPS op)
{
	mutex::scoped_lock lk(mutex, defer_lock);

	if (op == READ) {
		fFLShminfo = fMST.getTable_read(MasterSegmentTable::EMFreeList);
 		lk.lock();
	}
	else {
		fFLShminfo = fMST.getTable_write(MasterSegmentTable::EMFreeList);
		flLocked = true;
	}

	if (!fPFreeListImpl || fPFreeListImpl->key() != (unsigned)fFLShminfo->tableShmkey) {
		if (fFreeList != NULL) {
			fFreeList = NULL;
		}
		if (fFLShminfo->allocdSize == 0)
		{
			if (op == READ) {
 				lk.unlock();
				fMST.getTable_upgrade(MasterSegmentTable::EMFreeList);
				flLocked = true;
				if (fFLShminfo->allocdSize == 0)
					growFLShmseg();
				flLocked = false;		// has to be done holding the write lock
				fMST.getTable_downgrade(MasterSegmentTable::EMFreeList);
			}
			else
				growFLShmseg();
		}
		else {
			fPFreeListImpl = FreeListImpl::makeFreeListImpl(fFLShminfo->tableShmkey, 0);
			ASSERT(fPFreeListImpl);
			if (r_only)
				fPFreeListImpl->makeReadOnly();

			fFreeList = fPFreeListImpl->get();
			if (fFreeList == NULL) {
				log_errno("ExtentMap::grabFreeList(): shmat");
				throw runtime_error("ExtentMap::grabFreeList(): shmat failed.  Check the error log.");
			}
		 	if (op == READ)
		 		lk.unlock();
		}
	}
	else 
	{
		fFreeList = fPFreeListImpl->get();
		if (op == READ)
 			lk.unlock();
	}
}

void ExtentMap::releaseEMEntryTable(OPS op)
{
	if (op == READ)
		fMST.releaseTable_read(MasterSegmentTable::EMTable);
	else {
		/* 
		   Note: Technically we should mark it unlocked after it's unlocked,
		   however, that's a race condition.  The only reason the up operation
		   here will fail is if the underlying semaphore doesn't exist anymore
		   or there is a locking logic error somewhere else.  Either way,
		   declaring the EM unlocked here is OK.  Same with all similar assignments.
		 */
		emLocked = false;
		fMST.releaseTable_write(MasterSegmentTable::EMTable);
	}
}

void ExtentMap::releaseFreeList(OPS op)
{
	if (op == READ)
		fMST.releaseTable_read(MasterSegmentTable::EMFreeList);
	else {
		flLocked = false;
		fMST.releaseTable_write(MasterSegmentTable::EMFreeList);
	}
}

key_t ExtentMap::chooseEMShmkey()
{
	int fixedKeys = 1;
	key_t ret;

	if (fEMShminfo->tableShmkey + 1 == (key_t) (fShmKeys.KEYRANGE_EXTENTMAP_BASE + 
		   fShmKeys.KEYRANGE_SIZE - 1) || (unsigned)fEMShminfo->tableShmkey < fShmKeys.KEYRANGE_EXTENTMAP_BASE)
		ret = fShmKeys.KEYRANGE_EXTENTMAP_BASE + fixedKeys;
	else
		ret = fEMShminfo->tableShmkey + 1;

	return ret;
}

key_t ExtentMap::chooseFLShmkey()
{
	int fixedKeys = 1, ret;

	if (fFLShminfo->tableShmkey + 1 == (key_t) (fShmKeys.KEYRANGE_EMFREELIST_BASE + 
		   fShmKeys.KEYRANGE_SIZE - 1) || (unsigned)fFLShminfo->tableShmkey < fShmKeys.KEYRANGE_EMFREELIST_BASE)
		ret = fShmKeys.KEYRANGE_EMFREELIST_BASE + fixedKeys;
	else 
		ret = fFLShminfo->tableShmkey + 1;

	return ret;
}

/* Must be called holding the EM write lock 
   Returns with the new shmseg mapped */
void ExtentMap::growEMShmseg(size_t nrows)
{
	size_t allocSize;
	key_t newshmkey;

	if (fEMShminfo->allocdSize == 0)
		allocSize = EM_INITIAL_SIZE;
	else
		allocSize = fEMShminfo->allocdSize + EM_INCREMENT;

	newshmkey = chooseEMShmkey();
	ASSERT((allocSize == EM_INITIAL_SIZE && !fPExtMapImpl) || fPExtMapImpl);

	//Use the larger of the calculated value or the specified value
	allocSize = max(allocSize, nrows * sizeof(EMEntry));

	if (!fPExtMapImpl)
	{
		fPExtMapImpl = ExtentMapImpl::makeExtentMapImpl(newshmkey, allocSize, r_only);
	}
	else
	{
		fPExtMapImpl->grow(newshmkey, allocSize);
	}
	fEMShminfo->tableShmkey = newshmkey;
	fEMShminfo->allocdSize = allocSize;
	if (r_only)
		fPExtMapImpl->makeReadOnly();

	fExtentMap = fPExtMapImpl->get();
}

/* Must be called holding the FL lock 
   Returns with the new shmseg mapped */
void ExtentMap::growFLShmseg()
{
	size_t allocSize;
	key_t newshmkey;

	if (fFLShminfo->allocdSize == 0)
		allocSize = EM_FREELIST_INITIAL_SIZE;
	else
		allocSize = fFLShminfo->allocdSize + EM_FREELIST_INCREMENT;

	newshmkey = chooseFLShmkey();
	ASSERT((allocSize == EM_FREELIST_INITIAL_SIZE && !fPFreeListImpl) || fPFreeListImpl);

	if (!fPFreeListImpl)
		fPFreeListImpl = FreeListImpl::makeFreeListImpl(newshmkey, allocSize, false);
	else
		fPFreeListImpl->grow(newshmkey, allocSize);
	fFLShminfo->tableShmkey = newshmkey;
	fFreeList = fPFreeListImpl->get();
	// init freelist entry
	if (fFLShminfo->allocdSize == 0) {
 		fFreeList->size = (1ULL << 36) / 1024;
		fFLShminfo->currentSize = sizeof(InlineLBIDRange);
	}
	fFLShminfo->allocdSize = allocSize;
	if (r_only)
		fPFreeListImpl->makeReadOnly();

	fFreeList = fPFreeListImpl->get();
}

// @bug 1509.  Added new version of lookup that returns the first and last lbid for the extent that contains the 
// given lbid.
int ExtentMap::lookup(LBID_t lbid, LBID_t& firstLbid, LBID_t& lastLbid) 
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("lookup");
		TRACER_ADDINPUT(lbid);
		TRACER_ADDOUTPUT(firstLbid);
		TRACER_ADDOUTPUT(lastLbid);
		TRACER_WRITE;
	}

#endif
	int entries, i;
	LBID_t lastBlock;

#ifdef BRM_DEBUG
//printEM();
	if (lbid < 0) {
		log("ExtentMap::lookup(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
		cout << "ExtentMap::lookup(): lbid must be >= 0.  Lbid passed was " << lbid << endl;
		throw invalid_argument("ExtentMap::lookup(): lbid must be >= 0");
	}
#endif

	grabEMEntryTable(READ);
	entries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (i = 0; i < entries; i++) {
		if (fExtentMap[i].range.size != 0) {
			lastBlock = fExtentMap[i].range.start + 
				(static_cast<LBID_t>(fExtentMap[i].range.size) * 1024) - 1;
			if (lbid >= fExtentMap[i].range.start && lbid <= lastBlock) {
				firstLbid = fExtentMap[i].range.start;
				lastLbid = lastBlock;
				releaseEMEntryTable(READ);
				return 0;
			}
		}
	}
	releaseEMEntryTable(READ);
	return -1;
}

// @bug 1055+.  New functions added for multiple files per OID enhancement.
int ExtentMap::lookupLocal(LBID_t lbid, int& OID, uint16_t& dbRoot, uint32_t& partitionNum, uint16_t& segmentNum, uint32_t& fileBlockOffset)
{
#ifdef BRM_INFO
        if (fDebug)
        {
                TRACER_WRITELATER("lookupLocal");
                TRACER_ADDINPUT(lbid);
                TRACER_ADDOUTPUT(OID);
                TRACER_ADDSHORTOUTPUT(dbRoot);
                TRACER_ADDOUTPUT(partitionNum);
                TRACER_ADDSHORTOUTPUT(segmentNum);
                TRACER_ADDOUTPUT(fileBlockOffset);
                TRACER_WRITE;
        }

#endif
#ifdef EM_AS_A_TABLE_POC__
	if (lbid >= (1LL << 54))
	{
		OID = 1084;
		dbRoot = 1;
		partitionNum = 0;
		segmentNum = 0;
		fileBlockOffset = 0;
		return 0;
	}
#endif
        int entries, i, offset;
        LBID_t lastBlock;

        if (lbid < 0) {
		ostringstream oss;
		oss << "ExtentMap::lookupLocal(): invalid lbid requested: " << lbid;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
        }

        grabEMEntryTable(READ);

        entries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
        for (i = 0; i < entries; i++) {
                if (fExtentMap[i].range.size != 0) {
                        lastBlock = fExtentMap[i].range.start +
                                (static_cast<LBID_t>(fExtentMap[i].range.size) * 1024) - 1;
                        if (lbid >= fExtentMap[i].range.start && lbid <= lastBlock) {
                                OID = fExtentMap[i].fileID;
				dbRoot = fExtentMap[i].dbRoot;
				segmentNum = fExtentMap[i].segmentNum;
				partitionNum = fExtentMap[i].partitionNum;

				// TODO:  Offset logic.
                                offset = lbid - fExtentMap[i].range.start;
                                fileBlockOffset = fExtentMap[i].blockOffset + offset;

                                releaseEMEntryTable(READ);
                                return 0;
                        }
                }
        }
        releaseEMEntryTable(READ);
        return -1;
}

int ExtentMap::lookupLocal(int OID, uint32_t partitionNum, uint16_t segmentNum, uint32_t fileBlockOffset, LBID_t& LBID)
{
#ifdef BRM_INFO
        if (fDebug)
        {
                TRACER_WRITELATER("lookupLocal");
                TRACER_ADDINPUT(OID);
                TRACER_ADDINPUT(partitionNum);
                TRACER_ADDSHORTINPUT(segmentNum);
                TRACER_ADDINPUT(fileBlockOffset);
                TRACER_ADDOUTPUT(LBID);
                TRACER_WRITE;
        }

#endif
        int entries, i, offset;

        if (OID < 0 || fileBlockOffset < 0) {
                log("ExtentMap::lookup(): OID and FBO must be >= 0", logging::LOG_TYPE_DEBUG);
                throw invalid_argument("ExtentMap::lookup(): OID and FBO must be >= 0");
        }

        grabEMEntryTable(READ);

        entries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
        for (i = 0; i < entries; i++) {

		// TODO:  Blockoffset logic.
                if (fExtentMap[i].range.size != 0 &&
                        fExtentMap[i].fileID == OID &&
			fExtentMap[i].partitionNum == partitionNum &&
			fExtentMap[i].segmentNum == segmentNum &&
                        fExtentMap[i].blockOffset <= fileBlockOffset &&
                        fileBlockOffset <= (fExtentMap[i].blockOffset +
                        (static_cast<LBID_t>(fExtentMap[i].range.size) * 1024) - 1)) 
		{

                        offset = fileBlockOffset - fExtentMap[i].blockOffset;
                        LBID = fExtentMap[i].range.start + offset;
                        releaseEMEntryTable(READ);
                        return 0;
                }
        }
        releaseEMEntryTable(READ);
        return -1;
}

int ExtentMap::lookupLocal_DBroot(int OID, uint16_t dbroot, uint32_t partitionNum, uint16_t segmentNum,
		uint32_t fileBlockOffset, LBID_t& LBID)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("lookupLocal");
		TRACER_ADDINPUT(OID);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDINPUT(fileBlockOffset);
		TRACER_ADDOUTPUT(LBID);
		TRACER_WRITE;
	}

#endif
	int entries, i, offset;

	if (OID < 0 || fileBlockOffset < 0) {
		log("ExtentMap::lookup(): OID and FBO must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("ExtentMap::lookup(): OID and FBO must be >= 0");
	}

	grabEMEntryTable(READ);

	entries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (i = 0; i < entries; i++) {

		// TODO:  Blockoffset logic.
		if (fExtentMap[i].range.size != 0 &&
				fExtentMap[i].fileID == OID &&
				fExtentMap[i].dbRoot == dbroot &&
				fExtentMap[i].partitionNum == partitionNum &&
				fExtentMap[i].segmentNum == segmentNum &&
				fExtentMap[i].blockOffset <= fileBlockOffset &&
				fileBlockOffset <= (fExtentMap[i].blockOffset +
						(static_cast<LBID_t>(fExtentMap[i].range.size) * 1024) - 1))
		{

			offset = fileBlockOffset - fExtentMap[i].blockOffset;
			LBID = fExtentMap[i].range.start + offset;
			releaseEMEntryTable(READ);
			return 0;
		}
	}
	releaseEMEntryTable(READ);
	return -1;
}

// @bug 1055-.

//------------------------------------------------------------------------------
// Lookup/return starting LBID for the specified OID, partition, segment, and
// file block offset.
//------------------------------------------------------------------------------
int ExtentMap::lookupLocalStartLbid(int      OID,
                                    uint32_t partitionNum,
                                    uint16_t segmentNum,
                                    uint32_t fileBlockOffset,
                                    LBID_t&  LBID)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("lookupLocalStartLbid");
		TRACER_ADDINPUT(OID);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDINPUT(fileBlockOffset);
		TRACER_ADDOUTPUT(LBID);
		TRACER_WRITE;
	}
#endif
	int entries, i;

	if (OID < 0 || fileBlockOffset < 0) {
		log("ExtentMap::lookupLocalStartLbid(): OID and FBO must be >= 0",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument("ExtentMap::lookupLocalStartLbid(): "
			"OID and FBO must be >= 0");
	}

	grabEMEntryTable(READ);
	entries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (i = 0; i < entries; i++) {
		if (fExtentMap[i].range.size   != 0 &&
			fExtentMap[i].fileID       == OID &&
			fExtentMap[i].partitionNum == partitionNum &&
			fExtentMap[i].segmentNum   == segmentNum &&
			fExtentMap[i].blockOffset  <= fileBlockOffset &&
			fileBlockOffset <= (fExtentMap[i].blockOffset +
			(static_cast<LBID_t>(fExtentMap[i].range.size) * 1024) - 1)) 
		{
			LBID = fExtentMap[i].range.start;
			releaseEMEntryTable(READ);
			return 0;
		}
	}
	releaseEMEntryTable(READ);

	return -1;
}

//------------------------------------------------------------------------------
// Creates a "stripe" of column extents across a table, for the specified
// columns and DBRoot.
//   cols         - Vector of columns OIDs and widths to be allocated
//   dbRoot       - DBRoot to be used for new extents
//   partitionNum - when creating the first extent for a column (on dbRoot),
//                  partitionNum must be specified as an input argument.
//                  If not the first extent on dbRoot, then partitionNum
//                  for the new extents will be assigned and returned, based
//                  on the current last extent for dbRoot.
// output:
//   partitionNum - Partition number for new extents
//   segmentNum   - Segment number for new exents
//   extents      - starting Lbid, numBlocks, and FBO for new extents
//------------------------------------------------------------------------------
void ExtentMap::createStripeColumnExtents(
	const vector<CreateStripeColumnExtentsArgIn>& cols,
	uint16_t  dbRoot,
	uint32_t& partitionNum,
	uint16_t& segmentNum,
    vector<CreateStripeColumnExtentsArgOut>& extents)
{
	LBID_t    startLbid;
	int       allocSize;
	uint32_t startBlkOffset;

	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);

	OID_t     baselineOID=-1;
	uint16_t baselineSegmentNum=-1;
	uint32_t baselinePartNum=-1;

	for (uint32_t i = 0; i < cols.size(); i++) {
		createColumnExtent_DBroot(
			cols[i].oid, 
			cols[i].width,
			dbRoot,
            cols[i].colDataType,
			partitionNum,
			segmentNum,
			startLbid,
			allocSize,
			startBlkOffset,
			false);
		if (i == 0) {
			baselineOID        = cols[i].oid;
			baselineSegmentNum = segmentNum;
			baselinePartNum    = partitionNum;
		}
		else {
			if ((segmentNum   != baselineSegmentNum) ||
				(partitionNum != baselinePartNum)) {
				ostringstream oss;
				oss << "ExtentMap::createStripeColumnExtents(): "
					"Inconsistent segment extent creation: " <<
					"DBRoot: "         << dbRoot <<
					"OID1: "           << baselineOID <<
					"; Part#: "        << baselinePartNum <<
					"; Seg#: "         << baselineSegmentNum <<
					" <versus> OID2: " << cols[i].oid <<
					"; Part#: "        << partitionNum <<
					"; Seg#: "         << segmentNum;
				log(oss.str(), logging::LOG_TYPE_CRITICAL);
				throw invalid_argument(oss.str());
			}
		}
		CreateStripeColumnExtentsArgOut extentInfo;
		extentInfo.startLbid      = startLbid;
		extentInfo.allocSize      = allocSize;
		extentInfo.startBlkOffset = startBlkOffset;
		extents.push_back( extentInfo );
	}
}

//------------------------------------------------------------------------------
// Creates an extent for a column file on the specified DBRoot.  This is the
// external API function referenced by the dbrm wrapper class.
// required input:
//   OID          - column OID for which the extent is to be created
//   colWidth     - width of column in bytes
//   dbRoot       - DBRoot where extent is to be added
//   partitionNum - when creating the first extent for a column (on dbRoot),
//                  partitionNum must be specified as an input argument.
//                  If not the first extent on dbRoot, then partitionNum
//                  for the new extent will be assigned and returned, based
//                  on the current last extent for dbRoot.
//   useLock      - Grab ExtentMap and FreeList WRITE lock to perform work
// output:
//   partitionNum - partition number for the new extent
//   segmentNum   - segment number for the new extent
//   lbid         - starting LBID of the created extent
//   allocdsize   - number of LBIDs allocated 
//   startBlockOffset-starting block of the created extent
//------------------------------------------------------------------------------
void ExtentMap::createColumnExtent_DBroot(int OID,
	uint32_t  colWidth,
	uint16_t  dbRoot,
    execplan::CalpontSystemCatalog::ColDataType colDataType,
	uint32_t& partitionNum,
	uint16_t& segmentNum,
	LBID_t&    lbid,
	int&       allocdsize,
	uint32_t& startBlockOffset,
	bool       useLock) // defaults to true
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("createColumnExtent_DBroot");
		TRACER_ADDINPUT(OID);
		TRACER_ADDINPUT(colWidth);
		TRACER_ADDSHORTINPUT(dbRoot);
		TRACER_ADDOUTPUT(partitionNum);
		TRACER_ADDSHORTOUTPUT(segmentNum);
		TRACER_ADDINT64OUTPUT(lbid);
		TRACER_ADDOUTPUT(allocdsize);
		TRACER_ADDOUTPUT(startBlockOffset);
		TRACER_WRITE;
	}
#endif

#ifdef BRM_DEBUG
	if (OID <= 0) {
		log("ExtentMap::createColumnExtent_DBroot(): OID must be > 0",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument(
			"ExtentMap::createColumnExtent_DBroot(): OID must be > 0");
	}
#endif

	// Convert extent size in rows to extent size in 8192-byte blocks.
	// extentRows should be multiple of blocksize (8192).
	const unsigned EXTENT_SIZE = (getExtentRows() * colWidth) / BLOCK_SIZE;

	if (useLock) {
		grabEMEntryTable(WRITE);
		grabFreeList(WRITE);
	}

	if (fEMShminfo->currentSize == fEMShminfo->allocdSize)
		growEMShmseg();

//  size is the number of multiples of 1024 blocks.
//  ex: size=1 --> 1024 blocks
//      size=2 --> 2048 blocks
//      size=3 --> 3072 blocks, etc.
	uint32_t size = EXTENT_SIZE/1024;

	lbid = _createColumnExtent_DBroot(size, OID, colWidth,
		dbRoot, colDataType, partitionNum, segmentNum, startBlockOffset);

	allocdsize = EXTENT_SIZE;
}

//------------------------------------------------------------------------------
// Creates an extent for a column file for the specified DBRoot.  This is the
// internal implementation function.
// input:
//   size         - number of multiples of 1024 blocks allocated to the extent
//                  ex: size=1 --> 1024 blocks
//                      size=2 --> 2048 blocks
//                      size=3 --> 3072 blocks, etc.
//   OID          - column OID for which the extent is to be created
//   colWidth     - width of column in bytes
//   dbRoot       - dbRoot where extent is to be added
//   partitionNum - when creating the first extent for an empty dbRoot,
//                  partitionNum must be specified as an input argument.
// output:
//   partitionNum - when adding an extent to a dbRoot,
//                  partitionNum will be the assigned partition number
//   segmentNum   - segment number for the new extent
//   startBlockOffset-starting block of the created extent
// returns starting LBID of the created extent.
//------------------------------------------------------------------------------
LBID_t ExtentMap::_createColumnExtent_DBroot(uint32_t size, int OID,
	uint32_t  colWidth,
	uint16_t  dbRoot,
    execplan::CalpontSystemCatalog::ColDataType colDataType,
	uint32_t& partitionNum,
	uint16_t& segmentNum,
	uint32_t& startBlockOffset)
{
	int emptyEMEntry        = -1;
	int lastExtentIndex     = -1;
	uint32_t highestOffset = 0;
	uint32_t highestPartNum= 0;
	uint16_t highestSegNum = 0;
	const unsigned FILES_PER_COL_PART = getFilesPerColumnPartition();
	const unsigned EXTENT_ROWS        = getExtentRows();
	const unsigned EXTENTS_PER_SEGFILE= getExtentsPerSegmentFile();
	const unsigned DBROOT_COUNT       = getDbRootCount();

	// Variables that track list of segfiles in target (HWM) DBRoot & partition.
	// Map segment number to the highest fbo extent in each file
	typedef tr1::unordered_map<uint16_t,uint32_t> TargetDbRootSegsMap;
	typedef TargetDbRootSegsMap::iterator          TargetDbRootSegsMapIter;
	typedef TargetDbRootSegsMap::const_iterator    TargetDbRootSegsMapConstIter;
	TargetDbRootSegsMap targetDbRootSegs;

	uint32_t highEmptySegNum = 0; // high seg num for user specified partition;
	                               // only comes into play for empty DBRoot.
	bool bHighEmptySegNumSet = false;

	//--------------------------------------------------------------------------
	// First Step: Scan ExtentMap
	// 1. find HWM extent in relevant DBRoot
	// 2. if DBRoot is empty, track highest seg num in user specified partition
	// 3. Find first unused extent map entry
	//--------------------------------------------------------------------------
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	LBID_t startLBID = getLBIDsFromFreeList( size );

	// Find the first empty Entry; and find last extent for this OID and dbRoot
	for (int i = 0; i < emEntries; i++) {
		if (fExtentMap[i].range.size  != 0) {
			if (fExtentMap[i].fileID == OID) {

				// 1. Find HWM extent in relevant DBRoot
				if (fExtentMap[i].dbRoot == dbRoot) {
					if ( (fExtentMap[i].partitionNum >  highestPartNum) ||
						((fExtentMap[i].partitionNum == highestPartNum) &&
					 	(fExtentMap[i].blockOffset   >  highestOffset)) ||
						((fExtentMap[i].partitionNum == highestPartNum) &&
					 	(fExtentMap[i].blockOffset   == highestOffset)  &&
					 	(fExtentMap[i].segmentNum    >= highestSegNum)) ) {

						lastExtentIndex = i;
						highestPartNum  = fExtentMap[i].partitionNum;
						highestSegNum   = fExtentMap[i].segmentNum;
						highestOffset   = fExtentMap[i].blockOffset;
					}
				}

				// 2. for empty DBRoot track hi seg# in user specified part#
				if ((lastExtentIndex == -1) &&
					(fExtentMap[i].partitionNum == partitionNum)) {
					if ((fExtentMap[i].segmentNum > highEmptySegNum) ||
						(!bHighEmptySegNumSet)) {
						highEmptySegNum = fExtentMap[i].segmentNum;
						bHighEmptySegNumSet = true;
					}
				}
			}         // found extentmap entry for specified OID
		}             // found valid extentmap entry

		// 3. Find first available extent map entry that can be reused
		else if (emptyEMEntry < 0)
			emptyEMEntry = i;
	} // Loop through extent map entries

	if (emptyEMEntry == -1) {
		ostringstream oss;
		oss << "ExtentMap::_createColumnExtent_DBroot(): "
			"could not find an empty EMEntry for OID " << OID <<
			"; Extent Map is full",
		log(oss.str(),
			logging::LOG_TYPE_CRITICAL);
		throw logic_error( oss.str() );
	}

	//--------------------------------------------------------------------------
	// If DBRoot is not empty, then...
	// Second Step: Scan ExtentMap again after I know the last partition
	// 4. track highest seg num for HWM+1 partition
	// 5. track highest seg num for HWM    partition
	// 6. save list of segment numbers and fbos in target DBRoot and partition
	//
	// Scanning the extentmap a second time is not a good thing to be doing.
	// But the alternative isn't good either.  There is certain information
	// I need to capture about the last partition and DBRoot, and for the next
	// partition as well (which may contain segment files on other DBRoots),
	// but until I scan the extentmap, I don't know what my last partition is.
	// If I try to do this in a single scan, then I am forced to spend time
	// capturing information about partitions that turn out to be inconse-
	// quential because the "known" last partition will keep changing as I
	// scan the extentmap.
	//--------------------------------------------------------------------------
	bool bSegsOutOfService = false;
	int partHighSeg     = -1; // hi seg num for last partition
	int partHighSegNext = -1; // hi seg num for next partition

	if (lastExtentIndex >= 0) {
		uint32_t targetDbRootPart = fExtentMap[lastExtentIndex].partitionNum;
		uint32_t targetDbRootPartNext = targetDbRootPart + 1;
		partHighSeg                = fExtentMap[lastExtentIndex].segmentNum;
		targetDbRootSegs.insert( TargetDbRootSegsMap::value_type(
			fExtentMap[lastExtentIndex].segmentNum,
			fExtentMap[lastExtentIndex].blockOffset) );

		for (int i = 0; i < emEntries; i++) {
			if (fExtentMap[i].range.size  != 0) {
				if (fExtentMap[i].fileID == OID) {

					// 4. Track hi seg for hwm+1 partition
					if (fExtentMap[i].partitionNum == targetDbRootPartNext) {
						if (fExtentMap[i].segmentNum > partHighSegNext) {
							partHighSegNext = fExtentMap[i].segmentNum;
						}
					}

					// 5. Track hi seg for hwm partition
					else if (fExtentMap[i].partitionNum == targetDbRootPart) {
						if (fExtentMap[i].segmentNum > partHighSeg) {
							partHighSeg = fExtentMap[i].segmentNum;
						}

						// 6. Save list of seg files in target DBRoot/Partition,
						//    along with the highest fbo for each seg file
						if (fExtentMap[i].dbRoot == dbRoot) {
							if (fExtentMap[i].status == EXTENTOUTOFSERVICE)
								bSegsOutOfService = true;
							TargetDbRootSegsMapIter iter =
								targetDbRootSegs.find(fExtentMap[i].segmentNum);
							if (iter == targetDbRootSegs.end()) {
								targetDbRootSegs.insert(
									TargetDbRootSegsMap::value_type(
										fExtentMap[i].segmentNum,
										fExtentMap[i].blockOffset) );
							}
							else {
								if (fExtentMap[i].blockOffset > iter->second) {
									iter->second = fExtentMap[i].blockOffset;
								}
							}
						}
					}
				}   // found extentmap entry for specified OID
			}       // found valid extentmap entry
		}           // loop through extent map entries
	}               // (lastExtentIndex >= 0)

	//--------------------------------------------------------------------------
	// Third Step: Select partition and segment number for new extent
	// 1. Loop through targetDbRootSegs to find segment file for next extent
	// 2. Check for exceptions that warrant going to next physical partition
	//    a. See if any extents are marked outOfService
	//    b. See if extents are not evenly layered as expected
	// 3. Perform additional new partition/segment logic as applicable
	//    a. No action taken if 2a or 2b already detected need for new partition
	//    b. If HWM extent is in last file of DBRoot/Partition, see if next
	//       extent goes in new partition, or if wrap-around within current
	//       partition.
	//    c. If extent needs to go in next partition, figure out the next
	//       partition and the next available segment in that partition.
	// 4. Set blockOffset of new extent based on where extent is being added
	//--------------------------------------------------------------------------
	uint16_t newDbRoot       = dbRoot;
	uint32_t newPartitionNum = partitionNum;
	uint16_t newSegmentNum   = 0;
	uint32_t newBlockOffset  = 0;

	// If this is not the first extent for this OID and DBRoot then
	//   extrapolate part# and seg# from last extent; wrap around segment and
	//   partition number as needed.
	// else
	//   use part# that the user specifies
	if (lastExtentIndex >= 0) {
		bool startNewPartition       = false;
		bool startNewStripeInSegFile = false;
		const unsigned int filesPerDBRootPerPartition =
			FILES_PER_COL_PART/DBROOT_COUNT;

		int& lastExtIdx = lastExtentIndex;

		// Find first, last, next seg files in target partition and DBRoot
		uint16_t firstTargetSeg = fExtentMap[lastExtIdx].segmentNum;
		uint16_t lastTargetSeg  = fExtentMap[lastExtIdx].segmentNum;
		uint16_t nextTargetSeg  = fExtentMap[lastExtIdx].segmentNum;

		// 1. Loop thru targetDbRootSegs[] to find next segment after
		//    lastExtIdx in target list.
		//    We save low and high segment to use in wrap-around case.
		if (targetDbRootSegs.size() > 1) {
		 	bool bNextSegSet = false;
			for (TargetDbRootSegsMapConstIter iter=targetDbRootSegs.begin();
				iter!=targetDbRootSegs.end();
				++iter) {
				uint16_t targetSeg = iter->first;

				if (targetSeg      < firstTargetSeg)
					firstTargetSeg = targetSeg;
				else if (targetSeg > lastTargetSeg)
					lastTargetSeg  = targetSeg;
				if (targetSeg > fExtentMap[lastExtIdx].segmentNum) {
					if ((targetSeg < nextTargetSeg) || (!bNextSegSet)) {
						nextTargetSeg = targetSeg;
						bNextSegSet   = true;
					}
				}
			}
		}

		newPartitionNum = fExtentMap[lastExtIdx].partitionNum;

		// 2a. Skip to next physical partition if any extents in HWM partition/
		//     DBRoot are marked as outOfService
		if (bSegsOutOfService) {

//			cout << "Skipping to next partition (outOfService segs)" <<
//				": oid-"  << fExtentMap[lastExtentIndex].fileID <<
//				"; root-" << fExtentMap[lastExtentIndex].dbRoot <<
//				"; part-" << fExtentMap[lastExtentIndex].partitionNum << endl;

			startNewPartition = true;
		}

		// @bug 4765
		// 2b. Skip to next physical partition if we have a set of
		// segment files that are not "layered" as expected, meaning we
		// have > 1 layer of extents with an incomplete lower layer (could
		// be caused by the dropping of logical partitions).
		else if (targetDbRootSegs.size() < filesPerDBRootPerPartition) {
			for (TargetDbRootSegsMapConstIter iter=targetDbRootSegs.begin();
				iter!=targetDbRootSegs.end(); 
				++iter) {
				if (iter->second > 0) {

//					cout << "Skipping to next partition (unbalanced)" <<
//						": oid-"  << fExtentMap[lastExtentIndex].fileID <<
//						"; root-" << fExtentMap[lastExtentIndex].dbRoot <<
//						"; part-" << fExtentMap[lastExtentIndex].partitionNum <<
//						"; seg-"  << iter->first  <<  
//						"; hifbo-"<< iter->second << endl;

					startNewPartition = true;
					break;
				}
			}
		}

		// 3a.If we already detected need for new partition, then take no action
		if (startNewPartition) {
			// no action taken here; we take additional action later.
		}

		// 3b.If HWM extent is in last seg file for this partition and DBRoot,
		//    find out if we need to add a new partition for next extent.
		else if (targetDbRootSegs.size() >= filesPerDBRootPerPartition) {
			if (fExtentMap[lastExtIdx].segmentNum == lastTargetSeg) {
				// Use blockOffset of lastExtIdx to see if we need to add
				// the next extent to a new partition.
				if (fExtentMap[lastExtIdx].blockOffset ==
					((EXTENTS_PER_SEGFILE - 1) *
					(EXTENT_ROWS * colWidth / BLOCK_SIZE)) ) {
					startNewPartition = true;
				}
				else { // Wrap-around; add extent to low seg in this partition
					startNewStripeInSegFile = true;
					newSegmentNum = firstTargetSeg;
				}
			}
			else {
				newSegmentNum = nextTargetSeg;
			}
		}
		else { // Select next segment file in current HWM partition
			newSegmentNum = partHighSeg + 1;
		}

		// 3c. Find new partition and segment if we can't create
		//     an extent for this DBRoot in the current HWM partition.
		if (startNewPartition) {
			newPartitionNum++;
			if (partHighSegNext == -1)
				newSegmentNum = 0;
			else
				newSegmentNum = partHighSegNext + 1;
		}

		// 4. Set blockOffset (fbo) for new extent relative to it's seg file
		// case1: Init fbo to 0 if first extent in partition/DbRoot
		// case2: Init fbo to 0 if first extent in segment file (other than
		//        first segment in this partition/DbRoot, which case1 handled)
		// case3: Init fbo based on previous extent

		// case1: leave newBlockOffset set to 0
		if (startNewPartition) {
			//...no action necessary
		}

		// case2: leave newBlockOffset set to 0
		else if((fExtentMap[lastExtIdx].blockOffset == 0) &&
				(newSegmentNum > firstTargetSeg)) {
			//...no action necessary
		}

		// case3: Init blockOffset based on previous extent.  If we are adding
		//        extent to 1st seg file, then need to bump up the offset; else
		//        adding extent to same stripe and can repeat the same offset.
		else {
			if (startNewStripeInSegFile) {      // start next stripe
				newBlockOffset = static_cast<uint64_t>
					(fExtentMap[lastExtIdx].range.size) * 1024 +
					fExtentMap[lastExtIdx].blockOffset;
			}
			else {								// next extent, same stripe
				newBlockOffset = fExtentMap[lastExtIdx].blockOffset;
			}
		}
	}   // lastExtentIndex >= 0
	else {	// Empty DBRoot; use part# that the user specifies
		if (bHighEmptySegNumSet)
			newSegmentNum = highEmptySegNum + 1;
		else
			newSegmentNum = 0;
	}

	//--------------------------------------------------------------------------
	// Fourth Step: Construct the new extentmap entry
	//--------------------------------------------------------------------------

	makeUndoRecord(&fExtentMap[emptyEMEntry], sizeof(EMEntry));
	EMEntry* e      = &fExtentMap[emptyEMEntry];

	e->range.start  = startLBID;
	e->range.size   = size;
	e->fileID       = OID;
    if (isUnsigned(colDataType))
    {
        e->partition.cprange.lo_val=numeric_limits<uint64_t>::max();
        e->partition.cprange.hi_val=0;
    }
    else
    {
        e->partition.cprange.lo_val=numeric_limits<int64_t>::max();
        e->partition.cprange.hi_val=numeric_limits<int64_t>::min();
    }
	e->partition.cprange.sequenceNum = 0;

	e->colWid       = colWidth;

	e->dbRoot       = newDbRoot;
	e->partitionNum = newPartitionNum;
	e->segmentNum   = newSegmentNum;

	e->blockOffset  = newBlockOffset;
	e->HWM          = 0;
	e->status       = EXTENTUNAVAILABLE; // mark extent as in process

	// Partition, segment, and blockOffset 0 represents new table or column.
	// When DDL creates a table, we can mark the first extent as VALID, since
	// the table has no data.  Marking as VALID enables cpimport to update
	// the CP min/max for the first import.
	// If DDL is adding a column to an existing table, setting to VALID won't
	// hurt, because DDL resets to INVALID after the extent is created.
	if ((e->partitionNum == 0) &&
		(e->segmentNum   == 0) &&
		(e->blockOffset  == 0))
		e->partition.cprange.isValid = CP_VALID;
	else
		e->partition.cprange.isValid = CP_INVALID;

	partitionNum    = e->partitionNum;
	segmentNum      = e->segmentNum;
	startBlockOffset= e->blockOffset;

	makeUndoRecord(fEMShminfo, sizeof(MSTEntry));
	fEMShminfo->currentSize += sizeof(struct EMEntry);

	return startLBID;
}

//------------------------------------------------------------------------------
// Creates an extent for the exact segment column file specified by the
// requested OID, DBRoot, partition number, and segment number.  This is
// the external API function referenced by the dbrm wrapper class.
// required input:
//   OID          - column OID for which the extent is to be created
//   colWidth     - width of column in bytes
//   dbRoot       - DBRoot where extent is to be added
//   partitionNum - partitionNum
//   segmentNum   - segmentNum
// output:
//   lbid         - starting LBID of the created extent
//   allocdsize   - number of LBIDs allocated 
//   startBlockOffset-starting block of the created extent
//------------------------------------------------------------------------------
void ExtentMap::createColumnExtentExactFile(int OID,
	uint32_t  colWidth,
	uint16_t  dbRoot,
	uint32_t partitionNum,
	uint16_t segmentNum,
    execplan::CalpontSystemCatalog::ColDataType colDataType,
	LBID_t&    lbid,
	int&       allocdsize,
	uint32_t& startBlockOffset)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("createColumnExtentExactFile");
		TRACER_ADDINPUT(OID);
		TRACER_ADDINPUT(colWidth);
		TRACER_ADDSHORTINPUT(dbRoot);
		TRACER_ADDOUTPUT(partitionNum);
		TRACER_ADDSHORTOUTPUT(segmentNum);
		TRACER_ADDINT64OUTPUT(lbid);
		TRACER_ADDOUTPUT(allocdsize);
		TRACER_ADDOUTPUT(startBlockOffset);
		TRACER_WRITE;
	}
#endif

#ifdef BRM_DEBUG
	if (OID <= 0) {
		log("ExtentMap::createColumnExtentExactFile(): OID must be > 0",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument(
			"ExtentMap::createColumnExtentExactFile(): OID must be > 0");
	}
#endif

	// Convert extent size in rows to extent size in 8192-byte blocks.
	// extentRows should be multiple of blocksize (8192).
	const unsigned EXTENT_SIZE = (getExtentRows() * colWidth) / BLOCK_SIZE;
	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);

	if (fEMShminfo->currentSize == fEMShminfo->allocdSize)
		growEMShmseg();

//  size is the number of multiples of 1024 blocks.
//  ex: size=1 --> 1024 blocks
//      size=2 --> 2048 blocks
//      size=3 --> 3072 blocks, etc.
	uint32_t size = EXTENT_SIZE/1024;

	lbid = _createColumnExtentExactFile(size, OID, colWidth,
		dbRoot, partitionNum, segmentNum, colDataType, startBlockOffset);

	allocdsize = EXTENT_SIZE;
}

//------------------------------------------------------------------------------
// Creates an extent for the exact segment file specified by the requested
// OID, DBRoot, partition, and segment.  This is the internal implementation
// function.
// input:
//   size         - number of multiples of 1024 blocks allocated to the extent
//                  ex: size=1 --> 1024 blocks
//                      size=2 --> 2048 blocks
//                      size=3 --> 3072 blocks, etc.
//   OID          - column OID for which the extent is to be created
//   colWidth     - width of column in bytes
//   dbRoot       - dbRoot where extent is to be added
//   partitionNum - partitionNum
//   segmentNum   - segmentNum
// output:
//   startBlockOffset-starting block of the created extent
// returns starting LBID of the created extent.
//------------------------------------------------------------------------------
LBID_t ExtentMap::_createColumnExtentExactFile(uint32_t size, int OID,
	uint32_t  colWidth,
	uint16_t  dbRoot,
	uint32_t  partitionNum,
	uint16_t  segmentNum,
    execplan::CalpontSystemCatalog::ColDataType colDataType,
    uint32_t& startBlockOffset)
{
	int emptyEMEntry        = -1;
	int lastExtentIndex     = -1;
	uint32_t highestOffset = 0;

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	LBID_t startLBID = getLBIDsFromFreeList( size );

	// Find the first empty Entry; and find the last extent for this
	// combination of OID, partition, and segment.
	for (int i = 0; i < emEntries; i++) {
		if (fExtentMap[i].range.size != 0) {
			if (fExtentMap[i].fileID == OID) {
				if ((fExtentMap[i].dbRoot       == dbRoot) &&
					(fExtentMap[i].partitionNum == partitionNum) &&
					(fExtentMap[i].segmentNum   == segmentNum) &&
					(fExtentMap[i].blockOffset  >= highestOffset)) {
					lastExtentIndex = i;
					highestOffset = fExtentMap[i].blockOffset;
				}
			}
		}
		else if (emptyEMEntry < 0)
			emptyEMEntry = i;
	} // Loop through extent map entries

	if (emptyEMEntry == -1) {
		ostringstream oss;
		oss << "ExtentMap::_createColumnExtentExactFile(): "
			"could not find an empty EMEntry for OID " << OID <<
			"; Extent Map is full",
		log(oss.str(),
			logging::LOG_TYPE_CRITICAL);
		throw logic_error( oss.str() );
	}

	makeUndoRecord(&fExtentMap[emptyEMEntry], sizeof(EMEntry));
	EMEntry* e      = &fExtentMap[emptyEMEntry];

	e->range.start  = startLBID;
	e->range.size   = size;
	e->fileID       = OID;
    if (isUnsigned(colDataType))
    {
        e->partition.cprange.lo_val=numeric_limits<uint64_t>::max();
        e->partition.cprange.hi_val=0;
    }
    else
    {
        e->partition.cprange.lo_val=numeric_limits<int64_t>::max();
        e->partition.cprange.hi_val=numeric_limits<int64_t>::min();
    }
	e->partition.cprange.sequenceNum = 0;

	e->colWid       = colWidth;

	e->dbRoot       = dbRoot;
	e->partitionNum = partitionNum;
	e->segmentNum   = segmentNum;
	e->status       = EXTENTUNAVAILABLE; // mark extent as in process

	// If first extent for this OID, partition, dbroot, and segment then
	//   blockOffset is set to 0
	// else
	//   blockOffset is extrapolated from the last extent
	if (lastExtentIndex == -1) {
		e->blockOffset  = 0;
		e->HWM          = 0;
	}
	else
	{
		e->blockOffset  = static_cast<uint64_t>
			(fExtentMap[lastExtentIndex].range.size) * 1024 +
				fExtentMap[lastExtentIndex].blockOffset;
		e->HWM          = 0;
	}

	// Partition, segment, and blockOffset 0 represents new table or column.
	// When DDL creates a table, we can mark the first extent as VALID, since
	// the table has no data.  Marking as VALID enables cpimport to update
	// the CP min/max for the first import.
	// If DDL is adding a column to an existing table, setting to VALID won't
	// hurt, because DDL resets to INVALID after the extent is created.
	if ((e->partitionNum == 0) &&
		(e->segmentNum   == 0) &&
		(e->blockOffset  == 0))
		e->partition.cprange.isValid = CP_VALID;
	else
		e->partition.cprange.isValid = CP_INVALID;

	startBlockOffset= e->blockOffset;

	makeUndoRecord(fEMShminfo, sizeof(MSTEntry));
	fEMShminfo->currentSize += sizeof(struct EMEntry);

	return startLBID;
}

//------------------------------------------------------------------------------
// Creates an extent for a dictionary store file.  This is the external API
// function.
// input:
//   OID          - column OID for which the extent is to be created
//   dbRoot       - DBRoot to be assigned to the new extent
//   partitionNum - partition number to be assigned to the new extent
//   segmentNum   - segment number to be assigned to the new extent
// output:
//   lbid         - starting LBID of the created extent
//   allocdsize   - number LBIDs of allocated 
//------------------------------------------------------------------------------
void ExtentMap::createDictStoreExtent(int OID,
	uint16_t  dbRoot,
	uint32_t  partitionNum,
	uint16_t  segmentNum,
 	LBID_t&    lbid,
	int&       allocdsize)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("createDictStoreExtent");
		TRACER_ADDINPUT(OID);
		TRACER_ADDSHORTINPUT(dbRoot);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDINT64OUTPUT(lbid);
		TRACER_ADDOUTPUT(allocdsize);
		TRACER_WRITE;
	}
#endif

#ifdef BRM_DEBUG
	if (OID <= 0) {
		log("ExtentMap::createDictStoreExtent(): OID must be > 0",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument(
			"ExtentMap::createDictStoreExtent(): OID must be > 0");
	}
#endif

	// Convert extent size in rows to extent size in 8192-byte blocks.
	// extentRows should be multiple of blocksize (8192).
	const unsigned EXTENT_SIZE= (getExtentRows() * DICT_COL_WIDTH) / BLOCK_SIZE;

	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);

	if (fEMShminfo->currentSize == fEMShminfo->allocdSize)
		growEMShmseg();

//  size is the number of multiples of 1024 blocks.
//  ex: size=1 --> 1024 blocks
//      size=2 --> 2048 blocks
//      size=3 --> 3072 blocks, etc.
	uint32_t size = EXTENT_SIZE/1024;

	lbid = _createDictStoreExtent(size, OID,
		dbRoot, partitionNum, segmentNum);

	allocdsize = EXTENT_SIZE;
}

//------------------------------------------------------------------------------
// Creates an extent for a dictionary store file.  This is the internal
// implementation function.
// input:
//   size         - number of multiples of 1024 blocks allocated to the extent
//                  ex: size=1 --> 1024 blocks
//                      size=2 --> 2048 blocks
//                      size=3 --> 3072 blocks, etc.
//   OID          - column OID for which the extent is to be created
//   dbRoot       - DBRoot to be assigned to the new extent
//   partitionNum - partition number to be assigned to the new extent
//   segmentNum   - segment number to be assigned to the new extent
// returns starting LBID of the created extent.
//------------------------------------------------------------------------------
LBID_t ExtentMap::_createDictStoreExtent(uint32_t size, int OID,
	uint16_t  dbRoot,
	uint32_t  partitionNum,
	uint16_t  segmentNum)
{
	int emptyEMEntry        = -1;
	int lastExtentIndex     = -1;
	uint32_t highestOffset = 0;

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	LBID_t startLBID = getLBIDsFromFreeList( size );

	// Find the first empty Entry; and find the last extent for this
	// combination of OID, partition, and segment.
	for (int i = 0; i < emEntries; i++) {
		if (fExtentMap[i].range.size != 0) {
			if ((fExtentMap[i].fileID       == OID) && 
				(fExtentMap[i].partitionNum == partitionNum) &&
				(fExtentMap[i].segmentNum   == segmentNum) &&
				(fExtentMap[i].blockOffset  >= highestOffset)) {
				lastExtentIndex = i;
				highestOffset = fExtentMap[i].blockOffset;
			}
		}
		else if (emptyEMEntry < 0)
			emptyEMEntry = i;
	} // Loop through extent map entries

	if (emptyEMEntry == -1) {
		ostringstream oss;
		oss << "ExtentMap::_createDictStoreExtent(): "
			"could not find an empty EMEntry for OID " << OID <<
			"; Extent Map is full",
		log(oss.str(),
			logging::LOG_TYPE_CRITICAL);
		throw logic_error( oss.str() );
	}

	makeUndoRecord(&fExtentMap[emptyEMEntry], sizeof(EMEntry));
	EMEntry* e      = &fExtentMap[emptyEMEntry];

	e->range.start  = startLBID;
	e->range.size   = size;
	e->fileID       = OID;
	e->status       = EXTENTUNAVAILABLE;// @bug 1911 mark extent as in process
    e->partition.cprange.lo_val=numeric_limits<int64_t>::max();
    e->partition.cprange.hi_val=numeric_limits<int64_t>::min();
	e->partition.cprange.sequenceNum = 0;
	e->partition.cprange.isValid     = CP_INVALID;

	// If this is first extent for this OID, partition, segment then
	//   everything is set to 0 or taken from user input
	// else
	//   everything is extrapolated from the last extent
	if (lastExtentIndex == -1) {
		e->blockOffset  = 0;
		e->HWM          = 0;
		e->segmentNum   = segmentNum;
		e->partitionNum = partitionNum;
		e->dbRoot       = dbRoot;
		e->colWid       = 0; // we don't store col width for dictionaries;
		                     // this helps to flag this as a dictionary extent
	} else {
		e->blockOffset  = static_cast<uint64_t>
			(fExtentMap[lastExtentIndex].range.size) * 1024 +
				fExtentMap[lastExtentIndex].blockOffset;
		e->HWM          = 0;
		e->segmentNum   = fExtentMap[lastExtentIndex].segmentNum;
		e->partitionNum = fExtentMap[lastExtentIndex].partitionNum;
		e->dbRoot       = fExtentMap[lastExtentIndex].dbRoot;
		e->colWid       = fExtentMap[lastExtentIndex].colWid;
	}

	makeUndoRecord(fEMShminfo, sizeof(MSTEntry));
	fEMShminfo->currentSize += sizeof(struct EMEntry);

	return startLBID;
}

//------------------------------------------------------------------------------
// Finds and returns the starting LBID for an LBID range taken from the
// free list.
// input:
//   size - number of multiples of 1024 blocks needed from the free list
//          ex: size=1 --> 1024 blocks
//              size=2 --> 2048 blocks
//              size=3 --> 3072 blocks, etc.
// returns selected starting LBID.
//------------------------------------------------------------------------------
LBID_t ExtentMap::getLBIDsFromFreeList ( uint32_t size )
{
	LBID_t ret = -1;
	int i;
	int flEntries = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);

	for (i = 0; i < flEntries; i++) {
		if (size <= fFreeList[i].size) {
			makeUndoRecord(&fFreeList[i], sizeof(InlineLBIDRange));
			ret = fFreeList[i].start;
			fFreeList[i].start += size * 1024;
			fFreeList[i].size -= size;
			if (fFreeList[i].size == 0) {
				makeUndoRecord(fFLShminfo, sizeof(MSTEntry));
				fFLShminfo->currentSize -= sizeof(InlineLBIDRange);
			}
			break;
		}
	}

	if (i == flEntries) {
		log("ExtentMap::getLBIDsFromFreeList(): out of LBID space");
		throw runtime_error(
			"ExtentMap::getLBIDsFromFreeList(): out of LBID space");
	}

	return ret;
}

#ifdef BRM_DEBUG
void ExtentMap::printEM(const EMEntry& em) const
{
	cout << " Start "
			<< em.range.start << " Size "
			<< (long) em.range.size << " OID "
			<< (long) em.fileID << " offset "
			<< (long) em.blockOffset
			<< " LV " << em.partition.cprange.lo_val
			<< " HV " << em.partition.cprange.hi_val;
	cout << endl;
}


void ExtentMap::printEM(const OID_t& oid) const
{
	int emEntries = 0;

    if (fEMShminfo)
		emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	cout << "Extent Map (OID=" << oid << ")" << endl;
	for(int idx=0; idx < emEntries ; idx++) {
		struct EMEntry& em = fExtentMap[idx];
		if (em.fileID==oid && em.range.size != 0)
			printEM(em);
	}

	cout << endl;
}

void ExtentMap::printEM() const
{

	int emEntries = 0;
    if (fEMShminfo)
		emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	cout << "Extent Map (" << emEntries << ")" << endl;
	for(int idx=0; idx < emEntries ; idx++) {
		struct EMEntry& em = fExtentMap[idx];
		if (em.range.size != 0)
			printEM(em);
	}
	cout << endl;
}

void ExtentMap::printFL() const {

	int flEntries = 0;
    if (fFLShminfo)
		flEntries = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);

	cout << "Free List" << endl;
	for(int idx=0; idx < flEntries; idx++) {

		cout << idx << " "
			<< fFreeList[idx].start << " "
			<< fFreeList[idx].size
			<< endl;
	}

	cout << endl;
}
#endif

//------------------------------------------------------------------------------
// Rollback (delete) the extents that logically follow the specified extent for
// the given OID and DBRoot.  HWM for the last extent is reset to the specified
// value.
// input:
//   oid          - OID of the last logical extent to be retained
//   bDeleteAll   - Flag indicates whether all extents for oid and dbroot are
//                  to be deleted; else part#, seg#, and hwm are used.
//   dbRoot       - DBRoot of the extents to be considered.
//   partitionNum - partition number of the last logical extent to be retained
//   segmentNum   - segment number of the last logical extent to be retained
//   hwm          - HWM to be assigned to the last logical extent retained
//------------------------------------------------------------------------------
void ExtentMap::rollbackColumnExtents_DBroot ( int oid,
	bool      bDeleteAll,
	uint16_t dbRoot,
	uint32_t partitionNum,
	uint16_t segmentNum,
	HWM_t     hwm)
{
	//bool oidExists = false;

#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("rollbackColumnExtents");
		TRACER_ADDINPUT(oid);
		TRACER_ADDBOOLINPUT(bDeleteAll);
		TRACER_ADDSHORTINPUT(dbRoot);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDINPUT(hwm);
		TRACER_WRITE;
	}
#endif

#ifdef BRM_DEBUG
	if (oid < 0) {
		log("ExtentMap::rollbackColumnExtents_DBroot(): OID must be >= 0",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument(
			"ExtentMap::rollbackColumnExtents_DBroot(): OID must be >= 0");
	}
#endif

	uint32_t fboLo = 0;
	uint32_t fboHi = 0;
	uint32_t fboLoPreviousStripe = 0;

	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	for (int i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size  != 0) && 
			(fExtentMap[i].fileID      == oid) &&
			(fExtentMap[i].dbRoot      == dbRoot)) {

			//oidExists = true;

			// Don't rollback extents that are out of service
			if (fExtentMap[i].status == EXTENTOUTOFSERVICE)
				continue;

			// If bDeleteAll is true, then we delete extent w/o regards to
			// partition number, segment number, or HWM
			if (bDeleteAll) {
				deleteExtent( i );                                     // case 0
				continue;
			}

			// Calculate fbo range for the stripe containing the given hwm
			if (fboHi == 0) {
				uint32_t range = fExtentMap[i].range.size * 1024;
				fboLo = hwm - (hwm % range);
				fboHi = fboLo + range - 1;
				if (fboLo > 0)
					fboLoPreviousStripe = fboLo - range;
			}

			// Delete, update, or ignore this extent:
			// Later partition:
			//   case 1: extent in later partition than last extent, so delete
			// Same partition:
			//   case 2: extent is in later stripe than last extent, so delete
			//   case 3: extent is in earlier stripe in the same partition.
			//           No action necessary for case3B and case3C.
			//     case 3A: extent is in trailing segment in previous stripe.
			//              This extent is now the last extent in that segment
			//              file, so reset the local HWM if it was altered.
			//     case 3B: extent in previous stripe but not a trailing segment
			//     case 3C: extent is in stripe that precedes previous stripe
			//   case 4: extent is in the same partition and stripe as the
			//           last logical extent we are to keep.
			//     case 4A: extent is in later segment so can be deleted
			//     case 4B: extent is in earlier segment, reset HWM if changed
			//     case 4C: this is last logical extent, reset HWM if changed
			// Earlier partition:
			//   case 5: extent is in earlier parition, no action necessary

			if (fExtentMap[i].partitionNum > partitionNum) {
				deleteExtent( i );                                     // case 1
			}
			else if (fExtentMap[i].partitionNum == partitionNum) {
				if (fExtentMap[i].blockOffset > fboHi) {
					deleteExtent( i );                                 // case 2
				}
				else if (fExtentMap[i].blockOffset < fboLo) {
					if (fExtentMap[i].blockOffset >= fboLoPreviousStripe) {
						if (fExtentMap[i].segmentNum > segmentNum) {
							if (fExtentMap[i].HWM != (fboLo - 1)) {
								makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
								fExtentMap[i].HWM    = fboLo - 1;      //case 3A
								fExtentMap[i].status = EXTENTAVAILABLE;
							}
						}
						else {
							// not a trailing segment in prev stripe     case 3B
						}
					}
					else {
						// extent precedes previous stripe               case 3C
					}
				}
				else { // extent is in same stripe
					if (fExtentMap[i].segmentNum > segmentNum) {
						deleteExtent( i );                            // case 4A
					}
					else if (fExtentMap[i].segmentNum < segmentNum) {
						if (fExtentMap[i].HWM != fboHi) {
							makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
							fExtentMap[i].HWM    = fboHi;             // case 4B
							fExtentMap[i].status = EXTENTAVAILABLE;
						}
					}
					else { // fExtentMap[i].segmentNum == segmentNum
						if (fExtentMap[i].HWM != hwm) {
							makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
							fExtentMap[i].HWM    = hwm;               // case 4C
							fExtentMap[i].status = EXTENTAVAILABLE;
						}
					}
				}
			}
			else {
				// extent in earlier partition; no action necessary       case 5
			}
		}  // extent map entry with matching oid
	}      // loop through the extent map

	// If this function is called, we are already in error recovery mode; so
	// don't worry about reporting an error if the OID is not found, because
	// we don't want/need the extents for that OID anyway.
	//if (!oidExists)
	//{
	//	ostringstream oss;
	//	oss << "ExtentMap::rollbackColumnExtents_DBroot(): "
	//		"Rollback failed: no extents exist for: OID-" << oid <<
	//		"; dbRoot-"    << dbRoot       <<
	//		"; partition-" << partitionNum <<
	//		"; segment-"   << segmentNum   <<
	//		"; hwm-"       << hwm;
	//	log(oss.str(), logging::LOG_TYPE_CRITICAL);
	//	throw invalid_argument(oss.str());
	//}
}

//------------------------------------------------------------------------------
// Rollback (delete) the extents that follow the extents in partitionNum,
// for the given dictionary OID & DBRoot.  The specified hwms represent the HWMs
// to be reset for each of segment store file in this partition.  An HWM will
// not be given for "every" segment file if we are rolling back to a point where
// we had not yet created all the segment files in the partition.  In any case,
// any extents for the "oid" that follow partitionNum, should be deleted.
// Likewise, any extents in the same partition, whose segment file is not in
// segNums[], should be deleted as well.  If hwms is empty, then this DBRoot
// must have been empty at the start of the job, so all the extents for the
// specified oid and dbRoot can be deleted.
// input:
//   oid          - OID of the "last" extents to be retained
//   dbRoot       - DBRoot of the extents to be considered.
//   partitionNum - partition number of the last extents to be retained
//   segNums      - list of segment files with extents to be restored
//   hwms         - HWMs to be assigned to the last retained extent in each of
//                      the corresponding segment store files in segNums.
//                  hwms[0] applies to segment store file segNums[0];
//                  hwms[1] applies to segment store file segNums[1]; etc.
//------------------------------------------------------------------------------
void ExtentMap::rollbackDictStoreExtents_DBroot ( int oid,
	uint16_t            dbRoot,
	uint32_t            partitionNum,
	const vector<uint16_t>& segNums,
	const vector<HWM_t>& hwms)
{
	//bool oidExists = false;

#ifdef BRM_INFO
	if (fDebug)
	{
		ostringstream oss;
    	for (unsigned int k=0; k<hwms.size(); k++)
			oss << "; hwms[" << k << "]-"  << hwms[k];
		const string& hwmString(oss.str());

		// put TRACE inside separate scope {} to insure that temporary
		// hwmString still exists when tracer destructor tries to print it.
		{
			TRACER_WRITELATER("rollbackDictStoreExtents_DBroot");
			TRACER_ADDINPUT(oid);
			TRACER_ADDSHORTINPUT(dbRoot);
			TRACER_ADDINPUT(partitionNum);
			TRACER_ADDSTRINPUT(hwmString);
			TRACER_WRITE;
		}
	}
#endif

	// Delete all extents for the specified OID and DBRoot,
	// if we are not given any hwms and segment files.
	bool bDeleteAll = false;
	if (hwms.size() == 0)
		bDeleteAll = true;

	// segToHwmMap maps segment file number to corresponding pair<hwm,fboLo>
	tr1::unordered_map<uint16_t, pair<uint32_t,uint32_t> > segToHwmMap;
	tr1::unordered_map<uint16_t, pair<uint32_t,uint32_t> >::const_iterator
		segToHwmMapIter;

	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	for (int i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size  != 0) && 
			(fExtentMap[i].fileID      == oid) &&
			(fExtentMap[i].dbRoot      == dbRoot)) {

			//oidExists = true;

			// Don't rollback extents that are out of service
			if (fExtentMap[i].status == EXTENTOUTOFSERVICE)
				continue;

			// If bDeleteAll is true, then we delete extent w/o regards to
			// partition number, segment number, or HWM
			if (bDeleteAll) {
				deleteExtent( i );                                     // case 0
				continue;
			}

			// Calculate fbo's for the list of hwms we are given; and store
			// the fbo and hwm in a map, using the segment file number as a key.
			if (segToHwmMap.size() == 0) {
				uint32_t range = fExtentMap[i].range.size * 1024;
				pair<uint32_t,uint32_t> segToHwmMapEntry;
				for (unsigned int k=0; k<hwms.size(); k++) {
					uint32_t fboLo = hwms[k] - (hwms[k] % range);
					segToHwmMapEntry.first    = hwms[k];
					segToHwmMapEntry.second   = fboLo;
					segToHwmMap[ segNums[k] ] = segToHwmMapEntry;
				}
			}

			// Delete, update, or ignore this extent:
			// Later partition:
			//   case 1: extent is in later partition, so delete the extent
			// Same partition:
			//   case 2: extent is in trailing seg file we don't need; so delete
			//   case 3: extent is in partition and segment file of interest
			//     case 3A: earlier extent in segment file; no action necessary
			//     case 3B: specified HWM falls in this extent, so reset HWM
			//     case 3C: later extent in segment file; so delete the extent
			// Earlier partition:
			//   case 4: extent is in earlier parition, no action necessary

			if (fExtentMap[i].partitionNum > partitionNum) {
				deleteExtent( i );                                     // case 1
			}
			else if (fExtentMap[i].partitionNum == partitionNum) {
				unsigned segNum = fExtentMap[i].segmentNum;
				segToHwmMapIter = segToHwmMap.find( segNum );
				if (segToHwmMapIter == segToHwmMap.end()) {
					deleteExtent( i );                                 // case 2
				}
				else { // segment number in the map of files to keep
					uint32_t fboLo = segToHwmMapIter->second.second;
					if (fExtentMap[i].blockOffset < fboLo) {
						// no action necessary                           case 3A
					}
					else if (fExtentMap[i].blockOffset == fboLo) {
						uint32_t hwm = segToHwmMapIter->second.first;
						if (fExtentMap[i].HWM != hwm) {
							makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
							fExtentMap[i].HWM  = hwm;
							fExtentMap[i].status = EXTENTAVAILABLE;   // case 3B
						}
					}
					else {
						deleteExtent( i );                            // case 3C
					}
				}
			}
			else {
				// extent in earlier partition; no action necessary       case 4
			}
		}  // extent map entry with matching oid
	}      // loop through the extent map

	// If this function is called, we are already in error recovery mode; so
	// don't worry about reporting an error if the OID is not found, because
	// we don't want/need the extents for that OID anyway.
	//if (!oidExists)
	//{
	//	ostringstream oss;
	//	oss << "ExtentMap::rollbackDictStoreExtents_DBroot(): "
	//		"Rollback failed: no extents exist for: OID-" << oid <<
	//		"; dbRoot-"    << dbRoot       <<
	//		"; partition-" << partitionNum;
	//	log(oss.str(), logging::LOG_TYPE_CRITICAL);
	//	throw invalid_argument(oss.str());
	//}
}

//------------------------------------------------------------------------------
// Delete the extents specified and reset hwm
//------------------------------------------------------------------------------
void ExtentMap::deleteEmptyColExtents(const ExtentsInfoMap_t& extentsInfo)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("deleteEmptyColExtents");
		TRACER_WRITE;
	}
#endif

	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);

	uint32_t fboLo = 0;
	uint32_t fboHi = 0;
	uint32_t fboLoPreviousStripe = 0;

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	ExtentsInfoMap_t::const_iterator it;

	for (int i = 0; i < emEntries; i++) {
		if (fExtentMap[i].range.size  != 0) 
		{
			it = extentsInfo.find ( fExtentMap[i].fileID );
			if ( it != extentsInfo.end() ) 
			{
				// Don't rollback extents that are out of service
				if (fExtentMap[i].status == EXTENTOUTOFSERVICE)
					continue;

				// Calculate fbo range for the stripe containing the given hwm
				if (fboHi == 0) {
					uint32_t range = fExtentMap[i].range.size * 1024;
					fboLo = it->second.hwm - (it->second.hwm % range);
					fboHi = fboLo + range - 1;
					if (fboLo > 0)
						fboLoPreviousStripe = fboLo - range;
				}

				// Delete, update, or ignore this extent:
				// Later partition:
				//   case 1: extent in later partition than last extent, so delete
				// Same partition:
				//   case 2: extent is in later stripe than last extent, so delete
				//   case 3: extent is in earlier stripe in the same partition.
				//           No action necessary for case3B and case3C.
				//     case 3A: extent is in trailing segment in previous stripe.
				//              This extent is now the last extent in that segment
				//              file, so reset the local HWM if it was altered.
				//     case 3B: extent in previous stripe but not a trailing segment
				//     case 3C: extent is in stripe that precedes previous stripe
				//   case 4: extent is in the same partition and stripe as the
				//           last logical extent we are to keep.
				//     case 4A: extent is in later segment so can be deleted
				//     case 4B: extent is in earlier segment, reset HWM if changed
				//     case 4C: this is last logical extent, reset HWM if changed
				// Earlier partition:
				//   case 5: extent is in earlier parition, no action necessary

				if (fExtentMap[i].partitionNum > it->second.partitionNum) {
					deleteExtent( i );                                 // case 1
				}
				else if (fExtentMap[i].partitionNum == it->second.partitionNum) 
				{
					if (fExtentMap[i].blockOffset > fboHi) 
					{
						deleteExtent( i );                             // case 2
					}
					else if (fExtentMap[i].blockOffset < fboLo) 
					{
						if (fExtentMap[i].blockOffset >= fboLoPreviousStripe) 
						{
							if (fExtentMap[i].segmentNum > it->second.segmentNum) 
							{
								if (fExtentMap[i].HWM != (fboLo - 1)) 
								{
									makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
									fExtentMap[i].HWM    = fboLo - 1;  //case 3A
									fExtentMap[i].status = EXTENTAVAILABLE;
								}
							}
							else 
							{
							// not a trailing segment in prev stripe     case 3B
							}
						}
						else 
						{
							// extent precedes previous stripe           case 3C
						}
					}
					else 
					{ // extent is in same stripe
						if (fExtentMap[i].segmentNum > it->second.segmentNum) 
						{
							deleteExtent( i );                        // case 4A
						}
						else if (fExtentMap[i].segmentNum < it->second.segmentNum) 
						{
							if (fExtentMap[i].HWM != fboHi) {
								makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
								fExtentMap[i].HWM    = fboHi;         // case 4B
								fExtentMap[i].status = EXTENTAVAILABLE;
							}
						}
						else 
						{ // fExtentMap[i].segmentNum == segmentNum
							if (fExtentMap[i].HWM != it->second.hwm) 
							{
								makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
								fExtentMap[i].HWM    = it->second.hwm;// case 4C
								fExtentMap[i].status = EXTENTAVAILABLE;
							}
						}
					}
				}
				else 
				{
					// extent in earlier partition; no action necessary   case 5
				}
			}  // extent map entry with matching oid
		}
	}      // loop through the extent map
}

void ExtentMap::deleteEmptyDictStoreExtents(const ExtentsInfoMap_t& extentsInfo)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("deleteEmptyDictStoreExtents");
		TRACER_WRITE;
	}
#endif

	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);

	ExtentsInfoMap_t::const_iterator it;

	uint32_t fboLo = 0;
	uint32_t fboHi = 0;

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	it  = extentsInfo.begin();
	if ( it->second.newFile ) //The extent is the new extent
	{
		for (int i = 0; i < emEntries; i++) 
		{
			if (fExtentMap[i].range.size  != 0) 
			{
				it = extentsInfo.find ( fExtentMap[i].fileID );
				if ( it != extentsInfo.end() )
				{
					if ((fExtentMap[i].partitionNum == it->second.partitionNum)
					&& (fExtentMap[i].segmentNum == it->second.segmentNum)
					&& (fExtentMap[i].dbRoot == it->second.dbRoot) )
						deleteExtent( i );   
				}
			}
		}
	}
	else //The extent is the old one
	{

	  for (int i = 0; i < emEntries; i++) 
	  {
		if (fExtentMap[i].range.size  != 0) 
		{
			it = extentsInfo.find ( fExtentMap[i].fileID );
			if ( it != extentsInfo.end() )
			{
				// Don't rollback extents that are out of service
				if (fExtentMap[i].status == EXTENTOUTOFSERVICE)
					continue;

				// Calculate fbo
				if (fboHi == 0) 
				{
					uint32_t range = fExtentMap[i].range.size * 1024;
					fboLo = it->second.hwm- (it->second.hwm % range);
					fboHi = fboLo + range - 1;
				}

				// Delete, update, or ignore this extent:
				// Later partition:
				//   case 1: extent is in later partition, so delete the extent
				// Same partition:
				//   case 2: extent is in partition and segment file of interest
				//     case 2A: earlier extent in segment file; no action necessary
				//     case 2B: specified HWM falls in this extent, so reset HWM
				//     case 2C: later extent in segment file; so delete the extent
				// Earlier partition:
				//   case 3: extent is in earlier parition, no action necessary

				if (fExtentMap[i].partitionNum > it->second.partitionNum) 
				{
					deleteExtent( i );                                 // case 1
				}
				else if (fExtentMap[i].partitionNum == it->second.partitionNum) 
				{
					if ( fExtentMap[i].segmentNum == it->second.segmentNum) 
					{ 
						if (fExtentMap[i].blockOffset < fboLo) 
						{
						// no action necessary                           case 2A
						}
						else if (fExtentMap[i].blockOffset == fboLo) 
						{
							if (fExtentMap[i].HWM != it->second.hwm) {
								makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
								fExtentMap[i].HWM  = it->second.hwm;
								fExtentMap[i].status = EXTENTAVAILABLE;//case 2B
							}
						}
						else 
						{
							deleteExtent( i );                        // case 3C
						}
					}
					else
					{
						// no action necessary  
					}
				}
				else {
				// extent in earlier partition; no action necessary       case 4
				}
			}  // extent map entry with matching oid
		}
	  }      // loop through the extent map
	}
}
//------------------------------------------------------------------------------
// Delete all the extents for the specified OID
//------------------------------------------------------------------------------
void ExtentMap::deleteOID(int OID)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("deleteOID");
		TRACER_ADDINPUT(OID);
		TRACER_WRITE;
	}
#endif

	bool OIDExists = false;

#ifdef BRM_DEBUG
	if (OID < 0) {
		log("ExtentMap::deleteOID(): OID must be >= 0", logging::LOG_TYPE_DEBUG);
		throw invalid_argument("ExtentMap::deleteOID(): OID must be >= 0");
	}
#endif

	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	for (int emIndex = 0; emIndex < emEntries; emIndex++) {

		if (fExtentMap[emIndex].range.size > 0 &&
            fExtentMap[emIndex].fileID == OID) {
			OIDExists = true;

			deleteExtent( emIndex );
		}
	}

	if (!OIDExists)
	{
		ostringstream oss;
		oss << "ExtentMap::deleteOID(): There are no extent entries for OID " << OID << endl;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}
}



//------------------------------------------------------------------------------
// Delete all the extents for the specified OIDs
//------------------------------------------------------------------------------
void ExtentMap::deleteOIDs(const OidsMap_t& OIDs)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("deleteOIDs");
		TRACER_WRITE;
	}
#endif
	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);
	OidsMap_t::const_iterator it;
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	for (int emIndex = 0; emIndex < emEntries; emIndex++) 
	{
		if (fExtentMap[emIndex].range.size > 0 )
		{
			it = OIDs.find ( fExtentMap[emIndex].fileID );
			if ( it != OIDs.end() )
				deleteExtent( emIndex );
		}
	}
}


//------------------------------------------------------------------------------
// Delete the specified extent from the extentmap and return to the free list.
// emIndex - the index (from the extent map) of the extent to be deleted
//------------------------------------------------------------------------------
void ExtentMap::deleteExtent(int emIndex)
{
	int flIndex, freeFLIndex, flEntries, preceedingExtent, succeedingExtent;
	LBID_t flBlockEnd, emBlockEnd;

	flEntries = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);

	emBlockEnd = fExtentMap[emIndex].range.start +
		(static_cast<LBID_t>(fExtentMap[emIndex].range.size) * 1024);

	//scan the freelist to see where this entry fits in
	for (flIndex = 0, preceedingExtent = -1, succeedingExtent = -1, freeFLIndex = -1; 
		flIndex < flEntries; flIndex++) {
		if (fFreeList[flIndex].size == 0) 
			freeFLIndex = flIndex;
		else {
			flBlockEnd = fFreeList[flIndex].start + 
				(static_cast<LBID_t>(fFreeList[flIndex].size) * 1024);
  
			if (emBlockEnd == fFreeList[flIndex].start)
				succeedingExtent = flIndex;
			else if (flBlockEnd == fExtentMap[emIndex].range.start)
				preceedingExtent = flIndex;
		}
	}

	//update the freelist

	//this space is in between 2 blocks in the FL
	if (preceedingExtent != -1 && succeedingExtent != -1) {
		makeUndoRecord(&fFreeList[preceedingExtent], sizeof(InlineLBIDRange));

		// migrate the entry upward if there's a space
		if (freeFLIndex < preceedingExtent && freeFLIndex != -1) {
			makeUndoRecord(&fFreeList[freeFLIndex], sizeof(InlineLBIDRange));
			memcpy(&fFreeList[freeFLIndex], &fFreeList[preceedingExtent], sizeof(InlineLBIDRange));
			fFreeList[preceedingExtent].size = 0;
			preceedingExtent = freeFLIndex;
		}

		fFreeList[preceedingExtent].size += fFreeList[succeedingExtent].size +
			fExtentMap[emIndex].range.size;
		makeUndoRecord(&fFreeList[succeedingExtent], sizeof(InlineLBIDRange));
		fFreeList[succeedingExtent].size = 0;
		makeUndoRecord(fFLShminfo, sizeof(MSTEntry));
		fFLShminfo->currentSize -= sizeof(InlineLBIDRange);
	}

	//this space has a free block at the end
	else if (succeedingExtent != -1) {
		makeUndoRecord(&fFreeList[succeedingExtent], sizeof(InlineLBIDRange));

		// migrate the entry upward if there's a space
		if (freeFLIndex < succeedingExtent && freeFLIndex != -1) {
			makeUndoRecord(&fFreeList[freeFLIndex], sizeof(InlineLBIDRange));
			memcpy(&fFreeList[freeFLIndex], &fFreeList[succeedingExtent], sizeof(InlineLBIDRange));
			fFreeList[succeedingExtent].size = 0;
			succeedingExtent = freeFLIndex;
		}

		fFreeList[succeedingExtent].start = fExtentMap[emIndex].range.start;
		fFreeList[succeedingExtent].size += fExtentMap[emIndex].range.size;
	}

	//this space has a free block at the beginning
	else if (preceedingExtent != -1) {
		makeUndoRecord(&fFreeList[preceedingExtent], sizeof(InlineLBIDRange));

		// migrate the entry upward if there's a space
		if (freeFLIndex < preceedingExtent && freeFLIndex != -1) {
			makeUndoRecord(&fFreeList[freeFLIndex], sizeof(InlineLBIDRange));
			memcpy(&fFreeList[freeFLIndex], &fFreeList[preceedingExtent], sizeof(InlineLBIDRange));
			fFreeList[preceedingExtent].size = 0;
			preceedingExtent = freeFLIndex;
		}

		fFreeList[preceedingExtent].size += fExtentMap[emIndex].range.size;
	}

	//the freelist has no adjacent blocks, so make a new entry
	else {
		if (fFLShminfo->currentSize == fFLShminfo->allocdSize) {
			growFLShmseg();
#ifdef BRM_DEBUG
			if (freeFLIndex != -1) {
				log("ExtentMap::deleteOID(): found a free FL entry in a supposedly full shmseg", logging::LOG_TYPE_DEBUG);
				throw logic_error("ExtentMap::deleteOID(): found a free FL entry in a supposedly full shmseg");
			}
#endif
			freeFLIndex = flEntries;  // happens to be the right index
			flEntries = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);
		}
#ifdef BRM_DEBUG
		if (freeFLIndex == -1) {
			log("ExtentMap::deleteOID(): no available free list entries?", logging::LOG_TYPE_DEBUG);
			throw logic_error("ExtentMap::deleteOID(): no available free list entries?");
		}
#endif
		makeUndoRecord(&fFreeList[freeFLIndex], sizeof(InlineLBIDRange));
		fFreeList[freeFLIndex].start = fExtentMap[emIndex].range.start;
		fFreeList[freeFLIndex].size = fExtentMap[emIndex].range.size;
		makeUndoRecord(&fFLShminfo, sizeof(MSTEntry));
		fFLShminfo->currentSize += sizeof(InlineLBIDRange);
	}

	//invalidate the entry in the Extent Map
	makeUndoRecord(&fExtentMap[emIndex], sizeof(EMEntry));
	fExtentMap[emIndex].range.size = 0;
	makeUndoRecord(&fEMShminfo, sizeof(MSTEntry));
	fEMShminfo->currentSize -= sizeof(struct EMEntry);
}

//------------------------------------------------------------------------------
// Returns the last local HWM for the specified OID for the given DBroot.  
// Also returns the DBRoot, and partition, and segment numbers for the relevant
// segment file. Technically, this function finds the "last" extent for the 
// specified OID, and returns the HWM for that extent.  It is assumed that the 
// HWM for the segment file containing this "last" extent, has been stored in 
// that extent's hwm; and that the hwm is not still hanging around in a previous
// extent for the same segment file.
// If no available or outOfService extent is found, then bFound is returned
// as false.
//------------------------------------------------------------------------------
HWM_t ExtentMap::getLastHWM_DBroot(int OID, uint16_t dbRoot,
   uint32_t& partitionNum, uint16_t& segmentNum, int& status, bool& bFound)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getLastHWM_DBroot");
		TRACER_ADDINPUT(OID);
		TRACER_ADDSHORTINPUT(dbRoot);
		TRACER_ADDOUTPUT(partitionNum);
		TRACER_ADDSHORTOUTPUT(segmentNum);
		TRACER_ADDOUTPUT(status);
		TRACER_WRITE;
	}
#endif

	uint32_t lastExtent = 0;
	int  lastExtentIndex = -1;
	partitionNum = 0;
	segmentNum   = 0;
	HWM_t hwm    = 0;
	bFound       = false;

	if (OID < 0) {
		ostringstream oss;
		oss << "ExtentMap::getLastHWM_DBroot(): invalid OID requested: " << OID;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	grabEMEntryTable(READ);

	// Searching the array in reverse order should be faster since the last
	// extent is usually at the bottom.  We still have to search the entire
	// array (just in case), but the number of operations per loop iteration
	// will be less.
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (int i = emEntries-1; i >= 0; i--) {
		if ((fExtentMap[i].range.size != 0)   && 
			(fExtentMap[i].fileID     == OID) &&
			(fExtentMap[i].dbRoot     == dbRoot) &&
			((fExtentMap[i].status    == EXTENTAVAILABLE) ||
			 (fExtentMap[i].status    == EXTENTOUTOFSERVICE))) {
			if ( (fExtentMap[i].partitionNum >  partitionNum) ||
				((fExtentMap[i].partitionNum == partitionNum) &&
				 (fExtentMap[i].blockOffset  >  lastExtent))  ||
				((fExtentMap[i].partitionNum == partitionNum) &&
				 (fExtentMap[i].blockOffset  == lastExtent) &&
				 (fExtentMap[i].segmentNum   >= segmentNum)) )
			{
				lastExtent      = fExtentMap[i].blockOffset;
				partitionNum    = fExtentMap[i].partitionNum;
				segmentNum      = fExtentMap[i].segmentNum;
				lastExtentIndex = i;
			}
		}
	}

	// save additional information before we release the read-lock
	if (lastExtentIndex != -1)
	{
		hwm    = fExtentMap[lastExtentIndex].HWM;
		status = fExtentMap[lastExtentIndex].status;
		bFound = true;
	}

	releaseEMEntryTable(READ);

	return hwm;
}

//------------------------------------------------------------------------------
// For the specified OID and PM number, this function will return a vector
// of objects carrying HWM info (for the last segment file) and block count 
// information about each DBRoot assigned to the specified PM.
//------------------------------------------------------------------------------
void ExtentMap::getDbRootHWMInfo(int OID, uint16_t pmNumber,
	EmDbRootHWMInfo_v& emDbRootHwmInfos)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getDbRootHWMInfo");
		TRACER_ADDINPUT(OID);
		TRACER_ADDSHORTINPUT(pmNumber);
		TRACER_WRITE;
	}
#endif

	if (OID < 0) {
		ostringstream oss;
		oss << "ExtentMap::getDbRootHWMInfo(): invalid OID requested: " << OID;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	// Determine List of DBRoots for specified PM, and construct map of
	// EmDbRootHWMInfo objects.
	tr1::unordered_map<uint16_t, EmDbRootHWMInfo> emDbRootMap;
	vector<int> dbRootList;
	getPmDbRoots( pmNumber, dbRootList );
	if ( dbRootList.size() >0 )
	{
		for (unsigned int iroot=0; iroot<dbRootList.size(); iroot++)
		{
			uint16_t rootID = dbRootList[iroot];
			EmDbRootHWMInfo emDbRootInfo(rootID);
			emDbRootMap[rootID] = emDbRootInfo;
		}
	}
	else
	{
		ostringstream oss;
		oss << "ExtentMap::getDbRootHWMInfo(): "
			"There are no DBRoots for OID " << OID <<
			" and PM " << pmNumber << endl;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	grabEMEntryTable(READ);
	tr1::unordered_map<uint16_t, EmDbRootHWMInfo>::iterator emIter;

	// Searching the array in reverse order should be faster since the last
	// extent is usually at the bottom.  We still have to search the entire
	// array (just in case), but the number of operations per loop iteration
	// will be less.
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (int i = emEntries-1; i >= 0; i--) {
		if ((fExtentMap[i].range.size != 0)   && 
			(fExtentMap[i].fileID     == OID)) {

			// Include this extent in the search, only if the extent's
			// DBRoot falls in the list of DBRoots for this PM.
			emIter = emDbRootMap.find( fExtentMap[i].dbRoot );
			if (emIter == emDbRootMap.end())
				continue;

			EmDbRootHWMInfo& emDbRoot = emIter->second;

			if ((fExtentMap[i].status != EXTENTOUTOFSERVICE) &&
				(fExtentMap[i].HWM != 0))
				emDbRoot.totalBlocks += (fExtentMap[i].HWM + 1);

			if ( (fExtentMap[i].partitionNum >  emDbRoot.partitionNum) ||
				((fExtentMap[i].partitionNum == emDbRoot.partitionNum) &&
			 	(fExtentMap[i].blockOffset   >  emDbRoot.fbo))         ||
				((fExtentMap[i].partitionNum == emDbRoot.partitionNum) &&
			 	(fExtentMap[i].blockOffset   == emDbRoot.fbo) &&
			 	(fExtentMap[i].segmentNum    >= emDbRoot.segmentNum)) )
			{
				emDbRoot.fbo              = fExtentMap[i].blockOffset;
				emDbRoot.partitionNum     = fExtentMap[i].partitionNum;
				emDbRoot.segmentNum       = fExtentMap[i].segmentNum;
				emDbRoot.localHWM         = fExtentMap[i].HWM;
				emDbRoot.startLbid        = fExtentMap[i].range.start;
				emDbRoot.status           = fExtentMap[i].status;
				emDbRoot.hwmExtentIndex   = i;
			}
		}
	}

	releaseEMEntryTable(READ);

	for (tr1::unordered_map<uint16_t, EmDbRootHWMInfo>::iterator iter=
		emDbRootMap.begin(); iter != emDbRootMap.end(); ++iter)
	{
		EmDbRootHWMInfo& emDbRoot = iter->second;
		if (emDbRoot.hwmExtentIndex != -1)
		{
			// @bug 5349: make sure HWM extent for each DBRoot is AVAILABLE
			if (emDbRoot.status == EXTENTUNAVAILABLE)
			{
				ostringstream oss;
				oss << "ExtentMap::getDbRootHWMInfo(): " <<
					"OID " << OID <<
					" has HWM extent that is UNAVAILABLE for " <<
					"DBRoot"      << emDbRoot.dbRoot       <<
					"; part#: "   << emDbRoot.partitionNum <<
					", seg#: "    << emDbRoot.segmentNum   <<
					", fbo: "     << emDbRoot.fbo          <<
					", localHWM: "<< emDbRoot.localHWM     <<
					", lbid: "    << emDbRoot.startLbid    << endl;
				log(oss.str(), logging::LOG_TYPE_CRITICAL);
				throw runtime_error(oss.str());
			}

			// In the loop above we ignored "all" the extents with HWM of 0,
			// which is okay most of the time, because each segment file's HWM
			// is carried in the last extent only.  BUT if we have a segment
			// file with HWM=0, having a single extent and a single block at
			// the "end" of the data, we still need to account for this last
			// block.  So we increment the block count for this isolated case.
	    	if ((emDbRoot.localHWM == 0) &&
				(emDbRoot.status == EXTENTAVAILABLE))
			{
				emDbRoot.totalBlocks++;
			}
		}
	}

	// Copy internal map to the output vector argument
	for (tr1::unordered_map<uint16_t, EmDbRootHWMInfo>::iterator iter=
		emDbRootMap.begin(); iter != emDbRootMap.end(); ++iter)
	{
		emDbRootHwmInfos.push_back( iter->second );
	}
}

//------------------------------------------------------------------------------
// Return the existence (bFound) and state (status) for the segment file
// containing the extents for the specified OID, partition, and segment.
// If no extents are found, no exception is thrown.  We instead just return
// bFound=false, so that the application can take the necessary action.
// The value returned in the "status" variable is based on the first extent
// found, since all the extents in a segment file should have the same state.
//------------------------------------------------------------------------------
void ExtentMap::getExtentState(int OID, uint32_t partitionNum,
	uint16_t segmentNum, bool& bFound, int& status)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getExtentState");
		TRACER_ADDINPUT(OID);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDOUTPUT(status);
		TRACER_WRITE;
	}
#endif
	int i, emEntries;
	bFound = false;
	status = EXTENTAVAILABLE;

	if (OID < 0) {
		ostringstream oss;
		oss << "ExtentMap::getExtentState(): invalid OID requested: " << OID;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	grabEMEntryTable(READ);

	emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size  != 0) &&
			(fExtentMap[i].fileID      == OID) && 
			(fExtentMap[i].partitionNum== partitionNum) &&
			(fExtentMap[i].segmentNum  == segmentNum)) {
			bFound = true;
			status = fExtentMap[i].status;
			break;
		}
	}

	releaseEMEntryTable(READ);
}

//------------------------------------------------------------------------------
// Returns the HWM for the specified OID, partition, and segment numbers.
// Used to get the HWM for a specific column or dictionary store segment file.
//------------------------------------------------------------------------------
HWM_t ExtentMap::getLocalHWM(int OID, uint32_t partitionNum,
   uint16_t segmentNum, int& status)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getLocalHWM");
		TRACER_ADDINPUT(OID);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDOUTPUT(status);
		TRACER_WRITE;
	}
#endif

#ifdef EM_AS_A_TABLE_POC__
	if (OID == 1084)
	{
		return 0;
	}
#endif

	int i, emEntries;
	HWM_t ret = 0;
	bool OIDPartSegExists = false;

	if (OID < 0) {
		ostringstream oss;
		oss << "ExtentMap::getLocalHWM(): invalid OID requested: " << OID;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	grabEMEntryTable(READ);

	emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size  != 0) &&
			(fExtentMap[i].fileID      == OID) && 
			(fExtentMap[i].partitionNum== partitionNum) &&
			(fExtentMap[i].segmentNum  == segmentNum)) {
			OIDPartSegExists = true;
			status = fExtentMap[i].status;
			if (fExtentMap[i].HWM != 0) {
				ret = fExtentMap[i].HWM;
				releaseEMEntryTable(READ);
				return ret;
			}
		}
	}

	releaseEMEntryTable(READ);
	if (OIDPartSegExists)
		return 0;
	else
	{
		ostringstream oss;
		oss << "ExtentMap::getLocalHWM(): There are no extent entries for OID "<<
			OID << "; partition " << partitionNum << "; segment " << 
			segmentNum << endl;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}
}

//------------------------------------------------------------------------------
// Sets the HWM for the specified OID, partition, and segment number.
// In addition, the HWM for the old HWM extent (for this segment file),
// is set to 0, so that the latest HWM is only carried in the last extent
// (per segment file).
// Used for dictionary or column OIDs to set the HWM for specific segment file.
//------------------------------------------------------------------------------
void ExtentMap::setLocalHWM(int OID, uint32_t partitionNum,
	uint16_t segmentNum, HWM_t newHWM, bool firstNode, bool uselock)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("setLocalHWM");
		TRACER_ADDINPUT(OID);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDINPUT(newHWM);
		TRACER_WRITE;
	}

	bool addedAnExtent = false;

	if (OID < 0) {
		log("ExtentMap::setLocalHWM(): OID must be >= 0",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument(
			"ExtentMap::setLocalHWM(): OID must be >= 0");
	}
#endif

	int lastExtentIndex     = -1;
	int oldHWMExtentIndex   = -1;
	uint32_t highestOffset = 0;

	if (uselock)
		grabEMEntryTable(WRITE);

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (int i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size  != 0) && 
			(fExtentMap[i].fileID      == OID) && 
			(fExtentMap[i].partitionNum== partitionNum) &&
			(fExtentMap[i].segmentNum  == segmentNum)) {

			// Find current HWM extent
			if (fExtentMap[i].blockOffset >= highestOffset) {
				highestOffset   = fExtentMap[i].blockOffset;
				lastExtentIndex = i;
			}

			// Find previous HWM extent
			if (fExtentMap[i].HWM != 0) {
				oldHWMExtentIndex = i;
			}
		}
	}

	if (lastExtentIndex == -1) {
		ostringstream oss;
		oss << "ExtentMap::setLocalHWM(): Bad OID/partition/segment argument; "
			"no extent entries for OID " << OID << "; partition " <<
			partitionNum << "; segment " << segmentNum << endl;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	if (newHWM >= (fExtentMap[lastExtentIndex].blockOffset + 
		   fExtentMap[lastExtentIndex].range.size*1024)) {
		ostringstream oss;
		oss << "ExtentMap::setLocalHWM(): "
			"new HWM is past the end of the file for OID " << OID << "; partition " <<
			partitionNum << "; segment " << segmentNum << "; HWM " << newHWM;
		log(oss.str(), logging::LOG_TYPE_DEBUG);
		throw invalid_argument(oss.str());
	}

	// Save HWM in last extent for this segment file; and mark as AVAILABLE
	makeUndoRecord(&fExtentMap[lastExtentIndex], sizeof(EMEntry));
	fExtentMap[lastExtentIndex].HWM    = newHWM;
	fExtentMap[lastExtentIndex].status = EXTENTAVAILABLE;

	// Reset HWM in old HWM extent to 0
	if ((oldHWMExtentIndex != -1) && (oldHWMExtentIndex != lastExtentIndex)) {
		makeUndoRecord(&fExtentMap[oldHWMExtentIndex], sizeof(EMEntry));
		fExtentMap[oldHWMExtentIndex].HWM = 0;
#ifdef BRM_DEBUG 
		addedAnExtent = true;
#endif
	}

#ifdef BRM_DEBUG 
	if (firstNode) {
		ostringstream os;
		os << "ExtentMap::setLocalHWM(): firstLBID=" << fExtentMap[lastExtentIndex].range.start <<
				" lastLBID=" << fExtentMap[lastExtentIndex].range.start +
				fExtentMap[lastExtentIndex].range.size*1024 - 1 << " newHWM=" << fExtentMap[lastExtentIndex].HWM
				<< " min=" << fExtentMap[lastExtentIndex].partition.cprange.lo_val << " max=" <<
				fExtentMap[lastExtentIndex].partition.cprange.hi_val << " seq=" <<
				fExtentMap[lastExtentIndex].partition.cprange.sequenceNum << " status=";
		switch (fExtentMap[lastExtentIndex].partition.cprange.isValid) {
			case CP_INVALID: os << "invalid."; break;
			case CP_UPDATING: os << "updating."; break;
			case CP_VALID: os << "valid."; break;
			default: os << "unknown(!!)."; break;
		}
		if (addedAnExtent)
			os << "  Data extended into a new extent.";
		log(os.str(), logging::LOG_TYPE_DEBUG);
	}
#endif
}

void ExtentMap::bulkSetHWM(const vector<BulkSetHWMArg> &v, bool firstNode)
{
	grabEMEntryTable(WRITE);

	for (uint32_t i = 0; i < v.size(); i++)
		setLocalHWM(v[i].oid, v[i].partNum, v[i].segNum, v[i].hwm, firstNode, false);
}

class BUHasher {
public:
	inline uint64_t operator()(const BulkUpdateDBRootArg &b) const
		{ return b.startLBID; }
};

class BUEqual {
public:
	inline bool operator()(const BulkUpdateDBRootArg &b1, const BulkUpdateDBRootArg &b2) const
		{ return b1.startLBID == b2.startLBID; }
};

void ExtentMap::bulkUpdateDBRoot(const vector<BulkUpdateDBRootArg> &args)
{
	tr1::unordered_set<BulkUpdateDBRootArg, BUHasher, BUEqual> sArgs;
	tr1::unordered_set<BulkUpdateDBRootArg, BUHasher, BUEqual>::iterator sit;
	BulkUpdateDBRootArg key;
	int emEntries;

	for (uint32_t i = 0; i < args.size(); i++)
		sArgs.insert(args[i]);

	grabEMEntryTable(WRITE);

	emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (int i = 0; i < emEntries; i++) {
		key.startLBID = fExtentMap[i].range.start;
		sit = sArgs.find(key);
		if (sit != sArgs.end())
			fExtentMap[i].dbRoot = sit->dbRoot;
	}
}

void ExtentMap::getExtents(int OID, vector<struct EMEntry>& entries,
	bool sorted, bool notFoundErr, bool incOutOfService)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getExtents");
		TRACER_ADDINPUT(OID);
		TRACER_WRITE;
	}
#endif
	int i, emEntries;

	entries.clear();

	if (OID < 0) {
		ostringstream oss;
		oss << "ExtentMap::getExtents(): invalid OID requested: " << OID;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	grabEMEntryTable(READ);
	emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	if (incOutOfService) {
		for (i = 0 ; i < emEntries; i++)
			if ((fExtentMap[i].fileID == OID) &&
				(fExtentMap[i].range.size != 0))
				entries.push_back(fExtentMap[i]);
	}
	else {
		for (i = 0 ; i < emEntries; i++)
			if ((fExtentMap[i].fileID     == OID) &&
				(fExtentMap[i].range.size != 0)   &&
				(fExtentMap[i].status     != EXTENTOUTOFSERVICE))
				entries.push_back(fExtentMap[i]);
	}
	releaseEMEntryTable(READ);

	if (sorted)
		sort<vector<struct EMEntry>::iterator>(entries.begin(), entries.end());
}

void ExtentMap::getExtents_dbroot(int OID, vector<struct EMEntry>& entries, const uint16_t dbroot)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getExtents");
		TRACER_ADDINPUT(OID);
		TRACER_WRITE;
	}
#endif

#ifdef EM_AS_A_TABLE_POC__
	if (OID == 1084)
	{
		EMEntry fakeEntry;
		fakeEntry.range.start = (1LL << 54);
		fakeEntry.range.size = 4;
		fakeEntry.fileID = 1084;
		fakeEntry.blockOffset = 0;
		fakeEntry.HWM = 1;
		fakeEntry.partitionNum = 0;
		fakeEntry.segmentNum = 0;
		fakeEntry.dbRoot = 1;
		fakeEntry.colWid = 4;
		fakeEntry.status = EXTENTAVAILABLE;
		fakeEntry.partition.cprange.hi_val = numeric_limits<int64_t>::min() + 2;
		fakeEntry.partition.cprange.lo_val = numeric_limits<int64_t>::max();
		fakeEntry.partition.cprange.sequenceNum = 0;
		fakeEntry.partition.cprange.isValid = CP_INVALID;
		entries.push_back(fakeEntry);
		return;
	}
#endif

	int i, emEntries;

	entries.clear();

	if (OID < 0) {
		ostringstream oss;
		oss << "ExtentMap::getExtents(): invalid OID requested: " << OID;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	grabEMEntryTable(READ);
	emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	for (i = 0 ; i < emEntries; i++)
		if ((fExtentMap[i].fileID == OID) &&
			(fExtentMap[i].range.size != 0) && (fExtentMap[i].dbRoot == dbroot))
				entries.push_back(fExtentMap[i]);

	releaseEMEntryTable(READ);
}

//------------------------------------------------------------------------------
// Get the number of extents for the specified OID and DBRoot.
// OutOfService extents are included/excluded depending on the
// value of the incOutOfService flag.
//------------------------------------------------------------------------------
void ExtentMap::getExtentCount_dbroot(int OID, uint16_t dbroot,
	bool incOutOfService, uint64_t& numExtents)
{
	int i, emEntries;

	if (OID < 0) {
		ostringstream oss;
		oss << "ExtentMap::getExtentsCount_dbroot(): invalid OID requested: " <<
			OID;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	grabEMEntryTable(READ);
	emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	numExtents = 0;

	if (incOutOfService) {
		for (i = 0 ; i < emEntries; i++) {
			if ((fExtentMap[i].fileID     == OID) &&
				(fExtentMap[i].range.size != 0)   &&
				(fExtentMap[i].dbRoot     == dbroot))
				numExtents++;
		}
	}
	else {
		for (i = 0 ; i < emEntries; i++) {
			if ((fExtentMap[i].fileID     == OID)    &&
				(fExtentMap[i].range.size != 0)      &&
				(fExtentMap[i].dbRoot     == dbroot) &&
				(fExtentMap[i].status     != EXTENTOUTOFSERVICE))
				numExtents++;
		}
	}

	releaseEMEntryTable(READ);
}

//------------------------------------------------------------------------------
// Gets the DBRoot for the specified system catalog OID.
// Function assumes the specified System Catalog OID is fully contained on
// a single DBRoot, as the function only searches for and returns the first
// DBRoot entry that is found in the extent map.
//------------------------------------------------------------------------------
void ExtentMap::getSysCatDBRoot(OID_t oid, uint16_t& dbRoot)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getSysCatDBRoot");
		TRACER_ADDINPUT(oid);
		TRACER_ADDSHORTOUTPUT(dbRoot);
		TRACER_WRITE;
	}
#endif

	bool bFound = false;
	grabEMEntryTable(READ);
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	for (int i = 0 ; i < emEntries; i++) {
		if ((fExtentMap[i].range.size != 0) &&
		    (fExtentMap[i].fileID     == oid)) {
			dbRoot = fExtentMap[i].dbRoot;
			bFound = true;
			break;
		}
	}

	releaseEMEntryTable(READ);

	if (!bFound) {
		ostringstream oss;
		oss << "ExtentMap::getSysCatDBRoot(): OID not found: " << oid;
		log(oss.str(), logging::LOG_TYPE_WARNING);
		throw logic_error(oss.str());
	}
}

//------------------------------------------------------------------------------
// Delete all extents for the specified OID(s) and partition number.
// @bug 5237 - Removed restriction that prevented deletion of segment files in
//             the last partition (for a DBRoot).
//------------------------------------------------------------------------------
void ExtentMap::deletePartition(const set<OID_t>& oids,
	const set<LogicalPartition>& partitionNums, string& emsg)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("deletePartition");
		ostringstream oss;
		set<LogicalPartition>::const_iterator partIt;
		oss << "partitionNums: " 
		for (partIt=partitionNums.begin(); it!=partitionNums.end(); ++it)
			oss << (*it) << " ";

		oss << endl;
		oss << "OIDS: ";
		set<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}
#endif
	if (oids.size() == 0)
		return;

	int rc = 0;

	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);
	set<LogicalPartition> foundPartitions;
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	vector<uint32_t> extents;

	// First: validate against referencing non-existent logical partitions
	std::set<OID_t>::const_iterator it;
	for (int i = 0; i < emEntries; i++) 
	{
		LogicalPartition lp(fExtentMap[i].dbRoot,
			fExtentMap[i].partitionNum, fExtentMap[i].segmentNum);
		if ((fExtentMap[i].range.size != 0) &&
			(partitionNums.find(lp)   != partitionNums.end()))
		{
			it = oids.find( fExtentMap[i].fileID );
			if (it != oids.end())
			{
				foundPartitions.insert(lp);
				extents.push_back(i);
			}
		}
	}

	if (foundPartitions.size() != partitionNums.size())
	{
		set<LogicalPartition>::const_iterator partIt;
		Message::Args args;
		ostringstream oss;
		for (partIt = partitionNums.begin();
			partIt != partitionNums.end(); ++partIt)
		{
			if (foundPartitions.find((*partIt)) == foundPartitions.end())
			{
				if (!oss.str().empty())
					oss << ", ";
				oss << (*partIt).toString();
			}
		}
		args.add(oss.str());
		emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NOT_EXIST,args);
		rc = ERR_PARTITION_NOT_EXIST;
	}

	// this has to be the last error code to set and can not be over-written
	if (foundPartitions.empty())
		rc = WARN_NO_PARTITION_PERFORMED;

	// really delete extents
	for (uint32_t i = 0; i < extents.size(); i++)
	{
		deleteExtent(extents[i]);
	}

	// @bug 4772 throw exception on any error because they are all warnings.
	if (rc)
		throw IDBExcept(emsg, rc);
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s) and partition
// number.
// @bug 5237 - Removed restriction that prevented deletion of segment files in
//             the last partition (for a DBRoot).
//------------------------------------------------------------------------------
void ExtentMap::markPartitionForDeletion(const set<OID_t>& oids,
	const set<LogicalPartition>& partitionNums, string& emsg)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("markPartitionForDeletion");
		ostringstream oss;
		set<LogicalPartition>::const_iterator partIt;
		oss << "partitionNums: " 
		for (partIt=partitionNums.begin(); it!=partitionNums.end(); ++it)
			oss << (*it) << " ";

		oss << endl;
		oss << "OIDS: ";
		set<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}
#endif
	if (oids.size() == 0)
		return;

	int rc = 0;

	grabEMEntryTable(WRITE);
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	set<LogicalPartition> foundPartitions;
	vector<uint32_t> extents;
	bool partitionAlreadyDisabled = false;

	// Identify not exists partition first. Then mark disable.
	std::set<OID_t>::const_iterator it;
	for (int i = 0; i < emEntries; i++) 
	{
		LogicalPartition lp(fExtentMap[i].dbRoot,
			fExtentMap[i].partitionNum, fExtentMap[i].segmentNum);
		if ((fExtentMap[i].range.size != 0) && 
			(partitionNums.find(lp)   != partitionNums.end()))
		{
			it = oids.find( fExtentMap[i].fileID );
			if (it != oids.end()) 
			{
				if (fExtentMap[i].status == EXTENTOUTOFSERVICE)
				{
					partitionAlreadyDisabled = true;
				}
				foundPartitions.insert(lp);
				extents.push_back(i);
			}
		}
	}

	// really disable partitions
	for (uint32_t i = 0; i < extents.size(); i++)
	{
		makeUndoRecord(&fExtentMap[extents[i]], sizeof(EMEntry));
		fExtentMap[extents[i]].status = EXTENTOUTOFSERVICE;
	}

	// validate against referencing non-existent logical partitions
	if (foundPartitions.size() != partitionNums.size())
	{
		set<LogicalPartition>::const_iterator partIt;
		Message::Args args;
		ostringstream oss;
		for (partIt = partitionNums.begin();
			partIt != partitionNums.end(); ++partIt)
		{
			if (foundPartitions.find((*partIt)) == foundPartitions.end())
			{
				if (!oss.str().empty())
					oss << ", ";
				oss << (*partIt).toString();
			}
		}
		args.add(oss.str());
		emsg = emsg + string("\n") + IDBErrorInfo::instance()->errorMsg(
			ERR_PARTITION_NOT_EXIST, args);
		rc = ERR_PARTITION_NOT_EXIST;
	}

	// check already disabled error now, which could be a non-error
	if (partitionAlreadyDisabled)
	{
		emsg = emsg + string("\n") + IDBErrorInfo::instance()->errorMsg(
			ERR_PARTITION_ALREADY_DISABLED);
		rc = ERR_PARTITION_ALREADY_DISABLED;
	}

	// this rc has to be the last one set and can not be over-written by others.
	if (foundPartitions.empty())
	{
		rc = WARN_NO_PARTITION_PERFORMED;
	}

	// @bug 4772 throw exception on any error because they are all warnings.
	if (rc)
		throw IDBExcept(emsg, rc);
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s) 
//------------------------------------------------------------------------------
void ExtentMap::markAllPartitionForDeletion(const set<OID_t>& oids)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("markPartitionForDeletion");
		ostringstream oss;
		oss << "OIDS: ";
		set<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}
#endif
	if (oids.size() == 0)
		return;

	set<OID_t>::const_iterator it;

	grabEMEntryTable(WRITE);
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (int i = 0; i < emEntries; i++) {
		if (fExtentMap[i].range.size  != 0  ) {
			it = oids.find( fExtentMap[i].fileID );
			if (it != oids.end()) {
				makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
				fExtentMap[i].status = EXTENTOUTOFSERVICE;
			}
		}
	}
}

//------------------------------------------------------------------------------
// Restore all extents for the specified OID(s) and partition number.
//------------------------------------------------------------------------------
void ExtentMap::restorePartition(const set<OID_t>& oids,
  const set<LogicalPartition>& partitionNums, string& emsg)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("restorePartition");
		ostringstream oss;
		set<LogicalPartition>::const_iterator partIt;
		oss << "partitionNums: " 
		for (partIt=partitionNums.begin(); it!=partitionNums.end(); ++it)
			oss << (*it) << " ";
		oss << endl;
		oss << "OIDS: ";
		set<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}
#endif
	if (oids.size() == 0)
		return;

	set<OID_t>::const_iterator it;
	grabEMEntryTable(WRITE);

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	vector<uint32_t> extents;
	set<LogicalPartition> foundPartitions;
	bool partitionAlreadyEnabled = false;

	for (int i = 0; i < emEntries; i++) 
	{
		LogicalPartition lp(fExtentMap[i].dbRoot, fExtentMap[i].partitionNum, fExtentMap[i].segmentNum);
		if ((fExtentMap[i].range.size  != 0  ) && partitionNums.find(lp) != partitionNums.end())
		{
			it = oids.find( fExtentMap[i].fileID );
			if (it != oids.end()) 
			{
				if (fExtentMap[i].status == EXTENTAVAILABLE)
				{
					partitionAlreadyEnabled = true;
				}
				extents.push_back(i);
				foundPartitions.insert(lp);
			}
		}
	}

	if (foundPartitions.size() != partitionNums.size())
	{
		set<LogicalPartition>::const_iterator partIt;
		Message::Args args;
		ostringstream oss;
		for (partIt = partitionNums.begin(); partIt != partitionNums.end(); ++partIt)
		{
			if (foundPartitions.empty() || foundPartitions.find((*partIt)) == foundPartitions.end())
			{
				if (!oss.str().empty())
					oss << ", ";
				oss << (*partIt).toString();
			}
		}
		args.add(oss.str());
		emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NOT_EXIST, args);
		throw IDBExcept(emsg, ERR_PARTITION_NOT_EXIST);
	}

	// really enable partitions
	for (uint32_t i = 0; i < extents.size(); i++)
	{
		makeUndoRecord(&fExtentMap[extents[i]], sizeof(EMEntry));
		fExtentMap[extents[i]].status = EXTENTAVAILABLE;
	}

	if (partitionAlreadyEnabled)
	{
		emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_ALREADY_ENABLED);
		throw IDBExcept(emsg, ERR_PARTITION_ALREADY_ENABLED);
	}
}

//------------------------------------------------------------------------------
// Return all the out-of-service partitions for the specified OID.
//------------------------------------------------------------------------------
void ExtentMap::getOutOfServicePartitions(OID_t oid,
	set<LogicalPartition>& partitionNums) 
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getExtents");
		TRACER_ADDINPUT(oid);
		TRACER_WRITE;
	}
#endif

	partitionNums.clear();

	if (oid < 0) {
		ostringstream oss;
		oss << "ExtentMap::getOutOfServicePartitions(): "
			"invalid OID requested: " << oid;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	grabEMEntryTable(READ);
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (int i=0; i<emEntries; i++) {
		if ((fExtentMap[i].range.size != 0  ) && 
			(fExtentMap[i].fileID     == oid) &&
			(fExtentMap[i].status     == EXTENTOUTOFSERVICE)) {

			// need to be logical partition number
			LogicalPartition lp(fExtentMap[i].dbRoot,
				fExtentMap[i].partitionNum,
				fExtentMap[i].segmentNum);
			partitionNums.insert(lp);
		}
	}
	releaseEMEntryTable(READ);
}

//------------------------------------------------------------------------------
// Delete all extents for the specified dbroot
//------------------------------------------------------------------------------
void ExtentMap::deleteDBRoot(uint16_t dbroot)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("deleteDBRoot");
		ostringstream oss;
		oss << "dbroot: " << dbroot;
		TRACER_WRITEDIRECT(oss.str());
	}
#endif

	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);

	for (unsigned i = 0; i < fEMShminfo->allocdSize/sizeof(struct EMEntry); i++)
		if (fExtentMap[i].range.size != 0 && fExtentMap[i].dbRoot == dbroot)
			deleteExtent(i);
}

//------------------------------------------------------------------------------
// Does the specified DBRoot have any extents.
// Throws exception if extentmap shared memory is not loaded.
//------------------------------------------------------------------------------
bool ExtentMap::isDBRootEmpty(uint16_t dbroot)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("isDBRootEmpty");
		TRACER_ADDINPUT(OID);
		TRACER_WRITE;
	}
#endif

	bool bEmpty = true;
	int i, emEntries;
	grabEMEntryTable(READ);
	emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	if (fEMShminfo->currentSize == 0) {
		throw runtime_error(
			"ExtentMap::isDBRootEmpty() shared memory not loaded");
	}

	for (i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size != 0)   &&
			(fExtentMap[i].dbRoot     == dbroot)) {
			bEmpty = false;
			break;
		}
	}

	releaseEMEntryTable(READ);

	return bEmpty;
}

void ExtentMap::lookup(OID_t OID, LBIDRange_v& ranges)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("lookup");
		TRACER_ADDINPUT(OID);
		TRACER_WRITE;
	}
#endif

#ifdef EM_AS_A_TABLE_POC__
	if (OID == 1084)
	{
		EMEntry fakeEntry;
		fakeEntry.range.start = (1LL << 54);
		fakeEntry.range.size = 4;
#if 0
		fakeEntry.fileID = 1084;
		fakeEntry.blockOffset = 0;
		fakeEntry.HWM = 1;
		fakeEntry.partitionNum = 0;
		fakeEntry.segmentNum = 0;
		fakeEntry.dbRoot = 1;
		fakeEntry.colWid = 4;
		fakeEntry.status = EXTENTAVAILABLE;
		fakeEntry.partition.cprange.hi_val = numeric_limits<int64_t>::min() + 2;
		fakeEntry.partition.cprange.lo_val = numeric_limits<int64_t>::max();
		fakeEntry.partition.cprange.sequenceNum = 0;
		fakeEntry.partition.cprange.isValid = CP_INVALID;
#endif
		ranges.push_back(fakeEntry.range);
		return;
	}
#endif

	int i, emEntries;
	LBIDRange tmp;

	ranges.clear();

	if (OID < 0) {
		ostringstream oss;
		oss << "ExtentMap::lookup(): invalid OID requested: " << OID;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	grabEMEntryTable(READ);
	emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (i = 0 ; i < emEntries; i++)
		if ((fExtentMap[i].fileID     == OID) &&
			(fExtentMap[i].range.size != 0) &&
			(fExtentMap[i].status     != EXTENTOUTOFSERVICE)) {
			tmp.start = fExtentMap[i].range.start;
			tmp.size = fExtentMap[i].range.size * 1024;
			ranges.push_back(tmp);
		}
	releaseEMEntryTable(READ);
}


int ExtentMap::checkConsistency()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("checkConsistency");
#endif

	/*
	 LBID space consistency checks
		1. verify that every LBID is either in the EM xor the freelist
			a. for every segment in the EM, make sure there is no overlapping entry in the FL
			b. scan both lists to verify that the entire space is represented
		2. verify that there are no adjacent entries in the freelist
	 OID consistency
		3. make sure there are no gaps in the file offsets
		4. make sure that only the last extent has a non-zero HWM
	 Struct integrity
		5. verify that the number of entries in each table is consistent with 
			the recorded current size
	*/

 	LBID_t emBegin, emEnd, flBegin, flEnd;
	int i, j, flEntries, emEntries;
	uint32_t usedEntries;

	grabEMEntryTable(READ);
	try {
		grabFreeList(READ);
	}
	catch (...) {
		releaseEMEntryTable(READ);
		throw;
	}

	flEntries = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);
	emEntries = fEMShminfo->allocdSize/sizeof(EMEntry);

	// test 1a - make sure every entry in the EM is not overlapped by an entry in the FL
	for (i = 0; i < emEntries; i++) {
		if (fExtentMap[i].range.size != 0) {
			emBegin = fExtentMap[i].range.start;
			emEnd = emBegin + (fExtentMap[i].range.size*1024) - 1;

			for (j = 0; j < flEntries; j++) {
				if (fFreeList[j].size != 0) {
					flBegin = fFreeList[j].start;
					flEnd = flBegin + (fFreeList[j].size*1024) - 1;

					//em entry overlaps the beginning
					//em entry is contained within
					//em entry overlaps the end
					if ((emBegin <= flBegin && emEnd >= flBegin) ||
						(emBegin >= flBegin && emEnd <= flEnd) ||
						(emBegin <= flEnd && emEnd >= flEnd)) {
						cerr << "EM::checkConsistency(): Improper LBID allocation detected" << endl;
						throw logic_error("EM checkConsistency test 1a (data structures are read-locked)");
					}
				}
			}
		}
	}
	cout << "test 1a passed\n";

	//test 1b - verify that the entire LBID space is accounted for

	int lbid, oldlbid;

	lbid = 0;
	while (lbid < 67108864) {    // 2^26  (2^36/1024)
		oldlbid = lbid;
		for (i = 0; i < flEntries; i++) {
 			if (fFreeList[i].start % 1024 != 0) {
 				cerr << "EM::checkConsistency(): A freelist entry is not 1024-block aligned" << endl;
 				throw logic_error("EM checkConsistency test 1b (data structures are read-locked)");
 			}
			if (fFreeList[i].start/1024 == lbid)
				lbid += fFreeList[i].size;
		}
		for (i = 0; i < emEntries; i++) {
 			if (fExtentMap[i].range.start % 1024 != 0) {
				cerr << "EM::checkConsistency(): An extent map entry is not 1024-block aligned " <<i << " " << fExtentMap[i].range.start <<  endl;
 				throw logic_error("EM checkConsistency test 1b (data structures are read-locked)");
 			}
			if (fExtentMap[i].range.start/1024 == lbid)
				lbid += fExtentMap[i].range.size;
		}
		if (oldlbid == lbid) {
			cerr << "EM::checkConsistency(): There is a gap in the LBID space at block #" <<
					static_cast<uint64_t>(lbid*1024) << endl;
			throw logic_error("EM checkConsistency test 1b (data structures are read-locked)");
		}
	}
	cout << "test 1b passed\n";

	// test 1c - verify that no dbroot is < 1
	bool errorOut = false;
	for (i = 0; i < emEntries; i++) {
		if (fExtentMap[i].range.size != 0) {
			//cout << "EM[" << i << "]: dbRoot=" << fExtentMap[i].dbRoot(listMan) << endl;
			if (fExtentMap[i].dbRoot == 0) {
				errorOut = true;
				cerr << "EM::checkConsistency(): index " << i << " has a 0 dbroot\n";
			}
		}
	}
	if (errorOut)
		throw logic_error("EM checkConsistency test 1c (data structures are read-locked)");

	cout << "test 1c passed\n";

#if 0  // a test ported from the tek2 branch, which requires a RID field to be stored; not relevant here
	// test 1d - verify that each <OID, RID> pair is unique
	cout << "Running test 1d\n";

	set<OIDRID> uniquer;
	for (i = 0; i < emEntries; i++) {
		if (fExtentMap[i].size != 0 && !fExtentMap[i].isDict()) {
			OIDRID element(fExtentMap[i].fileID, fExtentMap[i].rid);
			if (uniquer.insert(element).second == false)
				throw logic_error("EM consistency test 1d failed (data structures are read-locked)");
		}
	}
	uniquer.clear();
	cout << "Test 1d passed\n";
#endif

	// test 2 - verify that the freelist is consolidated
	for (i = 0; i < flEntries; i++) {
		if (fFreeList[i].size != 0) {
			flEnd = fFreeList[i].start + (fFreeList[i].size * 1024);
			for (j = i + 1; j < flEntries; j++) 
				if (fFreeList[j].size != 0 && fFreeList[j].start == flEnd)
					throw logic_error("EM checkConsistency test 2 (data structures are read-locked)");
		}
	}
	cout << "test 2 passed\n";

// needs to be updated
#if 0
	// test 3 - scan the extent map to make sure files have no LBID gaps
	vector<OID_t> oids;
	vector< vector<uint32_t> > fbos;

	for (i = 0; i < emEntries; i++) {
		if (fExtentMap[i].size != 0) {
			for (j = 0; j < (int)oids.size(); j++)
				if (oids[j] == fExtentMap[i].fileID)
					break;
			if (j == (int)oids.size()) {
				oids.push_back(fExtentMap[i].fileID);
				fbos.push_back(vector<uint32_t>());
			}
			fbos[j].push_back(fExtentMap[i].blockOffset);
		}
	}

	for (i = 0; i < (int)fbos.size(); i++)
		sort<vector<uint32_t>::iterator>(fbos[i].begin(), fbos[i].end());

	const unsigned EXTENT_SIZE = getExtentSize();
	for (i = 0; i < (int)fbos.size(); i++) {
		for (j = 0; j < (int)fbos[i].size(); j++) {
			if (fbos[i][j] != static_cast<uint32_t>(j * EXTENT_SIZE)) {
				cerr << "EM: OID " << oids[i] << " has no extent at FBO " <<
						j * EXTENT_SIZE << endl;
				throw logic_error("EM checkConsistency test 3 (data structures are read-locked)");
			}
		}
	}

	fbos.clear();
	oids.clear();
#endif


	// test 5a - scan freelist to make sure the current size is accurate

	for (i = 0, usedEntries = 0; i < emEntries; i++)
		if (fExtentMap[i].range.size != 0)
			usedEntries++;

	if (usedEntries != fEMShminfo->currentSize/sizeof(EMEntry)) {
		cerr << "checkConsistency: used extent map entries = " << usedEntries 
				<< " metadata says " << fEMShminfo->currentSize/sizeof(EMEntry)
				<< endl;
		throw logic_error("EM checkConsistency test 5a (data structures are read-locked)");
	}

	for (i = 0, usedEntries = 0; i < flEntries; i++)
		if (fFreeList[i].size != 0)
			usedEntries++; 

	if (usedEntries != fFLShminfo->currentSize/sizeof(InlineLBIDRange)) {
		cerr << "checkConsistency: used freelist entries = " << usedEntries 
				<< " metadata says " << fFLShminfo->currentSize/sizeof(InlineLBIDRange) 
				<< endl;
		throw logic_error("EM checkConsistency test 5a (data structures are read-locked)");
	}
	cout << "test 5a passed\n";

	releaseFreeList(READ);
	releaseEMEntryTable(READ);
	return 0;
}


void ExtentMap::setReadOnly()
{
	r_only = true;
}

void ExtentMap::undoChanges()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("undoChanges");
#endif
	Undoable::undoChanges();
	finishChanges();
}

void ExtentMap::confirmChanges()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("confirmChanges");
#endif
	Undoable::confirmChanges();
	finishChanges();
}

void ExtentMap::finishChanges()
{
	if (flLocked)
		releaseFreeList(WRITE);
	if (emLocked)
		releaseEMEntryTable(WRITE);
}

const bool * ExtentMap::getEMFLLockStatus()
{
	return &flLocked;
}

const bool * ExtentMap::getEMLockStatus()
{
	return &emLocked;
}

//------------------------------------------------------------------------------
// Reload Config cache if config file time stamp has changed
//------------------------------------------------------------------------------
void ExtentMap::checkReloadConfig()
{
	config::Config* cf = config::Config::makeConfig();

	// Immediately return if Calpont.xml timestamp has not changed
	if (cf->getCurrentMTime() == fCacheTime)
		return;

	//--------------------------------------------------------------------------
	// Initialize outdated attribute still used by primitiveserver.
	// Hardcode to 8K for now, since that's all we support.
	//--------------------------------------------------------------------------
	ExtentSize = 0x2000;

//	string es = cf->getConfig("ExtentMap", "ExtentSize");
//	if (es.length() == 0) es = "8K";
//	if (es == "8K" || es == "8k")
//	{
//		ExtentSize = 0x2000;
//	}
//	else if (es == "1K" || es == "1k")
//	{
//		ExtentSize = 0x400;
//	}
//	else if (es == "64K" || es == "64k")
//	{
//		ExtentSize = 0x10000;
//	}
//	else
//	{
//		throw logic_error("Invalid ExtentSize found in config file!");
//	}

	//--------------------------------------------------------------------------
	// Initialize number of rows per extent
	// Hardcode to 8M for now, since that's all we support.
	//--------------------------------------------------------------------------
	ExtentRows = 0x800000;

//	string er = cf->getConfig("ExtentMap", "ExtentRows");
//	if (er.length() == 0) er = "8M";
//	if (er == "8M" || er == "8m")
//	{
//		ExtentRows = 0x800000;
//	}
//	else if (er == "1M" || er == "1m")
//	{
//		ExtentRows = 0x100000;
//	}
//	else if (er == "64M" || er == "64m")
//	{
//		ExtentRows = 0x4000000;
//	}
//	else
//	{
//		throw logic_error("Invalid ExtentRows found in config file!");
//	}

	//--------------------------------------------------------------------------
	// Initialize segment files per physical partition
	//--------------------------------------------------------------------------
	string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
	filesPerColumnPartition = cf->uFromText(fpc);
	if (filesPerColumnPartition == 0)
		filesPerColumnPartition = 4;

	// Get latest Calpont.xml timestamp after first access forced a reload
	fCacheTime = cf ->getLastMTime();

	//--------------------------------------------------------------------------
	// Initialize extents per segment file
	//--------------------------------------------------------------------------
	string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
	extentsPerSegmentFile = cf->uFromText(epsf);
	if (extentsPerSegmentFile == 0)
		extentsPerSegmentFile = 2;
}

//------------------------------------------------------------------------------
// Returns the number of extents in a segment file.
// Mutex lock and call to checkReloadConfig() not currently necessary since,
// going with hardcoded value.  See checkReloadConfig().
//------------------------------------------------------------------------------
unsigned ExtentMap::getExtentSize()       // dmc-should deprecate
{
//	boost::mutex::scoped_lock lk(fConfigCacheMutex);
//	checkReloadConfig( );

	ExtentSize = 0x2000;
	return ExtentSize;
}

//------------------------------------------------------------------------------
// Returns the number or rows per extent.  Only supported values are 1m, 8m,
// and 64m.
// Mutex lock and call to checkReloadConfig() not currently necessary since,
// going with hardcoded value.  See checkReloadConfig().
//------------------------------------------------------------------------------
unsigned ExtentMap::getExtentRows()
{
//	boost::mutex::scoped_lock lk(fConfigCacheMutex);
//	checkReloadConfig( );

	ExtentRows = 0x800000;
	return ExtentRows;
}

//------------------------------------------------------------------------------
// Returns the number of column segment files for an OID, that make up a
// partition.
//------------------------------------------------------------------------------
unsigned ExtentMap::getFilesPerColumnPartition()
{
	boost::mutex::scoped_lock lk(fConfigCacheMutex);
	checkReloadConfig( );

	return filesPerColumnPartition;
}

//------------------------------------------------------------------------------
// Returns the number of extents in a segment file.
//------------------------------------------------------------------------------
unsigned ExtentMap::getExtentsPerSegmentFile()
{
	boost::mutex::scoped_lock lk(fConfigCacheMutex);
	checkReloadConfig( );

	return extentsPerSegmentFile;
}

//------------------------------------------------------------------------------
// Returns the number of DBRoots to be used in storing db column files.
//------------------------------------------------------------------------------
unsigned ExtentMap::getDbRootCount()
{
	oam::OamCache* oamcache = oam::OamCache::makeOamCache();
	unsigned int rootCnt = oamcache->getDBRootCount();

	return rootCnt;
}

//------------------------------------------------------------------------------
// Get list of DBRoots that map to the specified PM.  DBRoot list is cached
// internally in fPmDbRootMap after getting from Calpont.xml via OAM.
//------------------------------------------------------------------------------
void ExtentMap::getPmDbRoots( int pm, vector<int>& dbRootList )
{
	oam::OamCache* oamcache = oam::OamCache::makeOamCache();
	oam::OamCache::PMDbrootsMap_t pmDbroots = oamcache->getPMToDbrootsMap();

	dbRootList.clear();
	dbRootList = (*pmDbroots)[pm];
}

vector<InlineLBIDRange> ExtentMap::getFreeListEntries()
{
	vector<InlineLBIDRange> v;
	grabEMEntryTable(READ);
	grabFreeList(READ);

	int allocdSize = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);

	for (int i = 0; i < allocdSize; i++)
		v.push_back(fFreeList[i]);

	releaseFreeList(READ);
	releaseEMEntryTable(READ);
	return v;
}

void ExtentMap::dumpTo(ostream& os)
{
	grabEMEntryTable(READ);
	unsigned emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (unsigned i = 0; i < emEntries; i++)
	{
		if (fExtentMap[i].range.size != 0) {
			os << fExtentMap[i].range.start << '|'
				<< fExtentMap[i].range.size << '|'
				<< fExtentMap[i].fileID << '|'
				<< fExtentMap[i].blockOffset << '|'
				<< fExtentMap[i].HWM << '|'
				<< fExtentMap[i].partitionNum << '|'
				<< fExtentMap[i].segmentNum << '|'
				<< fExtentMap[i].dbRoot << '|'
				<< fExtentMap[i].colWid << '|'
				<< fExtentMap[i].status << '|'
				<< fExtentMap[i].partition.cprange.hi_val << '|'
				<< fExtentMap[i].partition.cprange.lo_val << '|'
				<< fExtentMap[i].partition.cprange.sequenceNum << '|'
				<< (int)fExtentMap[i].partition.cprange.isValid << '|'
				<< endl;
		}
	}
	releaseEMEntryTable(READ);
}

/*int ExtentMap::physicalPartitionNum(const set<OID_t>& oids,
	                       const set<uint32_t>& partitionNums, 
	                       vector<PartitionInfo>& partitionInfos)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("physicalPartitionNum");
		ostringstream oss;
		set<uint32_t>::const_iterator partIt;
		oss << "partitionNums: " 
		for (partIt=partitionNums.begin(); it!=partitionNums.end(); ++it)
			oss << (*it) << " ";
		oss << endl;
		TRACER_WRITEDIRECT(oss.str());
	}
#endif

	set<OID_t>::const_iterator it;
	grabEMEntryTable(READ);

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	PartitionInfo partInfo;
	vector<uint32_t> extents;
	set<uint32_t> foundPartitions;
	for (int i = 0; i < emEntries; i++) 
	{
		if ((fExtentMap[i].range.size  != 0  ) && 
			partitionNums.find(logicalPartitionNum(fExtentMap[i])) != partitionNums.end())
		{
			it = oids.find( fExtentMap[i].fileID );
			if (it != oids.end()) 
			{
				partInfo.oid = fExtentMap[i].fileID;
				partInfo.lp.dbroot = fExtentMap[i].dbRoot;
				partInfo.lp.pp = fExtentMap[i].partitionNum;
				partInfo.lp.seg = fExtentMap[i].segmentNum;
				partitionInfos.push_back(partInfo);
			}
		}
	}
	releaseEMEntryTable(READ);
	return 0;
}
*/

}	//namespace
// vim:ts=4 sw=4:

