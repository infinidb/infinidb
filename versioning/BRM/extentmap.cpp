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
 * $Id: extentmap.cpp 1586 2012-06-06 19:32:57Z dcathey $
 *
 ****************************************************************************/

#include <iostream>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <algorithm>
#include <ios>
#include <cerrno>
#include <sstream>
#include <fstream>
#include <vector>
#include <boost/scoped_array.hpp>
#include <limits>
#include <boost/thread.hpp>

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
namespace bi=boost::interprocess;

#include "brmtypes.h"
#include "configcpp.h"
#include "rwlock.h"
#include "calpontsystemcatalog.h"
#include "mastersegmenttable.h"
#include "blocksize.h"
#include "dataconvert.h"
#ifdef BRM_INFO
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

namespace
{
unsigned ExtentSize = 0; // dmc-need to deprecate
unsigned ExtentRows              = 0;
unsigned filesPerColumnPartition = 0;
unsigned extentsPerSegmentFile   = 0;
unsigned dbRootCount             = 0;

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
// Version 3 EmEntry methods
//------------------------------------------------------------------------------

EMEntry_V3::EMEntry_V3()
{
	fileID = 0;
	blockOffset = 0;
	HWM = 0;
	txnID = 0;
	secondHWM = 0;
	nextHeader = 0;
}

EMEntry_V3::EMEntry_V3(const EMEntry_V3& e) 
{
	range.start = e.range.start;
	range.size = e.range.size;
	fileID = e.fileID;
	blockOffset = e.blockOffset;
	HWM = e.HWM;
	txnID = e.txnID;
	secondHWM = e.secondHWM;
	partition = e.partition;
	nextHeader = e.nextHeader;
}

EMEntry_V3& EMEntry_V3::operator= (const EMEntry_V3& e)
{
	range.start = e.range.start;
	range.size = e.range.size;
	fileID = e.fileID;
	blockOffset = e.blockOffset;
	HWM = e.HWM;
	txnID = e.txnID;
	secondHWM = e.secondHWM;
	partition = e.partition;
	nextHeader = e.nextHeader;
	return *this;
}

bool EMEntry_V3::operator< (const EMEntry_V3& e) const
{
	if (range.start < e.range.start)
		return true;
	return false;
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
	//int err;

	fExtentMap = NULL;
	fFreeList = NULL;
	fCurrentEMShmkey = -1;
	fCurrentFLShmkey = -1;
	fEMShmid = -1;
	fFLShmid = -1;
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

int ExtentMap::_markInvalid(const LBID_t lbid)
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
				fExtentMap[i].partition.cprange.lo_val=numeric_limits<int64_t>::max();
				fExtentMap[i].partition.cprange.hi_val=numeric_limits<int64_t>::min();
				incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);
				return 0;
			}
		}
	}
	throw logic_error("ExtentMap::markInvalid(): lbid isn't allocated");
}

int ExtentMap::markInvalid(const LBID_t lbid)
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
	
	grabEMEntryTable(WRITE);
	return _markInvalid(lbid);
}

/**
* @brief calls markInvalid(LBID_t lbid) for each extent containing any lbid in vector<LBID_t>& lbids
*
**/

int ExtentMap::markInvalid(const vector<LBID_t>& lbids)
{
	uint i, size = lbids.size();

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
		try {
			_markInvalid(lbids[i]);
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
*
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
				if (curSequence == seqNum)
				{
					ostringstream os;

					makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));
					fExtentMap[i].partition.cprange.hi_val = max;
					fExtentMap[i].partition.cprange.lo_val = min;
					fExtentMap[i].partition.cprange.isValid = CP_VALID;
					incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);
					if (firstNode) {
						os << "ExtentMap::setMaxMin(): casual partitioning update: firstLBID=" <<
							fExtentMap[i].range.start << " lastLBID=" << fExtentMap[i].range.start +
							fExtentMap[i].range.size*1024 - 1 << " OID=" << fExtentMap[i].fileID <<
							" min=" << fExtentMap[i].partition.cprange.lo_val << " max=" <<
							fExtentMap[i].partition.cprange.hi_val << " seq=" <<
							fExtentMap[i].partition.cprange.sequenceNum;
						log(os.str(), logging::LOG_TYPE_DEBUG);
					}
					return 0;
				}
				//special val to indicate a reset--used by editem -c.
				//Also used by COMMIT and ROLLBACK to invalidate CP.
				else if (seqNum == -1)
				{
					makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));
					fExtentMap[i].partition.cprange.hi_val = max;
					fExtentMap[i].partition.cprange.lo_val = min;
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
					ostringstream os;

					makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));
					fExtentMap[i].partition.cprange.hi_val = it->second.max;
					fExtentMap[i].partition.cprange.lo_val = it->second.min;
					fExtentMap[i].partition.cprange.isValid = CP_VALID;
					incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);
					extentsUpdated++;
					if (firstNode) {
						os << "ExtentMap::setExtentsMaxMin(): casual partitioning update: firstLBID=" <<
							fExtentMap[i].range.start << " lastLBID=" << fExtentMap[i].range.start +
							fExtentMap[i].range.size*1024 - 1 << " OID=" << fExtentMap[i].fileID <<
							" min=" << fExtentMap[i].partition.cprange.lo_val << " max=" <<
							fExtentMap[i].partition.cprange.hi_val << " seq=" <<
							fExtentMap[i].partition.cprange.sequenceNum;
						log(os.str(), logging::LOG_TYPE_DEBUG);
					}
				}
				//special val to indicate a reset--used by editem -c
				else if (it->second.seqNum == -1)
				{
					makeUndoRecord(&fExtentMap[i], sizeof(struct EMEntry));
					fExtentMap[i].partition.cprange.hi_val = it->second.max;
					fExtentMap[i].partition.cprange.lo_val = it->second.min;
					fExtentMap[i].partition.cprange.isValid = CP_INVALID;
					incSeqNum(fExtentMap[i].partition.cprange.sequenceNum);
					extentsUpdated++;
				}
				// else sequnce has changed since start of the query.  Don't update the EM entry.
				else
					extentsUpdated++;

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

#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("mergeExtentsMaxMin");
		unsigned int count = 1;
		for(it = cpMap.begin(); it != cpMap.end(); ++it)
		{
			std::ostringstream oss;
			oss << "  "   << count                <<
				". LBID: "<< (*it).first          <<
				"; max: " << (*it).second.max     <<
				"; min: " << (*it).second.min     <<
				"; seq: " << (*it).second.seqNum  <<
				"; chr: " << (*it).second.isChar  <<
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
				switch (fExtentMap[i].partition.cprange.isValid)
				{
					// Merge input min/max with current min/max
					case CP_VALID:
					{
						if (!isValidCPRange( it->second.max,
											 it->second.min ))
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
							fExtentMap[i].partition.cprange.lo_val))
						{
							// Swap byte order to do binary string comparison
							if (it->second.isChar)
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
												it->second.min ))
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
// (min()+1).
//------------------------------------------------------------------------------
bool ExtentMap::isValidCPRange(int64_t max, int64_t min) const
{
	if ( (min <= (numeric_limits<int64_t>::min()+1)) ||
		 (max <= (numeric_limits<int64_t>::min()+1)) )
	{
		return false;
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

void ExtentMap::writeData(int fd, u_int8_t *buf, off_t offset, int size) const
{
	int errCount, err, progress;
	off_t seekerr = -1;
	
	for (errCount = 0; errCount < MAX_IO_RETRIES && seekerr != offset; errCount++) {
		seekerr = lseek(fd, offset, SEEK_SET);
		if (seekerr < 0)
			log_errno(string("ExtentMap::writeData(): lseek"));
	}
	if (errCount == MAX_IO_RETRIES) {
		log(string("ExtentMap:writeData(): lseek failed too many times"));
		throw std::ios_base::failure("ExtentMap::writeData(): lseek failed "
				"too many times");
	}
	
	for (progress = 0, errCount = 0; progress < size && errCount < MAX_IO_RETRIES;) {
		err = write(fd, &buf[progress], size - progress);
		if (err < 0) {
			if (errno != EINTR) {  // EINTR isn't really an error
				errCount++;
				log_errno(string("ExtentMap::writeData(): write (retrying)"));
			}
		}
		else 
			progress += err;		
	}
	
	if (errCount == MAX_IO_RETRIES) 
		throw std::ios_base::failure("ExtentMap::writeData(): write error");	
}


void ExtentMap::readData(int fd, u_int8_t *buf, off_t offset, int size)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("readData");
		TRACER_ADDINPUT(fd);
		TRACER_ADDINPUT(offset);
		TRACER_ADDINPUT(size);
		TRACER_WRITE;
	}	
#endif

	int errCount, err, progress;
	off_t seekerr = -1;
	
	for (errCount = 0; errCount < MAX_IO_RETRIES && seekerr != offset; errCount++) {
		seekerr = lseek(fd, offset, SEEK_SET);
		if (seekerr < 0)
			log_errno("ExtentMap::readData(): lseek (retrying)");
	}
	if (errCount == MAX_IO_RETRIES) {
		log("ExtentMap::readData(): lseek failed too many times");
		throw std::ios_base::failure("ExtentMap::readData(): lseek failed "
				"too many times");
	}

	/* XXXPAT:  for later: check that there are 'size' bytes to read */	

	for (progress = 0, errCount = 0; progress < size && errCount < MAX_IO_RETRIES;) {
		err = read(fd, &buf[progress], size - progress);
		if (err < 0) {
			if (errno != EINTR) {  // EINTR isn't really an error
				errCount++; 
				log_errno("ExtentMap::readData(): read (retrying)");
			}
		}
		else if (err > 0)
			progress += err;		
		else
			errCount++;		// count early EOF as an error
	}
	if (errCount == MAX_IO_RETRIES) {
		log("ExtentMap::readData(): read failed too many times");
		throw std::ios_base::failure("ExtentMap::readData(): read error");	
	}
}	

/* 
	The file layout looks like this:

	EM Magic (32-bits)
	number of EM entries  (32-bits)
	number of FL entries  (32-bits)
	struct EMEntry
	    ...   (* numEM)
	struct InlineLBIDRange
		...   (* numFL)
*/

void ExtentMap::load(const std::string& filename, bool fixFL)
{	
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("load");
		TRACER_ADDSTRINPUT(filename);
		TRACER_WRITE;
	}	
#endif

	int currentSize, loadSize[3];
	ifstream in;

	grabEMEntryTable(WRITE);
	try {
		grabFreeList(WRITE);
	}
	catch(...) {
		releaseEMEntryTable(WRITE);
		throw;
	}

	in.open(filename.c_str(), ios_base::in|ios_base::binary);
	if (!in) {
		log_errno("ExtentMap::load(): open");
		releaseFreeList(WRITE);
		releaseEMEntryTable(WRITE);
		throw std::ios_base::failure("ExtentMap::load(): open failed. Check the error log.");
	}
	
	in.exceptions(ios_base::badbit | ios_base::failbit);
	
	try {
		in.read((char *) &loadSize, 3*sizeof(int));
	} 
	catch(...) {
		in.close();
		releaseFreeList(WRITE);
		releaseEMEntryTable(WRITE);
		throw;
	}
	
	const int emVersion = loadSize[0];
	const size_t emNumElements = loadSize[1];
	int flNumElements = loadSize[2];
 	int emEntrySz=0;

	/* What's a safe upper limit on the # of EM and FL entries? */
	/* No backwards compatability between version 4 and previous version */
	if ( !(emVersion==EM_MAGIC_V4) || emNumElements < 0 || flNumElements < 0) {
		in.close();
		releaseFreeList(WRITE);
		releaseEMEntryTable(WRITE);
		log("ExtentMap::load(): That file is not a valid ExtentMap image");
		throw std::runtime_error("ExtentMap::load(): That file is not a valid ExtentMap image");
	}

	if (!fixFL && emNumElements > 0 && flNumElements == 0)
	{
		in.close();
		releaseFreeList(WRITE);
		releaseEMEntryTable(WRITE);
		log("ExtentMap::load(): there are no free list entries in that file");
		throw std::runtime_error("ExtentMap::load(): there are no free list entries in that file");
	}

	if (fixFL) flNumElements = 1;

	memset(fExtentMap, 0, fEMShminfo->allocdSize);
	memset(fFreeList, 0, fFLShminfo->allocdSize);
	fEMShminfo->currentSize = 0;
	fFLShminfo->currentSize = 0;

	// determine which version of the extentmap is in the backup file
	// set element size according to version in the backup file.
	// Version 4 not compatible with previous versions.  This code was
    // left in place as an example for how to handle conversion if we
	// wanted to do so in the future. The version check at the front of
    // this function currently blocks any previous version.

	if (emVersion == EM_MAGIC_V3)
		emEntrySz=sizeof(struct EMEntry_V3);
	else
		emEntrySz=sizeof(struct EMEntry);

	// @Bug 3498
	// Calculate how big an extent map we're going to need and allocate it in one call
	if ((fEMShminfo->allocdSize / sizeof(EMEntry)) < emNumElements)
	{
		size_t nrows = emNumElements;
		//Round up to the nearest EM_INCREMENT_ROWS
		if ((nrows % EM_INCREMENT_ROWS) != 0)
		{
			nrows /= EM_INCREMENT_ROWS;
			nrows++;
			nrows *= EM_INCREMENT_ROWS;
		}
		growEMShmseg(nrows);
	}

	// allocate shared memory for freelist
	for (currentSize = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);
			currentSize < flNumElements; 
			currentSize = fFLShminfo->allocdSize/sizeof(InlineLBIDRange)) {
		growFLShmseg();
	}
	
	try {
		/* XXXPAT: in the valgrind/memcheck environment, for some reason the 
		OS gives EFAULT on a read directly into shmsegs */
	
		scoped_array<u_int8_t> buf(new u_int8_t[emNumElements * emEntrySz]);
		scoped_array<u_int8_t> buf2(new u_int8_t[flNumElements * sizeof(InlineLBIDRange)]);

		in.read((char *) buf.get(), emNumElements * emEntrySz);

		// Version 4 not compatible with previous versions.  This code was
    	// left in place as an example for how to handle conversion if we
		// wanted to do so in the future. The version check at the front of
    	// this function currently blocks any previous version.
		if (emVersion == EM_MAGIC_V3) {
			//u_int8_t v3Copy[emNumElements * sizeof(struct EMEntry_V3)];
			uint8_t* v3Copy = (uint8_t*)alloca(emNumElements * sizeof(struct EMEntry_V3));
			//u_int8_t v4Copy[emNumElements * sizeof(struct EMEntry)];
			uint8_t* v4Copy = (uint8_t*)alloca(emNumElements * sizeof(struct EMEntry));
			memcpy(v3Copy, buf.get(), emNumElements*sizeof(struct EMEntry_V3));
			v3Tov4(v3Copy, v4Copy, emNumElements);
			memcpy(fExtentMap, v4Copy, emNumElements * sizeof(struct EMEntry));
		}
		else {
			memcpy(fExtentMap, buf.get(), emNumElements * sizeof(struct EMEntry));

			//@bug 1911 - verify status value is valid
			for(size_t idx=0; idx < emNumElements; idx++)
				if (fExtentMap[idx].status<EXTENTSTATUSMIN ||
					fExtentMap[idx].status>EXTENTSTATUSMAX)
					fExtentMap[idx].status=EXTENTAVAILABLE;
		}

#ifdef DUMP_EXTENT_MAP
		EMEntry* emSrc = reinterpret_cast<EMEntry*>(buf.get());
		cout << "lbid\tsz\toid\tfbo\thwm\tpart#\tseg#\tDBRoot\twid\tst\thi\tlo\tsq\tv" << endl;
		for (int i = 0; i < emNumElements; i++)
		{
			cout << 
			emSrc[i].range.start
			<< '\t' << emSrc[i].range.size
			<< '\t' << emSrc[i].fileID
			<< '\t' << emSrc[i].blockOffset
			<< '\t' << emSrc[i].HWM
			<< '\t' << emSrc[i].partitionNum
			<< '\t' << emSrc[i].segmentNum
			<< '\t' << emSrc[i].dbRoot
			<< '\t' << emSrc[i].colWid
			<< '\t' << emSrc[i].status
			<< '\t' << emSrc[i].partition.cprange.hi_val
			<< '\t' << emSrc[i].partition.cprange.lo_val
			<< '\t' << emSrc[i].partition.cprange.sequenceNum
			<< '\t' << (int)(emSrc[i].partition.cprange.isValid)
			<< endl;
		}
#endif

		fEMShminfo->currentSize = emNumElements * sizeof(struct EMEntry);

		if (fixFL)
		{
			LBID_t maxStart = 0;
			uint32_t newSize = (1ULL << 36) / 1024;
			EMEntry* emSrc = reinterpret_cast<EMEntry*>(buf.get());
			for (size_t i = 0; i < emNumElements; i++)
			{
				if (emSrc[i].range.start > maxStart)
				{
					maxStart = emSrc[i].range.start;
					newSize = emSrc[i].range.size;
				}
			}
			InlineLBIDRange fixedFL;
			fixedFL.start = maxStart + newSize * 1024;
			fixedFL.size = (1ULL << 36) / 1024 - fixedFL.start / 1024;
			memcpy(buf2.get(), &fixedFL, sizeof(InlineLBIDRange));
		}
		else
		{
			in.read((char *) buf2.get(), flNumElements * sizeof(InlineLBIDRange));
		}
		memcpy(fFreeList, buf2.get(), flNumElements * sizeof(InlineLBIDRange));
		fFLShminfo->currentSize = 0;
		for (int i = 0; i < flNumElements; i++)
			if (fFreeList[i].size != 0)
				fFLShminfo->currentSize += sizeof(InlineLBIDRange);
		
		//fFLShminfo->currentSize = flNumElements * sizeof(InlineLBIDRange);
#ifdef DUMP_EXTENT_MAP
		cout << "Free list entries:" << endl;
		cout << "start\tsize" << endl;
		for (int i = 0; i < flNumElements; i++)
		{
			cout <<
			fFreeList[i].start
			<< '\t' << fFreeList[i].size
			<< endl;
		}
#endif
	} catch(...) {
		in.close();
		releaseFreeList(WRITE);
		releaseEMEntryTable(WRITE);
		throw;
	}
	
	in.close();
	releaseFreeList(WRITE);
	releaseEMEntryTable(WRITE);
}

//------------------------------------------------------------------------------
// Convert version 3 data to version 4 data
// v3 - byte array of v3 EMEntry data. source data
// v4 - empty byte array data of v4 EMEntry data. destination data.
//
// Version 4 not compatible with previous versions.  This function was
// included as an example for how to handle conversion if we wanted to
// do so for future version changes.
//------------------------------------------------------------------------------
void ExtentMap::v3Tov4(uint8_t* v3, uint8_t* v4, const uint32_t emNum)
{
	EMEntry_V3* v3Buf = reinterpret_cast<EMEntry_V3*>(v3);
	EMEntry* v4Buf = reinterpret_cast<EMEntry*>(v4);

	for (uint32_t idx=0; idx < emNum; idx++)
	{
		v4Buf[idx].range.start 	= v3Buf[idx].range.start;
		v4Buf[idx].range.size 	= v3Buf[idx].range.size;
		v4Buf[idx].fileID 		= v3Buf[idx].fileID;
		v4Buf[idx].blockOffset 	= v3Buf[idx].blockOffset;
		v4Buf[idx].HWM 			= v3Buf[idx].HWM;

		v4Buf[idx].partition.cprange = v3Buf[idx].partition.cprange;
	
		v4Buf[idx].partitionNum = 0;
		v4Buf[idx].segmentNum   = 0;
		v4Buf[idx].dbRoot       = 0;
		v4Buf[idx].colWid       = 0;
	}
}

void ExtentMap::save(const std::string& filename)
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
	ofstream out;

	// Make em writes to disk use a buffer size of StrmBufSize bytes (instead of the default 8K)
	const unsigned StrmBufSize = 1 * 1024 * 1024;
	scoped_array<char> buf(new char[StrmBufSize]);
	out.rdbuf()->pubsetbuf(buf.get(), StrmBufSize);

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

	utmp = ::umask(0);
	out.open(filename.c_str(), ios_base::out|ios_base::binary);
	::umask(utmp);
	if (!out) {
		log_errno("ExtentMap::save(): open");
		releaseFreeList(READ);
		releaseEMEntryTable(READ);
		throw std::ios_base::failure("ExtentMap::save(): open failed. Check the error log.");
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
//		if (fFreeList[i].size > 0) {
			try {
				out.write((char *) &fFreeList[i], sizeof(InlineLBIDRange));
			}
			catch(...) {
				out.close();
				releaseFreeList(READ);
				releaseEMEntryTable(READ);
				throw;
			}
//		}
	}
	out.close();
	releaseFreeList(READ);
	releaseEMEntryTable(READ);
}

/* always returns holding the EM lock, and with the EM seg mapped */
void ExtentMap::grabEMEntryTable(OPS op)
{
	mutex::scoped_lock lk(mutex);

	if (op == READ) {
		/* 
		XXXPAT: possible race condition here.  fEMShminfo is read in 
		many places.  Given that the assigned value is always the same
		during read access, is there any possibility that the assignments 
		here could mess up a simultaneous read?  (Same for fFLShminfo)

		If so, we can make a second mutex for the freelist side, then
		use each mutex to wrap each grab* function entirely, but that
		would preempt the scheduling policy implemented in RWLock.

		NOTE: this code is the same in CopyLocks, VSS, and VBBM.
		*/
		fEMShminfo = fMST.getTable_read(MasterSegmentTable::EMTable);
 		(void)0; //lk.lock();
	}
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
 				(void)0; //lk.unlock();
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
				throw std::runtime_error("ExtentMap::grabEMEntryTable(): shmat failed.  Check the error log.");
			}
			if (op == READ)
 				(void)0; //lk.unlock();
		}
	}
	else 
	{
		fExtentMap = fPExtMapImpl->get();
		if (op == READ)
 			(void)0; //lk.unlock();
	}
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
				throw std::runtime_error("ExtentMap::grabFreeList(): shmat failed.  Check the error log.");
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
	allocSize = std::max(allocSize, nrows * sizeof(EMEntry));

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
int ExtentMap::lookupLocal(LBID_t lbid, int& OID, uint16_t& dbRoot, uint32_t& partitionNum, uint16_t& segmentNum, u_int32_t& fileBlockOffset)
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

        releaseEMEntryTable(READ);
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
	int entries, i, offset;

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
			offset = fileBlockOffset - fExtentMap[i].blockOffset;
			LBID = fExtentMap[i].range.start;
			releaseEMEntryTable(READ);
			return 0;
		}
	}
	releaseEMEntryTable(READ);

	return -1;
}

//------------------------------------------------------------------------------
// Creates an extent for a column file.  This is the external API function.
// input:
//   OID          - column OID for which the extent is to be created
//   colWidth     - width of column in bytes
//   dbRoot       - when creating the first extent for a column,
//                  dbRoot must be specified as an input argument.
//   partitionNum - when creating the first extent for a column,
//                  partitionNum must be specified as an input argument.
// output:
//   dbRoot       - when adding an extent to a column,
//                  dbRoot will be the DBRoot assigned to the new extent
//   partitionNum - when adding an extent to a column,
//                  partitionNum will be the assigned partition number
//   segmentNum   - segment number assigned to the new extent
//   lbid         - starting LBID of the created extent
//   allocdsize   - number of LBIDs allocated 
//   startBlockOffset-starting block of the created extent
//------------------------------------------------------------------------------
void ExtentMap::createColumnExtent(int OID,
	u_int32_t  colWidth,
	u_int16_t& dbRoot,
	u_int32_t& partitionNum,
	u_int16_t& segmentNum,
	LBID_t&    lbid,
	int&       allocdsize,
	u_int32_t& startBlockOffset)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("createColumnExtent");
		TRACER_ADDINPUT(OID);
		TRACER_ADDINPUT(colWidth);
		TRACER_ADDSHORTOUTPUT(dbRoot);
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
		log("ExtentMap::createColumnExtent(): OID must be > 0",
			logging::LOG_TYPE_DEBUG);
		throw std::invalid_argument(
			"ExtentMap::createColumnExtent(): OID must be > 0");
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
	u_int32_t size = EXTENT_SIZE/1024;

	lbid = _createColumnExtent(size, OID, colWidth,
		dbRoot, partitionNum, segmentNum, startBlockOffset);

	allocdsize = EXTENT_SIZE;
}

//------------------------------------------------------------------------------
// Creates an extent for a column file.  This is the internal implementation
// function.
// input:
//   size         - number of multiples of 1024 blocks allocated to the extent
//                  ex: size=1 --> 1024 blocks
//                      size=2 --> 2048 blocks
//                      size=3 --> 3072 blocks, etc.
//   OID          - column OID for which the extent is to be created
//   colWidth     - width of column in bytes
//   dbRoot       - when creating the first extent for a column,
//                  dbRoot must be specified as an input argument.
//   partitionNum - when creating the first extent for a column,
//                  partitionNum must be specified as an input argument.
// output:
//   dbRoot       - when adding an extent to a column,
//                  dbRoot will be returned as an output argument.
//   partitionNum - when adding an extent to a column,
//                  partitionNum will be the assigned partition number
//   segmentNum   - segment number assigned to the new extent
//   startBlockOffset-starting block of the created extent
// returns starting LBID of the created extent.
//------------------------------------------------------------------------------
LBID_t ExtentMap::_createColumnExtent(u_int32_t size, int OID,
	u_int32_t  colWidth,
	u_int16_t& dbRoot,
	u_int32_t& partitionNum,
	u_int16_t& segmentNum,
	u_int32_t& startBlockOffset)
{
	int emptyEMEntry        = -1;
	int lastExtentIndex     = -1;
	u_int32_t highestOffset = 0;
	u_int32_t highestPartNum= 0;
	u_int16_t highestSegNum = 0;

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	LBID_t startLBID = getLBIDsFromFreeList( size );

	// Find the first empty Entry; and find the last extent for this OID.
	for (int i = 0; i < emEntries; i++) {
		if (fExtentMap[i].range.size  != 0) {
			if (fExtentMap[i].fileID == OID) {
				if ( (fExtentMap[i].partitionNum  >  highestPartNum) ||
					((fExtentMap[i].partitionNum  == highestPartNum) &&
					 (fExtentMap[i].blockOffset   >  highestOffset)) ||
					((fExtentMap[i].partitionNum  == highestPartNum) &&
					 (fExtentMap[i].blockOffset   == highestOffset)  &&
					 (fExtentMap[i].segmentNum    >= highestSegNum)) ) {

					lastExtentIndex = i;
					highestPartNum  = fExtentMap[i].partitionNum;
					highestSegNum   = fExtentMap[i].segmentNum;
					highestOffset   = fExtentMap[i].blockOffset;
				}
			}
		}
		else if (emptyEMEntry < 0)
			emptyEMEntry = i;
	}

#ifdef BRM_DEBUG
	if (emptyEMEntry == -1) {
		log("ExtentMap::createColumnExtent(): could not find an empty EMEntry",
			logging::LOG_TYPE_DEBUG);
		throw std::logic_error(
			"ExtentMap::createColumnExtent(): could not find an empty EMEntry");
	}
#endif

	u_int16_t newDbRoot       = dbRoot;
	u_int32_t newPartitionNum = partitionNum;
	u_int16_t newSegmentNum   = 0;
	u_int32_t newBlockOffset  = 0;
	char      initialCPIsValid= CP_INVALID;

	// If this is first extent for this OID then
	//   leave part#, seg#, etc set to 0, except dbRoot which user specifies
	// else
	//   extrapolate values from last extent; wrap around segment and partition
	//   number to 0 as needed, and increment partition number when the number
	//   of rows is a multiple of the partition row count.
	if (lastExtentIndex >= 0) {
		bool startNewPartition = false;

		newSegmentNum   = fExtentMap[lastExtentIndex].segmentNum + 1;
		newPartitionNum = fExtentMap[lastExtentIndex].partitionNum;
		newDbRoot       = fExtentMap[lastExtentIndex].dbRoot     + 1;
		if (newDbRoot > getDbRootCount())
			newDbRoot = 1;
		if (newSegmentNum >= getFilesPerColumnPartition()) {
			newSegmentNum = 0;

			// Use blockOffset of lastExtentIndex to see if we need to add
			// the next extent to a new partition.
			if (fExtentMap[lastExtentIndex].blockOffset ==
				((getExtentsPerSegmentFile() - 1) *
				 (getExtentRows() * colWidth / BLOCK_SIZE)) ) {
				newPartitionNum++;
				startNewPartition = true;
			}
		}

		// Set blockOffset for new extent relative to it's seg file
		// case1: Init blockOffset to 0 if first extent in a partition
		// case2: Init blockOffset to 0 if first extent in segment file
		//        (other than segment 0, which case1 handled)
		// case3: Init blockOffset based on previous extent

		// case1: leave newBlockOffset set to 0
		if (startNewPartition) {
			//...no action necessary
		}

		// case2: leave newBlockOffset set to 0
		else if((fExtentMap[lastExtentIndex].blockOffset == 0) &&
				(newSegmentNum != 0)) {
			//...no action necessary
		}

		// case3: Init blockOffset based on previous extent.  If we are adding
		//        extent to seg file 0, then need to bump up the offset; else
		//        adding extent to same stripe and can repeat the same offset.
		else {
			if (newSegmentNum == 0) {	// start next stripe
				newBlockOffset = static_cast<u_int64_t>
					(fExtentMap[lastExtentIndex].range.size) * 1024 +
					fExtentMap[lastExtentIndex].blockOffset;
			}
			else {						// next extent, same stripe
				newBlockOffset = fExtentMap[lastExtentIndex].blockOffset;
			}
		}
	}
	else {
		// Go ahead and mark very 1st extent as valid.  At this point, it is an
		// empty column with no data, so no need to scan; so okay to mark valid.
		initialCPIsValid = CP_VALID;
	}

	makeUndoRecord(&fExtentMap[emptyEMEntry], sizeof(EMEntry));
	EMEntry* e      = &fExtentMap[emptyEMEntry];

	e->range.start  = startLBID;
	e->range.size   = size;
	e->fileID       = OID;
	e->partition.cprange.hi_val      = numeric_limits<int64_t>::min();
	e->partition.cprange.lo_val      = numeric_limits<int64_t>::max();
	e->partition.cprange.sequenceNum = 0;
	e->partition.cprange.isValid     = initialCPIsValid;
	e->colWid       = colWidth;

	e->dbRoot       = newDbRoot;
	//@Bug 3294. Use the passed in partition number for alter table drop partition.
	if ( (partitionNum != 0) && (newPartitionNum < partitionNum) )
		e->partitionNum = partitionNum;
	else
		e->partitionNum = newPartitionNum;
		
	e->segmentNum   = newSegmentNum;

	e->blockOffset  = newBlockOffset;
	e->HWM          = 0;
	e->status       = EXTENTUNAVAILABLE; // mark extent as in process

	partitionNum    = e->partitionNum;
	segmentNum      = e->segmentNum;
	dbRoot          = e->dbRoot;
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
	u_int16_t  dbRoot,
	u_int32_t  partitionNum,
	u_int16_t  segmentNum,
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
		throw std::invalid_argument(
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
	u_int32_t size = EXTENT_SIZE/1024;

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
LBID_t ExtentMap::_createDictStoreExtent(u_int32_t size, int OID,
	u_int16_t  dbRoot,
	u_int32_t  partitionNum,
	u_int16_t  segmentNum)
{
	int emptyEMEntry        = -1;
	int lastExtentIndex     = -1;
	u_int32_t highestOffset = 0;

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
	}

#ifdef BRM_DEBUG
	if (emptyEMEntry == -1) {
		log("ExtentMap::createDictStoreExtent(): could not find an empty EMEntry",
			logging::LOG_TYPE_DEBUG);
		throw std::logic_error(
			"ExtentMap::createDictStoreExtent(): could not find an empty EMEntry");
	}
#endif

	makeUndoRecord(&fExtentMap[emptyEMEntry], sizeof(EMEntry));
	EMEntry* e      = &fExtentMap[emptyEMEntry];

	e->range.start  = startLBID;
	e->range.size   = size;
	e->fileID       = OID;
	e->status       = EXTENTUNAVAILABLE;// @bug 1911 mark extent as in process
	e->partition.cprange.hi_val      = numeric_limits<int64_t>::min();
	e->partition.cprange.lo_val      = numeric_limits<int64_t>::max();
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
		e->blockOffset  = static_cast<u_int64_t>
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
LBID_t ExtentMap::getLBIDsFromFreeList ( u_int32_t size )
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
		throw std::runtime_error(
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
// the given OID.  HWM for the last extent is reset to the specified value.
// input:
//   oid          - OID of the last logical extent to be retained
//   partitionNum - partition number of the last logical extent to be retained
//   segmentNum   - segment number of the last logical extent to be retained
//   hwm          - HWM to be assigned to the last logical extent retained
//------------------------------------------------------------------------------
void ExtentMap::rollbackColumnExtents ( int oid,
	u_int32_t partitionNum,
	u_int16_t segmentNum,
	HWM_t    hwm)
{
	//bool oidExists = false;

#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("rollbackColumnExtents");
		TRACER_ADDINPUT(oid);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDINPUT(hwm);
		TRACER_WRITE;
	}
#endif

#ifdef BRM_DEBUG
	if (oid < 0) {
		log("ExtentMap::rollbackColumnExtents(): OID must be >= 0",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument(
			"ExtentMap::rollbackColumnExtents(): OID must be >= 0");
	}
#endif

	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);
	
	u_int32_t fboLo = 0;
	u_int32_t fboHi = 0;
	u_int32_t fboLoPreviousStripe = 0;
	
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	for (int i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size  != 0) && 
			(fExtentMap[i].fileID      == oid)) {

			//oidExists = true;

			// Calculate fbo range for the stripe containing the given hwm
			if (fboHi == 0) {
				u_int32_t range = fExtentMap[i].range.size * 1024;
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
	//	oss << "ExtentMap::rollbackColumnExtents(): "
	//		"Rollback failed: no extents exist for: OID-" << oid <<
	//		"; partition-" << partitionNum <<
	//		"; segment-"   << segmentNum   <<
	//		"; hwm-"       << hwm;
	//	log(oss.str(), logging::LOG_TYPE_CRITICAL);
	//	throw std::invalid_argument(oss.str());
	//}
}

//------------------------------------------------------------------------------
// Rollback (delete) the extents that follow the extents in partitionNum,
// for the given dictionary OID.  The specified hwms represent the HWMs to be
// reset for each of the segment store files in this partition.  An HWM will
// not be given for "every" segment file if we are rolling back to a point where
// we had not yet created all the segment files in the partition.  In any case,
// any extents for the "oid" that follow partitionNum, should be deleted.
// Likewise, any extents in the same partition, which trail the applicable
// element in hwms[] should be deleted as well.
// input:
//   oid          - OID of the "last" extents to be retained
//   partitionNum - partition number of the last extents to be retained
//   hwm          - HWMs to be assigned to the last retained extent in each of
//                      the corresponding segment store file.
//                  hwm[0] is assigned to segment store file 0;
//                  hwm[1] is assigned to segment store file 1; etc.
//------------------------------------------------------------------------------
void ExtentMap::rollbackDictStoreExtents ( int oid,
	u_int32_t            partitionNum,
	const vector<HWM_t>& hwms)
{
	//bool oidExists = false;

#ifdef BRM_INFO
	if (fDebug)
	{
		ostringstream oss;
    	for (unsigned int k=0; k<hwms.size(); k++)
			oss << "; hwms[" << k << "]-"  << hwms[k];
		const std::string& hwmString(oss.str());

		// put TRACE inside separate scope {} to insure that temporary
		// hwmString still exists when tracer destructor tries to print it.
		{
			TRACER_WRITELATER("rollbackDictStoreExtents");
			TRACER_ADDINPUT(oid);
			TRACER_ADDINPUT(partitionNum);
			TRACER_ADDSTRINPUT(hwmString);
			TRACER_WRITE;
		}
	}
#endif

	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);

	//u_int32_t fboLo[ hwms.size() ];
	uint32_t* fboLo = (uint32_t*)alloca(hwms.size() * sizeof(uint32_t));
	//u_int32_t fboHi[ hwms.size() ];
	uint32_t* fboHi = (uint32_t*)alloca(hwms.size() * sizeof(uint32_t));
	memset( fboLo, 0, sizeof(u_int32_t)*hwms.size() );
	memset( fboHi, 0, sizeof(u_int32_t)*hwms.size() );

	unsigned int lastSegFileToKeepInPartition = hwms.size() - 1;

	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);

	for (int i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size  != 0) && 
			(fExtentMap[i].fileID      == oid)) {

			//oidExists = true;

			// Calculate fbo's for the list of hwms we are given
			if (fboHi[0] == 0) {
				u_int32_t range = fExtentMap[i].range.size * 1024;
				for (unsigned int k=0; k<hwms.size(); k++) {
					fboLo[k] = hwms[k] - (hwms[k] % range);
					fboHi[k] = fboLo[k] + range - 1;
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
				if (fExtentMap[i].segmentNum > lastSegFileToKeepInPartition) {
					deleteExtent( i );                                 // case 2
				}
				else { // segmentNum <= lastSegFileToKeepInPartition
					unsigned segNum = fExtentMap[i].segmentNum;

					if (fExtentMap[i].blockOffset < fboLo[segNum]) {
						// no action necessary                           case 3A
					}
					else if (fExtentMap[i].blockOffset == fboLo[segNum]) {
						if (fExtentMap[i].HWM != hwms[ segNum ]) {
							makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
							fExtentMap[i].HWM  = hwms[ segNum ];
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
	//	oss << "ExtentMap::rollbackDictStoreExtents(): "
	//		"Rollback failed: no extents exist for: OID-" << oid <<
	//		"; partition-" << partitionNum;
	//	log(oss.str(), logging::LOG_TYPE_CRITICAL);
	//	throw std::invalid_argument(oss.str());
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
	
	u_int32_t fboLo = 0;
	u_int32_t fboHi = 0;
	u_int32_t fboLoPreviousStripe = 0;
	
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	ExtentsInfoMap_t::const_iterator it;
	
	for (int i = 0; i < emEntries; i++) {
		if (fExtentMap[i].range.size  != 0) 
		{
			it = extentsInfo.find ( fExtentMap[i].fileID );
			if ( it != extentsInfo.end() ) 		
			{

				// Calculate fbo range for the stripe containing the given hwm
				if (fboHi == 0) {
					u_int32_t range = fExtentMap[i].range.size * 1024;
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
					deleteExtent( i );                                     // case 1
				}
				else if (fExtentMap[i].partitionNum == it->second.partitionNum) 
				{
					if (fExtentMap[i].blockOffset > fboHi) 
					{
						deleteExtent( i );                                 // case 2
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
									fExtentMap[i].HWM    = fboLo - 1;      //case 3A
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
							// extent precedes previous stripe               case 3C
						}
					}
					else 
					{ // extent is in same stripe
						if (fExtentMap[i].segmentNum > it->second.segmentNum) 
						{
							deleteExtent( i );                            // case 4A
						}
						else if (fExtentMap[i].segmentNum < it->second.segmentNum) 
						{
							if (fExtentMap[i].HWM != fboHi) {
								makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
								fExtentMap[i].HWM    = fboHi;             // case 4B
								fExtentMap[i].status = EXTENTAVAILABLE;
							}
						}
						else 
						{ // fExtentMap[i].segmentNum == segmentNum
							if (fExtentMap[i].HWM != it->second.hwm) 
							{
								makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
								fExtentMap[i].HWM    = it->second.hwm;               // case 4C
								fExtentMap[i].status = EXTENTAVAILABLE;
							}
						}
					}
				}
				else 
				{
					// extent in earlier partition; no action necessary       case 5
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
	
	u_int32_t fboLo = 0;
	u_int32_t fboHi = 0;

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
					if ( (fExtentMap[i].partitionNum == it->second.partitionNum)  && (fExtentMap[i].segmentNum == it->second.segmentNum)  && (fExtentMap[i].dbRoot == it->second.dbRoot) )
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

			// Calculate fbo
				if (fboHi == 0) 
				{
					u_int32_t range = fExtentMap[i].range.size * 1024;			
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
					deleteExtent( i );                                     // case 1
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
								fExtentMap[i].status = EXTENTAVAILABLE;   // case 2B
							}
						}
						else 
						{
							deleteExtent( i );                            // case 3C
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
		throw std::invalid_argument(oss.str());
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
				throw std::logic_error("ExtentMap::deleteOID(): found a free FL entry in a supposedly full shmseg");
			}
#endif
			freeFLIndex = flEntries;  // happens to be the right index
			flEntries = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);
		}
#ifdef BRM_DEBUG
		if (freeFLIndex == -1) {
			log("ExtentMap::deleteOID(): no available free list entries?", logging::LOG_TYPE_DEBUG);
			throw std::logic_error("ExtentMap::deleteOID(): no available free list entries?");
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
// Returns the sum of the HWM's for the specified OID, which should give the
// effective absolute HWM.  Another way to do it would be to find the last
// extent, and use the partition and segment number of that extent, and the
// local HWM for that extent to calculate the absolute HWM.  This function
// should only be used for column OIDs, as there is no concept of an absolute
// HWM for dictionary store extents.
//------------------------------------------------------------------------------
HWM_t ExtentMap::getHWM(int OID) 
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getHWM");
		TRACER_ADDINPUT(OID);
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
	HWM_t ret = 0; // used to add up block count, and then subtract 1 to get HWM
	bool OIDExists = false;

	if (OID < 0) {
		ostringstream oss;
		oss << "ExtentMap::getHWM(): invalid OID requested: " << OID;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

	grabEMEntryTable(READ);
	
	uint32_t  lastPart =  0;
	uint16_t  lastSeg  =  0;
	u_int32_t lastFbo  =  0;
	int       lastExtentIndex= -1;

	emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size != 0)   &&
			(fExtentMap[i].fileID     == OID)) {
			OIDExists = true;
			if (fExtentMap[i].HWM != 0) {
				ret += (fExtentMap[i].HWM + 1); // add to total block count
			}

			// Track the index of the last extent
			if ( (fExtentMap[i].status == EXTENTAVAILABLE) &&
			   ( (fExtentMap[i].partitionNum >  lastPart) ||
				((fExtentMap[i].partitionNum == lastPart) &&
				 (fExtentMap[i].blockOffset  >  lastFbo)) ||
				((fExtentMap[i].partitionNum == lastPart) &&
				 (fExtentMap[i].blockOffset  == lastFbo) &&
				 (fExtentMap[i].segmentNum   >= lastSeg)) ) ) {
					lastFbo   = fExtentMap[i].blockOffset;
					lastPart  = fExtentMap[i].partitionNum;
					lastSeg   = fExtentMap[i].segmentNum;
					lastExtentIndex = i;
			}
		}
	}

	// In the loop above we ignored "all" the extents with HWM of 0,
	// which is okay most of the time, because each segment file's HWM
	// is carried in the last extent only.  BUT if we have a segment
	// file with HWM=0, having a single extent and a single block at
	// the end of the data, we still need to account for this last block.
	// So we increment the block count for this isolated case.
	if ((lastExtentIndex != -1) &&
	    (fExtentMap[lastExtentIndex].HWM == 0))
	{
		ret++;
	}

	releaseEMEntryTable(READ);

	if (ret > 0)
		ret--; // HWM is 1 less than total cumulative block count

	if (OIDExists)
		return ret;
	else
	{
		ostringstream oss;
		oss << "ExtentMap::getHWM(): There are no extent entries for OID " << OID << endl;
		log(oss.str(), logging::LOG_TYPE_WARNING);	
		throw std::invalid_argument(oss.str());
	} 
}

//------------------------------------------------------------------------------
// Returns the last local HWM for the specified OID.  Also returns the
// DBRoot, and partition, and segment numbers for the relevant segment file.
// Technically, this function finds the "last" extent for the specified OID,
// and returns the HWM for that extent.  It is assumed that the HWM for the
// segment file containing this "last" extent, has been stored in that extent's
// hwm; and that the hwm is not still hanging around in a previous extent for
// the same segment file.
//------------------------------------------------------------------------------
HWM_t ExtentMap::getLastLocalHWM(int OID, uint16_t& dbRoot,
   uint32_t& partitionNum, uint16_t& segmentNum)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getLastLocalHWM");
		TRACER_ADDINPUT(OID);
		TRACER_ADDSHORTOUTPUT(dbRoot);
		TRACER_ADDOUTPUT(partitionNum);
		TRACER_ADDSHORTOUTPUT(segmentNum);
		TRACER_WRITE;
	}	
#endif

#ifdef EM_AS_A_TABLE_POC__
	if (OID == 1084)
	{
		return 0;
	}
#endif

	u_int32_t lastExtent = 0;
	int  lastExtentIndex = -1;
	bool OIDExists=false;
	dbRoot       = 0;
	int firstdbRoot = 0;
	partitionNum = 0;
	segmentNum   = 0;
	HWM_t hwm    = 0;

	if (OID < 0) {
		ostringstream oss;
		oss << "ExtentMap::getLastLocalHWM(): invalid OID requested: " << OID;
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
			(fExtentMap[i].status     != EXTENTOUTOFSERVICE)) {
			OIDExists=true;
			if (fExtentMap[i].segmentNum==0) firstdbRoot = fExtentMap[i].dbRoot;
			if ( (fExtentMap[i].status == EXTENTAVAILABLE) &&
			   ( (fExtentMap[i].partitionNum >  partitionNum) ||
				((fExtentMap[i].partitionNum == partitionNum) &&
				 (fExtentMap[i].blockOffset  >  lastExtent))  ||
				((fExtentMap[i].partitionNum == partitionNum) &&
				 (fExtentMap[i].blockOffset  == lastExtent) &&
				 (fExtentMap[i].segmentNum   >= segmentNum)) ) )
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
		dbRoot = fExtentMap[lastExtentIndex].dbRoot;
		hwm    = fExtentMap[lastExtentIndex].HWM;
	}
	else
	{
		dbRoot=firstdbRoot;
	}

	releaseEMEntryTable(READ);

	if (lastExtentIndex != -1 || (OIDExists && lastExtentIndex==-1) ) // @bug 1911 return 0 when OID exists but extent is marked
	{
		return hwm;
	}
	else
	{
		ostringstream oss;
		oss << "ExtentMap::getLastLocalHWM(): "
			"There are no extent entries for OID " << OID << endl;
		log(oss.str(), logging::LOG_TYPE_WARNING);	
		throw std::invalid_argument(oss.str());
	}
}

//------------------------------------------------------------------------------
// Returns the HWM for the specified OID, partition, and segment numbers.
// Used to get the HWM for a specific column or dictionary store segment file.
//------------------------------------------------------------------------------
HWM_t ExtentMap::getLocalHWM(int OID, uint32_t partitionNum,
   uint16_t segmentNum)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getLocalHWM");
		TRACER_ADDINPUT(OID);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
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
		throw std::invalid_argument(oss.str());
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
	uint16_t segmentNum, HWM_t newHWM, bool firstNode, bool useLock)
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
#endif

	int lastExtentIndex     = -1;
	int oldHWMExtentIndex   = -1;
	u_int32_t highestOffset = 0;
	bool addedAnExtent = false;

#ifdef BRM_DEBUG
	if (OID < 0) {
		log("ExtentMap::setLocalHWM(): OID must be >= 0",
			logging::LOG_TYPE_DEBUG);
		throw invalid_argument(
			"ExtentMap::setLocalHWM(): OID must be >= 0");
	}
#endif

	if (useLock)
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
		throw std::invalid_argument(oss.str());
	}

	if (newHWM >= (fExtentMap[lastExtentIndex].blockOffset + 
		   fExtentMap[lastExtentIndex].range.size*1024)) {
		ostringstream oss;
		oss << "ExtentMap::setLocalHWM(): "
			"new HWM is past the end of the file for OID " << OID << "; partition " <<
			partitionNum << "; segment " << segmentNum << "; HWM " << newHWM;
		log(oss.str(), logging::LOG_TYPE_DEBUG);
		throw std::invalid_argument(oss.str());
	}

	// Save HWM in last extent for this segment file; and mark as AVAILABLE
	makeUndoRecord(&fExtentMap[lastExtentIndex], sizeof(EMEntry));
	fExtentMap[lastExtentIndex].HWM    = newHWM;
	fExtentMap[lastExtentIndex].status = EXTENTAVAILABLE;

	// Reset HWM in old HWM extent to 0
	if ((oldHWMExtentIndex != -1) && (oldHWMExtentIndex != lastExtentIndex)) {
		makeUndoRecord(&fExtentMap[oldHWMExtentIndex], sizeof(EMEntry));
		fExtentMap[oldHWMExtentIndex].HWM = 0;
		addedAnExtent = true;
	}

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
}





void ExtentMap::getExtents(int OID, std::vector<struct EMEntry>& entries,
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
/*
	if (entries.empty()) {
		if (notFoundErr) {
			ostringstream oss;
			oss << "ExtentMap::getExtents(): OID not found: " << OID;
			log(oss.str(), logging::LOG_TYPE_WARNING);
			if (OID < 3000)
				log("System Catalog might need to be updated", logging::LOG_TYPE_WARNING);
			throw logic_error(oss.str());
		}
	}
*/
	if (sorted)
		std::sort<std::vector<struct EMEntry>::iterator>(entries.begin(), entries.end());
}

//------------------------------------------------------------------------------
// Returns the DBRoot and partition number associated with the first extent
// pertaining to the specified oid.
// input:
//   oid - OID of interest
//   incOutOfService - include/exclude out-of-service partitions in the search
// output:
//   dbRoot       - DBRoot of first extent for specified OID
//   partitionNum - partition number of first extent for specified OID
//------------------------------------------------------------------------------
void ExtentMap::getStartExtent(OID_t oid, bool incOutOfService,
                               uint16_t& dbRoot, uint32_t& partitionNum)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getStartExtent");
		TRACER_ADDINPUT(oid);
		TRACER_ADDSHORTOUTPUT(dbRoot);
		TRACER_ADDOUTPUT(partitionNum);
		TRACER_WRITE;
	}	
#endif

	if (oid < 0) {
		ostringstream oss;
		oss << "ExtentMap::getStartExtent(): invalid OID requested: " << oid;
		log(oss.str(), logging::LOG_TYPE_CRITICAL);
		throw invalid_argument(oss.str());
	}

#ifdef EM_AS_A_TABLE_POC__
	if (oid == 1084)
	{
		dbRoot = 1;
		partitionNum = 0;
		return;
	}
#endif

	partitionNum = numeric_limits<uint32_t>::max();
	dbRoot       = 1;
	bool bFound  = false;

	grabEMEntryTable(READ);
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	if (incOutOfService) {
		for (int i = 0 ; i < emEntries; i++) {
			if ((fExtentMap[i].fileID     == oid) &&
			    (fExtentMap[i].range.size != 0)   &&
			    (fExtentMap[i].segmentNum == 0)) {
				if (fExtentMap[i].partitionNum < partitionNum) {
					dbRoot       = fExtentMap[i].dbRoot;
					partitionNum = fExtentMap[i].partitionNum;
					bFound       = true;
				}
			}
		}
	}
	else {
		for (int i = 0 ; i < emEntries; i++) {
			if ((fExtentMap[i].fileID     == oid) &&
			    (fExtentMap[i].range.size != 0)   &&
				(fExtentMap[i].status     != EXTENTOUTOFSERVICE) &&
			    (fExtentMap[i].segmentNum == 0)) {
				if (fExtentMap[i].partitionNum < partitionNum) {
					dbRoot       = fExtentMap[i].dbRoot;
					partitionNum = fExtentMap[i].partitionNum;
					bFound       = true;
				}
			}
		}
	}
	releaseEMEntryTable(READ);

	if (!bFound) {
		ostringstream oss;
		oss << "ExtentMap::getStartExtent(): OID not found: " << oid;
		log(oss.str(), logging::LOG_TYPE_WARNING);
		throw logic_error(oss.str());
	}
}

//------------------------------------------------------------------------------
// Delete all extents for the specified OID(s) and partition number.
//------------------------------------------------------------------------------
void ExtentMap::deletePartition(const std::set<OID_t>& oids,
	uint32_t partitionNum)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("deletePartition");
		std::ostringstream oss;
		oss << "partitionNum: " << partitionNum << "; OIDS: ";
		std::set<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}	
#endif
	if (oids.size() == 0)
		return;

	std::set<OID_t>::const_iterator it;
	OID_t    firstOid                    = *oids.begin();
	bool     partitionNumIsLastPartition = false;
	bool	 partitionExist = true;
	uint32_t lastPartitionNum = 0;
	grabEMEntryTable(WRITE);
	grabFreeList(WRITE);

	// We assume all the OIDs belong to the same table, and thus have the
	// same last partition.  We find the last partition for the first OID
	// and ensure that we are not disabling the last partition.
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (int i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size != 0  ) && 
			(fExtentMap[i].fileID     == firstOid)) {
			if (fExtentMap[i].partitionNum > lastPartitionNum) {
				lastPartitionNum = fExtentMap[i].partitionNum;	
			}
		}
	}
	
	if (partitionNum == lastPartitionNum)
	{
		partitionNumIsLastPartition = true;
	}
	else if (partitionNum > lastPartitionNum) 
	{
		partitionExist = false;
	}

	//@Bug 3363 Error out before deleting
	if (partitionNumIsLastPartition) {
        string emsg = IDBErrorInfo::instance()->errorMsg(ERR_INVALID_LAST_PARTITION);
		throw IDBExcept(emsg, ERR_INVALID_LAST_PARTITION);
	}
	
	if (!partitionExist)
	{
		string emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NOT_EXIST);
		throw IDBExcept(emsg, ERR_PARTITION_NOT_EXIST);
	}
	
	partitionExist = false;
	
	for (int i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size  != 0  ) && 
			(fExtentMap[i].partitionNum== partitionNum)) {
			it = oids.find( fExtentMap[i].fileID );
			if (it != oids.end())
			{
				partitionExist = true;
				deleteExtent( i );
			}
		}
	}
	
	if (!partitionExist)
	{
		string emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NOT_EXIST);
		throw IDBExcept(emsg, ERR_PARTITION_NOT_EXIST);
	}
	
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s) and partition
// number.
//------------------------------------------------------------------------------
void ExtentMap::markPartitionForDeletion(const std::set<OID_t>& oids,
	uint32_t partitionNum)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("markPartitionForDeletion");
		std::ostringstream oss;
		oss << "partitionNum: " << partitionNum << "; OIDS: ";
		std::set<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}
#endif
	if (oids.size() == 0)
		return;

	std::set<OID_t>::const_iterator it;
	OID_t    firstOid                    = *oids.begin();
	bool     partitionNumIsLastPartition = false;
	bool	 partitionExist = true;
	uint32_t lastPartitionNum = 0;
	grabEMEntryTable(WRITE);

	// We assume all the OIDs belong to the same table, and thus have the
	// same last partition.  We find the last partition for the first OID
	// and ensure that we are not disabling the last partition.
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (int i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size != 0  ) && 
			(fExtentMap[i].fileID     == firstOid)) {
			if (fExtentMap[i].partitionNum > lastPartitionNum) {
				lastPartitionNum = fExtentMap[i].partitionNum;	
			}
		}
	}
	
	//@Bug 3363 Error out before disable partition
	if (partitionNum == lastPartitionNum)
	{
		partitionNumIsLastPartition = true;
	}
	else if (partitionNum > lastPartitionNum) 
	{
		partitionExist = false;
	}
	
	
	if (partitionNumIsLastPartition) {
		string emsg = IDBErrorInfo::instance()->errorMsg(ERR_INVALID_LAST_PARTITION);
		throw IDBExcept(emsg, ERR_INVALID_LAST_PARTITION);
	}

	if (!partitionExist)
	{
		string emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NOT_EXIST);
		throw IDBExcept(emsg, ERR_PARTITION_NOT_EXIST);
	}
	
	partitionExist = false;
	
	for (int i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size  != 0  ) && 
			(fExtentMap[i].partitionNum== partitionNum)) {
			it = oids.find( fExtentMap[i].fileID );
			if (it != oids.end()) {
				partitionExist = true;
				makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
				if (fExtentMap[i].status == EXTENTOUTOFSERVICE)
				{
					string emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_ALREADY_DISABLED);
					throw IDBExcept(emsg, ERR_PARTITION_ALREADY_DISABLED);
				}
				fExtentMap[i].status = EXTENTOUTOFSERVICE;
			}
		}
	}
	
	if (!partitionExist)
	{
		string emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NOT_EXIST);
		throw IDBExcept(emsg, ERR_PARTITION_NOT_EXIST);
	}
	
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s) 
//------------------------------------------------------------------------------
void ExtentMap::markAllPartitionForDeletion(const std::set<OID_t>& oids)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("markPartitionForDeletion");
		std::ostringstream oss;
		oss << "OIDS: ";
		std::set<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}
#endif
	if (oids.size() == 0)
		return;

	std::set<OID_t>::const_iterator it;
	
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
void ExtentMap::restorePartition(const std::set<OID_t>& oids,
	uint32_t partitionNum)
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("restorePartition");
		std::ostringstream oss;
		oss << "partitionNum: " << partitionNum << "; OIDS: ";
		std::set<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}	
#endif
	if (oids.size() == 0)
		return;

	std::set<OID_t>::const_iterator it;
	grabEMEntryTable(WRITE);
	bool partitionExist = false;
	
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	
	for (int i = 0; i < emEntries; i++) {
		if ((fExtentMap[i].range.size  != 0  ) && 
			(fExtentMap[i].partitionNum== partitionNum)) {
			it = oids.find( fExtentMap[i].fileID );
			if (it != oids.end()) {
				partitionExist = true;
				makeUndoRecord(&fExtentMap[i], sizeof(EMEntry));
				if (fExtentMap[i].status == EXTENTAVAILABLE)
				{
					string emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_ALREADY_ENABLED);
					throw IDBExcept(emsg, ERR_PARTITION_ALREADY_ENABLED);
				}
				fExtentMap[i].status = EXTENTAVAILABLE;
			}
		}
	}
	
	if (!partitionExist)
	{
		string emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NOT_EXIST);
		throw IDBExcept(emsg, ERR_PARTITION_NOT_EXIST);
	}
}

//------------------------------------------------------------------------------
// Return all the out-of-service partitions for the specified OID.
//------------------------------------------------------------------------------
void ExtentMap::getOutOfServicePartitions(OID_t oid,
	std::vector<uint32_t>& partitionNums) 
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

	std::set<uint32_t> partSet;
	uint32_t lastPartitionNumInserted = 0; // optimize by caching last insert

	grabEMEntryTable(READ);
	int emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	for (int i=0; i<emEntries; i++) {
		if ((fExtentMap[i].range.size != 0  ) && 
			(fExtentMap[i].fileID     == oid) &&
			(fExtentMap[i].status     == EXTENTOUTOFSERVICE)) {

			if ((partSet.size() == 0) ||
				(lastPartitionNumInserted != fExtentMap[i].partitionNum)) {
				partSet.insert( fExtentMap[i].partitionNum );
				lastPartitionNumInserted = fExtentMap[i].partitionNum;
			}
		}
	}
	releaseEMEntryTable(READ);

	std::set<uint32_t>::const_iterator it;
	for (it=partSet.begin(); it!=partSet.end(); ++it)
	{
		partitionNums.push_back( *it );
	}
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
	u_int32_t usedEntries;
	
	grabEMEntryTable(READ);
	try {
		grabFreeList(READ);
	}
	catch (...) {
		releaseEMEntryTable(READ);
		throw;
	}
	
	flEntries = fFLShminfo->allocdSize/sizeof(InlineLBIDRange);
	emEntries = fEMShminfo->allocdSize/sizeof(struct EMEntry);
	
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
						throw std::logic_error("EM checkConsistency test 1a (data structures are read-locked)");
					}
				}
			}
		}
	}
	
	//test 1b - verify that the entire LBID space is accounted for
	/* XXXPAT:  For some reason the compiler complains when we assign
	2^36 as a constant to a 64-bit int.  Why? */
	
	int lbid, oldlbid;
	
	lbid = 0;
	while (lbid < 67108864) {    // 2^26  (2^36/1024)
		oldlbid = lbid;
		for (i = 0; i < flEntries; i++) {
 			if (fFreeList[i].start % 1024 != 0) {
 				cerr << "EM::checkConsistency(): A freelist entry is not 1024-block aligned" << endl;
 				throw std::logic_error("EM checkConsistency test 1b (data structures are read-locked)");
 			}
			if (fFreeList[i].start/1024 == lbid)
				lbid += fFreeList[i].size;
		}
		for (i = 0; i < emEntries; i++) {
 			if (fExtentMap[i].range.start % 1024 != 0) {
				cerr << "EM::checkConsistency(): An extent map entry is not 1024-block aligned " <<i << " " <<fExtentMap[i].range.start <<  endl;
 				throw std::logic_error("EM checkConsistency test 1b (data structures are read-locked)");
 			}
			if (fExtentMap[i].range.start/1024 == lbid)
				lbid += fExtentMap[i].range.size;
		}
		if (oldlbid == lbid) {
			cerr << "EM::checkConsistency(): There is a gap in the LBID space at block #" <<
					static_cast<u_int64_t>(lbid*1024) << endl;
			throw std::logic_error("EM checkConsistency test 1b (data structures are read-locked)");
		}
	}
	
	// test 2 - verify that the freelist is consolidated
	for (i = 0; i < flEntries; i++) {
		if (fFreeList[i].size != 0) {
			flEnd = fFreeList[i].start + (fFreeList[i].size * 1024);
			for (j = i + 1; j < flEntries; j++) 
				if (fFreeList[j].size != 0 && fFreeList[j].start == flEnd)
					throw std::logic_error("EM checkConsistency test 2 (data structures are read-locked)");
		}
	}
	
	// test 3 - scan the extent map to make sure files have no LBID gaps
	vector<OID_t> oids;
	vector< vector<u_int32_t> > fbos;
	
	for (i = 0; i < emEntries; i++) {
		if (fExtentMap[i].range.size != 0) {
			for (j = 0; j < (int)oids.size(); j++)
				if (oids[j] == fExtentMap[i].fileID)
					break;
			if (j == (int)oids.size()) {
				oids.push_back(fExtentMap[i].fileID);
				fbos.push_back(vector<u_int32_t>());
			}
			fbos[j].push_back(fExtentMap[i].blockOffset);
		}
	}

	for (i = 0; i < (int)fbos.size(); i++)
		sort<vector<u_int32_t>::iterator>(fbos[i].begin(), fbos[i].end());
	
	const unsigned EXTENT_SIZE = getExtentSize();
	for (i = 0; i < (int)fbos.size(); i++) {
		for (j = 0; j < (int)fbos[i].size(); j++) {
			if (fbos[i][j] != static_cast<u_int32_t>(j * EXTENT_SIZE)) {
				cerr << "EM: OID " << oids[i] << " has no extent at FBO " <<
						j * EXTENT_SIZE << endl;
				throw std::logic_error("EM checkConsistency test 3 (data structures are read-locked)");
			}
		}
	}
	
	fbos.clear();
	oids.clear();
	
	// test 5a - scan freelist to make sure the current size is accurate
	
	for (i = 0, usedEntries = 0; i < emEntries; i++)
		if (fExtentMap[i].range.size != 0)
			usedEntries++;
	
	if (usedEntries != fEMShminfo->currentSize/sizeof(struct EMEntry)) {
		std::cerr << "checkConsistency: used extent map entries = " << usedEntries 
				<< " metadata says " << fEMShminfo->currentSize/sizeof(struct EMEntry) 
				<< std::endl;
		throw std::logic_error("EM checkConsistency test 5a (data structures are read-locked)");
	}
	
	for (i = 0, usedEntries = 0; i < flEntries; i++)
		if (fFreeList[i].size != 0)
			usedEntries++; 
	
	if (usedEntries != fFLShminfo->currentSize/sizeof(InlineLBIDRange)) {
		std::cerr << "checkConsistency: used freelist entries = " << usedEntries 
				<< " metadata says " << fFLShminfo->currentSize/sizeof(InlineLBIDRange) 
				<< std::endl;
		throw std::logic_error("EM checkConsistency test 5a (data structures are read-locked)");
	}
	
	releaseFreeList(READ);
	releaseEMEntryTable(READ);
	return 0;
}

void ExtentMap::setReadOnly()
{
	r_only = true;
}

inline void ExtentMap::makeUndoRecord(void *start, int size)
{
	ImageDelta d;

 	d.start = start;
 	d.size = size;
 	memcpy(d.data, start, size);
  	undoRecords.push_back(d);
}

void ExtentMap::undoChanges()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("undoChanges");
#endif
	vector<ImageDelta>::iterator it;

	for (it = undoRecords.begin(); it != undoRecords.end(); it++)
		memcpy((*it).start, (*it).data, (*it).size);

	finishChanges();
}

void ExtentMap::confirmChanges()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("confirmChanges");
#endif
	finishChanges();
}

void ExtentMap::finishChanges()
{

	undoRecords.clear();
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

unsigned ExtentMap::getExtentSize() const       // dmc-should deprecate
{
	if (ExtentSize == 0)
	{
		config::Config* cf = config::Config::makeConfig();
		string es = cf->getConfig("ExtentMap", "ExtentSize");
		if (es.length() == 0) es = "8K";
		if (es == "8K" || es == "8k")
		{
			ExtentSize = 0x2000;
		}
		else if (es == "1K" || es == "1k")
		{
			ExtentSize = 0x400;
		}
		else if (es == "64K" || es == "64k")
		{
			ExtentSize = 0x10000;
		}
		else
		{
			throw std::logic_error("Invalid ExtentSize found in config file!");
		}
	}

	return ExtentSize;
}

//------------------------------------------------------------------------------
// Returns the number or rows per extent.  Only supported values are 1m, 8m,
// and 64m.
//------------------------------------------------------------------------------
unsigned ExtentMap::getExtentRows() const
{
	if (ExtentRows == 0)
	{
		config::Config* cf = config::Config::makeConfig();
		string er = cf->getConfig("ExtentMap", "ExtentRows");
		if (er.length() == 0) er = "8M";
		if (er == "8M" || er == "8m")
		{
			ExtentRows = 0x800000;
		}
		else if (er == "1M" || er == "1m")
		{
			ExtentRows = 0x100000;
		}
		else if (er == "64M" || er == "64m")
		{
			ExtentRows = 0x4000000;
		}
		else
		{
			throw std::logic_error("Invalid ExtentRows found in config file!");
		}
	}

	return ExtentRows;
}

//------------------------------------------------------------------------------
// Returns the number of column segment files for an OID, that make up a
// partition.
//------------------------------------------------------------------------------
unsigned ExtentMap::getFilesPerColumnPartition() const
{
	if (filesPerColumnPartition == 0)
	{
		config::Config* cf = config::Config::makeConfig();
		string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
		filesPerColumnPartition = cf->uFromText(fpc);
		if (filesPerColumnPartition == 0)
			filesPerColumnPartition = 32;
	}

	return filesPerColumnPartition;
}

//------------------------------------------------------------------------------
// Returns the number of extents in a segment file.
//------------------------------------------------------------------------------
unsigned ExtentMap::getExtentsPerSegmentFile() const
{
	if (extentsPerSegmentFile == 0)
	{
		config::Config* cf = config::Config::makeConfig();
		string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
		extentsPerSegmentFile = cf->uFromText(epsf);
		if (extentsPerSegmentFile == 0)
			extentsPerSegmentFile = 4;
	}

	return extentsPerSegmentFile;
}

//------------------------------------------------------------------------------
// Returns the number of DBRoots to be used in storing db column files.
//------------------------------------------------------------------------------
unsigned ExtentMap::getDbRootCount() const
{
	if (dbRootCount == 0)
	{
		config::Config* cf = config::Config::makeConfig();
		string root = cf->getConfig("SystemConfig","DBRootCount");
		dbRootCount = cf->uFromText(root);
		if (dbRootCount == 0)
			dbRootCount = 1;
	}

	return dbRootCount;
}

// jer - MultiFile Branch end

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

}	//namespace
