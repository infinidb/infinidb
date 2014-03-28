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
* $Id: lbidlist.cpp 9655 2013-06-25 23:08:13Z xlou $
*
******************************************************************************/
#include <iostream>
#include "primitivemsg.h"
#include "blocksize.h"
#include "lbidlist.h"
#include "calpontsystemcatalog.h"
#include "brm.h"
#include "brmtypes.h"

#define IS_VERBOSE (fDebug >= 4)
#define IS_DETAIL  (fDebug >= 3)
#define IS_SUMMARY (fDebug >= 2)

using namespace std;
using namespace execplan;
using namespace BRM;

namespace joblist
{

inline uint64_t order_swap(uint64_t x)
{
    return (x>>56) |
        ((x<<40) & 0x00FF000000000000ULL) |
        ((x<<24) & 0x0000FF0000000000ULL) |
        ((x<<8)  & 0x000000FF00000000ULL) |
        ((x>>8)  & 0x00000000FF000000ULL) |
        ((x>>24) & 0x0000000000FF0000ULL) |
        ((x>>40) & 0x000000000000FF00ULL) |
        (x<<56);
}

LBIDList::LBIDList()
{
	throw logic_error("Don't use LBIDList()");
}

  /** @LBIDList(oid, debug)
  *
  *   Create a new LBIDList structure.
  *	  Used to apply CP logic but cannot perform updates of CP data
  */

LBIDList::LBIDList(const int debug)
{
    fDebug=debug;
}

  /** @LBIDList(oid, debug)
  *
  *   Create a new LBIDList structure.
  *	  ctor that prepares the object for updates
  *	  of the CP data.
  */

LBIDList::LBIDList(const CalpontSystemCatalog::OID oid,
				const int debug)
{
	init(oid, debug);
}

  /** @LBIDList::Init() Initializes a LBIDList structure
  *
  *   Create a new LBIDList structure and initialize it
  */

void LBIDList::init(const CalpontSystemCatalog::OID oid,
				const int debug)
{
    LBIDRange LBIDR;
    fDebug=debug;
	int err=0;

#ifdef DEBUG
    if (IS_VERBOSE)
      cout << "LBIDList Init for " << oid
			<< " size = " << LBIDRanges.size()
			<< " " << this << endl;
#endif

	if (!em) {
		em.reset(new DBRM);
	}
	try {
    	err=em->lookup(oid, LBIDRanges);
	} catch (exception &e)
	{
		cout << "LBIDList::init(): DBRM lookup error: " << e.what() << endl;
		throw;
	}

	if (err) {
		cout << "Lookup error ret " << err << endl;
		throw runtime_error("LBIDList::init(): DBRM lookup failure");
	}

#ifdef DEBUG
    const int RangeCount = LBIDRanges.size();
    if (IS_VERBOSE)
      cout << "LBIDList Init got " << RangeCount << " ranges " << endl;
    for (int i=0; i<RangeCount; i++)
    {
      LBIDR = LBIDRanges.at(i);
      if (IS_VERBOSE)
        cout << "Start = " << LBIDR.start << ", Len = " << LBIDR.size << endl;
    }
#endif

}

LBIDList::~LBIDList()
{
	std::vector<MinMaxPartition*>::value_type ptr;
	while (!lbidPartitionVector.empty())
	{
		ptr = lbidPartitionVector.back();
		lbidPartitionVector.pop_back();
		delete ptr;
	}
#ifdef DEBUG
    if (IS_VERBOSE)
      cout << "LBIDList::~LBIDList() object deleted " << this << endl << endl;
#endif
}


void LBIDList::Dump(long Index, int Count) const
{
    LBIDRange LBIDR;

    const int RangeCount = LBIDRanges.size();
    cout << "LBIDList::Dump with " << RangeCount << "ranges" << endl;

    for (int i=0; i<RangeCount; i++)
    {
      LBIDR = LBIDRanges.at(i);
      cout << "Start = " << LBIDR.start << ", Len = " << LBIDR.size << endl;
    }

    cout << endl;
}

// Store max/min structure for update later. lbidPartitionVector serves a list of lbid,max,min to update
// when the primitive returns with valid max/min values and the brm returns an invalid flag for the
// requested lbid
//
bool LBIDList::GetMinMax(int64_t& min, int64_t& max, int64_t& seq, int64_t lbid,
	const std::vector<struct BRM::EMEntry>* pEMEntries,
    execplan::CalpontSystemCatalog::ColDataType colDataType)
{
    bool bRet = true;
    MinMaxPartition *mmp=NULL;
    LBIDRange LBIDR;
    const int RangeCount = LBIDRanges.size();
    int32_t seq32=0;

    for(int i=0;i<RangeCount;i++)
    {
      LBIDR = LBIDRanges.at(i);
      if(lbid == LBIDR.start)
      {
        int retVal = -1;
		if (pEMEntries && pEMEntries->size() > 0)
		{
			// @bug 2968 - Get CP info from snapshot of ExtentMap taken at
			// the start of the query.
			retVal = getMinMaxFromEntries(min, max, seq32, lbid, *pEMEntries);
		}
		else
		{
			if (em) retVal=em->getExtentMaxMin(lbid, max, min, seq32);
		}
        seq = seq32;
#ifdef DEBUG
        if (IS_VERBOSE)
          cout << i
			<< " GetMinMax() ret " << retVal
			<< " Min " << min
			<< " Max " << max
          	<< " lbid " << lbid
			<< " seq32 " << seq32 << endl;
#endif

        if(retVal != BRM::CP_VALID) // invalid extent that needs to be validated
        {
#ifdef DEBUG
			cout << "Added Mx/Mn to partitionVector" << endl;
#endif
            mmp = new MinMaxPartition();
            mmp->lbid = (int64_t)LBIDR.start;
            mmp->lbidmax = (int64_t)(LBIDR.start+LBIDR.size);
            mmp->seq = seq32;
            if (isUnsigned(colDataType))
            {
                mmp->max = 0;
                mmp->min = static_cast<int64_t>(numeric_limits<uint64_t>::max());
            }
            else
            {
                mmp->max = numeric_limits<int64_t>::min();
                mmp->min = numeric_limits<int64_t>::max();
            }
            mmp->isValid = retVal;
            mmp->blksScanned = 0;
            lbidPartitionVector.push_back(mmp);
            bRet = false;
        }
        return bRet;
      }
    }

    return false;
}

bool LBIDList::GetMinMax(int64_t *min, int64_t *max, int64_t *seq,
	int64_t lbid, const tr1::unordered_map<int64_t, BRM::EMEntry> &entries,
    execplan::CalpontSystemCatalog::ColDataType colDataType)
{
	tr1::unordered_map<int64_t, BRM::EMEntry>::const_iterator it = entries.find(lbid);

	if (it == entries.end())
		return false;

	const BRM::EMEntry &entry = it->second;

	if (entry.partition.cprange.isValid != BRM::CP_VALID) {
	    MinMaxPartition *mmp;
        mmp = new MinMaxPartition();
        mmp->lbid = lbid;
        mmp->lbidmax = lbid + (entry.range.size * 1024);
        mmp->seq = entry.partition.cprange.sequenceNum;
        if (isUnsigned(colDataType))
        {
            mmp->max = 0;
            mmp->min = static_cast<int64_t>(numeric_limits<uint64_t>::max());
        }
        else
        {
            mmp->max = numeric_limits<int64_t>::min();
            mmp->min = numeric_limits<int64_t>::max();
        }
        mmp->isValid = entry.partition.cprange.isValid;
        mmp->blksScanned = 0;
        lbidPartitionVector.push_back(mmp);
		return false;
	}

	*min = entry.partition.cprange.lo_val;
	*max = entry.partition.cprange.hi_val;
	*seq = entry.partition.cprange.sequenceNum;
	return true;
}


// Get the min, max, and sequence number for the specified LBID by searching
// the given vector of ExtentMap entries.
int LBIDList::getMinMaxFromEntries(int64_t& min, int64_t& max, int32_t& seq,
	int64_t lbid, const std::vector<struct BRM::EMEntry>& EMEntries)
{
	for (unsigned i=0; i<EMEntries.size(); i++)
	{
		int64_t lastLBID = EMEntries[i].range.start + (EMEntries[i].range.size * 1024) - 1;
		if (lbid >= EMEntries[i].range.start && lbid <= lastLBID)
		{
			min =  EMEntries[i].partition.cprange.lo_val;
			max =  EMEntries[i].partition.cprange.hi_val;
			seq =  EMEntries[i].partition.cprange.sequenceNum;
			return EMEntries[i].partition.cprange.isValid;
		}
	}

	return BRM::CP_INVALID;
}

//
  void LBIDList::UpdateMinMax(int64_t min, int64_t max, int64_t lbid, CalpontSystemCatalog::ColDataType type,
		  bool validData)
  {
    MinMaxPartition *mmp=NULL;
#ifdef DEBUG
    cout << "UpdateMinMax() Mn/Mx " << min << "/" << max
		<< " lbid " << lbid
		<< " sz " << lbidPartitionVector.size() << endl;
#endif

    for(uint i = 0; i<lbidPartitionVector.size(); i++)
    {

      mmp = lbidPartitionVector.at(i);
      if ( (lbid>=mmp->lbid) && (lbid<mmp->lbidmax) )
      {
        mmp->blksScanned++;

#ifdef DEBUG
        if (IS_VERBOSE)
          cout << "UpdateMinMax() old Mn/Mx "
			<< mmp->min << "/" << mmp->max
			<< " lbid " << lbid
			<< " seq " << mmp->seq
			<< " valid " << mmp->isValid << endl;
#endif
        if (!validData) {
        	//cout << "Invalidating an extent b/c of a versioned block!\n";
        	mmp->isValid = BRM::CP_UPDATING;
        	return;
        }

		if (mmp->isValid == BRM::CP_INVALID)
		{
			if (execplan::isCharType(type))
			{
				if (order_swap(min) < order_swap(mmp->min) ||
				  mmp->min == numeric_limits<int64_t>::max())
					mmp->min = min;
				if (order_swap(max) > order_swap(mmp->max) ||
				  mmp->max == numeric_limits<int64_t>::min())
					mmp->max = max;
			}
			else if (execplan::isUnsigned(type))
			{
	        	if (static_cast<uint64_t>(min) < static_cast<uint64_t>(mmp->min))
					mmp->min = min;
		        if (static_cast<uint64_t>(max) > static_cast<uint64_t>(mmp->max))
					mmp->max = max;
			}
			else
			{
	        	if (min < mmp->min)
					mmp->min = min;
		        if (max > mmp->max)
					mmp->max = max;
			}

		}

#ifdef DEBUG
        if (IS_VERBOSE)
          cout << "UpdateMinMax() new Mn/Mx "
			<< mmp->min << "/" << mmp->max << " "
			<< " lbid " << lbid
			<< " seq " << mmp->seq
			<< " valid " << mmp->isValid << endl;
#endif
        return;
      }
    }
  }

void LBIDList::UpdateAllPartitionInfo()
{
    MinMaxPartition *mmp=NULL;
#ifdef DEBUG
    if (IS_VERBOSE)
        cout << "LBIDList::UpdateAllPartitionInfo() size " << lbidPartitionVector.size() << endl;
#endif

    // @bug 1970 - Added new dbrm interface that takes a vector of CPInfo objects to set the min and max for multiple
    // extents at a time.  This cuts the number of calls way down and improves performance.
    CPInfo cpInfo;
    vector<CPInfo> vCpInfo;

    uint cpUpdateInterval = 25000;
    for (uint i=0;i<lbidPartitionVector.size(); i++)
    {

        mmp = lbidPartitionVector.at(i);

        //@bug4543: only update extentmap CP if blks were scanned for this extent
        if ((mmp->isValid == BRM::CP_INVALID) && (mmp->blksScanned > 0))
        {
            cpInfo.firstLbid = mmp->lbid;
            cpInfo.max = mmp->max;
            cpInfo.min = mmp->min;
            cpInfo.seqNum = (int32_t)mmp->seq;
            vCpInfo.push_back(cpInfo);

            // Limit the number of extents to update at a time.  A map is created within the call and this will prevent unbounded
            // memory.  Probably will never approach this limit but just in case.
            if ((i+1)%cpUpdateInterval == 0 || (i+1) == lbidPartitionVector.size())
            {
                em->setExtentsMaxMin(vCpInfo);
                vCpInfo.clear();
            }

#ifdef DEBUG
            if (IS_VERBOSE)
                cout << "LBIDList::UpdateAllPartitionInfo() updated mmp.lbid " << mmp->lbid
                << " mmp->max " << mmp->max
                << " mmp->min " << mmp->min
                << " seq "      << mmp->seq
                << " blksScanned " << mmp->blksScanned
                << endl;
#endif
            mmp->isValid = BRM::CP_VALID;
        }

        //delete mmp;
    }

    // Send the last batch of CP info to BRM.
    if (!vCpInfo.empty())
    {
        em->setExtentsMaxMin(vCpInfo);
    }
}

bool LBIDList::IsRangeBoundary(uint64_t lbid)
{
    const int RangeCount = LBIDRanges.size();
    LBIDRange LBIDR;
    for(int i=0;i<RangeCount;i++)
    {
      LBIDR = LBIDRanges.at(i);
      if(lbid == (uint64_t)(LBIDR.start))
        return true;
    }
    return false;
}

/* test datatype for casual partitioning predicate optimization
 *
 *   returns true if casual partitioning predicate optimization is possible for datatype.
 *   returns false if casual partitioning predicate optimization is not possible for datatype.
 */

bool LBIDList::CasualPartitionDataType(const CalpontSystemCatalog::ColDataType type, const uint8_t size) const
{
	switch(type) {
		case CalpontSystemCatalog::CHAR:
			return size <9;
		case CalpontSystemCatalog::VARCHAR:
			return size <8;
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::DECIMAL:
		case CalpontSystemCatalog::DATE:
		case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::UTINYINT:
        case CalpontSystemCatalog::USMALLINT:
        case CalpontSystemCatalog::UDECIMAL:
        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UBIGINT:
			return true;
		default:
			return false;
	}
}

/* Check for casual partitioning predicate optimization. This function applies the predicate using
 * column Min/Max values to determine if the scan is required.
 *
 *   returns true if scan should be executed.
 *   returns false if casual partitioning predicate optimization has eliminated the scan.
 */

template<class T>
inline bool LBIDList::compareVal(const T& Min, const T& Max, const T& value, char op, uint8_t lcf)
{
	switch(op) {
		case COMPARE_LT:
		case COMPARE_NGE:
			if (value <= Min) {
				return false;
			}
			break;
		case COMPARE_LE:
		case COMPARE_NGT:
			if (value < Min) {
				return false;
			}
			break;
		case COMPARE_GT:
		case COMPARE_NLE:
			if (value >= Max) {
				return false;
			}
			break;
		case COMPARE_GE:
		case COMPARE_NLT:
			if (value > Max) {
				return false;
			}
			break;
		case COMPARE_EQ:
			if (value < Min || value > Max || lcf > 0) {
				return false;
			}
			break;
		case COMPARE_NE:
			// @bug 3087
			if ( value == Min && value == Max && lcf == 0) {
				return false;
			}
			break;
	}
	return true;
}

bool LBIDList::checkSingleValue(int64_t min, int64_t max, int64_t value,
								execplan::CalpontSystemCatalog::ColDataType type)
{
	if (isCharType(type))
	{
		uint64_t mmin = order_swap(min);
		uint64_t mmax = order_swap(max);
		uint64_t vvalue = order_swap(value);
		return (vvalue >= mmin && vvalue <= mmax);
	}
	else if (isUnsigned(type))
	{
		return (static_cast<uint64_t>(value) >= static_cast<uint64_t>(min) &&
				static_cast<uint64_t>(value) <= static_cast<uint64_t>(max));
	}
	else
	{
		return (value >= min && value <= max);
	}
}

bool LBIDList::checkRangeOverlap(int64_t min, int64_t max, int64_t tmin, int64_t tmax,
								 execplan::CalpontSystemCatalog::ColDataType type)
{
	if (isCharType(type))
	{
		uint64_t min2 = order_swap(min);
		uint64_t max2 = order_swap(max);
		uint64_t tmin2 = order_swap(tmin);
		uint64_t tmax2 = order_swap(tmax);
		return (tmin2 <= max2 && tmax2 >= min2);
	}
	else if (isUnsigned(type))
	{
		return (static_cast<uint64_t>(tmin) <= static_cast<uint64_t>(max) &&
				static_cast<uint64_t>(tmax) >= static_cast<uint64_t>(min));
	}
	else
	{
		return (tmin <= max && tmax >= min);
	}
}

bool LBIDList::CasualPartitionPredicate(const int64_t Min,
										const int64_t Max,
										const messageqcpp::ByteStream* bs,
										const uint16_t NOPS,
										const execplan::CalpontSystemCatalog::ColType& ct,
										const uint8_t BOP)
{

	int length = bs->length(), pos = 0;
	const char *MsgDataPtr = (const char *) bs->buf();
	bool scan = true;
	int64_t value=0;
    bool bIsUnsigned = execplan::isUnsigned(ct.colDataType);
    bool bIsChar = execplan::isCharType(ct.colDataType);
	for (int i=0; i<NOPS; i++) {
		scan = true;
		pos += ct.colWidth + 2;  // predicate + op + lcf

		if (pos > length) {
#ifdef DEBUG
			cout << "CasualPartitionPredicate: Filter parsing went beyond the end of the filter string!" << endl;
#endif
			return true;
		}

		char op = *MsgDataPtr++;
		uint8_t lcf = *(uint8_t*)MsgDataPtr++;
        if (bIsUnsigned)
        {
            switch (ct.colWidth)
            {
                case 1: {
                    uint8_t val = *(int8_t*)MsgDataPtr;
                    value = val;
                    break;
                }
                case 2: {
                    uint16_t val = *(int16_t*)MsgDataPtr;
                    value = val;
                    break;
                }
                case 4: {
                    uint32_t val = *(int32_t*)MsgDataPtr;
                    value = val;
                    break;
                }
                case 8: {
                    uint64_t val = *(int64_t*)MsgDataPtr;
                    value = static_cast<int64_t>(val);
                }
            }
        }
        else
        {
            switch (ct.colWidth)
            {
                case 1: {
                    int8_t val = *(int8_t*)MsgDataPtr;
                    value = val;
                    break;
                }
                case 2: {
                    int16_t val = *(int16_t*)MsgDataPtr;
                    value = val;
                    break;
                }
                case 4: {
                    int32_t val = *(int32_t*)MsgDataPtr;
                    value = val;
                    break;
                }
                case 8: {
                    int64_t val = *(int64_t*)MsgDataPtr;
                    value = val;
                }
            }
        }
		// Should we also check for empty here?
		if (isNull(value, ct))	// This will work even if the data column is unsigned.
		{
			continue;
		}

		MsgDataPtr += ct.colWidth;

		if (bIsChar && 1 < ct.colWidth)
        {
			scan = compareVal(order_swap(Min), order_swap(Max), order_swap(value),
			  op, lcf);
// 			cout << "scan=" << (uint) scan << endl;
		}
		else if (bIsUnsigned)
		{
			scan = compareVal(static_cast<uint64_t>(Min), static_cast<uint64_t>(Max), static_cast<uint64_t>(value), op, lcf);
		}
		else
		{
			scan = compareVal(Min, Max, value, op, lcf);
		}

		if (BOP == BOP_AND && !scan) {
			break;
		}

		if (BOP == BOP_OR && scan) {
			break;
		}
		//TODO: What about BOP_NONE?

	} // for()
#ifdef DEBUG
	if (IS_VERBOSE)
		cout << "CPPredicate " << (scan==true ? "TRUE":"FALSE") << endl;
#endif

	return scan;
} // CasualPartitioningPredicate

void LBIDList::copyLbidList(const LBIDList& rhs)
{
	em = rhs.em;
	vector<MinMaxPartition*>::value_type ptr;
	while (!lbidPartitionVector.empty())
	{
		ptr = lbidPartitionVector.back();
		lbidPartitionVector.pop_back();
		delete ptr;
	}
	lbidPartitionVector.clear();	//Overkill...
	vector<MinMaxPartition*>::const_iterator iter = rhs.lbidPartitionVector.begin();
	vector<MinMaxPartition*>::const_iterator end = rhs.lbidPartitionVector.end();
	while (iter != end)
	{
		MinMaxPartition* mmp = new MinMaxPartition();
		*mmp = **iter;
		lbidPartitionVector.push_back(mmp);
		++iter;
	}
	LBIDRanges = rhs.LBIDRanges;
	fDebug = rhs.fDebug;
}

} //namespace joblist

// vim:ts=4 sw=4:
