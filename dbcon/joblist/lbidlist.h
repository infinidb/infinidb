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

/***********************************************************************
*   $Id: lbidlist.h 9142 2012-12-11 22:18:06Z pleblanc $
*
*
***********************************************************************/
/** @file */

#ifndef JOBLIST_LBIDLIST_H
#define JOBLIST_LBIDLIST_H

#include <boost/shared_ptr.hpp>
#include "joblisttypes.h"
#include "calpontsystemcatalog.h"
#include "brmtypes.h"
#include "bytestream.h"
#include <iostream>
#include "brm.h"
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif

namespace joblist 
{ 

typedef BRM::LBIDRange_v LBIDRangeVector;

/** @brief struct MinMaxPartition
 *
 */
struct MinMaxPartition
{
	int64_t lbid;
	int64_t lbidmax;
	int64_t min;
	int64_t max;
	int64_t seq;
	int     isValid;
};

/** @brief class LBIDList
 *
 */
class LBIDList
{
public:

	explicit LBIDList(const execplan::CalpontSystemCatalog::OID oid,
					const int debug);

	explicit LBIDList(const int debug);

	void init(const execplan::CalpontSystemCatalog::OID oid,
			const int debug);

	virtual ~LBIDList();

	void Dump(long Index, int Count) const;
	u_int32_t GetRangeSize() const { return LBIDRanges.size() ? LBIDRanges.at(0).size : 0; }

	// New functions to handle min/max values per lbid for casual partitioning;
	// If pEMEntries is provided, then min/max will be extracted from that
	// vector, else extents in BRM will be searched.
	bool GetMinMax(int64_t& min, int64_t& max, int64_t& seq, int64_t lbid,
			const std::vector<struct BRM::EMEntry>* pEMEntries);

	bool GetMinMax(int64_t *min, int64_t *max, int64_t *seq, int64_t lbid,
			const std::tr1::unordered_map<int64_t, BRM::EMEntry> &entries);

	void UpdateMinMax(int64_t min, int64_t max, int64_t lbid,
	  execplan::CalpontSystemCatalog::ColDataType type, bool validData = true);

	void UpdateAllPartitionInfo();

	bool IsRangeBoundary(uint64_t lbid);

	bool CasualPartitionPredicate(const int64_t Min, 
				const int64_t Max, 
				const messageqcpp::ByteStream* MsgDataPtr,
				const uint16_t NOPS, 
				const execplan::CalpontSystemCatalog::ColType& ct, 
				const uint8_t BOP);

	bool checkSingleValue(int64_t min, int64_t max, int64_t value, bool isCharColumn);
	
	bool checkRangeOverlap(int64_t min, int64_t max, int64_t tmin, int64_t tmax, bool isCharColumn);

	// check the column data type and the column size to determine if it
	// is a data type  to apply casual paritioning.
	bool CasualPartitionDataType(const uint8_t type, const uint8_t size) const;

	LBIDList(const LBIDList& rhs) { copyLbidList(rhs); }

	LBIDList& operator=(const LBIDList& rhs) { copyLbidList(rhs); return *this;}

private:
	LBIDList();

	void copyLbidList(const LBIDList& rhs);

	template<class T>
	inline bool compareVal(const T& Min, const T& Max, const T& value, char op, uint8_t lcf);

	inline bool isChar(execplan::CalpontSystemCatalog::ColDataType type)
	{
		return (execplan::CalpontSystemCatalog::VARCHAR == type || execplan::CalpontSystemCatalog::CHAR == type);
	}

	template <class T>
	inline bool checkNull(execplan::CalpontSystemCatalog::ColDataType type, T val, T nullVal, T nullCharVal);

	inline bool checkNull32(execplan::CalpontSystemCatalog::ColDataType type, int32_t val);
	inline bool checkNull64(execplan::CalpontSystemCatalog::ColDataType type, int64_t val);

	int  getMinMaxFromEntries(int64_t& min, int64_t& max, int32_t& seq,
			int64_t lbid, const std::vector<struct BRM::EMEntry>& EMEntries);

	boost::shared_ptr<BRM::DBRM> em;
	std::vector<MinMaxPartition*> lbidPartitionVector;
	LBIDRangeVector LBIDRanges;
	int fDebug;

}; // LBIDList


} // joblist
#endif
