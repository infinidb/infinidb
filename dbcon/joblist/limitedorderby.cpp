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

//  $Id: limitedorderby.cpp 9581 2013-05-31 13:46:14Z pleblanc $


#include <iostream>
//#define NDEBUG
#include <cassert>
#include <string>
using namespace std;

#include <boost/shared_array.hpp>
using namespace boost;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "rowgroup.h"
using namespace rowgroup;

#include "idborderby.h"
using namespace ordering;

#include "jlf_common.h"
#include "limitedorderby.h"


namespace joblist
{


// LimitedOrderBy class implementation
LimitedOrderBy::LimitedOrderBy() : fStart(0), fCount(-1)
{
	fRule.fIdbCompare = this;
}


LimitedOrderBy::~LimitedOrderBy()
{
}


void LimitedOrderBy::initialize(const RowGroup& rg, const JobInfo& jobInfo)
{
	fRm = &jobInfo.rm;
	fErrorCode = ERR_LIMIT_TOO_BIG;

	// locate column position in the rowgroup
	map<uint, uint> keyToIndexMap;
	for (uint64_t i = 0; i < rg.getKeys().size(); ++i)
	{
		if (keyToIndexMap.find(rg.getKeys()[i]) == keyToIndexMap.end())
			keyToIndexMap.insert(make_pair(rg.getKeys()[i], i));
	}

	vector<pair<uint32_t, bool> >::const_iterator i = jobInfo.orderByColVec.begin();
	for ( ; i != jobInfo.orderByColVec.end(); i++)
	{
		map<uint, uint>::iterator j = keyToIndexMap.find(i->first);
		idbassert(j != keyToIndexMap.end());
		fOrderByCond.push_back(IdbSortSpec(j->second, i->second));
	}

	// limit row count info
	fStart = jobInfo.limitStart;
	fCount = jobInfo.limitCount;

//	fMemSize = (fStart + fCount) * rg.getRowSize();

	IdbOrderBy::initialize(rg);
}


uint64_t LimitedOrderBy::getKeyLength() const
{
	//return (fRow0.getSize() - 2);
	return fRow0.getColumnCount();
}


void LimitedOrderBy::processRow(const rowgroup::Row& row)
{
	// check if this is a distinct row
	if (fDistinct && fDistinctMap->find(row.getPointer()) != fDistinctMap->end())
		return;

	// @bug5312, limit count is 0, do nothing.
	if (fCount == 0)
		return;

	// if the row count is less than the limit
	if (fOrderByQueue.size() < fStart+fCount)
	{
		copyRow(row, &fRow0);
		//memcpy(fRow0.getData(), row.getData(), row.getSize());
		OrderByRow newRow(fRow0, fRule);
		fOrderByQueue.push(newRow);

		// add to the distinct map
		if (fDistinct)
			fDistinctMap->insert(fRow0.getPointer());
			//fDistinctMap->insert(make_pair((fRow0.getData()+2), fRow0.getData()));

		fRowGroup.incRowCount();
		fRow0.nextRow();

		if (fRowGroup.getRowCount() >= fRowsPerRG)
		{
			fDataQueue.push(fData);
			uint64_t newSize = fRowsPerRG * fRowGroup.getRowSize();
			if (!fRm->getMemory(newSize))
			{
				cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode)
					 << " @" << __FILE__ << ":" << __LINE__;
				throw IDBExcept(fErrorCode);
			}
			fMemSize += newSize;
			fData.reinit(fRowGroup, fRowsPerRG);
			fRowGroup.setData(&fData);
			fRowGroup.resetRowGroup(0);
			fRowGroup.getRow(0, &fRow0);
		}
	}

	else if (fOrderByCond.size() > 0 && fRule.less(row.getPointer(), fOrderByQueue.top().fData))
	{
		OrderByRow swapRow = fOrderByQueue.top();
		row1.setData(swapRow.fData);
		if (!fDistinct)
		{
			copyRow(row, &row1);
			//memcpy(swapRow.fData, row.getData(), row.getSize());
		}
		else
		{
			fDistinctMap->erase(row.getPointer());
			copyRow(row, &row1);
			fDistinctMap->insert(row1.getPointer());
			//fDistinctMap->erase(fDistinctMap->find(row.getData() + 2));
			//memcpy(swapRow.fData, row.getData(), row.getSize());
			//fDistinctMap->insert(make_pair((swapRow.fData+2), swapRow.fData));
		}
		fOrderByQueue.pop();
		fOrderByQueue.push(swapRow);
	}
}


void LimitedOrderBy::finalize()
{
	if (fRowGroup.getRowCount() > 0)
		fDataQueue.push(fData);

	if (fStart != 0)
	{
		uint64_t newSize = fRowsPerRG * fRowGroup.getRowSize();
		if (!fRm->getMemory(newSize))
		{
			cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode)
				 << " @" << __FILE__ << ":" << __LINE__;
			throw IDBExcept(fErrorCode);
		}
		fMemSize += newSize;
		fData.reinit(fRowGroup, fRowsPerRG);
		fRowGroup.setData(&fData);
		fRowGroup.resetRowGroup(0);
		fRowGroup.getRow(0, &fRow0);
		queue<RGData> tempQueue;
		uint64_t i = 0;
		while ((fOrderByQueue.size() > fStart) && (i++ < fCount))
		{
			const OrderByRow& topRow = fOrderByQueue.top();
			row1.setData(topRow.fData);
			copyRow(row1, &fRow0);
			//memcpy(fRow0.getData(), topRow.fData, fRow0.getSize());
			fRowGroup.incRowCount();
			fRow0.nextRow();
			fOrderByQueue.pop();

			if (fRowGroup.getRowCount() >= fRowsPerRG)
			{
				tempQueue.push(fData);
				if (!fRm->getMemory(newSize))
				{
					cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode)
					 << " @" << __FILE__ << ":" << __LINE__;
					throw IDBExcept(fErrorCode);
				}
				fMemSize += newSize;
				fData.reinit(fRowGroup, fRowsPerRG);
				//fData.reset(new uint8_t[fRowGroup.getDataSize(fRowsPerRG)]);
				fRowGroup.setData(&fData);
				fRowGroup.resetRowGroup(0);
				fRowGroup.getRow(0, &fRow0);
			}
		}
		if (fRowGroup.getRowCount() > 0)
			tempQueue.push(fData);

		fDataQueue = tempQueue;
	}
}


const string LimitedOrderBy::toString() const
{
	ostringstream oss;
	oss << "OrderBy   cols: ";
	vector<IdbSortSpec>::const_iterator i = fOrderByCond.begin();
	for (; i != fOrderByCond.end(); i++)
		oss << "(" << i->fIndex << ","
			<< ((i->fAsc)?"Asc":"Desc") << ","
			<< ((i->fNf)?"null first":"null last") << ") ";

	oss << " start-" << fStart << " count-" << fCount;
	if (fDistinct)
		oss << " distinct";
	oss << endl;

	return oss.str();
}


}
// vim:ts=4 sw=4:

