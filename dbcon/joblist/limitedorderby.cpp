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

//  $Id: limitedorderby.cpp 7705 2011-05-17 17:18:13Z xlou $


#include <iostream>
//#define NDEBUG
#include <cassert>
#include <string>
#include <stack>
using namespace std;

#include <boost/shared_array.hpp>
using namespace boost;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "dataconvert.h"
using namespace dataconvert;

#include "jobstep.h"
#include "jlf_common.h"
#include "limitedorderby.h"


namespace joblist
{

int IntCompare::operator()(IdbOrderBy* l, uint8_t* r1, uint8_t* r2)
{
	l->row1().setData(r1);
	l->row2().setData(r2);

	bool b1 = l->row1().isNullValue(fIndex);
	bool b2 = l->row2().isNullValue(fIndex);

	int ret = 0;

	if (b1 == true || b2 == true)
	{
		if (b1 == false && b2 == true)
			ret = 1;
		else if (b1 == true && b2 == false)
			ret = -1;
	}
	else
	{
		int64_t v1 = l->row1().getIntField(fIndex);
		int64_t v2 = l->row2().getIntField(fIndex);

		if (v1 > v2)
			ret = 1;
		else if (v1 < v2)
			ret = -1;
	}

	return ret * fAsc;
}


int UintCompare::operator()(IdbOrderBy* l, uint8_t* r1, uint8_t* r2)
{
	l->row1().setData(r1);
	l->row2().setData(r2);

	bool b1 = l->row1().isNullValue(fIndex);
	bool b2 = l->row2().isNullValue(fIndex);

	int ret = 0;

	if (b1 == true || b2 == true)
	{
		if (b1 == false && b2 == true)
			ret = 1;
		else if (b1 == true && b2 == false)
			ret = -1;
	}
	else
	{
		uint64_t v1 = l->row1().getUintField(fIndex);
		uint64_t v2 = l->row2().getUintField(fIndex);

		if (v1 > v2)
			ret = 1;
		else if (v1 < v2)
			ret = -1;
	}

	return ret * fAsc;
}


int StringCompare::operator()(IdbOrderBy* l, uint8_t* r1, uint8_t* r2)
{
	l->row1().setData(r1);
	l->row2().setData(r2);

	bool b1 = l->row1().isNullValue(fIndex);
	bool b2 = l->row2().isNullValue(fIndex);

	int ret = 0;

	if (b1 == true || b2 == true)
	{
		if (b1 == false && b2 == true)
			ret = 1;
		else if (b1 == true && b2 == false)
			ret = -1;
	}
	else
	{
		string v1 = l->row1().getStringField(fIndex);
		string v2 = l->row2().getStringField(fIndex);

		if (v1 > v2)
			ret = 1;
		else if (v1 < v2)
			ret = -1;
	}

	return ret * fAsc;
}


int DoubleCompare::operator()(IdbOrderBy* l, uint8_t* r1, uint8_t* r2)
{
	l->row1().setData(r1);
	l->row2().setData(r2);

	bool b1 = l->row1().isNullValue(fIndex);
	bool b2 = l->row2().isNullValue(fIndex);

	int ret = 0;

	if (b1 == true || b2 == true)
	{
		if (b1 == false && b2 == true)
			ret = 1;
		else if (b1 == true && b2 == false)
			ret = -1;
	}
	else
	{
		double v1 = l->row1().getDoubleField(fIndex);
		double v2 = l->row2().getDoubleField(fIndex);

		if (v1 > v2)
			ret = 1;
		else if (v1 < v2)
			ret = -1;
	}
	
	return ret * fAsc;
}


int FloatCompare::operator()(IdbOrderBy* l, uint8_t* r1, uint8_t* r2)
{
	l->row1().setData(r1);
	l->row2().setData(r2);

	bool b1 = l->row1().isNullValue(fIndex);
	bool b2 = l->row2().isNullValue(fIndex);

	int ret = 0;

	if (b1 == true || b2 == true)
	{
		if (b1 == false && b2 == true)
			ret = 1;
		else if (b1 == true && b2 == false)
			ret = -1;
	}
	else
	{
		float v1 = l->row1().getFloatField(fIndex);
		float v2 = l->row2().getFloatField(fIndex);

		if (v1 > v2)
			ret = 1;
		else if (v1 < v2)
			ret = -1;
	}

	return ret * fAsc;
}


// IdbOrderBy class implementation
IdbOrderBy::IdbOrderBy() :
	fDistinct(false), fMemSize(0), fRowsPerRG(8192), fErrorCode(0), fRm(NULL)
{
}


IdbOrderBy::~IdbOrderBy()
{
	if (fRm)
		fRm->returnMemory(fMemSize);

	// delete compare objects
	vector<Compare*>::iterator i = fRule.fCompares.begin();
	while (i != fRule.fCompares.end())
		delete *i++;
}


void IdbOrderBy::initialize(const RowGroup& rg)
{
	// initialize rows
	fRowGroup = rg;

	uint64_t newSize = fRowsPerRG * rg.getRowSize();
	if (!fRm->getMemory(newSize))
	{
		cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode)
			 << " @" << __FILE__ << ":" << __LINE__;
		throw IDBExcept(fErrorCode);
	}
	fMemSize += newSize;
	fData.reset(new uint8_t[fRowGroup.getDataSize(fRowsPerRG)]);
	fRowGroup.setData(fData.get());
	fRowGroup.resetRowGroup(0);
	fRowGroup.initRow(&fRow0);
	fRowGroup.initRow(&fRow1);
	fRowGroup.initRow(&fRow2);
	fRowGroup.getRow(0, &fRow0);

	// set compare functors
	const vector<CalpontSystemCatalog::ColDataType>& types = fRowGroup.getColTypes();
	for (vector<pair<int, bool> >::iterator i = fOrderByCond.begin(); i != fOrderByCond.end(); i++)
	{
		switch (types[i->first])
		{
			case CalpontSystemCatalog::TINYINT:
			case CalpontSystemCatalog::SMALLINT:
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::BIGINT:
			case CalpontSystemCatalog::DECIMAL:
			{
				Compare* c = new IntCompare(i->first, i->second);
				fRule.fCompares.push_back(c);
				break;
			}
			case CalpontSystemCatalog::CHAR:
			case CalpontSystemCatalog::VARCHAR:
			{
				Compare* c = new StringCompare(i->first, i->second);
				fRule.fCompares.push_back(c);
				break;
			}
			case CalpontSystemCatalog::DOUBLE:
			{
				Compare* c = new DoubleCompare(i->first, i->second);
				fRule.fCompares.push_back(c);
				break;
			}
			case CalpontSystemCatalog::FLOAT:
			{
				Compare* c = new FloatCompare(i->first, i->second);
				fRule.fCompares.push_back(c);
				break;
			}
			case CalpontSystemCatalog::DATE:
			case CalpontSystemCatalog::DATETIME:
			{
				Compare* c = new UintCompare(i->first, i->second);
				fRule.fCompares.push_back(c);
				break;
			}
			default:
			{
				break;
			}
		}
	}

	if (fDistinct)
	{
		uint64_t keyLength = getKeyLength();
		utils::TupleHasher   hasher(keyLength);
		utils::TupleComparator comp(keyLength);
		fPool.reset(new utils::SimplePool);
		utils::SimpleAllocator<pair<uint8_t* const, uint8_t*> > alloc(fPool);
		fDistinctMap.reset(new DistinctMap_t(10, hasher, comp, alloc));
	}
}


bool IdbOrderBy::getData(boost::shared_array<uint8_t>& data)
{
	if (fDataQueue.empty())
		return false;

	data = fDataQueue.front();
	fDataQueue.pop();

	return true;
}


//bool IdbOrderBy::CompareRule::less(const uint8_t* r1, const uint8_t* r2)
bool IdbOrderBy::CompareRule::less(uint8_t* r1, uint8_t* r2)
{
	for (vector<Compare*>::iterator i = fCompares.begin(); i != fCompares.end(); i++)
	{
		int c = ((*(*i))(fOrderBy, r1, r2));

		if (c < 0)
			return true;
		else if (c > 0)
			return false;
	}

	return false;
}


// LimitedOrderBy class implementation
LimitedOrderBy::LimitedOrderBy() : fStart(0), fCount(-1)
{
	fRule.fOrderBy = this;
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
		assert(j != keyToIndexMap.end());
		fOrderByCond.push_back(make_pair(j->second, i->second));
	}

	// limit row count info
	fStart = jobInfo.limitStart;
	fCount = jobInfo.limitCount;

//	fMemSize = (fStart + fCount) * rg.getRowSize();

	IdbOrderBy::initialize(rg);
}


uint64_t LimitedOrderBy::getKeyLength() const
{
	return (fRow0.getSize() - 2);
}


void LimitedOrderBy::processRow(const rowgroup::Row& row)
{
	// check if this is a distinct row
	if (fDistinct && fDistinctMap->find(row.getData() + 2) != fDistinctMap->end())
		return;

	// if the row count is less than the limit
	if (fOrderByQueue.size() < fStart+fCount)
	{
		memcpy(fRow0.getData(), row.getData(), row.getSize());
		OrderByRow newRow(fRow0, fRule);
		fOrderByQueue.push(newRow);

		// add to the distinct map
		if (fDistinct)
			fDistinctMap->insert(make_pair((fRow0.getData()+2), fRow0.getData()));

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
			fData.reset(new uint8_t[fRowGroup.getDataSize(fRowsPerRG)]);
			fRowGroup.setData(fData.get());
			fRowGroup.resetRowGroup(0);
			fRowGroup.getRow(0, &fRow0);
		}
	}

	else if (fOrderByCond.size() > 0 && fRule.less(row.getData(), fOrderByQueue.top().fData))
	{
		OrderByRow swapRow = fOrderByQueue.top();
		if (!fDistinct)
		{
			memcpy(swapRow.fData, row.getData(), row.getSize());
		}
		else
		{
			fDistinctMap->erase(fDistinctMap->find(row.getData() + 2));
			memcpy(swapRow.fData, row.getData(), row.getSize());
			fDistinctMap->insert(make_pair((swapRow.fData+2), swapRow.fData));
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
		fData.reset(new uint8_t[fRowGroup.getDataSize(fRowsPerRG)]);
		fRowGroup.setData(fData.get());
		fRowGroup.resetRowGroup(0);
		fRowGroup.getRow(0, &fRow0);
		queue<shared_array<uint8_t> > tempQueue;
		uint64_t i = 0;
		while ((fOrderByQueue.size() > fStart) && (i++ < fCount))
		{
			const OrderByRow& topRow = fOrderByQueue.top();
			memcpy(fRow0.getData(), topRow.fData, fRow0.getSize());
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
				fData.reset(new uint8_t[fRowGroup.getDataSize(fRowsPerRG)]);
				fRowGroup.setData(fData.get());
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
	vector<pair<int, bool> >::const_iterator i = fOrderByCond.begin();
	for (; i != fOrderByCond.end(); i++)
		oss << "(" << i->first << "," << ((i->second)?1:-1) << ") ";
	oss << " start-" << fStart << " count-" << fCount;
	if (fDistinct)
		oss << " distinct";
	oss << endl;

	return oss.str();
}


}
// vim:ts=4 sw=4:

