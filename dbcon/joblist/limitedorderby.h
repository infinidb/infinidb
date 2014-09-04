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

//  $Id: limitedorderby.h 8436 2012-04-04 18:18:21Z rdempsey $


/** @file */

#ifndef IDB_ORDER_BY_H
#define IDB_ORDER_BY_H

#include <utility>
#include <queue>
#include <vector>
#include <sstream>
#include <boost/shared_array.hpp>
#include "rowgroup.h"
#include "hasher.h"
#include "simpleallocator.h"


namespace joblist
{


// forward reference
struct JobInfo;
class  ResourceManager;
class  IdbOrderBy;

// compare functor for different datatypes
// cannot use template because Row's getXxxField method.
class Compare
{
public:
	Compare(int i, bool b) : fIndex(i), fAsc(b ? 1 : -1) {}
	virtual ~Compare() {}

	virtual int operator()(IdbOrderBy*, uint8_t*, uint8_t*) = 0;

protected:
	int fIndex;
	int fAsc;
};


class IntCompare : public Compare
{
public:
	IntCompare(int i, bool b) : Compare(i, b) {}

	int operator()(IdbOrderBy*, uint8_t*, uint8_t*);
};


class UintCompare : public Compare
{
public:
	UintCompare(int i, bool b) : Compare(i, b) {}

	int operator()(IdbOrderBy*, uint8_t*, uint8_t*);
};


class StringCompare : public Compare
{
public:
	StringCompare(int i, bool b) : Compare(i, b) {}

	int operator()(IdbOrderBy*, uint8_t*, uint8_t*);
};


class DoubleCompare : public Compare
{
public:
	DoubleCompare(int i, bool b) : Compare(i, b) {}

	int operator()(IdbOrderBy*, uint8_t*, uint8_t*);
};


class FloatCompare : public Compare
{
public:
	FloatCompare(int i, bool b) : Compare(i, b) {}

	int operator()(IdbOrderBy*, uint8_t*, uint8_t*);
};


// base classs for order by clause used in IDB
class IdbOrderBy
{
public:
	IdbOrderBy();
	virtual ~IdbOrderBy();

	virtual void initialize(const rowgroup::RowGroup&);
	virtual void processRow(const rowgroup::Row&) = 0;
	virtual uint64_t getKeyLength() const = 0;
	virtual const std::string toString() const = 0;

	bool getData(boost::shared_array<uint8_t>& data);

	rowgroup::Row& row1() { return fRow1; }
	rowgroup::Row& row2() { return fRow2; }

	void distinct(bool b) { fDistinct = b; }
	bool distinct() const { return fDistinct; }

protected:
	class CompareRule
	{
		public:
		CompareRule() : fOrderBy(NULL) {}

//		bool less(const uint8_t* r1, const uint8_t* r2);
		bool less(uint8_t* r1, uint8_t* r2);

		std::vector<Compare*>           fCompares;
		IdbOrderBy*                     fOrderBy;
	};

	class OrderByRow
	{
		public:
		OrderByRow(const rowgroup::Row& r, CompareRule& c) : fData(r.getData()), fRule(&c) {}

		bool operator < (const OrderByRow& rhs) const { return fRule->less(fData, rhs.fData); }

		uint8_t*                        fData;
		CompareRule*                    fRule;
	};

	std::vector<std::pair<int, bool> >  fOrderByCond;
	std::priority_queue<OrderByRow>     fOrderByQueue;
	rowgroup::RowGroup                  fRowGroup;
	rowgroup::Row                       fRow0;
	rowgroup::Row                       fRow1;
	rowgroup::Row                       fRow2;
	CompareRule                         fRule;

	boost::shared_array<uint8_t>        fData;
	std::queue<boost::shared_array<uint8_t> > fDataQueue;

	typedef std::tr1::unordered_map<uint8_t*, uint8_t*, utils::TupleHasher, utils::TupleComparator,
	            utils::SimpleAllocator<std::pair<uint8_t* const, uint8_t*> > > DistinctMap_t;
	boost::shared_ptr<utils::SimplePool> fPool;
	boost::scoped_ptr<DistinctMap_t>    fDistinctMap;

	bool                                fDistinct;
	uint64_t                            fMemSize;
	uint64_t                            fRowsPerRG;
	uint64_t                            fErrorCode;
	ResourceManager*                    fRm;
};


// LIMIT class
// This version is for subqueries, limit the result set to fit in memory,
// use ORDER BY to make the results consistent.
// The actual output are the first or last # of rows, which are NOT ordered.
class LimitedOrderBy : public IdbOrderBy
{
public:
	LimitedOrderBy();
	virtual ~LimitedOrderBy();

	void initialize(const rowgroup::RowGroup&, const JobInfo&);
	void processRow(const rowgroup::Row&);
	uint64_t getKeyLength() const;
	const std::string toString() const;

	void finalize();

protected:
	uint64_t                            fStart;
	uint64_t                            fCount;
};


}

#endif  // IDB_ORDER_BY_H

