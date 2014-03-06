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

//  $Id: idborderby.h 4012 2013-07-24 21:04:45Z pleblanc $


/** @file */

#ifndef IDB_ORDER_BY_H
#define IDB_ORDER_BY_H

#include <queue>
#include <utility>
#include <vector>
#include <sstream>
#include <boost/shared_array.hpp>

#ifdef _MSC_VER
#include <unordered_set>
#else
#include <tr1/unordered_set>
#endif

#include "rowgroup.h"
#include "hasher.h"
#include "stlpoolallocator.h"


// forward reference
namespace joblist
{
class  ResourceManager;
}


namespace ordering
{


// forward reference
class  IdbCompare;

// order by specification
struct IdbSortSpec
{
	int fIndex;
	int fAsc;   // <ordering specification> ::= ASC | DESC
	int fNf;    // <null ordering> ::= NULLS FIRST | NULLS LAST

	IdbSortSpec() : fIndex(-1), fAsc(1), fNf(1) {}
	IdbSortSpec(int i, bool b) : fIndex(i), fAsc(b ? 1 : -1), fNf(fAsc) {}
	IdbSortSpec(int i, bool b, bool n) : fIndex(i), fAsc(b ? 1 : -1), fNf(n ? 1 : -1) {}
};


// compare functor for different datatypes
// cannot use template because Row's getXxxField method.
class Compare
{
public:
	Compare(const IdbSortSpec& spec) : fSpec(spec) {}
	virtual ~Compare() {}

	virtual int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) = 0;

protected:
	IdbSortSpec fSpec;
};


class IntCompare : public Compare
{
public:
	IntCompare(const IdbSortSpec& spec) : Compare(spec) {}

	int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};


class UintCompare : public Compare
{
public:
	UintCompare(const IdbSortSpec& spec) : Compare(spec) {}

	int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};


class StringCompare : public Compare
{
public:
	StringCompare(const IdbSortSpec& spec) : Compare(spec) {}

	int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};


class DoubleCompare : public Compare
{
public:
	DoubleCompare(const IdbSortSpec& spec) : Compare(spec) {}

	int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};


class FloatCompare : public Compare
{
public:
	FloatCompare(const IdbSortSpec& spec) : Compare(spec) {}

	int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};


class CompareRule
{
public:
	CompareRule(IdbCompare* c = NULL) : fIdbCompare(c) {}


	bool less(rowgroup::Row::Pointer r1, rowgroup::Row::Pointer r2);

	void compileRules(const std::vector<IdbSortSpec>&, const rowgroup::RowGroup&);

	std::vector<Compare*>           fCompares;
	IdbCompare*                     fIdbCompare;
};


class IdbCompare
{
public:
	IdbCompare() {};
	virtual ~IdbCompare() {};

	virtual void initialize(const rowgroup::RowGroup&);
	void setStringTable(bool b);

	rowgroup::Row& row1() { return fRow1; }
	rowgroup::Row& row2() { return fRow2; }

protected:
	rowgroup::RowGroup              fRowGroup;
	rowgroup::Row                   fRow1;
	rowgroup::Row                   fRow2;
};


class OrderByRow
{
public:
	OrderByRow(const rowgroup::Row& r, CompareRule& c) : fData(r.getPointer()), fRule(&c) {}

	bool operator < (const OrderByRow& rhs) const { return fRule->less(fData, rhs.fData); }

	rowgroup::Row::Pointer                        fData;
	CompareRule*                    fRule;
};


class EqualCompData : public IdbCompare
{
public:
	EqualCompData(std::vector<uint64_t>& v) : fIndex(v) {}
	EqualCompData(std::vector<uint64_t>& v, const rowgroup::RowGroup& rg) :
		fIndex(v) { initialize(rg); }

	~EqualCompData() {};

	bool operator()(rowgroup::Row::Pointer, rowgroup::Row::Pointer);

//protected:
	std::vector<uint64_t>           fIndex;
};


class OrderByData : public IdbCompare
{
public:
	OrderByData(const std::vector<IdbSortSpec>&, const rowgroup::RowGroup&);
	virtual ~OrderByData() {};

	bool operator() (rowgroup::Row::Pointer p1, rowgroup::Row::Pointer p2) { return fRule.less(p1, p2); }
	const CompareRule& rule() const { return fRule; }

protected:
	CompareRule                     fRule;
};


// base classs for order by clause used in IDB
class IdbOrderBy : public IdbCompare
{
public:
	IdbOrderBy();
	virtual ~IdbOrderBy();

	virtual void initialize(const rowgroup::RowGroup&);
	virtual void processRow(const rowgroup::Row&) = 0;
	virtual uint64_t getKeyLength() const = 0;
	virtual const std::string toString() const = 0;

	bool getData(rowgroup::RGData& data);

	void distinct(bool b) { fDistinct = b; }
	bool distinct() const { return fDistinct; }

protected:
	std::vector<IdbSortSpec>            fOrderByCond;
	std::priority_queue<OrderByRow>     fOrderByQueue;
	rowgroup::Row                       fRow0;
	CompareRule                         fRule;

	rowgroup::RGData        fData;
	std::queue<rowgroup::RGData> fDataQueue;

	struct Hasher {
		IdbOrderBy *ts;
		utils::Hasher_r h;
		uint32_t colCount;
		Hasher(IdbOrderBy *t, uint32_t c) : ts(t), colCount(c) { }
		uint64_t operator()(const rowgroup::Row::Pointer &) const;
	};
	struct Eq {
		IdbOrderBy *ts;
		uint32_t colCount;
		Eq(IdbOrderBy *t, uint32_t c) : ts(t), colCount(c) { }
		bool operator()(const rowgroup::Row::Pointer &, const rowgroup::Row::Pointer &) const;
	};

	typedef std::tr1::unordered_set<rowgroup::Row::Pointer, Hasher, Eq,
	            utils::STLPoolAllocator<rowgroup::Row::Pointer> > DistinctMap_t;
	boost::scoped_ptr<DistinctMap_t>    fDistinctMap;
	rowgroup::Row row1, row2;  // scratch space for Hasher & Eq

	bool                                fDistinct;
	uint64_t                            fMemSize;
	uint64_t                            fRowsPerRG;
	uint64_t                            fErrorCode;
	joblist::ResourceManager*           fRm;
};


}

#endif  // IDB_ORDER_BY_H

