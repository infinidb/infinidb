/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#ifndef TJOINER_H_
#define TJOINER_H_

#include <iostream>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/scoped_array.hpp>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif

#include "rowgroup.h"
#include "joiner.h"
#include "fixedallocator.h"
#include "joblisttypes.h"
#include "funcexpwrapper.h"
#include "stlpoolallocator.h"
#include "hasher.h"

namespace joiner
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
	
class TypelessData
{
public:
	uint8_t *data;
	uint len;

	TypelessData() : data(NULL), len(0) { }
	inline bool operator==(const TypelessData &) const;
	void serialize(messageqcpp::ByteStream &) const;
	void deserialize(messageqcpp::ByteStream &, utils::FixedAllocator &);
	void deserialize(messageqcpp::ByteStream &, utils::PoolAllocator &);
	std::string toString() const;
};

inline bool TypelessData::operator==(const TypelessData &t) const
{
	if (len != t.len)
		return false;
	if (len == 0)  // special value to force mismatches
		return false;
	return (memcmp(data, t.data, len) == 0);
}

/* This function makes the keys for string & compound joins.  The length of the
 * key is limited by keylen.  Keys that are longer are assigned a length of 0 on return,
 * signifying that it shouldn't match anything.
 */
extern TypelessData makeTypelessKey(const rowgroup::Row &,
	const std::vector<uint> &, uint keylen, utils::FixedAllocator *fa);
extern TypelessData makeTypelessKey(const rowgroup::Row &,
	const std::vector<uint> &, utils::PoolAllocator *fa);


class TupleJoiner
{
public:
	struct hasher {
		inline size_t operator()(int64_t val) const
		{ return static_cast<std::size_t>(val); }
		inline size_t operator()(const TypelessData &e) const
		{ return utils::Hasher()((char *) e.data, e.len); }
	};

	/* ctor to use for numeric join */
	TupleJoiner(
		const rowgroup::RowGroup &smallInput,
		const rowgroup::RowGroup &largeInput,
		uint smallJoinColumn,
		uint largeJoinColumn,
		joblist::JoinType jt);

	/* ctor to use for string & compound join */
	TupleJoiner(
		const rowgroup::RowGroup &smallInput,
		const rowgroup::RowGroup &largeInput,
		const std::vector<uint> &smallJoinColumns,
		const std::vector<uint> &largeJoinColumns,
		joblist::JoinType jt);

	~TupleJoiner();

	size_t size() const;
	void insert(rowgroup::Row &r);
	void doneInserting();

	/* match() returns the small-side rows that match the large-side row.
		On a UM join, it uses largeSideRow,
		on a PM join, it uses index and threadID.
	*/
	void match(rowgroup::Row &largeSideRow, uint index, uint threadID,
		std::vector<rowgroup::Row::Pointer> *matches);

	/* On a PM left outer join + aggregation, the result is already complete.
		No need to match, just mark.
	*/
	void markMatches(uint threadID, uint rowCount);

	/* For small outer joins, this is how matches are marked now. */
	void markMatches(uint threadID, const std::vector<rowgroup::Row::Pointer> &matches);

	/* Some accessors */
	inline bool inPM() const { return joinAlg == PM; }
	inline bool inUM() const { return joinAlg == UM; }
	void setInPM();
	void setInUM();
	void setThreadCount(uint cnt);
	void setPMJoinResults(boost::shared_array<std::vector<uint32_t> >,
		uint threadID);
	boost::shared_array<std::vector<uint32_t> > getPMJoinArrays(uint threadID);
	std::vector<rowgroup::Row::Pointer> *getSmallSide() { return &rows; }
	inline bool smallOuterJoin() { return ((joinType & joblist::SMALLOUTER) != 0); }
	inline bool largeOuterJoin() { return ((joinType & joblist::LARGEOUTER) != 0); }
	inline bool innerJoin() { return joinType == joblist::INNER; }
	inline bool fullOuterJoin() { return (smallOuterJoin() && largeOuterJoin()); }
	inline joblist::JoinType getJoinType() { return joinType; }
	inline const rowgroup::RowGroup &getSmallRG() { return smallRG; }
	inline const rowgroup::RowGroup &getLargeRG() { return largeRG; }
	inline uint getSmallKeyColumn() { return smallKeyColumns[0]; }
	inline uint getLargeKeyColumn() { return largeKeyColumns[0]; }
	bool hasNullJoinColumn(const rowgroup::Row &largeRow) const;
	void getUnmarkedRows(std::vector<rowgroup::Row::Pointer> *out);
	std::string getTableName() const;
	void setTableName(const std::string &tname);

	/* To allow sorting */
	bool operator<(const TupleJoiner &) const;

	uint64_t getMemUsage() const;

	/* Typeless join interface */
	inline bool isTypelessJoin() { return typelessJoin; }
	inline const std::vector<uint> & getSmallKeyColumns() { return smallKeyColumns; }
	inline const std::vector<uint> & getLargeKeyColumns() { return largeKeyColumns; }
	inline uint getKeyLength() { return keyLength; }
	
	/* Runtime casual partitioning support */
	inline const boost::scoped_array<bool> &discreteCPValues() { return discreteValues; }
	inline const boost::scoped_array<std::vector<int64_t> > &getCPData() { return cpValues; }
	inline void setUniqueLimit(uint limit) { uniqueLimit = limit; }

	/* Semi-join interface */
	inline bool semiJoin() { return ((joinType & joblist::SEMI) != 0); }
	inline bool antiJoin() { return ((joinType & joblist::ANTI) != 0); }
	inline bool scalar() { return ((joinType & joblist::SCALAR) != 0); }
	inline bool matchnulls() { return ((joinType & joblist::MATCHNULLS) != 0); }
	inline bool hasFEFilter() { return fe; }
	inline boost::shared_ptr<funcexp::FuncExpWrapper> getFcnExpFilter() { return fe; }
	void setFcnExpFilter(boost::shared_ptr<funcexp::FuncExpWrapper> fe);
	inline bool evaluateFilter(rowgroup::Row &r, uint index) { return fes[index].evaluate(&r); }
	inline uint64_t getJoinNullValue() { return joblist::BIGINTNULL; }   // a normalized NULL value
	inline uint64_t smallNullValue() { return nullValueForJoinColumn; }

private:
	typedef std::tr1::unordered_multimap<int64_t, uint8_t *, hasher, std::equal_to<int64_t>,
	  utils::STLPoolAllocator<std::pair<const int64_t, uint8_t *> > > hash_t;
	typedef std::tr1::unordered_multimap<int64_t, rowgroup::Row::Pointer, hasher, std::equal_to<int64_t>,
	  utils::STLPoolAllocator<std::pair<const int64_t, rowgroup::Row::Pointer> > > sthash_t;
	typedef std::tr1::unordered_multimap<TypelessData, rowgroup::Row::Pointer, hasher, std::equal_to<TypelessData>,
	  utils::STLPoolAllocator<std::pair<const TypelessData, rowgroup::Row::Pointer> > > typelesshash_t;

	typedef hash_t::iterator iterator;
	typedef typelesshash_t::iterator thIterator;

	TupleJoiner();
	TupleJoiner(const TupleJoiner &);
	TupleJoiner & operator=(const TupleJoiner &);

	iterator begin() { return h->begin(); }
	iterator end() { return h->end(); }
	
	
	rowgroup::RGData smallNullMemory;

	
	boost::scoped_ptr<hash_t> h;  // used for UM joins on ints
	boost::scoped_ptr<sthash_t> sth;  // used for UM join on ints where the backing table uses a string table
	std::vector<rowgroup::Row::Pointer> rows;   // used for PM join

	/* This struct is rough.  The BPP-JL stores the parsed results for
	the logical block being processed.  There are X threads at once, so
	up to X logical blocks being processed.  For each of those there's a vector
	of matches.  Each match is an index into 'rows'. */
	boost::shared_array<boost::shared_array<std::vector<uint32_t> > > pmJoinResults;
	rowgroup::RowGroup smallRG, largeRG;
	boost::scoped_array<rowgroup::Row> smallRow;
	//boost::shared_array<uint8_t> smallNullMemory;
	rowgroup::Row smallNullRow;

	enum JoinAlg {
		INSERTING,
		PM,
		UM,
		LARGE
	};
	JoinAlg joinAlg;
	joblist::JoinType joinType;
	boost::shared_ptr<utils::PoolAllocator> _pool;	// pool for the table and nodes
	uint threadCount;
	std::string tableName;

	/* vars, & fcns for typeless join */
	bool typelessJoin;
	std::vector<uint> smallKeyColumns, largeKeyColumns;
	boost::scoped_ptr<typelesshash_t> ht;  // used for UM join on strings
	uint keyLength;
	utils::FixedAllocator storedKeyAlloc;
	boost::scoped_array<utils::FixedAllocator> tmpKeyAlloc;
	
	/* semi-join vars & fcns */
	boost::shared_ptr<funcexp::FuncExpWrapper> fe;
	boost::scoped_array<funcexp::FuncExpWrapper> fes;  // holds X copies of fe, one per thread
	// this var is only used to normalize the NULL values for single-column joins,
	// will have to change when/if we need to support that for compound or string joins
	int64_t nullValueForJoinColumn;
	
	/* Runtime casual partitioning support */
	void updateCPData(const rowgroup::Row &r);
	boost::scoped_array<bool> discreteValues;
	boost::scoped_array<std::vector<int64_t> > cpValues;    // if !discreteValues, [0] has min, [1] has max
	uint uniqueLimit;
};

}

#endif

