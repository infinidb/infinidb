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

//
// C++ Implementation: joiner
//
// Description: 
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include "tuplejoiner.h"
#include <algorithm>
#include <vector>
#include <limits>
#ifdef _MSC_VER
#include <unordered_set>
#else
#include <tr1/unordered_set>
#endif
#include "hasher.h"
#include "lbidlist.h"

using namespace std;
using namespace rowgroup;
using namespace utils;
using namespace execplan;
using namespace joblist;

namespace joiner {

TupleJoiner::TupleJoiner(
	const rowgroup::RowGroup &smallInput,
	const rowgroup::RowGroup &largeInput,
	uint smallJoinColumn,
	uint largeJoinColumn,
	JoinType jt) :
	smallRG(smallInput), largeRG(largeInput), joinAlg(INSERTING), joinType(jt),
	threadCount(1), typelessJoin(false), uniqueLimit(100)
{
	Row smallR;

	STLPoolAllocator<pair<const int64_t, uint8_t *> > alloc(64*1024*1024 + 1);
	_pool = alloc.getPoolAllocator();
	
	h.reset(new hash_t(10, hasher(), hash_t::key_equal(), alloc));
	smallRG.initRow(&smallR);
	if (smallOuterJoin() || largeOuterJoin() || semiJoin() || antiJoin()) {
		smallNullMemory.reset(new uint8_t[smallR.getSize()]);
		smallR.setData(smallNullMemory.get());
		smallR.initToNull();
	}
	smallKeyColumns.push_back(smallJoinColumn);
	largeKeyColumns.push_back(largeJoinColumn);
	discreteValues.reset(new bool[1]);
	cpValues.reset(new vector<int64_t>[1]);
	discreteValues[0] = false;
	cpValues[0].push_back(numeric_limits<int64_t>::max());
	cpValues[0].push_back(numeric_limits<int64_t>::min());
	nullValueForJoinColumn = smallR.getSignedNullValue(smallJoinColumn);
}

TupleJoiner::TupleJoiner(
	const rowgroup::RowGroup &smallInput,
	const rowgroup::RowGroup &largeInput,
	const vector<uint> &smallJoinColumns,
	const vector<uint> &largeJoinColumns,
	JoinType jt) :
	smallRG(smallInput), largeRG(largeInput), joinAlg(INSERTING),
	joinType(jt), threadCount(1), typelessJoin(true),
	smallKeyColumns(smallJoinColumns), largeKeyColumns(largeJoinColumns),
	uniqueLimit(100)
{
	Row smallR;

	STLPoolAllocator<pair<const TypelessData, uint8_t *> > alloc(64*1024*1024 + 1);
	_pool = alloc.getPoolAllocator();
	
	ht.reset(new typelesshash_t(10, hasher(), typelesshash_t::key_equal(), alloc));
	smallRG.initRow(&smallR);
	if (smallOuterJoin() || largeOuterJoin() || semiJoin() || antiJoin()) {
		smallNullMemory.reset(new uint8_t[smallR.getSize()]);
		smallR.setData(smallNullMemory.get());
		smallR.initToNull();
	}

	for (uint i = keyLength = 0; i < smallKeyColumns.size(); i++)
		if (smallRG.getColTypes()[smallKeyColumns[i]] == CalpontSystemCatalog::CHAR ||
		  smallRG.getColTypes()[smallKeyColumns[i]] == CalpontSystemCatalog::VARCHAR)
			keyLength += smallRG.getColumnWidth(smallKeyColumns[i]) + 1;  // +1 null char
		else
			keyLength += 8;
	storedKeyAlloc = FixedAllocator(keyLength);
	
	discreteValues.reset(new bool[smallKeyColumns.size()]);
	cpValues.reset(new vector<int64_t>[smallKeyColumns.size()]);
	for (uint i = 0; i < smallKeyColumns.size(); i++) {
		discreteValues[i] = false;
		cpValues[i].push_back(numeric_limits<int64_t>::max());
		cpValues[i].push_back(numeric_limits<int64_t>::min());
	}
}

TupleJoiner::TupleJoiner() { throw runtime_error("TupleJoiner() shouldn't be called."); }

TupleJoiner::TupleJoiner(const TupleJoiner &j) 
{ 
	throw runtime_error("TupleJoiner(TupleJoiner) shouldn't be called.");
}

TupleJoiner & TupleJoiner::operator=(const TupleJoiner &j) 
{
	throw runtime_error("TupleJoiner::operator=() shouldn't be called.");
	return *this;
}

TupleJoiner::~TupleJoiner()
{
}

bool TupleJoiner::operator<(const TupleJoiner &tj) const
{
	return size() < tj.size();
}

void TupleJoiner::insert(Row &r) {
	r.zeroRid();
	updateCPData(r);
	if (joinAlg == UM)
		if (typelessJoin)
			ht->insert(pair<TypelessData, uint8_t *>
			  (makeTypelessKey(r, smallKeyColumns, keyLength, &storedKeyAlloc), 
			  r.getData()));
		else {
			int64_t smallKey = r.getIntField(smallKeyColumns[0]);
			if (UNLIKELY(smallKey == nullValueForJoinColumn))
				h->insert(pair<int64_t, uint8_t *>(getJoinNullValue(), r.getData()));
			else
				h->insert(pair<int64_t, uint8_t *>(smallKey, r.getData()));
		}
	else
		rows.push_back(r.getData());
}

void TupleJoiner::match(rowgroup::Row &largeSideRow, uint largeRowIndex, uint threadID,
	vector<uint8_t *> *matches)
{
	uint i;
	bool isNull = hasNullJoinColumn(largeSideRow);

	matches->clear();
	if (inPM()) { 
		vector<uint32_t> &v = pmJoinResults[threadID][largeRowIndex];
		uint size = v.size();
		for (i = 0; i < size; i++)
			if (v[i] < rows.size())
					matches->push_back(rows[v[i]]);

		if (UNLIKELY((semiJoin() || antiJoin()) && matches->size() == 0))
			matches->push_back(smallNullMemory.get());
	}
	else if (LIKELY(!isNull)) {
		if (UNLIKELY(typelessJoin)) {
			TypelessData largeKey;
			thIterator it;
			pair<thIterator, thIterator> range;

			largeKey = makeTypelessKey(largeSideRow, largeKeyColumns, keyLength, &tmpKeyAlloc[threadID]);
			it = ht->find(largeKey);
			if (it == ht->end() && !(joinType & (LARGEOUTER | MATCHNULLS)))
				return;
			range = ht->equal_range(largeKey);
			for (; range.first != range.second; ++range.first)
				matches->push_back(range.first->second);
		}
		else {
			int64_t largeKey;
			iterator it;
			pair<iterator, iterator> range;

			largeKey = largeSideRow.getIntField(largeKeyColumns[0]);
			it = h->find(largeKey);
			if (it == end() && !(joinType & (LARGEOUTER | MATCHNULLS)))
				return;
			range = h->equal_range(largeKey);
			for (; range.first != range.second; ++range.first)
				matches->push_back(range.first->second);
		}
	}
	if (UNLIKELY(largeOuterJoin() && matches->size() == 0))
		matches->push_back(smallNullMemory.get());

	if (UNLIKELY(inUM() && (joinType & MATCHNULLS) && !isNull && !typelessJoin)) {
		pair<iterator, iterator> range = h->equal_range(getJoinNullValue());
		for (; range.first != range.second; ++range.first)
			matches->push_back(range.first->second);
	}
	/* Bug 3524.  For 'not in' queries this matches everything.
	 */
	if (UNLIKELY(inUM() && isNull && antiJoin() && (joinType & MATCHNULLS))) {
		if (!typelessJoin) {
			iterator it;
			for (it = h->begin(); it != h->end(); ++it)
				matches->push_back(it->second);
		}
		else {
			thIterator it;
			for (it = ht->begin(); it != ht->end(); ++it)
				matches->push_back(it->second);
		}
	}
}

void TupleJoiner::doneInserting()
{

	// a minor textual cleanup
#ifdef TJ_DEBUG
	#define CHECKSIZE \
		if (uniquer.size() > uniqueLimit) { \
			cout << "too many discrete values\n"; \
			return; \
		}
#else
	#define CHECKSIZE \
		if (uniquer.size() > uniqueLimit) \
			return;
#endif
	
	uint col;

	/* Put together the discrete values for the runtime casual partitioning restriction */

	for (col = 0; col < smallKeyColumns.size(); col++) {
		tr1::unordered_set<int64_t> uniquer;
		tr1::unordered_set<int64_t>::iterator uit;
		hash_t::iterator hit;
		typelesshash_t::iterator thit;
		uint i, pmpos = 0, rowCount;
		Row smallRow;
	
		smallRG.initRow(&smallRow);
		if (!smallRow.isNumeric(smallKeyColumns[col]))
			continue;
		
		rowCount = size();
		if (joinAlg == PM)
			pmpos = 0;
		else if (typelessJoin)
			thit = ht->begin();
		else
			hit = h->begin();
			
		for (i = 0; i < rowCount; i++) {
			if (joinAlg == PM)
				smallRow.setData(rows[pmpos++]);
			else if (typelessJoin) {
				smallRow.setData(thit->second);
				++thit;
			}
			else {
				smallRow.setData(hit->second);
				++hit;
			}
			
			uniquer.insert(smallRow.getIntField(smallKeyColumns[col]));
			CHECKSIZE;
		}

		discreteValues[col] = true;
		cpValues[col].clear();
#ifdef TJ_DEBUG
		cout << "inserting " << uniquer.size() << " discrete values\n";
#endif
		for (uit = uniquer.begin(); uit != uniquer.end(); ++uit)
			cpValues[col].push_back(*uit);
	}
}

void TupleJoiner::setInPM()
{
	joinAlg = PM;
}

void TupleJoiner::setInUM()
{
	vector<uint8_t *> empty;
	Row smallRow;
	uint i, size;

	joinAlg = UM;
	size = rows.size();
	smallRG.initRow(&smallRow);
#ifdef TJ_DEBUG
	cout << "converting array to hash, size = " << size << "\n";
#endif
	for (i = 0; i < size; i++) {
		smallRow.setData(rows[i]);
		insert(smallRow);
	}
#ifdef TJ_DEBUG
	cout << "done\n";
#endif
	rows.swap(empty);
	if (typelessJoin) {
		tmpKeyAlloc.reset(new FixedAllocator[threadCount]);
		for (i = 0; i < threadCount; i++)
			tmpKeyAlloc[i] = FixedAllocator(keyLength, true);
	}
}

void TupleJoiner::setPMJoinResults(boost::shared_array<vector<uint32_t> > jr,
	uint threadID)
{
	pmJoinResults[threadID] = jr;
}

void TupleJoiner::markMatches(uint threadID, uint rowCount)
{
	boost::shared_array<vector<uint32_t> > matches = pmJoinResults[threadID];
	uint i, j;

	for (i = 0; i < rowCount; i++)
		for (j = 0; j < matches[i].size(); j++) {
			if (matches[i][j] < rows.size()) {
				smallRow[threadID].setData(rows[matches[i][j]]);
				smallRow[threadID].markRow();
			}
		}
}

void TupleJoiner::markMatches(uint threadID, const vector<uint8_t *> &matches)
{
	uint rowCount = matches.size();
	uint i;

	for (i = 0; i < rowCount; i++) {
			smallRow[threadID].setData(matches[i]);
			smallRow[threadID].markRow();
	}
}

boost::shared_array<std::vector<uint32_t> > TupleJoiner::getPMJoinArrays(uint threadID)
{
	return pmJoinResults[threadID];
}

void TupleJoiner::setThreadCount(uint cnt)
{
	threadCount = cnt;
	pmJoinResults.reset(new boost::shared_array<vector<uint32_t> >[cnt]);
	smallRow.reset(new Row[cnt]);
	for (uint i = 0; i < cnt; i++)
		smallRG.initRow(&smallRow[i]);
	if (typelessJoin) {
		tmpKeyAlloc.reset(new FixedAllocator[threadCount]);
		for (uint i = 0; i < threadCount; i++)
			tmpKeyAlloc[i] = FixedAllocator(keyLength, true);
	}
	if (fe) {
		fes.reset(new funcexp::FuncExpWrapper[cnt]);
		for (uint i = 0; i < cnt; i++)
			fes[i] = *fe;
	}
}

void TupleJoiner::getUnmarkedRows(vector<uint8_t *> *out)
{
	Row smallR;

	smallRG.initRow(&smallR);
	out->clear();
	if (inPM()) {
		uint i, size;

		size = rows.size();
		for (i = 0; i < size; i++) {
			smallR.setData(rows[i]);
			if (!smallR.isMarked())
				out->push_back(rows[i]);
		}
	}
	else {
		if (typelessJoin) {
			typelesshash_t::iterator it;

			for (it = ht->begin(); it != ht->end(); ++it) {
				smallR.setData(it->second);
				if (!smallR.isMarked())
					out->push_back(it->second);
			}
		}
		else {
			iterator it;

			for (it = begin(); it != end(); ++it) {
				smallR.setData(it->second);
				if (!smallR.isMarked())
					out->push_back(it->second);
			}
		}
	}
}

uint64_t TupleJoiner::getMemUsage() const
{
	if (inUM() && typelessJoin)
		return (_pool->getMemUsage() + (smallRG.getRowSize() * ht->size())
			+ storedKeyAlloc.getMemUsage());
	else if (inUM())
		return _pool->getMemUsage() + (smallRG.getRowSize() * h->size());
	else
		return (rows.size() * (smallRG.getRowSize() + sizeof(uint8_t *)));
}

void TupleJoiner::setFcnExpFilter(boost::shared_ptr<funcexp::FuncExpWrapper> pt)
{
	fe = pt;
	if (fe)
		joinType |= WITHFCNEXP;
	else
		joinType &= ~WITHFCNEXP;
}

void TupleJoiner::updateCPData(const Row &r)
{
	int64_t val;
	uint col;

	if (antiJoin() || largeOuterJoin())
		return;

	for (col = 0; col < smallKeyColumns.size(); col++) {
		if (r.isLongString(smallKeyColumns[col]))
			continue;
			
		int64_t &min = cpValues[col][0], &max = cpValues[col][1];
		val = r.getIntField(smallKeyColumns[col]);
		if (r.isNumeric(smallKeyColumns[col])) {
			if (val > max)
				max = val;
			if (val < min)
				min = val;
		}
		else {
			if (order_swap(val) < order_swap(min) ||
			  min == numeric_limits<int64_t>::max())
				min = val;
			if (order_swap(val) > order_swap(max) ||
			  max == numeric_limits<int64_t>::min())
				max = val;
		}
	}
}

TypelessData makeTypelessKey(const Row &r, const vector<uint> &keyCols,
	uint keylen, FixedAllocator *fa)
{
	TypelessData ret;
	uint off = 0, i, j;
	execplan::CalpontSystemCatalog::ColDataType type;

	ret.data = (uint8_t *) fa->allocate();
	for (i = 0; i < keyCols.size(); i++) {
		type = r.getColTypes()[keyCols[i]];
		if (type == CalpontSystemCatalog::VARCHAR || type == CalpontSystemCatalog::CHAR) {
			// this is a string, copy a normalized version
			uint8_t *str = &(r.getData()[r.getOffset(keyCols[i])]);
			uint width = r.getColumnWidth(keyCols[i]);
			for (j = 0; j < width && str[j] != 0; j++) {
				ret.data[off++] = str[j];
				if (off > keylen)
					goto toolong;
			}
			ret.data[off++] = 0;
			if (off > keylen)
				goto toolong;
		}
		else {
			if (off + 8 > keylen)
				goto toolong;
			*((int64_t *) &ret.data[off]) = r.getIntField(keyCols[i]);
			off += 8;
		}
	}
	ret.len = off;
	return ret;
toolong:
	ret.len = 0;
	return ret;
}

TypelessData::TypelessData() : data(NULL), len(0)
{ }

bool TypelessData::operator==(const TypelessData &t) const
{
	if (len != t.len)
		return false;
	if (len == 0)  // special value to force mismatches
		return false;
	return (memcmp(data, t.data, len) == 0);
}

string TypelessData::toString() const
{
	uint i;
	ostringstream os;
	
	os << hex;
	for (i = 0; i < len; i++) {
		os << (uint) data[i] << " ";
	}
	os << dec;
	return os.str();
}

void TypelessData::serialize(messageqcpp::ByteStream &b) const
{
	b << (messageqcpp::ByteStream::quadbyte)len;
	b.append(data, len);
}

void TypelessData::deserialize(messageqcpp::ByteStream &b, utils::FixedAllocator &fa)
{
	messageqcpp::ByteStream::quadbyte qb;
	b >> qb;
	len = qb;
	data = (uint8_t *) fa.allocate();
	memcpy(data, b.buf(), len);
	b.advance(len);
}

size_t TupleJoiner::hasher::operator()(const TypelessData &e) const
{
	char *data = (char *) e.data;
	uint len = e.len;
	return utils::Hasher()(data, len);
}

bool TupleJoiner::hasNullJoinColumn(const Row &r) const
{
	for (uint i = 0; i < largeKeyColumns.size(); i++)
		if (r.isNullValue(largeKeyColumns[i]))
			return true;
	return false;
}

string TupleJoiner::getTableName() const
{
	return tableName;
}

void TupleJoiner::setTableName(const string &tname)
{
	tableName = tname;
}

};
