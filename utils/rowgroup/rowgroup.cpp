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

//  $Id: rowgroup.cpp 4021 2013-07-26 22:08:16Z xlou $

//
// C++ Implementation: rowgroup
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <vector>
//#define NDEBUG
#include <cassert>
#include <string>
#include <sstream>
#include <iterator>
using namespace std;

#include <boost/shared_array.hpp>
using namespace boost;

#include "bytestream.h"
using namespace messageqcpp;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "joblisttypes.h"
#include "nullvaluemanip.h"
#include "rowgroup.h"

namespace rowgroup
{

StringStore::StringStore() : empty(true), fUseStoreStringMutex(false) { }

StringStore::StringStore(const StringStore &)
{
	throw logic_error("Don't call StringStore copy ctor");
}

StringStore & StringStore::operator=(const StringStore &)
{
	throw logic_error("Don't call StringStore operator=");
}

StringStore::~StringStore()
{
#if 0
	// for mem usage debugging
	uint32_t i;
	uint64_t inUse = 0, allocated = 0;

	for (i = 0; i < mem.size(); i++) {
		MemChunk *tmp = (MemChunk *) mem.back().get();
		inUse += tmp->currentSize;
		allocated += tmp->capacity;
	}
	if (allocated > 0)
		cout << "~SS: " << inUse << "/" << allocated << " = " << (float) inUse/(float) allocated << endl;
#endif
}

uint32_t StringStore::storeString(const uint8_t *data, uint32_t len)
{
	MemChunk *lastMC = NULL;
	uint32_t ret = 0;

	empty = false;   // At least a NULL is being stored.

	// Sometimes the caller actually wants "" to be returned.......   argggghhhh......
	//if (len == 0)
	//	return numeric_limits<uint32_t>::max();

	if ((len == 8 || len == 9) &&
	  *((uint64_t *) data) == *((uint64_t *) joblist::CPNULLSTRMARK.c_str()))
		return numeric_limits<uint32_t>::max();

	//@bug6065, make StringStore::storeString() thread safe
	boost::mutex::scoped_lock lk(fMutex, defer_lock);
	if (fUseStoreStringMutex)
		lk.lock();

	if (mem.size() > 0)
		lastMC = (MemChunk *) mem.back().get();

	if ((lastMC == NULL) || (lastMC->capacity - lastMC->currentSize < len)) {
		// mem usage debugging
		//if (lastMC)
		//cout << "Memchunk efficiency = " << lastMC->currentSize << "/" << lastMC->capacity << endl;
		shared_array<uint8_t> newOne(new uint8_t[CHUNK_SIZE + sizeof(MemChunk)]);
		mem.push_back(newOne);
		lastMC = (MemChunk *) mem.back().get();
		lastMC->currentSize = 0;
		lastMC->capacity = CHUNK_SIZE;
		memset(lastMC->data, 0, CHUNK_SIZE);
	}

	ret = ((mem.size()-1) * CHUNK_SIZE) + lastMC->currentSize;
	memcpy(&(lastMC->data[lastMC->currentSize]), data, len);
	/*
	cout << "stored: '" << hex;
	for (uint32_t i = 0; i < len ; i++) {
		cout << (char) lastMC->data[lastMC->currentSize + i];
	}
	cout << "' at position " << lastMC->currentSize << " len " << len << dec << endl;
	*/
	lastMC->currentSize += len;

	return ret;
}

void StringStore::serialize(ByteStream &bs) const
{
	uint32_t i;
	MemChunk *mc;

	bs << (uint32_t) mem.size();
	bs << (uint8_t) empty;
	for (i = 0; i < mem.size(); i++) {
		mc = (MemChunk *) mem[i].get();
		bs << (uint32_t) mc->currentSize;
		//cout << "serialized " << mc->currentSize << " bytes\n";
		bs.append(mc->data, mc->currentSize);
	}
}

uint32_t StringStore::deserialize(ByteStream &bs)
{
	uint32_t i;
	uint32_t count;
	uint32_t size;
	uint8_t *buf;
	MemChunk *mc;
	uint8_t tmp8;
	uint32_t ret = 0;

	//mem.clear();
	bs >> count;
	mem.resize(count);
	bs >> tmp8;
	empty = (bool) tmp8;
	ret += 5;
	for (i = 0; i < count; i++) {
		bs >> size;
		//cout << "deserializing " << size << " bytes\n";
		buf = bs.buf();
		mem[i].reset(new uint8_t[size + sizeof(MemChunk)]);
		mc = (MemChunk *) mem[i].get();
		mc->currentSize = size;
		mc->capacity = size;
		memcpy(mc->data, buf, size);
		bs.advance(size);
		ret += (size + 4);
	}
	return ret;
}

void StringStore::clear()
{
	vector<shared_array<uint8_t> > emptyv;
	mem.swap(emptyv);
	empty = true;
}

//uint32_t rgDataCount = 0;

RGData::RGData()
{
	//cout << "rgdata++ = " << __sync_add_and_fetch(&rgDataCount, 1) << endl;
}

RGData::RGData(const RowGroup &rg, uint32_t rowCount)
{
	//cout << "rgdata++ = " << __sync_add_and_fetch(&rgDataCount, 1) << endl;
	rowData.reset(new uint8_t[rg.getDataSize(rowCount)]);
	if (rg.usesStringTable() && rowCount > 0)
		strings.reset(new StringStore());

#ifdef VALGRIND
	/* In a PM-join, we can serialize entire tables; not every value has been
	 * filled in yet.  Need to look into that.  Valgrind complains that
	 * those bytes are uninitialized, this suppresses that error.
	 */
	memset(rowData.get(), 0, rg.getDataSize(rowCount));   // XXXPAT: make valgrind happy temporarily
#endif
}

RGData::RGData(const RowGroup &rg)
{
	//cout << "rgdata++ = " << __sync_add_and_fetch(&rgDataCount, 1) << endl;
	rowData.reset(new uint8_t[rg.getMaxDataSize()]);

	if (rg.usesStringTable())
		strings.reset(new StringStore());

#ifdef VALGRIND
	/* In a PM-join, we can serialize entire tables; not every value has been
	 * filled in yet.  Need to look into that.  Valgrind complains that
	 * those bytes are uninitialized, this suppresses that error.
	 */
	memset(rowData.get(), 0, rg.getMaxDataSize());
#endif
}

void RGData::reinit(const RowGroup &rg, uint32_t rowCount)
{
	rowData.reset(new uint8_t[rg.getDataSize(rowCount)]);

	if (rg.usesStringTable())
		strings.reset(new StringStore());
	else
		strings.reset();

#ifdef VALGRIND
	/* In a PM-join, we can serialize entire tables; not every value has been
	 * filled in yet.  Need to look into that.  Valgrind complains that
	 * those bytes are uninitialized, this suppresses that error.
	 */
	memset(rowData.get(), 0, rg.getDataSize(rowCount));
#endif
}

void RGData::reinit(const RowGroup &rg)
{
	reinit(rg, 8192);
}

RGData::RGData(const RGData &r) : rowData(r.rowData), strings(r.strings)
{
	//cout << "rgdata++ = " << __sync_add_and_fetch(&rgDataCount, 1) << endl;
}

RGData::~RGData()
{
	//cout << "rgdata-- = " << __sync_sub_and_fetch(&rgDataCount, 1) << endl;
}

void RGData::serialize(ByteStream &bs, uint32_t amount) const
{
	//cout << "serializing!\n";
	bs << (uint32_t) RGDATA_SIG;
	bs << (uint32_t) amount;
	bs.append(rowData.get(), amount);
	if (strings && !strings->isEmpty()) {
		bs << (uint8_t) 1;
		strings->serialize(bs);
	}
	else
		bs << (uint8_t) 0;
}

uint32_t RGData::deserialize(ByteStream &bs, bool hasLenField)
{
	uint32_t amount, sig;
	uint8_t *buf;
	uint8_t tmp8;
	uint32_t ret = 0;

	bs.peek(sig);
	if (sig == RGDATA_SIG) {
		bs >> sig;
		bs >> amount;
		ret += 8;
		rowData.reset(new uint8_t[amount]);
		buf = bs.buf();
		memcpy(rowData.get(), buf, amount);
		bs.advance(amount);
		bs >> tmp8;
		ret += amount + 1;
		if (tmp8) {
			strings.reset(new StringStore());
			ret += strings->deserialize(bs);
		}
		else
			strings.reset();
	}
	// crude backward compat.  Remove after conversions are finished.
	else {
		if (hasLenField) {
			bs >> amount;
			ret += 4;
		}
		else
			amount = bs.length();
		rowData.reset(new uint8_t[amount]);
		strings.reset();
		buf = bs.buf();
		memcpy(rowData.get(), buf, amount);
		bs.advance(amount);
		ret += amount;
	}
	return ret;
}

void RGData::clear()
{
	rowData.reset();
	strings.reset();
}

Row::Row() : data(NULL), strings(NULL) { }

Row::Row(const Row &r) : columnCount(r.columnCount), baseRid(r.baseRid),
	oldOffsets(r.oldOffsets), stOffsets(r.stOffsets),
	offsets(r.offsets), colWidths(r.colWidths), types(r.types), data(r.data),
	scale(r.scale), precision(r.precision), strings(r.strings),
	useStringTable(r.useStringTable), hasLongStringField(r.hasLongStringField),
	sTableThreshold(r.sTableThreshold), forceInline(r.forceInline)
{ }

Row::~Row() { }

Row & Row::operator=(const Row &r)
{
	columnCount = r.columnCount;
	baseRid = r.baseRid;
	oldOffsets = r.oldOffsets;
	stOffsets = r.stOffsets;
	offsets = r.offsets;
	colWidths = r.colWidths;
	types = r.types;
	data = r.data;
	scale = r.scale;
	precision = r.precision;
	strings = r.strings;
	useStringTable = r.useStringTable;
	hasLongStringField = r.hasLongStringField;
	sTableThreshold = r.sTableThreshold;
	forceInline = r.forceInline;
	return *this;
}

string Row::toString() const
{
	ostringstream os;
	uint32_t i;

	//os << getRid() << ": ";
	os << (int) useStringTable << ": ";
	for (i = 0; i < columnCount; i++) {
		if (isNullValue(i))
			os << "NULL ";
		else
			switch (types[i]) {
				case CalpontSystemCatalog::CHAR:
				case CalpontSystemCatalog::VARCHAR: {
					const string &tmp = getStringField(i);
					os << "(" << getStringLength(i) << ") '" << tmp << "' ";
					break;
				}
				case CalpontSystemCatalog::FLOAT:
				case CalpontSystemCatalog::UFLOAT:
					os << getFloatField(i) << " ";
					break;
				case CalpontSystemCatalog::DOUBLE:
				case CalpontSystemCatalog::UDOUBLE:
					os << getDoubleField(i) << " ";
					break;
				case CalpontSystemCatalog::LONGDOUBLE:
					os << getLongDoubleField(i) << " ";
					break;
				case CalpontSystemCatalog::VARBINARY: {
					uint32_t len = getVarBinaryLength(i);
					const uint8_t* val = getVarBinaryField(i);
					os << "0x" << hex;
					while (len-- > 0) {
						os << (uint32_t)(*val >> 4);
						os << (uint32_t)(*val++ & 0x0F);
					}
					os << " " << dec;
					break;
				}
				default:
					os << getIntField(i) << " ";
					break;
			}
	}
	return os.str();
}

string Row::toCSV() const
{
	ostringstream os;

	for (uint32_t i = 0; i < columnCount; i++) {
		if (i > 0)
		{
			os << ",";
		}

		if (isNullValue(i))
			os << "NULL";
		else
			switch (types[i]) {
				case CalpontSystemCatalog::CHAR:
				case CalpontSystemCatalog::VARCHAR:
					os << getStringField(i).c_str();
					break;
				case CalpontSystemCatalog::FLOAT:
				case CalpontSystemCatalog::UFLOAT:
					os << getFloatField(i);
					break;
				case CalpontSystemCatalog::DOUBLE:
				case CalpontSystemCatalog::UDOUBLE:
					os << getDoubleField(i);
					break;
				case CalpontSystemCatalog::LONGDOUBLE:
					os << getLongDoubleField(i);
					break;
				case CalpontSystemCatalog::VARBINARY: {
					uint32_t len = getVarBinaryLength(i);
					const uint8_t* val = getVarBinaryField(i);
					os << "0x" << hex;
					while (len-- > 0) {
						os << (uint32_t)(*val >> 4);
						os << (uint32_t)(*val++ & 0x0F);
					}
					os << dec;
					break;
				}
				default:
					os << getIntField(i);
					break;
			}
	}
	return os.str();
}

void Row::initToNull()
{
	uint32_t i;

	for (i = 0; i < columnCount; i++) {
		switch (types[i]) {
			case CalpontSystemCatalog::TINYINT:
				data[offsets[i]] = joblist::TINYINTNULL; break;
			case CalpontSystemCatalog::SMALLINT:
				*((int16_t *) &data[offsets[i]]) = static_cast<int16_t>(joblist::SMALLINTNULL); break;
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
				*((int32_t *) &data[offsets[i]]) = static_cast<int32_t>(joblist::INTNULL); break;
			case CalpontSystemCatalog::FLOAT:
			case CalpontSystemCatalog::UFLOAT:
				*((int32_t *) &data[offsets[i]]) = static_cast<int32_t>(joblist::FLOATNULL); break;
			case CalpontSystemCatalog::DATE:
				*((int32_t *) &data[offsets[i]]) = static_cast<int32_t>(joblist::DATENULL); break;
			case CalpontSystemCatalog::BIGINT:
				if (precision[i] != 9999)
					*((uint64_t *) &data[offsets[i]]) = joblist::BIGINTNULL;
				else  // work around for count() in outer join result.
					*((uint64_t *) &data[offsets[i]]) = 0;
				break;
			case CalpontSystemCatalog::DOUBLE:
			case CalpontSystemCatalog::UDOUBLE:
				*((uint64_t *) &data[offsets[i]]) = joblist::DOUBLENULL; break;
			case CalpontSystemCatalog::DATETIME:
				*((uint64_t *) &data[offsets[i]]) = joblist::DATETIMENULL; break;
			case CalpontSystemCatalog::CHAR:
			case CalpontSystemCatalog::VARCHAR:
			case CalpontSystemCatalog::STRINT: {
				if (inStringTable(i)) {
					setStringField(joblist::CPNULLSTRMARK, i);
					break;
				}
				uint32_t len = getColumnWidth(i);

				switch (len) {
					case 1:	data[offsets[i]] = joblist::CHAR1NULL; break;
					case 2: *((uint16_t *) &data[offsets[i]]) = joblist::CHAR2NULL; break;
					case 3:
					case 4: *((uint32_t *) &data[offsets[i]]) = joblist::CHAR4NULL; break;
					case 5:
					case 6:
					case 7:
					case 8: *((uint64_t *) &data[offsets[i]]) = joblist::CHAR8NULL;
							break;
					default:
						*((uint64_t *) &data[offsets[i]]) = *((uint64_t *) joblist::CPNULLSTRMARK.c_str());
						memset(&data[offsets[i] + 8], 0, len - 8);
						//strcpy((char *) &data[offsets[i]], joblist::CPNULLSTRMARK.c_str());
						break;
				}
				break;
			}
			case CalpontSystemCatalog::VARBINARY:
				*((uint16_t *) &data[offsets[i]]) = 0; break;
			case CalpontSystemCatalog::DECIMAL:
			case CalpontSystemCatalog::UDECIMAL:
			{
				uint32_t len = getColumnWidth(i);
				switch (len) {
					case 1 : data[offsets[i]] = joblist::TINYINTNULL; break;
					case 2 : *((int16_t *) &data[offsets[i]]) = static_cast<int16_t>(joblist::SMALLINTNULL); break;
					case 4 : *((int32_t *) &data[offsets[i]]) = static_cast<int32_t>(joblist::INTNULL); break;
					default: *((int64_t *) &data[offsets[i]]) = static_cast<int64_t>(joblist::BIGINTNULL); break;
				}
				break;
			}
			case CalpontSystemCatalog::UTINYINT:
				data[offsets[i]] = joblist::UTINYINTNULL; break;
			case CalpontSystemCatalog::USMALLINT:
				*((uint16_t *) &data[offsets[i]]) = joblist::USMALLINTNULL; break;
			case CalpontSystemCatalog::UMEDINT:
			case CalpontSystemCatalog::UINT:
				*((uint32_t *) &data[offsets[i]]) = joblist::UINTNULL; break;
			case CalpontSystemCatalog::UBIGINT:
				*((uint64_t *) &data[offsets[i]]) = joblist::UBIGINTNULL; break;
			case CalpontSystemCatalog::LONGDOUBLE: {
				// no NULL value for long double yet, this is a nan.
				memset(&data[offsets[i]], 0xFF, getColumnWidth(i));
				break;
			}
			default:
				ostringstream os;
				os << "Row::initToNull(): got bad column type (" << types[i] <<
					").  Width=" << getColumnWidth(i) << endl;
				os << toString();
				throw logic_error(os.str());
		}
	}
}

bool Row::isNullValue(uint32_t colIndex) const
{
	switch (types[colIndex]) {
		case CalpontSystemCatalog::TINYINT:
			return (data[offsets[colIndex]] == joblist::TINYINTNULL);
		case CalpontSystemCatalog::SMALLINT:
			return (*((int16_t *) &data[offsets[colIndex]]) == static_cast<int16_t>(joblist::SMALLINTNULL));
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			return (*((int32_t *) &data[offsets[colIndex]]) == static_cast<int32_t>(joblist::INTNULL));
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
			return (*((int32_t *) &data[offsets[colIndex]]) == static_cast<int32_t>(joblist::FLOATNULL));
		case CalpontSystemCatalog::DATE:
			return (*((int32_t *) &data[offsets[colIndex]]) == static_cast<int32_t>(joblist::DATENULL));
		case CalpontSystemCatalog::BIGINT:
			return (*((int64_t *) &data[offsets[colIndex]]) == static_cast<int64_t>(joblist::BIGINTNULL));
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
			return (*((uint64_t *) &data[offsets[colIndex]]) == joblist::DOUBLENULL);
		case CalpontSystemCatalog::DATETIME:
			return (*((uint64_t *) &data[offsets[colIndex]]) == joblist::DATETIMENULL);
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		case CalpontSystemCatalog::STRINT: {
			uint32_t len = getColumnWidth(colIndex);
			if (inStringTable(colIndex)) {
				uint32_t offset, length;
				offset = *((uint32_t *) &data[offsets[colIndex]]);
				length = *((uint32_t *) &data[offsets[colIndex] + 4]);
				return strings->isNullValue(offset, length);
			}
			if (data[offsets[colIndex]] == 0)   // empty string
				return true;
			switch (len) {
				case 1: return (data[offsets[colIndex]] == joblist::CHAR1NULL);
				case 2: return (*((uint16_t *) &data[offsets[colIndex]]) == joblist::CHAR2NULL);
				case 3:
				case 4: return (*((uint32_t *) &data[offsets[colIndex]]) == joblist::CHAR4NULL);
				case 5:
				case 6:
				case 7:
				case 8: return
					(*((uint64_t *) &data[offsets[colIndex]]) == joblist::CHAR8NULL);
				default:
					return (*((uint64_t *) &data[offsets[colIndex]]) == *((uint64_t *) joblist::CPNULLSTRMARK.c_str()));
			}
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
		case CalpontSystemCatalog::UDECIMAL:
		{
			uint32_t len = getColumnWidth(colIndex);
			switch (len) {
				case 1 : return (data[offsets[colIndex]] == joblist::TINYINTNULL);
				case 2 : return (*((int16_t *) &data[offsets[colIndex]]) == static_cast<int16_t>(joblist::SMALLINTNULL));
				case 4 : return (*((int32_t *) &data[offsets[colIndex]]) == static_cast<int32_t>(joblist::INTNULL));
				default: return (*((int64_t *) &data[offsets[colIndex]]) == static_cast<int64_t>(joblist::BIGINTNULL));
			}
			break;
		}
		case CalpontSystemCatalog::VARBINARY: {
			uint32_t pos = offsets[colIndex];
			if (inStringTable(colIndex)) {
				uint32_t offset, length;
				offset = *((uint32_t *) &data[pos]);
				length = *((uint32_t *) &data[pos+4]);
				return strings->isNullValue(offset, length);
			}
			if (*((uint16_t*) &data[pos]) == 0)
				return true;
			else
				if ((strncmp((char *) &data[pos+2], joblist::CPNULLSTRMARK.c_str(), 8) == 0) &&
				  *((uint16_t*) &data[pos]) == joblist::CPNULLSTRMARK.length())
					return true;

			break;
		}
		case CalpontSystemCatalog::UTINYINT:
			return (data[offsets[colIndex]] == joblist::UTINYINTNULL);
		case CalpontSystemCatalog::USMALLINT:
			return (*((uint16_t *) &data[offsets[colIndex]]) == joblist::USMALLINTNULL);
		case CalpontSystemCatalog::UMEDINT:
		case CalpontSystemCatalog::UINT:
			return (*((uint32_t *) &data[offsets[colIndex]]) == joblist::UINTNULL);
		case CalpontSystemCatalog::UBIGINT:
			return (*((uint64_t *) &data[offsets[colIndex]]) == joblist::UBIGINTNULL);
		case CalpontSystemCatalog::LONGDOUBLE:
			// return false;  // no NULL value for long double yet
			break;
		default: {
			ostringstream os;
			os << "Row::isNullValue(): got bad column type (" << types[colIndex] <<
				").  Width=" << getColumnWidth(colIndex) << endl;
			os << toString() << endl;
			throw logic_error(os.str());
		}
	}

	return false;
}

uint64_t Row::getNullValue(uint32_t colIndex) const
{
	return utils::getNullValue(types[colIndex], getColumnWidth(colIndex));
#if 0
	switch (types[colIndex]) {
		case CalpontSystemCatalog::TINYINT:
			return joblist::TINYINTNULL;
		case CalpontSystemCatalog::SMALLINT:
			return joblist::SMALLINTNULL;
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			return joblist::INTNULL;
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
			return joblist::FLOATNULL;
		case CalpontSystemCatalog::DATE:
			return joblist::DATENULL;
		case CalpontSystemCatalog::BIGINT:
			return joblist::BIGINTNULL;
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
			return joblist::DOUBLENULL;
		case CalpontSystemCatalog::DATETIME:
			return joblist::DATETIMENULL;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		case CalpontSystemCatalog::STRINT: {
			uint32_t len = getColumnWidth(colIndex);
			switch (len) {
				case 1: return joblist::CHAR1NULL;
				case 2: return joblist::CHAR2NULL;
				case 3:
				case 4: return joblist::CHAR4NULL;
				case 5:
				case 6:
				case 7:
				case 8: return joblist::CHAR8NULL;
				default:
					throw logic_error("Row::getNullValue() Can't return the NULL string");
			}
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
		case CalpontSystemCatalog::UDECIMAL:
		{
			uint32_t len = getColumnWidth(colIndex);
			switch (len) {
				case 1 : return joblist::TINYINTNULL;
				case 2 : return joblist::SMALLINTNULL;
				case 4 : return joblist::INTNULL;
				default: return joblist::BIGINTNULL;
			}
			break;
		}
		case CalpontSystemCatalog::UTINYINT:
			return joblist::UTINYINTNULL;
		case CalpontSystemCatalog::USMALLINT:
			return joblist::USMALLINTNULL;
		case CalpontSystemCatalog::UMEDINT:
		case CalpontSystemCatalog::UINT:
			return joblist::UINTNULL;
		case CalpontSystemCatalog::UBIGINT:
			return joblist::UBIGINTNULL;
		case CalpontSystemCatalog::LONGDOUBLE:
			return -1;  // no NULL value for long double yet, this is a nan.
		case CalpontSystemCatalog::VARBINARY:
		default:
			ostringstream os;
			os << "Row::getNullValue(): got bad column type (" << types[colIndex] <<
				").  Width=" << getColumnWidth(colIndex) << endl;
			os << toString() << endl;
			throw logic_error(os.str());
	}
#endif
}

/* This fcn might produce overflow warnings from the compiler, but that's OK.
 * The overflow is intentional...
 */
int64_t Row::getSignedNullValue(uint32_t colIndex) const
{
	return utils::getSignedNullValue(types[colIndex], getColumnWidth(colIndex));
#if 0
	switch (types[colIndex]) {
		case CalpontSystemCatalog::TINYINT:
			return (int64_t) ((int8_t) joblist::TINYINTNULL);
		case CalpontSystemCatalog::SMALLINT:
			return (int64_t) ((int16_t) joblist::SMALLINTNULL);
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			return (int64_t) ((int32_t) joblist::INTNULL);
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
			return (int64_t) ((int32_t) joblist::FLOATNULL);
		case CalpontSystemCatalog::DATE:
			return (int64_t) ((int32_t) joblist::DATENULL);
		case CalpontSystemCatalog::BIGINT:
			return joblist::BIGINTNULL;
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
			return joblist::DOUBLENULL;
		case CalpontSystemCatalog::DATETIME:
			return joblist::DATETIMENULL;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		case CalpontSystemCatalog::STRINT: {
			uint32_t len = getColumnWidth(colIndex);
			switch (len) {
				case 1: return (int64_t) ((int8_t) joblist::CHAR1NULL);
				case 2: return (int64_t) ((int16_t) joblist::CHAR2NULL);
				case 3:
				case 4: return (int64_t) ((int32_t) joblist::CHAR4NULL);
				case 5:
				case 6:
				case 7:
				case 8: return joblist::CHAR8NULL;
				default:
					throw logic_error("Row::getSignedNullValue() Can't return the NULL string");
			}
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
		case CalpontSystemCatalog::UDECIMAL: {
			uint32_t len = getColumnWidth(colIndex);
			switch (len) {
				case 1 : return (int64_t) ((int8_t)  joblist::TINYINTNULL);
				case 2 : return (int64_t) ((int16_t) joblist::SMALLINTNULL);
				case 4 : return (int64_t) ((int32_t) joblist::INTNULL);
				default: return joblist::BIGINTNULL;
			}
			break;
		}
		case CalpontSystemCatalog::UTINYINT:
			return (int64_t) ((int8_t) joblist::UTINYINTNULL);
		case CalpontSystemCatalog::USMALLINT:
			return (int64_t) ((int16_t) joblist::USMALLINTNULL);
		case CalpontSystemCatalog::UMEDINT:
		case CalpontSystemCatalog::UINT:
			return (int64_t) ((int32_t) joblist::UINTNULL);
		case CalpontSystemCatalog::UBIGINT:
			return (int64_t)joblist::UBIGINTNULL;
		case CalpontSystemCatalog::LONGDOUBLE:
			return -1;  // no NULL value for long double yet, this is a nan.
		case CalpontSystemCatalog::VARBINARY:
		default:
			ostringstream os;
			os << "Row::getSignedNullValue(): got bad column type (" << types[colIndex] <<
				").  Width=" << getColumnWidth(colIndex) << endl;
			os << toString() << endl;
			throw logic_error(os.str());
	}
#endif
}

RowGroup::RowGroup() : columnCount(0), data(NULL), rgData(NULL), strings(NULL),
	useStringTable(true), hasLongStringField(false), sTableThreshold(20)
{ }

RowGroup::RowGroup(uint32_t colCount,
	const vector<uint32_t> &positions,
	const vector<uint32_t> &roids,
	const vector<uint32_t> &tkeys,
	const vector<CalpontSystemCatalog::ColDataType> &colTypes,
	const vector<uint32_t> &cscale,
	const vector<uint32_t> &cprecision,
	uint32_t stringTableThreshold,
	bool stringTable,
	const vector<bool> &forceInlineData
	) :
	columnCount(colCount), data(NULL), oldOffsets(positions), oids(roids), keys(tkeys),
	types(colTypes), scale(cscale), precision(cprecision), rgData(NULL), strings(NULL),
	sTableThreshold(stringTableThreshold)
{
	uint32_t i;

	forceInline.reset(new bool[columnCount]);
	if (forceInlineData.empty())
		for (i = 0; i < columnCount; i++)
			forceInline[i] = false;
	else
		for (i = 0; i < columnCount; i++)
			forceInline[i] = forceInlineData[i];

	colWidths.resize(columnCount);
	stOffsets.resize(columnCount + 1);
	stOffsets[0] = 2;  // 2-byte rid
	hasLongStringField = false;
	for (i = 0; i < columnCount; i++) {
		colWidths[i] = positions[i+1] - positions[i];
		if (colWidths[i] >= sTableThreshold && !forceInline[i]) {
			hasLongStringField = true;
			stOffsets[i+1] = stOffsets[i] + 8;
		}
		else
			stOffsets[i+1] = stOffsets[i] + colWidths[i];
	}
	useStringTable = (stringTable && hasLongStringField);
	offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
}

RowGroup::RowGroup(const RowGroup &r) :
	columnCount(r.columnCount), data(r.data), oldOffsets(r.oldOffsets),
	stOffsets(r.stOffsets), colWidths(r.colWidths),
	oids(r.oids), keys(r.keys), types(r.types), scale(r.scale), precision(r.precision),
	rgData(r.rgData), strings(r.strings), useStringTable(r.useStringTable),
	hasLongStringField(r.hasLongStringField), sTableThreshold(r.sTableThreshold),
	forceInline(r.forceInline)
{
	//stOffsets and oldOffsets are sometimes empty...
	//offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
	offsets = 0;
	if (useStringTable && !stOffsets.empty())
		offsets = &stOffsets[0];
	else if (!useStringTable && !oldOffsets.empty())
		offsets = &oldOffsets[0];
}

RowGroup & RowGroup::operator=(const RowGroup &r)
{
	columnCount = r.columnCount;
	oldOffsets = r.oldOffsets;
	stOffsets = r.stOffsets;
	colWidths = r.colWidths;
	oids = r.oids;
	keys = r.keys;
	types = r.types;
	data = r.data;
	scale = r.scale;
	precision = r.precision;
	rgData = r.rgData;
	strings = r.strings;
	useStringTable = r.useStringTable;
	hasLongStringField = r.hasLongStringField;
	sTableThreshold = r.sTableThreshold;
	forceInline = r.forceInline;
	//offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
	offsets = 0;
	if (useStringTable && !stOffsets.empty())
		offsets = &stOffsets[0];
	else if (!useStringTable && !oldOffsets.empty())
		offsets = &oldOffsets[0];
	return *this;
}

RowGroup::~RowGroup() { }

void RowGroup::resetRowGroup(uint64_t rid)
{
	*((uint32_t *) &data[rowCountOffset]) = 0;
	*((uint64_t *) &data[baseRidOffset]) = rid;
	*((uint16_t *) &data[statusOffset]) = 0;
	*((uint32_t *) &data[dbRootOffset]) = 0;
	if (strings)
		strings->clear();
}

void RowGroup::serialize(ByteStream &bs) const
{
	bs << columnCount;
	serializeInlineVector<uint32_t>(bs, oldOffsets);
	serializeInlineVector<uint32_t>(bs, stOffsets);
	serializeInlineVector<uint32_t>(bs, colWidths);
	serializeInlineVector<uint32_t>(bs, oids);
	serializeInlineVector<uint32_t>(bs, keys);
	serializeInlineVector<CalpontSystemCatalog::ColDataType>(bs, types);
	serializeInlineVector<uint32_t>(bs, scale);
	serializeInlineVector<uint32_t>(bs, precision);
	bs << (uint8_t) useStringTable;
	bs << (uint8_t) hasLongStringField;
	bs << sTableThreshold;
	bs.append((uint8_t *) &forceInline[0], sizeof(bool) * columnCount);
}

void RowGroup::deserialize(ByteStream &bs)
{
	uint8_t tmp8;

	bs >> columnCount;
	deserializeInlineVector<uint32_t>(bs, oldOffsets);
	deserializeInlineVector<uint32_t>(bs, stOffsets);
	deserializeInlineVector<uint32_t>(bs, colWidths);
	deserializeInlineVector<uint32_t>(bs, oids);
	deserializeInlineVector<uint32_t>(bs, keys);
	deserializeInlineVector<CalpontSystemCatalog::ColDataType>(bs, types);
	deserializeInlineVector<uint32_t>(bs, scale);
	deserializeInlineVector<uint32_t>(bs, precision);
	bs >> tmp8;
	useStringTable = (bool) tmp8;
	bs >> tmp8;
	hasLongStringField = (bool) tmp8;
	bs >> sTableThreshold;
	forceInline.reset(new bool[columnCount]);
	memcpy(&forceInline[0], bs.buf(), sizeof(bool) * columnCount);
	bs.advance(sizeof(bool) * columnCount);
	//offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
	offsets = 0;
	if (useStringTable && !stOffsets.empty())
		offsets = &stOffsets[0];
	else if (!useStringTable && !oldOffsets.empty())
		offsets = &oldOffsets[0];
}

void RowGroup::serializeRGData(ByteStream &bs) const
{
	//cout << "****** serializing\n" << toString() << en
//	if (useStringTable || !hasLongStringField)
		rgData->serialize(bs, getDataSize());
//	else {
//		uint64_t size;
//		RGData *compressed = convertToStringTable(&size);
//		compressed->serialize(bs, size);
//		if (compressed != rgData)
//			delete compressed;
//	}
}

uint32_t RowGroup::getDataSize() const
{
	return headerSize + (getRowCount() * offsets[columnCount]);
}

uint32_t RowGroup::getDataSize(uint64_t n) const
{
	return headerSize + (n * offsets[columnCount]);
}

uint32_t RowGroup::getMaxDataSize() const
{
	return headerSize + (8192 * offsets[columnCount]);
}

uint32_t RowGroup::getMaxDataSizeWithStrings() const
{
	return headerSize + (8192 * oldOffsets[columnCount]);
}

uint32_t RowGroup::getEmptySize() const
{
	return headerSize;
}

uint32_t RowGroup::getStatus() const
{
	return *((uint16_t *) &data[statusOffset]);
}

void RowGroup::setStatus(uint16_t err)
{
	*((uint16_t *) &data[statusOffset]) = err;
}

uint32_t RowGroup::getColumnWidth(uint32_t col) const
{
	return colWidths[col];
}

uint32_t RowGroup::getColumnCount() const
{
	return columnCount;
}

string RowGroup::toString() const
{
	ostringstream os;
	ostream_iterator<int> oIter1(os, "\t");

	os << "columncount = " << columnCount << endl;
	os << "oids:\t\t"; copy(oids.begin(), oids.end(), oIter1);
	os << endl;
	os << "keys:\t\t"; copy(keys.begin(), keys.end(), oIter1);
	os << endl;
	os << "offsets:\t"; copy(&offsets[0], &offsets[columnCount+1], oIter1);
	os << endl;
	os << "colWidths:\t"; copy(colWidths.begin(), colWidths.end(), oIter1);
	os << endl;
	os << "types:\t\t"; copy(types.begin(), types.end(), oIter1);
	os << endl;
	os << "scales:\t\t"; copy(scale.begin(), scale.end(), oIter1);
	os << endl;
	os << "precisions:\t"; copy(precision.begin(), precision.end(), oIter1);
	os << endl;
	if (useStringTable)
		os << "uses a string table\n";
	else
		os << "doesn't use a string table\n";
	//os << "strings = " << hex << (int64_t) strings << "\n";
	//os << "data = " << (int64_t) data << "\n" << dec;
	if (data != NULL) {
		Row r;
		initRow(&r);
		getRow(0, &r);
		os << "rowcount = " << getRowCount() << endl;
		os << "base rid = " << getBaseRid() << endl;
		os << "status = " << getStatus() << endl;
		os << "dbroot = " << getDBRoot() << endl;
		os << "row data...\n";
		for (uint32_t i = 0; i < getRowCount(); i++) {
			os << r.toString() << endl;
			r.nextRow();
		}
	}
	return os.str();
}

boost::shared_array<int> makeMapping(const RowGroup &r1, const RowGroup &r2)
{
	shared_array<int> ret(new int[r1.getColumnCount()]);
	//bool reserved[r2.getColumnCount()];
	bool* reserved = (bool*)alloca(r2.getColumnCount() * sizeof(bool));
	uint32_t i, j;

	for (i = 0; i < r2.getColumnCount(); i++)
		reserved[i] = false;

	for (i = 0; i < r1.getColumnCount(); i++) {
		for (j = 0; j < r2.getColumnCount(); j++)
			if ((r1.getKeys()[i] == r2.getKeys()[j]) && !reserved[j]) {
				ret[i] = j;
				reserved[j] = true;
				break;
			}
		if (j == r2.getColumnCount())
			ret[i] = -1;
	}
	return ret;
}

void applyMapping(const boost::shared_array<int>& mapping, const Row &in, Row *out)
{
	applyMapping(mapping.get(), in, out);
}

void applyMapping(const std::vector<int>& mapping, const Row &in, Row *out)
{
	applyMapping((int *) &mapping[0], in, out);
}

void applyMapping(const int *mapping, const Row &in, Row *out)
{
	uint32_t i;

	for (i = 0; i < in.getColumnCount(); i++)
		if (mapping[i] != -1)
		{
			if (UNLIKELY(in.isLongString(i)))
				out->setStringField(in.getStringPointer(i), in.getStringLength(i), mapping[i]);
				//out->setStringField(in.getStringField(i), mapping[i]);
			else if (UNLIKELY(in.isShortString(i)))
				out->setUintField(in.getUintField(i), mapping[i]);
			else if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::VARBINARY))
				out->setVarBinaryField(in.getVarBinaryField(i), in.getVarBinaryLength(i), mapping[i]);
			else if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::LONGDOUBLE))
				out->setLongDoubleField(in.getLongDoubleField(i), mapping[i]);
			else if (in.isUnsigned(i))
				out->setUintField(in.getUintField(i), mapping[i]);
			else
				out->setIntField(in.getIntField(i), mapping[i]);
		}
}

RowGroup& RowGroup::operator+=(const RowGroup& rhs)
{
	boost::shared_array<bool> tmp;
	uint32_t i, j;
	//not appendable if data is set
	assert(!data);

	tmp.reset(new bool[columnCount + rhs.columnCount]);
	for (i = 0; i < columnCount; i++)
		tmp[i] = forceInline[i];
	for (j = 0; j < rhs.columnCount; i++, j++)
		tmp[i] = rhs.forceInline[j];
	forceInline.swap(tmp);

	columnCount += rhs.columnCount;
	oids.insert(oids.end(), rhs.oids.begin(), rhs.oids.end());
	keys.insert(keys.end(), rhs.keys.begin(), rhs.keys.end());
	types.insert(types.end(), rhs.types.begin(), rhs.types.end());
	scale.insert(scale.end(), rhs.scale.begin(), rhs.scale.end());
	precision.insert(precision.end(), rhs.precision.begin(), rhs.precision.end());
	colWidths.insert(colWidths.end(), rhs.colWidths.begin(), rhs.colWidths.end());

	//    +4  +4  +8       +2 +4  +8
	// (2, 6, 10, 18) + (2, 4, 8, 16) = (2, 6, 10, 18, 20, 24, 32)
	for (i = 1; i < rhs.stOffsets.size(); i++) {
		stOffsets.push_back(stOffsets.back() + rhs.stOffsets[i] - rhs.stOffsets[i - 1]);
		oldOffsets.push_back(oldOffsets.back() + rhs.oldOffsets[i] - rhs.oldOffsets[i - 1]);
	}

	hasLongStringField = rhs.hasLongStringField || hasLongStringField;
	offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);

	return *this;
}

RowGroup operator+(const RowGroup& lhs, const RowGroup& rhs)
{
	RowGroup temp(lhs);
	return temp += rhs;
}

uint32_t RowGroup::getDBRoot() const
{
	return *((uint32_t *) &data[dbRootOffset]);
}

void RowGroup::addToSysDataList(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList)
{
	execplan::ColumnResult *cr;

	rowgroup::Row row;
	initRow(&row);
	uint32_t rowCount = getRowCount();
	uint32_t columnCount = getColumnCount();

	for (uint32_t i = 0; i < rowCount; i++)
	{
		getRow(i, &row);
		for (uint32_t j = 0; j < columnCount; j++)
		{
			int idx = sysDataList.findColumn(getOIDs()[j]);
			if(idx >= 0) {
				cr = sysDataList.sysDataVec[idx];
			}
			else {
				cr = new execplan::ColumnResult();
				cr->SetColumnOID(getOIDs()[j]);
				sysDataList.push_back(cr);
			}
			// @todo more data type checking. for now only check string, midint and bigint
			switch ((getColTypes()[j]))
			{
				case CalpontSystemCatalog::CHAR:
				case CalpontSystemCatalog::VARCHAR:
				{
					switch (getColumnWidth(j))
					{
						case 1:
							cr->PutData(row.getUintField<1>(j));
							break;
						case 2:
							cr->PutData(row.getUintField<2>(j));
							break;
						case 4:
							cr->PutData(row.getUintField<4>(j));
							break;
						case 8:
							cr->PutData(row.getUintField<8>(j));
							break;
						default:
						{
							string s = row.getStringField(j);
							cr->PutStringData(string(s.c_str(), strlen(s.c_str())));
						}
					}
					break;
				}
				case CalpontSystemCatalog::MEDINT:
				case CalpontSystemCatalog::INT:
				case CalpontSystemCatalog::UINT:
					cr->PutData(row.getIntField<4>(j));
					break;
				case CalpontSystemCatalog::DATE:
					cr->PutData(row.getUintField<4>(j));
					break;
				default:
					cr->PutData(row.getIntField<8>(j));
			}
			cr->PutRid(row.getFileRelativeRid());
		}
	}
}

void RowGroup::setDBRoot(uint32_t dbroot)
{
	*((uint32_t *) &data[dbRootOffset]) = dbroot;
}

RGData RowGroup::duplicate()
{
	RGData ret(*this, getRowCount());
	if (useStringTable) {
		// this isn't a straight memcpy of everything b/c it might be remapping strings.
		// think about a big memcpy + a remap operation; might be faster.
		Row r1, r2;
		RowGroup rg(*this);
		rg.setData(&ret);
		rg.resetRowGroup(getBaseRid());
		rg.setStatus(getStatus());
		rg.setRowCount(getRowCount());
		rg.setDBRoot(getDBRoot());
		initRow(&r1);
		initRow(&r2);
		getRow(0, &r1);
		rg.getRow(0, &r2);
		for (uint32_t i = 0; i < getRowCount(); i++) {
			copyRow(r1, &r2);
			r1.nextRow();
			r2.nextRow();
		}
	}
	else
		memcpy(ret.rowData.get(), data, getDataSize());
	return ret;
}


void Row::setStringField(const std::string &val, uint32_t colIndex)
{
	uint32_t length;
	uint32_t offset;

	//length = strlen(val.c_str()) + 1;
	length = val.length();
	if (length > getColumnWidth(colIndex))
		length = getColumnWidth(colIndex);

	if (inStringTable(colIndex)) {
		offset = strings->storeString((const uint8_t *) val.data(), length);
		*((uint32_t *) &data[offsets[colIndex]]) = offset;
		*((uint32_t *) &data[offsets[colIndex] + 4]) = length;
//		cout << " -- stored offset " << *((uint32_t *) &data[offsets[colIndex]])
//				<< " length " << *((uint32_t *) &data[offsets[colIndex] + 4])
//				<< endl;
	}
	else {
		memcpy(&data[offsets[colIndex]], val.data(), length);
		memset(&data[offsets[colIndex] + length], 0,
			offsets[colIndex + 1] - (offsets[colIndex] + length));
	}
}

void RowGroup::append(RGData &rgd)
{
	RowGroup tmp(*this);
	Row src, dest;

	tmp.setData(&rgd);
	initRow(&src);
	initRow(&dest);
	tmp.getRow(0, &src);
	getRow(getRowCount(), &dest);

	for (uint32_t i = 0; i < tmp.getRowCount(); i++, src.nextRow(), dest.nextRow()) {
		//cerr << "appending row: " << src.toString() << endl;
		copyRow(src, &dest);
	}

	setRowCount(getRowCount() + tmp.getRowCount());
}

void RowGroup::append(RowGroup &rg)
{
	append(*rg.getRGData());
}

void RowGroup::append(RGData &rgd, uint32_t startPos)
{
	RowGroup tmp(*this);
	Row src, dest;

	tmp.setData(&rgd);
	initRow(&src);
	initRow(&dest);
	tmp.getRow(0, &src);
	getRow(startPos, &dest);

	for (uint32_t i = 0; i < tmp.getRowCount(); i++, src.nextRow(), dest.nextRow()) {
		//cerr << "appending row: " << src.toString() << endl;
		copyRow(src, &dest);
	}

	setRowCount(getRowCount() + tmp.getRowCount());
}

void RowGroup::append(RowGroup &rg, uint32_t startPos)
{
	append(*rg.getRGData(), startPos);
}

RowGroup RowGroup::truncate(uint32_t cols)
{
	idbassert(cols <= columnCount);

	RowGroup ret(*this);
	ret.columnCount = cols;
	ret.oldOffsets.resize(cols+1);
	ret.stOffsets.resize(cols+1);
	ret.colWidths.resize(cols);
	ret.oids.resize(cols);
	ret.keys.resize(cols);
	ret.types.resize(cols);
	ret.scale.resize(cols);
	ret.precision.resize(cols);
	ret.forceInline.reset(new bool[cols]);
	memcpy(ret.forceInline.get(), forceInline.get(), cols * sizeof(bool));

	ret.hasLongStringField = false;
	for (uint32_t i = 0; i < columnCount; i++) {
		if (colWidths[i] >= sTableThreshold && !forceInline[i]) {
			ret.hasLongStringField = true;
			break;
		}
	}
	ret.useStringTable = (ret.useStringTable && ret.hasLongStringField);
	ret.offsets = (ret.useStringTable ? &ret.stOffsets[0] : &ret.oldOffsets[0]);
	return ret;
}

}

// vim:ts=4 sw=4:

