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

//  $Id: rowgroup.h 4020 2013-07-26 21:41:28Z pleblanc $

//
// C++ Interface: rowgroup
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//
/** @file */

#ifndef ROWGROUP_H_
#define ROWGROUP_H_

#include <vector>
#include <string>
#include <stdexcept>
//#define NDEBUG
#include <cassert>
#include <boost/shared_array.hpp>
#include <cmath>
#include <cfloat>
#ifdef __linux__
#include <execinfo.h>
#endif

#if defined(_MSC_VER) && !defined(isnan)
#define isnan _isnan
#endif

#include "hasher.h"

#include "joblisttypes.h"
#include "bytestream.h"
#include "calpontsystemcatalog.h"
#include "exceptclasses.h"

#include "branchpred.h"

namespace rowgroup
{

/*
	The format of the data RowGroup points to is currently ...

	32-bit - Row Count
	64-bit - Base Rid
	16-bit - Status
	Row*

	where
	Row =	16-bit rid # relative to the base rid
			field1
			field2
			...

	RowGroup.getRow() points a Row instance to the specified row within the
	RowGroup data
*/

// VS'08 carps that struct MemChunk is not default copyable because of the zero-length array.
// This may be so, and we'll get link errors if someone trys, but so far no one has.
#ifdef _MSC_VER
#pragma warning (push)
#pragma warning (disable : 4200)
#endif

class StringStore
{
public:
	StringStore();
	virtual ~StringStore();

	inline std::string getString(uint32_t offset, uint32_t length) const;
	uint32_t storeString(const uint8_t *data, uint32_t length);  //returns the offset
	inline const uint8_t * getPointer(uint32_t offset) const;
	inline bool isEmpty() const;
	inline uint64_t getSize() const;
	inline bool isNullValue(uint32_t offset, uint32_t length) const;
	inline bool equals(const std::string &str, uint32_t offset, uint32_t length) const;

	void clear();

	void serialize(messageqcpp::ByteStream &) const;
	uint deserialize(messageqcpp::ByteStream &);

private:
	StringStore(const StringStore &);
	StringStore & operator=(const StringStore &);
	static const uint32_t CHUNK_SIZE = 64*1024;    // allocators like powers of 2

	// This is an overlay b/c the underlying data needs to be any size,
	// and alloc'd in one chunk.  data can't be a sepatate dynamic chunk.
	struct MemChunk
	{
		uint32_t currentSize;
		uint32_t capacity;
		uint8_t data[];
	};

	std::vector<boost::shared_array<uint8_t> > mem;
	bool empty;
};

#ifdef _MSC_VER
#pragma warning (pop)
#endif

class RowGroup;
class Row;

/* TODO: OO the rowgroup data to the extent there's no measurable performance hit. */
class RGData
{
public:
	RGData();   // useless unless followed by an = or a deserialize operation
	RGData(const RowGroup &rg, uint rowCount);   // allocates memory for rowData
	explicit RGData(const RowGroup &rg);
	RGData(const RGData &);
	virtual ~RGData();

	inline RGData & operator=(const RGData &);

	// amount should be the # returned by RowGroup::getDataSize()
	void serialize(messageqcpp::ByteStream &, uint amount) const;
	
	// the 'hasLengthField' is there b/c PM aggregation (and possibly others) currently sends 
	// inline data with a length field.  Once that's converted to string table format, that
	// option can go away.
	uint deserialize(messageqcpp::ByteStream &, bool hasLengthField=false);  // returns the # of bytes read

	inline uint64_t getStringTableMemUsage();
	void clear();
	void reinit(const RowGroup &rg);
	void reinit(const RowGroup &rg, uint rowCount);
	inline void setStringStore(boost::shared_ptr<StringStore> &ss) { strings = ss; }

	// this will use the pre-configured Row to figure out where row # num is, then set the Row
	// to point to it.  It's a shortcut around using a RowGroup to do the same thing for cases
	// where it's inconvenient to instantiate one.
	inline void getRow(uint num, Row *row);
	
	boost::shared_array<uint8_t> rowData;
	boost::shared_ptr<StringStore> strings;

private:
	//boost::shared_array<uint8_t> rowData;
	//boost::shared_ptr<StringStore> strings;

	// Need sig to support backward compat.  RGData can deserialize both forms.
	static const uint32_t RGDATA_SIG = 0xffffffff;  //won't happen for 'old' Rowgroup data

friend class RowGroup;
};

class Row
{
	public:
		struct Pointer {
			inline Pointer() : data(NULL), strings(NULL) { }

			// Pointer(uint8_t*) implicitly makes old code compatible with the string table impl;
			// make it explicit to identify things that still might need to be changed
			inline Pointer(uint8_t *d) : data(d), strings(NULL) { }
			inline Pointer(uint8_t *d, StringStore *s) : data(d), strings(s) { }
			uint8_t *data;
			StringStore *strings;
		};

		Row();
		Row(const Row &);
		~Row();

		Row & operator=(const Row &);
		bool operator==(const Row &) const;

		//void setData(uint8_t *rowData, StringStore *ss);
		inline void setData(const Pointer &);   // convenience fcn, can go away
		inline uint8_t * getData() const;

		inline void setPointer(const Pointer &);
		inline Pointer getPointer() const;

		inline void nextRow();
		inline uint getColumnWidth(uint colIndex) const;
		inline uint getColumnCount() const;
		inline uint getSize() const;		// this is only accurate if there is no string table
		// if a string table is being used, getRealSize() takes into account variable-length strings
		inline uint getRealSize() const;
		inline uint getOffset(uint colIndex) const;
		inline uint getScale(uint colIndex) const;
		inline uint getPrecision(uint colIndex) const;
		inline execplan::CalpontSystemCatalog::ColDataType getColType(uint colIndex) const;
		inline execplan::CalpontSystemCatalog::ColDataType* getColTypes();
		inline const execplan::CalpontSystemCatalog::ColDataType* getColTypes() const;

		// this returns true if the type is not CHAR or VARCHAR
		inline bool isCharType(uint colIndex) const;
        inline bool isUnsigned(uint colIndex) const;
		inline bool isShortString(uint colIndex) const;
		inline bool isLongString(uint colIndex) const;

		template<int len> inline uint64_t getUintField(uint colIndex) const;
		inline uint64_t getUintField(uint colIndex) const;
		template<int len> inline int64_t getIntField(uint colIndex) const;
		inline int64_t getIntField(uint colIndex) const;
		template<int len> inline bool equals(uint64_t val, uint colIndex) const;
		inline bool equals(const std::string &val, uint colIndex) const;

		inline double getDoubleField(uint colIndex) const;
		inline float getFloatField(uint colIndex) const;
		inline double getDecimalField(uint colIndex) const {return 0.0;} // TODO: Do something here
		inline long double getLongDoubleField(uint colIndex) const;

		inline uint64_t getBaseRid() const;
		inline uint64_t getRid() const;
		inline uint16_t getRelRid() const;   // returns a rid relative to this logical block
		inline uint64_t getExtentRelativeRid() const;   // returns a rid relative to the extent it's in
		inline uint64_t getFileRelativeRid() const; // returns a file-relative rid
		inline void getLocation(uint32_t *partNum, uint16_t *segNum, uint8_t *extentNum,
				uint16_t *blockNum, uint16_t *rowNum);

		template<int len> void setUintField(uint64_t val, uint colIndex);

		/* Note: these 2 fcns avoid 1 array lookup per call.  Using them only
		in projection on the PM resulted in a 2.8% performance gain on
		the queries listed in bug 2223.
		TODO: apply them everywhere else possible, and write equivalents
		for the other types as well as the getters.
		*/
		template<int len> void setUintField_offset(uint64_t val, uint offset);
		inline void nextRow(uint size);

		inline void setUintField(uint64_t val, uint colIndex);
		template<int len> void setIntField(int64_t, uint colIndex);
		inline void setIntField(int64_t, uint colIndex);

		inline void setDoubleField(double val, uint colIndex);
		inline void setFloatField(float val, uint colIndex);
		inline void setDecimalField(double val, uint colIndex) { };  // TODO: Do something here
		inline void setLongDoubleField(long double val, uint colIndex);

		inline void setRid(uint64_t rid);

		// is string efficient for this?
		inline std::string getStringField(uint colIndex) const;
		inline const uint8_t * getStringPointer(uint colIndex) const;
		inline uint32_t getStringLength(uint colIndex) const;
		void setStringField(const std::string &val, uint colIndex);
		inline void setStringField(const uint8_t *, uint len, uint colIndex);

		// support VARBINARY
		// Add 2-byte length at the beginning of the field.  NULL and zero length field are
		// treated the same, could use one of the length bit to distinguish these two cases.
		inline std::string getVarBinaryStringField(uint colIndex) const;
		inline void setVarBinaryField(const std::string &val, uint colIndex);
		// No string construction is necessary for better performance.
		inline uint getVarBinaryLength(uint colIndex) const;
		inline const uint8_t* getVarBinaryField(uint colIndex) const;
		inline const uint8_t* getVarBinaryField(uint& len, uint colIndex) const;
		inline void setVarBinaryField(const uint8_t* val, uint len, uint colIndex);

		uint64_t getNullValue(uint colIndex) const;
		bool isNullValue(uint colIndex) const;

		// when NULLs are pulled out via getIntField(), they come out with these values.
		// Ex: the 1-byte int null value is 0x80.  When it gets cast to an int64_t
		// it becomes 0xffffffffffffff80, which won't match anything returned by getNullValue().
		int64_t getSignedNullValue(uint colIndex) const;

		// copy data in srcIndex field to destIndex, all data type
		inline void copyField(uint destIndex, uint srcIndex) const;

		// copy data in srcIndex field to destAddr, all data type
		//inline void copyField(uint8_t* destAddr, uint srcIndex) const;
		
		// an adapter for code that uses the copyField call above;
		// that's not string-table safe, this one is
		inline void copyField(Row &dest, uint destIndex, uint srcIndex) const;

		std::string toString() const;
		std::string toCSV() const;

		/* These fcns are used only in joins.  The RID doesn't matter on the side that
		gets hashed.  We steal that field here to "mark" a row. */
		inline void markRow();
		inline void zeroRid();
		inline bool isMarked();
		void initToNull();

		inline bool usesStringTable() const { return useStringTable; }
		inline bool hasLongString() const { return hasLongStringField; }
		inline uint64_t hash(const std::vector<uint> &keyColumns) const;
		inline uint64_t hash(uint lastCol) const;  // generates a hash for cols [0-lastCol]
		inline uint64_t hash() const;  // generates a hash for all cols

		// these are for cases when you already know the type definitions are the same.
		// a fcn to check the type defs seperately doesn't exist yet.
		inline bool equals(const Row &, const std::vector<uint> &keyColumns) const;
		inline bool equals(const Row &, uint lastCol) const;
		inline bool equals(const Row &) const;

	private:
		uint columnCount;
		uint64_t baseRid;

		// the next 6 point to memory owned by RowGroup
		uint *oldOffsets;
		uint *stOffsets;
		uint *offsets;
		uint *colWidths;
		execplan::CalpontSystemCatalog::ColDataType *types;
		uint8_t *data;
		uint *scale;
		uint *precision;

		StringStore *strings;
		bool useStringTable;
		bool hasLongStringField;
		uint sTableThreshold;
		boost::shared_array<bool> forceInline;
		inline bool inStringTable(uint32_t col) const;

		friend class RowGroup;
};

inline Row::Pointer Row::getPointer() const { return Pointer(data, strings); }
inline uint8_t * Row::getData() const { return data; }

inline void Row::setPointer(const Pointer &p)
{
	data = p.data;
	strings = p.strings;
	bool hasStrings = (strings != 0);
	if (useStringTable != hasStrings) {
		useStringTable = hasStrings;
		offsets = (useStringTable ? stOffsets : oldOffsets);
	}
}

inline void Row::setData(const Pointer &p) { setPointer(p); }

inline void Row::nextRow() { data += offsets[columnCount]; }

inline uint Row::getColumnCount() const { return columnCount; }

inline uint Row::getColumnWidth(uint col) const
{
	return colWidths[col];
}

inline uint Row::getSize() const
{
	return offsets[columnCount];
}

inline uint Row::getRealSize() const
{
	if (!useStringTable)
		return getSize();
	
	uint ret = 2;
	for (uint i = 0; i < columnCount; i++) {
		if (!inStringTable(i))
			ret += getColumnWidth(i);
		else
			ret += getStringLength(i);
	}
	return ret;
}

inline uint Row::getScale(uint col) const
{
	return scale[col];
}

inline uint Row::getPrecision(uint col) const
{
	return precision[col];
}

inline execplan::CalpontSystemCatalog::ColDataType Row::getColType(uint colIndex) const
{
	return types[colIndex];
}

inline execplan::CalpontSystemCatalog::ColDataType* Row::getColTypes()
{
	return types;
}

inline const execplan::CalpontSystemCatalog::ColDataType* Row::getColTypes() const
{
	return types;
}

inline bool Row::isCharType(uint colIndex) const
{
    return execplan::isCharType(types[colIndex]);
}

inline bool Row::isUnsigned(uint colIndex) const
{
    return execplan::isUnsigned(types[colIndex]);
}

inline bool Row::isShortString(uint colIndex) const
{
	return (getColumnWidth(colIndex) <= 8 && isCharType(colIndex));
}

inline bool Row::isLongString(uint colIndex) const
{
	return (getColumnWidth(colIndex) > 8 && isCharType(colIndex));
}

inline bool Row::inStringTable(uint col) const
{
	return strings && getColumnWidth(col) >= sTableThreshold && !forceInline[col];
}

template<int len>
inline bool Row::equals(uint64_t val, uint colIndex) const
{
/* I think the compiler will optimize away the switch stmt */
	switch (len) {
		case 1: return data[offsets[colIndex]] == val;
		case 2: return *((uint16_t *) &data[offsets[colIndex]]) == val;
		case 4: return *((uint32_t *) &data[offsets[colIndex]]) == val;
		case 8: return *((uint64_t *) &data[offsets[colIndex]]) == val;
		default:
			idbassert(0);
			throw std::logic_error("Row::equals(): bad length.");
	}
}

inline bool Row::equals(const std::string &val, uint colIndex) const
{
	if (inStringTable(colIndex)) {
		uint32_t offset = *((uint32_t *) &data[offsets[colIndex]]);
		uint32_t length = *((uint32_t *) &data[offsets[colIndex] + 4]);
		return strings->equals(val, offset, length);
	}
	else
		return (strncmp(val.c_str(), (char *) &data[offsets[colIndex]], getColumnWidth(colIndex)) == 0);
}

template<int len>
inline uint64_t Row::getUintField(uint colIndex) const
{
	/* I think the compiler will optimize away the switch stmt */
	switch (len) {
		case 1: return data[offsets[colIndex]];
		case 2: return *((uint16_t *) &data[offsets[colIndex]]);
		case 4: return *((uint32_t *) &data[offsets[colIndex]]);
		case 8: return *((uint64_t *) &data[offsets[colIndex]]);
		default:
			idbassert(0);
			throw std::logic_error("Row::getUintField(): bad length.");
	}
}

inline uint64_t Row::getUintField(uint colIndex) const
{
	switch (getColumnWidth(colIndex)) {
		case 1: return data[offsets[colIndex]];
		case 2: return *((uint16_t *) &data[offsets[colIndex]]);
		case 4: return *((uint32_t *) &data[offsets[colIndex]]);
		case 8: return *((uint64_t *) &data[offsets[colIndex]]);
		default:
			idbassert(0);
			throw std::logic_error("Row::getUintField(): bad length.");
	}
}

template<int len>
inline int64_t Row::getIntField(uint colIndex) const
{
	/* I think the compiler will optimize away the switch stmt */
	switch (len) {
		case 1: return (int8_t) data[offsets[colIndex]];
		case 2: return *((int16_t *) &data[offsets[colIndex]]);
		case 4: return *((int32_t *) &data[offsets[colIndex]]);
		case 8: return *((int64_t *) &data[offsets[colIndex]]);
		default:
			idbassert(0);
			throw std::logic_error("Row::getIntField(): bad length.");
	}
}

inline int64_t Row::getIntField(uint colIndex) const
{
	/* I think the compiler will optimize away the switch stmt */
	switch (getColumnWidth(colIndex)) {
		case 1: return (int8_t) data[offsets[colIndex]];
		case 2: return *((int16_t *) &data[offsets[colIndex]]);
		case 4: return *((int32_t *) &data[offsets[colIndex]]);
		case 8: return *((int64_t *) &data[offsets[colIndex]]);
		default:
			idbassert(0);
			throw std::logic_error("Row::getIntField(): bad length.");
	}
}

inline const uint8_t * Row::getStringPointer(uint colIndex) const
{
	if (inStringTable(colIndex))
		return strings->getPointer(*((uint32_t *) &data[offsets[colIndex]]));
	return &data[offsets[colIndex]];
}

inline uint32_t Row::getStringLength(uint colIndex) const
{
	if (inStringTable(colIndex))
		return *((uint32_t *) &data[offsets[colIndex] + 4]);
	return strnlen((char *) &data[offsets[colIndex]], getColumnWidth(colIndex));
}

inline void Row::setStringField(const uint8_t *strdata, uint length, uint colIndex)
{
	uint32_t offset;

	if (length > getColumnWidth(colIndex))
		length = getColumnWidth(colIndex);

	if (inStringTable(colIndex)) {
		offset = strings->storeString(strdata, length);
		*((uint32_t *) &data[offsets[colIndex]]) = offset;
		*((uint32_t *) &data[offsets[colIndex] + 4]) = length;
//		cout << " -- stored offset " << *((uint32_t *) &data[offsets[colIndex]])
//				<< " length " << *((uint32_t *) &data[offsets[colIndex] + 4])
//				<< endl;
	}
	else {
		memcpy(&data[offsets[colIndex]], strdata, length);
		memset(&data[offsets[colIndex] + length], 0,
			offsets[colIndex + 1] - (offsets[colIndex] + length));
	}
}

inline std::string Row::getStringField(uint colIndex) const
{
	if (inStringTable(colIndex))
		return strings->getString(*((uint32_t *) &data[offsets[colIndex]]),
				*((uint32_t *) &data[offsets[colIndex] + 4]));
	if (types[colIndex] == execplan::CalpontSystemCatalog::VARCHAR)
		return std::string((char *) &data[offsets[colIndex]]);
	else   // types where we can't rely on NULL termination...
		return std::string((char *) &data[offsets[colIndex]],
						   strnlen((char *) &data[offsets[colIndex]], getColumnWidth(colIndex)));
//		return std::string((char *) &data[offsets[colIndex]], getColumnWidth(colIndex));
}

inline std::string Row::getVarBinaryStringField(uint colIndex) const
{
	if (inStringTable(colIndex))
		return getStringField(colIndex);
	return std::string((char*) &data[offsets[colIndex]+2], *((uint16_t*) &data[offsets[colIndex]]));
}

inline uint Row::getVarBinaryLength(uint colIndex) const
{
	if (inStringTable(colIndex))
		return *((uint32_t *) &data[offsets[colIndex] + 4]);
	return *((uint16_t*) &data[offsets[colIndex]]);
}

inline const uint8_t* Row::getVarBinaryField(uint colIndex) const
{
	if (inStringTable(colIndex))
		return strings->getPointer(*((uint32_t *) &data[offsets[colIndex]]));
	return &data[offsets[colIndex] + 2];
}

inline const uint8_t* Row::getVarBinaryField(uint& len, uint colIndex) const
{
	if (inStringTable(colIndex)) {
		len = *((uint32_t *) &data[offsets[colIndex] + 4]);
		return getVarBinaryField(colIndex);
	}
	else {
		len = *((uint16_t*) &data[offsets[colIndex]]);
		return &data[offsets[colIndex] + 2];
	}
}

inline double Row::getDoubleField(uint colIndex) const
{
	return *((double *) &data[offsets[colIndex]]);
}

inline float Row::getFloatField(uint colIndex) const
{
	return *((float *) &data[offsets[colIndex]]);
}

inline long double Row::getLongDoubleField(uint colIndex) const
{
	return *((long double *) &data[offsets[colIndex]]);
}

inline uint64_t Row::getRid() const
{
	return baseRid + *((uint16_t *) data);
}

inline uint16_t Row::getRelRid() const
{
	return *((uint16_t *) data);
}

inline uint64_t Row::getBaseRid() const
{
	return baseRid;
}

inline void Row::markRow()
{
	*((uint16_t *) data) = 0xffff;
}

inline void Row::zeroRid()
{
	*((uint16_t *) data) = 0;
}

inline bool Row::isMarked()
{
	return *((uint16_t *) data) == 0xffff;
}

/* Begin speculative code! */
inline uint Row::getOffset(uint colIndex) const
{
	return offsets[colIndex];
}

template<int len>
inline void Row::setUintField_offset(uint64_t val, uint offset)
{
	switch (len) {
		case 1: data[offset] = val; break;
		case 2: *((uint16_t *) &data[offset]) = val; break;
		case 4: *((uint32_t *) &data[offset]) = val; break;
		case 8: *((uint64_t *) &data[offset]) = val; break;
		default:
			idbassert(0);
			throw std::logic_error("Row::setUintField called on a non-uint field");
	}
}

inline void Row::nextRow(uint size)
{
	data += size;
}

template<int len>
inline void Row::setUintField(uint64_t val, uint colIndex)
{
	switch (len) {
		case 1: data[offsets[colIndex]] = val; break;
		case 2: *((uint16_t *) &data[offsets[colIndex]]) = val; break;
		case 4: *((uint32_t *) &data[offsets[colIndex]]) = val; break;
		case 8: *((uint64_t *) &data[offsets[colIndex]]) = val; break;
		default:
			idbassert(0);
			throw std::logic_error("Row::setUintField called on a non-uint field");
	}
}

inline void Row::setUintField(uint64_t val, uint colIndex)
{
	switch (getColumnWidth(colIndex)) {
		case 1: data[offsets[colIndex]] = val; break;
		case 2: *((uint16_t *) &data[offsets[colIndex]]) = val; break;
		case 4: *((uint32_t *) &data[offsets[colIndex]]) = val; break;
		case 8: *((uint64_t *) &data[offsets[colIndex]]) = val; break;
		default:
			idbassert(0);
			throw std::logic_error("Row::setUintField: bad length");
	}
}

template<int len>
inline void Row::setIntField(int64_t val, uint colIndex)
{
	switch (len) {
		case 1: *((int8_t *) &data[offsets[colIndex]]) = val; break;
		case 2: *((int16_t *) &data[offsets[colIndex]]) = val; break;
		case 4: *((int32_t *) &data[offsets[colIndex]]) = val; break;
		case 8: *((int64_t *) &data[offsets[colIndex]]) = val; break;
		default:
			idbassert(0);
			throw std::logic_error("Row::setIntField: bad length");
	}
}

inline void Row::setIntField(int64_t val, uint colIndex)
{
	switch (getColumnWidth(colIndex)) {
		case 1: *((int8_t *) &data[offsets[colIndex]]) = val; break;
		case 2: *((int16_t *) &data[offsets[colIndex]]) = val; break;
		case 4: *((int32_t *) &data[offsets[colIndex]]) = val; break;
		case 8: *((int64_t *) &data[offsets[colIndex]]) = val; break;
		default:
			idbassert(0);
			throw std::logic_error("Row::setIntField: bad length");
	}
}

inline void Row::setDoubleField(double val, uint colIndex)
{
	*((double *) &data[offsets[colIndex]]) = val;
}

inline void Row::setFloatField(float val, uint colIndex)
{
	//N.B. There is a bug in boost::any or in gcc where, if you store a nan, you will get back a nan,
	//  but not necessarily the same bits that you put in. This only seems to be for float (double seems
	//  to work).
	if (isnan(val))
		setUintField<4>(joblist::FLOATNULL, colIndex);
	else
		*((float *) &data[offsets[colIndex]]) = val;
}

inline void Row::setLongDoubleField(long double val, uint colIndex)
{
	*((long double *) &data[offsets[colIndex]]) = val;
}

inline void Row::setVarBinaryField(const std::string &val, uint colIndex)
{
	if (inStringTable(colIndex))
		setStringField(val, colIndex);
	else {
		*((uint16_t*) &data[offsets[colIndex]]) = static_cast<uint16_t>(val.length());
		memcpy(&data[offsets[colIndex] + 2], val.data(), val.length());
	}
}

inline void Row::setVarBinaryField(const uint8_t *val, uint len, uint colIndex)
{
	if (len > getColumnWidth(colIndex))
		len = getColumnWidth(colIndex);
	if (inStringTable(colIndex)) {
		uint32_t offset = strings->storeString(val, len);
		*((uint32_t *) &data[offsets[colIndex]]) = offset;
		*((uint32_t *) &data[offsets[colIndex] + 4]) = len;
	}
	else {
		*((uint16_t*) &data[offsets[colIndex]]) = len;
		memcpy(&data[offsets[colIndex] + 2], val, len);
	}
}

inline void Row::copyField(uint destIndex, uint srcIndex) const
{
	uint n = offsets[destIndex + 1] - offsets[destIndex];
	memmove(&data[offsets[destIndex]], &data[offsets[srcIndex]], n);
}

inline void Row::copyField(Row &out, uint destIndex, uint srcIndex) const
{
	if (UNLIKELY(types[srcIndex] == execplan::CalpontSystemCatalog::VARBINARY))
		out.setVarBinaryField(getVarBinaryStringField(srcIndex), destIndex);
	else if (UNLIKELY(isLongString(srcIndex)))
		out.setStringField(getStringPointer(srcIndex), getStringLength(srcIndex), destIndex);
		//out.setStringField(getStringField(srcIndex), destIndex);
	else if (UNLIKELY(isShortString(srcIndex)))
		out.setUintField(getUintField(srcIndex), destIndex);
	else if (UNLIKELY(types[srcIndex] == execplan::CalpontSystemCatalog::LONGDOUBLE))
		out.setLongDoubleField(getLongDoubleField(srcIndex), destIndex);
	else
		out.setIntField(getIntField(srcIndex), destIndex);
}

inline void Row::setRid(uint64_t rid)
{
	*((uint16_t *) data) = rid & 0xffff;
}

inline uint64_t Row::hash() const
{
	return hash(columnCount-1);
}

inline uint64_t Row::hash(const std::vector<uint> &keyCols) const
{
	utils::Hasher_r h;
	uint32_t ret = 0;
	
	for (uint i = 0; i < keyCols.size(); i++) {
		const uint &col = keyCols[i];
		if (UNLIKELY(isLongString(col)))
			ret = h((const char *) getStringPointer(col), getStringLength(col), ret);
		else {
			ret = h((const char *) &data[offsets[i]], colWidths[i], ret);
		}
	}
	ret = h.finalize(ret, keyCols.size() << 2);
	return ret;
}

inline uint64_t Row::hash(uint lastCol) const
{
	utils::Hasher_r h;
	uint32_t ret = 0;

	// Sometimes we ask this to hash 0 bytes, and it comes through looking like
	// lastCol = -1.  Return 0.
	if (lastCol >= columnCount)
		return 0;

	// Two rows that store identical values but are in different formats will return different hashes
	// if this fast path is used.  Also can't use any column offsets in this fcn.

	//if (!useStringTable) {
	//	ret = h((const char *) &data[offsets[0]], offsets[lastCol+1] - offsets[0], 0);
	//	return h.finalize(ret, offsets[lastCol+1]);
	//}
	
	for (uint i = 0; i <= lastCol; i++)
		if (UNLIKELY(isLongString(i)))
			ret = h((const char *) getStringPointer(i), getStringLength(i), ret);
		else
			ret = h((const char *) &data[offsets[i]], colWidths[i], ret);
	ret = h.finalize(ret, lastCol << 2);   // arbitary choice for the 2nd param
	return ret;
}

inline bool Row::equals(const Row &r2, const std::vector<uint> &keyCols) const
{
	for (uint i = 0; i < keyCols.size(); i++) {
		const uint &col = keyCols[i];
		if (!isLongString(col)) {
			if (getUintField(col) != r2.getUintField(col))
				return false;
		}
		else {
			if (getStringLength(col) != r2.getStringLength(col))
				return false;
			if (memcmp(getStringPointer(col), r2.getStringPointer(col), getStringLength(col)))
				return false;
		}
	}
	return true;
}

inline bool Row::equals(const Row &r2, uint lastCol) const
{
	if (lastCol >= columnCount)
		return true;

	if (!useStringTable && !r2.useStringTable)
		return !(memcmp(&data[offsets[0]], &r2.data[offsets[0]], offsets[lastCol+1] - offsets[0]));
	
	for (uint i = 0; i <= lastCol; i++)
		if (!isLongString(i)) {
			if (getUintField(i) != r2.getUintField(i))
				return false;
		}
		else {
			uint len = getStringLength(i);
			if (len != r2.getStringLength(i))
				return false;
			if (memcmp(getStringPointer(i), r2.getStringPointer(i), len))
				return false;
		}
	return true;
}

inline bool Row::equals(const Row &r2) const
{
	return equals(r2, columnCount-1);
}


/** @brief RowGroup is a lightweight interface for processing packed row data

	A RowGroup is an interface for parsing and/or modifying blocks of data formatted as described at the top of
	this file.  Its lifecycle can be tied to a producer or consumer's lifecycle.
	Only one instance is required to process any number of blocks with a
	given column configuration.  The column configuration is specified in the
	constructor, and the block data to process is specified through the
	setData() function.	 It will not copy or take ownership of the data it processes;
	the caller should do that.

	Row and RowGroup share some bits.  RowGroup owns the memory they share.
*/
class RowGroup : public messageqcpp::Serializeable
{
public:
	/** @brief The default ctor.  It does nothing.  Need to init by assignment or deserialization */
	RowGroup();

	/** @brief The RowGroup ctor, which specifies the column config to process

	@param colCount The number of columns
	@param positions An array specifying the offsets within the packed data
			of a row where each column begins.  It should have colCount + 1
			entries.  The first offset is 2, because a row begins with a 2-byte
			RID.  The last entry should be the offset of the last column +
			its length, which is also the size of the entire row including the rid.
	@param coids An array of oids for each column.
	@param tkeys An array of unique id for each column.
	@param colTypes An array of COLTYPEs for each column.
	@param scale An array specifying the scale of DECIMAL types (0 for non-decimal)
	@param precision An array specifying the precision of DECIMAL types (0 for non-decimal)
	*/

	RowGroup(uint colCount,
			const std::vector<uint> &positions,
			const std::vector<uint> &cOids,
			const std::vector<uint> &tkeys,
			const std::vector<execplan::CalpontSystemCatalog::ColDataType> &colTypes,
			const std::vector<uint> &scale,
			const std::vector<uint> &precision,
			uint stringTableThreshold,
			bool useStringTable = true,
			const std::vector<bool> &forceInlineData = std::vector<bool>()
			);

	/** @brief The copiers.  It copies metadata, not the row data */
	RowGroup(const RowGroup &);

	/** @brief Assignment operator.  It copies metadata, not the row data */
	RowGroup & operator=(const RowGroup &);

	~RowGroup();

	inline void initRow(Row *, bool forceInlineData = false) const;
	inline uint32_t getRowCount() const;
	inline void incRowCount();
	inline void setRowCount(uint32_t num);
	inline void getRow(uint rowNum, Row *) const;
	inline uint getRowSize() const;
	inline uint getRowSizeWithStrings() const;
	inline uint64_t getBaseRid() const;
	void setData(RGData *rgd);
	inline void setData(uint8_t *d);
	inline uint8_t * getData() const;
	inline RGData * getRGData() const;

	uint getStatus() const;
	void setStatus(uint16_t);

	uint getDBRoot() const;
	void setDBRoot(uint);

	uint getDataSize() const;
	uint getDataSize(uint64_t n) const;
	uint getMaxDataSize() const;
	uint getMaxDataSizeWithStrings() const;
	uint getEmptySize() const;

	// this returns the size of the row data with the string table
	inline uint64_t getSizeWithStrings() const;

	// sets the row count to 0 and the baseRid to something
	// effectively initializing whatever chunk of memory
	// data points to
	void resetRowGroup(uint64_t baseRid);

	/* The Serializeable interface */
	void serialize(messageqcpp::ByteStream&) const;
	void deserialize(messageqcpp::ByteStream&);

	uint getColumnWidth(uint col) const;
	uint getColumnCount() const;
	inline const std::vector<uint> & getOffsets() const;
	inline const std::vector<uint> & getOIDs() const;
	inline const std::vector<uint> & getKeys() const;
	inline const std::vector<uint> & getColWidths() const;
    inline execplan::CalpontSystemCatalog::ColDataType getColType(uint colIndex) const;
	inline const std::vector<execplan::CalpontSystemCatalog::ColDataType>& getColTypes() const;
	inline std::vector<execplan::CalpontSystemCatalog::ColDataType>& getColTypes();
	inline boost::shared_array<bool> &getForceInline();
	static inline uint getHeaderSize() { return headerSize; }
	
	// this returns true if the type is CHAR or VARCHAR
	inline bool isCharType(uint colIndex) const;
    inline bool isUnsigned(uint colIndex) const;
	inline bool isShortString(uint colIndex) const;
	inline bool isLongString(uint colIndex) const;

	inline const std::vector<uint> & getScale() const;
	inline const std::vector<uint> & getPrecision() const;

	inline bool usesStringTable() const;
	inline void setUseStringTable(bool);

//	RGData *convertToInlineData(uint64_t *size = NULL) const;  // caller manages the memory returned by this
//	void convertToInlineDataInPlace();
//	RGData *convertToStringTable(uint64_t *size = NULL) const;
//	void convertToStringTableInPlace();
	void serializeRGData(messageqcpp::ByteStream &) const;
	inline uint getStringTableThreshold() const;
	
	void append(RGData &);
	void append(RowGroup &);
	void append(RGData &, uint pos);   // insert starting at position 'pos'
	void append(RowGroup &, uint pos);

	RGData duplicate();   // returns a copy of the attached RGData

	std::string toString() const;

	/** operator+=
	*
	* append the metadata of another RowGroup to this RowGroup
	*/
	RowGroup& operator+=(const RowGroup& rhs);

	// returns a RowGroup with only the first cols columns.  Useful for generating a
	// RowGroup where the first cols make up a key of some kind, and the rest is irrelevant.
	RowGroup truncate(uint cols); 

	/** operator<
	 *
	 * Orders RG's based on baseRid
	 */
	inline bool operator<(const RowGroup& rhs) const;

	void addToSysDataList(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList);

	/* Base RIDs are now a combination of partition#, segment#, extent#, and block#. */
	inline void setBaseRid(const uint32_t &partNum, const uint16_t &segNum,
			const uint8_t &extentNum, const uint16_t &blockNum);
	inline void getLocation(uint32_t *partNum, uint16_t *segNum, uint8_t *extentNum,
			uint16_t *blockNum);

	inline void setStringStore(boost::shared_ptr<StringStore>);

private:
	uint columnCount;
	uint8_t *data;

	std::vector<uint> oldOffsets; //inline data offsets
	std::vector<uint> stOffsets;  //string table offsets
	uint *offsets;   //offsets either points to oldOffsets or stOffsets
	std::vector<uint> colWidths;
	// oids: the real oid of the column, may have duplicates with alias.
	// This oid is necessary for front-end to decide the real column width.
	std::vector<uint> oids;
	// keys: the unique id for pair(oid, alias). bug 1632.
	// Used to map the projected column and rowgroup index
	std::vector<uint> keys;
	std::vector<execplan::CalpontSystemCatalog::ColDataType> types;

	// DECIMAL support.  For non-decimal fields, the values are 0.
	std::vector<uint> scale;
	std::vector<uint> precision;

	// string table impl
	RGData *rgData;
	StringStore *strings;   // note, strings and data belong to rgData
	bool useStringTable;
	bool hasLongStringField;
	uint sTableThreshold;
	boost::shared_array<bool> forceInline;
	
	static const uint headerSize = 18;
	static const uint rowCountOffset = 0;
	static const uint baseRidOffset = 4;
	static const uint statusOffset = 12;
	static const uint dbRootOffset = 14;
};

inline uint64_t convertToRid(const uint32_t &partNum, const uint16_t &segNum,
		const uint8_t &extentNum, const uint16_t &blockNum);
inline void getLocationFromRid(uint64_t rid, uint32_t *partNum,
		uint16_t *segNum, uint8_t *extentNum, uint16_t *blockNum);

/** operator+
*
* add the metadata of 2 RowGroups together and return a new RowGroup
*/
RowGroup operator+(const RowGroup& lhs, const RowGroup& rhs);

boost::shared_array<int> makeMapping(const RowGroup &r1, const RowGroup &r2);
void applyMapping(const boost::shared_array<int> &mapping, const Row &in, Row *out);
void applyMapping(const std::vector<int> &mapping, const Row &in, Row *out);
void applyMapping(const int *mapping, const Row &in, Row *out);

/* PL 8/10/09: commented the asserts for now b/c for the fcns that are called
every row, they're a measurable performance penalty */
inline uint32_t RowGroup::getRowCount() const
{
// 	idbassert(data);
// 	if (!data) throw std::logic_error("RowGroup::getRowCount(): data is NULL!");
	return *((uint32_t *) &data[rowCountOffset]);
}

inline void RowGroup::incRowCount()
{
// 	idbassert(data);
	++(*((uint32_t *) &data[rowCountOffset]));
}

inline void RowGroup::setRowCount(uint32_t num)
{
// 	idbassert(data);
	*((uint32_t *) &data[rowCountOffset]) = num;
}

inline void RowGroup::getRow(uint rowNum, Row *r) const
{
// 	idbassert(data);
	if (useStringTable != r->usesStringTable())
		initRow(r);
	r->baseRid = getBaseRid();
	r->data = &(data[headerSize + (rowNum * offsets[columnCount])]);
	r->strings = strings;
}

inline void RowGroup::setData(uint8_t *d)
{
	data = d;
	strings = NULL;
	rgData = NULL;
	setUseStringTable(false);
}

inline void RowGroup::setData(RGData *rgd)
{
	data = rgd->rowData.get();
	strings = rgd->strings.get();
	rgData = rgd;
}

inline uint8_t * RowGroup::getData() const
{
	//assert(!useStringTable);
	return data;
}

inline RGData * RowGroup::getRGData() const
{
	return rgData;
}

inline void RowGroup::setUseStringTable(bool b)
{
	useStringTable = (b && hasLongStringField);
	//offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
	offsets = 0;
	if (useStringTable && !stOffsets.empty())
		offsets = &stOffsets[0];
	else if (!useStringTable && !oldOffsets.empty())
		offsets = &oldOffsets[0];
	if (!useStringTable)
		strings = NULL;
}

inline uint64_t RowGroup::getBaseRid() const
{
	return *((uint64_t *) &data[baseRidOffset]);
}

inline bool RowGroup::operator<(const RowGroup &rhs) const
{
	return (getBaseRid() < rhs.getBaseRid());
}

void RowGroup::initRow(Row *r, bool forceInlineData) const
{
	r->columnCount = columnCount;
	if (LIKELY(!types.empty())) {
    	r->colWidths = (uint *) &colWidths[0];
		r->types = (execplan::CalpontSystemCatalog::ColDataType *) &(types[0]);
		r->scale = (uint *) &(scale[0]);
		r->precision = (uint *) &(precision[0]);
	}
	if (forceInlineData) {
		r->useStringTable = false;
		r->oldOffsets = (uint *) &(oldOffsets[0]);
		r->stOffsets = (uint *) &(stOffsets[0]);
		r->offsets = (uint *) &(oldOffsets[0]);
	}
	else {
		r->useStringTable = useStringTable;
		r->oldOffsets = (uint *) &(oldOffsets[0]);
		r->stOffsets = (uint *) &(stOffsets[0]);
		r->offsets = offsets;
	}
	r->hasLongStringField = hasLongStringField;
	r->sTableThreshold = sTableThreshold;
	r->forceInline = forceInline;
}

inline uint RowGroup::getRowSize() const
{
	return offsets[columnCount];
}

inline uint RowGroup::getRowSizeWithStrings() const
{
	return oldOffsets[columnCount];
}

inline uint64_t RowGroup::getSizeWithStrings() const
{
	if (strings == NULL)
		return getDataSize();
	else
		return getDataSize() + strings->getSize();
}

inline bool RowGroup::isCharType(uint colIndex) const
{
    return execplan::isCharType(types[colIndex]);
}

inline bool RowGroup::isUnsigned(uint colIndex) const
{
    return execplan::isUnsigned(types[colIndex]);
}

inline bool RowGroup::isShortString(uint colIndex) const
{
	return ((getColumnWidth(colIndex) <= 7 && types[colIndex] == execplan::CalpontSystemCatalog::VARCHAR) ||
		(getColumnWidth(colIndex) <= 8 && types[colIndex] == execplan::CalpontSystemCatalog::CHAR));
}

inline bool RowGroup::isLongString(uint colIndex) const
{
	return ((getColumnWidth(colIndex) > 7 && types[colIndex] == execplan::CalpontSystemCatalog::VARCHAR) ||
		(getColumnWidth(colIndex) > 8 && types[colIndex] == execplan::CalpontSystemCatalog::CHAR) ||
		types[colIndex] == execplan::CalpontSystemCatalog::VARBINARY);
}

inline bool RowGroup::usesStringTable() const
{
	return useStringTable;
}

inline const std::vector<uint> & RowGroup::getOffsets() const
{
	return oldOffsets;
}

inline const std::vector<uint> & RowGroup::getOIDs() const
{
	return oids;
}

inline const std::vector<uint> & RowGroup::getKeys() const
{
	return keys;
}

inline execplan::CalpontSystemCatalog::ColDataType RowGroup::getColType(uint colIndex) const
{
	return types[colIndex];
}

inline const std::vector<execplan::CalpontSystemCatalog::ColDataType>& RowGroup::getColTypes() const
{
	return types;
}

inline std::vector<execplan::CalpontSystemCatalog::ColDataType>& RowGroup::getColTypes()
{
	return types;
}

inline const std::vector<uint> & RowGroup::getScale() const
{
	return scale;
}

inline const std::vector<uint> & RowGroup::getPrecision() const
{
	return precision;
}

inline const std::vector<uint> & RowGroup::getColWidths() const
{
	return colWidths;
}

inline boost::shared_array<bool> & RowGroup::getForceInline()
{
	return forceInline;
}

// These type defs look stupid at first, yes.  I want to see compiler errors
// if/when we change the widths of these parms b/c this fcn will have to be
// reevaluated.
inline uint64_t convertToRid(const uint32_t &partitionNum,
		const uint16_t &segmentNum, const uint8_t &exNum, const uint16_t &blNum)
{
	uint64_t partNum = partitionNum, segNum = segmentNum, extentNum = exNum,
			blockNum = blNum;

	// extentNum gets trunc'd to 6 bits, blockNums to 10 bits
	extentNum &= 0x3f;
	blockNum &= 0x3ff;

	return (partNum << 32) | (segNum << 16) | (extentNum << 10) | blockNum;
}

inline void RowGroup::setBaseRid(const uint32_t &partNum, const uint16_t &segNum,
		const uint8_t &extentNum, const uint16_t &blockNum)
{
	*((uint64_t *) &data[baseRidOffset]) = convertToRid(partNum, segNum,
			extentNum, blockNum);
}

inline uint RowGroup::getStringTableThreshold() const 
{
	return sTableThreshold;
}

inline void RowGroup::setStringStore(boost::shared_ptr<StringStore> ss)
{
	if (useStringTable) {
		rgData->setStringStore(ss);
		strings = rgData->strings.get();
	}
}

inline void getLocationFromRid(uint64_t rid, uint32_t *partNum,
		uint16_t *segNum, uint8_t *extentNum, uint16_t *blockNum)
{
	if (partNum) *partNum = rid >> 32;
	if (segNum) *segNum = rid >> 16;
	if (extentNum) *extentNum = (rid >> 10) & 0x3f;
	if (blockNum) *blockNum = rid & 0x3ff;
}

inline void RowGroup::getLocation(uint32_t *partNum, uint16_t *segNum,
		uint8_t *extentNum, uint16_t *blockNum)
{
	getLocationFromRid(getBaseRid(), partNum, segNum, extentNum, blockNum);
}

inline uint64_t Row::getExtentRelativeRid() const
{
	uint64_t blockNum = baseRid & 0x3ff;
	return (blockNum << 13) | (getRelRid() & 0x1fff);
}

inline uint64_t Row::getFileRelativeRid() const
{
	uint64_t extentNum = (baseRid >> 10) & 0x3f;
	uint64_t blockNum = baseRid & 0x3ff;
	return (extentNum << 23) | (blockNum << 13) | (getRelRid() & 0x1fff);
}

inline void Row::getLocation(uint32_t *partNum, uint16_t *segNum, uint8_t *extentNum,
				uint16_t *blockNum, uint16_t *rowNum)
{
	getLocationFromRid(baseRid, partNum, segNum, extentNum, blockNum);
	if (rowNum) *rowNum = getRelRid();
}

inline void copyRow(const Row &in, Row *out, uint colCount)
{
	if (&in == out)
		return;

	out->setRid(in.getRelRid());
	if (!in.usesStringTable() && !out->usesStringTable()) {
		memcpy(out->getData(), in.getData(), std::min(in.getOffset(colCount), out->getOffset(colCount)));
		return;
	}

	for (uint i = 0; i < colCount; i++) {
		if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::VARBINARY))
			out->setVarBinaryField(in.getVarBinaryStringField(i), i);
		else if (UNLIKELY(in.isLongString(i)))
			//out->setStringField(in.getStringField(i), i);
			out->setStringField(in.getStringPointer(i), in.getStringLength(i), i);
		else if (UNLIKELY(in.isShortString(i)))
			out->setUintField(in.getUintField(i), i);
		else if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::LONGDOUBLE))
			out->setLongDoubleField(in.getLongDoubleField(i), i);
		else
			out->setIntField(in.getIntField(i), i);
	}
}

inline void copyRow(const Row &in, Row *out)
{
	copyRow(in, out, std::min(in.getColumnCount(), out->getColumnCount()));
}

inline std::string StringStore::getString(uint32_t off, uint32_t len) const
{
	if (off == std::numeric_limits<uint32_t>::max())
		return joblist::CPNULLSTRMARK;

	MemChunk *mc;
	uint chunk = off / CHUNK_SIZE;
	uint offset = off % CHUNK_SIZE;
	// this has to handle uninitialized data as well.  If it's uninitialized it doesn't matter
	// what gets returned, it just can't go out of bounds.
	if (mem.size() <= chunk)
		return joblist::CPNULLSTRMARK;
	mc = (MemChunk *) mem[chunk].get();
	if ((offset + len) > mc->currentSize)
		return joblist::CPNULLSTRMARK;
	return std::string((char *) &(mc->data[offset]), len);
}

inline const uint8_t * StringStore::getPointer(uint32_t off) const
{
	if (off == std::numeric_limits<uint32_t>::max())
		return (const uint8_t *) joblist::CPNULLSTRMARK.c_str();

	uint chunk = off / CHUNK_SIZE;
	uint offset = off % CHUNK_SIZE;
	MemChunk *mc;
	// this has to handle uninitialized data as well.  If it's uninitialized it doesn't matter
	// what gets returned, it just can't go out of bounds.
	if (UNLIKELY(mem.size() <= chunk))
		return (const uint8_t *) joblist::CPNULLSTRMARK.c_str();
	mc = (MemChunk *) mem[chunk].get();
	if (offset > mc->currentSize)
		return (const uint8_t *) joblist::CPNULLSTRMARK.c_str();
	return &(mc->data[offset]);
}

inline bool StringStore::isNullValue(uint32_t off, uint32_t len) const
{
	if (off == std::numeric_limits<uint32_t>::max() || len == 0)
		return true;

	if (len < 8)
		return false;
	
	uint chunk = off / CHUNK_SIZE;
	uint offset = off % CHUNK_SIZE;
	MemChunk *mc;
	if (mem.size() <=  chunk)
		return true;
	mc = (MemChunk *) mem[chunk].get();
	if ((offset + len) > mc->currentSize)
		return true;
	if (mc->data[offset] == 0)    // "" = NULL string for some reason...
		return true;
	return (*((uint64_t *) &mc->data[offset]) == *((uint64_t *) joblist::CPNULLSTRMARK.c_str()));
}

inline bool StringStore::equals(const std::string &str, uint32_t off, uint32_t len) const
{
	if (off == std::numeric_limits<uint32_t>::max() || len == 0)
		return str == joblist::CPNULLSTRMARK;

	uint chunk = off / CHUNK_SIZE;
	uint offset = off % CHUNK_SIZE;
	if (mem.size() <=  chunk)
		return false;
	MemChunk *mc = (MemChunk *) mem[chunk].get();
	if ((offset + len) > mc->currentSize)
		return false;

	return (strncmp(str.c_str(), (const char *) &mc->data[offset], len) == 0);
}

inline bool StringStore::isEmpty() const
{
	return empty;
}

inline uint64_t StringStore::getSize() const
{
	uint i;
	uint64_t ret = 0;
	MemChunk *mc;

	for (i = 0; i < mem.size(); i++) {
		mc = (MemChunk *) mem[i].get();
		ret += mc->capacity;
	}
	return ret;
}

inline RGData & RGData::operator=(const RGData &r)
{
	rowData = r.rowData;
	strings = r.strings;
	return *this;
}

inline void RGData::getRow(uint num, Row *row)
{
	uint size = row->getSize();
	row->setData(Row::Pointer(&rowData[RowGroup::getHeaderSize() + (num * size)], strings.get()));
}

}

#endif
// vim:ts=4 sw=4:

