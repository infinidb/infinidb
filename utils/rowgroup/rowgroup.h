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

//  $Id: rowgroup.h 3845 2013-05-31 21:08:53Z rdempsey $

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

#if defined(_MSC_VER) && !defined(isnan)
#define isnan _isnan
#endif

#include "joblisttypes.h"
#include "bytestream.h"
#include "calpontsystemcatalog.h"
#include "exceptclasses.h"

/* branch prediction macros for gcc.  Is there a better place for them? */
#if !defined(__GNUC__) || (__GNUC__ == 2 && __GNUC_MINOR__ < 96)
#ifndef __builtin_expect
#define __builtin_expect(x, expected_value) (x)
#endif
#endif

#ifndef LIKELY
#define LIKELY(x)   __builtin_expect((x),1)
#define UNLIKELY(x) __builtin_expect((x),0)
#endif

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

class Row
{
	public:
		Row();
		Row(const Row &);
		~Row();

		Row & operator=(const Row &);

		inline void setData(uint8_t *rowData);
		inline uint8_t * getData() const;
		inline void nextRow();
		inline uint getColumnWidth(uint colIndex) const;
		inline uint getColumnCount() const;
		inline uint getSize() const;
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
		inline void setStringField(const std::string &val, uint colIndex);

		// support VARBINARY
		// Add 2-byte length at the beginning of the field.  NULL and zero length field are
		// treated the same, could use one of the length bit to distinguish these two cases.
		inline std::string getVarBinaryStringField(uint colIndex) const;
		inline void setVarBinaryField(const std::string &val, uint colIndex);
		// No string construction is necessary for better performance.
		inline uint getVarBinaryLength(uint colIndex) const;
		inline uint8_t* getVarBinaryField(uint colIndex) const;
		inline uint8_t* getVarBinaryField(uint& len, uint colIndex) const;
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
		inline void copyField(uint8_t* destAddr, uint srcIndex) const;

		std::string toString() const;
		std::string toCSV() const;

		/* These fcns are used only in joins.  The RID doesn't matter on the side that
		gets hashed.  We steal that field here to "mark" a row. */
		inline void markRow();
		inline void zeroRid();
		inline bool isMarked();
		void initToNull();

	private:
		uint columnCount;
		uint64_t baseRid;

		// the next 5 point to memory owned by RowGroup
		uint *offsets;
		execplan::CalpontSystemCatalog::ColDataType *types;
		uint8_t *data;
		uint *scale;
		uint *precision;

		friend class RowGroup;
};

inline void Row::setData(uint8_t *rowData) { data = rowData; }

inline uint8_t * Row::getData() const { return data; }

inline void Row::nextRow() { data += offsets[columnCount]; }

inline uint Row::getColumnCount() const { return columnCount; }

inline uint Row::getColumnWidth(uint col) const
{
	return offsets[col+1] - offsets[col];
}

inline uint Row::getSize() const
{
	return offsets[columnCount];
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
	return (strncmp(val.c_str(), (char *) &data[offsets[colIndex]], offsets[colIndex + 1] - offsets[colIndex]) == 0);
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

inline std::string Row::getStringField(uint colIndex) const
{
	return std::string((char *) &data[offsets[colIndex]], offsets[colIndex + 1] - offsets[colIndex]);
}

inline std::string Row::getVarBinaryStringField(uint colIndex) const
{
	return std::string((char*) &data[offsets[colIndex]+2], *((uint16_t*) &data[offsets[colIndex]]));
}

inline uint Row::getVarBinaryLength(uint colIndex) const
{
	return *((uint16_t*) &data[offsets[colIndex]]);
}

inline uint8_t* Row::getVarBinaryField(uint colIndex) const
{
	return &data[offsets[colIndex] + 2];
}

inline uint8_t* Row::getVarBinaryField(uint& len, uint colIndex) const
{
	len = *((uint16_t*) &data[offsets[colIndex]]);
	return &data[offsets[colIndex] + 2];
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

inline void Row::setStringField(const std::string &val, uint colIndex)
{
	uint64_t fieldWidth = offsets[colIndex + 1] - offsets[colIndex];
	uint64_t len = (fieldWidth > val.length()) ? val.length() : fieldWidth;
	memcpy(&data[offsets[colIndex]], val.c_str(), len);
	if (fieldWidth > val.length())
		memset(&data[offsets[colIndex] + len], 0, fieldWidth - len);
}

inline void Row::setVarBinaryField(const std::string &val, uint colIndex)
{
	*((uint16_t*) &data[offsets[colIndex]]) = static_cast<uint16_t>(val.length());
	memcpy(&data[offsets[colIndex] + 2], val.c_str(), val.length());
//	memset(&data[offsets[colIndex] + 2 + val.length()], 0,
//		offsets[colIndex + 1] - (offsets[colIndex] + 2 + val.length()));
}

inline void Row::setVarBinaryField(const uint8_t* val, uint len, uint colIndex)
{
	*((uint16_t*) &data[offsets[colIndex]]) = len;
	memcpy(&data[offsets[colIndex] + 2], val, len);
//	memset(&data[offsets[colIndex] + 2 + len], 0,
//		offsets[colIndex + 1] - (offsets[colIndex] + 2 + len));
}

inline void Row::copyField(uint destIndex, uint srcIndex) const
{
	uint n = offsets[destIndex + 1] - offsets[destIndex];
	memmove(&data[offsets[destIndex]], &data[offsets[srcIndex]], n);
}

inline void Row::copyField(uint8_t* destAddr, uint srcIndex) const
{
	uint n = offsets[srcIndex + 1] - offsets[srcIndex];
	memmove(destAddr, &data[offsets[srcIndex]], n);
}

inline void Row::setRid(uint64_t rid)
{
	*((uint16_t *) data) = rid & 0x1fff;
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
			const std::vector<uint> &precision);

	/** @brief The copiers.  It copies metadata, not the row data */
	RowGroup(const RowGroup &);

	/** @brief Assignment operator.  It copies metadata, not the row data */
	RowGroup & operator=(const RowGroup &);

	~RowGroup();

	inline void initRow(Row *) const;
	inline uint32_t getRowCount() const;
	inline void incRowCount();
	inline void setRowCount(uint32_t num);
	inline void getRow(uint rowNum, Row *) const;
	inline uint getRowSize() const;
	inline uint64_t getBaseRid() const;
	inline void setData(uint8_t *rgData);
	inline uint8_t * getData() const;

	uint getStatus() const;
	void setStatus(uint16_t);

	uint getDBRoot() const;
	void setDBRoot(uint);

	uint getDataSize() const;
	uint getDataSize(uint64_t n) const;
	uint getMaxDataSize() const;
	uint getEmptySize() const;

	// sets the row count to 0 and the baseRid to something
	// effectively initializing whatever chunk of memory
	// data points to
	void resetRowGroup(uint64_t baseRid);

	/* The Serializeable interface */
	void serialize(messageqcpp::ByteStream&) const;
	void deserialize(messageqcpp::ByteStream&);

	uint getColumnWidth(uint col) const;
	uint getColumnCount() const;
	const std::vector<uint> & getOffsets() const;
	const std::vector<uint> & getOIDs() const;
	const std::vector<uint> & getKeys() const;
    inline execplan::CalpontSystemCatalog::ColDataType getColType(uint colIndex) const;
	const std::vector<execplan::CalpontSystemCatalog::ColDataType>& getColTypes() const;
	std::vector<execplan::CalpontSystemCatalog::ColDataType>& getColTypes();

	// this returns true if the type is not CHAR or VARCHAR
	inline bool isCharType(uint colIndex) const;
    inline bool isUnsigned(uint colIndex) const;
	inline bool isShortString(uint colIndex) const;
	inline bool isLongString(uint colIndex) const;

	const std::vector<uint> & getScale() const;
	const std::vector<uint> & getPrecision() const;

	std::string toString() const;

	/** operator+=
	*
	* append the metadata of another RowGroup to this RowGroup
	*/
	RowGroup& operator+=(const RowGroup& rhs);

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

private:
	uint columnCount;
	uint8_t *data;

	std::vector<uint> offsets;
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
	r->baseRid = getBaseRid();
	r->data = &(data[headerSize + (rowNum * offsets[columnCount])]);
}

inline void RowGroup::setData(uint8_t *rgData)
{
	data = rgData;
}

inline uint8_t * RowGroup::getData() const
{
	return data;
}

inline uint64_t RowGroup::getBaseRid() const
{
	return *((uint64_t *) &data[baseRidOffset]);
}

inline bool RowGroup::operator<(const RowGroup &rhs) const
{
	return (getBaseRid() < rhs.getBaseRid());
}

inline void RowGroup::initRow(Row *r) const
{
	r->columnCount = columnCount;
	r->offsets = (uint *) &(offsets[0]);
	// @bug 3756
	//For some queries (e.g. select group_concat('hello') ... these vectors are not used, so don't reference them
	if (LIKELY(!types.empty()))
	{
		r->types = (execplan::CalpontSystemCatalog::ColDataType *) &(types[0]);
		r->scale = (uint *) &(scale[0]);
		r->precision = (uint *) &(precision[0]);
	}
}

inline uint RowGroup::getRowSize() const
{
	return offsets[columnCount];
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
	return (getColumnWidth(colIndex) <= 8 && isCharType(colIndex));
}

inline bool RowGroup::isLongString(uint colIndex) const
{
	return (getColumnWidth(colIndex) > 8 && isCharType(colIndex));
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



}

#endif
// vim:ts=4 sw=4:

