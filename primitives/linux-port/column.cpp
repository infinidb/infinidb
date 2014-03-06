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

/*****************************************************************************
 * $Id: column.cpp 2103 2013-06-04 17:53:38Z dcathey $
 *
 ****************************************************************************/
#include <iostream>
#include <sstream>
//#define NDEBUG
#include <cassert>
#include <cmath>
#ifndef _MSC_VER
#include <pthread.h>
#else
#endif
using namespace std;

#include <boost/scoped_array.hpp>
using namespace boost;

#include "primitiveprocessor.h"
#include "messagelog.h"
#include "messageobj.h"
#include "we_type.h"
#include "stats.h"
#include "primproc.h"
using namespace logging;
using namespace dbbc;
using namespace primitives;
using namespace primitiveprocessor;
using namespace execplan;

namespace
{

inline uint64_t order_swap(uint64_t x)
{
    uint64_t ret = (x>>56) |
        ((x<<40) & 0x00FF000000000000ULL) |
        ((x<<24) & 0x0000FF0000000000ULL) |
        ((x<<8)  & 0x000000FF00000000ULL) |
        ((x>>8)  & 0x00000000FF000000ULL) |
        ((x>>24) & 0x0000000000FF0000ULL) |
        ((x>>40) & 0x000000000000FF00ULL) |
        (x<<56);
	return ret;
}

template <int W>
inline string fixChar(int64_t intval);

idb_regex_t placeholderRegex;

template <class T>
inline int  compareBlock(  const void * a, const void * b )
{
      return ( (*(T*)a) - (*(T*)b) );
}

//this function is out-of-band, we don't need to inline it
void logIt(int mid, int arg1, const string& arg2=string())
{
	MessageLog logger(LoggingID(28));
	logging::Message::Args args;
	Message msg(mid);

	args.add(arg1);
	if (arg2.length() > 0)
		args.add(arg2);
	msg.format(args);
	logger.logErrorMessage(msg);
}

//FIXME: what are we trying to accomplish here? It looks like we just want to count
// the chars in a string arg?
PrimitiveProcessor::p_DataValue convertToPDataValue(const void* val, int W)
{
	PrimitiveProcessor::p_DataValue dv;
	string str;
	if (8 == W)
		str = fixChar<8>(*reinterpret_cast<const int64_t*>(val));
	else
		str = reinterpret_cast<const char*>(val);
	dv.len = static_cast<int>(str.length());
	dv.data = reinterpret_cast<const uint8_t*>(val);
	return dv;
}


template<class T>
inline bool colCompare_(const T& val1, const T& val2, uint8_t COP)
{
	switch(COP) {
		case COMPARE_NIL:
			return false;
		case COMPARE_LT:
			return val1 < val2;
		case COMPARE_EQ:
			return val1 == val2;
		case COMPARE_LE:
			return val1 <= val2;
		case COMPARE_GT:
			return val1 > val2;
		case COMPARE_NE:
			return val1 != val2;
		case COMPARE_GE:
			return val1 >= val2;
		default:
			logIt(34, COP, "colCompare");
			return false;						// throw an exception here?
	}
}

template<class T>
inline bool colCompare_(const T& val1, const T& val2, uint8_t COP, uint8_t rf)
{
	switch(COP) {
		case COMPARE_NIL:
			return false;
		case COMPARE_LT:
			return val1 < val2 || (val1 == val2 && (rf & 0x01));
		case COMPARE_LE:
			return val1 < val2 || (val1 == val2 && rf ^ 0x80);
		case COMPARE_EQ:
			return val1 == val2 && rf == 0;
		case COMPARE_NE:
			return val1 != val2 || rf != 0;
		case COMPARE_GE:
			return val1 > val2 || (val1 == val2 && rf ^ 0x01);
		case COMPARE_GT:
			return val1 > val2 || (val1 == val2 && (rf & 0x80));
		default:
			logIt(34, COP, "colCompare_l");
			return false;						// throw an exception here?
	}
}

bool isLike(const char *val, const idb_regex_t *regex)
{
	if (!regex)
		throw runtime_error("PrimitiveProcessor::isLike: Missing regular expression for LIKE operator");

#ifdef POSIX_REGEX
	return (regexec(&regex->regex, val, 0, NULL, 0) == 0);
#else
	return regex_match(val, regex->regex);
#endif
}

//@bug 1828  Like must be a string compare.
inline bool colStrCompare_(uint64_t val1, uint64_t val2, uint8_t COP, uint8_t rf, const idb_regex_t* regex)
{
	switch(COP) {
		case COMPARE_NIL:
			return false;
		case COMPARE_LT:
			return val1 < val2 || (val1 == val2 && rf != 0);
		case COMPARE_LE:
			return val1 <= val2;
		case COMPARE_EQ:
			return val1 == val2 && rf == 0;
		case COMPARE_NE:
			return val1 != val2 || rf != 0;
		case COMPARE_GE:
			return val1 > val2 || (val1 == val2 && rf == 0);
		case COMPARE_GT:
			return val1 > val2;
		case COMPARE_LIKE:
		case COMPARE_NLIKE: {
			/* LIKE comparisons are string comparisons so we reverse the order again.
				Switching the order twice is probably as efficient as evaluating a guard.  */
			char tmp[9];
			val1 = order_swap(val1);
			memcpy(tmp, &val1, 8);
			tmp[8] = '\0';
			return (COP & COMPARE_NOT ? !isLike(tmp, regex) : isLike(tmp, regex));
		}
		default:
			logIt(34, COP, "colCompare_l");
			return false;						// throw an exception here?
	}
}

#if 0
inline bool colStrCompare_(uint64_t val1, uint64_t val2, uint8_t COP, const idb_regex_t* regex)
{
	switch(COP) {
		case COMPARE_NIL:
			return false;
		case COMPARE_LT:
			return val1 < val2;
		case COMPARE_LE:
			return val1 <= val2;
		case COMPARE_EQ:
			return val1 == val2;
		case COMPARE_NE:
			return val1 != val2;
		case COMPARE_GE:
			return val1 >= val2;
		case COMPARE_GT:
			return val1 > val2;
		case COMPARE_LIKE:
		case COMPARE_NOT | COMPARE_LIKE: {
			/* LIKE comparisons are string comparisons so we reverse the order again.
				Switching the order twice is probably as efficient as evaluating a guard.  */
			char tmp[9];
			val1 = order_swap(val1);
			memcpy(tmp, &val1, 8);
			tmp[8] = '\0';
			return (COP & COMPARE_NOT ? !isLike(tmp, regex) : isLike(tmp, regex));
		}
		default:
			logIt(34, COP, "colCompare");
			return false;						// throw an exception here?
	}
}
#endif

template<int>
inline bool isEmptyVal(uint8_t type, const uint8_t* val8);

template<>
inline bool isEmptyVal<8>(uint8_t type, const uint8_t* ival)
{
	const uint64_t* val = reinterpret_cast<const uint64_t*>(ival);

	switch (type)
	{
	case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE:
		return (joblist::DOUBLEEMPTYROW == *val);
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::DATE:
	case CalpontSystemCatalog::DATETIME:
	case CalpontSystemCatalog::VARBINARY:
		return (*val == joblist::CHAR8EMPTYROW);
    case CalpontSystemCatalog::UBIGINT:
        return (joblist::UBIGINTEMPTYROW == *val);
	default:
		break;
	}

	return (joblist::BIGINTEMPTYROW == *val);
}

template<>
inline bool isEmptyVal<4>(uint8_t type, const uint8_t* ival)
{
	const uint32_t* val = reinterpret_cast<const uint32_t*>(ival);

	switch (type)
	{
	case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT:
		return (joblist::FLOATEMPTYROW == *val);
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::DATE:
	case CalpontSystemCatalog::DATETIME:
		return (joblist::CHAR4EMPTYROW == *val);
    case CalpontSystemCatalog::UINT:
        return (joblist::UINTEMPTYROW == *val);
	default:
		break;
	}

	return (joblist::INTEMPTYROW == *val);
}

template<>
inline bool isEmptyVal<2>(uint8_t type, const uint8_t* ival)
{
	const uint16_t* val = reinterpret_cast<const uint16_t*>(ival);

	switch (type)
	{
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::DATE:
	case CalpontSystemCatalog::DATETIME:
		return (joblist::CHAR2EMPTYROW == *val);
    case CalpontSystemCatalog::USMALLINT:
        return (joblist::USMALLINTEMPTYROW == *val);
	default:
		break;
	}

	return (joblist::SMALLINTEMPTYROW == *val);
}

template<>
inline bool isEmptyVal<1>(uint8_t type, const uint8_t* ival)
{
	const uint8_t* val = reinterpret_cast<const uint8_t*>(ival);

	switch (type)
	{
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::DATE:
	case CalpontSystemCatalog::DATETIME:
		return (*val == joblist::CHAR1EMPTYROW);
    case CalpontSystemCatalog::UTINYINT:
        return (*val == joblist::UTINYINTEMPTYROW);
	default:
		break;
	}

	return (*val == joblist::TINYINTEMPTYROW);
}

template<int>
inline bool isNullVal(uint8_t type, const uint8_t* val8);

template<>
inline bool isNullVal<8>(uint8_t type, const uint8_t* ival)
{
	const uint64_t* val = reinterpret_cast<const uint64_t*>(ival);

	switch (type)
	{
	case CalpontSystemCatalog::DOUBLE:
	case CalpontSystemCatalog::UDOUBLE:
		return (joblist::DOUBLENULL == *val);
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::DATE:
	case CalpontSystemCatalog::DATETIME:
	case CalpontSystemCatalog::VARBINARY:
		//@bug 339 might be a token here
		//TODO: what's up with the second const here?
		return (*val == joblist::CHAR8NULL || 0xFFFFFFFFFFFFFFFELL == *val);
    case CalpontSystemCatalog::UBIGINT:
        return (joblist::UBIGINTNULL == *val);
	default:
		break;
	}

	return (joblist::BIGINTNULL == *val);
}

template<>
inline bool isNullVal<4>(uint8_t type, const uint8_t* ival)
{
	const uint32_t* val = reinterpret_cast<const uint32_t*>(ival);

	switch (type)
	{
	case CalpontSystemCatalog::FLOAT:
	case CalpontSystemCatalog::UFLOAT:
		return (joblist::FLOATNULL == *val);
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
		return (joblist::CHAR4NULL == *val);
	case CalpontSystemCatalog::DATE:
	case CalpontSystemCatalog::DATETIME:
		return (joblist::DATENULL == *val);
    case CalpontSystemCatalog::UINT:
        return (joblist::UINTNULL == *val);
	default:
		break;
	}

	return (joblist::INTNULL == *val);
}

template<>
inline bool isNullVal<2>(uint8_t type, const uint8_t* ival)
{
	const uint16_t* val = reinterpret_cast<const uint16_t*>(ival);

	switch (type)
	{
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::DATE:
	case CalpontSystemCatalog::DATETIME:
		return (joblist::CHAR2NULL == *val);
    case CalpontSystemCatalog::USMALLINT:
        return (joblist::USMALLINTNULL == *val);
	default:
		break;
	}

	return (joblist::SMALLINTNULL == *val);
}

template<>
inline bool isNullVal<1>(uint8_t type, const uint8_t* ival)
{
	const uint8_t* val = reinterpret_cast<const uint8_t*>(ival);

	switch (type)
	{
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::DATE:
	case CalpontSystemCatalog::DATETIME:
		return (*val == joblist::CHAR1NULL);
    case CalpontSystemCatalog::UTINYINT:
        return (joblist::UTINYINTNULL == *val);
	default:
		break;
	}

	return (*val == joblist::TINYINTNULL);
}

/* A generic isNullVal */
inline bool isNullVal(uint32_t length, uint8_t type, const uint8_t *val8)
{
	switch (length) {
		case 8: return isNullVal<8>(type, val8);
		case 4: return isNullVal<4>(type, val8);
		case 2: return isNullVal<2>(type, val8);
		case 1: return isNullVal<1>(type, val8);
	};
	return false;
}

// Set the minimum and maximum in the return header if we will be doing a block scan and
// we are dealing with a type that is comparable as a 64 bit integer.  Subsequent calls can then
// skip this block if the value being searched is outside of the Min/Max range.
inline bool isMinMaxValid(const NewColRequestHeader *in) {
	if (in->NVALS != 0) {
		return false;
	}
	else {
		switch (in->DataType)
		{
		case CalpontSystemCatalog::CHAR:
			return (in->DataSize<9);
		case CalpontSystemCatalog::VARCHAR:
			return (in->DataSize<8);
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::DATE:
		case CalpontSystemCatalog::BIGINT:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::UTINYINT:
        case CalpontSystemCatalog::USMALLINT:
        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UBIGINT:
			return true;
		case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
			return (in->DataSize <= 8);
		default:
			return false;
		}
	}
}

//char(8) values lose their null terminator
template <int W>
inline string fixChar(int64_t intval)
{
	char chval[W + 1];	
	memcpy(chval, &intval, W);
	chval[W] = '\0';

	return string(chval);
}

inline bool colCompare(int64_t val1, int64_t val2, uint8_t COP, uint8_t rf, int type, uint8_t width, const idb_regex_t& regex, bool isNull=false)
{
// 	cout << "comparing " << hex << val1 << " to " << val2 << endl;

	if (COMPARE_NIL == COP) return false;

	//@bug 425 added isNull condition
	else if ( !isNull && (type == CalpontSystemCatalog::FLOAT || type == CalpontSystemCatalog::DOUBLE)) {
		double dVal1, dVal2;
		if (type == CalpontSystemCatalog::FLOAT)
		{
		   dVal1 = *((float *) &val1);
		   dVal2 = *((float *) &val2);
		}
		else
		{
		   dVal1 = *((double *) &val1);
		   dVal2 = *((double *) &val2);
		}
		return colCompare_(dVal1, dVal2, COP);
	}

	else if ( (type == CalpontSystemCatalog::CHAR || type == CalpontSystemCatalog::VARCHAR) && !isNull )
	{
		if (!regex.used && !rf)
			return colCompare_(order_swap(val1), order_swap(val2), COP);
		else
			return colStrCompare_(order_swap(val1), order_swap(val2), COP, rf, &regex);
	}

	/* isNullVal should work on the normalized value on little endian machines */
	else {
		bool val2Null = isNullVal(width, type, (uint8_t *) &val2);
		if (isNull == val2Null || (val2Null && COP == COMPARE_NE))
			return colCompare_(val1, val2, COP, rf);
		else
			return false;
	}
}

inline bool colCompareUnsigned(uint64_t val1, uint64_t val2, uint8_t COP, uint8_t rf, int type, uint8_t width, const idb_regex_t& regex, bool isNull=false)
{
// 	cout << "comparing unsigned" << hex << val1 << " to " << val2 << endl;

	if (COMPARE_NIL == COP) return false;

	/* isNullVal should work on the normalized value on little endian machines */
    bool val2Null = isNullVal(width, type, (uint8_t *) &val2);
    if (isNull == val2Null || (val2Null && COP == COMPARE_NE))
        return colCompare_(val1, val2, COP, rf);
    else
        return false;
}

inline void store(const NewColRequestHeader *in,
	NewColResultHeader *out,
	unsigned outSize,
	unsigned *written,
	uint16_t rid, const uint8_t *block8)
{
	uint8_t* out8 = reinterpret_cast<uint8_t*>(out);

	if (in->OutputType & OT_RID) {
#ifdef PRIM_DEBUG
		if (*written + 2 > outSize) {
			logIt(35, 1);
			throw logic_error("PrimitiveProcessor::store(): output buffer is too small");
		}
#endif
		out->RidFlags |= (1 << (rid >> 10)); // set the (row/1024)'th bit
 		memcpy(&out8[*written], &rid, 2);
		*written += 2;
	}

	if (in->OutputType & OT_TOKEN || in->OutputType & OT_DATAVALUE) {
#ifdef PRIM_DEBUG
		if (*written + in->DataSize > outSize) {
			logIt(35, 2);
			throw logic_error("PrimitiveProcessor::store(): output buffer is too small");
		}
#endif

		void* ptr1 = &out8[*written];
		const uint8_t* ptr2 = &block8[0];
		switch (in->DataSize)
		{
		default:
		case 8:
			ptr2 += (rid << 3);
			memcpy(ptr1, ptr2, 8);
			break;
		case 4:
			ptr2 += (rid << 2);
			memcpy(ptr1, ptr2, 4);
			break;
		case 2:
			ptr2 += (rid << 1);
			memcpy(ptr1, ptr2, 2);
			break;
		case 1:
			ptr2 += (rid << 0);
			memcpy(ptr1, ptr2, 1);
			break;
		}
		*written += in->DataSize;
	}
	out->NVALS++;
}

template<int W>
inline uint64_t nextUnsignedColValue(int type,
				const uint16_t *ridArray,
				int NVALS,
				int *index,
				bool *done,
				bool *isNull,
				bool *isEmpty,
				uint16_t *rid,
				uint8_t OutputType, uint8_t *val8, unsigned itemsPerBlk)
{
    const uint8_t* vp = 0;
    if (ridArray == NULL) {
        while (static_cast<unsigned>(*index) < itemsPerBlk &&
            isEmptyVal<W>(type, &val8[*index*W]) &&
            (OutputType & OT_RID))
        {
            (*index)++;
        }

        if (static_cast<unsigned>(*index) >= itemsPerBlk) {
            *done = true;
            return 0;
        }

        vp = &val8[*index*W];
        *isNull = isNullVal<W>(type, vp);
        *isEmpty = isEmptyVal<W>(type, vp);
        *rid = (*index)++;
    }
    else {
        while (*index < NVALS &&
            isEmptyVal<W>(type, &val8[ridArray[*index] * W]))
        {
            (*index)++;
        }

        if (*index >= NVALS) {
            *done = true;
            return 0;
        }

        vp = &val8[ridArray[*index] * W];
        *isNull = isNullVal<W>(type, vp);
        *isEmpty = isEmptyVal<W>(type, vp);
        *rid = ridArray[(*index)++];
    }
    // at this point, nextRid is the index to return, and index is...
    //   if RIDs are not specified, nextRid + 1,
    //	 if RIDs are specified, it's the next index in the rid array.
    //Bug 838, tinyint null problem
    switch (W)
    {
    case 1:
        return reinterpret_cast<uint8_t *> (val8)[*rid];
    case 2:
        return reinterpret_cast<uint16_t *>(val8)[*rid];
    case 4:
        return reinterpret_cast<uint32_t *>(val8)[*rid];
    case 8:
        return reinterpret_cast<uint64_t *>(val8)[*rid];
    default:
        logIt(33, W);

#ifdef PRIM_DEBUG
        throw logic_error("PrimitiveProcessor::nextColValue() bad width");
#endif
        return -1;
    }
}

template<int W>
inline int64_t nextColValue(int type,
				const uint16_t *ridArray,
				int NVALS,
				int *index,
				bool *done,
				bool *isNull,
				bool *isEmpty,
				uint16_t *rid,
				uint8_t OutputType, uint8_t *val8, unsigned itemsPerBlk)
{
    const uint8_t* vp = 0;
    if (ridArray == NULL) 
    {
        while (static_cast<unsigned>(*index) < itemsPerBlk &&
            isEmptyVal<W>(type, &val8[*index*W]) &&
            (OutputType & OT_RID))
        {
            (*index)++;
        }

        if (static_cast<unsigned>(*index) >= itemsPerBlk) {
            *done = true;
            return 0;
        }

        vp = &val8[*index*W];
        *isNull = isNullVal<W>(type, vp);
        *isEmpty = isEmptyVal<W>(type, vp);
        *rid = (*index)++;
    }
    else 
    {
        while (*index < NVALS &&
            isEmptyVal<W>(type, &val8[ridArray[*index] * W]))
        {
            (*index)++;
        }

        if (*index >= NVALS) 
        {
            *done = true;
            return 0;
        }

        vp = &val8[ridArray[*index] * W];
        *isNull = isNullVal<W>(type, vp);
        *isEmpty = isEmptyVal<W>(type, vp);
        *rid = ridArray[(*index)++];
    }

	// at this point, nextRid is the index to return, and index is...
	//   if RIDs are not specified, nextRid + 1,
	//	 if RIDs are specified, it's the next index in the rid array.
	//Bug 838, tinyint null problem
	switch (W)
	{
	case 1:
		return reinterpret_cast<int8_t *> (val8)[*rid];
	case 2:
		return reinterpret_cast<int16_t *>(val8)[*rid];
	case 4:
#if 0
		if (type == CalpontSystemCatalog::FLOAT) {
			// convert the float to a 64-bit type, return that w/o conversion
			int32_t* val32 = reinterpret_cast<int32_t *>(val8);
			double dTmp;
			dTmp = (double) *((float *) &val32[*rid]);
			return *((int64_t *) &dTmp);
		} else {
			return reinterpret_cast<int32_t *>(val8)[*rid];
		}
#else
			return reinterpret_cast<int32_t *>(val8)[*rid];
#endif
	case 8:
		return reinterpret_cast<int64_t *>(val8)[*rid];
	default:
		logIt(33, W);

#ifdef PRIM_DEBUG
		throw logic_error("PrimitiveProcessor::nextColValue() bad width");
#endif
		return -1;
	}
}


// done should be init'd to false and
// index should be init'd to 0 on the first call
// done == true when there are no more elements to return.
inline uint64_t nextUnsignedColValueHelper(int type,
				int width,
				const uint16_t *ridArray,
				int NVALS,
				int *index,
				bool *done,
				bool *isNull,
				bool *isEmpty,
				uint16_t *rid,
				uint8_t OutputType, uint8_t *val8, unsigned itemsPerBlk)
{
	switch (width)
	{
	case 8:
		return nextUnsignedColValue<8>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
			itemsPerBlk);
	case 4:
		return nextUnsignedColValue<4>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
			itemsPerBlk);
	case 2:
		return nextUnsignedColValue<2>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
			itemsPerBlk);
	case 1:
		return nextUnsignedColValue<1>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
			itemsPerBlk);
	default:
		idbassert(0);
	}
	/*NOTREACHED*/
	return 0;
}

// done should be init'd to false and
// index should be init'd to 0 on the first call
// done == true when there are no more elements to return.
inline int64_t nextColValueHelper(int type,
				int width,
				const uint16_t *ridArray,
				int NVALS,
				int *index,
				bool *done,
				bool *isNull,
				bool *isEmpty,
				uint16_t *rid,
				uint8_t OutputType, uint8_t *val8, unsigned itemsPerBlk)
{
	switch (width)
	{
	case 8:
		return nextColValue<8>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
			itemsPerBlk);
	case 4:
		return nextColValue<4>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
			itemsPerBlk);
	case 2:
		return nextColValue<2>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
			itemsPerBlk);
	case 1:
		return nextColValue<1>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
			itemsPerBlk);
	default:
		idbassert(0);
	}
	/*NOTREACHED*/
	return 0;
}
#if 0
inline void p_Col_noprid(const NewColRequestHeader *in, NewColResultHeader *out,
	unsigned outSize, unsigned *written, int* block)
{

	int argIndex, argOffset;
	uint16_t rid;
	const ColArgs *args;
	const uint8_t *in8 = reinterpret_cast<const uint8_t *>(in);
	int64_t argVal, colVal;
    uint64_t uargVal, ucolVal;

	int8_t *val8 = reinterpret_cast<int8_t *>(block);
	int16_t *val16 = reinterpret_cast<int16_t *>(block);
	int32_t *val32 = reinterpret_cast<int32_t *>(block);
	int64_t *val64 = reinterpret_cast<int64_t *>(block);
    uint8_t *uval8 = reinterpret_cast<uint8_t *>(block);
    uint16_t *uval16 = reinterpret_cast<uint16_t *>(block);
    uint32_t *uval32 = reinterpret_cast<uint32_t *>(block);
    uint64_t *uval64 = reinterpret_cast<uint64_t *>(block);

	placeholderRegex.used = false;

	//cout << "NOPRID" << endl;

	for (argIndex = 0; argIndex < in->NVALS; argIndex++) {
		argOffset = sizeof(NewColRequestHeader) + (argIndex * (sizeof(ColArgs) +
			sizeof(int16_t) + in->DataSize));
		args = reinterpret_cast<const ColArgs *>(&in8[argOffset]);

		rid = *reinterpret_cast<const uint16_t *>(&in8[argOffset + sizeof(ColArgs) +
			in->DataSize]);

        if (isUnsigned((CalpontSystemCatalog::ColDataType)in->DataType))
        {
            switch (in->DataSize) 
            {
                case 1: 
                    uargVal = *reinterpret_cast<const uint8_t *>(args->val[0]); 
                    ucolVal = uval8[rid]; 
                    break;
                case 2: 
                    uargVal = *reinterpret_cast<const uint16_t *>(args->val);
                    ucolVal = uval16[rid];
                    break;
                case 4:
                    uargVal = *reinterpret_cast<const uint32_t *>(args->val);
                    ucolVal = uval32[rid];
                    break;
                case 8: 
                    uargVal = *reinterpret_cast<const uint64_t *>(args->val);
                    ucolVal = uval64[rid];
                    break;
                default:
                    logIt(33, in->DataSize);
        #ifdef PRIM_DEBUG
                    throw logic_error("PrimitiveProcessor::p_Col_noprid(): bad width");
        #endif
                    return;
            }

            if (colCompare(ucolVal, uargVal, args->COP, args->rf, in->DataType, in->DataSize, placeholderRegex))
                store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t *>(block));
        }
        else
        {
            switch (in->DataSize) 
            {
                case 1: 
                    argVal = args->val[0]; 
                    colVal = val8[rid]; 
                    break;
                case 2: 
                    argVal = *reinterpret_cast<const int16_t *>(args->val);
                    colVal = val16[rid];
                    break;
                case 4:
                    argVal = *reinterpret_cast<const int32_t *>(args->val);
                    colVal = val32[rid];
                    break;
                case 8: argVal = *reinterpret_cast<const int64_t *>(args->val);
                    colVal = val64[rid];
                    break;
                default:
                    logIt(33, in->DataSize);
        #ifdef PRIM_DEBUG
                    throw logic_error("PrimitiveProcessor::p_Col_noprid(): bad width");
        #endif
                    return;
            }

            if (colCompare(colVal, argVal, args->COP, args->rf, in->DataType, in->DataSize, placeholderRegex))
                store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t *>(block));
        }
	}
}
#endif
template<int W>
inline void p_Col_ridArray(NewColRequestHeader *in,
                           NewColResultHeader *out,
                           unsigned outSize,
                           unsigned *written, int* block, Stats* fStatsPtr, unsigned itemsPerBlk,
                           boost::shared_ptr<ParsedColumnFilter> parsedColumnFilter)
{
    uint16_t *ridArray=0;
    uint8_t *in8 = reinterpret_cast<uint8_t *>(in);
    const uint8_t filterSize = sizeof(uint8_t) + sizeof(uint8_t) + W;

    placeholderRegex.used = false;

    if (in->NVALS>0)
        ridArray = reinterpret_cast<uint16_t *>(&in8[sizeof(NewColRequestHeader) +
                                                     (in->NOPS * filterSize)]);

    if (ridArray && 1 == in->sort )
    {
        qsort(ridArray, in->NVALS, sizeof(uint16_t), compareBlock<uint16_t>);
        if (fStatsPtr)
#ifdef _MSC_VER
            fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, 'O');
#else
            fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, 'O');
#endif
    }

    // Set boolean indicating whether to capture the min and max values.
    out->ValidMinMax = isMinMaxValid(in);
    if (out->ValidMinMax)
    {
        if (isUnsigned((CalpontSystemCatalog::ColDataType)in->DataType))
        {
            out->Min = static_cast<int64_t>(numeric_limits<uint64_t>::max());
            out->Max = 0;
        }
        else
        {
            out->Min = numeric_limits<int64_t>::max();
            out->Max = numeric_limits<int64_t>::min();
        }
    }
    else
    {
        out->Min = 0;
        out->Max = 0;
    }

    const ColArgs *args=NULL;
    int64_t val=0;
    uint64_t uval=0;
    int nextRidIndex=0, argIndex=0;
    bool done=false, cmp=false, isNull=false, isEmpty=false;
    uint16_t rid=0;
    prestored_set_t::const_iterator it;

    int64_t* std_argVals = (int64_t*)alloca(in->NOPS * sizeof(int64_t));
    uint8_t* std_cops = (uint8_t*)alloca(in->NOPS * sizeof(uint8_t));
    uint8_t* std_rfs = (uint8_t*)alloca(in->NOPS * sizeof(uint8_t));
    int64_t *argVals = NULL;
    uint64_t *uargVals = NULL;
    uint8_t *cops = NULL;
    uint8_t *rfs = NULL;

    scoped_array<idb_regex_t> std_regex;
    idb_regex_t* regex = NULL;
    uint8_t likeOps = 0;

    // no pre-parsed column filter is set, parse the filter in the message
    if (parsedColumnFilter.get() == NULL)
    {
        std_regex.reset(new idb_regex_t[in->NOPS]);
        regex = &(std_regex[0]);

        if (isUnsigned((CalpontSystemCatalog::ColDataType)in->DataType))
        {
            uargVals = reinterpret_cast<uint64_t*>(std_argVals);
            cops = std_cops;
            rfs = std_rfs;

            for (argIndex = 0; argIndex < in->NOPS; argIndex++)
            {
                args = reinterpret_cast<const ColArgs *>(&in8[sizeof(NewColRequestHeader) +
                                                              (argIndex * filterSize)]);
                cops[argIndex] = args->COP;
                rfs[argIndex] = args->rf;

                switch (W)
                {
                    case 1: uargVals[argIndex] = *reinterpret_cast<const uint8_t *>(args->val); break;
                    case 2: uargVals[argIndex] = *reinterpret_cast<const uint16_t *>(args->val); break;
                    case 4: uargVals[argIndex] = *reinterpret_cast<const uint32_t *>(args->val); break;
                    case 8: uargVals[argIndex] = *reinterpret_cast<const uint64_t *>(args->val); break;
                }
                regex[argIndex].used = false;
            }
        }
        else
        {
            argVals = std_argVals;
            cops = std_cops;
            rfs = std_rfs;

            for (argIndex = 0; argIndex < in->NOPS; argIndex++)
            {
                args = reinterpret_cast<const ColArgs *>(&in8[sizeof(NewColRequestHeader) +
                                                              (argIndex * filterSize)]);
                cops[argIndex] = args->COP;
                rfs[argIndex] = args->rf;

                switch (W)
                {
                    case 1: argVals[argIndex] = args->val[0]; break;
                    case 2: argVals[argIndex] = *reinterpret_cast<const int16_t *>(args->val); break;
                    case 4:
#if 0
                        if (in->DataType == CalpontSystemCatalog::FLOAT)
                        {
                            double dTmp;

                            dTmp = (double) *((const float *) args->val);
                            argVals[argIndex] = *((int64_t *) &dTmp);
                        }
                        else
                            argVals[argIndex] = *reinterpret_cast<const int32_t *>(args->val);
#else
                        argVals[argIndex] = *reinterpret_cast<const int32_t *>(args->val);
#endif
                        break;
                    case 8: argVals[argIndex] = *reinterpret_cast<const int64_t *>(args->val); break;
                }
                if (COMPARE_LIKE & args->COP)
                {
                    PrimitiveProcessor::p_DataValue dv = convertToPDataValue(&argVals[argIndex], W);
                    int err = PrimitiveProcessor::convertToRegexp(&regex[argIndex], &dv);
                    if (err)
                    {
                        throw runtime_error("PrimitiveProcessor::p_Col_ridarray(): Could not create regular expression for LIKE operator");
                    }
                    ++likeOps;
                }
                else
                    regex[argIndex].used = false;
            }
        }
    }
    // we have a pre-parsed filter, and it's in the form of op and value arrays
    else if (parsedColumnFilter->columnFilterMode == TWO_ARRAYS)
    {
        argVals = parsedColumnFilter->prestored_argVals.get();
        uargVals = reinterpret_cast<uint64_t*>(parsedColumnFilter->prestored_argVals.get());
        cops = parsedColumnFilter->prestored_cops.get();
        rfs = parsedColumnFilter->prestored_rfs.get();
        regex = parsedColumnFilter->prestored_regex.get();
        likeOps = parsedColumnFilter->likeOps;

    }
    // else we have a pre-parsed filter, and it's an unordered set for quick == comparisons

    if (isUnsigned((CalpontSystemCatalog::ColDataType)in->DataType))
    {
        uval = nextUnsignedColValue<W>(in->DataType, ridArray, in->NVALS, &nextRidIndex, &done, &isNull,
                               &isEmpty, &rid, in->OutputType, reinterpret_cast<uint8_t *>(block), itemsPerBlk);
    }
    else
    {
        val = nextColValue<W>(in->DataType, ridArray, in->NVALS, &nextRidIndex, &done, &isNull,
                              &isEmpty, &rid, in->OutputType, reinterpret_cast<uint8_t *>(block), itemsPerBlk);
    }

    while (!done)
    {
        if (cops == NULL)    // implies parsedColumnFilter && columnFilterMode == SET
        {
            /* bug 1920: ignore NULLs in the set and in the column data */
            if (!(isNull && in->BOP == BOP_AND))
            {
                if (isUnsigned((CalpontSystemCatalog::ColDataType)in->DataType))
                {
                    it = parsedColumnFilter->prestored_set->find(*reinterpret_cast<int64_t*>(&uval));
                }
                else
                {
                    it = parsedColumnFilter->prestored_set->find(val);
                }
                if (in->BOP == BOP_OR)
                {
                    // assume COP == COMPARE_EQ
                    if (it != parsedColumnFilter->prestored_set->end())
                    {
                        store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t *>(block));
                    }
                }
                else if (in->BOP == BOP_AND)
                {
                    // assume COP == COMPARE_NE
                    if (it == parsedColumnFilter->prestored_set->end())
                    {
                        store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t *>(block));
                    }
                }
            }
        }
        else
        {
            for (argIndex = 0; argIndex < in->NOPS; argIndex++)
            {
                if (isUnsigned((CalpontSystemCatalog::ColDataType)in->DataType))
                {
                    cmp = colCompareUnsigned(uval, uargVals[argIndex], cops[argIndex],
                                     rfs[argIndex], in->DataType, W, regex[argIndex], isNull);
                }
                else
                {
                    cmp = colCompare(val, argVals[argIndex], cops[argIndex],
                                     rfs[argIndex], in->DataType, W, regex[argIndex], isNull);
                }

                if (in->NOPS == 1)
                {
                    if (cmp == true)
                    {
                        store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t *>(block));
                    }
                    break;
                }
                else if (in->BOP == BOP_AND && cmp == false)
                {
                    break;
                }
                else if (in->BOP == BOP_OR && cmp == true)
                {
                    store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t *>(block));
                    break;
                }
            }

            if ((argIndex == in->NOPS && in->BOP == BOP_AND) || in->NOPS == 0)
            {
                store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t *>(block));
            }
        }

        // Set the min and max if necessary.  Ignore nulls.
        if (out->ValidMinMax && !isNull && !isEmpty)
        {

            if ((in->DataType == CalpontSystemCatalog::CHAR || in->DataType == CalpontSystemCatalog::VARCHAR ) && 1 < W)
            {
                if (colCompare(out->Min, val, COMPARE_GT, false, in->DataType, W, placeholderRegex))
                    out->Min = val;
                if (colCompare(out->Max, val, COMPARE_LT, false, in->DataType, W, placeholderRegex))
                    out->Max = val;
            }
            else
            if (isUnsigned((CalpontSystemCatalog::ColDataType)in->DataType))
            {
                if (static_cast<uint64_t>(out->Min) > uval)
                    out->Min = static_cast<int64_t>(uval);
                if (static_cast<uint64_t>(out->Max) < uval)
                    out->Max = static_cast<int64_t>(uval);;
            }
            else
            {
                if (out->Min > val)
                    out->Min = val;
                if (out->Max < val)
                    out->Max = val;
            }
        }

        if (isUnsigned((CalpontSystemCatalog::ColDataType)in->DataType))
        {
            uval = nextUnsignedColValue<W>(in->DataType, ridArray, in->NVALS, &nextRidIndex, &done,
                                  &isNull, &isEmpty, &rid, in->OutputType, reinterpret_cast<uint8_t *>(block),
                                  itemsPerBlk);
        }
        else
        {
            val = nextColValue<W>(in->DataType, ridArray, in->NVALS, &nextRidIndex, &done,
                                  &isNull, &isEmpty, &rid, in->OutputType, reinterpret_cast<uint8_t *>(block),
                                  itemsPerBlk);
        }
    }

    if (fStatsPtr)
#ifdef _MSC_VER
        fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, 'K');
#else
        fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, 'K');
#endif
}

} //namespace anon

namespace primitives
{

void PrimitiveProcessor::p_Col(NewColRequestHeader *in, NewColResultHeader *out,
	unsigned outSize, unsigned *written)
{
	memcpy(out, in, sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
	out->NVALS = 0;
	out->LBID = in->LBID;
	out->ism.Command = COL_RESULTS;
	out->OutputType = in->OutputType;
	out->RidFlags = 0;
	*written = sizeof(NewColResultHeader);
	unsigned itemsPerBlk = 0;

	if (logicalBlockMode)
		itemsPerBlk = BLOCK_SIZE;
	else
		itemsPerBlk = BLOCK_SIZE/in->DataSize;

	//...Initialize I/O counts;
	out->CacheIO    = 0;
	out->PhysicalIO = 0;

#if 0
	// short-circuit the actual block scan for testing
	if (out->LBID >= 802816)
	{
	out->ValidMinMax = false;
	out->Min = 0;
	out->Max = 0;
	return;
	}
#endif

	if (fStatsPtr)
#ifdef _MSC_VER
		fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, 'B');
#else
		fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, 'B');
#endif
	switch (in->DataSize)
	{
	case 8:
		p_Col_ridArray<8>(in, out, outSize, written, block, fStatsPtr, itemsPerBlk, parsedColumnFilter);
		break;
	case 4:
		p_Col_ridArray<4>(in, out, outSize, written, block, fStatsPtr, itemsPerBlk, parsedColumnFilter);
		break;
	case 2:
		p_Col_ridArray<2>(in, out, outSize, written, block, fStatsPtr, itemsPerBlk, parsedColumnFilter);
		break;
	case 1:
		p_Col_ridArray<1>(in, out, outSize, written, block, fStatsPtr, itemsPerBlk, parsedColumnFilter);
		break;
	default:
		idbassert(0);
		break;
	}
	if (fStatsPtr)
#ifdef _MSC_VER
		fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, 'C');
#else
		fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, 'C');
#endif
}

boost::shared_ptr<ParsedColumnFilter> PrimitiveProcessor::parseColumnFilter
	(const uint8_t *filterString, uint32_t colWidth, uint32_t colType, uint32_t filterCount, 
	uint32_t BOP)
{
	boost::shared_ptr<ParsedColumnFilter> ret;
	uint32_t argIndex;
	const ColArgs *args;
	bool convertToSet = true;

	if (filterCount == 0)
		return ret;

	ret.reset(new ParsedColumnFilter());

	ret->columnFilterMode = TWO_ARRAYS;
	ret->prestored_argVals.reset(new int64_t[filterCount]);
	ret->prestored_cops.reset(new uint8_t[filterCount]);
	ret->prestored_rfs.reset(new uint8_t[filterCount]);
	ret->prestored_regex.reset(new idb_regex_t[filterCount]);

	/*
	for (unsigned ii = 0; ii < filterCount; ii++)
	{
		ret->prestored_argVals[ii] = 0;
		ret->prestored_cops[ii] = 0;
		ret->prestored_rfs[ii] = 0;
		ret->prestored_regex[ii].used = 0;
	}
	*/

    const uint8_t filterSize = sizeof(uint8_t) + sizeof(uint8_t) + colWidth;

	/*  Decide which structure to use.  I think the only cases where we can use the set
		are when NOPS > 1, BOP is OR, and every COP is ==,
		and when NOPS > 1, BOP is AND, and every COP is !=.

		Parse the filter predicates and insert them into argVals and cops.
		If there were no predicates that violate the condition for using a set,
		insert argVals into a set.
	*/
	if (filterCount == 1)
		convertToSet = false;

	for (argIndex = 0; argIndex < filterCount; argIndex++)
	{
		args = reinterpret_cast<const ColArgs *>(filterString + (argIndex * filterSize));
		ret->prestored_cops[argIndex] = args->COP;
		ret->prestored_rfs[argIndex] = args->rf;
		if ((BOP == BOP_OR && args->COP != COMPARE_EQ) ||
			 (BOP == BOP_AND && args->COP != COMPARE_NE) ||
			 (args->COP == COMPARE_NIL))
			convertToSet = false;

        if (isUnsigned((CalpontSystemCatalog::ColDataType)colType))
        {
            switch (colWidth) {
                case 1: 
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const uint8_t*>(args->val); 
                    break;
                case 2: 
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const uint16_t*>(args->val); 
                    break;
                case 4:
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const uint32_t*>(args->val);
                    break;
                case 8: 
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const uint64_t*>(args->val);
                    break;
            }
        }
        else
        {
            switch (colWidth) {
                case 1: 
                    ret->prestored_argVals[argIndex] = args->val[0]; 
                    break;
                case 2: 
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const int16_t*>(args->val); 
                    break;
                case 4:
    #if 0
                    if (colType == CalpontSystemCatalog::FLOAT) {
                        double dTmp;

                        dTmp = (double) *((const float *) args->val);
                        ret->prestored_argVals[argIndex] = *((int64_t *) &dTmp);
                    }
                    else
                        ret->prestored_argVals[argIndex] =
                            *reinterpret_cast<const int32_t *>(args->val);
    #else
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const int32_t*>(args->val);
    #endif
                    break;
                case 8: 
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const int64_t*>(args->val);
                    break;
            }
        }
// 		cout << "inserted* " << hex << ret->prestored_argVals[argIndex] << dec <<
// 		  " COP = " << (int) ret->prestored_cops[argIndex] << endl;

		if (COMPARE_LIKE & args->COP)
		{
			PrimitiveProcessor::p_DataValue dv = convertToPDataValue(&ret->prestored_argVals[argIndex], colWidth);
			int err = PrimitiveProcessor::convertToRegexp(&ret->prestored_regex[argIndex], &dv);
			if (err) 
			{
				throw runtime_error("PrimitiveProcessor::parseColumnFilter(): Could not create regular expression for LIKE operator");
			}
			++ret->likeOps;
		}
		else
		{
			ret->prestored_regex[argIndex].used = false;
		}

	}
	if (convertToSet) {
		ret->columnFilterMode = UNORDERED_SET;
		ret->prestored_set.reset(new prestored_set_t());
		// @bug 2584, use COMPARE_NIL for "= null" to allow "is null" in OR expression
		for (argIndex = 0; argIndex < filterCount; argIndex++)
			if (ret->prestored_rfs[argIndex] == 0)
				ret->prestored_set->insert(ret->prestored_argVals[argIndex]);
	}

	return ret;
}

} // namespace primitives
// vim:ts=4 sw=4:

