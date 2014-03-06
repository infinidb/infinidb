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

/***********************************************************************
*   $Id: simplecolumn_uint.h 8536 2012-05-21 21:27:17Z dhall $
*
*
***********************************************************************/
/** @file */

#ifndef SIMPLECOLUMNUINT_H
#define SIMPLECOLUMNUINT_H
#include <string>

#include "simplecolumn.h"
#include "objectreader.h"
#include "joblisttypes.h"
#include "rowgroup.h"

/**
 * Namespace
 */
namespace execplan { 
/**
 * @brief A class to represent a simple returned column
 * 
 * This class is a specialization of class ReturnedColumn that handles
 * a column name.
 */
template <int len>
class SimpleColumn_UINT : public SimpleColumn {

/**
 * Public stuff
 */
public:
 
	/** Constructors */ 
	SimpleColumn_UINT();
	SimpleColumn_UINT(const std::string& token, const uint32_t sessionID = 0);
	SimpleColumn_UINT(const std::string& schema, 
		              const std::string& table, 
		              const std::string& col, 
		              const bool isInfiniDB,
		              const uint32_t sessionID = 0);
	SimpleColumn_UINT(const SimpleColumn& rhs, const uint32_t sessionID = 0);
	
	/** Destructor */
	virtual ~SimpleColumn_UINT(){}
	
	inline virtual SimpleColumn_UINT* clone() const
	{
	    return new SimpleColumn_UINT<len> (*this);
	}	
	
	/** Evaluate methods */
	virtual inline const std::string& getStrVal(rowgroup::Row& row, bool& isNull);
	virtual inline int64_t getIntVal(rowgroup::Row& row, bool& isNull);
    virtual inline uint64_t getUintVal(rowgroup::Row& row, bool& isNull);
	virtual inline float getFloatVal(rowgroup::Row& row, bool& isNull);
	virtual inline double getDoubleVal(rowgroup::Row& row, bool& isNull);	
	virtual inline IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull);
		
	/** The serialize interface */
	virtual void serialize(messageqcpp::ByteStream&) const;
	virtual void unserialize(messageqcpp::ByteStream&);	
	uint64_t fNullVal;

private:
	void setNullVal();
			
};

template<int len>
SimpleColumn_UINT<len>::SimpleColumn_UINT():SimpleColumn() 
{
	setNullVal();
}

template<int len>
SimpleColumn_UINT<len>::SimpleColumn_UINT(const std::string& token, const uint32_t sessionID):
		SimpleColumn(token, sessionID) 
{
	setNullVal();
}

template<int len>
SimpleColumn_UINT<len>::SimpleColumn_UINT(const std::string& schema, 
		           const std::string& table, 
		           const std::string& col, 
		           const bool isInfiniDB,
		           const uint32_t sessionID) :
		          SimpleColumn(schema, table, col, isInfiniDB, sessionID) 
{
	setNullVal();
}

template<int len>
SimpleColumn_UINT<len>::SimpleColumn_UINT(const SimpleColumn& rhs, const uint32_t sessionID):
		SimpleColumn(rhs,sessionID) 
{
	setNullVal();
}

template<int len>
void SimpleColumn_UINT<len>::setNullVal()
{
	switch (len)
	{
		case 8:
			fNullVal = joblist::UBIGINTNULL;
			break;
		case 4:
			fNullVal = joblist::UINTNULL;
			break;
		case 2:
			fNullVal = joblist::USMALLINTNULL;
			break;
		case 1:
			fNullVal = joblist::UTINYINTNULL;
			break;
		default:
			fNullVal = joblist::UBIGINTNULL;
	}
}

template<int len>
inline const std::string& SimpleColumn_UINT<len>:: getStrVal(rowgroup::Row& row, bool& isNull)
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	else
	{
#ifndef __LP64__
		snprintf(tmp, 21, "%llu", row.getUintField<len>(fInputIndex));
#else
		snprintf(tmp, 21, "%lu", row.getUintField<len>(fInputIndex));
#endif
	}
	fResult.strVal = std::string(tmp);
    return fResult.strVal;
}

template<int len>
inline int64_t SimpleColumn_UINT<len>:: getIntVal(rowgroup::Row& row, bool& isNull) 
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	return (int64_t)row.getUintField<len>(fInputIndex);		
}
	
template<int len>
inline uint64_t SimpleColumn_UINT<len>:: getUintVal(rowgroup::Row& row, bool& isNull) 
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	return (uint64_t)row.getUintField<len>(fInputIndex);		
}
	
template<int len>
inline float SimpleColumn_UINT<len>::getFloatVal(rowgroup::Row& row, bool& isNull)
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	return (float)row.getUintField<len>(fInputIndex);	
}
	
template<int len>
inline double SimpleColumn_UINT<len>::getDoubleVal(rowgroup::Row& row, bool& isNull) 
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	return (double)row.getUintField<len>(fInputIndex);	
}
		
template<int len>
inline IDB_Decimal SimpleColumn_UINT<len>::getDecimalVal(rowgroup::Row& row, bool& isNull)
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	fResult.decimalVal.value = (uint64_t)row.getUintField<len>(fInputIndex);
	fResult.decimalVal.precision = 65;
	fResult.decimalVal.scale = 0;
	return fResult.decimalVal;	
}

template<int len>
void SimpleColumn_UINT<len>::serialize(messageqcpp::ByteStream& b) const
{
	switch (len)
	{
		case 1:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_UINT1;
			break;
		case 2:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_UINT2;
			break;
		case 4:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_UINT4;
			break;
		case 8:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_UINT8;
			break;
	}
	SimpleColumn::serialize(b);
}

template<int len>
void SimpleColumn_UINT<len>::unserialize(messageqcpp::ByteStream& b)
{
	switch (len)
	{
		case 1:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_UINT1);
			break;
		case 2:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_UINT2);
			break;
		case 4:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_UINT4);
			break;
		case 8:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_UINT8);
			break;
	}
	SimpleColumn::unserialize(b);
}

} 
#endif //SIMPLECOLUMN_INT_H

