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
*   $Id: simplecolumn_decimal.h 9635 2013-06-19 21:42:30Z bwilkinson $
*
*
***********************************************************************/
/** @file */

#ifndef SIMPLECOLUMNDECIMAL_H
#define SIMPLECOLUMNDECIMAL_H
#include <string>
#include <cmath>

#include "simplecolumn.h"
#include "objectreader.h"
#include "joblisttypes.h"
#include "rowgroup.h"
#include "dataconvert.h"

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
class SimpleColumn_Decimal : public SimpleColumn {

/**
 * Public stuff
 */
public:
 
	/** Constructors */ 
	SimpleColumn_Decimal();
	SimpleColumn_Decimal(const std::string& token, const uint32_t sessionID = 0);
	SimpleColumn_Decimal(const std::string& schema, 
		               const std::string& table, 
		               const std::string& col, 
		               const bool isInfiniDB,
		               const uint32_t sessionID = 0);
	SimpleColumn_Decimal(const SimpleColumn& rhs, const uint32_t sessionID = 0);
	
	/** Destructor */
	virtual ~SimpleColumn_Decimal(){}
	
	inline virtual SimpleColumn_Decimal* clone() const
	{
	    return new SimpleColumn_Decimal<len> (*this);
	}	
	
	/** Evaluate methods */
	virtual inline const std::string& getStrVal(rowgroup::Row& row, bool& isNull);
	virtual inline int64_t getIntVal(rowgroup::Row& row, bool& isNull);
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
SimpleColumn_Decimal<len>::SimpleColumn_Decimal():SimpleColumn() 
{
	setNullVal();
}

template<int len>
SimpleColumn_Decimal<len>::SimpleColumn_Decimal(const std::string& token, const uint32_t sessionID):
		SimpleColumn(token, sessionID) 
{
	setNullVal();
}

template<int len>
SimpleColumn_Decimal<len>::SimpleColumn_Decimal(const std::string& schema, 
		           const std::string& table, 
		           const std::string& col, 
		           const bool isInfiniDB,
		           const uint32_t sessionID) :
		           SimpleColumn(schema, table, col, isInfiniDB, sessionID) 
{
	setNullVal();
}

template<int len>
SimpleColumn_Decimal<len>::SimpleColumn_Decimal(const SimpleColumn& rhs, const uint32_t sessionID):
		SimpleColumn(rhs,sessionID) 
{
	setNullVal();
}

template<int len>
void SimpleColumn_Decimal<len>::setNullVal()
{
	switch (len)
	{
		case 8:
			fNullVal = joblist::BIGINTNULL;
			break;
		case 4:
			fNullVal = joblist::INTNULL;
			break;
		case 2:
			fNullVal = joblist::SMALLINTNULL;
			break;
		case 1:
			fNullVal = joblist::TINYINTNULL;
			break;
		default:
			fNullVal = joblist::BIGINTNULL;
	}
}

template<int len>
inline const std::string& SimpleColumn_Decimal<len>:: getStrVal(rowgroup::Row& row, bool& isNull)
{ 
	dataconvert::DataConvert::decimalToString((int64_t)row.getIntField<len>(fInputIndex), fResultType.scale, tmp, 22, fResultType.colDataType);
	fResult.strVal = std::string(tmp);
    return fResult.strVal;
}

template<int len>
inline int64_t SimpleColumn_Decimal<len>:: getIntVal(rowgroup::Row& row, bool& isNull) 
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	return (int64_t)(row.getIntField<len>(fInputIndex) / pow((double)10, fResultType.scale));		
}
	
template<int len>
inline float SimpleColumn_Decimal<len>::getFloatVal(rowgroup::Row& row, bool& isNull)
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	return (row.getIntField<len>(fInputIndex) / pow((double)10, fResultType.scale));
}
	
template<int len>
inline double SimpleColumn_Decimal<len>::getDoubleVal(rowgroup::Row& row, bool& isNull) 
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	return (row.getIntField<len>(fInputIndex) / pow((double)10, fResultType.scale));
}
		
template<int len>
inline IDB_Decimal SimpleColumn_Decimal<len>::getDecimalVal(rowgroup::Row& row, bool& isNull)
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	fResult.decimalVal.value = (int64_t)row.getIntField<len>(fInputIndex);
	fResult.decimalVal.precision = fResultType.precision;
	fResult.decimalVal.scale = fResultType.scale;
	return fResult.decimalVal;	
}

template<int len>
void SimpleColumn_Decimal<len>::serialize(messageqcpp::ByteStream& b) const
{
	switch (len)
	{
		case 1:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_DECIMAL1;
			break;
		case 2:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_DECIMAL2;
			break;
		case 4:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_DECIMAL4;
			break;
		case 8:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_DECIMAL8;
			break;
	}
	SimpleColumn::serialize(b);
}

template<int len>
void SimpleColumn_Decimal<len>::unserialize(messageqcpp::ByteStream& b)
{
	switch (len)
	{
		case 1:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_DECIMAL1);
			break;
		case 2:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_DECIMAL2);
			break;
		case 4:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_DECIMAL4);
			break;
		case 8:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_DECIMAL8);
			break;
	}
	SimpleColumn::unserialize(b);
}

} 
#endif //SIMPLECOLUMN_INT_H

