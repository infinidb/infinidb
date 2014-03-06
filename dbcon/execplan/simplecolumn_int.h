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
*   $Id: simplecolumn_int.h 9635 2013-06-19 21:42:30Z bwilkinson $
*
*
***********************************************************************/
/** @file */

#ifndef SIMPLECOLUMNINT_H
#define SIMPLECOLUMNINT_H
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
class SimpleColumn_INT : public SimpleColumn {

/**
 * Public stuff
 */
public:
 
	/** Constructors */ 
	SimpleColumn_INT();
	SimpleColumn_INT(const std::string& token, const uint32_t sessionID = 0);
	SimpleColumn_INT(const std::string& schema, 
		               const std::string& table, 
		               const std::string& col, 
		               const bool isInfiniDB,
		               const uint32_t sessionID = 0);
	SimpleColumn_INT(const SimpleColumn& rhs, const uint32_t sessionID = 0);
	
	/** Destructor */
	virtual ~SimpleColumn_INT(){}
	
	inline virtual SimpleColumn_INT* clone() const
	{
	    return new SimpleColumn_INT<len> (*this);
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
SimpleColumn_INT<len>::SimpleColumn_INT():SimpleColumn() 
{
	setNullVal();
}

template<int len>
SimpleColumn_INT<len>::SimpleColumn_INT(const std::string& token, const uint32_t sessionID):
		SimpleColumn(token, sessionID) 
{
	setNullVal();
}

template<int len>
SimpleColumn_INT<len>::SimpleColumn_INT(const std::string& schema, 
		           const std::string& table, 
		           const std::string& col, 
		           const bool isInfiniDB,
		           const uint32_t sessionID) :
		          SimpleColumn(schema, table, col, isInfiniDB, sessionID) 
{
	setNullVal();
}

template<int len>
SimpleColumn_INT<len>::SimpleColumn_INT(const SimpleColumn& rhs, const uint32_t sessionID):
		SimpleColumn(rhs,sessionID) 
{
	setNullVal();
}

template<int len>
void SimpleColumn_INT<len>::setNullVal()
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
inline const std::string& SimpleColumn_INT<len>:: getStrVal(rowgroup::Row& row, bool& isNull)
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	else
	{
#ifndef __LP64__
		snprintf(tmp, 20, "%lld", (int64_t)row.getIntField<len>(fInputIndex));
#else
		snprintf(tmp, 20, "%ld", (int64_t)row.getIntField<len>(fInputIndex));
#endif
	}
	fResult.strVal = std::string(tmp);
    return fResult.strVal;
}

template<int len>
inline int64_t SimpleColumn_INT<len>:: getIntVal(rowgroup::Row& row, bool& isNull) 
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	return (int64_t)row.getIntField<len>(fInputIndex);		
}
	
template<int len>
inline uint64_t SimpleColumn_INT<len>:: getUintVal(rowgroup::Row& row, bool& isNull) 
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	return (uint64_t)row.getIntField<len>(fInputIndex);		
}
	
template<int len>
inline float SimpleColumn_INT<len>::getFloatVal(rowgroup::Row& row, bool& isNull)
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	return (float)row.getIntField<len>(fInputIndex);	
}
	
template<int len>
inline double SimpleColumn_INT<len>::getDoubleVal(rowgroup::Row& row, bool& isNull) 
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	return (double)row.getIntField<len>(fInputIndex);	
}
		
template<int len>
inline IDB_Decimal SimpleColumn_INT<len>::getDecimalVal(rowgroup::Row& row, bool& isNull)
{ 
	if (row.equals<len>(fNullVal, fInputIndex))
		isNull = true;
	fResult.decimalVal.value = (int64_t)row.getIntField<len>(fInputIndex);
	fResult.decimalVal.precision = 65;
	fResult.decimalVal.scale = 0;
	return fResult.decimalVal;	
}

template<int len>
void SimpleColumn_INT<len>::serialize(messageqcpp::ByteStream& b) const
{
	switch (len)
	{
		case 1:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_INT1;
			break;
		case 2:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_INT2;
			break;
		case 4:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_INT4;
			break;
		case 8:
			b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN_INT8;
			break;
	}
	SimpleColumn::serialize(b);
}

template<int len>
void SimpleColumn_INT<len>::unserialize(messageqcpp::ByteStream& b)
{
	switch (len)
	{
		case 1:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_INT1);
			break;
		case 2:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_INT2);
			break;
		case 4:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_INT4);
			break;
		case 8:
			ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN_INT8);
			break;
	}
	SimpleColumn::unserialize(b);
}

} 
#endif //SIMPLECOLUMN_INT_H

