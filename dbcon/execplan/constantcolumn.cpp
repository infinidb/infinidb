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
*   $Id: constantcolumn.cpp 9474 2013-05-02 15:28:09Z rdempsey $
*
*
***********************************************************************/
#include <string>
#include <iostream>
#include <sstream>
using namespace std;

#include "bytestream.h"
#include "constantcolumn.h"
#include "objectreader.h"
using namespace messageqcpp;

#include "dataconvert.h"
#include "calpontsystemcatalog.h"

namespace execplan {
/**
 *  Constructors/Destructors
 */
ConstantColumn::ConstantColumn() :
        ReturnedColumn(),
		fType(0)
{
}

ConstantColumn::ConstantColumn(const string& sql, TYPE type) :
  ReturnedColumn(),
	fConstval(sql),
	fType(type),
	fData(sql)
{
	fResult.strVal = sql;		
	
	if (type == LITERAL && sql.length() < 9)
	{
		memcpy(tmp, sql.c_str(), sql.length());
		memset(tmp+sql.length(), 0, 8);
		fResult.uintVal = uint64ToStr(*((uint64_t *) tmp));
		fResult.intVal = (int64_t)fResult.uintVal;
	}
	else
	{
		fResult.intVal = atoll(sql.c_str());
		fResult.uintVal = strtoull(sql.c_str(), NULL, 0);
	}
	
	fResult.floatVal = atof(sql.c_str());
	fResult.doubleVal = atof(sql.c_str());
	
	// decimal for constant should be constructed by the caller and call the decimal constructor
	fResult.decimalVal.value = fResult.intVal;
	fResult.decimalVal.scale = 0;
	fResult.decimalVal.precision = 18;

	// @bug 3381, default null item to integer type.
	if (fType == ConstantColumn::NULLDATA)
	{
        if (fResult.uintVal > (uint64_t)MAX_BIGINT)
        {
            fResultType.colDataType = CalpontSystemCatalog::UBIGINT;
        }
        else
        {
            fResultType.colDataType = CalpontSystemCatalog::BIGINT;
        }
		fResultType.colWidth = 8;
	}
	else
	{
		fResultType.colDataType = CalpontSystemCatalog::VARCHAR;
		fResultType.colWidth = sql.length();
	}
}

ConstantColumn::ConstantColumn(const string& sql, const double val) :
	ReturnedColumn(),
	fConstval(sql),
	fType(NUM),
	fData(sql)
{
	fResult.strVal = sql;
	fResult.doubleVal = val;
	fResult.intVal = (int64_t)val;
	fResult.uintVal = (uint64_t)val;
	fResult.floatVal = (float)val;
	// decimal for constant should be constructed by the caller and call the decimal constructor
	fResult.decimalVal.value = fResult.intVal;
	fResult.decimalVal.scale = 0;
	fResult.decimalVal.precision = 18;
	fResultType.colDataType = CalpontSystemCatalog::DOUBLE;
	fResultType.colWidth = 8;
}
			
ConstantColumn::ConstantColumn(const string& sql, const int64_t val, TYPE type) :
  ReturnedColumn(),
  fConstval(sql),
	fType(type),
	fData(sql)
{
	fResult.strVal = sql;
	fResult.intVal = val;
	fResult.uintVal = (uint64_t)fResult.intVal;
	fResult.floatVal = (float)fResult.intVal;
	fResult.doubleVal = (double)fResult.intVal;
	fResult.decimalVal.value = fResult.intVal;
	fResult.decimalVal.scale = 0;
	fResultType.colDataType = CalpontSystemCatalog::BIGINT;
	fResultType.colWidth = 8;
}

ConstantColumn::ConstantColumn(const string& sql, const uint64_t val, TYPE type) :
  ReturnedColumn(),
  fConstval(sql),
	fType(type),
	fData(sql)
{
	fResult.strVal = sql;
	fResult.uintVal = val;
	fResult.intVal = (int64_t)fResult.uintVal;
	fResult.floatVal = (float)fResult.uintVal;
	fResult.doubleVal = (double)fResult.uintVal;
	fResult.decimalVal.value = fResult.uintVal;
	fResult.decimalVal.scale = 0;
	fResultType.colDataType = CalpontSystemCatalog::UBIGINT;
	fResultType.colWidth = 8;
}

ConstantColumn::ConstantColumn(const string& sql, const IDB_Decimal& val) :
  ReturnedColumn(),
  fConstval(sql),
  fType(NUM),
	fData(sql)
{
	fResult.strVal = sql;
	fResult.intVal = (int64_t)atoll(sql.c_str());
    fResult.uintVal = strtoull(sql.c_str(), NULL, 0);
	fResult.floatVal = atof(sql.c_str());
	fResult.doubleVal = atof(sql.c_str());
	fResult.decimalVal = val;
	fResultType.colDataType = CalpontSystemCatalog::DECIMAL;
	fResultType.colWidth = 8;
	fResultType.scale = val.scale;
	fResultType.precision = val.precision;
}

ConstantColumn::ConstantColumn( const ConstantColumn& rhs):
    ReturnedColumn(rhs),
    fConstval (rhs.constval()),
    fType (rhs.type()),
    fData (rhs.data())
{
    sequence(rhs.sequence());
    fAlias = rhs.alias();
    fResult = rhs.fResult;
    fResultType = rhs.fResultType;

	if (fRegex.get() != NULL)
	{
		fRegex.reset(new CNX_Regex());
#ifdef _MSC_VER
		*fRegex = dataconvert::DataConvert::constructRegexp(fResult.strVal);
#else
		string str = dataconvert::DataConvert::constructRegexp(fResult.strVal);
		regcomp(fRegex.get(), str.c_str(), REG_NOSUB | REG_EXTENDED);
#endif
	}
}

ConstantColumn::ConstantColumn(const int64_t val, TYPE type) :
  ReturnedColumn(),
	fType(type)
{
	ostringstream oss;
	oss << val;
	fConstval = oss.str();
	fData = oss.str();
	fResult.strVal = fData;
	fResult.intVal = val;
	fResult.uintVal = (uint64_t)fResult.intVal;
	fResult.floatVal = (float)fResult.intVal;
	fResult.doubleVal = (double)fResult.intVal;
	fResult.decimalVal.value = fResult.intVal;
	fResult.decimalVal.scale = 0;
	fResultType.colDataType = CalpontSystemCatalog::BIGINT;
	fResultType.colWidth = 8;
}

ConstantColumn::ConstantColumn(const uint64_t val, TYPE type) :
  ReturnedColumn(),
	fType(type)
{
	ostringstream oss;
	oss << val;
	fConstval = oss.str();
	fData = oss.str();
	fResult.strVal = fData;
	fResult.intVal = (int64_t)val;
	fResult.uintVal = val;
	fResult.floatVal = (float)fResult.uintVal;
	fResult.doubleVal = (double)fResult.uintVal;
	fResult.decimalVal.value = fResult.uintVal;
	fResult.decimalVal.scale = 0;
	fResultType.colDataType = CalpontSystemCatalog::UBIGINT;
	fResultType.colWidth = 8;
}

ConstantColumn::~ConstantColumn()
{
#ifndef _MSC_VER
	if (fRegex.get() != NULL)
		regfree(fRegex.get());
#endif
}

const string ConstantColumn::toString() const
{
	ostringstream oss;
	oss << "ConstantColumn: " << fConstval << " intVal=" << fResult.intVal << " uintVal=" << fResult.uintVal;
	oss << '(';
	if (fType == LITERAL)
		oss << 'l';
	else if (fType == NUM)
		oss << 'n';
	else
	    oss << "null";
	oss << ')';
	oss << " resultType=" << colDataTypeToString(fResultType.colDataType);
	if (fAlias.length() > 0) oss << "/Alias: " << fAlias;
	return oss.str();
}

const string ConstantColumn::data() const
{
	return fData;
}

ostream& operator<<(ostream& output, const ConstantColumn& rhs)
{
	output << rhs.toString();

	return output;
}

void ConstantColumn::serialize(messageqcpp::ByteStream& b) const
{
	
	b << (ObjectReader::id_t) ObjectReader::CONSTANTCOLUMN;
	ReturnedColumn::serialize(b);
	b << fConstval;
	b << (uint32_t) fType;
	//b << fAlias;
	b << fData;
	b << static_cast<const ByteStream::doublebyte>(fReturnAll);	
	b << (uint64_t)fResult.intVal;
	b << fResult.uintVal;
	b << (*(uint64_t*)(&fResult.doubleVal));
	b << (*(uint32_t*)(&fResult.floatVal));
	b << (uint8_t)fResult.boolVal;
	b << fResult.strVal;
	b << (uint64_t)fResult.decimalVal.value;
	b << (uint8_t)fResult.decimalVal.scale;
	b << (uint8_t)fResult.decimalVal.precision;
}

void ConstantColumn::unserialize(messageqcpp::ByteStream& b)
{
	
	ObjectReader::checkType(b, ObjectReader::CONSTANTCOLUMN);
	ReturnedColumn::unserialize(b);
	//uint64_t val;
	
	b >> fConstval;
	b >> (uint32_t&) fType;
	b >> fData;
	b >> reinterpret_cast< ByteStream::doublebyte&>(fReturnAll);	
	b >> (uint64_t&)fResult.intVal;
	b >> fResult.uintVal;
	b >> (uint64_t&)fResult.doubleVal;
	b >> (uint32_t&)fResult.floatVal;
	b >> (uint8_t&)fResult.boolVal;	
	b >> fResult.strVal;
	b >> (uint64_t&)fResult.decimalVal.value;
	b >> (uint8_t&)fResult.decimalVal.scale;
	b >> (uint8_t&)fResult.decimalVal.precision;
}

bool ConstantColumn::operator==(const ConstantColumn& t) const
{
	const ReturnedColumn *rc1, *rc2;

	rc1 = static_cast<const ReturnedColumn*>(this);
	rc2 = static_cast<const ReturnedColumn*>(&t);
	if (*rc1 != *rc2)
		return false;
	if (fConstval != t.fConstval)
		return false;
	if (fType != t.fType)
		return false;
	if (fAlias != t.fAlias)
		return false;
	if (fData != t.fData)
		return false;
	if (fReturnAll != t.fReturnAll)
		return false;
	return true;
}

bool ConstantColumn::operator==(const TreeNode* t) const
{
	const ConstantColumn *o;

	o = dynamic_cast<const ConstantColumn*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool ConstantColumn::operator!=(const ConstantColumn& t) const
{
	return (!(*this == t));
}

bool ConstantColumn::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

void ConstantColumn::constructRegex()
{
	//fRegex = new regex_t();
	fRegex.reset(new CNX_Regex());
#ifdef _MSC_VER
	*fRegex = dataconvert::DataConvert::constructRegexp(fResult.strVal);
#else
	string str = dataconvert::DataConvert::constructRegexp(fResult.strVal);
	regcomp(fRegex.get(), str.c_str(), REG_NOSUB | REG_EXTENDED);
#endif
}

}
// vim:ts=4 sw=4:
