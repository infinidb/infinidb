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

//   $Id: treenode.h 7858 2011-07-15 19:16:15Z chao $


/** @file */

#ifndef CALPONT_TREENODE_H
#define CALPONT_TREENODE_H

#include <string>
#include <iostream>
#include <cmath>
#include <boost/shared_ptr.hpp>
#include <boost/regex.hpp>
#include <unistd.h>

#include "calpontsystemcatalog.h"
#include "exceptclasses.h"
#include "dataconvert.h"

#ifdef _MSC_VER
#define snprintf _snprintf
#endif

namespace messageqcpp {
class ByteStream;
}

namespace rowgroup {
class Row;
}

/**
 * Namespace
 */
namespace execplan{

typedef execplan::CalpontSystemCatalog::ColType Type;

/**
 * @brief IDB_Decimal type
 *
 */
struct IDB_Decimal
{
	IDB_Decimal(): value(0), scale(0), precision(0) {}
	IDB_Decimal(int64_t val, int8_t s, uint8_t p) :
				value (val),
				scale(s),
				precision(p) {}

	long double decimalDiff(const IDB_Decimal& d) const
	{
		return ((long double)(value) / pow((long double)10, scale - d.scale)) - d.value;
	}
	bool operator==(const IDB_Decimal& rhs) const
	{
		if (scale == rhs.scale)
			return value == rhs.value;
		else
			return (decimalDiff(rhs) == 0.0);
	}
	bool operator>(const IDB_Decimal& rhs) const
	{
		if (scale == rhs.scale)
			return value > rhs.value;
		else
			return (decimalDiff(rhs) > 0.0);
	}
	bool operator<(const IDB_Decimal& rhs) const
	{
		if (scale == rhs.scale)
			return value < rhs.value;
		else
			return (decimalDiff(rhs) < 0.0);
	}
	bool operator>=(const IDB_Decimal& rhs) const
	{
		if (scale == rhs.scale)
			return value >= rhs.value;
		else
			return (decimalDiff(rhs) >= 0.0);
	}
	bool operator<=(const IDB_Decimal& rhs) const
	{
		if (scale == rhs.scale)
			return value <= rhs.value;
		else
			return (decimalDiff(rhs) <= 0.0);
	}
	bool operator!=(const IDB_Decimal& rhs) const
	{
		if (scale == rhs.scale)
			return value != rhs.value;
		else
			return (decimalDiff(rhs) != 0.0);
	}

	int64_t value;
	int8_t  scale;      // 0~18
	uint8_t precision;  // 1~18
};
typedef IDB_Decimal CNX_Decimal;

/**
 * @brief IDB_Regex struct
 *
 */
typedef boost::regex IDB_Regex;
typedef IDB_Regex CNX_Regex;

typedef boost::shared_ptr<IDB_Regex> SP_IDB_Regex;
typedef SP_IDB_Regex SP_CNX_Regex;

const int64_t IDB_pow[19] = {
1,
10,
100,
1000,
10000,
100000,
1000000,
10000000,
100000000,
1000000000,
10000000000LL,
100000000000LL,
1000000000000LL,
10000000000000LL,
100000000000000LL,
1000000000000000LL,
10000000000000000LL,
100000000000000000LL,
1000000000000000000LL
};

/** Trim trailing 0 from val. All insignificant zeroes to the right of the 
 *  decimal point are removed. Also, the decimal point is not included on 
 *  whole numbers. It works like %g flag with printf, but always print 
 *  double value in fixed-point notation.
 *
 *  @parm val valid double value in fixed-point notation from printf %f. 
 *            No format validation is perfomed in this function.
 *  @parm length length of the buffer val 
 */
inline std::string removeTrailing0(char* val, uint length)
{
	char* ptr = val;
	uint i = 0;
	bool decimal_point = false;
	for (; i < length; i++, ptr++)
	{
		if (*ptr == '+' || *ptr == '-' || (*ptr >= '0' && *ptr <= '9'))
		{
			continue;
		}
		if (*ptr == '.')
		{
			decimal_point = true;
			continue;
		}
		*ptr = 0;
		break;
	}
	
	if (decimal_point)
	{
		for (i = i-1; i >= 0; i--)
		{
			if (val[i] == '0')
			{
				val[i] = 0;
			}
			else if (val[i] == '.')
			{
				val[i] = 0;
				break;
			}
			else
			{
				break;
			}
		}
	}
	return std::string(val);
}

/**
 * @brief Result type added for F&E framework
 *
 */
struct Result
{
	Result():intVal(0), origIntVal(0), dummy(0),
			doubleVal(0), floatVal(0), boolVal(false),
			strVal(""), decimalVal(IDB_Decimal(0,0,0)),
			valueConverted(false){}
	int64_t intVal;
	uint64_t origIntVal;
	// clear up the memory following origIntVal to make sure null terminated string
	// when converting origIntVal
	uint64_t dummy;
	double doubleVal;
	float floatVal;
	bool boolVal;
	std::string strVal;
	IDB_Decimal decimalVal;
	bool valueConverted;
};

/**
 * @brief An abstract class to represent a node data on the expression tree
 *
 */
class TreeNode
{
public:
	TreeNode();
	TreeNode(const TreeNode& rhs);
	virtual ~TreeNode();
	virtual const std::string data() const = 0;
	virtual void data(const std::string data) = 0;
	virtual const std::string toString() const = 0;
	virtual TreeNode* clone() const = 0;

	/**
	 * Interface for serialization
	 */

	/** @brief Convert *this to a stream of bytes
	 *
	 * Convert *this to a stream of bytes.
	 * @param b The ByteStream to add *this to.
	 */
	virtual void serialize(messageqcpp::ByteStream& b) const = 0;

	/** @brief Construct a TreeNode from a stream of bytes
	 *
	 * Construct a TreeNode from a stream of bytes.
	 * @param b The ByteStream to parse
	 * @return The newly allocated TreeNode
	 */
	virtual void unserialize(messageqcpp::ByteStream &b) = 0;

	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
	 */
	virtual bool operator==(const TreeNode* t) const = 0;

	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
	 */
	virtual bool operator!=(const TreeNode* t) const = 0;

	/***********************************************************************
	 *                     F&E framework                                   *
	 ***********************************************************************/
	virtual std::string getStrVal(rowgroup::Row& row, bool& isNull) {return fResult.strVal;}
	virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull) {return fResult.intVal;}
	virtual float getFloatVal(rowgroup::Row& row, bool& isNull) {return fResult.floatVal;}
	virtual double getDoubleVal(rowgroup::Row& row, bool& isNull) {return fResult.doubleVal;}
	virtual IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull) {return fResult.decimalVal;}
	virtual bool getBoolVal(rowgroup::Row& row, bool& isNull) {return fResult.boolVal;}
	virtual int32_t getDateIntVal(rowgroup::Row& row, bool& isNull) {return fResult.intVal;}
	virtual int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull) {return fResult.intVal;}
	virtual void evaluate(rowgroup::Row& row, bool& isNull) {}

	inline bool getBoolVal();
	inline std::string getStrVal();
	inline int64_t getIntVal();
	inline float getFloatVal();
	inline double getDoubleVal();
	inline IDB_Decimal getDecimalVal();
	inline int32_t getDateIntVal();
	inline int64_t getDatetimeIntVal();

	virtual const execplan::CalpontSystemCatalog::ColType& resultType() const { return fResultType; }
	virtual execplan::CalpontSystemCatalog::ColType& resultType() { return fResultType; }
	virtual void resultType ( const execplan::CalpontSystemCatalog::ColType& resultType ) ;
	virtual void operationType(const Type& type) {fOperationType = type;}
	virtual const execplan::CalpontSystemCatalog::ColType& operationType() const {return fOperationType;}

	// result mutator and accessor. for speical functor to set for optimization.
	virtual void result(const Result& result) { fResult = result; }
	virtual const Result& result() const { return fResult; }

	// regex mutator and accessor
	virtual void regex(SP_IDB_Regex regex) { fRegex = regex; }
	virtual SP_IDB_Regex regex() const { return fRegex; }

protected:
	Result fResult;
	execplan::CalpontSystemCatalog::ColType fResultType; // mapped from mysql data type
	execplan::CalpontSystemCatalog::ColType fOperationType; // operator type, could be different from the result type
	SP_IDB_Regex fRegex;

	// double's range is +/-1.7E308 with at least 15 digits of precision
	char tmp[312]; // for conversion use

private:
	//default okay
	//TreeNode& operator=(const TreeNode& rhs);
};

inline bool TreeNode::getBoolVal()
{
	switch (fResultType.colDataType)
	{
		case CalpontSystemCatalog::CHAR:
			if (fResultType.colWidth <= 8)
				return (atoi((char*)(&fResult.origIntVal)) != 0);
			return (atoi(fResult.strVal.c_str()) != 0);
		case CalpontSystemCatalog::VARCHAR:
			if (fResultType.colWidth <= 7)
				return (atoi((char*)(&fResult.origIntVal)) != 0);
			return (atoi(fResult.strVal.c_str()) != 0);
		//FIXME: Huh???
		case CalpontSystemCatalog::VARBINARY:
			if (fResultType.colWidth <= 7)
				return (atoi((char*)(&fResult.origIntVal)) != 0);
			return (atoi(fResult.strVal.c_str()) != 0);
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::DATE:
		case CalpontSystemCatalog::DATETIME:
			return (fResult.intVal != 0);
		case CalpontSystemCatalog::FLOAT:
			return (fResult.floatVal != 0);
		case CalpontSystemCatalog::DOUBLE:
			return (fResult.doubleVal != 0);
		case CalpontSystemCatalog::DECIMAL:
			return (fResult.decimalVal.value != 0);
		default:
			throw logging::InvalidConversionExcept("TreeNode::getBoolVal: Invalid conversion.");
	}
	return fResult.boolVal;
}

inline std::string TreeNode::getStrVal()
{
	switch (fResultType.colDataType)
	{
		case CalpontSystemCatalog::CHAR:
			if (fResultType.colWidth <= 8)
				return (char*)(&fResult.origIntVal);
			return fResult.strVal;
		case CalpontSystemCatalog::VARCHAR:
			if (fResultType.colWidth <= 7)
				return (char*)(&fResult.origIntVal);
			return fResult.strVal;
		//FIXME: ???
		case CalpontSystemCatalog::VARBINARY:
			if (fResultType.colWidth <= 7)
				return (char*)(&fResult.origIntVal);
			return fResult.strVal;
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::INT:
		{
#ifndef __LP64__
			snprintf(tmp, 20, "%lld", fResult.intVal);
#else
			snprintf(tmp, 20, "%ld", fResult.intVal);
#endif
			return std::string(tmp);
		}
		case CalpontSystemCatalog::FLOAT:
		{
			snprintf(tmp, 312, "%f", fResult.floatVal);
			//return std::string(tmp);
			return removeTrailing0(tmp, 312);
		}
		case CalpontSystemCatalog::DOUBLE:
		{
			snprintf(tmp, 312, "%f", fResult.doubleVal);
			return removeTrailing0(tmp, 312);
			//return std::string(tmp);
		}
		case CalpontSystemCatalog::DECIMAL:
		{
			dataconvert::DataConvert::decimalToString(fResult.decimalVal.value, fResult.decimalVal.scale, tmp, 22);
			return std::string(tmp);
		}
		case CalpontSystemCatalog::DATE:
		{
			dataconvert::DataConvert::dateToString(fResult.intVal, tmp, 255);
			return std::string(tmp);
		}
		case CalpontSystemCatalog::DATETIME:
		{
			dataconvert::DataConvert::datetimeToString(fResult.intVal, tmp, 255);
			return std::string(tmp);
		}
		default:
			throw logging::InvalidConversionExcept("TreeNode::getStrVal: Invalid conversion.");
	}
	return fResult.strVal;
}

inline int64_t TreeNode::getIntVal()
{
	switch (fResultType.colDataType)
	{
		case CalpontSystemCatalog::CHAR:
			if (fResultType.colWidth <= 8)
				return fResult.intVal;
			return atoll(fResult.strVal.c_str());
		case CalpontSystemCatalog::VARCHAR:
			if (fResultType.colWidth <= 7)
				return fResult.intVal;
			return atoll(fResult.strVal.c_str());
		//FIXME: ???
		case CalpontSystemCatalog::VARBINARY:
			if (fResultType.colWidth <= 7)
				return fResult.intVal;
			return atoll(fResult.strVal.c_str());
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			return fResult.intVal;
		case CalpontSystemCatalog::FLOAT:
			return (int64_t)fResult.floatVal;
		case CalpontSystemCatalog::DOUBLE:
			return (int64_t)fResult.doubleVal;
		case CalpontSystemCatalog::DECIMAL:
		{
			return (int64_t)(fResult.decimalVal.value / pow((double)10, fResult.decimalVal.scale));
		}
		case CalpontSystemCatalog::DATE:
		case CalpontSystemCatalog::DATETIME:
			return fResult.intVal;
		default:
			throw logging::InvalidConversionExcept("TreeNode::getIntVal: Invalid conversion.");
	}
	return fResult.intVal;
}
inline float TreeNode::getFloatVal()
{
	switch (fResultType.colDataType)
	{
		case CalpontSystemCatalog::CHAR:
			if (fResultType.colWidth <= 8)
				return atof((char*)(&fResult.origIntVal));
			return atof(fResult.strVal.c_str());
		case CalpontSystemCatalog::VARCHAR:
			if (fResultType.colWidth <= 7)
				return atof((char*)(&fResult.origIntVal));
			return atof(fResult.strVal.c_str());
		//FIXME: ???
		case CalpontSystemCatalog::VARBINARY:
			if (fResultType.colWidth <= 7)
				return atof((char*)(&fResult.origIntVal));
			return atof(fResult.strVal.c_str());
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			return (float)fResult.intVal;
		case CalpontSystemCatalog::FLOAT:
			return fResult.floatVal;
		case CalpontSystemCatalog::DOUBLE:
			return (float)fResult.doubleVal;
		case CalpontSystemCatalog::DECIMAL:
		{
			return (fResult.decimalVal.value / pow((double)10, fResult.decimalVal.scale));
		}
		case CalpontSystemCatalog::DATE:
		case CalpontSystemCatalog::DATETIME:
			return (float)fResult.intVal;
		default:
			throw logging::InvalidConversionExcept("TreeNode::getFloatVal: Invalid conversion.");
	}
	return fResult.floatVal;
}
inline double TreeNode::getDoubleVal()
{
	switch (fResultType.colDataType)
	{
		case CalpontSystemCatalog::CHAR:
			if (fResultType.colWidth <= 8)
				return strtod((char*)(&fResult.origIntVal), NULL);
			return strtod(fResult.strVal.c_str(), NULL);
		case CalpontSystemCatalog::VARCHAR:
			if (fResultType.colWidth <= 7)
				return strtod((char*)(&fResult.origIntVal), NULL);
			return strtod(fResult.strVal.c_str(), NULL);
		//FIXME: ???
		case CalpontSystemCatalog::VARBINARY:
			if (fResultType.colWidth <= 7)
				return strtod((char*)(&fResult.origIntVal), NULL);
			return strtod(fResult.strVal.c_str(), NULL);
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			return (double)fResult.intVal;
		case CalpontSystemCatalog::FLOAT:
			return (double)fResult.floatVal;
		case CalpontSystemCatalog::DOUBLE:
			return fResult.doubleVal;
		case CalpontSystemCatalog::DECIMAL:
		{
			// this may not be accurate. if this is problematic, change to pre-calculated power array.
			return (double)(fResult.decimalVal.value / pow((double)10, fResult.decimalVal.scale));
		}
		case CalpontSystemCatalog::DATE:
		case CalpontSystemCatalog::DATETIME:
			return (double)fResult.intVal;
		default:
			throw logging::InvalidConversionExcept("TreeNode::getDoubleVal: Invalid conversion.");
	}
	return fResult.doubleVal;
}
inline IDB_Decimal TreeNode::getDecimalVal()
{
	switch (fResultType.colDataType)
	{
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
			throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: non-support conversion from string");
		case CalpontSystemCatalog::VARBINARY:
			throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: non-support conversion from binary string");
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::TINYINT:
			fResult.decimalVal.value =(int64_t)(fResult.intVal * pow((double)10, fResultType.scale));
			fResult.decimalVal.scale = fResultType.scale;
			fResult.decimalVal.precision = fResultType.precision;
			break;
		case CalpontSystemCatalog::DATE:
		case CalpontSystemCatalog::DATETIME:
			throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: Invalid conversion from datetime.");
		case CalpontSystemCatalog::FLOAT:
			throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: non-support conversion from float");
		case CalpontSystemCatalog::DOUBLE:
			throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: non-support conversion from double");
		case CalpontSystemCatalog::DECIMAL:
			return fResult.decimalVal;
		default:
			throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: Invalid conversion.");
	}
	return fResult.decimalVal;
}

inline int64_t TreeNode::getDatetimeIntVal()
{
	if (fResultType.colDataType == execplan::CalpontSystemCatalog::DATE)
		return (fResult.intVal & 0x00000000FFFFFFC0LL) << 32;
	else if (fResultType.colDataType == execplan::CalpontSystemCatalog::DATETIME)
		//return (fResult.intVal & 0xFFFFFFFFFFF00000LL);
		return (fResult.intVal);
	else
		return getIntVal();
}

inline int32_t TreeNode::getDateIntVal()
{
	if (fResultType.colDataType == execplan::CalpontSystemCatalog::DATETIME)
		return (int32_t)(fResult.intVal >> 32) & 0xFFFFFFC0;
	else if (fResultType.colDataType == execplan::CalpontSystemCatalog::DATE)
		return fResult.intVal & 0xFFFFFFC0;
	else
		return getIntVal();
}

typedef boost::shared_ptr<TreeNode> STNP;

/**
* Operations
*/
std::ostream& operator<<(std::ostream& output, const TreeNode& rhs);

}

#endif
