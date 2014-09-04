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
*   $Id: constantcolumn.h 8436 2012-04-04 18:18:21Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef CONSTANTCOLUMN_H
#define CONSTANTCOLUMN_H
#include <string>

#include "returnedcolumn.h"

namespace messageqcpp {
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan { 
class CalpontSystemCatalog;
/**
 * @brief A class to represent a constant return column
 * 
 * This class is a specialization of class ReturnedColumn that
 * handles a constant column such as number and literal string.
 */
class ConstantColumn : public ReturnedColumn {
public:

	enum TYPE
	{
	    NUM,
	    LITERAL,
	    NULLDATA
	};

	ConstantColumn();
	ConstantColumn(const std::string& sql, TYPE type = LITERAL);
	ConstantColumn(const int64_t val, TYPE type = NUM); //deprecate
	
	virtual ~ConstantColumn();
	
	/**
	 * Accessor Methods
	 */
	inline unsigned int type() const { return fType; }
	inline void type (unsigned int type) { fType = type; }
	inline const std::string& constval() const { return fConstval; }
	inline void constval(const std::string& constval) { fConstval = constval; }
	virtual const std::string data() const;
	virtual void data(const std::string data) { fData = data; }
	virtual const std::string toString() const;
	
  /** return a copy of this pointer
	 *
	 * deep copy of this pointer and return the copy
	 */	
	inline virtual ConstantColumn* clone() const {return new ConstantColumn (*this);}
		
	/**
	 * The serialization interface
	 */
	virtual void serialize(messageqcpp::ByteStream&) const;
	virtual void unserialize(messageqcpp::ByteStream&);
	
	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
		 */
	virtual bool operator==(const TreeNode* t) const;
	
	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
	 */
	bool operator==(const ConstantColumn& t) const;
	
	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
	 */
	virtual bool operator!=(const TreeNode* t) const;
	 
	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
	 */
	bool operator!=(const ConstantColumn& t) const;
	
private:
	std::string fConstval;
	int fType; 
	std::string fData;		
	
	/***********************************************************
	 *                  F&E framework                          *
	 ***********************************************************/
public:	
	ConstantColumn(const std::string& sql, const double val);
	ConstantColumn(const std::string& sql, const int64_t val, TYPE type = NUM);
	ConstantColumn(const std::string& sql, const IDB_Decimal& val);
	ConstantColumn(const ConstantColumn& rhs);
	virtual void evaluate(rowgroup::Row& row) {}
	virtual void constructRegex();
	
	virtual bool getBoolVal(rowgroup::Row& row, bool& isNull)
	{
		isNull = isNull || (fType == NULLDATA);
		return TreeNode::getBoolVal();		
	}
	virtual std::string getStrVal(rowgroup::Row& row, bool& isNull) 
	{ 
		isNull = isNull || (fType == NULLDATA);
		return fResult.strVal; 
	}
	
	virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull) 
	{ 
		isNull = isNull || (fType == NULLDATA);
		return fResult.intVal;
	}
	
	virtual float getFloatVal(rowgroup::Row& row, bool& isNull)
	{
		isNull = isNull || (fType == NULLDATA);
		return fResult.floatVal;
	}
	
	virtual double getDoubleVal(rowgroup::Row& row, bool& isNull) 
	{
		isNull = isNull || (fType == NULLDATA);
		return fResult.doubleVal; 
	}
	
	virtual IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull)
	{
		isNull = isNull || (fType == NULLDATA);
		return fResult.decimalVal;
	}
	
	virtual int32_t getDateIntVal(rowgroup::Row& row, bool& isNull)
	{
		isNull = isNull || (fType == NULLDATA);
		if (!fResult.valueConverted)
		{
			fResult.intVal = dataconvert::DataConvert::stringToDate(fResult.strVal);
			fResult.valueConverted = true;
		}
		return fResult.intVal;
	}
	
	virtual int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull)
	{
		isNull = isNull || (fType == NULLDATA);
		if (!fResult.valueConverted)
		{
			fResult.intVal = dataconvert::DataConvert::stringToDatetime(fResult.strVal);
			fResult.valueConverted = true;
		}
		return fResult.intVal;
	}
		
	inline float getFloatVal() const { return fResult.floatVal; }

	inline  double getDoubleVal() const { return fResult.doubleVal; }

};

/**
 * ostream operator
 */
std::ostream& operator<<(std::ostream& output, const ConstantColumn& rhs);

} 
#endif //CONSTANTCOLUMN_H

