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
*   $Id: functioncolumn.h 9679 2013-07-11 22:32:03Z zzhu $
*
*
***********************************************************************/
/** @file */

#ifndef FUNCTIONCOLUMN_H
#define FUNCTIONCOLUMN_H
#include <string>
#include <iosfwd>
#include <vector>

#include "returnedcolumn.h"
#include "functor.h"

namespace messageqcpp {
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan {

/**
 * @brief A class to represent a functional column
 *
 * This class is a specialization of class ReturnedColumn that
 * handles a functional column like "extract(...)" and "substr(...)"
 */
class FunctionColumn : public ReturnedColumn {

public:
	FunctionColumn();
	FunctionColumn(std::string& funcName);
	FunctionColumn(const std::string& functionName, const std::string& funcParmsInString, const uint32_t sessionID = 0);
	FunctionColumn( const FunctionColumn& rhs, const uint32_t sessionID = 0);
	virtual ~FunctionColumn();

	/** get function name
	 *
	 * get the function name for this function column
	 */
	inline const std::string& functionName() const
	{
		return fFunctionName;
	}

	/** set function name
	 *
	 * set the function name for this function column
	 */
	inline void functionName(const std::string functionName)
	{
		fFunctionName = functionName;
	}

	/** get function parameters
	 *
	 * get the function parameters for this function column.
	 * @return a vector of paramenters in string format
	 */
	inline const funcexp::FunctionParm& functionParms() const
	{
		return fFunctionParms;
	}

	/** set function parameters
	 *
	 * set the function parameters for this function column.
	 */
	void functionParms(const funcexp::FunctionParm& functionParms)
	{
		fFunctionParms = functionParms;
	}

	/** set function parameters
	 *
	 * set the function parameters for this function column.
	 * pass in the functionParms with parenthesis as one string.
	 * tokenize the string with ' ' or ',' and form a vector of
	 * parameters in string.
	 */
	void funcParms(const std::string& funcParmsInString);

	/** get table alias name
	 *
	 * get the table alias name for this function
	 */
	inline const std::string& tableAlias () const { return fTableAlias; }

	/** set table alias name
	 *
	 * set the table alias name for this function
	 */
	inline void tableAlias (const std::string& tableAlias) { fTableAlias = tableAlias; }

	virtual const std::string data() const;
	virtual void data(const std::string data) { fData = data; }

	virtual const std::string toString() const;

	/** return a copy of this pointer
	 *
	 * deep copy of this pointer and return the copy
	 */
	inline virtual FunctionColumn* clone() const
	{
		return new FunctionColumn (*this);
	}

	/**
	 * The serialization interface
	 */
	virtual void serialize(messageqcpp::ByteStream&) const;
	virtual void unserialize(messageqcpp::ByteStream&);

	virtual bool hasAggregate();
	virtual bool hasWindowFunc();
	virtual void setDerivedTable();
	virtual void replaceRealCol(std::vector<SRCP>&);
	virtual const std::vector<SimpleColumn*>& simpleColumnList() const
	{ return fSimpleColumnList; }

	virtual void setSimpleColumnList();
	/**
	 * Return the tableAlias name of the table that the column arguments belong to.
	 * 
	 * @param TableAliasName that will be set in the function
	 * @return true, if all arguments belong to one table
	 *         false, if multiple tables are involved in the function
	 */
	virtual bool singleTable(CalpontSystemCatalog::TableAliasName& tan);

private:
	/**
	 * Fields
	 */
	std::string fFunctionName;	/// function name
	std::string fTableAlias;	/// table alias which has the column
	std::string fData;			/// SQL representation

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
	bool operator==(const FunctionColumn& t) const;

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
	bool operator!=(const FunctionColumn& t) const;

	/***********************************************************
	 *				  F&E framework						  *
	 ***********************************************************/
public:
	virtual const std::string& getStrVal(rowgroup::Row& row, bool& isNull)
	{
		fResult.strVal = fFunctor->getStrVal(row, fFunctionParms, isNull, fOperationType);
		return fResult.strVal;
	}
	virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull)
	{
		return fFunctor->getIntVal(row, fFunctionParms, isNull, fOperationType);
	}
	virtual uint64_t getUintVal(rowgroup::Row& row, bool& isNull)
	{
		return fFunctor->getUintVal(row, fFunctionParms, isNull, fOperationType);
	}
	virtual float getFloatVal(rowgroup::Row& row, bool& isNull)
	{
		return fFunctor->getFloatVal(row, fFunctionParms, isNull, fOperationType);
	}
	virtual double getDoubleVal(rowgroup::Row& row, bool& isNull)
	{
		return fFunctor->getDoubleVal(row, fFunctionParms, isNull, fOperationType);
	}
	virtual IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull)
	{
		IDB_Decimal decimal = fFunctor->getDecimalVal(row, fFunctionParms, isNull, fOperationType);
		if (fResultType.scale == decimal.scale)
			return decimal;

		if (fResultType.scale > decimal.scale)
			decimal.value *= IDB_pow[fResultType.scale-decimal.scale];
		else
			decimal.value = (int64_t)(decimal.value > 0 ?
			                (double)decimal.value/IDB_pow[decimal.scale-fResultType.scale] + 0.5 :
			                (double)decimal.value/IDB_pow[decimal.scale-fResultType.scale] - 0.5);
		decimal.scale = fResultType.scale;
		decimal.precision = fResultType.precision;
		return decimal;
	}
	virtual bool getBoolVal(rowgroup::Row& row, bool& isNull)
	{
		return fFunctor->getBoolVal(row, fFunctionParms, isNull, fOperationType);
	}
	virtual int32_t getDateIntVal(rowgroup::Row& row, bool& isNull)
	{
		return fFunctor->getDateIntVal(row, fFunctionParms, isNull, fOperationType);
	}
	virtual int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull)
	{
		return fFunctor->getDatetimeIntVal(row, fFunctionParms, isNull, fOperationType);
	}


private:
	funcexp::FunctionParm fFunctionParms;
	funcexp::Func* fFunctor;   /// functor to execute this function
};

/**
* ostream operator
*/
std::ostream& operator<<(std::ostream& output, const FunctionColumn& rhs);

}
#endif //FUNCTIONCOLUMN_H

