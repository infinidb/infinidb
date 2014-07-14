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
*   $Id: arithmeticcolumn.h 9679 2013-07-11 22:32:03Z zzhu $
*
*
***********************************************************************/
/** @file */

#ifndef ARITHMETICCOLUMN_H
#define ARITHMETICCOLUMN_H
#include <string>
#include <iosfwd>
#include <stack>

#include "returnedcolumn.h"
#include "expressionparser.h"

/**
 * Namespace
 */
namespace execplan {
	class SimpleColumn;
	class AggregateColumn;

/**
 * @brief A class to represent a arithmetic expression return column
 *
 * This class is a specialization of class ReturnedColumn that
 * handles an arithmetic expression.
 */
class ArithmeticColumn : public ReturnedColumn {
public:
	ArithmeticColumn();
	ArithmeticColumn( const std::string& sql, const uint32_t sessionID=0 );
	ArithmeticColumn( const ArithmeticColumn& rhs, const uint32_t sessionID=0 );
	virtual ~ArithmeticColumn();

	inline ParseTree* expression() const
	{
	    return fExpression;
	}

	/** get table alias name
	 *
	 * get the table alias name for this arithmetic function
	 */
	inline const std::string& tableAlias () const
	{
		return fTableAlias;
	}

	/** set table alias name
	 *
	 * set the table alias name for this arithmetic function
	 */
	inline void tableAlias (const std::string& tableAlias)
	{
	    fTableAlias = tableAlias;
	}

	/** set the parse tree
	 *
	 * set the parse tree to expression.
	 * @note this object takes ownership of the tree. The supplied pointer is set to null.
	 */
	void expression (ParseTree*& expression);

	/**
	 * get asc flag
	 */
	inline const bool asc() const { return fAsc; }

	/**
	 * set asc flag
	 */
	inline void asc(const bool asc) { fAsc = asc; }

	/**
	 * get SQL representation of this object
	 */
	virtual const std::string data() const { return fData; }

	/**
	 * set SQL representation of this object
	 */
	virtual void data(const std::string data) { fData = data; }

	/**
	 * virtual stream method
	 */
	virtual const std::string toString() const;

	/** return a copy of this pointer
	 *
	 * deep copy of this pointer and return the copy
	 */
	inline virtual ArithmeticColumn* clone() const
	{
		return new ArithmeticColumn (*this);
	}

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
	bool operator==(const ArithmeticColumn& t) const;

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
	bool operator!=(const ArithmeticColumn& t) const;

	virtual bool hasAggregate();
	virtual bool hasWindowFunc();

	virtual void setDerivedTable();
	virtual void replaceRealCol(std::vector<SRCP>&);
	virtual const std::vector<SimpleColumn*>& simpleColumnList() const
	{ return fSimpleColumnList; }
	virtual void setSimpleColumnList();

	/**
	 * Return the table that the column arguments belong to.
	 * 
	 * @return tablename, if all arguments belong to one table
	 *         empty string "", if multiple tables are involved in this func
	 */
	virtual bool singleTable(CalpontSystemCatalog::TableAliasName& tan);

private:
	std::string fTableAlias;   // table alias for this column
	bool fAsc;                 // asc flag for order by column
	std::string fData;

	/** build expression tree
	 *
	 * this function is called by the constructor. the incomming
	 * sql string is parsed and tokenized and built into a
	 * parse tree.
	 */
	void buildTree();

	/** get next token from expression
	 *
	 * this is a util function used by buildTree(). next token string
	 * is retrived from expression from curPos, until end char.
	 * return the retrived token. curPos reference is updated to the
	 * new position after end char
	 */
	const std::string nextToken(std::string::size_type &curPos, char end) const;

	/***********************************************************
	 *                 F&E framework                           *
	 ***********************************************************/
public:
	virtual const std::string& getStrVal(rowgroup::Row& row, bool& isNull)
	{
		return fExpression->getStrVal(row, isNull);
	}

	virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull)
	{
		return fExpression->getIntVal(row, isNull);
	}

    virtual uint64_t getUintVal(rowgroup::Row& row, bool& isNull)
    {
        return fExpression->getUintVal(row, isNull);
    }

	virtual float getFloatVal(rowgroup::Row& row, bool& isNull)
	{
		return fExpression->getFloatVal(row, isNull);
	}

	virtual double getDoubleVal(rowgroup::Row& row, bool& isNull)
	{
		return fExpression->getDoubleVal(row, isNull);
	}

	virtual IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull)
	{
		return fExpression->getDecimalVal(row, isNull);
	}

	virtual int32_t getDateIntVal(rowgroup::Row& row, bool& isNull)
	{
		return fExpression->getDateIntVal(row, isNull);
	}

	virtual int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull)
	{
		return fExpression->getDatetimeIntVal(row, isNull);
	}

	virtual bool getBoolVal(rowgroup::Row& row, bool& isNull)
	{
		return fExpression->getBoolVal(row, isNull);
	}


private:
	ParseTree* fExpression;
	void evaluate(rowgroup::Row& row) {}
};

/**
*  ostream operator
*/
std::ostream& operator<<(std::ostream& output, const ArithmeticColumn& rhs);

}
#endif //ARITHMETICCOLUMN_H

