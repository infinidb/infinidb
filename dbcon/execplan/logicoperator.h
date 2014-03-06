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
*   $Id: logicoperator.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef LOGICOPERATOR_H
#define LOGICOPERATOR_H
#include <string>
#include <iosfwd>
#include <boost/shared_ptr.hpp>

//#include "expressionparser.h"
#include "operator.h"
#include "parsetree.h"

namespace messageqcpp {
class ByteStream;
}

namespace rowgroup {
class Row;
}

/**
 * Namespace
 */
namespace execplan { 
	class ParseTree;
/**@brief a class to represent an operator 
 * 
 * This class is a representation of an predicate operator as 
 * "AND, OR, LIKE" or arithmetic operator as "+, -, *, /, (, ), 
 * "unary+, unary-, function(, function)"
 */

class LogicOperator : public Operator {

/**
 * Public stuff
 */
public:
	/**
	 * Constructors
	 */
	LogicOperator();
	LogicOperator(const std::string& operatorName);
	LogicOperator(const LogicOperator& rhs);
	
	/**
	 * Destructors
	 */
	virtual ~LogicOperator();
	
	/**
	 * Accessor Methods
	 */
	
  /** return a copy of this pointer
	 *
	 * deep copy of this pointer and return the copy
	 */	
	inline virtual LogicOperator* clone() const
	{
	    return new LogicOperator (*this);
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
	bool operator==(const LogicOperator& t) const;
	
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
	bool operator!=(const LogicOperator& t) const;
	//template <typename result_t>
	//result_t evaluate(result_t op1, result_t op2);
	
	// F&E framework
	inline virtual bool getBoolVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) 
	{
		switch (fOp)
		{
			case OP_AND:
				return (lop->getBoolVal(row, isNull) && rop->getBoolVal(row, isNull));
			case OP_OR:
			{
				if (lop->getBoolVal(row, isNull))
					return true;
				isNull = false;
				//return (lop->getBoolVal(row, isNull) || rop->getBoolVal(row, isNull));
				return rop->getBoolVal(row, isNull);
			}
			case OP_XOR:
			{
				// Logical XOR. Returns NULL if either operand is NULL. 
				// For non-NULL operands, evaluates to 1 if an odd number of operands is nonzero, 
				// otherwise 0 is returned. 
				bool lopv = lop->getBoolVal(row, isNull);
				if (isNull)
					return false;
				bool ropv = rop->getBoolVal(row, isNull);
				if (isNull)
					return false;
				if ((lopv && !ropv) || (ropv && !lopv))
					return true;
				else
					return false;
			}
			default:
				throw std::runtime_error("invalid logical operation");
		}
	}
	
	inline virtual void evaluate(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) 
	{
		fResult.boolVal = getBoolVal(row, isNull, lop, rop);
	}
	
private:
	// default okay
	//Operator& operator=(const Operator& rhs);
	//std::string fData;	
	
};

//typedef boost::shared_ptr<Operator> SOP;

std::ostream& operator<<(std::ostream& os, const LogicOperator& rhs);
} 

#endif

