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
*   $Id: aggregatecolumn.h 8436 2012-04-04 18:18:21Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef AGGREGATECOLUMN_H
#define AGGREGATECOLUMN_H
#include <string>

#include "calpontselectexecutionplan.h"
#include "returnedcolumn.h"

namespace messageqcpp {
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan { 
/**
 * @brief A class to represent a aggregate return column
 * 
 * This class is a specialization of class ReturnedColumn that
 * handles an aggregate function call (e.g., SUM, COUNT, MIN, MAX).
 */
class AggregateColumn : public ReturnedColumn {

public:
	enum AggOp
	{
		NOOP = 0,
		COUNT_ASTERISK,
		COUNT,
		SUM,
		AVG,
		MIN,
		MAX,
		CONSTANT,
		DISTINCT_COUNT,
		DISTINCT_SUM,
		DISTINCT_AVG,
		STDDEV_POP,
		STDDEV_SAMP,
		VAR_POP,
		VAR_SAMP,
		BIT_AND,
		BIT_OR,
		BIT_XOR,
		GROUP_CONCAT
	};

	typedef std::vector<SRCP> ColumnList;
	/**
	 * Constructors
	 */
	AggregateColumn();
	
	AggregateColumn(const u_int32_t sessionID);

	AggregateColumn(const AggOp aggop, ReturnedColumn* parm, const u_int32_t sessionID=0);
	
	AggregateColumn(const AggOp aggop, const std::string& content, const u_int32_t sessionID=0);

	AggregateColumn(const std::string& functionName, ReturnedColumn* parm, const u_int32_t sessionID=0);
	
	AggregateColumn(const std::string& functionName, const std::string& content, const u_int32_t sessionID=0);
	
	AggregateColumn( const AggregateColumn& rhs, const u_int32_t sessionID=0 );
	
	/**
	 * Destructors
	 */
	virtual ~AggregateColumn();
		 
	/**
	 * Accessor Methods
	 */
	inline virtual const std::string& functionName() const
	{
		return fFunctionName;
	}
	
	virtual void functionName(const std::string& functionName);
		
	virtual const u_int8_t aggOp() const {return fAggOp;}
	virtual void aggOp(const u_int8_t aggOp) {fAggOp = aggOp;}
	
	/** get function parms
	 *
	 * set the function parms from this object
	 */
	virtual const SRCP& functionParms() const
	{
		return fFunctionParms;
	}
	
	/** set function parms
	 *
	 * set the function parms for this object
	 */
	virtual void functionParms(const SRCP& functionParms);
    
  /** return a copy of this pointer
	 *
	 * deep copy of this pointer and return the copy
	 */	
	inline virtual AggregateColumn* clone() const
	{
	    return new AggregateColumn (*this);
	}
    
	/**
   * table alias name
   */
	virtual const std::string& tableAlias() const { return fTableAlias; }
	virtual void tableAlias (const std::string& tableAlias) { fTableAlias = tableAlias; }
    
  /**
   * ASC flag
   */
	inline virtual const bool asc() const { return fAsc; }
	inline virtual void asc(const bool asc) { fAsc = asc; }	

	/**
	 * fData: SQL representation of this object
	 */
	virtual const std::string data() const { return fData; }
	virtual void data(const std::string& data) { fData = data; }
		
	/**
	 * Overloaded stream operator
	 */
	virtual const std::string toString() const;

	/**
	 * Serialize interface
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
	virtual bool operator==(const AggregateColumn& t) const;
	
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
	virtual bool operator!=(const AggregateColumn& t) const;
	
	/** @brief push back arg to group by column list*/
	virtual void addGroupByCol(SRCP ac) {fGroupByColList.push_back(ac);}
	
	/** @brief push back arg to project by column list*/
	virtual void addProjectCol(SRCP ac) {fProjectColList.push_back(ac);}
	virtual const ColumnList& groupByColList() const { return fGroupByColList;}
	virtual const ColumnList& projectColList() const { return fProjectColList;}
	
	/** @brief constant argument for aggregate with constant */
	inline const SRCP& constCol() const { return fConstCol; }
	inline void constCol(const SRCP& constCol) { fConstCol = constCol; }	

protected:
	std::string fFunctionName;	// deprecated field
	u_int8_t fAggOp;

	/**
	 * A ReturnedColumn objects that are the arguments to this function
	 */
	SRCP fFunctionParms;
	
	/** table alias
	 * A string to represent table alias name which contains this column
	 */ 
	std:: string fTableAlias;
	
	/**
	 * Flag to indicate asc or desc order for order by column
	 */
	bool fAsc;    	
	std::string fData;
	ColumnList fGroupByColList;
	ColumnList fProjectColList;
	SRCP fConstCol;
	
	/***********************************************************
	 *                 F&E framework                           *
	 ***********************************************************/
public:
	virtual std::string getStrVal(rowgroup::Row& row, bool& isNull) 
	{ 
		evaluate(row, isNull);
		return TreeNode::getStrVal(); 
	}
	
	virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull) 
	{ 
		evaluate(row, isNull);
		return TreeNode::getIntVal(); 
	}
	
	virtual float getFloatVal(rowgroup::Row& row, bool& isNull)
	{
		evaluate(row, isNull);
		return TreeNode::getFloatVal();
	}
	
	virtual double getDoubleVal(rowgroup::Row& row, bool& isNull) 
	{
		evaluate(row, isNull);
		return TreeNode::getDoubleVal(); 
	}
	
	virtual IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull)
	{
		evaluate(row, isNull);
		return TreeNode::getDecimalVal();
	}
	virtual int32_t getDateIntVal(rowgroup::Row& row, bool& isNull)
	{
		evaluate(row, isNull);
		return TreeNode::getDateIntVal();
	}
	virtual int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull)
	{
		evaluate(row, isNull);
		return TreeNode::getDatetimeIntVal();
	}
	
private:
	void evaluate(rowgroup::Row& row, bool& isNull);
};

/**
* stream operator
*/
std::ostream& operator<<(std::ostream& os, const AggregateColumn& rhs);
/**
 * utility function to extract all aggregate columns from a parse tree
 */
void getAggCols(ParseTree* n, void* obj);

} 
#endif //AGGREGATECOLUMN_H

