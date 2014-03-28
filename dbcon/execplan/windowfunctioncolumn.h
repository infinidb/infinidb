/* Copyright (C) 2013 Calpont Corp.

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
*   $Id: windowfunctioncolumn.h 9679 2013-07-11 22:32:03Z zzhu $
*
*
***********************************************************************/

/** @file */

#ifndef WINDOW_FUNCTION_COLUMN_H
#define WINDOW_FUNCTION_COLUMN_H
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

// This enum is made consistant with mysql item_func_window
enum WF_FRAME
{
	WF_PRECEDING = 0,
	WF_FOLLOWING,
	WF_UNBOUNDED_PRECEDING,
	WF_UNBOUNDED_FOLLOWING,
	WF_CURRENT_ROW,
	WF_UNKNOWN
};

struct WF_Boundary
{
	WF_Boundary() {}
	WF_Boundary(WF_FRAME frame):fFrame(frame) {}
	~WF_Boundary() {}
	const std::string toString() const;
	void serialize(messageqcpp::ByteStream&) const;
	void unserialize(messageqcpp::ByteStream&);
	SRCP fVal;  /// has to evaluate to unsigned value
	SRCP fBound; /// order by col +, -, date_add or date_sub for RANGE window
	enum WF_FRAME fFrame;
};

struct WF_Frame
{
	WF_Frame(): fIsRange(true)
	{
		fStart.fFrame = WF_UNBOUNDED_PRECEDING;
		fEnd.fFrame = WF_UNBOUNDED_FOLLOWING;
	}
	~WF_Frame() {}
	const std::string toString() const;
	void serialize(messageqcpp::ByteStream&) const;
	void unserialize(messageqcpp::ByteStream&);
	WF_Boundary fStart;
	WF_Boundary fEnd;
	bool fIsRange; /// RANGE or ROWS
};

/**
 * @brief A class to represent the order by clause of window function
 */
struct WF_OrderBy
{
	WF_OrderBy() {}
	WF_OrderBy(std::vector<SRCP> orders): fOrders(orders) {}
	~WF_OrderBy() {};
	const std::string toString() const;
	void serialize(messageqcpp::ByteStream&) const;
	void unserialize(messageqcpp::ByteStream&);
	std::vector<SRCP> fOrders;
	WF_Frame fFrame;
};

/**
 * @brief A class to represent a functional column
 * 
 * This class is a specialization of class ReturnedColumn that
 * handles a window function.
 */
class WindowFunctionColumn : public ReturnedColumn {

public:
	WindowFunctionColumn();
	WindowFunctionColumn(const std::string& functionName, const uint32_t sessionID = 0);
	WindowFunctionColumn(const WindowFunctionColumn& rhs, const uint32_t sessionID = 0);
	virtual ~WindowFunctionColumn() {}
	
	/** get function name */
	inline const std::string& functionName() const
	{
		return fFunctionName;
	}
	
	/** set function name */
	inline void functionName(const std::string functionName)
	{
		fFunctionName = functionName;
	}
	
	/** get function parameters */
	inline const std::vector<SRCP>& functionParms() const
	{
		return fFunctionParms;
	}
	
	/** set function parameters*/
	inline void functionParms(const std::vector<SRCP>& functionParms)
	{
		fFunctionParms = functionParms;
	}
	
	/** get partition columns */
	inline const std::vector<SRCP>& partitions() const
	{
		return fPartitions;
	}
	
	/** set partition columns */
	inline void partitions(const std::vector<SRCP>& partitions)
	{
		fPartitions = partitions;
	}
	
	/** get order by clause */
	inline const WF_OrderBy& orderBy() const
	{
		return fOrderBy;
	}
	
	/** set order by clause */
	inline void orderBy(const WF_OrderBy& orderBy)
	{
		fOrderBy = orderBy;
	}
	
	/** make a clone of this window function */	
	inline virtual WindowFunctionColumn* clone() const
	{
		return new WindowFunctionColumn (*this);
	}
	
	std::vector<SRCP> getColumnList() const;
	
	/** output the function for debug purpose */
	const std::string toString() const;
		
	/**
	 * The serialization interface
	 */
	virtual void serialize(messageqcpp::ByteStream&) const;
	virtual void unserialize(messageqcpp::ByteStream&);
		
	// util function for connector to use. 
	void addToPartition(std::vector<SRCP>& groupByList);

	virtual bool hasAggregate() {return false;}
	virtual bool hasWindowFunc();
	void adjustResultType();

private:
	/**
	 * Fields
	 */
	std::string fFunctionName;					/// function name
	std::vector<SRCP> fFunctionParms;		/// function arguments
	std::vector<SRCP> fPartitions;			/// partition by clause
	WF_OrderBy fOrderBy;								/// order by clause
	
	// not support for window functions for now.
	virtual bool operator==(const TreeNode* t) const { return false; }
	bool operator==(const WindowFunctionColumn& t) const;
	virtual bool operator!=(const TreeNode* t) const { return false; }
	bool operator!=(const WindowFunctionColumn& t) const;
	
	/***********************************************************
	 *                 F&E framework                           *
	 ***********************************************************/
public:
	virtual const std::string& getStrVal(rowgroup::Row& row, bool& isNull) 
	{ 
		evaluate(row, isNull);
		return TreeNode::getStrVal(); 
	}
	
	virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull) 
	{ 
		evaluate(row, isNull);
		return TreeNode::getIntVal(); 
	}

	virtual uint64_t getUintVal(rowgroup::Row& row, bool& isNull) 
	{ 
		evaluate(row, isNull);
		return TreeNode::getUintVal(); 
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
* ostream operator
*/
std::ostream& operator<<(std::ostream& output, const WindowFunctionColumn& rhs);

/**
 * utility function to extract all window function columns from a parse tree
 */
void getWindowFunctionCols(ParseTree* n, void* obj);

} 
#endif //WINDOW_FUNCTION_COLUMN_H
