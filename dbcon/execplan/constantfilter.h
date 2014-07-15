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
*   $Id: constantfilter.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef CONSTANTFILTER_H
#define CONSTANTFILTER_H
#include <string>
#include <iosfwd>
#include <vector>

#include "filter.h"
#include "simplefilter.h"
#include "simplecolumn.h"
#include "operator.h"

/**
 * Namespace
 */
namespace execplan {

class ReturnedColumn;
class AggregateColumn;
class WindowFunctionColumn;

/**
 * @brief A class to represent a simple column op constant predicate
 *
 * This class is a specialization of class Filter that handles one or
 * more simple filters, where one side of operand is a constant. It
 * contains a list of simple filters and every simplefilter has the
 * same column. All the simple filters are connected by the same
 * operator (either "and" or "or"). This class is introduced for
 * easy operation combine for primitive processor.
 */
class ConstantFilter : public Filter {

public:
    /**
	 * Types and constants
	 */
    typedef std::vector<SSFP> FilterList;

	/**
	 * Constructors
	 */
	ConstantFilter();
	ConstantFilter(const SOP& op, ReturnedColumn* lhs, ReturnedColumn* rhs);
	ConstantFilter(SimpleFilter* sf);

	//not needed yet
	//ConstantFilter(const ConstantFilter& rhs);

	/**
	 * Destructors
	 */
	virtual ~ConstantFilter();

	/**
	 * Accessor Methods
	 */
    const FilterList& filterList() const {return fFilterList;}
    void filterList( const FilterList& filterList) {fFilterList = filterList; }
    const SOP& op() const {return fOp;}
    void op(const SOP& op){fOp = op;}
    const SRCP& col() const {return fCol;}
    void col(const SRCP& col) {fCol = col;}

	/**
	 * Operations
	 */
    void pushFilter (SimpleFilter* sf) {SSFP ssfp(sf); fFilterList.push_back(ssfp);}

	//virtual const std::string data() const;
	virtual const std::string toString() const;

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
	bool operator==(const ConstantFilter& t) const;

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
	bool operator!=(const ConstantFilter& t) const;

	/** @brief test if this filter can be combined with the argument filter
	  *  This is for operation combine optimization
	  *  @param f the filter that this fiter tries to combine with
	  *  @param op the operator that connects the two filters. if one or both of the
	  *  two filters is constantFilter, need to make sure operator is consistant.
	  *  @return a filter(constantfilter) if successfully combined. otherwise
	  *     	 return NULL
	  */
	// Used by Oracle frontend, deprecated now. 
	//virtual Filter* combinable(Filter* f, Operator* op);

	/** get function name
	 *
	 * get the function name for this function column
	 */
	std::string functionName() const { return fFunctionName; }

	/** set function name
	 *
	 * set the function name for this function column
	 */
	void functionName(const std::string& functionName) { fFunctionName = functionName; }

	void setDerivedTable();
	virtual void replaceRealCol(std::vector<SRCP>&);
	virtual bool hasAggregate();

private:
	SOP fOp;       /// connect operator (and or)
	FilterList fFilterList; /// vector of simple filters
	SRCP fCol;     /// the common column
	std::string fFunctionName;  /// function name

	/***********************************************************
	 *                  F&E framework                          *
	 ***********************************************************/
public:
	ConstantFilter(const ConstantFilter& rhs);
	inline virtual ConstantFilter* clone() const
	{
	    return new ConstantFilter (*this);
	}

	inline virtual bool getBoolVal(rowgroup::Row& row, bool& isNull);

	// get all simple columns involved in this column
	const std::vector<SimpleColumn*>& simpleColumnList();
	// walk through the constant filter operands to re-populate fSimpleColumnList
	void setSimpleColumnList();

	// get all aggregate columns involved in this column
	const std::vector<AggregateColumn*>& aggColumnList() const { return fAggColumnList; }

private:
	std::vector<SimpleColumn*> fSimpleColumnList;
	std::vector<AggregateColumn*> fAggColumnList;
	std::vector<WindowFunctionColumn*> fWindowFunctionColumnList;
};

inline bool ConstantFilter::getBoolVal(rowgroup::Row& row, bool& isNull)
{
	switch(fOp->op())
	{
		case OP_AND:
			for (uint32_t i = 0; i < fFilterList.size(); i++)
				if (!fFilterList[i]->getBoolVal(row, isNull))
					return false;
			return true;
		case OP_OR:
			for (uint32_t i = 0; i < fFilterList.size(); i++)
				if (fFilterList[i]->getBoolVal(row, isNull))
					return true;
			return false;
		default:
		{
			std::ostringstream oss;
			oss << "ConstantFilter:: Non support logic operation: " << fOp->op();
			throw logging::InvalidOperationExcept(oss.str());
		}
	}
}

std::ostream& operator<<(std::ostream& output, const ConstantFilter& rhs);

}
#endif //CONSTANTFILTER_H

