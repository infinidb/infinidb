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
*   $Id: simplefilter.h 8816 2012-08-15 18:51:49Z dhall $
*
*
***********************************************************************/
/** @file */

#ifndef SIMPLEFILTER_H
#define SIMPLEFILTER_H
#include <string>
#include <iosfwd>
#include <boost/shared_ptr.hpp>

#include "filter.h"
#include "predicateoperator.h"
#include "returnedcolumn.h"

/**
 * Namespace
 */
namespace execplan { 
	class AggregateColumn;
	
/**
 * @brief A class to represent a simple WHERE clause predicate
 *
 * This class is a specialization of class Filter that handles simple 
 * binary operation WHERE clauses, e.g. last_name = ‘SMITH’.
 */ 
class SimpleFilter : public Filter {

public:
    /** index flag */
    enum IndexFlag
    {
        NOINDEX = 0,
        LEFT,
        RIGHT,
        BOTH
    };
    
    enum JoinFlag
    {
        EQUA = 0,
        ANTI,
        SEMI
    };

	SimpleFilter();
	SimpleFilter(const std::string& sql);
	SimpleFilter(const SOP& op, ReturnedColumn* lhs, ReturnedColumn* rhs);
	SimpleFilter(const SimpleFilter& rhs);
	
	virtual ~SimpleFilter();
	
	inline virtual SimpleFilter* clone() const
	{
	    return new SimpleFilter (*this);
	}
	
	const SOP& op() const
	{
		return fOp;
	}
	
	void op(const SOP& op)
	{
		fOp = op;
	}
	
	inline ReturnedColumn* lhs() const
	{
		return fLhs;
	}
	
	virtual const std::string data() const;
	
	/** assign fLhs
	 *
	 * this call takes over the ownership of the input pointer. Caller needs
	 * to put the input pointer to 0 after the call; or to remember not to
	 * delete the pointer accidentally.
	 */
	void lhs(ReturnedColumn* lhs);
	
	inline ReturnedColumn* rhs() const
	{
		return fRhs;
	}
	
	/** assign fRhs
	 *
	 * this call takes over the ownership of the pointer rhs. Called need
	 * to put the input pointer to 0 after the all; or remember not to
	 * delete the pointer accidentally.
	 */
	void rhs(ReturnedColumn* rhs);
		
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
	bool operator==(const SimpleFilter& t) const;
	
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
	bool operator!=(const SimpleFilter& t) const;
	
	/** @brief determin if this simple filter is a pure filter (col/const)
	 */
	bool pureFilter();
	
	/** @brief test if this filter can be combined with the argument filter
	  *  This is for operation combine optimization
	  *  @param f the filter that this fiter tries to combine with
	  *  @param op the operator that connects the two filters. if one or both of the
	  *  two filters is constantFilter, need to make sure operator is consistant.
	  *  @return a filter(constantfilter) if successfully combined. otherwise
	  *     	 return NULL
	  */
	 virtual Filter* combinable(Filter* f, Operator* op);
	 
	 /** @brief assign indexflag to indicate which side col is index */
	 void indexFlag (const int indexFlag) {fIndexFlag = indexFlag;}
	 const int indexFlag() const {return fIndexFlag;}
	 
	 /** @brief assign joinflag to indicate which join type */
	 void joinFlag (const int joinFlag) {fJoinFlag = joinFlag;}
	 const int joinFlag() const {return fJoinFlag;}
	 
	 /** @brief this function is called by the connector to set constant values according to the compare type */
	 void convertConstant();
	
private:
	SOP fOp;			/// operator
	ReturnedColumn *fLhs;	/// left operand
	ReturnedColumn *fRhs;	/// right operand
	int fIndexFlag;         /// which side col is index	 
	int fJoinFlag;          /// hash join type

	void parse (std::string);
	
	/***********************************************************
	 *                  F&E framework                          *
	 ***********************************************************/
public:
	inline virtual bool getBoolVal(rowgroup::Row& row, bool& isNull);
	inline virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull);
	inline virtual double getDoubleVal(rowgroup::Row& row, bool& isNull);

	// get all simple columns involved in this column
	const std::vector<SimpleColumn*>& simpleColumnList() const { return fSimpleColumnList; }
	
	// get all aggregate columns involved in this column
	const std::vector<AggregateColumn*>& aggColumnList() const { return fAggColumnList; }
	
private:
	std::vector<SimpleColumn*> fSimpleColumnList;
	std::vector<AggregateColumn*> fAggColumnList;
};

inline bool SimpleFilter::getBoolVal(rowgroup::Row& row, bool& isNull)
{
	return (reinterpret_cast<PredicateOperator*>(fOp.get())->getBoolVal(row, isNull, fLhs, fRhs));
}

inline int64_t SimpleFilter::getIntVal(rowgroup::Row& row, bool& isNull)
{
	return (reinterpret_cast<PredicateOperator*>(fOp.get())->getBoolVal(row, isNull, fLhs, fRhs)? 1 : 0);
}

inline double SimpleFilter::getDoubleVal(rowgroup::Row& row, bool& isNull)
{
	return getIntVal(row, isNull);
}

typedef boost::shared_ptr<SimpleFilter> SSFP;

std::ostream& operator<<(std::ostream& output, const SimpleFilter& rhs);

} 
#endif //SIMPLEFILTER_H

