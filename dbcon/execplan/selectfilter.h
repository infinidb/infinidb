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
*   $Id: selectfilter.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef SELECTFILTER_H
#define SELECTFILTER_H
#include <string>

#include "filter.h"
#include "returnedcolumn.h"
#include "operator.h"
#include "calpontselectexecutionplan.h"

/**
 * Namespace
 */
namespace execplan { 
/**
 * @brief A class to represent a subselect predicate
 *
 * This class is a specialization of class Filter that handles a
 * subselect predicate like "col1 = (select col2 from table2)"
 */
class SelectFilter : public Filter {
/**
 * Public stuff
 */
public:
	
	/**
	 * Constructors / copy constructor
	 */
	SelectFilter();
	SelectFilter(const SelectFilter& rhs);

	/** Constructors
	 * 
	 * pass all parts in ctor
	 * @note SimpleFilter takes ownership of all these pointers
	 */
	SelectFilter(const std::vector<SRCP>& cols, const SOP& op, SCSEP& sub, bool correlated = false);
	
	/**
	 * Destructors
	 */
	virtual ~SelectFilter();
	
	/**
	 * Accessor Methods
	 */
	inline const std::vector<SRCP>& cols() const { return fCols; }
	
	void cols(const std::vector<SRCP>& cols) { fCols = cols; }
	
	inline const SOP& op() const { return fOp; }
	
	inline void op (const SOP& op) { fOp = op; }
	
	inline const SCSEP& sub() const {	return fSub; }
	
	inline void sub(SCSEP& sub) { fSub = sub;	}
	
	const bool correlated() const { return fCorrelated; }
	void correlated(const bool correlated) { fCorrelated = correlated; }

	virtual const std::string toString() const;
	
	virtual inline const std::string data() const {return fData;}
	virtual inline void data( const std::string data ) {fData = data;}
	
	const uint64_t returnedColPos() const { return fReturnedColPos; }
	void returnedColPos(const uint64_t returnedColPos) {fReturnedColPos = returnedColPos;}
	
	/**
	 * The serialization interface
	 */
	virtual void serialize(messageqcpp::ByteStream&) const;
	virtual void unserialize(messageqcpp::ByteStream&);
		
	/** return a copy of this pointer
	 *
	 * deep copy of this pointer and return the copy
	 */	
	inline virtual SelectFilter* clone() const
	{
	    return new SelectFilter (*this);
	}	

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
	bool operator==(const SelectFilter& t) const;
	
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
	bool operator!=(const SelectFilter& t) const;
	
private:
	//default okay?
	//SelectFilter& operator=(const SelectFilter& rhs);

	 std::vector<SRCP> fCols;
	 SOP fOp;
	 SCSEP fSub;
	 bool fCorrelated;
	 std::string fData;
	 uint64_t fReturnedColPos;	// offset in fSub->returnedColList to indicate the start of projection
};

std::ostream& operator<<(std::ostream& output, const SelectFilter& rhs);

} 
#endif //SELECTFILTER_H

