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
*   $Id: filter.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef EXECPLAN_FILTER_H
#define EXECPLAN_FILTER_H
#include <string>
#include <iosfwd>

#include "treenode.h"

namespace messageqcpp {
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan {

class Operator;

/** @brief A class to represent a generic filter predicate
 *
 * ******************************* Abstract Class ****************************
 * Filter does not have any pure virtual methods, but its author
 *   defined it as an abstract class, so you should not use it directly.
 *   Inherit from it instead and create only objects from the derived classes
 * *****************************************************************************
 */
class Filter : public TreeNode {
//friend std::ostream &operator<< (std::ostream &, Filter &);

/**
 * Public stuff
 */
public:

	/**
	 * Constructors
	 */
	Filter();
	Filter(const std::string& sql);
	// not needed yet
	//Filter(const Filter& rhs);

	/**
	 * Destructors
	 */
	virtual ~Filter();
	/**
	 * Accessor Methods
	 */

	/**
	 * Operations
	 */
	virtual const std::string toString() const;

	virtual const std::string data() const { return fData; }
	virtual void data(const std::string data) { fData = data; }

    /** return a copy of this pointer
	 *
	 * deep copy of this pointer and return the copy
	 */
	inline virtual Filter* clone() const
	{
	    return new Filter (*this);
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
	bool operator==(const Filter& t) const;

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
	bool operator!=(const Filter& t) const;

	 /** @brief test if this filter can be combined with the argument filter
	  *  This is for operation combine optimization
	  *  @param f the filter that this fiter tries to combine with
	  *  @param op the operator that connects the two filters. if one or both of the
	  *  two filters is constantFilter, need to make sure operator is consistant.
	  *  @return a filter(constantfilter) if successfully combined. otherwise
	  *     	 return NULL
	  * For Oracle front end. Deprecated now.
	  */
	//virtual Filter* combinable(Filter* f, Operator* op);
	virtual uint64_t cardinality () const { return fCardinality; }
	virtual void cardinality (const uint64_t cardinality) { fCardinality = cardinality;}

protected:
    uint64_t fCardinality;

private:
	//default okay
	//Filter& operator=(const Filter& rhs);

	std::string fData;

};

std::ostream& operator<<(std::ostream& os, const Filter& rhs);

}
#endif //EXECPLAN_FILTER_H

