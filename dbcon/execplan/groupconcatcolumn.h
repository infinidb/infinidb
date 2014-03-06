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
*   $Id: groupconcatcolumn.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef GROUPCONCATCOLUMN_H
#define GROUPCONCATCOLUMN_H
#include <string>

#include "calpontselectexecutionplan.h"
#include "aggregatecolumn.h"

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
class GroupConcatColumn : public AggregateColumn {

public:

	/**
	 * Constructors
	 */
	GroupConcatColumn();

	GroupConcatColumn(const uint32_t sessionID);

	GroupConcatColumn(const GroupConcatColumn& rhs, const uint32_t sessionID=0);

	/**
	 * Destructors
	 */
	virtual ~GroupConcatColumn();

	/**
	 * Overloaded stream operator
	 */
	virtual const std::string toString() const;

	/** return a copy of this pointer
	 *
	 * deep copy of this pointer and return the copy
	 */
	virtual GroupConcatColumn* clone() const { return new GroupConcatColumn(*this); }

	/**
	 * Accessors and Mutators
	 */
	void orderCols(const std::vector<SRCP>& orderCols) { fOrderCols = orderCols; }
	std::vector<SRCP>& orderCols() { return fOrderCols; }
	void separator(const std::string& separator) { fSeparator = separator; }
	std::string& separator() { return fSeparator; }

	/**
	 * Serialize interface
	 */
	virtual void serialize(messageqcpp::ByteStream&) const;
	virtual void unserialize(messageqcpp::ByteStream&);

	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return true iff every member of t is a duplicate copy of every member of this;
	 *         false otherwise
	 */
	virtual bool operator==(const TreeNode* t) const;

	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return true iff every member of t is a duplicate copy of every member of this;
	 *         false otherwise
	 */
	virtual bool operator==(const GroupConcatColumn& t) const;

	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return false iff every member of t is a duplicate copy of every member of this;
	 *         true otherwise
	 */
	virtual bool operator!=(const TreeNode* t) const;

	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return false iff every member of t is a duplicate copy of every member of this;
     *         true otherwise
	 */
	virtual bool operator!=(const GroupConcatColumn& t) const;

private:
	std::vector<SRCP> fOrderCols;
	std::string fSeparator;
};

/**
* stream operator
*/
std::ostream& operator<<(std::ostream& os, const GroupConcatColumn& rhs);

}
#endif //GROUPCONCATCOLUMN_H

