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
*   $Id: rowcolumn.h 6309 2010-03-04 19:33:12Z zzhu $
*
*
***********************************************************************/
/** @file */

#ifndef ROWCOLUMN_H
#define ROWCOLUMN_H
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "returnedcolumn.h"
#include "treenode.h"
#include "calpontsystemcatalog.h"
#include "dataconvert.h"

namespace messageqcpp {
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan { 

class ParseTree;
/**
 * @brief A class to represent a simple returned column
 * 
 * This class is a specialization of class ReturnedColumn that handles
 * a group of columns. Mostly used in subquery context. This class is
 * internal to the connector. No serialization interface is provided. 
 * The joblist factory will not recognize this class.
 */
class RowColumn : public ReturnedColumn {

public:
 
	/**
	 * Constructors
	 */ 
	RowColumn(const uint32_t sessionID = 0);
	RowColumn(const RowColumn& rhs, const uint32_t sessionID = 0);
	
	/**
	 * Destructor
	 */
	virtual ~RowColumn();
	
	/**
	 * Accessor Methods
	 */
	const std::vector<SRCP>& columnVec() const { return fColumnVec; }
	void columnVec( const std::vector<SRCP>& columnVec ) { fColumnVec = columnVec; }
	 
	/** return a copy of this pointer
	 *
	 * deep copy of this pointer and return the copy
	 */	
	inline virtual RowColumn* clone() const
	{
	    return new RowColumn (*this);
	}	
	/**
	 * Overloaded assignment operator
	 */
	RowColumn& operator=(const RowColumn& rhs);
	
	/**
	 * The serialize interface
	 */
	//virtual void serialize(messageqcpp::ByteStream&) const;
	//virtual void unserialize(messageqcpp::ByteStream&);

	virtual const std::string toString() const;

	/**
	 * Serialization interface
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
	bool operator==(const RowColumn& t) const;
	
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
	bool operator!=(const RowColumn& t) const;
	virtual bool hasAggregate() {return false;}
	virtual bool hasWindowFunc() {return false;}

private:
	/**
	 * Fields
	 */
	std::vector<SRCP> fColumnVec;
			
};

/** dummy class. For the connector to use in gp_walk*/
class SubSelect : public ReturnedColumn
{
public:
	SubSelect(): ReturnedColumn() {}
	~SubSelect() {}
	SubSelect* clone() const { return new SubSelect(); }
	virtual bool hasAggregate() {return false;}
	virtual bool hasWindowFunc() {return false;}
	virtual const std::string toString() const;
};


/**
 * ostream operator
 */
std::ostream& operator<<(std::ostream& output, const RowColumn& rhs);

}
#endif //SIMPLECOLUMN_H

