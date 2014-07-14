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
*   $Id: simplecolumn.h 9679 2013-07-11 22:32:03Z zzhu $
*
*
***********************************************************************/
/** @file */

#ifndef PSEUDOCOLUMN_H
#define PSEUDOCOLUMN_H
#include <string>
#include "simplecolumn.h"

namespace messageqcpp {
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan {

const uint32_t PSEUDO_UNKNOWN = 0;
const uint32_t PSEUDO_EXTENTRELATIVERID = 1;
const uint32_t PSEUDO_DBROOT = 2;
const uint32_t PSEUDO_PM = 3;
const uint32_t PSEUDO_SEGMENT = 4;
const uint32_t PSEUDO_SEGMENTDIR = 5;
const uint32_t PSEUDO_EXTENTMIN = 6;
const uint32_t PSEUDO_EXTENTMAX = 7;
const uint32_t PSEUDO_BLOCKID = 8;
const uint32_t PSEUDO_EXTENTID = 9;
const uint32_t PSEUDO_PARTITION = 10;
const uint32_t PSEUDO_LOCALPM = 11;

/**
 * @brief A class to represent a pseudo column
 *
 * This class is a specialization of class SimpleColumn that handles
 * a pseudocolumn.
 */
class PseudoColumn : public SimpleColumn {

public:

	/**
	 * Constructors
	 */
	PseudoColumn();
	PseudoColumn(const uint32_t pseudoType);
	PseudoColumn(const std::string& token,
	             const uint32_t pseudoType,
	             const uint32_t sessionID = 0);
	PseudoColumn(const std::string& schema,
	             const std::string& table,
	             const std::string& col,
	             const uint32_t pseudoType,
	             const uint32_t sessionID = 0);
	PseudoColumn(const std::string& schema,
	             const std::string& table,
	             const std::string& col,
	             const bool isInfiniDB,
	             const uint32_t pseudoType,
	             const uint32_t sessionID = 0);
	PseudoColumn(const SimpleColumn& rhs, const uint32_t pseudoType, const uint32_t sessionID = 0);
	PseudoColumn(const PseudoColumn& rhs, const uint32_t sessionID = 0);

	/**
	 * Destructor
	 */
	virtual ~PseudoColumn();

	/** return a copy of this pointer
	 *
	 * deep copy of this pointer and return the copy
	 */
	inline virtual PseudoColumn* clone() const
	{
		return new PseudoColumn (*this);
	}
	
	/**
	 * Overloaded assignment operator
	 */
	PseudoColumn& operator=(const PseudoColumn& rhs);
	
	/**
	 * Accessor and mutator
	 */
	const uint32_t pseudoType() const
	{
		return fPseudoType;
	}
	
	void pseudoType (const uint32_t pseudoType)
	{
		fPseudoType = pseudoType;
	}
	
	/**
	 * The serialize interface
	 */
	virtual void serialize(messageqcpp::ByteStream&) const;
	virtual void unserialize(messageqcpp::ByteStream&);

	virtual const std::string toString() const;

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
	bool operator==(const PseudoColumn& t) const;

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
	bool operator!=(const PseudoColumn& t) const;
	
	static uint32_t pseudoNameToType(std::string& name);


private:
	/**
	 * Fields
	 */
	uint32_t fPseudoType;
	void adjustResultType();
};

} // namespace execplan

#endif //PSEUDOCOLUMN_H

