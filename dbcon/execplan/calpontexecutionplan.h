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
*   $Id: calpontexecutionplan.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef CALPONTEXECUTIONPLAN_H
#define CALPONTEXECUTIONPLAN_H
#include <string>
#include <boost/shared_ptr.hpp>

namespace messageqcpp {
	class ByteStream;
}

/**
 * Namespace
 */
namespace execplan { 
/**
 * ******************************* Abstract Class ****************************
 * CalpontExecutionPlan does not have any pure virtual methods, but its author
 *   defined it as an abstract class, so you should not use it directly.
 *   Inherit from it instead and create only objects from the derived classes
 * *****************************************************************************
 */
class CalpontExecutionPlan {
/**
 * Public stuff
 */
public:

	/**
	 * Constructors
	 */
	CalpontExecutionPlan();	 
	/**
	 * Destructors
	 */
	virtual ~CalpontExecutionPlan();	 
	/**
	 * Accessor Methods
	 */
	/**
	 * Operations
	 */
	/*
	 * The serialization interface
	 */
	/** @brief Convert *this to a stream of bytes
	 *
	 * Convert *this to a stream of bytes.
	 * @param b The ByteStream to write the bytes to.
	 */
	virtual void serialize(messageqcpp::ByteStream& b) const = 0;
	
	/** @brief Construct a CalpontExecutionPlan from a stream of bytes
	 *
	 * Construct a CalpontExecutionPlan from a stream of bytes.
	 * @param b The ByteStream to read from.
	 */
	virtual void unserialize(messageqcpp::ByteStream& b) = 0;
	
	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
	 */
	virtual bool operator==(const CalpontExecutionPlan* t) const = 0;
	
	/** @brief Do a deep, strict (as opposed to semantic) equivalence test
	 *
	 * Do a deep, strict (as opposed to semantic) equivalence test.
	 * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
	 */
	virtual bool operator!=(const CalpontExecutionPlan* t) const = 0;
	
/**
 * Protected stuff
 */
protected:
	/**
	 * Fields
	 */
	/**
	 * 
	 */
	/**
	 * Constructors
	 */
	/**
	 * Accessor Methods
	 */
	/**
	 * Operations
	 */
/**
 * Private stuff
 */
private:
	/**
	 * Fields
	 */
	/**
	 * 
	 */
	/**
	 * Constructors
	 */
	/**
	 * Accessor Methods
	 */
	/**
	 * Operations
	 */
};

typedef boost::shared_ptr<CalpontExecutionPlan> SCEP;

} 
#endif //CALPONTEXECUTIONPLAN_H

