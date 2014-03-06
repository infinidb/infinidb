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
*   $Id: intervalcolumn.h 9679 2013-07-11 22:32:03Z zzhu $
*
*
***********************************************************************/

/** @file */

#ifndef INTERVALCOLUMN_H
#define INTERVALCOLUMN_H
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
/**
 * @brief A class to represent a functional column
 * 
 * This class is a specialization of class ReturnedColumn that
 * handles a window function.
 */
class IntervalColumn : public ReturnedColumn {

public:
	
	/* from my_time.h */
	enum interval_type
	{
	  INTERVAL_YEAR, INTERVAL_QUARTER, INTERVAL_MONTH, INTERVAL_WEEK, INTERVAL_DAY,
	  INTERVAL_HOUR, INTERVAL_MINUTE, INTERVAL_SECOND, INTERVAL_MICROSECOND,
	  INTERVAL_YEAR_MONTH, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE,
	  INTERVAL_DAY_SECOND, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND,
	  INTERVAL_MINUTE_SECOND, INTERVAL_DAY_MICROSECOND, INTERVAL_HOUR_MICROSECOND,
	  INTERVAL_MINUTE_MICROSECOND, INTERVAL_SECOND_MICROSECOND, INTERVAL_LAST
	};
	
	IntervalColumn();
	IntervalColumn(SRCP&, int);
	IntervalColumn(const IntervalColumn& rhs, const uint32_t sessionID = 0);
	virtual ~IntervalColumn() {}
	const SRCP& val() const 
	{
		return fVal;
	}
	void val(const SRCP& val)
	{
		fVal = val;
	}
	const int intervalType() const
	{
		return fIntervalType;
	}
	void intervalType(int intervalType)
	{
		fIntervalType = intervalType;
	}
	const std::string toString() const;
	inline virtual IntervalColumn* clone() const
	{
		return new IntervalColumn (*this);
	}
	
	virtual bool hasAggregate() {return false;}
	virtual bool hasWindowFunc() {return false;}
	
private:
	/**
	 * Fields
	 */
	SRCP fVal;
	int fIntervalType;

	// okay to be private for now.
	virtual bool operator==(const TreeNode* t) const { return false; }
	bool operator==(const IntervalColumn& t) const;
	virtual bool operator!=(const TreeNode* t) const { return false; }
	bool operator!=(const IntervalColumn& t) const;
};

/**
* ostream operator
*/
std::ostream& operator<<(std::ostream& output, const IntervalColumn& rhs);

} 
#endif //INTERVALCOLUMN_H
