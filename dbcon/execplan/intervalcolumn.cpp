/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/***********************************************************************
*   $Id: intervalcolumn.cpp 9414 2013-04-22 22:18:30Z xlou $
*
*
***********************************************************************/

#include <string>
#include <iostream>
#include <sstream>
using namespace std;

#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>
using namespace boost;

#include "intervalcolumn.h"
using namespace funcexp;

#ifdef _MSC_VER
#define strcasecmp stricmp
#endif

namespace execplan {

/**
 * Constructors/Destructors
 */
IntervalColumn::IntervalColumn()
{}

IntervalColumn::IntervalColumn(SRCP& val, int intervalType):
	fVal(val->clone()), fIntervalType(intervalType)
{
	cout << "intervalType=" << fIntervalType << endl;
	}

IntervalColumn::IntervalColumn( const IntervalColumn& rhs, const u_int32_t sessionID):
    ReturnedColumn(rhs, sessionID),
    fVal(rhs.val()),
    fIntervalType(rhs.intervalType())
{}

/**
 * Methods
 */
const string IntervalColumn::toString() const
{
  ostringstream output;
	output << "INTERVAL" << endl;
	if (fVal)
		output << fVal->toString();
	output << " IntervalType=" << fIntervalType << endl;
	return output.str();
}

ostream& operator<<(ostream& output, const IntervalColumn& rhs)
{
	output << rhs.toString();
	return output;
}

}   //namespace
