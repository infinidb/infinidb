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

//  $Id: limitedorderby.h 9414 2013-04-22 22:18:30Z xlou $


/** @file */

#ifndef LIMITED_ORDER_BY_H
#define LIMITED_ORDER_BY_H

#include <string>
#include "rowgroup.h"
#include "idborderby.h"


namespace joblist
{


// forward reference
struct JobInfo;


// ORDER BY with LIMIT class
// This version is for subqueries, limit the result set to fit in memory,
// use ORDER BY to make the results consistent.
// The actual output are the first or last # of rows, which are NOT ordered.
class LimitedOrderBy : public ordering::IdbOrderBy
{
public:
	LimitedOrderBy();
	virtual ~LimitedOrderBy();

	void initialize(const rowgroup::RowGroup&, const JobInfo&);
	void processRow(const rowgroup::Row&);
	uint64_t getKeyLength() const;
	const std::string toString() const;

	void finalize();

protected:
	uint64_t                            fStart;
	uint64_t                            fCount;
};


}

#endif  // LIMITED_ORDER_BY_H

