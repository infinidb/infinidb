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

//  $Id: framebound.h 3932 2013-06-25 16:08:10Z xlou $


#ifndef UTILS_FRAMEBOUND_H
#define UTILS_FRAMEBOUND_H

#include <boost/shared_ptr.hpp>

#include "rowgroup.h"
#include "windowfunctionstep.h"


namespace ordering
{
// forward reference
class EqualCompData;
};

namespace windowfunction
{


const int WF__UNBOUNDED_PRECEDING  = 0;
const int WF__CONSTANT_PRECEDING   = 1;
const int WF__EXPRESSION_PRECEDING = 2;
const int WF__CURRENT_ROW          = 3;
const int WF__UNBOUNDED_FOLLOWING  = 4;
const int WF__CONSTANT_FOLLOWING   = 5;
const int WF__EXPRESSION_FOLLOWING = 6;


const int WF__BOUND_ALL     = -1;   // unbounded - unbounded
const int WF__BOUND_ROLLING = -2;   // unbounded - current row

/** @brief class FrameBound
 *
 */
class FrameBound
{
public:
	/** @brief FrameBound constructor
	 *  @param  t, frame type
	 */
	FrameBound(int t = 0) : fBoundType(t), fStart(true) {};

	/** @brief FrameBound destructor
	 */
	virtual ~FrameBound() {};

	/** @brief clone
	 */
	virtual FrameBound* clone()
	{ return new FrameBound(*this); }

	/** @brief virtual void getBound
	 *  @param  b: partition start position
	 *  @param  e: partition end   position
	 *  @param  c: current position
	 *  @return  : frame position
	 */
	virtual int64_t getBound(int64_t b, int64_t e, int64_t c);

	virtual const std::string toString() const;

	void setRowData(const boost::shared_ptr<std::vector<joblist::RowPosition> >& d) {fRowData = d;}
	void setRowMetaData(const rowgroup::RowGroup& g, const rowgroup::Row& r)
	{ fRowGroup = g; fRow = r; }

	int64_t boundType() const  { return fBoundType; }
	void boundType(int64_t t)  { fBoundType = t;    }

	bool start() const  { return fStart; }
	void start(bool s)  { fStart = s;    }

	const boost::shared_ptr<ordering::EqualCompData>& peer() const  { return fPeer; }
	void peer(const boost::shared_ptr<ordering::EqualCompData>& p)  { fPeer = p; }

	// for string table
	void setCallback(joblist::WindowFunctionStep* step) { fStep = step; }
	rowgroup::Row::Pointer getPointer(joblist::RowPosition& r)
	{ return fStep->getPointer(r, fRowGroup, fRow); }

protected:
	// boundary type
	int64_t                                     fBoundType;
	bool                                        fStart;

	// data
	boost::shared_ptr<std::vector<joblist::RowPosition> > fRowData;

	// row meta data
	rowgroup::RowGroup                          fRowGroup;
	rowgroup::Row                               fRow;

	// functor for peer checking
	boost::shared_ptr<ordering::EqualCompData>  fPeer;

	// pointer back to step
	joblist::WindowFunctionStep*                fStep;
};


extern std::map<int, std::string> colType2String;


} // namespace

#endif  // UTILS_FRAMEBOUND_H

// vim:ts=4 sw=4:
