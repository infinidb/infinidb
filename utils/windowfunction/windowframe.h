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

//  $Id: windowframe.h 3932 2013-06-25 16:08:10Z xlou $


#ifndef UTILS_WINDOWFRAME_H
#define UTILS_WINDOWFRAME_H

#include <utility>              // std::pair, std::make_pair
#include <boost/shared_ptr.hpp>
#include "framebound.h"

namespace windowfunction
{

const int64_t WF__FRAME_ROWS  = 0;
const int64_t WF__FRAME_RANGE = 1;


/** @brief class WindowFrame
 *
 */
class WindowFrame
{
public:
	/** @brief WindowFrame constructor
	 */
	WindowFrame(int64_t t, boost::shared_ptr<FrameBound>& u, boost::shared_ptr<FrameBound>& l) :
		fUnit(t), fUpper(u), fLower(l)
	{}

	/** @brief WindowFrame copy constructor
	 */
	WindowFrame(const WindowFrame& rhs) :
		fUnit(rhs.fUnit),
		fUpper(rhs.fUpper->clone()),
		fLower(rhs.fLower->clone())
	{}

	/** @brief WindowFrame destructor
	 */
	virtual ~WindowFrame() {};

	/** @brief clone
	 */
	virtual WindowFrame* clone() { return new WindowFrame(*this); };

	/** @brief virtual void getWindow
	 */
	std::pair<uint64_t, uint64_t> getWindow(int64_t, int64_t, int64_t);

	const std::string toString() const;

	/** @brief set methods
	 */
	void setRowMetaData(const rowgroup::RowGroup& g, const rowgroup::Row& r)
			{ fUpper->setRowMetaData(g, r); fLower->setRowMetaData(g, r); }
	void setRowData(boost::shared_ptr<std::vector<joblist::RowPosition> >& d)
			{ fUpper->setRowData(d); fLower->setRowData(d); }
	void setCallback(joblist::WindowFunctionStep* s)
			{ fUpper->setCallback(s); fLower->setCallback(s); }

	int64_t unit() const { return fUnit; }
	void unit(int64_t t) { fUnit = t;    }
	const boost::shared_ptr<FrameBound>& upper() const { return fUpper; }
	void upper(const boost::shared_ptr<FrameBound>& u) { fUpper = u;    }
	const boost::shared_ptr<FrameBound>& lower() const { return fLower; }
	void lower(const boost::shared_ptr<FrameBound>& l) { fLower = l;    }

protected:

	// type
	int64_t                                  fUnit;

	// data
	boost::shared_ptr<FrameBound>            fUpper;
	boost::shared_ptr<FrameBound>            fLower;
};


} // namespace

#endif  // UTILS_WINDOWFRAME_H

// vim:ts=4 sw=4:
