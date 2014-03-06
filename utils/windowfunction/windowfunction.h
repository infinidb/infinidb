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

//  $Id: windowfunction.h 3932 2013-06-25 16:08:10Z xlou $


#ifndef UTILS_WINDOWFUNCTION_H
#define UTILS_WINDOWFUNCTION_H

#include <vector>
#include <utility>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

#include "rowgroup.h"
#include "windowfunctionstep.h"

namespace ordering
{
// forward reference
class EqualCompData;
class OrderByData;
};


namespace windowfunction
{

// forward reference
class WindowFunctionType;
class WindowFrame;


/** @brief class WindowFunction
 *
 */
class WindowFunction
{
public:
	/** @brief WindowFunction constructor
	 * @param f: shared pointer to a WindowFuncitonType
	 * @param p: shared pointer to equal compare functor
	 * @param o: shared pointer to order by functor
	 * @param w: shared pointer to window specification
	 * @param r: row meta data
	 */
	WindowFunction(boost::shared_ptr<WindowFunctionType>& f,
                     boost::shared_ptr<ordering::EqualCompData>& p,
                     boost::shared_ptr<ordering::OrderByData>& o,
                     boost::shared_ptr<WindowFrame>& w,
                     const rowgroup::RowGroup& g,
                     const rowgroup::Row& r);

	/** @brief WindowFunction destructor
	 */
	virtual ~WindowFunction();

	/** @brief virtual void Run method
	 */
	void operator()();

	const std::string toString() const;

	void setCallback(joblist::WindowFunctionStep*, int);
	const rowgroup::Row& getRow() const;


protected:

	// cancellable sort function
	void sort(std::vector<joblist::RowPosition>::iterator, uint64_t);

	// special window frames
	void processUnboundedWindowFrame1();
	void processUnboundedWindowFrame2();
	void processUnboundedWindowFrame3();
	void processExprWindowFrame();

	// for string table
	rowgroup::Row::Pointer getPointer(joblist::RowPosition& r)
	{ return fStep->getPointer(r, fRowGroup, fRow); }

	// function type
	boost::shared_ptr<WindowFunctionType>       fFunctionType;

	// window clause
	boost::shared_ptr<ordering::EqualCompData>  fPartitionBy;
	boost::shared_ptr<ordering::OrderByData>    fOrderBy;
	boost::shared_ptr<WindowFrame>              fFrame;
	std::vector<std::pair<int64_t, int64_t> >   fPartition;

	// data
	boost::shared_ptr<std::vector<joblist::RowPosition> > fRowData;

	// row meta data
	rowgroup::RowGroup                          fRowGroup;
	rowgroup::Row                               fRow;

	// pointer back to step
	joblist::WindowFunctionStep*                fStep;
	int                                         fId;

	friend class joblist::WindowFunctionStep;
};


} // namespace

#endif  // UTILS_WINDOWFUNCTION_H

// vim:ts=4 sw=4:
