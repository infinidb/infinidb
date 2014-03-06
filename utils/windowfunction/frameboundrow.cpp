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

//  $Id: frameboundrow.cpp 3932 2013-06-25 16:08:10Z xlou $


//#define NDEBUG
#include <cassert>
#include <cmath>
#include <sstream>
#include <iomanip>
using namespace std;

#include "idberrorinfo.h"
#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "idborderby.h"
using namespace ordering;

#include "treenode.h"
#include "frameboundrow.h"


namespace windowfunction
{


int64_t FrameBoundRow::getBound(int64_t b, int64_t e, int64_t c)
{
	return c;
}


const string FrameBoundRow::toString() const
{
	return FrameBound::toString();
}


int64_t FrameBoundConstantRow::getBound(int64_t b, int64_t e, int64_t c)
{
	if (fBoundType < WF__CURRENT_ROW)
	{
		if (fOffset <= (c - b))
			c -= fOffset;
		else
			c = b - (!fStart ? 1 : 0);
	}
	else
	{
		if (fOffset <= (e - c))
			c += fOffset;
		else
			c = e + (fStart ? 1 : 0);
	}

	return c;
}


const string FrameBoundConstantRow::toString() const
{
    ostringstream oss;
	oss << fOffset << " " << FrameBound::toString();
	return oss.str();
}


template<typename T>
int64_t FrameBoundExpressionRow<T>::getBound(int64_t b, int64_t e, int64_t c)
{
	// set row data
	// get expression int value
	fRow.setData(getPointer(fRowData->at(c)));

	if (fRow.isNullValue(fExprIdx))
		throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_BOUND_OUT_OF_RANGE, "NULL"),
						ERR_WF_BOUND_OUT_OF_RANGE);

	getOffset();

	if (fOffset < 0)
	{
		throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_BOUND_OUT_OF_RANGE, fOffset),
				ERR_WF_BOUND_OUT_OF_RANGE);
	}

	return FrameBoundConstantRow::getBound(b, e, c);
}


template<typename T>
const string FrameBoundExpressionRow<T>::toString() const
{
    ostringstream oss;
	oss << "value_expr " << FrameBound::toString();
	return oss.str();
}


template<typename T> void FrameBoundExpressionRow<T>::getOffset()
{
}


template<> void FrameBoundExpressionRow<int64_t>::getOffset()
{
	fOffset = fRow.getIntField(fExprIdx);
}


template<> void FrameBoundExpressionRow<uint64_t>::getOffset()
{
	fOffset = fRow.getUintField(fExprIdx);
}


template<> void FrameBoundExpressionRow<double>::getOffset()
{
	fOffset = (int64_t) fRow.getDoubleField(fExprIdx);
}


template<> void FrameBoundExpressionRow<float>::getOffset()
{
	fOffset = (int64_t) fRow.getFloatField(fExprIdx);
}


template class FrameBoundExpressionRow<int64_t>;
template class FrameBoundExpressionRow<double>;
template class FrameBoundExpressionRow<float>;
template class FrameBoundExpressionRow<uint64_t>;


}   //namespace
// vim:ts=4 sw=4:

