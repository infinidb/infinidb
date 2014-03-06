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

//  $Id: wf_row_number.cpp 3932 2013-06-25 16:08:10Z xlou $


//#define NDEBUG
#include <cassert>
#include <cmath>
#include <sstream>
#include <iomanip>
using namespace std;

#include <boost/shared_ptr.hpp>
using namespace boost;

#include "loggingid.h"
#include "errorcodes.h"
#include "idberrorinfo.h"
using namespace logging;

#include "rowgroup.h"
using namespace rowgroup;

#include "idborderby.h"
using namespace ordering;

#include "joblisttypes.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "windowfunctionstep.h"
using namespace joblist;

#include "wf_row_number.h"


namespace windowfunction
{


boost::shared_ptr<WindowFunctionType> WF_row_number::makeFunction(int id, const string& name, int ct)
{
	boost::shared_ptr<WindowFunctionType> func(new WF_row_number(id, name));
	return func;
}


WindowFunctionType* WF_row_number::clone() const
{
	return new WF_row_number(*this);
}


void WF_row_number::resetData()
{
	fRowNumber= 0;

	WindowFunctionType::resetData();
}


void WF_row_number::operator()(int64_t b, int64_t e, int64_t c)
{
	b = fPartition.first;
	e = fPartition.second;
	for (c = b; c <= e; c++)
	{
		if (c % 1000 == 0 && fStep->cancelled())
			break;

		fRow.setData(getPointer(fRowData->at(c)));
		fRowNumber++;

		setIntValue(fFieldIndex[0], fRowNumber);
	}
}


}   //namespace
// vim:ts=4 sw=4:

