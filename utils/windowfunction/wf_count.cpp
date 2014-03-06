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

//  $Id: wf_count.cpp 3932 2013-06-25 16:08:10Z xlou $


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

#include "wf_count.h"


namespace windowfunction
{


template<typename T>
boost::shared_ptr<WindowFunctionType> WF_count<T>::makeFunction(int id, const string& name, int ct)
{
	boost::shared_ptr<WindowFunctionType> func;
	switch (ct)
	{
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		case CalpontSystemCatalog::VARBINARY:
		{
			func.reset(new WF_count<string>(id, name));
			break;
		}
		default:
		{
			func.reset(new WF_count<int64_t>(id, name));
			break;
		}
	}

	return func;
}


template<typename T>
WindowFunctionType* WF_count<T>::clone() const
{
	return new WF_count(*this);
}


template<typename T>
void WF_count<T>::resetData()
{
	fCount = 0;
	fSet.clear();

	WindowFunctionType::resetData();
}


template<typename T>
void WF_count<T>::operator()(int64_t b, int64_t e, int64_t c)
{
	if ((fFrameUnit == WF__FRAME_ROWS) ||
		(fPrev == -1) ||
		(!fPeer->operator()(getPointer(fRowData->at(c)), getPointer(fRowData->at(fPrev)))))
	{
		// for unbounded - current row special handling
		if (fPrev >= b && fPrev < c)
			b = c;
		else if (fPrev <= e && fPrev > c)
			e = c;

		// for count(*), the column is optimized out, index[1] does not exist.
		uint64_t colIn = (fFunctionId == WF__COUNT_ASTERISK) ? 0 : fFieldIndex[1];
		for (int64_t i = b; i <= e; i++)
		{
			if (i % 1000 == 0 && fStep->cancelled())
				break;

			if (fFunctionId == WF__COUNT_ASTERISK)
			{
				fCount++;
				continue;
			}

			fRow.setData(getPointer(fRowData->at(i)));
			if (fRow.isNullValue(colIn) == true)
				continue;

			if (fFunctionId != WF__COUNT_DISTINCT)
			{
				fCount++;
			}
			else
			{
				T valIn;
				getValue(colIn, valIn);
				if (fSet.find(valIn) == fSet.end())
				{
					fCount++;

					if (fFunctionId == WF__COUNT_DISTINCT)
					fSet.insert(valIn);
				}
			}
		}
	}

	setValue(CalpontSystemCatalog::BIGINT, b, e, c, &fCount);

	fPrev = c;
}


template
boost::shared_ptr<WindowFunctionType> WF_count<int64_t>::makeFunction(int, const string&, int);


}   //namespace
// vim:ts=4 sw=4:

