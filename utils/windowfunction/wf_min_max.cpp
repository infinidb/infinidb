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

//  $Id: wf_min_max.cpp 3932 2013-06-25 16:08:10Z xlou $


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

#include "joblisttypes.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "windowfunctionstep.h"
using namespace joblist;

#include "wf_min_max.h"


namespace windowfunction
{


template<typename T>
boost::shared_ptr<WindowFunctionType> WF_min_max<T>::makeFunction(int id, const string& name, int ct)
{
	boost::shared_ptr<WindowFunctionType> func;
	switch (ct)
	{
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::DECIMAL:
		{
			func.reset(new WF_min_max<int64_t>(id, name));
			break;
		}
		case CalpontSystemCatalog::UTINYINT:
		case CalpontSystemCatalog::USMALLINT:
		case CalpontSystemCatalog::UMEDINT:
		case CalpontSystemCatalog::UINT:
		case CalpontSystemCatalog::UBIGINT:
		case CalpontSystemCatalog::UDECIMAL:
		case CalpontSystemCatalog::DATE:
		case CalpontSystemCatalog::DATETIME:
		{
			func.reset(new WF_min_max<uint64_t>(id, name));
			break;
		}
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
		{
			func.reset(new WF_min_max<double>(id, name));
			break;
		}
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
		{
			func.reset(new WF_min_max<float>(id, name));
			break;
		}
		default:
		{
			func.reset(new WF_min_max<string>(id, name));
			break;
		}
	}

	return func;
}


template<typename T>
WindowFunctionType* WF_min_max<T>::clone() const
{
	return new WF_min_max<T>(*this);
}


template<typename T>
void WF_min_max<T>::resetData()
{
	fCount = 0;

	WindowFunctionType::resetData();
}


template<typename T>
void WF_min_max<T>::operator()(int64_t b, int64_t e, int64_t c)
{
	// for unbounded - current row special handling
	if (fPrev >= b && fPrev < c)
		b = c;
	else if (fPrev <= e && fPrev > c)
		e = c;

	uint64_t colIn = fFieldIndex[1];
	for (int64_t i = b; i <= e; i++)
	{
		if (i % 1000 == 0 && fStep->cancelled())
			break;

		fRow.setData(getPointer(fRowData->at(i)));
		if (fRow.isNullValue(colIn) == true)
			continue;

		T valIn;
		getValue(colIn, valIn);
		if ((fCount == 0) ||
			(valIn < fValue && fFunctionId == WF__MIN) ||
			(valIn > fValue && fFunctionId == WF__MAX))
		{
			fValue = valIn;
		}

		fCount++;
	}

	T* v = ((fCount > 0) ? &fValue : NULL);
	setValue(fRow.getColType(fFieldIndex[0]), b, e, c, v);

	fPrev = c;
}


template
boost::shared_ptr<WindowFunctionType> WF_min_max<int64_t>::makeFunction(int, const string&, int);


}   //namespace
// vim:ts=4 sw=4:

