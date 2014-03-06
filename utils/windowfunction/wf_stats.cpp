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

//  $Id: wf_stats.cpp 3932 2013-06-25 16:08:10Z xlou $


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

#include "wf_stats.h"


namespace windowfunction
{


template<typename T>
boost::shared_ptr<WindowFunctionType> WF_stats<T>::makeFunction(int id, const string& name, int ct)
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
			func.reset(new WF_stats<int64_t>(id, name));
			break;
		}
		case CalpontSystemCatalog::UTINYINT:
		case CalpontSystemCatalog::USMALLINT:
		case CalpontSystemCatalog::UMEDINT:
		case CalpontSystemCatalog::UINT:
		case CalpontSystemCatalog::UBIGINT:
		case CalpontSystemCatalog::UDECIMAL:
		{
			func.reset(new WF_stats<uint64_t>(id, name));
			break;
		}
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
		{
			func.reset(new WF_stats<double>(id, name));
			break;
		}
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
		{
			func.reset(new WF_stats<float>(id, name));
			break;
		}
		default:
		{
			string errStr = name + "(" + colType2String[ct] + ")";
			errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_INVALID_PARM_TYPE, errStr);
			cerr << errStr << endl;
			throw IDBExcept(errStr, ERR_WF_INVALID_PARM_TYPE);

			break;
		}
	}

	return func;
}


template<typename T>
WindowFunctionType* WF_stats<T>::clone() const
{
	return new WF_stats<T>(*this);
}


template<typename T>
void WF_stats<T>::resetData()
{
	fSum1 = 0;
	fSum2 = 0;
	fCount = 0;
	fStats = 0.0;

	WindowFunctionType::resetData();
}


template<typename T>
void WF_stats<T>::operator()(int64_t b, int64_t e, int64_t c)
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
			long double val = (long double) valIn;

			fSum1 += val;
			fSum2 += val * val;
			fCount++;
		}

		if ((fCount > 0) &&
			!(fCount == 1 && (fFunctionId == WF__STDDEV_SAMP || fFunctionId == WF__VAR_SAMP)))
		{
			int scale = fRow.getScale(colIn);
			long double factor = pow(10.0, scale);
			if (scale != 0) // adjust the scale if necessary
			{
				fSum1 /= factor;
				fSum2 /= factor*factor;
			}

			long double stat = fSum1 * fSum1 / fCount;
			stat = fSum2 - stat;

			if (fFunctionId == WF__STDDEV_POP)
				stat = sqrt(stat / fCount);
			else if (fFunctionId == WF__STDDEV_SAMP)
				stat = sqrt(stat / (fCount - 1));
			else if (fFunctionId == WF__VAR_POP)
				stat = stat / fCount;
			else if (fFunctionId == WF__VAR_SAMP)
				stat = stat / (fCount - 1);

			fStats = (double) stat;
		}
	}

	if (fCount == 0)
	{
		setValue(CalpontSystemCatalog::DOUBLE, b, e, c, (double*) NULL);
	}
	else if (fCount == 1 && (fFunctionId == WF__STDDEV_SAMP || fFunctionId == WF__VAR_SAMP))
	{
		setValue(CalpontSystemCatalog::DOUBLE, b, e, c, (double*) NULL);
	}
	else
	{
		setValue(CalpontSystemCatalog::DOUBLE, b, e, c, &fStats);
	}

	fPrev = c;
}


template
boost::shared_ptr<WindowFunctionType> WF_stats<int64_t>::makeFunction(int, const string&, int);


}   //namespace
// vim:ts=4 sw=4:

