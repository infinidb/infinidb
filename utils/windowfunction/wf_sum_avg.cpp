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

//  $Id: wf_sum_avg.cpp 3932 2013-06-25 16:08:10Z xlou $


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

#include "wf_sum_avg.h"


namespace
{


template<typename T>
void checkSumLimit(T sum, T val)
{
}


template<>
void checkSumLimit<int64_t>(int64_t sum, int64_t val)
{
	if (((sum >= 0) && ((numeric_limits<int64_t>::max() - sum) < val)) ||
		((sum <  0) && ((numeric_limits<int64_t>::min() - sum) > val)))
	{
		string errStr = "SUM(int):";

		ostringstream oss;
		oss << sum << "+" << val;
		if (sum > 0)
			oss << " > " << numeric_limits<uint64_t>::max();
		else
			oss << " < " << numeric_limits<uint64_t>::min();
		errStr += oss.str();

		errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_OVERFLOW, errStr);
		cerr << errStr << endl;
		throw IDBExcept(errStr, ERR_WF_OVERFLOW);
	}
}


template<>
void checkSumLimit<uint64_t>(uint64_t sum, uint64_t val)
{
	if ((sum >= 0) && ((numeric_limits<uint64_t>::max() - sum) < val))
	{
		string errStr = "SUM(unsigned):";

		ostringstream oss;
		oss << sum << "+" << val << " > " << numeric_limits<uint64_t>::max();
		errStr += oss.str();

		errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_OVERFLOW, errStr);
		cerr << errStr << endl;
		throw IDBExcept(errStr, ERR_WF_OVERFLOW);
	}
}


template<typename T>
T calculateAvg(T sum, uint64_t count, int s)
{
	T avg = ((long double) sum) / count;
	return avg;
}


long double avgWithLimit(long double sum, uint64_t count, int scale, long double u, long double l)
{
	long double factor = pow(10.0, scale);
	long double avg = sum / count;
	avg *= factor;
	avg += (avg < 0) ? (-0.5) : (0.5);
	if (avg > u || avg < l)
	{
		string errStr = string("AVG") + (l < 0 ? "(int):" : "(unsign)");
		ostringstream oss;
		oss << avg;
		errStr += oss.str();

		errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_OVERFLOW, errStr);
		cerr << errStr << endl;
		throw IDBExcept(errStr, ERR_WF_OVERFLOW);
	}

	return avg;
}


template<>
int64_t calculateAvg<int64_t>(int64_t sum, uint64_t count, int scale)
{
	int64_t t = (int64_t) avgWithLimit(sum, count, scale,
				numeric_limits<int64_t>::max(), numeric_limits<int64_t>::min());
	return t;
}


template<>
uint64_t calculateAvg<uint64_t>(uint64_t sum, uint64_t count, int scale)
{
	uint64_t t = (uint64_t) avgWithLimit(sum, count, scale, numeric_limits<uint64_t>::max(), 0);
	return t;
}


}

namespace windowfunction
{

template<typename T>
boost::shared_ptr<WindowFunctionType> WF_sum_avg<T>::makeFunction(int id, const string& name, int ct)
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
			func.reset(new WF_sum_avg<int64_t>(id, name));
			break;
		}
		case CalpontSystemCatalog::UTINYINT:
		case CalpontSystemCatalog::USMALLINT:
		case CalpontSystemCatalog::UMEDINT:
		case CalpontSystemCatalog::UINT:
		case CalpontSystemCatalog::UBIGINT:
		case CalpontSystemCatalog::UDECIMAL:
		{
			func.reset(new WF_sum_avg<uint64_t>(id, name));
			break;
		}
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
		{
			func.reset(new WF_sum_avg<double>(id, name));
			break;
		}
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
		{
			func.reset(new WF_sum_avg<float>(id, name));
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
WindowFunctionType* WF_sum_avg<T>::clone() const
{
	return new WF_sum_avg<T>(*this);
}


template<typename T>
void WF_sum_avg<T>::resetData()
{
	fAvg = 0;
	fSum = 0;
	fCount = 0;
	fSet.clear();

	WindowFunctionType::resetData();
}


template<typename T>
void WF_sum_avg<T>::operator()(int64_t b, int64_t e, int64_t c)
{
	uint64_t colOut = fFieldIndex[0];

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
		int scale = fRow.getScale(colOut) - fRow.getScale(colIn);
		for (int64_t i = b; i <= e; i++)
		{
			if (i % 1000 == 0 && fStep->cancelled())
				break;

			fRow.setData(getPointer(fRowData->at(i)));
			if (fRow.isNullValue(colIn) == true)
				continue;

			T valIn;
			getValue(colIn, valIn);
			checkSumLimit(fSum, valIn);

			if ((!fDistinct) || (fSet.find(valIn) == fSet.end()))
			{
				fSum += valIn;
				fCount++;

				if (fDistinct)
					fSet.insert(valIn);
			}
		}

		if ((fCount > 0) && (fFunctionId == WF__AVG || fFunctionId == WF__AVG_DISTINCT))
			 fAvg = (T) calculateAvg(fSum, fCount, scale);
	}

	T* v = NULL;
	if (fCount > 0)
	{
		if (fFunctionId == WF__AVG || fFunctionId == WF__AVG_DISTINCT)
			v = &fAvg;
		else
			v = &fSum;
	}
	setValue(fRow.getColType(colOut), b, e, c, v);

	fPrev = c;
}


template
boost::shared_ptr<WindowFunctionType> WF_sum_avg<int64_t>::makeFunction(int, const string&, int);


}   //namespace
// vim:ts=4 sw=4:

