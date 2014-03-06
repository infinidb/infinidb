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

//  $Id: wf_percentile.cpp 3932 2013-06-25 16:08:10Z xlou $


//#define NDEBUG
#include <cassert>
#include <cmath>
#include <sstream>
#include <iomanip>
using namespace std;

#include <boost/shared_ptr.hpp>
#include <boost/scoped_array.hpp>
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
#include "constantcolumn.h"
using namespace execplan;

#include "windowfunctionstep.h"
using namespace joblist;

#include "wf_percentile.h"


namespace windowfunction
{

template<typename T>
boost::shared_ptr<WindowFunctionType> WF_percentile<T>::makeFunction(int id, const string& name, int ct)
{
	boost::shared_ptr<WindowFunctionType> func;
	if (id == WF__PERCENTILE_DISC)
	{
		switch (ct)
		{
			case CalpontSystemCatalog::TINYINT:
			case CalpontSystemCatalog::SMALLINT:
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::BIGINT:
			case CalpontSystemCatalog::DECIMAL:
			{
				func.reset(new WF_percentile<int64_t>(id, name));
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
				func.reset(new WF_percentile<uint64_t>(id, name));
				break;
			}
			case CalpontSystemCatalog::DOUBLE:
			case CalpontSystemCatalog::UDOUBLE:
			{
				func.reset(new WF_percentile<double>(id, name));
				break;
			}
			case CalpontSystemCatalog::FLOAT:
			case CalpontSystemCatalog::UFLOAT:
			{
				func.reset(new WF_percentile<float>(id, name));
				break;
			}
			default:
			{
				if (id == WF__PERCENTILE_DISC)
				{
					func.reset(new WF_percentile<string>(id, name));
				}
				else
				{
					string errStr = name + "(" + colType2String[ct] + ")";
					errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_INVALID_PARM_TYPE, errStr);
					cerr << errStr << endl;
					throw IDBExcept(errStr, ERR_WF_INVALID_PARM_TYPE);
				}
				break;
			}
		}
	}
	else
	{
		switch (ct)
		{
			case CalpontSystemCatalog::TINYINT:
			case CalpontSystemCatalog::SMALLINT:
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::BIGINT:
			case CalpontSystemCatalog::DECIMAL:
			case CalpontSystemCatalog::UTINYINT:
			case CalpontSystemCatalog::USMALLINT:
			case CalpontSystemCatalog::UMEDINT:
			case CalpontSystemCatalog::UINT:
			case CalpontSystemCatalog::UBIGINT:
			case CalpontSystemCatalog::UDECIMAL:
			case CalpontSystemCatalog::DOUBLE:
			case CalpontSystemCatalog::UDOUBLE:
			case CalpontSystemCatalog::FLOAT:
			case CalpontSystemCatalog::UFLOAT:
			{
				func.reset(new WF_percentile<double>(id, name));
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
	}

	return func;
}


template<typename T>
WindowFunctionType* WF_percentile<T>::clone() const
{
	return new WF_percentile(*this);
}


template<typename T>
void WF_percentile<T>::resetData()
{
	WindowFunctionType::resetData();
}


template<typename T>
void WF_percentile<T>::parseParms(const std::vector<execplan::SRCP>& parms)
{
	// parms[0]: nve
	ConstantColumn* cc = dynamic_cast<ConstantColumn*>(parms[0].get());
	if (cc != NULL)
	{
		fNveNull = false;
		fNve = cc->getDoubleVal(fRow, fNveNull);  // row not used, no need to setData.
		if (!fNveNull && (fNve < 0 || fNve > 1))
		{
			ostringstream oss;
			oss << fNve;
			throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_ARG_OUT_OF_RANGE,
						oss.str()), ERR_WF_ARG_OUT_OF_RANGE);
		}
	}

	// workaround for the within group order by column index
	idbassert(fPeer->fIndex.size() > 0);
	fFieldIndex.push_back(fPeer->fIndex[0]);
}


template<typename T>
void WF_percentile<T>::operator()(int64_t b, int64_t e, int64_t c)
{
	int64_t idx = fFieldIndex[1];
	fRow.setData(getPointer(fRowData->at(b)));
	if (idx != -1)
	{
		if (idx != -1)
		{
			fNveNull = fRow.isNullValue(idx);
			implicit2T(idx, fNve, 0);
			if (!fNveNull && (fNve < 0 || fNve > 1))
			{
				ostringstream oss;
				oss << fNve;
				throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_ARG_OUT_OF_RANGE,
							oss.str()), ERR_WF_ARG_OUT_OF_RANGE);
			}
		}
	}

	if (fNveNull)
	{
		for (c = b; c <= e; c++)
		{
			if (c % 1000 == 0 && fStep->cancelled())
				break;

			fRow.setData(getPointer(fRowData->at(c)));
			setValue(fRow.getColType(fFieldIndex[0]), b, e, c, (T*) NULL);
		}

		return;
	}

	idx = fFieldIndex[2];
	int64_t rank = 0;
	int64_t dups = 0;
	int64_t b1 = -1;
	int64_t e1 = -1;
	scoped_array<int64_t> rk(new int64_t[e - b + 1]);
	for (c = b; c <= e; c++)
	{
		if (c % 1000 == 0 && fStep->cancelled())
			break;

		fRow.setData(getPointer(fRowData->at(c)));
		if (fRow.isNullValue(idx))
			continue;

		// ignore nulls
		if (b1 == -1)
			b1 = c;
		e1 = c;

		if (fFunctionId == WF__PERCENTILE_DISC)
		{
			// need cume_rank
			if (c != b &&
				fPeer->operator()(getPointer(fRowData->at(c)), getPointer(fRowData->at(c-1))))
			{
				dups++;
			}
			else
			{
				rank++;
				rank += dups;
				dups = 0;
			}

			rk[c-b] = rank;
		}
	}

	T* p = NULL;
	T v;
	int ct = (fFunctionId == WF__PERCENTILE_CONT) ?
	          CalpontSystemCatalog::DOUBLE : fRow.getColType(idx);
	if (b1 != -1)
	{
		double cnt = (e1 - b1 + 1);
		if (fFunctionId == WF__PERCENTILE_CONT)
		{
			// @bug5820, this "rn" is the normalized row number, not the real row number.
			// Using real row number here will introduce a small calculation error in double result.
			double rn = fNve * (cnt - 1);
			double crn = ceil(rn);
			double frn = floor(rn);
			double vd = 0;
			if (crn == rn && rn == frn)
			{
				fRow.setData(getPointer(fRowData->at((size_t) rn + (size_t) b1)));
				implicit2T(idx, vd, 0);
			}
			else
			{
				double cv = 0.0, fv = 0.0;
				fRow.setData(getPointer(fRowData->at((size_t) frn + (size_t) b1)));
				implicit2T(idx, fv, 0);
				fRow.setData(getPointer(fRowData->at((size_t) crn + (size_t) b1)));
				implicit2T(idx, cv, 0);
				vd = (crn - rn) * fv + (rn - frn) * cv;
			}

			v = *(reinterpret_cast<T*>(&vd));
			p = &v;
		}
		else  // (fFunctionId == WF__PERCENTILE_DISC)
		{
			int prevRank = ++rank + dups;
			double cumeDist = 1;
			fRow.setData(getPointer(fRowData->at(e1)));
			for (c = e1; c >= b1; c--)
			{
				int currRank = rk[c-b];
				if (currRank != prevRank)
				{
					cumeDist = ((double) (prevRank-1)) / cnt;
					if (cumeDist < fNve)
						break;

					prevRank = currRank;
				}
			}

			c++;

			fRow.setData(getPointer(fRowData->at(c)));
			getValue(idx, v);

			p = &v;
		}
	}

	for (c = b; c <= e; c++)
	{
		if (c % 1000 == 0 && fStep->cancelled())
			break;

		fRow.setData(getPointer(fRowData->at(c)));
		setValue(ct, b, e, c, p);
	}
}


template
boost::shared_ptr<WindowFunctionType> WF_percentile<int64_t>::makeFunction(int, const string&, int);


}   //namespace
// vim:ts=4 sw=4:

