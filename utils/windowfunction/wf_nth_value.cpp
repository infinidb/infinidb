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

//  $Id: wf_nth_value.cpp 3932 2013-06-25 16:08:10Z xlou $


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
#include "constantcolumn.h"
using namespace execplan;

#include "windowfunctionstep.h"
using namespace joblist;

#include "wf_nth_value.h"


namespace windowfunction
{


template<typename T>
boost::shared_ptr<WindowFunctionType> WF_nth_value<T>::makeFunction(int id, const string& name, int ct)
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
			func.reset(new WF_nth_value<int64_t>(id, name));
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
			func.reset(new WF_nth_value<uint64_t>(id, name));
			break;
		}
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
		{
			func.reset(new WF_nth_value<double>(id, name));
			break;
		}
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
		{
			func.reset(new WF_nth_value<float>(id, name));
			break;
		}
		default:
		{
			func.reset(new WF_nth_value<string>(id, name));
			break;
		}
	}

	return func;
}


template<typename T>
WindowFunctionType* WF_nth_value<T>::clone() const
{
	return new WF_nth_value<T>(*this);
}


template<typename T>
void WF_nth_value<T>::resetData()
{
	WindowFunctionType::resetData();
}


template<typename T>
void WF_nth_value<T>::parseParms(const std::vector<execplan::SRCP>& parms)
{
	// parms[0]: value-expr
	// skip

	// parms[1]: nth value
	ConstantColumn* cc = dynamic_cast<ConstantColumn*>(parms[1].get());
	if (cc != NULL)
	{
		fNthNull = false;
		fNth = cc->getIntVal(fRow, fNthNull);  // row not used, no need to setData.
		if (fNth <= 0)
		{
			ostringstream oss;
			oss << fNth;
			throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_ARG_OUT_OF_RANGE,
						oss.str()), ERR_WF_ARG_OUT_OF_RANGE);
		}
	}

	// parms[2]: from first | from last
	bool isNull = false;
	cc = dynamic_cast<ConstantColumn*>(parms[2].get());
	idbassert(cc != NULL);
	fFromFirst = (cc->getIntVal(fRow, isNull) > 0);

	// parms[3]: respect null | ignore null
	cc = dynamic_cast<ConstantColumn*>(parms[3].get());
	idbassert(cc != NULL);
	fRespectNulls = (cc->getIntVal(fRow, isNull) > 0);
}


template<typename T>
void WF_nth_value<T>::operator()(int64_t b, int64_t e, int64_t c)
{
	int64_t s = b;
	int64_t t = e;
	if (c != WF__BOUND_ALL)
		s = t = c;

	for (int64_t c = s; c <= t; c++)
	{
		if (c % 1000 == 0 && fStep->cancelled())
			break;

		int64_t idx = fFieldIndex[2];
		fRow.setData(getPointer(fRowData->at(c)));
		if (idx != -1)
		{
			double tmp = 1.0;
			fNthNull = fRow.isNullValue(idx);
			if (!fNthNull)
			{
				implicit2T(idx, tmp, 0);
				if (tmp <= 0)
				{
					ostringstream oss;
					oss << tmp;
					throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_ARG_OUT_OF_RANGE,
								oss.str()), ERR_WF_ARG_OUT_OF_RANGE);
				}

				if (tmp > e) // prevent integer overflow
					tmp = e + 1;

				fNth = (int64_t) tmp;
			}
		}


		bool isNull = true;
		if ((!fNthNull) && ((b + fNth - 1) <= e))
		{
			uint64_t colIn = fFieldIndex[1];
			if (fFromFirst)
			{
				int64_t k = b;
				fRow.setData(getPointer(fRowData->at(k)));
				if (fRespectNulls == false && fRow.isNullValue(colIn) == true)
				{
					while (++k < e)
					{
						fRow.setData(getPointer(fRowData->at(k)));
						if (fRow.isNullValue(colIn) == false)
							break;
					}
				}

				int64_t n = k + fNth - 1;
				if (n <= e)
				{
					fRow.setData(getPointer(fRowData->at(n)));
					getValue(colIn, fValue);
					isNull = fRow.isNullValue(colIn);
				}
			}
			else    // from last
			{
				int64_t k = e;
				fRow.setData(getPointer(fRowData->at(k)));
				if (fRespectNulls == false && fRow.isNullValue(colIn) == true)
				{
					while (--k > b)
					{
						fRow.setData(getPointer(fRowData->at(k)));
						if (fRow.isNullValue(colIn) == false)
							break;
					}
				}

				int64_t n = k - fNth + 1;
				if (n >= b)
				{
					fRow.setData(getPointer(fRowData->at(n)));
					getValue(colIn, fValue);
					isNull = fRow.isNullValue(colIn);
				}
			}
		}

		T* v = (isNull) ? NULL : &fValue;
		setValue(fRow.getColType(fFieldIndex[0]), b, e, c, v);
	}
}


template
boost::shared_ptr<WindowFunctionType> WF_nth_value<int64_t>::makeFunction(int, const string&, int);


}   //namespace
// vim:ts=4 sw=4:

