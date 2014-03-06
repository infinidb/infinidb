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

//  $Id: wf_lead_lag.cpp 3932 2013-06-25 16:08:10Z xlou $


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

#include "wf_lead_lag.h"


namespace windowfunction
{


template<typename T>
boost::shared_ptr<WindowFunctionType> WF_lead_lag<T>::makeFunction(int id, const string& name, int ct)
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
			func.reset(new WF_lead_lag<int64_t>(id, name));
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
			func.reset(new WF_lead_lag<uint64_t>(id, name));
			break;
		}
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
		{
			func.reset(new WF_lead_lag<double>(id, name));
			break;
		}
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
		{
			func.reset(new WF_lead_lag<float>(id, name));
			break;
		}
		default:
		{
			func.reset(new WF_lead_lag<string>(id, name));
			break;
		}
	}

	return func;
}


template<typename T>
WindowFunctionType* WF_lead_lag<T>::clone() const
{
	return new WF_lead_lag<T>(*this);
}


template<typename T>
void WF_lead_lag<T>::resetData()
{
	WindowFunctionType::resetData();
}


template<typename T>
void WF_lead_lag<T>::parseParms(const std::vector<execplan::SRCP>& parms)
{
	// lead | lag
	fLead = 1;
	if (fFunctionId == WF__LAG)
		fLead = -1;

	// parms[0]: value-expr
	// skip

	// parms[1]: offset
	ConstantColumn* cc = dynamic_cast<ConstantColumn*>(parms[1].get());
	if (cc != NULL)
	{
		fOffsetNull = false;
		fOffset = cc->getIntVal(fRow, fOffsetNull) * fLead;  // row not used, no need to setData.
	}

	// parms[2]: default value
	cc = dynamic_cast<ConstantColumn*>(parms[2].get());
	if (cc != NULL)
	{
		fDefNull = false;
		getConstValue(cc, fDefault, fDefNull);
	}

	// parms[3]: respect null | ignore null
	cc = dynamic_cast<ConstantColumn*>(parms[3].get());
	idbassert(cc != NULL);
	bool isNull = false;  // dummy, harded coded
	fRespectNulls = (cc->getIntVal(fRow, isNull) > 0);
}


template<typename T>
void WF_lead_lag<T>::operator()(int64_t b, int64_t e, int64_t c)
{
	uint64_t colIn = fFieldIndex[1];
	bool isNull = true;
	for (int64_t c = b; c <= e; c++)
	{
		if (c % 1000 == 0 && fStep->cancelled())
			break;

		fRow.setData(getPointer(fRowData->at(c)));
		// get offset if not constant
		int64_t idx = fFieldIndex[2];
		if (idx != -1)
		{
			double tmp = 0.0;  // use double to cover all column types
			fOffsetNull = fRow.isNullValue(idx);
			if (!fOffsetNull)
			{
				implicit2T(idx, tmp, 0);

				if (tmp > e) // prevent integer overflow
					tmp = e + 1;
				else if (tmp + e < 0)
					tmp += e - 1;

				fOffset = (int64_t) tmp;
				fOffset *= fLead;
			}
		}

		// get default if not constant
		idx = fFieldIndex[3];
		if (idx != -1)
		{
			fDefNull = fRow.isNullValue(idx);
			if (!fDefNull)
				implicit2T(idx, fDefault, (int) fRow.getScale(idx));
		}

		int64_t o = c + fOffset;
		if (o < b || o > e || fOffsetNull) // out of bound
		{
			T* v = (fDefNull) ? NULL : &fDefault;
			setValue(fRow.getColType(fFieldIndex[0]), b, e, c, v);
			continue;
		}

		if (fRespectNulls == false && fRow.isNullValue(colIn) == true)
		{
			if(fOffset > 0)
			{
				while (++o < e)
				{
					fRow.setData(getPointer(fRowData->at(o)));
					if (fRow.isNullValue(colIn) == false)
						break;
				}

				if (o <= e)
				{
					fRow.setData(getPointer(fRowData->at(o)));
					getValue(colIn, fValue);
					isNull = fRow.isNullValue(colIn);
				}
			}
			else if (fOffset < 0)
			{
				while (--o > b)
				{
					fRow.setData(getPointer(fRowData->at(o)));
					if (fRow.isNullValue(colIn) == false)
						break;
				}

				if (o >= b)
				{
					fRow.setData(getPointer(fRowData->at(o)));
					getValue(colIn, fValue);
					isNull = fRow.isNullValue(colIn);
				}
			}

			T* v = NULL;
			if (!isNull)
				v = &fValue;
			else if (!fDefNull)
				v = &fDefault;

			setValue(fRow.getColType(fFieldIndex[0]), b, e, c, v);
		}
		else
		{
			fRow.setData(getPointer(fRowData->at(o)));
			getValue(colIn, fValue);
			isNull = fRow.isNullValue(colIn);

			T* v = NULL;
			if (!isNull)
				v = &fValue;
			else if (!fDefNull)
				v = &fDefault;
			setValue(fRow.getColType(fFieldIndex[0]), b, e, c, v);
		}
	}
}


template
boost::shared_ptr<WindowFunctionType> WF_lead_lag<int64_t>::makeFunction(int, const string&, int);

template void WF_lead_lag<int64_t>::parseParms(const std::vector<execplan::SRCP>&);
template void WF_lead_lag<uint64_t>::parseParms(const std::vector<execplan::SRCP>&);
template void WF_lead_lag<float>::parseParms(const std::vector<execplan::SRCP>&);
template void WF_lead_lag<double>::parseParms(const std::vector<execplan::SRCP>&);
template void WF_lead_lag<string>::parseParms(const std::vector<execplan::SRCP>&);

}   //namespace
// vim:ts=4 sw=4:

