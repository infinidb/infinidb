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

//  $Id: wf_ntile.cpp 3932 2013-06-25 16:08:10Z xlou $


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
#include "constantcolumn.h"
using namespace execplan;

#include "windowfunctionstep.h"
using namespace joblist;

#include "wf_ntile.h"


namespace windowfunction
{


boost::shared_ptr<WindowFunctionType> WF_ntile::makeFunction(int id, const string& name, int ct)
{
	boost::shared_ptr<WindowFunctionType> func(new WF_ntile(id, name));
	return func;
}


WindowFunctionType* WF_ntile::clone() const
{
	return new WF_ntile(*this);
}


void WF_ntile::resetData()
{
	WindowFunctionType::resetData();
}


void WF_ntile::parseParms(const std::vector<execplan::SRCP>& parms)
{
	// parms[0]: nt
	ConstantColumn* cc = dynamic_cast<ConstantColumn*>(parms[0].get());
	if (cc != NULL)
	{
		fNtileNull = false;
		fNtile = cc->getIntVal(fRow, fNtileNull);  // row not used, no need to setData.
		if (!fNtileNull && fNtile <= 0)
		{
			ostringstream oss;
			oss << fNtile;
			throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_ARG_OUT_OF_RANGE,
						oss.str()), ERR_WF_ARG_OUT_OF_RANGE);
		}
	}
}


void WF_ntile::operator()(int64_t b, int64_t e, int64_t c)
{
	int64_t idx = fFieldIndex[1];
	if (idx != -1)
	{
		fRow.setData(getPointer(fRowData->at(b)));
		if (idx != -1)
		{
			double tmp = 1.0;
			fNtileNull = fRow.isNullValue(idx);
			if (!fNtileNull)
				implicit2T(idx, tmp, 0);

			if (!fNtileNull && tmp <= 0)
			{
				ostringstream oss;
				oss << tmp;
				throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_ARG_OUT_OF_RANGE,
							oss.str()), ERR_WF_ARG_OUT_OF_RANGE);
			}

			if (tmp > e) // prevent integer overflow
				tmp = e + 1;

			fNtile = (int64_t) tmp;
		}
	}

	c = b;
	if (!fNtileNull)
	{
		int64_t rowPerBucket = (e - b + 1) / fNtile;
		int64_t n = rowPerBucket * fNtile;
		int64_t x = (e-b+1) - n; // extra
		int64_t y = 0;
		int64_t z = 0;

		while (c <= e)
		{
			if (c % 1000 == 0 && fStep->cancelled())
				break;

			y = rowPerBucket + ((x-- > 0) ? 1 : 0);
			z++;
			for (int64_t i = 0; i < y && c <= e; i++)
			{
				fRow.setData(getPointer(fRowData->at(c++)));
				setIntValue(fFieldIndex[0], z);
			}
		}
	}
	else
	{
		while (c <= e)
		{
			if (c % 1000 == 0 && fStep->cancelled())
				break;

			fRow.setData(getPointer(fRowData->at(c++)));
			setIntValue(fFieldIndex[0], joblist::BIGINTNULL);
		}
	}
}


}   //namespace
// vim:ts=4 sw=4:

