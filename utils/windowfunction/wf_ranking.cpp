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

//  $Id: wf_ranking.cpp 3932 2013-06-25 16:08:10Z xlou $


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

#include "wf_ranking.h"


namespace windowfunction
{


boost::shared_ptr<WindowFunctionType> WF_ranking::makeFunction(int id, const string& name, int ct)
{
	boost::shared_ptr<WindowFunctionType> func(new WF_ranking(id, name));
	return func;
}


WindowFunctionType* WF_ranking::clone() const
{
	return new WF_ranking(*this);
}


void WF_ranking::resetData()
{
	fRank = 0;
	fDups = 0;

	WindowFunctionType::resetData();
}


void WF_ranking::operator()(int64_t b, int64_t e, int64_t c)
{
	// one row handling
	if (fPartition.first == fPartition.second)
	{
		fRow.setData(getPointer(fRowData->at(fPartition.first)));
		int64_t r = (fFunctionId == WF__PERCENT_RANK) ? 0 : 1;
		if (fFunctionId == WF__RANK || fFunctionId == WF__DENSE_RANK)
			setIntValue(fFieldIndex[0], r);
		else
			setDoubleValue(fFieldIndex[0], r);

		return;
	}

	// more than one row, e > b
	b = fPartition.first;
	e = fPartition.second;
	double n1 = e - b;    // count(*) - 1, n will not be 0.
	for (c = b; c <= e; c++)
	{
		if (c % 1000 == 0 && fStep->cancelled())
			break;

		if (c != b &&
			fPeer->operator()(getPointer(fRowData->at(c)), getPointer(fRowData->at(c-1))))
		{
			fDups++;
		}
		else
		{
			fRank++;
			if (fFunctionId != WF__DENSE_RANK)
				fRank += fDups;
			fDups = 0;
		}

		fRow.setData(getPointer(fRowData->at(c)));
		if (fFunctionId != WF__PERCENT_RANK)
			setIntValue(fFieldIndex[0], fRank);
		else
			setDoubleValue(fFieldIndex[0], (fRank-1)/n1);
	}

	// Two-pass, need to find peers.
	if (fFunctionId == WF__CUME_DIST)
	{
		int prevRank = ++fRank + fDups;  // hypothetical row at (e+1)
		double n0 = (e - b + 1);         // count(*)
		double cumeDist = 1;
		fRow.setData(getPointer(fRowData->at(e)));
		for (c = e; c >= b; c--)
		{
			if (c % 1000 == 0 && fStep->cancelled())
				break;

			fRow.setData(getPointer(fRowData->at(c)));
			int currRank = getIntValue(fFieldIndex[0]);
			if (currRank != prevRank)
			{
				cumeDist = ((double) (prevRank-1)) / n0;
				prevRank = currRank;
			}

			setDoubleValue(fFieldIndex[0], cumeDist);
		}
	}
}


}   //namespace
// vim:ts=4 sw=4:

