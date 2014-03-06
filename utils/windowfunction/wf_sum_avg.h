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

//  $Id: wf_sum_avg.h 3868 2013-06-06 22:13:05Z xlou $


#ifndef UTILS_WF_SUM_AVG_H
#define UTILS_WF_SUM_AVG_H

#include <set>
#include "windowfunctiontype.h"


namespace windowfunction
{


template<typename T>
class WF_sum_avg : public WindowFunctionType
{
public:
	WF_sum_avg(int id, const std::string& name) :
		WindowFunctionType(id, name), fDistinct(id != WF__SUM && id != WF__AVG) {resetData();}

	// pure virtual in base
	void operator()(int64_t b, int64_t e, int64_t c);
	WindowFunctionType* clone() const;
	void resetData();

	static boost::shared_ptr<WindowFunctionType> makeFunction(int, const string&, int);

protected:
	T           fAvg;
	T           fSum;
	uint64_t    fCount;
	bool        fDistinct;
	std::set<T> fSet;
};


} // namespace

#endif  // UTILS_WF_SUM_AVG_H

// vim:ts=4 sw=4:

