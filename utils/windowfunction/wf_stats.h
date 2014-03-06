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

//  $Id: wf_stats.h 3868 2013-06-06 22:13:05Z xlou $


#ifndef UTILS_WF_STATS_H
#define UTILS_WF_STATS_H

#include "windowfunctiontype.h"


namespace windowfunction
{


template<typename T>
class WF_stats : public WindowFunctionType
{
public:
	WF_stats(int id, const std::string& name) : WindowFunctionType(id, name) {resetData();}

	// pure virtual in base
	void operator()(int64_t b, int64_t e, int64_t c);
	WindowFunctionType* clone() const;
	void resetData();

	static boost::shared_ptr<WindowFunctionType> makeFunction(int, const string&, int);

protected:
	long double fSum1;
	long double fSum2;
	uint64_t    fCount;
	double      fStats;
};


} // namespace

#endif  // UTILS_WF_STATS_H

// vim:ts=4 sw=4:

