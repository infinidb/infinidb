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

// $Id: tpchrf2.h 2101 2013-01-21 14:12:52Z rdempsey $

#ifndef TPCHRF2_H__
#define TPCHRF2_H__

#include <string>
#include <istream>
#include <cstddef>
#include <limits>
#include <stdint.h>

namespace tpch
{

class RF2
{
public:
	RF2(const std::string& sn, uint32_t sid, uint32_t tflg=0, int c=std::numeric_limits<int>::max(),
		int p=1, bool d=false, bool v=false);
	~RF2();

	int run(std::istream& in);

private:
	//RF2(const RF2& rhs);
	//RF2& operator=(const RF2& rhs);

	std::string fSchema;
	uint32_t fSessionID;
	uint32_t fTflg;
	int fIntvl;
	int fPack;
	bool fDflg;
	bool fVflg;
};

}

#endif
