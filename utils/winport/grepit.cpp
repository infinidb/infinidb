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

#include <iostream>
#include <string>
using namespace std;

#include <boost/regex.hpp>

#include "grepit.h"

namespace
{
	const int lineLen = 1024;
}

namespace winport
{
	bool grepit(istream& is, const string& pattern)
	{
		boost::regex pat(pattern);
		string cInput;
		getline(is, cInput);
		while (is.good())
		{
			if (boost::regex_match(cInput, pat))
				return true;
			getline(is, cInput);
		}
		return false;
	}
}
