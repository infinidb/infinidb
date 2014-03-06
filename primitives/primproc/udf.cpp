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

// $Id: udf.cpp 2035 2013-01-21 14:12:19Z rdempsey $

#ifndef _MSC_VER
#include <dlfcn.h>
#endif
#include <string>
#include <sstream>
#include <iostream>
using namespace std;

#include "primproc.h"
using namespace primitiveprocessor;

namespace
{
const string UdfLibName("libcaludf.so");
}

namespace primitiveprocessor
{

void loadUDFs()
{
#ifndef _MSC_VER
	int flags = RTLD_NOW;

	void* libPtr = 0;

	UDFFcnPtr_t fcnPtr;

	libPtr = dlopen(UdfLibName.c_str(), flags);

	if (libPtr == 0)
		return;

	unsigned i = 1;
	for (;;)
	{
		ostringstream oss;
		oss << "cpfunc" << i;
		fcnPtr = (UDFFcnPtr_t)dlsym(libPtr, oss.str().c_str());
		if (fcnPtr == 0)
			break;
		UDFFcnMap[i] = fcnPtr;
		i++;
	}
	cout << "loaded " << UDFFcnMap.size() << " UDF's" << endl;
#endif
}

}
// vim:ts=4 sw=4:

