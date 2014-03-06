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
//#define NDEBUG
#include <cassert>
#include <string>
#include <cstdio>
#include <cstring>
using namespace std;

namespace
{
typedef int64_t longlong;

string toString(int64_t t1, unsigned scale)
{
	longlong int_val = (longlong)t1;
	// MySQL seems to round off values unless we use the string store method. Groan.
	// Taken from tablefuncs.cpp

	//biggest Calpont supports is DECIMAL(18,x), or 18 total digits+dp+sign
	const int ctmp_size = 18+1+1+1;
	char ctmp[ctmp_size];
	snprintf(ctmp, ctmp_size,
#if __WORDSIZE <= 32
		"%lld",
#else
		"%ld",
#endif
		int_val);
	//we want to move the last dt_scale chars right by one spot to insert the dp
	//we want to move the trailing null as well, so it's really dt_scale+1 chars
	size_t l1 = strlen(ctmp);
	//need to make sure we have enough leading zeros for this to work...
	//at this point scale is always > 0
	char* ptr = &ctmp[0];
	if (int_val < 0)
	{
		ptr++;
		idbassert(l1 >= 2);
		l1--;
	}
	if (scale > l1)
	{
		const char* zeros = "000000000000000000"; //18 0's
		size_t diff = scale - l1; //this will always be > 0
		memmove((ptr + diff), ptr, l1 + 1); //also move null
		memcpy(ptr, zeros, diff);
		l1 = 0;
	}
	else
		l1 -= scale;
	memmove((ptr + l1 + 1), (ptr + l1), scale + 1); //also move null
	*(ptr + l1) = '.';

	return string(ctmp);
}
}

int main(int argc, char** argv)
{
	int64_t x;
	string xstr;

	x = 10001LL;
	xstr = toString(x, 2);
	idbassert(xstr == "100.01");

	x = -10001LL;
	xstr = toString(x, 2);
	idbassert(xstr == "-100.01");

	x = 999999999999999999LL;
	xstr = toString(x, 2);
	idbassert(xstr == "9999999999999999.99");

	x = -999999999999999999LL;
	xstr = toString(x, 2);
	idbassert(xstr == "-9999999999999999.99");

	x = 1LL;
	xstr = toString(x, 5);
	idbassert(xstr == ".00001");

	x = -1LL;
	xstr = toString(x, 5);
	idbassert(xstr == "-.00001");

	x = -1LL;
	xstr = toString(x, 16);
	idbassert(xstr == "-.0000000000000001");

	return 0;
}

