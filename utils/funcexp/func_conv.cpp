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

/****************************************************************************
* $Id: func_conv.cpp 3011 2012-02-29 22:02:53Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <unistd.h>
#include <limits.h>
using namespace std;

#include "functor_str.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

namespace funcexp
{
char digit_upper[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

int64_t convStrToNum(const string& str, int base, bool unsignedFlag)
{
	int negative;
	uint64_t cutoff, cutlim, i, j, save;
	int overflow;
	
	// to skip the leading spaces.
	for (i = 0; i < str.length() && str.c_str()[i] == ' '; i++)
	{}

	if (i == str.length())
	{
		return 0L;
	}

	if (str.c_str()[i] == '-')
	{
		negative = 1;
		++i;
	}
	else if (str.c_str()[i] == '+')
	{
		negative = 0;
		++i;
	}
	else
		negative = 0;

	save = i;
	
	cutoff = (~(uint64_t) 0) / (uint64_t) base;
	cutlim = (uint) ((~(uint64_t) 0) % (uint64_t) base);
	
	overflow = 0;
	j = 0;
	for (; i < str.length(); i++)
	{
		unsigned char c= str.c_str()[i];
		if (c>='0' && c<='9')
			c -= '0';
		else if (c>='A' && c<='Z')
			c = c - 'A' + 10;
		else if (c>='a' && c<='z')
			c = c - 'a' + 10;
		else
			break;
		if (c >= base)
			break;
		if (j > cutoff || (j == cutoff && c > cutlim))
			overflow = 1;
		else
		{
			j *= (uint64_t) base;
			j += c;
		}
	}
	
	if (i == save)
		return 0L;

	if (!unsignedFlag)
	{
		if (negative)
		{
			if (j  > (uint64_t) numeric_limits<int64_t>::min())
				overflow = 1;
		}
		else if (j > (uint64_t) numeric_limits<int64_t>::max())
		{
			overflow = 1;
		}
	}

	if (overflow)
	{
		if (unsignedFlag)
			return (~(uint64_t) 0);
		return negative ? numeric_limits<int64_t>::min() : numeric_limits<int64_t>::max();
	}

	return (negative ? -((int64_t) j) : (int64_t) j);
}

char *convNumToStr(int64_t val,char *dst,int radix)
{
	char buffer[65];
	register char *p;
	int64_t long_val;
	uint64_t uval= (uint64_t) val;
	
	if (radix < 0)
	{
		if (radix < -36 || radix > -2) return (char*) 0;
		if (val < 0) {
			*dst++ = '-';
			uval = (uint64_t)0 - uval;
		}
		radix = -radix;
	}
	else
	{
		if (radix > 36 || radix < 2) return (char*) 0;
	}
	if (uval == 0)
	{
		*dst++='0';
		*dst='\0';
		return dst;
	}
	p = &buffer[sizeof(buffer)-1];
	*p = '\0';

	while (uval > (uint64_t) LONG_MAX)
	{
		uint64_t quo= uval/(uint) radix;
		uint rem= (uint) (uval- quo* (uint) radix);
		*--p = digit_upper[rem];
		uval= quo;
	}
	long_val= (int64_t) uval;
	while (long_val != 0)
	{
		int64_t quo= long_val/radix;
		*--p = digit_upper[(unsigned char) (long_val - quo*radix)];
		long_val= quo;
	}
	while ((*dst++ = *p++) != 0) ;
	return dst-1;
}

CalpontSystemCatalog::ColType Func_conv::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}


string Func_conv::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	string res= parm[0]->data()->getStrVal(row, isNull);
	string str;
	char ans[65],*ptr;
	int64_t dec;
	int64_t from_base = parm[1]->data()->getIntVal(row, isNull);
	int64_t to_base = parm[2]->data()->getIntVal(row, isNull);

	if (isNull || abs(static_cast<int>(to_base)) > 36 || abs(static_cast<int>(to_base)) < 2 ||
      abs(static_cast<int>(from_base)) > 36 || abs(static_cast<int>(from_base)) < 2 || !(res.length()))
	{
		isNull = true;
		return "";
	}
	isNull = false;

	if (from_base < 0)
		dec= convStrToNum(res, -from_base, false);
	else
		dec= (int64_t) convStrToNum( res, from_base, true);

	ptr= convNumToStr(dec, ans, to_base);

	str.append(ans, 0, (uint)(ptr-ans));

	return str;
}


} // namespace funcexp
// vim:ts=4 sw=4:
