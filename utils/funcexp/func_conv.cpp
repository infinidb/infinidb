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
* $Id: func_conv.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
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

namespace
{
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
	cutlim = (uint32_t) ((~(uint64_t) 0) % (uint64_t) base);
	
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
}

namespace funcexp
{
namespace helpers
{
const char *convNumToStr(int64_t val,char *dst,int radix)
{
	if (radix == 16 || radix == -16)
#ifdef _MSC_VER
		sprintf(dst, "%llX", val);
#else
		sprintf(dst, "%lX", val);
#endif
	else if (radix == 8 || radix == -8)
#ifdef _MSC_VER
		sprintf(dst, "%llo", val);
#else
		sprintf(dst, "%lo", val);
#endif
	else if (radix == 10)
	{
		uint64_t uval = static_cast<uint64_t>(val);
#ifdef _MSC_VER
		sprintf(dst, "%llu", uval);
#else
		sprintf(dst, "%lu", uval);
#endif
	}
	else if (radix == -10)
#ifdef _MSC_VER
		sprintf(dst, "%lld", val);
#else
		sprintf(dst, "%ld", val);
#endif
	else if (radix == 2 || radix == -2)
	{
		char tmp[65];
		char* ptr = &tmp[64];
		*ptr-- = 0;
		for (int i = 0; i < 64; i++)
		{
			if (val&1)
				*ptr-- = '1';
			else
				*ptr-- = '0';
			val >>= 1;
		}
		ptr = strchr(tmp, '1');
		if (ptr == 0)
			strcpy(dst, &tmp[63]);
		else
			strcpy(dst, ptr);
	}
	else if (radix == 4 || radix == -4)
	{
		char tmp[33];
		char* ptr = &tmp[32];
		*ptr-- = 0;
		for (int i = 0; i < 32; i++)
		{
			*ptr-- = '0' + (val&3);
			val >>= 2;
		}
		ptr = strpbrk(tmp, "123");
		if (ptr == 0)
			strcpy(dst, &tmp[31]);
		else
			strcpy(dst, ptr);
	}
#if 0
	else if (radix == 8 || radix == -8)
	{
		char tmp[23];
		char* ptr = &tmp[22];
		*ptr-- = 0;
		for (int i = 0; i < 22; i++)
		{
			*ptr-- = '0' + (val&7);
			val >>= 3;
		}
		ptr = strpbrk(tmp, "1234567");
		if (ptr == 0)
			strcpy(dst, &tmp[21]);
		else
			strcpy(dst, ptr);
	}
	else if (radix == 16 || radix == -16)
	{
		char tmp[17];
		char* ptr = &tmp[16];
		*ptr-- = 0;
		for (int i = 0; i < 16; i++)
		{
			int v = val&0xf;
			if (v > 9)
				*ptr-- = 'A' + v - 10;
			else
				*ptr-- = '0' + v;
			val >>= 4;
		}
		ptr = strpbrk(tmp, "123456789ABCDEF");
		if (ptr == 0)
			strcpy(dst, &tmp[15]);
		else
			strcpy(dst, ptr);
	}
#endif
	else if (radix == 32 || radix == -32)
	{
		char tmp[14];
		char* ptr = &tmp[13];
		*ptr-- = 0;
		for (int i = 0; i < 13; i++)
		{
			int v = val&0x1f;
			if (v > 9)
				*ptr-- = 'A' + v - 10;
			else
				*ptr-- = '0' + v;
			val >>= 5;
		}
		ptr = strpbrk(tmp, "123456789ABCDEFGHIJKLMNOPQRSTUV");
		if (ptr == 0)
			strcpy(dst, &tmp[12]);
		else
			strcpy(dst, ptr);
	}
	else
		*dst = 0;
	return dst;
}
} //namespace funcexp::helpers

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
	const string& res= parm[0]->data()->getStrVal(row, isNull);
	string str;
	char ans[65];
	int64_t dec;
	int64_t from_base = parm[1]->data()->getIntVal(row, isNull);
	int64_t to_base = parm[2]->data()->getIntVal(row, isNull);

	if (isNull || abs(static_cast<int>(to_base)) > 36 || abs(static_cast<int>(to_base)) < 2 ||
      abs(static_cast<int>(from_base)) > 36 || abs(static_cast<int>(from_base)) < 2 || !(res.length()))
	{
		isNull = true;
		return "";
	}

	if (from_base < 0)
		dec= convStrToNum(res, -from_base, false);
	else
		dec= (int64_t) convStrToNum( res, from_base, true);

	str = helpers::convNumToStr(dec, ans, to_base);

	isNull = str.empty();

	return str;
}


} // namespace funcexp
// vim:ts=4 sw=4:
