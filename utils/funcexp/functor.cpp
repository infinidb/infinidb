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
* $Id: functor.cpp 3898 2013-06-17 20:41:05Z rdempsey $
*
*
****************************************************************************/
#ifndef _MSC_VER
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#endif
#include <inttypes.h>
#include <string>
#include <sstream>
using namespace std;

#include "joblisttypes.h"

#include "dataconvert.h"
using namespace dataconvert;

#include "idberrorinfo.h"
#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "functor.h"
#include "funchelpers.h"

using namespace funcexp;

namespace funcexp
{

void Func::init()
{
	uint32_t fni = joblist::FLOATNULL;
	float* fp = reinterpret_cast<float*>(&fni);
	fFloatNullVal = *fp;

	uint64_t dni = joblist::DOUBLENULL;
	double* dp = reinterpret_cast<double*>(&dni);
	fDoubleNullVal = *dp;

}


Func::Func()
{
	init();
}


Func::Func(const string& funcName) : fFuncName(funcName)
{
	init();
}


uint32_t Func::stringToDate(const string str)
{
	int64_t ret = DataConvert::stringToDate(str);
	if (ret == -1)
	{
		Message::Args args;
		args.add("date");
		args.add(str);
		unsigned errcode = ERR_INCORRECT_VALUE;
		throw IDBExcept(IDBErrorInfo::instance()->errorMsg(errcode, args), errcode);
	}

	return ret;
}


uint64_t Func::stringToDatetime(const string str)
{
	int64_t ret = DataConvert::stringToDatetime(str);
	if (ret == -1)
	{
		Message::Args args;
		args.add("datetime");
		args.add(str);
		unsigned errcode = ERR_INCORRECT_VALUE;
		throw IDBExcept(IDBErrorInfo::instance()->errorMsg(errcode, args), errcode);
	}

	return ret;
}


uint32_t Func::intToDate(int64_t i)
{
	if ((uint64_t) i > 0xFFFFFFFFL)
		return ((((uint32_t) (i >> 32)) & 0xFFFFFFC0L) | 0x3E);

	return i;
}


uint64_t Func::intToDatetime(int64_t i)
{
	if ((uint64_t) i < 0xFFFFFFFFL)
		return (i << 32);

	return i;
}


string Func::intToString(int64_t i)
{
    return helpers::intToString(i);
}


string Func::doubleToString(double d)
{
    return helpers::doubleToString(d);
}


} // namespace funcexp
// vim:ts=4 sw=4:



