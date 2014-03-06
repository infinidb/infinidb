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

//
// $Id: timestamp.cpp 9210 2013-01-21 14:10:42Z rdempsey $
//

#include <unistd.h>
#include <sys/time.h>
#include <ctime>
#include <string>
#include <cstring>
#include <cstdio>
using namespace std;

#include "timestamp.h"

namespace
{
const struct timeval zerotime = {0, 0};
}

namespace joblist
{

JSTimeStamp::JSTimeStamp() :
	fFirstInsertTime(zerotime),
	fLastInsertTime(zerotime),
	fEndofInputTime(zerotime),
	fFirstReadTime(zerotime),
	fLastReadTime(zerotime)
{
}

/* static */
const string JSTimeStamp::format(const struct timeval& tvbuf)
{
	string res;
	char timeString[50];
	struct tm tmbuf;
#ifdef _MSC_VER
	errno_t p = 0;
	time_t t = tvbuf.tv_sec;
	p = localtime_s(&tmbuf, &t);
	if (p != 0)
	{
		memset(&tmbuf, 0, sizeof(tmbuf));
		strcpy(timeString, "UNKNOWN");
	}
	else
	{
		if (strftime(timeString, 50, "%Y-%m-%d %H:%M:%S", &tmbuf) == 0)
			strcpy(timeString, "UNKNOWN");
	}
#else
	localtime_r(&tvbuf.tv_sec, &tmbuf);
	strftime(timeString, 50, "%F %T", &tmbuf);
#endif
	const int len = strlen(timeString);
	snprintf(&timeString[len], (50-len), ".%06lu", tvbuf.tv_usec);
	res = timeString;
	return res;
}

/* static */
const string JSTimeStamp::tsdiffstr(const struct timeval& t2, const struct timeval& t1)
{
	string res;
	int ds;
	int dus;
	char timeString[50];
	dus = t2.tv_usec - t1.tv_usec;
	ds = t2.tv_sec - t1.tv_sec;
	if (dus < 0)
	{
		ds--;
		dus += 1000000;
	}
	snprintf(timeString, 50, "%d.%06d", ds, dus);
	res = timeString;
	return res;
}

/* static */
const string JSTimeStamp::timeNow()
{
	struct timeval tv;
	gettimeofday(&tv, 0);
	return JSTimeStamp::format(tv);
}

} //namespace joblist

