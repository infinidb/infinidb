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

/***********************************************************************
*   $Id: profiling.cpp 8436 2012-04-04 18:18:21Z rdempsey $
*
*
***********************************************************************/

#include<iostream>
#include<iomanip>
#include<limits>
using namespace std;

#include "profiling.h"

namespace joblist
{
const timespec timespec_ini()
{
	timespec t;
	t.tv_sec  = t.tv_nsec = 0L;
	return t;
}
const timespec TimespecIni_c = timespec_ini();

const timespec timespec_max()
{
	timespec t;
	t.tv_sec  = t.tv_nsec = numeric_limits<long>::max();
	return t;
}
const timespec TimespecMax_c = timespec_max();

const timespec timespec_min()
{
	timespec t;
	t.tv_sec  = t.tv_nsec = numeric_limits<long>::min();
	return t;
}
const timespec TimespecMin_c = timespec_min();

int64_t timespec_compare(const timespec &tv1, const timespec &tv2)
{
	if (tv1.tv_sec > tv2.tv_sec)
		return 1;
	if (tv1.tv_sec < tv2.tv_sec)
		return -1;

	return (tv1.tv_nsec - tv2.tv_nsec);
}

timespec timespec_add(const timespec &tv1, const timespec &tv2) 
{
	timespec ts;
	ts.tv_sec = tv1.tv_sec + tv2.tv_sec;
	ts.tv_nsec = tv1.tv_nsec + tv2.tv_nsec;
	if (ts.tv_nsec >= 1000000000)
	{
		ts.tv_sec += 1;
		ts.tv_nsec -= 1000000000;
	}
	return ts;
}

timespec timespec_sub(const struct timespec &tv1, const struct timespec &tv2) 
{
	timespec ts;
	if (tv2.tv_nsec < tv1.tv_nsec)
	{
		ts.tv_sec = tv2.tv_sec - tv1.tv_sec - 1;
		ts.tv_nsec = tv2.tv_nsec + 1000000000 - tv1.tv_nsec;
	}
	else
	{
		ts.tv_sec = tv2.tv_sec - tv1.tv_sec;
		ts.tv_nsec = tv2.tv_nsec - tv1.tv_nsec;
	}
	return ts;
}

static string ind_s[] = {"  "};

ProfilePoint::ProfilePoint(const string& pp) :
    fIntBegin(TimespecIni_c),
    fTtl(TimespecIni_c),
    fMax(TimespecMin_c),
    fMin(TimespecMax_c),
    fCount(0),
    fPercent(0),
    fCheckpoint(pp)
{}

void ProfilePoint::stop()
{
	timespec t;
#ifdef _MSC_VER
	t.tv_nsec = t.tv_sec = 0;
#else
	clock_gettime(CLOCK_REALTIME, &t);
#endif
	timespec interval = timespec_sub(fIntBegin, t);
	if (timespec_compare(interval, fMax) > 0) fMax = interval;
	if (timespec_compare(interval, fMin) < 0) fMin = interval;
	fTtl = timespec_add(fTtl, interval);
	++fCount;
}

timespec ProfilePoint::average() const
{
	timespec t;
	double avg = (((double) fTtl.tv_sec) * 1000000000 + fTtl.tv_nsec) / (double) fCount;
	t.tv_sec  = (long) (avg / 1000000000);
	t.tv_nsec = (long) (avg - (double) t.tv_sec * 1000000000);

	return t;
}

ostream& operator<<(ostream& os, const timespec& t)
{
	os << t.tv_sec << "." << setw(9) << setfill('0') << t.tv_nsec << "s";
	return os;
}

ostream& operator<<(ostream& os, const ProfilePoint& p)
{
	if (p.fCount > 0)
		os  << "  " << setw(6) << setfill(' ') << fixed << setprecision(2)
			<< p.fPercent << "% time spent at "
			<< p.fCheckpoint << ":\n      total=" << p.fTtl << ", max=" << p.fMax
			<< ", avg=" << p.average() << ", min=" << p.fMin << ",  #=" << p.fCount << endl;
	return os;
}

ProfileGroup::ProfileGroup()
{}

void ProfileGroup::initialize(uint64_t pointCount, const string* pointName)
{
	for (uint64_t i = 0; i < pointCount; i++)
	{
		ProfilePoint pp(pointName[i]);
		fPoints.push_back(pp);
	}
}

ostream& operator<<(ostream& os, ProfileGroup& group)
{
	vector<ProfilePoint>::iterator i = group.fPoints.begin(), end = group.fPoints.end();
	timespec ts = (i++)->ttl();

	// fPoints[0] has the overall timing of this group, use that to calculate percentage
	double totalNsec = (double) ts.tv_sec*1000000000 + (double) ts.tv_nsec;
	os << "  ==> thread " << group.fPoints[0].fCheckpoint << " uses " << ts << endl;

	if (ts.tv_sec == 0 && ts.tv_nsec == 0)
		return os;

	while (i != end)
	{
		i->fPercent = ((double) i->fTtl.tv_sec*1000000000 + (double) i->fTtl.tv_nsec) / totalNsec;
		i->fPercent *= 100;

		os << *i++;
	}

	return os;
}

ProfileData::ProfileData() : fName(), fTs1(TimespecIni_c), fTs2(TimespecIni_c)
{}

void ProfileData::initialize(const string& dataName)
{
	fName = dataName;
#ifdef _MSC_VER
	fTs1.tv_nsec = fTs1.tv_sec = 0;
#else
	clock_gettime(CLOCK_REALTIME, &fTs1);
#endif
}

void ProfileData::restart()
{
#ifdef _MSC_VER
	fTs1.tv_nsec = fTs1.tv_sec = 0;
#else
	clock_gettime(CLOCK_REALTIME, &fTs1);
#endif
}

void ProfileData::start(uint64_t group, uint64_t point)
{
	fPrfGroup[group].fPoints[point].start();
}

void ProfileData::stop(uint64_t group, uint64_t point)
{
	fPrfGroup[group].fPoints[point].stop();
}


ostream& operator<<(ostream& os, ProfileData& data)
{
#ifdef _MSC_VER
	data.fTs2.tv_nsec = data.fTs2.tv_sec = 0;
#else
	clock_gettime(CLOCK_REALTIME, &(data.fTs2));
#endif
	if (data.fName.length() > 0) os << data.fName << " spent: ";
	os << timespec_sub(data.fTs1, data.fTs2) << endl;
	vector<ProfileGroup>::iterator i = data.fPrfGroup.begin(), end = data.fPrfGroup.end();
	while (i != end)
		os << *i++;

	return os;
}

}
