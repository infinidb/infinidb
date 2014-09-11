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
*   $Id: profiling.h 7409 2011-02-08 14:38:50Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef JOBLIST_PROFILING_H_
#define JOBLIST_PROFILING_H_

#include<string>
#include<vector>
#include<map>
#include<ostream>
#include<stdint.h>
#include <unistd.h>

namespace joblist
{
// forward reference for ostream& operator<<(std::ostream&, ProfileGroup&);
class ProfileGroup;

class ProfilePoint
{
public:
	// constructor, default copy constructor will be enough.
	ProfilePoint(const std::string&);

#if defined(CLOCK_REALTIME)
	void start()   { clock_gettime(CLOCK_REALTIME, &fIntBegin); }
#elif defined(_my_pthread_h)
	//FIXME: This looks suspect!
	void start()   { fIntBegin.tv.i64 = fIntBegin.max_timeout_msec = 0; }
#else
	void start()   { fIntBegin.tv_sec = fIntBegin.tv_nsec = 0; }
#endif
	void stop();
	timespec ttl() { return fTtl; }
	timespec average() const;

	friend std::ostream& operator<<(std::ostream&, const ProfilePoint&);
	friend std::ostream& operator<<(std::ostream&, ProfileGroup&);

private:
	timespec fIntBegin;				// interval timing start
	timespec fTtl;					// total time
	timespec fMax;					// max interval
	timespec fMin;					// min interval
	uint64_t fCount;				// checkpoint access count
	double fPercent;                // percentage in whole process
	std::string fCheckpoint;		// name of the point
};


class ProfileGroup
{
public:
	// constructor, default copy constructor will be enough.
	ProfileGroup();

	// initialize the ProfilePoint vector
	void initialize(uint64_t pointCount, const std::string* pointName);

	friend class ProfileData;

	// output to a stream
	friend std::ostream& operator<<(std::ostream&, ProfileGroup&);

protected:
	std::vector<ProfilePoint> fPoints;
};


class ProfileData
{
public:
	// constructor, default copy constructor will be enough.
	ProfileData();

	// initialize the ProfilePoint vector
	void initialize(const std::string& name);

	// reset start time to current time
	void restart();

	// start/stop timing at group[g] point[p]
	void start(uint64_t g, uint64_t p);
	void stop(uint64_t g, uint64_t p);

	// add a group
	void addGroup(ProfileGroup& g) { fPrfGroup.push_back(g); }

	// output to a stream
	friend std::ostream& operator<<(std::ostream&, ProfileData&);

private:
	std::string fName;
	timespec fTs1, fTs2;

	std::vector<ProfileGroup> fPrfGroup;
};

extern const timespec timespec_ini();
extern const timespec timespec_max();
extern const timespec timespec_min();
extern int64_t timespec_compare(const timespec &tv1, const timespec &tv2);
extern timespec timespec_add(const timespec &tv1, const timespec &tv2);
extern timespec timespec_sub(const timespec &tv1, const timespec &tv2);

extern std::ostream& operator<<(std::ostream&, const timespec&);
extern std::ostream& operator<<(std::ostream&, const ProfilePoint&);
extern std::ostream& operator<<(std::ostream&, ProfileGroup&);
extern std::ostream& operator<<(std::ostream&, ProfileData&);

}

#endif // JOBLIST_PROFILING_H_
