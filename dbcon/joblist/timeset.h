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

/******************************************************************************
 * $Id: timeset.h 9655 2013-06-25 23:08:13Z xlou $
 *
 *****************************************************************************/

/** @file timeset.h
 * Used to maintain collection of timers tracking elapsed times for set of tasks
 */

#ifndef TIMESET_H
#define TIMESET_H

#include <ctime>
#include <iostream>
#include <string>
#include <map>

namespace joblist
{

//------------------------------------------------------------------------------
/** @brief Maintains a group of simulated timers and their elapsed times.
 *
 * This class maintains a collection of timers that can be used to collect
 * statistics about the cumulative elapsed time spent on selected tasks.
 */
//------------------------------------------------------------------------------
class TimeSet
{
	// TimerMap maps a timer to its start time
	// ElapsedMap maps a timer to its cumulative elapsed time
	typedef  std::map<std::string, timespec > TimerMap;
	typedef  std::map<std::string, timespec > ElapsedMap;

  public:
	TimeSet() : fTimer(), fElapsed() {}
	void   displayAll() const;
	void   display   (const std::string& key) const;
	double totalTime () const;
	double totalTime (const std::string& key) const;
	void   holdTimer (const std::string& key);
	void   startTimer(const std::string& key);
	void   stopTimer (const std::string& key);
	void   setTimer  (const std::string& key, bool start = true);
	TimeSet& operator+=(const TimeSet& rhs);
	void clear() { fTimer.clear(); fElapsed.clear(); }

  private:
	void timespec_sub(const struct timespec &tv1, // start time
	                  const struct timespec &tv2, // end time
	                  struct timespec       &diff) const;
	void timespec_add(const struct timespec &tv1,
	                  const struct timespec &tv2,
	                  struct timespec       &sum) const;

	TimerMap   fTimer;
	ElapsedMap fElapsed;
};

//------------------------------------------------------------------------------
// Print the contents (to std::cout) of all timers in the ElapsedMap map.
//------------------------------------------------------------------------------
inline
void TimeSet::displayAll() const
{
	ElapsedMap::const_iterator itend = fElapsed.end();
	for (ElapsedMap::const_iterator it = fElapsed.begin(); it != itend; ++it)
	{
		double t;
#if defined(_MSC_VER) && defined(_my_pthread_h)
		//FIXME
		t = 0.0;
#else
		t =  (double)it->second.tv_sec +
					(double)it->second.tv_nsec / 1000000000.0;
#endif
		std::cout << "TimeSet " << it->first << ": " << t << "s\n";
	}
	std::cout.flush();
}

//------------------------------------------------------------------------------
// Print the specified timer to std::cout.
// If specified timer is in the ElapsedMap map, then the contents of that
// timer will be printed, else if the specified timer is in the TimerMap map,
// then the contents of that timer will be printed.
// key (in) - string that identifies the timer to be printed.
//------------------------------------------------------------------------------
inline
void TimeSet::display(const std::string& key) const
{
	ElapsedMap::const_iterator em = fElapsed.find(key);
	if (fElapsed.end() != em)
	{
		double t;
#if defined(_MSC_VER) && defined(_my_pthread_h)
		t = 0.0;
#else
		t =  (double)em->second.tv_sec +
					(double)em->second.tv_nsec / 1000000000.0;
#endif
		std::cout << "TimeSet elapse " << em->first << ": " << t << "s\n";
	}
	else
	{
		TimerMap::const_iterator tm = fTimer.find(key);
		if (fTimer.end() != tm)
		{
			double t;
#if defined(_MSC_VER) && defined(_my_pthread_h)
			t = 0.0;
#else
			t =  (double)tm->second.tv_sec +
						(double)tm->second.tv_nsec / 1000000000.0;
#endif
			std::cout << "TimeSet start " << tm->first << ": " << t << "s\n";
		}
	}
	std::cout.flush();
}

//------------------------------------------------------------------------------
// Return sum of all timer elapsed times.
//------------------------------------------------------------------------------
inline
double TimeSet::totalTime() const
{
	struct timespec tSum               = {0,0};
	ElapsedMap::const_iterator itend   = fElapsed.end();
	for (ElapsedMap::const_iterator it = fElapsed.begin(); it != itend; ++it)
	{
#if defined(_MSC_VER) && !defined(_my_pthread_h)
		tSum.tv_sec  += it->second.tv_sec;
		tSum.tv_nsec += it->second.tv_nsec;
#endif
	}
	double totSeconds;
#if defined(_MSC_VER) && defined(_my_pthread_h)
	totSeconds = 0.0;
#else
	totSeconds = (double)tSum.tv_sec + (double)tSum.tv_nsec/1000000000.0;
#endif

	return totSeconds;
}

//------------------------------------------------------------------------------
// Return current elapsed time for the specified timer.
// key (in) - string that identifies the timer of interest.
//------------------------------------------------------------------------------
inline
double TimeSet::totalTime(const std::string& key) const
{
	ElapsedMap::const_iterator el = fElapsed.find(key);
	if (fElapsed.end() != el)
	{
		double totSeconds;
#if defined(_MSC_VER) && defined(_my_pthread_h)
		totSeconds = 0.0;
#else
		totSeconds =
			(double)el->second.tv_sec +
			(double)el->second.tv_nsec/1000000000.0;
#endif
		return totSeconds;
	}
	else
	{
		return 0;
	}
}

//------------------------------------------------------------------------------
// "Hold" the specified timer.
// Elapsed time is recorded, and added to total elapsed time.
// Unlike stopTimer(), the start time is not reset to current time; instead
// startTimer() or setTimer() must be called by the application code when it
// is time to reset the start time and effectively resume or restart the timer.
// key (in) - string that identifies the timer to be "held".
//------------------------------------------------------------------------------
inline
void TimeSet::holdTimer(const std::string& key)
{
	TimerMap::iterator it = fTimer.find(key);
	if (fTimer.end() != it)
	{
		struct timespec tEnd;
		struct timespec tDiff;
#if defined(CLOCK_REALTIME)
		clock_gettime(CLOCK_REALTIME, &tEnd);
#elif defined(_MSC_VER) && defined(_my_pthread_h)
		tEnd.tv.i64 = tEnd.max_timeout_msec = 0;
#else
		tEnd.tv_sec = tEnd.tv_nsec = 0;
#endif
		timespec_sub( it->second, tEnd, tDiff );
		struct timespec tElapsed = fElapsed[key];
		timespec_add( tElapsed, tDiff, fElapsed[key] );
	}
}

//------------------------------------------------------------------------------
// Start the specified timer.
// Start time is set to current time.
// key (in) - string that identifies the timer to be started.
//------------------------------------------------------------------------------
inline
void TimeSet::startTimer(const std::string& key)
{
	struct timespec ts;
#if defined(CLOCK_REALTIME)
	clock_gettime(CLOCK_REALTIME, &ts);
#elif defined(_MSC_VER) && defined(_my_pthread_h)
	ts.tv.i64 = ts.max_timeout_msec = 0;
#else
	ts.tv_sec = ts.tv_nsec = 0;
#endif
	fTimer[key] = ts;
}

//------------------------------------------------------------------------------
// Stop the specified timer.
// Elapsed time is recorded, and start time is set to current time.
// key (in) - string that identifies the timer to be stopped.
//------------------------------------------------------------------------------
inline
void TimeSet::stopTimer(const std::string& key)
{
	TimerMap::iterator it = fTimer.find(key);
	if (fTimer.end() != it)
	{
		struct timespec tEnd;
		struct timespec tDiff;
#if defined(CLOCK_REALTIME)
		clock_gettime(CLOCK_REALTIME, &tEnd);
#elif defined(_MSC_VER) && defined(_my_pthread_h)
		tEnd.tv.i64 = tEnd.max_timeout_msec = 0;
#else
		tEnd.tv_sec = tEnd.tv_nsec = 0;
#endif
		timespec_sub( it->second, tEnd, tDiff );
		fElapsed[key] = tDiff;
		fTimer[key]   = tEnd;
	}
}

//------------------------------------------------------------------------------
// Start or stop the specified timer.
// key (in)   - string that identifies the timer to be started or stopped.
// start (in) - boolean indicating whether to start or stop the timer.
//------------------------------------------------------------------------------
inline
void TimeSet::setTimer(const std::string& key, bool start /*= true*/)
{
	if (start)
		startTimer(key);
	else
		stopTimer(key);
}

//------------------------------------------------------------------------------
// Adds the specified TimeSet elapsed times to "this" TimeSet.
// rhs (in) - TimeSet with elapsed times to be added to "this" TimeSet.
//------------------------------------------------------------------------------
inline
TimeSet& TimeSet::operator+=(const TimeSet& rhs)
{
	ElapsedMap::const_iterator itend = rhs.fElapsed.end();
	for (ElapsedMap::const_iterator it = rhs.fElapsed.begin(); it != itend; ++it)
	{
		struct timespec tLhs = fElapsed[it->first];
		timespec_add( tLhs, it->second, fElapsed[it->first] );
	}

	return (*this);
}

//------------------------------------------------------------------------------
// Takes the difference between 2 timespec values.
// tv1  (in)  - first timespec value (subtracted from tv2)
// tv2  (in)  - second timespec value
// diff (out) - value of tv2 - tv1
//------------------------------------------------------------------------------
inline
void TimeSet::timespec_sub(const struct timespec &tv1, // start time
                           const struct timespec &tv2, // end time
                           struct timespec       &diff) const
{
#if defined(_MSC_VER) && defined(_my_pthread_h)
	diff.tv.i64 = diff.max_timeout_msec = 0;
#else
	if (tv2.tv_nsec < tv1.tv_nsec)
	{
		diff.tv_sec  = tv2.tv_sec  - tv1.tv_sec - 1;
		diff.tv_nsec = tv2.tv_nsec + 1000000000 - tv1.tv_nsec;
	}
	else
	{
		diff.tv_sec  = tv2.tv_sec  - tv1.tv_sec;
		diff.tv_nsec = tv2.tv_nsec - tv1.tv_nsec;
	}
#endif
}

//------------------------------------------------------------------------------
// Add 2 timespec values.
// tv1 (in)  - first timespec value
// tv2 (in)  - second timespec value
// sum (out) - sum of tv1 and tv2
//------------------------------------------------------------------------------
inline
void TimeSet::timespec_add(const struct timespec &tv1, // start time
	                       const struct timespec &tv2, // end time
	                       struct timespec       &sum) const
{
#if defined(_MSC_VER) && defined(_my_pthread_h)
	sum.tv.i64 = sum.max_timeout_msec = 0;
#else
	sum.tv_sec  = tv1.tv_sec  + tv2.tv_sec;
	sum.tv_nsec = tv1.tv_nsec + tv2.tv_nsec;
	if (sum.tv_nsec >= 1000000000)
	{
		sum.tv_sec  += 1;
		sum.tv_nsec -= 1000000000;
	}
#endif
}

};
#endif
