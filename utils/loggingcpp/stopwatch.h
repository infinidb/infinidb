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
// $Id$
//
// Timer class used to spit out times and percentages.  Temporarily lives in joblist.
// This is strictly a debugging utility class that accumulates times between .start() and
// .stop() calls for a particular string.  It can be useful in tdrivers.
//
// It outputs a report when finish() is called with
// the total times by string and their percentage of the total.  The total is tracked from
// the first time start() is called until finish() is called.  You should always match your
// start and stop calls up.
//
// This works fine as long as you don't use it with huge numbers of calls.  If you call start 
// and stop too many times the overhead of the StopWatch class will start factoring in to your
// results.
//
// How to use:
// 
// 	StopWatch timer;
// 	timer.start("Loop only");
// 	for(int i = 0; i < 6999075; i++)
// 	{
// 	}
// 	timer.stop("Loop only");
// 
// 	timer.start("Loop Plus");
// 	for(int i = 0; i < 100000; i++)
// 	{
// 		timer.start("Inside loop");
// 		timer.stop("Inside loop");
// 	}
// 	timer.stop("Loop Plus");
// 	timer.finish();
//
// Produces this:
// 
// Seconds  Percentage  Calls      Description
// 0.02865  9.377%      1          Loop only
// 0.27680  90.59%      1          Loop Plus
// 0.12138  39.72%      100000     Inside loop
// 
// 0.30553  100  %      1          Total
// 
// Note that you can have overlapping timers which will make your percentages add up to more than 100%.
#ifndef LOGGING_STOPWATCH_H
#define LOGGING_STOPWATCH_H
#include <iostream>
#include <fstream>
#include <list>
#include <sstream>
#include <time.h>
#include <sys/time.h>
#include <vector>
#include <stdexcept>
#include <map>
#include <unistd.h>

namespace logging
{

class StopWatch {
	public:
		void start(const std::string& message);
		bool stop(const std::string& message, const int limit);
		void stop(const std::string& message);
		void finish();
		
		bool isActive() 
		{
			return fOpenCalls > 0;
		}
		StopWatch() : fStarted(false), fId(-1), fOpenCalls(0), fOutputToFile(false), fLogFile("") {};
		StopWatch(int id) : fStarted(false), fId(id), fOpenCalls(0), fOutputToFile(false), fLogFile("") {};
		StopWatch(const std::string& fileName) : fStarted(false), fId(-1), fOpenCalls(0), fOutputToFile(true), fLogFile(fileName) {}
		struct ::timeval fTvLast;
		int getId() { return fId; }

	private:
		class ProcessStats 
		{
			public:
			
			std::string fProcess;
			struct timeval fTvProcessStarted;
			double fTotalSeconds;
			int64_t fStartCount;
			int64_t fStopCount;

			ProcessStats() : fProcess(""), fTotalSeconds(0.0), fStartCount(0), fStopCount(0) {};

			void processStart() 
			{
				gettimeofday(&fTvProcessStarted, 0);
				fStartCount++;
			}

			void processStop()
			{
				struct timeval tvStop;
				gettimeofday(&tvStop, 0);
				fStopCount++;
				fTotalSeconds += 
					(tvStop.tv_sec + (tvStop.tv_usec / 1000000.0)) -
					(fTvProcessStarted.tv_sec + (fTvProcessStarted.tv_usec / 1000000.0));

			}
		};

 		struct timeval fTvStart;
		std::vector <ProcessStats> fProcessStats;
		bool fStarted;
		int fId;
		int fOpenCalls;
		bool fOutputToFile;
		std::string fLogFile;
};

} // end of logging namespace

#endif //  STOPWATCH_H
