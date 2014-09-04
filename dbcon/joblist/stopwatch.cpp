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
// $Id: stopwatch.cpp 8436 2012-04-04 18:18:21Z rdempsey $
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
// and stop too many times the overhead of the Stopwatch class will start factoring in to your
// results.
//
// How to use:
// 
// 	Stopwatch timer;
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

#include <iostream>
#include <fstream>
#include <list>
#include <sstream>
#include <time.h>
#include <sys/time.h>
#include <vector>
#include <stdexcept>

using namespace std;

class Stopwatch {
	public:
		void start(const string& message);
		void stop(const string& message);
		void finish() {
			
			// Calculate the total seconds elapsed.
			struct timeval tvStop;
			gettimeofday(&tvStop, 0);			
			double totalSeconds = 
				(tvStop.tv_sec + (tvStop.tv_usec / 1000000.0)) -
				(fTvStart.tv_sec + (fTvStart.tv_usec / 1000000.0));			

			profLog << endl;
			profLog << "Seconds  Percentage  Calls      Description" << endl;

			// Add a last entry into the vector for total.
			ProcessStats total;
			total.fTotalSeconds = totalSeconds;
			total.fProcess = "Total";
			total.fStartCount = 1;
			fProcessStats.push_back(total);

			for(uint i = 0; i < fProcessStats.size(); i++) {

				if(i == (fProcessStats.size() - 1)) {
					profLog << endl;
				}

				// Seconds.
				string seconds;
				ostringstream oss;
				oss << fProcessStats[i].fTotalSeconds;
				seconds = oss.str();
				seconds.resize(11, ' ');
				profLog << seconds << "  ";

				// Percentage.
				string percentage;
				ostringstream oss2;
				oss2 << (fProcessStats[i].fTotalSeconds / totalSeconds) * 100.0;
				percentage = oss2.str();
				percentage.resize(5, ' ');
				profLog << percentage << "%      ";

				// Times Initiated.
				ostringstream oss3;
				oss3 << fProcessStats[i].fStartCount;
				string timesInitiated = oss3.str();
				timesInitiated.resize(10, ' ');
				profLog << timesInitiated << " ";
				
				// Description.
				profLog << fProcessStats[i].fProcess << endl;
			}

			// Clear everything out.
			fStarted = false;
			fProcessStats.clear();
			
		}
		Stopwatch() : fStarted(false) {profLog.open("prof.log", std::ios::app);};

	private:
		class ProcessStats 
		{
			public:
			
			string fProcess;
			struct timeval fTvProcessStarted;
			double fTotalSeconds;
			long fStartCount;
			long fStopCount;

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
		vector <ProcessStats> fProcessStats;
		bool fStarted;
		ofstream profLog;
};

void Stopwatch::stop(const string& message) {
	bool found = false;
	uint idx = 0;
	for(uint i = 0; i < fProcessStats.size(); i++) {
		if(fProcessStats[i].fProcess == message) {
			idx = i;
			found = true;
			break;
		}
	}
	if(!found) {
		throw std::runtime_error("Stopwatch::stop " + message + " called without calling start first.");
	}
	fProcessStats[idx].processStop();
}

void Stopwatch::start(const string& message) {
	bool found = false;
	uint idx = 0;
	ProcessStats processStats;
	if(!fStarted) {
		fStarted = true;
		gettimeofday(&fTvStart, 0);
	}
	for(uint i = 0; i < fProcessStats.size(); i++) {
		if(fProcessStats[i].fProcess == message) {
			idx = i;
			found = true;
			break;
		}
	}
	if(!found) {
		fProcessStats.push_back(processStats);
		idx = fProcessStats.size() - 1;
	}
	fProcessStats[idx].fProcess = message;
	fProcessStats[idx].processStart();
}
