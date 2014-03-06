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
#include <iostream>
#include <fstream>
#include <list>
#include <sstream>
#include <time.h>
#include <sys/time.h>
#include <vector>
#include <stdexcept>
#include <sys/types.h>
#include <cstring>
#include <stdint.h>
#include "stopwatch.h"
using namespace std;

namespace logging
{

void StopWatch::stop(const string& message)
{
	stop(message, 1);
}

bool StopWatch::stop(const string& message, const int limit) {
	gettimeofday(&fTvLast, 0);
	fOpenCalls--;
	bool found = false;
	uint32_t idx = 0;
	for(uint32_t i = 0; i < fProcessStats.size(); i++) {
		if(fProcessStats[i].fProcess == message) {
			idx = i;
			found = true;
			break;
		}
	}
	if(!found) {
		//throw std::runtime_error("StopWatch::stop " + message + " called without calling start first.");
		std::cerr << "StopWatch receiving STOP for unknown event: " << message << std::endl;
		return false;
	}
	fProcessStats[idx].processStop();
	if(fProcessStats[idx].fStopCount >= limit)
		return true;
	return false;
}

void StopWatch::start(const string& message) {
	fOpenCalls++;
	gettimeofday(&fTvLast, 0);
	bool found = false;
	uint32_t idx = 0;
	ProcessStats processStats;
	if(!fStarted) {
		fStarted = true;
		gettimeofday(&fTvStart, 0);
	}
	for(uint32_t i = 0; i < fProcessStats.size(); i++) {
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

void StopWatch::finish() {
	ostringstream oss;
			
	oss << endl;
	oss << "Seconds      Percentage        Calls      Description" << endl;

	// total seconds elapsed
	double totalSeconds = 1.0;

	// Add a last entry into the vector for total.
	ProcessStats total;
	total.fProcess = "Total";
	if (fProcessStats.size() > 0)
	{
		// Calculate the total seconds elapsed.
		totalSeconds =
			(fTvLast.tv_sec + (fTvLast.tv_usec / 1000000.0)) -
			(fTvStart.tv_sec + (fTvStart.tv_usec / 1000000.0));			
		total.fTotalSeconds = totalSeconds;
		total.fStartCount   = 1;
    }
	else
	{
		total.fTotalSeconds = 0.0;
		total.fStartCount   = 0;
	}
	fProcessStats.push_back(total);

	for(uint32_t i = 0; i < fProcessStats.size(); i++) {

		if(i == (fProcessStats.size() - 1)) {
			oss << endl;
		}

		// Seconds.
		string seconds;
		ostringstream ossTemp;
		ossTemp << fProcessStats[i].fTotalSeconds;
		seconds = ossTemp.str();
		seconds.resize(11, ' ');
		oss << seconds << "  ";

		// Percentage.
		string percentage;
		ossTemp.str(""); // clear the stream.
		ossTemp << (fProcessStats[i].fTotalSeconds / totalSeconds) * 100.0;
		percentage = ossTemp.str();
		percentage.resize(11, ' ');
		oss << percentage << "%      ";

		// Times Initiated.
		ossTemp.str(""); // clear the stream.
		ossTemp << fProcessStats[i].fStartCount;
		string timesInitiated = ossTemp.str();
		timesInitiated.resize(10, ' ');
		oss << timesInitiated << " ";
				
		// Description.
		if(fId >= 0)
			oss << fId << ": " << fProcessStats[i].fProcess << endl;
		else	
			oss << fProcessStats[i].fProcess << endl;
	}
	if(fOutputToFile) {

		ofstream profLog;
		profLog.open(fLogFile.c_str(), std::ios::app);

		// Output the date and time.
	        time_t t = time(0);
	        char timeString[50];
	        ctime_r(&t, timeString);
        	timeString[ strlen(timeString)-1 ] = '\0';
		profLog << endl << timeString;

		// Output the stopwatch info.		
		profLog << oss.str();
	}
	else {
		cout << oss.str();
	}

	// Clear everything out.
	fStarted = false;
	fProcessStats.clear();
			
}

} // end of logging namespace
