/* Copyright (C) 2013 Calpont Corp.

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

/*
* $Id: stats.cpp 2035 2013-01-21 14:12:19Z rdempsey $
*/

#include <ctime>
#include <sys/time.h>
#ifndef _MSC_VER
#include <pthread.h>
#else
typedef int pthread_t;
#endif
#include <iomanip>
#include <map>
//#define NDEBUG
#include <cassert>
#include <csignal>
#include <fstream>
using namespace std;

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
using namespace boost;

#include "stats.h"
#include "messagelog.h"
#include "exceptclasses.h"

using namespace BRM;

namespace
{

void pause_(unsigned delay)
{
        struct timespec req;
        struct timespec rem;

        req.tv_sec = delay;
        req.tv_nsec = 0;

        rem.tv_sec = 0;
        rem.tv_nsec = 0;
#ifdef _MSC_VER
		Sleep(req.tv_sec * 1000);
#else
again:
	if (nanosleep(&req, &rem) != 0)
		if (rem.tv_sec > 0 || rem.tv_nsec > 0) {
			req = rem;
			goto again;
		}
#endif
}

const string timestr()
{
	// Get a timestamp for output.
	struct tm tm;
	struct timeval tv;

	gettimeofday(&tv, 0);
	localtime_r(reinterpret_cast<time_t*>(&tv.tv_sec), &tm);

	ostringstream oss;
	oss << setfill('0')
		<< setw(2) << tm.tm_hour << ':' 
		<< setw(2) << tm.tm_min << ':'  
		<< setw(2) << tm.tm_sec << '.'
		<< setw(4) << tv.tv_usec/100;

	return oss.str();
}

class TraceFile
{
public:
	TraceFile(uint32_t sessionID, const char* name) 
	{
		if (sessionID > 0 )
		{
			const char* outName;
			if (name == 0)
				outName = "lbids";
			else
				outName = name;
			ostringstream oss;
#ifdef _MSC_VER
			oss << "C:/Calpont/log/trace/" << outName << '.' << sessionID;
#else
			oss << "/var/log/Calpont/trace/" << outName << '.' << sessionID;
#endif
			oFile.reset(new ofstream());
			oFile->open(oss.str().c_str(), ios_base::out | ios_base::ate | ios_base::app);
		}
	}

	~TraceFile()
	{
	}

	void close()
	{
		if (oFile)
		{
			oFile->close();
		}
	}

	void log(OID_t oid, uint64_t lbid, pthread_t thdid, char event='\0')
	{
		*oFile << oid << ' ' << timestr() << ' ' << lbid
			<< ' ' << thdid;
		if (event != '\0')
			*oFile << ' ' << event;
		*oFile << endl;
		oFile->flush();
	}

private:
	//Compiler defaults okay
	//TraceFile(const TraceFile& rhs);
	//TraceFile operator=(const TraceFile& rhs);
	shared_ptr<ofstream> oFile;

};

struct TraceFileInfo
{
	TraceFileInfo(uint32_t session=0, const char* name=0) : traceFile(session, name), lastTouched(0) { }
	~TraceFileInfo() { }

	void log(OID_t oid, uint64_t lbid, pthread_t thdid, char event='\0')
	{
		traceFile.log(oid, lbid, thdid, event);
		lastTouched = time(0);
	}

	void close() { traceFile.close(); }

	TraceFile traceFile;
	time_t lastTouched;

private:
	//Compiler defaults okay
	//TraceFileInfo(const TraceFileInfo& rhs);
	//TraceFileInfo operator=(const TraceFileInfo& rhs);
};

//map a session id to a trace file
typedef map<uint32_t, TraceFileInfo> TraceFileMap_t;

TraceFileMap_t traceFileMap;
//map mutex
mutex traceFileMapMutex;

class StatMon
{
public:
	StatMon()
	{
#ifndef _MSC_VER
	sigset_t sigset;
	sigemptyset(&sigset);
	sigaddset(&sigset, SIGPIPE);
	sigaddset(&sigset, SIGUSR1);
	sigaddset(&sigset, SIGUSR2);
	pthread_sigmask(SIG_BLOCK, &sigset, 0);
#endif
	}
	void operator()() const
	{
		//struct timespec ts = { 60 * 1, 0 };
		mutex::scoped_lock lk(traceFileMapMutex);
		TraceFileMap_t::iterator iter;
		TraceFileMap_t::iterator end;
		for (;;)
		{
			lk.unlock();
			time_t beforeSleep = time(0);
			//nanosleep(&ts, 0);
			pause_(60);
			lk.lock();
			iter = traceFileMap.begin();
			end = traceFileMap.end();
			while (iter != end)
			{
				if (iter->second.lastTouched < beforeSleep)
				{
					//remove this session trace file
					iter->second.close();
					traceFileMap.erase(iter++);
				}
				else
					++iter;
			}
		}
	}
private:
	//Compiler defaults okay
	//StatMon(const StatMon& rhs);
	//StatMon operator=(const StatMon& rhs);
};

}

namespace dbbc
{

Stats::Stats() :
	fMonitorp(0)
{

	fMonitorp = new boost::thread(StatMon());
}

Stats::Stats(const char *name) :
	fMonitorp(0), fName(name)
{
	fMonitorp = new boost::thread(StatMon());
	//fName << name;
}

Stats::~Stats()
{
	delete fMonitorp;
}

void Stats::touchedLBID(uint64_t lbid, pthread_t thdid, uint32_t session)
{
	if (lbid < 0 || session == 0) return;

	mutex::scoped_lock lk(traceFileMapMutex);
	TraceFileMap_t::iterator iter = traceFileMap.find(session);
	if (iter == traceFileMap.end())
	{
		traceFileMap[session] = TraceFileInfo(session);
		iter = traceFileMap.find(session);
		idbassert(iter != traceFileMap.end());
	}
	iter->second.log(lbid2oid(lbid), lbid, thdid);
}

void Stats::markEvent(const uint64_t lbid, const pthread_t thdid, const uint32_t session, const char event)
{
	if (lbid < 0 || session == 0) return;

	mutex::scoped_lock lk(traceFileMapMutex);
	TraceFileMap_t::iterator iter = traceFileMap.find(session);
	if (iter == traceFileMap.end())
	{
		traceFileMap[session] = TraceFileInfo(session, fName);
		iter = traceFileMap.find(session);
		idbassert(iter != traceFileMap.end());
	}
	iter->second.log(lbid2oid(lbid), lbid, thdid, event);
}

}

