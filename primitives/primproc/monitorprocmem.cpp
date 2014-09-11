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
 * $Id: monitorprocmem.cpp 1615 2011-03-04 19:22:52Z rdempsey $
 *
 *****************************************************************************/

#ifdef _MSC_VER
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <process.h>
#include <psapi.h>
#endif
#include <sys/types.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <cstdlib>
using namespace std;

#include "loggingid.h"
#include "messageids.h"
#include "pp_logger.h"

#include "monitorprocmem.h"

namespace primitiveprocessor
{

//------------------------------------------------------------------------------
// Thread entry point function, that drives a thread to periodically (every
// 15 seconds by default) monitor the memory usage for the process we are
// running in. If we exceed the memory percentage specified to the constructor,
// this thread will terminate the process by calling exit().
//------------------------------------------------------------------------------
void MonitorProcMem::operator()() const
{
    const unsigned logRssTooBig = logging::M0045;
    const size_t mt = memTotal();

    while (1)
    {
        size_t rssMb = rss();
        size_t pct = rssMb * 100 / mt;
        if (pct > fMaxPct)
        {
            cerr << "PrimProc: Too much memory allocated!" << endl;
            logging::Message::Args args;
            
            fMsgLog->logMessage ( logRssTooBig,
                                 logging::Message::Args(),
                                 true );
            exit(1);
        }
        pause_();
    }
}

//------------------------------------------------------------------------------
// Returns the maximum memory available on the current host this process is
// running on.
//------------------------------------------------------------------------------
size_t MonitorProcMem::memTotal() const
{
	size_t memTot;

#if defined(_MSC_VER)
	MEMORYSTATUSEX memStat;
	memStat.dwLength = sizeof(memStat);
	if (GlobalMemoryStatusEx(&memStat) == 0)
		//FIXME: Assume 2GB?
		memTot = 2 * 1024 * 1024;
	else
	{
#ifndef _WIN64
		memStat.ullTotalPhys = std::min(memStat.ullTotalVirtual, memStat.ullTotalPhys);
#endif
		//We now have the total phys mem in bytes
		memTot = memStat.ullTotalPhys / 1024;
	}
#elif defined(__FreeBSD__)
	string cmd("sysctl -a | awk '/realmem/ {print int(($2+1023)/1024);}'");
	FILE* cmdPipe;
	char input[80];
	cmdPipe = popen(cmd.c_str(), "r");
	input[0] = '\0';
	fgets(input, 80, cmdPipe);
	input[79] = '\0';
	pclose(cmdPipe);
	memTot = atoi(input);
#else
	ifstream in("/proc/meminfo");
	string x;

	in >> x;
	in >> memTot;
#endif

	//memTot is now in KB, convert to MB
	memTot /= 1024;

	return memTot;
}

//------------------------------------------------------------------------------
// Returns the RSS memory usage for the current process by reading the pid's
// statm file.
//------------------------------------------------------------------------------
size_t MonitorProcMem::rss() const
{
	uint64_t rss;

#if defined(_MSC_VER)
    HANDLE hProcess;
    PROCESS_MEMORY_COUNTERS pmc;
    hProcess = OpenProcess(  PROCESS_QUERY_INFORMATION |
                                    PROCESS_VM_READ,
                                    FALSE, fPid );
    if (NULL == hProcess)
        return 0;

    if ( GetProcessMemoryInfo( hProcess, &pmc, sizeof(pmc)) )
		rss = pmc.WorkingSetSize;
	else
		rss = 0;

    CloseHandle( hProcess );
#elif defined(__FreeBSD__)
	ostringstream cmd;
	cmd << "ps -a -o rss -p " << getpid() << " | tail +2";
	FILE* cmdPipe;
	char input[80];
	cmdPipe = popen(cmd.str().c_str(), "r");
	input[0] = '\0';
	fgets(input, 80, cmdPipe);
	input[79] = '\0';
	pclose(cmdPipe);
	rss = atoi(input) * 1024LL;
#else
	ostringstream pstat;
	pstat << "/proc/" << fPid << "/statm";
	ifstream in(pstat.str().c_str());
	size_t x;

	in >> x;
	in >> rss;

	//rss is now in pages, convert to MB
	rss *= fPageSize;
#endif
	rss /= (1024ULL * 1024ULL);

	return static_cast<size_t>(rss);
}

//------------------------------------------------------------------------------
// Stays in "sleep" state till fSleepSec seconds have elapsed.
//------------------------------------------------------------------------------
void MonitorProcMem::pause_( ) const
{
    struct timespec req;
    struct timespec rem;

    req.tv_sec  = fSleepSec;
    req.tv_nsec = 0;

    rem.tv_sec  = 0;
    rem.tv_nsec = 0;

    while (1)
    {
#ifdef _MSC_VER
		Sleep(req.tv_sec * 1000);
#else
        if (nanosleep(&req, &rem) != 0)
        {
            if (rem.tv_sec > 0 || rem.tv_nsec > 0)
            {
                req = rem;
                continue;
            }
        }
#endif
        break;
    }
}

} // end of namespace
