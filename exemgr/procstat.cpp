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

// $Id: procstat.cpp 940 2013-01-21 14:11:31Z rdempsey $

#ifdef _MSC_VER
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <process.h>
#include <psapi.h>
#endif
#include <sys/types.h>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <stdint.h>
using namespace std;

#include "procstat.h"

namespace procstat
{

size_t ProcStat::rss() const
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
	rss *= fPagesize;
#endif
	rss /= (1024ULL * 1024ULL);
	return static_cast<size_t>(rss);
}


/*static*/
size_t ProcStat::memTotal()
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

}

