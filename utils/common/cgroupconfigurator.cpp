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

#include "cgroupconfigurator.h"
#include "configcpp.h"
#include "logger.h"
#include <fstream>
#include <boost/regex.hpp>
#ifdef _MSC_VER
#include "unistd.h"
#include "sysinfo.h"
#else
#include <sys/sysinfo.h>
#endif
using namespace boost;
using namespace std;


// minor space-savers
#define RETURN_NO_GROUP(err) { \
    if (!printedWarning) { \
        printedWarning = true; \
        ostringstream os; \
        os << "CGroup warning!  The group " << cGroupName << " does not exist."; \
        cerr << os.str() << endl; \
        log(logging::LOG_TYPE_WARNING, os.str()); \
    } \
    return err; \
}

#define RETURN_READ_ERROR(err) { \
    if (!printedWarning) { \
        printedWarning = true; \
        ostringstream os; \
        os << "CGroup warning!  Could not read the file " << filename << "."; \
        cerr << os.str() << endl; \
        log(logging::LOG_TYPE_WARNING, os.str()); \
    } \
    return err; \
}

namespace {

void log(logging::LOG_TYPE whichLogFile, const string &msg)
{
	logging::Logger logger(12);  //12 = configcpp
	logger.logMessage(whichLogFile, msg, logging::LoggingID(12));
}

}

namespace utils {

CGroupConfigurator::CGroupConfigurator()
{
    config = config::Config::makeConfig();

    cGroupName = config->getConfig("SystemConfig", "CGroup");

    if (cGroupName.empty())
        cGroupDefined = false;
    else
        cGroupDefined = true;
    totalMemory = 0;
    totalSwap = 0;
    printedWarning = false;
}

CGroupConfigurator::~CGroupConfigurator()
{
}

uint32_t CGroupConfigurator::getNumCoresFromCGroup()
{
    ostringstream filename;
    filename << "/sys/fs/cgroup/cpuset/" << cGroupName << "/cpus";

    ifstream in(filename.str().c_str());
    string cpusString;
    uint32_t cpus = 0;

    if (!in)
        RETURN_NO_GROUP(0);
    try {

        // Need to parse & count how many CPUs we have access to
        in >> cpusString;
    }
    catch (...) {
        RETURN_READ_ERROR(0);
    }

    // the file has comma-deliminted CPU ranges like "0-7,9,11-12".
    size_t first = 0, last;
    bool lastRange = false;
    while (!lastRange) {
        size_t dash;
        string oneRange;

        last = cpusString.find(',', first);
        if (last == string::npos) {
            lastRange = true;
            oneRange = cpusString.substr(first);
        }
        else
            oneRange = cpusString.substr(first, last - first - 1);

        if ((dash = oneRange.find('-')) == string::npos)  // single-cpu range
            cpus++;
        else {
            const char *data = oneRange.c_str();
            uint32_t firstCPU = strtol(data, NULL, 10);
            uint32_t lastCPU = strtol(&data[dash+1], NULL, 10);
            cpus += lastCPU - firstCPU + 1;
        }
        first = last + 1;
    }
    //cout << "found " << cpus << " CPUS in the string " << cpusString << endl;
    return cpus;
}

uint32_t CGroupConfigurator::getNumCoresFromProc()
{
#ifdef _MSC_VER
	SYSTEM_INFO siSysInfo;
	GetSystemInfo(&siSysInfo);
	return siSysInfo.dwNumberOfProcessors;
#else
	ifstream cpuinfo("/proc/cpuinfo");

	if (!cpuinfo.good())
		return 0;

	unsigned nc = 0;

	regex re("Processor\\s*:\\s*[0-9]+", regex::normal|regex::icase);

	string line;

	getline(cpuinfo, line);

	unsigned i = 0;
	while (i < 10000 && cpuinfo.good() && !cpuinfo.eof())
	{
		if (regex_match(line, re))
			nc++;

		getline(cpuinfo, line);

		++i;
	}

	return nc;
#endif
}



uint32_t CGroupConfigurator::getNumCores()
{
    /*
        Detect if InfiniDB is in a C-Group
            - get the group ID
        If not, get the number of cores from /proc
    */
    uint32_t ret;

    if (!cGroupDefined)
        ret = getNumCoresFromProc();
    else {
        ret = getNumCoresFromCGroup();
        if (ret == 0)
            ret = getNumCoresFromProc();
    }
    //cout << "There are " << ret << " cores available" << endl;
    return ret;
}

uint64_t CGroupConfigurator::getTotalMemory()
{
    uint64_t ret;

    if (totalMemory != 0)
        return totalMemory;

    if (!cGroupDefined)
        ret = getTotalMemoryFromProc();
    else {
        ret = getTotalMemoryFromCGroup();
        if (ret == 0)
            ret = getTotalMemoryFromProc();
    }
    //cout << "Total mem available is " << ret << endl;
    totalMemory = ret;
    return totalMemory;
}

uint64_t CGroupConfigurator::getTotalMemoryFromProc()
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

	//memTot is now in KB, convert to bytes
	memTot *= 1024;

	return memTot;
}

uint64_t CGroupConfigurator::getTotalMemoryFromCGroup()
{
    ifstream in;
    uint64_t ret;
    ostringstream os;
    string filename;

    os << "/sys/fs/cgroup/memory/" << cGroupName << "/memory.limit_in_bytes";
    filename = os.str();

    in.open(filename.c_str());
    if (!in)
        RETURN_NO_GROUP(0);

    try {
        in >> ret;
    }
    catch (...) {
        RETURN_READ_ERROR(0);
    }
    return ret;
}

uint64_t CGroupConfigurator::getFreeMemory()
{
    uint64_t ret;

    if (!cGroupDefined)
        ret = getFreeMemoryFromProc();
    else {
        uint64_t usage = getMemUsageFromCGroup();
        if (usage == 0)
            ret = getFreeMemoryFromProc();
        else
            ret = getTotalMemory() - usage;
    }
    //cout << "free memory = " << ret << endl;
    return ret;
}

uint64_t CGroupConfigurator::getMemUsageFromCGroup()
{
    uint64_t ret = 0;
    bool found = false;
    char oneline[80];

    if (memUsageFilename.empty()) {
        ostringstream filename;
        filename << "/sys/fs/cgroup/memory/" << cGroupName << "/memory.stat";
        memUsageFilename = filename.str();
    }

    ifstream in(memUsageFilename.c_str());
    string &filename = memUsageFilename;
    if (!in)
        RETURN_NO_GROUP(0);

    try {
        while (in && !found) {
            in.getline(oneline, 80);

            if (strncmp(oneline, "rss", 2) == 0) {
                ret = atoll(&oneline[3]);
                found = true;
            }
        }
    }
    catch(...) {
        RETURN_READ_ERROR(0);
    }

    return ret;
}

uint64_t CGroupConfigurator::getFreeMemoryFromProc()
{
	uint64_t memFree  = 0;
	uint64_t buffers  = 0;
	uint64_t cached   = 0;
    uint64_t memTotal = 0;

#if defined(_MSC_VER)
	MEMORYSTATUSEX memStat;
	memStat.dwLength = sizeof(memStat);
	if (GlobalMemoryStatusEx(&memStat))
	{
		memFree  = memStat.ullAvailPhys;
#ifndef _WIN64
        uint64_t tmp = getTotalMemoryFromProc();
		if (memFree > tmp)
			memFree = tmp;
#endif
	}
#elif defined(__FreeBSD__)
	// FreeBSD is not supported, no optimization.
	memFree = 0;
#else
	ifstream in("/proc/meminfo");
	string x;

	in >> x;         // MemTotal:
	in >> memTotal;
	in >> x;         // kB

	in >> x;         // MemFree:
	in >> memFree;
	in >> x;         // kB

	in >> x;         // Buffers:
	in >> buffers;
	in >> x;         // kB

	in >> x;         // Cached:
	in >> cached;
#endif

	// amount available for application
	memFree = memFree + buffers + cached;
	memFree *= 1024;
    return memFree;
}

uint64_t CGroupConfigurator::getTotalSwapSpace()
{
    int64_t ret;

    if (totalSwap != 0)
        return totalSwap;

    if (!cGroupDefined)
        ret = getTotalSwapFromSysinfo();
    else {
        ret = getTotalMemAndSwapFromCGroup();
        // if no limit is set in the cgroup, the file contains maxint64. Use sysinfo in that case.
        if (ret == -1 || ret == numeric_limits<int64_t>::max())
            ret = getTotalSwapFromSysinfo();
        else
            ret -= getTotalMemory();
    }
    //cout << "total swap=" << ret << endl;
    totalSwap = ret;
    return ret;
}

uint64_t CGroupConfigurator::getTotalSwapFromSysinfo()
{
    struct sysinfo si;

    sysinfo(&si);
    return si.totalswap;
}

int64_t CGroupConfigurator::getTotalMemAndSwapFromCGroup()
{
    int64_t ret;
    ifstream in;
    string filename;
    ostringstream os;

    os << "/sys/fs/cgroup/memory/" << cGroupName << "/memory.memsw.limit_in_bytes";
    filename = os.str();
    in.open(filename.c_str());
    if (!in)
        RETURN_NO_GROUP(-1);

    try {
        in >> ret;
    }
    catch(...) {
        RETURN_READ_ERROR(-1);
    }
    return ret;
}

uint64_t CGroupConfigurator::getSwapInUse()
{
    int64_t ret;

    if (!cGroupDefined)
        ret = getSwapInUseFromSysinfo();
    else {
        ret = getSwapInUseFromCGroup();
        if (ret == -1)
            ret = getSwapInUseFromSysinfo();
    }
    //cout << "current swap in use=" << ret << endl;
    return ret;
}

int64_t CGroupConfigurator::getSwapInUseFromCGroup()
{
    int64_t ret = -1;
    ifstream in;
    bool found = false;
    char oneline[80];

    if (usedSwapFilename.empty()) {
        ostringstream os;
        os << "/sys/fs/cgroup/memory/" << cGroupName << "/memory.stat";
        usedSwapFilename = os.str();
    }
    string &filename = usedSwapFilename;
    in.open(filename.c_str());
    if (!in)
        RETURN_NO_GROUP(-1);

    try {
        while (in && !found) {
            in.getline(oneline, 80);

            if (strncmp(oneline, "swap", 4) == 0) {
                ret = atoll(&oneline[5]);
                found = true;
            }
        }
    }
    catch(...) {
        RETURN_READ_ERROR(-1);
    }
    return ret;
}

uint64_t CGroupConfigurator::getSwapInUseFromSysinfo()
{
    struct sysinfo si;

    sysinfo(&si);
    return si.totalswap - si.freeswap;
}


}
