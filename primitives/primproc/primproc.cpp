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
 *   $Id: primproc.cpp 2147 2013-08-14 20:44:44Z bwilkinson $
 *
 *
 ***********************************************************************/
#include <unistd.h>
#include <string>
#include <iostream>
#ifdef QSIZE_DEBUG
#include <iomanip>
#include <fstream>
#endif
#include <csignal>
#include <sys/time.h>
#ifndef _MSC_VER
#include <sys/resource.h>
#include <tr1/unordered_set>
#else
#include <unordered_set>
#endif
#include <clocale>
#include <iterator>
#include <algorithm>
//#define NDEBUG
#include <cassert>
using namespace std;

#include <boost/thread.hpp>
#include <boost/regex.hpp>
#include <boost/tokenizer.hpp>
using namespace boost;

#include "configcpp.h"
using namespace config;

#include "messageids.h"
using namespace logging;

#include "primproc.h"
#include "primitiveserver.h"
#include "MonitorProcMem.h"
#include "pp_logger.h"
#include "umsocketselector.h"
using namespace primitiveprocessor;

#include "liboamcpp.h"
using namespace oam;

#include "utils_utf8.h"

#include "IDBPolicy.h"
using namespace idbdatafile;

#include "cgroupconfigurator.h"

namespace primitiveprocessor
{

extern uint32_t BPPCount;
extern uint32_t blocksReadAhead;
extern uint32_t defaultBufferSize;
extern uint32_t connectionsPerUM;
extern uint32_t highPriorityThreads;
extern uint32_t medPriorityThreads;
extern uint32_t lowPriorityThreads;
extern int  directIOFlag;
extern int  noVB;


DebugLevel gDebugLevel;
Logger* mlp;
string systemLang;
bool utf8 = false;

bool isDebug( const DebugLevel level )
{
        return level <= gDebugLevel;
}

}

namespace
{

int toInt(const string& val)
{
        if (val.length() == 0)
                return -1;
        return static_cast<int>(Config::fromText(val));
}

void setupSignalHandlers()
{
#ifndef _MSC_VER
		signal(SIGHUP, SIG_IGN);

        struct sigaction ign;

        memset(&ign, 0, sizeof(ign));
        ign.sa_handler = SIG_IGN;
        sigaction(SIGPIPE, &ign, 0);

        memset(&ign, 0, sizeof(ign));
        ign.sa_handler = SIG_IGN;
        sigaction(SIGUSR1, &ign, 0);

        memset(&ign, 0, sizeof(ign));
        ign.sa_handler = SIG_IGN;
        sigaction(SIGUSR2, &ign, 0);

	sigset_t sigset;
	sigemptyset(&sigset);
	sigaddset(&sigset, SIGPIPE);
	sigaddset(&sigset, SIGUSR1);
	sigaddset(&sigset, SIGUSR2);
	sigprocmask(SIG_BLOCK, &sigset, 0);

#endif
}

void setupCwd(Config* cf)
{
        string workdir = cf->getConfig("SystemConfig", "WorkingDir");
        if (workdir.length() == 0)
                workdir = ".";
        (void)chdir(workdir.c_str());
        if (access(".", W_OK) != 0)
                (void)chdir("/tmp");
}

int setupResources()
{
#ifndef _MSC_VER
        struct rlimit rlim;

        if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
                return -1;
        }
        rlim.rlim_cur = rlim.rlim_max = 65536;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
                return -2;
        }

        if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
                return -3;
        }

        if (rlim.rlim_cur != 65536) {
                return -4;
        }
#endif
        return 0;
}

#ifdef QSIZE_DEBUG
class QszMonThd
{
public:
	QszMonThd(PrimitiveServer* psp, ofstream* qszlog) : fPsp(psp), fQszLog(qszlog)
	{
	}

	~QszMonThd()
	{
	}

	void operator()()
	{
		for (;;)
		{
			uint32_t qd = fPsp->getProcessorThreadPool()->getWaiting();
			if (fQszLog)
			{
				// Get a timestamp for output.
				struct tm tm;
				struct timeval tv;

				gettimeofday(&tv, 0);
				localtime_r(&tv.tv_sec, &tm);

				ostringstream oss;
				oss << setfill('0')
					<< setw(2) << tm.tm_hour << ':'
					<< setw(2) << tm.tm_min << ':'
					<< setw(2) << tm.tm_sec
					<< '.'
					<< setw(4) << tv.tv_usec/100
					;

				*fQszLog << oss.str() << ' ' << qd << endl;
			}
			struct timespec req = { 0, 1000 * 100 }; //100 usec
			nanosleep(&req, 0);
		}
	}
private:
	//defaults okay
	//QszMonThd(const QszMonThd& rhs);
	//QszMonThd& operator=(const QszMonThd& rhs);
	const PrimitiveServer* fPsp;
	ofstream* fQszLog;
};
#endif

#ifndef _MSC_VER
#define DUMP_CACHE_CONTENTS
#endif
#ifdef DUMP_CACHE_CONTENTS
void* waitForSIGUSR1(void* p)
{
#if defined(__LP64__) || defined(_MSC_VER)
	ptrdiff_t tmp = reinterpret_cast<ptrdiff_t>(p);
	int cacheCount = static_cast<int>(tmp);
#else
	int cacheCount = reinterpret_cast<int>(p);
#endif
#ifndef _MSC_VER
	sigset_t oset;
	int rec_sig;
	int32_t rpt_state=0;
	sigfillset(&oset);
	sigdelset(&oset, SIGUSR1);
	sigdelset(&oset, SIGUSR2);
	pthread_sigmask(SIG_SETMASK, &oset, 0);

	sigemptyset(&oset);
	sigaddset(&oset, SIGUSR1);
	sigaddset(&oset, SIGUSR2);
#endif
	for (;;)
	{
		sigwait(&oset, &rec_sig);
		if (rec_sig == SIGUSR1)
		{
#ifdef _MSC_VER
			ofstream out("C:/Calpont/log/trace/pplru.dat");
#else
			ofstream out("/var/log/Calpont/trace/pplru.dat");
#endif
			for (int i = 0; i < cacheCount; i++)
			{
				BRPp[i]->formatLRUList(out);
				out << "###" << endl;
			}
		} else
		if (rec_sig == SIGUSR2)
		{
			// is reporting currently on?
			rpt_state = BRPp[0]->ReportingFrequency();
			if (rpt_state>0)
				rpt_state=0; // turn reporting off
			else
				rpt_state=1; // fbm will set to the value from config file

			for (int i = 0; i < cacheCount; i++)
				BRPp[i]->setReportingFrequency(rpt_state);
			cout << "@@@" << endl;
		}
	}
	return 0;
}
#endif

}

int main(int argc, char* argv[])
{
	// get and set locale language
	systemLang = funcexp::utf8::idb_setlocale();
    if ( systemLang != "en_US.UTF-8" &&
        systemLang.find("UTF") != string::npos )
        utf8 = true;

	Config* cf = Config::makeConfig();

	setupSignalHandlers();

	setupCwd(cf);

	mlp = new primitiveprocessor::Logger();

	int rc;
	rc = setupResources();
	if (rc) {
		Message::Args args;
		args.add(rc);
		//mlp->logMessage(logging::M0016, args);
	}

	int serverThreads = 1;
	int serverQueueSize = 10;
	int processorWeight = 8*1024;
	int processorQueueSize = 10*1024;
	int BRPBlocksPct = 70;
	uint32_t BRPBlocks = 1887437;
	int BRPThreads = 16;
	int cacheCount = 1;
	int maxBlocksPerRead = 256;  // 1MB
	bool rotatingDestination = false;
	uint32_t deleteBlocks = 128;
	bool PTTrace = false;
	int temp;
	string strTemp;
	int priority = -1;
	const string primitiveServers("PrimitiveServers");
	const string jobListStr("JobList");
	const string dbbc("DBBC");
	const string ExtentMapStr("ExtentMap");
	uint64_t extentRows = 8*1024*1024;
	uint64_t MaxExtentSize = 0;
	double prefetchThreshold;
	uint64_t PMSmallSide = 67108864;
	BPPCount = 16;
	int numCores = -1;
	int configNumCores = -1;
	uint32_t highPriorityPercentage, medPriorityPercentage, lowPriorityPercentage;
	utils::CGroupConfigurator cg;

	gDebugLevel = primitiveprocessor::NONE;

	temp = toInt(cf->getConfig(primitiveServers, "ServerThreads"));
	if (temp > 0)
		serverThreads = temp;

	temp = toInt(cf->getConfig(primitiveServers, "ServerQueueSize"));
	if (temp > 0)
		serverQueueSize = temp;

	temp = toInt(cf->getConfig(primitiveServers, "ProcessorThreshold"));
	if (temp > 0)
		processorWeight = temp;

	temp = toInt(cf->getConfig(primitiveServers, "ProcessorQueueSize"));
	if (temp > 0)
		processorQueueSize = temp;

	temp = toInt(cf->getConfig(primitiveServers, "DebugLevel"));
	if (temp > 0)
		gDebugLevel = (DebugLevel)temp;

	highPriorityPercentage = 0;
	temp = toInt(cf->getConfig(primitiveServers, "HighPriorityPercentage"));
	if (temp >= 0)
		highPriorityPercentage = temp;

	medPriorityPercentage = 0;
	temp = toInt(cf->getConfig(primitiveServers, "MediumPriorityPercentage"));
	if (temp >= 0)
		medPriorityPercentage = temp;

	lowPriorityPercentage = 0;
	temp = toInt(cf->getConfig(primitiveServers, "LowPriorityPercentage"));
	if (temp >= 0)
		lowPriorityPercentage = temp;

	temp = toInt(cf->getConfig(ExtentMapStr, "ExtentRows"));
	if (temp > 0)
		extentRows = temp;

	temp = toInt(cf->getConfig(primitiveServers, "ConnectionsPerPrimProc"));
	if (temp > 0)
		connectionsPerUM = temp;
	else
		connectionsPerUM = 1;

	// set to smallest extent size
	// do not allow to read beyond the end of an extent
	const int MaxReadAheadSz = (extentRows)/BLOCK_SIZE;
	//defaultBufferSize = 512 * 1024; // @bug 2627 - changed default dict buffer from 256K to 512K, allows for cols w/ length of 61.
	defaultBufferSize = 100*1024;  // 1/17/12 - made the dict buffer dynamic, max size for a numeric col is 80k + ovrhd


	// This parm controls whether we rotate through the output sockets
	// when deciding where to send response messages, or whether to simply
	// send the response to the socket of origin.  Should normally be set
	// to 'y', for install types 1 and 3.
	string strVal = cf->getConfig(primitiveServers, "RotatingDestination");
	//XXX: Permanently disable for now...
	strVal = "N";
	if ((strVal == "y") || (strVal == "Y")) {
		rotatingDestination = true;

		// Disable destination rotation if UM and PM are running on same
		// server, because we could accidentally end up sending DMLProc
		// responses to ExeMgr and vice versa, if we rotated socket dest.
		temp = toInt(cf->getConfig("Installation", "ServerTypeInstall"));
		if ((temp == oam::INSTALL_COMBINE_DM_UM_PM) ||
			(temp == oam::INSTALL_COMBINE_PM_UM))
			rotatingDestination = false;
	}

	temp = toInt(cf->getConfig(dbbc, "NumBlocksPct"));
	if (temp > 0)
		BRPBlocksPct = temp;

#ifdef _MSC_VER
	MEMORYSTATUSEX memStat;
	memStat.dwLength = sizeof(memStat);
	if (GlobalMemoryStatusEx(&memStat) == 0)
		//FIXME: Assume 2GB?
		BRPBlocks = 2621 * BRPBlocksPct;
	else
	{
#ifndef _WIN64
		memStat.ullTotalPhys = std::min(memStat.ullTotalVirtual, memStat.ullTotalPhys);
#endif
		//We now have the total phys mem in bytes
		BRPBlocks = memStat.ullTotalPhys / (8 * 1024) / 100 * BRPBlocksPct;
	}
#else
	// _SC_PHYS_PAGES is in 4KB units. Dividing by 200 converts to 8KB and gets ready to work in pct
	// _SC_PHYS_PAGES should always be >> 200 so we shouldn't see a total loss of precision
	//BRPBlocks = sysconf(_SC_PHYS_PAGES) / 200 * BRPBlocksPct;
	BRPBlocks = ((BRPBlocksPct/100.0) * (double) cg.getTotalMemory()) / 8192;
#endif
#if 0
	temp = toInt(cf->getConfig(dbbc, "NumThreads"));
	if (temp > 0)
		BRPThreads = temp;
#endif
	temp = toInt(cf->getConfig(dbbc, "NumCaches"));
	if (temp > 0)
		cacheCount = temp;

	temp = toInt(cf->getConfig(dbbc, "NumDeleteBlocks"));
	if (temp > 0)
		deleteBlocks = temp;

	if ((uint32_t)(.01 * BRPBlocks) < deleteBlocks)
		deleteBlocks = (uint32_t)(.01 * BRPBlocks);

	temp = toInt(cf->getConfig(primitiveServers, "ColScanBufferSizeBlocks"));
	if (temp > (int) MaxReadAheadSz || temp < 1)
		maxBlocksPerRead = MaxReadAheadSz;
	else if (temp > 0)
		maxBlocksPerRead = temp;

	temp = toInt(cf->getConfig(primitiveServers, "ColScanReadAheadBlocks"));
	if (temp > (int) MaxReadAheadSz || temp < 0)
		blocksReadAhead = MaxReadAheadSz;
	else if (temp > 0)
	{
		//make sure we've got an integral factor of extent size
		for (; (MaxExtentSize%temp)!=0; ++temp);
			blocksReadAhead=temp;
	}

	temp = toInt(cf->getConfig(primitiveServers, "PTTrace"));
	if (temp > 0)
		PTTrace = true;

	temp = toInt(cf->getConfig(primitiveServers, "PrefetchThreshold"));
	if (temp < 0 || temp > 100)
		prefetchThreshold = 0;
	else
		prefetchThreshold = temp/100.0;

	int maxPct = 0; //disable by default
	temp = toInt(cf->getConfig(primitiveServers, "MaxPct"));
	if (temp >= 0)
		maxPct = temp;

	// @bug4507, configurable pm aggregation AggregationMemoryCheck
    // We could use this same mechanism for other growing buffers.
	int aggPct = 95;
	temp = toInt(cf->getConfig("SystemConfig", "MemoryCheckPercent"));
	if (temp >= 0)
		aggPct = temp;

	//...Start the thread to monitor our memory usage
	new boost::thread(utils::MonitorProcMem(maxPct, aggPct, 28));

	// config file priority is 40..1 (highest..lowest)
	string sPriority = cf->getConfig(primitiveServers, "Priority");
	if (sPriority.length() > 0)
		temp = toInt(sPriority);
	else
		temp = 21;

	// convert config file value to setpriority(2) value (-20..19, -1 is the default)
	if (temp > 0)
		priority = 20 - temp;
	else if (temp < 0)
		priority = 19;

	if (priority < -20) priority = -20;
#ifdef _MSC_VER
	//FIXME:
#else
	setpriority(PRIO_PROCESS, 0, priority);
#endif
	//..Instantiate UmSocketSelector singleton.  Disable rotating destination
	//..selection if no UM IP addresses are in the Calpo67108864LLnt.xml file.
	UmSocketSelector* pUmSocketSelector = UmSocketSelector::instance();
	if (rotatingDestination)
	{
		if (pUmSocketSelector->ipAddressCount() < 1)
			rotatingDestination = false;
	}

	//See if we want to override the calculated #cores
	temp = toInt(cf->getConfig(primitiveServers, "NumCores"));
	if (temp > 0)
		configNumCores = temp;

	if (configNumCores <= 0)
	{
		//count the actual #cores

		numCores = cg.getNumCores();
		if (numCores == 0)
			numCores = 8;
	}
	else
		numCores = configNumCores;

	//based on the #cores, calculate some thread parms
	if (numCores > 0)
	{
		BRPThreads = 2 * numCores;
		//there doesn't seem much benefit to having more than this, and sometimes it causes problems.
		//DBBC.NumThreads can override this cap
		BRPThreads = std::min(BRPThreads, 32);
	}

	// the default is ~10% low, 30% medium, 60% high, (where 2*cores = 100%)
	if (highPriorityPercentage == 0 && medPriorityPercentage == 0 &&
	  lowPriorityPercentage == 0) {
		lowPriorityThreads = max(1, (2*numCores)/10);
		medPriorityThreads = max(1, (2*numCores)/3);
		highPriorityThreads = (2 * numCores) - lowPriorityThreads - medPriorityThreads;
	}
	else {
		uint32_t totalThreads = (uint32_t) ((lowPriorityPercentage + medPriorityPercentage +
		  highPriorityPercentage) / 100.0 * (2*numCores));

		if (totalThreads == 0)
			totalThreads = 1;

		lowPriorityThreads = (uint32_t) (lowPriorityPercentage/100.0 * (2*numCores));
		medPriorityThreads = (uint32_t) (medPriorityPercentage/100.0 * (2*numCores));
		highPriorityThreads = totalThreads - lowPriorityThreads - medPriorityThreads;
	}

	BPPCount = highPriorityThreads + medPriorityThreads + lowPriorityThreads;

	// let the user override if they want
	temp = toInt(cf->getConfig(primitiveServers, "BPPCount"));
	if (temp > 0 && temp < (int) BPPCount)
		BPPCount = temp;

	temp = toInt(cf->getConfig(dbbc, "NumThreads"));
	if (temp > 0)
		BRPThreads = temp;

#ifndef _MSC_VER
	// @bug4598, switch for O_DIRECT to support gluster fs.
	// directIOFlag == O_DIRECT, by default
	strVal = cf->getConfig(primitiveServers, "DirectIO");
	if ((strVal == "n") || (strVal == "N"))
		directIOFlag = 0;
#endif

	IDBPolicy::configIDBPolicy();

	// no versionbuffer if using HDFS for performance reason
	if (IDBPolicy::useHdfs())
		noVB = 1;

	cout << "Starting PrimitiveServer: st = " << serverThreads << ", sq = " << serverQueueSize <<
		", pw = " << processorWeight << ", pq = " << processorQueueSize <<
		", nb = " << BRPBlocks << ", nt = " << BRPThreads << ", nc = " << cacheCount <<
		", ra = " << blocksReadAhead <<  ", db = " << deleteBlocks << ", mb = " << maxBlocksPerRead <<
		", rd = " << rotatingDestination << ", tr = " << PTTrace <<
		", ss = " << PMSmallSide << ", bp = " << BPPCount << endl;

	PrimitiveServer server(serverThreads, serverQueueSize, processorWeight, processorQueueSize,
		rotatingDestination, BRPBlocks, BRPThreads, cacheCount, maxBlocksPerRead, blocksReadAhead,
		deleteBlocks, PTTrace, prefetchThreshold, PMSmallSide);

#ifdef QSIZE_DEBUG
	thread* qszMonThd;
	if (gDebugLevel >= STATS)
	{
#ifdef _MSC_VER
		ofstream* qszLog = new ofstream("C:/Calpont/log/trace/ppqsz.dat");
#else
		ofstream* qszLog = new ofstream("/var/log/Calpont/trace/ppqsz.dat");
#endif
		if (!qszLog->good())
		{
			qszLog->close();
			delete qszLog;
			qszLog = 0;
		}
		qszMonThd = new thread(QszMonThd(&server, qszLog));
	}
#endif

#ifdef DUMP_CACHE_CONTENTS
	{
		//Need to use pthreads API here...
		pthread_t thd1;
		pthread_attr_t attr1;
		pthread_attr_init(&attr1);
		pthread_attr_setdetachstate(&attr1, PTHREAD_CREATE_DETACHED);
		pthread_create(&thd1, &attr1, waitForSIGUSR1, reinterpret_cast<void*>(cacheCount));
	}
#endif

	server.start();

	cerr << "server.start() exited!" << endl;

	return 1;
}
// vim:ts=4 sw=4:
