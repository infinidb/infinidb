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
// $Id: bppseeder.cpp 1975 2012-10-25 22:16:45Z pleblanc $
// C++ Implementation: bppseeder
//
// Description: 
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <unistd.h>
#include <sstream>
#ifndef _MSC_VER
#include <pthread.h>
#include <boost/date_time/posix_time/posix_time.hpp>
#else
typedef int pthread_t;
#endif
#include <boost/thread.hpp>

#include "bppseeder.h"
#include "primitiveserver.h"
#include "pp_logger.h"
#include "errorcodes.h"
#include "calpontsystemcatalog.h"
#include "blockcacheclient.h"

using namespace messageqcpp;
using namespace std;



namespace primitiveprocessor
{

struct PTLogs{
	PTLogs() {};
	PTLogs(const int t, const char * fname):thdId(t) {logFD.open(fname, ios_base::app | ios_base::ate);}
	~PTLogs() {logFD.close();}

	int thdId;
	ofstream logFD;
};

typedef PTLogs PTLogs_t;
typedef boost::shared_ptr<PTLogs_t> SPPTLogs_t;
typedef std::tr1::unordered_map<pthread_t, SPPTLogs_t> PTLogsMap_t;

PTLogsMap_t gFDList;
SPPTLogs_t gLogFD;
boost::mutex gFDMutex; //pthread_mutex_t gFDMutex=PTHREAD_MUTEX_INITIALIZER;
int gThdCnt=0;

extern dbbc::BlockRequestProcessor **BRPp;
extern int fCacheCount;


void timespec_sub(const struct timespec &tv1,
				const struct timespec &tv2,
				double &tm) 
{
	tm = (double)(tv2.tv_sec - tv1.tv_sec) + 1.e-9*(tv2.tv_nsec - tv1.tv_nsec);
}

BPPSeeder::BPPSeeder(const SBS &b,
					const SP_UM_MUTEX& w,
					const SP_UM_IOSOCK& s,
					const int pmThreads,
					const bool trace) :
					bs(b), writelock(w), sock(s), fPMThreads(pmThreads), fTrace(trace),
					failCount(0),
					firstRun(true)
{
	uint8_t *buf = b->buf();
	uint pos = sizeof(ISMPacketHeader);

	sessionID = *((uint32_t *) &buf[pos]); pos += 4;
	stepID = *((uint32_t *) &buf[pos]); pos += 4;
	uniqueID = *((uint32_t *) &buf[pos]);
}

BPPSeeder::BPPSeeder(const BPPSeeder &b)
					: bs(b.bs), writelock(b.writelock), sock(b.sock),
					fPMThreads(b.fPMThreads), fTrace(b.fTrace), uniqueID(b.uniqueID),
					sessionID(b.sessionID), stepID(b.stepID), failCount(b.failCount), bpp(b.bpp),
					firstRun(b.firstRun)
{
}

BPPSeeder::~BPPSeeder()
{
}

int BPPSeeder::operator()()
{
	uint32_t pos;
	const uint8_t *buf = bs->buf();
	BPPMap::iterator it;
	ostringstream logData;
	struct timespec tm;
	struct timespec tm2;
	double tm3=0;
	bool ptLock=false;
	bool gotBPP = false;
	const uint maxFailCount = 20000;   // current 1ms pause in W-threadpool, this is 20s
	PTLogs_t* logFD=NULL;
	int ret = 0;
	pthread_t tid=0;
	boost::mutex::scoped_lock scoped(bppLock, boost::defer_lock_t());

	try {
	if (firstRun) {
		pos = sizeof(ISMPacketHeader) - 2;
		uint16_t status = *((uint16_t *) &buf[pos]); pos += 2;

		sessionID = *((uint32_t *) &buf[pos]); pos += 4;
		stepID = *((uint32_t *) &buf[pos]); pos += 4;
		uniqueID = *((uint32_t *) &buf[pos]);
		if (0 < status)
		{
			sendErrorMsg(uniqueID, status, stepID);
			return ret;
		}

		//if (!(sessionID & 0x80000000))
		//cout << "got request for <" << sessionID <<", " << stepID << ">\n";
retry:
		scoped.lock();
		if (!bppv) {
			it = bppMap.find(uniqueID);
			if (it == bppMap.end()) {
				/* mitigate a small race between creation and use */
				scoped.unlock();
				if (++failCount > maxFailCount) {
#if 0   // for debugging
#ifndef _MSC_VER
					boost::posix_time::ptime pt = boost::posix_time::microsec_clock::local_time();
					if (sessionID & 0x80000000)
						cout << "BPPSeeder couldn't find the sessionID/stepID pair.  sessionID=" 
							<< (int) (sessionID ^ 0x80000000) << " stepID=" << stepID << " (syscat)" << pt << endl;
					else 
						cout << "BPPSeeder couldn't find the sessionID/stepID pair.  sessionID=" 
							<< sessionID << " stepID=" << stepID << pt << endl;
#endif
					throw logic_error("BPPSeeder couldn't find the sessionID/stepID pair");
#endif
					return 0;
				}
				if (!isSysCat()) 
					return -1;
				else {   // syscat queries aren't run by a threadpool, can't reschedule those jobs
					usleep(1000);
					goto retry;
				}
			}
			bppv = it->second;
		}
		bpp = bppv->next();
		scoped.unlock();
		if (!bpp) {
			if (isSysCat()) {
				usleep(1000);
				goto retry;
			}
			return -1;    // all BPP instances are busy, make threadpool reschedule
		}
		gotBPP = true;
		bpp->resetBPP(*bs, writelock, sock);
		firstRun = false;
	}   // firstRun
	
	
	if (fTrace)
	{
		PTLogsMap_t::iterator it;
#ifdef _MSC_VER
		tid = GetCurrentThreadId();
#else
		tid = pthread_self();
#endif

		// only lock map while inserted objects
		// once there is an object for each thread
		// there is not need to lock
		if (gFDList.size()<(uint)fPMThreads) {
			gFDMutex.lock();
			ptLock=true;
		}

		it = gFDList.find(tid);
		if (it==gFDList.end())
		{
			ostringstream LogFileName;
			SPPTLogs_t spof;
#ifdef _MSC_VER
			LogFileName << "C:/Calpont/log/trace/pt." << tid;
#else
			LogFileName << "/var/log/Calpont/trace/pt." << tid;
#endif
			spof.reset(new PTLogs_t(gThdCnt, LogFileName.str().c_str()));
			gThdCnt++;
			// TODO: add error checking
			if (spof->logFD.is_open()) {
				gFDList[tid] = spof;
				logFD = spof.get();
			}
		} else
			logFD =(*it).second.get();
		
		if (ptLock) {
			gFDMutex.unlock();
			ptLock=false;
		}
		clock_gettime(CLOCK_MONOTONIC, &tm);
  	} // if (fTrace)

	uint retries = 0;
restart:
	try {
		ret = (*bpp)();
	}
	catch (NeedToRestartJob &e) {
		ostringstream os;
		// experimentally the race can exist longer than 10s.  "No way" should
		// it take 10 minutes.  If it does, the user will have to resubmit their 
		// query.

		// 9/27/12 - changed the timeout to 2 mins b/c people report the system
		// is hung if it does nothing for 10 mins.  2 mins should still be more
		// than enough
		if (++retries == 120) {
			os << e.what() << ": Restarted a syscat job " << retries << " times, bailing\n";
			throw NeedToRestartJob(os.str());
		}
		flushSyscatOIDs();
		bs->rewind();
		bpp->resetBPP(*bs, writelock, sock);
#ifdef _MSC_VER
		Sleep(1 * 1000);
#else
		sleep(1);
#endif
		goto restart;
	}

	if (ret)
		return ret;

	if (fTrace)
		if (logFD && logFD->logFD.is_open()) {
			clock_gettime(CLOCK_MONOTONIC, &tm2);
			timespec_sub(tm, tm2, tm3);
			logFD->logFD
				<< left << setw(3) << logFD->thdId
				<< right << fixed << ((double)(tm.tv_sec+(1.e-9*tm.tv_nsec))) << " "
				<< right << fixed << tm3 << " "
				<< right << setw(6) << bpp->getSessionID() << " " 
				<< right << setw(4) << bpp->getStepID() << " " 
				<< right << setw(2) << bpp->FilterCount() << " " 
				<< right << setw(2) << bpp->ProjectCount() << " " 
				<< right << setw(9) << bpp->PhysIOCount() << " " 
				<< right << setw(9) << bpp->CachedIOCount() << " " 
				<< right << setw(9) << bpp->BlocksTouchedCount()
				<< endl;
		} // if (logFD...

	}
	catch (scalar_exception &se)
	{
		if (gotBPP)
			bpp->busy(false);
		if (ptLock) {
			gFDMutex.unlock();
			ptLock=false;
		}
	}
	catch(exception& ex)
	{
		if (gotBPP)
			bpp->busy(false);
		if (ptLock) {
			gFDMutex.unlock();
			ptLock=false;
		}
		catchHandler(ex.what(), uniqueID, stepID);
		cout << "BPPSeeder step " << stepID << " caught an exception: " <<  ex.what() << endl;
	}
	catch(...)
	{
		if (gotBPP)
			bpp->busy(false);
		if (ptLock) {
			gFDMutex.unlock();
			ptLock=false;
		}
		string msg("BPPSeeder caught an unknown exception");
		catchHandler(msg, uniqueID, stepID);
		cout << msg << endl;
	}
	return ret;
}

void BPPSeeder::catchHandler(const string& ex, uint32_t id, uint32_t step)
{
	Logger log;
	log.logMessage(ex);
	sendErrorMsg(id, logging::bppSeederErr, step);
}


void BPPSeeder::sendErrorMsg(uint32_t id, uint16_t status, uint32_t step)
{

	ISMPacketHeader ism;
	PrimitiveHeader ph = {0};
	
	ism.Status =  status;	
	ph.UniqueID = id;
	ph.StepID = step;
	ByteStream msg(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
	msg.append((uint8_t *) &ism, sizeof(ism));
	msg.append((uint8_t *) &ph, sizeof(ph));

	boost::mutex::scoped_lock lk(*writelock);
	sock->write(msg);
}

bool BPPSeeder::isSysCat()
{
	const uint8_t *buf;
	uint sessionIDOffset = sizeof(ISMPacketHeader);
	uint32_t sessionID;

	buf = bs->buf();
	sessionID = *((uint32_t *) &buf[sessionIDOffset]);
	return (sessionID & 0x80000000);
}

uint32_t BPPSeeder::getID()
{
	return uniqueID;
}

/* This is part of the syscat-retry hack.  We should get rid of it once we
 * track down the source of the problem.
 */
void BPPSeeder::flushSyscatOIDs()
{
	vector<BRM::OID_t> syscatOIDs;

	syscatOIDs = execplan::getAllSysCatOIDs();

	for (int i = 0; i < fCacheCount; i++) {
		dbbc::blockCacheClient bc(*BRPp[i]);
		bc.flushOIDs((const uint32_t *) &syscatOIDs[0], syscatOIDs.size());
	}
}

};

