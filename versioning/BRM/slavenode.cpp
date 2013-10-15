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

/*****************************************************************************
 * $Id: slavenode.cpp 1931 2013-07-08 16:53:02Z bpaul $
 *
 ****************************************************************************/

#include <iostream>
#include <signal.h>
#include <string>
#include <clocale>
#ifdef _MSC_VER
#include <process.h>
#include <winsock2.h>
#endif
#include "slavedbrmnode.h"
#include "slavecomm.h"
#include "liboamcpp.h"
#include "brmtypes.h"
#include "rwlockmonitor.h"

#include "utils_utf8.h"
#include "IDBPolicy.h"

using namespace BRM;
using namespace std;

SlaveComm *comm;
bool die;
boost::thread_group monitorThreads;

void fail()
{
	try {
		oam::Oam oam;

		oam.processInitFailure();
	}
	catch (exception&) {
		cerr << "failed to notify OAM of server failure" << endl;
	}
}

void stop(int sig)
{
	if (!die) {
		die = true;
		comm->stop();
		monitorThreads.interrupt_all();
	}
}

void reset(int sig)
{
        comm->reset();
}

int main(int argc, char **argv)
{

	// get and set locale language - BUG 5362
	string systemLang = "C";
	systemLang = funcexp::utf8::idb_setlocale();

	BRM::logInit ( BRM::SubSystemLogId_workerNode );

	string nodeName;
	SlaveDBRMNode slave;
	string arg;
	int err = 0;
	ShmKeys keys;

	if (argc < 2) {
		ostringstream os;
		os << "Usage: " << argv[0] << " DBRM_WorkerN";
		cerr << os.str() << endl;
		log(os.str());
		fail();
		exit(1);
	}

	idbdatafile::IDBPolicy::configIDBPolicy();

	nodeName = argv[1];	
	try {
		comm = new SlaveComm(nodeName, &slave);
	}
	catch (exception &e) {
		ostringstream os;
		os << "An error occured: " << e.what();
		cerr << os.str() << endl;
		log(os.str());
		fail();
		exit(1);
	}
#ifdef SIGHUP
 	signal(SIGHUP, reset);
#endif
 	signal(SIGINT, stop);
	signal(SIGTERM, stop);
#ifdef SIGPIPE
	signal(SIGPIPE, SIG_IGN);
#endif

	if (!(argc >= 3 && (arg = argv[2]) == "fg"))
		err = fork();

	if (err == 0) {

		/* Start 4 threads to monitor write lock state */
		monitorThreads.create_thread(RWLockMonitor
				(&die, slave.getEMFLLockStatus(), keys.KEYRANGE_EMFREELIST_BASE));
		monitorThreads.create_thread(RWLockMonitor
				(&die, slave.getEMLockStatus(), keys.KEYRANGE_EXTENTMAP_BASE));
		monitorThreads.create_thread(RWLockMonitor
				(&die, slave.getVBBMLockStatus(), keys.KEYRANGE_VBBM_BASE));
		monitorThreads.create_thread(RWLockMonitor
				(&die, slave.getVSSLockStatus(), keys.KEYRANGE_VSS_BASE));

		try {
			oam::Oam oam;
			
			oam.processInitComplete("DBRMWorkerNode");
		}
		catch (exception &e) {
			ostringstream os;
			os << "failed to notify OAM: " << e.what();
			os << " continuing anyway";
			cerr << os.str() << endl;
			log(os.str(), logging::LOG_TYPE_WARNING);
		}

		try {
			comm->run();
		}
		catch (exception &e) {
			ostringstream os;
			os << "An error occurred: " << e.what();
			cerr << os.str() << endl;
			log(os.str());
			exit(1);
		}
	}
	else if (err < 0) {
		perror(argv[0]);
		log_errno(string(argv[0]));
		fail();
	}

	exit(0);
}
