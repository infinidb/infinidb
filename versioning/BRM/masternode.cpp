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

/*****************************************************************************
 * $Id: masternode.cpp 1934 2013-07-08 21:44:52Z bpaul $
 *
 ****************************************************************************/

/*
 * The executable source that runs a Master DBRM Node.
 */

#include "masterdbrmnode.h"
#include "liboamcpp.h"
#include <unistd.h>
#include <signal.h>
#include <exception>
#include <string>
#include <clocale>
#include "brmtypes.h"
#include "utils_utf8.h"

#define MAX_RETRIES 10

BRM::MasterDBRMNode *m;
bool die;

using namespace std;
using namespace BRM;

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

void stop(int num)
{
#ifdef BRM_VERBOSE
	std::cerr << "stopping..." << std::endl;
#endif
	die = true;
	if (m != NULL)
		m->stop();
}

void restart(int num)
{
#ifdef BRM_VERBOSE
	std::cerr << "stopping this instance..." << std::endl;
#endif
	if (m != NULL)
		m->stop();
}

/*  doesn't quite work yet...
void reload(int num)
{
#ifdef BRM_VERBOSE
	std::cerr << "reloading the config file" << std::endl;
#endif
	m->lock();
	try {
		m->reload();
	}
	catch (std::exception &e) {
		m->setReadOnly(true);
		std::cerr << "Reload failed.  Check the config file.  Reverting to read-only mode." 
			<< std::endl;
	}
	m->unlock();
}
*/

int main(int argc, char **argv)
{
    // get and set locale language - BUG 5362
	string systemLang = "C";
	systemLang = funcexp::utf8::idb_setlocale();

	BRM::logInit ( BRM::SubSystemLogId_controllerNode );

	int retries = 0, err;
	std::string arg;

	die = false;

	if (!(argc >= 2 && (arg = argv[1]) == "fg")) {
		if ((err = fork()) < 0){
			perror(argv[0]);
			log_errno(string(argv[0]));
			fail();
			exit(1);
		}
		if (err > 0)
			exit(0);
	}

	(void)config::Config::makeConfig();

	/* XXXPAT: we might want to install signal handlers for every signal */

	signal(SIGINT, stop);
	signal(SIGTERM, stop);
#ifndef _MSC_VER
	signal(SIGHUP, SIG_IGN);
	signal(SIGUSR1, restart);
	signal(SIGPIPE, SIG_IGN);
#endif

	m = NULL;
	while (retries < MAX_RETRIES && !die) {
		try {
			if (m != NULL)
				delete m;
			m = new BRM::MasterDBRMNode();
			try {
				oam::Oam oam;

				oam.processInitComplete("DBRMControllerNode");
			}
			catch (exception &e) {
				ostringstream os;

				os << "failed to notify OAM: " << e.what();
				os << " continuing anyway";
				cerr << os.str() << endl;
				log(os.str(), logging::LOG_TYPE_WARNING);
			}
			m->run();
			retries = 0;
			delete m;
			m = NULL;
		}
		catch (std::exception &e) {
			ostringstream os;
			os << e.what();
			os << "... attempt #" << retries+1 << "/" << MAX_RETRIES << " to restart the  DBRM controller node";
			cerr << os.str() << endl;
			log(os.str());
			sleep(5);
		}
		retries++;
	}
	if (retries == MAX_RETRIES) {
		log(string("Exiting after too many errors"));
		fail();
	}
	std::cerr << "Exiting..." << std::endl;
	exit(0);
}

